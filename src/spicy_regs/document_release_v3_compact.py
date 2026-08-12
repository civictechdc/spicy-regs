"""Inactive-row measurement and atomic compaction for ``DocumentRelease`` v3."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from spicy_regs.document_release_v3 import (
    FORMAT,
    FORMAT_VERSION,
    ROLE_SCHEMA_IDS,
    TABLE_SCHEMAS,
    DocumentReleaseV3Error,
    canonical_json_bytes,
    canonical_json_text,
    release_id,
    require_memory_limit,
    sha256_bytes,
    sha256_file,
    validate_object_key,
)
from spicy_regs.document_release_v3_diff import release_member_paths
from spicy_regs.document_release_v3_verify import verify_release_or_raise
from spicy_regs.document_release_v3_writer import BoundedParquetWriter, atomic_publish_directory


@dataclass(frozen=True, slots=True)
class CompactionMetrics:
    """Exact referenced and inactive row totals used by compaction policy."""

    active_document_versions: int
    referenced_document_versions: int
    inactive_document_versions: int
    referenced_eligibility_rows: int
    inactive_eligibility_rows: int
    referenced_passages: int
    inactive_passages: int
    inactive_ratio: float
    delta_generations: int | None
    recommended: bool

    def as_dict(self) -> dict[str, int | float | bool | None]:
        return {
            "activeDocumentVersions": self.active_document_versions,
            "referencedDocumentVersions": self.referenced_document_versions,
            "inactiveDocumentVersions": self.inactive_document_versions,
            "referencedEligibilityRows": self.referenced_eligibility_rows,
            "inactiveEligibilityRows": self.inactive_eligibility_rows,
            "referencedPassages": self.referenced_passages,
            "inactivePassages": self.inactive_passages,
            "inactiveRatio": self.inactive_ratio,
            "deltaGenerations": self.delta_generations,
            "recommended": self.recommended,
        }


def _sql_paths(paths: tuple[Path, ...]) -> str:
    if not paths:
        raise DocumentReleaseV3Error("required release role has no member")
    return "[" + ",".join("'" + str(path).replace("'", "''") + "'" for path in paths) + "]"


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> Any:
    row = connection.execute(sql).fetchone()
    if row is None:
        raise DocumentReleaseV3Error("compaction aggregate query returned no row")
    return row[0]


def compaction_metrics(
    release_dir: Path,
    *,
    delta_generations: int | None = None,
    inactive_ratio_threshold: float = 0.10,
    delta_generation_threshold: int = 8,
    memory_limit: str = "512MB",
) -> CompactionMetrics:
    """Measure exact inactive rows without materializing corpus-sized sets."""

    require_memory_limit(memory_limit)
    if delta_generations is not None and delta_generations < 0:
        raise DocumentReleaseV3Error("delta_generations must be non-negative")
    if not 0 <= inactive_ratio_threshold <= 1:
        raise DocumentReleaseV3Error("inactive_ratio_threshold must be between zero and one")
    verify_release_or_raise(release_dir, memory_limit=memory_limit)
    current = release_member_paths(release_dir, "current-documents")
    documents = release_member_paths(release_dir, "documents")
    evidence = release_member_paths(release_dir, "eligibility-evidence")
    passages = release_member_paths(release_dir, "passages")
    connection = duckdb.connect()
    try:
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute(f"CREATE VIEW c AS SELECT * FROM read_parquet({_sql_paths(current)})")
        connection.execute(f"CREATE VIEW d AS SELECT * FROM read_parquet({_sql_paths(documents)})")
        connection.execute(f"CREATE VIEW e AS SELECT * FROM read_parquet({_sql_paths(evidence)})")
        connection.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet({_sql_paths(passages)})")
        active = int(_scalar(connection, "SELECT count(*) FROM c WHERE state='active'"))
        document_rows = int(_scalar(connection, "SELECT count(*) FROM d"))
        evidence_rows = int(_scalar(connection, "SELECT count(*) FROM e"))
        passage_rows = int(_scalar(connection, "SELECT count(*) FROM p"))
        inactive_documents = int(
            _scalar(
                connection,
                "SELECT count(*) FROM d LEFT JOIN c ON d.document_version_id=c.document_version_id "
                "AND c.state='active' WHERE c.document_version_id IS NULL",
            )
        )
        inactive_evidence = int(
            _scalar(
                connection,
                "SELECT count(*) FROM e LEFT JOIN c ON e.document_version_id=c.document_version_id "
                "AND c.state='active' WHERE c.document_version_id IS NULL",
            )
        )
        inactive_passages = int(
            _scalar(
                connection,
                "SELECT count(*) FROM p LEFT JOIN c ON p.document_version_id=c.document_version_id "
                "AND c.state='active' WHERE c.document_version_id IS NULL",
            )
        )
    finally:
        connection.close()
    referenced = document_rows + evidence_rows + passage_rows
    inactive = inactive_documents + inactive_evidence + inactive_passages
    ratio = 0.0 if referenced == 0 else inactive / referenced
    recommended = ratio > inactive_ratio_threshold or (
        delta_generations is not None and delta_generations > delta_generation_threshold
    )
    return CompactionMetrics(
        active_document_versions=active,
        referenced_document_versions=document_rows,
        inactive_document_versions=inactive_documents,
        referenced_eligibility_rows=evidence_rows,
        inactive_eligibility_rows=inactive_evidence,
        referenced_passages=passage_rows,
        inactive_passages=inactive_passages,
        inactive_ratio=ratio,
        delta_generations=delta_generations,
        recommended=recommended,
    )


def _load_json(path: Path) -> dict[str, Any]:
    from spicy_regs.document_release_v3 import parse_canonical_json

    value = parse_canonical_json(path.read_bytes(), label=path.name)
    if not isinstance(value, dict):
        raise DocumentReleaseV3Error(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    temporary.write_bytes(canonical_json_bytes(value))
    os.replace(temporary, path)


def _member_path(root: Path, descriptor: dict[str, Any]) -> Path:
    key = validate_object_key(descriptor["objectKey"], "member objectKey")
    return root.joinpath(*key.split("/"))


def _rewrite_filtered_role(
    root: Path,
    role: str,
    *,
    active_paths: tuple[Path, ...],
    row_batch_size: int,
    row_batch_utf8_bytes: int,
    memory_limit: str,
) -> None:
    member_paths = release_member_paths(root, role)
    connection = duckdb.connect()
    try:
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute(f"CREATE TEMP VIEW active AS SELECT * FROM read_parquet({_sql_paths(active_paths)})")
        for path in member_paths:
            escaped = str(path).replace("'", "''")
            query = connection.execute(
                f"SELECT source.* FROM read_parquet('{escaped}') source "
                "JOIN active ON source.document_version_id=active.document_version_id "
                "WHERE active.state='active'"
            ).fetch_record_batch(rows_per_batch=row_batch_size)
            replacement = path.with_name(f".{path.name}.compact")
            writer = BoundedParquetWriter(
                replacement,
                TABLE_SCHEMAS[ROLE_SCHEMA_IDS[role]],
                max_rows=row_batch_size,
                max_utf8_bytes=row_batch_utf8_bytes,
                compression="zstd",
            )
            try:
                for batch in query:
                    for row in batch.to_pylist():
                        writer.write(row)
            finally:
                writer.close()
            os.replace(replacement, path)
    finally:
        connection.close()


def _refresh_descriptor(root: Path, descriptor: dict[str, Any]) -> None:
    path = _member_path(root, descriptor)
    digest, size = sha256_file(path)
    descriptor["sha256"] = digest
    descriptor["byteSize"] = size
    if descriptor["recordCount"] is not None and descriptor["mediaType"] == "application/vnd.apache.parquet":
        descriptor["recordCount"] = pq.ParquetFile(path).metadata.num_rows


def _refresh_partition_receipt(
    root: Path,
    manifest: dict[str, Any],
    *,
    completed_at: str,
    row_batch_utf8_bytes: int,
) -> None:
    receipt_descriptor = next(member for member in manifest["members"] if member["role"] == "partition-receipt")
    other_members = [member for member in manifest["members"] if member["role"] != "partition-receipt"]
    for descriptor in other_members:
        _refresh_descriptor(root, descriptor)
    document_count = sum(member["recordCount"] or 0 for member in other_members if member["role"] == "documents")
    passage_count = sum(member["recordCount"] or 0 for member in other_members if member["role"] == "passages")
    rendition_bytes = 0
    for member in other_members:
        if member["role"] != "rendition-pack-index":
            continue
        path = _member_path(root, member)
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=2_000, columns=["byte_length"]):
            rendition_bytes += sum(batch.column("byte_length").to_pylist())
    task_key = sha256_bytes(
        canonical_json_bytes(
            {
                "stageIdentity": "document-release-v3-compaction",
                "inputDigests": sorted(member["sha256"] for member in other_members),
                "partitionId": manifest["scope"]["id"],
            }
        )
    )
    receipt_path = _member_path(root, receipt_descriptor)
    replacement = receipt_path.with_name(f".{receipt_path.name}.compact")
    writer = BoundedParquetWriter(
        replacement,
        TABLE_SCHEMAS[ROLE_SCHEMA_IDS["partition-receipt"]],
        max_rows=1,
        max_utf8_bytes=row_batch_utf8_bytes,
        compression="zstd",
    )
    writer.write(
        {
            "task_key": f"urn:spicy-regs:document-release-v3-task:{task_key}",
            "attempt_id": f"compaction-{uuid.uuid4()}",
            "partition_id": manifest["scope"]["id"],
            "state": "committed",
            "document_count": document_count,
            "passage_count": passage_count,
            "rendition_byte_count": rendition_bytes,
            "started_at": completed_at,
            "completed_at": completed_at,
            "member_digests_json": canonical_json_text(sorted(member["sha256"] for member in other_members)),
        }
    )
    writer.close()
    os.replace(replacement, receipt_path)
    _refresh_descriptor(root, receipt_descriptor)


def _refresh_manifest_counts(manifest: dict[str, Any]) -> None:
    manifest["members"].sort(key=lambda member: member["objectKey"])
    manifest["counts"] = {
        "memberCount": len(manifest["members"]),
        "totalByteSize": sum(member["byteSize"] for member in manifest["members"]),
        "totalRecordCount": sum(member["recordCount"] or 0 for member in manifest["members"]),
    }


def compact_release(
    source_release: Path,
    output_dir: Path,
    *,
    row_batch_size: int = 2_000,
    row_batch_utf8_bytes: int = 16 * 1024 * 1024,
    memory_limit: str = "512MB",
) -> Path:
    """Remove inactive document/evidence/passage rows and reseal atomically.

    Rendition packs remain content-addressed and immutable.  Keeping unused pack
    ranges avoids rewriting large source bytes during row compaction; a later
    pack-garbage-collection operation can deduplicate those immutable objects.
    """

    require_memory_limit(memory_limit)
    source_release = Path(source_release).resolve()
    output_dir = Path(output_dir).resolve()
    verify_release_or_raise(source_release, memory_limit=memory_limit)
    if output_dir.exists() or output_dir.is_symlink():
        raise DocumentReleaseV3Error(f"refusing to replace existing compaction output: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.compacting-", dir=output_dir.parent))
    try:
        shutil.copytree(source_release, work, dirs_exist_ok=True, copy_function=shutil.copy2)
        active_paths = release_member_paths(source_release, "current-documents")
        # Resolve copied paths, not source paths, for the compaction join.
        copied_active_paths = tuple(work / path.relative_to(source_release) for path in active_paths)
        for role in ("documents", "eligibility-evidence", "passages"):
            _rewrite_filtered_role(
                work,
                role,
                active_paths=copied_active_paths,
                row_batch_size=row_batch_size,
                row_batch_utf8_bytes=row_batch_utf8_bytes,
                memory_limit=memory_limit,
            )

        source_root = _load_json(source_release / "release.json")
        completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        content = source_root["content"]
        partition_references: list[dict[str, Any]] = []
        all_members: list[dict[str, Any]] = []
        for reference in content["partitionManifests"]:
            path = work / reference["objectKey"]
            manifest = _load_json(path)
            _refresh_partition_receipt(
                work,
                manifest,
                completed_at=completed_at,
                row_batch_utf8_bytes=row_batch_utf8_bytes,
            )
            _refresh_manifest_counts(manifest)
            _write_json(path, manifest)
            digest, size = sha256_file(path)
            refreshed = dict(reference)
            refreshed["sha256"] = digest
            refreshed["byteSize"] = size
            partition_references.append(refreshed)
            all_members.extend(manifest["members"])

        global_reference = content["globalManifest"]
        global_path = work / global_reference["objectKey"]
        global_manifest = _load_json(global_path)
        build_receipt_descriptor = next(
            member for member in global_manifest["members"] if member["role"] == "build-receipt"
        )
        for descriptor in global_manifest["members"]:
            if descriptor["role"] != "build-receipt":
                _refresh_descriptor(work, descriptor)

        connection = duckdb.connect()
        try:
            current = release_member_paths(work, "current-documents")
            documents = release_member_paths(work, "documents")
            evidence = release_member_paths(work, "eligibility-evidence")
            passages = release_member_paths(work, "passages")
            failures = release_member_paths(work, "failures")
            connection.execute(f"CREATE VIEW c AS SELECT * FROM read_parquet({_sql_paths(current)})")
            connection.execute(f"CREATE VIEW d AS SELECT * FROM read_parquet({_sql_paths(documents)})")
            connection.execute(f"CREATE VIEW e AS SELECT * FROM read_parquet({_sql_paths(evidence)})")
            connection.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet({_sql_paths(passages)})")
            connection.execute(f"CREATE VIEW f AS SELECT * FROM read_parquet({_sql_paths(failures)})")
            counts = dict(content["counts"])
            counts["documentVersionCount"] = int(_scalar(connection, "SELECT count(*) FROM d"))
            counts["eligibilityEvidenceCount"] = int(_scalar(connection, "SELECT count(*) FROM e"))
            counts["passageCount"] = int(_scalar(connection, "SELECT count(*) FROM p"))
            counts["failureRecordCount"] = int(_scalar(connection, "SELECT count(*) FROM f"))
            coverage = dict(content["coverage"])
            coverage["normalizedTextUtf8ByteCount"] = int(
                _scalar(connection, "SELECT coalesce(sum(octet_length(encode(normalized_text))),0) FROM d")
            )
            coverage["passageTextUtf8ByteCount"] = int(
                _scalar(connection, "SELECT coalesce(sum(octet_length(encode(text))),0) FROM p")
            )
        finally:
            connection.close()

        receipt_path = _member_path(work, build_receipt_descriptor)
        prior_receipt = _load_json(receipt_path)
        prior_receipt["outputIdentities"] = [
            {"manifestId": reference["manifestId"], "sha256": reference["sha256"]} for reference in partition_references
        ]
        prior_receipt["counts"] = counts
        prior_receipt["coverage"] = coverage
        prior_receipt["completedAt"] = completed_at
        prior_receipt["configurationIdentity"] = (
            "urn:spicy-regs:document-release-v3-compaction:"
            + hashlib.sha256(str(source_root["releaseId"]).encode()).hexdigest()
        )
        _write_json(receipt_path, prior_receipt)
        _refresh_descriptor(work, build_receipt_descriptor)
        _refresh_manifest_counts(global_manifest)
        _write_json(global_path, global_manifest)
        global_digest, global_size = sha256_file(global_path)
        refreshed_global_reference = dict(global_reference)
        refreshed_global_reference["sha256"] = global_digest
        refreshed_global_reference["byteSize"] = global_size
        all_members.extend(global_manifest["members"])

        counts["partitionManifestCount"] = len(partition_references)
        counts["memberCount"] = len(all_members)
        counts["totalMemberByteSize"] = sum(member["byteSize"] for member in all_members)
        new_content = dict(content)
        new_content["globalManifest"] = refreshed_global_reference
        new_content["partitionManifests"] = sorted(partition_references, key=lambda reference: reference["manifestId"])
        new_content["counts"] = counts
        new_content["coverage"] = coverage
        new_root = {
            "format": FORMAT,
            "formatVersion": FORMAT_VERSION,
            "releaseId": release_id(new_content),
            "content": new_content,
            "annotations": {
                "createdAt": completed_at,
                "buildRunId": f"document-release-v3-compaction-{uuid.uuid4()}",
                "compactedFrom": source_root["releaseId"],
            },
        }
        _write_json(work / "release.json", new_root)
        verify_release_or_raise(work, memory_limit=memory_limit)
        atomic_publish_directory(work, output_dir)
        return output_dir
    except Exception:
        raise

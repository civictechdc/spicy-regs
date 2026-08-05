"""Bounded writers and atomic publication for ``DocumentRelease`` v3.

The reference vertical slice accepts a JSON Lines selection ledger.  It keeps
only one source record and bounded Arrow row batches in memory, writes rendition
packs while hashing them, seals every member in subordinate manifests, verifies
the complete temporary distribution, and exposes the release with one atomic
directory rename.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.document_release_v3 import (
    BUILD_RECEIPT_SCHEMA_ID,
    CANONICAL_JSON_PROFILE,
    ELIGIBILITY_DATA_SCHEMA_ID,
    FORMAT,
    FORMAT_VERSION,
    PARTITION_RECEIPT_SCHEMA_ID,
    RENDITION_PACK_INDEX_SCHEMA_ID,
    RENDITION_UTF8_COORDINATE_SCHEMA_ID,
    ROLE_SCHEMA_IDS,
    TABLE_SCHEMAS,
    DocumentReleaseV3Error,
    ManifestReference,
    MemberDescriptor,
    canonical_json_bytes,
    canonical_json_text,
    make_subordinate_manifest,
    release_id,
    require_memory_limit,
    require_sha256,
    role_for_schema,
    schema_documents,
    schema_set_id,
    sha256_bytes,
    sha256_file,
)


Disposition = Literal["active", "deleted", "excluded", "accepted-failure"]
EligibilityState = Literal["eligible", "ineligible", "unverified"]

_PARTITION_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_UTF8_TEXTUAL_MEDIA_TYPES = frozenset(
    {
        "application/json",
        "application/json; charset=utf-8",
        "application/xhtml+xml",
        "application/xml",
        "application/xml; charset=utf-8",
        "text/html",
        "text/html; charset=utf-8",
        "text/plain",
        "text/plain; charset=utf-8",
        "text/xml",
        "text/xml; charset=utf-8",
    }
)
_SOURCE_INPUT_KEYS = frozenset(
    {
        "documentId",
        "sourceInputId",
        "sourceId",
        "sourcePartition",
        "disposition",
        "previousActive",
        "oldDocumentVersionId",
        "oldEligibilityState",
        "sourceRecordId",
        "sourceVersion",
        "renditionPath",
        "mediaType",
        "title",
        "publishedAt",
        "updatedAt",
        "documentType",
        "language",
        "eligibilityState",
        "eligibilityAuthorityId",
        "eligibilityEvidenceKind",
        "eligibilityBasis",
        "eligibilityReasonCode",
        "exclusionPolicyId",
        "failureId",
        "failureStage",
        "failureClass",
        "failureRetryable",
        "failureAttemptCount",
        "failureDiagnosticCode",
        "failureFinalDisposition",
    }
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_string(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value:
        raise DocumentReleaseV3Error(f"{label} must be a non-empty string")
    return value


def _parse_timestamp(value: str | None, label: str) -> datetime | None:
    if value is None:
        return None
    if not _RFC3339_UTC.fullmatch(value):
        raise DocumentReleaseV3Error(f"{label} must be an RFC 3339 UTC instant")
    return datetime.fromisoformat(value.removesuffix("Z") + "+00:00")


def _stable_urn(kind: str, value: Mapping[str, Any]) -> str:
    return f"urn:spicyregs:{kind}:v3:{sha256_bytes(canonical_json_bytes(dict(value)))}"


@dataclass(frozen=True, slots=True)
class SourceInput:
    """One selected source input or prior-only deletion in the build ledger."""

    document_id: str
    source_input_id: str | None
    source_id: str
    source_partition: str | None
    disposition: Disposition
    previous_active: bool = False
    old_document_version_id: str | None = None
    old_eligibility_state: str | None = None
    source_record_id: str | None = None
    source_version: str | None = None
    rendition_path: Path | None = None
    media_type: str | None = None
    title: str | None = None
    published_at: str | None = None
    updated_at: str | None = None
    document_type: str | None = None
    language: str | None = None
    eligibility_state: EligibilityState | None = None
    eligibility_authority_id: str | None = None
    eligibility_evidence_kind: str | None = None
    eligibility_basis: str | None = None
    eligibility_reason_code: str | None = None
    exclusion_policy_id: str | None = None
    failure_id: str | None = None
    failure_stage: str | None = None
    failure_class: str | None = None
    failure_retryable: bool | None = None
    failure_attempt_count: int | None = None
    failure_diagnostic_code: str | None = None
    failure_final_disposition: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], *, base_path: Path) -> SourceInput:
        unexpected = set(value) - _SOURCE_INPUT_KEYS
        if unexpected:
            raise DocumentReleaseV3Error(f"source input has unknown fields: {sorted(unexpected)}")
        disposition = value.get("disposition")
        if disposition not in {"active", "deleted", "excluded", "accepted-failure"}:
            raise DocumentReleaseV3Error(f"source input disposition is invalid: {disposition!r}")
        source_input_id = _require_string(value.get("sourceInputId"), "sourceInputId", nullable=True)
        previous_active = value.get("previousActive", False)
        if not isinstance(previous_active, bool):
            raise DocumentReleaseV3Error("previousActive must be boolean")
        if source_input_id is None and not (disposition == "deleted" and previous_active):
            raise DocumentReleaseV3Error("sourceInputId may be null only for a prior-only deletion")
        rendition_value = value.get("renditionPath")
        rendition_path: Path | None = None
        if rendition_value is not None:
            rendition_text = _require_string(rendition_value, "renditionPath")
            assert rendition_text is not None
            candidate = Path(rendition_text)
            rendition_path = candidate if candidate.is_absolute() else base_path / candidate
            rendition_path = rendition_path.resolve()
        item = cls(
            document_id=str(_require_string(value.get("documentId"), "documentId")),
            source_input_id=source_input_id,
            source_id=str(_require_string(value.get("sourceId"), "sourceId")),
            source_partition=_require_string(value.get("sourcePartition"), "sourcePartition", nullable=True),
            disposition=disposition,
            previous_active=previous_active,
            old_document_version_id=_require_string(
                value.get("oldDocumentVersionId"), "oldDocumentVersionId", nullable=True
            ),
            old_eligibility_state=_require_string(
                value.get("oldEligibilityState"), "oldEligibilityState", nullable=True
            ),
            source_record_id=_require_string(value.get("sourceRecordId"), "sourceRecordId", nullable=True),
            source_version=_require_string(value.get("sourceVersion"), "sourceVersion", nullable=True),
            rendition_path=rendition_path,
            media_type=_require_string(value.get("mediaType"), "mediaType", nullable=True),
            title=_require_string(value.get("title"), "title", nullable=True),
            published_at=_require_string(value.get("publishedAt"), "publishedAt", nullable=True),
            updated_at=_require_string(value.get("updatedAt"), "updatedAt", nullable=True),
            document_type=_require_string(value.get("documentType"), "documentType", nullable=True),
            language=_require_string(value.get("language"), "language", nullable=True),
            eligibility_state=value.get("eligibilityState"),
            eligibility_authority_id=_require_string(
                value.get("eligibilityAuthorityId"), "eligibilityAuthorityId", nullable=True
            ),
            eligibility_evidence_kind=_require_string(
                value.get("eligibilityEvidenceKind"), "eligibilityEvidenceKind", nullable=True
            ),
            eligibility_basis=_require_string(value.get("eligibilityBasis"), "eligibilityBasis", nullable=True),
            eligibility_reason_code=_require_string(
                value.get("eligibilityReasonCode"), "eligibilityReasonCode", nullable=True
            ),
            exclusion_policy_id=_require_string(value.get("exclusionPolicyId"), "exclusionPolicyId", nullable=True),
            failure_id=_require_string(value.get("failureId"), "failureId", nullable=True),
            failure_stage=_require_string(value.get("failureStage"), "failureStage", nullable=True),
            failure_class=_require_string(value.get("failureClass"), "failureClass", nullable=True),
            failure_retryable=value.get("failureRetryable"),
            failure_attempt_count=value.get("failureAttemptCount"),
            failure_diagnostic_code=_require_string(
                value.get("failureDiagnosticCode"), "failureDiagnosticCode", nullable=True
            ),
            failure_final_disposition=_require_string(
                value.get("failureFinalDisposition"), "failureFinalDisposition", nullable=True
            ),
        )
        item.validate()
        return item

    def validate(self) -> None:
        if self.disposition == "active":
            required = {
                "source_record_id": self.source_record_id,
                "source_version": self.source_version,
                "rendition_path": self.rendition_path,
                "media_type": self.media_type,
                "document_type": self.document_type,
                "language": self.language,
                "eligibility_state": self.eligibility_state,
                "eligibility_authority_id": self.eligibility_authority_id,
                "eligibility_evidence_kind": self.eligibility_evidence_kind,
                "eligibility_basis": self.eligibility_basis,
                "eligibility_reason_code": self.eligibility_reason_code,
            }
            missing = sorted(name for name, value in required.items() if value is None)
            if missing:
                raise DocumentReleaseV3Error(f"active source input is missing fields: {missing}")
            if self.eligibility_state not in {"eligible", "ineligible", "unverified"}:
                raise DocumentReleaseV3Error("active source input has an invalid eligibilityState")
            if self.eligibility_evidence_kind not in {
                "source-assertion",
                "deterministic-policy",
                "sealed-qualification",
            }:
                raise DocumentReleaseV3Error("active source input has an invalid eligibilityEvidenceKind")
            assert self.rendition_path is not None
            if not self.rendition_path.is_file():
                raise DocumentReleaseV3Error(f"rendition does not exist: {self.rendition_path}")
            if self.media_type not in _UTF8_TEXTUAL_MEDIA_TYPES:
                raise DocumentReleaseV3Error(
                    "the reference producer accepts exact UTF-8 plain text, HTML, XML, and JSON media types only"
                )
            _parse_timestamp(self.published_at, "publishedAt")
            _parse_timestamp(self.updated_at, "updatedAt")
        elif self.disposition == "deleted":
            if self.exclusion_policy_id is not None or self.failure_id is not None:
                raise DocumentReleaseV3Error("deleted input cannot name exclusion or failure state")
        elif self.disposition == "excluded":
            if self.exclusion_policy_id is None:
                raise DocumentReleaseV3Error("excluded input requires exclusionPolicyId")
        elif self.disposition == "accepted-failure":
            required_failure = (
                self.failure_id,
                self.failure_stage,
                self.failure_class,
                self.failure_retryable,
                self.failure_attempt_count,
                self.failure_diagnostic_code,
                self.failure_final_disposition,
            )
            if any(value is None for value in required_failure):
                raise DocumentReleaseV3Error("accepted-failure input requires complete failure fields")
            if self.failure_final_disposition != "accepted-terminal":
                raise DocumentReleaseV3Error("accepted-failure requires final disposition accepted-terminal")


def iter_source_inputs(path: Path) -> Iterator[SourceInput]:
    """Stream a closed JSON Lines source-selection ledger."""

    path = Path(path).resolve()
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise DocumentReleaseV3Error(f"source input line {line_number} is invalid JSON: {error}") from error
            if not isinstance(value, dict):
                raise DocumentReleaseV3Error(f"source input line {line_number} must be an object")
            try:
                yield SourceInput.from_dict(value, base_path=path.parent)
            except DocumentReleaseV3Error as error:
                raise DocumentReleaseV3Error(f"source input line {line_number}: {error}") from error


@dataclass(frozen=True, slots=True)
class BuildConfig:
    """Identity-bearing policy and bounded-resource settings for one release."""

    implementation_id: str
    implementation_version: str
    runtime_profile_id: str
    processing_policy_id: str
    normalizer_id: str
    segmenter_id: str
    rendition_policy_id: str
    eligibility_policy_id: str
    failure_policy_id: str
    diagnostic_registry_id: str
    selection_id: str
    selector_type: str
    selector_digest: str
    effective_at: str
    partition_id: str = "default"
    source_revision: str | None = None
    previous_release_id: str | None = None
    previous_artifact_digest: str | None = None
    row_batch_size: int = 2_000
    row_batch_utf8_bytes: int = 16 * 1024 * 1024
    max_passage_utf8_bytes: int = 1 * 1024 * 1024
    max_rendition_pack_bytes: int = 512 * 1024 * 1024
    max_document_bytes: int = 64 * 1024 * 1024
    max_oversized_document_bytes: int = 1 * 1024 * 1024 * 1024
    compression: str = "zstd"
    build_run_id: str = field(default_factory=lambda: f"document-release-v3-{uuid.uuid4()}")
    created_at: str = field(default_factory=_utc_now)
    build_started_at: str | None = None
    build_completed_at: str | None = None

    def __post_init__(self) -> None:
        string_fields = (
            "implementation_id",
            "implementation_version",
            "runtime_profile_id",
            "processing_policy_id",
            "normalizer_id",
            "segmenter_id",
            "rendition_policy_id",
            "eligibility_policy_id",
            "failure_policy_id",
            "diagnostic_registry_id",
            "selection_id",
            "selector_type",
            "partition_id",
            "build_run_id",
        )
        for name in string_fields:
            _require_string(getattr(self, name), name)
        if _PARTITION_ID.fullmatch(self.partition_id) is None:
            raise DocumentReleaseV3Error("partition_id must use portable lowercase filename characters")
        require_sha256(self.selector_digest, "selector_digest")
        if not _RFC3339_UTC.fullmatch(self.effective_at):
            raise DocumentReleaseV3Error("effective_at must be an RFC 3339 UTC instant")
        if not _RFC3339_UTC.fullmatch(self.created_at):
            raise DocumentReleaseV3Error("created_at must be an RFC 3339 UTC instant")
        for name in ("build_started_at", "build_completed_at"):
            value = getattr(self, name)
            if value is not None and not _RFC3339_UTC.fullmatch(value):
                raise DocumentReleaseV3Error(f"{name} must be an RFC 3339 UTC instant")
        if (self.previous_release_id is None) != (self.previous_artifact_digest is None):
            raise DocumentReleaseV3Error("previous release id and artifact digest must be supplied together")
        if self.previous_artifact_digest is not None:
            require_sha256(self.previous_artifact_digest, "previous_artifact_digest")
        for name in (
            "row_batch_size",
            "row_batch_utf8_bytes",
            "max_passage_utf8_bytes",
            "max_rendition_pack_bytes",
            "max_document_bytes",
            "max_oversized_document_bytes",
        ):
            if getattr(self, name) <= 0:
                raise DocumentReleaseV3Error(f"{name} must be greater than zero")
        if self.max_oversized_document_bytes < self.max_document_bytes:
            raise DocumentReleaseV3Error("max_oversized_document_bytes must be at least max_document_bytes")

    def identity(self, *, partition_ids: Sequence[str] | None = None) -> str:
        semantic = {
            "implementationId": self.implementation_id,
            "implementationVersion": self.implementation_version,
            "runtimeProfileId": self.runtime_profile_id,
            "processingPolicyId": self.processing_policy_id,
            "normalizerId": self.normalizer_id,
            "segmenterId": self.segmenter_id,
            "renditionPolicyId": self.rendition_policy_id,
            "eligibilityPolicyId": self.eligibility_policy_id,
            "failurePolicyId": self.failure_policy_id,
            "diagnosticRegistryId": self.diagnostic_registry_id,
            "selectionId": self.selection_id,
            "selectorType": self.selector_type,
            "selectorDigest": self.selector_digest,
            "effectiveAt": self.effective_at,
            "rowBatchSize": self.row_batch_size,
            "rowBatchUtf8Bytes": self.row_batch_utf8_bytes,
            "maxPassageUtf8Bytes": self.max_passage_utf8_bytes,
            "maxRenditionPackBytes": self.max_rendition_pack_bytes,
            "maxDocumentBytes": self.max_document_bytes,
            "maxOversizedDocumentBytes": self.max_oversized_document_bytes,
            "compression": self.compression,
        }
        if partition_ids is None:
            semantic["partitionId"] = self.partition_id
        else:
            semantic["partitionIds"] = list(partition_ids)
        return "urn:spicyregs:document-release-v3-config:" + sha256_bytes(canonical_json_bytes(semantic))


class BoundedParquetWriter:
    """Write exact-schema Parquet row groups under row and UTF-8 limits."""

    def __init__(
        self,
        path: Path,
        schema: pa.Schema,
        *,
        max_rows: int,
        max_utf8_bytes: int,
        compression: str,
    ) -> None:
        self.path = path
        self.schema = schema
        self.max_rows = max_rows
        self.max_utf8_bytes = max_utf8_bytes
        self._writer = pq.ParquetWriter(path, schema, compression=compression, version="2.6")
        self._rows: list[dict[str, Any]] = []
        self._utf8_bytes = 0
        self.record_count = 0
        self.max_buffered_rows = 0
        self.max_buffered_utf8_bytes = 0
        self._closed = False

    @staticmethod
    def _estimated_utf8_bytes(row: Mapping[str, Any]) -> int:
        total = 0
        for value in row.values():
            if isinstance(value, str):
                total += len(value.encode("utf-8"))
            elif isinstance(value, bytes):
                total += len(value)
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                total += sum(len(item.encode("utf-8")) for item in value if isinstance(item, str))
        return total

    def write(self, row: Mapping[str, Any]) -> None:
        if self._closed:
            raise DocumentReleaseV3Error(f"writer for {self.path} is already closed")
        row_bytes = self._estimated_utf8_bytes(row)
        if self._rows and (len(self._rows) >= self.max_rows or self._utf8_bytes + row_bytes > self.max_utf8_bytes):
            self.flush()
        self._rows.append(dict(row))
        self._utf8_bytes += row_bytes
        self.max_buffered_rows = max(self.max_buffered_rows, len(self._rows))
        self.max_buffered_utf8_bytes = max(self.max_buffered_utf8_bytes, self._utf8_bytes)
        if len(self._rows) >= self.max_rows or self._utf8_bytes >= self.max_utf8_bytes:
            self.flush()

    def flush(self) -> None:
        if not self._rows:
            return
        table = pa.Table.from_pylist(self._rows, schema=self.schema)
        self._writer.write_table(table, row_group_size=len(self._rows))
        self.record_count += len(self._rows)
        self._rows.clear()
        self._utf8_bytes = 0

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        if self.record_count == 0:
            self._writer.write_table(self.schema.empty_table())
        self._writer.close()
        self._closed = True

    def __enter__(self) -> BoundedParquetWriter:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class _PackOutput:
    pack_path: Path
    index_path: Path
    rendition_count: int
    rendition_bytes: int


class RenditionPackWriter:
    """Stream immutable rendition bytes into bounded packs and exact indexes."""

    def __init__(self, root: Path, config: BuildConfig, *, shared_seen_path: Path | None = None) -> None:
        self.root = root
        self.config = config
        self.outputs: list[_PackOutput] = []
        self._sequence = 0
        self._pack_stream: Any | None = None
        self._pack_path: Path | None = None
        self._index_writer: BoundedParquetWriter | None = None
        self._index_path: Path | None = None
        self._size = 0
        self._count = 0
        self._shared_seen = shared_seen_path is not None
        self._seen_path = shared_seen_path or self.root / "renditions" / ".rendition-seen.sqlite"
        self._seen = sqlite3.connect(self._seen_path)
        self._seen.execute(
            "CREATE TABLE IF NOT EXISTS seen_renditions "
            "(digest BLOB PRIMARY KEY, media_type TEXT NOT NULL) WITHOUT ROWID"
        )

    def _open_pack(self) -> None:
        suffix = f"{self.config.partition_id}-{self._sequence:05d}"
        self._sequence += 1
        self._pack_path = self.root / "renditions" / f"pack-{suffix}.bin"
        self._index_path = self.root / "renditions" / f"pack-index-{suffix}.parquet"
        self._pack_stream = self._pack_path.open("xb")
        self._index_writer = BoundedParquetWriter(
            self._index_path,
            TABLE_SCHEMAS[RENDITION_PACK_INDEX_SCHEMA_ID],
            max_rows=self.config.row_batch_size,
            max_utf8_bytes=self.config.row_batch_utf8_bytes,
            compression=self.config.compression,
        )
        self._size = 0
        self._count = 0

    def _close_pack(self) -> None:
        if self._pack_stream is None:
            return
        self._pack_stream.flush()
        os.fsync(self._pack_stream.fileno())
        self._pack_stream.close()
        assert self._index_writer is not None
        assert self._pack_path is not None
        assert self._index_path is not None
        self._index_writer.close()
        self.outputs.append(
            _PackOutput(
                pack_path=self._pack_path,
                index_path=self._index_path,
                rendition_count=self._count,
                rendition_bytes=self._size,
            )
        )
        self._pack_stream = None
        self._pack_path = None
        self._index_writer = None
        self._index_path = None

    def add(self, rendition: bytes, *, media_type: str) -> str:
        digest = hashlib.sha256(rendition).digest()
        seen = self._seen.execute("SELECT media_type FROM seen_renditions WHERE digest=?", (digest,)).fetchone()
        if seen is not None:
            if seen[0] != media_type:
                raise DocumentReleaseV3Error("identical rendition bytes were declared with different media types")
            return digest.hex()
        if self._pack_stream is None:
            self._open_pack()
        elif self._size and self._size + len(rendition) > self.config.max_rendition_pack_bytes:
            self._close_pack()
            self._open_pack()
        assert self._pack_stream is not None
        assert self._index_writer is not None
        offset = self._size
        view = memoryview(rendition)
        for start in range(0, len(view), 1 << 20):
            self._pack_stream.write(view[start : start + (1 << 20)])
        self._index_writer.write(
            {
                "rendition_digest": digest,
                "byte_offset": offset,
                "byte_length": len(rendition),
                "media_type": media_type,
            }
        )
        self._size += len(rendition)
        self._count += 1
        self._seen.execute("INSERT INTO seen_renditions(digest,media_type) VALUES (?,?)", (digest, media_type))
        return digest.hex()

    def close(self) -> None:
        self._close_pack()
        self._seen.commit()
        self._seen.close()
        if not self._shared_seen:
            self._seen_path.unlink(missing_ok=True)


@dataclass(slots=True)
class _BuildState:
    selected_document_count: int = 0
    previous_active_document_count: int = 0
    universe_count: int = 0
    active_document_count: int = 0
    deleted_document_count: int = 0
    excluded_document_count: int = 0
    accepted_failure_count: int = 0
    document_version_count: int = 0
    passage_count: int = 0
    rendition_count: int = 0
    eligibility_evidence_count: int = 0
    source_disposition_count: int = 0
    failure_count: int = 0
    normalized_text_bytes: int = 0
    passage_text_bytes: int = 0
    rendition_bytes: int = 0
    retry_count: int = 0
    max_document_bytes_observed: int = 0
    max_writer_buffer_rows: int = 0
    max_writer_buffer_utf8_bytes: int = 0
    sources: set[str] = field(default_factory=set)
    eligibility: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    source_rollups: dict[str, dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))

    def merge(self, other: _BuildState) -> None:
        for name in (
            "selected_document_count",
            "previous_active_document_count",
            "universe_count",
            "active_document_count",
            "deleted_document_count",
            "excluded_document_count",
            "accepted_failure_count",
            "document_version_count",
            "passage_count",
            "rendition_count",
            "eligibility_evidence_count",
            "source_disposition_count",
            "failure_count",
            "normalized_text_bytes",
            "passage_text_bytes",
            "rendition_bytes",
            "retry_count",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        for name in (
            "max_document_bytes_observed",
            "max_writer_buffer_rows",
            "max_writer_buffer_utf8_bytes",
        ):
            setattr(self, name, max(getattr(self, name), getattr(other, name)))
        self.sources.update(other.sources)
        for eligibility, count in other.eligibility.items():
            self.eligibility[eligibility] += count
        for source_id, rollup in other.source_rollups.items():
            for disposition, count in rollup.items():
                self.source_rollups[source_id][disposition] += count

    def counts(self, *, member_count: int, member_bytes: int, partition_count: int) -> dict[str, int]:
        return {
            "selectedDocumentCount": self.selected_document_count,
            "previousActiveDocumentCount": self.previous_active_document_count,
            "reconciliationUniverseCount": self.universe_count,
            "activeDocumentCount": self.active_document_count,
            "deletedDocumentCount": self.deleted_document_count,
            "excludedDocumentCount": self.excluded_document_count,
            "acceptedTerminalFailureCount": self.accepted_failure_count,
            "documentVersionCount": self.document_version_count,
            "passageCount": self.passage_count,
            "renditionCount": self.rendition_count,
            "eligibilityEvidenceCount": self.eligibility_evidence_count,
            "sourceDispositionCount": self.source_disposition_count,
            "failureRecordCount": self.failure_count,
            "partitionManifestCount": partition_count,
            "memberCount": member_count,
            "totalMemberByteSize": member_bytes,
        }

    def coverage(self) -> dict[str, int]:
        return {
            "sourceCount": len(self.sources),
            "eligibleActiveDocumentCount": self.eligibility["eligible"],
            "ineligibleActiveDocumentCount": self.eligibility["ineligible"],
            "unverifiedActiveDocumentCount": self.eligibility["unverified"],
            "normalizedTextUtf8ByteCount": self.normalized_text_bytes,
            "passageTextUtf8ByteCount": self.passage_text_bytes,
            "renditionByteCount": self.rendition_bytes,
        }


def _segment_utf8(text: str, max_bytes: int) -> Iterator[tuple[int, int, str]]:
    encoded = text.encode("utf-8")
    if not encoded:
        yield 0, 0, ""
        return
    start = 0
    while start < len(encoded):
        end = min(start + max_bytes, len(encoded))
        while end < len(encoded) and (encoded[end] & 0xC0) == 0x80:
            end -= 1
        if end == start:
            end = start + 1
            while end < len(encoded) and (encoded[end] & 0xC0) == 0x80:
                end += 1
        yield start, end, encoded[start:end].decode("utf-8")
        start = end


def _member_descriptor(
    root: Path,
    path: Path,
    *,
    role: str,
    record_count: int | None,
    schema_id: str | None,
    partition_id: str | None,
    media_type: str,
) -> MemberDescriptor:
    digest, size = sha256_file(path)
    return MemberDescriptor(
        object_key=path.relative_to(root).as_posix(),
        role=role,
        media_type=media_type,
        byte_size=size,
        sha256=digest,
        record_count=record_count,
        schema_id=schema_id,
        partition_id=partition_id,
    )


def _write_canonical_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_json_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())


def atomic_publish_directory(work_root: Path, output_dir: Path) -> None:
    """Rename a private build directory under a conditional publication lock."""

    work_root = Path(work_root).resolve()
    output_dir = Path(output_dir).resolve()
    lock_path = output_dir.parent / f".{output_dir.name}.publish.lock"
    try:
        descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise DocumentReleaseV3Error(f"publication is already in progress for: {output_dir}") from error
    try:
        with os.fdopen(descriptor, "wb") as lock:
            lock.write(work_root.name.encode("utf-8"))
            lock.flush()
            os.fsync(lock.fileno())
        if output_dir.exists() or output_dir.is_symlink():
            raise DocumentReleaseV3Error(f"refusing to replace existing output: {output_dir}")
        os.rename(work_root, output_dir)
    finally:
        lock_path.unlink(missing_ok=True)


def _inventory_digest(dispositions_paths: Sequence[Path], *, temp_directory: Path, memory_limit: str) -> str:
    connection = duckdb.connect()
    try:
        connection.execute(f"SET memory_limit = '{memory_limit}'")
        escaped_temp = str(temp_directory).replace("'", "''")
        connection.execute(f"SET temp_directory = '{escaped_temp}'")
        cursor = connection.execute(
            "SELECT document_id, source_input_id FROM read_parquet(?) "
            "WHERE selected_current ORDER BY document_id, source_input_id",
            [[str(path) for path in dispositions_paths]],
        )
        # DuckDB's NOCASE order is not the required byte order.  The build's
        # UTF-8 identifiers are ASCII in the reference slice; reject anything
        # else here so this optimization cannot silently produce a wrong digest.
        digest = hashlib.sha256()
        digest.update(b"[")
        first = True
        previous: tuple[bytes, bytes] | None = None
        while batch := cursor.fetchmany(2_000):
            for document_id, source_input_id in batch:
                document_bytes = document_id.encode("utf-8")
                source_bytes = source_input_id.encode("utf-8")
                current = (document_bytes, source_bytes)
                if previous is not None and current < previous:
                    raise DocumentReleaseV3Error("inventory identifiers require exact UTF-8 byte ordering")
                previous = current
                if not first:
                    digest.update(b",")
                digest.update(canonical_json_bytes({"documentId": document_id, "sourceInputId": source_input_id}))
                first = False
        digest.update(b"]")
        return digest.hexdigest()
    finally:
        connection.close()


def _table_paths(root: Path, partition_id: str) -> dict[str, Path]:
    return {
        "current-documents": root / "data" / f"current-documents-{partition_id}.parquet",
        "documents": root / "data" / f"documents-{partition_id}.parquet",
        "eligibility-evidence": root / "data" / f"eligibility-evidence-{partition_id}.parquet",
        "passages": root / "data" / f"passages-{partition_id}.parquet",
        "source-dispositions": root / "data" / f"source-dispositions-{partition_id}.parquet",
        "changes": root / "data" / f"changes-{partition_id}.parquet",
        "failures": root / "data" / f"failures-{partition_id}.parquet",
        "coverage": root / "data" / f"coverage-{partition_id}.parquet",
        "partition-receipt": root / "receipts" / f"partitions-{partition_id}.parquet",
    }


def _write_partition(
    root: Path,
    source_inputs: Iterable[SourceInput],
    config: BuildConfig,
    *,
    started_at: str,
    shared_rendition_seen_path: Path | None = None,
    write_coverage: bool = True,
) -> tuple[list[MemberDescriptor], _BuildState]:
    paths = _table_paths(root, config.partition_id)
    writers: dict[str, BoundedParquetWriter] = {}
    for role, path in paths.items():
        if role == "partition-receipt" or (role == "coverage" and not write_coverage):
            continue
        schema_id = ROLE_SCHEMA_IDS[role]
        writers[role] = BoundedParquetWriter(
            path,
            TABLE_SCHEMAS[schema_id],
            max_rows=config.row_batch_size,
            max_utf8_bytes=config.row_batch_utf8_bytes,
            compression=config.compression,
        )
    pack_writer = RenditionPackWriter(root, config, shared_seen_path=shared_rendition_seen_path)
    state = _BuildState()
    try:
        for item in source_inputs:
            item.validate()
            if item.source_partition not in {None, config.partition_id}:
                raise DocumentReleaseV3Error(
                    f"source input partition {item.source_partition!r} does not match build partition {config.partition_id!r}"
                )
            state.sources.add(item.source_id)
            state.universe_count += 1
            state.source_disposition_count += 1
            if item.source_input_id is not None:
                state.selected_document_count += 1
                state.source_rollups[item.source_id]["selected"] += 1
            if item.previous_active:
                state.previous_active_document_count += 1

            document_version_id: str | None = None
            exclusion_policy_id: str | None = None
            failure_id: str | None = None
            if item.disposition == "active":
                assert item.rendition_path is not None
                rendition_size = item.rendition_path.stat().st_size
                if rendition_size > config.max_oversized_document_bytes:
                    raise DocumentReleaseV3Error(
                        f"rendition {item.rendition_path} exceeds max oversized document bytes"
                    )
                rendition = item.rendition_path.read_bytes()
                state.max_document_bytes_observed = max(state.max_document_bytes_observed, rendition_size)
                try:
                    normalized_text = rendition.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise DocumentReleaseV3Error(f"rendition {item.rendition_path} is not valid UTF-8") from error
                rendition_digest = pack_writer.add(rendition, media_type=str(item.media_type))
                normalized_digest = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
                evidence_data = canonical_json_text(
                    {"basis": item.eligibility_basis, "sourceInputId": item.source_input_id}
                )
                evidence_digest = hashlib.sha256(evidence_data.encode("utf-8")).hexdigest()
                document_version_id = _stable_urn(
                    "document-version",
                    {
                        "documentId": item.document_id,
                        "eligibilityEvidenceDigest": evidence_digest,
                        "normalizerId": config.normalizer_id,
                        "processingPolicyId": config.processing_policy_id,
                        "renditionDigest": rendition_digest,
                        "segmenterId": config.segmenter_id,
                        "sourceRecordId": item.source_record_id,
                        "sourceVersion": item.source_version,
                    },
                )
                evidence_id = _stable_urn(
                    "eligibility-evidence",
                    {
                        "documentVersionId": document_version_id,
                        "evidenceDigest": evidence_digest,
                        "policyId": config.eligibility_policy_id,
                    },
                )
                writers["current-documents"].write(
                    {
                        "document_id": item.document_id,
                        "document_version_id": document_version_id,
                        "state": "active",
                        "source_id": item.source_id,
                        "source_partition": item.source_partition,
                        "eligibility_state": item.eligibility_state,
                    }
                )
                writers["documents"].write(
                    {
                        "document_id": item.document_id,
                        "document_version_id": document_version_id,
                        "rendition_digest": bytes.fromhex(rendition_digest),
                        "source_record_id": item.source_record_id,
                        "source_version": item.source_version,
                        "title": item.title,
                        "published_at": _parse_timestamp(item.published_at, "publishedAt"),
                        "updated_at": _parse_timestamp(item.updated_at, "updatedAt"),
                        "document_type": item.document_type,
                        "language": item.language,
                        "normalized_text": normalized_text,
                        "normalized_text_digest": bytes.fromhex(normalized_digest),
                        "normalizer_id": config.normalizer_id,
                        "segmenter_id": config.segmenter_id,
                        "processing_policy_id": config.processing_policy_id,
                        "eligibility_state": item.eligibility_state,
                        "eligibility_evidence_id": evidence_id,
                    }
                )
                writers["eligibility-evidence"].write(
                    {
                        "eligibility_evidence_id": evidence_id,
                        "document_version_id": document_version_id,
                        "policy_id": config.eligibility_policy_id,
                        "authority_id": item.eligibility_authority_id,
                        "evidence_kind": item.eligibility_evidence_kind,
                        "verdict": item.eligibility_state,
                        "evidence_schema_id": ELIGIBILITY_DATA_SCHEMA_ID,
                        "evidence_data": evidence_data,
                        "evidence_digest": bytes.fromhex(evidence_digest),
                        "reason_code": item.eligibility_reason_code,
                    }
                )
                for ordinal, (start, end, passage_text) in enumerate(
                    _segment_utf8(normalized_text, config.max_passage_utf8_bytes)
                ):
                    passage_digest = hashlib.sha256(passage_text.encode("utf-8")).hexdigest()
                    passage_id = _stable_urn(
                        "passage",
                        {
                            "documentVersionId": document_version_id,
                            "endUtf8Byte": end,
                            "ordinal": ordinal,
                            "startUtf8Byte": start,
                            "textDigest": passage_digest,
                        },
                    )
                    writers["passages"].write(
                        {
                            "passage_id": passage_id,
                            "document_id": item.document_id,
                            "document_version_id": document_version_id,
                            "ordinal": ordinal,
                            "text": passage_text,
                            "text_digest": bytes.fromhex(passage_digest),
                            "normalized_start_utf8_byte": start,
                            "normalized_end_utf8_byte": end,
                            "coordinate_scheme": RENDITION_UTF8_COORDINATE_SCHEMA_ID,
                            "coordinate_data": canonical_json_text(
                                {
                                    "endUtf8Byte": end,
                                    "mediaType": item.media_type,
                                    "renditionSha256": rendition_digest,
                                    "startUtf8Byte": start,
                                }
                            ),
                            "processing_steps": [config.normalizer_id, config.segmenter_id],
                        }
                    )
                    state.passage_count += 1
                    state.passage_text_bytes += len(passage_text.encode("utf-8"))
                state.active_document_count += 1
                state.document_version_count += 1
                state.eligibility_evidence_count += 1
                state.normalized_text_bytes += len(normalized_text.encode("utf-8"))
                state.eligibility[str(item.eligibility_state)] += 1
                state.source_rollups[item.source_id]["active"] += 1
            elif item.disposition == "deleted":
                writers["current-documents"].write(
                    {
                        "document_id": item.document_id,
                        "document_version_id": None,
                        "state": "deleted",
                        "source_id": item.source_id,
                        "source_partition": item.source_partition,
                        "eligibility_state": None,
                    }
                )
                state.deleted_document_count += 1
                state.source_rollups[item.source_id]["deleted"] += 1
            elif item.disposition == "excluded":
                exclusion_policy_id = item.exclusion_policy_id
                state.excluded_document_count += 1
                state.source_rollups[item.source_id]["excluded"] += 1
            else:
                failure_id = item.failure_id
                writers["failures"].write(
                    {
                        "failure_id": item.failure_id,
                        "source_input_id": item.source_input_id,
                        "document_id": item.document_id,
                        "stage": item.failure_stage,
                        "failure_class": item.failure_class,
                        "retryable": item.failure_retryable,
                        "attempt_count": item.failure_attempt_count,
                        "diagnostic_code": item.failure_diagnostic_code,
                        "final_disposition": item.failure_final_disposition,
                        "failure_policy_id": config.failure_policy_id,
                    }
                )
                state.accepted_failure_count += 1
                state.failure_count += 1
                assert item.failure_attempt_count is not None
                state.retry_count += max(0, item.failure_attempt_count - 1)
                state.source_rollups[item.source_id]["accepted-failure"] += 1

            disposition_code = {
                "active": "spicyregs.disposition.active",
                "deleted": "spicyregs.disposition.source-deleted"
                if item.source_input_id is not None
                else "spicyregs.disposition.left-selection",
                "excluded": "spicyregs.disposition.policy-excluded",
                "accepted-failure": "spicyregs.disposition.accepted-failure",
            }[item.disposition]
            writers["source-dispositions"].write(
                {
                    "document_id": item.document_id,
                    "source_input_id": item.source_input_id,
                    "selected_current": item.source_input_id is not None,
                    "previous_active": item.previous_active,
                    "disposition": item.disposition,
                    "document_version_id": document_version_id,
                    "exclusion_policy_id": exclusion_policy_id,
                    "failure_id": failure_id,
                    "disposition_code": disposition_code,
                }
            )
            if item.old_document_version_id != document_version_id or item.disposition == "deleted":
                if item.disposition == "deleted":
                    change_kind = "delete"
                elif item.old_document_version_id is None:
                    change_kind = "add"
                elif item.old_eligibility_state != item.eligibility_state:
                    change_kind = "eligibility"
                else:
                    change_kind = "update"
                writers["changes"].write(
                    {
                        "document_id": item.document_id,
                        "old_document_version_id": item.old_document_version_id,
                        "new_document_version_id": document_version_id,
                        "change_kind": change_kind,
                    }
                )
        if write_coverage:
            for source_id in sorted(state.source_rollups):
                rollup = state.source_rollups[source_id]
                writers["coverage"].write(
                    {
                        "source_id": source_id,
                        "selected_document_count": rollup["selected"],
                        "active_document_count": rollup["active"],
                        "deleted_document_count": rollup["deleted"],
                        "excluded_document_count": rollup["excluded"],
                        "accepted_terminal_failure_count": rollup["accepted-failure"],
                    }
                )
    finally:
        for writer in writers.values():
            writer.close()
        pack_writer.close()

    state.max_writer_buffer_rows = max((writer.max_buffered_rows for writer in writers.values()), default=0)
    state.max_writer_buffer_utf8_bytes = max((writer.max_buffered_utf8_bytes for writer in writers.values()), default=0)
    state.rendition_count = sum(output.rendition_count for output in pack_writer.outputs)
    state.rendition_bytes = sum(output.rendition_bytes for output in pack_writer.outputs)
    members: list[MemberDescriptor] = []
    for role, writer in writers.items():
        members.append(
            _member_descriptor(
                root,
                writer.path,
                role=role,
                record_count=writer.record_count,
                schema_id=ROLE_SCHEMA_IDS[role],
                partition_id=config.partition_id,
                media_type="application/vnd.apache.parquet",
            )
        )
    for output in pack_writer.outputs:
        members.append(
            _member_descriptor(
                root,
                output.pack_path,
                role="rendition-pack",
                record_count=None,
                schema_id=None,
                partition_id=config.partition_id,
                media_type="application/octet-stream",
            )
        )
        members.append(
            _member_descriptor(
                root,
                output.index_path,
                role="rendition-pack-index",
                record_count=output.rendition_count,
                schema_id=RENDITION_PACK_INDEX_SCHEMA_ID,
                partition_id=config.partition_id,
                media_type="application/vnd.apache.parquet",
            )
        )

    completed_at = config.build_completed_at or _utc_now()
    task_key = "urn:spicyregs:document-release-v3-task:" + sha256_bytes(
        canonical_json_bytes(
            {
                "configurationIdentity": config.identity(),
                "inputDigests": sorted(member.sha256 for member in members),
                "policyIdentity": config.processing_policy_id,
                "stageIdentity": "document-release-v3-partition",
            }
        )
    )
    receipt_writer = BoundedParquetWriter(
        paths["partition-receipt"],
        TABLE_SCHEMAS[PARTITION_RECEIPT_SCHEMA_ID],
        max_rows=1,
        max_utf8_bytes=config.row_batch_utf8_bytes,
        compression=config.compression,
    )
    receipt_writer.write(
        {
            "task_key": task_key,
            "attempt_id": config.build_run_id,
            "partition_id": config.partition_id,
            "state": "committed",
            "document_count": state.document_version_count,
            "passage_count": state.passage_count,
            "rendition_byte_count": state.rendition_bytes,
            "started_at": started_at,
            "completed_at": completed_at,
            "member_digests_json": canonical_json_text(sorted(member.sha256 for member in members)),
        }
    )
    receipt_writer.close()
    members.append(
        _member_descriptor(
            root,
            receipt_writer.path,
            role="partition-receipt",
            record_count=1,
            schema_id=PARTITION_RECEIPT_SCHEMA_ID,
            partition_id=config.partition_id,
            media_type="application/vnd.apache.parquet",
        )
    )
    return members, state


def _write_aggregate_coverage(root: Path, state: _BuildState, config: BuildConfig) -> MemberDescriptor:
    path = root / "data" / "coverage-global.parquet"
    writer = BoundedParquetWriter(
        path,
        TABLE_SCHEMAS[ROLE_SCHEMA_IDS["coverage"]],
        max_rows=config.row_batch_size,
        max_utf8_bytes=config.row_batch_utf8_bytes,
        compression=config.compression,
    )
    for source_id in sorted(state.source_rollups):
        rollup = state.source_rollups[source_id]
        writer.write(
            {
                "source_id": source_id,
                "selected_document_count": rollup["selected"],
                "active_document_count": rollup["active"],
                "deleted_document_count": rollup["deleted"],
                "excluded_document_count": rollup["excluded"],
                "accepted_terminal_failure_count": rollup["accepted-failure"],
            }
        )
    writer.close()
    return _member_descriptor(
        root,
        path,
        role="coverage",
        record_count=writer.record_count,
        schema_id=ROLE_SCHEMA_IDS["coverage"],
        partition_id=None,
        media_type="application/vnd.apache.parquet",
    )


def build_release_from_partitions(
    partition_inputs: Mapping[str, Iterable[SourceInput]],
    output_dir: Path,
    config: BuildConfig,
    *,
    verifier_memory_limit: str = "512MB",
) -> Path:
    """Build one release from named partitions, then verify and publish it atomically."""

    output_dir = Path(output_dir).resolve()
    require_memory_limit(verifier_memory_limit)
    partition_ids = sorted(partition_inputs)
    if not partition_ids:
        raise DocumentReleaseV3Error("a release requires at least one partition input")
    for partition_id in partition_ids:
        if not isinstance(partition_id, str) or _PARTITION_ID.fullmatch(partition_id) is None:
            raise DocumentReleaseV3Error(
                f"partition input id {partition_id!r} must use portable lowercase filename characters"
            )
    if output_dir.exists() or output_dir.is_symlink():
        raise DocumentReleaseV3Error(f"refusing to replace existing output: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    work_root = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.building-", dir=output_dir.parent))
    started_at = config.build_started_at or _utc_now()
    try:
        for directory in ("data", "manifests", "receipts", "renditions", "schemas"):
            (work_root / directory).mkdir()

        schema_members: list[MemberDescriptor] = []
        schema_descriptors: list[dict[str, Any]] = []
        for schema_id, schema_document in sorted(schema_documents().items()):
            key = hashlib.sha256(schema_id.encode("utf-8")).hexdigest()[:24]
            schema_path = work_root / "schemas" / f"{key}.json"
            _write_canonical_json(schema_path, schema_document)
            descriptor = _member_descriptor(
                work_root,
                schema_path,
                role="schema",
                record_count=1,
                schema_id=None,
                partition_id=None,
                media_type="application/schema+json",
            )
            schema_members.append(descriptor)
            schema_descriptors.append(
                {
                    "schemaId": schema_id,
                    "schemaVersion": "1.0",
                    "schemaSha256": descriptor.sha256,
                    "roles": list(role_for_schema(schema_id)),
                }
            )
        schema_descriptors.sort(key=lambda descriptor: descriptor["schemaId"])
        schema_set = {
            "schemaSetId": schema_set_id(schema_descriptors),
            "schemas": schema_descriptors,
        }

        multiple_partitions = len(partition_ids) > 1
        shared_seen_path = work_root / "renditions" / ".rendition-seen.sqlite" if multiple_partitions else None
        state = _BuildState()
        partition_references: list[ManifestReference] = []
        all_partition_members: list[MemberDescriptor] = []
        dispositions_paths: list[Path] = []
        for partition_id in partition_ids:
            partition_config = replace(config, partition_id=partition_id)
            partition_members, partition_state = _write_partition(
                work_root,
                partition_inputs[partition_id],
                partition_config,
                started_at=started_at,
                shared_rendition_seen_path=shared_seen_path,
                write_coverage=not multiple_partitions,
            )
            state.merge(partition_state)
            all_partition_members.extend(partition_members)
            dispositions_paths.append(_table_paths(work_root, partition_id)["source-dispositions"])
            partition_manifest = make_subordinate_manifest(
                scope_kind="partition", scope_id=partition_id, members=partition_members
            )
            partition_manifest_path = work_root / "manifests" / f"partition-{partition_id}.json"
            _write_canonical_json(partition_manifest_path, partition_manifest)
            partition_manifest_digest, partition_manifest_size = sha256_file(partition_manifest_path)
            partition_references.append(
                ManifestReference(
                    manifest_id=f"partition:{partition_id}",
                    scope_kind="partition",
                    scope_id=partition_id,
                    object_key=partition_manifest_path.relative_to(work_root).as_posix(),
                    byte_size=partition_manifest_size,
                    sha256=partition_manifest_digest,
                )
            )
        if shared_seen_path is not None:
            shared_seen_path.unlink(missing_ok=True)

        inventory_digest = _inventory_digest(
            dispositions_paths,
            temp_directory=work_root,
            memory_limit=verifier_memory_limit,
        )

        aggregate_members: list[MemberDescriptor] = []
        if multiple_partitions:
            aggregate_members.append(_write_aggregate_coverage(work_root, state, config))
        receipt_count_members = [*schema_members, *aggregate_members, *all_partition_members]
        configuration_identity = (
            config.identity() if partition_ids == [config.partition_id] else config.identity(partition_ids=partition_ids)
        )

        receipt = {
            "conformanceClass": "DocumentRelease Producer",
            "specificationVersion": "2026-08-04",
            "implementation": {
                "implementationId": config.implementation_id,
                "implementationVersion": config.implementation_version,
                "runtimeProfileId": config.runtime_profile_id,
            },
            "configurationIdentity": configuration_identity,
            "inputIdentities": [
                {"selectionId": config.selection_id, "selectorDigest": config.selector_digest},
                *(
                    []
                    if config.previous_release_id is None
                    else [
                        {
                            "releaseId": config.previous_release_id,
                            "artifactDigest": config.previous_artifact_digest,
                        }
                    ]
                ),
            ],
            "outputIdentities": [
                {"manifestId": reference.manifest_id, "sha256": reference.sha256}
                for reference in partition_references
            ],
            "counts": state.counts(
                member_count=len(receipt_count_members),
                member_bytes=sum(member.byte_size for member in receipt_count_members),
                partition_count=len(partition_references),
            ),
            "coverage": state.coverage(),
            "failureTotal": state.failure_count,
            "retryTotal": state.retry_count,
            "startedAt": started_at,
            "completedAt": config.build_completed_at or _utc_now(),
            "verifier": {"implementationId": "spicyregs-document-release-v3-verify", "version": "1.0"},
            "verdict": "pass",
            "verificationCode": "valid",
        }
        build_receipt_path = work_root / "receipts" / "build.json"
        _write_canonical_json(build_receipt_path, receipt)
        build_receipt_member = _member_descriptor(
            work_root,
            build_receipt_path,
            role="build-receipt",
            record_count=1,
            schema_id=BUILD_RECEIPT_SCHEMA_ID,
            partition_id=None,
            media_type="application/json",
        )

        global_members = [*schema_members, *aggregate_members, build_receipt_member]
        global_manifest = make_subordinate_manifest(scope_kind="global", scope_id="global", members=global_members)
        global_manifest_path = work_root / "manifests" / "global.json"
        _write_canonical_json(global_manifest_path, global_manifest)
        global_manifest_digest, global_manifest_size = sha256_file(global_manifest_path)
        global_reference = ManifestReference(
            manifest_id="global:global",
            scope_kind="global",
            scope_id="global",
            object_key="manifests/global.json",
            byte_size=global_manifest_size,
            sha256=global_manifest_digest,
        )

        all_members = [*global_members, *all_partition_members]
        counts = state.counts(
            member_count=len(all_members),
            member_bytes=sum(member.byte_size for member in all_members),
            partition_count=len(partition_references),
        )
        content = {
            "schemaSet": schema_set,
            "producer": {
                "product": "spicyregs",
                "implementationId": config.implementation_id,
                "implementationVersion": config.implementation_version,
                "sourceRevision": config.source_revision,
                "runtimeProfileId": config.runtime_profile_id,
            },
            "processingPolicy": {
                "processingPolicyId": config.processing_policy_id,
                "normalizerId": config.normalizer_id,
                "segmenterId": config.segmenter_id,
                "renditionPolicyId": config.rendition_policy_id,
                "eligibilityPolicyId": config.eligibility_policy_id,
                "failurePolicyId": config.failure_policy_id,
                "diagnosticRegistryId": config.diagnostic_registry_id,
                "canonicalJsonProfile": CANONICAL_JSON_PROFILE,
            },
            "sourceSelection": {
                "selectionId": config.selection_id,
                "selectorType": config.selector_type,
                "selectorDigest": config.selector_digest,
                "inventoryDigest": inventory_digest,
                "effectiveAt": config.effective_at,
                "selectedDocumentCount": state.selected_document_count,
            },
            "previousRelease": (
                None
                if config.previous_release_id is None
                else {
                    "releaseId": config.previous_release_id,
                    "artifactDigest": config.previous_artifact_digest,
                }
            ),
            "globalManifest": global_reference.as_dict(),
            "partitionManifests": [reference.as_dict() for reference in partition_references],
            "counts": counts,
            "coverage": state.coverage(),
        }
        root = {
            "format": FORMAT,
            "formatVersion": FORMAT_VERSION,
            "releaseId": release_id(content),
            "content": content,
            "annotations": {
                "createdAt": config.created_at,
                "buildRunId": config.build_run_id,
            },
        }
        _write_canonical_json(work_root / "release.json", root)

        from spicy_regs.document_release_v3_verify import verify_release_or_raise

        verify_release_or_raise(work_root, memory_limit=verifier_memory_limit)
        atomic_publish_directory(work_root, output_dir)
        return output_dir
    except Exception:
        # Retain the private temporary namespace for diagnosis.  It has a unique
        # name and no published root path, so consumers cannot mistake it for a
        # release.  Callers may remove it after recording the failure.
        raise


def build_release(
    source_inputs: Iterable[SourceInput],
    output_dir: Path,
    config: BuildConfig,
    *,
    verifier_memory_limit: str = "512MB",
) -> Path:
    """Build one single-partition release using the original producer API."""

    return build_release_from_partitions(
        {config.partition_id: source_inputs},
        output_dir,
        config,
        verifier_memory_limit=verifier_memory_limit,
    )


def build_release_from_jsonl(
    input_path: Path,
    output_dir: Path,
    config: BuildConfig,
    *,
    verifier_memory_limit: str = "512MB",
) -> Path:
    """Stream a JSON Lines selection ledger into a sealed release."""

    return build_release(
        iter_source_inputs(input_path),
        output_dir,
        config,
        verifier_memory_limit=verifier_memory_limit,
    )


def build_release_from_partition_jsonl(
    partition_inputs: Mapping[str, Path],
    output_dir: Path,
    config: BuildConfig,
    *,
    verifier_memory_limit: str = "512MB",
) -> Path:
    """Stream named JSON Lines selection ledgers into one sealed release."""

    return build_release_from_partitions(
        {partition_id: iter_source_inputs(path) for partition_id, path in partition_inputs.items()},
        output_dir,
        config,
        verifier_memory_limit=verifier_memory_limit,
    )

"""Exact active-set and change-table construction for ``DocumentRelease`` v3."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import duckdb

from spicy_regs.document_release_v3 import (
    CHANGES_SCHEMA_ID,
    DocumentReleaseV3Error,
    parse_canonical_json,
    require_memory_limit,
    validate_object_key,
)
from spicy_regs.document_release_v3_verify import verify_release_or_raise
from spicy_regs.document_release_v3_writer import BoundedParquetWriter


def release_member_paths(release_dir: Path, role: str) -> tuple[Path, ...]:
    """Resolve all declared members for one role after path-safe parsing.

    Callers verify the release before using returned data.  This helper exists
    for producer maintenance commands; it does not relax complete-distribution
    verification.
    """

    release_dir = Path(release_dir).resolve()
    root = parse_canonical_json((release_dir / "release.json").read_bytes(), label="release.json")
    if not isinstance(root, dict) or not isinstance(root.get("content"), dict):
        raise DocumentReleaseV3Error("release root is not an object with content")
    content = root["content"]
    references = [content["globalManifest"], *content["partitionManifests"]]
    paths: list[Path] = []
    for reference in references:
        key = validate_object_key(reference["objectKey"], "manifest objectKey")
        manifest = parse_canonical_json((release_dir / key).read_bytes(), label=key)
        for descriptor in manifest["members"]:
            if descriptor["role"] == role:
                member_key = validate_object_key(descriptor["objectKey"], "member objectKey")
                paths.append(release_dir / member_key)
    return tuple(paths)


def _sql_paths(paths: tuple[Path, ...]) -> str:
    if not paths:
        raise DocumentReleaseV3Error("release has no current-documents members")
    return "[" + ",".join("'" + str(path).replace("'", "''") + "'" for path in paths) + "]"


def iter_release_diff(
    previous_release: Path,
    current_release: Path,
    *,
    memory_limit: str = "512MB",
) -> Iterator[dict[str, Any]]:
    """Yield the exact logical active-set delta between two verified releases."""

    require_memory_limit(memory_limit)
    verify_release_or_raise(previous_release, memory_limit=memory_limit)
    verify_release_or_raise(current_release, memory_limit=memory_limit)
    previous_paths = release_member_paths(previous_release, "current-documents")
    current_paths = release_member_paths(current_release, "current-documents")
    connection = duckdb.connect()
    try:
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute(f"CREATE VIEW previous_current AS SELECT * FROM read_parquet({_sql_paths(previous_paths)})")
        connection.execute(f"CREATE VIEW current_current AS SELECT * FROM read_parquet({_sql_paths(current_paths)})")
        reader = connection.execute(
            "SELECT coalesce(c.document_id,p.document_id) AS document_id,"
            "p.document_version_id AS old_document_version_id,"
            "CASE WHEN c.state='active' THEN c.document_version_id ELSE NULL END AS new_document_version_id,"
            "CASE "
            "WHEN (p.document_id IS NULL OR p.state<>'active') AND c.state='active' THEN 'add' "
            "WHEN p.state='active' AND (c.document_id IS NULL OR c.state<>'active') THEN 'delete' "
            "WHEN p.state='active' AND c.state='active' AND p.document_version_id<>c.document_version_id "
            "AND p.eligibility_state<>c.eligibility_state THEN 'eligibility' "
            "WHEN p.state='active' AND c.state='active' AND p.document_version_id<>c.document_version_id "
            "THEN 'update' ELSE NULL END AS change_kind "
            "FROM previous_current p FULL OUTER JOIN current_current c USING(document_id) "
            "WHERE ((p.document_id IS NULL OR p.state<>'active') AND c.state='active') "
            "OR (p.state='active' AND (c.document_id IS NULL OR c.state<>'active')) "
            "OR (p.state='active' AND c.state='active' AND p.document_version_id<>c.document_version_id) "
            "ORDER BY document_id"
        ).fetch_record_batch(rows_per_batch=2_000)
        for batch in reader:
            yield from batch.to_pylist()
    finally:
        connection.close()


def write_release_diff(
    previous_release: Path,
    current_release: Path,
    output_path: Path,
    *,
    row_batch_size: int = 2_000,
    row_batch_utf8_bytes: int = 16 * 1024 * 1024,
    memory_limit: str = "512MB",
) -> Path:
    """Write an exact, bounded Parquet change table for two releases."""

    from spicy_regs.document_release_v3 import TABLE_SCHEMAS

    output_path = Path(output_path).resolve()
    if output_path.exists():
        raise DocumentReleaseV3Error(f"refusing to replace existing diff output: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer = BoundedParquetWriter(
        output_path,
        TABLE_SCHEMAS[CHANGES_SCHEMA_ID],
        max_rows=row_batch_size,
        max_utf8_bytes=row_batch_utf8_bytes,
        compression="zstd",
    )
    try:
        for row in iter_release_diff(previous_release, current_release, memory_limit=memory_limit):
            writer.write(row)
    finally:
        writer.close()
    return output_path


def active_identity_map(release_dir: Path, *, memory_limit: str = "512MB") -> Mapping[str, str]:
    """Return a small-release active identity map for tests and reports.

    Scale code should use :func:`iter_release_diff`; this convenience helper is
    intentionally explicit about materializing the result.
    """

    require_memory_limit(memory_limit)
    verify_release_or_raise(release_dir, memory_limit=memory_limit)
    paths = release_member_paths(release_dir, "current-documents")
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"SELECT document_id,document_version_id FROM read_parquet({_sql_paths(paths)}) "
            "WHERE state='active' ORDER BY document_id"
        ).fetchall()
        return dict(rows)
    finally:
        connection.close()

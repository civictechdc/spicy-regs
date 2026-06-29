"""Tests for the derived-data comment-text backfill.

Exercises the in-place enrichment over a comments frame, the partition walker
(which must read agency_code from the directory name), and the incremental /
limit / overwrite semantics — all against the fake in-memory S3 resource.
"""

from __future__ import annotations

import json

import polars as pl

from spicy_regs.backfill_derived_text import (
    backfill_comment_partitions,
    enrich_comments_with_derived_text,
)
from tests.conftest import COMMENT_SCHEMA

# Reuse the fake S3 surface from the derived-text unit tests.
from tests.test_derived_text import _FakeS3Resource, _store


def _factory():
    return lambda: _FakeS3Resource(_store())


def _attach() -> str:
    return json.dumps([{"title": "x", "formats": [{"url": "https://x/a.pdf", "format": "pdf"}]}])


def _frame(rows: list[dict]) -> pl.DataFrame:
    base = {k: None for k in COMMENT_SCHEMA}
    return pl.DataFrame([{**base, **r} for r in rows], schema=COMMENT_SCHEMA)


def test_enrich_fills_from_derived_data_using_agency_column() -> None:
    df = _frame(
        [
            {"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach()},
            {"comment_id": "ACF-2025-0038-0015", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach()},
        ]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats == {"selected": 2, "ok": 2, "missing": 0}
    by_id = {r["comment_id"]: r for r in out.iter_rows(named=True)}
    assert by_id["ACF-2025-0038-0004"]["text_content"] == "Wisconsin DCF comment body"
    assert by_id["ACF-2025-0038-0015"]["text_content"] == "first attachment\n\nsecond attachment"
    assert all(r["text_extraction_status"] == "ok" for r in out.iter_rows(named=True))


def test_enrich_with_explicit_agency_override() -> None:
    # Partition files drop agency_code; the agency is supplied explicitly.
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "attachments_json": _attach()}]
    ).drop("agency_code")
    out, stats = enrich_comments_with_derived_text(df, agency="ACF", resource_factory=_factory())
    assert stats["ok"] == 1
    assert out.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"


def test_enrich_skips_no_attachment_and_already_filled() -> None:
    df = _frame(
        [
            {"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": None},
            {"comment_id": "ACF-2025-0038-0015", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach(), "text_extraction_status": "ok", "text_content": "kept"},
        ]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats["selected"] == 0
    assert out.filter(pl.col("comment_id") == "ACF-2025-0038-0015").row(0, named=True)["text_content"] == "kept"


def test_enrich_missing_derived_text_counts_as_missing() -> None:
    df = _frame(
        [{"comment_id": "ACF-2025-0038-9999", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach()}]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats == {"selected": 1, "ok": 0, "missing": 1}
    # Left NULL so the PDF-download fallback can still pick it up.
    assert out.row(0, named=True)["text_extraction_status"] is None


def test_enrich_respects_limit() -> None:
    df = _frame(
        [
            {"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach()},
            {"comment_id": "ACF-2025-0038-0015", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": _attach()},
        ]
    )
    _, stats = enrich_comments_with_derived_text(df, resource_factory=_factory(), limit=1)
    assert stats["selected"] == 1
    assert stats["ok"] == 1


def test_backfill_partitions_reads_agency_from_path(tmp_path) -> None:
    part_dir = tmp_path / "comments" / "agency" / "agency_code=ACF"
    part_dir.mkdir(parents=True)
    # Partition file has NO agency_code column (mirrors production layout).
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "attachments_json": _attach()}]
    ).drop("agency_code")
    df.write_parquet(part_dir / "part-0.parquet")

    totals, changed = backfill_comment_partitions(
        tmp_path / "comments" / "agency", resource_factory=_factory()
    )
    assert totals["ok"] == 1
    assert len(changed) == 1
    written = pl.read_parquet(part_dir / "part-0.parquet")
    assert "agency_code" not in written.columns  # schema preserved
    assert written.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"

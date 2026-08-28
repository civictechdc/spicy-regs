"""Tests for the derived-data comment-text backfill.

Exercises the in-place enrichment over a comments frame, the partition walker
(which must read agency_code from the directory name), and the incremental /
limit / overwrite semantics — all against the fake in-memory S3 resource.
"""

from __future__ import annotations

import json

import duckdb
import polars as pl

from spicy_regs.backfill_derived_text import (
    _backfill_agency_in_catalog,
    backfill_comment_partitions,
    enrich_comments_with_derived_text,
)
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
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


def test_discover_from_derived_finds_rows_with_no_attachments_json() -> None:
    # Legacy row: no attachments_json recorded at all (ingested before that
    # column existed), yet the fake derived-data store has extracted text for
    # it. Default mode must skip it; discover_from_derived must find it.
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": None}]
    )

    default_out, default_stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert default_stats == {"selected": 0, "ok": 0, "missing": 0}
    assert default_out.row(0, named=True)["text_content"] is None

    discover_out, discover_stats = enrich_comments_with_derived_text(
        df, resource_factory=_factory(), discover_from_derived=True
    )
    assert discover_stats == {"selected": 1, "ok": 1, "missing": 0}
    assert discover_out.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"
    assert discover_out.row(0, named=True)["text_extraction_status"] == "ok"


def test_discover_from_derived_is_incremental() -> None:
    # A second run over the already-filled frame must not reselect the row.
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": None}]
    )
    filled, _ = enrich_comments_with_derived_text(df, resource_factory=_factory(), discover_from_derived=True)

    _, stats = enrich_comments_with_derived_text(filled, resource_factory=_factory(), discover_from_derived=True)
    assert stats == {"selected": 0, "ok": 0, "missing": 0}


def test_discover_from_derived_still_counts_true_misses() -> None:
    # No attachments_json AND no derived-data text available at all.
    df = _frame(
        [{"comment_id": "ACF-2025-0038-9999", "docket_id": "ACF-2025-0038", "agency_code": "ACF", "attachments_json": None}]
    )
    _, stats = enrich_comments_with_derived_text(df, resource_factory=_factory(), discover_from_derived=True)
    assert stats == {"selected": 1, "ok": 0, "missing": 1}


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


def _seed_catalog(con, rows: list[dict]) -> None:
    """Insert comment rows into the local catalog table (all COMMENT columns)."""
    iceberg._ensure_table(con, COMMENT)
    frame = pl.DataFrame(
        [{**{c: None for c in COMMENT.schema}, **r} for r in rows], schema=COMMENT.schema
    )
    col_list = ", ".join(f'"{c}"' for c in COMMENT.schema)
    con.register("_seed_src", frame.to_arrow())
    con.execute(f"INSERT INTO {iceberg._qualified(COMMENT)} ({col_list}) SELECT {col_list} FROM _seed_src;")
    con.unregister("_seed_src")


def test_catalog_backfill_upserts_filled_rows_in_place() -> None:
    # Local DuckDB standing in for the attached R2 catalog (same alias the
    # connector uses), so the DELETE+INSERT upsert is exercised without network.
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                # found in the fake derived-data store → gets filled
                {"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
                 "attachments_json": _attach(), "modify_date": "2025-01-01", "comment": "orig body"},
                # attachment but no derived text → counts as missing, stays NULL
                {"comment_id": "ACF-2025-0038-9999", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
                 "attachments_json": _attach(), "modify_date": "2025-01-01"},
                # no attachment → not a candidate at all
                {"comment_id": "ACF-2025-0038-0007", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
                 "attachments_json": None, "modify_date": "2025-01-01"},
            ],
        )

        stats = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert stats == {"selected": 2, "ok": 1, "missing": 1}

        out = dict(
            con.execute(
                f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
            ).fetchall()
        )
        assert out["ACF-2025-0038-0004"] == "Wisconsin DCF comment body"
        assert out["ACF-2025-0038-9999"] is None  # missing left NULL for the PDF fallback
        assert out["ACF-2025-0038-0007"] is None  # non-candidate untouched

        # Upsert preserves other columns and doesn't duplicate the row.
        row = con.execute(
            f"SELECT comment, text_extraction_status FROM {iceberg._qualified(COMMENT)} "
            f"WHERE comment_id = 'ACF-2025-0038-0004'"
        ).fetchall()
        assert len(row) == 1
        assert row[0] == ("orig body", "ok")
        count = con.execute(f"SELECT count(*) FROM {iceberg._qualified(COMMENT)}").fetchone()
        assert count is not None and count[0] == 3

        # Idempotent: a second run finds nothing new (the filled row now has a status).
        again = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert again == {"selected": 1, "ok": 0, "missing": 1}
    finally:
        con.close()


def test_catalog_backfill_discover_from_derived_finds_legacy_rows_without_attachments_json() -> None:
    # This is the exact trap the docstring warns about: rows ingested before
    # attachments_json was recorded have it NULL, so the default (attachment-
    # gated) catalog query never selects them even though Mirrulations has
    # extracted text ready and waiting. discover_from_derived must find them.
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                # legacy row: attachments_json was never populated
                {"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
                 "attachments_json": None, "modify_date": "2025-01-01"},
                # legacy row with no derived-data text available either
                {"comment_id": "ACF-2025-0038-9999", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
                 "attachments_json": None, "modify_date": "2025-01-01"},
            ],
        )

        default_stats = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert default_stats == {"selected": 0, "ok": 0, "missing": 0}, "default mode must not touch legacy rows"

        discover_stats = _backfill_agency_in_catalog(
            con, COMMENT, "ACF", resource_factory=_factory(), discover_from_derived=True
        )
        assert discover_stats == {"selected": 2, "ok": 1, "missing": 1}

        out = dict(
            con.execute(
                f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
            ).fetchall()
        )
        assert out["ACF-2025-0038-0004"] == "Wisconsin DCF comment body"
        assert out["ACF-2025-0038-9999"] is None

        # Idempotent: a second discover run finds nothing new for the filled row.
        again = _backfill_agency_in_catalog(
            con, COMMENT, "ACF", resource_factory=_factory(), discover_from_derived=True
        )
        assert again == {"selected": 1, "ok": 0, "missing": 1}
    finally:
        con.close()

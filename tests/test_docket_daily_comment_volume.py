"""Hermetic tests for the per-docket daily comment-volume rollup (no network).

Each test writes a small local ``comments.parquet`` fixture into the output
directory, so the transform takes its local-file path and never reaches R2, then
runs the real published query and asserts on the rows it produces.
"""

from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.docket_daily_comment_volume import DocketDailyCommentVolumeRollup
from spicy_regs.transforms.build_docket_daily_comment_volume import (
    OUTPUT,
    SCHEMA,
    build_docket_daily_comment_volume,
)
from spicy_regs.transforms.comments_source import resolve_comments_source

# The package re-exports the build *function* under the module's name, so fetch
# the module itself (for monkeypatching ROW_GROUP_SIZE) via importlib.
module = importlib.import_module("spicy_regs.transforms.build_docket_daily_comment_volume")

# The comments columns the rollup reads, plus one it must ignore. All VARCHAR,
# as in the published comments.parquet.
_COMMENT_SCHEMA = pa.schema(
    [
        ("comment_id", pa.string()),
        ("docket_id", pa.string()),
        ("agency_code", pa.string()),
        ("posted_date", pa.string()),
        ("receive_date", pa.string()),
    ]
)


def _comment(
    comment_id: str,
    docket_id: str | None,
    posted_date: str | None,
    *,
    agency_code: str | None = "EPA",
    receive_date: str | None = None,
) -> dict:
    return {
        "comment_id": comment_id,
        "docket_id": docket_id,
        "agency_code": agency_code,
        "posted_date": posted_date,
        "receive_date": receive_date,
    }


def _write_comments(output_dir: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows, schema=_COMMENT_SCHEMA), output_dir / "comments.parquet")


def _build(output_dir: Path, rows: list[dict]) -> list[dict]:
    _write_comments(output_dir, rows)
    out = build_docket_daily_comment_volume(output_dir)
    assert out == output_dir / OUTPUT
    return pq.read_table(out).to_pylist()


def test_published_schema_matches_the_declared_columns(tmp_output: Path) -> None:
    _build(tmp_output, [_comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z")])
    assert pq.read_schema(tmp_output / OUTPUT).remove_metadata() == SCHEMA


def test_counts_comments_per_docket_per_posted_day(tmp_output: Path) -> None:
    rows = _build(
        tmp_output,
        [
            _comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z"),
            _comment("C-2", "EPA-2024-0001", "2024-06-20T04:00:00Z"),
            _comment("C-3", "EPA-2024-0001", "2024-06-21T04:00:00Z"),
            _comment("C-4", "FDA-2024-0010", "2024-06-20T04:00:00Z", agency_code="FDA"),
        ],
    )
    assert rows == [
        {"docket_id": "EPA-2024-0001", "agency_code": "EPA", "day": date(2024, 6, 20), "comment_count": 2},
        {"docket_id": "EPA-2024-0001", "agency_code": "EPA", "day": date(2024, 6, 21), "comment_count": 1},
        {"docket_id": "FDA-2024-0010", "agency_code": "FDA", "day": date(2024, 6, 20), "comment_count": 1},
    ]


def test_buckets_on_posted_date_not_receive_date(tmp_output: Path) -> None:
    rows = _build(
        tmp_output,
        [
            _comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z", receive_date="2024-06-02T23:59:00Z"),
            _comment("C-2", "EPA-2024-0001", "2024-06-20T04:00:00Z", receive_date="2024-06-10T12:00:00Z"),
        ],
    )
    assert [(r["day"], r["comment_count"]) for r in rows] == [(date(2024, 6, 20), 2)]


def test_accepts_plain_dates_and_utc_timestamps(tmp_output: Path) -> None:
    # Both shapes appear in the corpus; each lands on its calendar date. The
    # midnight-Eastern 05:00Z form must not drift to another day.
    rows = _build(
        tmp_output,
        [
            _comment("C-1", "EPA-2024-0001", "2024-01-05T05:00:00Z"),
            _comment("C-2", "EPA-2024-0001", "2024-01-05"),
            _comment("C-3", "EPA-2024-0001", "2024-01-05T00:00:00Z"),
        ],
    )
    assert [(r["day"], r["comment_count"]) for r in rows] == [(date(2024, 1, 5), 3)]


def test_skips_null_and_unparseable_posted_dates(tmp_output: Path) -> None:
    rows = _build(
        tmp_output,
        [
            _comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z"),
            _comment("C-2", "EPA-2024-0001", None),
            _comment("C-3", "EPA-2024-0001", "not a date"),
            _comment("C-4", "EPA-2024-0001", ""),
        ],
    )
    assert [(r["day"], r["comment_count"]) for r in rows] == [(date(2024, 6, 20), 1)]


def test_skips_comments_without_a_docket_or_agency(tmp_output: Path) -> None:
    rows = _build(
        tmp_output,
        [
            _comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z"),
            _comment("C-2", None, "2024-06-20T04:00:00Z"),
            _comment("C-3", "EPA-2024-0001", "2024-06-20T04:00:00Z", agency_code=None),
        ],
    )
    assert [(r["docket_id"], r["comment_count"]) for r in rows] == [("EPA-2024-0001", 1)]


def test_strips_stray_quotes_from_docket_ids(tmp_output: Path) -> None:
    rows = _build(
        tmp_output,
        [
            _comment("C-1", '"EPA-2024-0001"', "2024-06-20T04:00:00Z"),
            _comment("C-2", "EPA-2024-0001", "2024-06-20T04:00:00Z"),
        ],
    )
    assert [(r["docket_id"], r["comment_count"]) for r in rows] == [("EPA-2024-0001", 2)]


def test_monthly_sums_match_comments_index_counting(tmp_output: Path) -> None:
    """Summed per month, the series equals what ``comments_index`` counts."""
    import duckdb

    fixture = [
        _comment(f"C-{i}", "EPA-2024-0001", f"2024-0{1 + i % 3}-{1 + i % 27:02d}T05:00:00Z") for i in range(60)
    ] + [_comment("C-x", "EPA-2024-0001", None), _comment("C-y", None, "2024-01-01T05:00:00Z")]
    _build(tmp_output, fixture)
    con = duckdb.connect()
    daily = con.execute(
        f"""SELECT year(day), month(day), SUM(comment_count)::BIGINT
            FROM read_parquet('{tmp_output / OUTPUT}') GROUP BY 1, 2 ORDER BY 1, 2"""
    ).fetchall()
    # Same filters and month extraction as iceberg._build_comments_index.
    index = con.execute(
        f"""SELECT EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::BIGINT,
                   EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::BIGINT,
                   COUNT(*)::BIGINT
            FROM read_parquet('{tmp_output / "comments.parquet"}')
            WHERE posted_date IS NOT NULL AND agency_code IS NOT NULL AND docket_id IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 2"""
    ).fetchall()
    assert daily == index
    assert sum(n for _, _, n in daily) == 60


def test_empty_input_still_writes_the_schema(tmp_output: Path) -> None:
    rows = _build(tmp_output, [_comment("C-1", None, None)])
    assert rows == []
    assert pq.read_schema(tmp_output / OUTPUT).remove_metadata() == SCHEMA


def test_sorted_by_docket_with_non_overlapping_row_group_stats(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single-docket reader must be able to prune on row-group min/max stats."""
    monkeypatch.setattr(module, "ROW_GROUP_SIZE", 2048)
    # Write the fixture deliberately out of order.
    fixture = [
        _comment(f"C-{d}-{k}", f"AG-2024-{d:05d}", f"2024-03-{1 + k:02d}T04:00:00Z", agency_code="AG")
        for d in reversed(range(4000))
        for k in range(2)
    ]
    rows = _build(tmp_output, fixture)

    keys = [(r["docket_id"], r["day"]) for r in rows]
    assert keys == sorted(keys)
    assert len(rows) == 8000

    meta = pq.ParquetFile(tmp_output / OUTPUT).metadata
    assert meta.num_row_groups > 1
    previous_max = None
    for i in range(meta.num_row_groups):
        stats = meta.row_group(i).column(0).statistics
        assert stats is not None and stats.has_min_max
        if previous_max is not None:
            assert stats.min >= previous_max
        previous_max = stats.max


# --- plumbing -------------------------------------------------------------


def test_comments_source_prefers_a_local_file(tmp_path: Path) -> None:
    local = tmp_path / "comments.parquet"
    local.touch()
    assert resolve_comments_source(tmp_path, consumer="test") == str(local)


def test_comments_source_falls_back_to_the_public_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example.dev/")
    assert resolve_comments_source(tmp_path, consumer="test") == "https://data.example.dev/comments.parquet"


def test_comments_source_defaults_to_the_production_bucket(tmp_path: Path) -> None:
    assert resolve_comments_source(tmp_path, consumer="test") == "https://data.spicy-regs.dev/comments.parquet"


@pytest.mark.parametrize("bad", ["http://data.example.dev", "https://data.example.dev/'; DROP TABLE x --"])
def test_comments_source_rejects_an_unsafe_public_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", bad)
    with pytest.raises(RuntimeError):
        resolve_comments_source(tmp_path, consumer="test")


def test_pipeline_declares_no_primed_inputs_and_publishes_its_output() -> None:
    # comments.parquet is read by projection, not primed (it is ~4.6 GB).
    assert DocketDailyCommentVolumeRollup.inputs == ()
    assert DocketDailyCommentVolumeRollup.output == OUTPUT


def test_pipeline_runs_end_to_end_from_a_local_comments_file(tmp_output: Path) -> None:
    _write_comments(tmp_output, [_comment("C-1", "EPA-2024-0001", "2024-06-20T04:00:00Z")])
    DocketDailyCommentVolumeRollup(output_dir=tmp_output, skip_upload=True).run()
    assert pq.read_table(tmp_output / OUTPUT).num_rows == 1

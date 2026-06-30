"""Tests for the R2 Data Catalog (Iceberg) connector.

The live ``merge_and_export`` needs a real R2 Data Catalog, so these tests
exercise the catalog-independent pieces — the SQL building / dedup / export
logic — against a *local* in-memory DuckDB attached under the same alias the
connector uses. ``_ensure_table``, ``_merge``, and ``_export_parquet`` all take
the connection as an argument precisely so this is possible without network.

A genuinely end-to-end run against R2 is covered by the manual
"ETL (new pipeline – vetting)" workflow with ``--use-iceberg``.
"""

from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg


def _write_staging(staging_dir: Path, agency: str, rows: list[dict]) -> None:
    """Mimic transforms.write_staging: staging_dir/dockets/{agency}.parquet."""
    type_dir = staging_dir / DOCKET.name
    type_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=DOCKET.schema).write_parquet(type_dir / f"{agency}.parquet")


def _write_comment_staging(staging_dir: Path, agency: str, rows: list[dict]) -> None:
    """Mimic transforms.write_staging: staging_dir/comments/{agency}.parquet."""
    type_dir = staging_dir / COMMENT.name
    type_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=COMMENT.schema).write_parquet(type_dir / f"{agency}.parquet")


def _comment(comment_id: str, docket_id: str, agency: str, posted_date: str, modify_date: str | None = None) -> dict:
    row = {col: None for col in COMMENT.schema}
    row.update(
        comment_id=comment_id,
        docket_id=docket_id,
        agency_code=agency,
        posted_date=posted_date,
        modify_date=modify_date or posted_date,
    )
    return row


@pytest.fixture
def local_catalog():
    """A local DuckDB standing in for the attached R2 catalog (alias reg_catalog)."""
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        yield con
    finally:
        con.close()


def _docket(docket_id: str, agency: str, title: str, modify_date: str) -> dict:
    row = {col: None for col in DOCKET.schema}
    row.update(docket_id=docket_id, agency_code=agency, title=title, modify_date=modify_date)
    return row


def test_is_configured(monkeypatch) -> None:
    for var in iceberg._REQUIRED_ENV:
        monkeypatch.delenv(var, raising=False)
    assert iceberg.is_configured() is False

    for var in iceberg._REQUIRED_ENV:
        monkeypatch.setenv(var, "x")
    assert iceberg.is_configured() is True


def test_namespace_empty_defaults(monkeypatch) -> None:
    # Unset -> default; explicitly empty (e.g. an unset GH secret) -> default too.
    monkeypatch.delenv("R2_CATALOG_NAMESPACE", raising=False)
    assert iceberg._namespace() == "default"
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "")
    assert iceberg._namespace() == "default"
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "custom")
    assert iceberg._namespace() == "custom"


def test_connect_raises_when_unconfigured(monkeypatch) -> None:
    for var in iceberg._REQUIRED_ENV:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(RuntimeError, match="R2_CATALOG_URI"):
        iceberg._connect()


def test_merge_inserts_then_dedups(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)

    # First batch: two distinct dockets.
    staging = tmp_path / "staging"
    _write_staging(
        staging,
        "EPA",
        [
            _docket("EPA-1", "EPA", "First", "2025-01-01"),
            _docket("EPA-2", "EPA", "Second", "2025-01-02"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)
    assert con.execute(f"SELECT count(*) FROM {iceberg._qualified(DOCKET)}").fetchone()[0] == 2

    # Second batch: one updated row (newer modify_date) + one brand-new row.
    # An older copy of EPA-1 in the same batch must lose to the newer one.
    staging2 = tmp_path / "staging2"
    _write_staging(
        staging2,
        "EPA",
        [
            _docket("EPA-1", "EPA", "First UPDATED", "2025-02-01"),
            _docket("EPA-1", "EPA", "stale dup", "2024-12-31"),
            _docket("EPA-3", "EPA", "Third", "2025-02-02"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging2, DOCKET), DOCKET)

    rows = dict(
        con.execute(
            f'SELECT docket_id, title FROM {iceberg._qualified(DOCKET)} ORDER BY docket_id'
        ).fetchall()
    )
    assert rows == {"EPA-1": "First UPDATED", "EPA-2": "Second", "EPA-3": "Third"}


def test_merge_keeps_existing_when_incoming_is_older(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)

    staging = tmp_path / "staging"
    _write_staging(staging, "EPA", [_docket("EPA-1", "EPA", "Newer", "2025-05-01")])
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)

    # An older row for the same key must NOT overwrite the newer one.
    staging2 = tmp_path / "staging2"
    _write_staging(staging2, "EPA", [_docket("EPA-1", "EPA", "Older", "2025-01-01")])
    iceberg._merge(con, iceberg._staging_files(staging2, DOCKET), DOCKET)

    title = con.execute(
        f'SELECT title FROM {iceberg._qualified(DOCKET)} WHERE docket_id = \'EPA-1\''
    ).fetchone()[0]
    assert title == "Newer"


def test_export_parquet_matches_published_shape(tmp_path, local_catalog) -> None:
    con = local_catalog
    iceberg._ensure_table(con, DOCKET)
    staging = tmp_path / "staging"
    _write_staging(
        staging,
        "EPA",
        [
            _docket("EPA-2", "EPA", "b", "2025-01-02"),
            _docket("EPA-1", "EPA", "a", "2025-01-01"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, DOCKET), DOCKET)

    out = tmp_path / "output"
    out_file = iceberg._export_parquet(con, DOCKET, out)
    assert out_file == out / "dockets.parquet"

    df = pl.read_parquet(out_file)
    # Same columns as the published schema, sorted by (agency_code, modify_date).
    assert df.columns == list(DOCKET.schema)
    assert df["docket_id"].to_list() == ["EPA-1", "EPA-2"]


def test_merge_and_export_noop_without_staging(tmp_path) -> None:
    # No staging files for dockets -> returns None and never touches the catalog.
    assert iceberg.merge_and_export(tmp_path / "empty", tmp_path / "out", DOCKET) is None


# --- comments path (catalog table + derived index) -------------------------


def test_build_comments_index_counts_per_partition(tmp_path, local_catalog) -> None:
    """The index derived from the catalog must hold one row per
    (agency, docket, year, month) with the right counts and schema."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    staging = tmp_path / "staging"
    _write_comment_staging(
        staging,
        "EPA",
        [
            # Two comments in the same partition (EPA, EPA-1, 2025-01).
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z"),
            _comment("c2", "EPA-1", "EPA", "2025-01-20T00:00:00Z"),
            # A different month -> its own partition.
            _comment("c3", "EPA-1", "EPA", "2025-02-03T00:00:00Z"),
            # A different docket.
            _comment("c4", "EPA-2", "EPA", "2025-01-09T00:00:00Z"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(staging, COMMENT), COMMENT)

    out = tmp_path / "output"
    index_file = iceberg._build_comments_index(con, COMMENT, out)
    assert index_file == out / "comments_index.parquet"

    df = pl.read_parquet(index_file)
    assert set(df.columns) == {"agency_code", "docket_id", "year", "month", "row_count"}
    got = {
        (r["agency_code"], r["docket_id"], r["year"], r["month"]): r["row_count"]
        for r in df.iter_rows(named=True)
    }
    assert got == {
        ("EPA", "EPA-1", 2025, 1): 2,
        ("EPA", "EPA-1", 2025, 2): 1,
        ("EPA", "EPA-2", 2025, 1): 1,
    }


def test_build_comments_index_rebuilds_after_merge(tmp_path, local_catalog) -> None:
    """A second merge (new rows + a dedup'd update) must be reflected in a
    full index rebuild — the index always mirrors the current table."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)
    out = tmp_path / "output"

    s1 = tmp_path / "s1"
    _write_comment_staging(s1, "EPA", [_comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z")])
    iceberg._merge(con, iceberg._staging_files(s1, COMMENT), COMMENT)
    iceberg._build_comments_index(con, COMMENT, out)

    s2 = tmp_path / "s2"
    _write_comment_staging(
        s2,
        "EPA",
        [
            # Re-posted c1 (same id) must not inflate the count.
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z", modify_date="2025-03-01T00:00:00Z"),
            _comment("c5", "EPA-1", "EPA", "2025-01-25T00:00:00Z"),
        ],
    )
    iceberg._merge(con, iceberg._staging_files(s2, COMMENT), COMMENT)
    index_file = iceberg._build_comments_index(con, COMMENT, out)

    df = pl.read_parquet(index_file)
    assert df.height == 1
    row = next(df.iter_rows(named=True))
    assert (row["agency_code"], row["docket_id"], row["year"], row["month"]) == ("EPA", "EPA-1", 2025, 1)
    assert row["row_count"] == 2  # c1 (deduped) + c5


def test_merge_comments_noop_without_staging(tmp_path) -> None:
    # No staged comments -> returns None and never touches the catalog.
    assert iceberg.merge_comments(tmp_path / "empty", tmp_path / "out", COMMENT) is None


# --- catalog seed loader ---------------------------------------------------


def _write_partition(comments_dir: Path, agency: str, docket: str, year: int, month: int, rows: list[dict]) -> None:
    """Write a published-layout partition file: agency_code=/docket_id=/year=/month=/part-0.parquet."""
    part = (
        comments_dir
        / f"agency_code={agency}"
        / f"docket_id={docket}"
        / f"year={year}"
        / f"month={month}"
    )
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=COMMENT.schema).write_parquet(part / "part-0.parquet")


def test_seed_comments_from_parquet_loads_partition_tree(tmp_path, local_catalog) -> None:
    """The seed loader copies the published partition tree into the catalog table."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    comments_dir = tmp_path / "comments"
    _write_partition(
        comments_dir, "EPA", "EPA-1", 2025, 1,
        [
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z"),
            _comment("c2", "EPA-1", "EPA", "2025-01-20T00:00:00Z"),
        ],
    )
    _write_partition(
        comments_dir, "EPA", "EPA-1", 2025, 2,
        [_comment("c3", "EPA-1", "EPA", "2025-02-03T00:00:00Z")],
    )

    glob = str(comments_dir / "agency_code=EPA/docket_id=*/year=*/month=*/part-0.parquet")
    total = iceberg.seed_comments_from_parquet(con, glob, COMMENT)
    assert total == 3

    # agency_code / docket_id survive as real columns (read with hive off).
    rows = con.execute(
        f'SELECT comment_id, agency_code, docket_id FROM {iceberg._qualified(COMMENT)} '
        "ORDER BY comment_id"
    ).fetchall()
    assert rows == [
        ("c1", "EPA", "EPA-1"),
        ("c2", "EPA", "EPA-1"),
        ("c3", "EPA", "EPA-1"),
    ]


def test_seed_comments_replace_agency_is_idempotent(tmp_path, local_catalog) -> None:
    """Loading the same agency twice with replace_agency must not duplicate rows."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    comments_dir = tmp_path / "comments"
    _write_partition(
        comments_dir, "EPA", "EPA-1", 2025, 1,
        [
            _comment("c1", "EPA-1", "EPA", "2025-01-15T00:00:00Z"),
            _comment("c2", "EPA-1", "EPA", "2025-01-20T00:00:00Z"),
        ],
    )
    glob = str(comments_dir / "agency_code=EPA/docket_id=*/year=*/month=*/part-0.parquet")

    first = iceberg.seed_comments_from_parquet(con, glob, COMMENT, replace_agency="EPA")
    assert first == 2
    # Re-running the same agency replaces, not appends.
    second = iceberg.seed_comments_from_parquet(con, glob, COMMENT, replace_agency="EPA")
    assert second == 2

    # A different agency present in the table is untouched by replacing EPA.
    _write_partition(
        comments_dir, "DOL", "DOL-1", 2025, 3,
        [_comment("d1", "DOL-1", "DOL", "2025-03-01T00:00:00Z")],
    )
    dol_glob = str(comments_dir / "agency_code=DOL/docket_id=*/year=*/month=*/part-0.parquet")
    iceberg.seed_comments_from_parquet(con, dol_glob, COMMENT, replace_agency="DOL")
    iceberg.seed_comments_from_parquet(con, glob, COMMENT, replace_agency="EPA")  # again
    counts = dict(
        con.execute(
            f'SELECT agency_code, count(*) FROM {iceberg._qualified(COMMENT)} GROUP BY agency_code'
        ).fetchall()
    )
    assert counts == {"EPA": 2, "DOL": 1}


def test_seed_comments_tolerates_missing_columns(tmp_path, local_catalog) -> None:
    """An older partition missing a later-added column loads with NULLs, not an error."""
    con = local_catalog
    iceberg._ensure_table(con, COMMENT)

    # A partition file written with a reduced (older) schema — no text_content etc.
    reduced = {"comment_id": pl.Utf8, "docket_id": pl.Utf8, "agency_code": pl.Utf8, "posted_date": pl.Utf8}
    part = tmp_path / "comments" / "agency_code=EPA" / "docket_id=EPA-9" / "year=2024" / "month=5"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{"comment_id": "old1", "docket_id": "EPA-9", "agency_code": "EPA", "posted_date": "2024-05-01T00:00:00Z"}],
        schema=reduced,
    ).write_parquet(part / "part-0.parquet")

    glob = str(tmp_path / "comments" / "agency_code=*/docket_id=*/year=*/month=*/part-0.parquet")
    total = iceberg.seed_comments_from_parquet(con, glob, COMMENT)
    assert total == 1

    text_content = con.execute(
        f"SELECT text_content FROM {iceberg._qualified(COMMENT)} WHERE comment_id = 'old1'"
    ).fetchone()[0]
    assert text_content is None

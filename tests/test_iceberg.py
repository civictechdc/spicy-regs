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

from spicy_regs.schemas import DOCKET
from spicy_regs.sources import iceberg


def _write_staging(staging_dir: Path, agency: str, rows: list[dict]) -> None:
    """Mimic transforms.write_staging: staging_dir/dockets/{agency}.parquet."""
    type_dir = staging_dir / DOCKET.name
    type_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=DOCKET.schema).write_parquet(type_dir / f"{agency}.parquet")


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

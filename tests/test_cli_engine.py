"""Tests for the CLI's DuckDB query engine (offline except the integration test)."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from spicy_regs.cli import engine
from spicy_regs.cli._output import jsonify
from tests.conftest import COMMENT_SCHEMA, DOCKET_SCHEMA, write_parquet_from_dicts


@pytest.fixture
def data_dir(tmp_path: Path, sample_dockets: list[dict]) -> Path:
    write_parquet_from_dicts(tmp_path / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
    return tmp_path


def test_local_view_specs_only_lists_present_files(data_dir: Path):
    specs = engine.local_view_specs(data_dir)
    assert set(specs) == {"dockets"}
    assert specs["dockets"].kind == "local"
    assert specs["dockets"].location == str(data_dir / "dockets.parquet")


def test_local_view_specs_partitioned_comments(data_dir: Path, sample_comments: list[dict]):
    partition = data_dir / "comments" / "agency_code=EPA"
    partition.mkdir(parents=True)
    records = [{k: v for k, v in c.items() if k != "agency_code"} for c in sample_comments if c["agency_code"] == "EPA"]
    schema = {k: v for k, v in COMMENT_SCHEMA.items() if k != "agency_code"}
    write_parquet_from_dicts(partition / "part-0.parquet", records, schema)

    specs = engine.local_view_specs(data_dir)
    assert set(specs) == {"dockets", "comments"}
    assert "hive_partitioning=true" in specs["comments"].sql

    con = engine.connect({"comments": specs["comments"]})
    result = engine.run_query(con, "SELECT DISTINCT agency_code FROM comments", max_rows=0)
    assert result.rows == [("EPA",)]


def test_remote_view_specs_cover_all_tables():
    specs = engine.remote_view_specs("https://example.com/base/")
    assert set(specs) == set(engine.TABLES)
    assert specs["dockets"].location == "https://example.com/base/dockets.parquet"
    assert specs["dockets"].kind == "r2"


def test_resolve_auto_prefers_local_and_falls_back_to_remote(data_dir: Path):
    specs = engine.resolve_view_specs("auto", data_dir, "https://example.com")
    assert set(specs) == set(engine.TABLES)
    assert specs["dockets"].kind == "local"
    assert all(specs[t].kind == "r2" for t in engine.TABLES if t != "dockets")


def test_resolve_rejects_unknown_source(tmp_path: Path):
    with pytest.raises(ValueError, match="Unknown source"):
        engine.resolve_view_specs("ftp", tmp_path, "https://example.com")


def test_run_query_truncation(data_dir: Path):
    con = engine.connect(engine.local_view_specs(data_dir))
    truncated = engine.run_query(con, "SELECT docket_id FROM dockets ORDER BY 1", max_rows=2)
    assert len(truncated.rows) == 2
    assert truncated.truncated is True

    exact = engine.run_query(con, "SELECT docket_id FROM dockets ORDER BY 1", max_rows=3)
    assert len(exact.rows) == 3
    assert exact.truncated is False

    unlimited = engine.run_query(con, "SELECT docket_id FROM dockets ORDER BY 1", max_rows=0)
    assert len(unlimited.rows) == 3
    assert unlimited.truncated is False


def test_connect_skips_unreadable_view(data_dir: Path, capsys):
    missing = engine.ViewSpec(
        table="documents",
        kind="local",
        location=str(data_dir / "documents.parquet"),
        sql=f"SELECT * FROM read_parquet('{data_dir / 'documents.parquet'}')",
    )
    con = engine.connect({**engine.local_view_specs(data_dir), "documents": missing})
    assert "warning: table documents not available" in capsys.readouterr().err
    # The healthy view still works.
    result = engine.run_query(con, "SELECT count(*) FROM dockets", max_rows=1)
    assert result.rows == [(3,)]


def test_escape_sql_string():
    assert engine.escape_sql_string("O'Brien") == "O''Brien"


def test_jsonify_coercions():
    assert jsonify(datetime(2024, 6, 1, 12, 30)) == "2024-06-01T12:30:00"
    assert jsonify(Decimal("1.50")) == "1.50"
    assert jsonify(b"\x01\xff") == "01ff"
    assert jsonify([1, Decimal("2")]) == [1, "2"]
    assert jsonify({"k": datetime(2024, 1, 1)}) == {"k": "2024-01-01T00:00:00"}
    assert jsonify(None) is None


@pytest.mark.integration
def test_remote_describe_against_live_r2(tmp_path: Path):
    specs = engine.resolve_view_specs("r2", tmp_path)
    con = engine.connect({"dockets": specs["dockets"]})
    result = engine.run_query(con, "DESCRIBE dockets", max_rows=0)
    column_names = {row[0] for row in result.rows}
    assert {"docket_id", "agency_code", "title"} <= column_names

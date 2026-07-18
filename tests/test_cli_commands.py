"""End-to-end tests of the spicy-regs commands against local parquet fixtures."""

import csv
import io
import json
from pathlib import Path

import pytest

from spicy_regs.cli import main
from tests.conftest import COMMENT_SCHEMA, DOCKET_SCHEMA, DOCUMENT_SCHEMA, write_parquet_from_dicts


@pytest.fixture
def data_dir(
    tmp_path: Path, sample_dockets: list[dict], sample_documents: list[dict], sample_comments: list[dict]
) -> Path:
    write_parquet_from_dicts(tmp_path / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
    write_parquet_from_dicts(tmp_path / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)
    write_parquet_from_dicts(tmp_path / "comments.parquet", sample_comments, COMMENT_SCHEMA)
    return tmp_path


def run_cli(args: list[str]) -> int:
    with pytest.raises(SystemExit) as excinfo:
        main(args)
    code = excinfo.value.code
    assert isinstance(code, int)
    return code


def test_query_table_format(data_dir: Path, capsys):
    sql = "SELECT agency_code, count(*) AS n FROM dockets GROUP BY 1 ORDER BY 1"
    assert run_cli(["query", sql, "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "agency_code" in out
    assert "EPA" in out
    assert "(2 rows)" in out


def test_query_json_format(data_dir: Path, capsys):
    sql = "SELECT docket_id FROM dockets ORDER BY 1 LIMIT 1"
    assert run_cli(["query", sql, "--source", "local", "-o", str(data_dir), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out) == [{"docket_id": "EPA-2024-0001"}]


def test_query_joins_across_tables(data_dir: Path, capsys):
    sql = (
        "SELECT d.docket_id, count(c.comment_id) AS n_comments "
        "FROM dockets d JOIN comments c USING (docket_id) "
        "GROUP BY 1 ORDER BY n_comments DESC, d.docket_id LIMIT 1"
    )
    assert run_cli(["query", sql, "--source", "local", "-o", str(data_dir), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out) == [{"docket_id": "EPA-2024-0001", "n_comments": 2}]


def test_query_csv_to_output_file(data_dir: Path, tmp_path: Path, capsys):
    out_file = tmp_path / "result.csv"
    sql = "SELECT docket_id, agency_code FROM dockets ORDER BY 1"
    args = ["query", sql, "--source", "local", "-o", str(data_dir), "--format", "csv", "--output", str(out_file)]
    assert run_cli(args) == 0
    assert f"Wrote 3 rows to {out_file}" in capsys.readouterr().out
    rows = list(csv.reader(io.StringIO(out_file.read_text())))
    assert rows[0] == ["docket_id", "agency_code"]
    assert len(rows) == 4


def test_query_max_rows_truncates(data_dir: Path, capsys):
    sql = "SELECT docket_id FROM dockets ORDER BY 1"
    assert run_cli(["query", sql, "--source", "local", "-o", str(data_dir), "--max-rows", "1"]) == 0
    assert "more available" in capsys.readouterr().out


def test_query_bad_sql_fails_cleanly(data_dir: Path, capsys):
    assert run_cli(["query", "SELECT nope FROM missing", "--source", "local", "-o", str(data_dir)]) == 1
    assert "Query failed" in capsys.readouterr().err


def test_query_empty_local_dir_errors(tmp_path: Path, capsys):
    assert run_cli(["query", "SELECT 1", "--source", "local", "-o", str(tmp_path)]) == 1
    assert "No tables available" in capsys.readouterr().err


def test_tables_lists_local_and_missing(data_dir: Path, capsys):
    assert run_cli(["tables", "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "dockets" in out
    assert "Not available from this source" in out
    assert "feed_summary" in out


def test_tables_json(data_dir: Path, capsys):
    assert run_cli(["tables", "--source", "local", "-o", str(data_dir), "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert {entry["table"] for entry in payload} == {"dockets", "documents", "comments"}
    assert all(entry["source"] == "local" for entry in payload)


def test_describe_local_table(data_dir: Path, capsys):
    assert run_cli(["describe", "dockets", "--source", "local", "-o", str(data_dir), "--format", "json"]) == 0
    columns = {row["column_name"] for row in json.loads(capsys.readouterr().out)}
    assert {"docket_id", "agency_code", "title"} <= columns


def test_describe_unavailable_table(data_dir: Path, capsys):
    assert run_cli(["describe", "feed_summary", "--source", "local", "-o", str(data_dir)]) == 1
    assert "not available locally" in capsys.readouterr().err


def test_stats(data_dir: Path, capsys):
    assert run_cli(["stats", "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "DOCKETS" in out
    assert "Rows: 3" in out
    assert "EPA: 2" in out


def test_sample_with_agency_filter(data_dir: Path, capsys):
    assert run_cli(["sample", "dockets", "-n", "10", "--agency", "EPA", "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "EPA-2024-0001" in out
    assert "FDA-2024-0010" not in out


def test_search_hits_multiple_tables(data_dir: Path, capsys):
    assert run_cli(["search", "drug", "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "DOCKETS" in out
    assert "FDA-2024-0010" in out


def test_search_respects_limit(data_dir: Path, capsys):
    assert run_cli(["search", "e", "--limit", "1", "--source", "local", "-o", str(data_dir)]) == 0
    assert "showing first matches" in capsys.readouterr().out


def test_agencies(data_dir: Path, capsys):
    assert run_cli(["agencies", "--source", "local", "-o", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "EPA" in out
    assert "FDA" in out


def test_no_command_prints_help(capsys):
    assert run_cli([]) == 0
    assert "Available commands" in capsys.readouterr().out

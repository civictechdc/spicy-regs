"""Golden authority parser rollup and Public-Law join coverage."""

from __future__ import annotations

import pyarrow.parquet as pq

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_authority_edges import COLUMNS, build_authority_edges


def test_authority_edges_retain_failures_and_join_public_laws(tmp_path):
    write_parquet_rows(
        tmp_path / "unified_agenda.parquet",
        columns=("rin", "agenda_edition", "legal_authority_json"),
        rows=[
            {
                "rin": "2060-AV12",
                "agenda_edition": "202404",
                "legal_authority_json": (
                    '["42 U.S.C. 7401 et seq.", "Public Law 117-58", "sec. 553 of title 5", "Clean Air Act"]'
                ),
            }
        ],
    )
    output = build_authority_edges(
        tmp_path,
        run_id="authority-golden",
        asserted_at="2026-07-23T12:00:00Z",
    )
    rows = pq.read_table(output).to_pylist()
    assert pq.ParquetFile(output).schema_arrow.names == list(COLUMNS)
    assert {(row["usc_title"], row["usc_section"]) for row in rows if row["authority_type"] == "usc"} == {
        ("42", "7401"),
        ("5", "553"),
    }
    assert [row["pl_number"] for row in rows if row["authority_type"] == "public_law"] == ["117-58"]
    failed = [row for row in rows if row["parse_status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["authority_raw"] == "Clean Air Act"

    write_parquet_rows(
        tmp_path / "congress_bills.parquet",
        columns=("bill_id", "pl_number"),
        rows=[{"bill_id": "117-hr-3684", "pl_number": "117-58"}],
    )
    import duckdb

    joined = duckdb.sql(
        f"""
        SELECT a.pl_number, b.bill_id
        FROM read_parquet('{output}') a
        JOIN read_parquet('{tmp_path / "congress_bills.parquet"}') b USING (pl_number)
        """
    ).fetchall()
    assert joined == [("117-58", "117-hr-3684")]

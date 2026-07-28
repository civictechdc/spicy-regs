"""Golden authority parser rollup and Public-Law join coverage."""

from __future__ import annotations

import json

import pyarrow.parquet as pq

from spicy_regs.ontology.common import ATTESTATION_COLUMNS, write_parquet_rows
from spicy_regs.transforms.build_authority_edges import (
    COLUMNS,
    IDENTITY_COLUMNS,
    build_authority_edges,
)


def _edges(tmp_path, authorities: list[str]) -> list[dict]:
    """Build the table from one agenda entry and return its rows."""
    write_parquet_rows(
        tmp_path / "unified_agenda.parquet",
        columns=("rin", "agenda_edition", "legal_authority_json"),
        rows=[
            {
                "rin": "2060-AV12",
                "agenda_edition": "202404",
                "legal_authority_json": json.dumps(authorities),
            }
        ],
    )
    output = build_authority_edges(
        tmp_path,
        run_id="authority-regression",
        asserted_at="2026-07-27T12:00:00Z",
    )
    return pq.read_table(output).to_pylist()


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


def test_two_executive_orders_in_one_authority_string_stay_two_rows(tmp_path):
    """The dedup key must discriminate rows that differ only by EO number.

    These two rows share every other emitted value — same RIN, edition, raw
    string, type, and status — so a key that omitted ``executive_order``
    silently collapsed one of the orders.
    """
    rows = _edges(tmp_path, ["Executive Order 13985 and Executive Order 14008"])

    orders = [row["executive_order"] for row in rows if row["authority_type"] == "eo"]
    assert sorted(orders) == ["13985", "14008"]
    assert all(row["authority_raw"] == "Executive Order 13985 and Executive Order 14008" for row in rows)


def test_statute_at_large_rows_carry_their_parsed_fields(tmp_path):
    """Statutes at Large survive parsing with volume-page intact, one row each."""
    rows = _edges(tmp_path, ["Pub. L. 108-173, 117 Stat. 2066", "117 Stat. 429 and 118 Stat. 430"])

    statutes = [row for row in rows if row["authority_type"] == "statute_at_large"]
    assert sorted(row["statute_at_large"] for row in statutes) == ["117-2066", "117-429", "118-430"]
    assert all(row["usc_title"] is None and row["pl_number"] is None for row in statutes)
    # The Public Law cited alongside its Statutes at Large page keeps its own row.
    assert [row["pl_number"] for row in rows if row["authority_type"] == "public_law"] == ["108-173"]


def test_genuinely_identical_citations_still_collapse(tmp_path):
    """Deduplication still applies when nothing distinguishes two parses."""
    rows = _edges(tmp_path, ["E.O. 12866", "E.O. 12866", "42 U.S.C. 7401", "42 U.S.C. 7401"])

    assert len(rows) == 2
    assert {(row["authority_type"], row["executive_order"], row["usc_section"]) for row in rows} == {
        ("eo", "12866", None),
        ("usc", None, "7401"),
    }


def test_cited_range_is_published_as_endpoints_and_answers_a_7401_filter(tmp_path):
    """The regression behind discovery question 2's recall of 0.8125.

    All five missing RINs stated authority as ``42 U.S.C. 7401-7671q.``; the
    hyphenated tail was published as one opaque section, invisible to an exact
    filter. Both spellings of the range now answer ``usc_section = '7401'``,
    and both carry the far endpoint instead of discarding it.
    """
    rows = _edges(tmp_path, ["42 U.S.C. 7401-7671q.", "42 U.S.C. 7401 to 7671q."])

    assert [(row["usc_section"], row["usc_section_end"]) for row in rows] == [("7401", "7671q")] * 2
    matched = [row for row in rows if (row["usc_title"], row["usc_section"]) == ("42", "7401")]
    assert len(matched) == 2
    # Two raw spellings, two rows: the range is not multiplied into members.
    assert sorted(row["authority_raw"] for row in rows) == [
        "42 U.S.C. 7401 to 7671q.",
        "42 U.S.C. 7401-7671q.",
    ]


def test_no_section_reaches_the_table_that_its_source_string_does_not_name(tmp_path):
    """Ranges are never expanded into members, real or invented.

    ``7401-7671q`` is the whole Clean Air Act. Its sections are sparse and
    lettered, so materializing the interval would publish sections that may not
    exist — false positives in place of the false negatives, on a filter whose
    precision is 1.000.
    """
    raws = [
        "42 U.S.C. 7401-7671q.",
        "42 U.S.C. 7401 to 7671q.",
        "42 U.S.C. 1395-1397",
        "42 U.S.C. 7401, 7412, and 7413 et seq.",
        "12 U.S.C. 1831p-1",
    ]
    rows = _edges(tmp_path, raws)

    for row in rows:
        for value in (row["usc_section"], row["usc_section_end"]):
            if value is not None:
                assert value in row["authority_raw"].lower(), row
    # 1396 lies inside a cited range and is named by nothing; it must be absent.
    assert "1396" not in {row["usc_section"] for row in rows}
    assert len(rows) == len(raws) + 2  # the three-section list adds two members


def test_every_emitted_citation_column_discriminates_rows(tmp_path):
    """No published parse field may sit outside the dedup key.

    ``IDENTITY_COLUMNS`` is derived from ``COLUMNS`` precisely so a future
    parsed field cannot be published without discriminating rows; this pins the
    consequence rather than the derivation.
    """
    assert set(COLUMNS) - set(IDENTITY_COLUMNS) == set(ATTESTATION_COLUMNS)

    rows = _edges(
        tmp_path,
        ["Executive Order 13985 and Executive Order 14008", "117 Stat. 429 and 118 Stat. 430"],
    )
    keys = [tuple(row[column] for column in IDENTITY_COLUMNS) for row in rows]
    assert len(keys) == len(set(keys)) == 4

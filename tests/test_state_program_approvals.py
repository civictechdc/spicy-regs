"""Hermetic tests for the ``state_program_approvals`` rollup (no network).

Body fixtures are trimmed from real federalregister.gov full-text HTML:
2026-07885 (Virginia, 9VAC5-10-20) and 2026-20635 (Maryland, COMAR 26.11.08).
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs.sources.fr_bodies import FrBodyUnavailable
from spicy_regs.transforms.state_programs import (
    CITATION_STATES,
    PARSER_VERSION,
    extract_citations,
    program_for,
    states_in_title,
)

# The package re-exports the build function under the module's own name.
bspa = importlib.import_module("spicy_regs.transforms.build_state_program_approvals")

# -- CFR allow-list -----------------------------------------------------------


@pytest.mark.parametrize(
    "refs, expected",
    [
        ([{"title": 40, "part": 52}], ("sip", 40, 52)),
        ([{"title": "40", "part": "52"}], ("sip", 40, 52)),  # string-typed part (recent rows)
        ([{"title": 40, "part": "271"}], ("rcra", 40, 271)),
        ([{"title": 30, "part": 946}], ("smcra", 30, 946)),
        ([{"title": 29, "part": "1952"}], ("osha_state_plan", 29, 1952)),
        # First allow-listed reference wins; non-listed ones before it are skipped.
        ([{"title": 40, "part": 81}, {"title": 40, "part": 52}, {"title": 40, "part": 70}], ("sip", 40, 52)),
    ],
)
def test_program_for_allow_listed(refs, expected):
    assert program_for(refs) == expected


@pytest.mark.parametrize(
    "refs",
    [
        None,
        [],
        [{"title": 33, "part": 165}],  # Coast Guard safety zone: names a state, isn't a state program
        [{"title": 33, "part": 117}],  # drawbridge
        [{"title": 40, "part": 63}],  # NESHAP delegation, deliberately excluded
        [{"title": 40, "part": None}],
        [{"title": 40, "part": "52a"}],
        ["not a dict"],
    ],
)
def test_program_for_rejects(refs):
    assert program_for(refs) is None


# -- State matching -----------------------------------------------------------


@pytest.mark.parametrize(
    "title, expected",
    [
        ("Air Plan Approval; Virginia; Definition of Volatile Organic Compound", ["VA"]),
        ("Air Plan Approval; West Virginia; Regional Haze", ["WV"]),
        (
            "Approval and Promulgation of Air Quality Implementation Plans; District of Columbia, Maryland, "
            "and Virginia; Attainment Demonstration",
            ["DC", "MD", "VA"],
        ),
        ("State Implementation Plans; States of Arizona, California, and Nevada; Correction", ["AZ", "CA", "NV"]),
        ("Approval of Air Quality Implementation Plans; State of Utah; Logan, Utah-Idaho PM2.5", ["UT"]),
        ("Air Plan Approval; FL; GA; KY: Definition of Chemical Process Plants", ["FL", "GA", "KY"]),
        ("Air Plan Approval; NC; Miscellaneous NSR Revisions", ["NC"]),
        ("Air Plan Approval; ID, Updates to Incorporations by Reference", ["ID"]),
        ("Air Plan Approval; OR; Regional Haze Plan", ["OR"]),
        ("Approval of State Plans for Designated Facilities; City of Memphis, TN; Hospital Incinerators", ["TN"]),
        # No segment: first mention wins.
        ("Revisions to the California State Implementation Plan, South Coast AQMD", ["CA"]),
        ("Virginia Regulatory Program", ["VA"]),
        ("Approval and Promulgation of Implementation Plans Kentucky: 1-Hour Ozone Maintenance Plan", ["KY"]),
        # Area and company names don't add states.
        (
            "Air Plan Approval; Indiana; Redesignation of the Indiana Portion of the Cincinnati, "
            "Ohio-Kentucky-Indiana Area",
            ["IN"],
        ),
        ("Air Plan Approval; Virginia; Source-Specific Permits in the Metropolitan Washington Area", ["VA"]),
        ("Air Plan Approval; Virginia; Opacity Limit for Dryer Stacks at Georgia-Pacific Corporation", ["VA"]),
        ("Determination of Attainment; Washington, DC-MD-VA Nonattainment Area", ["DC"]),
        ("Ozone Maintenance Plan for the Kansas City Area, Missouri", ["MO"]),
        ("Air Plan Approval; Washington; Spokane Carbon Monoxide", ["WA"]),
        # National rules name no state.
        ("Finding of Failure To Submit State Implementation Plans for the 2015 Ozone NAAQS", []),
        ("", []),
        (None, []),
    ],
)
def test_states_in_title(title, expected):
    assert states_in_title(title) == expected


# -- Citation extraction ------------------------------------------------------

_VA_BODY = """
<div class="body">
<p>Revision A23 updates the definition of VOC in the Virginia SIP (9 VAC 5-10-20 (General Definitions)).
The Virginia State Air Pollution Control Board adopted the revision on September 13, 2023, and the revision
became effective on April 11, 2024.</p>
<p>DATES: This final rule is effective on May 26, 2026.</p>
<p>... adding the entry "Section 5-10-20" after the entry (with the state effective day of 5/19/17) ...</p>
<table class="wide"><caption><p>EPA&mdash;Approved Virginia Regulations and Statutes</p></caption>
<thead><tr><th>State citation</th><th>Title/subject</th><th>State
   effective
   date</th><th>EPA approval date</th><th>Explanation [former SIP citation]</th></tr></thead>
<tbody>
<tr><td colspan="5"><strong>9 VAC 5, Chapter 10 General Definitions [Part I]</strong></td></tr>
<tr><td>&emsp;</td><td></td><td></td><td></td><td></td></tr>
<tr><td colspan="5">* * * * * * *</td></tr>
<tr><td>5-10-20</td><td>Terms Defined</td><td>04/11/24</td><td>4/23/2026, 90 FR [INSERT PAGE]</td><td>Revised.</td></tr>
</tbody></table>
</div>
"""

_MD_BODY = """
<p>Code of Maryland Regulations (COMAR) 26.11.08.01 defines large combustors.</p>
<p>See also COMAR 26.11.09.08 and 40 CFR 52.1070.</p>
<p>2. Amend &sect; 52.1070, the table in paragraph (c) by revising the entries for "10.18.08/26.11.08.01".</p>
<table><thead><tr><th>Citation</th><th>Title/subject</th><th>State effective date</th>
<th>EPA approval date</th><th>Additional explanation</th></tr></thead>
<tbody>
<tr><td colspan="5"><strong>Code of Maryland Administrative Regulations (COMAR)</strong></td></tr>
<tr><td colspan="5"><strong>26.11.08 Control of Incinerators</strong></td></tr>
<tr><td>10.18.08/26.11.08.01</td><td>Definitions</td><td>5/4/2020</td><td>10/8/2026</td><td></td></tr>
<tr><td>26.11.08.07</td><td>Small Municipal Waste Combustors</td><td>12/06/2018</td><td>10/8/2026</td><td>Added.</td></tr>
</tbody></table>
<table><tr><th>Pollutant</th><th>Limit</th></tr><tr><td>26.11.99.99</td><td>5 ppm</td></tr></table>
"""


def test_extract_virginia():
    citations, effective = extract_citations("VA", _VA_BODY)
    assert citations == ["9VAC5-10-20"]
    # From the amendatory table, not the prose about the *prior* entry (5/19/17)
    # or the federal rule's own effective date.
    assert effective == "2024-04-11"


def test_extract_virginia_relative_table_cells_and_chapters():
    body = (
        "<p>See 9VAC5-140 and 9 VAC 5-40-7800.</p>"
        "<table><tr><th>State citation</th><th>Title</th><th>State effective date</th></tr>"
        "<tr><td colspan='3'>9 VAC 5, Chapter 40</td></tr>"
        "<tr><td>5-40-7810</td><td>Applicability</td><td>01/01/2020</td></tr></table>"
    )
    citations, _ = extract_citations("VA", body)
    assert citations == ["9VAC5-140", "9VAC5-40-7800", "9VAC5-40-7810"]


def test_extract_maryland():
    citations, effective = extract_citations("MD", _MD_BODY)
    # Prose (COMAR-prefixed) + amendatory-table cells, renumbered entries split.
    # The non-amendatory table's "26.11.99.99" is ignored.
    assert citations == [
        "COMAR 10.18.08",
        "COMAR 26.11.08.01",
        "COMAR 26.11.08.07",
        "COMAR 26.11.09.08",
    ]
    assert effective == "2020-05-04"  # latest date in the state-effective-date column


def test_extract_proposed_rule_prose_date_and_empty():
    body = "<p>The regulation became effective on March 3, 2025 in Maryland.</p><p>No citations here.</p>"
    assert extract_citations("MD", body) == ([], "2025-03-03")
    assert extract_citations("VA", "<p>nothing</p>") == ([], None)


def test_extract_unsupported_state():
    assert "TX" not in CITATION_STATES
    with pytest.raises(KeyError):
        extract_citations("TX", "<p></p>")


# -- Phase 2 orchestration ----------------------------------------------------


def _row(doc: str, state: str, url: str | None = None) -> dict:
    return {
        "document_number": doc,
        "publication_date": "2026-01-01",
        "jurisdiction": state,
        "state_citations_json": None,
        "state_effective_date": None,
        "_body_html_url": url if url is not None else f"https://fr.test/{doc}.html",
    }


def test_attach_citations_reuses_prior_and_fetches_each_body_once():
    rows = [_row("A", "VA"), _row("B", "VA"), _row("B", "MD"), _row("C", "TX")]
    prior = {("A", "VA"): ('["9VAC5-1-1"]', "2020-01-01")}
    calls: list[str] = []

    def fetch(url: str) -> str:
        calls.append(url)
        return _VA_BODY + _MD_BODY

    fetched, failed = bspa.attach_citations(rows, prior, fetch, pause_s=0)
    assert (fetched, failed) == (1, 0)
    assert calls == ["https://fr.test/B.html"]  # A reused; B fetched once for VA + MD; TX unsupported
    by_key = {(r["document_number"], r["jurisdiction"]): r for r in rows}
    assert by_key["A", "VA"]["state_citations_json"] == '["9VAC5-1-1"]'
    assert json.loads(by_key["B", "VA"]["state_citations_json"]) == ["9VAC5-10-20"]
    assert "COMAR 26.11.08.07" in json.loads(by_key["B", "MD"]["state_citations_json"])
    assert by_key["C", "TX"]["state_citations_json"] is None


def test_attach_citations_caps_fetches_newest_first():
    rows = [_row(f"D{i}", "VA") | {"publication_date": f"2026-01-{i + 1:02d}"} for i in range(5)]
    calls: list[str] = []

    def fetch(url: str) -> str:
        calls.append(url)
        return _VA_BODY

    assert bspa.attach_citations(rows, {}, fetch, pause_s=0, max_fetches=2) == (2, 0)
    assert calls == ["https://fr.test/D4.html", "https://fr.test/D3.html"]
    assert [r["state_citations_json"] is not None for r in rows] == [False, False, False, True, True]


def test_attach_citations_tolerates_isolated_failures():
    rows = [_row(str(i), "VA") for i in range(100)]

    def fetch(url: str) -> str:
        if url.endswith("/7.html"):
            raise FrBodyUnavailable(f"{url}: HTTP 404")
        return _VA_BODY

    fetched, failed = bspa.attach_citations(rows, {}, fetch, pause_s=0)
    assert (fetched, failed) == (99, 1)
    assert rows[7]["state_citations_json"] is None  # retried next run
    assert rows[8]["state_citations_json"] == '["9VAC5-10-20"]'


def test_attach_citations_aborts_on_widespread_failure():
    rows = [_row(str(i), "VA") for i in range(20)]

    def fetch(url: str) -> str:
        raise FrBodyUnavailable(f"{url}: HTTP 503 after 4 attempts")

    with pytest.raises(RuntimeError, match="refusing to write"):
        bspa.attach_citations(rows, {}, fetch, pause_s=0)


# -- End to end ---------------------------------------------------------------


def _write_fr(path: Path) -> None:
    def doc(num, title, refs, dtype="Rule", date="2026-04-23"):
        return {
            "document_number": num,
            "title": title,
            "document_type": dtype,
            "publication_date": date,
            "cfr_references_json": json.dumps(refs),
            "docket_ids_json": json.dumps([f"EPA-{num}"]),
            "html_url": f"https://www.federalregister.gov/d/{num}",
            "body_html_url": f"https://fr.test/{num}.html",
        }

    rows = [
        doc("2026-07885", "Air Plan Approval; Virginia; Definition of VOC", [{"title": 40, "part": 52}]),
        doc(
            "2025-05913",
            "Air Plan Approval; District of Columbia, Maryland, Virginia; Clean Data Determination",
            [{"title": "40", "part": "52"}],
            date="2025-04-04",
        ),
        doc("2024-00001", "Texas: Final Authorization of State Hazardous Waste", [{"title": 40, "part": 271}]),
        doc("2024-00002", "Safety Zone; James River, Virginia", [{"title": 33, "part": 165}]),
        doc("2024-00003", "Air Plan Approval; Virginia; Notice only", [{"title": 40, "part": 52}], dtype="Notice"),
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_build_end_to_end(tmp_path, monkeypatch):
    _write_fr(tmp_path / "federal_register.parquet")
    monkeypatch.setattr(bspa.r2, "download", lambda key, dest: False)
    monkeypatch.setattr(bspa, "FETCH_PAUSE_S", 0)

    fetched: list[str] = []

    def fetch(url: str) -> str:
        fetched.append(url)
        return _VA_BODY + _MD_BODY

    out = bspa.build_state_program_approvals(tmp_path, fetch=fetch)
    table = pq.read_table(out)

    # Schema matches what the data dictionary documents.
    con = duckdb.connect()
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()
    assert [(c[0], c[1]) for c in described] == dd.DERIVED_SCHEMAS["state_program_approvals"]

    rows = table.to_pylist()
    keys = [(r["jurisdiction"], r["document_number"]) for r in rows]
    # Sorted by jurisdiction, newest first within a state. Safety zone and the
    # Notice are excluded.
    assert keys == [
        ("DC", "2025-05913"),
        ("MD", "2025-05913"),
        ("TX", "2024-00001"),
        ("VA", "2026-07885"),
        ("VA", "2025-05913"),
    ]
    by_key = {(r["jurisdiction"], r["document_number"]): r for r in rows}
    va = by_key["VA", "2026-07885"]
    assert va["program"] == "sip" and (va["cfr_title"], va["cfr_part"]) == (40, 52)
    assert va["state_citations_json"] == '["9VAC5-10-20"]'
    assert va["state_effective_date"] == "2024-04-11"
    assert by_key["TX", "2024-00001"]["program"] == "rcra"
    assert by_key["TX", "2024-00001"]["state_citations_json"] is None
    assert by_key["DC", "2025-05913"]["state_citations_json"] is None
    assert sorted(fetched) == ["https://fr.test/2025-05913.html", "https://fr.test/2026-07885.html"]

    meta = pq.read_metadata(out).metadata
    assert meta[b"spicy_regs.citation_parser_version"] == str(PARSER_VERSION).encode()

    # A second run with the first output as the prior table fetches nothing.
    prior = tmp_path / "_state_program_approvals_prior.parquet"
    out.rename(prior)
    fetched.clear()
    bspa.build_state_program_approvals(tmp_path, fetch=fetch)
    assert fetched == []
    assert not prior.exists()


def test_prior_with_other_parser_version_is_ignored(tmp_path):
    prior = tmp_path / "prior.parquet"
    row = {c: None for c in bspa.COLUMNS} | {
        "document_number": "X",
        "jurisdiction": "VA",
        "state_citations_json": "[]",
    }
    table = pa.Table.from_pylist([row], schema=bspa.SCHEMA)
    pq.write_table(table.replace_schema_metadata({b"spicy_regs.citation_parser_version": b"0"}), prior)
    assert bspa.load_prior_citations(prior) == {}
    pq.write_table(
        table.replace_schema_metadata({b"spicy_regs.citation_parser_version": str(PARSER_VERSION).encode()}),
        prior,
    )
    assert bspa.load_prior_citations(prior) == {("X", "VA"): ("[]", None)}

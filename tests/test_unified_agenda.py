"""Hermetic tests for the Unified Agenda ingest (no network).

Covers the two pieces with real logic: parsing the reginfo.gov
``REGINFO_RIN_DATA`` XML export into normalized RIN dicts
(``UnifiedAgendaReader._fetch_edition`` / ``_normalize``) and mapping those onto
the published 17-column schema (``_shape``, including timetable-date derivation
and the deterministic per-RIN URL). The reader's HTTP download is monkeypatched
so no network is touched.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from spicy_regs.sources import unified_agenda
from spicy_regs.sources.unified_agenda import UnifiedAgendaReader
from spicy_regs.transforms.build_unified_agenda import COLUMNS, _iso_dates, _shape

# A two-record slice of the real export, matching the observed tag structure:
# <REGINFO_RIN_DATA> root, repeated <RIN_INFO> records, nested AGENCY / CFR_LIST /
# LEGAL_AUTHORITY_LIST / TIMETABLE_LIST. The second record exercises empty lists
# and an unparseable ("To Be Determined") timetable date.
_FIXTURE_XML = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<REGINFO_RIN_DATA RUN_DATE="2025-11-01-05:00">
    <RIN_INFO>
        <RIN>2060-AV12</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
            <PUBLICATION_TITLE>The Unified Agenda</PUBLICATION_TITLE>
        </PUBLICATION>
        <AGENCY>
            <CODE>2060</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </AGENCY>
        <PARENT_AGENCY>
            <CODE>2000</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </PARENT_AGENCY>
        <RULE_TITLE>A Planned Rule</RULE_TITLE>
        <ABSTRACT>Plans to do a thing.</ABSTRACT>
        <PRIORITY_CATEGORY>Other Significant</PRIORITY_CATEGORY>
        <RIN_STATUS>Active</RIN_STATUS>
        <RULE_STAGE>Proposed Rule Stage</RULE_STAGE>
        <MAJOR>Yes</MAJOR>
        <CFR_LIST>
            <CFR>40 CFR 60</CFR>
            <CFR>40 CFR 63</CFR>
        </CFR_LIST>
        <LEGAL_AUTHORITY_LIST>
            <LEGAL_AUTHORITY>42 U.S.C. 7401</LEGAL_AUTHORITY>
        </LEGAL_AUTHORITY_LIST>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>NPRM</TTBL_ACTION>
                <TTBL_DATE>06/15/2024</TTBL_DATE>
                <FR_CITATION>89 FR 12345</FR_CITATION>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>NPRM Comment Period End</TTBL_ACTION>
                <TTBL_DATE>08/14/2024</TTBL_DATE>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>Final Rule</TTBL_ACTION>
                <TTBL_DATE>03/01/2025</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
    <RIN_INFO>
        <RIN>1234-AB56</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
        </PUBLICATION>
        <AGENCY>
            <CODE>1234</CODE>
            <NAME>Some Agency</NAME>
        </AGENCY>
        <RULE_TITLE>Another Rule</RULE_TITLE>
        <RULE_STAGE>Long-Term Actions</RULE_STAGE>
        <CFR_LIST/>
        <LEGAL_AUTHORITY_LIST/>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>Interim Final Rule</TTBL_ACTION>
                <TTBL_DATE>To Be Determined</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
</REGINFO_RIN_DATA>
"""


def _read_fixture(edition: str = "202510") -> list[dict]:
    """Run the reader over the inline fixture with the network download stubbed."""
    reader = UnifiedAgendaReader(editions=(edition,))

    def fake_download(self, ed):
        assert ed == edition
        return _FIXTURE_XML

    # Patch the bound method on the class so iter_records exercises the real
    # iterparse + _normalize path against the fixture bytes. The stubbed
    # _download ignores the HTTP client, so none needs to be set.
    with patch.object(UnifiedAgendaReader, "_download", fake_download):
        return list(reader._fetch_edition(edition))


def test_reader_parses_records_and_stamps_edition():
    records = _read_fixture("202510")
    assert [r["rin"] for r in records] == ["2060-AV12", "1234-AB56"]
    # Edition is stamped on every record (half the dedup key).
    assert all(r["agenda_edition"] == "202510" for r in records)
    # AGENCY/ACRONYM wins for agency_code, falling back to CODE when absent.
    assert records[0]["agency_code"] == "EPA"
    assert records[1]["agency_code"] == "1234"
    assert records[0]["agency_name"] == "Environmental Protection Agency"


def test_reader_normalizes_lists_and_timetable():
    rec = _read_fixture("202510")[0]
    assert rec["cfr_references"] == ["40 CFR 60", "40 CFR 63"]
    assert rec["legal_authority"] == ["42 U.S.C. 7401"]
    assert rec["timetable"][0] == {
        "action": "NPRM",
        "date": "06/15/2024",
        "fr_citation": "89 FR 12345",
    }
    # Optional child (FR_CITATION) absent -> None.
    assert rec["timetable"][1]["fr_citation"] is None


def test_shape_produces_exact_schema():
    row = _shape(_read_fixture("202510")[0])
    assert set(row) == set(COLUMNS)


def test_shape_maps_serializes_and_derives():
    row = _shape(_read_fixture("202510")[0])
    assert row["rin"] == "2060-AV12"
    assert row["agenda_edition"] == "202510"
    assert row["rule_stage"] == "Proposed Rule Stage"
    assert row["priority_category"] == "Other Significant"
    assert row["major"] == "Yes"
    assert row["publication_id"] == "202510"
    # Array fields serialize to JSON strings.
    assert json.loads(row["timetable_json"])[0]["action"] == "NPRM"
    assert json.loads(row["cfr_references_json"]) == ["40 CFR 60", "40 CFR 63"]
    assert json.loads(row["legal_authority_json"]) == ["42 U.S.C. 7401"]
    # Timetable dates -> ISO; first = earliest, next = earliest later action.
    assert row["first_action_date"] == "2024-06-15"
    assert row["next_action_date"] == "2024-08-14"
    # Deterministic per-RIN reginfo detail URL.
    assert row["url"] == "https://www.reginfo.gov/public/do/eAgendaViewRule?pubId=202510&RIN=2060-AV12"


def test_shape_handles_missing_and_unparseable_dates():
    row = _shape(_read_fixture("202510")[1])
    # No parseable timetable dates -> both action dates null.
    assert row["first_action_date"] is None
    assert row["next_action_date"] is None
    # Empty lists still serialize to "[]".
    assert row["cfr_references_json"] == "[]"
    assert row["legal_authority_json"] == "[]"
    # Absent fields are null.
    assert row["abstract"] is None
    assert row["priority_category"] is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("06/15/2024", ["2024-06-15"]),
        ("02/29/2024", ["2024-02-29"]),  # a real leap day survives
        ("06/00/2024", ["2024-06-01"]),  # reginfo's month-only marker
        ("02/30/2024", []),  # never existed — dropped, not moved to the 29th
        ("02/29/2023", []),  # not a leap year
        ("04/31/2024", []),
        ("06/32/2024", []),  # was clamped to the 31st
        ("13/01/2024", []),
        ("00/15/2024", []),
        ("To Be Determined", []),
    ],
)
def test_iso_dates_rejects_impossible_calendar_dates(raw, expected):
    assert _iso_dates([{"action": "NPRM", "date": raw}]) == expected


def test_shape_drops_impossible_dates_without_fabricating_a_neighbour():
    doc = dict(_read_fixture("202510")[0])
    doc["timetable"] = [
        {"action": "NPRM", "date": "02/30/2024"},
        {"action": "Final Rule", "date": "07/04/2024"},
    ]
    row = _shape(doc)
    # The impossible date is absent, so the real one becomes the first action.
    assert row["first_action_date"] == "2024-07-04"
    assert row["next_action_date"] is None
    # The raw timetable is still carried through verbatim.
    assert json.loads(row["timetable_json"])[0]["date"] == "02/30/2024"


def test_download_rejects_non_xml_body(monkeypatch):
    """A non-XML body (e.g. the old eAgendaXmlReport HTML page) yields nothing."""

    class _Resp:
        status_code = 200
        content = b"<!DOCTYPE html><html><body>listing page</body></html>"

        def raise_for_status(self):
            return None

    reader = UnifiedAgendaReader(editions=("202510",))

    class _Client:
        def get(self, url, params=None):
            return _Resp()

    monkeypatch.setattr(reader, "_client", _Client())
    assert reader._download("202510") is None
    assert list(reader._fetch_edition("202510")) == []


def test_default_edition_is_yyyymm():
    ed = unified_agenda.DEFAULT_EDITION
    assert len(ed) == 6 and ed.isdigit()
    assert ed[4:] in {"04", "10"}

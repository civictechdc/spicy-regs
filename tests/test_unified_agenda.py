"""Hermetic tests for the Unified Agenda ingest (no network).

Covers the two pieces with real logic: the raw-entry → published-schema mapping
(``_shape``) and the edition-pagination loop the reader uses (following
``next_page_url`` and stamping the ``agenda_edition`` dedup key).
"""

from __future__ import annotations

import json

from spicy_regs.sources.unified_agenda import UnifiedAgendaReader
from spicy_regs.transforms.build_unified_agenda import COLUMNS, _shape

_RAW_ENTRY = {
    "rin": "2060-AV12",
    "agency_code": "EPA",
    "agency_name": "Environmental Protection Agency",
    "title": "A Planned Rule",
    "abstract": "Plans to do a thing.",
    "rin_status": "Active",
    "rule_stage": "Proposed Rule",
    "priority_category": "Other Significant",
    "agenda_edition": "202404",
    "major": True,
    "publication_id": "PUB-123",
    "timetable": [{"action": "NPRM", "date": "2024-06-00"}],
    "cfr_references": [{"title": 40, "part": 60}],
    "legal_authority": ["42 U.S.C. 7401"],
    "first_action_date": "2024-06-01",
    "next_action_date": "2024-12-01",
    "url": "https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=2060-AV12",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ENTRY)
    # Every published column present, and nothing extra.
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_ENTRY)
    assert row["rin"] == "2060-AV12"
    assert row["agenda_edition"] == "202404"
    assert row["rule_stage"] == "Proposed Rule"
    # Array fields serialize to JSON strings.
    assert json.loads(row["timetable_json"])[0]["action"] == "NPRM"
    assert json.loads(row["cfr_references_json"])[0]["part"] == 60
    assert json.loads(row["legal_authority_json"]) == ["42 U.S.C. 7401"]
    # Scalars stringify (schema is all-VARCHAR).
    assert row["major"] == "True"
    assert row["url"].startswith("https://www.reginfo.gov/")


def test_shape_accepts_camelcase_key_drift():
    """reginfo.gov field names are unverified, so common casings must resolve."""
    row = _shape(
        {
            "RIN": "1234-AB56",
            "agencyName": "Some Agency",
            "ruleStage": "Final Rule",
            "agendaEdition": "202410",
            "nextActionDate": "2025-01-15",
        }
    )
    assert row["rin"] == "1234-AB56"
    assert row["agency_name"] == "Some Agency"
    assert row["rule_stage"] == "Final Rule"
    assert row["agenda_edition"] == "202410"
    assert row["next_action_date"] == "2025-01-15"


def test_shape_handles_missing_arrays():
    row = _shape({"rin": "x", "agenda_edition": "202404"})
    assert row["timetable_json"] == "[]"
    assert row["cfr_references_json"] == "[]"
    assert row["legal_authority_json"] == "[]"
    assert row["abstract"] is None
    assert row["url"] is None


def test_reader_paginates_and_stamps_edition(monkeypatch):
    """The reader must follow ``next_page_url`` and stamp the agenda_edition key.

    We simulate a two-page edition response. Entries missing an inline edition
    must still receive one (it is half of the dedup key); an entry that already
    carries an edition is left untouched.
    """
    reader = UnifiedAgendaReader(editions=("202404",))

    pages = {
        None: {
            "count": 3,
            "results": [{"rin": "A"}, {"rin": "B"}],
            "next_page_url": "PAGE2",
        },
        "PAGE2": {
            "count": 3,
            "results": [{"rin": "C", "agenda_edition": "209999"}],
            "next_page_url": None,
        },
    }

    def fake_get(url, params):
        # First call passes params (url is the endpoint); paged calls pass the url token.
        key = None if params is not None else url
        return pages[key]

    monkeypatch.setattr(reader, "_get", fake_get)
    entries = list(reader._fetch_edition("202404"))

    assert [e["rin"] for e in entries] == ["A", "B", "C"]
    # Entries without an inline edition are stamped with the fetched edition.
    assert entries[0]["agenda_edition"] == "202404"
    assert entries[1]["agenda_edition"] == "202404"
    # An entry that already carried an edition is left as-is (setdefault).
    assert entries[2]["agenda_edition"] == "209999"

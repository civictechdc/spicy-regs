"""Hermetic tests for the CourtListener APA-litigation ingest (no network).

Covers the pieces with real logic: the raw-search-result → published-schema
mapping (``_shape``, including array-field JSON serialization and URL
absolutization) and the reader's cursor ``next``-following pagination +
``max_records`` bound.
"""

from __future__ import annotations

import json

from spicy_regs.sources.courtlistener import CourtListenerReader
from spicy_regs.transforms.build_courtlistener import COLUMNS, _shape

_RAW_DOCKET = {
    "docket_id": 73613631,
    "caseName": "HENNEPIN COUNTY, MINNESOTA v. U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
    "case_name_full": "",
    "court_id": "dcd",
    "court": "District Court, District of Columbia",
    "court_citation_string": "D.D.C.",
    "docketNumber": "1:26-cv-02460",
    "dateFiled": "2026-07-14",
    "dateTerminated": None,
    "dateArgued": None,
    "suitNature": "899 Other Statutes: Administrative Procedures Act/Review or Appeal of Agency Decision",
    "cause": "05:551 Administrative Procedure Act",
    "jurisdictionType": "U.S. Government Defendant",
    "juryDemand": "None",
    "assignedTo": "Christopher Reid Cooper",
    "referredTo": None,
    "party": [
        "HENNEPIN COUNTY, MINNESOTA",
        "U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
        None,  # blanks are dropped
    ],
    "attorney": ["Skye Perryman", "Allison Marcy Zieve"],
    "firm": ["Democracy Forward", "Public Citizen Litigation Group"],
    "pacer_case_id": "294455",
    "docket_absolute_url": "/docket/73613631/hennepin-county-minnesota-v-us-dept-of-hhs/",
    "meta": {"date_created": "2026-07-14T17:38:04.836183Z"},
    # Intentionally dropped by _shape (document-level, not docket-level):
    "recap_documents": [{"docket_entry_id": 471006633}],
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_DOCKET)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_DOCKET)
    # Integer id stringifies (schema is all-VARCHAR).
    assert row["cl_docket_id"] == "73613631"
    assert row["case_name"].startswith("HENNEPIN COUNTY")
    # Empty full caption normalizes to NULL, not "".
    assert row["case_name_full"] is None
    assert row["court_id"] == "dcd"
    assert row["court_citation_string"] == "D.D.C."
    assert row["docket_number"] == "1:26-cv-02460"
    assert row["date_filed"] == "2026-07-14"
    assert row["date_terminated"] is None
    assert row["nature_of_suit"].startswith("899 ")
    assert row["cause"] == "05:551 Administrative Procedure Act"
    assert row["jurisdiction_type"] == "U.S. Government Defendant"
    assert row["assigned_to"] == "Christopher Reid Cooper"
    assert row["pacer_case_id"] == "294455"
    assert row["date_created"] == "2026-07-14T17:38:04.836183Z"
    # docket_absolute_url is absolutized against the CourtListener host.
    assert row["absolute_url"] == (
        "https://www.courtlistener.com/docket/73613631/hennepin-county-minnesota-v-us-dept-of-hhs/"
    )
    # Array fields serialize to JSON, dropping blank/None entries.
    parties = json.loads(row["parties_json"])
    assert parties == [
        "HENNEPIN COUNTY, MINNESOTA",
        "U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
    ]
    assert json.loads(row["attorneys_json"]) == ["Skye Perryman", "Allison Marcy Zieve"]
    assert json.loads(row["firms_json"]) == ["Democracy Forward", "Public Citizen Litigation Group"]


def test_shape_handles_missing_fields():
    row = _shape({"docket_id": 42})
    assert row["cl_docket_id"] == "42"
    assert row["case_name"] is None
    assert row["parties_json"] == "[]"
    assert row["attorneys_json"] == "[]"
    assert row["firms_json"] == "[]"
    assert row["absolute_url"] is None
    assert row["date_created"] is None


def _page(ids: list[int], next_url: str | None) -> dict:
    return {
        "count": 99,
        "next": next_url,
        "previous": None,
        "results": [{"docket_id": i} for i in ids],
    }


def test_pagination_follows_cursor_next(monkeypatch):
    """The reader must follow the cursor ``next`` URL until it is null."""
    reader = CourtListenerReader()
    pages = {
        None: _page([1, 2], "CURSOR2"),  # first request (params carried; url is base)
        "CURSOR2": _page([3, 4], None),
    }
    calls: list[str | None] = []

    def fake_get(url: str, params: dict | None) -> dict | None:
        key = None if params is not None else url
        calls.append(key)
        return pages[key]

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [d["docket_id"] for d in reader._paginate()]
    assert got == [1, 2, 3, 4]
    assert calls == [None, "CURSOR2"]


def test_pagination_respects_max_records(monkeypatch):
    reader = CourtListenerReader(max_records=3)

    def fake_get(url: str, params: dict | None) -> dict | None:
        # One big page; max_records must stop iteration mid-page.
        return _page([1, 2, 3, 4, 5], None)

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [d["docket_id"] for d in reader._paginate()]
    assert got == [1, 2, 3]


def test_since_sets_filed_after_param(monkeypatch):
    """``since`` must become a MM/DD/YYYY ``filed_after`` search param."""
    from datetime import date

    reader = CourtListenerReader(since=date(2024, 7, 1))
    captured: dict[str, object] = {}

    def fake_get(url: str, params: dict | None) -> dict | None:
        if params is not None:
            captured.update(params)
        return _page([1], None)

    monkeypatch.setattr(reader, "_get", fake_get)
    list(reader._paginate())
    assert captured["filed_after"] == "07/01/2024"
    assert captured["type"] == "r"
    assert captured["nature_of_suit"] == "899"

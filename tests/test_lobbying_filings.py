"""Hermetic tests for the Senate LDA lobbying-filings ingest (no network).

Covers the pieces with real logic: the raw-filing → published-schema mapping
(``_shape``, including the nested activity/government-entity projections) and the
reader's DRF ``next``-following pagination + ``max_records`` bound.
"""

from __future__ import annotations

import json

from spicy_regs.sources.lobbying_filings import LobbyingFilingsReader
from spicy_regs.transforms.build_lobbying_filings import COLUMNS, _shape

_RAW_FILING = {
    "filing_uuid": "7866327b-c892-4430-b9f0-1f0f679c58c6",
    "filing_type": "Q1",
    "filing_year": 2024,
    "filing_period": "first_quarter",
    "dt_posted": "2024-01-02T10:13:41-05:00",
    "income": "30000.00",
    "expenses": None,
    "filing_document_url": "https://lda.senate.gov/filings/public/filing/7866327b/print/",
    "registrant": {"id": 35707, "name": "SMITH GARSON"},
    "client": {"id": 58116, "client_id": 58116, "name": "E-COM 9-1-1 DISPATCH CENTER"},
    "lobbying_activities": [
        {
            "general_issue_code": "TEC",
            "general_issue_code_display": "Telecommunications",
            "description": "Emergency dispatch technology funding.",
            "government_entities": [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"}],
        },
        {
            "general_issue_code": "BUD",
            "general_issue_code_display": "Budget/Appropriations",
            "description": "Appropriations.",
            # Same chamber lobbied again — should dedup to one entity.
            "government_entities": [
                {"id": 2, "name": "HOUSE OF REPRESENTATIVES"},
                {"id": 1, "name": "SENATE"},
            ],
        },
    ],
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_FILING)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_FILING)
    assert row["filing_uuid"] == "7866327b-c892-4430-b9f0-1f0f679c58c6"
    assert row["filing_type"] == "Q1"
    # Integer scalars stringify (schema is all-VARCHAR).
    assert row["filing_year"] == "2024"
    assert row["registrant_id"] == "35707"
    assert row["client_id"] == "58116"
    assert row["registrant_name"] == "SMITH GARSON"
    assert row["client_name"] == "E-COM 9-1-1 DISPATCH CENTER"
    assert row["income"] == "30000.00"
    assert row["expenses"] is None
    assert row["url"] == "https://lda.senate.gov/filings/public/filing/7866327b/print/"
    # Activities project to issue codes + descriptions.
    acts = json.loads(row["lobbying_activities_json"])
    assert [a["general_issue_code"] for a in acts] == ["TEC", "BUD"]
    assert acts[0]["general_issue_code_display"] == "Telecommunications"
    # Government entities flatten + dedup across activities.
    ents = json.loads(row["government_entities_json"])
    assert {e["name"] for e in ents} == {"HOUSE OF REPRESENTATIVES", "SENATE"}
    assert len(ents) == 2


def test_shape_handles_missing_nested():
    row = _shape({"filing_uuid": "x"})
    assert row["registrant_name"] is None
    assert row["client_id"] is None
    assert row["lobbying_activities_json"] == "[]"
    assert row["government_entities_json"] == "[]"


def _page(uuids: list[str], next_url: str | None) -> dict:
    return {
        "count": 99,
        "next": next_url,
        "previous": None,
        "results": [{"filing_uuid": u} for u in uuids],
    }


def test_pagination_follows_next(monkeypatch):
    """The reader must follow the DRF ``next`` URL until it is null."""
    reader = LobbyingFilingsReader()
    pages = {
        None: _page(["a", "b"], "PAGE2"),  # first request (params, url ignored by stub)
        "PAGE2": _page(["c", "d"], None),
    }
    calls: list[str | None] = []

    def fake_get(url: str, params: dict | None) -> dict | None:
        # First call carries params (url is the base); later calls pass the next url.
        key = None if params is not None else url
        calls.append(key)
        return pages[key]

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [f["filing_uuid"] for f in reader._paginate()]
    assert got == ["a", "b", "c", "d"]
    assert calls == [None, "PAGE2"]


def test_pagination_respects_max_records(monkeypatch):
    reader = LobbyingFilingsReader(max_records=3)

    def fake_get(url: str, params: dict | None) -> dict | None:
        # One big page; max_records must stop iteration mid-page.
        return _page(["a", "b", "c", "d", "e"], None)

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [f["filing_uuid"] for f in reader._paginate()]
    assert got == ["a", "b", "c"]

"""Hermetic tests for the OpenFEC committees ingest (no network).

Covers the pieces with real logic: the raw-committee → published-schema mapping
(``_shape`` / ``_json_array``), the API-key resolution fallback chain, and the
page/per_page pagination with the ``pagination.pages`` walk and short-page stop.
"""

from __future__ import annotations

import json

from spicy_regs.sources.fec_committees import (
    API_KEY_ENV_VARS,
    FecCommitteesReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_fec_committees import COLUMNS, _shape

_RAW_COMMITTEE = {
    "committee_id": "C00684373",
    "name": "EXAMPLE FOR PRESIDENT",
    "committee_type": "P",
    "committee_type_full": "Presidential",
    "designation": "P",
    "designation_full": "Principal campaign committee",
    "party": "DEM",
    "party_full": "DEMOCRATIC PARTY",
    "state": "MA",
    "treasurer_name": "DOE, JANE",
    "organization_type": None,
    "organization_type_full": None,
    "filing_frequency": "A",
    "first_file_date": "2018-08-03",
    "last_file_date": "2021-02-01",
    "cycles": [2018, 2020, 2022],
    "candidate_ids": ["P00008052"],
    # Fields present in the payload but intentionally not published:
    "affiliated_committee_name": "NONE",
    "designated_agent_name": "DOE, JANE",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_COMMITTEE)
    # Every published column present, and nothing extra (16-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 16


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_COMMITTEE)
    assert row["committee_id"] == "C00684373"
    assert row["name"] == "EXAMPLE FOR PRESIDENT"
    assert row["committee_type"] == "P"
    assert row["committee_type_full"] == "Presidential"
    assert row["designation_full"] == "Principal campaign committee"
    assert row["party_full"] == "DEMOCRATIC PARTY"
    assert row["state"] == "MA"
    assert row["treasurer_name"] == "DOE, JANE"
    assert row["first_file_date"] == "2018-08-03"
    assert row["last_file_date"] == "2021-02-01"
    # Array fields are serialized to JSON strings.
    assert json.loads(row["cycles_json"]) == [2018, 2020, 2022]
    assert json.loads(row["candidate_ids_json"]) == ["P00008052"]


def test_shape_handles_missing_fields_and_null_arrays():
    row = _shape({"committee_id": "C99999999"})
    assert row["committee_id"] == "C99999999"
    # Missing scalars degrade to null, not KeyError.
    assert row["name"] is None
    assert row["organization_type_full"] is None
    assert row["party_full"] is None
    # Missing / null array fields serialize to an empty JSON array, not null.
    assert row["cycles_json"] == "[]"
    assert row["candidate_ids_json"] == "[]"


def test_shape_omits_unpublished_organization_type_code():
    # organization_type (the bare code) is not in the pinned schema; only
    # organization_type_full is published.
    assert "organization_type" not in _shape(_RAW_COMMITTEE)


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("FEC_API_KEY", "fec-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_reader_yields_nothing_without_key(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = FecCommitteesReader()
    # No key configured: a keyless run is a no-op, not a crash.
    assert list(reader.iter_records()) == []


# -- pagination --------------------------------------------------------------


def _committee(cid: str) -> dict:
    return {"committee_id": cid}


def test_paginate_walks_pages_until_reported_last_page(monkeypatch):
    """Full pages advance; the envelope's ``pagination.pages`` bounds the walk."""
    reader = FecCommitteesReader(api_key="test", per_page=2)
    pages = {
        1: {
            "results": [_committee("C1"), _committee("C2")],
            "pagination": {"page": 1, "pages": 2},
        },
        2: {
            "results": [_committee("C3"), _committee("C4")],
            "pagination": {"page": 2, "pages": 2},
        },
        # Page 3 exists in the stub but must never be requested.
        3: {"results": [_committee("C5")], "pagination": {"page": 3, "pages": 2}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page, {"results": []}))
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1", "C2", "C3", "C4"]


def test_paginate_stops_on_short_page(monkeypatch):
    """A page shorter than per_page ends pagination even if pages says more."""
    reader = FecCommitteesReader(api_key="test", per_page=3)
    pages = {
        1: {
            "results": [_committee("C1"), _committee("C2")],  # short -> stop
            "pagination": {"page": 1, "pages": 9},
        },
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page, {"results": []}))
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1", "C2"]


def test_paginate_respects_max_pages_cap(monkeypatch):
    """``max_pages`` caps the walk regardless of the reported page count."""
    reader = FecCommitteesReader(api_key="test", per_page=1, max_pages=1)
    pages = {
        1: {"results": [_committee("C1")], "pagination": {"page": 1, "pages": 100}},
        2: {"results": [_committee("C2")], "pagination": {"page": 2, "pages": 100}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page, {"results": []}))
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1"]

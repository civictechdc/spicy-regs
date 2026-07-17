"""Hermetic tests for the Congress.gov bill ingest (no network).

Covers the pieces with real logic: the raw-bill → published-schema mapping
(``_shape`` / ``_bill_id``), the API-key resolution fallback chain, and the
offset/limit pagination with early stop at the ``since`` watermark.
"""

from __future__ import annotations

from datetime import date

from spicy_regs.sources.congress_bills import (
    API_KEY_ENV_VARS,
    CongressBillsReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_congress_bills import COLUMNS, _bill_id, _shape

_RAW_BILL = {
    "congress": 118,
    "type": "HR",
    "number": 1234,
    "title": "A Bill To Do A Thing",
    "originChamber": "House",
    "latestAction": {"actionDate": "2024-03-01", "text": "Referred to committee."},
    "updateDate": "2024-03-05",
    "url": "https://api.congress.gov/v3/bill/118/hr/1234?format=json",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_BILL)
    # Every published column present, and nothing extra (10-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 10


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_BILL)
    # bill_id is built from congress + lowercased type + number.
    assert row["bill_id"] == "118-hr-1234"
    assert row["congress"] == "118"
    assert row["bill_type"] == "hr"  # lowercased
    assert row["bill_number"] == "1234"  # int stringified
    assert row["title"] == "A Bill To Do A Thing"
    assert row["origin_chamber"] == "House"
    # Nested latestAction is flattened.
    assert row["latest_action_date"] == "2024-03-01"
    assert row["latest_action_text"] == "Referred to committee."
    assert row["update_date"] == "2024-03-05"


def test_shape_handles_missing_nested_and_scalars():
    row = _shape({"congress": 117, "type": "S", "number": 5})
    assert row["bill_id"] == "117-s-5"
    # Missing nested objects degrade to null, not KeyError.
    assert row["latest_action_date"] is None
    assert row["latest_action_text"] is None
    assert row["origin_chamber"] is None


def test_bill_id_requires_all_parts():
    assert _bill_id({"congress": 118, "type": "hr", "number": 1}) == "118-hr-1"
    # Any missing component yields None (row is dropped downstream).
    assert _bill_id({"type": "hr", "number": 1}) is None
    assert _bill_id({"congress": 118, "number": 1}) is None
    assert _bill_id({"congress": 118, "type": "hr"}) is None


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("CONGRESS_GOV_API_KEY", "congress-key")
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
    reader = CongressBillsReader()
    # No key configured: a keyless run is a no-op, not a crash.
    assert list(reader.iter_records()) == []


# -- pagination --------------------------------------------------------------


def _bill(n: int, day: str) -> dict:
    return {"congress": 118, "type": "hr", "number": n, "updateDate": day}


def test_paginate_walks_offsets_until_short_page(monkeypatch):
    """Full pages advance the offset; a short page ends pagination."""
    reader = CongressBillsReader(api_key="test", per_page=2)
    pages = {
        0: {"bills": [_bill(1, "2024-03-05"), _bill(2, "2024-03-04")]},
        2: {"bills": [_bill(3, "2024-03-03")]},  # short page -> stop
    }
    monkeypatch.setattr(reader, "_get_page", lambda offset: pages.get(offset, {"bills": []}))
    got = [b["number"] for b in reader._paginate()]
    assert got == [1, 2, 3]


def test_paginate_stops_at_since_watermark(monkeypatch):
    """Bills come newest-updated first; crossing ``since`` stops the walk."""
    reader = CongressBillsReader(api_key="test", per_page=3, since=date(2024, 3, 4))
    pages = {
        0: {
            "bills": [
                _bill(1, "2024-03-06"),  # newer -> kept
                _bill(2, "2024-03-04"),  # == watermark -> kept
                _bill(3, "2024-03-01"),  # older -> stop here
            ]
        },
    }
    monkeypatch.setattr(reader, "_get_page", lambda offset: pages.get(offset, {"bills": []}))
    got = [b["number"] for b in reader._paginate()]
    assert got == [1, 2]

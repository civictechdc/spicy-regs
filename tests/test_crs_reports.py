"""Hermetic tests for the Congress.gov CRS report ingest (no network).

Covers the pieces with real logic: the raw-report → published-schema mapping
(``_shape``), the API-key resolution fallback chain, and the offset/limit
pagination with early stop at the ``since`` watermark.
"""

from __future__ import annotations

from datetime import date

from spicy_regs.sources.crs_reports import (
    API_KEY_ENV_VARS,
    CrsReportsReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_crs_reports import COLUMNS, _shape

_RAW_REPORT = {
    "contentType": "Reports",
    "id": "R48641",
    "publishDate": "2026-07-16T04:00:00Z",
    "status": "Active",
    "title": "Proposals to Limit Member of Congress Financial Activities",
    "updateDate": "2026-07-17T22:38:55Z",
    "url": "https://api.congress.gov/v3/crsreport/R48641",
    "version": 15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_REPORT)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_REPORT)
    assert row["report_id"] == "R48641"
    assert row["title"] == "Proposals to Limit Member of Congress Financial Activities"
    assert row["report_type"] == "Reports"
    assert row["status"] == "Active"
    assert row["published_date"] == "2026-07-16T04:00:00Z"
    assert row["update_date"] == "2026-07-17T22:38:55Z"
    # version int is stringified.
    assert row["version"] == "15"
    assert row["url"] == "https://api.congress.gov/v3/crsreport/R48641"


def test_shape_handles_missing_scalars():
    row = _shape({"id": "IN12713"})
    assert row["report_id"] == "IN12713"
    assert row["title"] is None
    assert row["report_type"] is None
    assert row["version"] is None


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
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_reader_yields_nothing_without_key(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = CrsReportsReader()
    # No key configured: a keyless run is a no-op, not a crash.
    assert list(reader.iter_records()) == []


# -- pagination --------------------------------------------------------------


def _report(rid: str, day: str) -> dict:
    return {"id": rid, "updateDate": day}


def test_paginate_walks_offsets_until_short_page(monkeypatch):
    """Full pages advance the offset; a short page ends pagination."""
    reader = CrsReportsReader(api_key="test", per_page=2)
    pages = {
        0: {"CRSReports": [_report("R1", "2026-07-05"), _report("R2", "2026-07-04")]},
        2: {"CRSReports": [_report("R3", "2026-07-03")]},  # short page -> stop
    }
    monkeypatch.setattr(reader, "_get_page", lambda offset: pages.get(offset, {"CRSReports": []}))
    got = [r["id"] for r in reader._paginate()]
    assert got == ["R1", "R2", "R3"]


def test_paginate_stops_at_since_watermark(monkeypatch):
    """Reports come newest-updated first; crossing ``since`` stops the walk."""
    reader = CrsReportsReader(api_key="test", per_page=3, since=date(2026, 7, 4))
    pages = {
        0: {
            "CRSReports": [
                _report("R1", "2026-07-06"),  # newer -> kept
                _report("R2", "2026-07-04"),  # == watermark -> kept
                _report("R3", "2026-07-01"),  # older -> stop here
            ]
        },
    }
    monkeypatch.setattr(reader, "_get_page", lambda offset: pages.get(offset, {"CRSReports": []}))
    got = [r["id"] for r in reader._paginate()]
    assert got == ["R1", "R2"]

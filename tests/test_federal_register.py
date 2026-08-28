"""Hermetic tests for the Federal Register ingest (no network).

Covers the two pieces with real logic: the raw-doc → published-schema mapping
(``_shape``) and the date-window subdivision the reader uses to work around the
API's per-query truncation.
"""

from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from spicy_regs.sources import federal_register as federal_register_source
from spicy_regs.sources.federal_register import FederalRegisterReader
from spicy_regs.transforms.build_federal_register import COLUMNS, _shape

_RAW_DOC = {
    "document_number": "2024-00001",
    "title": "A Proposed Rule",
    "abstract": "Does a thing.",
    "type": "Proposed Rule",
    "publication_date": "2024-03-01",
    "effective_on": None,
    "docket_ids": ["EPA-HQ-OAR-2024-0001", "FRL-1234-01-OAR"],
    "regulation_id_numbers": ["2060-AV12"],
    "cfr_references": [{"title": 40, "part": 60, "chapter": None, "citation_url": None}],
    "agencies": [
        {
            "raw_name": "ENVIRONMENTAL PROTECTION AGENCY",
            "name": "Environmental Protection Agency",
            "slug": "environmental-protection-agency",
        },
        {"raw_name": "No slug agency", "name": "No slug agency"},
    ],
    "volume": 89,
    "start_page": 1234,
    "end_page": 1240,
    "executive_order_number": None,
    "html_url": "https://www.federalregister.gov/d/2024-00001",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_DOC)
    # Every published column present, and nothing extra.
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_DOC)
    assert row["document_number"] == "2024-00001"
    assert row["document_type"] == "Proposed Rule"  # API `type` -> document_type
    # Array fields serialize to JSON strings.
    assert json.loads(row["docket_ids_json"]) == ["EPA-HQ-OAR-2024-0001", "FRL-1234-01-OAR"]
    assert json.loads(row["regulation_id_numbers_json"]) == ["2060-AV12"]
    assert json.loads(row["cfr_references_json"])[0]["part"] == 60
    # agency_slugs is a comma-joined string of slugs, skipping agencies with none.
    assert row["agency_slugs"] == "environmental-protection-agency"
    # Integer scalars stringify (schema is all-VARCHAR).
    assert row["volume"] == "89"
    assert row["start_page"] == "1234"
    # Null passthrough, and modify_date is unknown from the REST API.
    assert row["effective_on"] is None
    assert row["executive_order_number"] is None
    assert row["modify_date"] is None


def test_shape_handles_missing_arrays():
    row = _shape({"document_number": "x"})
    assert row["docket_ids_json"] == "[]"
    assert row["regulation_id_numbers_json"] == "[]"
    assert row["cfr_references_json"] == "[]"
    assert row["agencies_json"] == "[]"
    assert row["agency_slugs"] is None


def _doc(n: int, day: str) -> dict:
    return {"document_number": f"D{n}", "publication_date": day}


def test_window_subdivides_on_truncation(monkeypatch):
    """A truncated window must split and recurse so no document is dropped.

    We simulate an API that returns only 1 of 2 documents for any multi-day
    window, but the full set for a single day. The reader should recover all
    documents by subdividing down to single days.
    """
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 2))

    def fake_page_window(gte: date, lte: date):
        if gte == lte:
            # Single day: return that day's full (small) result set.
            return [_doc(gte.day, gte.isoformat())], 1
        # Multi-day window: report 2 total but only hand back 1 (truncated).
        return [_doc(gte.day, gte.isoformat())], 2

    monkeypatch.setattr(reader, "_page_window", fake_page_window)
    # _fetch_window doesn't need the httpx client once _page_window is stubbed.
    got = [d["document_number"] for d in reader._fetch_window(date(2024, 1, 1), date(2024, 1, 2))]
    assert sorted(got) == ["D1", "D2"]


def test_single_day_truncation_aborts_instead_of_dropping_documents(monkeypatch):
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 1))
    monkeypatch.setattr(
        reader,
        "_page_window",
        lambda gte, lte: ([_doc(1, gte.isoformat())], 2),
    )

    with pytest.raises(RuntimeError, match="truncated"):
        list(reader._fetch_window(date(2024, 1, 1), date(2024, 1, 1)))


def test_reported_result_cap_subdivides_even_when_visible_rows_are_complete(monkeypatch):
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 2))
    monkeypatch.setattr(federal_register_source, "RESULT_CAP", 2)

    def fake_page_window(gte: date, lte: date):
        if gte == lte:
            return [_doc(gte.day, gte.isoformat())], 1
        return [
            _doc(1, "2024-01-01"),
            _doc(2, "2024-01-02"),
        ], 2

    monkeypatch.setattr(reader, "_page_window", fake_page_window)

    got = [d["document_number"] for d in reader._fetch_window(date(2024, 1, 1), date(2024, 1, 2))]
    assert got == ["D1", "D2"]


def test_capped_single_day_refuses_ambiguous_source_state(monkeypatch):
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 1))
    monkeypatch.setattr(federal_register_source, "RESULT_CAP", 2)
    monkeypatch.setattr(
        reader,
        "_page_window",
        lambda gte, lte: ([_doc(1, gte.isoformat()), _doc(2, gte.isoformat())], 2),
    )

    with pytest.raises(RuntimeError, match="truncated.*2/2"):
        list(reader._fetch_window(date(2024, 1, 1), date(2024, 1, 1)))


def test_archive_fetch_uses_bounded_top_level_windows(monkeypatch):
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 12, 31))
    windows: list[tuple[date, date]] = []

    def fake_fetch_window(gte: date, lte: date):
        windows.append((gte, lte))
        return iter(())

    monkeypatch.setattr(reader, "_fetch_window", fake_fetch_window)
    assert list(reader.iter_records()) == []
    assert len(windows) > 1
    assert all((lte - gte).days < federal_register_source.MAX_WINDOW_DAYS for gte, lte in windows)


def test_request_exhaustion_aborts_instead_of_returning_partial_data(monkeypatch):
    reader = FederalRegisterReader()

    class FailingClient:
        def get(self, url, params=None):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr(reader, "_client", FailingClient())
    monkeypatch.setattr(federal_register_source, "_MAX_RETRIES", 1)

    with pytest.raises(RuntimeError, match="request failed after 1 attempts"):
        reader._get("https://example.test/documents.json", None)

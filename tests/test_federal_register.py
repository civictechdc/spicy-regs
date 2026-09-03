"""Hermetic tests for the Federal Register ingest (no network).

Covers the two pieces with real logic: the raw-doc → published-schema mapping
(``_shape``) and the date-window subdivision the reader uses to work around the
API's per-query truncation.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

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


# The two signals that make a window's contents unknowable, and so must force a
# split: fewer rows than the reported ``count``, or a ``count`` sitting on the
# API's cap (which clamps at 10,000 and hides the true total). ``result_cap``
# is the value RESULT_CAP is set to for the case, so the cap case can be
# written with 2 rows instead of 10,000.
_AMBIGUOUS_WINDOWS = [
    pytest.param(1, 2, federal_register_source.RESULT_CAP, id="rows-short-of-reported-count"),
    pytest.param(2, 2, 2, id="reported-count-at-api-cap"),
]


@pytest.mark.parametrize(("rows", "count", "result_cap"), _AMBIGUOUS_WINDOWS)
def test_ambiguous_window_subdivides_so_no_document_is_dropped(monkeypatch, rows, count, result_cap):
    """An ambiguous multi-day window must split and recurse until days answer completely.

    The fake API is ambiguous for any multi-day window but returns each single
    day's full (small) result set, so a correct reader recovers both documents.
    """
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 2))
    monkeypatch.setattr(federal_register_source, "RESULT_CAP", result_cap)

    def fake_page_window(gte: date, lte: date):
        if gte == lte:
            return [_doc(gte.day, gte.isoformat())], 1
        # Rows from an ambiguous window must never reach the caller, so they are
        # numbered apart: publishing them unsplit fails the assertion below.
        return [_doc(900 + n, gte.isoformat()) for n in range(1, rows + 1)], count

    monkeypatch.setattr(reader, "_page_window", fake_page_window)
    # _fetch_window doesn't need the httpx client once _page_window is stubbed.
    got = [d["document_number"] for d in reader._fetch_window(date(2024, 1, 1), date(2024, 1, 2))]
    assert sorted(got) == ["D1", "D2"]


@pytest.mark.parametrize(("rows", "count", "result_cap"), _AMBIGUOUS_WINDOWS)
def test_ambiguous_single_day_refuses_instead_of_publishing_partial_data(monkeypatch, rows, count, result_cap):
    """A single day cannot be subdivided further, so ambiguity there must abort the run.

    This replaced a tolerant path that logged the gap and published the partial
    page; without the refusal an API limit becomes silent corpus loss.
    """
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 1, 1))
    monkeypatch.setattr(federal_register_source, "RESULT_CAP", result_cap)
    monkeypatch.setattr(
        reader,
        "_page_window",
        lambda gte, lte: ([_doc(n, gte.isoformat()) for n in range(1, rows + 1)], count),
    )

    with pytest.raises(RuntimeError, match=f"truncated.*{rows}/{count}"):
        list(reader._fetch_window(date(2024, 1, 1), date(2024, 1, 1)))


def test_archive_fetch_uses_bounded_top_level_windows(monkeypatch):
    """Top-level windows must be capped in width *and* tile [since, until] exactly.

    Width alone is not enough: a stride that skipped or repeated a day would
    still produce narrow windows, so the gap/overlap check is what catches an
    off-by-one in the walk.
    """
    reader = FederalRegisterReader(since=date(2024, 1, 1), until=date(2024, 12, 31))
    windows: list[tuple[date, date]] = []

    def fake_fetch_window(gte: date, lte: date):
        windows.append((gte, lte))
        return iter(())

    monkeypatch.setattr(reader, "_fetch_window", fake_fetch_window)
    assert list(reader.iter_records()) == []
    assert len(windows) > 1
    assert all((lte - gte).days < federal_register_source.MAX_WINDOW_DAYS for gte, lte in windows)
    assert windows[0][0] == reader.since
    assert windows[-1][1] == reader.until
    assert all(nxt[0] == prev[1] + timedelta(days=1) for prev, nxt in zip(windows, windows[1:]))


def test_request_exhaustion_aborts_instead_of_returning_partial_data(monkeypatch):
    reader = FederalRegisterReader()

    class FailingClient:
        def get(self, url, params=None):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr(reader, "_client", FailingClient())
    monkeypatch.setattr(federal_register_source, "_MAX_RETRIES", 1)

    with pytest.raises(RuntimeError, match="request failed after 1 attempts"):
        reader._get("https://example.test/documents.json", None)

"""Hermetic tests for the USASpending.gov recipient ingest (no network).

Covers the pieces with real logic: the raw-recipient → published-schema mapping
(``_shape``), and the page/limit pagination with early stop on ``hasNext=false``,
a short page, and the ``max_pages`` bound.
"""

from __future__ import annotations

from spicy_regs.sources.usaspending import (
    DEFAULT_MAX_PAGES,
    PER_PAGE,
    UsaSpendingRecipientsReader,
)
from spicy_regs.transforms.build_usaspending_recipients import COLUMNS, _shape

_RAW_RECIPIENT = {
    "id": "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P",
    "duns": "834951691",
    "uei": "ZFN2JJXBLZT3",
    "name": "LOCKHEED MARTIN CORP",
    "recipient_level": "P",
    "amount": 63465270734.15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_RECIPIENT)
    # Every published column present, and nothing extra (6-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 6


def test_shape_maps_and_stringifies_fields():
    row = _shape(_RAW_RECIPIENT)
    assert row["recipient_id"] == "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P"
    assert row["uei"] == "ZFN2JJXBLZT3"
    assert row["duns"] == "834951691"
    assert row["name"] == "LOCKHEED MARTIN CORP"
    assert row["recipient_level"] == "P"
    # Numeric amount is coerced to a string, not left as a float.
    assert row["total_award_amount"] == "63465270734.15"


def test_shape_handles_missing_fields():
    row = _shape({"id": "x-R"})
    assert row["recipient_id"] == "x-R"
    # Missing scalars degrade to None, not KeyError.
    assert row["uei"] is None
    assert row["duns"] is None
    assert row["name"] is None
    assert row["recipient_level"] is None
    # Missing amount stays None (not the string "None").
    assert row["total_award_amount"] is None


# -- construction ------------------------------------------------------------


def test_reader_clamps_per_page_and_max_pages():
    # per_page is capped at the API max; max_pages defaults to the top-N bound.
    reader = UsaSpendingRecipientsReader(per_page=9999)
    assert reader.per_page == PER_PAGE
    assert reader.max_pages == DEFAULT_MAX_PAGES


# -- pagination --------------------------------------------------------------


def _recipient(n: int) -> dict:
    return {"id": f"r{n}-R", "uei": f"U{n}", "name": f"ORG {n}", "recipient_level": "R", "amount": n}


def test_paginate_follows_has_next_then_stops(monkeypatch):
    """Full pages with hasNext advance; hasNext=false ends pagination."""
    reader = UsaSpendingRecipientsReader(per_page=2, max_pages=10)
    pages = {
        1: {"results": [_recipient(1), _recipient(2)], "page_metadata": {"hasNext": True}},
        2: {"results": [_recipient(3), _recipient(4)], "page_metadata": {"hasNext": False}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page))
    got = [r["id"] for r in reader._paginate()]
    assert got == ["r1-R", "r2-R", "r3-R", "r4-R"]


def test_paginate_stops_on_short_page(monkeypatch):
    """A page shorter than per_page ends the walk even if hasNext lies True."""
    reader = UsaSpendingRecipientsReader(per_page=2, max_pages=10)
    pages = {
        1: {"results": [_recipient(1), _recipient(2)], "page_metadata": {"hasNext": True}},
        2: {"results": [_recipient(3)], "page_metadata": {"hasNext": True}},  # short -> stop
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page))
    got = [r["id"] for r in reader._paginate()]
    assert got == ["r1-R", "r2-R", "r3-R"]


def test_paginate_respects_max_pages_bound(monkeypatch):
    """The top-N bound stops the walk even when the API keeps offering more."""
    reader = UsaSpendingRecipientsReader(per_page=2, max_pages=1)
    pages = {
        1: {"results": [_recipient(1), _recipient(2)], "page_metadata": {"hasNext": True}},
        2: {"results": [_recipient(3), _recipient(4)], "page_metadata": {"hasNext": True}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page))
    got = [r["id"] for r in reader._paginate()]
    assert got == ["r1-R", "r2-R"]


def test_paginate_stops_on_none_payload(monkeypatch):
    """A failed page (None after retries) ends pagination without crashing."""
    reader = UsaSpendingRecipientsReader(per_page=2, max_pages=10)
    monkeypatch.setattr(reader, "_get_page", lambda page: None)
    assert list(reader._paginate()) == []

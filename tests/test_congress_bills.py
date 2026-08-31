"""Hermetic tests for the Congress.gov bill ingest (no network).

Covers the pieces with real logic: the raw-bill → published-schema mapping
(``_shape`` / ``_bill_id``), the API-key resolution fallback chain, the
offset/limit pagination, and the request params that bound the fetch window.

The params tests are regression cover for the 510-day freeze: ``sort`` has to
reach the API as ``updateDate+desc`` (a space, which httpx form-encodes to
``+``) and the window has to be bounded server-side by ``fromDateTime``.
"""

from __future__ import annotations

from datetime import date, timedelta

import httpx
import pytest

from spicy_regs.sources.congress_bills import (
    API_KEY_ENV_VARS,
    SORT_NEWEST_FIRST,
    CongressBillsReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_congress_bills import (
    COLUMNS,
    MAX_WINDOW_DAYS,
    _bill_id,
    _bounded_until,
    _shape,
)

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


def test_paginate_does_not_stop_on_an_out_of_order_page(monkeypatch):
    """Row order must not truncate the walk — the server bounds the window.

    This is the 510-day freeze in miniature: with ``sort`` silently ignored the
    rows arrive shuffled, and the old client-side watermark stop quit after the
    first row. Every bill on the page has to survive.
    """
    reader = CongressBillsReader(api_key="test", per_page=3, since=date(2025, 4, 4))
    pages = {
        0: {
            "bills": [
                _bill(1, "2025-04-07"),
                _bill(2, "2025-01-02"),  # out of order — must NOT end the walk
                _bill(3, "2026-03-24"),
            ]
        },
        3: {"bills": [_bill(4, "2024-02-07")]},  # short page -> stop
    }
    monkeypatch.setattr(reader, "_get_page", lambda offset: pages.get(offset, {"bills": []}))
    got = [b["number"] for b in reader._paginate()]
    assert got == [1, 2, 3, 4]


# -- request params ----------------------------------------------------------


def _params_for(monkeypatch, reader: CongressBillsReader) -> dict:
    """Capture the query params ``_get_page`` would send."""
    captured: dict = {}

    def fake_get(url, params):
        captured.update(params or {})
        return None

    monkeypatch.setattr(reader, "_get", fake_get)
    reader._get_page(0)
    return captured


def test_sort_param_survives_url_encoding(monkeypatch):
    """httpx must encode ``sort`` to ``updateDate+desc``, not ``updateDate%2Bdesc``.

    The API answers 200 to both but only honours the former; the ``%2B`` form
    returns rows in arbitrary order, which is what froze this table.
    """
    params = _params_for(monkeypatch, CongressBillsReader(api_key="test"))
    assert params["sort"] == SORT_NEWEST_FIRST
    url = httpx.Request("GET", "https://api.congress.gov/v3/bill", params=params).url
    assert "sort=updateDate+desc" in str(url)
    assert "%2B" not in str(url)


def test_since_becomes_a_server_side_from_datetime(monkeypatch):
    reader = CongressBillsReader(api_key="test", since=date(2025, 4, 4))
    assert _params_for(monkeypatch, reader)["fromDateTime"] == "2025-04-04T00:00:00Z"


def test_no_since_sends_no_window_bound(monkeypatch):
    """A full backfill must not accidentally bound itself."""
    params = _params_for(monkeypatch, CongressBillsReader(api_key="test"))
    assert "fromDateTime" not in params


def test_since_and_until_become_server_side_bounds(monkeypatch):
    reader = CongressBillsReader(api_key="test", since=date(2025, 4, 4), until=date(2025, 7, 3))
    params = _params_for(monkeypatch, reader)
    assert params["fromDateTime"] == "2025-04-04T00:00:00Z"
    assert params["toDateTime"] == "2025-07-03T00:00:00Z"


def test_no_until_sends_no_upper_bound(monkeypatch):
    params = _params_for(monkeypatch, CongressBillsReader(api_key="test", since=date(2025, 4, 4)))
    assert "toDateTime" not in params


# -- catch-up windowing ------------------------------------------------------


def test_window_is_capped_so_a_deep_backfill_converges():
    """An unbounded catch-up publishes nothing and retries forever; cap it.

    The 510-day freeze needed 238k bills in one walk. That run hit the job
    timeout at 190k and persisted nothing. Capping the window makes each run
    publish and advance the watermark.
    """
    since = date(2025, 4, 4)
    until = _bounded_until(since, None, today=date(2026, 8, 30))
    assert until == since + timedelta(days=MAX_WINDOW_DAYS)


def test_explicit_until_is_still_capped():
    since = date(2025, 4, 4)
    # An operator asking for the whole 510-day span still gets one window.
    assert _bounded_until(since, date(2026, 8, 30)) == since + timedelta(days=MAX_WINDOW_DAYS)


def test_short_explicit_window_is_left_alone():
    since, until = date(2026, 8, 1), date(2026, 8, 10)
    assert _bounded_until(since, until) == until


def test_caught_up_run_stops_at_today():
    """Steady state: the cap must not push the window past now."""
    since, today = date(2026, 8, 27), date(2026, 8, 30)
    assert _bounded_until(since, None, today=today) == today


def test_backwards_window_is_rejected():
    with pytest.raises(ValueError, match="precedes"):
        _bounded_until(date(2026, 8, 10), date(2026, 8, 1))


def test_full_backfill_has_no_upper_bound():
    """No prior table means no watermark to window from."""
    assert _bounded_until(None, None) is None

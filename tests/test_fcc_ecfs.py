"""Hermetic tests for the FCC ECFS ingest (no network).

Covers the pieces with real logic: the raw-payload → published-schema mappings
(``_shape_proceeding`` / ``_shape_filing``), the API-key resolution fallback
chain, the date-window subdivision around the API's 10K result ceiling, and
the two-ended fetch that recovers an overloaded single day.
"""

from __future__ import annotations

import json
from datetime import date

from spicy_regs.sources.fcc_ecfs import (
    API_KEY_ENV_VARS,
    MAX_RESULT_WINDOW,
    FccEcfsFilingsReader,
    FccEcfsProceedingsReader,
    _range_param,
    _resolve_api_key,
)
from spicy_regs.transforms.build_fcc_ecfs import (
    FILING_COLUMNS,
    PROCEEDING_COLUMNS,
    _shape_filing,
    _shape_proceeding,
)

_RAW_PROCEEDING = {
    "name": "17-108",
    "id_proceeding": 301759,
    "description": "Restoring Internet Freedom",
    "description_display": "Restoring Internet Freedom (display)",
    "bureau": {"code": "WC", "name": "Wireline Competition Bureau", "edocs_bureau_code": "WCB"},
    "flag_rulemaking_or_docket": "D",
    "filingStatus": "OPENALL",
    "date_proceeding_created": "2017-04-26T14:49:35.900Z",
    "date_closed": "2099-12-31T23:59:59.999Z",
    "comment_start_date": None,
    "comment_end_date": None,
    "comment_reply_start_date": None,
    "comment_reply_end_date": None,
    "filed_by": "Some.User",
    # Present in the payload but intentionally not published:
    "applicant_name": "",
    "flag_internet_file": "Y",
}

_RAW_FILING = {
    "id_submission": "26109947027",
    "proceedings": [
        {"name": "17-108", "bureau_code": "WC", "bureau_name": "Wireline Competition Bureau"},
        {"name": "23-320"},
    ],
    "submissiontype": {"description": "COMMENT", "short": "COMMENT", "id": 7, "type": "CO"},
    "express_comment": 1,
    "date_received": "2026-06-29T12:00:00.000Z",
    "date_submission": "2026-06-28T09:23:59.264Z",
    "date_disseminated": "2026-06-29T15:00:32.590Z",
    "filingstatus": {"description": "DISSEMINATED", "id": 30},
    "viewingstatus": {"description": "Unrestricted", "id": 10},
    "exparte_or_late_filed": "N",
    "filers": [{"name": "King"}],
    "authors": [],
    "lawfirms": [{"name": "Firm LLP"}],
    "bureaus": [],
    "text_data": "I support this rule.",
    "total_page_count": 3,
    "documents": [{"filename": "comment.pdf", "src": "https://www.fcc.gov/ecfs/document/1/x.pdf"}],
    # Present in the payload but intentionally not published:
    "_index": "filings.2026.6",
    "created": True,
}


# -- shaping: proceedings ------------------------------------------------------


def test_shape_proceeding_produces_exact_schema():
    row = _shape_proceeding(_RAW_PROCEEDING)
    assert set(row) == set(PROCEEDING_COLUMNS)
    assert len(PROCEEDING_COLUMNS) == 14


def test_shape_proceeding_maps_fields():
    row = _shape_proceeding(_RAW_PROCEEDING)
    assert row["name"] == "17-108"
    # Integer id stringifies (schema is all-VARCHAR).
    assert row["id_proceeding"] == "301759"
    # description_display wins over description.
    assert row["description"] == "Restoring Internet Freedom (display)"
    # Nested bureau object flattens.
    assert row["bureau_code"] == "WC"
    assert row["bureau_name"] == "Wireline Competition Bureau"
    assert row["rulemaking_or_docket"] == "D"
    assert row["filing_status"] == "OPENALL"
    assert row["date_created"] == "2017-04-26T14:49:35.900Z"
    assert row["filed_by"] == "Some.User"


def test_shape_proceeding_accepts_flat_bureau_fields():
    # Filings embed proceedings with flat bureau_code/bureau_name instead of a
    # nested bureau object; the shaper accepts both.
    row = _shape_proceeding({"name": "23-320", "bureau_code": "WC", "bureau_name": "Wireline"})
    assert row["bureau_code"] == "WC"
    assert row["bureau_name"] == "Wireline"


def test_shape_proceeding_handles_missing_fields():
    row = _shape_proceeding({"name": "96-45"})
    assert row["name"] == "96-45"
    assert row["id_proceeding"] is None
    assert row["bureau_code"] is None
    assert row["date_closed"] is None


# -- shaping: filings ----------------------------------------------------------


def test_shape_filing_produces_exact_schema():
    row = _shape_filing(_RAW_FILING)
    assert set(row) == set(FILING_COLUMNS)
    assert len(FILING_COLUMNS) == 18


def test_shape_filing_maps_and_serializes_fields():
    row = _shape_filing(_RAW_FILING)
    assert row["id_submission"] == "26109947027"
    # Nested description objects flatten to their descriptions.
    assert row["submission_type"] == "COMMENT"
    assert row["filing_status"] == "DISSEMINATED"
    assert row["viewing_status"] == "Unrestricted"
    # Integer flags/counts stringify (schema is all-VARCHAR).
    assert row["express_comment"] == "1"
    assert row["total_page_count"] == "3"
    # Array fields serialize to JSON strings of names.
    assert json.loads(row["proceeding_names_json"]) == ["17-108", "23-320"]
    assert json.loads(row["filers_json"]) == ["King"]
    assert json.loads(row["authors_json"]) == []
    assert json.loads(row["lawfirms_json"]) == ["Firm LLP"]
    # Documents keep only filename + src.
    docs = json.loads(row["documents_json"])
    assert docs == [{"filename": "comment.pdf", "src": "https://www.fcc.gov/ecfs/document/1/x.pdf"}]
    assert row["text_data"] == "I support this rule."
    assert row["filing_url"] == "https://www.fcc.gov/ecfs/filing/26109947027"


def test_shape_filing_handles_missing_fields():
    row = _shape_filing({"id_submission": 123})
    assert row["id_submission"] == "123"
    assert row["submission_type"] is None
    assert row["proceeding_names_json"] == "[]"
    assert row["documents_json"] == "[]"
    assert row["filing_url"] == "https://www.fcc.gov/ecfs/filing/123"


def test_shape_filing_without_id_has_no_url():
    row = _shape_filing({})
    assert row["id_submission"] is None
    assert row["filing_url"] is None


# -- API-key resolution --------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("FCC_API_KEY", "fcc-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_and_skips_blank(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "   ")
    monkeypatch.setenv("FCC_API_KEY", "fcc-key")
    assert _resolve_api_key() == "fcc-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    assert _resolve_api_key() is None


def test_reader_without_key_yields_nothing(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    reader = FccEcfsProceedingsReader()
    assert list(reader.iter_records()) == []


# -- range-filter rendering ----------------------------------------------------


def test_range_param_renders_ecfs_bracket_syntax():
    assert _range_param(date(2024, 1, 1), date(2024, 1, 31)) == "[gte]2024-01-01[lte]2024-01-31"


# -- window subdivision & pagination --------------------------------------------


class _FakeWindowReader(FccEcfsFilingsReader):
    """Reader with ``_page_window`` stubbed by a per-day record table."""

    def __init__(self, records_by_day: dict[date, list[dict]], **kwargs):
        super().__init__(api_key="test-key", **kwargs)
        self._by_day = records_by_day
        self.window_calls: list[tuple[date, date, bool]] = []

    def _page_window(self, gte: date, lte: date, *, ascending: bool) -> tuple[list[dict], bool]:
        self.window_calls.append((gte, lte, ascending))
        records = [r for day, recs in self._by_day.items() if gte <= day <= lte for r in recs]
        if not ascending:
            records = list(reversed(records))
        if len(records) > MAX_RESULT_WINDOW:
            return records[:MAX_RESULT_WINDOW], False
        return records, True


def _recs(day: str, n: int, prefix: str) -> list[dict]:
    return [{"id_submission": f"{prefix}-{i}", "date_received": day} for i in range(n)]


def test_small_window_fetches_in_one_query():
    by_day = {date(2024, 1, 1): _recs("2024-01-01", 5, "a"), date(2024, 1, 2): _recs("2024-01-02", 5, "b")}
    reader = _FakeWindowReader(by_day, since=date(2024, 1, 1), until=date(2024, 1, 2))
    records = list(reader.iter_records())
    assert len(records) == 10
    assert reader.window_calls == [(date(2024, 1, 1), date(2024, 1, 2), True)]


def test_overloaded_window_subdivides_to_days():
    # Day 1 and day 2 together exceed the ceiling; each alone fits — the reader
    # must split rather than silently truncate.
    by_day = {
        date(2024, 1, 1): _recs("2024-01-01", 6_000, "a"),
        date(2024, 1, 2): _recs("2024-01-02", 6_000, "b"),
    }
    reader = _FakeWindowReader(by_day, since=date(2024, 1, 1), until=date(2024, 1, 2))
    records = list(reader.iter_records())
    assert len(records) == 12_000
    ids = {r["id_submission"] for r in records}
    assert len(ids) == 12_000


def test_overloaded_single_day_fetches_both_ends():
    # 15K on one day: ascending reaches the first 10K, descending the last 10K;
    # the overlap makes the union complete.
    day = date(2024, 1, 1)
    by_day = {day: _recs("2024-01-01", 15_000, "a")}
    reader = _FakeWindowReader(by_day, since=day, until=day)
    records = list(reader.iter_records())
    assert len(records) == 15_000
    assert len({r["id_submission"] for r in records}) == 15_000
    # Exactly one ascending and one descending pass over the day.
    assert (day, day, True) in reader.window_calls
    assert (day, day, False) in reader.window_calls


def test_scoped_filings_reader_walks_each_proceeding():
    by_day = {date(2024, 1, 1): _recs("2024-01-01", 3, "a")}
    reader = _FakeWindowReader(by_day, since=date(2024, 1, 1), until=date(2024, 1, 1), proceedings=("17-108", "23-320"))
    records = list(reader.iter_records())
    # One full walk per proceeding (the fake table is not proceeding-aware, so
    # each walk returns the same 3 records).
    assert len(records) == 6
    assert len(reader.window_calls) == 2

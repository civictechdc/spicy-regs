"""Hermetic tests for the per-bill subject enrichment (no network).

Covers the pieces with real logic: the BILLSTATUS XML parse, the two-carrier
selection and its coverage floor, the ``/subjects`` page walk, and the three
outcomes the transform depends on — an answer, a definitive "not held", and a
failure that must leave the bill for the next run rather than pin an empty
answer to it.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.bill_subjects import (
    API_HOURLY_BUDGET,
    CARRIER_API,
    CARRIER_BULKDATA,
    DELAY_SECONDS,
    FIRST_CONGRESS,
    BillSubjects,
    BillSubjectsFetcher,
    parse_billstatus_subjects,
    resolve_carrier,
)
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS
from spicy_regs.transforms.enrich_bill_subjects import (
    COLUMNS,
    MAX_BILLS_PER_RUN,
    _pending_bills,
    _shape,
    enrich_bill_subjects,
)

# A BILLSTATUS record in the shape GPO actually serves: <policyArea> appears
# both as a direct child of <bill> and again inside <subjects>, and a term can
# repeat across containers.
_BILLSTATUS = """<?xml version="1.0" encoding="UTF-8"?>
<billStatus>
  <bill>
    <congress>118</congress>
    <type>HR</type>
    <policyArea><name>Environmental Protection</name></policyArea>
    <subjects>
      <legislativeSubjects>
        <item><name>Air quality</name></item>
        <item><name>Congressional oversight</name></item>
        <item><name>Air quality</name></item>
        <item><name>  </name></item>
      </legislativeSubjects>
      <policyArea>
        <name>Environmental Protection</name>
        <updateDate>2024-01-02T00:00:00Z</updateDate>
      </policyArea>
    </subjects>
  </bill>
</billStatus>
"""


def _keyless(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _write_bills(path, rows):
    """Write a minimal congress_bills.parquet fixture."""
    columns = ("bill_id", "congress", "bill_type", "bill_number")
    schema = pa.schema([(c, pa.string()) for c in columns])
    pq.write_table(pa.Table.from_pylist([dict(zip(columns, r)) for r in rows], schema=schema), path)


# -- BILLSTATUS parse --------------------------------------------------------


def test_parse_billstatus_reads_policy_area_and_subjects():
    policy_area, subjects = parse_billstatus_subjects(_BILLSTATUS)
    assert policy_area == "Environmental Protection"
    # Repeated terms collapse, whitespace-only names are dropped, order is kept.
    assert subjects == ("Air quality", "Congressional oversight")


def test_parse_billstatus_survives_malformed_xml():
    # A truncated download must degrade to "no answer here", not raise.
    assert parse_billstatus_subjects("<billStatus><bill>") == (None, ())


def test_parse_billstatus_handles_a_bill_with_no_assignment():
    assert parse_billstatus_subjects("<billStatus><bill/></billStatus>") == (None, ())


# -- carrier selection -------------------------------------------------------


def test_keyless_runs_pick_the_bulkdata_carrier(monkeypatch):
    _keyless(monkeypatch)
    assert resolve_carrier() == CARRIER_BULKDATA
    assert BillSubjectsFetcher().carrier == CARRIER_BULKDATA


def test_a_key_picks_the_deeper_congress_api_carrier(monkeypatch):
    _keyless(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "a-key")
    assert resolve_carrier() == CARRIER_API
    assert BillSubjectsFetcher().carrier == CARRIER_API


def test_the_api_carrier_refuses_to_run_without_a_key(monkeypatch):
    _keyless(monkeypatch)
    with pytest.raises(ValueError, match="api.data.gov key"):
        BillSubjectsFetcher(carrier=CARRIER_API)


def test_each_carrier_declares_its_coverage_floor(monkeypatch):
    _keyless(monkeypatch)
    # GPO's bulk data starts at the 108th Congress; the API reaches back to CRS's
    # own indexing floor at the 93rd.
    assert BillSubjectsFetcher().first_congress == FIRST_CONGRESS[CARRIER_BULKDATA] == 108
    assert FIRST_CONGRESS[CARRIER_API] == 93


def test_a_run_cannot_outspend_the_documented_hourly_budget():
    """Congress.gov states 5,000 requests an hour; a capped run must fit inside it."""
    per_hour = 3600 / DELAY_SECONDS[CARRIER_API]
    assert per_hour < API_HOURLY_BUDGET
    # And the run itself has to finish inside the workflow's 30-minute timeout.
    assert MAX_BILLS_PER_RUN[CARRIER_API] * DELAY_SECONDS[CARRIER_API] < 30 * 60
    assert MAX_BILLS_PER_RUN[CARRIER_BULKDATA] * DELAY_SECONDS[CARRIER_BULKDATA] < 30 * 60


def test_each_carrier_gets_its_own_crawl_rate(monkeypatch):
    _keyless(monkeypatch)
    assert BillSubjectsFetcher().delay == DELAY_SECONDS[CARRIER_BULKDATA]
    assert BillSubjectsFetcher(api_key="k").delay == DELAY_SECONDS[CARRIER_API]


# -- the /subjects page walk -------------------------------------------------


def _api_fetcher(monkeypatch, pages):
    fetcher = BillSubjectsFetcher(api_key="test", carrier=CARRIER_API, delay=0)
    calls: list[int] = []

    def fake_get_json(url, *, params):
        calls.append(params["offset"])
        return pages[params["offset"]]

    monkeypatch.setattr(fetcher, "_get_json", fake_get_json)
    return fetcher, calls


def test_one_call_carries_both_fields(monkeypatch):
    """policyArea and legislativeSubjects arrive together — no second request."""
    fetcher, calls = _api_fetcher(
        monkeypatch,
        {
            0: {
                "pagination": {"count": 2},
                "subjects": {
                    "policyArea": {"name": "Health"},
                    "legislativeSubjects": [{"name": "Medicare"}, {"name": "Drug safety"}],
                },
            }
        },
    )
    result = fetcher.subjects_for("118", "HR", "1")
    assert result == BillSubjects("Health", ("Medicare", "Drug safety"), CARRIER_API)
    assert calls == [0]


def test_a_fat_bill_walks_offsets_until_the_count_is_met(monkeypatch):
    fetcher, calls = _api_fetcher(
        monkeypatch,
        {
            0: {
                "pagination": {"count": 3},
                "subjects": {
                    "policyArea": {"name": "Taxation"},
                    "legislativeSubjects": [{"name": "A"}, {"name": "B"}],
                },
            },
            250: {
                "pagination": {"count": 3},
                "subjects": {"legislativeSubjects": [{"name": "C"}]},
            },
        },
    )
    result = fetcher.subjects_for("118", "HR", "1")
    assert result is not None
    assert result.policy_area == "Taxation"
    assert result.subjects == ("A", "B", "C")
    assert calls == [0, 250]


def test_a_failed_later_page_publishes_nothing_rather_than_a_truncated_list(monkeypatch):
    fetcher, _ = _api_fetcher(
        monkeypatch,
        {
            0: {
                "pagination": {"count": 400},
                "subjects": {
                    "policyArea": {"name": "Taxation"},
                    "legislativeSubjects": [{"name": "A"}],
                },
            },
            250: None,  # transport gave up
        },
    )
    assert fetcher.subjects_for("118", "HR", "1") is None
    assert fetcher.counts.failed == 1


# -- the three outcomes ------------------------------------------------------


def test_a_404_is_an_answer_not_a_failure(monkeypatch):
    _keyless(monkeypatch)
    fetcher = BillSubjectsFetcher(delay=0)
    monkeypatch.setattr(fetcher, "_get_text", lambda url: _absent())
    result = fetcher.subjects_for("108", "hr", "1")
    # The carrier answered: it does not hold this bill. Recorded so the next run
    # doesn't ask again.
    assert result == BillSubjects(None, (), CARRIER_BULKDATA, held=False)
    assert (fetcher.counts.answered, fetcher.counts.not_held, fetcher.counts.failed) == (1, 1, 0)


def test_a_held_bill_with_no_terms_is_counted_apart_from_a_404(monkeypatch):
    """Both publish a null policy_area; only one of them means "never heard of it"."""
    _keyless(monkeypatch)
    fetcher = BillSubjectsFetcher(delay=0)
    monkeypatch.setattr(fetcher, "_get_text", lambda url: "<billStatus><bill/></billStatus>")
    fetcher.subjects_for("118", "hr", "1")
    assert (fetcher.counts.unassigned, fetcher.counts.not_held) == (1, 0)


def test_every_answer_lands_in_exactly_one_bucket(monkeypatch):
    _keyless(monkeypatch)
    fetcher = BillSubjectsFetcher(delay=0)
    bodies = iter([_BILLSTATUS, "<billStatus><bill/></billStatus>", _absent()])
    monkeypatch.setattr(fetcher, "_get_text", lambda url: next(bodies))
    for n in range(3):
        fetcher.subjects_for("118", "hr", str(n))
    counts = fetcher.counts
    buckets = counts.with_policy_area + counts.subjects_only + counts.unassigned + counts.not_held
    assert buckets == counts.answered == 3


def _absent():
    from spicy_regs.sources.bill_subjects import _ABSENT

    return _ABSENT


def test_counts_tally_policy_areas_for_the_run_report(monkeypatch):
    _keyless(monkeypatch)
    fetcher = BillSubjectsFetcher(delay=0)
    monkeypatch.setattr(fetcher, "_get_text", lambda url: _BILLSTATUS)
    fetcher.subjects_for("118", "hr", "1")
    fetcher.subjects_for("118", "hr", "2")
    assert fetcher.counts.with_policy_area == 2
    assert fetcher.counts.policy_areas == {"Environmental Protection": 2}


# -- the published shape -----------------------------------------------------


def test_shape_produces_exact_schema():
    row = _shape("118-hr-1", "Health", ("Medicare",), CARRIER_API, "2026-08-22T00:00:00+00:00")
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 6
    assert json.loads(row["subjects_json"]) == ["Medicare"]
    assert row["subject_count"] == "1"
    assert row["carrier"] == CARRIER_API


# -- the transform: bounded, resumable ---------------------------------------


class _StubFetcher:
    """Answers from a canned map; anything unmapped is a transport failure."""

    def __init__(self, answers, carrier=CARRIER_BULKDATA):
        self.answers = answers
        self.carrier = carrier
        self.asked: list[str] = []
        from spicy_regs.sources.bill_subjects import FetchCounts

        self.counts = FetchCounts()

    @property
    def first_congress(self):
        return FIRST_CONGRESS[self.carrier]

    def subjects_for(self, congress, bill_type, bill_number):
        key = f"{congress}-{bill_type}-{bill_number}"
        self.asked.append(key)
        answer = self.answers.get(key)
        if answer is None:
            self.counts.failed += 1
            return None
        self.counts.answered += 1
        return answer

    def close(self):
        pass


def _answer(policy_area, *subjects):
    return BillSubjects(policy_area, subjects, CARRIER_BULKDATA)


def test_a_run_is_capped_and_the_next_one_resumes_where_it_stopped(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [(f"118-hr-{n}", "118", "hr", str(n)) for n in range(1, 6)],
    )
    answers = {f"118-hr-{n}": _answer("Health", "Medicare") for n in range(1, 6)}

    first = _StubFetcher(answers)
    enrich_bill_subjects(tmp_path, max_bills=2, fetcher=first)
    assert len(first.asked) == 2

    second = _StubFetcher(answers)
    out = enrich_bill_subjects(tmp_path, max_bills=2, fetcher=second)
    # The second run asks about two *different* bills and keeps the first two.
    assert len(second.asked) == 2
    assert set(second.asked).isdisjoint(first.asked)
    assert pq.ParquetFile(out).metadata.num_rows == 4
    assert pq.ParquetFile(out).schema_arrow.names == list(COLUMNS)


def test_a_failed_fetch_leaves_the_bill_un_enriched_for_the_next_run(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [("118-hr-1", "118", "hr", "1"), ("118-hr-2", "118", "hr", "2")],
    )
    # hr-2 has no canned answer, so the stub reports a transport failure.
    flaky = _StubFetcher({"118-hr-1": _answer("Health")})
    out = enrich_bill_subjects(tmp_path, max_bills=2, fetcher=flaky)

    rows = pq.read_table(out).to_pylist()
    assert [r["bill_id"] for r in rows] == ["118-hr-1"]

    # Next run picks the failed bill straight back up — no half-written row to
    # mistake for an answer.
    recovered = _StubFetcher({"118-hr-2": _answer("Taxation")})
    out = enrich_bill_subjects(tmp_path, max_bills=2, fetcher=recovered)
    assert recovered.asked == ["118-hr-2"]
    assert {r["bill_id"] for r in pq.read_table(out).to_pylist()} == {"118-hr-1", "118-hr-2"}


def test_a_definitive_miss_is_recorded_and_not_asked_again(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [("118-hr-1", "118", "hr", "1")])
    first = _StubFetcher({"118-hr-1": BillSubjects(None, (), CARRIER_BULKDATA)})
    enrich_bill_subjects(tmp_path, max_bills=5, fetcher=first)

    second = _StubFetcher({"118-hr-1": _answer("Health")})
    enrich_bill_subjects(tmp_path, max_bills=5, fetcher=second)
    assert second.asked == []


def test_switching_to_a_deeper_carrier_re_asks_what_the_other_one_lacked(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [("118-hr-1", "118", "hr", "1")])
    enrich_bill_subjects(
        tmp_path,
        max_bills=5,
        fetcher=_StubFetcher({"118-hr-1": BillSubjects(None, (), CARRIER_BULKDATA)}),
    )
    # A key appears; the Congress.gov carrier may well hold what GPO did not.
    api = _StubFetcher({"118-hr-1": BillSubjects("Health", (), CARRIER_API)}, carrier=CARRIER_API)
    out = enrich_bill_subjects(tmp_path, max_bills=5, fetcher=api)
    assert api.asked == ["118-hr-1"]
    rows = pq.read_table(out).to_pylist()
    assert (rows[0]["policy_area"], rows[0]["carrier"]) == ("Health", CARRIER_API)


def test_bills_below_the_carrier_floor_are_never_asked_for(tmp_path):
    bills = tmp_path / "congress_bills.parquet"
    _write_bills(
        bills,
        [
            ("107-hr-1", "107", "hr", "1"),  # below GPO's 108th-Congress floor
            ("118-hr-1", "118", "hr", "1"),
            ("118-hr-2", "118", "hr", None),  # unusable: no bill number
        ],
    )
    pending = _pending_bills(
        bills,
        tmp_path / "_absent_prior.parquet",
        carrier=CARRIER_BULKDATA,
        have_prior=False,
        limit=10,
    )
    assert [p[0] for p in pending] == ["118-hr-1"]


def test_a_missing_bill_table_fails_loudly(tmp_path):
    with pytest.raises(RuntimeError, match="congress_bills.parquet"):
        enrich_bill_subjects(tmp_path, max_bills=1, fetcher=_StubFetcher({}))

from datetime import date

from scripts.check_rollup_freshness import evaluate_comment_coverage, evaluate_date_rows, evaluate_row_changes


def test_date_freshness_checks_both_congress_watermarks():
    rows = [
        ("congress_bills", "update watermark", "2026-07-19", 100),
        ("congress_bills", "latest action", "2025-04-02", 100),
    ]
    failures = evaluate_date_rows(rows, date(2026, 7, 20))
    assert len(failures) == 1
    assert "latest action" in failures[0]


def test_row_count_state_flags_a_stalled_source():
    state = {"usaspending_recipients": {"count": 100_000, "last_changed": "2026-07-01"}}
    rows = [("usaspending_recipients", "row count", None, 100_000)]
    failures = evaluate_row_changes(rows, state, date(2026, 7, 20))
    assert failures


def test_row_count_change_resets_the_clock():
    state = {"usaspending_recipients": {"count": 99_000, "last_changed": "2026-07-01"}}
    rows = [("usaspending_recipients", "row count", None, 100_000)]
    assert evaluate_row_changes(rows, state, date(2026, 7, 20)) == []
    assert state["usaspending_recipients"] == {"count": 100_000, "last_changed": "2026-07-20"}


def test_base_tables_are_monitored():
    """dockets/documents must stay covered — nothing watched them for 8 weeks."""
    from scripts.check_rollup_freshness import DATE_CHECKS

    monitored = {(c.table, c.column) for c in DATE_CHECKS}
    assert ("dockets", "modify_date") in monitored
    assert ("documents", "modify_date") in monitored


def test_base_table_checks_avoid_future_dated_posted_date():
    """documents.posted_date carries future effective dates and can't detect a stall."""
    from scripts.check_rollup_freshness import DATE_CHECKS

    assert ("documents", "posted_date") not in {(c.table, c.column) for c in DATE_CHECKS}


def test_frozen_dockets_watermark_fails():
    """The exact production regression: dockets stuck at 2026-07-02."""
    rows = [("dockets", "modify_date", "2026-07-02T20:46:17Z", 276_326)]
    failures = evaluate_date_rows(rows, date(2026, 8, 26))
    assert len(failures) == 1
    assert "dockets" in failures[0]


def test_current_dockets_watermark_passes():
    rows = [("dockets", "modify_date", "2026-08-25T12:00:00Z", 278_000)]
    assert evaluate_date_rows(rows, date(2026, 8, 26)) == []


def _baseline(counts):
    return {"comments_coverage": {"agencies": dict(counts), "as_of": "2026-09-05"}}


def test_comment_coverage_first_run_sets_baseline():
    state = {}
    assert evaluate_comment_coverage({"EPA": 100, "VA": 386_000}, state, date(2026, 9, 5)) == []
    assert state["comments_coverage"]["agencies"] == {"EPA": 100, "VA": 386_000}


def test_comment_coverage_flags_the_197_failure():
    """103 of 179 agencies left: the vanished agencies must fail, and keep failing."""
    state = _baseline({"EPA": 1_000_000, "VA": 386_000, "ERULE": 1})
    failures = evaluate_comment_coverage({"EPA": 1_000_000}, state, date(2026, 9, 7))
    assert any("VA vanished" in f for f in failures)
    assert any("ERULE vanished" in f for f in failures)
    assert any("total fell" in f for f in failures)
    # Baseline untouched, so the next night fails again instead of going green.
    assert state["comments_coverage"]["agencies"]["VA"] == 386_000
    assert evaluate_comment_coverage({"EPA": 1_000_000}, state, date(2026, 9, 8))


def test_comment_coverage_flags_a_stub_agency():
    state = _baseline({"EPA": 25_000_000, "VA": 386_000})
    failures = evaluate_comment_coverage({"EPA": 25_000_000, "VA": 12}, state, date(2026, 9, 9))
    # 386k of 25.4M is under the 2% total budget, so only the per-agency check fires.
    assert failures == ["comments: agency VA fell 386,000 -> 12 rows"]


def test_comment_coverage_tolerates_growth_and_dedupe_trickle():
    state = _baseline({"EPA": 1_000_000, "OMB": 800})
    counts = {"EPA": 999_600, "OMB": 300, "NEW": 5}
    assert evaluate_comment_coverage(counts, state, date(2026, 9, 9)) == []
    assert state["comments_coverage"]["agencies"] == counts


def test_comment_coverage_accept_rebaselines():
    state = _baseline({"EPA": 1_000, "GONE": 5_000})
    assert evaluate_comment_coverage({"EPA": 1_000}, state, date(2026, 9, 9), accept=True) == []
    assert state["comments_coverage"]["agencies"] == {"EPA": 1_000}

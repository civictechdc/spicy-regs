from datetime import date

from scripts.check_rollup_freshness import evaluate_date_rows, evaluate_row_changes


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

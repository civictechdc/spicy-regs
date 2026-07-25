#!/usr/bin/env python3
"""Check public external-source rollups for stale watermarks and stalled growth.

Date-backed sources are checked against source-appropriate age budgets. Sources
without a meaningful update date are either tracked by row-count change
(``usaspending_recipients``) or explicitly skipped with a reason. The state file
lets the daily workflow remember when a row count last changed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

DEFAULT_BASE_URL = "https://r2.spicy-regs.dev"
FreshnessRow = tuple[str, str, str | None, int]
FreshnessState = dict[str, dict[str, int | str]]


@dataclass(frozen=True)
class DateCheck:
    table: str
    column: str
    budget_days: int
    label: str | None = None


DATE_CHECKS = (
    DateCheck("federal_register", "publication_date", 3),
    DateCheck("lobbying_filings", "dt_posted", 3),
    # Both metrics matter: update_date alone can stay green while recent
    # legislative actions are missing from an incompletely seeded congress.
    DateCheck("congress_bills", "update_date", 7, "update watermark"),
    DateCheck("congress_bills", "latest_action_date", 7, "latest action"),
    DateCheck("cfr_sections", "last_modified", 45),
    DateCheck("fec_committees", "last_file_date", 14),
    DateCheck("court_dockets", "date_filed", 14),
    DateCheck("gao_reports", "published_date", 14),
    DateCheck("crs_reports", "published_date", 14),
)

ROW_CHANGE_BUDGETS = {"usaspending_recipients": 14}
SKIPPED = {
    "unified_agenda": "semiannual edition, not a daily date watermark",
    "sam_entities": "registration_date does not change when an existing entity is refreshed",
    "court_opinions": "Supreme Court opinions are seasonal and have no daily freshness promise",
    # Promote these to DateChecks (date_created / date_received) once the first
    # publish lands — checking before then would 404 the whole freshness query.
    "fcc_proceedings": "not yet published to R2; first backfill pending",
    "fcc_filings": "not yet published to R2; first backfill pending",
}


def _query(base_url: str) -> str:
    branches = []
    for check in DATE_CHECKS:
        label = check.label or check.column
        branches.append(
            f"""SELECT '{check.table}' AS table_name, '{label}' AS metric,
                       CAST(MAX(TRY_CAST({check.column} AS TIMESTAMP)) AS VARCHAR) AS latest,
                       COUNT(*)::BIGINT AS row_count
                FROM read_parquet('{base_url}/{check.table}.parquet')"""
        )
    for table in ROW_CHANGE_BUDGETS:
        branches.append(
            f"""SELECT '{table}' AS table_name, 'row count' AS metric,
                       NULL::VARCHAR AS latest, COUNT(*)::BIGINT AS row_count
                FROM read_parquet('{base_url}/{table}.parquet')"""
        )
    return "\nUNION ALL\n".join(branches)


def _parse_latest(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None


def evaluate_date_rows(rows: Sequence[FreshnessRow], today: date) -> list[str]:
    """Return human-readable failures for the date-backed result rows."""
    budgets = {(c.table, c.label or c.column): c.budget_days for c in DATE_CHECKS}
    failures: list[str] = []
    for table, metric, raw_latest, row_count in rows:
        budget = budgets.get((table, metric))
        if budget is None:
            continue
        latest = _parse_latest(raw_latest)
        if latest is None:
            failures.append(f"{table} ({metric}): no parseable watermark across {row_count:,} rows")
            continue
        age = (today - latest).days
        status = "OK" if age <= budget else "STALE"
        print(f"{status}: {table} ({metric}) latest={latest} age={age}d budget={budget}d rows={row_count:,}")
        if age > budget:
            failures.append(f"{table} ({metric}) is {age}d old (budget {budget}d)")
    return failures


def evaluate_row_changes(
    rows: Sequence[FreshnessRow],
    state: FreshnessState,
    today: date,
) -> list[str]:
    """Update row-count state and flag tables unchanged beyond their budget."""
    failures: list[str] = []
    for table, metric, _latest, row_count in rows:
        if metric != "row count" or table not in ROW_CHANGE_BUDGETS:
            continue
        previous = state.get(table, {})
        previous_count = previous.get("count")
        if previous_count != row_count:
            state[table] = {"count": row_count, "last_changed": today.isoformat()}
            print(f"OK: {table} row count changed {previous_count!r} -> {row_count:,}")
            continue
        raw_changed = str(previous.get("last_changed", today.isoformat()))
        try:
            last_changed = date.fromisoformat(raw_changed)
        except ValueError:
            last_changed = today
        age = (today - last_changed).days
        budget = ROW_CHANGE_BUDGETS[table]
        status = "OK" if age <= budget else "STALE"
        print(f"{status}: {table} rows={row_count:,} unchanged={age}d budget={budget}d")
        if age > budget:
            failures.append(f"{table} row count has not changed for {age}d (budget {budget}d)")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("SPICY_REGS_R2_URL", DEFAULT_BASE_URL))
    parser.add_argument("--state-file", type=Path, default=Path(".rollup-freshness-state.json"))
    parser.add_argument("--today", type=date.fromisoformat, default=date.today(), help="Testing override (YYYY-MM-DD)")
    args = parser.parse_args()

    try:
        state = json.loads(args.state_file.read_text()) if args.state_file.exists() else {}
    except (OSError, json.JSONDecodeError):
        state = {}

    con = duckdb.connect()
    try:
        rows = con.execute(_query(args.base_url)).fetchall()
    finally:
        con.close()

    failures = evaluate_date_rows(rows, args.today)
    failures.extend(evaluate_row_changes(rows, state, args.today))
    args.state_file.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")

    for table, reason in SKIPPED.items():
        print(f"SKIP: {table} — {reason}")
    if failures:
        print("\nFreshness failures:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("\nAll monitored external-source rollups are within budget.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

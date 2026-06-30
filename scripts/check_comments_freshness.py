#!/usr/bin/env python3
"""Flag staleness between ``comments_index`` and the row-level ``comments`` surface.

The comment *counts* (``comments_index``, ``feed_summary``, ``agency_stats``) and
the row-level ``comments`` table are produced by different steps. If they drift —
the index advertises comments for a month the row data doesn't actually contain —
then count queries look current while ``SELECT ... FROM comments`` returns nothing
for recent dockets. That exact gap (OMB-2026-0034 showed ~39k June-2026 comments
in the index but zero rows in ``comments``) is what this check catches.

It compares, per ``agency_code``, the latest ``(year, month)`` present in
``comments_index`` against the latest ``posted_date`` month actually materialized
in the ``comments`` read surface, and reports every agency whose rows lag the
index (or are missing entirely).

The check runs against whatever the MCP server is configured to read: the R2 Data
Catalog (Iceberg) when ``R2_CATALOG_*`` is set, otherwise the public monolithic
``comments.parquet``. So it validates the *actual* surface users query.

Usage:
    uv run python scripts/check_comments_freshness.py
    uv run python scripts/check_comments_freshness.py --agency OMB
    uv run python scripts/check_comments_freshness.py --limit 50

Exit code is 1 when any agency is flagged (so it can drive a cron/CI alert), 0
otherwise.
"""

from __future__ import annotations

import argparse
import sys

from spicy_regs import mcp_server

# Per-agency: index's latest (year, month) as YYYYMM vs the latest posted_date
# month actually present in the comments rows. A frugal aggregation — MAX is
# streaming and the GROUP BY is ~hundreds of agencies — so it stays well within
# memory even over tens of millions of comment rows.
_FRESHNESS_SQL = """
WITH idx AS (
    SELECT agency_code,
           MAX(year * 100 + month) AS idx_max_ym,
           CAST(SUM(row_count) AS BIGINT) AS idx_rows
    FROM comments_index
    {idx_where}
    GROUP BY agency_code
),
rows AS (
    SELECT agency_code,
           MAX(
               EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP)) * 100
               + EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))
           ) AS rows_max_ym,
           COUNT(*) AS actual_rows
    FROM comments
    WHERE posted_date IS NOT NULL
    {rows_where}
    GROUP BY agency_code
)
SELECT i.agency_code,
       i.idx_max_ym,
       r.rows_max_ym,
       i.idx_rows,
       COALESCE(r.actual_rows, 0) AS actual_rows
FROM idx i
LEFT JOIN rows r USING (agency_code)
WHERE r.agency_code IS NULL
   OR r.rows_max_ym IS NULL
   OR r.rows_max_ym < i.idx_max_ym
ORDER BY i.idx_rows DESC
"""


def _fmt_ym(ym: int | None) -> str:
    if ym is None:
        return "—"
    return f"{ym // 100:04d}-{ym % 100:02d}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", help="Restrict the check to a single agency_code")
    parser.add_argument(
        "--limit", type=int, default=100, help="Max flagged agencies to print (default 100)"
    )
    args = parser.parse_args()

    if args.agency:
        safe = args.agency.replace("'", "''")
        idx_where = f"WHERE agency_code = '{safe}'"
        rows_where = f"AND agency_code = '{safe}'"
    else:
        idx_where = ""
        rows_where = ""

    sql = _FRESHNESS_SQL.format(idx_where=idx_where, rows_where=rows_where)

    con = mcp_server._connect()
    try:
        flagged = con.execute(sql).fetchall()
    finally:
        con.close()

    if not flagged:
        print("OK: every agency's comment rows are at least as current as the index.")
        return 0

    print(f"STALE: {len(flagged)} agency partition(s) lag the comments index\n")
    print(f"{'agency':<10} {'index_to':<9} {'rows_to':<9} {'index_rows':>12} {'actual_rows':>12}")
    print("-" * 56)
    for agency, idx_ym, rows_ym, idx_rows, actual_rows in flagged[: args.limit]:
        print(
            f"{agency:<10} {_fmt_ym(idx_ym):<9} {_fmt_ym(rows_ym):<9} "
            f"{idx_rows:>12,} {actual_rows:>12,}"
        )
    if len(flagged) > args.limit:
        print(f"... and {len(flagged) - args.limit} more (raise --limit to see them)")
    return 1


if __name__ == "__main__":
    sys.exit(main())

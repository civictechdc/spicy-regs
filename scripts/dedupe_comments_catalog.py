#!/usr/bin/env python3
"""Audit and remove duplicate rows from the Iceberg ``comments`` catalog table.

The one-time seed that loaded the pre-existing R2 partition tree into the catalog
duplicated rows for agencies that were loaded more than once — its per-agency
``DELETE`` did not remove prior rows on the R2 Data Catalog, so re-loads
accumulated exact copies (e.g. OMB ~2.5x, NARA ~3.8x; FWS/EPA/DOT clean). Comments
added later by the daily merge are unaffected — this is historical, not ongoing.

Default mode is a **read-only audit**: it reports every agency whose row count
exceeds its distinct ``comment_id`` count, plus totals. Nothing is written.

``--apply`` rebuilds the table deduplicated (one row per ``comment_id``, latest
``modify_date``) via ``CREATE OR REPLACE TABLE`` — an atomic swap that never
relies on ``DELETE`` (the very operation that left the duplicates), so it can't
double the problem: it either swaps in the clean snapshot or fails without
touching the existing data. After a successful apply, re-run the mirror
(``publish_comments_mirror.py``) so the public files the UI reads are refreshed.

Safe rollout: run with no flags first (audit), eyeball the report, then ``--apply``,
then run once more with no flags to confirm the table is clean.

Reads/writes the catalog via ``R2_CATALOG_*``. See
``.github/workflows/dedupe-comments-catalog.yml``.

Usage:
    uv run python scripts/dedupe_comments_catalog.py            # audit only (default)
    uv run python scripts/dedupe_comments_catalog.py --apply    # rebuild deduplicated
"""

from __future__ import annotations

import argparse

from loguru import logger

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rebuild the table deduplicated. Without this flag the script only audits.",
    )
    args = parser.parse_args()

    if not iceberg.is_configured():
        logger.error("R2 Data Catalog is not configured (R2_CATALOG_*); cannot proceed")
        return 1

    comments_rt = RECORD_TYPES["comments"]
    con = iceberg._connect()
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET memory_limit='6GB'")

        dupes = iceberg.audit_duplicates(con, comments_rt)
        if not dupes:
            logger.info("No duplication: every agency's comment rows are unique by comment_id.")
            return 0

        total_dupe_rows = sum(rows - distinct for _, rows, distinct in dupes)
        logger.warning("{} agency(ies) carry duplicate comment rows ({:,} duplicate rows total):", len(dupes), total_dupe_rows)
        logger.warning("{:<10} {:>14} {:>14} {:>8}", "agency", "rows", "distinct_ids", "factor")
        for agency, rows, distinct in dupes:
            factor = rows / distinct if distinct else 0
            logger.warning("{:<10} {:>14,} {:>14,} {:>7.2f}x", agency, rows, distinct, factor)

        if not args.apply:
            logger.info("Audit only (pass --apply to rebuild the table deduplicated).")
            return 0

        logger.info("Rebuilding {} deduplicated (CREATE OR REPLACE, keeping latest modify_date per comment_id)...", comments_rt.name)
        before, after = iceberg.dedupe_table(con, comments_rt)
        logger.info("Rebuilt: {:,} rows -> {:,} rows ({:,} duplicates removed)", before, after, before - after)

        remaining = iceberg.audit_duplicates(con, comments_rt)
        if remaining:
            logger.error(
                "Dedup did NOT fully take — {} agency(ies) still show duplicates. "
                "The catalog may not honor CREATE OR REPLACE as expected; investigate before re-running.",
                len(remaining),
            )
            return 1
        logger.info("Verified clean: rows now equal distinct comment_id counts for every agency.")
        logger.info("Next: run publish_comments_mirror.py to refresh the public files the UI reads.")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

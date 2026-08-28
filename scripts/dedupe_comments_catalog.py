#!/usr/bin/env python3
"""Audit and remove duplicate rows from the Iceberg ``comments`` catalog table.

The one-time seed that loaded the pre-existing R2 partition tree into the catalog
duplicated rows for agencies that were loaded more than once — its per-agency
``DELETE`` did not remove prior rows on the R2 Data Catalog, so re-loads
accumulated exact copies (e.g. OMB ~2.5x, NARA ~3.8x; FWS/EPA/DOT clean).

The daily merge is affected by the *same* unreliable ``DELETE`` (see
``iceberg._merge``), just at a far smaller scale: only comment_ids that are
re-merged — a comment whose ``modify_date`` advanced, or a key re-staged by the
redundant sweep — can leave their old row behind, so a handful of near-1.00x
duplicates trickle in per run. The read surface hides them (``mcp_server`` dedups
the ``comments`` view on read); this job is how the physical rows are reclaimed,
so it is worth re-running periodically, not only after the seed.

Default mode is a **read-only audit**: it reports every agency whose row count
exceeds its distinct ``comment_id`` count, plus totals. Nothing is written.

``--apply`` rebuilds the table deduplicated (one row per ``comment_id``, latest
``modify_date``): it writes a fresh sibling table **one agency at a time**, then
replaces the live table with it via ``DROP`` + per-agency ``INSERT`` (DuckDB's
Iceberg REST integration does not support ``ALTER TABLE RENAME``). It never uses
``DELETE`` (the operation that left the duplicates) and never rewrites the whole
table in one statement (which OOMs the runner on both the read and Iceberg-write
sides) — only bounded per-agency writes. The sibling is kept until the rebuilt
table's row count is verified, so an interrupted swap loses nothing and re-running
resumes it. After a successful apply, re-run the mirror
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
import tempfile
from os import getenv

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
        # Keep peak memory bounded: fewer threads, a firm memory cap, and on-disk
        # spill for the per-agency dedup windows. The defaults suit the CI runner;
        # override via DEDUP_MEMORY_LIMIT / DEDUP_THREADS on a bigger host (a large
        # agency's window sort buffers whole rows, comment text included, so 5GB is
        # tight for the worst agency).
        mem_limit = getenv("DEDUP_MEMORY_LIMIT", "5GB")
        threads = getenv("DEDUP_THREADS", "2")
        logger.info("duckdb dedup limits: memory_limit={} threads={}", mem_limit, threads)
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET memory_limit='{mem_limit}'")
        con.execute(f"SET threads={threads}")
        con.execute(f"SET temp_directory='{tempfile.gettempdir()}/duckdb_dedup_spill'")

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

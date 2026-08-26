#!/usr/bin/env python3
"""Seed the Iceberg catalog ``dockets`` table from the published ``dockets.parquet``.

Cutover repair. When the dockets ETL moved onto the R2 Data Catalog, ``comments``
got a real seed (``scripts/seed_comments_catalog.py``) and ``dockets`` got none —
so the catalog table only ever held rows staged *after* the cutover: ~5.4k of
~276k. Every run exported that sliver as the "full" public snapshot,
``_assert_upload_safe`` correctly refused to overwrite production with it, and
the refusal was swallowed by a discarded ``executor.map`` iterator (fixed in
#176). ``dockets.parquet`` sat frozen at 2026-07-02 with the ETL green.

This backfills the missing historical rows so the next ETL export is full-size
and publishes normally. It inserts **only** docket_ids the catalog lacks: the
rows already there are the post-cutover updates and are newer than the frozen
snapshot, so overwriting them from Parquet would roll back eight weeks of
ingested changes. That also makes a re-run a no-op instead of a double-load.

Unlike the comments tree, the source here is one monolithic file — no globbing,
no per-agency chunking. It is read over the S3 API (not the public URL) to match
the comments seeder and to sidestep edge-cache range-read corruption.

Needs both credential sets in the environment:

* R2 S3 keys (to read the source snapshot):
  ``R2_ACCESS_KEY_ID``, ``R2_SECRET_ACCESS_KEY``, ``R2_ENDPOINT``,
  ``R2_BUCKET_NAME`` (default ``spicy-regs``)
* R2 Data Catalog creds (to write the table):
  ``R2_CATALOG_URI``, ``R2_CATALOG_WAREHOUSE``, ``R2_CATALOG_TOKEN``
  (+ optional ``R2_CATALOG_NAMESPACE``)

Usage:
    uv run python scripts/seed_dockets_catalog.py --dry-run   # report, write nothing
    uv run python scripts/seed_dockets_catalog.py             # backfill the catalog
    uv run python scripts/seed_dockets_catalog.py --export    # + write the public snapshot locally
"""

from __future__ import annotations

import argparse
import sys
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas import DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.sources.r2 import upload_file

# The catalog table is expected to be badly under-populated (that is the bug);
# a source snapshot that is *itself* small would mean R2 already lost the
# historical rows, and backfilling from it would cement the loss. Refuse rather
# than seed from a truncated source.
MIN_SOURCE_ROWS = 100_000


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-key", default="dockets.parquet", help="Source object key in the R2 bucket")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be backfilled and write nothing",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export the public dockets.parquet snapshot locally after backfilling",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Publish the exported snapshot to R2 (implies --export). The next scheduled "
        "ETL would publish it anyway; use this to close the gap immediately.",
    )
    args = parser.parse_args()

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    source_uri = f"s3://{bucket}/{args.source_key}"

    out_file: Path | None = None
    con = iceberg._connect()  # iceberg + httpfs loaded, catalog attached
    try:
        iceberg.create_s3_secret(con)
        iceberg._ensure_table(con, DOCKET)

        qualified = iceberg._qualified(DOCKET)
        before = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
        source_rows = con.execute(f"SELECT count(*) FROM read_parquet('{iceberg._sql_str(source_uri)}')").fetchone()[0]

        logger.info("Catalog {} holds {:,} rows", DOCKET.name, before)
        logger.info("Source {} holds {:,} rows", source_uri, source_rows)

        if source_rows < MIN_SOURCE_ROWS:
            logger.error(
                "Source snapshot has only {:,} rows (expected >= {:,}). Refusing to "
                "backfill from what looks like a truncated file.",
                source_rows,
                MIN_SOURCE_ROWS,
            )
            return 1

        missing = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT DISTINCT "{DOCKET.dedup_key}" AS k
                FROM read_parquet('{iceberg._sql_str(source_uri)}')
                WHERE "{DOCKET.dedup_key}" IS NOT NULL
            ) src
            WHERE NOT EXISTS (
                SELECT 1 FROM {qualified} t WHERE t."{DOCKET.dedup_key}" = src.k
            );
            """
        ).fetchone()[0]
        logger.info("{:,} source docket_id(s) are missing from the catalog", missing)

        if args.dry_run:
            logger.info("--dry-run: nothing written. Would backfill {:,} row(s).", missing)
            return 0

        if not missing:
            logger.info("Catalog already holds every source docket_id — nothing to backfill.")
            inserted, total = 0, before
        else:
            inserted, total = iceberg.backfill_missing_from_parquet(con, source_uri, DOCKET)
            logger.info("Backfilled {:,} row(s); catalog {} now holds {:,}", inserted, DOCKET.name, total)

        if total < source_rows:
            logger.warning(
                "Catalog ({:,}) still holds fewer rows than the source ({:,}) — investigate "
                "before relying on the next export.",
                total,
                source_rows,
            )

        if args.export or args.upload:
            out_file = iceberg._export_parquet(con, DOCKET, args.output_dir)
            exported = con.execute(
                f"SELECT count(*) FROM read_parquet('{iceberg._sql_str(str(out_file))}')"
            ).fetchone()[0]
            logger.info("Exported public snapshot to {} ({:,} rows)", out_file, exported)
    finally:
        con.close()

    if args.upload and out_file is not None:
        # Goes through the ordinary publish path, shrink guard included — if the
        # backfill somehow left the export short, this refuses rather than
        # overwriting production with a truncated file.
        logger.info("Publishing {} to R2...", out_file.name)
        upload_file(out_file, remote_key=args.source_key)

    logger.info("Done. Verify with: uv run python scripts/check_rollup_freshness.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Publish the public comments read-mirror from the Iceberg catalog.

The browser UI reads comments as public Parquet on R2 (it can't reach the
credentialed R2 Data Catalog). Once the ETL routes comments through Iceberg it
writes rows into the catalog and only republishes the small
``comments_index.parquet`` — so the public surface the UI actually reads goes
stale:

* ``comments.parquet``                                  — the flat monolith (UI full scans)
* ``comments/agency/agency_code={X}/part-0.parquet``    — the per-agency tree (UI agency/docket views)

This regenerates that whole mirror from the catalog (the write-side system of
record) and uploads it, restoring the dual model for comments so the UI serves
current data without ever touching the catalog.

Reads the catalog (``R2_CATALOG_*``) and writes public Parquet (``R2_*``). It is
read-only against the catalog; the only writes are the public mirror files. Runs
on a daily cron after the ETL batches settle, plus manual dispatch — see
``.github/workflows/publish-comments-mirror.yml``. ``upload_file`` refuses to
overwrite a much smaller remote object, so a truncated export can't wipe the live
files.

Usage:
    uv run python scripts/publish_comments_mirror.py
    uv run python scripts/publish_comments_mirror.py --skip-upload   # build locally only
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, r2
from spicy_regs.transforms import partition_comments

# The mirror runs with R2_ALLOW_SHRINK=1 (see the workflow): re-exporting from the
# catalog legitimately changes on-disk size — the sorted, freshly-compressed
# partitions can be several times *smaller* than the old published files while
# holding *more* rows — so r2's byte-size shrink guard produces false positives.
# This row-count floor replaces that protection with one appropriate for a full
# export: a catastrophically empty/broken catalog read (which would otherwise wipe
# the public files) is caught here before anything is uploaded. The live table is
# tens of millions of rows; a floor well below that only trips on real breakage.
MIN_EXPECTED_ROWS = 1_000_000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--skip-upload", action="store_true", help="Build the mirror locally but don't publish to R2"
    )
    args = parser.parse_args()

    if not iceberg.is_configured():
        logger.error("R2 Data Catalog is not configured (R2_CATALOG_*); cannot export the mirror")
        return 1

    output_dir: Path = args.output_dir
    comments_rt = RECORD_TYPES["comments"]

    # 1. Monolith + index straight from the catalog.
    result = iceberg.export_public_comments(output_dir, comments_rt)

    # Floor check (replaces the bypassed byte-size shrink guard): refuse to publish
    # a catastrophically small export that would wipe the live public files.
    n_rows = pq.ParquetFile(result["comments"]).metadata.num_rows
    if n_rows < MIN_EXPECTED_ROWS:
        logger.error(
            "Exported monolith has only {:,} rows (< {:,} floor); the catalog read looks "
            "broken — refusing to publish and overwrite the live files",
            n_rows,
            MIN_EXPECTED_ROWS,
        )
        return 1
    logger.info("Exported monolith has {:,} rows", n_rows)

    # 2. Derive the per-agency tree the UI reads for scoped queries from that monolith.
    partition_dir = partition_comments(output_dir)
    agency_files = sorted(partition_dir.glob("agency_code=*/part-0.parquet"))
    logger.info("Built {} agency partition(s)", len(agency_files))

    if args.skip_upload:
        logger.info("--skip-upload set; mirror left in {}", output_dir)
        return 0

    # 3. Publish: monolith, then the partitions + refreshed index.
    r2.upload_file(result["comments"], remote_key="comments.parquet")
    r2.upload_comment_partitions(output_dir, agency_files)
    logger.info("Published comments mirror: monolith + {} partition(s) + index", len(agency_files))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Seed the Iceberg catalog ``comments`` table from the existing R2 partitions.

One-time loader for the catalog cutover (see PR #89). The partitioned
``comments/`` tree on R2 is already current; this copies it into the R2 Data
Catalog ``comments`` table so the MCP servers can serve row-level reads from the
catalog instead of the frozen monolithic ``comments.parquet``. Loading per agency
keeps memory bounded and gives progress. After loading it rebuilds
``comments_index.parquet`` from the catalog so a quick diff against the published
index confirms the load is complete.

Needs both credential sets in the environment:

* R2 S3 keys (to read the source partitions):
  ``R2_ACCESS_KEY_ID``, ``R2_SECRET_ACCESS_KEY``, ``R2_ENDPOINT``,
  ``R2_BUCKET_NAME`` (default ``spicy-regs``)
* R2 Data Catalog creds (to write the table):
  ``R2_CATALOG_URI``, ``R2_CATALOG_WAREHOUSE``, ``R2_CATALOG_TOKEN``
  (+ optional ``R2_CATALOG_NAMESPACE``)

Usage:
    uv run python scripts/seed_comments_catalog.py
    uv run python scripts/seed_comments_catalog.py --agency OMB        # one agency
    uv run python scripts/seed_comments_catalog.py --append            # add to a non-empty table
    uv run python scripts/seed_comments_catalog.py --upload-index      # publish the rebuilt index
"""

from __future__ import annotations

import argparse
import sys
from os import getenv
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2

load_dotenv()

_REQUIRED_S3_ENV = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")


def _create_s3_secret(con) -> None:
    """Register R2's S3 API as a DuckDB secret so the partition tree can be globbed.

    Reads over the public HTTPS URL can't list directories (the whole reason the
    catalog cutover exists), so the source partitions are read via the S3 API,
    which does support listing/globbing.
    """
    missing = [v for v in _REQUIRED_S3_ENV if not getenv(v)]
    if missing:
        raise RuntimeError("Missing R2 S3 env var(s): " + ", ".join(missing))
    # R2_ENDPOINT is a full URL for boto3; DuckDB wants the bare host + USE_SSL.
    endpoint = getenv("R2_ENDPOINT", "")
    host = urlparse(endpoint).netloc or endpoint.replace("https://", "").replace("http://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2_s3_secret (
            TYPE S3,
            KEY_ID '{iceberg._sql_str(getenv("R2_ACCESS_KEY_ID", ""))}',
            SECRET '{iceberg._sql_str(getenv("R2_SECRET_ACCESS_KEY", ""))}',
            ENDPOINT '{iceberg._sql_str(host)}',
            URL_STYLE 'path',
            USE_SSL true,
            REGION 'auto'
        );
        """
    )


def _agencies(con, bucket: str, only: str | None) -> list[str]:
    """Agencies to load, from the published index (or just ``only`` when given)."""
    if only:
        return [only]
    index_uri = f"s3://{bucket}/comments_index.parquet"
    rows = con.execute(
        f"SELECT DISTINCT agency_code FROM read_parquet('{iceberg._sql_str(index_uri)}') "
        "WHERE agency_code IS NOT NULL ORDER BY agency_code"
    ).fetchall()
    return [r[0] for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", help="Load only this agency_code (default: all)")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--append", action="store_true",
        help="Allow loading into a non-empty catalog table (default: refuse). "
             "The load is idempotent per agency (each agency's rows are replaced), "
             "so this is safe for resuming an interrupted run.",
    )
    parser.add_argument(
        "--upload-index", action="store_true",
        help="Publish the rebuilt comments_index.parquet to R2 after loading",
    )
    args = parser.parse_args()

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")

    con = iceberg._connect()  # iceberg + httpfs loaded, catalog attached
    try:
        _create_s3_secret(con)
        iceberg._ensure_table(con, COMMENT)

        existing = con.execute(
            f"SELECT count(*) FROM {iceberg._qualified(COMMENT)}"
        ).fetchone()[0]
        if existing and not args.append:
            logger.error(
                "Catalog comments table already has {:,} rows. Re-run with --append "
                "to load anyway — the load is idempotent per agency (each agency's "
                "rows are replaced), so this is safe.", existing,
            )
            return 1

        agencies = _agencies(con, bucket, args.agency)
        logger.info("Seeding {} agency partition set(s) into the catalog", len(agencies))

        total = existing
        for i, agency in enumerate(agencies, 1):
            safe = iceberg._sql_str(agency)
            glob = (
                f"s3://{bucket}/comments/agency_code={safe}/"
                "docket_id=*/year=*/month=*/part-0.parquet"
            )
            try:
                # replace_agency makes each agency load idempotent: a re-run
                # (resume after a timeout, or over an already-seeded table)
                # replaces that agency's rows rather than duplicating them.
                total = iceberg.seed_comments_from_parquet(
                    con, glob, COMMENT, replace_agency=agency
                )
            except Exception as exc:  # noqa: BLE001 — keep going, report at the end
                logger.warning("  [{}/{}] {}: skipped ({})", i, len(agencies), agency, exc)
                continue
            logger.info("  [{}/{}] {}: catalog now holds {:,} rows", i, len(agencies), agency, total)

        logger.info("Load complete: {:,} rows in the catalog comments table", total)

        index_file = iceberg._build_comments_index(con, COMMENT, args.output_dir)
        logger.info("Rebuilt comments index at {}", index_file)
    finally:
        con.close()

    if args.upload_index:
        logger.info("Uploading rebuilt comments index to R2...")
        r2.upload_file(index_file, remote_key="comments_index.parquet")

    logger.info(
        "Done. Verify with: uv run python scripts/check_comments_freshness.py"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

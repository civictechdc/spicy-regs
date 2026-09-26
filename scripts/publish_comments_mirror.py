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
``.github/workflows/publish-comments-mirror.yml``.

Two checks run before anything is uploaded: a total row-count floor, and an
agency-set guard that refuses to publish when an agency in the currently
published ``comments_index.parquet`` is absent from the new export. The second
exists because a dedupe swap that died mid-refill left the catalog holding 103 of
179 agencies, and the mirror republished that for weeks with nothing noticing
(#197). A deliberate removal is let through with ``--allow-missing-agency``. The
same comparison also refuses an agency that loses most of its rows (override:
``--allow-agency-shrink``).

Usage:
    uv run python scripts/publish_comments_mirror.py
    uv run python scripts/publish_comments_mirror.py --skip-upload   # build locally only
    uv run python scripts/publish_comments_mirror.py --allow-missing-agency ERULE
"""

from __future__ import annotations

import argparse
import tempfile
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


# Per-agency shrink guard: the agency-set check alone passes an agency that is
# still present but holds a sliver of its rows. After #197's recovery started,
# VA reappeared with 12 comments of its ~386k, and publishing overwrote its full
# partition with that stub. An agency trips only when it loses at least half its
# rows *and* at least MIN_AGENCY_DROP rows, so small agencies and the normal
# dedupe trickle (a handful of duplicate rows) never do. A dedupe that
# legitimately collapses a heavily duplicated agency needs --allow-agency-shrink.
MAX_AGENCY_DROP_RATIO = 0.5
MIN_AGENCY_DROP = 1_000


def _index_counts(index_file: Path) -> dict[str, int]:
    table = pq.read_table(index_file, columns=["agency_code", "row_count"])
    counts: dict[str, int] = {}
    for agency, n in zip(table.column("agency_code").to_pylist(), table.column("row_count").to_pylist()):
        if agency is not None:
            counts[agency] = counts.get(agency, 0) + (n or 0)
    return counts


def missing_agencies(published_index: Path, new_index: Path, allowed: set[str]) -> list[str]:
    """Agencies in the published index but absent from the new one, minus ``allowed``."""
    return sorted(_index_counts(published_index).keys() - _index_counts(new_index).keys() - allowed)


def shrunken_agencies(published_index: Path, new_index: Path, allowed: set[str]) -> list[tuple[str, int, int]]:
    """``(agency, published_rows, new_rows)`` for agencies that lost most of their rows.

    Only agencies present in both indexes; absent ones are :func:`missing_agencies`.
    """
    old, new = _index_counts(published_index), _index_counts(new_index)
    shrunk = []
    for agency, before in old.items():
        after = new.get(agency)
        if after is None or agency in allowed:
            continue
        if before - after >= MIN_AGENCY_DROP and after < before * (1 - MAX_AGENCY_DROP_RATIO):
            shrunk.append((agency, before, after))
    return sorted(shrunk, key=lambda r: r[1] - r[2], reverse=True)


def check_agencies(new_index: Path, allow_missing: set[str], allow_shrink: set[str]) -> bool:
    """False when the new export drops or guts an agency the live mirror publishes.

    Compares against the ``comments_index.parquet`` currently on R2. When there
    is none yet (first publish, or R2 not configured) there is nothing to
    compare, so the check passes; a failed download raises rather than passing.
    """
    with tempfile.TemporaryDirectory() as tmp:
        published = Path(tmp) / "comments_index.parquet"
        if not r2.download_from_r2("comments_index.parquet", published):
            logger.warning("No published comments_index.parquet to compare against; skipping agency checks")
            return True
        missing = missing_agencies(published, new_index, allow_missing)
        shrunk = shrunken_agencies(published, new_index, allow_shrink)
    ok = True
    if missing:
        logger.error(
            "Export drops {} agency(ies) the live mirror still publishes: {}. The catalog has "
            "likely lost rows (see #197) — refusing to publish. If the removal is deliberate, "
            "re-run with --allow-missing-agency for each code.",
            len(missing),
            " ".join(missing),
        )
        ok = False
    if shrunk:
        logger.error(
            "Export shrinks {} agency(ies) by more than half: {}. Refusing to publish. If the "
            "shrink is deliberate (e.g. a dedupe of a heavily duplicated agency), re-run with "
            "--allow-agency-shrink for each code.",
            len(shrunk),
            ", ".join(f"{a} {b:,} -> {n:,}" for a, b, n in shrunk),
        )
        ok = False
    if ok:
        logger.info("Agency checks passed: no published agency is missing or gutted in the export")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--skip-upload", action="store_true", help="Build the mirror locally but don't publish to R2"
    )
    parser.add_argument(
        "--allow-missing-agency",
        action="append",
        default=[],
        metavar="CODE",
        help="Publish even though this agency is absent from the export (repeatable)",
    )
    parser.add_argument(
        "--allow-agency-shrink",
        action="append",
        default=[],
        metavar="CODE",
        help="Publish even though this agency loses most of its rows in the export (repeatable)",
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

    allow_missing = {a.upper() for a in args.allow_missing_agency}
    allow_shrink = {a.upper() for a in args.allow_agency_shrink}
    if not check_agencies(result["index"], allow_missing, allow_shrink):
        return 1

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

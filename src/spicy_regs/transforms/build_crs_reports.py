"""Transform: build ``crs_reports.parquet`` from the Congress.gov REST API.

Produces an 8-column all-VARCHAR schema keyed on ``report_id`` (e.g. ``R48641``,
``IN12713``) — the Congressional Research Service analysis layer over the
policy questions behind the rulemakings this dataset tracks.

Incremental by design (mirrors ``build_congress_bills``). A full re-fetch of the
entire CRS archive every run would be wasteful *and* would trip the R2
catastrophic-shrink guard on any short run. Instead we:

1. Best-effort download the prior ``crs_reports.parquet`` from R2.
2. Fetch only reports updated since its max ``update_date`` (minus a short
   overlap to catch late-updated reports). Reports come newest-updated first, so
   the reader stops as soon as it pages past that watermark.
3. Dedup the union on ``report_id``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill.

Scope is deliberately **list-level only**: every column comes from the
``/crsreport`` list payload, so there are no per-report detail fetches (no N+1).
The per-report detail endpoint carries authors, formats, and related materials
that a later enrichment pass could add.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.crs_reports import CrsReportsReader

OUTPUT = "crs_reports.parquet"

# Re-scan this many days before the last stored update_date on each run, so
# reports updated after our previous run's cutoff are picked up.
OVERLAP_DAYS = 3

# The published schema: 8 columns, all VARCHAR, in a fixed order. ``report_id``
# is the primary / dedup key.
COLUMNS = (
    "report_id",
    "title",
    "report_type",
    "status",
    "published_date",
    "update_date",
    "version",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (version comes as an int.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov CRS report onto the published column shape."""
    return {
        "report_id": doc.get("id"),
        "title": doc.get("title"),
        "report_type": doc.get("contentType"),
        "status": doc.get("status"),
        "published_date": doc.get("publishDate"),
        "update_date": doc.get("updateDate"),
        "version": _s(doc.get("version")),
        "url": doc.get("url"),
    }


def _prior_max_update_date(prior_file: Path) -> date | None:
    """Largest ``update_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(update_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_crs_reports(output_dir: Path, *, since: date | None = None) -> Path:
    """Build ``crs_reports.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_crs_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("CRS reports: merging against prior table {}", prior_file)
    else:
        logger.info("CRS reports: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_update_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("CRS reports: fetching reports updated since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    reader = CrsReportsReader(since=since)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_crs_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CRS reports: fetched {:,} reports this run", len(rows))

    # 4. Merge prior + new, dedup on report_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY report_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE report_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY update_date DESC, report_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CRS reports: {:,} rows", total)
    return out_file

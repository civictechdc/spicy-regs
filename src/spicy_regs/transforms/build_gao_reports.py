"""Transform: build ``gao_reports.parquet`` from the GAO reports RSS feed.

Produces an 8-column all-VARCHAR schema keyed on ``report_id`` (e.g.
``gao-26-107974``) — the Government Accountability Office oversight layer over
the rulemakings this dataset tracks.

**Incremental accumulator.** The GAO RSS feed is a recent-items window (~25
newest products), not the full archive, and GAO's bulk/search surfaces are
bot-blocked (see :mod:`spicy_regs.sources.gao_reports`). So rather than a
watermark-bounded re-fetch, each run parses the whole feed and *appends* any
previously unseen products to the prior published table, deduping on
``report_id`` and preferring the freshly fetched row. Over successive daily runs
the table grows into a rolling history.

Because the merge is append-only against a growing table, this never shrinks the
output — so it stays clear of the R2 catastrophic-shrink guard.

``agencies_json`` and ``topics_json`` are pinned but reserved: the RSS feed does
not carry structured agency/topic tags, so they default to ``[]`` today and a
later enrichment pass (or a future non-blocked GAO source) can populate them.
"""

from __future__ import annotations

from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.gao_reports import GaoReportsReader

OUTPUT = "gao_reports.parquet"

# The published schema: 8 columns, all VARCHAR, in a fixed order. ``report_id``
# is the primary / dedup key.
COLUMNS = (
    "report_id",
    "title",
    "report_type",
    "published_date",
    "abstract",
    "agencies_json",
    "topics_json",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

# GAO's reports RSS feed carries published products (reports & testimonies).
# The feed does not tag a finer product type, so we default to this label.
_DEFAULT_REPORT_TYPE = "Report"


def _report_id(link: str | None) -> str | None:
    """Extract the ``gao-##-######`` id from a product URL, or None.

    Links look like ``https://www.gao.gov/products/gao-26-107974``; the id is the
    last path segment, lowercased.
    """
    if not link:
        return None
    segment = link.rstrip("/").rsplit("/", 1)[-1].strip().lower()
    return segment or None


def _published_date(pub_date: str | None) -> str | None:
    """Parse the RFC-822 ``pubDate`` to an ISO date string, or None.

    Falls back to None (rather than raising) on an unparseable value so one odd
    item can't fail the run.
    """
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).date().isoformat()
    except (TypeError, ValueError):
        return None


def _shape(item: dict) -> dict:
    """Map one raw GAO RSS item onto the published column shape."""
    return {
        "report_id": _report_id(item.get("link")),
        "title": item.get("title"),
        "report_type": _DEFAULT_REPORT_TYPE,
        "published_date": _published_date(item.get("pub_date")),
        "abstract": item.get("description"),
        # Reserved — the RSS feed carries no structured agency/topic tags.
        "agencies_json": "[]",
        "topics_json": "[]",
        "url": item.get("link"),
    }


def build_gao_reports(output_dir: Path, *, max_records: int | None = None) -> Path:
    """Build ``gao_reports.parquet`` (append-only merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_gao_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a fresh start).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("GAO reports: accumulating onto prior table {}", prior_file)
    else:
        logger.info("GAO reports: no prior table found — starting fresh")

    # 2. Fetch + shape the current feed window into a "new rows" parquet.
    reader = GaoReportsReader(max_records=max_records)
    rows = [_shape(item) for item in reader.iter_records()]
    new_file = output_dir / "_gao_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("GAO reports: fetched {:,} items this run", len(rows))

    # 3. Merge prior + new, dedup on report_id preferring the new row.
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")

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
            ORDER BY published_date DESC, report_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("GAO reports: {:,} rows", total)
    return out_file

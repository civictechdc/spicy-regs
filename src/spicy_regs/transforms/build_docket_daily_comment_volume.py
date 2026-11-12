"""Transform: build the per-docket daily comment-volume rollup.

Produces ``docket_daily_comment_volume.parquet``: one row per
``(docket_id, day)`` with the number of comments posted to that docket that day.
It replaces the browser-side scan the docket Comments tab and the /lab volume
curve run today (``getCommentVolumeByDay`` / the ``volumeByDay`` branch of the
docket comment summary in ``spicy-regs-ui``), which read a whole agency's comment
partition just to draw one docket's series.

**Date basis: ``posted_date``.** The rollup buckets on the day regulations.gov
*posted* a comment, not the day it was *received*:

* It is what the UI computes today, so switching the UI to this file changes no
  chart.
* It is the basis ``comments_index`` partitions on, so a docket's daily counts
  summed over a month equal that month's ``comments_index.row_count``.
* ``posted_date`` is always midnight US Eastern stored in UTC (``04:00Z`` /
  ``05:00Z``), so its UTC calendar date *is* the Eastern posting date.
  ``receive_date`` carries real submission times, so a UTC truncation would
  push evening (Eastern) submissions onto the next day.
* ``receive_date`` is null far more often (~42K rows vs ~2K).

The trade-off: ``posted_date`` reflects when the agency got through its review
queue, so a burst of submissions can show up later and flatter than it arrived.

**Filters** mirror ``comments_index`` (and therefore what the UI can resolve):
rows need a ``docket_id``, an ``agency_code``, and a ``posted_date`` that casts to
a ``DATE``. ``docket_id`` is trimmed of stray quotes the same way. Rows are not
deduplicated on ``comment_id``, matching both the UI query and ``comments_index``.

**Layout.** ~375K rows / ~1.4 MB today (60K dockets). Sorted by
``(docket_id, day)`` with small row groups so a reader filtering one docket over
HTTP prunes on the row-group ``docket_id`` min/max stats and fetches only the
footer plus one (rarely two) small row group.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.comments_source import resolve_comments_source

OUTPUT = "docket_daily_comment_volume.parquet"

# Small row groups keep a single-docket read to a few tens of KB: the footer
# plus the one row group whose docket_id range covers the requested docket.
ROW_GROUP_SIZE = 16_384

# The published schema, in a fixed order.
SCHEMA = pa.schema(
    [
        ("docket_id", pa.string()),
        ("agency_code", pa.string()),
        ("day", pa.date32()),
        ("comment_count", pa.int64()),
    ]
)


def build_query(comments_source: str, out_file: str) -> str:
    """Return the ``COPY ... TO`` statement that materializes the rollup.

    Split out so the exact published SQL can be exercised against a local
    fixture without R2 access.
    """
    comments_source = comments_source.replace("'", "''")
    out_file = out_file.replace("'", "''")
    return f"""
    COPY (
        SELECT
            TRIM(docket_id, '"') AS docket_id,
            agency_code,
            TRY_CAST(posted_date AS DATE) AS day,
            CAST(COUNT(*) AS BIGINT) AS comment_count
        FROM read_parquet('{comments_source}')
        WHERE docket_id IS NOT NULL
          AND agency_code IS NOT NULL
          AND TRY_CAST(posted_date AS DATE) IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY docket_id, day
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE});
    """


def build_docket_daily_comment_volume(output_dir: Path) -> Path:
    """Build ``docket_daily_comment_volume.parquet`` from ``comments.parquet``.

    Reads a local ``comments.parquet`` in ``output_dir`` when present, otherwise
    the public R2 copy (projecting only the four columns it needs).
    """
    import duckdb

    comments_source = resolve_comments_source(output_dir, consumer="docket_daily_comment_volume")
    out_file = output_dir / OUTPUT

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    if comments_source.startswith("https://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    logger.info("Building docket daily comment volume via DuckDB...")
    con.execute(build_query(comments_source, str(out_file)))
    con.close()

    meta = pq.ParquetFile(out_file).metadata
    logger.info(
        "Docket daily comment volume: {:,} rows in {} row groups, {:.2f} MB",
        meta.num_rows,
        meta.num_row_groups,
        out_file.stat().st_size / (1024 * 1024),
    )
    return out_file

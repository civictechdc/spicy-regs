"""Transform: build the discovery *spike* signal rollup.

Of the four feed discovery signals, only **spike** scans ``documents.parquet``
in the browser (surge / closing / discussed already ride ``comments_index`` /
``feed_summary``). This bakes the per-agency spike so the feed stops scanning
the ~57 MB documents file on every load.

Spike = agencies whose document output in the last 30 days is ≥ 2× their
prior-year monthly mean (requiring ≥ 24 documents in the prior year to avoid
tiny-denominator noise). Recomputed each run since it is CURRENT_DATE-relative;
the full qualifying set is baked (no LIMIT) so the UI applies its own top-N.
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_discovery_signals(output_dir: Path) -> Path:
    """Build ``discovery_signals.parquet`` (the per-agency spike signal)."""
    import duckdb

    documents_file = output_dir / "documents.parquet"
    if not documents_file.exists():
        raise FileNotFoundError(f"documents.parquet not found in {output_dir}")

    logger.info("Building discovery signals (spike) via DuckDB...")

    out_file = output_dir / "discovery_signals.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    query = f"""
    COPY (
        WITH per AS (
            SELECT agency_code,
              COUNT(*) FILTER (
                WHERE TRY_CAST(posted_date AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
              ) AS recent_30d,
              COUNT(*) FILTER (
                WHERE TRY_CAST(posted_date AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '13' MONTH
                  AND TRY_CAST(posted_date AS TIMESTAMP) <  CURRENT_TIMESTAMP - INTERVAL '1' MONTH
              ) AS prior_yr
            FROM read_parquet('{documents_file}')
            WHERE TRY_CAST(posted_date AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '13' MONTH
              AND agency_code IS NOT NULL
            GROUP BY 1
        )
        SELECT agency_code,
               recent_30d,
               (prior_yr / 12.0) AS baseline,
               (recent_30d / NULLIF(prior_yr / 12.0, 0)) AS ratio
        FROM per
        WHERE prior_yr >= 24
          AND (recent_30d / NULLIF(prior_yr / 12.0, 0)) >= 2.0
        ORDER BY ratio DESC
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Discovery signals (spike): {:,} rows", rows)

    return out_file

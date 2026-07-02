"""Transform: build the rulemaking-lifecycles rollup (Proposed→Rule pairing).

Replaces the multi-CTE self-join over ``documents.parquet`` that the browser ran
on the agency profile / lab panels. Bakes both shapes the UI needs into one
artifact, discriminated by a ``kind`` column, for **all** agencies (the UI
filters by agency and slices its own samples/percentiles client-side):

* ``kind = 'pair'``  — completed rulemakings: the earliest Proposed Rule paired
  with the earliest matching Rule for the same docket, with the completion
  ``days``. Powers the per-agency duration percentiles, the completed strip-plot
  sample, and the government-wide benchmark.
* ``kind = 'stuck'`` — Proposed Rules with no matching Rule in the docket
  (``final_date``/``days`` NULL). The UI derives ``days_open`` from
  ``proposed_date`` live and applies its own age-window + tier sampling.

Bounded to ``proposed_date >= 2010`` (matching the browser floor); otherwise
date-independent, so it does not need a daily rebuild for correctness.
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_rulemaking_lifecycles(output_dir: Path) -> Path:
    """Build ``rulemaking_lifecycles.parquet`` (completed pairs + stuck proposals)."""
    import duckdb

    documents_file = output_dir / "documents.parquet"
    if not documents_file.exists():
        raise FileNotFoundError(f"documents.parquet not found in {output_dir}")

    logger.info("Building rulemaking lifecycles via DuckDB...")

    out_file = output_dir / "rulemaking_lifecycles.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    query = f"""
    COPY (
        WITH props AS (
            SELECT docket_id, agency_code,
                   ANY_VALUE(title) AS title,
                   MIN(TRY_CAST(posted_date AS DATE)) AS proposed_date
            FROM read_parquet('{documents_file}')
            WHERE document_type = 'Proposed Rule'
            GROUP BY 1, 2
        ),
        finals AS (
            SELECT docket_id, MIN(TRY_CAST(posted_date AS DATE)) AS final_date
            FROM read_parquet('{documents_file}')
            WHERE document_type = 'Rule'
            GROUP BY 1
        ),
        pairs AS (
            SELECT 'pair' AS kind, p.docket_id, p.agency_code, p.title,
                   p.proposed_date, f.final_date,
                   date_diff('day', p.proposed_date, f.final_date) AS days
            FROM props p
            JOIN finals f USING (docket_id)
            WHERE p.proposed_date IS NOT NULL
              AND f.final_date IS NOT NULL
              AND f.final_date >= p.proposed_date
              AND p.proposed_date >= DATE '2010-01-01'
        ),
        stuck AS (
            SELECT 'stuck' AS kind, p.docket_id, p.agency_code, p.title,
                   p.proposed_date, CAST(NULL AS DATE) AS final_date,
                   CAST(NULL AS BIGINT) AS days
            FROM props p
            LEFT JOIN finals f USING (docket_id)
            WHERE f.docket_id IS NULL
              AND p.proposed_date IS NOT NULL
              AND p.proposed_date >= DATE '2010-01-01'
        )
        SELECT * FROM pairs
        UNION ALL
        SELECT * FROM stuck
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Rulemaking lifecycles: {:,} rows (pairs + stuck)", rows)

    return out_file

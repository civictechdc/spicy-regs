#!/usr/bin/env python3
"""
Migrate comments.parquet → partitioned comments structure.

Reads the monolithic comments.parquet and writes it into Hive-partitioned
files at:

    comments/agency_code={A}/docket_id={D}/year={Y}/month={M}/part-0.parquet

Also builds the comments_index.parquet used by the frontend and feed summary.

Usage:
    uv run python scripts/migrate_comments_partitioned.py [--output-dir output]
"""

from pathlib import Path
import sys

import duckdb
import pyarrow.parquet as pq
from loguru import logger


def migrate(output_dir: Path) -> None:
    comments_file = output_dir / "comments.parquet"
    if not comments_file.exists():
        logger.error("comments.parquet not found in {}", output_dir)
        sys.exit(1)

    comments_dir = output_dir / "comments"
    comments_dir.mkdir(parents=True, exist_ok=True)

    total_rows = pq.ParquetFile(comments_file).metadata.num_rows
    logger.info("Migrating {:,} rows from comments.parquet...", total_rows)

    # Get the list of columns from the file
    schema = pq.read_schema(comments_file)
    col_names = [f.name for f in schema]
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in col_names)

    # Discover all partitions
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{spill_dir}'")

    logger.info("Discovering partitions...")
    partitions = con.execute(f"""
        SELECT DISTINCT
            agency_code,
            TRIM(docket_id, '"') AS docket_id,
            EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::INT AS year,
            EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::INT AS month
        FROM read_parquet('{comments_file}')
        WHERE posted_date IS NOT NULL
          AND agency_code IS NOT NULL
          AND docket_id IS NOT NULL
    """).fetchall()

    logger.info("Found {:,} partitions to write", len(partitions))

    # Write each partition
    written = 0

    for i, (agency, docket, year, month) in enumerate(partitions):
        partition_path = (
            comments_dir
            / f"agency_code={agency}"
            / f"docket_id={docket}"
            / f"year={year}"
            / f"month={month}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        partition_file = partition_path / "part-0.parquet"

        docket_escaped = str(docket).replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT {col_select}
                FROM read_parquet('{comments_file}')
                WHERE agency_code = '{agency}'
                  AND TRIM(docket_id, '"') = '{docket_escaped}'
                  AND EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP)) = {year}
                  AND EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP)) = {month}
                ORDER BY posted_date
            ) TO '{partition_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        written += 1
        if written % 1000 == 0:
            logger.info("  written {:,}/{:,} partitions", written, len(partitions))

    con.close()

    logger.info("Written {:,} partition files", written)

    # Build the index
    logger.info("Building comments index...")
    from spicy_regs.pipeline.transform import update_comments_index

    all_partition_files = list(comments_dir.rglob("part-0.parquet"))
    index_path = update_comments_index(output_dir, all_partition_files)
    logger.info("Index written to {}", index_path)

    logger.info("Migration complete!")
    logger.info("You can now upload with:")
    logger.info("  uv run python -m spicy_regs.pipeline.upload_r2 {}", output_dir / "comments_index.parquet")
    logger.info("  And upload the comments/ directory to R2")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate comments to partitioned format")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    migrate(args.output_dir)

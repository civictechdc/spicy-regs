"""Transform: build the pre-computed feed summary rollup."""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_feed_summary(output_dir: Path) -> Path:
    """Build pre-computed feed summary with docket info, comment counts, and comment end dates.

    Comment counts come from ``comments_index.parquet`` (a tiny file with
    per-partition row counts) rather than scanning the full 24.7M-row
    comments dataset.  Falls back to the monolithic ``comments.parquet``
    if the index doesn't exist yet.

    Joins dockets + comments (counts) + documents (max comment_end_date)
    into a single small Parquet file sorted by modify_date DESC.
    """
    import duckdb

    dockets_file = output_dir / "dockets.parquet"
    comments_index_file = output_dir / "comments_index.parquet"
    comments_file = output_dir / "comments.parquet"
    documents_file = output_dir / "documents.parquet"

    if not dockets_file.exists():
        raise FileNotFoundError(f"dockets.parquet not found in {output_dir}")

    logger.info("Building feed summary via DuckDB...")

    summary_file = output_dir / "feed_summary.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # Build the query dynamically based on which files exist.
    # Prefer the comments index (tiny) over the monolithic comments file.
    comment_join = ""
    comment_col = "0 AS comment_count,"
    if comments_index_file.exists():
        comment_join = f"""
        LEFT JOIN (
            SELECT docket_id, CAST(SUM(row_count) AS BIGINT) AS comment_count
            FROM read_parquet('{comments_index_file}')
            GROUP BY docket_id
        ) cc ON cc.docket_id = d.docket_id
        """
        comment_col = "COALESCE(cc.comment_count, 0) AS comment_count,"
    elif comments_file.exists():
        comment_join = f"""
        LEFT JOIN (
            SELECT
                TRIM(docket_id, '"') AS docket_id,
                COUNT(*) AS comment_count
            FROM read_parquet('{comments_file}')
            GROUP BY TRIM(docket_id, '"')
        ) cc ON cc.docket_id = d.docket_id
        """
        comment_col = "COALESCE(cc.comment_count, 0) AS comment_count,"

    doc_join = ""
    doc_cols = "NULL AS comment_end_date, NULL AS date_created,"
    if documents_file.exists():
        doc_join = f"""
        LEFT JOIN (
            SELECT
                TRIM(docket_id, '"') AS docket_id,
                MAX(comment_end_date) AS comment_end_date,
                MIN(posted_date) AS date_created
            FROM read_parquet('{documents_file}')
            GROUP BY TRIM(docket_id, '"')
        ) da ON da.docket_id = d.docket_id
        """
        doc_cols = "da.comment_end_date, da.date_created,"

    query = f"""
    COPY (
        SELECT
            d.docket_id,
            d.agency_code,
            d.title,
            d.docket_type,
            d.modify_date,
            d.abstract,
            {comment_col}
            {doc_cols}
        FROM (
            SELECT * REPLACE (TRIM(docket_id, '"') AS docket_id)
            FROM read_parquet('{dockets_file}')
        ) d
        {comment_join}
        {doc_join}
        ORDER BY d.modify_date DESC
    ) TO '{summary_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """

    con.execute(query)
    con.close()

    file_size = summary_file.stat().st_size / (1024 * 1024)

    # Get row count for logging
    row_count = pq.ParquetFile(summary_file).metadata.num_rows
    logger.info("Feed summary: {:,} rows, {:.1f} MB", row_count, file_size)

    return summary_file

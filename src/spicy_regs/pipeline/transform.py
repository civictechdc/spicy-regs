"""
Transform tasks: convert raw records to Parquet staging files and
merge staging into final output with schema evolution.
"""

from shutil import move
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute
import pyarrow.parquet as pq
from loguru import logger


def write_staging(
    agency: str,
    data_type: str,
    records: list[dict],
    staging_dir: Path,
    schema: dict,
) -> int:
    """Write parsed records to a staging Parquet file for one agency/data_type."""
    if not records:
        return 0

    staging_type_dir = staging_dir / data_type
    staging_type_dir.mkdir(parents=True, exist_ok=True)
    staging_file = staging_type_dir / f"{agency}.parquet"

    df = pl.DataFrame(records, schema=schema)
    df.write_parquet(staging_file, compression="zstd")
    return len(records)


def merge_staging_files(
    staging_dir: Path,
    output_dir: Path,
    data_types_to_merge: list[str],
    schemas: dict[str, dict],
) -> None:
    """
    Merge staging files into final output using PyArrow streaming.
    Handles schema evolution by adding missing columns with nulls.

    schemas: mapping of data_type name -> {column_name: polars_type}
    """
    for data_type in data_types_to_merge:
        staging_type_dir = staging_dir / data_type
        output_file = output_dir / f"{data_type}.parquet"

        if not staging_type_dir.exists():
            continue

        staging_files = list(staging_type_dir.glob("*.parquet"))
        if not staging_files:
            continue

        logger.info("Merging {} staging files for {}...", len(staging_files), data_type)

        files_to_merge = []
        if output_file.exists():
            files_to_merge.append(output_file)
        files_to_merge.extend(staging_files)

        if len(files_to_merge) == 1 and not output_file.exists():
            move(files_to_merge[0], output_file)
            logger.info("{}: moved single file", data_type)
            continue

        temp_output = output_dir / f"{data_type}_merged.parquet"
        target_columns = list(schemas[data_type].keys())
        target_schema = pa.schema([(col, pa.large_string()) for col in target_columns])

        total_rows = 0
        with pq.ParquetWriter(temp_output, target_schema, compression="zstd") as writer:
            for file_path in files_to_merge:
                table = pq.read_table(file_path)

                existing_cols = set(table.column_names)
                for col in target_columns:
                    if col not in existing_cols:
                        null_array = pa.nulls(table.num_rows, type=pa.large_string())
                        table = table.append_column(col, null_array)

                table = table.select(target_columns)
                table = table.cast(target_schema)
                writer.write_table(table)
                total_rows += table.num_rows
                del table

        if output_file.exists():
            output_file.unlink()
        temp_output.rename(output_file)

        logger.info("{}: merged {:,} total rows", data_type, total_rows)


def partition_comments(output_dir: Path) -> Path:
    """Partition comments.parquet by agency_code into Hive-style directory.

    Streams the file in batches, groups each batch by agency_code, and
    appends to per-agency Parquet files.  After all batches, each file is
    re-read, sorted by (docket_id, posted_date), and rewritten.

    Peak memory ≈ batch_size rows + largest single-agency file during the
    final sort pass, rather than the full 24.7M-row table.

    Output: comments/agency/agency_code={X}/part-0.parquet
    Returns the partition output directory.
    """
    comments_file = output_dir / "comments.parquet"
    if not comments_file.exists():
        raise FileNotFoundError(f"comments.parquet not found in {output_dir}")

    partition_dir = output_dir / "comments" / "agency"
    partition_dir.mkdir(parents=True, exist_ok=True)

    pf = pq.ParquetFile(comments_file)
    total_rows = pf.metadata.num_rows
    logger.info("Partitioning {:,} rows by agency_code (streaming)...", total_rows)

    # --- Pass 1: Stream batches and append to per-agency files ---
    # Track per-agency ParquetWriters so we can append across batches.
    writers: dict[str, pq.ParquetWriter] = {}
    # Schema without the agency_code column (it's in the directory name)
    target_schema = None
    agency_row_counts: dict[str, int] = {}
    processed = 0

    for batch in pf.iter_batches(batch_size=500_000):
        table = pa.Table.from_batches([batch])
        if target_schema is None:
            target_schema = pa.schema(
                [f for f in table.schema if f.name != "agency_code"]
            )

        # Group by agency_code
        agencies = table.column("agency_code").to_pylist()
        unique_agencies = set(a for a in agencies if a is not None)

        for agency in unique_agencies:
            mask = pa.compute.equal(table.column("agency_code"), agency)
            agency_table = table.filter(mask).drop(["agency_code"])
            agency_table = agency_table.cast(target_schema)

            if agency not in writers:
                agency_dir = partition_dir / f"agency_code={agency}"
                agency_dir.mkdir(parents=True, exist_ok=True)
                out_path = agency_dir / "part-0.parquet"
                writers[agency] = pq.ParquetWriter(
                    out_path, target_schema, compression="zstd"
                )
                agency_row_counts[agency] = 0

            writers[agency].write_table(agency_table)
            agency_row_counts[agency] += agency_table.num_rows
            del agency_table

        processed += table.num_rows
        del table
        logger.info("  partitioned {:,}/{:,} rows", processed, total_rows)

    # Close all writers
    for w in writers.values():
        w.close()
    writers.clear()

    # --- Pass 2: Sort each per-agency file by (docket_id, posted_date) ---
    logger.info("Sorting {} agency partitions...", len(agency_row_counts))
    for agency in sorted(agency_row_counts):
        part_path = partition_dir / f"agency_code={agency}" / "part-0.parquet"
        # Read via ParquetFile to avoid pq.read_table inferring hive partitions
        # (which would re-add agency_code as a column).
        table = pq.ParquetFile(part_path).read()
        sort_indices = pa.compute.sort_indices(
            table,
            sort_keys=[("docket_id", "ascending"), ("posted_date", "ascending")],
        )
        table = table.take(sort_indices)
        pq.write_table(table, part_path, compression="zstd", row_group_size=500_000)
        del table, sort_indices

    logger.info(
        "Partitioned {:,} rows into {} agencies in {}",
        total_rows,
        len(agency_row_counts),
        partition_dir,
    )
    return partition_dir


def build_feed_summary(output_dir: Path) -> Path:
    """Build pre-computed feed summary with docket info, comment counts, and comment end dates.

    Uses DuckDB to query the Parquet files directly on disk instead of
    loading them into memory.  The comments table (24.7M rows, 3 GB+) is
    never materialised — DuckDB streams the aggregation.

    Joins dockets + comments (counts) + documents (max comment_end_date)
    into a single small Parquet file sorted by modify_date DESC.
    """
    import duckdb

    dockets_file = output_dir / "dockets.parquet"
    comments_file = output_dir / "comments.parquet"
    documents_file = output_dir / "documents.parquet"

    if not dockets_file.exists():
        raise FileNotFoundError(f"dockets.parquet not found in {output_dir}")

    logger.info("Building feed summary via DuckDB...")

    summary_file = output_dir / "feed_summary.parquet"

    con = duckdb.connect()

    # Build the query dynamically based on which files exist
    comment_join = ""
    comment_col = "0 AS comment_count,"
    if comments_file.exists():
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

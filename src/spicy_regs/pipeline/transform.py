"""
Transform tasks: convert raw records to Parquet staging files and
merge staging into final output with schema evolution.
"""

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
    dedup_keys: dict[str, str],
) -> None:
    """
    Merge staging files into final output using DuckDB streaming.

    Handles schema evolution via ``union_by_name=true`` (missing columns
    become NULL) and deduplicates by primary key, keeping the row with the
    most recent ``modify_date`` per key.  This prevents incremental ETL
    runs from accumulating duplicate rows when the same source JSON is
    re-downloaded with an updated ``modifyDate``.

    schemas:    mapping of data_type name -> {column_name: polars_type}
    dedup_keys: mapping of data_type name -> primary-key column name
                (e.g. ``"dockets": "docket_id"``).  Dedup is by
                (primary key) keeping ``MAX(modify_date)``.
    """
    import duckdb

    for data_type in data_types_to_merge:
        staging_type_dir = staging_dir / data_type
        output_file = output_dir / f"{data_type}.parquet"

        if not staging_type_dir.exists():
            continue

        staging_files = list(staging_type_dir.glob("*.parquet"))
        if not staging_files:
            continue

        logger.info("Merging {} staging files for {}...", len(staging_files), data_type)

        files_to_merge: list[Path] = []
        if output_file.exists():
            files_to_merge.append(output_file)
        files_to_merge.extend(staging_files)

        # Drop corrupt files so DuckDB doesn't abort the whole merge.
        valid_files: list[Path] = []
        for file_path in files_to_merge:
            try:
                pq.ParquetFile(file_path)
            except Exception as e:
                logger.warning(
                    "{}: skipping corrupt file {}: {}",
                    data_type, file_path.name, e,
                )
                continue
            valid_files.append(file_path)

        if not valid_files:
            continue

        target_columns = list(schemas[data_type].keys())
        key_col = dedup_keys.get(data_type)
        if key_col is None:
            raise ValueError(
                f"merge_staging_files: no dedup key configured for '{data_type}'"
            )
        if key_col not in target_columns or "modify_date" not in target_columns:
            raise ValueError(
                f"merge_staging_files: schema for '{data_type}' must include "
                f"'{key_col}' and 'modify_date'"
            )

        temp_output = output_dir / f"{data_type}_merged.parquet"

        # Escape single quotes in paths for inline SQL.
        files_sql = ", ".join(f"'{str(p).replace(chr(39), chr(39) * 2)}'" for p in valid_files)
        col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in target_columns)

        query = f"""
        COPY (
            SELECT {col_select}
            FROM read_parquet([{files_sql}], union_by_name=true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "{key_col}"
                ORDER BY modify_date DESC NULLS LAST
            ) = 1
        ) TO '{str(temp_output).replace(chr(39), chr(39) * 2)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000);
        """

        spill_dir = output_dir / ".duckdb_tmp"
        spill_dir.mkdir(exist_ok=True)

        con = duckdb.connect()
        try:
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET threads=2")
            con.execute(f"SET temp_directory='{spill_dir}'")
            con.execute(query)
        finally:
            con.close()

        if output_file.exists():
            output_file.unlink()
        temp_output.rename(output_file)

        total_rows = pq.ParquetFile(output_file).metadata.num_rows
        logger.info(
            "{}: merged {:,} deduped rows (key={})",
            data_type, total_rows, key_col,
        )


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
    #
    # Uses DuckDB (not PyArrow) because PyArrow's default ``string`` type
    # has 32-bit offsets — concatenating chunks during ``take()`` over a
    # multi-million-row agency with long ``comment`` bodies overflows the
    # 2 GB per-column limit (``offset overflow while concatenating``).
    # DuckDB handles variable-width strings without that cap.
    import duckdb

    logger.info("Sorting {} agency partitions...", len(agency_row_counts))
    # Column list without agency_code — we pass this explicitly to the
    # SELECT so DuckDB doesn't re-infer the hive partition column and
    # bake it back into the file.
    select_cols = ", ".join(
        f'"{f.name}"' for f in target_schema if f.name != "agency_code"
    )
    # Allow DuckDB to spill to disk when a single agency's decompressed
    # string columns exceed RAM.  Reuse partition_dir as the spill area
    # so temp files land on the same volume as the output.
    spill_dir = partition_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    for agency in sorted(agency_row_counts):
        part_path = partition_dir / f"agency_code={agency}" / "part-0.parquet"
        tmp_path = part_path.with_suffix(".sorted.parquet")
        # Fresh connection per agency so memory is fully released between
        # large partitions (CEQ / FWS / CFPB can each exceed RAM once
        # string columns are decompressed).
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit='16GB'")
            con.execute("SET preserve_insertion_order=false")
            con.execute("SET threads=2")
            con.execute(f"SET temp_directory='{spill_dir}'")
            con.execute(f"""
            COPY (
                SELECT {select_cols} FROM read_parquet('{part_path}', hive_partitioning=false)
                ORDER BY docket_id, posted_date
            ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000);
            """)
        finally:
            con.close()
        tmp_path.replace(part_path)

    # Clean up spill directory
    if spill_dir.exists():
        for p in spill_dir.glob("*"):
            p.unlink()
        spill_dir.rmdir()

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

"""Transform: partition the monolithic comments.parquet by agency_code."""

from pathlib import Path

import pyarrow as pa
import pyarrow.compute
import pyarrow.parquet as pq
from loguru import logger


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
            mask = pa.compute.equal(table.column("agency_code"), agency)  # ty: ignore[unresolved-attribute]
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
    assert target_schema is not None, "target_schema is populated in the first batch above"
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

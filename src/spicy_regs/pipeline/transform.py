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

    Reads the merged comments.parquet, groups by agency_code, and writes
    sorted partitions to comments/agency/agency_code={X}/part-0.parquet.

    Returns the partition output directory.
    """
    comments_file = output_dir / "comments.parquet"
    if not comments_file.exists():
        raise FileNotFoundError(f"comments.parquet not found in {output_dir}")

    partition_dir = output_dir / "comments" / "agency"
    partition_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Reading comments.parquet for partitioning...")
    table = pq.read_table(comments_file)
    total_rows = table.num_rows
    logger.info("Read {:,} rows, partitioning by agency_code...", total_rows)

    # Get unique agency codes
    agency_col = table.column("agency_code")
    agencies = sorted(set(agency_col.to_pylist()))
    agencies = [a for a in agencies if a is not None]

    for agency in agencies:
        mask = pa.compute.equal(agency_col, agency)
        agency_table = table.filter(mask)

        # Sort by docket_id, posted_date within partition
        sort_indices = pa.compute.sort_indices(
            agency_table,
            sort_keys=[("docket_id", "ascending"), ("posted_date", "ascending")],
        )
        agency_table = agency_table.take(sort_indices)

        agency_dir = partition_dir / f"agency_code={agency}"
        agency_dir.mkdir(parents=True, exist_ok=True)
        out_path = agency_dir / "part-0.parquet"

        pq.write_table(
            agency_table,
            out_path,
            compression="zstd",
            row_group_size=500_000,
        )
        logger.info("  {}: {:,} rows", agency, agency_table.num_rows)
        del agency_table

    del table
    logger.info("Partitioned {:,} rows into {} agencies", total_rows, len(agencies))
    return partition_dir


def build_feed_summary(output_dir: Path) -> Path:
    """Build pre-computed feed summary with docket info, comment counts, and comment end dates.

    Joins dockets + comments (counts) + documents (max comment_end_date)
    into a single small parquet file sorted by modify_date DESC.
    """
    dockets_file = output_dir / "dockets.parquet"
    comments_file = output_dir / "comments.parquet"
    documents_file = output_dir / "documents.parquet"

    if not dockets_file.exists():
        raise FileNotFoundError(f"dockets.parquet not found in {output_dir}")

    logger.info("Building feed summary...")

    # Read dockets
    dockets = pl.read_parquet(dockets_file)
    logger.info("  Dockets: {:,} rows", len(dockets))

    # Compute comment counts per docket
    comment_counts = pl.DataFrame({"docket_id": [], "comment_count": []}, schema={"docket_id": pl.Utf8, "comment_count": pl.Int64})
    if comments_file.exists():
        comments = pl.read_parquet(comments_file, columns=["docket_id"])
        comment_counts = (
            comments
            .with_columns(pl.col("docket_id").str.strip_chars('"'))
            .group_by("docket_id")
            .agg(pl.len().alias("comment_count"))
        )
        del comments
        logger.info("  Comment counts: {:,} dockets", len(comment_counts))

    # Compute max comment_end_date and earliest posted_date per docket from documents
    doc_aggs = pl.DataFrame(
        {"docket_id": [], "comment_end_date": [], "date_created": []},
        schema={"docket_id": pl.Utf8, "comment_end_date": pl.Utf8, "date_created": pl.Utf8},
    )
    if documents_file.exists():
        docs = pl.read_parquet(documents_file, columns=["docket_id", "comment_end_date", "posted_date"])
        docs = docs.with_columns(pl.col("docket_id").str.strip_chars('"'))
        doc_aggs = (
            docs
            .group_by("docket_id")
            .agg(
                pl.col("comment_end_date").drop_nulls().max().alias("comment_end_date"),
                pl.col("posted_date").drop_nulls().min().alias("date_created"),
            )
        )
        del docs
        logger.info("  Document aggregates: {:,} dockets", len(doc_aggs))

    # Join everything
    summary = (
        dockets
        .with_columns(pl.col("docket_id").str.strip_chars('"'))
        .join(comment_counts, on="docket_id", how="left")
        .join(doc_aggs, on="docket_id", how="left")
        .with_columns(pl.col("comment_count").fill_null(0))
        .sort("modify_date", descending=True)
    )
    del dockets

    summary_file = output_dir / "feed_summary.parquet"
    summary.write_parquet(summary_file, compression="zstd", row_group_size=50_000)

    file_size = summary_file.stat().st_size / (1024 * 1024)
    logger.info("Feed summary: {:,} rows, {:.1f} MB", len(summary), file_size)

    return summary_file

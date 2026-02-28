"""
Transform tasks: convert raw records to Parquet staging files and
merge staging into final output with schema evolution.
"""

from shutil import move
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from prefect import task


@task(name="write-staging")
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


@task(name="merge-staging-files")
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
                writer.write_table(table)
                total_rows += table.num_rows
                del table

        if output_file.exists():
            output_file.unlink()
        temp_output.rename(output_file)

        logger.info("{}: merged {:,} total rows", data_type, total_rows)

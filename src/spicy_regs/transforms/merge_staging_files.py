"""Transform: merge per-agency staging Parquet into the deduplicated dataset."""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


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
            con.execute("SET memory_limit='4GB'")
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

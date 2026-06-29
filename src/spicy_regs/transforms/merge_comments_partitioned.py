"""Transform: merge staging comments into Hive-partitioned output files."""

from pathlib import Path

from loguru import logger


def merge_comments_partitioned(
    staging_dir: Path,
    output_dir: Path,
    schema: dict,
    dedup_key: str,
) -> list[Path]:
    """Merge staging comments into partitioned output by agency/docket/year/month.

    Instead of merging all 24.7M comments into one monolithic file (which
    OOM's on CI runners), this writes each batch's comments directly into
    small Hive-partitioned files::

        comments/agency_code={A}/docket_id={D}/year={Y}/month={M}/part-0.parquet

    For each affected partition, the existing partition file is downloaded
    from R2 (if it exists), merged with the new staging data, deduplicated
    by ``dedup_key`` (keeping the latest ``modify_date``), and written back.

    Returns the list of changed partition file paths.
    """
    import duckdb

    from spicy_regs.sources.r2 import download_from_r2

    staging_type_dir = staging_dir / "comments"
    if not staging_type_dir.exists():
        return []

    staging_files = list(staging_type_dir.glob("*.parquet"))
    if not staging_files:
        return []

    comments_dir = output_dir / "comments"
    target_columns = list(schema.keys())

    # Escape single quotes in paths for SQL.
    def sql_path(p: Path) -> str:
        return str(p).replace("'", "''")

    files_sql = ", ".join(f"'{sql_path(p)}'" for p in staging_files)
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in target_columns)

    logger.info("Processing {} comment staging files into partitions...", len(staging_files))

    # Load staging into a temp table for efficient per-partition queries.
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")

    con.execute(f"""
        CREATE TABLE _staging AS
        SELECT {col_select},
            TRIM(docket_id, '"') AS _clean_docket,
            EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::INT AS _year,
            EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::INT AS _month
        FROM read_parquet([{files_sql}], union_by_name=true)
        WHERE posted_date IS NOT NULL
          AND agency_code IS NOT NULL
          AND docket_id IS NOT NULL
    """)

    partitions = con.execute("""
        SELECT DISTINCT agency_code, _clean_docket, _year, _month
        FROM _staging
    """).fetchall()

    if not partitions:
        con.close()
        return []

    logger.info("Found {} affected comment partitions", len(partitions))

    # Download existing partitions from R2 for dedup.
    for agency, docket, year, month in partitions:
        partition_file = (
            comments_dir
            / f"agency_code={agency}"
            / f"docket_id={docket}"
            / f"year={year}"
            / f"month={month}"
            / "part-0.parquet"
        )
        if not partition_file.exists():
            partition_file.parent.mkdir(parents=True, exist_ok=True)
            r2_key = str(partition_file.relative_to(output_dir))
            download_from_r2(r2_key, partition_file)

    # Merge each partition: staging rows + existing → dedup → write.
    col_select_plain = ", ".join(f'"{c}"' for c in target_columns)
    changed: list[Path] = []

    def existing_partition_select(path: Path) -> str:
        """Project ``target_columns`` from an existing partition file.

        Older partitions written before a schema change may be missing
        newly-added columns. Reading them with a plain ``SELECT "col"`` would
        raise a binder error, so emit ``NULL AS "col"`` for any column absent
        from the file (DuckDB ``union_by_name`` only fills NULLs across a
        multi-file read, not a single-file one). This lets incremental runs
        merge old partitions into the evolved schema instead of breaking.
        """
        present = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{sql_path(path)}')"
            ).fetchall()
        }
        return ", ".join(
            f'"{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"'
            for c in target_columns
        )

    for agency, docket, year, month in partitions:
        partition_file = (
            comments_dir
            / f"agency_code={agency}"
            / f"docket_id={docket}"
            / f"year={year}"
            / f"month={month}"
            / "part-0.parquet"
        )
        temp_file = partition_file.with_suffix(".tmp.parquet")
        docket_escaped = str(docket).replace("'", "''")

        staging_sql = f"""
            SELECT {col_select_plain} FROM _staging
            WHERE agency_code = '{agency}'
              AND _clean_docket = '{docket_escaped}'
              AND _year = {year} AND _month = {month}
        """

        if partition_file.exists():
            existing_sql = f"""
                UNION ALL
                SELECT {existing_partition_select(partition_file)}
                FROM read_parquet('{sql_path(partition_file)}')
            """
        else:
            existing_sql = ""

        con.execute(f"""
            COPY (
                SELECT {col_select_plain} FROM (
                    {staging_sql}
                    {existing_sql}
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY "{dedup_key}"
                    ORDER BY modify_date DESC NULLS LAST
                ) = 1
                ORDER BY posted_date
            ) TO '{sql_path(temp_file)}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        temp_file.replace(partition_file)
        changed.append(partition_file)

    con.close()
    logger.info("Updated {} comment partitions", len(changed))
    return changed

"""Transform: build the per-agency materialized rollups (stats + monthly volume)."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from loguru import logger


def build_agency_rollups(output_dir: Path) -> tuple[Path, Path]:
    """Build the per-agency materialized rollups for the directory + profile pages.

    Produces two small, denormalized "index-file" artifacts that replace
    full scans of ``documents.parquet`` in the browser (see issue #54):

    * ``agency_stats.parquet`` — one row per agency with dockets / documents /
      comments counts (the directory + profile dimension table). Comment counts
      come from the tiny ``comments_index.parquet`` rather than scanning the
      24.7M-row comments dataset, falling back to the monolithic
      ``comments.parquet`` if the index doesn't exist yet.
    * ``agency_monthly_volume.parquet`` — per-agency monthly document count
      typed by ``document_type``, so it serves both the directory activity
      sparkline and the profile activity panel.

    Both rollups are identical across all viewers and cheap to materialize.
    """
    import duckdb

    dockets_file = output_dir / "dockets.parquet"
    documents_file = output_dir / "documents.parquet"
    comments_index_file = output_dir / "comments_index.parquet"
    comments_file = output_dir / "comments.parquet"

    if not dockets_file.exists():
        raise FileNotFoundError(f"dockets.parquet not found in {output_dir}")

    logger.info("Building agency rollups via DuckDB...")

    stats_file = output_dir / "agency_stats.parquet"
    volume_file = output_dir / "agency_monthly_volume.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # --- agency_stats: per-agency dockets / documents / comments counts ---
    # Build the CTEs dynamically so the rollup works whether or not the
    # documents / comments artifacts are present yet.
    ctes = [
        f"""dk AS (
            SELECT agency_code, COUNT(*) AS docket_count
            FROM read_parquet('{dockets_file}')
            GROUP BY agency_code
        )"""
    ]
    union_parts = ["SELECT agency_code FROM dk"]
    joins = ["LEFT JOIN dk ON dk.agency_code = a.agency_code"]
    doc_count_col = "0 AS document_count"
    comment_count_col = "0 AS comment_count"

    if documents_file.exists():
        ctes.append(
            f"""doc AS (
            SELECT agency_code, COUNT(*) AS document_count
            FROM read_parquet('{documents_file}')
            GROUP BY agency_code
        )"""
        )
        union_parts.append("SELECT agency_code FROM doc")
        joins.append("LEFT JOIN doc ON doc.agency_code = a.agency_code")
        doc_count_col = "COALESCE(doc.document_count, 0) AS document_count"

    if comments_index_file.exists():
        ctes.append(
            f"""cmt AS (
            SELECT agency_code, CAST(SUM(row_count) AS BIGINT) AS comment_count
            FROM read_parquet('{comments_index_file}')
            GROUP BY agency_code
        )"""
        )
        union_parts.append("SELECT agency_code FROM cmt")
        joins.append("LEFT JOIN cmt ON cmt.agency_code = a.agency_code")
        comment_count_col = "COALESCE(cmt.comment_count, 0) AS comment_count"
    elif comments_file.exists():
        ctes.append(
            f"""cmt AS (
            SELECT agency_code, COUNT(*) AS comment_count
            FROM read_parquet('{comments_file}')
            GROUP BY agency_code
        )"""
        )
        union_parts.append("SELECT agency_code FROM cmt")
        joins.append("LEFT JOIN cmt ON cmt.agency_code = a.agency_code")
        comment_count_col = "COALESCE(cmt.comment_count, 0) AS comment_count"

    stats_query = f"""
    COPY (
        WITH {", ".join(ctes)},
        agencies AS ({" UNION ".join(union_parts)})
        SELECT
            a.agency_code,
            COALESCE(dk.docket_count, 0) AS docket_count,
            {doc_count_col},
            {comment_count_col}
        FROM agencies a
        {" ".join(joins)}
        WHERE a.agency_code IS NOT NULL
        ORDER BY comment_count DESC, document_count DESC, a.agency_code
    ) TO '{stats_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(stats_query)

    # --- agency_monthly_volume: per-agency monthly document count by type ---
    if documents_file.exists():
        volume_query = f"""
        COPY (
            SELECT
                agency_code,
                EXTRACT(YEAR FROM TRY_CAST(posted_date AS DATE)) AS year,
                EXTRACT(MONTH FROM TRY_CAST(posted_date AS DATE)) AS month,
                document_type,
                COUNT(*) AS document_count
            FROM read_parquet('{documents_file}')
            WHERE posted_date IS NOT NULL
              AND TRY_CAST(posted_date AS DATE) IS NOT NULL
            GROUP BY agency_code, year, month, document_type
            ORDER BY agency_code, year, month
        ) TO '{volume_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        con.execute(volume_query)
    else:
        # No documents yet — still emit the artifact with a stable schema so
        # the frontend can always read it.
        pl.DataFrame(
            [],
            schema={
                "agency_code": pl.Utf8,
                "year": pl.Int64,
                "month": pl.Int64,
                "document_type": pl.Utf8,
                "document_count": pl.Int64,
            },
        ).write_parquet(volume_file, compression="zstd")

    con.close()

    stats_rows = pq.ParquetFile(stats_file).metadata.num_rows
    volume_rows = pq.ParquetFile(volume_file).metadata.num_rows
    logger.info(
        "Agency rollups: agency_stats {:,} rows, agency_monthly_volume {:,} rows",
        stats_rows,
        volume_rows,
    )

    return stats_file, volume_file

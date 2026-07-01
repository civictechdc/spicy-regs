"""Transform: build the per-agency stats rollup (dockets/documents/comments counts).

Split out of the former monolithic ``build_agency_rollups`` so it can be
materialized by its own decoupled rollup pipeline. Produces
``agency_stats.parquet`` — the directory + profile dimension table.
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger


def build_agency_stats(output_dir: Path) -> Path:
    """Build ``agency_stats.parquet`` — one row per agency with dockets /
    documents / comments counts.

    Comment counts come from the tiny ``comments_index.parquet`` rather than
    scanning the 24.7M-row comments dataset, falling back to the monolithic
    ``comments.parquet`` if the index doesn't exist yet. The CTEs are built
    dynamically so the rollup works whether or not the documents / comments
    artifacts are present yet.
    """
    import duckdb

    dockets_file = output_dir / "dockets.parquet"
    documents_file = output_dir / "documents.parquet"
    comments_index_file = output_dir / "comments_index.parquet"
    comments_file = output_dir / "comments.parquet"

    if not dockets_file.exists():
        raise FileNotFoundError(f"dockets.parquet not found in {output_dir}")

    logger.info("Building agency stats via DuckDB...")

    stats_file = output_dir / "agency_stats.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

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
    con.close()

    stats_rows = pq.ParquetFile(stats_file).metadata.num_rows
    logger.info("Agency stats: {:,} rows", stats_rows)

    return stats_file

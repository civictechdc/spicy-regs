"""Cloudflare R2 Data Catalog (Apache Iceberg) connector.

The internal, write-side table format for the ETL. The catalog is **R2 Data
Catalog** — a managed Iceberg REST catalog built into the same R2 bucket the
project already publishes Parquet to, so no separate metastore (Glue/Nessie/
Postgres) has to be stood up.

Everything is driven through DuckDB's ``iceberg`` + ``httpfs`` extensions:
DuckDB >= 1.4 can ``ATTACH`` a REST catalog and run ``MERGE INTO``, which lets
us reuse the exact DuckDB merge idiom already proven in
:mod:`spicy_regs.transforms.merge_staging_files` (dedup by primary key, keep the
row with the most recent ``modify_date``) — only now it's a *row-level* upsert
into a versioned table instead of a whole-file rewrite.

This module is the "Iceberg load" stage only, mirroring the thin-wrapper style
of :mod:`spicy_regs.sources.r2`:

* :func:`merge_and_export` — ensure the table exists, MERGE the per-agency
  staging Parquet in, then export a public ``{name}.parquet`` snapshot so the
  no-credentials CLI / MCP read path keeps working (the "dual model").

Credentials are read from the environment, alongside the existing ``R2_*`` vars:

* ``R2_CATALOG_URI``        — the Iceberg REST catalog endpoint (catalog-uri)
* ``R2_CATALOG_WAREHOUSE``  — the warehouse name
* ``R2_CATALOG_TOKEN``      — an R2 API token with R2 + data-catalog permissions
* ``R2_CATALOG_NAMESPACE``  — Iceberg namespace/schema (optional, default ``default``)
"""

from os import getenv
from pathlib import Path

from loguru import logger

from spicy_regs.schemas import RecordType

# DuckDB alias the attached catalog is addressed by (``<alias>.<namespace>.<table>``).
_CATALOG_ALIAS = "reg_catalog"

# Required environment variables for the catalog connection.
_REQUIRED_ENV = ("R2_CATALOG_URI", "R2_CATALOG_WAREHOUSE", "R2_CATALOG_TOKEN")


def is_configured() -> bool:
    """True when every credential needed to reach the catalog is present."""
    return all(getenv(var) for var in _REQUIRED_ENV)


def _namespace() -> str:
    return getenv("R2_CATALOG_NAMESPACE", "default")


def _schema_ref() -> str:
    """Quoted ``alias."namespace"`` reference (the default namespace is a keyword)."""
    return f'{_CATALOG_ALIAS}."{_namespace()}"'


def _sql_str(value: str) -> str:
    """Escape a value for inlining inside a single-quoted SQL literal."""
    return value.replace("'", "''")


def _connect():
    """Open a DuckDB connection with the R2 Data Catalog attached.

    ``CREATE SECRET`` / ``ATTACH`` do not accept bind parameters, so the
    credentials are inlined with single-quote escaping. The token never leaves
    this process — it is read from the environment, used to attach, and the
    connection is closed by the caller.
    """
    import duckdb

    if not is_configured():
        missing = [var for var in _REQUIRED_ENV if not getenv(var)]
        raise RuntimeError(
            "R2 Data Catalog is not configured; missing env var(s): " + ", ".join(missing)
        )

    uri = getenv("R2_CATALOG_URI", "")
    warehouse = getenv("R2_CATALOG_WAREHOUSE", "")
    token = getenv("R2_CATALOG_TOKEN", "")

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{_sql_str(token)}');")
    con.execute(
        f"ATTACH '{_sql_str(warehouse)}' AS {_CATALOG_ALIAS} "
        f"(TYPE ICEBERG, ENDPOINT '{_sql_str(uri)}');"
    )
    return con


def _qualified(record_type: RecordType) -> str:
    """Fully-qualified catalog table identifier: ``alias."namespace"."name"``."""
    return f'{_schema_ref()}."{record_type.name}"'


def _ensure_table(con, record_type: RecordType) -> None:
    """Create the namespace + table (all columns VARCHAR) if they don't exist.

    The schema mirrors the published Parquet: every column is a UTF-8 string
    (see :mod:`spicy_regs.schemas.regulations`), so a flat ``VARCHAR`` table is
    a faithful representation and keeps ``MERGE``/export trivially type-safe.
    """
    columns = ", ".join(f'"{col}" VARCHAR' for col in record_type.schema)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_schema_ref()};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {_qualified(record_type)} ({columns});")


def _staging_files(staging_dir: Path, record_type: RecordType) -> list[Path]:
    """Per-agency staging Parquet files for this record type (see write_staging)."""
    staging_type_dir = staging_dir / record_type.name
    if not staging_type_dir.exists():
        return []
    return sorted(staging_type_dir.glob("*.parquet"))


def _merge(con, staging_files: list[Path], record_type: RecordType) -> None:
    """Row-level upsert of the staged rows into the Iceberg table via ``MERGE INTO``.

    Mirrors the dedup semantics of ``transforms.merge_staging_files``: collapse
    the staging rows to one per key (latest ``modify_date`` wins), then merge
    that against the table — updating only when the incoming row is newer and
    inserting brand-new keys. ``modify_date`` is an ISO-8601 string, so the
    lexical ``>`` comparison orders chronologically.
    """
    cols = list(record_type.schema)
    key = record_type.dedup_key

    files_sql = ", ".join(f"'{_sql_str(str(p))}'" for p in staging_files)
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
    set_clause = ", ".join(f'"{c}" = s."{c}"' for c in cols if c != key)
    insert_cols = ", ".join(f'"{c}"' for c in cols)
    insert_vals = ", ".join(f's."{c}"' for c in cols)

    con.execute(
        f"""
        MERGE INTO {_qualified(record_type)} AS t
        USING (
            SELECT {col_select}
            FROM read_parquet([{files_sql}], union_by_name=true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "{key}"
                ORDER BY modify_date DESC NULLS LAST
            ) = 1
        ) AS s
        ON t."{key}" = s."{key}"
        WHEN MATCHED AND (t.modify_date IS NULL OR s.modify_date > t.modify_date)
            THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED
            THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """
    )


def _export_parquet(con, record_type: RecordType, output_dir: Path) -> Path:
    """Write the full table back out as the public ``{name}.parquet`` snapshot.

    Reuses the published layout's sort + compression (zstd, sorted by
    ``agency_code, modify_date`` for dockets) so downstream consumers — the CLI
    ``download`` and the anonymous MCP server — see byte-for-byte the same shape
    they do today. This is what makes the "dual model" work: Iceberg is the
    system of record, public Parquet is the read mirror.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{record_type.name}.parquet"

    sort_cols = [c for c in ("agency_code", "modify_date") if c in record_type.schema]
    order_by = f"ORDER BY {', '.join(sort_cols)}" if sort_cols else ""

    con.execute(
        f"""
        COPY (SELECT * FROM {_qualified(record_type)} {order_by})
        TO '{_sql_str(str(out_file))}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        """
    )
    return out_file


def merge_and_export(staging_dir: Path, output_dir: Path, record_type: RecordType) -> Path | None:
    """Upsert staged rows into the catalog table, then export the public Parquet.

    Returns the path to the exported ``{name}.parquet`` (so the pipeline can
    publish it via the existing R2 upload), or ``None`` when there was nothing
    staged for this record type.
    """
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        logger.info("iceberg: no staging files for {}; skipping merge", record_type.name)
        return None

    con = _connect()
    try:
        _ensure_table(con, record_type)
        logger.info(
            "iceberg: MERGE {} staging file(s) into {}",
            len(staging_files), _qualified(record_type),
        )
        _merge(con, staging_files, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: {} now holds {:,} rows", record_type.name, total)
        out_file = _export_parquet(con, record_type, output_dir)
        logger.info("iceberg: exported public snapshot to {}", out_file)
        return out_file
    finally:
        con.close()

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
* :func:`merge_comments` — the comments variant. Comments are tens of millions
  of rows, so there is no monolithic ``comments.parquet`` snapshot to keep
  fresh; the catalog table *is* the read surface (the MCP server queries it
  directly when the catalog is configured). Instead of exporting the whole
  table, this MERGEs the staged rows and rebuilds the tiny
  ``comments_index.parquet`` (per-partition row counts) that the feed summary
  and agency rollups read for comment counts.

Credentials are read from the environment, alongside the existing ``R2_*`` vars:

* ``R2_CATALOG_URI``        — the Iceberg REST catalog endpoint (catalog-uri)
* ``R2_CATALOG_WAREHOUSE``  — the warehouse name
* ``R2_CATALOG_TOKEN``      — an R2 API token with R2 + data-catalog permissions
* ``R2_CATALOG_NAMESPACE``  — Iceberg namespace/schema (optional, default ``default``)
"""

import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from os import getenv
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import urlparse

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
    # `or "default"` (not getenv's default arg) so an env var set to an empty
    # string — e.g. a GitHub Actions `${{ secrets.R2_CATALOG_NAMESPACE }}` that
    # resolves to "" when the secret is unset — still falls back to "default".
    return getenv("R2_CATALOG_NAMESPACE") or "default"


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
        raise RuntimeError("R2 Data Catalog is not configured; missing env var(s): " + ", ".join(missing))

    uri = getenv("R2_CATALOG_URI", "")
    warehouse = getenv("R2_CATALOG_WAREHOUSE", "")
    token = getenv("R2_CATALOG_TOKEN", "")

    con = duckdb.connect()
    # avro is installed explicitly before iceberg: DuckDB 1.5's iceberg extension
    # pulls in `avro` to read Iceberg manifests and otherwise auto-installs it
    # lazily during LOAD, a nested install that fails in sandboxes without a
    # writable home directory. Provisioning it on the top-level path avoids that.
    # Best-effort: older DuckDB (<1.5) has no separate avro extension, so ignore
    # the error there and let iceberg use its bundled Avro path.
    try:
        con.execute("INSTALL avro; LOAD avro;")
    except duckdb.Error as avro_exc:
        logger.info("avro not separately provisioned ({}); using iceberg's bundled path", avro_exc)
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{_sql_str(token)}');")
    con.execute(f"ATTACH '{_sql_str(warehouse)}' AS {_CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{_sql_str(uri)}');")
    return con


_T = TypeVar("_T")

# A catalog operation that hits a transient R2 error is retried from a fresh
# connection. Sized like the R2 download retry (sources/r2.py) to outlast a
# short outage: on 2026-11-22 R2 returned HTTP 500 for ~7 minutes and an ETL
# batch died reading an Iceberg manifest mid-MERGE (run 44961875658).
_CATALOG_MAX_ATTEMPTS = 12
_CATALOG_MAX_BACKOFF_S = 60
# DuckDB reports the status as "... (HTTP 500)" or "... (HTTP code 502)".
_TRANSIENT_HTTP_STATUS = re.compile(r"\bHTTP (?:code )?(?:5\d\d|429)\b")


def _is_transient_catalog_error(exc: BaseException) -> bool:
    """True for a DuckDB HTTP error carrying a 5xx/429 status."""
    import duckdb

    return isinstance(exc, duckdb.HTTPException) and bool(_TRANSIENT_HTTP_STATUS.search(str(exc)))


def _with_catalog_retry(what: str, work: Callable[[Any], _T]) -> _T:
    """Run ``work(con)`` on a fresh catalog connection, retrying transient errors.

    Each attempt opens its own connection (``ATTACH`` itself can hit the outage,
    and a connection that saw an error mid-transaction is not reused). Only
    callers whose ``work`` is safe to re-run may use this; see :func:`_merge`.
    """
    for attempt in range(1, _CATALOG_MAX_ATTEMPTS + 1):
        con = None
        try:
            con = _connect()
            return work(con)
        except Exception as exc:
            if attempt == _CATALOG_MAX_ATTEMPTS or not _is_transient_catalog_error(exc):
                raise
            backoff = min(2**attempt, _CATALOG_MAX_BACKOFF_S)
            logger.warning(
                "iceberg: {} failed ({}), attempt {}/{}; retrying in {}s",
                what,
                exc,
                attempt,
                _CATALOG_MAX_ATTEMPTS,
                backoff,
            )
        finally:
            if con is not None:
                con.close()
        time.sleep(backoff)
    raise AssertionError("unreachable")  # pragma: no cover


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


@contextmanager
def _transaction(con) -> Iterator[None]:
    """Run the enclosed statements as one catalog commit, or not at all.

    Outside a transaction every ``DELETE`` / ``INSERT`` is its own Iceberg commit,
    so a job killed between the two (the ETL's 60-minute timeout, #202) leaves the
    deleted rows gone until something re-inserts them. Inside one, DuckDB sends
    both snapshots to the catalog in a single commit at ``COMMIT``; a process that
    dies first commits nothing. Verified against an Iceberg REST catalog.
    """
    con.execute("BEGIN TRANSACTION;")
    try:
        yield
    except BaseException:
        try:
            con.execute("ROLLBACK;")
        except Exception as rollback_exc:  # the original error is the one worth raising
            logger.warning("iceberg: ROLLBACK failed after an aborted write: {}", rollback_exc)
        raise
    con.execute("COMMIT;")


def _merge(con, staging_files: list[Path], record_type: RecordType) -> None:
    """Row-level upsert of the staged rows into the Iceberg table.

    Expressed as ``DELETE`` + ``INSERT`` rather than ``MERGE INTO``: the R2 Data
    Catalog is an Iceberg table, and DuckDB's iceberg engine raises
    ``NotImplementedException`` for ``MERGE INTO``/``ON CONFLICT`` — only plain
    ``DELETE``/``INSERT`` are supported (the same DML :func:`seed_comments_from_parquet`
    uses).

    Mirrors the dedup semantics of ``transforms.merge_staging_files``: collapse
    the staging rows to one per key (latest ``modify_date`` wins), keep only the
    keys whose incoming row is brand-new or strictly newer than the table's, then
    replace exactly those keys. Deleting by key (not by agency) is essential — a
    since-year-filtered run stages only a slice, so an agency-wide delete would
    drop the rows it isn't re-inserting. ``modify_date`` is an ISO-8601 string,
    so the lexical ``>`` comparison orders chronologically.

    CAVEAT — the ``DELETE`` does not reliably remove prior rows on the R2 Data
    Catalog (the same limitation that made the one-time seed duplicate rows and
    that :func:`dedupe_table` works around by never deleting). So a key that is
    *re-merged* — an existing comment whose ``modify_date`` advanced, or a key the
    redundant daily sweep re-stages — can be left behind next to its replacement,
    growing physical duplicate ``comment_id`` rows over time. Brand-new keys are
    unaffected (nothing to delete). This is why the read surface dedups on read
    (``mcp_server`` wraps the ``comments`` view in a per-``comment_id`` QUALIFY)
    and physical duplicates are reclaimed out-of-band by ``dedupe_table``; a
    delete-free incremental upsert is not possible here because the only reliable
    removal primitive on this catalog is a whole-table ``DROP`` + rebuild.

    Safe to re-run after a failure (which :func:`_with_catalog_retry` relies
    on): the DELETE + INSERT is one commit, and if that commit landed despite
    an error, the re-run's winners exclude those keys because their incoming
    ``modify_date`` is no longer strictly newer than the table's.
    """
    cols = list(record_type.schema)
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    # Temp names are per-record-type so a dockets + comments run on one
    # connection can't collide.
    staged = f"_staged_{record_type.name}"
    winners = f"_winners_{record_type.name}"

    files_sql = ", ".join(f"'{_sql_str(str(p))}'" for p in staging_files)
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
    col_list = ", ".join(f'"{c}"' for c in cols)

    # 1. Collapse staging to one row per key (latest modify_date wins).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {staged} AS
        SELECT {col_select}
        FROM read_parquet([{files_sql}], union_by_name=true)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "{key}"
            ORDER BY modify_date DESC NULLS LAST
        ) = 1;
        """
    )
    # 2. Keep only rows that should win over the table: a new key, or one whose
    #    incoming modify_date is strictly newer (matching the old MERGE guard).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {winners} AS
        SELECT s.*
        FROM {staged} s
        LEFT JOIN {tbl} t ON t."{key}" = s."{key}"
        WHERE t."{key}" IS NULL
           OR t.modify_date IS NULL
           OR s.modify_date > t.modify_date;
        """
    )
    # 3. Upsert = delete exactly the winning keys, then insert their rows, as one
    #    commit so an interrupted run can't leave the keys deleted.
    with _transaction(con):
        con.execute(f'DELETE FROM {tbl} WHERE "{key}" IN (SELECT "{key}" FROM {winners});')
        con.execute(f"INSERT INTO {tbl} ({col_list}) SELECT {col_list} FROM {winners};")

    con.execute(f"DROP TABLE IF EXISTS {staged};")
    con.execute(f"DROP TABLE IF EXISTS {winners};")


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


# Rows a partition buffers before the bucketing COPY in _export_parquet_by_agency
# flushes it. DuckDB's default (524,288) is sized for narrow rows; with comment
# text and ~180 agencies open at once that buffering alone can exhaust the runner.
_BUCKET_FLUSH_ROWS = 50_000
# Rows per batch streamed from each agency's sort into the monolith writer; also
# the monolith's row-group size, matching _export_parquet.
_MONOLITH_BATCH_ROWS = 100_000


def _export_parquet_by_agency(con, record_type: RecordType, output_dir: Path) -> Path:
    """Write ``{name}.parquet`` sorted by ``agency_code, modify_date`` without a global sort.

    Produces the same file as :func:`_export_parquet` (same columns, zstd, rows
    ordered by agency_code with NULLs last, then modify_date) but bounds memory by
    the largest agency rather than the whole table, which is what OOM'd the comments
    mirror once the table passed ~27M rows:

    1. One scan of the catalog table, bucketed to local Parquet by agency rank
       (``PARTITION_BY``). Ranks come from the same statement's ``DISTINCT``, so
       they match the rows written. The Iceberg table is unpartitioned, so scanning
       it once beats one filtered scan per agency.
    2. Bucket by bucket in rank order: sort that agency's rows by ``modify_date``
       (spilling as needed) and stream them into a single ``ParquetWriter``. Each
       bucket is deleted once appended, so local disk peaks at about twice the
       compressed table.

    Written to a temp file and renamed, so a failed export never leaves a partial
    monolith where the uploader would pick it up.
    """
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    if "agency_code" not in record_type.schema or "modify_date" not in record_type.schema:
        return _export_parquet(con, record_type, output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{record_type.name}.parquet"
    tmp_file = out_file.with_suffix(".tmp.parquet")
    buckets_dir = output_dir / f".{record_type.name}_by_agency"
    shutil.rmtree(buckets_dir, ignore_errors=True)

    tbl = _qualified(record_type)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)
    con.execute(f"SET partitioned_write_flush_threshold={_BUCKET_FLUSH_ROWS}")
    try:
        # NULL agency_code ranks last, matching ORDER BY's default NULLS LAST.
        con.execute(
            f"""
            COPY (
                WITH ranks AS (
                    SELECT agency_code,
                           row_number() OVER (ORDER BY agency_code NULLS LAST) AS _bucket
                    FROM (SELECT DISTINCT agency_code FROM {tbl})
                )
                SELECT t.*, r._bucket
                FROM {tbl} t
                JOIN ranks r ON t.agency_code IS NOT DISTINCT FROM r.agency_code
            ) TO '{_sql_str(str(buckets_dir))}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (_bucket), ROW_GROUP_SIZE 10000);
            """
        )
        buckets = sorted(
            (p for p in buckets_dir.glob("_bucket=*") if p.is_dir()),
            key=lambda p: int(p.name.split("=", 1)[1]),
        )
        if not buckets:
            # Empty table: nothing to bucket, but still publish a schema-correct file.
            return _export_parquet(con, record_type, output_dir)

        logger.info("iceberg: writing {} sorted by agency across {} bucket(s)", out_file.name, len(buckets))
        writer = None
        try:
            for bucket in buckets:
                glob = _sql_str(str(bucket / "*.parquet"))
                reader = con.execute(
                    f"SELECT {col_list} FROM read_parquet('{glob}', hive_partitioning=false) ORDER BY modify_date"
                ).fetch_record_batch(_MONOLITH_BATCH_ROWS)
                for batch in reader:
                    if writer is None:
                        writer = pq.ParquetWriter(tmp_file, batch.schema, compression="zstd")
                    writer.write_table(pa.Table.from_batches([batch]), row_group_size=_MONOLITH_BATCH_ROWS)
                shutil.rmtree(bucket)
        finally:
            if writer is not None:
                writer.close()
        tmp_file.replace(out_file)
        return out_file
    finally:
        con.execute("RESET partitioned_write_flush_threshold")
        shutil.rmtree(buckets_dir, ignore_errors=True)
        tmp_file.unlink(missing_ok=True)


def _build_comments_index(con, record_type: RecordType, output_dir: Path) -> Path:
    """Rebuild ``comments_index.parquet`` from the catalog comments table.

    The index is the small per-``(agency_code, docket_id, year, month)`` row-count
    artifact that ``build_feed_summary`` / ``build_agency_rollups`` read instead
    of scanning the full comments table. With comments living in the catalog
    there is no partitioned ``comments/`` tree to count, so the index is derived
    straight from the table — keeping the (agency_code, docket_id, year, month,
    row_count) schema of the retired partition-tree index. It counts catalog
    rows only; it is not a listing of files on R2 (#201).

    ``year`` / ``month`` come from ``posted_date`` to match the partitioning the
    legacy path used; ``docket_id`` is trimmed of stray quotes for the same
    reason. Written atomically via a temp file so a crashed rebuild can't leave a
    half-written index in place.
    """
    index_file = output_dir / "comments_index.parquet"
    tmp_file = index_file.with_suffix(".tmp.parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT
                agency_code,
                TRIM(docket_id, '"') AS docket_id,
                EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS year,
                EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::BIGINT AS month,
                CAST(COUNT(*) AS BIGINT) AS row_count
            FROM {_qualified(record_type)}
            WHERE posted_date IS NOT NULL
              AND agency_code IS NOT NULL
              AND docket_id IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ) TO '{_sql_str(str(tmp_file))}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    tmp_file.replace(index_file)
    return index_file


def seed_comments_from_parquet(
    con, source_glob: str, record_type: RecordType, replace_agency: str | None = None
) -> int:
    """Bulk-load published comment Parquet into the catalog table; return its count.

    One-time cutover helper: the partitioned ``comments/`` tree on R2 is already
    current, so this copies it straight into the catalog ``comments`` table
    instead of re-ingesting from Mirrulations. ``source_glob`` is read with
    ``hive_partitioning=false`` because the partition files already carry
    ``agency_code`` / ``docket_id`` as columns (year/month live only in the path
    and are not table columns).

    When ``replace_agency`` is given, all existing rows for that ``agency_code``
    are deleted before the insert. The loader runs one agency at a time, so this
    makes each agency load idempotent — re-running (after a timeout, or over an
    already-seeded table) replaces that agency's rows instead of duplicating
    them, since the plain ``INSERT`` does no dedup.

    Columns absent from every file in the glob (an older partition written before
    a column was added) are inserted as ``NULL``, so a mixed-vintage tree loads
    cleanly. The connection + any S3 secret are set up by the caller
    so this stays testable against a local catalog and local files.
    """
    columns = list(record_type.schema)
    esc = _sql_str(source_glob)
    present = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)"
        ).fetchall()
    }
    projection = ", ".join(
        f'CAST("{c}" AS VARCHAR) AS "{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"' for c in columns
    )
    col_list = ", ".join(f'"{c}"' for c in columns)
    # One commit: an agency-wide DELETE that lands without its INSERT would leave
    # the agency empty in the catalog.
    with _transaction(con):
        if replace_agency is not None:
            con.execute(f"DELETE FROM {_qualified(record_type)} WHERE agency_code = '{_sql_str(replace_agency)}';")
        con.execute(
            f"""
            INSERT INTO {_qualified(record_type)} ({col_list})
            SELECT {projection}
            FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false);
            """
        )
    return con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]


_REQUIRED_S3_ENV = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")


def create_s3_secret(con) -> None:
    """Register R2's S3 API as a DuckDB secret so bucket objects can be read/globbed.

    Reads over the public HTTPS URL can't list directories (the whole reason the
    catalog cutover exists), so source Parquet is read via the S3 API, which does
    support listing/globbing. Shared by the catalog seeding scripts.
    """
    missing = [v for v in _REQUIRED_S3_ENV if not getenv(v)]
    if missing:
        raise RuntimeError("Missing R2 S3 env var(s): " + ", ".join(missing))
    # R2_ENDPOINT is a full URL for boto3; DuckDB wants the bare host + USE_SSL.
    endpoint = getenv("R2_ENDPOINT", "")
    host = urlparse(endpoint).netloc or endpoint.replace("https://", "").replace("http://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2_s3_secret (
            TYPE S3,
            KEY_ID '{_sql_str(getenv("R2_ACCESS_KEY_ID", ""))}',
            SECRET '{_sql_str(getenv("R2_SECRET_ACCESS_KEY", ""))}',
            ENDPOINT '{_sql_str(host)}',
            URL_STYLE 'path',
            USE_SSL true,
            REGION 'auto'
        );
        """
    )


def backfill_missing_from_parquet(con, source_uri: str, record_type: RecordType) -> tuple[int, int]:
    """Insert published-Parquet rows the catalog table is missing; return ``(inserted, total)``.

    The cutover counterpart to :func:`seed_comments_from_parquet`, for a table
    that was routed through the catalog without ever being seeded from the
    Parquet that preceded it. ``dockets`` was exactly that: the Iceberg path
    landed while ``comments`` got a real seed and ``dockets`` got none, so the
    catalog table only ever accumulated rows staged *after* the cutover (~5.4k of
    ~276k). Every run then exported that sliver as the "full" public snapshot and
    the shrink guard refused it, freezing the published table for eight weeks.

    Rows are matched on ``record_type.dedup_key`` and only source keys **absent**
    from the table are inserted. That ordering matters: the catalog's rows are
    *newer* than the frozen snapshot (they are the post-cutover updates), so
    overwriting them from Parquet would silently roll back eight weeks of
    ingested changes. Backfilling only what is missing keeps the newer row
    authoritative in every case, and makes a re-run a no-op rather than a
    duplicate load — this does a plain ``INSERT``, with no dedup of its own.

    Expressed as an anti-join rather than ``NOT IN`` because a single NULL key on
    either side makes ``NOT IN`` return NULL for every row and quietly insert
    nothing. The source is de-duplicated on its own key (latest ``modify_date``
    wins) so a snapshot with repeats can't fan out into duplicate rows.
    """
    key = record_type.dedup_key
    columns = list(record_type.schema)
    esc = _sql_str(source_uri)
    qualified = _qualified(record_type)

    present = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)"
        ).fetchall()
    }
    if key not in present:
        raise RuntimeError(f"Source {source_uri} has no '{key}' column; refusing to backfill {record_type.name}")

    projection = ", ".join(
        f'CAST("{c}" AS VARCHAR) AS "{c}"' if c in present else f'CAST(NULL AS VARCHAR) AS "{c}"' for c in columns
    )
    col_list = ", ".join(f'"{c}"' for c in columns)

    before = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    con.execute(
        f"""
        INSERT INTO {qualified} ({col_list})
        WITH src AS (
            SELECT {projection}
            FROM read_parquet('{esc}', union_by_name=true, hive_partitioning=false)
            QUALIFY row_number() OVER (PARTITION BY "{key}" ORDER BY "modify_date" DESC NULLS LAST) = 1
        )
        SELECT {col_list} FROM src
        WHERE src."{key}" IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {qualified} t WHERE t."{key}" = src."{key}");
        """
    )
    total = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
    return total - before, total


def upsert_comment_text(con, record_type: RecordType, agency: str, updates) -> None:
    """Upsert filled ``text_content`` / ``text_extraction_status`` for one agency.

    Shared helper for the durable text-fill paths (derived-data backfill and PDF
    enrichment). ``updates`` is a polars DataFrame with columns
    ``comment_id, _new_text, _new_status``; every row whose ``comment_id`` matches
    the agency's rows gets ``text_content`` / ``text_extraction_status`` refreshed
    (``COALESCE`` keeps the existing value when the incoming column is NULL). The
    upsert is scoped to a single ``agency_code`` so it never touches the whole
    tens-of-millions-row table, and is expressed as DELETE+INSERT because DuckDB's
    Iceberg engine has no ``MERGE INTO`` (see :func:`_merge`). No-ops on an empty
    frame; the caller is expected to have handled that case already.

    CRITICAL — self-contained temp table: the INSERT reads from an independent
    ``_uct_replacement`` temp table (a full snapshot of the affected rows with the
    two columns overridden in place), **never** from a projection over the live
    catalog table. An earlier version projected the overrides straight off
    ``{tbl} t JOIN updates`` in the INSERT's SELECT; on the R2 Data Catalog that
    dropped column writes — ``text_content`` landed but ``text_extraction_status``
    came back NULL, so incremental re-runs kept re-selecting already-filled rows.
    Snapshotting into a self-contained temp table removes the live-table reference
    from the write path and fixes it (see PR #117). Plain DuckDB does not reproduce
    the catalog behavior, so this invariant is verified by a scoped catalog run,
    not the unit test — keep the ``_uct_replacement`` indirection intact.
    """
    if updates.is_empty():
        return

    tbl = _qualified(record_type)
    ag = _sql_str(agency)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)

    con.register("_uct_updates_src", updates.to_arrow())
    try:
        con.execute("CREATE OR REPLACE TEMP TABLE _uct_updates AS SELECT * FROM _uct_updates_src;")
    finally:
        con.unregister("_uct_updates_src")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _uct_replacement AS
        SELECT {col_list} FROM {tbl}
        WHERE agency_code = '{ag}' AND comment_id IN (SELECT comment_id FROM _uct_updates);
        """
    )
    con.execute(
        """
        UPDATE _uct_replacement AS r
        SET text_content = COALESCE(u._new_text, r.text_content),
            text_extraction_status = COALESCE(u._new_status, r.text_extraction_status)
        FROM _uct_updates u
        WHERE r.comment_id = u.comment_id;
        """
    )
    with _transaction(con):
        con.execute(
            f"DELETE FROM {tbl} WHERE agency_code = '{ag}' AND comment_id IN (SELECT comment_id FROM _uct_replacement);"
        )
        con.execute(f"INSERT INTO {tbl} ({col_list}) SELECT {col_list} FROM _uct_replacement;")
    con.execute("DROP TABLE IF EXISTS _uct_updates;")
    con.execute("DROP TABLE IF EXISTS _uct_replacement;")


def merge_comments(staging_dir: Path, output_dir: Path, record_type: RecordType) -> Path | None:
    """Upsert staged comments into the catalog, then rebuild the comments index.

    Unlike :func:`merge_and_export`, this does **not** write a monolithic
    ``comments.parquet`` — the comments table is too large to re-export on every
    run, and the catalog is the read surface. Returns the path to the refreshed
    ``comments_index.parquet`` (which the pipeline publishes to R2), or ``None``
    when there was nothing staged.
    """
    staging_files = _staging_files(staging_dir, record_type)
    if not staging_files:
        logger.info("iceberg: no staging files for {}; skipping merge", record_type.name)
        return None

    def work(con) -> Path:
        _ensure_table(con, record_type)
        logger.info(
            "iceberg: MERGE {} staging file(s) into {}",
            len(staging_files),
            _qualified(record_type),
        )
        _merge(con, staging_files, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: {} now holds {:,} rows", record_type.name, total)
        index_file = _build_comments_index(con, record_type, output_dir)
        logger.info("iceberg: rebuilt comments index at {}", index_file)
        return index_file

    return _with_catalog_retry(f"merge {record_type.name}", work)


# DuckDB settings for the full-table mirror export. A global sort of the comments
# table buffers whole rows (comment text included) per thread, and at ~27M rows a
# 6GB cap with the default 4 threads OOM'd the runner most nights from 2026-11-21
# on ("5.5 GiB/5.5 GiB used"). The export now sorts one agency at a time
# (_export_parquet_by_agency); these settings add headroom on top: fewer threads
# shrink the per-thread sort state so more of it spills, and the higher cap uses a
# 16GB ubuntu-latest runner. Both are overridable for a bigger or smaller host.
_EXPORT_MEMORY_LIMIT = "10GB"
_EXPORT_THREADS = 2


def _tune_for_export(con) -> None:
    """Apply the memory/thread settings for a full-table export.

    Disables insertion-order preservation so the sort can spill to disk, and caps
    memory and threads (``MIRROR_DUCKDB_MEMORY_LIMIT`` / ``MIRROR_DUCKDB_THREADS``
    override the defaults). ``MIRROR_DUCKDB_TEMP_DIR`` points the spill at another
    disk; otherwise DuckDB's default temp directory is used (this never runs on
    the read-only serverless host).
    """
    memory_limit = getenv("MIRROR_DUCKDB_MEMORY_LIMIT") or _EXPORT_MEMORY_LIMIT
    threads = int(getenv("MIRROR_DUCKDB_THREADS") or _EXPORT_THREADS)
    temp_dir = getenv("MIRROR_DUCKDB_TEMP_DIR")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{_sql_str(memory_limit)}'")
    con.execute(f"SET threads={threads}")
    if temp_dir:
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{_sql_str(temp_dir)}'")
    logger.info(
        "iceberg: export tuned to memory_limit={} threads={} temp_directory={}",
        memory_limit,
        threads,
        con.execute("SELECT current_setting('temp_directory')").fetchone()[0],
    )


def export_public_comments(output_dir: Path, record_type: RecordType) -> dict[str, Path]:
    """Rebuild the public comments read-mirror from the catalog.

    The browser UI can't reach the credentialed catalog, so it reads comments as
    public Parquet on R2. :func:`merge_comments` only republishes the tiny index,
    so the public monolith + per-agency tree the UI reads otherwise go stale. This
    regenerates that mirror from the catalog — the write-side system of record —
    restoring the dual model for comments:

    * ``comments.parquet``       — the flat monolith the UI full-scans
    * ``comments_index.parquet`` — per-partition row counts

    The caller derives the per-agency tree the UI reads for scoped queries
    (``comments/agency/agency_code={X}/part-0.parquet``) from the monolith via
    :func:`spicy_regs.transforms.partition_comments`. Returns the written paths
    keyed ``"comments"`` and ``"index"``.
    """
    con = _connect()
    try:
        _tune_for_export(con)
        _ensure_table(con, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: exporting {:,} catalog rows to the public comments mirror", total)
        monolith = _export_parquet_by_agency(con, record_type, output_dir)
        index_file = _build_comments_index(con, record_type, output_dir)
        logger.info("iceberg: wrote public monolith {} and index {}", monolith, index_file)
        return {"comments": monolith, "index": index_file}
    finally:
        con.close()


def audit_duplicates(con, record_type: RecordType) -> list[tuple[str, int, int]]:
    """Per-agency (agency_code, rows, distinct_keys) where rows exceed distinct keys.

    A read-only duplication report over the catalog table: any agency whose row
    count is above its distinct ``dedup_key`` count carries duplicate rows. Sorted
    by the number of duplicate rows, worst first. Empty when the table is clean.
    """
    key = record_type.dedup_key
    tbl = _qualified(record_type)
    rows = con.execute(
        f"""
        SELECT agency_code,
               count(*) AS rows,
               count(DISTINCT "{key}") AS distinct_keys
        FROM {tbl}
        WHERE agency_code IS NOT NULL
        GROUP BY agency_code
        HAVING count(*) > count(DISTINCT "{key}")
        ORDER BY count(*) - count(DISTINCT "{key}") DESC
        """
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def dedupe_table(con, record_type: RecordType) -> tuple[int, int]:
    """Collapse the catalog table to one row per ``dedup_key`` (latest modify_date).

    Builds a fresh deduped sibling table one agency at a time, then replaces the
    live table with it. Three constraints drive the shape:

    * Never ``DELETE``. The historical duplication came from loads whose ``DELETE``
      didn't remove prior rows on the R2 Data Catalog, so a delete-based cleanup
      could double the problem.
    * Never touch tens of millions of rows in one statement. A global
      ``ROW_NUMBER() OVER (PARTITION BY key)`` OOMs the runner on the read side,
      and a single ``CREATE OR REPLACE ... AS SELECT`` of the whole table OOMs it
      on the Iceberg *write* side. The build works per agency, and — because even
      one large agency's dedup window buffers whole rows (comment text included)
      and overflows the runner's memory limit despite on-disk spill — each agency
      is further split into hash buckets of the dedup key sized to
      ``DEDUP_ROWS_PER_BATCH`` rows. A key hashes to exactly one bucket and
      duplicates only ever share a key, so per-bucket dedup equals per-agency
      equals global dedup here; small agencies stay a single write.
    * Never ``ALTER TABLE RENAME``. DuckDB's Iceberg REST integration does not
      implement it (``NotImplementedException: Alter Schema Entry``), so the swap
      replaces the live table by ``DROP`` + ``CREATE`` + per-agency ``INSERT``
      from the sibling — all operations this catalog supports. The sibling is the
      durable copy: it is dropped only after the rebuilt table's row count is
      verified, so an interruption mid-swap loses nothing.

    A swap marker table (``{name}_dedup_swap``) is created once the sibling is
    fully built and before the live table is dropped, and removed only after the
    rebuilt row count is verified. Its presence is what tells a re-run that the
    sibling is complete and the live table may be partial, so the re-run resumes
    the swap from the sibling. Without it a half-refilled live table looks like a
    healthy one: on 2026-09-13 a re-run rebuilt from a live table holding 103 of
    179 agencies and dropped the sibling that still held all of them (#197).

    Returns ``(rows_before, rows_after)``; ``rows_after`` equals the number of
    distinct keys when the rebuild succeeds.
    """
    key = record_type.dedup_key
    name = record_type.name
    tbl = _qualified(record_type)
    dedup_tbl = f'{_schema_ref()}."{name}_dedup"'
    marker_tbl = f'{_schema_ref()}."{name}_dedup_swap"'
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in record_type.schema)
    col_list = ", ".join(f'"{c}"' for c in record_type.schema)

    def _exists(ident: str) -> bool:
        try:
            con.execute(f"SELECT 1 FROM {ident} LIMIT 1")
            return True
        except Exception:
            return False

    def _agencies(ident: str) -> set:
        return {r[0] for r in con.execute(f"SELECT DISTINCT agency_code FROM {ident}").fetchall()}

    def _where(agency) -> str:
        return "agency_code IS NULL" if agency is None else f"agency_code = '{_sql_str(agency)}'"

    def _copy_agency(agency, expected_rows: int) -> None:
        # One agency's INSERT from the sibling, retried on a transient catalog
        # error (the 2026-09-06 swap died on a single 502). Each INSERT is one
        # Iceberg commit, so after a failure the agency holds either none of its
        # rows or all of them; re-check before retrying so a commit that landed
        # despite the error isn't inserted twice.
        attempts = int(getenv("DEDUP_SWAP_RETRIES", "3"))
        for attempt in range(1, attempts + 1):
            try:
                con.execute(
                    f"INSERT INTO {tbl} ({col_list}) SELECT {col_list} FROM {dedup_tbl} WHERE {_where(agency)};"
                )
                return
            except Exception as e:
                have = con.execute(f"SELECT count(*) FROM {tbl} WHERE {_where(agency)}").fetchone()[0]
                if have == expected_rows:
                    return
                if have != 0 or attempt == attempts:
                    raise
                logger.warning("iceberg: swap INSERT for {} failed ({}); retry {}/{}", agency, e, attempt, attempts - 1)
                time.sleep(2**attempt)

    def _replace_live_from_sibling() -> int:
        # Swap without RENAME: rebuild the live table from the deduped sibling
        # using DROP/CREATE/INSERT (per-agency, so no whole-table statement). The
        # sibling still holds every row throughout, so this is safe to re-run if
        # interrupted; the marker and then the sibling are dropped only once the
        # rebuilt row count matches.
        expected = con.execute(f"SELECT count(*) FROM {dedup_tbl}").fetchone()[0]
        per_agency = con.execute(f"SELECT agency_code, count(*) FROM {dedup_tbl} GROUP BY agency_code").fetchall()
        if not _exists(marker_tbl):
            con.execute(f"CREATE TABLE IF NOT EXISTS {marker_tbl} (expected_rows BIGINT);")
            con.execute(f"INSERT INTO {marker_tbl} VALUES ({int(expected)});")
        con.execute(f"DROP TABLE IF EXISTS {tbl};")
        con.execute(f"CREATE TABLE {tbl} ({col_defs});")
        for agency, n in per_agency:
            _copy_agency(agency, n)
        rebuilt = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        if rebuilt != expected:
            raise RuntimeError(
                f"dedupe rebuild mismatch: {tbl} has {rebuilt:,} rows, expected {expected:,}; "
                f"{dedup_tbl} left in place as the safe copy — re-run to retry the swap"
            )
        # Marker first: a crash between the two drops leaves a sibling with no
        # marker, which the next run treats as a stale build and rebuilds from the
        # (now complete) live table.
        con.execute(f"DROP TABLE IF EXISTS {marker_tbl};")
        con.execute(f"DROP TABLE IF EXISTS {dedup_tbl};")
        return rebuilt

    live_exists, sibling_exists = _exists(tbl), _exists(dedup_tbl)

    # Resume an interrupted swap. The marker means the sibling was complete when
    # the swap began, so the live table (whether dropped or partly refilled) is
    # not to be trusted. A missing live table with a sibling also means the swap
    # had started (the DROP only follows a finished build); that covers a swap
    # interrupted before markers existed.
    if sibling_exists and (_exists(marker_tbl) or not live_exists):
        logger.warning("iceberg: resuming interrupted dedupe swap of {} from {}", tbl, dedup_tbl)
        after = _replace_live_from_sibling()
        return after, after

    # A sibling with no marker is normally a build that died before the swap, and
    # is safe to discard: it was built from the live table, so every agency in it
    # is also in the live table. An agency only in the sibling means the live
    # table lost rows the sibling still holds — a pre-marker interrupted swap, as
    # on 2026-09-06. Discarding the sibling then destroys the only full copy.
    if sibling_exists and live_exists:
        lost = _agencies(dedup_tbl) - _agencies(tbl)
        if lost:
            raise RuntimeError(
                f"{dedup_tbl} holds {len(lost)} agency(ies) missing from {tbl} "
                f"(e.g. {sorted(str(a) for a in lost)[:10]}); the live table looks like a "
                f"partly-refilled interrupted swap. Refusing to discard the sibling — restore "
                f"the live table from it before re-running."
            )

    before = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]

    # Build the deduped sibling fresh (discarding any partial one from an aborted
    # run), one agency at a time.
    con.execute(f"DROP TABLE IF EXISTS {dedup_tbl};")
    con.execute(f"CREATE TABLE {dedup_tbl} ({col_defs});")

    agencies = [r[0] for r in con.execute(f"SELECT DISTINCT agency_code FROM {tbl}").fetchall()]
    counts = dict(con.execute(f"SELECT agency_code, count(*) FROM {tbl} GROUP BY agency_code").fetchall())
    # A single agency's dedup window buffers whole rows (comment text included), so
    # the largest agencies overflow the runner's memory limit even with spilling on.
    # Split each agency into ceil(rows / DEDUP_ROWS_PER_BATCH) hash buckets of the
    # dedup key to bound each window's input; a key hashes to one bucket and dupes
    # share a key, so this doesn't change the result. Small agencies -> 1 bucket.
    rows_per_batch = int(getenv("DEDUP_ROWS_PER_BATCH", "100000"))
    logger.info("iceberg: rebuilding {} deduplicated across {} agency bucket(s)", name, len(agencies))
    for agency in agencies:
        where = "agency_code IS NULL" if agency is None else f"agency_code = '{_sql_str(agency)}'"
        n_buckets = max(1, -(-counts.get(agency, 0) // rows_per_batch))  # ceil division
        for bucket in range(n_buckets):
            # hash() is UBIGINT, so the modulo is non-negative; skip the filter
            # entirely for single-bucket agencies to keep their write unchanged.
            bucket_filter = "" if n_buckets == 1 else f' AND hash("{key}") % {n_buckets} = {bucket}'
            con.execute(
                f"""
                INSERT INTO {dedup_tbl} ({col_list})
                SELECT {col_list}
                FROM {tbl}
                WHERE {where}{bucket_filter}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY "{key}"
                    ORDER BY modify_date DESC NULLS LAST
                ) = 1;
                """
            )

    after = con.execute(f"SELECT count(*) FROM {dedup_tbl}").fetchone()[0]

    # Replace the live table with the deduped sibling (no RENAME — see docstring).
    logger.info("iceberg: swapping deduped {} into place ({:,} -> {:,} rows)", name, before, after)
    _replace_live_from_sibling()
    return before, after


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

    def work(con) -> Path:
        _ensure_table(con, record_type)
        logger.info(
            "iceberg: MERGE {} staging file(s) into {}",
            len(staging_files),
            _qualified(record_type),
        )
        _merge(con, staging_files, record_type)
        total = con.execute(f"SELECT count(*) FROM {_qualified(record_type)}").fetchone()[0]
        logger.info("iceberg: {} now holds {:,} rows", record_type.name, total)
        out_file = _export_parquet(con, record_type, output_dir)
        logger.info("iceberg: exported public snapshot to {}", out_file)
        return out_file

    return _with_catalog_retry(f"merge {record_type.name}", work)

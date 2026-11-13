"""Backfill ``documents.subtype`` onto the already-published ``documents.parquet``.

#218 added ``subtype`` to the documents schema, but only documents ingested
after that change get it: the manifest marks every older document JSON as
processed, so incremental runs never re-read them. This one-off (re-runnable)
backfill reads only ``(document_id, subtype)`` from those JSONs and
left-joins it into the published file. Every other column and every row stays
exactly as published (#221).

Two phases, kept separate so the long, read-only one never holds the
catalog-write lock the ETL waits on:

* **harvest**: picks the published documents whose ``subtype`` is NULL, finds
  their raw JSON keys in the published ``manifest.parquet`` (the manifest is
  the list of keys the ETL actually ingested, so no S3 listing and no guessing
  at the agency directory, which differs from the docket prefix for
  ``*_FRDOC_*`` dockets), GETs each object anonymously from Mirrulations S3, and
  writes ``(document_id, subtype, modify_date)`` part files. Each part is a
  checkpoint: a re-run skips document IDs that already have a part row, so an
  interrupted harvest resumes, and keys that failed transiently are retried.
* **apply**: fetches a fresh ``documents.parquet``, overlays the harvested
  subtypes with ``coalesce(published, harvested)``, rewrites the file with the
  same layout ``merge_staging_files`` produces (``ORDER BY docket_id``, ZSTD,
  100k-row groups), checks that only ``subtype`` changed, and uploads it once
  through :func:`spicy_regs.sources.r2.upload_file`, which keeps the shrink
  guard.

Why not ``run-pipeline --full-refresh`` or a re-merge: see #221. In short, a
full refresh rebuilds the manifest and documents file from one batch and drops
``text_content``, and a re-merge breaks ``modify_date`` ties arbitrarily.

Usage::

    uv run backfill-document-subtype harvest --document-types "Proposed Rule,Rule,Notice"
    uv run backfill-document-subtype apply            # writes locally, no upload
    uv run backfill-document-subtype apply --upload   # publishes to R2
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from typing import Annotated, Any

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.sources import mirrulations, r2

DOCUMENTS_KEY = "documents.parquet"
MANIFEST_KEY = "manifest.parquet"

# GETs are tiny and I/O-bound; ~2M of them finish far sooner with a wider pool
# than the ETL's per-agency default. The S3 resource's connection pool is sized
# to match (see ``mirrulations.s3_resource``).
DEFAULT_WORKERS = 64

# Rows per checkpoint part. Each part is written only after its chunk is fully
# downloaded, so an interruption loses at most one chunk of work.
DEFAULT_CHUNK_SIZE = 50_000

PART_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("subtype", pa.string()),
        ("modify_date", pa.string()),
    ]
)


def _sql_str(value: str | Path) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _parts_glob(work_dir: Path) -> Path:
    return work_dir / "parts" / "*.parquet"


def _existing_parts(work_dir: Path) -> list[Path]:
    return sorted((work_dir / "parts").glob("*.parquet"))


# --- harvest ---------------------------------------------------------------


def select_candidate_keys(
    documents_path: Path,
    manifest_path: Path,
    *,
    harvested_dir: Path | None = None,
    document_types: list[str] | None = None,
    agencies: list[str] | None = None,
    limit: int | None = None,
) -> list[str]:
    """Return the raw document JSON keys whose published row has a NULL subtype.

    A manifest key ``.../text-{docket}/documents/{document_id}.json`` joins to
    the published row by the filename stem. The ``_content.htm`` renditions and
    other non-JSON siblings are filtered out by the pattern. Document IDs that
    already have a row in ``harvested_dir``'s parts are skipped, which is what
    makes a re-run resume.
    """
    doc_cols = _parquet_columns(documents_path)
    # A file written before #218 has no subtype column at all; every row is a candidate.
    null_subtype = "d.subtype IS NULL" if "subtype" in doc_cols else "TRUE"

    filters = [null_subtype]
    if document_types:
        filters.append(f"d.document_type IN ({', '.join(_sql_str(t) for t in document_types)})")
    if agencies:
        filters.append(f"d.agency_code IN ({', '.join(_sql_str(a) for a in agencies)})")

    done_cte = ""
    if harvested_dir is not None and _existing_parts(harvested_dir):
        done_cte = f", done AS (SELECT DISTINCT document_id FROM read_parquet({_sql_str(_parts_glob(harvested_dir))}))"
        filters.append("k.document_id NOT IN (SELECT document_id FROM done)")

    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    WITH keys AS (
        SELECT key, regexp_extract(key, '/documents/([^/]+)\\.json$', 1) AS document_id
        FROM read_parquet({_sql_str(manifest_path)})
        WHERE key LIKE '%/text-%/documents/%.json'
    ){done_cte}
    SELECT k.key
    FROM keys k
    JOIN read_parquet({_sql_str(documents_path)}) d ON d.document_id = k.document_id
    WHERE {" AND ".join(filters)}
    ORDER BY k.key
    {limit_clause}
    """
    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order=false")
        return [row[0] for row in con.execute(query).fetchall()]
    finally:
        con.close()


def subtype_row(payload: dict) -> dict[str, Any] | None:
    """Pull ``(document_id, subtype, modify_date)`` out of one raw document JSON.

    Uses the same fields ``_extract_document`` does, so a backfilled value is
    exactly what a fresh ingest would have produced.
    """
    data = payload.get("data") or {}
    document_id = data.get("id")
    if not document_id:
        return None
    attrs = data.get("attributes") or {}
    return {
        "document_id": document_id,
        "subtype": attrs.get("subtype"),
        "modify_date": attrs.get("modifyDate"),
    }


@dataclass
class HarvestResult:
    candidates: int
    harvested: int
    with_subtype: int
    transient_failures: int
    parse_failures: int


def _next_part_index(work_dir: Path) -> int:
    existing = _existing_parts(work_dir)
    if not existing:
        return 0
    return max(int(p.stem.split("-")[-1]) for p in existing) + 1


def harvest_keys(
    keys: list[str],
    work_dir: Path,
    *,
    s3_resource: Any,
    bucket: str = mirrulations.BUCKET,
    workers: int = DEFAULT_WORKERS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> HarvestResult:
    """Download ``keys`` in chunks and write one checkpoint part per chunk.

    Every successfully parsed document is written, including those whose
    subtype is NULL, so the resume filter in :func:`select_candidate_keys`
    doesn't fetch them again. Transient failures get one in-run retry; whatever
    still fails is left out of the parts and is picked up by the next run.
    """
    parts_dir = work_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    part_index = _next_part_index(work_dir)

    harvested = with_subtype = 0
    failures = mirrulations.DownloadFailures()
    total = len(keys)
    for start in range(0, total, chunk_size):
        chunk = keys[start : start + chunk_size]
        label = f"subtype {start + len(chunk)}/{total}"
        chunk_failures = mirrulations.DownloadFailures()
        payloads = list(
            mirrulations.download_keys(s3_resource, bucket, chunk, workers, label=label, failures=chunk_failures)
        )
        if chunk_failures.transient:
            retry = mirrulations.DownloadFailures()
            payloads.extend(
                mirrulations.download_keys(
                    s3_resource, bucket, list(chunk_failures.transient), workers, label=f"{label} retry", failures=retry
                )
            )
            chunk_failures.transient = retry.transient
            chunk_failures.parse.extend(retry.parse)

        rows = [r for r in (subtype_row(p) for p in payloads) if r is not None]
        if rows:
            table = pa.Table.from_pylist(rows, schema=PART_SCHEMA)
            pq.write_table(table, parts_dir / f"part-{part_index:05d}.parquet", compression="zstd")
            part_index += 1
        harvested += len(rows)
        with_subtype += sum(1 for r in rows if r["subtype"] is not None)
        failures.transient.extend(chunk_failures.transient)
        failures.parse.extend(chunk_failures.parse)
        logger.info(
            "{}: {:,} harvested so far, {:,} with a subtype",
            label,
            harvested,
            with_subtype,
        )

    return HarvestResult(
        candidates=total,
        harvested=harvested,
        with_subtype=with_subtype,
        transient_failures=len(failures.transient),
        parse_failures=len(failures.parse),
    )


# --- apply -----------------------------------------------------------------


@dataclass
class ApplyResult:
    rows: int
    filled: int
    subtype_before: int
    subtype_after: int


def apply_subtypes(documents_path: Path, work_dir: Path, output_path: Path) -> ApplyResult:
    """Write ``documents_path`` with harvested subtypes overlaid to ``output_path``.

    Only NULL subtypes are filled (``coalesce(published, harvested)``), so a
    value the ETL has written since the harvest is never overwritten. When a
    document has more than one harvested JSON, the one with the latest
    ``modifyDate`` wins, mirroring the merge's dedup rule.

    Raises if anything other than ``subtype`` differs between the input and
    output: row count, the set of document IDs, or a checksum over every
    other column.
    """
    parts = _existing_parts(work_dir)
    if not parts:
        raise FileNotFoundError(f"No harvested parts under {work_dir / 'parts'}; run harvest first")

    columns = _parquet_columns(documents_path)
    other_columns = [c for c in columns if c != "subtype"]
    has_subtype = "subtype" in columns
    docs = f"read_parquet({_sql_str(documents_path)})"

    # Keep the published column order; append subtype if the file predates it.
    if has_subtype:
        select = "d.* REPLACE (coalesce(d.subtype, s.subtype) AS subtype)"
    else:
        select = "d.*, CAST(s.subtype AS VARCHAR) AS subtype"
    sort_clause = "ORDER BY d.docket_id" if "docket_id" in columns else ""

    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order=false")
        spill_dir = output_path.parent / ".duckdb_tmp"
        spill_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={_sql_str(spill_dir)}")

        con.execute(
            f"""
            CREATE TEMP TABLE subtypes AS
            SELECT document_id, subtype
            FROM (
                SELECT document_id, arg_max(subtype, coalesce(modify_date, '')) AS subtype
                FROM read_parquet({_sql_str(_parts_glob(work_dir))})
                GROUP BY document_id
            )
            WHERE subtype IS NOT NULL
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT {select}
                FROM {docs} d
                LEFT JOIN subtypes s ON s.document_id = d.document_id
                {sort_clause}
            ) TO {_sql_str(output_path)}
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """
        )

        def fingerprint(source: str) -> tuple:
            cols = ", ".join(f'"{c}"' for c in other_columns)
            subtype_count = "count(subtype)" if source != docs or has_subtype else "0"
            row = con.execute(
                f"""
                SELECT count(*), count(DISTINCT document_id),
                       sum(hash({cols})::HUGEINT), {subtype_count}
                FROM {source}
                """
            ).fetchone()
            # An aggregate with no GROUP BY always returns exactly one row.
            assert row is not None
            return row

        before = fingerprint(docs)
        after = fingerprint(f"read_parquet({_sql_str(output_path)})")
    finally:
        con.close()

    if before[:3] != after[:3]:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(
            "subtype backfill changed more than subtype: "
            f"(rows, distinct ids, checksum) before={before[:3]} after={after[:3]}"
        )

    result = ApplyResult(rows=after[0], filled=after[3] - before[3], subtype_before=before[3], subtype_after=after[3])
    logger.info(
        "documents: {:,} rows, subtype filled on {:,} ({:,} → {:,} non-null)",
        result.rows,
        result.filled,
        result.subtype_before,
        result.subtype_after,
    )
    return result


def _remote_version(remote_key: str) -> str | None:
    """ETag (or Last-Modified) of the published object, for a lost-update check."""
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        return None
    resp = httpx.head(f"{public_url.rstrip('/')}/{remote_key}", follow_redirects=True)
    resp.raise_for_status()
    return resp.headers.get("etag") or resp.headers.get("last-modified")


# --- CLI -------------------------------------------------------------------

app = App(name="backfill-document-subtype", help="Backfill documents.subtype on the published documents.parquet.")


def _split(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def _fetch(remote_key: str, local: Path, *, refresh: bool) -> None:
    if local.exists() and not refresh:
        logger.info("Using local {}", local)
        return
    local.parent.mkdir(parents=True, exist_ok=True)
    if not r2.download(remote_key, local):
        raise RuntimeError(f"Could not download {remote_key} (is R2_PUBLIC_URL set?)")


@app.command
def harvest(
    *,
    output_dir: Annotated[Path, Parameter(help="Where documents/manifest are cached")] = Path("output"),
    work_dir: Annotated[Path | None, Parameter(help="Checkpoint dir (default: OUTPUT_DIR/subtype_backfill)")] = None,
    document_types: Annotated[
        str | None, Parameter(help='Comma-separated document_type filter, e.g. "Proposed Rule,Rule,Notice"')
    ] = None,
    agency: Annotated[str | None, Parameter(help="Comma-separated agency_code filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max documents to fetch this run")] = None,
    workers: Annotated[int, Parameter(help="Concurrent S3 GETs")] = DEFAULT_WORKERS,
    chunk_size: Annotated[int, Parameter(help="Documents per checkpoint part")] = DEFAULT_CHUNK_SIZE,
    refresh: Annotated[bool, Parameter(help="Re-download documents/manifest even if cached locally")] = False,
) -> None:
    """Fetch (document_id, subtype) from Mirrulations for rows whose subtype is NULL."""
    load_dotenv()
    work_dir = work_dir or output_dir / "subtype_backfill"
    documents_path = output_dir / DOCUMENTS_KEY
    manifest_path = output_dir / MANIFEST_KEY
    _fetch(DOCUMENTS_KEY, documents_path, refresh=refresh)
    _fetch(MANIFEST_KEY, manifest_path, refresh=refresh)

    keys = select_candidate_keys(
        documents_path,
        manifest_path,
        harvested_dir=work_dir,
        document_types=_split(document_types),
        agencies=_split(agency),
        limit=limit,
    )
    logger.info("{:,} document JSONs to fetch", len(keys))
    result = harvest_keys(
        keys,
        work_dir,
        s3_resource=mirrulations.s3_resource(max_pool_connections=workers),
        workers=workers,
        chunk_size=chunk_size,
    )
    logger.info(
        "Harvest done: {:,}/{:,} fetched, {:,} with a subtype, {:,} transient failures (re-run to retry), "
        "{:,} unparseable",
        result.harvested,
        result.candidates,
        result.with_subtype,
        result.transient_failures,
        result.parse_failures,
    )


@app.command
def apply(
    *,
    output_dir: Annotated[Path, Parameter(help="Where documents.parquet is written")] = Path("output"),
    work_dir: Annotated[Path | None, Parameter(help="Checkpoint dir (default: OUTPUT_DIR/subtype_backfill)")] = None,
    upload: Annotated[bool, Parameter(help="Publish the result to R2 (needs R2 credentials)")] = False,
) -> None:
    """Overlay harvested subtypes onto a fresh documents.parquet and optionally publish it."""
    load_dotenv()
    work_dir = work_dir or output_dir / "subtype_backfill"
    documents_path = output_dir / DOCUMENTS_KEY
    patched_path = output_dir / "documents_subtype.parquet"

    # Always start from the currently published file so an ETL publish that
    # happened during the harvest isn't rolled back.
    version = _remote_version(DOCUMENTS_KEY)
    _fetch(DOCUMENTS_KEY, documents_path, refresh=True)

    result = apply_subtypes(documents_path, work_dir, patched_path)
    if result.filled == 0:
        logger.info("Nothing to fill; leaving documents.parquet as published")
        patched_path.unlink(missing_ok=True)
        return

    patched_path.replace(documents_path)
    if not upload:
        logger.info("--upload not set; patched file left at {}", documents_path)
        return

    if version is not None and _remote_version(DOCUMENTS_KEY) != version:
        raise RuntimeError("documents.parquet was republished while the backfill ran; re-run apply")
    r2.upload_file(documents_path, remote_key=DOCUMENTS_KEY)


def main(argv: Iterable[str] | None = None) -> None:
    app(list(argv) if argv is not None else None)


if __name__ == "__main__":
    main()

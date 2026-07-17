"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces the exact 22 all-VARCHAR columns the existing consumers expect (the
``fr-docket-links`` rollup and the UI's ``normalizeFRRow``), so bringing FR
ingestion in-repo is a drop-in replacement for the former external path.

Incremental by design. A full re-fetch of the ~793K-document archive every run
would be wasteful *and* would trip the R2 catastrophic-shrink guard on any short
run. Instead we:

1. Best-effort download the prior ``federal_register.parquet`` from R2.
2. Fetch only documents published since its max ``publication_date`` (minus a
   short overlap to catch late-posted / corrected documents).
3. Dedup the union on ``document_number``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill from the FR epoch.

``modify_date`` is not exposed by the REST API; freshly fetched rows carry NULL
for it while the merge preserves whatever the prior table already had. No current
consumer reads it.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.federal_register import FR_EPOCH, FederalRegisterReader

OUTPUT = "federal_register.parquet"

# Re-scan this many days before the last stored publication_date on each run, so
# documents added/corrected after their nominal publication date are picked up.
OVERLAP_DAYS = 7

# The published schema: 22 columns, all VARCHAR, in the exact order the existing
# table uses.
COLUMNS = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies_json",
    "agency_slugs",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "cfr_references_json",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "subtype",
    "executive_order_number",
    "modify_date",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (volume/pages/EO # come as ints.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw FR API document onto the published column shape."""
    agencies = doc.get("agencies") or []
    slugs = ",".join(a["slug"] for a in agencies if isinstance(a, dict) and a.get("slug"))
    return {
        "document_number": doc.get("document_number"),
        "title": doc.get("title"),
        "abstract": doc.get("abstract"),
        "document_type": doc.get("type"),
        "publication_date": doc.get("publication_date"),
        "effective_on": doc.get("effective_on"),
        "comments_close_on": doc.get("comments_close_on"),
        "signing_date": doc.get("signing_date"),
        "agencies_json": json.dumps(agencies),
        "agency_slugs": slugs or None,
        "docket_ids_json": json.dumps(doc.get("docket_ids") or []),
        "regulation_id_numbers_json": json.dumps(doc.get("regulation_id_numbers") or []),
        "cfr_references_json": json.dumps(doc.get("cfr_references") or []),
        "html_url": doc.get("html_url"),
        "pdf_url": doc.get("pdf_url"),
        "body_html_url": doc.get("body_html_url"),
        "volume": _s(doc.get("volume")),
        "start_page": _s(doc.get("start_page")),
        "end_page": _s(doc.get("end_page")),
        "subtype": doc.get("subtype"),
        "executive_order_number": _s(doc.get("executive_order_number")),
        "modify_date": None,
    }


def _prior_max_publication_date(prior_file: Path) -> date | None:
    """Largest ``publication_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(publication_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_federal_register(output_dir: Path, *, since: date | None = None) -> Path:
    """Build ``federal_register.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fr_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("FR: merging against prior table {}", prior_file)
    else:
        logger.info("FR: no prior table found — full backfill from {}", FR_EPOCH)

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_publication_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else FR_EPOCH
    logger.info("FR: fetching documents published since {}", since)

    # 3. Fetch + shape into a "new rows" parquet.
    reader = FederalRegisterReader(since=since)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_fr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("FR: fetched {:,} documents this run", len(rows))

    # 4. Merge prior + new, dedup on document_number preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY document_number ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE document_number IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY publication_date DESC, document_number
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Federal Register: {:,} rows", total)
    return out_file

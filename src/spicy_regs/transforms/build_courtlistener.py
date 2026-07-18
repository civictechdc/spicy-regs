"""Transform: build ``court_dockets.parquet`` from the CourtListener v4 API.

Produces a pinned, all-VARCHAR schema keyed by ``cl_docket_id`` (the CourtListener
docket id). Array-valued fields (parties, attorneys, law firms) are serialized as
JSON strings so the published table stays flat and portable; the large per-entry
``recap_documents`` blob is intentionally dropped (it is document-level, not
docket-level metadata).

Scope is APA / agency-review litigation: RECAP dockets with nature-of-suit 899
("Administrative Procedures Act/Review or Appeal of Agency Decision"). These are
the suits challenging federal agency action — the litigation counterpart to the
rulemakings in ``dockets``/``documents``.

**How it joins the corpus.** There is no machine RIN/FR key on a court docket, so
the links are name- and topic-based:

* by **agency** — the defendant agency appears in ``case_name`` and ``parties_json``
  (e.g. "U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES"), joinable by name to
  ``agency_stats`` / the FR ``agency_slugs``;
* by **topic/statute** — ``cause`` names the statute invoked (e.g. "05:551
  Administrative Procedure Act") and ``case_name`` names the challenged program.

Incremental by design (mirrors ``build_lobbying_filings``): a full re-fetch every
run would be wasteful *and* would trip the R2 catastrophic-shrink guard on any
short run. Instead we:

1. Best-effort download the prior ``court_dockets.parquet`` from R2.
2. Fetch only dockets filed since its max ``date_filed`` (minus a short overlap to
   catch newly-indexed / corrected dockets).
3. Dedup the union on ``cl_docket_id``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill. The reader is
functional keyless, so this runs in CI with or without ``COURTLISTENER_API_TOKEN``.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.courtlistener import CourtListenerReader

OUTPUT = "court_dockets.parquet"

CL_BASE_URL = "https://www.courtlistener.com"

# Re-scan this many days before the last stored date_filed on each run, so
# dockets indexed/corrected after the watermark are picked up.
OVERLAP_DAYS = 14

# The published schema: all VARCHAR, keyed by cl_docket_id. Array fields are JSON
# strings.
COLUMNS = (
    "cl_docket_id",
    "case_name",
    "case_name_full",
    "court_id",
    "court",
    "court_citation_string",
    "docket_number",
    "date_filed",
    "date_terminated",
    "date_argued",
    "nature_of_suit",
    "cause",
    "jurisdiction_type",
    "jury_demand",
    "assigned_to",
    "referred_to",
    "parties_json",
    "attorneys_json",
    "firms_json",
    "pacer_case_id",
    "date_created",
    "absolute_url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (ids come as ints.)"""
    if value is None:
        return None
    return str(value)


def _strings(value: object) -> list[str]:
    """Normalize a search-result array field to a list of strings, dropping blanks."""
    if not isinstance(value, list):
        return []
    return [str(v) for v in value if v is not None and str(v).strip()]


def _abs_url(docket: dict) -> str | None:
    """Make the docket's site-relative ``docket_absolute_url`` an absolute URL."""
    rel = docket.get("docket_absolute_url")
    if not rel:
        return None
    return f"{CL_BASE_URL}{rel}" if str(rel).startswith("/") else str(rel)


def _shape(docket: dict) -> dict:
    """Map one raw CourtListener RECAP search result onto the published columns."""
    meta = docket.get("meta") or {}
    return {
        "cl_docket_id": _s(docket.get("docket_id")),
        "case_name": docket.get("caseName"),
        "case_name_full": docket.get("case_name_full") or None,
        "court_id": docket.get("court_id"),
        "court": docket.get("court"),
        "court_citation_string": docket.get("court_citation_string"),
        "docket_number": docket.get("docketNumber"),
        "date_filed": docket.get("dateFiled"),
        "date_terminated": docket.get("dateTerminated"),
        "date_argued": docket.get("dateArgued"),
        "nature_of_suit": docket.get("suitNature"),
        "cause": docket.get("cause"),
        "jurisdiction_type": docket.get("jurisdictionType"),
        "jury_demand": docket.get("juryDemand"),
        "assigned_to": docket.get("assignedTo"),
        "referred_to": docket.get("referredTo"),
        "parties_json": json.dumps(_strings(docket.get("party"))),
        "attorneys_json": json.dumps(_strings(docket.get("attorney"))),
        "firms_json": json.dumps(_strings(docket.get("firm"))),
        "pacer_case_id": _s(docket.get("pacer_case_id")),
        "date_created": meta.get("date_created") if isinstance(meta, dict) else None,
        "absolute_url": _abs_url(docket),
    }


def _prior_max_date_filed(prior_file: Path) -> date | None:
    """Largest ``date_filed`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(date_filed) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_courtlistener(
    output_dir: Path,
    *,
    since: date | None = None,
    max_records: int | None = None,
) -> Path:
    """Build ``court_dockets.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_cl_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("CourtListener: merging against prior table {}", prior_file)
    else:
        logger.info("CourtListener: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_date_filed(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("CourtListener: fetching dockets filed since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    reader = CourtListenerReader(since=since, max_records=max_records)
    rows = [_shape(d) for d in reader.iter_records()]
    new_file = output_dir / "_cl_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CourtListener: fetched {:,} dockets this run", len(rows))

    # 4. Merge prior + new, dedup on cl_docket_id preferring the new row.
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
                    PARTITION BY cl_docket_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE cl_docket_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY date_filed DESC, cl_docket_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court dockets: {:,} rows", total)
    return out_file

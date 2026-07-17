"""Transform: build ``lobbying_filings.parquet`` from the Senate LDA REST API.

Produces a pinned, all-VARCHAR schema keyed by ``filing_uuid``. Nested and
array-valued fields (lobbying activities, the government entities lobbied) are
serialized as JSON strings so the published table stays flat and portable.

Incremental by design (mirrors ``build_federal_register``): a full re-fetch of
the multi-million-row LDA archive every run would be wasteful *and* would trip
the R2 catastrophic-shrink guard on any short run. Instead we:

1. Best-effort download the prior ``lobbying_filings.parquet`` from R2.
2. Fetch only filings posted since its max ``dt_posted`` (minus a short overlap
   to catch late-posted / amended filings).
3. Dedup the union on ``filing_uuid``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill. The reader is
functional keyless, so this runs in CI with or without ``LDA_API_KEY``.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.lobbying_filings import LobbyingFilingsReader

OUTPUT = "lobbying_filings.parquet"

# Re-scan this many days before the last stored dt_posted on each run, so
# filings posted/amended after the watermark are picked up.
OVERLAP_DAYS = 7

# The published schema: all VARCHAR, keyed by filing_uuid. Array/nested fields
# are JSON strings.
COLUMNS = (
    "filing_uuid",
    "filing_type",
    "filing_year",
    "filing_period",
    "dt_posted",
    "registrant_name",
    "registrant_id",
    "client_name",
    "client_id",
    "income",
    "expenses",
    "lobbying_activities_json",
    "government_entities_json",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (ids come as ints.)"""
    if value is None:
        return None
    return str(value)


def _activities(filing: dict) -> list[dict]:
    """Project lobbying_activities to their issue codes + descriptions."""
    out: list[dict] = []
    for act in filing.get("lobbying_activities") or []:
        if not isinstance(act, dict):
            continue
        out.append(
            {
                "general_issue_code": act.get("general_issue_code"),
                "general_issue_code_display": act.get("general_issue_code_display"),
                "description": act.get("description"),
            }
        )
    return out


def _government_entities(filing: dict) -> list[dict]:
    """Collect the distinct government entities (agencies/chambers) lobbied.

    They are nested under each lobbying activity; flatten + dedup on entity id
    (falling back to name) so a filing that lobbied the same chamber across
    several issues lists it once.
    """
    seen: set[object] = set()
    out: list[dict] = []
    for act in filing.get("lobbying_activities") or []:
        if not isinstance(act, dict):
            continue
        for ent in act.get("government_entities") or []:
            if not isinstance(ent, dict):
                continue
            key = ent.get("id") if ent.get("id") is not None else ent.get("name")
            if key in seen:
                continue
            seen.add(key)
            out.append({"id": ent.get("id"), "name": ent.get("name")})
    return out


def _shape(filing: dict) -> dict:
    """Map one raw LDA filing onto the published column shape."""
    registrant = filing.get("registrant") or {}
    client = filing.get("client") or {}
    return {
        "filing_uuid": filing.get("filing_uuid"),
        "filing_type": filing.get("filing_type"),
        "filing_year": _s(filing.get("filing_year")),
        "filing_period": filing.get("filing_period"),
        "dt_posted": filing.get("dt_posted"),
        "registrant_name": registrant.get("name") if isinstance(registrant, dict) else None,
        "registrant_id": _s(registrant.get("id")) if isinstance(registrant, dict) else None,
        "client_name": client.get("name") if isinstance(client, dict) else None,
        "client_id": _s(client.get("id") or client.get("client_id")) if isinstance(client, dict) else None,
        "income": filing.get("income"),
        "expenses": filing.get("expenses"),
        "lobbying_activities_json": json.dumps(_activities(filing)),
        "government_entities_json": json.dumps(_government_entities(filing)),
        "url": filing.get("filing_document_url"),
    }


def _prior_max_dt_posted(prior_file: Path) -> date | None:
    """Largest ``dt_posted`` date in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(dt_posted) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_lobbying_filings(
    output_dir: Path,
    *,
    since: date | None = None,
    filing_year: int | None = None,
    max_records: int | None = None,
) -> Path:
    """Build ``lobbying_filings.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_lda_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("LDA: merging against prior table {}", prior_file)
    else:
        logger.info("LDA: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_dt_posted(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("LDA: fetching filings posted since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    reader = LobbyingFilingsReader(since=since, filing_year=filing_year, max_records=max_records)
    rows = [_shape(f) for f in reader.iter_records()]
    new_file = output_dir / "_lda_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("LDA: fetched {:,} filings this run", len(rows))

    # 4. Merge prior + new, dedup on filing_uuid preferring the new row.
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
                    PARTITION BY filing_uuid ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE filing_uuid IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY dt_posted DESC, filing_uuid
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Lobbying filings: {:,} rows", total)
    return out_file

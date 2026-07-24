"""Transform: build ``congress_bills.parquet`` from the Congress.gov REST API.

Produces an all-VARCHAR schema keyed on ``bill_id`` (e.g. ``118-hr-1234``),
including the enacted public-law number exposed by the list payload. That
``pl_number`` is the direct join key from ``authority_edges``.

Incremental by design. A full re-fetch of the entire bill archive every run
would be wasteful *and* would trip the R2 catastrophic-shrink guard on any short
run. Instead we:

1. Best-effort download the prior ``congress_bills.parquet`` from R2.
2. Fetch only bills updated since its max ``update_date`` (minus a short overlap
   to catch late-updated bills). Bills come newest-updated first, so the reader
   stops as soon as it pages past that watermark.
3. Dedup the union on ``bill_id``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill.

Scope is deliberately **list-level only**: every column comes from the ``/bill``
list payload, so there are no per-bill detail fetches (no N+1).
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import CongressBillsReader

OUTPUT = "congress_bills.parquet"

# Re-scan this many days before the last stored update_date on each run, so bills
# updated after our previous run's cutoff are picked up.
OVERLAP_DAYS = 3

# The published schema: 11 columns, all VARCHAR, in a fixed order. ``bill_id`` is
# the primary / dedup key.
#
# ``policy_area`` is intentionally omitted — the ``/bill`` list endpoint doesn't
# return it (it's a detail-endpoint-only field); it could be added later via a
# per-bill detail enrichment pass.
COLUMNS = (
    "bill_id",
    "congress",
    "bill_type",
    "bill_number",
    "pl_number",
    "title",
    "origin_chamber",
    "latest_action_date",
    "latest_action_text",
    "update_date",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])
_PUBLIC_LAW_ACTION = re.compile(
    r"\b(?:public\s+law|pub\.?\s*l\.?)\s*(?:no\.?\s*:?\s*)?"
    r"(?P<congress>[1-9]\d*)\s*[-–—]\s*(?P<number>[1-9]\d*)",
    re.IGNORECASE,
)
_PUBLIC_LAW_ACTION_SQL = (
    r"(?i)(?:public\s+law|pub\.?\s*l\.?)\s*(?:no\.?\s*:?\s*)?"
    r"([1-9][0-9]*)\s*[-–—]\s*([1-9][0-9]*)"
)


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (congress/number come as ints.)"""
    if value is None:
        return None
    return str(value)


def _bill_id(doc: dict) -> str | None:
    """Build ``{congress}-{type}-{number}`` (e.g. ``118-hr-1234``), or None."""
    congress = doc.get("congress")
    bill_type = doc.get("type")
    number = doc.get("number")
    if congress is None or not bill_type or number is None:
        return None
    return f"{congress}-{str(bill_type).lower()}-{number}"


def _public_law_number(doc: dict) -> str | None:
    """Return the enacted Public Law number as ``<congress>-<number>``.

    The Congress list endpoint exposes enactment in ``latestAction.text``
    (for example, ``Became Public Law No: 117-108.``). ``laws`` is also
    accepted for detail/law-endpoint payloads, but no N+1 detail fetch is
    required to populate the published join key.
    """
    congress = _s(doc.get("congress"))
    for law in doc.get("laws") or []:
        if not isinstance(law, dict):
            continue
        law_type = str(law.get("type") or "").strip().lower().replace(".", "")
        if law_type not in {"public law", "publ", "public"}:
            continue
        raw_number = str(law.get("number") or "").strip().replace("–", "-").replace("—", "-")
        if re.fullmatch(r"[1-9]\d*-[1-9]\d*", raw_number):
            return "-".join(str(int(part)) for part in raw_number.split("-", 1))
        if congress and raw_number.isdigit() and int(raw_number) > 0:
            return f"{int(congress)}-{int(raw_number)}"
    action_text = str((doc.get("latestAction") or {}).get("text") or "")
    if match := _PUBLIC_LAW_ACTION.search(action_text):
        return f"{int(match.group('congress'))}-{int(match.group('number'))}"
    return None


def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov bill onto the published column shape."""
    latest_action = doc.get("latestAction") or {}
    return {
        "bill_id": _bill_id(doc),
        "congress": _s(doc.get("congress")),
        "bill_type": (str(doc["type"]).lower() if doc.get("type") else None),
        "bill_number": _s(doc.get("number")),
        "pl_number": _public_law_number(doc),
        "title": doc.get("title"),
        "origin_chamber": doc.get("originChamber"),
        "latest_action_date": latest_action.get("actionDate"),
        "latest_action_text": latest_action.get("text"),
        "update_date": doc.get("updateDate"),
        "url": doc.get("url"),
    }


def _prior_max_update_date(prior_file: Path) -> date | None:
    """Largest ``update_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(update_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _backfilled_pl_number_sql() -> str:
    """SQL projection that migrates legacy rows without replaying the API."""
    pattern = _PUBLIC_LAW_ACTION_SQL.replace("'", "''")
    congress = f"CAST(CAST(regexp_extract(latest_action_text, '{pattern}', 1) AS BIGINT) AS VARCHAR)"
    number = f"CAST(CAST(regexp_extract(latest_action_text, '{pattern}', 2) AS BIGINT) AS VARCHAR)"
    parsed = (
        f"CASE WHEN regexp_matches(coalesce(latest_action_text, ''), '{pattern}') "
        f"THEN concat({congress}, '-', {number}) END"
    )
    return f"coalesce(pl_number, {parsed}) AS pl_number"


def build_congress_bills(output_dir: Path, *, since: date | None = None) -> Path:
    """Build ``congress_bills.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_congress_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("Congress bills: merging against prior table {}", prior_file)
    else:
        logger.info("Congress bills: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_update_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("Congress bills: fetching bills updated since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    reader = CongressBillsReader(since=since)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_congress_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("Congress bills: fetched {:,} bills this run", len(rows))

    # 4. Merge prior + new, dedup on bill_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    output_cols = ", ".join(_backfilled_pl_number_sql() if column == "pl_number" else column for column in COLUMNS)
    if have_prior:
        # A prior table may predate ``pl_number``. UNION BY NAME first adds the
        # nullable column; the output projection then derives historical values
        # from latest_action_text already stored in the prior table. This makes
        # the join-key migration complete without an expensive full API replay.
        union = (
            f"SELECT *, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT *, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {output_cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY bill_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE bill_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY update_date DESC, bill_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Congress bills: {:,} rows", total)
    return out_file

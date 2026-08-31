"""Transform: build ``congress_bills.parquet`` from the Congress.gov REST API.

Produces a 10-column all-VARCHAR schema keyed on ``bill_id`` (e.g.
``118-hr-1234``), the legislative record complementary to the regulations.gov
``dockets``/``documents`` view.

Incremental by design. A full re-fetch of the entire bill archive every run
would be wasteful *and* would trip the R2 catastrophic-shrink guard on any short
run. Instead we:

1. Best-effort download the prior ``congress_bills.parquet`` from R2.
2. Fetch only bills updated since its max ``update_date`` (minus a short overlap
   to catch late-updated bills), through at most :data:`MAX_WINDOW_DAYS` later.
   Those bounds go to the API as ``fromDateTime``/``toDateTime``, so the server
   returns the window and the reader pages it to exhaustion.
3. Dedup the union on ``bill_id``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill.

**Why the window is capped.** A run publishes nothing until its walk finishes,
so one unbounded catch-up is all-or-nothing: the first attempt at the 510-day
freeze fetched 190,000 of 238,197 bills, hit the job timeout, and persisted
nothing — it would have retried the same doomed walk every night. Capping the
window makes each run publish and advance the watermark, so a deep backfill
converges over successive runs instead of never. Chunks can also be driven
explicitly with ``CONGRESS_SINCE``/``CONGRESS_UNTIL``.

Scope is deliberately **list-level only**: every column comes from the ``/bill``
list payload, so there are no per-bill detail fetches (no N+1).
"""

from __future__ import annotations

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

# Largest span one run will fetch. Sized off measured throughput (~3,050
# bills/min against this API) and the ~467 bills/day average across the 510-day
# freeze: ~90 days is ~42k bills, roughly 14 minutes — comfortably inside the
# job timeout even if a stretch runs several times denser than average.
MAX_WINDOW_DAYS = 90

# The published schema: 10 columns, all VARCHAR, in a fixed order. ``bill_id`` is
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
    "title",
    "origin_chamber",
    "latest_action_date",
    "latest_action_text",
    "update_date",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


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


def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov bill onto the published column shape."""
    latest_action = doc.get("latestAction") or {}
    return {
        "bill_id": _bill_id(doc),
        "congress": _s(doc.get("congress")),
        "bill_type": (str(doc["type"]).lower() if doc.get("type") else None),
        "bill_number": _s(doc.get("number")),
        "title": doc.get("title"),
        "origin_chamber": doc.get("originChamber"),
        "latest_action_date": latest_action.get("actionDate"),
        "latest_action_text": latest_action.get("text"),
        "update_date": doc.get("updateDate"),
        "url": doc.get("url"),
    }


def _bounded_until(since: date | None, until: date | None, *, today: date | None = None) -> date | None:
    """Return a deterministic end date no more than one catch-up window ahead."""
    if since is None:
        return until
    if until is not None and until < since:
        raise ValueError(f"Congress until date {until} precedes since date {since}")
    return min(until or today or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))


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


def build_congress_bills(output_dir: Path, *, since: date | None = None, until: date | None = None) -> Path:
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
    until = _bounded_until(since, until)
    logger.info(
        "Congress bills: fetching bills updated {} through {}",
        since or "the beginning",
        until or "now",
    )

    # 3. Fetch + shape into a "new rows" parquet.
    reader = CongressBillsReader(since=since, until=until)
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

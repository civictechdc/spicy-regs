"""Transform: build ``fec_committees.parquet`` from the OpenFEC API.

Produces a pinned 16-column all-VARCHAR schema keyed on ``committee_id`` (e.g.
``C00684373``) — the Federal Election Commission committee/PAC **reference
dimension** that sits alongside the regulations.gov corpus. Downstream this
feeds the dashboard's ally/opposition (stance) map, giving commenter and
co-filer names a set of political committees to resolve against.

Scope is deliberately committees-only. Itemized contributions
(``/schedules/schedule_a``) run into the hundreds of millions of rows and are
out of scope for this pass; a future bounded-by-organization contributions pass
could follow, keyed on the ``committee_id`` this table establishes.

Incremental by design. Committees are a reference dimension, not a time series,
so there is no watermark: each run walks the full committee list and merges it
with the prior published table. A full re-fetch that produced *fewer* rows than
before (e.g. a truncated run) would trip the R2 catastrophic-shrink guard, so
merging against the prior table both preserves coverage and keeps the row count
monotonic. Concretely we:

1. Best-effort download the prior ``fec_committees.parquet`` from R2.
2. Walk every page of ``/committees`` and shape the rows.
3. Dedup the union on ``committee_id``, preferring the freshly fetched row.

With no prior table (first run) step 3 is just the fresh fetch.

Array fields (``cycles``, ``candidate_ids``) are serialized to JSON strings so
the whole schema stays flat VARCHAR, matching the other external-source tables.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.fec_committees import FecCommitteesReader

OUTPUT = "fec_committees.parquet"

# The published schema: 16 columns, all VARCHAR, in a fixed order.
# ``committee_id`` is the primary / dedup key. Array fields are serialized to
# JSON strings (``*_json``).
COLUMNS = (
    "committee_id",
    "name",
    "committee_type",
    "committee_type_full",
    "designation",
    "designation_full",
    "party",
    "party_full",
    "state",
    "treasurer_name",
    "organization_type_full",
    "filing_frequency",
    "first_file_date",
    "last_file_date",
    "cycles_json",
    "candidate_ids_json",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _json_array(value: object) -> str:
    """Serialize a list-ish field to a JSON string, defaulting to ``[]``."""
    return json.dumps(value if isinstance(value, list) else [])


def _shape(doc: dict) -> dict:
    """Map one raw OpenFEC committee onto the published column shape."""
    return {
        "committee_id": doc.get("committee_id"),
        "name": doc.get("name"),
        "committee_type": doc.get("committee_type"),
        "committee_type_full": doc.get("committee_type_full"),
        "designation": doc.get("designation"),
        "designation_full": doc.get("designation_full"),
        "party": doc.get("party"),
        "party_full": doc.get("party_full"),
        "state": doc.get("state"),
        "treasurer_name": doc.get("treasurer_name"),
        "organization_type_full": doc.get("organization_type_full"),
        "filing_frequency": doc.get("filing_frequency"),
        "first_file_date": doc.get("first_file_date"),
        "last_file_date": doc.get("last_file_date"),
        "cycles_json": _json_array(doc.get("cycles")),
        "candidate_ids_json": _json_array(doc.get("candidate_ids")),
    }


def build_fec_committees(output_dir: Path) -> Path:
    """Build ``fec_committees.parquet`` (full walk merged with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fec_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a clean build).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("FEC committees: merging against prior table {}", prior_file)
    else:
        logger.info("FEC committees: no prior table found — clean build")

    # 2. Fetch + shape into a "new rows" parquet.
    reader = FecCommitteesReader()
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_fec_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("FEC committees: fetched {:,} committees this run", len(rows))

    # 3. Merge prior + new, dedup on committee_id preferring the new row.
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
                    PARTITION BY committee_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE committee_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY committee_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FEC committees: {:,} rows", total)
    return out_file

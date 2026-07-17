"""Transform: build ``unified_agenda.parquet`` from the reginfo.gov export.

Produces the pinned 17 all-VARCHAR columns keyed by (``rin``, ``agenda_edition``).
The Unified Agenda is a Tier-1 rulemaking-lifecycle source: keyed by RIN, it is
the upstream, forward-looking companion to ``federal_register`` (also RIN-keyed)
and the regulations.gov ``dockets``/``documents`` view — it lists actions
agencies *plan* to take, long before they publish.

Incremental by design. Each semiannual edition is a full re-statement of every
agency's active agenda, so re-fetching all historical editions every run would be
wasteful *and* would trip the R2 catastrophic-shrink guard on any short run.
Instead we:

1. Best-effort download the prior ``unified_agenda.parquet`` from R2.
2. Fetch only the requested edition(s) (defaulting to the current edition).
3. Union prior + new and dedup on (``rin``, ``agenda_edition``), preferring the
   freshly fetched row so a re-run of an edition refreshes it in place.

With no prior table (first run) step 3 keeps only the freshly fetched editions.

.. note::
   The reginfo.gov endpoint/params are **not** validated against a live run here
   (see :mod:`spicy_regs.sources.unified_agenda`); the transform is exercised only
   with hermetic fixtures. Field names read off each raw entry
   (:func:`_shape`) likewise require confirmation against a real payload.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.unified_agenda import DEFAULT_EDITION, UnifiedAgendaReader

OUTPUT = "unified_agenda.parquet"

# The published schema: 17 columns, all VARCHAR; array-valued fields serialized
# as JSON strings. Primary / dedup key is (rin, agenda_edition).
COLUMNS = (
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "abstract",
    "rin_status",
    "rule_stage",
    "priority_category",
    "agenda_edition",
    "major",
    "publication_id",
    "timetable_json",
    "cfr_references_json",
    "legal_authority_json",
    "first_action_date",
    "next_action_date",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (some fields arrive as ints/bools.)"""
    if value is None:
        return None
    return str(value)


def _first(doc: dict, *keys: str) -> object:
    """Return the first present, non-None value among ``keys`` (source key drift)."""
    for key in keys:
        if key in doc and doc[key] is not None:
            return doc[key]
    return None


def _shape(doc: dict) -> dict:
    """Map one raw Unified Agenda entry onto the published column shape.

    reginfo.gov's exact field names are unverified (see the source module), so
    the common casings are accepted and the first present one wins.
    """
    return {
        "rin": _s(_first(doc, "rin", "RIN")),
        "agency_code": _s(_first(doc, "agency_code", "agencyCode")),
        "agency_name": _s(_first(doc, "agency_name", "agencyName", "agency")),
        "title": _s(_first(doc, "title", "ruleTitle")),
        "abstract": _s(_first(doc, "abstract")),
        "rin_status": _s(_first(doc, "rin_status", "rinStatus", "status")),
        "rule_stage": _s(_first(doc, "rule_stage", "ruleStage", "stage")),
        "priority_category": _s(_first(doc, "priority_category", "priorityCategory", "priority")),
        "agenda_edition": _s(_first(doc, "agenda_edition", "agendaEdition", "publication")),
        "major": _s(_first(doc, "major")),
        "publication_id": _s(_first(doc, "publication_id", "publicationId")),
        "timetable_json": json.dumps(_first(doc, "timetable", "timetables") or []),
        "cfr_references_json": json.dumps(_first(doc, "cfr_references", "cfrReferences") or []),
        "legal_authority_json": json.dumps(_first(doc, "legal_authority", "legalAuthority") or []),
        "first_action_date": _s(_first(doc, "first_action_date", "firstActionDate")),
        "next_action_date": _s(_first(doc, "next_action_date", "nextActionDate")),
        "url": _s(_first(doc, "url")),
    }


def build_unified_agenda(output_dir: Path, *, editions: tuple[str, ...] | None = None) -> Path:
    """Build ``unified_agenda.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_ua_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means no merge base).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("Unified Agenda: merging against prior table {}", prior_file)
    else:
        logger.info("Unified Agenda: no prior table found — starting fresh")

    # 2. Fetch + shape the requested edition(s) into a "new rows" parquet.
    editions = editions or (DEFAULT_EDITION,)
    reader = UnifiedAgendaReader(editions=editions)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_ua_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("Unified Agenda: fetched {:,} entries this run", len(rows))

    # 3. Merge prior + new, dedup on (rin, agenda_edition) preferring the new row.
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
                    PARTITION BY rin, agenda_edition ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE rin IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY agenda_edition DESC, rin
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Unified Agenda: {:,} rows", total)
    return out_file

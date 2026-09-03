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

The reginfo.gov XML export the reader parses is documented in
:mod:`spicy_regs.sources.unified_agenda`; :func:`_shape` maps the reader's
normalized per-RIN dict onto the pinned 17 columns.
"""

from __future__ import annotations

import json
import re
from datetime import date
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


# reginfo timetable dates are ``MM/DD/YYYY`` (day may be ``00`` for month-only);
# anything else (e.g. ``To Be Determined``) is not a real date and is skipped.
_MDY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _iso_dates(timetable: list) -> list[str]:
    """Return the timetable's real action dates as sorted ISO ``YYYY-MM-DD``.

    A date the calendar rejects (``02/30/2024``) is dropped, never clamped into a
    *different* real date; ``00`` stays reginfo's month-only marker for the 1st.
    """
    dates: set[str] = set()
    for entry in timetable:
        raw = (entry or {}).get("date")
        if not isinstance(raw, str):
            continue
        m = _MDY.match(raw.strip())
        if not m:
            continue
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            action_date = date(year, month, day or 1)
        except ValueError:
            continue
        dates.add(action_date.isoformat())
    return sorted(dates)


def _shape(doc: dict) -> dict:
    """Map one normalized Unified Agenda RIN dict onto the published column shape.

    The reader (:mod:`spicy_regs.sources.unified_agenda`) already normalizes the
    XML tags onto these keys. ``first_action_date`` / ``next_action_date`` are
    derived from the timetable (earliest action, then the earliest later action);
    ``url`` is the deterministic per-RIN reginfo detail page.
    """
    timetable = doc.get("timetable") or []
    dates = _iso_dates(timetable)
    first_action = dates[0] if dates else None
    next_action = None
    if dates:
        earliest = dates[0]  # str, not str | None — keeps the comparison well-typed
        next_action = next((d for d in dates if d > earliest), None)

    rin = _s(doc.get("rin"))
    edition = _s(doc.get("agenda_edition"))
    url = None
    if rin and edition:
        url = f"https://www.reginfo.gov/public/do/eAgendaViewRule?pubId={edition}&RIN={rin}"

    return {
        "rin": rin,
        "agency_code": _s(doc.get("agency_code")),
        "agency_name": _s(doc.get("agency_name")),
        "title": _s(doc.get("title")),
        "abstract": _s(doc.get("abstract")),
        "rin_status": _s(doc.get("rin_status")),
        "rule_stage": _s(doc.get("rule_stage")),
        "priority_category": _s(doc.get("priority_category")),
        "agenda_edition": edition,
        "major": _s(doc.get("major")),
        "publication_id": _s(doc.get("publication_id")),
        "timetable_json": json.dumps(timetable),
        "cfr_references_json": json.dumps(doc.get("cfr_references") or []),
        "legal_authority_json": json.dumps(doc.get("legal_authority") or []),
        "first_action_date": first_action,
        "next_action_date": next_action,
        "url": url,
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

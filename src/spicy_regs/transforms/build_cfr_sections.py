"""Transform: build ``cfr_sections.parquet`` from the GovInfo CFR API.

Produces an all-VARCHAR, section-metadata-only view of the Code of Federal
Regulations: one row per CFR granule (a section-level unit within a title's
annual edition), keyed on ``granule_id``. Section citations (``cfr_ref``,
``title``, ``part``, ``section``) are the join keys back to Federal Register
``cfr_references_json`` and, transitively, to regulations.gov activity.

Scope note: SECTION METADATA + CITATIONS ONLY — the full regulatory *text* of
each section is deliberately out of scope for this pass (it is far heavier). See
``sources/cfr_sections.py`` for the rationale.

Incremental by design. A full re-fetch of every CFR title-year every run would
be wasteful *and* would trip the R2 catastrophic-shrink guard on any short run.
Instead we:

1. Best-effort download the prior ``cfr_sections.parquet`` from R2.
2. Fetch granules (bounded by ``since_year`` when provided).
3. Dedup the union on ``granule_id``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill. Incremental
freshness is driven by ``edition_year`` / ``last_modified``.

Field derivation (live-validated, no N+1). ``_shape`` derives every published
column from the granule's **list-level** fields plus the ID grammar — it never
calls the per-granule ``/summary`` endpoint (the only place ``cfrTitle`` /
``cfrPart`` / ``heading`` live), which would be an N+1 fetch across thousands of
granules per package. CFR title comes from the ``title(\\d+)`` token in the
package/granule ID, edition year from the ``CFR-(\\d{4})`` token, and part /
section from the ``part(\\d+)`` / ``sec(…)`` tokens on the granule ID (both
nullable — section granularity varies by CFR title, so ``section`` is often
null; some titles express sections as ``CONTENT`` granules with no ``part``
token, leaving ``part`` null too).
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.cfr_sections import CfrSectionsReader

OUTPUT = "cfr_sections.parquet"

# The published schema: all VARCHAR, in a fixed order. ``granule_id`` is the
# primary/dedup key.
COLUMNS = (
    "granule_id",
    "package_id",
    "cfr_ref",
    "title",
    "part",
    "section",
    "heading",
    "structure_level",
    "edition_year",
    "last_modified",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (title/part come as ints.)"""
    if value is None:
        return None
    return str(value)


def _cfr_ref(title: object, part: object, section: object) -> str | None:
    """Compose a compact CFR citation like ``40-60.1`` from title/part/section."""
    if title is None or part is None:
        return None
    if section is None:
        return f"{title}-{part}"
    return f"{title}-{part}.{section}"


# ID-grammar parsers. GovInfo CFR IDs look like:
#   package: CFR-2024-title48-vol5
#   granule: CFR-2024-title48-vol5-chap7-appA / …-part700 / …-sec60-1
# Everything the schema needs is derivable from these tokens — no /summary call.
_EDITION_RE = re.compile(r"CFR-(\d{4})")
_TITLE_RE = re.compile(r"title(\d+)")
_PART_RE = re.compile(r"part(\d+)")
_SECTION_RE = re.compile(r"sec([\w.-]+)")


def _first(pattern: re.Pattern[str], text: str | None) -> str | None:
    """Return the first capture group of ``pattern`` in ``text``, else None."""
    if not text:
        return None
    match = pattern.search(text)
    return match.group(1) if match else None


def _shape(granule: dict) -> dict:
    """Map one raw GovInfo CFR granule onto the published all-VARCHAR shape.

    Derives everything from **list-level** fields + the ID grammar (no per-granule
    ``/summary`` call — see module docstring). Reads defensively so an unexpected
    shape yields NULLs rather than raising.
    """
    granule_id = _s(granule.get("granuleId"))
    package_id = _s(granule.get("_package_id") or granule.get("packageId"))

    # CFR title number + edition year: prefer the package id (always well-formed),
    # fall back to the granule id, then dateIssued's leading year for the edition.
    id_for_meta = package_id or granule_id
    edition_year = _first(_EDITION_RE, id_for_meta)
    if edition_year is None:
        date_issued = _s(granule.get("dateIssued"))
        edition_year = date_issued[:4] if date_issued else None
    title_num = _first(_TITLE_RE, id_for_meta)

    # Part / section from the granule id (both nullable — see module docstring).
    part = _first(_PART_RE, granule_id)
    section = _first(_SECTION_RE, granule_id)

    return {
        "granule_id": granule_id,
        "package_id": package_id,
        "cfr_ref": _cfr_ref(title_num, part, section),
        "title": title_num,
        "part": part,
        "section": section,
        # The granule list-level ``title`` field is the heading text.
        "heading": _s(granule.get("title")),
        "structure_level": _s(granule.get("granuleClass")),
        "edition_year": edition_year,
        "last_modified": _s(granule.get("_package_last_modified") or granule.get("lastModified")),
        "url": f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}" if package_id and granule_id else None,
    }


def build_cfr_sections(output_dir: Path, *, since_year: int | None = None) -> Path:
    """Build ``cfr_sections.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_cfr_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("CFR: merging against prior table {}", prior_file)
    else:
        logger.info("CFR: no prior table found — full backfill")

    # 2. Fetch + shape into a "new rows" parquet. (Keyless runs yield nothing.)
    reader = CfrSectionsReader(since_year=since_year)
    rows = [_shape(granule) for granule in reader.iter_records()]
    new_file = output_dir / "_cfr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CFR: fetched {:,} granules this run", len(rows))

    # 3. Merge prior + new, dedup on granule_id preferring the new row.
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
                    PARTITION BY granule_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE granule_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY edition_year DESC, granule_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CFR sections: {:,} rows", total)
    return out_file

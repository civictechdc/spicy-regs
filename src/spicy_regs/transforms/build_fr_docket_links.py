"""Transform: build the Federal Register ↔ docket link table.

Replaces the ``docket_ids_json LIKE '%"<id>"%'`` full-scan over the 793K-row
``federal_register.parquet`` that the docket page ran on every load. Explodes
each FR document's ``docket_ids_json`` array into one row per (docket_id, FR
doc), carrying the display columns the docket page needs, and sorts by
``docket_id`` so ``WHERE docket_id = ?`` prunes row groups instead of scanning.

``federal_register.parquet`` is produced by a **separate** federalregister.gov
ingestion path (not this repo's ETL); this rollup reads it from R2 as a base
input. Exploding preserves the exact matching semantics of the old ``LIKE``
(verified equal on 200 sampled dockets), including the pre-existing quirk where
a few array elements join two IDs — no regression introduced here.

The emitted ``docket_id`` is the identifier the reference states, not the
string that states it: that upstream path supplies decorated values ("Docket
No. FAA-2026-3485", "Doc. No. AMS-SC-24-0046") while the docket spine keys on
the bare id, so raw matching recovered 1 of 14 real edges on the sampled
snapshot (docs/corpus-edge-coverage-findings-2026-07-24.md §1). A reference the
Regulations.gov scheme cannot express keeps its stated value — quarantined for
inspection, never dropped and never forced onto a docket it does not name — and
``docket_ids_json`` still carries the raw array verbatim.

Normalizing makes two references that state one docket identical, so the rows
are distinct: one link per (docket, FR document), however many ways the array
spelled it. A reference that states nothing at all is the one thing dropped
rather than quarantined — 54 of the pinned corpus's 893,822 are bare labels
("Docket No.", "MM Docket No.") whose stated value is decoration only.

Beside the identifier the table publishes ``docket_key``, the comparison key
:func:`normalize_docket_id` derives — decoration stripped under the strict
separator rule, whitespace repaired, upper-cased. It is a **key, not a
resolution**. Deriving it once here is what
``tools/build_agency_crosswalk_artifact.py`` was doing on every read to recover
88,073 link rows the raw join dropped (finding #1, proven in 54f07a6); a joiner
that keys on the column instead of re-deriving it cannot disagree with the
table about what the key is.

What the column does **not** do is license a join. The refusal lives with
whoever holds the docket spine, because only there is the question askable: a
key that names more than one docket is quarantined, never resolved. This build
reads ``federal_register.parquet`` alone and has no spine to ask, so it emits
the key as a pure function of the reference and answers nothing about which
docket it names. (Zero keys covered two dockets across all 276,326 in the
54f07a6 pin, so the refusal path is exercised only by tests — which is a reason
to keep it, not to drop it.)
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import (
    docket_reference_as_stated,
    normalize_docket_id,
    normalize_docket_reference,
)


def build_fr_docket_links(output_dir: Path) -> Path:
    """Build ``fr_docket_links.parquet`` (exploded FR→docket links + display cols)."""
    import duckdb

    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")

    logger.info("Building FR docket links via DuckDB...")

    out_file = output_dir / "fr_docket_links.parquet"

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # One implementation of the docket grammar, shared with the RKAF projection
    # (docpipeline/rkaf_projection.py) rather than restated in SQL — including
    # the pre-cleaning, so a stringified null ("nan") states no docket here
    # either. ``null_handling="special"`` hands the function a SQL NULL instead
    # of short-circuiting to NULL around it, which is what lets these two decide
    # for themselves what states nothing; returning NULL needs no flag.
    for name, function in (
        ("normalize_docket_reference", normalize_docket_reference),
        ("docket_reference_as_stated", docket_reference_as_stated),
        ("normalize_docket_id", normalize_docket_id),
    ):
        con.create_function(name, function, ["VARCHAR"], "VARCHAR", null_handling="special")

    # Carries the columns the docket page's FR section renders (normalizeFRRow
    # rebuilds `docket_ids` from docket_ids_json, so keep that column). Sorted by
    # docket_id with a small row-group size so per-docket lookups prune.
    # DISTINCT because an array that states one docket both ways ("Docket No.
    # FAA-2026-3485" and "FAA-2026-3485") normalizes to one link, and the docket
    # page must not render that FR document twice.
    query = f"""
    COPY (
        SELECT DISTINCT
            COALESCE(
                normalize_docket_reference(link.docket_id),
                docket_reference_as_stated(link.docket_id)
            ) AS docket_id,
            -- Keyed off the identifier rather than the raw string, because the
            -- label grammar already uncovered what a department-prefixed label
            -- was hiding ("DHS Docket No. USCIS-2025-0004"), which the strict
            -- decoration rule alone cannot reach. NULL rather than '' when
            -- nothing survives: a column that says "no key" should say so.
            NULLIF(
                normalize_docket_id(
                    COALESCE(
                        normalize_docket_reference(link.docket_id),
                        docket_reference_as_stated(link.docket_id)
                    )
                ),
                ''
            ) AS docket_key,
            fr.document_number,
            fr.title,
            fr.abstract,
            fr.document_type,
            fr.subtype,
            fr.publication_date,
            fr.effective_on,
            fr.comments_close_on,
            fr.signing_date,
            fr.agency_slugs,
            fr.docket_ids_json,
            fr.regulation_id_numbers_json,
            fr.html_url,
            fr.pdf_url,
            fr.executive_order_number
        FROM read_parquet('{fr_file}') fr,
             UNNEST(CAST(json_extract(fr.docket_ids_json, '$') AS VARCHAR[])) AS link(docket_id)
        WHERE fr.docket_ids_json IS NOT NULL
          AND docket_reference_as_stated(link.docket_id) <> ''
        -- document_number breaks every remaining tie, so the row order is a
        -- function of the data rather than of DuckDB's scheduling. Without it
        -- `preserve_insertion_order=false` made nine identical-input builds
        -- produce nine distinct digests, which also defeated hardlink sharing
        -- between generations (83 MB -> 982 MB on this checkout).
        ORDER BY docket_id, fr.publication_date DESC, fr.document_number
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FR docket links: {:,} rows", rows)

    return out_file

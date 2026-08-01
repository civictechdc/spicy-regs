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
"""

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_docket_reference


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
    # (docpipeline/rkaf_projection.py) rather than restated in SQL. Refusal is
    # the function's whole point, so it must be allowed to return NULL —
    # ``null_handling="special"`` is what permits that.
    con.create_function(
        "normalize_docket_reference",
        normalize_docket_reference,
        ["VARCHAR"],
        "VARCHAR",
        null_handling="special",
    )

    # Carries the columns the docket page's FR section renders (normalizeFRRow
    # rebuilds `docket_ids` from docket_ids_json, so keep that column). Sorted by
    # docket_id with a small row-group size so per-docket lookups prune.
    query = f"""
    COPY (
        SELECT
            COALESCE(
                normalize_docket_reference(link.docket_id),
                TRIM(link.docket_id)
            ) AS docket_id,
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
          AND link.docket_id IS NOT NULL
          AND TRIM(link.docket_id) <> ''
        ORDER BY docket_id, fr.publication_date DESC
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """
    con.execute(query)
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("FR docket links: {:,} rows", rows)

    return out_file

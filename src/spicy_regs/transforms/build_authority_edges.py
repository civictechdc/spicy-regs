"""Transform: parse Unified Agenda legal-authority strings into quarantined edges."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import parse_authority_citation
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    iter_parquet_rows,
    parse_json_list,
    unique_rows,
    write_parquet_rows,
)

OUTPUT = "authority_edges.parquet"
ACTOR_ID = "spicy-regs:authority-parser:v1"

COLUMNS = (
    "rin",
    "authority_raw",
    "usc_title",
    "usc_section",
    "pl_number",
    "authority_type",
    "parse_status",
    "agenda_edition",
    *ATTESTATION_COLUMNS,
)


def build_authority_edges(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build ``authority_edges.parquet`` while retaining every failed parse."""
    source = output_dir / "unified_agenda.parquet"
    if not source.exists():
        raise FileNotFoundError(f"unified_agenda.parquet not found in {output_dir}")

    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="authority-edges")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    rows: list[dict] = []
    for agenda in iter_parquet_rows(source):
        rin = agenda.get("rin")
        edition = agenda.get("agenda_edition")
        authorities = parse_json_list(
            agenda.get("legal_authority_json"),
            stats=json_stats,
            table="unified_agenda",
            row_id=f"{rin}:{edition}",
            column="legal_authority_json",
        )
        if authorities is None:
            continue
        for authority_raw in authorities:
            raw = "" if authority_raw is None else str(authority_raw).strip()
            for parsed in parse_authority_citation(raw):
                rows.append(
                    {
                        "rin": None if rin is None else str(rin).strip().upper(),
                        "authority_raw": raw,
                        "usc_title": parsed.usc_title,
                        "usc_section": parsed.usc_section,
                        "pl_number": parsed.pl_number,
                        "authority_type": parsed.authority_type,
                        "parse_status": parsed.parse_status,
                        "agenda_edition": edition,
                        **provenance,
                    }
                )

    rows = unique_rows(
        rows,
        key_columns=(
            "rin",
            "authority_raw",
            "usc_title",
            "usc_section",
            "pl_number",
            "authority_type",
            "parse_status",
            "agenda_edition",
        ),
    )
    rows.sort(
        key=lambda row: (
            row.get("rin") or "",
            row.get("agenda_edition") or "",
            row.get("authority_raw") or "",
            row.get("authority_type") or "",
        )
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("authority_edges")
    failures = sum(row["parse_status"] == "failed" for row in rows)
    logger.info("Authority edges: {:,} rows ({:,} retained parse failures)", len(rows), failures)
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file

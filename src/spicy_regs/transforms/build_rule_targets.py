"""Transform: build the normalized docket ↔ CFR ↔ RIN rule-identity spine."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import (
    CfrCitation,
    normalize_regsgov_identifier,
    normalize_rin,
    parse_cfr_citation,
)
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    iter_parquet_rows,
    parse_json_list,
    write_parquet_rows,
)

OUTPUT = "rule_targets.parquet"
ACTOR_ID = "spicy-regs:rule-targets:v1"

COLUMNS = (
    "docket_id",
    "cfr_ref",
    "cfr_title",
    "cfr_part",
    "cfr_section",
    "rin",
    "source",
    "evidence_id",
    "first_seen",
    "last_seen",
    *ATTESTATION_COLUMNS,
)

SOURCES = frozenset(
    {
        "fr_cfr_ref",
        "docket_rin",
        "document_rin",
        "document_fr_doc",
    }
)


def _date_bounds(*values: object) -> tuple[str | None, str | None]:
    dates = sorted(str(value) for value in values if value)
    return (dates[0], dates[-1]) if dates else (None, None)


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"rule_targets inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def build_rule_targets(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build action-specific docket-to-RIN and docket-to-CFR evidence.

    Unified Agenda values describe an editioned observation of a durable
    agenda item. They are deliberately absent here: equality on a RIN does not
    authorize projecting an agenda-level CFR reference onto every docket that
    happens to carry that RIN.
    """
    paths = _require_inputs(
        output_dir,
        ("dockets", "documents", "federal_register", "fr_docket_links"),
    )
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="rule-targets")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()

    # The table's specified logical key deliberately retains corroborating
    # sources while folding repeated evidence from the same source into a date
    # span. ``evidence_id`` remains one concrete (lexicographically stable)
    # source row that can be inspected.
    edges: dict[tuple[str, str | None, str | None, str], dict] = {}
    trusted_dockets: set[str] = set()

    def add_edge(
        *,
        docket_id: object,
        citation: CfrCitation | None,
        rin: object,
        source: str,
        evidence_id: object,
        first_seen: object = None,
        last_seen: object = None,
    ) -> None:
        docket = normalize_regsgov_identifier(docket_id)
        if docket is None or docket not in trusted_dockets or source not in SOURCES:
            return
        normalized_rin = normalize_rin(rin)
        cfr_ref = citation.cfr_ref if citation else None
        key = (docket, cfr_ref, normalized_rin, source)
        first, last = _date_bounds(first_seen, last_seen)
        candidate = {
            "docket_id": docket,
            "cfr_ref": cfr_ref,
            "cfr_title": citation.title if citation else None,
            "cfr_part": citation.part if citation else None,
            "cfr_section": citation.section if citation else None,
            "rin": normalized_rin,
            "source": source,
            "evidence_id": None if evidence_id is None else str(evidence_id),
            "first_seen": first,
            "last_seen": last,
            **provenance,
        }
        existing = edges.get(key)
        if existing is None:
            edges[key] = candidate
            return
        existing["first_seen"], existing["last_seen"] = _date_bounds(
            existing.get("first_seen"),
            existing.get("last_seen"),
            first,
            last,
        )
        evidence = sorted(value for value in (existing.get("evidence_id"), candidate.get("evidence_id")) if value)
        existing["evidence_id"] = evidence[0] if evidence else None

    for row in iter_parquet_rows(paths["dockets"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        rin = normalize_rin(row.get("rin"))
        if rin:
            add_edge(
                docket_id=docket,
                citation=None,
                rin=rin,
                source="docket_rin",
                evidence_id=docket,
                first_seen=row.get("modify_date"),
                last_seen=row.get("modify_date"),
            )

    documents_by_fr_doc: dict[str, list[dict]] = defaultdict(list)
    for row in iter_parquet_rows(paths["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is not None:
            # Documents are themselves sourced from Regulations.gov. They can
            # legitimately arrive before the corresponding docket record.
            trusted_dockets.add(docket)
        document_id = row.get("document_id")
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=document_id,
            column="additional_rins",
        )
        if raw_rins is not None:
            for raw_rin in raw_rins:
                rin = normalize_rin(raw_rin)
                if docket is None or not rin:
                    continue
                add_edge(
                    docket_id=docket,
                    citation=None,
                    rin=rin,
                    source="document_rin",
                    evidence_id=document_id,
                    first_seen=row.get("posted_date"),
                    last_seen=row.get("modify_date") or row.get("posted_date"),
                )
        fr_doc_num = row.get("fr_doc_num")
        if fr_doc_num and docket is not None:
            documents_by_fr_doc[str(fr_doc_num)].append({**row, "docket_id": docket})

    linked_dockets_by_fr_doc: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["fr_docket_links"], columns=("document_number", "docket_id")):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if row.get("document_number") and docket in trusted_dockets:
            linked_dockets_by_fr_doc[str(row["document_number"])].add(docket)

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = row.get("document_number")
        if not document_number:
            continue
        raw_cfr = parse_json_list(
            row.get("cfr_references_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="cfr_references_json",
        )
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_cfr is None or raw_rins is None:
            continue
        citations = [citation for raw in raw_cfr for citation in parse_cfr_citation(raw)]
        citations = list(dict.fromkeys(citations))
        rins = list(dict.fromkeys(rin for value in raw_rins if (rin := normalize_rin(value))))
        publication_date = row.get("publication_date")

        linked_dockets = linked_dockets_by_fr_doc.get(str(document_number), set())
        for docket in linked_dockets:
            for citation in citations:
                for rin in rins or (None,):
                    add_edge(
                        docket_id=docket,
                        citation=citation,
                        rin=rin,
                        source="fr_cfr_ref",
                        evidence_id=document_number,
                        first_seen=publication_date,
                        last_seen=publication_date,
                    )

        # A regulations.gov document's frDocNum is independent corroboration of
        # the same target, so it intentionally receives a different source value.
        for document in documents_by_fr_doc.get(str(document_number), ()):
            for citation in citations or (None,):
                for rin in rins or (None,):
                    add_edge(
                        docket_id=document.get("docket_id"),
                        citation=citation,
                        rin=rin,
                        source="document_fr_doc",
                        evidence_id=document.get("document_id"),
                        first_seen=document.get("posted_date") or publication_date,
                        last_seen=document.get("modify_date") or publication_date,
                    )

    rows = sorted(
        edges.values(),
        key=lambda row: (
            row["docket_id"] or "",
            row["cfr_ref"] or "",
            row["rin"] or "",
            row["source"] or "",
        ),
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("rule_targets")
    logger.info("Rule targets: {:,} rows across {:,} dockets", len(rows), len({r["docket_id"] for r in rows}))
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file

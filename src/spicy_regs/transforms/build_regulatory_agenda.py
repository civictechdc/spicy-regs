"""Transform: materialize durable agenda items and qualified action links."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    iter_parquet_rows,
    parse_json_list,
    stable_id,
    write_parquet_rows,
)

ITEMS_OUTPUT = "regulatory_agenda_items.parquet"
RELATIONSHIPS_OUTPUT = "agenda_item_proceedings.parquet"
ITEM_ACTOR_ID = "spicy-regs:regulatory-agenda-items:v1"
RELATIONSHIP_ACTOR_ID = "spicy-regs:agenda-item-proceedings:v1"

ITEM_COLUMNS = (
    "agenda_item_id",
    "rin",
    "scope_status",
    "scope_basis",
    "linked_proceeding_count",
    "observation_count",
    "latest_agenda_edition",
    "first_seen",
    "last_seen",
    *ATTESTATION_COLUMNS,
)

RELATIONSHIP_COLUMNS = (
    "relationship_id",
    "agenda_item_id",
    "rin",
    "proceeding_id",
    "relationship_role",
    "source",
    "evidence_id",
    "evidence_uri",
    "evidence_date",
    *ATTESTATION_COLUMNS,
)

SOURCES = frozenset(
    {"docket_rin", "document_rin", "federal_register_rin"}
)
SCOPE_STATUSES = frozenset({"recurring", "single_observed", "unresolved"})
_RIN = re.compile(r"^\d{4}-[A-Z]{2}\d{2}$")


def _rin(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized if _RIN.fullmatch(normalized) else None


def _agenda_date(edition: object) -> str | None:
    text = str(edition or "").strip()
    if re.fullmatch(r"\d{6}", text) and 1 <= int(text[4:]) <= 12:
        return f"{text[:4]}-{text[4:]}-01"
    return None


def _source_date(*values: object) -> str | None:
    dates = sorted(str(value)[:10] for value in values if value)
    return dates[-1] if dates else None


def _agenda_item_id(rin: str) -> str:
    return f"urn:rkaf:us:rin:{rin}"


def _source_uri(source: str, evidence_id: str) -> str:
    escaped = quote(evidence_id, safe="-._~")
    if source == "docket_rin":
        return f"https://www.regulations.gov/docket/{escaped}"
    if source == "document_rin":
        return f"https://www.regulations.gov/document/{escaped}"
    if source == "federal_register_rin":
        return f"https://www.federalregister.gov/d/{escaped}"
    raise ValueError(f"unknown agenda relationship evidence source: {source}")


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"regulatory agenda inputs missing from {output_dir}: "
            f"{', '.join(missing)}"
        )
    return paths


def build_regulatory_agenda(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> tuple[Path, Path]:
    """Build one agenda item per RIN and provenance-bearing action links.

    Unified Agenda equality creates the item and its editioned observations,
    but never an action relationship. Only a docket, regulations.gov document,
    or Federal Register artifact that directly reports the RIN can link the
    agenda item to an independently assembled Proceeding.
    """
    paths = _require_inputs(
        output_dir,
        (
            "dockets",
            "documents",
            "federal_register",
            "unified_agenda",
            "proceedings",
        ),
    )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="regulatory-agenda",
    )
    item_provenance = context.provenance(
        method="deterministic",
        actor_id=ITEM_ACTOR_ID,
    )
    relationship_provenance = context.provenance(
        method="deterministic",
        actor_id=RELATIONSHIP_ACTOR_ID,
    )
    json_stats = JsonReadStats()

    proceedings_by_docket: dict[str, set[str]] = defaultdict(set)
    proceedings_by_fr_document: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["proceedings"]):
        proceeding_id = str(row.get("proceeding_id") or "").strip()
        if not proceeding_id:
            continue
        dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        if dockets is not None:
            for value in dockets:
                if docket := normalize_regsgov_identifier(value):
                    proceedings_by_docket[docket].add(proceeding_id)
        fr_documents = parse_json_list(
            row.get("fr_document_numbers_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="fr_document_numbers_json",
        )
        if fr_documents is not None:
            for value in fr_documents:
                proceedings_by_fr_document[str(value)].add(proceeding_id)

    observed_rins: set[str] = set()
    seen_dates: dict[str, set[str]] = defaultdict(set)
    observation_keys: dict[str, set[tuple[str, str]]] = defaultdict(set)
    latest_agenda: dict[str, dict] = {}
    relationships: dict[tuple[str, str, str, str], dict] = {}

    def observe(rin: str, *dates: object) -> None:
        observed_rins.add(rin)
        seen_dates[rin].update(
            str(value)[:10] for value in dates if value
        )

    def add_relationship(
        *,
        rin: str,
        proceeding_id: str,
        source: str,
        evidence_id: object,
        evidence_date: object,
    ) -> None:
        evidence = str(evidence_id or "").strip()
        if source not in SOURCES or not evidence or not proceeding_id:
            return
        key = (rin, proceeding_id, source, evidence)
        date = _source_date(evidence_date)
        relationships.setdefault(
            key,
            {
                "relationship_id": stable_id(
                    "agenda_proceeding_relationship",
                    *key,
                ),
                "agenda_item_id": _agenda_item_id(rin),
                "rin": rin,
                "proceeding_id": proceeding_id,
                "relationship_role": "agenda_tracks_proceeding",
                "source": source,
                "evidence_id": evidence,
                "evidence_uri": _source_uri(source, evidence),
                "evidence_date": date,
                **relationship_provenance,
            },
        )
        if date:
            seen_dates[rin].add(date)

    for row in iter_parquet_rows(paths["dockets"]):
        rin = _rin(row.get("rin"))
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if not rin:
            continue
        observe(rin, row.get("modify_date"))
        if docket is None:
            continue
        targets = proceedings_by_docket.get(docket, set())
        if len(targets) == 1:
            add_relationship(
                rin=rin,
                proceeding_id=next(iter(targets)),
                source="docket_rin",
                evidence_id=docket,
                evidence_date=row.get("modify_date"),
            )

    for row in iter_parquet_rows(paths["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        if raw_rins is None:
            continue
        for value in raw_rins:
            rin = _rin(value)
            if not rin:
                continue
            evidence_date = _source_date(
                row.get("modify_date"),
                row.get("posted_date"),
            )
            observe(rin, evidence_date)
            if docket is None:
                continue
            targets = proceedings_by_docket.get(docket, set())
            if len(targets) == 1:
                add_relationship(
                    rin=rin,
                    proceeding_id=next(iter(targets)),
                    source="document_rin",
                    evidence_id=row.get("document_id"),
                    evidence_date=evidence_date,
                )

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = str(row.get("document_number") or "").strip()
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_rins is None:
            continue
        for value in raw_rins:
            rin = _rin(value)
            if not rin:
                continue
            observe(rin, row.get("publication_date"))
            targets = proceedings_by_fr_document.get(document_number, set())
            if len(targets) == 1:
                add_relationship(
                    rin=rin,
                    proceeding_id=next(iter(targets)),
                    source="federal_register_rin",
                    evidence_id=document_number,
                    evidence_date=row.get("publication_date"),
                )

    for row in iter_parquet_rows(paths["unified_agenda"]):
        rin = _rin(row.get("rin"))
        if not rin:
            continue
        edition = str(row.get("agenda_edition") or "").strip()
        url = str(row.get("url") or "").strip()
        edition_date = _agenda_date(edition)
        observe(
            rin,
            edition_date,
            row.get("first_action_date"),
            row.get("next_action_date"),
        )
        observation_keys[rin].add((edition, url))
        if rin not in latest_agenda or edition > str(
            latest_agenda[rin].get("agenda_edition") or ""
        ):
            latest_agenda[rin] = row

    linked_proceedings: dict[str, set[str]] = defaultdict(set)
    for row in relationships.values():
        linked_proceedings[row["rin"]].add(row["proceeding_id"])

    item_rows: list[dict] = []
    for rin in sorted(observed_rins):
        linked_count = len(linked_proceedings.get(rin, ()))
        latest = latest_agenda.get(rin, {})
        priority = " ".join(
            str(latest.get("priority_category") or "").casefold().split()
        )
        if priority == "routine and frequent":
            scope_status = "recurring"
            scope_basis = "latest_agenda_priority_routine_and_frequent"
        elif linked_count == 1:
            scope_status = "single_observed"
            scope_basis = "one_evidence_linked_proceeding"
        elif linked_count == 0:
            scope_status = "unresolved"
            scope_basis = "zero_evidence_linked_proceedings"
        else:
            scope_status = "unresolved"
            scope_basis = "multiple_evidence_linked_proceedings"
        dates = sorted(seen_dates.get(rin, ()))
        item_rows.append(
            {
                "agenda_item_id": _agenda_item_id(rin),
                "rin": rin,
                "scope_status": scope_status,
                "scope_basis": scope_basis,
                "linked_proceeding_count": str(linked_count),
                "observation_count": str(len(observation_keys.get(rin, ()))),
                "latest_agenda_edition": latest.get("agenda_edition"),
                "first_seen": dates[0] if dates else None,
                "last_seen": dates[-1] if dates else None,
                **item_provenance,
            }
        )

    relationship_rows = sorted(
        relationships.values(),
        key=lambda row: (
            row["rin"],
            row["proceeding_id"],
            row["source"],
            row["evidence_id"],
        ),
    )
    items_file = write_parquet_rows(
        output_dir / ITEMS_OUTPUT,
        columns=ITEM_COLUMNS,
        rows=item_rows,
    )
    relationships_file = write_parquet_rows(
        output_dir / RELATIONSHIPS_OUTPUT,
        columns=RELATIONSHIP_COLUMNS,
        rows=relationship_rows,
    )
    json_stats.log("regulatory_agenda")
    logger.info(
        "Regulatory agenda: {:,} items ({:,} recurring; {:,} unresolved), "
        "{:,} action relationships",
        len(item_rows),
        sum(row["scope_status"] == "recurring" for row in item_rows),
        sum(row["scope_status"] == "unresolved" for row in item_rows),
        len(relationship_rows),
    )
    assert pq.ParquetFile(items_file).schema_arrow.names == list(ITEM_COLUMNS)
    assert pq.ParquetFile(relationships_file).schema_arrow.names == list(
        RELATIONSHIP_COLUMNS
    )
    assert all(row["scope_status"] in SCOPE_STATUSES for row in item_rows)
    return items_file, relationships_file

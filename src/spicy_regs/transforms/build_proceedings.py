"""Transform: promote action-specific evidence into first-class proceedings."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import (
    canonical_cfr_iri,
    normalize_regsgov_identifier,
)
from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    iter_parquet_rows,
    parse_json_list,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)

OUTPUT = "proceedings.parquet"
ACTOR_ID = "spicy-regs:proceedings:v2"

COLUMNS = (
    "proceeding_id",
    "rin",
    "docket_ids_json",
    "title",
    "agency_code",
    "current_stage",
    "stage_events_json",
    "fr_document_numbers_json",
    "cfr_refs_json",
    "cfr_target_iris_json",
    "authority_refs_json",
    "identity_predecessors_json",
    *ATTESTATION_COLUMNS,
)

STAGES = frozenset(
    {"prerule", "proposed", "supplemental", "final", "withdrawn", "longterm"}
)
_RIN = re.compile(r"^\d{4}-[A-Z]{2}\d{2}$")
_STAGE_KIND = {
    "prerule": "proceedingPrerule",
    "proposed": "proceedingProposed",
    "supplemental": "proceedingSupplemental",
    "final": "proceedingFinal",
    "withdrawn": "proceedingWithdrawn",
    "longterm": "proceedingLongterm",
}


def _rin(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized if _RIN.fullmatch(normalized) else None


def _stage_from_document(document_type: object, title: object) -> str | None:
    kind = str(document_type or "").casefold()
    text = f"{kind} {str(title or '').casefold()}"
    if "withdraw" in text:
        return "withdrawn"
    if "supplement" in text and ("proposed" in text or "proposal" in text):
        return "supplemental"
    if kind == "rule" or "final rule" in text:
        return "final"
    if kind == "proposed rule" or "proposed rule" in text:
        return "proposed"
    return None


def _current_stage_from_events(events: list[dict]) -> str | None:
    """Return the unique stage at the latest evidenced date."""
    for event in events:
        stage = event.get("stage")
        if stage in STAGES and event.get("event_kind") != _STAGE_KIND[stage]:
            raise ValueError(
                f"stage event kind disagrees with stage: "
                f"{stage!r} / {event.get('event_kind')!r}"
            )
    dated = [event for event in events if event.get("effective_date")]
    if not dated:
        return None
    latest_date = max(str(event["effective_date"]) for event in dated)
    latest_stages = {
        str(event["stage"])
        for event in dated
        if str(event["effective_date"]) == latest_date
        and event.get("stage") in STAGES
    }
    return next(iter(latest_stages)) if len(latest_stages) == 1 else None


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"proceedings inputs missing from {output_dir}: {', '.join(missing)}"
        )
    return paths


def build_proceedings(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build proceedings from dockets and Federal Register action artifacts.

    A RIN identifies a Regulatory Agenda item, not an action. It is retained as
    denormalized evidence when exactly one RIN is observed for an action, but
    it never groups dockets, creates an agenda-only proceeding, or preserves a
    stable proceeding id. One Federal Register artifact may explicitly connect
    multiple trusted dockets; otherwise docket and artifact identities stay
    separate.
    """
    paths = _require_inputs(
        output_dir,
        (
            "dockets",
            "documents",
            "federal_register",
            "fr_docket_links",
            "rule_targets",
        ),
    )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="proceedings",
    )
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    prior_file = output_dir / "_proceedings_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior_proceedings = read_parquet_rows(prior_file)

    def empty_group(
        *,
        dockets: set[str],
        identity: tuple[object, ...],
    ) -> dict:
        return {
            "dockets": set(dockets),
            "identity": identity,
            "rins": set(),
            "titles": [],
            "agencies": [],
            "events": [],
            "fr_documents": set(),
            "cfr_refs": set(),
            "cfr_target_iris": set(),
        }

    # Establish source-backed docket membership before trusting FR link rows.
    trusted_dockets: set[str] = set()
    action_dockets: set[str] = set()
    docket_metadata: dict[str, dict] = {}
    for row in iter_parquet_rows(paths["dockets"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        docket_metadata[docket] = row
        if _rin(row.get("rin")) or "rulemaking" in str(
            row.get("docket_type") or ""
        ).casefold():
            action_dockets.add(docket)

    for row in iter_parquet_rows(paths["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            continue
        trusted_dockets.add(docket)
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        has_rin = raw_rins is not None and any(_rin(value) for value in raw_rins)
        if has_rin or _stage_from_document(
            row.get("document_type"),
            row.get("title"),
        ):
            action_dockets.add(docket)

    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(
        paths["fr_docket_links"],
        columns=("document_number", "docket_id"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if row.get("document_number") and docket in trusted_dockets:
            document_number = str(row["document_number"])
            linked_dockets_by_fr[document_number].add(docket)
            action_dockets.add(docket)

    # Docket identity is action-specific. A Federal Register document is the
    # only cross-docket union signal used by this carrier.
    parent: dict[str, str] = {}

    def find(node: str) -> str:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        winner, loser = sorted((left_root, right_root))
        parent[loser] = winner

    for docket in action_dockets:
        find(docket)
    for dockets in linked_dockets_by_fr.values():
        ordered = sorted(dockets)
        for docket in ordered:
            find(docket)
        for docket in ordered[1:]:
            union(ordered[0], docket)

    members_by_root: dict[str, set[str]] = defaultdict(set)
    for docket in parent:
        members_by_root[find(docket)].add(docket)

    groups: dict[str, dict] = {}
    group_key_by_docket: dict[str, str] = {}
    for dockets in sorted(members_by_root.values(), key=lambda values: min(values)):
        anchor = min(dockets)
        key = f"docket:{anchor}"
        groups[key] = empty_group(
            dockets=dockets,
            identity=("docket", anchor),
        )
        for docket in dockets:
            group_key_by_docket[docket] = key

    def ensure_fr(document_number: str) -> tuple[str, dict]:
        key = f"fr-document:{document_number}"
        return key, groups.setdefault(
            key,
            empty_group(
                dockets=set(),
                identity=("fr-document", document_number),
            ),
        )

    def add_event(
        group: dict,
        *,
        stage: str | None,
        date: object,
        source: str,
        evidence_id: object,
    ) -> None:
        if stage not in STAGES:
            return
        event = {
            "stage": stage,
            "event_kind": _STAGE_KIND[stage],
            "effective_date": None if date is None else str(date)[:10],
            "source": source,
            "evidence_id": None if evidence_id is None else str(evidence_id),
        }
        if event not in group["events"]:
            group["events"].append(event)

    for docket, row in docket_metadata.items():
        key = group_key_by_docket.get(docket)
        if key is None:
            continue
        group = groups[key]
        if rin := _rin(row.get("rin")):
            group["rins"].add(rin)
        if row.get("title"):
            group["titles"].append(
                (str(row.get("modify_date") or ""), str(row["title"]))
            )
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))

    for row in iter_parquet_rows(paths["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        if key is None:
            continue
        group = groups[key]
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        if raw_rins is not None:
            group["rins"].update(
                rin for value in raw_rins if (rin := _rin(value)) is not None
            )
        if row.get("title"):
            group["titles"].append(
                (str(row.get("posted_date") or ""), str(row["title"]))
            )
        if row.get("agency_code"):
            group["agencies"].append(str(row["agency_code"]))
        add_event(
            group,
            stage=_stage_from_document(
                row.get("document_type"),
                row.get("title"),
            ),
            date=row.get("posted_date"),
            source="documents.document_type",
            evidence_id=row.get("document_id"),
        )

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = str(row.get("document_number") or "").strip()
        if not document_number:
            continue
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        rins = (
            set()
            if raw_rins is None
            else {rin for value in raw_rins if (rin := _rin(value)) is not None}
        )
        stage = _stage_from_document(row.get("document_type"), row.get("title"))
        linked_keys = {
            group_key_by_docket[docket]
            for docket in linked_dockets_by_fr.get(document_number, ())
            if docket in group_key_by_docket
        }
        if linked_keys:
            # All trusted dockets named by one FR artifact were unioned above.
            key = next(iter(linked_keys))
            if len(linked_keys) != 1:
                raise RuntimeError(
                    f"FR document {document_number} spans unmerged docket components"
                )
            group = groups[key]
        elif rins or stage:
            _, group = ensure_fr(document_number)
        else:
            continue
        group["fr_documents"].add(document_number)
        group["rins"].update(rins)
        if row.get("title"):
            group["titles"].append(
                (str(row.get("publication_date") or ""), str(row["title"]))
            )
        add_event(
            group,
            stage=stage,
            date=row.get("publication_date"),
            source="federal_register.document_type",
            evidence_id=document_number,
        )

    for row in iter_parquet_rows(paths["rule_targets"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        key = group_key_by_docket.get(docket or "")
        if key is None:
            continue
        group = groups[key]
        if rin := _rin(row.get("rin")):
            group["rins"].add(rin)
        if row.get("cfr_ref"):
            group["cfr_refs"].add(str(row["cfr_ref"]))
            try:
                group["cfr_target_iris"].add(
                    canonical_cfr_iri(
                        row.get("cfr_title"),
                        row.get("cfr_part"),
                        row.get("cfr_section"),
                    )
                )
            except ValueError:
                logger.warning(
                    "proceedings: retained compact CFR ref but could not "
                    "project Rulespec target {}",
                    row.get("cfr_ref"),
                )

    # Stable partner ids follow action evidence, never RIN equality. Docket
    # overlap preserves ordinary continuity; FR overlap preserves a provisional
    # document-based proceeding when its docket is discovered later.
    prior_identity: list[tuple[str, set[str], set[str]]] = []
    for row in prior_proceedings:
        proceeding_id = str(row.get("proceeding_id") or "")
        if not proceeding_id:
            continue
        raw_dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings_prior",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        raw_fr_documents = parse_json_list(
            row.get("fr_document_numbers_json"),
            stats=json_stats,
            table="proceedings_prior",
            row_id=proceeding_id,
            column="fr_document_numbers_json",
        )
        prior_identity.append(
            (
                proceeding_id,
                set() if raw_dockets is None else set(map(str, raw_dockets)),
                (
                    set()
                    if raw_fr_documents is None
                    else set(map(str, raw_fr_documents))
                ),
            )
        )

    prior_by_id = {
        prior_id: (prior_dockets, prior_fr_documents)
        for prior_id, prior_dockets, prior_fr_documents in prior_identity
    }
    prior_ids_by_docket: dict[str, set[str]] = defaultdict(set)
    prior_ids_by_fr: dict[str, set[str]] = defaultdict(set)
    for prior_id, prior_dockets, prior_fr_documents in prior_identity:
        for docket in prior_dockets:
            prior_ids_by_docket[docket].add(prior_id)
        for document_number in prior_fr_documents:
            prior_ids_by_fr[document_number].add(prior_id)

    predecessor_ids_by_group: dict[str, set[str]] = defaultdict(set)
    candidate_edges: list[tuple[int, str, str]] = []
    for group_key, group in groups.items():
        current_dockets = set(group["dockets"])
        current_fr_documents = set(group["fr_documents"])
        plausible_prior_ids: set[str] = set()
        for docket in current_dockets:
            plausible_prior_ids.update(prior_ids_by_docket.get(docket, ()))
        for document_number in current_fr_documents:
            plausible_prior_ids.update(prior_ids_by_fr.get(document_number, ()))
        for prior_id in plausible_prior_ids:
            prior_dockets, prior_fr_documents = prior_by_id[prior_id]
            docket_overlap = len(current_dockets & prior_dockets)
            fr_overlap = len(current_fr_documents & prior_fr_documents)
            if not docket_overlap and not fr_overlap:
                continue
            predecessor_ids_by_group[group_key].add(prior_id)
            score = docket_overlap * 100 + fr_overlap * 10
            candidate_edges.append((-score, prior_id, group_key))

    proceeding_id_by_group: dict[str, str] = {}
    claimed_prior_ids: set[str] = set()
    for _, prior_id, group_key in sorted(candidate_edges):
        if group_key in proceeding_id_by_group or prior_id in claimed_prior_ids:
            continue
        proceeding_id_by_group[group_key] = prior_id
        claimed_prior_ids.add(prior_id)
    for group_key, group in groups.items():
        proceeding_id_by_group.setdefault(
            group_key,
            stable_id("proceeding", *group["identity"]),
        )

    rows: list[dict] = []
    for group_key, group in groups.items():
        events = sorted(
            group["events"],
            key=lambda event: (
                event.get("effective_date") or "",
                event.get("stage") or "",
                event.get("evidence_id") or "",
            ),
        )
        titles = sorted(group["titles"])
        rins = sorted(group["rins"])
        proceeding_id = proceeding_id_by_group[group_key]
        matched_predecessors = predecessor_ids_by_group.get(group_key, set())
        predecessors = sorted(matched_predecessors - {proceeding_id})
        rows.append(
            {
                "proceeding_id": proceeding_id,
                # Compatibility/query aid only; never the row's identity.
                "rin": rins[0] if len(rins) == 1 else None,
                "docket_ids_json": canonical_json(sorted(group["dockets"])),
                "title": titles[-1][1] if titles else None,
                "agency_code": (
                    Counter(group["agencies"]).most_common(1)[0][0]
                    if group["agencies"]
                    else None
                ),
                "current_stage": _current_stage_from_events(events),
                "stage_events_json": canonical_json(events),
                "fr_document_numbers_json": canonical_json(
                    sorted(group["fr_documents"])
                ),
                "cfr_refs_json": canonical_json(sorted(group["cfr_refs"])),
                "cfr_target_iris_json": canonical_json(
                    sorted(group["cfr_target_iris"])
                ),
                # Unified Agenda authority belongs to the editioned agenda
                # observation and is never fanned out to an action.
                "authority_refs_json": "[]",
                "identity_predecessors_json": canonical_json(predecessors),
                **{
                    **provenance,
                    "supersedes_id": (
                        proceeding_id
                        if proceeding_id in matched_predecessors
                        else None
                    ),
                },
            }
        )

    rows.sort(key=lambda row: row["proceeding_id"])
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("proceedings")
    logger.info(
        "Proceedings: {:,} rows ({:,} multi-docket; {:,} FR-only; "
        "{:,} with one action-evidenced RIN)",
        len(rows),
        sum(len(json.loads(row["docket_ids_json"])) > 1 for row in rows),
        sum(row["docket_ids_json"] == "[]" for row in rows),
        sum(row["rin"] is not None for row in rows),
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file

"""Transform: promote rulemaking lifecycle evidence into first-class proceedings."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

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
ACTOR_ID = "spicy-regs:proceedings:v1"

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
    "authority_refs_json",
    "identity_predecessors_json",
    *ATTESTATION_COLUMNS,
)

STAGES = frozenset({"prerule", "proposed", "supplemental", "final", "withdrawn", "longterm"})
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


def _agenda_date(edition: object) -> str | None:
    text = str(edition or "")
    if re.fullmatch(r"\d{6}", text) and 1 <= int(text[4:]) <= 12:
        return f"{text[:4]}-{text[4:]}-01"
    return None


def _stage_from_agenda(value: object) -> str | None:
    text = str(value or "").casefold()
    if "withdraw" in text:
        return "withdrawn"
    if "long-term" in text or "long term" in text:
        return "longterm"
    if "pre-rule" in text or "prerule" in text:
        return "prerule"
    if "supplement" in text:
        return "supplemental"
    if "final" in text:
        return "final"
    if "proposed" in text:
        return "proposed"
    return None


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


def _require_inputs(output_dir: Path, names: tuple[str, ...]) -> dict[str, Path]:
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    missing = [path.name for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"proceedings inputs missing from {output_dir}: {', '.join(missing)}")
    return paths


def build_proceedings(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build docket components within RINs plus docket-identified proceedings.

    A RIN is strong identity evidence but is not globally unique to one
    proceeding: agencies reuse some RINs for recurring rule families. Dockets
    sharing a RIN are therefore merged only when one Federal Register document
    explicitly links them. This keeps legitimate multi-docket proceedings
    together without collapsing thousands of unrelated recurring actions.
    """
    paths = _require_inputs(
        output_dir,
        (
            "dockets",
            "documents",
            "federal_register",
            "unified_agenda",
            "fr_docket_links",
            "rule_targets",
            "authority_edges",
        ),
    )
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="proceedings")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()
    ambiguous = Counter()
    prior_file = output_dir / "_proceedings_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior_proceedings = read_parquet_rows(prior_file)

    def empty_group(
        *,
        rin: str | None,
        dockets: set[str],
        identity: tuple[object, ...],
    ) -> dict:
        return {
            "rin": rin,
            "dockets": set(dockets),
            "identity": identity,
            "titles": [],
            "agencies": [],
            "events": [],
            "fr_documents": set(),
            "cfr_refs": set(),
            "authority_refs": set(),
        }

    # First pass: collect every RIN↔docket assertion and strong co-docket links.
    # A tiny union-find over (RIN, docket) pairs gives deterministic components.
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(node: tuple[str, str]) -> tuple[str, str]:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: tuple[str, str], right: tuple[str, str]) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        winner, loser = sorted((left_root, right_root))
        parent[loser] = winner

    rins_by_docket: dict[str, set[str]] = defaultdict(set)

    def register(rin: str, docket_id: object) -> tuple[str, str] | None:
        if not docket_id:
            return None
        docket = str(docket_id)
        node = (rin, docket)
        find(node)
        rins_by_docket[docket].add(rin)
        return node

    docket_metadata: dict[str, dict] = {}
    for row in iter_parquet_rows(paths["dockets"]):
        docket = row.get("docket_id")
        if not docket:
            continue
        docket_id = str(docket)
        docket_metadata[docket_id] = row
        if rin := _rin(row.get("rin")):
            register(rin, docket_id)

    for row in iter_parquet_rows(paths["documents"]):
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
            if rin := _rin(value):
                register(rin, row.get("docket_id"))

    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(paths["fr_docket_links"], columns=("document_number", "docket_id")):
        if row.get("document_number") and row.get("docket_id"):
            linked_dockets_by_fr[str(row["document_number"])].add(str(row["docket_id"]))

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = str(row.get("document_number") or "")
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_rins is None:
            continue
        dockets = sorted(linked_dockets_by_fr.get(document_number, ()))
        for value in raw_rins:
            rin = _rin(value)
            if not rin:
                continue
            nodes = [node for docket in dockets if (node := register(rin, docket))]
            for node in nodes[1:]:
                union(nodes[0], node)

    for row in iter_parquet_rows(paths["rule_targets"]):
        if rin := _rin(row.get("rin")):
            register(rin, row.get("docket_id"))

    members_by_component: dict[tuple[str, tuple[str, str]], set[str]] = defaultdict(set)
    for node in parent:
        members_by_component[(node[0], find(node))].add(node[1])

    groups: dict[str, dict] = {}
    group_key_by_pair: dict[tuple[str, str], str] = {}
    group_keys_by_rin: dict[str, list[str]] = defaultdict(list)
    group_keys_by_docket: dict[str, set[str]] = defaultdict(set)
    for (rin, _), dockets in sorted(
        members_by_component.items(),
        key=lambda item: (item[0][0], min(item[1])),
    ):
        anchor = min(dockets)
        key = f"rin:{rin}:docket:{anchor}"
        groups[key] = empty_group(
            rin=rin,
            dockets=dockets,
            identity=("rin-docket", rin, anchor),
        )
        group_keys_by_rin[rin].append(key)
        for docket in dockets:
            group_key_by_pair[(rin, docket)] = key
            group_keys_by_docket[docket].add(key)

    def ensure_docket(docket_id: str) -> tuple[str, dict]:
        key = f"docket:{docket_id}"
        group_keys_by_docket[docket_id].add(key)
        return key, groups.setdefault(
            key,
            empty_group(
                rin=None,
                dockets={docket_id},
                identity=("docket", docket_id),
            ),
        )

    def ensure_unscoped_rin(rin: str) -> tuple[str, dict]:
        key = f"rin:{rin}:unscoped"
        return key, groups.setdefault(
            key,
            empty_group(rin=rin, dockets=set(), identity=("rin", rin)),
        )

    for docket_id, row in docket_metadata.items():
        keys = group_keys_by_docket.get(docket_id, set())
        if not keys and "rulemaking" in str(row.get("docket_type") or "").casefold():
            key, _ = ensure_docket(docket_id)
            keys = {key}
        for key in keys:
            group = groups[key]
            if row.get("title"):
                group["titles"].append((str(row.get("modify_date") or ""), str(row["title"])))
            if row.get("agency_code"):
                group["agencies"].append(str(row["agency_code"]))

    def add_event(group: dict, *, stage: str | None, date: object, source: str, evidence_id: object) -> None:
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

    # Second pass: attach stage evidence to the now-stable components.
    for row in iter_parquet_rows(paths["documents"]):
        docket = str(row.get("docket_id") or "")
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        rins = [] if raw_rins is None else [rin for value in raw_rins if (rin := _rin(value))]
        target_keys = {key for rin in rins if (key := group_key_by_pair.get((rin, docket)))}
        if not target_keys:
            docket_targets = group_keys_by_docket.get(docket, set())
            if len(docket_targets) == 1:
                target_keys.update(docket_targets)
            elif len(docket_targets) > 1:
                ambiguous["document_without_unique_component"] += 1
        stage = _stage_from_document(row.get("document_type"), row.get("title"))
        if not target_keys and docket and stage and not group_keys_by_docket.get(docket):
            key, group = ensure_docket(str(docket))
            target_keys.add(key)
        for key in target_keys:
            group = groups[key]
            if row.get("title"):
                group["titles"].append((str(row.get("posted_date") or ""), str(row["title"])))
            if row.get("agency_code"):
                group["agencies"].append(str(row["agency_code"]))
            add_event(
                group,
                stage=stage,
                date=row.get("posted_date"),
                source="documents.document_type",
                evidence_id=row.get("document_id"),
            )

    for row in iter_parquet_rows(paths["federal_register"]):
        document_number = str(row.get("document_number") or "")
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        if raw_rins is None:
            continue
        rins = [rin for value in raw_rins if (rin := _rin(value))]
        linked_dockets = linked_dockets_by_fr.get(document_number, set())
        target_keys: set[str] = set()
        for rin in rins:
            matched = {key for docket in linked_dockets if (key := group_key_by_pair.get((rin, docket)))}
            if matched:
                target_keys.update(matched)
            elif len(group_keys_by_rin.get(rin, ())) == 1:
                target_keys.add(group_keys_by_rin[rin][0])
            elif not group_keys_by_rin.get(rin):
                key, _ = ensure_unscoped_rin(rin)
                target_keys.add(key)
            else:
                ambiguous["fr_document_without_component"] += 1
        if not target_keys and not rins:
            docket_targets: set[str] = set()
            for docket in linked_dockets:
                docket_targets.update(group_keys_by_docket.get(docket, set()))
            if len(docket_targets) == 1:
                target_keys.update(docket_targets)
            elif len(docket_targets) > 1:
                ambiguous["fr_document_without_unique_component"] += 1
        stage = _stage_from_document(row.get("document_type"), row.get("title"))
        for key in target_keys:
            group = groups[key]
            if document_number:
                group["fr_documents"].add(str(document_number))
            if row.get("title"):
                group["titles"].append((str(row.get("publication_date") or ""), str(row["title"])))
            add_event(
                group,
                stage=stage,
                date=row.get("publication_date"),
                source="federal_register.document_type",
                evidence_id=document_number,
            )

    for row in iter_parquet_rows(paths["unified_agenda"]):
        rin = _rin(row.get("rin"))
        if not rin:
            continue
        rin_keys = group_keys_by_rin.get(rin, ())
        if len(rin_keys) == 1:
            agenda_target_keys = set(rin_keys)
        elif not rin_keys:
            key, _ = ensure_unscoped_rin(rin)
            agenda_target_keys = {key}
        else:
            ambiguous["unified_agenda_reused_rin"] += 1
            continue
        edition_date = _agenda_date(row.get("agenda_edition"))
        for key in agenda_target_keys:
            group = groups[key]
            if row.get("title"):
                group["titles"].append((edition_date or "", str(row["title"])))
            if row.get("agency_code"):
                group["agencies"].append(str(row["agency_code"]))
            add_event(
                group,
                stage=_stage_from_agenda(row.get("rule_stage")),
                date=edition_date or row.get("first_action_date"),
                source="unified_agenda.rule_stage",
                evidence_id=f"{rin}:{row.get('agenda_edition') or ''}",
            )

    for row in iter_parquet_rows(paths["rule_targets"]):
        rin = _rin(row.get("rin"))
        docket = str(row.get("docket_id") or "")
        keys: set[str] = set()
        if rin and (key := group_key_by_pair.get((rin, docket))):
            keys.add(key)
        elif docket:
            keys.update(group_keys_by_docket.get(docket, ()))
        for key in keys:
            if key in groups and row.get("cfr_ref"):
                groups[key]["cfr_refs"].add(str(row["cfr_ref"]))

    for row in iter_parquet_rows(paths["authority_edges"]):
        rin = _rin(row.get("rin"))
        if not rin:
            continue
        rin_keys = group_keys_by_rin.get(rin, ())
        if len(rin_keys) == 1:
            group = groups[rin_keys[0]]
        elif not rin_keys:
            _, group = ensure_unscoped_rin(rin)
        else:
            ambiguous["authority_reused_rin"] += 1
            continue
        if row.get("usc_title") and row.get("usc_section"):
            group["authority_refs"].add(f"usc:{row['usc_title']}-{row['usc_section']}")
        elif row.get("pl_number"):
            group["authority_refs"].add(f"public_law:{row['pl_number']}")
        elif row.get("authority_raw"):
            group["authority_refs"].add(f"raw:{row['authority_raw']}")

    # Persistent partner ids are state, not a fresh hash of the current
    # component. A later backfill can add a lexically earlier docket, and new FR
    # evidence can merge two components. Reuse prior ids by strongest docket
    # overlap so those normal corpus changes do not rename a proceeding.
    prior_identity: list[tuple[str, str | None, set[str]]] = []
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
        prior_identity.append(
            (
                proceeding_id,
                _rin(row.get("rin")),
                set() if raw_dockets is None else {str(value) for value in raw_dockets},
            )
        )

    current_groups_by_rin: dict[str, list[str]] = defaultdict(list)
    for group_key, group in groups.items():
        if group["rin"]:
            current_groups_by_rin[group["rin"]].append(group_key)
    prior_count_by_rin = Counter(prior_rin for _, prior_rin, _ in prior_identity if prior_rin)

    predecessor_ids_by_group: dict[str, set[str]] = defaultdict(set)
    candidate_edges: list[tuple[int, str, str]] = []
    for group_key, group in groups.items():
        current_rin = group["rin"]
        current_dockets = set(group["dockets"])
        for prior_id, prior_rin, prior_dockets in prior_identity:
            if current_rin and prior_rin and current_rin != prior_rin:
                continue
            overlap = len(current_dockets & prior_dockets)
            same_unscoped_rin = (
                bool(current_rin) and current_rin == prior_rin and not current_dockets and not prior_dockets
            )
            unique_rin_scope_transition = (
                bool(current_rin)
                and current_rin == prior_rin
                and (not current_dockets or not prior_dockets)
                and len(current_groups_by_rin[current_rin]) == 1
                and prior_count_by_rin[current_rin] == 1
            )
            if not overlap and not same_unscoped_rin and not unique_rin_scope_transition:
                continue
            predecessor_ids_by_group[group_key].add(prior_id)
            score = overlap * 100 + int(current_rin == prior_rin)
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
        dated_events = [event for event in events if event.get("effective_date")]
        current_stage = (dated_events[-1] if dated_events else events[-1])["stage"] if events else None
        titles = sorted(group["titles"])
        rin = group["rin"]
        proceeding_id = proceeding_id_by_group[group_key]
        predecessors = sorted(predecessor_ids_by_group.get(group_key, ()))
        rows.append(
            {
                "proceeding_id": proceeding_id,
                "rin": rin,
                "docket_ids_json": canonical_json(sorted(group["dockets"])),
                "title": titles[-1][1] if titles else None,
                "agency_code": Counter(group["agencies"]).most_common(1)[0][0] if group["agencies"] else None,
                "current_stage": current_stage,
                "stage_events_json": canonical_json(events),
                "fr_document_numbers_json": canonical_json(sorted(group["fr_documents"])),
                "cfr_refs_json": canonical_json(sorted(group["cfr_refs"])),
                "authority_refs_json": canonical_json(sorted(group["authority_refs"])),
                "identity_predecessors_json": canonical_json(predecessors),
                **{
                    **provenance,
                    "supersedes_id": (
                        proceeding_id if proceeding_id in predecessor_ids_by_group.get(group_key, ()) else None
                    ),
                },
            }
        )

    rows.sort(key=lambda row: (row.get("rin") or "", row["proceeding_id"]))
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("proceedings")
    logger.info(
        "Proceedings: {:,} rows ({:,} multi-docket)",
        len(rows),
        sum(len(json.loads(row["docket_ids_json"])) > 1 for row in rows),
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    if ambiguous:
        logger.warning(
            "Proceedings left ambiguous RIN-scoped evidence unattached: {}",
            ", ".join(f"{name}={count:,}" for name, count in sorted(ambiguous.items())),
        )
    return out_file

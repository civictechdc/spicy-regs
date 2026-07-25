"""Concept registry, assignment, event, and convergence logic."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import Iterable, Sequence, cast

from loguru import logger

from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    RunContext,
    canonical_json,
    stable_id,
    text_digest,
)
from spicy_regs.ontology.invariants import (
    assert_append_only,
    assert_attestation_complete,
    assert_concept_graphs,
    resolve_replacement,
)
from spicy_regs.ontology.llm import (
    EVIDENCE_ALIGNMENT_PROVIDED,
    EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    OntologyModel,
    TagProposal,
    ontology_concept_payload,
    resolve_exact_evidence_offsets,
)
from spicy_regs.ontology.segmentation import TiktokenCounter
from spicy_regs.ontology.subjects import Subject

CONCEPT_COLUMNS = (
    "concept_id",
    "scheme",
    "pref_label",
    "alt_labels_json",
    "definition",
    "broader_id",
    "status",
    "replaced_by",
    "external_ids_json",
    *ATTESTATION_COLUMNS,
)

ASSIGNMENT_COLUMNS = (
    "assignment_id",
    "subject_type",
    "subject_id",
    "concept_id",
    "confidence",
    "evidence_json",
    *ATTESTATION_COLUMNS,
)

EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "payload_json",
    *ATTESTATION_COLUMNS,
)

SCHEMES = frozenset({"subject", "regulated_entity"})
CONCEPT_STATUSES = frozenset({"active", "deprecated", "candidate"})
EVENT_TYPES = frozenset({"merge", "split", "rename", "deprecate", "promote", "seed"})

SEED_ACTOR = "federal-register-thesaurus:v1"
MERGE_ACTOR = "spicy-regs:concept-convergence:v1"
CANDIDATE_REGISTRY_MAX_TOKENS = 2_400


def normalize_label(label: object) -> str:
    text = str(label or "").casefold()
    return " ".join(re.findall(r"[a-z0-9]+", text))


def _topic_parts(topic: object) -> tuple[str | None, str | None]:
    if isinstance(topic, str):
        return (topic.strip() or None, None)
    if isinstance(topic, dict):
        topic_fields = cast(dict[str, object], topic)
        label = topic_fields.get("name") or topic_fields.get("label") or topic_fields.get("title")
        slug = topic_fields.get("slug")
        return (
            str(label).strip() if label else None,
            str(slug).strip() if slug else None,
        )
    return (None, None)


def seed_concept(topic: object, context: RunContext) -> dict | None:
    """Create one stable active subject concept from an FR Thesaurus topic."""
    label, slug = _topic_parts(topic)
    if not label:
        return None
    normalized = normalize_label(label)
    if not normalized:
        return None
    external = [{"scheme": "federal_register_thesaurus", "value": label}]
    if slug:
        external[0]["iri"] = f"https://www.federalregister.gov/topics/{slug}"
    return {
        "concept_id": stable_id("concept", "subject", normalized),
        "scheme": "subject",
        "pref_label": label,
        "alt_labels_json": "[]",
        "definition": f"Federal Register Thesaurus topic covering {label}.",
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": canonical_json(external),
        **context.provenance(method="deterministic", actor_id=SEED_ACTOR),
    }


def candidate_concept(proposal: TagProposal, context: RunContext, *, actor_id: str) -> dict:
    label = str(proposal.proposed_label or "").strip()
    normalized = normalize_label(label)
    scheme = proposal.scheme if proposal.scheme in SCHEMES else "subject"
    return {
        "concept_id": stable_id("concept", scheme, normalized),
        "scheme": scheme,
        "pref_label": label,
        "alt_labels_json": "[]",
        "definition": proposal.definition,
        "broader_id": None,
        "status": "candidate",
        "replaced_by": None,
        "external_ids_json": canonical_json(list(proposal.external_ids)),
        **context.provenance(method="llm", actor_id=actor_id),
    }


def concept_aliases(concept: dict) -> set[str]:
    aliases = {normalize_label(concept.get("pref_label"))}
    try:
        values = json.loads(concept.get("alt_labels_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        values = []
    aliases.update(normalize_label(value) for value in values if value)
    aliases.discard("")
    return aliases


def merge_seed_registry(prior: Sequence[dict], seeds: Iterable[dict]) -> list[dict]:
    """Add new seeds without deleting or renaming prior registry entries."""
    concepts = [dict(row) for row in prior]
    aliases_by_scheme: dict[str, set[str]] = defaultdict(set)
    for concept in concepts:
        aliases_by_scheme[str(concept.get("scheme"))].update(concept_aliases(concept))
    for seed in seeds:
        scheme = str(seed.get("scheme"))
        normalized = normalize_label(seed.get("pref_label"))
        if not normalized or normalized in aliases_by_scheme[scheme]:
            continue
        concepts.append(dict(seed))
        aliases_by_scheme[scheme].add(normalized)
    assert_append_only(prior, concepts, id_column="concept_id")
    assert_concept_graphs(concepts)
    return concepts


def select_candidate_concepts(subject: Subject, concepts: Sequence[dict], *, limit: int = 40) -> list[dict]:
    """Bound prompt size using lexical overlap while retaining both facets."""
    normalized_subject = normalize_label(subject.text)
    tokens = set(normalized_subject.split())
    scored: list[tuple[float, str, dict]] = []
    for concept in concepts:
        if concept.get("status") == "deprecated":
            continue
        if concept.get("scheme") not in subject.allowed_schemes:
            continue
        aliases = concept_aliases(concept)
        label_tokens = set().union(*(alias.split() for alias in aliases)) if aliases else set()
        overlap = len(tokens & label_tokens) / max(1, len(label_tokens))
        substring = 1.0 if any(alias and alias in normalized_subject for alias in aliases) else 0.0
        score = max(overlap, substring)
        scored.append((score, str(concept.get("concept_id")), concept))
    scored.sort(key=lambda item: (-item[0], item[1]))
    counter = TiktokenCounter()
    prefix = [concept for _, _, concept in scored[: max(0, limit)]]
    if (
        counter.count(canonical_json([ontology_concept_payload(concept) for concept in prefix]))
        <= CANDIDATE_REGISTRY_MAX_TOKENS
    ):
        selected = prefix
    else:
        selected = []
        for _, _, concept in scored:
            if len(selected) >= limit:
                break
            proposed = [*selected, concept]
            if (
                counter.count(canonical_json([ontology_concept_payload(item) for item in proposed]))
                <= CANDIDATE_REGISTRY_MAX_TOKENS
            ):
                selected.append(concept)
    for scheme in subject.allowed_schemes:
        if not any(concept.get("scheme") == scheme for concept in selected):
            fallback = next(
                (concept for _, _, concept in scored if concept.get("scheme") == scheme),
                None,
            )
            if fallback is not None and (
                counter.count(canonical_json([ontology_concept_payload(item) for item in [*selected, fallback]]))
                <= CANDIDATE_REGISTRY_MAX_TOKENS
            ):
                selected.append(fallback)
    return selected


def match_existing_concept(proposal: TagProposal, concepts: Sequence[dict]) -> str | None:
    """Resolve a model-proposed label to an existing concept before minting."""
    if proposal.concept_id:
        return proposal.concept_id
    normalized = normalize_label(proposal.proposed_label)
    for concept in concepts:
        if concept.get("scheme") == proposal.scheme and normalized in concept_aliases(concept):
            return str(concept["concept_id"])
    return None


def make_assignment(
    *,
    subject: Subject,
    concept_id: str,
    proposal: TagProposal,
    context: RunContext,
    actor_id: str,
    ordinal: int,
    supersedes_id: str | None = None,
    validation: dict | None = None,
) -> dict:
    field_text = subject.fields.get(proposal.evidence_field)
    if field_text is None:
        raise ValueError(f"Unknown evidence field {proposal.evidence_field!r}")
    resolution = resolve_exact_evidence_offsets(
        field_text,
        proposal.evidence_text,
        proposal.evidence_start,
        proposal.evidence_end,
    )
    if resolution is None:
        raise ValueError("Assignment evidence does not resolve in its segment")
    local_start = resolution.start
    local_end = resolution.end
    alignment_method = (
        proposal.evidence_alignment_method
        if (
            resolution.method == EVIDENCE_ALIGNMENT_PROVIDED
            and proposal.evidence_alignment_method
            in {
                EVIDENCE_ALIGNMENT_PROVIDED,
                EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
            }
        )
        else resolution.method
    )
    source_start, source_end = (subject.source_spans or {}).get(
        proposal.evidence_field,
        (0, len(field_text)),
    )
    artifact_start = source_start + local_start
    artifact_end = source_start + local_end
    if artifact_end > source_end:
        raise ValueError("Assignment evidence exceeds its artifact source span")
    canonical_source_field = (subject.field_sources or {}).get(
        proposal.evidence_field,
        proposal.evidence_field,
    )
    span = {
        "text": proposal.evidence_text,
        "source_field": canonical_source_field,
        "evidence_field_key": proposal.evidence_field,
        "start_char": artifact_start,
        "end_char": artifact_end,
        "segment_start_char": local_start,
        "segment_end_char": local_end,
        "alignment_method": alignment_method,
        "segment_id": subject.segment_id,
        "segment_policy": subject.segment_policy,
        "element_id": (subject.element_ids or {}).get(proposal.evidence_field),
        "element_kind": (subject.element_kinds or {}).get(proposal.evidence_field),
        "source_sha256": (subject.source_sha256 or {}).get(proposal.evidence_field),
    }
    evidence: dict[str, object] = {
        "spans": [span],
        "justification": proposal.justification,
        "justifications": [proposal.justification],
        "artifact_sha256": subject.version_digest,
        # Retain the old key while readers migrate to artifact_sha256.
        "subject_sha256": subject.version_digest,
        "segment_sha256": subject.digest,
        "subject_profile": subject.profile_id,
        "source_table": subject.source_table,
        "segment_ids": [subject.segment_id],
        "segment_policy": subject.segment_policy,
        "truncated_fields": [],
    }
    if validation is not None:
        evidence["validation"] = validation
    assignment_id = stable_id(
        "assignment",
        context.run_id,
        subject.subject_type,
        subject.subject_id,
        concept_id,
        subject.version_digest,
        subject.segment_id,
        ordinal,
        supersedes_id,
    )
    return {
        "assignment_id": assignment_id,
        "subject_type": subject.subject_type,
        "subject_id": subject.subject_id,
        "concept_id": concept_id,
        "confidence": f"{proposal.confidence:.6f}",
        "evidence_json": canonical_json(evidence),
        **context.provenance(method="llm", actor_id=actor_id, supersedes_id=supersedes_id),
    }


def assignment_subject_digest(assignment: dict) -> str | None:
    try:
        evidence = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    value = evidence.get("artifact_sha256") or evidence.get("subject_sha256")
    return str(value) if value else None


def _evidence_payload(assignment: dict) -> dict:
    try:
        value = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return cast(dict, value) if isinstance(value, dict) else {}


def aggregate_segment_assignments(
    assignments: Sequence[dict],
    *,
    context: RunContext,
    actor_id: str,
    supersedes_by_key: dict[tuple[str, str, str], str] | None = None,
) -> list[dict]:
    """Combine segment proposals into one artifact-and-concept assertion."""
    grouped: dict[
        tuple[str, str, str, str],
        list[dict],
    ] = defaultdict(list)
    for assignment in assignments:
        evidence = _evidence_payload(assignment)
        key = (
            str(assignment.get("subject_type") or ""),
            str(assignment.get("subject_id") or ""),
            str(assignment.get("concept_id") or ""),
            str(evidence.get("artifact_sha256") or evidence.get("subject_sha256") or ""),
        )
        grouped[key].append(assignment)

    result: list[dict] = []
    for (
        subject_type,
        subject_id,
        concept_id,
        artifact_digest,
    ), rows in sorted(grouped.items()):
        span_by_key: dict[str, dict] = {}
        justifications: set[str] = set()
        segment_ids: set[str] = set()
        profiles: set[str] = set()
        source_tables: set[str] = set()
        provenance: list[dict[str, object]] = []
        for row in rows:
            evidence = _evidence_payload(row)
            for span_value in evidence.get("spans") or []:
                if not isinstance(span_value, dict):
                    continue
                span = cast(dict, span_value)
                span_key = canonical_json(
                    {
                        "element_id": span.get("element_id"),
                        "source_field": span.get("source_field"),
                        "start_char": span.get("start_char"),
                        "end_char": span.get("end_char"),
                        "text": span.get("text"),
                    }
                )
                span_by_key[span_key] = span
                if span.get("segment_id"):
                    segment_ids.add(str(span["segment_id"]))
            for justification in evidence.get("justifications") or [evidence.get("justification")]:
                if justification:
                    justifications.add(str(justification))
            if evidence.get("subject_profile"):
                profiles.add(str(evidence["subject_profile"]))
            if evidence.get("source_table"):
                source_tables.add(str(evidence["source_table"]))
            provenance.append(
                {
                    "assignment_id": row.get("assignment_id"),
                    "actor_id": row.get("actor_id"),
                    "run_id": row.get("run_id"),
                    "segment_ids": evidence.get("segment_ids") or [],
                }
            )
        spans = sorted(
            span_by_key.values(),
            key=lambda span: (
                str(span.get("source_field") or ""),
                int(span.get("start_char") or 0),
                int(span.get("end_char") or 0),
                str(span.get("segment_id") or ""),
            ),
        )
        evidence_set_digest = text_digest(canonical_json(spans))
        supersedes_id = (supersedes_by_key or {}).get((subject_type, subject_id, concept_id))
        assignment_id = stable_id(
            "assignment",
            context.run_id,
            subject_type,
            subject_id,
            concept_id,
            artifact_digest,
            evidence_set_digest,
            supersedes_id,
        )
        evidence = {
            "spans": spans,
            "justification": (sorted(justifications)[0] if justifications else ""),
            "justifications": sorted(justifications),
            "artifact_sha256": artifact_digest,
            "subject_sha256": artifact_digest,
            "subject_profile": (sorted(profiles)[0] if profiles else None),
            "source_table": (sorted(source_tables)[0] if source_tables else None),
            "segment_ids": sorted(segment_ids),
            "segment_policy": (spans[0].get("segment_policy") if spans else None),
            "proposal_provenance": provenance,
            "truncated_fields": [],
        }
        result.append(
            {
                "assignment_id": assignment_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "concept_id": concept_id,
                "confidence": (f"{max(float(row.get('confidence') or 0) for row in rows):.6f}"),
                "evidence_json": canonical_json(evidence),
                **context.provenance(
                    method="llm",
                    actor_id=actor_id,
                    supersedes_id=supersedes_id,
                ),
            }
        )
    return result


def supersede_assignment_with_validation(
    assignment: dict,
    *,
    validations: Sequence[dict],
    context: RunContext,
    actor_id: str,
) -> dict:
    """Append a validated assertion without mutating its proposal history."""
    if not validations:
        raise ValueError("At least one validation result is required")
    evidence = _evidence_payload(assignment)
    agrees = [validation for validation in validations if validation.get("agrees") is True]
    evidence["validation"] = {
        "agrees": bool(agrees),
        "accepted_span_count": len(agrees),
        "evaluated_span_count": len(validations),
        "spans": list(validations),
    }
    prior_confidence = float(assignment.get("confidence") or 0)
    confidence = (
        prior_confidence
        if agrees
        else min(
            prior_confidence,
            max(float(validation.get("confidence") or 0) for validation in validations),
        )
    )
    prior_id = str(assignment.get("assignment_id") or "")
    assignment_id = stable_id(
        "assignment",
        context.run_id,
        assignment.get("subject_type"),
        assignment.get("subject_id"),
        assignment.get("concept_id"),
        evidence.get("artifact_sha256"),
        text_digest(canonical_json(validations)),
        prior_id,
    )
    return {
        "assignment_id": assignment_id,
        "subject_type": assignment.get("subject_type"),
        "subject_id": assignment.get("subject_id"),
        "concept_id": assignment.get("concept_id"),
        "confidence": f"{confidence:.6f}",
        "evidence_json": canonical_json(evidence),
        **context.provenance(
            method="llm",
            actor_id=actor_id,
            supersedes_id=prior_id,
        ),
    }


def generate_for_subject(
    *,
    subject: Subject,
    concepts: list[dict],
    model: OntologyModel,
    context: RunContext,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Generate assignments, materializing justified novel tags as candidates."""
    prompt_concepts = select_candidate_concepts(subject, concepts)
    proposals = model.tag(subject, prompt_concepts)
    new_concepts: list[dict] = []
    assignments: list[dict] = []
    events: list[dict] = []
    for ordinal, proposal in enumerate(proposals):
        concept_id = match_existing_concept(proposal, concepts + new_concepts)
        if concept_id is None:
            candidate = candidate_concept(proposal, context, actor_id=model.model_id)
            if not candidate.get("pref_label"):
                continue
            duplicate = next(
                (
                    concept
                    for concept in concepts + new_concepts
                    if concept.get("scheme") == candidate.get("scheme")
                    and normalize_label(concept.get("pref_label")) == normalize_label(candidate.get("pref_label"))
                ),
                None,
            )
            candidate = duplicate or candidate
            concept_id = str(candidate["concept_id"])
            if duplicate is None:
                new_concepts.append(candidate)
                events.append(
                    make_event(
                        "seed",
                        {
                            "concept_id": concept_id,
                            "label": candidate["pref_label"],
                            "scheme": candidate["scheme"],
                            "source": "llm_candidate",
                            "justification": proposal.justification,
                        },
                        context=context,
                        method="llm",
                        actor_id=model.model_id,
                    )
                )
        assignments.append(
            make_assignment(
                subject=subject,
                concept_id=concept_id,
                proposal=proposal,
                context=context,
                actor_id=model.model_id,
                ordinal=ordinal,
            )
        )
    return new_concepts, assignments, events


def make_event(
    event_type: str,
    payload: dict,
    *,
    context: RunContext,
    method: str,
    actor_id: str,
    supersedes_id: str | None = None,
) -> dict:
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown concept event type: {event_type}")
    serialized = canonical_json(payload)
    return {
        "event_id": stable_id("event", event_type, serialized, context.run_id),
        "event_type": event_type,
        "payload_json": serialized,
        **context.provenance(method=method, actor_id=actor_id, supersedes_id=supersedes_id),
    }


def _char_ngrams(label: object, n: int = 3) -> Counter[str]:
    text = f"  {normalize_label(label)}  "
    return Counter(text[index : index + n] for index in range(max(0, len(text) - n + 1)))


def _cosine(left: Counter[str], right: Counter[str]) -> float:
    numerator = sum(value * right.get(key, 0) for key, value in left.items())
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0


def concept_similarity(left: dict, right: dict) -> float:
    """Label similarity blended with a deterministic character-ngram embedding."""
    label_ratio = SequenceMatcher(
        None,
        normalize_label(left.get("pref_label")),
        normalize_label(right.get("pref_label")),
    ).ratio()
    embedding = _cosine(_char_ngrams(left.get("pref_label")), _char_ngrams(right.get("pref_label")))
    alias_overlap = 1.0 if concept_aliases(left) & concept_aliases(right) else 0.0
    return max(alias_overlap, (label_ratio + embedding) / 2)


def coassignment_similarity(
    left_id: str,
    right_id: str,
    subjects_by_concept: dict[str, set[tuple[str, str]]],
) -> float:
    left = subjects_by_concept.get(left_id, set())
    right = subjects_by_concept.get(right_id, set())
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def merge_pass(
    concepts: Sequence[dict],
    assignments: Sequence[dict],
    *,
    context: RunContext,
    auto_threshold: float = 0.94,
    review_threshold: float = 0.82,
    high_usage: int = 5,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Apply high-confidence merges and return a human-review queue."""
    updated = [dict(concept) for concept in concepts]
    by_id = {str(concept["concept_id"]): concept for concept in updated}
    usage = Counter(str(row.get("concept_id")) for row in assignments if row.get("concept_id"))
    subjects_by_concept: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in assignments:
        if row.get("concept_id") and row.get("subject_type") and row.get("subject_id"):
            subjects_by_concept[str(row["concept_id"])].add((str(row["subject_type"]), str(row["subject_id"])))

    events: list[dict] = []
    review: list[dict] = []
    active = [
        concept
        for concept in updated
        if concept.get("status") in {"active", "candidate"} and not concept.get("replaced_by")
    ]
    pairs: list[tuple[float, float, str, str]] = []
    for index, left in enumerate(active):
        for right in active[index + 1 :]:
            if left.get("scheme") != right.get("scheme"):
                continue
            left_id, right_id = str(left["concept_id"]), str(right["concept_id"])
            label_score = concept_similarity(left, right)
            coassign = coassignment_similarity(left_id, right_id, subjects_by_concept)
            score = max(label_score, (0.8 * label_score) + (0.2 * coassign))
            if score >= review_threshold:
                pairs.append((score, coassign, left_id, right_id))
    pairs.sort(reverse=True)

    consumed: set[str] = set()
    for score, coassign, left_id, right_id in pairs:
        if left_id in consumed or right_id in consumed:
            continue
        left, right = by_id[left_id], by_id[right_id]
        pair_usage = usage[left_id] + usage[right_id]
        if score < auto_threshold:
            if pair_usage >= high_usage:
                review.append(
                    {
                        "left_id": left_id,
                        "left_label": left.get("pref_label"),
                        "right_id": right_id,
                        "right_label": right.get("pref_label"),
                        "score": round(score, 6),
                        "coassignment": round(coassign, 6),
                        "usage": pair_usage,
                    }
                )
            continue

        def winner_key(concept: dict) -> tuple[int, int, str]:
            status_rank = 1 if concept.get("status") == "active" else 0
            return (status_rank, usage[str(concept["concept_id"])], str(concept["concept_id"]))

        winner, loser = sorted((left, right), key=winner_key, reverse=True)
        winner_id, loser_id = str(winner["concept_id"]), str(loser["concept_id"])
        winner_labels = concept_aliases(winner)
        absorbed = [
            label
            for label in [loser.get("pref_label"), *json.loads(loser.get("alt_labels_json") or "[]")]
            if label and normalize_label(label) not in winner_labels
        ]
        winner["alt_labels_json"] = canonical_json(
            sorted(
                set(json.loads(winner.get("alt_labels_json") or "[]")) | set(absorbed),
                key=normalize_label,
            )
        )
        winner.update(context.provenance(method="embedding", actor_id=MERGE_ACTOR))
        loser["status"] = "deprecated"
        loser["replaced_by"] = winner_id
        loser.update(context.provenance(method="embedding", actor_id=MERGE_ACTOR, supersedes_id=winner_id))
        consumed.add(loser_id)
        events.append(
            make_event(
                "merge",
                {
                    "winner_id": winner_id,
                    "winner_label": winner.get("pref_label"),
                    "loser_id": loser_id,
                    "loser_label": loser.get("pref_label"),
                    "score": round(score, 6),
                    "coassignment": round(coassign, 6),
                    "absorbed_labels": absorbed,
                },
                context=context,
                method="embedding",
                actor_id=MERGE_ACTOR,
            )
        )

    assert_concept_graphs(updated)
    assert_attestation_complete(updated)
    return updated, events, review


def rescore_candidates(
    concepts: Sequence[dict],
    assignments: Sequence[dict],
    *,
    context: RunContext,
    promote_usage: int = 3,
    promote_confidence: float = 0.75,
    stale_days: int = 30,
) -> tuple[list[dict], list[dict]]:
    """Promote sustained candidates and deprecate unused stale candidates."""
    updated = [dict(concept) for concept in concepts]
    rows_by_concept: dict[str, list[dict]] = defaultdict(list)
    for assignment in assignments:
        if assignment.get("concept_id"):
            rows_by_concept[str(assignment["concept_id"])].append(assignment)
    events: list[dict] = []
    now = datetime.fromisoformat(context.asserted_at.replace("Z", "+00:00"))
    for concept in updated:
        if concept.get("status") != "candidate":
            continue
        concept_id = str(concept["concept_id"])
        rows = rows_by_concept.get(concept_id, [])
        confidences = [float(row.get("confidence") or 0) for row in rows]
        average = sum(confidences) / len(confidences) if confidences else 0.0
        if len(rows) >= promote_usage and average >= promote_confidence:
            concept["status"] = "active"
            concept.update(context.provenance(method="deterministic", actor_id=MERGE_ACTOR))
            events.append(
                make_event(
                    "promote",
                    {
                        "concept_id": concept_id,
                        "label": concept.get("pref_label"),
                        "usage": len(rows),
                        "average_confidence": round(average, 6),
                    },
                    context=context,
                    method="deterministic",
                    actor_id=MERGE_ACTOR,
                )
            )
            continue
        asserted = concept.get("asserted_at")
        try:
            created = datetime.fromisoformat(str(asserted).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            created = now
        if not rows and (now - created).days >= stale_days:
            concept["status"] = "deprecated"
            concept.update(context.provenance(method="deterministic", actor_id=MERGE_ACTOR))
            events.append(
                make_event(
                    "deprecate",
                    {"concept_id": concept_id, "label": concept.get("pref_label"), "reason": "stale_candidate"},
                    context=context,
                    method="deterministic",
                    actor_id=MERGE_ACTOR,
                )
            )
    assert_concept_graphs(updated)
    return updated, events


def latest_assignments(assignments: Sequence[dict]) -> list[dict]:
    """Resolve supersession so evaluation and usage count current assertions."""
    superseded = {str(row["supersedes_id"]) for row in assignments if row.get("supersedes_id")}
    latest = [row for row in assignments if str(row.get("assignment_id")) not in superseded]
    concepts = {str(row.get("concept_id")) for row in latest}
    if "" in concepts:
        logger.warning("Concept assignments include rows without a concept_id")
    return latest


def resolved_assignment_concept(assignment: dict, concepts: Sequence[dict]) -> str:
    return resolve_replacement(str(assignment["concept_id"]), concepts)

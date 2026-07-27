"""Concept registry, assignment, event, and convergence logic."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Iterable, Sequence, cast

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


def select_candidate_concepts_for_text(
    text: str,
    allowed_schemes: Sequence[str],
    concepts: Sequence[dict],
    *,
    limit: int = 40,
) -> list[dict]:
    """Bound a tag prompt's registry using text and the allowed facets."""
    normalized_subject = normalize_label(text)
    tokens = set(normalized_subject.split())
    allowed = tuple(str(scheme) for scheme in allowed_schemes)
    scored: list[tuple[float, str, dict]] = []
    for concept in concepts:
        if concept.get("status") == "deprecated":
            continue
        if concept.get("scheme") not in allowed:
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
    for scheme in allowed:
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


def select_candidate_concepts(subject: Subject, concepts: Sequence[dict], *, limit: int = 40) -> list[dict]:
    """Compatibility wrapper for ontology subjects using the shared selector."""
    return select_candidate_concepts_for_text(
        subject.text,
        subject.allowed_schemes,
        concepts,
        limit=limit,
    )


# --------------------------------------------------------------------------
# Anchored hybrid candidate selector (v2)
#
# ``select_candidate_concepts_for_text`` above scores every concept by
# unanchored substring containment inside one flat scheme gate. At 901 rows
# that is adequate; at the 513,236-row fused registry it fails twice over —
# short labels match inside longer words ("Ants" inside "pollutants") and the
# ``allowed_schemes`` gate hides every scheme a subject profile does not name,
# which is where 7 of the 8 exact-alias gold targets live
# (docs/evidence/gold-adjudication-2026-07-27/README.md, round 2).
#
# v2 is additive and independent: v1 is untouched so the two remain
# comparable on the same inputs. The design follows recommendations 1-4 of
# docs/evidence/candidate-selection-research-2026-07-27.md:
#
#   1. vocabulary conditioning — normalized alias table, per-alias ambiguity,
#      ≤2-character alias suppression, token IDF over the alias corpus;
#   2. channel A, anchored lexical — token-sequence (word-boundary) alias
#      containment, an IDF-floor anchor requirement, and a fixed linear
#      combination of MLLM-style deterministic features;
#   3. channel B, character 3-gram TF-IDF over each concept's label string
#      (the scispaCy candidate-generator recipe);
#   4. RRF fusion at k=60, then scheme-stratified quotas for the final list.
#
# Two deliberate differences from v1, both required by the finding above:
# there is no ``allowed_schemes`` gate (scheme balance is enforced by quota,
# not by exclusion), and there is no prompt-token trim — v2 currently feeds
# adjudication input, not a provider call, so it selects by rank alone.
# --------------------------------------------------------------------------

ANCHORED_SELECTOR_VERSION = "anchored-hybrid-v2"

# --- conditioning -----------------------------------------------------------
# MetaMap suppresses aliases of two characters or fewer; they carry no
# evidence and cost precision outright.
ANCHOR_MIN_ALIAS_CHARS = 3
# Aliases longer than this are still reachable through channel B, but are not
# indexed for token-sequence matching: they never appear verbatim in a segment
# and they set the per-call n-gram budget.
ANCHOR_MAX_ALIAS_TOKENS = 8
# A match must be anchored by at least one token that is specific in the
# vocabulary: present in no more than 1% of alias strings. Expressed as an IDF
# floor derived from that share so the threshold is scale-free — the same rule
# means the same thing on a 12-row fixture and on a 513k-row registry. The
# absolute minimum keeps the share from collapsing on small vocabularies: a
# token seen in two aliases is not evidence of genericness at any corpus size.
ANCHOR_MAX_ANCHOR_ALIAS_SHARE = 0.01
ANCHOR_MIN_ANCHOR_ALIAS_COUNT = 3

# --- channel A scoring ------------------------------------------------------
# A fixed, documented linear combination. No trained weights: nothing in this
# repo yet has labelled data to fit them with.
ANCHOR_PREF_LABEL_SCORE = 1.0
ANCHOR_ALT_LABEL_SCORE = 0.6
ANCHOR_SPREAD_WEIGHT = 0.8
ANCHOR_AMBIGUITY_WEIGHT = 0.5
# Ambiguity penalty saturates here: an alias shared by 32 or more distinct
# concepts is treated as maximally ambiguous.
ANCHOR_AMBIGUITY_SATURATION = 32.0

# --- channel B --------------------------------------------------------------
ANCHOR_CHAR_NGRAM_SIZE = 3
# The query is the segment's most specific terms rather than its full text:
# a 1,800-token segment's character profile is dominated by function words.
ANCHOR_SEGMENT_TOP_TERMS = 24

# --- fusion and quotas ------------------------------------------------------
ANCHOR_CHANNEL_DEPTH = 50
# Matches ``spicy_regs.docpipeline.retrieval.RETRIEVAL_RRF_K``; kept as a local
# constant so the ontology package does not import the pipeline.
ANCHOR_RRF_K = 60
# Structural protection for small schemes: without it the 440,599-row
# fast-topical scheme crowds out the 33-row policy-area scheme on score alone.
ANCHOR_SCHEME_QUOTAS: dict[str, int] = {
    "subject": 3,
    "crs-subjects": 3,
    "crs-policy-areas": 1,
    "epa-tsca": 1,
    "fast-topical": 2,
}
ANCHOR_WILDCARD_SLOTS = 2
ANCHOR_QUOTA_TOTAL = sum(ANCHOR_SCHEME_QUOTAS.values()) + ANCHOR_WILDCARD_SLOTS

ANCHOR_CONDITIONING_CACHE_SIZE = 4


@dataclass(frozen=True)
class _RegistryConditioning:
    """Everything v2 derives from a registry once, before any segment text."""

    concepts: tuple[dict, ...]
    concept_ids: tuple[str, ...]
    schemes: tuple[str, ...]
    scheme_set: frozenset[str]
    # normalized alias -> ((concept index, alias is the pref label), ...)
    alias_postings: dict[str, tuple[tuple[int, bool], ...]]
    # normalized alias -> number of distinct concepts sharing it
    alias_ambiguity: dict[str, int]
    token_idf: dict[str, float]
    unseen_token_idf: float
    idf_floor: float
    max_alias_tokens: int
    vectorizer: Any
    label_matrix: Any


# Keyed by ``id(concepts)``; the entry holds the sequence itself, so the id
# cannot be recycled underneath a live entry. In-place mutation of a cached
# registry is not detected — registries are loaded and then read.
_ANCHOR_CONDITIONING_CACHE: dict[int, tuple[Any, _RegistryConditioning]] = {}


def clear_anchored_conditioning_cache() -> None:
    """Drop the in-process conditioning cache (tests and long-lived workers)."""
    _ANCHOR_CONDITIONING_CACHE.clear()


def _concept_labels(concept: dict) -> list[tuple[str, bool]]:
    """Normalized (alias, is_pref_label) pairs for one concept, deduplicated."""
    try:
        alternatives = json.loads(concept.get("alt_labels_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        alternatives = []
    if not isinstance(alternatives, list):
        alternatives = []
    labels: list[tuple[str, bool]] = []
    seen: set[str] = set()
    for value, is_pref in [(concept.get("pref_label"), True), *((value, False) for value in alternatives)]:
        alias = normalize_label(value)
        if not alias or alias in seen:
            continue
        seen.add(alias)
        labels.append((alias, is_pref))
    return labels


def _condition_registry(concepts: Sequence[dict]) -> _RegistryConditioning:
    """Build (or reuse) the alias table, IDF table, and char-ngram matrix."""
    cached = _ANCHOR_CONDITIONING_CACHE.get(id(concepts))
    if cached is not None and cached[0] is concepts:
        return cached[1]

    eligible = [concept for concept in concepts if concept.get("status") != "deprecated"]
    postings: dict[str, list[tuple[int, bool]]] = defaultdict(list)
    label_documents: list[str] = []
    for index, concept in enumerate(eligible):
        labels = _concept_labels(concept)
        label_documents.append(" ".join(alias for alias, _ in labels))
        for alias, is_pref in labels:
            if len(alias) < ANCHOR_MIN_ALIAS_CHARS:
                continue
            if len(alias.split()) > ANCHOR_MAX_ALIAS_TOKENS:
                continue
            postings[alias].append((index, is_pref))

    document_frequency: Counter[str] = Counter()
    for alias in postings:
        document_frequency.update(set(alias.split()))
    alias_count = len(postings)
    token_idf = {token: math.log((alias_count + 1) / (count + 1)) + 1.0 for token, count in document_frequency.items()}
    unseen_token_idf = math.log(alias_count + 1) + 1.0
    # A token seen in at most ``share`` of aliases can anchor a match; the
    # floor is that token's IDF.
    anchor_ceiling = max(float(ANCHOR_MIN_ANCHOR_ALIAS_COUNT), ANCHOR_MAX_ANCHOR_ALIAS_SHARE * alias_count)
    idf_floor = math.log((alias_count + 1) / (anchor_ceiling + 1)) + 1.0

    vectorizer, label_matrix = _fit_char_ngram_matrix(label_documents)

    conditioning = _RegistryConditioning(
        concepts=tuple(eligible),
        concept_ids=tuple(str(concept.get("concept_id") or "") for concept in eligible),
        schemes=tuple(str(concept.get("scheme") or "") for concept in eligible),
        scheme_set=frozenset(str(concept.get("scheme") or "") for concept in eligible),
        alias_postings={alias: tuple(entries) for alias, entries in postings.items()},
        alias_ambiguity={alias: len({index for index, _ in entries}) for alias, entries in postings.items()},
        token_idf=token_idf,
        unseen_token_idf=unseen_token_idf,
        idf_floor=idf_floor,
        max_alias_tokens=max((len(alias.split()) for alias in postings), default=1),
        vectorizer=vectorizer,
        label_matrix=label_matrix,
    )
    if len(_ANCHOR_CONDITIONING_CACHE) >= ANCHOR_CONDITIONING_CACHE_SIZE:
        _ANCHOR_CONDITIONING_CACHE.pop(next(iter(_ANCHOR_CONDITIONING_CACHE)))
    _ANCHOR_CONDITIONING_CACHE[id(concepts)] = (concepts, conditioning)
    return conditioning


def _fit_char_ngram_matrix(label_documents: Sequence[str]) -> tuple[Any, Any]:
    """Fit the scispaCy-style char-3-gram TF-IDF matrix over label strings."""
    # Imported lazily: scikit-learn costs about a second to import and most
    # callers of this module never reach the v2 selector.
    import numpy
    from sklearn.feature_extraction.text import TfidfVectorizer

    vectorizer = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(ANCHOR_CHAR_NGRAM_SIZE, ANCHOR_CHAR_NGRAM_SIZE),
        dtype=numpy.float32,
    )
    try:
        matrix = vectorizer.fit_transform(label_documents)
    except ValueError:
        # Empty vocabulary: a registry whose labels carry no ngrams at all.
        return None, None
    return vectorizer, matrix


def _segment_term_weights(tokens: Sequence[str], conditioning: _RegistryConditioning) -> dict[str, float]:
    """TF-IDF weight of each distinct segment token, TF normalized by length."""
    counts = Counter(tokens)
    total = max(1, len(tokens))
    return {
        token: (count / total) * conditioning.token_idf.get(token, conditioning.unseen_token_idf)
        for token, count in counts.items()
    }


def _anchored_channel(
    tokens: Sequence[str],
    weights: dict[str, float],
    conditioning: _RegistryConditioning,
    *,
    depth: int,
) -> list[int]:
    """Channel A: word-boundary alias matches scored by fixed features."""
    peak = max(weights.values(), default=0.0)
    if not peak:
        return []
    best: dict[int, float] = {}
    seen_aliases: set[str] = set()
    span = min(conditioning.max_alias_tokens, ANCHOR_MAX_ALIAS_TOKENS, len(tokens))
    for size in range(1, span + 1):
        for start in range(len(tokens) - size + 1):
            alias_tokens = tokens[start : start + size]
            alias = " ".join(alias_tokens)
            if alias in seen_aliases:
                continue
            seen_aliases.add(alias)
            postings = conditioning.alias_postings.get(alias)
            if not postings:
                continue
            idfs = [conditioning.token_idf.get(token, conditioning.unseen_token_idf) for token in alias_tokens]
            if max(idfs) < conditioning.idf_floor:
                # Every token in the alias is vocabulary-generic: the match is
                # not anchored by anything specific enough to trust.
                continue
            spread = min(1.0, sum(weights[token] for token in set(alias_tokens)) / peak)
            ambiguity = conditioning.alias_ambiguity[alias]
            penalty = ANCHOR_AMBIGUITY_WEIGHT * min(
                1.0, math.log(max(1, ambiguity), 2) / math.log(ANCHOR_AMBIGUITY_SATURATION, 2)
            )
            for index, is_pref in postings:
                label_score = ANCHOR_PREF_LABEL_SCORE if is_pref else ANCHOR_ALT_LABEL_SCORE
                score = label_score + (ANCHOR_SPREAD_WEIGHT * spread) - penalty
                if score > best.get(index, -math.inf):
                    best[index] = score
    ordered = sorted(best.items(), key=lambda item: (-item[1], conditioning.concept_ids[item[0]]))
    return [index for index, _ in ordered[:depth]]


def _char_ngram_channel(
    weights: dict[str, float],
    conditioning: _RegistryConditioning,
    *,
    depth: int,
) -> list[int]:
    """Channel B: char-3-gram TF-IDF cosine against the segment's top terms."""
    if conditioning.label_matrix is None or not weights:
        return []
    import numpy

    top_terms = sorted(weights.items(), key=lambda item: (-item[1], item[0]))[:ANCHOR_SEGMENT_TOP_TERMS]
    query = conditioning.vectorizer.transform([" ".join(token for token, _ in top_terms)])
    if query.nnz == 0:
        return []
    # TfidfVectorizer L2-normalizes rows, so the product is the cosine.
    similarity = numpy.asarray((conditioning.label_matrix @ query.T).todense()).ravel()
    width = min(similarity.shape[0], depth * 4)
    if width < similarity.shape[0]:
        window = numpy.argpartition(-similarity, width - 1)[:width]
    else:
        window = numpy.arange(similarity.shape[0])
    ordered = sorted(
        (int(index) for index in window),
        key=lambda index: (-float(similarity[index]), conditioning.concept_ids[index]),
    )
    return [index for index in ordered[:depth] if similarity[index] > 0.0]


def _fuse_reciprocal_rank(channels: Sequence[Sequence[int]]) -> dict[int, float]:
    """Reciprocal-rank fusion at k=60 over each channel's ranking."""
    fused: dict[int, float] = defaultdict(float)
    for channel in channels:
        for rank, index in enumerate(channel, start=1):
            fused[index] += 1.0 / (ANCHOR_RRF_K + rank)
    return dict(fused)


def _apply_scheme_quotas(
    ranked: Sequence[int],
    conditioning: _RegistryConditioning,
    *,
    limit: int,
) -> list[int]:
    """Fill the shortlist by scheme quota, ceding empty schemes to wildcards."""
    quota_applies = limit >= ANCHOR_QUOTA_TOTAL and set(ANCHOR_SCHEME_QUOTAS) <= conditioning.scheme_set
    if not quota_applies:
        return list(ranked[:limit])
    chosen: list[int] = []
    taken: set[int] = set()
    for scheme in sorted(ANCHOR_SCHEME_QUOTAS):
        quota = ANCHOR_SCHEME_QUOTAS[scheme]
        for index in ranked:
            if len(chosen) >= limit:
                break
            if conditioning.schemes[index] != scheme or index in taken:
                continue
            chosen.append(index)
            taken.add(index)
            quota -= 1
            if quota <= 0:
                break
    # Wildcard pool: the reserved slots plus every slot a scheme could not fill.
    for index in ranked:
        if len(chosen) >= limit:
            break
        if index in taken:
            continue
        chosen.append(index)
        taken.add(index)
    order = {index: rank for rank, index in enumerate(ranked)}
    return sorted(chosen, key=lambda index: order[index])


def select_candidate_concepts_anchored_v2(
    text: str,
    concepts: Sequence[dict],
    *,
    limit: int = ANCHOR_QUOTA_TOTAL,
) -> list[dict]:
    """Select candidates by anchored lexical + char-ngram retrieval, fused.

    Two independent deterministic channels each retrieve their top
    ``ANCHOR_CHANNEL_DEPTH``; RRF at k=60 fuses them; scheme quotas cut the
    result to ``limit``. A registry whose schemes do not cover
    ``ANCHOR_SCHEME_QUOTAS`` (or a ``limit`` below the quota total) takes the
    fused ranking unmodified, so the selector still works on the 901-row
    single-scheme registry.

    Registry conditioning is cached in-process on the identity of ``concepts``:
    the first call over a large registry pays for the alias table and the
    char-ngram matrix, and later calls do not.

    Each returned row is a copy of the registry row carrying
    ``selector_version``; the registry rows themselves are never mutated.
    """
    if limit <= 0:
        return []
    conditioning = _condition_registry(concepts)
    tokens = normalize_label(text).split()
    if not tokens or not conditioning.concepts:
        return []
    weights = _segment_term_weights(tokens, conditioning)
    channels = [
        _anchored_channel(tokens, weights, conditioning, depth=ANCHOR_CHANNEL_DEPTH),
        _char_ngram_channel(weights, conditioning, depth=ANCHOR_CHANNEL_DEPTH),
    ]
    fused = _fuse_reciprocal_rank(channels)
    ranked = sorted(fused.items(), key=lambda item: (-item[1], conditioning.concept_ids[item[0]]))
    selected = _apply_scheme_quotas([index for index, _ in ranked], conditioning, limit=limit)
    return [{**conditioning.concepts[index], "selector_version": ANCHORED_SELECTOR_VERSION} for index in selected]


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

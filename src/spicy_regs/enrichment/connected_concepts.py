"""One-hop cross-domain expansion for managed vocabulary lookup.

RefSpec owns the concepts and reviewed mapping records.  This module only uses
those records to improve retrieval.  Source-domain concepts act as search
anchors; only concepts from the authorized output release are returned.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.concepts import (
    CANDIDATE_REGISTRY_MAX_TOKENS,
    normalize_label,
    select_candidate_concepts_anchored_v2,
)
from spicy_regs.ontology.concept_dimensions import (
    concept_source_vocabulary,
)
from spicy_regs.ontology.llm import ontology_concept_payload
from spicy_regs.ontology.segmentation import TiktokenCounter

CONNECTED_SELECTOR_VERSION = "anchored-hybrid-v2+mapped-neighbors-v2"
CONNECTED_INDEXED_REPRESENTATION_VERSION = (
    "unicode-nfkc-casefold-label-role-v2"
)
CONNECTED_PREF_LABEL_WEIGHT = 1.0
CONNECTED_ALT_LABEL_WEIGHT = 0.65
CONNECTED_CROSS_VOCABULARY_REUSE_DISCOUNT = 0.25

_RELATION_PRIORITY = {
    "http://www.w3.org/2004/02/skos/core#exactMatch": 0,
    "skos:exactMatch": 0,
    "http://www.w3.org/2004/02/skos/core#closeMatch": 1,
    "skos:closeMatch": 1,
    "http://www.w3.org/2004/02/skos/core#broadMatch": 2,
    "skos:broadMatch": 2,
    "http://www.w3.org/2004/02/skos/core#narrowMatch": 2,
    "skos:narrowMatch": 2,
    "http://www.w3.org/2004/02/skos/core#relatedMatch": 3,
    "skos:relatedMatch": 3,
}
_RELATION_WEIGHT = {
    "http://www.w3.org/2004/02/skos/core#exactMatch": 0.98,
    "skos:exactMatch": 0.98,
    "http://www.w3.org/2004/02/skos/core#closeMatch": 0.95,
    "skos:closeMatch": 0.95,
    "http://www.w3.org/2004/02/skos/core#narrowMatch": 0.80,
    "skos:narrowMatch": 0.80,
    "http://www.w3.org/2004/02/skos/core#broadMatch": 0.65,
    "skos:broadMatch": 0.65,
    "http://www.w3.org/2004/02/skos/core#relatedMatch": 0.50,
    "skos:relatedMatch": 0.50,
}


class ConnectedConceptSearchError(ValueError):
    """The lookup rows and RefSpec mapping records do not form a safe index."""


def _authored_labels(row: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Return normalized labels with preferred-label precedence."""

    labels: list[tuple[str, str]] = []
    seen: set[str] = set()
    preferred = normalize_label(row.get("pref_label"))
    if preferred:
        labels.append((preferred, "preferred"))
        seen.add(preferred)
    try:
        alternatives = json.loads(str(row.get("alt_labels_json") or "[]"))
    except json.JSONDecodeError:
        alternatives = []
    if not isinstance(alternatives, list):
        alternatives = []
    for value in alternatives:
        alternate = normalize_label(value)
        if alternate and alternate not in seen:
            labels.append((alternate, "alternate"))
            seen.add(alternate)
    return labels


def _cross_vocabulary_reuse(
    lookup_concepts: Sequence[dict[str, Any]],
) -> dict[str, int]:
    vocabularies_by_label: dict[str, set[str]] = defaultdict(set)
    for row in lookup_concepts:
        vocabulary = concept_source_vocabulary(row)
        for label, _ in _authored_labels(row):
            vocabularies_by_label[label].add(vocabulary)
    return {
        label: len(vocabularies)
        for label, vocabularies in vocabularies_by_label.items()
    }


def _label_support(
    text: str,
    row: Mapping[str, Any],
    *,
    reuse_by_label: Mapping[str, int],
) -> tuple[float, int, str]:
    """Score an exact authored phrase by role, specificity, and reuse."""

    normalized_query = normalize_label(text)
    normalized_text = f" {normalized_query} "
    query_token_count = max(1, len(normalized_query.split()))
    supports: list[tuple[float, int, str]] = []
    for label, role in _authored_labels(row):
        if f" {label} " not in normalized_text:
            continue
        base_weight = (
            CONNECTED_PREF_LABEL_WEIGHT
            if role == "preferred"
            else CONNECTED_ALT_LABEL_WEIGHT
        )
        reuse = max(1, int(reuse_by_label.get(label, 1)))
        label_token_count = len(label.split())
        query_coverage = label_token_count / query_token_count
        weight = (base_weight + query_coverage) / (
            1.0
            + (
                CONNECTED_CROSS_VOCABULARY_REUSE_DISCOUNT
                * (reuse - 1)
            )
        )
        supports.append((weight, label_token_count, label))
    return max(supports, default=(0.0, 0, ""))


def _mapping_value(mapping: object, attribute: str) -> str:
    value = getattr(mapping, attribute, None)
    if not isinstance(value, str) or not value:
        raise ConnectedConceptSearchError(
            f"mapping {attribute} must be non-empty text"
        )
    return value


def _mapping_path(mapping: object) -> dict[str, str]:
    relation = _mapping_value(mapping, "relation_iri")
    if relation not in _RELATION_PRIORITY:
        raise ConnectedConceptSearchError(
            f"unsupported concept mapping relation {relation!r}"
        )
    return {
        "mapping_iri": _mapping_value(mapping, "mapping_iri"),
        "relation_iri": relation,
        "source_member_iri": _mapping_value(
            mapping,
            "source_member_iri",
        ),
        "target_member_iri": _mapping_value(
            mapping,
            "target_member_iri",
        ),
        "source_release_iri": _mapping_value(
            mapping,
            "source_release_iri",
        ),
        "target_release_iri": _mapping_value(
            mapping,
            "target_release_iri",
        ),
        "direction": "sourceToTarget",
    }


def select_connected_candidate_concepts(
    text: str,
    *,
    lookup_concepts: Sequence[dict[str, Any]],
    output_concepts: Sequence[dict[str, Any]],
    mappings: Sequence[object],
    allowed_facets: Sequence[str] = ("subject",),
    limit: int = 12,
) -> list[dict[str, Any]]:
    """Return authorized concepts reached directly or through one mapping.

    ``lookup_concepts`` may contain anchor concepts from several domains.
    ``output_concepts`` is the authorized result universe.  A retrieved anchor
    can contribute only its explicitly mapped target; the anchor itself never
    leaks into the result unless it also belongs to the output universe.

    Expansion is deliberately one hop.  Mapping targets are not recursively
    expanded, and equal labels never create an implicit mapping.
    """

    if limit <= 0:
        return []
    output_by_id: dict[str, dict[str, Any]] = {}
    for row in output_concepts:
        concept_id = str(row.get("concept_id") or "")
        if not concept_id:
            raise ConnectedConceptSearchError(
                "every output concept needs concept_id"
            )
        if concept_id in output_by_id:
            raise ConnectedConceptSearchError(
                f"output concept {concept_id!r} appears twice"
            )
        output_by_id[concept_id] = dict(row)

    lookup_ids = {
        str(row.get("concept_id") or "")
        for row in lookup_concepts
    }
    if "" in lookup_ids or len(lookup_ids) != len(lookup_concepts):
        raise ConnectedConceptSearchError(
            "lookup concepts need unique, non-empty concept_id values"
        )
    paths_by_source: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_mapping_ids: set[str] = set()
    for mapping in mappings:
        path = _mapping_path(mapping)
        mapping_id = path["mapping_iri"]
        if mapping_id in seen_mapping_ids:
            raise ConnectedConceptSearchError(
                f"concept mapping {mapping_id!r} appears twice"
            )
        seen_mapping_ids.add(mapping_id)
        if path["source_member_iri"] not in lookup_ids:
            raise ConnectedConceptSearchError(
                f"mapping source {path['source_member_iri']!r} is not indexed"
            )
        if path["target_member_iri"] not in output_by_id:
            raise ConnectedConceptSearchError(
                f"mapping target {path['target_member_iri']!r} is not "
                "authorized for output"
            )
        paths_by_source[path["source_member_iri"]].append(path)
    for paths in paths_by_source.values():
        paths.sort(
            key=lambda path: (
                _RELATION_PRIORITY[path["relation_iri"]],
                path["mapping_iri"],
            )
        )

    # A wider anchor search prevents non-output anchors from consuming the
    # entire result budget before their mapped targets can be considered.
    anchors = select_candidate_concepts_anchored_v2(
        text,
        lookup_concepts,
        allowed_facets=allowed_facets,
        limit=max(24, limit * 4),
    )
    selected: list[dict[str, Any]] = []
    selected_by_id: dict[str, dict[str, Any]] = {}
    best_rank_by_id: dict[str, tuple[float, int, int, int]] = {}
    reuse_by_label = _cross_vocabulary_reuse(lookup_concepts)

    def include(
        row: Mapping[str, Any],
        *,
        channel: str,
        anchor_rank: int,
        support: tuple[float, int, str],
        route_weight: float,
        path: Mapping[str, str] | None = None,
    ) -> None:
        concept_id = str(row["concept_id"])
        rank = (
            support[0] * route_weight,
            support[1],
            1 if channel == "lexical" else 0,
            -anchor_rank,
        )
        existing = selected_by_id.get(concept_id)
        if existing is None:
            candidate = {
                **dict(row),
                "selector_version": CONNECTED_SELECTOR_VERSION,
                "candidate_channels": [channel],
                "mapping_paths": ([dict(path)] if path is not None else []),
                "selected_channel": channel,
                "selected_mapping_path": (
                    dict(path) if path is not None else None
                ),
            }
            selected.append(candidate)
            selected_by_id[concept_id] = candidate
            best_rank_by_id[concept_id] = rank
            return
        channels = existing["candidate_channels"]
        if channel not in channels:
            channels.append(channel)
        if path is not None and dict(path) not in existing["mapping_paths"]:
            existing["mapping_paths"].append(dict(path))
        if rank > best_rank_by_id[concept_id]:
            best_rank_by_id[concept_id] = rank
            existing["selected_channel"] = channel
            existing["selected_mapping_path"] = (
                dict(path) if path is not None else None
            )

    for anchor_rank, anchor in enumerate(anchors):
        anchor_id = str(anchor["concept_id"])
        support = _label_support(
            text,
            anchor,
            reuse_by_label=reuse_by_label,
        )
        direct = output_by_id.get(anchor_id)
        if direct is not None:
            include(
                direct,
                channel="lexical",
                anchor_rank=anchor_rank,
                support=support,
                route_weight=1.0,
            )
        for path in paths_by_source.get(anchor_id, ()):
            include(
                output_by_id[path["target_member_iri"]],
                channel="mappedNeighbor",
                anchor_rank=anchor_rank,
                support=support,
                route_weight=_RELATION_WEIGHT[
                    path["relation_iri"]
                ],
                path=path,
            )

    selected.sort(
        key=lambda row: (
            tuple(-value for value in best_rank_by_id[str(row["concept_id"])]),
            str(row["concept_id"]),
        )
    )
    selected = selected[:limit]
    counter = TiktokenCounter()
    while selected and (
        counter.count(
            canonical_json(
                [
                    ontology_concept_payload(concept)
                    for concept in selected
                ]
            )
        )
        > CANDIDATE_REGISTRY_MAX_TOKENS
    ):
        selected.pop()
    for rank, candidate in enumerate(selected, start=1):
        candidate["candidate_rank"] = rank
        candidate["candidate_score"] = best_rank_by_id[
            str(candidate["concept_id"])
        ][0]
        candidate["candidate_score_state"] = "produced"
        candidate["indexed_representation_version"] = (
            CONNECTED_INDEXED_REPRESENTATION_VERSION
        )
    return selected

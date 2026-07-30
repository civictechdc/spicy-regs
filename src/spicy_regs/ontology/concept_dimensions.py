"""Separate semantic tag facets from controlled-vocabulary identity."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

FACETS = frozenset({"subject", "regulated_entity"})
LOCAL_SOURCE_VOCABULARY = "spicy-regs-local"
# Compatibility identity used by older experiment fixtures and fused
# registries. New Federal Register seeds use the dated scheme below.
FEDERAL_REGISTER_SOURCE_VOCABULARY = "federal-register-thesaurus"
FEDERAL_REGISTER_THESAURUS_2025_SOURCE_VOCABULARY = (
    "urn:ref:federal-register-thesaurus:2025-04-01:scheme"
)
FEDERAL_REGISTER_TOPICS_SOURCE_VOCABULARY = (
    "federal-register-api-topics"
)
FEDERAL_REGISTER_EXTERNAL_ID_MARKERS = frozenset(
    {
        # Base seed rows.
        "federal_register_thesaurus",
        # Rows minted by fused-concept-registry-v1.
        "fr-thesaurus",
    }
)

# Fused-registry v1 stored these external vocabulary names in ``scheme``.
# Reading them is a compatibility path, not the shape written by new fusion.
LEGACY_EXTERNAL_SCHEME_FACETS = {
    FEDERAL_REGISTER_SOURCE_VOCABULARY: "subject",
    FEDERAL_REGISTER_THESAURUS_2025_SOURCE_VOCABULARY: "subject",
    FEDERAL_REGISTER_TOPICS_SOURCE_VOCABULARY: "subject",
    "crs-subjects": "subject",
    "crs-policy-areas": "subject",
    "epa-tsca": "regulated_entity",
    "fast-topical": "subject",
}


def _external_id_schemes(concept: Mapping[str, Any]) -> set[str]:
    try:
        values = json.loads(concept.get("external_ids_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        return set()
    if not isinstance(values, list):
        return set()
    return {
        str(item.get("scheme") or "").strip()
        for item in values
        if isinstance(item, dict) and str(item.get("scheme") or "").strip()
    }


def concept_facet(concept: Mapping[str, Any]) -> str:
    """Return the semantic tag-policy facet for a registry row."""
    facet = str(concept.get("facet") or "").strip()
    legacy_scheme = str(concept.get("scheme") or "").strip()
    if facet:
        if facet not in FACETS:
            raise ValueError(f"concept {concept.get('concept_id')!r} has unknown facet {facet!r}")
        if legacy_scheme in FACETS and legacy_scheme != facet:
            raise ValueError(
                f"concept {concept.get('concept_id')!r} disagrees on facet "
                f"({facet!r} != legacy scheme {legacy_scheme!r})"
            )
        return facet
    if legacy_scheme in FACETS:
        return legacy_scheme
    inferred = LEGACY_EXTERNAL_SCHEME_FACETS.get(legacy_scheme)
    if inferred:
        return inferred
    raise ValueError(f"concept {concept.get('concept_id')!r} has no usable semantic facet")


def concept_source_vocabulary(concept: Mapping[str, Any]) -> str:
    """Return the controlled vocabulary used for identity and provenance."""
    vocabulary = str(concept.get("source_vocabulary") or "").strip()
    if vocabulary:
        return vocabulary
    legacy_scheme = str(concept.get("scheme") or "").strip()
    if legacy_scheme and legacy_scheme not in FACETS:
        return legacy_scheme
    if FEDERAL_REGISTER_EXTERNAL_ID_MARKERS & _external_id_schemes(concept):
        return FEDERAL_REGISTER_SOURCE_VOCABULARY
    return LOCAL_SOURCE_VOCABULARY


def with_concept_dimensions(concept: Mapping[str, Any]) -> dict[str, Any]:
    """Copy one row with explicit facet and source-vocabulary dimensions."""
    return {
        **dict(concept),
        "facet": concept_facet(concept),
        "source_vocabulary": concept_source_vocabulary(concept),
    }

"""Tests for the anchored hybrid candidate selector (v2).

Every fixture here is synthetic and tiny: the properties under test are
structural (anchoring, suppression, fusion arithmetic, quota arithmetic), and a
structural property that only holds on a 513k-row registry is not a property.

The v1 ranking behavior and public signature remain available beside v2. Both
selectors now read explicit concept dimensions through the same compatibility
helper.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.ontology import concepts as ontology_concepts
from spicy_regs.ontology.concepts import (
    ANCHORED_SELECTOR_VERSION,
    ANCHOR_RRF_K,
    ANCHOR_SCHEME_QUOTAS,
    ANCHOR_WILDCARD_SLOTS,
    clear_anchored_conditioning_cache,
    select_candidate_concepts_anchored_v2,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = REPO_ROOT / "tools" / "build_gold_adjudication_input.py"


@pytest.fixture(autouse=True)
def _clean_conditioning_cache():
    clear_anchored_conditioning_cache()
    yield
    clear_anchored_conditioning_cache()


def concept(concept_id: str, pref_label: str, *, scheme: str = "subject", alt: list[str] | None = None) -> dict:
    facet = "regulated_entity" if scheme == "epa-tsca" else "subject"
    source_vocabulary = "federal-register-thesaurus" if scheme == "subject" else scheme
    return {
        "concept_id": concept_id,
        "facet": facet,
        "source_vocabulary": source_vocabulary,
        "scheme": facet,
        "pref_label": pref_label,
        "alt_labels_json": json.dumps(alt or []),
        "definition": f"Definition of {pref_label}.",
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": "[]",
    }


def _anchored_matches(text: str, registry: list[dict]) -> list[str]:
    """Concept ids channel A matches for ``text``, best first."""
    conditioning = ontology_concepts._condition_registry(registry)
    tokens = ontology_concepts.normalize_label(text).split()
    weights = ontology_concepts._segment_term_weights(tokens, conditioning)
    indexes = ontology_concepts._anchored_channel(tokens, weights, conditioning, depth=50)
    return [conditioning.concept_ids[index] for index in indexes]


def _ngram_matches(text: str, registry: list[dict], *, depth: int = 50) -> list[str]:
    conditioning = ontology_concepts._condition_registry(registry)
    tokens = ontology_concepts.normalize_label(text).split()
    weights = ontology_concepts._segment_term_weights(tokens, conditioning)
    indexes = ontology_concepts._char_ngram_channel(weights, conditioning, depth=depth)
    return [conditioning.concept_ids[index] for index in indexes]


# --- channel A: anchoring ---------------------------------------------------


def test_alias_does_not_match_inside_a_longer_word():
    """The v1 failure mode: "Ants" scoring on the "ants" inside "pollutants"."""
    registry = [
        concept("concept_ants", "Ants"),
        concept("concept_pollution", "Water pollution"),
    ]
    text = "The permit limits pollutants and other water pollution in the harbor."
    assert _anchored_matches(text, registry) == ["concept_pollution"]

    # And the whole word does match, so the rule is anchoring, not exclusion.
    assert "concept_ants" in _anchored_matches("Fire ants are a quarantine pest.", registry)


def test_multi_token_alias_requires_adjacent_tokens():
    registry = [
        concept("concept_seq", "Poultry inspection"),
        concept("concept_other", "Grain storage"),
    ]
    assert _anchored_matches("Poultry inspection records.", registry) == ["concept_seq"]
    # Same tokens, not adjacent: token-sequence containment, not bag of words.
    assert _anchored_matches("Poultry products and carcass inspection.", registry) == []


def test_short_aliases_are_suppressed():
    """MetaMap's rule: aliases of two characters or fewer carry no evidence."""
    registry = [
        concept("concept_two", "PA", alt=["US"]),
        concept("concept_three", "Lead"),
    ]
    text = "The us office in pa reported lead exposure."
    matched = _anchored_matches(text, registry)
    assert "concept_two" not in matched
    assert "concept_three" in matched


def test_generic_alias_below_the_idf_floor_cannot_anchor():
    """An alias made only of vocabulary-generic tokens is not evidence."""
    filler = [concept(f"concept_fill_{index:02d}", f"Program administration {index}") for index in range(40)]
    registry = [
        concept("concept_generic", "Program administration"),
        concept("concept_specific", "Beryllium exposure"),
        *filler,
    ]
    text = "Program administration of beryllium exposure limits."
    matched = _anchored_matches(text, registry)
    assert "concept_specific" in matched
    assert "concept_generic" not in matched


def test_ambiguous_aliases_are_down_weighted():
    """Two concepts, same evidence strength; the ambiguous alias ranks lower."""
    shared = [concept(f"concept_shared_{index}", "Regional office") for index in range(8)]
    registry = [concept("concept_unique", "Beryllium exposure"), *shared]
    matched = _anchored_matches("The regional office set beryllium exposure limits.", registry)
    assert matched[0] == "concept_unique"
    assert set(matched[1:]) == {row["concept_id"] for row in shared}


def test_pref_label_outranks_alt_label_on_equal_evidence():
    registry = [
        concept("concept_pref", "Beryllium exposure"),
        concept("concept_alt", "Metal dust limits", alt=["Beryllium exposure control"]),
    ]
    matched = _anchored_matches("Beryllium exposure control in the workplace.", registry)
    assert matched.index("concept_pref") < matched.index("concept_alt")


# --- channel B: character 3-gram TF-IDF -------------------------------------


def test_char_ngram_channel_surfaces_a_near_match():
    """A "Student loans" concept for "student loan relief": no token sequence matches."""
    registry = [
        concept("concept_loans", "Student loans"),
        concept("concept_bridges", "Bridge construction"),
        concept("concept_poultry", "Poultry inspection"),
    ]
    text = "Applications for student loan relief under the repayment plan."
    assert _anchored_matches(text, registry) == []
    assert _ngram_matches(text, registry)[0] == "concept_loans"
    assert [row["concept_id"] for row in select_candidate_concepts_anchored_v2(text, registry)][0] == "concept_loans"


# --- fusion -----------------------------------------------------------------


def test_reciprocal_rank_fusion_uses_k_60():
    assert ANCHOR_RRF_K == 60
    fused = ontology_concepts._fuse_reciprocal_rank([[7, 3], [3, 7, 5]])
    assert fused[7] == pytest.approx(1 / 61 + 1 / 62)
    assert fused[3] == pytest.approx(1 / 62 + 1 / 61)
    assert fused[5] == pytest.approx(1 / 63)


def test_fusion_promotes_a_concept_both_channels_agree_on():
    registry = [
        concept("concept_both", "Beryllium exposure"),
        concept("concept_lexical_only", "Exposure assessment methodology"),
        concept("concept_ngram_only", "Beryllium"),
    ]
    text = "Beryllium exposure limits for the workplace."
    ranked = [row["concept_id"] for row in select_candidate_concepts_anchored_v2(text, registry)]
    assert ranked[0] == "concept_both"


# --- scheme quotas ----------------------------------------------------------


def _quota_registry(per_scheme: int = 6, *, schemes: tuple[str, ...] | None = None) -> list[dict]:
    """A registry covering the quota table, every concept matching the probe."""
    registry: list[dict] = []
    for scheme in schemes if schemes is not None else tuple(sorted(ANCHOR_SCHEME_QUOTAS)):
        for index in range(per_scheme):
            registry.append(
                concept(
                    f"concept_{scheme}_{index:02d}",
                    f"Beryllium exposure {scheme} {index:02d}",
                    scheme=scheme,
                )
            )
    return registry


def test_scheme_quotas_bound_a_dominant_scheme():
    registry = _quota_registry()
    selected = select_candidate_concepts_anchored_v2("Beryllium exposure control limits.", registry, limit=12)
    assert len(selected) == 12
    counts: dict[str, int] = {}
    for row in selected:
        vocabulary = row["source_vocabulary"]
        counts[vocabulary] = counts.get(vocabulary, 0) + 1
    for scheme, quota in ANCHOR_SCHEME_QUOTAS.items():
        # Quota plus, at most, the wildcard slots a scheme can also win.
        assert quota <= counts.get(scheme, 0) <= quota + ANCHOR_WILDCARD_SLOTS
    assert sum(ANCHOR_SCHEME_QUOTAS.values()) + ANCHOR_WILDCARD_SLOTS == 12


def test_a_scheme_with_no_candidates_cedes_its_slots_to_the_wildcard_pool():
    registry = _quota_registry()
    # epa-tsca is present in the registry (so the quota table still applies)
    # but nothing in it can match the probe text.
    registry = [row for row in registry if row["source_vocabulary"] != "epa-tsca"]
    registry += [concept(f"concept_epa_{index}", f"Xylene isomer {index}", scheme="epa-tsca") for index in range(3)]

    selected = select_candidate_concepts_anchored_v2("Beryllium exposure control limits.", registry, limit=12)
    assert len(selected) == 12
    schemes = [row["source_vocabulary"] for row in selected]
    assert "epa-tsca" not in schemes
    # The ceded slot went to the wildcard pool, not to a shorter list.
    assert sum(1 for scheme in schemes if scheme == "fast-topical") >= ANCHOR_SCHEME_QUOTAS["fast-topical"]


def test_registry_without_the_quota_schemes_falls_back_to_fused_ranking():
    """The 901-row single-scheme registry must still work."""
    registry = _quota_registry(per_scheme=20, schemes=("subject",))
    selected = select_candidate_concepts_anchored_v2("Beryllium exposure control limits.", registry, limit=12)
    assert len(selected) == 12
    assert {row["facet"] for row in selected} == {"subject"}


def test_limit_below_the_quota_total_falls_back_to_fused_ranking():
    registry = _quota_registry()
    selected = select_candidate_concepts_anchored_v2("Beryllium exposure control limits.", registry, limit=4)
    assert len(selected) == 4


# --- contract ---------------------------------------------------------------


def test_selection_is_deterministic_and_leaves_the_registry_alone():
    registry = _quota_registry()
    snapshot = json.dumps(registry, sort_keys=True)
    text = "Beryllium exposure control limits."

    first = select_candidate_concepts_anchored_v2(text, registry, limit=12)
    second = select_candidate_concepts_anchored_v2(text, registry, limit=12)
    # A separate list object: recomputed conditioning, not a cache hit.
    third = select_candidate_concepts_anchored_v2(text, [dict(row) for row in registry], limit=12)

    assert first == second == third
    assert json.dumps(registry, sort_keys=True) == snapshot


def test_each_candidate_carries_the_selector_version():
    registry = _quota_registry()
    selected = select_candidate_concepts_anchored_v2("Beryllium exposure control limits.", registry, limit=12)
    assert ANCHORED_SELECTOR_VERSION == "anchored-hybrid-v2"
    assert {row["selector_version"] for row in selected} == {ANCHORED_SELECTOR_VERSION}


def test_same_label_authority_concepts_remain_separately_selectable() -> None:
    registry = [
        concept(
            "concept_fast_rights",
            "Civil rights",
            scheme="fast-topical",
        ),
        concept(
            "concept_crs_rights",
            "Civil rights",
            scheme="crs-subjects",
        ),
        concept(
            "concept_water",
            "Water quality",
            scheme="crs-subjects",
        ),
    ]
    selected = select_candidate_concepts_anchored_v2(
        "Civil rights and water quality.",
        registry,
        allowed_facets=("subject",),
        limit=3,
    )
    assert [row["pref_label"] for row in selected].count("Civil rights") == 2
    assert {row["concept_id"] for row in selected if row["pref_label"] == "Civil rights"} == {
        "concept_fast_rights",
        "concept_crs_rights",
    }
    assert {row["source_vocabulary"] for row in selected if row["pref_label"] == "Civil rights"} == {
        "fast-topical",
        "crs-subjects",
    }


def test_regulated_entity_homonyms_with_different_cas_ids_stay_distinct() -> None:
    first = concept("concept_chemical_a", "Example substance", scheme="epa-tsca")
    second = concept("concept_chemical_b", "Example substance", scheme="epa-tsca")
    first["external_ids_json"] = '[{"scheme":"cas","value":"71-43-2"}]'
    second["external_ids_json"] = '[{"scheme":"cas","value":"50-00-0"}]'
    selected = select_candidate_concepts_anchored_v2(
        "Example substance exposure.",
        [first, second],
        allowed_facets=("regulated_entity",),
        limit=2,
    )
    assert {row["concept_id"] for row in selected} == {
        "concept_chemical_a",
        "concept_chemical_b",
    }


def test_deprecated_concepts_are_never_selected():
    registry = [
        concept("concept_live", "Beryllium exposure"),
        {**concept("concept_dead", "Beryllium exposure limits"), "status": "deprecated"},
    ]
    selected = select_candidate_concepts_anchored_v2("Beryllium exposure limits.", registry)
    assert [row["concept_id"] for row in selected] == ["concept_live"]


def test_v1_selector_is_untouched_by_v2():
    """v2 is additive: v1 keeps its signature and its scheme gate."""
    registry = [
        concept("concept_a", "Beryllium exposure"),
        {
            **concept("concept_b", "Grain storage"),
            "facet": None,
            "source_vocabulary": None,
            "scheme": "other",
        },
    ]
    selected = ontology_concepts.select_candidate_concepts_for_text("Beryllium exposure.", ["subject"], registry)
    assert [row["concept_id"] for row in selected] == ["concept_a"]
    assert all("selector_version" not in row for row in selected)


def test_conditioning_is_cached_per_registry():
    registry = _quota_registry()
    select_candidate_concepts_anchored_v2("Beryllium exposure.", registry)
    first = ontology_concepts._condition_registry(registry)
    second = ontology_concepts._condition_registry(registry)
    assert first is second
    clear_anchored_conditioning_cache()
    assert ontology_concepts._condition_registry(registry) is not first


# --- builder wiring ---------------------------------------------------------


@pytest.fixture(scope="module")
def builder():
    spec = importlib.util.spec_from_file_location("build_gold_adjudication_input_v2", BUILDER_PATH)
    assert spec and spec.loader, f"could not load {BUILDER_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def adjudication_inputs():
    unit = ExtractionUnit(
        unit_id="processing_segment_synthetic",
        input={
            "subject": {
                "type": "cfr_section",
                "id": "SYNTHETIC-1",
                "profile": "cfr-section-v1",
                "source_table": "cfr_sections",
                "allowed_schemes": ["subject"],
                "artifact_digest": "digest-one",
            },
            "processing_segment": {
                "segment_id": "processing_segment_synthetic",
                "ordinal": 0,
                "segment_count": 1,
                "policy": "test-v1",
                "tokenizer": "test",
                "tokenizer_version": "1",
                "token_count": 5,
                "source_spans": {
                    "evidence_0": {
                        "source_field": "cfr_sections.xml_text",
                        "start_char": 0,
                        "end_char": 34,
                    }
                },
            },
            "non_evidentiary_context": {"artifact_context": {"artifact_title": "T"}, "headings": []},
            "untrusted_evidence_fields": {"fields": {"evidence_0": "Beryllium exposure control limits."}},
            "available_concepts": [],
        },
    )
    answers = {
        "artifacts": [
            {
                "profile_id": "cfr-section-v1",
                "subject_type": "cfr_section",
                "subject_id": "SYNTHETIC-1",
                "artifact_digest": "digest-one",
                "expected_tags": [
                    {
                        "gold_id": "gold_synthetic",
                        "scheme": "subject",
                        "label": "beryllium exposure",
                        "concept_id": "concept_subject_00",
                        "source_field": "cfr_sections.xml_text",
                        "start_char": 0,
                        "end_char": 18,
                        "exact_text": "Beryllium exposure",
                        "coordinate_resolution": "provided-offsets",
                        "containing_segment_ids": ["processing_segment_synthetic"],
                    }
                ],
            }
        ]
    }
    return answers, [unit], _quota_registry()


def test_builder_defaults_to_the_production_selector(builder, adjudication_inputs):
    answers, units, registry = adjudication_inputs
    document = builder.build_document(
        answers=answers,
        units=units,
        registry_rows=registry,
        file_metadata={},
        generated_at="2026-07-27T00:00:00+00:00",
    )
    selector = document["candidate_selector"]
    assert selector["selector"] == "lexical-overlap-v1"
    assert selector["method"] == "lexical-overlap-v1-limit-12"
    assert selector["payload_parity_applicable"] is True
    assert all("selector_version" not in row for row in document["items"][0]["candidates"])


def test_builder_records_the_v2_selector_choice(builder, adjudication_inputs):
    answers, units, registry = adjudication_inputs
    document = builder.build_document(
        answers=answers,
        units=units,
        registry_rows=registry,
        file_metadata={},
        generated_at="2026-07-27T00:00:00+00:00",
        selector="anchored-hybrid-v2",
    )
    selector = document["candidate_selector"]
    assert selector["selector"] == "anchored-hybrid-v2"
    assert selector["method"] == "anchored-hybrid-v2-limit-12"
    assert selector["function"].endswith("select_candidate_concepts_anchored_v2")
    # Parity is against a payload built by v1, so it is not claimed either way.
    assert selector["payload_parity_applicable"] is False
    assert selector["payload_parity_checked"] == 0
    assert selector["payload_parity_mismatches"] == []
    assert document["items"][0]["segment_context"][0]["selector_matches_payload"] is None

    candidates = document["items"][0]["candidates"]
    assert {row["selector_version"] for row in candidates} == {"anchored-hybrid-v2"}
    # The semantic facet stays ``subject`` while authority vocabularies vary.
    assert {row["scheme"] for row in candidates} == {"subject"}
    assert {row["source_vocabulary"] for row in candidates} > {"federal-register-thesaurus"}


def test_builder_binds_both_selectors_by_identity(builder):
    assert builder.SELECT_CANDIDATES is ontology_concepts.select_candidate_concepts_for_text
    assert builder.SELECT_CANDIDATES_V2 is ontology_concepts.select_candidate_concepts_anchored_v2
    assert builder.SELECTOR_CHOICES == ("lexical-overlap-v1", "anchored-hybrid-v2")


def test_builder_rejects_an_unknown_selector(builder, adjudication_inputs):
    answers, units, registry = adjudication_inputs
    with pytest.raises(builder.GoldAdjudicationError):
        builder.build_document(
            answers=answers,
            units=units,
            registry_rows=registry,
            file_metadata={},
            generated_at="2026-07-27T00:00:00+00:00",
            selector="anchored-hybrid-v3",
        )

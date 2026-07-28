"""Hermetic tests for the candidate-selector ablation harness.

Everything runs on a tiny synthetic registry with no files, no provider, and no
model weights. The property that matters most is parity: the configuration the
harness calls ``v2`` must be v2. It is asserted against the public selector
rather than by reading the harness's arithmetic, so a future change to either
side that breaks the comparison fails here.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.ontology.candidate_channels import build_dense_concept_index
from spicy_regs.ontology.concepts import (
    ANCHOR_QUOTA_TOTAL,
    ANCHOR_SOURCE_VOCABULARY_QUOTAS,
    _condition_registry,
    clear_anchored_conditioning_cache,
    select_candidate_concepts_anchored_v2,
)
from spicy_regs.ontology.concept_dimensions import concept_facet

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = REPO_ROOT / "tools" / "ablate_candidate_selectors.py"

SEGMENT_ONE = "processing_segment_one"
SEGMENT_TWO = "processing_segment_two"
LIMIT = 12


def _load_harness():
    spec = importlib.util.spec_from_file_location("ablate_candidate_selectors", HARNESS_PATH)
    assert spec and spec.loader, f"could not load {HARNESS_PATH}"
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: the harness declares dataclasses under
    # ``from __future__ import annotations``, and ``dataclasses`` resolves those
    # string annotations through ``sys.modules[cls.__module__]``.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load_harness()


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_anchored_conditioning_cache()
    yield
    clear_anchored_conditioning_cache()


# --------------------------------------------------------------------------
# synthetic inputs
# --------------------------------------------------------------------------


def _concept(
    concept_id: str,
    source_vocabulary: str,
    pref: str,
    alt: list[str] | None = None,
    *,
    facet: str | None = None,
) -> dict:
    resolved_facet = facet or ("regulated_entity" if source_vocabulary == "epa-tsca" else "subject")
    return {
        "concept_id": concept_id,
        "facet": resolved_facet,
        "source_vocabulary": source_vocabulary,
        # New rows keep the compatibility field equal to the semantic facet.
        "scheme": resolved_facet,
        "pref_label": pref,
        "alt_labels_json": json.dumps(alt or []),
        "definition": f"Concept covering {pref}.",
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": "[]",
        "actor_id": "test",
        "run_id": "test-run",
        "method": "deterministic",
        "asserted_at": "2026-07-27T00:00:00Z",
    }


@pytest.fixture
def registry_rows() -> list[dict]:
    """A registry covering every quota scheme, so the quota rule engages."""
    rows = [
        _concept("concept_mining", "federal-register-thesaurus", "Surface mining"),
        _concept("concept_runoff", "federal-register-thesaurus", "Surface runoff"),
        _concept("concept_reclaim", "federal-register-thesaurus", "Mine reclamation"),
        _concept("concept_water", "spicy-regs-local", "Water quality"),
        _concept("concept_fish", "spicy-regs-local", "Fishery management", ["Fisheries management"]),
        _concept("concept_coal", "crs-subjects", "Coal mining"),
        _concept("concept_permits", "crs-subjects", "Mining permits"),
        _concept("concept_streams", "crs-subjects", "Stream protection"),
        _concept("concept_energy", "crs-policy-areas", "Energy"),
        _concept("concept_environment", "crs-policy-areas", "Environmental Protection"),
        _concept("concept_selenium", "epa-tsca", "Selenium compounds"),
        _concept("concept_sulfate", "epa-tsca", "Sulfate salts"),
        _concept("concept_slope", "fast-topical", "Strip mining--Steep slopes"),
        _concept("concept_spoil", "fast-topical", "Mine spoil"),
        _concept("concept_hydrology", "fast-topical", "Hydrology"),
        _concept("concept_retired", "fast-topical", "Retired topic"),
    ]
    rows[-1]["status"] = "deprecated"
    return rows


def _unit(unit_id: str, text: str) -> ExtractionUnit:
    return ExtractionUnit(
        unit_id=unit_id,
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
                "segment_id": unit_id,
                "ordinal": 0,
                "segment_count": 2,
                "policy": "structure-overlap-v1",
            },
            "non_evidentiary_context": {"artifact_context": {"artifact_title": "Mining rule"}, "headings": []},
            "untrusted_evidence_fields": {"fields": {"evidence_0": text}},
            "available_concepts": [],
        },
    )


@pytest.fixture
def units() -> list[ExtractionUnit]:
    return [
        _unit(SEGMENT_ONE, "Surface mining operations must control surface runoff and protect water quality."),
        _unit(SEGMENT_TWO, "Fisheries management plans address stream protection near mine spoil."),
    ]


@pytest.fixture
def units_by_id(units) -> dict[str, ExtractionUnit]:
    return {unit.unit_id: unit for unit in units}


@pytest.fixture
def answers() -> dict:
    return {
        "artifacts": [
            {
                "profile_id": "cfr-section-v1",
                "subject_type": "cfr_section",
                "subject_id": "SYNTHETIC-1",
                "artifact_digest": "digest-one",
                "expected_tags": [
                    {
                        "gold_id": "gold_one",
                        "scheme": "subject",
                        "label": "Surface mining",
                        "containing_segment_ids": [SEGMENT_ONE],
                    },
                    {
                        "gold_id": "gold_two",
                        "scheme": "subject",
                        "label": "Fisheries management",
                        "containing_segment_ids": [SEGMENT_TWO],
                    },
                    {
                        "gold_id": "gold_three",
                        "scheme": "subject",
                        "label": "Acid mine drainage",
                        "containing_segment_ids": [SEGMENT_ONE, SEGMENT_TWO, "processing_segment_absent"],
                    },
                ],
            }
        ]
    }


@pytest.fixture
def resolved() -> dict:
    return {
        "items": [
            {"item_id": "gold-adjudication-gold_one", "adequate_target": True, "best_candidate_id": "concept_mining"},
            {"item_id": "gold-adjudication-gold_two", "adequate_target": False, "best_candidate_id": "concept_fish"},
            {"item_id": "gold-adjudication-gold_three", "adequate_target": True, "best_candidate_id": None},
        ]
    }


@dataclass(frozen=True)
class _Vectors:
    vectors: tuple[tuple[float, ...], ...]


class FakeDenseEmbedder:
    """Deterministic bag-of-words embedder, seeded from a stable digest."""

    dimensions = 256
    model_id = "fake-embedder:v1"

    def embed(self, texts: Any) -> _Vectors:
        return _Vectors(vectors=tuple(self._vector(str(text)) for text in texts))

    def _vector(self, text: str) -> tuple[float, ...]:
        slots = [0.0] * self.dimensions
        for token in re.findall(r"[a-z0-9]+", text.casefold()):
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            slots[int.from_bytes(digest[:4], "big") % self.dimensions] += 1.0
        norm = sum(value * value for value in slots) ** 0.5
        return tuple(value / norm for value in slots) if norm else tuple(slots)


@pytest.fixture
def dense_mapper(registry_rows):
    from spicy_regs.ontology.candidate_channels import DenseConceptMapper

    embedder = FakeDenseEmbedder()
    return DenseConceptMapper(index=build_dense_concept_index(registry_rows, embedder=embedder), embedder=embedder)


def _channels(harness, *, units_by_id, registry_rows, wanted, dense_mapper=None, keywords=()):
    conditioning = _condition_registry(registry_rows)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}
    return conditioning, {
        segment_id: harness.segment_channels(
            unit=unit,
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id=index_by_id,
            wanted=wanted,
            dense_mapper=dense_mapper,
            keywords=keywords,
            limit=LIMIT,
        )
        for segment_id, unit in units_by_id.items()
    }


# --------------------------------------------------------------------------
# gold items and targets
# --------------------------------------------------------------------------


def test_alias_index_covers_pref_and_alt_labels(harness, registry_rows):
    index = harness.alias_index(registry_rows)
    assert index["fisheries management"] == ["concept_fish"]
    assert index["fishery management"] == ["concept_fish"]


def test_adequate_concepts_keeps_only_graded_targets(harness, resolved):
    assert harness.adequate_concepts(resolved) == {"gold-adjudication-gold_one": "concept_mining"}


def test_gold_items_attach_mechanical_and_graded_targets(harness, answers, units_by_id, registry_rows, resolved):
    items = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate=harness.adequate_concepts(resolved),
    )
    by_id = {item.gold_id: item for item in items}
    assert by_id["gold_one"].exact_alias_ids == ("concept_mining",)
    assert by_id["gold_two"].exact_alias_ids == ("concept_fish",)
    assert by_id["gold_three"].exact_alias_ids == ()
    assert by_id["gold_one"].adequate_concept_id == "concept_mining"
    assert by_id["gold_two"].adequate_concept_id is None


def test_gold_items_drop_segments_with_no_stored_unit(harness, answers, units_by_id, registry_rows, resolved):
    items = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate=harness.adequate_concepts(resolved),
    )
    by_id = {item.gold_id: item for item in items}
    assert by_id["gold_three"].segment_ids == (SEGMENT_ONE, SEGMENT_TWO)


def test_gold_items_are_ordered_by_item_id(harness, answers, units_by_id, registry_rows, resolved):
    items = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate=harness.adequate_concepts(resolved),
    )
    assert [item.item_id for item in items] == sorted(item.item_id for item in items)


# --------------------------------------------------------------------------
# parity with the selectors under test
# --------------------------------------------------------------------------


def test_v2_configuration_reproduces_the_public_v2_selector(harness, registry_rows, units_by_id):
    assert LIMIT >= ANCHOR_QUOTA_TOTAL
    assert set(ANCHOR_SOURCE_VOCABULARY_QUOTAS) <= {str(row["source_vocabulary"]) for row in registry_rows}
    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A", "B"))
    for segment_id, unit in units_by_id.items():
        text = harness._segment_text(unit)
        expected = [
            str(concept["concept_id"])
            for concept in select_candidate_concepts_anchored_v2(
                text,
                registry_rows,
                limit=LIMIT,
                allowed_facets=unit.input["subject"]["allowed_schemes"],
            )
        ]
        selected, _ = harness.configuration_ranking(
            harness.CONFIGURATIONS_BY_NAME["v2"], channels[segment_id], conditioning, limit=LIMIT
        )
        assert selected == expected, segment_id


def test_v2_noquota_differs_from_v2_only_by_the_quota_step(harness, registry_rows, units_by_id):
    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A", "B"))
    segment = channels[SEGMENT_ONE]
    quota, ranked = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v2"], segment, conditioning, limit=LIMIT
    )
    plain, plain_ranked = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v2-noquota"], segment, conditioning, limit=LIMIT
    )
    assert ranked == plain_ranked
    assert plain == plain_ranked[:LIMIT]
    assert set(plain) <= set(ranked)
    assert set(quota) <= set(ranked)


def test_v1_configuration_uses_the_production_selector(harness, registry_rows, units_by_id):
    from spicy_regs.ontology.concepts import select_candidate_concepts_for_text

    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A", "B"))
    unit = units_by_id[SEGMENT_ONE]
    expected = [
        str(concept["concept_id"])
        for concept in select_candidate_concepts_for_text(
            harness._segment_text(unit), ["subject"], registry_rows, limit=LIMIT
        )
    ]
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v1"], channels[SEGMENT_ONE], conditioning, limit=LIMIT
    )
    assert selected == expected


def test_v1_only_ever_returns_the_gated_facet(harness, registry_rows, units_by_id):
    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A", "B"))
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v1"], channels[SEGMENT_ONE], conditioning, limit=LIMIT
    )
    facets = {concept_facet(row) for row in registry_rows if str(row["concept_id"]) in set(selected)}
    assert facets == {"subject"}


# --------------------------------------------------------------------------
# channels inside the harness
# --------------------------------------------------------------------------


def test_channel_c_enters_the_fusion(harness, registry_rows, units_by_id, dense_mapper):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("A", "B", "C"),
        dense_mapper=dense_mapper,
    )
    assert channels[SEGMENT_ONE].rankings["C"]
    with_dense, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v2+C"], channels[SEGMENT_ONE], conditioning, limit=LIMIT
    )
    assert len(with_dense) == LIMIT


def test_channel_d_is_empty_without_keywords(harness, registry_rows, units_by_id, dense_mapper):
    _, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("A", "B", "D"),
        dense_mapper=dense_mapper,
        keywords=(),
    )
    assert channels[SEGMENT_ONE].rankings["D"] == ()


def test_channel_d_uses_the_supplied_keywords(harness, registry_rows, units_by_id, dense_mapper):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("D",),
        dense_mapper=dense_mapper,
        keywords=("fisheries management",),
    )
    ranked = channels[SEGMENT_ONE].rankings["D"]
    assert conditioning.concept_ids[ranked[0]] == "concept_fish"


def test_channels_requiring_a_mapper_refuse_to_guess(harness, registry_rows, units_by_id):
    conditioning = _condition_registry(registry_rows)
    with pytest.raises(harness.AblationError):
        harness.segment_channels(
            unit=units_by_id[SEGMENT_ONE],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id={},
            wanted=("C",),
            dense_mapper=None,
            keywords=(),
            limit=LIMIT,
        )


def test_an_empty_segment_yields_empty_channels(harness, registry_rows, dense_mapper):
    conditioning = _condition_registry(registry_rows)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}
    channels = harness.segment_channels(
        unit=_unit("processing_segment_empty", "   "),
        registry_rows=registry_rows,
        conditioning=conditioning,
        index_by_id=index_by_id,
        wanted=("A", "B", "C", "D"),
        dense_mapper=dense_mapper,
        keywords=(),
        limit=LIMIT,
    )
    assert all(ranking == () for ranking in channels.rankings.values())
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v2+C+D"], channels, conditioning, limit=LIMIT
    )
    assert selected == []


def test_unknown_concept_ids_are_skipped_rather_than_guessed(harness):
    assert harness._ids_to_indices(["a", "ghost", "b", "a"], {"a": 3, "b": 1}) == (3, 1)


# --------------------------------------------------------------------------
# merging, measurement, rendering
# --------------------------------------------------------------------------


def test_merge_takes_the_union_ordered_by_best_rank(harness):
    merged = harness.merge_across_segments([["a", "b", "c"], ["d", "b"]], limit=3)
    assert merged == ["a", "d", "b"]


def test_merge_of_one_segment_is_that_segment(harness):
    assert harness.merge_across_segments([["a", "b"]], limit=5) == ["a", "b"]


def test_merge_respects_the_limit(harness):
    assert harness.merge_across_segments([["a", "b", "c", "d"]], limit=2) == ["a", "b"]


def test_measurement_counts_targets_ranks_and_scheme_mix(harness, registry_rows, units_by_id, answers, resolved):
    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A", "B"))
    items = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate=harness.adequate_concepts(resolved),
    )
    source_vocabulary_by_id = {
        concept_id: conditioning.source_vocabularies[index] for index, concept_id in enumerate(conditioning.concept_ids)
    }
    result = harness.measure_configuration(
        harness.CONFIGURATIONS_BY_NAME["v2"],
        items=items,
        channels_by_segment=channels,
        conditioning=conditioning,
        source_vocabulary_by_id=source_vocabulary_by_id,
        limit=LIMIT,
    )
    assert result["item_count"] == 3
    assert result["exact_alias_target_count"] == 2
    assert result["exact_alias_surfaced"] == 2
    assert result["exact_alias_surfaced_labels"] == ["Fisheries management", "Surface mining"]
    assert result["exact_alias_missed_labels"] == []
    assert result["adequate_target_count"] == 1
    assert result["adequate_kept"] == 1
    assert result["surfaced_rank_median"] is not None
    assert sum(result["source_vocabulary_mix"].values()) == result["candidate_slots"]
    assert set(result["source_vocabulary_mix"]) <= {str(row["source_vocabulary"]) for row in registry_rows}


def test_measurement_reports_a_target_that_never_surfaces(harness, registry_rows, units_by_id, answers, resolved):
    """A configuration whose channels retrieve nothing reports 0 surfaced, not a crash."""
    conditioning, channels = _channels(harness, units_by_id=units_by_id, registry_rows=registry_rows, wanted=("A",))
    items = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate=harness.adequate_concepts(resolved),
    )
    empty = harness.Configuration("empty", ("C",), False, "nothing retrieved")
    result = harness.measure_configuration(
        empty,
        items=items,
        channels_by_segment=channels,
        conditioning=conditioning,
        source_vocabulary_by_id={},
        limit=LIMIT,
    )
    assert result["exact_alias_surfaced"] == 0
    assert result["surfaced_rank_mean"] is None
    assert result["adequate_kept"] == 0


def test_markdown_table_names_every_configuration(harness):
    results = [
        {
            "configuration": "v2",
            "channels": ["A", "B"],
            "quotas": True,
            "exact_alias_surfaced": 4,
            "exact_alias_target_count": 8,
            "adequate_kept": 3,
            "adequate_target_count": 5,
            "surfaced_rank_mean": 4.5,
            "surfaced_rank_median": 4,
            "candidate_slots": 420,
            "source_vocabulary_mix": {"fast-topical": 210, "spicy-regs-local": 210},
            "exact_alias_surfaced_labels": ["medicaid"],
            "exact_alias_missed_labels": ["free speech"],
        }
    ]
    table = harness.markdown_table(results)
    assert "| v2 | A+B | yes | 4/8 | 3/5 | 4.5 | 4 |" in table
    assert "free speech" in table


def test_run_ablation_rejects_an_unknown_configuration(harness, tmp_path):
    with pytest.raises(harness.AblationError):
        harness.run_ablation(
            dataset_dir=tmp_path,
            selection_file=tmp_path / "selection.parquet",
            registry_file=tmp_path / "registry.parquet",
            resolved_file=tmp_path / "resolved.json",
            index_dir=tmp_path,
            output_dir=tmp_path,
            configuration_names=["v2", "v9-imaginary"],
        )


def test_every_declared_configuration_is_addressable(harness):
    assert [configuration.name for configuration in harness.CONFIGURATIONS] == list(harness.CONFIGURATIONS_BY_NAME)

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
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.ontology.candidate_channels import BM25ConceptMapper, build_dense_concept_index
from spicy_regs.ontology.common import read_parquet_rows, write_parquet_rows
from spicy_regs.ontology.concepts import (
    ANCHOR_QUOTA_TOTAL,
    ANCHOR_SOURCE_VOCABULARY_QUOTAS,
    CONCEPT_COLUMNS,
    _condition_registry,
    clear_anchored_conditioning_cache,
    select_candidate_concepts_anchored_v2,
)
from spicy_regs.ontology.concept_dimensions import concept_facet
from tests.managed_release_support import build_selected_managed_bundle

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


def _build_managed_bundle(root: Path) -> tuple[dict[str, Any], Path]:
    return build_selected_managed_bundle(root)


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
                "source_spans": {
                    "evidence_0": {
                        "source_field": "synthetic.text",
                        "start_char": 0,
                        "end_char": len(text),
                    }
                },
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
    max_input_tokens = 8

    def model_token_count(self, text: str) -> int:
        return len(re.findall(r"[a-z0-9]+", text.casefold())) + 2

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


@pytest.fixture
def bm25_mapper(registry_rows):
    return BM25ConceptMapper.build(registry_rows)


def _channels(
    harness,
    *,
    units_by_id,
    registry_rows,
    wanted,
    dense_mapper=None,
    bm25_mapper=None,
    keywords=(),
):
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
            bm25_mapper=bm25_mapper,
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


def test_reviewed_targets_are_never_rebound_into_a_new_candidate_universe(
    harness,
    resolved,
):
    bound, foreign = harness.bind_reviewed_adequate_targets(
        resolved,
        candidate_ids={"urn:test:managed:surface-mining"},
    )

    assert bound == {}
    assert foreign == [
        {
            "item_id": "gold-adjudication-gold_one",
            "concept_id": "concept_mining",
            "reason": "notExactMemberOfCandidateUniverse",
        }
    ]


def _managed_target_document(
    harness,
    *,
    support: dict[str, Any],
    manifest_digest: str,
    item_id: str,
    gold_id: str,
    label: str,
) -> dict[str, Any]:
    return {
        "schemaVersion": "spicy-managed-development-targets/1",
        "id": "urn:test:managed-development-targets:v1",
        "evaluationScope": "developmentOnly",
        "sourceEvidence": {
            "datasetId": harness.MANAGED_DEVELOPMENT_DATASET_ID,
            "gold": {
                "path": "gold.parquet",
                "digest": "sha256:" + "1" * 64,
                "rowCount": 1,
            },
            "selection": {
                "path": "selection.parquet",
                "digest": "sha256:" + "2" * 64,
                "rowCount": 1,
            },
        },
        "vocabularyUniverse": {
            "managedReleaseManifestDigest": manifest_digest,
            "publicationReleaseId": support["PUBLICATION_ID"],
            "referenceResourceRelease": {
                "id": support["RELEASE_ID"],
                "version": support["RELEASE_VERSION"],
                "digest": support["RELEASE_DIGEST"],
            },
            "registryImportSnapshot": {
                "id": support["IMPORT_ID"],
                "digest": support["IMPORT_DIGEST"],
            },
            "expressionCorpusSnapshot": {
                "id": support["CORPUS_ID"],
                "digest": support["CORPUS_DIGEST"],
            },
        },
        "review": {
            "status": "provisionalDevelopmentReview",
            "independentlyReviewed": False,
            "sealed": False,
        },
        "expectations": [
            {
                "itemId": item_id,
                "goldId": gold_id,
                "intendedMeaning": {"value": label, "language": "en"},
                "outcome": "represented",
                "registeredTargets": [
                    {
                        "conceptId": support["MEMBER_ID"],
                        "prefLabel": "Poultry slaughter inspection",
                        "grade": "exact",
                        "adequateForDevelopment": True,
                    }
                ],
                "rationale": "Exact preferred label.",
            }
        ],
    }


def test_managed_targets_bind_exact_members_and_release_pins(
    harness,
    tmp_path,
):
    support, manifest_path = _build_managed_bundle(tmp_path / "bundle")
    manifest_digest = "sha256:" + hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    candidate_registry = harness.load_candidate_registry(
        output_dir=tmp_path / "work",
        managed_release_manifest=manifest_path,
        managed_release_manifest_digest=manifest_digest,
    )
    item = harness.GoldItem(
        gold_id="gold_one",
        item_id="gold-adjudication-gold_one",
        label="Poultry slaughter inspection",
        scheme="subject",
        segment_ids=(SEGMENT_ONE,),
        exact_alias_ids=(support["MEMBER_ID"],),
        adequate_concept_id=None,
    )
    document = _managed_target_document(
        harness,
        support=support,
        manifest_digest=manifest_digest,
        item_id=item.item_id,
        gold_id=item.gold_id,
        label=item.label,
    )
    target_file = tmp_path / "targets.json"
    target_file.write_text(json.dumps(document))

    target_set = harness.load_managed_targets(
        target_file,
        candidate_registry=candidate_registry,
        items=[item],
        source_facts={
            "gold_sha256": "1" * 64,
            "selection_sha256": "2" * 64,
        },
        segmentation_facts={
            "gold_span_count": 1,
            "selected_segment_count": 1,
        },
    )
    attached = harness.attach_managed_targets([item], target_set)

    assert target_set.dataset_id == "urn:test:managed-development-targets:v1"
    assert attached[0].represented_target_ids == (support["MEMBER_ID"],)
    assert attached[0].adequate_target_ids == (support["MEMBER_ID"],)
    assert attached[0].not_represented is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda document: document["vocabularyUniverse"].__setitem__(
                "managedReleaseManifestDigest",
                "sha256:" + "f" * 64,
            ),
            "vocabulary universe differs",
        ),
        (
            lambda document: document["expectations"][0]["registeredTargets"][0].__setitem__(
                "conceptId", "concept_fused_old"
            ),
            "not an exact member",
        ),
        (
            lambda document: document["expectations"][0]["registeredTargets"][0].__setitem__(
                "prefLabel", "Rebound by label"
            ),
            "changed its preferred label",
        ),
        (
            lambda document: document["expectations"][0].__setitem__(
                "outcome",
                "notRepresented",
            ),
            "cannot mix notRepresented",
        ),
        (
            lambda document: document.__setitem__("expectations", []),
            "must cover the source evidence exactly",
        ),
        (
            lambda document: document["expectations"][0]["registeredTargets"][0].__setitem__(
                "grade",
                "broader",
            ),
            "unsupported grade",
        ),
        (
            lambda document: document["expectations"][0]["registeredTargets"].append(
                dict(document["expectations"][0]["registeredTargets"][0])
            ),
            "repeats managed target",
        ),
    ],
)
def test_managed_targets_reject_stale_or_rebound_answers(
    harness,
    tmp_path,
    mutation,
    message,
):
    support, manifest_path = _build_managed_bundle(tmp_path / "bundle")
    manifest_digest = "sha256:" + hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    candidate_registry = harness.load_candidate_registry(
        output_dir=tmp_path / "work",
        managed_release_manifest=manifest_path,
        managed_release_manifest_digest=manifest_digest,
    )
    item = harness.GoldItem(
        gold_id="gold_one",
        item_id="gold-adjudication-gold_one",
        label="Poultry slaughter inspection",
        scheme="subject",
        segment_ids=(SEGMENT_ONE,),
        exact_alias_ids=(support["MEMBER_ID"],),
        adequate_concept_id=None,
    )
    document = _managed_target_document(
        harness,
        support=support,
        manifest_digest=manifest_digest,
        item_id=item.item_id,
        gold_id=item.gold_id,
        label=item.label,
    )
    mutation(document)
    target_file = tmp_path / "targets.json"
    target_file.write_text(json.dumps(document))

    with pytest.raises(harness.AblationError, match=message):
        harness.load_managed_targets(
            target_file,
            candidate_registry=candidate_registry,
            items=[item],
            source_facts={
                "gold_sha256": "1" * 64,
                "selection_sha256": "2" * 64,
            },
            segmentation_facts={
                "gold_span_count": 1,
                "selected_segment_count": 1,
            },
        )


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


def test_dense_evidence_windows_restore_source_order_and_cover_every_character(harness):
    payload = json.loads(json.dumps(_unit("processing_segment_windows", "placeholder").input))
    fields = {
        "evidence_10": "Later evidence has several words and a final sentence.",
        "evidence_2": "Earlier evidence also has enough words to split cleanly.",
    }
    payload["untrusted_evidence_fields"]["fields"] = fields
    payload["processing_segment"]["source_spans"] = {
        "evidence_10": {
            "source_field": "synthetic.text",
            "start_char": 200,
            "end_char": 200 + len(fields["evidence_10"]),
        },
        "evidence_2": {
            "source_field": "synthetic.text",
            "start_char": 100,
            "end_char": 100 + len(fields["evidence_2"]),
        },
    }
    payload["non_evidentiary_context"] = {
        "artifact_context": {"artifact_title": "A gold-label-shaped context trap"},
        "headings": ["Another context trap"],
    }
    unit = ExtractionUnit(unit_id="processing_segment_windows", input=payload)

    def counter(text: str) -> int:
        return len(re.findall(r"[a-z0-9]+", text.casefold())) + 2

    windows = harness.dense_evidence_windows(
        unit,
        token_counter=counter,
        max_input_tokens=5,
    )

    assert list(dict.fromkeys(window.field_key for window in windows)) == ["evidence_2", "evidence_10"]
    for field_key, field_text in sorted(fields.items(), key=lambda item: int(item[0].split("_")[1])):
        assert "".join(window.text for window in windows if window.field_key == field_key) == field_text
    assert all(window.model_token_count <= 5 for window in windows)
    assert all("context trap" not in window.text for window in windows)
    assert len({window.window_id for window in windows}) == len(windows)

    changed_context = json.loads(json.dumps(payload))
    changed_context["non_evidentiary_context"] = {
        "artifact_context": {"artifact_title": "Completely different"},
        "headings": ["Still ignored"],
    }
    repeated = harness.dense_evidence_windows(
        ExtractionUnit(unit_id=unit.unit_id, input=changed_context),
        token_counter=counter,
        max_input_tokens=5,
    )
    assert repeated == windows


def test_dense_evidence_windows_refuse_missing_native_token_evidence(harness):
    with pytest.raises(harness.AblationError, match="model-native counter"):
        harness.dense_evidence_windows(
            _unit("processing_segment_windows", "some evidence"),
            token_counter=None,
            max_input_tokens=512,
        )


def test_packed_dense_evidence_restores_order_and_retains_every_source_fragment(harness):
    payload = json.loads(json.dumps(_unit("processing_segment_packed", "placeholder").input))
    fields = {
        "evidence_10": "four",
        "evidence_2": "three",
        "evidence_1": "two",
        "evidence_0": "one",
    }
    payload["untrusted_evidence_fields"]["fields"] = fields
    payload["processing_segment"]["source_spans"] = {
        field_key: {
            "source_field": "synthetic.text",
            "start_char": ordinal * 10,
            "end_char": ordinal * 10 + len(text),
        }
        for field_key, text in fields.items()
        for ordinal in [int(field_key.split("_")[1])]
    }
    unit = ExtractionUnit(unit_id="processing_segment_packed", input=payload)

    windows = harness.packed_dense_evidence_windows(
        unit,
        token_counter=lambda text: len(text.split()) + 2,
        max_input_tokens=8,
    )
    field_windows = harness.dense_evidence_windows(
        unit,
        token_counter=lambda text: len(text.split()) + 2,
        max_input_tokens=8,
    )

    assert len(windows) == 1
    assert len(windows) < len(field_windows)
    assert windows[0].text == "one\ntwo\nthree\nfour"
    assert [fragment.field_key for fragment in windows[0].fragments] == [
        "evidence_0",
        "evidence_1",
        "evidence_2",
        "evidence_10",
    ]
    rebuilt: dict[str, str] = {}
    for window in windows:
        for fragment in window.fragments:
            rebuilt.setdefault(fragment.field_key, "")
            rebuilt[fragment.field_key] += window.text[fragment.query_start_char : fragment.query_end_char]
    assert rebuilt == {
        field_key: fields[field_key] for field_key in ("evidence_0", "evidence_1", "evidence_2", "evidence_10")
    }
    provenance = windows[0].score_provenance()
    assert provenance["query_representation"] == harness.DENSE_PACKED_EVIDENCE_VERSION
    assert len(provenance["query_source_fragments"]) == 4
    assert "query_source_field" not in provenance


@pytest.mark.parametrize(
    ("max_input_tokens", "expected_queries"),
    [
        (6, ["abc\n", "def"]),
        (5, ["abc", "\nde", "f"]),
    ],
)
def test_packed_dense_evidence_keeps_lf_boundaries_with_source_provenance(
    harness,
    max_input_tokens,
    expected_queries,
):
    payload = json.loads(json.dumps(_unit("processing_segment_packed_boundary", "placeholder").input))
    fields = {
        "evidence_0": "abc",
        "evidence_1": "def",
    }
    payload["untrusted_evidence_fields"]["fields"] = fields
    payload["processing_segment"]["source_spans"] = {
        "evidence_0": {
            "source_field": "synthetic.text",
            "start_char": 10,
            "end_char": 13,
        },
        "evidence_1": {
            "source_field": "synthetic.text",
            "start_char": 20,
            "end_char": 23,
        },
    }
    unit = ExtractionUnit(unit_id="processing_segment_packed_boundary", input=payload)

    windows = harness.packed_dense_evidence_windows(
        unit,
        token_counter=lambda text: len(text) + 2,
        max_input_tokens=max_input_tokens,
    )

    assert [window.text for window in windows] == expected_queries
    assert "".join(window.text for window in windows) == "abc\ndef"
    rebuilt: dict[str, str] = {}
    for window in windows:
        assert window.fragments
        for fragment in window.fragments:
            rebuilt.setdefault(fragment.field_key, "")
            rebuilt[fragment.field_key] += window.text[fragment.query_start_char : fragment.query_end_char]
    assert rebuilt == fields


def test_packed_dense_evidence_omits_valid_empty_fields_without_extra_separators(harness):
    payload = json.loads(json.dumps(_unit("processing_segment_packed_empty", "placeholder").input))
    fields = {
        "evidence_0": "",
        "evidence_1": "alpha",
        "evidence_2": "",
        "evidence_3": "beta",
        "evidence_4": "",
    }
    payload["untrusted_evidence_fields"]["fields"] = fields
    payload["processing_segment"]["source_spans"] = {
        field_key: {
            "source_field": "synthetic.text",
            "start_char": ordinal * 10,
            "end_char": ordinal * 10 + len(text),
        }
        for field_key, text in fields.items()
        for ordinal in [int(field_key.split("_")[1])]
    }
    unit = ExtractionUnit(unit_id="processing_segment_packed_empty", input=payload)

    windows = harness.packed_dense_evidence_windows(
        unit,
        token_counter=lambda text: len(text.split()) + 2,
        max_input_tokens=8,
    )

    assert [window.text for window in windows] == ["alpha\nbeta"]
    assert [fragment.field_key for fragment in windows[0].fragments] == [
        "evidence_1",
        "evidence_3",
    ]

    all_empty = json.loads(json.dumps(payload))
    all_empty["untrusted_evidence_fields"]["fields"] = {field_key: "" for field_key in fields}
    all_empty["processing_segment"]["source_spans"] = {
        field_key: {
            "source_field": "synthetic.text",
            "start_char": ordinal * 10,
            "end_char": ordinal * 10,
        }
        for field_key in fields
        for ordinal in [int(field_key.split("_")[1])]
    }
    assert (
        harness.packed_dense_evidence_windows(
            ExtractionUnit(unit_id="processing_segment_packed_all_empty", input=all_empty),
            token_counter=lambda text: len(text.split()) + 2,
            max_input_tokens=8,
        )
        == ()
    )


def test_dense_window_query_set_records_complete_zero_truncation_coverage(
    harness,
    units_by_id,
    dense_mapper,
):
    by_segment, facts = harness._dense_evidence_window_set(
        units_by_id=units_by_id,
        segment_ids=sorted(units_by_id),
        mapper=dense_mapper,
    )

    assert set(by_segment) == set(units_by_id)
    assert facts["version"] == harness.DENSE_EVIDENCE_WINDOW_VERSION
    assert facts["query_count"] == sum(len(windows) for windows in by_segment.values())
    assert facts["queries_truncated"] == 0
    assert facts["source_character_count"] == facts["covered_character_count"]
    assert facts["query_set_digest"].startswith("sha256:")


def test_packed_dense_query_set_records_complete_zero_truncation_coverage(
    harness,
    units_by_id,
    dense_mapper,
):
    by_segment, facts = harness._packed_dense_evidence_window_set(
        units_by_id=units_by_id,
        segment_ids=sorted(units_by_id),
        mapper=dense_mapper,
    )

    assert set(by_segment) == set(units_by_id)
    assert facts["version"] == harness.DENSE_PACKED_EVIDENCE_VERSION
    assert facts["queries_truncated"] == 0
    assert facts["source_character_count"] == facts["covered_source_character_count"]
    assert facts["query_character_count"] == (facts["source_character_count"] + facts["separator_character_count"])
    assert facts["metadata_model_token_budget"] == 0
    assert facts["evidence_model_token_ceiling"] == dense_mapper.embedder.max_input_tokens
    assert facts["evidence_model_token_ceiling_includes_special_tokens"] is True
    assert facts["overlap_model_token_budget"] == 0
    assert facts["empty_source_field_count"] == 0
    assert facts["empty_source_field_policy"] == "validatedZeroWidthFieldsOmittedFromQueries"
    assert facts["query_set_digest"].startswith("sha256:")


def test_dense_window_ranking_max_pools_scores_and_keeps_the_winning_query(harness):
    windows = harness.dense_evidence_windows(
        _unit("processing_segment_windows", "surface mining fisheries management"),
        token_counter=lambda text: len(text.split()) + 2,
        max_input_tokens=4,
    )

    class Mapper:
        def rank(self, queries, *, depth):
            assert queries == [window.text for window in windows]
            assert depth == 12
            return [
                [("concept_b", 0.8), ("concept_a", 0.7)],
                [("concept_a", 0.9), ("concept_b", 0.8)],
            ]

    ranking, provenance = harness._window_scored_ranking(
        windows,
        mapper=Mapper(),
        depth=12,
    )

    assert ranking == [("concept_a", 0.9), ("concept_b", 0.8)]
    assert provenance["concept_a"]["query_window_id"] == windows[1].window_id
    assert provenance["concept_b"]["query_window_id"] == min(window.window_id for window in windows)


def test_channel_c_enters_the_fusion(harness, registry_rows, units_by_id, dense_mapper):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("A", "B", "C"),
        dense_mapper=dense_mapper,
    )
    assert channels[SEGMENT_ONE].rankings["C"]
    first_index = channels[SEGMENT_ONE].rankings["C"][0]
    first_concept = conditioning.concept_ids[first_index]
    assert isinstance(
        channels[SEGMENT_ONE].score_values["C"][first_concept],
        float,
    )
    assert channels[SEGMENT_ONE].score_kinds["C"] == "nativeMapperScore"
    with_dense, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["v2+C"], channels[SEGMENT_ONE], conditioning, limit=LIMIT
    )
    assert len(with_dense) == LIMIT


def test_channel_cw_coexists_with_whole_segment_dense_and_retains_query_provenance(
    harness,
    registry_rows,
    units_by_id,
    dense_mapper,
):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("C", "Cw"),
        dense_mapper=dense_mapper,
    )
    segment = channels[SEGMENT_ONE]
    assert segment.rankings["C"]
    assert segment.rankings["Cw"]
    first_id = conditioning.concept_ids[segment.rankings["Cw"][0]]
    provenance = segment.score_provenance["Cw"][first_id]
    assert segment.score_kinds["Cw"] == "maxWindowNativeMapperScore"
    assert provenance["query_representation"] == harness.DENSE_EVIDENCE_WINDOW_VERSION
    assert provenance["query_model_token_count"] <= dense_mapper.embedder.max_input_tokens
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["Cw-alone"],
        segment,
        conditioning,
        limit=LIMIT,
    )
    assert selected


def test_channel_cp_keeps_multi_fragment_query_provenance(
    harness,
    registry_rows,
    dense_mapper,
):
    payload = json.loads(json.dumps(_unit(SEGMENT_ONE, "placeholder").input))
    fields = {
        "evidence_1": "fisheries management",
        "evidence_0": "surface mining",
    }
    payload["untrusted_evidence_fields"]["fields"] = fields
    payload["processing_segment"]["source_spans"] = {
        "evidence_0": {
            "source_field": "synthetic.text",
            "start_char": 0,
            "end_char": len(fields["evidence_0"]),
        },
        "evidence_1": {
            "source_field": "synthetic.text",
            "start_char": 20,
            "end_char": 20 + len(fields["evidence_1"]),
        },
    }
    unit = ExtractionUnit(unit_id=SEGMENT_ONE, input=payload)
    conditioning = _condition_registry(registry_rows)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}

    segment = harness.segment_channels(
        unit=unit,
        registry_rows=registry_rows,
        conditioning=conditioning,
        index_by_id=index_by_id,
        wanted=("Cp",),
        dense_mapper=dense_mapper,
        bm25_mapper=None,
        keywords=(),
        limit=LIMIT,
    )

    assert segment.rankings["Cp"]
    first_id = conditioning.concept_ids[segment.rankings["Cp"][0]]
    provenance = segment.score_provenance["Cp"][first_id]
    assert segment.score_kinds["Cp"] == "maxPackedWindowNativeMapperScore"
    assert provenance["query_representation"] == harness.DENSE_PACKED_EVIDENCE_VERSION
    assert len(provenance["query_source_fragments"]) == 2
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["Cp-alone"],
        segment,
        conditioning,
        limit=LIMIT,
    )
    assert selected


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
    concept_id = conditioning.concept_ids[ranked[0]]
    assert concept_id == "concept_fish"
    assert isinstance(channels[SEGMENT_ONE].score_values["D"][concept_id], float)
    assert channels[SEGMENT_ONE].score_kinds["D"] == "bestKeywordNativeMapperScore"


def test_channel_e_enters_the_fusion(harness, registry_rows, units_by_id, bm25_mapper):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("B", "E"),
        bm25_mapper=bm25_mapper,
    )
    assert channels[SEGMENT_ONE].rankings["E"]
    first_index = channels[SEGMENT_ONE].rankings["E"][0]
    first_concept = conditioning.concept_ids[first_index]
    assert channels[SEGMENT_ONE].score_values["E"][first_concept] > 0.0
    assert channels[SEGMENT_ONE].score_kinds["E"] == "nativeMapperScore"
    selected, _ = harness.configuration_ranking(
        harness.CONFIGURATIONS_BY_NAME["BM25+B"],
        channels[SEGMENT_ONE],
        conditioning,
        limit=LIMIT,
    )
    assert 0 < len(selected) <= LIMIT
    assert "concept_mining" in selected


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
            bm25_mapper=None,
            keywords=(),
            limit=LIMIT,
        )
    with pytest.raises(harness.AblationError):
        harness.segment_channels(
            unit=units_by_id[SEGMENT_ONE],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id={},
            wanted=("Cp",),
            dense_mapper=None,
            bm25_mapper=None,
            keywords=(),
            limit=LIMIT,
        )
    with pytest.raises(harness.AblationError):
        harness.segment_channels(
            unit=units_by_id[SEGMENT_ONE],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id={},
            wanted=("Cw",),
            dense_mapper=None,
            bm25_mapper=None,
            keywords=(),
            limit=LIMIT,
        )
    with pytest.raises(harness.AblationError):
        harness.segment_channels(
            unit=units_by_id[SEGMENT_ONE],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id={},
            wanted=("E",),
            dense_mapper=None,
            bm25_mapper=None,
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
        wanted=("A", "B", "C", "Cw", "Cp", "D", "E"),
        dense_mapper=dense_mapper,
        bm25_mapper=BM25ConceptMapper.build(registry_rows),
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


def test_stored_keyword_input_is_retained_and_content_addressed(
    harness,
    tmp_path,
    units_by_id,
):
    keywords_file = tmp_path / "stored-keywords.json"
    keywords_file.write_text(
        json.dumps(
            {
                "keywords_by_segment": {
                    SEGMENT_ONE: ["surface mining", "water quality"],
                }
            },
            indent=2,
        )
        + "\n"
    )

    keywords, calls, facts = harness._resolve_keywords(
        units_by_id=units_by_id,
        segment_ids=[SEGMENT_ONE],
        generate=False,
        keywords_file=keywords_file,
        output_dir=tmp_path / "run",
    )

    payload = {SEGMENT_ONE: ["surface mining", "water quality"]}
    expected_digest = "sha256:" + hashlib.sha256(harness.canonical_json(payload).encode("utf-8")).hexdigest()
    assert keywords == {
        SEGMENT_ONE: ("surface mining", "water quality"),
    }
    assert calls == []
    assert facts["keyword_content_digest"] == expected_digest
    assert facts["keywords_by_segment"] == payload
    assert facts["keywords_file_sha256"] == hashlib.sha256(keywords_file.read_bytes()).hexdigest()


def test_generated_keyword_input_is_retained_and_content_addressed(
    harness,
    tmp_path,
    units_by_id,
    monkeypatch,
):
    from spicy_regs.docpipeline.adapters.openai import (
        OpenAIStructuredTextModel,
    )

    class FakeModel:
        model_id = "fake-keyword-model"

    monkeypatch.setattr(
        OpenAIStructuredTextModel,
        "from_environment",
        classmethod(lambda cls: FakeModel()),
    )
    monkeypatch.setattr(
        harness,
        "generate_keywords",
        lambda **kwargs: (
            {SEGMENT_ONE: ("surface mining", "water quality")},
            [{"segment_id": SEGMENT_ONE, "status": "complete"}],
        ),
    )

    keywords, calls, facts = harness._resolve_keywords(
        units_by_id=units_by_id,
        segment_ids=[SEGMENT_ONE],
        generate=True,
        keywords_file=None,
        output_dir=tmp_path / "run",
    )

    assert keywords[SEGMENT_ONE] == ("surface mining", "water quality")
    assert calls[0]["segment_id"] == SEGMENT_ONE
    assert facts["model_id"] == "fake-keyword-model"
    assert facts["keywords_by_segment"][SEGMENT_ONE] == [
        "surface mining",
        "water quality",
    ]
    keywords_path = Path(facts["keywords_file"])
    assert facts["keywords_file_sha256"] == hashlib.sha256(keywords_path.read_bytes()).hexdigest()
    assert facts["keyword_content_digest"].startswith("sha256:")


# --------------------------------------------------------------------------
# managed candidate source
# --------------------------------------------------------------------------


def test_candidate_source_refuses_an_implicit_legacy_registry(
    harness,
    tmp_path,
):
    with pytest.raises(harness.AblationError, match="managed release is required"):
        harness.load_candidate_registry(
            output_dir=tmp_path,
            registry_file=tmp_path / "legacy.parquet",
        )


def test_candidate_source_allows_an_explicit_migration_only_registry(
    harness,
    tmp_path,
    registry_rows,
):
    registry_file = write_parquet_rows(
        tmp_path / "legacy.parquet",
        columns=CONCEPT_COLUMNS,
        rows=registry_rows,
    )
    candidate_registry = harness.load_candidate_registry(
        output_dir=tmp_path,
        registry_file=registry_file,
        allow_legacy_registry=True,
    )

    assert candidate_registry.source_facts["mode"] == "legacyFusedRegistry"
    assert candidate_registry.source_facts["migration_only"] is True
    assert candidate_registry.managed_source is None


def test_physical_dense_index_identity_excludes_query_and_requested_channel_facts(harness):
    physical = {
        "kind": "dense",
        "model_id": "dense:model@revision",
        "registry_digest": "sha256:" + ("a" * 64),
    }
    first = harness._physical_index_facts(
        {
            **physical,
            "channels": ["C"],
            "query_representations": {"C": {"queries_truncated": 35}},
        }
    )
    second = harness._physical_index_facts(
        {
            **physical,
            "channels": ["C", "Cw"],
            "query_representations": {"Cw": {"queries_truncated": 0}},
        }
    )

    assert first == second == physical


def test_managed_release_projection_and_derived_index_keep_exact_lineage(
    harness,
    tmp_path,
):
    support, manifest_path = _build_managed_bundle(tmp_path / "bundle")
    manifest_digest = "sha256:" + hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    candidate_registry = harness.load_candidate_registry(
        output_dir=tmp_path / "run",
        managed_release_manifest=manifest_path,
        managed_release_manifest_digest=manifest_digest,
        candidate_default_language="en",
    )

    member_id = support["MEMBER_ID"]
    rows_by_id = {str(row["concept_id"]): row for row in candidate_registry.rows}
    assert rows_by_id[member_id]["pref_label"] == ("Poultry slaughter inspection")
    assert rows_by_id[member_id]["source_vocabulary"] == support["SCHEME_ID"]
    assert rows_by_id[member_id]["facet"] == "subject"
    assert {row["concept_id"] for row in read_parquet_rows(candidate_registry.selector_file)} == set(rows_by_id)
    assert candidate_registry.source_facts["publication_release_id"] == (support["PUBLICATION_ID"])
    assert candidate_registry.source_facts["permission_facet_iri"] == ("urn:ref:facet:general-subject")
    assert (
        candidate_registry.source_facts["permission_assignment_role_iri"]
        == "https://rulespec.org/ns/v1#assignmentPrimary"
    )
    assert candidate_registry.source_facts["permission_resource_route"] == "document"
    assert candidate_registry.source_facts["permission_coverage_report"]["id"].endswith(":coverage-report:v1")
    assert candidate_registry.source_facts["permission_registry_deployment"]["id"].endswith(
        ":registry-deployment:development-selected:v1"
    )
    assert set(candidate_registry.source_facts["permission_required_import_features"]) == {
        "labels",
        "languages",
        "notation",
        "notes",
        "hierarchy",
        "associativeRelations",
        "mappings",
        "status",
        "replacements",
        "identifiers",
        "membership",
    }
    assert candidate_registry.source_facts["selector_facet"] == "subject"

    bm25_facts = BM25ConceptMapper.build(candidate_registry.rows).facts()
    identity, lookup_index = harness.derive_lookup_index_identity(
        candidate_registry=candidate_registry,
        configurations=[
            harness.CONFIGURATIONS_BY_NAME["BM25-alone"],
        ],
        mapper_facts={},
        bm25_facts=bm25_facts,
    )
    source_facts, lineage = harness.finalize_candidate_lineage(
        candidate_registry=candidate_registry,
        lookup_index_identity=identity,
        lookup_index_manifest=lookup_index,
        mapper_facts={},
        channels=["E"],
    )

    assert lookup_index["id"].startswith("urn:spicy-regs:lookup-index:")
    assert lookup_index["digest"].startswith("sha256:")
    assert identity["physicalStructures"]["bm25Index"]["registry_digest"] == (bm25_facts["registry_digest"])
    assert "configurations" not in identity
    assert "fusion" not in identity
    assert source_facts["lookup_index_manifest"] == lookup_index
    assert source_facts["lookup_index_identity"] == identity
    assert lineage[member_id]["member_iri"] == member_id
    assert lineage[member_id]["release_iri"] == support["RELEASE_ID"]
    assert lineage[member_id]["scheme_iri"] == support["SCHEME_ID"]
    assert lineage[member_id]["expressions"][0]["expression_id"] == (support["EXPRESSION_ID"])
    assert lineage[member_id]["expression_corpus_snapshot"] == (
        candidate_registry.managed_source.expression_corpus_snapshot
    )
    assert lineage[member_id]["lookup_index_manifest"] == lookup_index
    assert lineage[member_id]["usage_ceiling"] == "candidateUseOnly"
    assert lineage[member_id]["indexed_expression_ids_by_channel"]["E"] == [support["EXPRESSION_ID"]]
    assert lineage[member_id]["available_expression_ids"] == support["EXPRESSION_IDS"]

    repeated_identity, repeated_lookup = harness.derive_lookup_index_identity(
        candidate_registry=candidate_registry,
        configurations=[
            harness.CONFIGURATIONS_BY_NAME["BM25-alone"],
        ],
        mapper_facts={},
        bm25_facts=bm25_facts,
    )
    assert repeated_identity == identity
    assert repeated_lookup == lookup_index

    fallback_identity, fallback_lookup = harness.derive_lookup_index_identity(
        candidate_registry=candidate_registry,
        configurations=[
            harness.CONFIGURATIONS_BY_NAME["C-alone"],
        ],
        mapper_facts={
            "kind": "char-ngram-fallback",
            "version": "char-ngram-test-v1",
        },
        bm25_facts={},
    )
    fallback_facts = fallback_identity["physicalStructures"]["denseOrFallbackIndex"]
    assert (
        fallback_facts["selectorRegistrySha256"]
        == hashlib.sha256(candidate_registry.selector_file.read_bytes()).hexdigest()
    )
    assert fallback_facts["selectorRowCount"] == len(candidate_registry.rows)
    assert fallback_identity["expressionCorpusSnapshot"] == (
        candidate_registry.managed_source.expression_corpus_snapshot
    )
    assert fallback_lookup["digest"] != lookup_index["digest"]

    flat = harness.flat_candidate_lineage_rows(
        [
            {
                "configuration": "BM25-alone",
                "evaluation_scope": "development_only",
                "items": [
                    {
                        "item_id": "gold-adjudication-one",
                        "label": "Poultry slaughter inspection",
                        "candidate_lineage": [
                            {
                                **lineage[member_id],
                                "candidate_rank": 1,
                                "channel_ranks": {"E": 1},
                                "channel_score_facts": {
                                    "E": {
                                        "rank": 1,
                                        "score": 4.25,
                                        "score_kind": "nativeMapperScore",
                                        "segment_id": "processing-segment-one",
                                    }
                                },
                                "limit": LIMIT,
                                "truncated": False,
                            }
                        ],
                    }
                ],
            }
        ]
    )
    assert len(flat) == 1
    row = flat[0]
    assert row["itemId"] == "gold-adjudication-one"
    assert row["configuration"] == "BM25-alone"
    assert row["conceptId"] == member_id
    assert row["channel"] == harness.BM25_CHANNEL_VERSION
    assert row["channelCode"] == "E"
    assert row["rank"] == 1
    assert row["candidateRank"] == 1
    assert row["score"] == 4.25
    assert row["scoreKind"] == "nativeMapperScore"
    assert row["scoreSourceSegment"] == "processing-segment-one"
    assert row["limit"] == LIMIT
    assert row["truncated"] is False
    assert row["facet"] == "subject"
    assert row["scheme"] == support["SCHEME_ID"]
    assert row["referenceResourceRelease"]["id"] == support["RELEASE_ID"]
    assert row["registryImportSnapshot"]["id"] == support["IMPORT_ID"]
    assert row["expressionCorpusSnapshot"]["id"] == support["CORPUS_ID"]
    assert row["lookupIndexManifest"] == lookup_index
    assert row["indexedExpressionIds"] == [support["EXPRESSION_ID"]]
    assert row["availableExpressionIds"] == support["EXPRESSION_IDS"]
    assert row["usageCeiling"] == "candidateUseOnly"
    assert row["evaluationScope"] == "development_only"

    cw_lineage = {
        **lineage[member_id],
        "channel_identities": {"Cw": harness.DENSE_EVIDENCE_WINDOW_VERSION},
        "indexed_expression_ids_by_channel": {
            "Cw": lineage[member_id]["indexed_expression_ids_by_channel"]["E"],
        },
    }
    cw_flat = harness.flat_candidate_lineage_rows(
        [
            {
                "configuration": "Cw-alone",
                "evaluation_scope": "development_only",
                "items": [
                    {
                        "item_id": "gold-adjudication-one",
                        "label": "Poultry slaughter inspection",
                        "candidate_lineage": [
                            {
                                **cw_lineage,
                                "candidate_rank": 1,
                                "channel_ranks": {"Cw": 1},
                                "channel_score_facts": {
                                    "Cw": {
                                        "rank": 1,
                                        "score": 0.75,
                                        "score_kind": "maxWindowNativeMapperScore",
                                        "segment_id": "processing-segment-one",
                                        "query_representation": harness.DENSE_EVIDENCE_WINDOW_VERSION,
                                        "query_window_id": "dense_query_window_one",
                                        "query_text_sha256": "sha256:" + ("b" * 64),
                                        "query_source_field": "rules.text",
                                        "query_source_start_char": 12,
                                        "query_source_end_char": 34,
                                        "query_model_token_count": 11,
                                    }
                                },
                                "limit": LIMIT,
                                "truncated": False,
                            }
                        ],
                    }
                ],
            }
        ]
    )
    assert cw_flat[0]["queryRepresentation"] == harness.DENSE_EVIDENCE_WINDOW_VERSION
    assert cw_flat[0]["queryWindowId"] == "dense_query_window_one"
    assert cw_flat[0]["querySourceField"] == "rules.text"
    assert cw_flat[0]["querySourceStartChar"] == 12
    assert cw_flat[0]["querySourceEndChar"] == 34
    assert cw_flat[0]["queryModelTokenCount"] == 11

    cp_lineage = {
        **lineage[member_id],
        "channel_identities": {"Cp": harness.DENSE_PACKED_EVIDENCE_VERSION},
        "indexed_expression_ids_by_channel": {
            "Cp": lineage[member_id]["indexed_expression_ids_by_channel"]["E"],
        },
    }
    cp_flat = harness.flat_candidate_lineage_rows(
        [
            {
                "configuration": "Cp-alone",
                "evaluation_scope": "development_only",
                "items": [
                    {
                        "item_id": "gold-adjudication-one",
                        "label": "Poultry slaughter inspection",
                        "candidate_lineage": [
                            {
                                **cp_lineage,
                                "candidate_rank": 1,
                                "channel_ranks": {"Cp": 1},
                                "channel_score_facts": {
                                    "Cp": {
                                        "rank": 1,
                                        "score": 0.8,
                                        "score_kind": "maxPackedWindowNativeMapperScore",
                                        "segment_id": "processing-segment-one",
                                        "query_representation": harness.DENSE_PACKED_EVIDENCE_VERSION,
                                        "query_window_id": "dense_packed_query_one",
                                        "query_text_sha256": "sha256:" + ("c" * 64),
                                        "query_source_fragments": [
                                            {
                                                "fieldKey": "evidence_0",
                                                "sourceField": "rules.text",
                                                "sourceStartChar": 12,
                                                "sourceEndChar": 20,
                                                "queryStartChar": 0,
                                                "queryEndChar": 8,
                                            },
                                            {
                                                "fieldKey": "evidence_1",
                                                "sourceField": "rules.notes",
                                                "sourceStartChar": 30,
                                                "sourceEndChar": 38,
                                                "queryStartChar": 9,
                                                "queryEndChar": 17,
                                            },
                                        ],
                                        "query_model_token_count": 10,
                                    }
                                },
                                "limit": LIMIT,
                                "truncated": False,
                            }
                        ],
                    }
                ],
            }
        ]
    )
    assert cp_flat[0]["queryRepresentation"] == harness.DENSE_PACKED_EVIDENCE_VERSION
    assert cp_flat[0]["queryWindowId"] == "dense_packed_query_one"
    assert len(cp_flat[0]["querySourceFragments"]) == 2
    assert "querySourceField" not in cp_flat[0]

    from spicy_regs.enrichment.experiment_artifacts import (
        write_experiment_artifacts,
    )

    artifacts = write_experiment_artifacts(
        tmp_path / "artifacts",
        {
            "inputs": {},
            "settings": {"limit": LIMIT},
            "results": [
                {
                    "configuration": "BM25-alone",
                    "item_count": 1,
                    "exact_alias_target_count": 1,
                    "exact_alias_surfaced": 1,
                    "adequate_target_count": 0,
                    "adequate_kept": 0,
                    "evaluation_scope": "development_only",
                    "accuracy_verdict_eligible": False,
                    "items": [],
                }
            ],
        },
        flat,
        decision="continue",
        rationale="The managed-release lineage survived the lookup run.",
    )
    written = read_parquet_rows(artifacts.candidates)
    assert written[0]["conceptId"] == member_id
    assert json.loads(written[0]["lookupIndexManifest"]) == lookup_index


def test_cli_requires_managed_release_or_explicit_legacy_opt_in(
    harness,
    tmp_path,
    capsys,
):
    with pytest.raises(SystemExit) as error:
        harness.main(["--output-dir", str(tmp_path)])

    assert error.value.code == 2
    assert "--allow-legacy-registry" in capsys.readouterr().err


def test_channel_lineage_does_not_claim_unindexed_note_expressions(harness):
    row = {
        "concept_id": "urn:test:concept:one",
        "pref_label": "Preferred",
        "alt_labels_json": '["Alternate"]',
        "definition": "Definition",
    }
    expressions = [
        {
            "expression_id": "urn:test:expression:pref",
            "semantic_property_iri": harness.SKOS_PREF_LABEL,
            "source_property_or_path": "source/labels/0",
            "original_literal": "Preferred",
        },
        {
            "expression_id": "urn:test:expression:alt",
            "semantic_property_iri": harness.SKOS_ALT_LABEL,
            "source_property_or_path": "source/labels/1",
            "original_literal": "Alternate",
        },
        {
            "expression_id": "urn:test:expression:definition",
            "semantic_property_iri": harness.SKOS_DEFINITION,
            "source_property_or_path": "source/notes/0",
            "original_literal": "Definition",
        },
        {
            "expression_id": "urn:test:expression:note",
            "semantic_property_iri": ("http://www.w3.org/2004/02/skos/core#scopeNote"),
            "source_property_or_path": "source/notes/1",
            "original_literal": "Available but not indexed",
        },
    ]

    indexed = harness._indexed_expression_ids_by_channel(
        row=row,
        expressions=expressions,
        channels=["A", "C", "E"],
        mapper_facts={"kind": "dense"},
        embedding_definition_kept=True,
    )

    assert indexed["A"] == [
        "urn:test:expression:pref",
        "urn:test:expression:alt",
    ]
    assert indexed["E"] == indexed["A"]
    assert indexed["C"] == [
        "urn:test:expression:pref",
        "urn:test:expression:alt",
        "urn:test:expression:definition",
    ]
    assert all("urn:test:expression:note" not in expression_ids for expression_ids in indexed.values())


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
        candidate_lineage_by_id={
            str(row["concept_id"]): {
                "member_iri": str(row["concept_id"]),
                "expressions": [],
            }
            for row in registry_rows
        },
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
    assert result["items"][0]["candidate_lineage"]
    first_lineage = result["items"][0]["candidate_lineage"][0]
    assert first_lineage["channel_ranks"]
    assert all(
        fact["score_kind"] == "rankOnly" and fact["score"] is None
        for fact in first_lineage["channel_score_facts"].values()
    )


def test_measurement_scores_managed_targets_and_excludes_not_represented(
    harness,
    registry_rows,
    units_by_id,
    answers,
):
    conditioning, channels = _channels(
        harness,
        units_by_id=units_by_id,
        registry_rows=registry_rows,
        wanted=("A", "B"),
    )
    base = harness.gold_items(
        answers=answers,
        units_by_id=units_by_id,
        aliases=harness.alias_index(registry_rows),
        adequate={},
    )
    items = [
        replace(
            base[0],
            registered_targets=(
                harness.GradedTarget(
                    concept_id="concept_mining",
                    pref_label="Surface mining",
                    grade="close",
                    adequate_for_development=True,
                ),
            ),
        ),
        replace(base[1], not_represented=True),
    ]
    result = harness.measure_configuration(
        harness.CONFIGURATIONS_BY_NAME["v2"],
        items=items,
        channels_by_segment=channels,
        conditioning=conditioning,
        source_vocabulary_by_id={
            concept_id: conditioning.source_vocabularies[index]
            for index, concept_id in enumerate(conditioning.concept_ids)
        },
        limit=LIMIT,
    )

    assert result["item_count"] == 2
    assert result["represented_item_count"] == 1
    assert result["represented_item_surfaced"] == 1
    assert result["not_represented_item_count"] == 1
    assert result["adequate_target_count"] == 1
    assert result["adequate_kept"] == 1
    assert result["target_grade_metrics"]["close"] == {
        "target_count": 1,
        "surfaced": 1,
        "recall_at_limit": 1.0,
    }


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


def test_default_configurations_do_not_require_keyword_provider_input(harness):
    assert harness.DEFAULT_CONFIGURATION_NAMES
    assert all(
        harness.CHANNEL_D not in harness.CONFIGURATIONS_BY_NAME[name].channels
        for name in harness.DEFAULT_CONFIGURATION_NAMES
    )

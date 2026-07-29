"""Regression checks for the train/holdout adoption boundary."""

from __future__ import annotations

from pathlib import Path

import pytest

from spicy_regs.evaluation_boundary import (
    DEFAULT_BOUNDARY_MANIFEST,
    DEVELOPMENT_DATASET_ID,
    EvaluationBoundaryError,
    EvaluationDataset,
    adoption_gate_facts,
    load_evaluation_dataset,
    partition_leakage_facts,
    require_adoption_ready,
)


def _concept(
    concept_id: str,
    label: str,
    *,
    aliases: str = "[]",
    hidden_aliases: str = "[]",
) -> dict[str, object]:
    return {
        "concept_id": concept_id,
        "facet": "subject",
        "source_vocabulary": "test-vocabulary",
        "scheme": "subject",
        "pref_label": label,
        "alt_labels_json": aliases,
        "hidden_labels_json": hidden_aliases,
        "status": "active",
    }


def _answers(
    train: tuple[str, str | None],
    holdout: tuple[str, str | None],
) -> dict[str, object]:
    return {
        "artifacts": [
            {
                "artifact_digest": "train-artifact",
                "split": "train",
                "expected_tags": [
                    {
                        "label": train[0],
                        "concept_id": train[1],
                    }
                ],
            },
            {
                "artifact_digest": "holdout-artifact",
                "split": "holdout",
                "expected_tags": [
                    {
                        "label": holdout[0],
                        "concept_id": holdout[1],
                    }
                ],
            },
        ]
    }


def _dataset(record: dict[str, object]) -> EvaluationDataset:
    return EvaluationDataset(
        dataset_id="test",
        role="mixed",
        forced_split=None,
        record=record,
        manifest_path=Path("/test/evaluation-boundary.json"),
        manifest_sha256="frozen-manifest",
    )


def _configuration() -> dict[str, object]:
    return {
        "candidate_selector": "selector-v2",
        "prompt_concept_limit": 12,
        "registry_sha256": "registry-digest",
        "tag_instructions_sha256": "instructions-digest",
        "tag_schema_sha256": "schema-digest",
        "prompt_input_token_budget": 8192,
        "prompt_safety_margin_tokens": 1024,
    }


def _eligible_record() -> dict[str, object]:
    return {
        "frozen": True,
        "frozen_configuration": _configuration(),
        "adjudication": {
            "status": "complete",
            "scope": "holdout",
            "blind_to_tagger_output": True,
            "agreement_published": True,
            "disagreement_resolution_status": "complete",
            "reviewers": [
                {
                    "model_family": "family-a",
                    "independent": True,
                },
                {
                    "model_family": "family-b",
                    "independent": True,
                },
            ],
        },
        "holdout_controls": {
            "configuration_frozen_before_labels": True,
            "tuning_access": False,
        },
    }


def test_original_35_are_permanently_frozen_as_development() -> None:
    dataset = load_evaluation_dataset(
        DEFAULT_BOUNDARY_MANIFEST,
        DEVELOPMENT_DATASET_ID,
    )
    assert dataset.role == "train"
    assert dataset.forced_split == "train"
    assert dataset.permanently_development is True
    assert dataset.record["accuracy_verdict_eligible"] is False


def test_partition_boundary_refuses_shared_concept_identity() -> None:
    answers = _answers(("Civil rights", "concept-civil"), ("Human rights", "concept-civil"))
    with pytest.raises(EvaluationBoundaryError, match="shared_concept_ids"):
        partition_leakage_facts(
            answers,
            [_concept("concept-civil", "Civil rights", aliases='["Human rights"]')],
        )


def test_partition_boundary_refuses_registered_alias_leakage() -> None:
    answers = _answers(
        ("Freedom of speech", "concept-speech"),
        ("Free speech", None),
    )
    with pytest.raises(EvaluationBoundaryError, match="shared_aliases"):
        partition_leakage_facts(
            answers,
            [
                _concept(
                    "concept-speech",
                    "Freedom of speech",
                    aliases='["Free speech"]',
                )
            ],
        )


@pytest.mark.parametrize(
    ("train_label", "holdout_label"),
    [
        ("Climatización", "CLIMATIZACIÓN"),
        ("Climate policy", "CLIMATE POLICY"),
        ("Weather governance", "WEATHER GOVERNANCE"),
    ],
)
def test_partition_boundary_normalizes_every_skos_label_role(
    train_label: str,
    holdout_label: str,
) -> None:
    answers = _answers(
        (train_label, "concept-climate"),
        (holdout_label, None),
    )
    with pytest.raises(EvaluationBoundaryError, match="shared_aliases"):
        partition_leakage_facts(
            answers,
            [
                _concept(
                    "concept-climate",
                    "Climatización",
                    aliases='["Climate policy"]',
                    hidden_aliases='["Weather governance"]',
                )
            ],
        )


def test_partition_boundary_expands_ambiguous_no_id_labels() -> None:
    answers = _answers(
        ("Expression rights", None),
        ("Free speech", None),
    )
    with pytest.raises(EvaluationBoundaryError, match="shared_concept_ids"):
        partition_leakage_facts(
            answers,
            [
                _concept("concept-speech", "Free speech"),
                _concept(
                    "concept-expression",
                    "Civil expression",
                    aliases='["Free speech", "Expression rights"]',
                ),
            ],
        )


def test_partition_boundary_indexes_deprecated_concept_aliases() -> None:
    answers = _answers(
        ("Historic water term", None),
        ("Water resources", None),
    )
    deprecated = _concept(
        "concept-water-old",
        "Water resources",
        aliases='["Historic water term"]',
    )
    deprecated["status"] = "deprecated"

    with pytest.raises(EvaluationBoundaryError, match="shared_concept_ids"):
        partition_leakage_facts(answers, [deprecated])


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("split", None, "split"),
        ("split", "validation", "split"),
        ("artifact_digest", "", "artifact digest"),
    ],
)
def test_partition_boundary_requires_artifact_identity(
    field: str,
    value: str | None,
    error: str,
) -> None:
    answers = _answers(("Water", None), ("Aviation", None))
    artifacts = answers["artifacts"]
    assert isinstance(artifacts, list)
    artifact = artifacts[0]
    assert isinstance(artifact, dict)
    if value is None:
        artifact.pop(field)
    else:
        artifact[field] = value

    with pytest.raises(EvaluationBoundaryError, match=error):
        partition_leakage_facts(answers, [])


def test_development_only_data_can_never_authorize_an_accuracy_verdict() -> None:
    dataset = load_evaluation_dataset(
        DEFAULT_BOUNDARY_MANIFEST,
        DEVELOPMENT_DATASET_ID,
    )
    answers = {
        "artifacts": [
            {
                "artifact_digest": "development-artifact",
                "split": "train",
                "expected_tags": [{"label": "Water", "concept_id": None}],
            }
        ]
    }
    gate = adoption_gate_facts(
        dataset,
        answers,
        {"passed": True},
        configuration=_configuration(),
    )
    assert gate["eligible"] is False
    assert "no held-out artifact is present" in gate["blockers"]
    with pytest.raises(EvaluationBoundaryError, match="verdict refused"):
        require_adoption_ready(gate)


def test_same_family_sessions_do_not_count_as_independent_families() -> None:
    record = {
        "frozen": True,
        "frozen_configuration": _configuration(),
        "adjudication": {
            "status": "complete",
            "scope": "holdout",
            "blind_to_tagger_output": True,
            "agreement_published": True,
            "disagreement_resolution_status": "complete",
            "reviewers": [
                {
                    "model_family": "family-a",
                    "independent": True,
                },
                {
                    "model_family": "family-a",
                    "independent": True,
                },
            ],
        },
        "holdout_controls": {
            "configuration_frozen_before_labels": True,
            "tuning_access": False,
        },
    }
    gate = adoption_gate_facts(
        _dataset(record),
        _answers(("Water", None), ("Aviation", None)),
        {"passed": True},
        configuration=_configuration(),
    )
    assert gate["eligible"] is False
    assert gate["independent_reviewer_families"] == ["family-a"]


def test_two_families_and_untouched_controls_clear_the_structural_gate() -> None:
    record = _eligible_record()
    gate = adoption_gate_facts(
        _dataset(record),
        _answers(("Water", None), ("Aviation", None)),
        {"passed": True},
        configuration=_configuration(),
    )
    assert gate["eligible"] is True
    require_adoption_ready(gate)


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        ("scope", "train", "not scoped to the holdout"),
        ("blind_to_tagger_output", False, "not blind to tagger output"),
        ("agreement_published", False, "agreement was not published"),
        (
            "disagreement_resolution_status",
            "pending",
            "disagreements are unresolved",
        ),
    ],
)
def test_adjudication_conditions_are_enforced(
    field: str,
    value: object,
    blocker: str,
) -> None:
    record = _eligible_record()
    adjudication = record["adjudication"]
    assert isinstance(adjudication, dict)
    adjudication[field] = value

    gate = adoption_gate_facts(
        _dataset(record),
        _answers(("Water", None), ("Aviation", None)),
        {"passed": True},
        configuration=_configuration(),
    )

    assert gate["eligible"] is False
    assert any(blocker in str(item) for item in gate["blockers"])


def test_configuration_and_registry_pins_are_enforced() -> None:
    actual = _configuration()
    actual["registry_sha256"] = "different-registry"

    gate = adoption_gate_facts(
        _dataset(_eligible_record()),
        _answers(("Water", None), ("Aviation", None)),
        {"passed": True},
        configuration=actual,
    )

    assert gate["eligible"] is False
    assert gate["configuration_mismatches"] == ["registry_sha256"]

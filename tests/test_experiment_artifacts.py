"""Focused tests for the lightweight experiment-lane artifact writer."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.enrichment.experiment_artifacts import (
    CORE_FILES,
    ExperimentArtifactError,
    write_experiment_artifacts,
)

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64
DIGEST_D = "sha256:" + "d" * 64

RELEASE = {
    "id": "urn:test:release:subjects",
    "version": "2026-07-29",
    "digest": DIGEST_A,
}
IMPORT = {"id": "urn:test:import:subjects", "digest": DIGEST_B}
CORPUS = {"id": "urn:test:corpus:subjects", "digest": DIGEST_C}
INDEX = {"id": "urn:test:index:subjects", "digest": DIGEST_D}
MANAGED_MANIFEST = "/data/managed-release/managed-release-bundle.json"


def _ablation_result() -> dict:
    return {
        "schema_version": "candidate-selector-ablation-v2",
        "generated_at": "2026-07-29T12:00:00+00:00",
        "inputs": {
            "dataset_dir": "/data/development-v1",
            "selection_file": "/data/development-v1/selection.parquet",
            "registry_file": "/data/managed-release/registry.parquet",
            "registry_sha256": "1" * 64,
            "gold_file": "/data/development-v1/gold.json",
            "gold_sha256": "2" * 64,
            "resolved_file": "/data/development-v1/resolved.json",
            "resolved_sha256": "3" * 64,
        },
        "settings": {
            "limit": 12,
            "dense_channel_version": "dense-v1",
            "bm25_channel_version": "bm25-v1",
        },
        "evaluation_boundary": {
            "dataset_id": "development-v1",
            "eligible": False,
        },
        "timings_seconds": {"measure": 0.25},
        "reviewed_target_binding": {
            "exactly_bound_count": 0,
            "foreign_or_unbound_count": 5,
            "label_rebinding_performed": False,
        },
        "results": [
            {
                "configuration": "BM25+B+C",
                "channels": ["E", "B", "C"],
                "quotas": True,
                "note": "hybrid development arm",
                "item_count": 10,
                "exact_alias_target_count": 8,
                "exact_alias_surfaced": 6,
                "adequate_target_count": 5,
                "adequate_kept": 4,
                "surfaced_rank_mean": 2.5,
                "evaluation_scope": "development_only",
                "accuracy_verdict_eligible": False,
                "items": [
                    {
                        "item_id": "item-1",
                        "candidates": ["concept-a", "concept-b"],
                        "candidate_lineage": [{"conceptId": "concept-a"}],
                        "adequate_rank": 1,
                    }
                ],
            }
        ],
    }


def _candidate_rows() -> list[dict]:
    common = {
        "itemId": "item-1",
        "configuration": "BM25+B+C",
        "referenceResourceRelease": RELEASE,
        "registryImportSnapshot": IMPORT,
        "expressionCorpusSnapshot": CORPUS,
        "lookupIndexManifest": INDEX,
    }
    return [
        {
            **common,
            "conceptId": "concept-b",
            "channel": "dense-v1",
            "rank": 2,
            "indexedExpressionIds": ["urn:test:expression:b"],
        },
        {
            **common,
            "conceptId": "concept-a",
            "channel": "bm25-v1",
            "rank": 1,
            "indexedExpressionIds": ["urn:test:expression:a"],
        },
    ]


def _zero_candidate_managed_result() -> dict:
    result = _ablation_result()
    result["inputs"].update(
        {
            "targets_file": "/data/development-v1/managed-targets.json",
            "targets_sha256": "4" * 64,
            "target_dataset_id": "urn:test:managed-targets:v1",
        }
    )
    result["inputs"]["candidate_source"] = {
        "mode": "managedRelease",
        "usage_ceiling": "candidateUseOnly",
        "bundle_manifest": MANAGED_MANIFEST,
        "bundle_manifest_digest": DIGEST_A,
        "publication_release_id": "urn:test:publication-release:subjects",
        "expression_corpus_snapshot": CORPUS,
        "lookup_index_manifest": INDEX,
    }
    result["results"][0]["exact_alias_surfaced"] = 0
    result["results"][0]["adequate_kept"] = 0
    result["results"][0]["items"] = []
    return result


def test_writer_emits_only_deterministic_development_artifacts(
    tmp_path: Path,
) -> None:
    first = write_experiment_artifacts(
        tmp_path / "first",
        _ablation_result(),
        _candidate_rows(),
        decision="continue",
        rationale="The hybrid arm retrieved six of eight available targets.",
    )
    second = write_experiment_artifacts(
        tmp_path / "second",
        _ablation_result(),
        list(reversed(_candidate_rows())),
        decision="continue",
        rationale="The hybrid arm retrieved six of eight available targets.",
    )

    assert {path.name for path in first.directory.iterdir()} == set(CORE_FILES)
    for name in CORE_FILES:
        assert (first.directory / name).read_bytes() == (second.directory / name).read_bytes()

    experiment = json.loads(first.experiment.read_text())
    assert experiment["eligibility"] == {
        "acceptedOutputEligible": False,
        "adoptionEligible": False,
        "candidateUseOnly": True,
        "promotionAuthorized": False,
        "scope": "developmentOnly",
    }
    assert experiment["identities"]["release"]["referenceResourceRelease"] == RELEASE
    assert experiment["identities"]["release"]["expressionCorpusSnapshot"] == CORPUS
    assert experiment["identities"]["index"]["lookupIndexManifest"] == INDEX
    assert experiment["identities"]["dataset"]["gold_sha256"] == "2" * 64

    metrics = json.loads(first.metrics.read_text())
    assert metrics["reviewedTargetBinding"]["foreign_or_unbound_count"] == 5
    result = metrics["results"][0]
    assert result["stages"]["available"] == {
        "count": 8,
        "population": 10,
        "rate": 0.8,
    }
    assert result["stages"]["retrieved"] == {
        "available": 8,
        "count": 6,
        "rate": 0.75,
    }
    assert result["stages"]["adequate"] == {
        "count": 4,
        "rate": 0.8,
        "reviewedTargets": 5,
    }
    assert "candidates" not in result["items"][0]
    assert "candidate_lineage" not in result["items"][0]

    rows = pq.read_table(first.candidates).to_pylist()
    assert [row["conceptId"] for row in rows] == ["concept-a", "concept-b"]
    assert json.loads(rows[0]["referenceResourceRelease"]) == RELEASE
    assert json.loads(rows[0]["lookupIndexManifest"]) == INDEX
    assert first.decision.read_text().startswith("# Development decision: Continue\n")
    assert "does not authorize adoption" in first.decision.read_text()


def test_managed_zero_candidate_run_preserves_source_identities(
    tmp_path: Path,
) -> None:
    artifacts = write_experiment_artifacts(
        tmp_path / "managed-zero",
        _zero_candidate_managed_result(),
        [],
        decision="investigate",
        rationale="No candidate rows surfaced; retain the exact source pins.",
    )

    experiment = json.loads(artifacts.experiment.read_text())
    assert experiment["identities"]["release"] == {
        "expressionCorpusSnapshot": CORPUS,
        "managedReleaseManifest": MANAGED_MANIFEST,
        "managedReleaseManifestDigest": DIGEST_A,
        "publicationReleaseId": "urn:test:publication-release:subjects",
    }
    assert experiment["identities"]["index"] == {
        "lookupIndexManifest": INDEX,
    }
    assert experiment["identities"]["dataset"]["targets_file"] == ("/data/development-v1/managed-targets.json")
    assert experiment["identities"]["dataset"]["targets_sha256"] == "4" * 64
    assert experiment["identities"]["dataset"]["target_dataset_id"] == ("urn:test:managed-targets:v1")
    assert pq.read_table(artifacts.candidates).num_rows == 0


def test_managed_target_metrics_define_reachable_recall(
    tmp_path: Path,
) -> None:
    result = _ablation_result()
    measured = result["results"][0]
    measured.update(
        {
            "represented_item_count": 7,
            "represented_item_surfaced": 5,
        }
    )

    artifacts = write_experiment_artifacts(
        tmp_path / "managed-metrics",
        result,
        _candidate_rows(),
        decision="continue",
        rationale=(
            "Five of the seven represented items surfaced; the other three "
            "items are not represented in this vocabulary release."
        ),
    )

    metrics = json.loads(artifacts.metrics.read_text())
    stages = metrics["results"][0]["stages"]
    assert stages["available"] == {
        "count": 7,
        "population": 10,
        "rate": 0.7,
    }
    assert stages["retrieved"] == {
        "available": 7,
        "count": 5,
        "rate": 0.714286,
    }


def test_managed_target_metrics_reject_incomplete_or_impossible_counts(
    tmp_path: Path,
) -> None:
    result = _ablation_result()
    result["results"][0]["represented_item_count"] = 7

    with pytest.raises(
        ExperimentArtifactError,
        match=r"represented_item_surfaced must be a non-negative integer",
    ):
        write_experiment_artifacts(
            tmp_path / "incomplete-managed-metrics",
            result,
            _candidate_rows(),
            decision="investigate",
            rationale="The managed result did not report its surfaced count.",
        )

    result["results"][0]["represented_item_surfaced"] = 8
    with pytest.raises(
        ExperimentArtifactError,
        match=("represented_item_surfaced cannot exceed represented_item_count"),
    ):
        write_experiment_artifacts(
            tmp_path / "impossible-managed-metrics",
            result,
            _candidate_rows(),
            decision="investigate",
            rationale="The managed result reported an impossible surfaced count.",
        )


@pytest.mark.parametrize(
    "missing_field",
    [
        "bundle_manifest",
        "bundle_manifest_digest",
        "expression_corpus_snapshot",
        "lookup_index_manifest",
    ],
)
def test_managed_run_rejects_missing_source_identity(
    tmp_path: Path,
    missing_field: str,
) -> None:
    result = _zero_candidate_managed_result()
    result["inputs"]["candidate_source"].pop(missing_field)

    with pytest.raises(ExperimentArtifactError, match="managed experiment requires"):
        write_experiment_artifacts(
            tmp_path / missing_field,
            result,
            [],
            decision="investigate",
            rationale="The managed source is incomplete.",
        )


@pytest.mark.parametrize(
    ("mutate_result", "mutate_candidates", "decision"),
    [
        (
            lambda result: result["results"][0].update({"accuracy_verdict_eligible": True}),
            lambda rows: None,
            "continue",
        ),
        (
            lambda result: None,
            lambda rows: rows[0].update({"acceptedOutputUse": True}),
            "investigate",
        ),
        (
            lambda result: None,
            lambda rows: None,
            "promote",
        ),
    ],
)
def test_writer_rejects_promotion_and_accepted_output_claims(
    tmp_path: Path,
    mutate_result,
    mutate_candidates,
    decision: str,
) -> None:
    result = _ablation_result()
    rows = _candidate_rows()
    mutate_result(result)
    mutate_candidates(rows)

    with pytest.raises(ExperimentArtifactError):
        write_experiment_artifacts(
            tmp_path / decision,
            result,
            rows,
            decision=decision,
            rationale="Keep this result inside development.",
        )

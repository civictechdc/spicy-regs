"""Frozen train/holdout boundaries for Rulespec accuracy claims."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.enrichment.reference_runtime import normalize_unicode_text
from spicy_regs.ontology.common import canonical_json

BOUNDARY_SCHEMA_VERSION = "rulespec-evaluation-boundary-v1"
DEFAULT_BOUNDARY_MANIFEST = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "evidence"
    / "gold-adjudication-2026-07-27"
    / "evaluation-boundary.json"
)
DEVELOPMENT_DATASET_ID = "rulespec-development-35-v1"
ADOPTION_SPLIT = "holdout"
TRAIN_SPLIT = "train"
FROZEN_CONFIGURATION_FIELDS = (
    "candidate_selector",
    "prompt_concept_limit",
    "registry_sha256",
    "tag_instructions_sha256",
    "tag_schema_sha256",
    "prompt_input_token_budget",
    "prompt_safety_margin_tokens",
)


class EvaluationBoundaryError(RuntimeError):
    """An evaluation input or verdict would cross the frozen boundary."""


@dataclass(frozen=True)
class EvaluationDataset:
    dataset_id: str
    role: str
    forced_split: str | None
    record: dict[str, Any]
    manifest_path: Path
    manifest_sha256: str
    policy: dict[str, Any] = field(default_factory=dict)

    @property
    def permanently_development(self) -> bool:
        return bool(self.record.get("permanently_development"))


def _required_text(record: Mapping[str, Any], key: str, label: str) -> str:
    value = str(record.get(key) or "").strip()
    if not value:
        raise EvaluationBoundaryError(f"{label}.{key} is required")
    return value


def load_evaluation_dataset(
    manifest_path: Path,
    dataset_id: str,
) -> EvaluationDataset:
    """Load one dataset declaration from the tracked boundary manifest."""
    path = Path(manifest_path)
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationBoundaryError(f"cannot read evaluation boundary {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("schema_version") != BOUNDARY_SCHEMA_VERSION:
        raise EvaluationBoundaryError(f"evaluation boundary must use schema {BOUNDARY_SCHEMA_VERSION!r}")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise EvaluationBoundaryError("evaluation boundary datasets must be an array")
    matches = [
        dict(record)
        for record in datasets
        if isinstance(record, dict) and str(record.get("dataset_id") or "") == dataset_id
    ]
    if len(matches) != 1:
        raise EvaluationBoundaryError(f"evaluation boundary must declare dataset {dataset_id!r} exactly once")
    record = matches[0]
    role = _required_text(record, "role", f"datasets[{dataset_id}]")
    if role not in {TRAIN_SPLIT, ADOPTION_SPLIT, "mixed"}:
        raise EvaluationBoundaryError(f"dataset {dataset_id!r} has unknown role {role!r}")
    forced = str(record.get("forced_split") or "").strip() or None
    if forced is not None and forced not in {TRAIN_SPLIT, ADOPTION_SPLIT}:
        raise EvaluationBoundaryError(f"dataset {dataset_id!r} has unknown forced split {forced!r}")
    if forced == ADOPTION_SPLIT:
        raise EvaluationBoundaryError("a holdout dataset may never use a forced split")
    if record.get("permanently_development") and role != TRAIN_SPLIT:
        raise EvaluationBoundaryError(f"permanently development dataset {dataset_id!r} must have train role")
    policy = manifest.get("adoption_policy")
    if not isinstance(policy, dict):
        raise EvaluationBoundaryError("evaluation boundary adoption_policy must be an object")
    minimum_holdout = policy.get("minimum_holdout_artifacts")
    minimum_families = policy.get("minimum_independent_reviewer_families")
    if not isinstance(minimum_holdout, int) or isinstance(minimum_holdout, bool) or minimum_holdout < 1:
        raise EvaluationBoundaryError("adoption_policy.minimum_holdout_artifacts must be a positive integer")
    if not isinstance(minimum_families, int) or isinstance(minimum_families, bool) or minimum_families < 2:
        raise EvaluationBoundaryError("adoption_policy.minimum_independent_reviewer_families must be at least 2")
    return EvaluationDataset(
        dataset_id=dataset_id,
        role=role,
        forced_split=forced,
        record=record,
        manifest_path=path,
        manifest_sha256=sha256_file(path),
        policy=dict(policy),
    )


def _assert_digest(label: str, actual_path: Path, expected: object) -> None:
    expected_text = str(expected or "")
    if not expected_text:
        raise EvaluationBoundaryError(f"evaluation boundary does not pin {label}")
    actual = sha256_file(actual_path)
    if actual != expected_text:
        raise EvaluationBoundaryError(f"{label} digest changed: expected {expected_text}, got {actual}")


def validate_frozen_dataset(
    dataset: EvaluationDataset,
    *,
    gold_path: Path,
    selection_path: Path,
    source_files: Mapping[str, str],
    gold_row_count: int,
    gold_artifact_count: int,
) -> None:
    """Verify every frozen input named by a dataset declaration."""
    record = dataset.record
    if record.get("frozen") is not True:
        raise EvaluationBoundaryError(f"dataset {dataset.dataset_id!r} is not frozen")
    _assert_digest("gold", gold_path, record.get("gold_sha256"))
    _assert_digest("selection", selection_path, record.get("selection_sha256"))
    expected_rows = record.get("gold_row_count")
    expected_artifacts = record.get("gold_artifact_count")
    if expected_rows != gold_row_count:
        raise EvaluationBoundaryError(f"gold row count changed: expected {expected_rows}, got {gold_row_count}")
    if expected_artifacts != gold_artifact_count:
        raise EvaluationBoundaryError(
            f"gold artifact count changed: expected {expected_artifacts}, got {gold_artifact_count}"
        )
    expected_sources = record.get("source_files")
    if not isinstance(expected_sources, dict) or not expected_sources:
        raise EvaluationBoundaryError("evaluation boundary does not pin source files")
    normalized_expected = {str(key): str(value) for key, value in expected_sources.items()}
    normalized_actual = {str(key): str(value) for key, value in source_files.items()}
    if normalized_expected != normalized_actual:
        raise EvaluationBoundaryError("source-file digests differ from the frozen evaluation boundary")


def gold_split(
    row: Mapping[str, Any],
    *,
    forced_split: str | None,
) -> str:
    """Read an explicit split, except for a manifest-pinned development set."""
    declared = str(row.get("split") or "").strip()
    if forced_split is not None:
        if declared and declared != forced_split:
            raise EvaluationBoundaryError(
                f"gold row {row.get('gold_id')} declares {declared!r}, "
                f"but the frozen dataset permanently forces {forced_split!r}"
            )
        return forced_split
    if not declared:
        raise EvaluationBoundaryError(f"gold row {row.get('gold_id')} has no explicit split")
    if declared not in {TRAIN_SPLIT, ADOPTION_SPLIT}:
        raise EvaluationBoundaryError(f"gold row {row.get('gold_id')} declares unknown split {declared!r}")
    return declared


def partition_leakage_facts(
    answers: Mapping[str, Any],
    concepts: Sequence[Mapping[str, Any]],
    *,
    require_complete: bool = False,
) -> dict[str, Any]:
    """Detect every RefSpec partition key crossing train and holdout.

    The historical boundary checked concept ids, registered aliases, and
    artifact digests.  REF-EVAL-012 also requires reviewed exact-match
    clusters, source identity, extracted or normalized text, and versioned
    near-duplicate clusters.  Optional legacy inputs remain readable, but a
    supplied key is never ignored.
    """

    def strings(value: object) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return set()
            if text[:1] in {"[", "{"}:
                try:
                    return strings(json.loads(text))
                except json.JSONDecodeError:
                    return {text}
            return {text}
        if isinstance(value, Mapping):
            result: set[str] = set()
            for child in value.values():
                result.update(strings(child))
            return result
        if isinstance(value, Sequence):
            result = set()
            for child in value:
                result.update(strings(child))
            return result
        text = str(value).strip()
        return {text} if text else set()

    def registered_aliases(concept: Mapping[str, Any]) -> set[str]:
        values: set[str] = set()
        for key in (
            "pref_label",
            "alt_labels_json",
            "hidden_labels_json",
            "prefLabel",
            "altLabel",
            "hiddenLabel",
            "skos:prefLabel",
            "skos:altLabel",
            "skos:hiddenLabel",
        ):
            values.update(strings(concept.get(key)))
        return {
            normalized
            for value in values
            if (normalized := normalize_unicode_text(value))
        }

    def cluster_ids(record: Mapping[str, Any]) -> set[str]:
        result: set[str] = set()
        for key in (
            "exact_match_cluster_id",
            "exactMatchCluster",
            "exact_match_clusters",
            "exactMatchClusters",
        ):
            result.update(strings(record.get(key)))
        return result

    def first_value(record: Mapping[str, Any], keys: Sequence[str]) -> str:
        for key in keys:
            value = str(record.get(key) or "").strip()
            if value:
                return value
        return ""

    concepts_by_id = {
        str(concept.get("concept_id") or ""): concept
        for concept in concepts
        if concept.get("concept_id")
    }
    aliases_by_concept = {
        concept_id: registered_aliases(concept)
        for concept_id, concept in concepts_by_id.items()
    }
    clusters_by_concept = {
        concept_id: cluster_ids(concept)
        for concept_id, concept in concepts_by_id.items()
    }
    concept_ids_by_alias: dict[str, set[str]] = defaultdict(set)
    for concept_id, concept_aliases in aliases_by_concept.items():
        for alias in concept_aliases:
            concept_ids_by_alias[alias].add(concept_id)

    concept_ids: dict[str, set[str]] = defaultdict(set)
    aliases: dict[str, set[str]] = defaultdict(set)
    exact_match_clusters: dict[str, set[str]] = defaultdict(set)
    source_identities: dict[str, set[str]] = defaultdict(set)
    artifact_digests: dict[str, set[str]] = defaultdict(set)
    text_digests: dict[str, set[str]] = defaultdict(set)
    near_duplicate_clusters: dict[str, set[str]] = defaultdict(set)
    complete_partition_key_items = 0
    for artifact in answers.get("artifacts", ()):
        if not isinstance(artifact, Mapping):
            continue
        split = str(artifact.get("split") or "").strip()
        if split not in {TRAIN_SPLIT, ADOPTION_SPLIT}:
            raise EvaluationBoundaryError(
                f"evaluation artifact has missing or unknown split {split!r}"
            )
        artifact_digest = str(artifact.get("artifact_digest") or "").strip()
        if not artifact_digest:
            raise EvaluationBoundaryError("evaluation artifact has no artifact digest")
        artifact_digests[split].add(artifact_digest)
        if require_complete:
            partition_keys = artifact.get("partitionKeys")
            if not isinstance(partition_keys, Mapping):
                raise EvaluationBoundaryError(
                    "adoption artifact is missing complete partitionKeys"
                )
            required_dimensions = {
                "conceptIdentity",
                "exactMatchCluster",
                "alias",
                "sourceIdentity",
                "artifactDigest",
                "textDigest",
                "nearDuplicateCluster",
            }
            missing_dimensions = sorted(
                required_dimensions - set(partition_keys)
            )
            if missing_dimensions:
                raise EvaluationBoundaryError(
                    "adoption artifact partitionKeys are missing dimensions: "
                    + ", ".join(missing_dimensions)
                )
            for dimension in (
                "sourceIdentity",
                "artifactDigest",
                "textDigest",
                "nearDuplicateCluster",
            ):
                if not strings(partition_keys[dimension]):
                    raise EvaluationBoundaryError(
                        f"adoption artifact has no {dimension} partition key"
                    )
            concept_ids[split].update(
                strings(partition_keys["conceptIdentity"])
            )
            exact_match_clusters[split].update(
                strings(partition_keys["exactMatchCluster"])
            )
            aliases[split].update(
                normalize_unicode_text(value)
                for value in strings(partition_keys["alias"])
                if normalize_unicode_text(value)
            )
            source_identities[split].update(
                strings(partition_keys["sourceIdentity"])
            )
            artifact_digests[split].update(
                strings(partition_keys["artifactDigest"])
            )
            text_digests[split].update(
                strings(partition_keys["textDigest"])
            )
            near_duplicate_clusters[split].update(
                strings(partition_keys["nearDuplicateCluster"])
            )
            if artifact_digest not in strings(
                partition_keys["artifactDigest"]
            ):
                raise EvaluationBoundaryError(
                    "artifact digest is absent from its partitionKeys"
                )
            complete_partition_key_items += 1
        source_identity = first_value(
            artifact,
            (
                "source_identity",
                "sourceIdentity",
                "source_resource_id",
                "sourceResource",
            ),
        )
        if not source_identity:
            profile_id = str(artifact.get("profile_id") or "").strip()
            subject_type = str(artifact.get("subject_type") or "").strip()
            subject_id = str(artifact.get("subject_id") or "").strip()
            if profile_id and subject_type and subject_id:
                source_identity = "|".join(
                    (profile_id, subject_type, subject_id)
                )
        if source_identity:
            source_identities[split].add(source_identity)
        for key in (
            "text_digest",
            "textDigest",
            "extracted_text_sha256",
            "normalized_text_digest",
            "normalizedTextDigest",
        ):
            text_digests[split].update(strings(artifact.get(key)))
        for key in (
            "near_duplicate_cluster",
            "nearDuplicateCluster",
            "near_duplicate_cluster_id",
            "nearDuplicateClusterId",
        ):
            near_duplicate_clusters[split].update(strings(artifact.get(key)))
        for expected in artifact.get("expected_tags", ()):
            if not isinstance(expected, Mapping):
                continue
            normalized = normalize_unicode_text(expected.get("label"))
            if normalized:
                aliases[split].add(normalized)
            for key in (
                "aliases",
                "current_aliases",
                "deprecated_aliases",
                "currentAliases",
                "deprecatedAliases",
            ):
                aliases[split].update(
                    normalize_unicode_text(value)
                    for value in strings(expected.get(key))
                    if normalize_unicode_text(value)
                )
            matched_ids = set(concept_ids_by_alias.get(normalized, ()))
            explicit_id = str(expected.get("concept_id") or "").strip()
            if explicit_id:
                matched_ids.add(explicit_id)
            concept_ids[split].update(matched_ids)
            for concept_id in matched_ids:
                aliases[split].update(aliases_by_concept.get(concept_id, ()))
                exact_match_clusters[split].update(
                    clusters_by_concept.get(concept_id, ())
                )
            exact_match_clusters[split].update(cluster_ids(expected))

    shared_ids = sorted(concept_ids[TRAIN_SPLIT] & concept_ids[ADOPTION_SPLIT])
    shared_aliases = sorted(aliases[TRAIN_SPLIT] & aliases[ADOPTION_SPLIT])
    shared_exact_clusters = sorted(
        exact_match_clusters[TRAIN_SPLIT]
        & exact_match_clusters[ADOPTION_SPLIT]
    )
    shared_sources = sorted(
        source_identities[TRAIN_SPLIT]
        & source_identities[ADOPTION_SPLIT]
    )
    shared_artifacts = sorted(artifact_digests[TRAIN_SPLIT] & artifact_digests[ADOPTION_SPLIT])
    shared_text = sorted(
        text_digests[TRAIN_SPLIT] & text_digests[ADOPTION_SPLIT]
    )
    shared_near_duplicates = sorted(
        near_duplicate_clusters[TRAIN_SPLIT]
        & near_duplicate_clusters[ADOPTION_SPLIT]
    )
    facts = {
        "train_concept_count": len(concept_ids[TRAIN_SPLIT]),
        "holdout_concept_count": len(concept_ids[ADOPTION_SPLIT]),
        "shared_concept_ids": shared_ids,
        "shared_aliases": shared_aliases,
        "shared_exact_match_clusters": shared_exact_clusters,
        "shared_source_identities": shared_sources,
        "shared_artifact_digests": shared_artifacts,
        "shared_text_digests": shared_text,
        "shared_near_duplicate_clusters": shared_near_duplicates,
        "complete_partition_key_items": complete_partition_key_items,
        "complete_partition_keys_required": require_complete,
        "passed": not any(
            (
                shared_ids,
                shared_aliases,
                shared_exact_clusters,
                shared_sources,
                shared_artifacts,
                shared_text,
                shared_near_duplicates,
            )
        ),
    }
    if not facts["passed"]:
        raise EvaluationBoundaryError(
            "train/holdout leakage detected: "
            + canonical_json(
                {
                    "shared_concept_ids": shared_ids,
                    "shared_aliases": shared_aliases,
                    "shared_exact_match_clusters": shared_exact_clusters,
                    "shared_source_identities": shared_sources,
                    "shared_artifact_digests": shared_artifacts,
                    "shared_text_digests": shared_text,
                    "shared_near_duplicate_clusters": shared_near_duplicates,
                }
            )
        )
    return facts


def adoption_gate_facts(
    dataset: EvaluationDataset | None,
    answers: Mapping[str, Any],
    leakage: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return explicit blockers for an adoption or accuracy verdict."""
    split_counts = Counter(
        str(artifact.get("split") or "") for artifact in answers.get("artifacts", ()) if isinstance(artifact, Mapping)
    )
    blockers: list[str] = []
    if dataset is None:
        blockers.append("evaluation inputs are not bound to a frozen manifest")
        record: Mapping[str, Any] = {}
        policy: Mapping[str, Any] = {}
    else:
        record = dataset.record
        policy = dataset.policy
        if record.get("frozen") is not True:
            blockers.append("the declared evaluation dataset is not frozen")
        if dataset.permanently_development:
            blockers.append("the declared dataset is permanently development-only")
        if dataset.role == TRAIN_SPLIT and split_counts[ADOPTION_SPLIT]:
            blockers.append("a train-only dataset contains holdout artifacts")
        if dataset.role == ADOPTION_SPLIT and split_counts[TRAIN_SPLIT]:
            blockers.append("a holdout-only dataset contains train artifacts")
    minimum_holdout = int(policy.get("minimum_holdout_artifacts") or 1)
    if split_counts[ADOPTION_SPLIT] < minimum_holdout:
        blockers.append("no held-out artifact is present")
    if leakage.get("passed") is not True:
        blockers.append(
            "train/holdout concept, alias, source, artifact, text, or "
            "near-duplicate leakage was not cleared"
        )

    pinned_configuration = record.get("frozen_configuration")
    configuration_mismatches: list[str] = []
    if not isinstance(pinned_configuration, Mapping):
        blockers.append("selector, registry, prompt, and schema configuration pins are absent")
    elif configuration is None:
        blockers.append("the evaluated configuration was not supplied to the adoption gate")
    else:
        for field_name in FROZEN_CONFIGURATION_FIELDS:
            expected = pinned_configuration.get(field_name)
            actual = configuration.get(field_name)
            if expected is None or expected != actual:
                configuration_mismatches.append(field_name)
        if configuration_mismatches:
            blockers.append("evaluated configuration differs from frozen pins: " + ", ".join(configuration_mismatches))

    adjudication = record.get("adjudication")
    reviewers: list[Mapping[str, Any]] = (
        [item for item in adjudication.get("reviewers", ()) if isinstance(item, Mapping)]
        if isinstance(adjudication, Mapping)
        else []
    )
    families = {
        str(reviewer.get("model_family") or "").strip().casefold()
        for reviewer in reviewers
        if reviewer.get("independent") is True and str(reviewer.get("model_family") or "").strip()
    }
    if not isinstance(adjudication, Mapping) or adjudication.get("status") != "complete":
        blockers.append("independent holdout adjudication is incomplete")
    if not isinstance(adjudication, Mapping) or adjudication.get("scope") != ADOPTION_SPLIT:
        blockers.append("adjudication is not scoped to the holdout")
    if not isinstance(adjudication, Mapping) or adjudication.get("blind_to_tagger_output") is not True:
        blockers.append("holdout adjudication was not blind to tagger output")
    if not isinstance(adjudication, Mapping) or adjudication.get("agreement_published") is not True:
        blockers.append("holdout adjudication agreement was not published")
    if not isinstance(adjudication, Mapping) or adjudication.get("disagreement_resolution_status") != "complete":
        blockers.append("holdout adjudication disagreements are unresolved")
    minimum_families = int(policy.get("minimum_independent_reviewer_families") or 2)
    if len(families) < minimum_families:
        blockers.append("holdout adjudication lacks two independent reviewer families")
    holdout_controls = record.get("holdout_controls")
    if not isinstance(holdout_controls, Mapping):
        blockers.append("holdout untouched-use controls are absent")
    else:
        if holdout_controls.get("configuration_frozen_before_labels") is not True:
            blockers.append("configuration was not frozen before holdout labels")
        if holdout_controls.get("tuning_access") is not False:
            blockers.append("holdout is not sealed against tuning access")

    return {
        "verdict_kind": "adoption_accuracy",
        "eligible": not blockers,
        "blockers": blockers,
        "artifacts_by_split": dict(sorted(split_counts.items())),
        "minimum_holdout_artifacts": minimum_holdout,
        "minimum_independent_reviewer_families": minimum_families,
        "independent_reviewer_families": sorted(families),
        "configuration": dict(configuration) if configuration is not None else None,
        "configuration_mismatches": configuration_mismatches,
        "dataset_id": dataset.dataset_id if dataset is not None else None,
        "manifest_sha256": dataset.manifest_sha256 if dataset is not None else None,
    }


def require_adoption_ready(gate: Mapping[str, Any]) -> None:
    """Refuse an adoption/accuracy verdict unless every holdout gate passed."""
    if gate.get("eligible") is True:
        return
    blockers = gate.get("blockers")
    detail = "; ".join(str(item) for item in blockers) if isinstance(blockers, list) else "unknown blocker"
    raise EvaluationBoundaryError(f"adoption/accuracy verdict refused: {detail}")

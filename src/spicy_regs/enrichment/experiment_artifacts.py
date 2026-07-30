"""Write lightweight, development-only candidate lookup experiment artifacts.

This module intentionally stops at the experiment boundary. It preserves the
identities and candidate lineage needed to reproduce and compare a run, but it
cannot authorize adoption, accepted output, publication, or deployment.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.ontology.common import canonical_json, text_digest, write_parquet_rows

CORE_FILES = (
    "experiment.json",
    "candidates.parquet",
    "metrics.json",
    "decision.md",
)
DEVELOPMENT_DECISIONS = frozenset({"continue", "investigate", "stop"})

_BASE_CANDIDATE_COLUMNS = (
    "itemId",
    "configuration",
    "conceptId",
    "channel",
    "rank",
    "referenceResourceRelease",
    "registryImportSnapshot",
    "expressionCorpusSnapshot",
    "lookupIndexManifest",
    "indexedExpressionIds",
)
_CANDIDATE_ID_FIELDS = frozenset({"candidateId", "candidate_id", "conceptId", "concept_id"})
_FORBIDDEN_TRUE_FIELDS = frozenset(
    {
        "acceptedoutputeligible",
        "acceptedoutputuse",
        "accuracyverdicteligible",
        "adoptioneligible",
        "adoptionready",
        "promotionauthorized",
        "productionconformanceeligible",
        "requireadoptionready",
        "requireadoptionverdict",
    }
)
_DEVELOPMENT_SCOPE_FIELDS = frozenset({"evaluationscope", "eligibilityscope"})
_ALLOWED_DEVELOPMENT_SCOPES = frozenset({"development", "developmentonly", "training"})
_USAGE_FIELDS = frozenset({"usageceiling", "usageeligibility"})
_FORBIDDEN_USAGE_VALUES = frozenset(
    {
        "acceptedoutput",
        "draftgenerationallowed",
        "localoperationaluse",
        "officialuse",
        "publicationallowed",
    }
)
_RELEASE_IDENTITY_FIELDS = (
    "referenceResourceRelease",
    "registryImportSnapshot",
    "expressionCorpusSnapshot",
    "publicationReleaseId",
    "managedReleaseManifest",
    "managedReleaseManifestDigest",
)
_INDEX_IDENTITY_FIELDS = ("lookupIndexManifest",)
_SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_MANAGED_SOURCE_ALIASES = {
    "referenceResourceRelease": (
        "referenceResourceRelease",
        "reference_resource_release",
    ),
    "registryImportSnapshot": (
        "registryImportSnapshot",
        "registry_import_snapshot",
    ),
    "expressionCorpusSnapshot": (
        "expressionCorpusSnapshot",
        "expression_corpus_snapshot",
    ),
    "publicationReleaseId": (
        "publicationReleaseId",
        "publication_release_id",
    ),
    "managedReleaseManifest": (
        "managedReleaseManifest",
        "bundle_manifest",
    ),
    "managedReleaseManifestDigest": (
        "managedReleaseManifestDigest",
        "bundle_manifest_digest",
    ),
    "lookupIndexManifest": (
        "lookupIndexManifest",
        "lookup_index_manifest",
    ),
}


class ExperimentArtifactError(ValueError):
    """The input cannot be represented as a development-only experiment."""


@dataclass(frozen=True, slots=True)
class ExperimentArtifacts:
    """Paths written for one deterministic experiment directory."""

    directory: Path
    experiment: Path
    candidates: Path
    metrics: Path
    decision: Path


def _normalized_token(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def _plain_json(value: object, label: str) -> Any:
    try:
        return json.loads(canonical_json(value))
    except (TypeError, ValueError) as error:
        raise ExperimentArtifactError(f"{label} must be JSON-compatible: {error}") from error


def _reject_output_claims(
    value: object,
    *,
    path: tuple[str, ...] = (),
) -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized_key = _normalized_token(key)
            child_path = (*path, key)
            normalized_path = tuple(_normalized_token(part) for part in path)
            if child is True and (
                normalized_key in _FORBIDDEN_TRUE_FIELDS
                or (normalized_key == "eligible" and "evaluationboundary" in normalized_path)
            ):
                raise ExperimentArtifactError(f"{'.'.join(child_path)} makes an adoption or accepted-output claim")
            if normalized_key in _DEVELOPMENT_SCOPE_FIELDS and isinstance(child, str):
                if _normalized_token(child) not in _ALLOWED_DEVELOPMENT_SCOPES:
                    raise ExperimentArtifactError(f"{'.'.join(child_path)} must remain development-only")
            if normalized_key in _USAGE_FIELDS and isinstance(child, str):
                if _normalized_token(child) in _FORBIDDEN_USAGE_VALUES:
                    raise ExperimentArtifactError(f"{'.'.join(child_path)} exceeds candidate-only use")
            _reject_output_claims(child, path=child_path)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _reject_output_claims(child, path=(*path, str(index)))


def _configuration_results(
    ablation_result: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw_results = ablation_result.get("results")
    if raw_results is None:
        raw_results = [ablation_result]
    if not isinstance(raw_results, Sequence) or isinstance(
        raw_results,
        (str, bytes, bytearray),
    ):
        raise ExperimentArtifactError("ablation results must be an array")
    results: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_results):
        if not isinstance(raw, Mapping):
            raise ExperimentArtifactError(f"ablation results[{index}] must be an object")
        results.append(_plain_json(dict(raw), f"ablation results[{index}]"))
    if not results:
        raise ExperimentArtifactError("ablation results must not be empty")
    return results


def _count(result: Mapping[str, Any], field: str) -> int:
    value = result.get(field)
    if type(value) is not int or value < 0:
        raise ExperimentArtifactError(
            f"{result.get('configuration', '<unknown>')}.{field} must be a non-negative integer"
        )
    return value


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _stage_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    population = _count(result, "item_count")
    managed_target_fields = (
        "represented_item_count",
        "represented_item_surfaced",
    )
    if any(field in result for field in managed_target_fields):
        available = _count(result, "represented_item_count")
        retrieved = _count(result, "represented_item_surfaced")
        available_field = "represented_item_count"
        retrieved_field = "represented_item_surfaced"
    else:
        available = _count(result, "exact_alias_target_count")
        retrieved = _count(result, "exact_alias_surfaced")
        available_field = "exact_alias_target_count"
        retrieved_field = "exact_alias_surfaced"
    adequate_population = _count(result, "adequate_target_count")
    adequate = _count(result, "adequate_kept")
    if available > population:
        raise ExperimentArtifactError(f"{available_field} cannot exceed item_count")
    if retrieved > available:
        raise ExperimentArtifactError(f"{retrieved_field} cannot exceed {available_field}")
    if adequate > adequate_population:
        raise ExperimentArtifactError("adequate_kept cannot exceed adequate_target_count")
    return {
        "available": {
            "count": available,
            "population": population,
            "rate": _rate(available, population),
        },
        "retrieved": {
            "count": retrieved,
            "available": available,
            "rate": _rate(retrieved, available),
        },
        "adequate": {
            "count": adequate,
            "reviewedTargets": adequate_population,
            "rate": _rate(adequate, adequate_population),
        },
    }


def _result_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    summary = {key: value for key, value in result.items() if key != "items"}
    items = result.get("items")
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
        summary["items"] = [
            {key: value for key, value in item.items() if key not in {"candidates", "candidate_lineage"}}
            for item in items
            if isinstance(item, Mapping)
        ]
    summary["stages"] = _stage_metrics(result)
    return summary


def _walk_named_values(value: object, field: str) -> list[Any]:
    found: list[Any] = []
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            if str(raw_key) == field:
                found.append(_plain_json(child, field))
            found.extend(_walk_named_values(child, field))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            found.extend(_walk_named_values(child, field))
    return found


def _one_exact_identity(
    sources: Sequence[object],
    field: str,
) -> Any | None:
    by_encoding: dict[str, Any] = {}
    for source in sources:
        for value in _walk_named_values(source, field):
            by_encoding[canonical_json(value)] = value
    if len(by_encoding) > 1:
        raise ExperimentArtifactError(f"one experiment cannot mix multiple {field} identities")
    return next(iter(by_encoding.values()), None)


def _one_aliased_identity(
    source: Mapping[str, Any],
    canonical_field: str,
    aliases: Sequence[str],
) -> Any | None:
    by_encoding: dict[str, Any] = {}
    for alias in aliases:
        if alias in source:
            value = _plain_json(source[alias], f"inputs.candidate_source.{alias}")
            by_encoding[canonical_json(value)] = value
    if len(by_encoding) > 1:
        raise ExperimentArtifactError(f"managed candidate source mixes multiple {canonical_field} identities")
    return next(iter(by_encoding.values()), None)


def _require_exact_reference(value: object, label: str) -> None:
    if not isinstance(value, Mapping):
        raise ExperimentArtifactError(f"{label} must be an exact id and digest reference")
    identifier = value.get("id")
    digest = value.get("digest")
    if not isinstance(identifier, str) or not identifier.strip():
        raise ExperimentArtifactError(f"{label}.id is required")
    if not isinstance(digest, str) or not _SHA256_DIGEST.fullmatch(digest):
        raise ExperimentArtifactError(f"{label}.digest must be sha256:<64 lowercase hex>")


def _managed_source_identities(
    inputs: Mapping[str, Any],
) -> dict[str, Any] | None:
    raw_source = inputs.get("candidate_source")
    if raw_source is None:
        return None
    if not isinstance(raw_source, Mapping):
        raise ExperimentArtifactError("inputs.candidate_source must be an object")
    mode = raw_source.get("mode")
    if _normalized_token(mode) != "managedrelease":
        return None

    identities = {
        canonical_field: value
        for canonical_field, aliases in _MANAGED_SOURCE_ALIASES.items()
        if (
            value := _one_aliased_identity(
                raw_source,
                canonical_field,
                aliases,
            )
        )
        is not None
    }
    for field in (
        "managedReleaseManifest",
        "managedReleaseManifestDigest",
        "expressionCorpusSnapshot",
        "lookupIndexManifest",
    ):
        if field not in identities:
            raise ExperimentArtifactError(f"managed experiment requires inputs.candidate_source.{field}")
    manifest = identities["managedReleaseManifest"]
    if not isinstance(manifest, str) or not manifest.strip():
        raise ExperimentArtifactError("inputs.candidate_source.managedReleaseManifest is required")
    manifest_digest = identities["managedReleaseManifestDigest"]
    if not isinstance(manifest_digest, str) or not _SHA256_DIGEST.fullmatch(manifest_digest):
        raise ExperimentArtifactError(
            "inputs.candidate_source.managedReleaseManifestDigest must be sha256:<64 lowercase hex>"
        )
    _require_exact_reference(
        identities["expressionCorpusSnapshot"],
        "inputs.candidate_source.expressionCorpusSnapshot",
    )
    _require_exact_reference(
        identities["lookupIndexManifest"],
        "inputs.candidate_source.lookupIndexManifest",
    )
    publication_release = identities.get("publicationReleaseId")
    if publication_release is not None and (
        not isinstance(publication_release, str) or not publication_release.strip()
    ):
        raise ExperimentArtifactError("inputs.candidate_source.publicationReleaseId must be nonempty")
    return identities


def _identity_sections(
    ablation_result: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    inputs = _plain_json(ablation_result.get("inputs", {}), "inputs")
    settings = _plain_json(ablation_result.get("settings", {}), "settings")
    if not isinstance(inputs, dict) or not isinstance(settings, dict):
        raise ExperimentArtifactError("ablation inputs and settings must be objects")

    managed_identities = _managed_source_identities(inputs)
    identity_sources: list[object] = [
        ablation_result,
        *(() if managed_identities is None else (managed_identities,)),
        *candidate_rows,
    ]
    release = {
        field: value
        for field in _RELEASE_IDENTITY_FIELDS
        if (value := _one_exact_identity(identity_sources, field)) is not None
    }
    index = {
        field: value
        for field in _INDEX_IDENTITY_FIELDS
        if (value := _one_exact_identity(identity_sources, field)) is not None
    }
    if not release:
        release = {
            key: inputs[key]
            for key in (
                "registry_file",
                "registry_sha256",
                "registry_row_count",
                "eligible_concept_count",
            )
            if key in inputs
        }
    for key in ("index", "index_identity", "index_manifest"):
        if key in ablation_result and key not in index:
            index[key] = _plain_json(ablation_result[key], key)

    configurations = [
        {key: result[key] for key in ("configuration", "channels", "quotas", "note") if key in result}
        for result in results
    ]
    configuration: dict[str, Any] = {
        "settings": settings,
        "configurations": configurations,
    }
    for key in ("concept_mapper", "bm25", "keywords"):
        if key in ablation_result:
            configuration[key] = _plain_json(ablation_result[key], key)

    dataset_keys = (
        "dataset_dir",
        "selection_file",
        "gold_file",
        "gold_sha256",
        "targets_file",
        "targets_sha256",
        "target_dataset_id",
        "resolved_file",
        "resolved_sha256",
    )
    dataset = {key: inputs[key] for key in dataset_keys if key in inputs}
    if "evaluation_boundary" in ablation_result:
        dataset["evaluation_boundary"] = _plain_json(
            ablation_result["evaluation_boundary"],
            "evaluation_boundary",
        )

    code: dict[str, Any] = {}
    for key in ("schema_version", "code", "code_identity", "source_revision"):
        if key in ablation_result:
            code[key] = _plain_json(ablation_result[key], key)
    code["channelVersions"] = {key: value for key, value in settings.items() if str(key).endswith("_version")}

    return {
        "input": inputs,
        "configuration": configuration,
        "release": release,
        "index": index,
        "code": code,
        "dataset": dataset,
    }


def _candidate_rows(
    candidate_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    rows: list[dict[str, Any]] = []
    for index, raw in enumerate(candidate_rows):
        if not isinstance(raw, Mapping):
            raise ExperimentArtifactError(f"candidate_rows[{index}] must be an object")
        row = _plain_json(dict(raw), f"candidate_rows[{index}]")
        if not any(field in row and str(row[field]).strip() for field in _CANDIDATE_ID_FIELDS):
            raise ExperimentArtifactError(f"candidate_rows[{index}] lacks a candidate or concept identifier")
        if not isinstance(row.get("channel"), str) or not row["channel"].strip():
            raise ExperimentArtifactError(f"candidate_rows[{index}].channel is required")
        if type(row.get("rank")) is not int or row["rank"] < 1:
            raise ExperimentArtifactError(f"candidate_rows[{index}].rank must be a positive integer")
        rows.append(row)
    rows.sort(key=canonical_json)
    columns = tuple(
        sorted(
            set(_BASE_CANDIDATE_COLUMNS).union(
                *(set(row) for row in rows),
            )
        )
    )
    return rows, columns


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def write_experiment_artifacts(
    output_dir: Path | str,
    ablation_result: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    decision: str,
    rationale: str,
) -> ExperimentArtifacts:
    """Write one deterministic, self-contained development experiment.

    The output directory must be absent or empty. The writer emits exactly the
    four files in :data:`CORE_FILES` and refuses any source claim that would make
    the experiment adoption-eligible or accepted-output eligible.
    """

    if not isinstance(ablation_result, Mapping):
        raise ExperimentArtifactError("ablation_result must be an object")
    normalized_decision = decision.strip().casefold()
    if normalized_decision not in DEVELOPMENT_DECISIONS:
        raise ExperimentArtifactError("decision must be continue, investigate, or stop")
    clean_rationale = rationale.strip()
    if not clean_rationale:
        raise ExperimentArtifactError("rationale must not be empty")

    plain_ablation = _plain_json(dict(ablation_result), "ablation_result")
    plain_candidates = [
        _plain_json(dict(row), f"candidate_rows[{index}]") if isinstance(row, Mapping) else row
        for index, row in enumerate(candidate_rows)
    ]
    _reject_output_claims(plain_ablation)
    _reject_output_claims(plain_candidates)

    results = _configuration_results(plain_ablation)
    rows, columns = _candidate_rows(candidate_rows)
    identities = _identity_sections(plain_ablation, rows, results)
    metrics_payload = {
        "schemaVersion": "spicy-regs-experiment-metrics/v1",
        "evaluationScope": "developmentOnly",
        "stageDefinitions": {
            "available": "The target is represented in the tested vocabulary universe.",
            "retrieved": "The available target appears in the candidate set.",
            "adequate": "A previously reviewed adequate target remains in the candidate set.",
        },
        "runTimingsSeconds": _plain_json(
            plain_ablation.get("timings_seconds", {}),
            "timings_seconds",
        ),
        "reviewedTargetBinding": _plain_json(
            plain_ablation.get("reviewed_target_binding", {}),
            "reviewed_target_binding",
        ),
        "results": [_result_summary(result) for result in results],
    }

    directory = Path(output_dir)
    if directory.exists() and any(directory.iterdir()):
        raise ExperimentArtifactError("output directory must be empty")
    directory.mkdir(parents=True, exist_ok=True)
    candidates_path = directory / "candidates.parquet"
    metrics_path = directory / "metrics.json"
    experiment_path = directory / "experiment.json"
    decision_path = directory / "decision.md"

    write_parquet_rows(candidates_path, columns=columns, rows=rows)
    _write_json(metrics_path, metrics_payload)
    experiment_payload = {
        "schemaVersion": "spicy-regs-candidate-experiment/v1",
        "experimentKind": "candidateSelectorAblation",
        "generatedAt": plain_ablation.get("generated_at"),
        "protocol": _plain_json(
            plain_ablation.get("experiment_protocol", {}),
            "experiment_protocol",
        ),
        "eligibility": {
            "scope": "developmentOnly",
            "candidateUseOnly": True,
            "adoptionEligible": False,
            "acceptedOutputEligible": False,
            "promotionAuthorized": False,
        },
        "identities": identities,
        "sourceAblationDigest": "sha256:" + text_digest(canonical_json(plain_ablation)),
        "candidateRowsDigest": "sha256:" + text_digest(canonical_json(rows)),
        "artifacts": {
            "candidates.parquet": "sha256:" + sha256_file(candidates_path),
            "metrics.json": "sha256:" + sha256_file(metrics_path),
        },
    }
    _write_json(experiment_path, experiment_payload)

    decision_path.write_text(
        "\n".join(
            (
                f"# Development decision: {normalized_decision.title()}",
                "",
                clean_rationale,
                "",
                "This decision applies only to the next development experiment. "
                "It does not authorize adoption, accepted output, promotion, "
                "publication, or deployment.",
                "",
            )
        ),
        encoding="utf-8",
    )

    actual_files = tuple(sorted(path.name for path in directory.iterdir()))
    if actual_files != tuple(sorted(CORE_FILES)):
        raise ExperimentArtifactError(f"experiment directory has unexpected files: {actual_files!r}")
    return ExperimentArtifacts(
        directory=directory,
        experiment=experiment_path,
        candidates=candidates_path,
        metrics=metrics_path,
        decision=decision_path,
    )

"""Compare segmentation arms through the real ontology tagging contract.

The dense and boundary experiment evaluates every segment without paying for
thousands of repeated ontology calls. This downstream experiment keeps every
curated gold artifact, selects each segment intersecting a gold span, and adds
deterministic non-intersecting controls from the same artifact. Every selected
segment then uses the production prompt, concept registry, exact grounding,
artifact-level aggregation, validation, checkpoints, and provider receipts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

from dotenv import load_dotenv

from spicy_regs.corpora.document_acceptance_scope import (
    DocumentAcceptanceScope,
    load_document_acceptance_scope,
)
from spicy_regs.corpora.segmentation_experiment import (
    ARMS,
    build_artifacts,
    validate_segmentation_experiment,
)
from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import (
    RunContext,
    canonical_json,
    read_parquet_rows,
    stable_id,
    text_digest,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import (
    ASSIGNMENT_COLUMNS,
    CONCEPT_COLUMNS,
    aggregate_segment_assignments,
    generate_for_subject,
    merge_seed_registry,
    normalize_label,
    seed_concept,
    select_candidate_concepts,
    supersede_assignment_with_validation,
)
from spicy_regs.ontology.invariants import assert_concept_graphs
from spicy_regs.ontology.llm import (
    EVIDENCE_ALIGNMENT_PROVIDED,
    EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    OntologyModel,
    OpenAIOntologyModel,
    PROMPT_INPUT_TOKEN_BUDGET,
    PROMPT_SAFETY_MARGIN_TOKENS,
    TAG_MAX_ITEMS,
    TAG_MAX_OUTPUT_TOKENS,
    VALIDATION_MAX_OUTPUT_TOKENS,
    model_call_metadata,
    model_run_configuration,
    model_tag_rejections,
    tag_prompt_token_estimate,
)
from spicy_regs.ontology.receipt import _valid_completed_model_call
from spicy_regs.ontology.subjects import Artifact, SourceElement, Subject

FORMAT_VERSION = 1
EXPERIMENT_VERSION = "segmentation-tagging-v4"
EVIDENCE_ALIGNMENT_POLICY = "provided-offsets-or-unique-exact-match-v1"
SKLEARN_VERSION = "1.7.2"
DEFAULT_BUDGET = 1_800
DEFAULT_NEGATIVE_SEGMENTS = 2
FINAL_GENERATION_STATUSES = frozenset({"tagged", "zero_tags", "rejected_output"})

SEGMENT_RESULT_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "profile_id",
    "subject_type",
    "subject_id",
    "artifact_digest",
    "segment_id",
    "ordinal",
    "segment_count",
    "selection_role",
    "gold_ids_json",
    "adversarial_case_ids_json",
    "status",
    "proposal_count",
    "rejection_count",
    "model_call_json",
)
RAW_ASSIGNMENT_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    *ASSIGNMENT_COLUMNS,
)
ASSIGNMENT_RESULT_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "assignment_stage",
    *ASSIGNMENT_COLUMNS,
)
VALIDATION_RESULT_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "work_id",
    "assignment_id",
    "segment_id",
    "source_field",
    "start_char",
    "end_char",
    "agrees",
    "confidence",
    "rationale",
    "actor_id",
    "model_call_json",
)
METRIC_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "scope",
    "profile_id",
    "artifact_count",
    "selected_segment_count",
    "gold_positive_count",
    "predicted_positive_count",
    "true_positive_count",
    "false_positive_count",
    "false_negative_count",
    "micro_precision",
    "micro_recall",
    "micro_f1",
    "artifact_macro_precision",
    "artifact_macro_recall",
    "artifact_macro_f1",
    "artifact_exact_match_rate",
    "zero_tag_rate",
    "grounded_assignment_rate",
    "grounded_span_rate",
    "validation_agreement_rate",
    "raw_assignment_count",
    "aggregated_assignment_count",
    "duplicate_raw_span_rate",
    "multi_segment_assignment_count",
    "cross_segment_disagreement_count",
    "cross_segment_disagreement_rate",
    "adversarial_segment_count",
    "prompt_injection_segment_count",
    "prompt_injection_assignment_count",
    "prompt_injection_grounded_assignment_rate",
    "novel_assignment_rate",
    "metric_provider",
)
TRANSITION_COLUMNS = (
    "transition_ordinal",
    "phase",
    "config_id",
    "segment_id",
    "assignment_id",
    "work_id",
    "status",
    "actor_id",
    "error_code",
    "model_call_json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_hashes(directory: Path) -> dict[str, dict[str, Any]]:
    import pyarrow.parquet as pq

    return {
        path.name: {
            "rows": pq.ParquetFile(path).metadata.num_rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(directory.glob("*.parquet"))
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid JSON object: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root must be an object: {path}")
    return cast(dict[str, Any], value)


def _parse_slices(row: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        values = json.loads(str(row.get("slices_json") or "[]"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{row.get('segment_id')}: invalid slices_json") from exc
    if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
        raise ValueError(f"{row.get('segment_id')}: slices must be objects")
    return [cast(dict[str, Any], value) for value in values]


def _dict_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, Any], row) for row in value if isinstance(row, dict)]


def _string_list(value: object) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return sorted(str(item) for item in parsed if item)


def _scoped_evaluation_rows(
    dataset_dir: Path,
    scope: DocumentAcceptanceScope | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    gold = read_parquet_rows(dataset_dir / "gold_spans.parquet")
    adversarial = read_parquet_rows(dataset_dir / "adversarial_cases.parquet")
    if scope is None:
        return gold, adversarial
    return (
        [
            row
            for row in gold
            if (
                str(row.get("gold_id")) in scope.included_gold_ids
                and str(row.get("artifact_digest")) in scope.included_artifact_digests
            )
        ],
        [row for row in adversarial if str(row.get("case_id")) in scope.included_adversarial_case_ids],
    )


def _overlaps(
    slice_row: dict[str, Any],
    gold: dict[str, Any],
) -> bool:
    return (
        str(slice_row.get("source_field")) == str(gold.get("source_field"))
        and int(str(slice_row.get("start_char"))) < int(str(gold.get("end_char")))
        and int(str(slice_row.get("end_char"))) > int(str(gold.get("start_char")))
    )


def _spread_controls(
    rows: Sequence[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0 or not rows:
        return []
    if len(rows) <= limit:
        return list(rows)
    if limit == 1:
        return [rows[len(rows) // 2]]
    indices = {round(index * (len(rows) - 1) / (limit - 1)) for index in range(limit)}
    return [rows[index] for index in sorted(indices)]


def _selected_tagging_configs(
    experiment_dir: Path,
    *,
    budget: int,
    config_ids: Sequence[str] | None,
) -> list[dict[str, Any]]:
    """Resolve an explicit tagging subset against upstream experiment configs."""
    base_manifest = _load_json_object(experiment_dir / "segmentation-experiment-manifest.json")
    available_configs = [
        cast(dict[str, Any], config)
        for config in base_manifest.get("configs", [])
        if (isinstance(config, dict) and config.get("config_id") and int(str(config.get("max_tokens") or 0)) == budget)
    ]
    available_ids = {str(config["config_id"]) for config in available_configs}
    selected_ids = (
        tuple(str(config_id) for config_id in config_ids)
        if config_ids is not None
        else tuple(str(config["config_id"]) for config in available_configs)
    )
    if not selected_ids or len(set(selected_ids)) != len(selected_ids) or not set(selected_ids) <= available_ids:
        raise ValueError("tagging config IDs must be unique configs at the selected budget")
    selected = set(selected_ids)
    configs = [config for config in available_configs if str(config["config_id"]) in selected]
    if config_ids is None and {str(config.get("arm")) for config in configs} != set(ARMS):
        raise RuntimeError("default tagging configs do not cover all five arms")
    return sorted(configs, key=lambda config: str(config["config_id"]))


def _select_tagging_segments(
    dataset_dir: Path,
    experiment_dir: Path,
    *,
    budget: int = DEFAULT_BUDGET,
    negative_segments_per_artifact: int = DEFAULT_NEGATIVE_SEGMENTS,
    scope: DocumentAcceptanceScope | None = None,
    config_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    if budget <= 0 or negative_segments_per_artifact < 0:
        raise ValueError("invalid tagging sample policy")
    gold_rows, adversarial_rows = _scoped_evaluation_rows(
        dataset_dir,
        scope,
    )
    gold_by_artifact: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for gold in gold_rows:
        gold_by_artifact[str(gold["artifact_digest"])].append(gold)
    adversarial_by_subject: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for case in adversarial_rows:
        adversarial_by_subject[
            (
                str(case["profile_id"]),
                str(case["subject_type"]),
                str(case["subject_id"]),
            )
        ].append(case)
    segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    selected_configs = {
        str(config["config_id"])
        for config in _selected_tagging_configs(
            experiment_dir,
            budget=budget,
            config_ids=config_ids,
        )
    }
    eligible_by_config_artifact: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    adversarial_by_artifact: dict[str, list[dict[str, Any]]] = defaultdict(list)
    profile_by_artifact: dict[str, str] = {}
    for row in segment_rows:
        config_id = str(row.get("config_id") or "")
        artifact_digest = str(row.get("artifact_digest") or "")
        profile_id = str(row.get("profile_id") or "")
        if config_id not in selected_configs or (
            scope is not None and artifact_digest not in scope.included_artifact_digests
        ):
            continue
        prior_profile = profile_by_artifact.setdefault(
            artifact_digest,
            profile_id,
        )
        if prior_profile != profile_id:
            raise RuntimeError(f"{artifact_digest}: artifact has inconsistent profiles")
        eligible_by_config_artifact[(config_id, artifact_digest)].append(row)
        adversarial = adversarial_by_subject.get(
            (
                profile_id,
                str(row.get("subject_type") or ""),
                str(row.get("subject_id") or ""),
            ),
            [],
        )
        for case in adversarial:
            if case not in adversarial_by_artifact[artifact_digest]:
                adversarial_by_artifact[artifact_digest].append(case)
    selected_artifacts = set(gold_by_artifact) | set(adversarial_by_artifact)
    for profile_id in sorted(set(profile_by_artifact.values())):
        if any(profile_by_artifact.get(artifact_digest) == profile_id for artifact_digest in selected_artifacts):
            continue
        profile_artifacts = sorted(
            artifact_digest
            for artifact_digest, candidate_profile in profile_by_artifact.items()
            if candidate_profile == profile_id
        )
        if not profile_artifacts:
            raise RuntimeError(f"{profile_id}: tagging profile has no eligible artifact")
        selected_artifacts.add(profile_artifacts[0])
    by_config_artifact = {
        key: rows for key, rows in eligible_by_config_artifact.items() if key[1] in selected_artifacts
    }
    expected_groups = {
        (config_id, artifact_digest) for config_id in selected_configs for artifact_digest in selected_artifacts
    }
    if set(by_config_artifact) != expected_groups:
        missing = expected_groups - set(by_config_artifact)
        raise RuntimeError(f"{len(missing)} tagging config/artifact groups are missing")

    selected: list[dict[str, Any]] = []
    covered_gold: set[tuple[str, str]] = set()
    for (config_id, artifact_digest), values in sorted(by_config_artifact.items()):
        rows = sorted(
            values,
            key=lambda row: (
                int(str(row.get("ordinal") or 0)),
                str(row.get("segment_id") or ""),
            ),
        )
        gold = gold_by_artifact.get(artifact_digest, [])
        adversarial = adversarial_by_artifact.get(
            artifact_digest,
            [],
        )
        positives: list[dict[str, Any]] = []
        negatives: list[dict[str, Any]] = []
        gold_ids_by_segment: dict[str, list[str]] = {}
        for row in rows:
            slices = _parse_slices(row)
            matched = [str(item["gold_id"]) for item in gold if any(_overlaps(value, item) for value in slices)]
            if matched:
                positives.append(row)
                gold_ids_by_segment[str(row["segment_id"])] = sorted(matched)
                covered_gold.update((config_id, gold_id) for gold_id in matched)
            else:
                negatives.append(row)
        controls = (
            negatives
            if adversarial
            else _spread_controls(
                negatives,
                negative_segments_per_artifact,
            )
        )
        for row in [*positives, *controls]:
            segment_id = str(row["segment_id"])
            selected.append(
                {
                    **row,
                    "selection_role": (
                        "gold-overlap"
                        if segment_id in gold_ids_by_segment
                        else ("adversarial-control" if adversarial else "within-artifact-control")
                    ),
                    "gold_ids_json": canonical_json(gold_ids_by_segment.get(segment_id, [])),
                    "adversarial_case_ids_json": canonical_json(sorted(str(case["case_id"]) for case in adversarial)),
                }
            )
    expected_gold = {(config_id, str(gold["gold_id"])) for config_id in selected_configs for gold in gold_rows}
    if covered_gold != expected_gold:
        raise RuntimeError(f"{len(expected_gold - covered_gold)} gold spans have no selected intersecting segment")
    return sorted(
        selected,
        key=lambda row: (
            str(row["config_id"]),
            str(row["profile_id"]),
            str(row["subject_type"]),
            str(row["subject_id"]),
            int(str(row["ordinal"])),
            str(row["segment_id"]),
        ),
    )


def select_tagging_segments(
    dataset_dir: Path,
    experiment_dir: Path,
    *,
    budget: int = DEFAULT_BUDGET,
    negative_segments_per_artifact: int = DEFAULT_NEGATIVE_SEGMENTS,
    scope_dir: Path | None = None,
    config_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate the base experiment, then select the bounded tag sample."""
    receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if receipt.get("status") != "pass":
        raise RuntimeError("base segmentation experiment did not validate")
    scope = load_document_acceptance_scope(dataset_dir, scope_dir) if scope_dir is not None else None
    return _select_tagging_segments(
        dataset_dir,
        experiment_dir,
        budget=budget,
        negative_segments_per_artifact=negative_segments_per_artifact,
        scope=scope,
        config_ids=config_ids,
    )


def tagging_preflight(
    dataset_dir: Path,
    experiment_dir: Path,
    *,
    registry_path: Path | None = None,
    budget: int = DEFAULT_BUDGET,
    negative_segments_per_artifact: int = DEFAULT_NEGATIVE_SEGMENTS,
    scope_dir: Path | None = None,
    config_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if receipt.get("status") != "pass":
        raise RuntimeError("base segmentation experiment did not validate")
    scope = load_document_acceptance_scope(dataset_dir, scope_dir) if scope_dir is not None else None
    selected_configs = _selected_tagging_configs(
        experiment_dir,
        budget=budget,
        config_ids=config_ids,
    )
    selected_config_ids = [str(config["config_id"]) for config in selected_configs]
    rows = _select_tagging_segments(
        dataset_dir,
        experiment_dir,
        budget=budget,
        negative_segments_per_artifact=(negative_segments_per_artifact),
        scope=scope,
        config_ids=selected_config_ids,
    )
    gold, adversarial = _scoped_evaluation_rows(
        dataset_dir,
        scope,
    )
    selected_adversarial = {case_id for row in rows for case_id in _string_list(row.get("adversarial_case_ids_json"))}
    result = {
        "format_version": FORMAT_VERSION,
        "document_scope_id": scope.scope_id if scope is not None else None,
        "document_scope_policy_version": (scope.scope_policy_version if scope is not None else None),
        "budget": budget,
        "negative_segments_per_artifact": (negative_segments_per_artifact),
        "config_ids": selected_config_ids,
        "config_count": len(selected_configs),
        "selected_profile_ids": sorted({str(row["profile_id"]) for row in rows}),
        "selected_profile_count": len({str(row["profile_id"]) for row in rows}),
        "artifact_count": len({str(row["artifact_digest"]) for row in rows}),
        "gold_span_count": len(gold),
        "adversarial_case_count": len(adversarial),
        "selected_adversarial_case_count": len(selected_adversarial),
        "selected_adversarial_case_ids": sorted(selected_adversarial),
        "selected_segment_count": len(rows),
        "gold_overlap_segment_count": sum(row["selection_role"] == "gold-overlap" for row in rows),
        "control_segment_count": sum(row["selection_role"] == "within-artifact-control" for row in rows),
        "adversarial_segment_count": sum(bool(_string_list(row.get("adversarial_case_ids_json"))) for row in rows),
        "selected_segments_by_config": dict(sorted(Counter(str(row["config_id"]) for row in rows).items())),
    }
    if registry_path is not None:
        if not registry_path.is_file():
            raise FileNotFoundError(f"concept registry missing: {registry_path}")
        subjects = _subjects_from_selected(
            dataset_dir,
            experiment_dir,
            rows,
        )
        registry_context = RunContext(
            "segmentation-tagging-preflight-registry",
            "1970-01-01T00:00:00Z",
        )
        concepts, _ = _gold_registry(
            read_parquet_rows(registry_path),
            gold,
            context=registry_context,
        )
        estimates = [
            tag_prompt_token_estimate(
                subjects[key],
                select_candidate_concepts(subjects[key], concepts),
            )
            for key in sorted(subjects)
        ]
        safe_prompt_limit = PROMPT_INPUT_TOKEN_BUDGET - PROMPT_SAFETY_MARGIN_TOKENS
        result.update(
            {
                "registry_sha256": _sha256(registry_path),
                "generation_call_count": len(estimates),
                "generation_prompt_input_token_estimate": sum(estimates),
                "generation_prompt_input_token_max": max(
                    estimates,
                    default=0,
                ),
                "generation_prompt_budget_failure_count": sum(estimate > safe_prompt_limit for estimate in estimates),
                "generation_max_output_token_cap": (len(estimates) * TAG_MAX_OUTPUT_TOKENS),
                "validation_call_upper_bound": (len(estimates) * TAG_MAX_ITEMS),
                "validation_max_output_token_cap_upper_bound": (
                    len(estimates) * TAG_MAX_ITEMS * VALIDATION_MAX_OUTPUT_TOKENS
                ),
                "api_call_lower_bound": len(estimates),
                "api_call_upper_bound": (len(estimates) * (1 + TAG_MAX_ITEMS)),
                "prompt_input_token_budget": (PROMPT_INPUT_TOKEN_BUDGET),
                "prompt_safety_margin_tokens": (PROMPT_SAFETY_MARGIN_TOKENS),
            }
        )
    return result


def _matching_element(
    artifact: Artifact,
    source_field: str,
    start_char: int,
    end_char: int,
) -> SourceElement | None:
    candidates = [
        element
        for element in artifact.elements
        if (element.source_field == source_field and element.start_char < end_char and element.end_char > start_char)
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda element: (
            not (element.start_char <= start_char and element.end_char >= end_char),
            -(
                min(element.end_char, end_char)
                - max(
                    element.start_char,
                    start_char,
                )
            ),
            element.ordinal,
        ),
    )


def _subjects_from_selected(
    dataset_dir: Path,
    experiment_dir: Path,
    selected_rows: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], Subject]:
    artifacts = {artifact.digest: artifact for artifact in build_artifacts(dataset_dir)}
    selected_groups: set[tuple[str, str]] = set()
    for row in selected_rows:
        selected_groups.add(
            (
                str(row["config_id"]),
                str(row["artifact_digest"]),
            )
        )
    full_rows_by_group: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in read_parquet_rows(experiment_dir / "experiment_segments.parquet"):
        group = (
            str(row["config_id"]),
            str(row["artifact_digest"]),
        )
        if group in selected_groups:
            full_rows_by_group[group].append(row)
    if set(full_rows_by_group) != selected_groups:
        raise RuntimeError("full segmentation adjacency is incomplete")
    full_ids_by_group: dict[tuple[str, str], list[str]] = {}
    for group, rows in full_rows_by_group.items():
        ordered = sorted(
            rows,
            key=lambda candidate: (
                int(str(candidate["ordinal"])),
                str(candidate["segment_id"]),
            ),
        )
        expected_count = int(str(ordered[0]["segment_count"]))
        if len(ordered) != expected_count or any(int(str(row["segment_count"])) != expected_count for row in ordered):
            raise RuntimeError(f"{group}: full segmentation count is inconsistent")
        full_ids_by_group[group] = [str(candidate["segment_id"]) for candidate in ordered]

    result: dict[tuple[str, str], Subject] = {}
    for row in selected_rows:
        config_id = str(row["config_id"])
        artifact_digest = str(row["artifact_digest"])
        segment_id = str(row["segment_id"])
        artifact = artifacts.get(artifact_digest)
        if artifact is None:
            raise RuntimeError(f"{segment_id}: artifact digest is not in the dataset")
        slices = _parse_slices(row)
        fields: dict[str, str] = {}
        source_spans: dict[str, tuple[int, int]] = {}
        source_sha256: dict[str, str] = {}
        field_sources: dict[str, str] = {}
        element_ids: dict[str, str] = {}
        element_kinds: dict[str, str] = {}
        parent_element_ids: dict[str, str | None] = {}
        heading_paths: set[str] = set()
        for index, value in enumerate(slices):
            source_field = str(value["source_field"])
            start = int(str(value["start_char"]))
            end = int(str(value["end_char"]))
            text = str(value["text"])
            if start < 0 or end <= start or artifact.raw_fields.get(source_field, "")[start:end] != text:
                raise RuntimeError(f"{segment_id}: slice does not resolve to source")
            field_key = f"{source_field}::experiment:{index}:{start}-{end}"
            fields[field_key] = text
            source_spans[field_key] = (start, end)
            source_sha256[field_key] = str(value["source_sha256"])
            field_sources[field_key] = source_field
            element = _matching_element(
                artifact,
                source_field,
                start,
                end,
            )
            if element is not None:
                element_ids[field_key] = element.element_id
                element_kinds[field_key] = element.kind
                parent_element_ids[field_key] = element.parent_element_id
                if element.ancestor_path:
                    heading_paths.add(" > ".join(element.ancestor_path))
        context_fields = dict(artifact.context_fields)
        if heading_paths:
            context_fields["heading_path"] = "\n".join(sorted(heading_paths))
        full_ids = full_ids_by_group[(config_id, artifact_digest)]
        selected_index = full_ids.index(segment_id)
        segment_digest = text_digest(
            canonical_json(
                {
                    "artifact_digest": artifact_digest,
                    "segment_id": segment_id,
                    "fields": fields,
                    "source_spans": source_spans,
                    "policy": row["policy_version"],
                }
            )
        )
        result[(config_id, segment_id)] = Subject(
            subject_type=artifact.subject_type,
            subject_id=artifact.subject_id,
            text="\n".join(fields.values()),
            fields=fields,
            digest=segment_digest,
            profile_id=artifact.profile_id,
            source_table=artifact.source_table,
            allowed_schemes=artifact.allowed_schemes,
            artifact_digest=artifact.digest,
            segment_id=segment_id,
            segment_ordinal=int(str(row["ordinal"])),
            segment_count=int(str(row["segment_count"])),
            segment_policy=str(row["policy_version"]),
            tokenizer=str(row["tokenizer"]),
            tokenizer_version=str(row["tokenizer_version"]),
            token_count=int(str(row["token_count"])),
            max_segment_tokens=int(str(row["max_tokens"])),
            min_segment_tokens=int(str(row["min_tokens"])),
            source_spans=source_spans,
            source_sha256=source_sha256,
            field_sources=field_sources,
            boundaries={field: str(row["boundary_method"]) for field in fields},
            element_ids=element_ids,
            element_kinds=element_kinds,
            parent_element_ids=parent_element_ids,
            context_fields=context_fields,
            previous_segment_id=(full_ids[selected_index - 1] if selected_index else None),
            next_segment_id=(full_ids[selected_index + 1] if selected_index + 1 < len(full_ids) else None),
        )
    return result


def _gold_registry(
    registry_rows: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
    *,
    context: RunContext,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    concepts = [dict(row) for row in registry_rows]
    seeds = [
        seed_concept({"name": str(row["concept_label"])}, context)
        for row in gold_rows
        if str(row.get("concept_scheme")) == "subject"
    ]
    concepts = merge_seed_registry(
        concepts,
        (seed for seed in seeds if seed is not None),
    )
    assert_concept_graphs(concepts)
    by_alias: dict[tuple[str, str], str] = {}
    for concept in sorted(
        concepts,
        key=lambda row: str(row.get("concept_id") or ""),
    ):
        key = (
            str(concept.get("scheme") or ""),
            normalize_label(concept.get("pref_label")),
        )
        by_alias.setdefault(key, str(concept["concept_id"]))
        try:
            aliases = json.loads(str(concept.get("alt_labels_json") or "[]"))
        except json.JSONDecodeError:
            aliases = []
        if isinstance(aliases, list):
            for alias in aliases:
                by_alias.setdefault(
                    (
                        str(concept.get("scheme") or ""),
                        normalize_label(alias),
                    ),
                    str(concept["concept_id"]),
                )
    gold_ids: dict[str, str] = {}
    for row in gold_rows:
        key = (
            str(row["concept_scheme"]),
            normalize_label(row["concept_label"]),
        )
        concept_id = by_alias.get(key)
        if not concept_id:
            raise RuntimeError(f"{row['gold_id']}: gold concept is absent from registry")
        gold_ids[str(row["gold_id"])] = concept_id
    return concepts, gold_ids


def _evidence(assignment: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(assignment.get("evidence_json") or "{}"))
    except json.JSONDecodeError:
        return {}
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _single_span_assignment(
    assignment: dict[str, Any],
    span: dict[str, Any],
) -> dict[str, Any]:
    evidence = _evidence(assignment)
    evidence["spans"] = [span]
    return {
        **assignment,
        "evidence_json": canonical_json(evidence),
    }


def _validation_work_id(
    config_id: str,
    assignment_id: str,
    span: dict[str, Any],
) -> str:
    return stable_id(
        "segmentation-tag-validation",
        config_id,
        assignment_id,
        span.get("segment_id"),
        span.get("source_field"),
        span.get("start_char"),
        span.get("end_char"),
    )


def _safe_error(error: BaseException) -> dict[str, str]:
    return {
        "error_code": type(error).__name__,
        "error_message": " ".join(str(error).split())[:500],
    }


def _metric_provider() -> str:
    import sklearn

    if sklearn.__version__ != SKLEARN_VERSION:
        raise RuntimeError(
            f"scikit-learn version differs from the pinned tagging contract: {sklearn.__version__} != {SKLEARN_VERSION}"
        )
    return f"scikit-learn:{sklearn.__version__}"


def _assignment_is_grounded(
    assignment: dict[str, Any],
    subjects: dict[tuple[str, str], Subject],
    config_id: str,
) -> bool:
    evidence = _evidence(assignment)
    artifact_digest = str(evidence.get("artifact_sha256") or evidence.get("subject_sha256") or "")
    spans = evidence.get("spans")
    if not isinstance(spans, list) or not spans:
        return False
    for value in spans:
        if not isinstance(value, dict):
            return False
        span = cast(dict[str, Any], value)
        if span.get("alignment_method") not in {
            EVIDENCE_ALIGNMENT_PROVIDED,
            EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
        }:
            return False
        segment_id = str(span.get("segment_id") or "")
        subject = subjects.get((config_id, segment_id))
        if subject is None or subject.version_digest != artifact_digest:
            return False
        field_key = str(span.get("evidence_field_key") or span.get("source_field") or "")
        field_text = subject.fields.get(field_key)
        source_span = (subject.source_spans or {}).get(field_key)
        try:
            start = int(str(span.get("start_char")))
            end = int(str(span.get("end_char")))
            local_start = int(str(span.get("segment_start_char")))
            local_end = int(str(span.get("segment_end_char")))
        except (TypeError, ValueError):
            return False
        if (
            not isinstance(field_text, str)
            or source_span is None
            or start != source_span[0] + local_start
            or end != source_span[0] + local_end
            or local_start < 0
            or local_end <= local_start
            or local_end > len(field_text)
            or field_text[local_start:local_end] != span.get("text")
            or (subject.field_sources or {}).get(field_key) != span.get("source_field")
        ):
            return False
    return True


def _raw_span_key(assignment: dict[str, Any]) -> str:
    spans = _evidence(assignment).get("spans")
    canonical_spans = (
        [
            {
                "source_field": span.get("source_field"),
                "start_char": span.get("start_char"),
                "end_char": span.get("end_char"),
                "text": span.get("text"),
            }
            for span in spans
            if isinstance(span, dict)
        ]
        if isinstance(spans, list)
        else []
    )
    return canonical_json(
        {
            "subject_type": assignment.get("subject_type"),
            "subject_id": assignment.get("subject_id"),
            "concept_id": assignment.get("concept_id"),
            "artifact_digest": _evidence(assignment).get("artifact_sha256"),
            "spans": canonical_spans,
        }
    )


def _metrics_for_scope(
    *,
    config: dict[str, Any],
    profile_id: str | None,
    gold_rows: Sequence[dict[str, Any]],
    gold_concept_ids: dict[str, str],
    selected_rows: Sequence[dict[str, Any]],
    segment_results: Sequence[dict[str, Any]],
    raw_assignments: Sequence[dict[str, Any]],
    aggregate_assignments: Sequence[dict[str, Any]],
    validations: Sequence[dict[str, Any]],
    subjects: dict[tuple[str, str], Subject],
) -> dict[str, Any]:
    from sklearn.metrics import precision_recall_fscore_support

    scoped_gold = [row for row in gold_rows if (profile_id is None or str(row["profile_id"]) == profile_id)]
    artifact_keys = sorted(
        {
            (
                str(row["subject_type"]),
                str(row["subject_id"]),
                str(row["artifact_digest"]),
            )
            for row in scoped_gold
        }
    )
    artifact_set = set(artifact_keys)
    target_ids = sorted(set(gold_concept_ids.values()))
    target_index = {concept_id: index for index, concept_id in enumerate(target_ids)}
    true_by_artifact: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in scoped_gold:
        true_by_artifact[
            (
                str(row["subject_type"]),
                str(row["subject_id"]),
                str(row["artifact_digest"]),
            )
        ].add(gold_concept_ids[str(row["gold_id"])])
    scoped_aggregates = [
        row
        for row in aggregate_assignments
        if (
            str(row.get("subject_type") or ""),
            str(row.get("subject_id") or ""),
            str(_evidence(row).get("artifact_sha256") or _evidence(row).get("subject_sha256") or ""),
        )
        in artifact_set
    ]
    predicted_by_artifact: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in scoped_aggregates:
        concept_id = str(row.get("concept_id") or "")
        if concept_id in target_index:
            predicted_by_artifact[
                (
                    str(row["subject_type"]),
                    str(row["subject_id"]),
                    str(_evidence(row)["artifact_sha256"]),
                )
            ].add(concept_id)
    y_true = [[int(concept_id in true_by_artifact[key]) for concept_id in target_ids] for key in artifact_keys]
    y_pred = [[int(concept_id in predicted_by_artifact[key]) for concept_id in target_ids] for key in artifact_keys]
    micro = precision_recall_fscore_support(
        y_true,
        y_pred,
        average="micro",
        zero_division=0,
    )
    artifact_macro = precision_recall_fscore_support(
        y_true,
        y_pred,
        average="samples",
        zero_division=0,
    )
    true_positive = sum(
        truth and predicted
        for truth_row, predicted_row in zip(y_true, y_pred)
        for truth, predicted in zip(truth_row, predicted_row)
    )
    false_positive = sum(
        not truth and predicted
        for truth_row, predicted_row in zip(y_true, y_pred)
        for truth, predicted in zip(truth_row, predicted_row)
    )
    false_negative = sum(
        truth and not predicted
        for truth_row, predicted_row in zip(y_true, y_pred)
        for truth, predicted in zip(truth_row, predicted_row)
    )
    artifact_exact_matches = sum(true_by_artifact[key] == predicted_by_artifact[key] for key in artifact_keys)
    scoped_selected = [
        row
        for row in selected_rows
        if (
            str(row["config_id"]) == str(config["config_id"])
            and (profile_id is None or str(row["profile_id"]) == profile_id)
        )
    ]
    scoped_segment_ids = {str(row["segment_id"]) for row in scoped_selected}
    adversarial_selected = [row for row in scoped_selected if _string_list(row.get("adversarial_case_ids_json"))]
    prompt_injection_segment_ids = {
        str(row["segment_id"])
        for row in adversarial_selected
        if "adversarial-prompt-injection" in _string_list(row.get("adversarial_case_ids_json"))
    }
    scoped_segment_results = [
        row
        for row in segment_results
        if (str(row["config_id"]) == str(config["config_id"]) and str(row["segment_id"]) in scoped_segment_ids)
    ]
    scoped_raw = [
        row
        for row in raw_assignments
        if (str(_evidence(row).get("artifact_sha256") or "") in {key[2] for key in artifact_set})
    ]
    prompt_injection_raw = [
        row
        for row in raw_assignments
        if any(
            str(span.get("segment_id") or "") in prompt_injection_segment_ids
            for span in _evidence(row).get("spans") or []
            if isinstance(span, dict)
        )
    ]
    scoped_validation = [
        row
        for row in validations
        if (str(row["config_id"]) == str(config["config_id"]) and str(row["segment_id"]) in scoped_segment_ids)
    ]
    grounded = sum(
        _assignment_is_grounded(
            row,
            subjects,
            str(config["config_id"]),
        )
        for row in scoped_aggregates
    )
    aggregate_span_count = sum(
        len([span for span in _evidence(row).get("spans") or [] if isinstance(span, dict)]) for row in scoped_aggregates
    )
    grounded_spans = sum(
        _assignment_is_grounded(
            _single_span_assignment(row, cast(dict[str, Any], span)),
            subjects,
            str(config["config_id"]),
        )
        for row in scoped_aggregates
        for span in _evidence(row).get("spans") or []
        if isinstance(span, dict)
    )
    multi_segment = [
        row
        for row in scoped_aggregates
        if len(
            {
                str(span.get("segment_id") or "")
                for span in _evidence(row).get("spans") or []
                if isinstance(span, dict) and span.get("segment_id")
            }
        )
        > 1
    ]
    validation_outcomes: dict[str, set[bool]] = defaultdict(set)
    for row in scoped_validation:
        validation_outcomes[str(row["assignment_id"])].add(row["agrees"] is True)
    disagreement_count = sum(len(validation_outcomes[str(row["assignment_id"])]) > 1 for row in multi_segment)
    unique_raw_spans = {_raw_span_key(row) for row in scoped_raw}
    novel = sum(str(row.get("concept_id") or "") not in target_index for row in scoped_aggregates)
    return {
        "config_id": config["config_id"],
        "arm": config["arm"],
        "max_tokens": config["max_tokens"],
        "scope": "all-gold-artifacts" if profile_id is None else "profile",
        "profile_id": profile_id,
        "artifact_count": len(artifact_keys),
        "selected_segment_count": len(scoped_selected),
        "gold_positive_count": sum(map(sum, y_true)),
        "predicted_positive_count": sum(map(sum, y_pred)),
        "true_positive_count": true_positive,
        "false_positive_count": false_positive,
        "false_negative_count": false_negative,
        "micro_precision": float(micro[0]),
        "micro_recall": float(micro[1]),
        "micro_f1": float(micro[2]),
        "artifact_macro_precision": float(artifact_macro[0]),
        "artifact_macro_recall": float(artifact_macro[1]),
        "artifact_macro_f1": float(artifact_macro[2]),
        "artifact_exact_match_rate": (artifact_exact_matches / len(artifact_keys) if artifact_keys else 1.0),
        "zero_tag_rate": (
            sum(row["status"] == "zero_tags" for row in scoped_segment_results) / len(scoped_segment_results)
            if scoped_segment_results
            else 0.0
        ),
        "grounded_assignment_rate": (grounded / len(scoped_aggregates) if scoped_aggregates else 1.0),
        "grounded_span_rate": (grounded_spans / aggregate_span_count if aggregate_span_count else 1.0),
        "validation_agreement_rate": (
            sum(row["agrees"] is True for row in scoped_validation) / len(scoped_validation)
            if scoped_validation
            else 1.0
        ),
        "raw_assignment_count": len(scoped_raw),
        "aggregated_assignment_count": len(scoped_aggregates),
        "duplicate_raw_span_rate": ((len(scoped_raw) - len(unique_raw_spans)) / len(scoped_raw) if scoped_raw else 0.0),
        "multi_segment_assignment_count": len(multi_segment),
        "cross_segment_disagreement_count": disagreement_count,
        "cross_segment_disagreement_rate": (disagreement_count / len(multi_segment) if multi_segment else 0.0),
        "adversarial_segment_count": len(adversarial_selected),
        "prompt_injection_segment_count": len(prompt_injection_segment_ids),
        "prompt_injection_assignment_count": len(prompt_injection_raw),
        "prompt_injection_grounded_assignment_rate": (
            sum(
                _assignment_is_grounded(
                    row,
                    subjects,
                    str(config["config_id"]),
                )
                for row in prompt_injection_raw
            )
            / len(prompt_injection_raw)
            if prompt_injection_raw
            else 1.0
        ),
        "novel_assignment_rate": (novel / len(scoped_aggregates) if scoped_aggregates else 0.0),
        "metric_provider": _metric_provider(),
    }


def _metric_rows(
    *,
    configs: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
    gold_concept_ids: dict[str, str],
    selected_rows: Sequence[dict[str, Any]],
    segment_results: Sequence[dict[str, Any]],
    raw_by_config: dict[str, list[dict[str, Any]]],
    aggregate_by_config: dict[str, list[dict[str, Any]]],
    validations: Sequence[dict[str, Any]],
    subjects: dict[tuple[str, str], Subject],
) -> list[dict[str, Any]]:
    profiles = sorted({str(row["profile_id"]) for row in gold_rows})
    rows: list[dict[str, Any]] = []
    for config in configs:
        config_id = str(config["config_id"])
        for profile_id in [None, *profiles]:
            rows.append(
                _metrics_for_scope(
                    config=config,
                    profile_id=profile_id,
                    gold_rows=gold_rows,
                    gold_concept_ids=gold_concept_ids,
                    selected_rows=selected_rows,
                    segment_results=segment_results,
                    raw_assignments=raw_by_config[config_id],
                    aggregate_assignments=aggregate_by_config[config_id],
                    validations=validations,
                    subjects=subjects,
                )
            )
    return rows


def _transition_rows(
    generation: BatchCheckpoint,
    validation: BatchCheckpoint,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ordinal = 0
    for phase, checkpoint in (
        ("generation", generation),
        ("validation", validation),
    ):
        for record in checkpoint.transitions():
            ordinal += 1
            rows.append(
                {
                    "transition_ordinal": ordinal,
                    "phase": phase,
                    "config_id": record.get("config_id"),
                    "segment_id": record.get("segment_id"),
                    "assignment_id": record.get("assignment_id"),
                    "work_id": record.get("work_id"),
                    "status": record.get("status"),
                    "actor_id": record.get("actor_id"),
                    "error_code": record.get("error_code"),
                    "model_call_json": (
                        canonical_json(record["model_call"]) if isinstance(record.get("model_call"), dict) else None
                    ),
                }
            )
    return rows


def _secret_matches(directory: Path) -> list[str]:
    matches: list[str] = []
    needles = (b"sk-proj-", b"OPENAI_API_KEY=", b"Bearer sk-")
    for path in sorted(value for value in directory.rglob("*") if value.is_file()):
        data = path.read_bytes()
        if any(needle in data for needle in needles):
            matches.append(str(path.relative_to(directory)))
    return matches


def _run_context(
    work_dir: Path,
    *,
    run_id: str | None,
    asserted_at: str | None,
    identity: dict[str, Any],
) -> RunContext:
    state_path = work_dir / "run-context.json"
    if state_path.exists():
        state = _load_json_object(state_path)
        if state.get("identity") != identity:
            raise RuntimeError("tagging checkpoint identity is incompatible")
        return RunContext(
            str(state["run_id"]),
            str(state["asserted_at"]),
        )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="segmentation-tagging",
    )
    state_path.write_text(
        json.dumps(
            {
                "identity": identity,
                "run_id": context.run_id,
                "asserted_at": context.asserted_at,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return context


def build_tagging_experiment(
    dataset_dir: Path,
    experiment_dir: Path,
    registry_path: Path,
    output_dir: Path,
    *,
    model: OntologyModel,
    budget: int = DEFAULT_BUDGET,
    negative_segments_per_artifact: int = DEFAULT_NEGATIVE_SEGMENTS,
    scope_dir: Path | None = None,
    config_ids: Sequence[str] | None = None,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> dict[str, Any]:
    """Run the common ontology path on a bounded gold-sample config set."""
    if output_dir.exists():
        raise FileExistsError(f"Refusing to replace tagging experiment: {output_dir}")
    base_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if base_receipt.get("status") != "pass":
        raise RuntimeError("base segmentation experiment did not validate")
    if not registry_path.is_file():
        raise FileNotFoundError(f"concept registry missing: {registry_path}")
    base_manifest = _load_json_object(experiment_dir / "segmentation-experiment-manifest.json")
    scope = load_document_acceptance_scope(dataset_dir, scope_dir) if scope_dir is not None else None
    selected_configs = _selected_tagging_configs(
        experiment_dir,
        budget=budget,
        config_ids=config_ids,
    )
    selected_config_ids = [str(config["config_id"]) for config in selected_configs]
    selected_rows = _select_tagging_segments(
        dataset_dir,
        experiment_dir,
        budget=budget,
        negative_segments_per_artifact=(negative_segments_per_artifact),
        scope=scope,
        config_ids=selected_config_ids,
    )
    subjects = _subjects_from_selected(
        dataset_dir,
        experiment_dir,
        selected_rows,
    )
    gold_rows, adversarial_rows = _scoped_evaluation_rows(
        dataset_dir,
        scope,
    )
    model_configuration = model_run_configuration(model)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    work_dir = output_dir.parent / f".{output_dir.name}.tagging-work"
    work_dir.mkdir(parents=True, exist_ok=True)
    identity = {
        "experiment_version": EXPERIMENT_VERSION,
        "base_experiment_id": base_receipt["experiment_id"],
        "registry_sha256": _sha256(registry_path),
        "model_id": model.model_id,
        "model_configuration": model_configuration,
        "budget": budget,
        "config_ids": selected_config_ids,
        "tag_max_output_tokens": TAG_MAX_OUTPUT_TOKENS,
        "validation_max_output_tokens": VALIDATION_MAX_OUTPUT_TOKENS,
        "evidence_alignment_policy": EVIDENCE_ALIGNMENT_POLICY,
        "negative_segments_per_artifact": (negative_segments_per_artifact),
        "document_scope_id": (scope.scope_id if scope is not None else None),
    }
    context = _run_context(
        work_dir,
        run_id=run_id,
        asserted_at=asserted_at,
        identity=identity,
    )
    registry_context = RunContext(
        f"{context.run_id}-gold-registry",
        context.asserted_at,
    )
    concepts, gold_concept_ids = _gold_registry(
        read_parquet_rows(registry_path),
        gold_rows,
        context=registry_context,
    )
    generation_checkpoint = BatchCheckpoint(
        work_dir,
        run_id=context.run_id,
        phase="tagging-generation",
    )
    validation_checkpoint = BatchCheckpoint(
        work_dir,
        run_id=context.run_id,
        phase="tagging-validation",
    )
    segment_results: list[dict[str, Any]] = []
    generated_concepts: dict[str, dict[str, Any]] = {}
    raw_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    config_by_id = {str(config["config_id"]): config for config in selected_configs}
    selected_by_key = {
        (
            str(row["config_id"]),
            str(row["segment_id"]),
        ): row
        for row in selected_rows
    }

    for key in sorted(subjects):
        config_id, segment_id = key
        subject = subjects[key]
        config = config_by_id[config_id]
        work_id = stable_id(
            "segmentation-tagging-work",
            config_id,
            subject.version_digest,
            segment_id,
            model.model_id,
        )
        cached = generation_checkpoint.get(
            subject.subject_type,
            subject.subject_id,
            artifact_digest=subject.version_digest,
            segment_id=segment_id,
            work_id=work_id,
        )
        if cached is None or cached.get("status") not in (FINAL_GENERATION_STATUSES):
            try:
                new_concepts, assignments, _ = generate_for_subject(
                    subject=subject,
                    concepts=concepts,
                    model=model,
                    context=context,
                )
                rejections = model_tag_rejections(model)
                status = "tagged" if assignments else ("rejected_output" if rejections else "zero_tags")
                cached = {
                    "subject_type": subject.subject_type,
                    "subject_id": subject.subject_id,
                    "artifact_digest": subject.version_digest,
                    "segment_id": segment_id,
                    "work_id": work_id,
                    "config_id": config_id,
                    "status": status,
                    "actor_id": model.model_id,
                    "concepts": new_concepts,
                    "assignments": assignments,
                    "rejections": rejections,
                    "model_call": model_call_metadata(model),
                }
                generation_checkpoint.append(cached)
            except Exception as exc:
                generation_checkpoint.append(
                    {
                        "subject_type": subject.subject_type,
                        "subject_id": subject.subject_id,
                        "artifact_digest": subject.version_digest,
                        "segment_id": segment_id,
                        "work_id": work_id,
                        "config_id": config_id,
                        "status": "retry_exhausted",
                        "actor_id": model.model_id,
                        "model_call": model_call_metadata(model),
                        **_safe_error(exc),
                    }
                )
                raise RuntimeError(
                    f"{config_id}/{segment_id}: tagging failed; the exact checkpoint is resumable"
                ) from exc
        assignments = _dict_rows(cached.get("assignments"))
        new_concepts = _dict_rows(cached.get("concepts"))
        rejections = _dict_rows(cached.get("rejections"))
        for concept in new_concepts:
            if concept.get("concept_id"):
                generated_concepts[str(concept["concept_id"])] = concept
        raw_by_config[config_id].extend(assignments)
        selected = selected_by_key[key]
        segment_results.append(
            {
                "config_id": config_id,
                "arm": config["arm"],
                "max_tokens": config["max_tokens"],
                "profile_id": subject.profile_id,
                "subject_type": subject.subject_type,
                "subject_id": subject.subject_id,
                "artifact_digest": subject.version_digest,
                "segment_id": segment_id,
                "ordinal": subject.segment_ordinal,
                "segment_count": subject.segment_count,
                "selection_role": selected["selection_role"],
                "gold_ids_json": selected["gold_ids_json"],
                "adversarial_case_ids_json": selected["adversarial_case_ids_json"],
                "status": cached["status"],
                "proposal_count": len(assignments),
                "rejection_count": len(rejections),
                "model_call_json": (
                    canonical_json(cached["model_call"]) if isinstance(cached.get("model_call"), dict) else None
                ),
            }
        )

    aggregate_by_config = {
        config_id: aggregate_segment_assignments(
            raw_by_config[config_id],
            context=context,
            actor_id=model.model_id,
        )
        for config_id in sorted(config_by_id)
    }
    concepts_by_id = {
        str(concept["concept_id"]): concept
        for concept in [
            *concepts,
            *generated_concepts.values(),
        ]
        if concept.get("concept_id")
    }
    validation_rows: list[dict[str, Any]] = []
    validated_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for config_id in sorted(aggregate_by_config):
        config = config_by_id[config_id]
        for assignment in aggregate_by_config[config_id]:
            assignment_id = str(assignment["assignment_id"])
            concept = concepts_by_id.get(str(assignment.get("concept_id") or ""))
            if concept is None:
                raise RuntimeError(f"{assignment_id}: assignment concept is missing")
            completed_validations: list[dict[str, Any]] = []
            for span_value in _evidence(assignment).get("spans") or []:
                if not isinstance(span_value, dict):
                    continue
                span = cast(dict[str, Any], span_value)
                segment_id = str(span.get("segment_id") or "")
                subject = subjects.get((config_id, segment_id))
                if subject is None:
                    raise RuntimeError(f"{assignment_id}: validation segment is missing")
                work_id = _validation_work_id(
                    config_id,
                    assignment_id,
                    span,
                )
                cached = validation_checkpoint.get(
                    subject.subject_type,
                    subject.subject_id,
                    artifact_digest=subject.version_digest,
                    segment_id=segment_id,
                    work_id=work_id,
                )
                if cached is None or cached.get("status") != "completed":
                    try:
                        proposal = model.validate(
                            subject=subject,
                            concept=concept,
                            assignment=_single_span_assignment(
                                assignment,
                                span,
                            ),
                        )
                        cached = {
                            "subject_type": subject.subject_type,
                            "subject_id": subject.subject_id,
                            "artifact_digest": subject.version_digest,
                            "segment_id": segment_id,
                            "work_id": work_id,
                            "config_id": config_id,
                            "assignment_id": assignment_id,
                            "status": "completed",
                            "actor_id": model.model_id,
                            "source_field": span.get("source_field"),
                            "start_char": span.get("start_char"),
                            "end_char": span.get("end_char"),
                            "agrees": proposal.agrees,
                            "confidence": proposal.confidence,
                            "rationale": proposal.rationale,
                            "model_call": model_call_metadata(model),
                        }
                        validation_checkpoint.append(cached)
                    except Exception as exc:
                        validation_checkpoint.append(
                            {
                                "subject_type": subject.subject_type,
                                "subject_id": subject.subject_id,
                                "artifact_digest": subject.version_digest,
                                "segment_id": segment_id,
                                "work_id": work_id,
                                "config_id": config_id,
                                "assignment_id": assignment_id,
                                "status": "retry_exhausted",
                                "actor_id": model.model_id,
                                "model_call": model_call_metadata(model),
                                **_safe_error(exc),
                            }
                        )
                        raise RuntimeError(
                            f"{config_id}/{assignment_id}: validation failed; the exact checkpoint is resumable"
                        ) from exc
                validation = {
                    "work_id": work_id,
                    "assignment_id": assignment_id,
                    "segment_id": segment_id,
                    "source_field": cached.get("source_field"),
                    "start_char": cached.get("start_char"),
                    "end_char": cached.get("end_char"),
                    "agrees": cached.get("agrees") is True,
                    "confidence": float(str(cached.get("confidence") or 0)),
                    "rationale": cached.get("rationale"),
                    "actor_id": model.model_id,
                    "model_call": cached.get("model_call"),
                }
                completed_validations.append(validation)
                validation_rows.append(
                    {
                        "config_id": config_id,
                        "arm": config["arm"],
                        "max_tokens": config["max_tokens"],
                        **validation,
                        "model_call_json": (
                            canonical_json(cached["model_call"])
                            if isinstance(
                                cached.get("model_call"),
                                dict,
                            )
                            else None
                        ),
                    }
                )
            if completed_validations:
                validated_by_config[config_id].append(
                    supersede_assignment_with_validation(
                        assignment,
                        validations=completed_validations,
                        context=context,
                        actor_id=model.model_id,
                    )
                )

    metric_rows = _metric_rows(
        configs=selected_configs,
        gold_rows=gold_rows,
        gold_concept_ids=gold_concept_ids,
        selected_rows=selected_rows,
        segment_results=segment_results,
        raw_by_config=raw_by_config,
        aggregate_by_config=aggregate_by_config,
        validations=validation_rows,
        subjects=subjects,
    )
    assignment_rows = []
    raw_assignment_rows = []
    for config_id in sorted(config_by_id):
        config = config_by_id[config_id]
        for row in raw_by_config[config_id]:
            raw_assignment_rows.append(
                {
                    "config_id": config_id,
                    "arm": config["arm"],
                    "max_tokens": config["max_tokens"],
                    **row,
                }
            )
        for stage, rows in (
            ("aggregated", aggregate_by_config[config_id]),
            ("validated", validated_by_config[config_id]),
        ):
            for row in rows:
                assignment_rows.append(
                    {
                        "config_id": config_id,
                        "arm": config["arm"],
                        "max_tokens": config["max_tokens"],
                        "assignment_stage": stage,
                        **row,
                    }
                )
    transition_rows = _transition_rows(
        generation_checkpoint,
        validation_checkpoint,
    )
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        write_parquet_rows(
            temporary / "tagging_segments.parquet",
            columns=SEGMENT_RESULT_COLUMNS,
            rows=segment_results,
        )
        write_parquet_rows(
            temporary / "tagging_raw_assignments.parquet",
            columns=RAW_ASSIGNMENT_COLUMNS,
            rows=raw_assignment_rows,
        )
        write_parquet_rows(
            temporary / "tagging_assignments.parquet",
            columns=ASSIGNMENT_RESULT_COLUMNS,
            rows=assignment_rows,
        )
        write_parquet_rows(
            temporary / "tagging_validations.parquet",
            columns=VALIDATION_RESULT_COLUMNS,
            rows=validation_rows,
        )
        write_parquet_rows(
            temporary / "tagging_metrics.parquet",
            columns=METRIC_COLUMNS,
            rows=metric_rows,
        )
        write_parquet_rows(
            temporary / "tagging_provider_transitions.parquet",
            columns=TRANSITION_COLUMNS,
            rows=transition_rows,
        )
        write_parquet_rows(
            temporary / "tagging_concepts.parquet",
            columns=CONCEPT_COLUMNS,
            rows=sorted(
                concepts_by_id.values(),
                key=lambda row: str(row["concept_id"]),
            ),
        )
        registry_artifact = temporary / "tagging_input_registry.parquet"
        shutil.copyfile(registry_path, registry_artifact)
        artifacts = _artifact_hashes(temporary)
        tagging_id = (
            "segmentation_tagging_"
            + hashlib.sha256(
                canonical_json({name: value["sha256"] for name, value in sorted(artifacts.items())}).encode()
            ).hexdigest()[:24]
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "experiment_version": EXPERIMENT_VERSION,
            "tagging_id": tagging_id,
            "run_id": context.run_id,
            "asserted_at": context.asserted_at,
            "dataset_evaluation_id": (base_manifest.get("dataset_evaluation_id")),
            "document_scope_id": (scope.scope_id if scope is not None else None),
            "document_scope_policy_version": (scope.scope_policy_version if scope is not None else None),
            "document_scope_manifest_sha256": (
                _sha256(scope_dir / "document-acceptance-manifest.json") if scope_dir is not None else None
            ),
            "base_experiment_id": base_receipt["experiment_id"],
            "base_experiment_manifest_sha256": _sha256(experiment_dir / "segmentation-experiment-manifest.json"),
            "registry_artifact": registry_artifact.name,
            "registry_sha256": _sha256(registry_artifact),
            "model_id": model.model_id,
            "model_configuration": model_configuration,
            "production_provider": model.production_provider,
            "budget": budget,
            "config_ids": selected_config_ids,
            "config_count": len(selected_config_ids),
            "tag_max_output_tokens": TAG_MAX_OUTPUT_TOKENS,
            "validation_max_output_tokens": (VALIDATION_MAX_OUTPUT_TOKENS),
            "evidence_alignment_policy": EVIDENCE_ALIGNMENT_POLICY,
            "negative_segments_per_artifact": (negative_segments_per_artifact),
            "sampling_policy": (
                "all gold-intersecting segments plus evenly spaced "
                "within-artifact non-intersecting controls; every segment "
                "of each model-processable adversarial artifact; at least one "
                "deterministically selected artifact from every accepted "
                "document profile; restricted to the declared document "
                "acceptance scope when present"
            ),
            "metric_scope": (
                "finite multilabel registry consisting of every curated "
                "gold concept; novel assignments are reported separately"
            ),
            "configs": selected_configs,
            "gold_span_count": len(gold_rows),
            "gold_artifact_count": len({str(row["artifact_digest"]) for row in gold_rows}),
            "adversarial_case_count": len(adversarial_rows),
            "selected_adversarial_case_ids": sorted(
                {case_id for row in selected_rows for case_id in _string_list(row.get("adversarial_case_ids_json"))}
            ),
            "selected_profile_ids": sorted({str(row["profile_id"]) for row in selected_rows}),
            "selected_profile_count": len({str(row["profile_id"]) for row in selected_rows}),
            "selected_segment_count": len(selected_rows),
            "artifacts": artifacts,
        }
        (temporary / "segmentation-tagging-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = _validate_tagging_experiment(
            dataset_dir,
            experiment_dir,
            temporary,
            scope_dir=scope_dir,
            scope=scope,
        )
        (temporary / "segmentation-tagging-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError("Tagging experiment validation failed: " + "; ".join(receipt["failures"]))
        temporary.replace(output_dir)
        shutil.rmtree(work_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _stored_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value).casefold()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def _stringified_row(
    row: dict[str, Any],
    columns: Sequence[str],
) -> dict[str, str | None]:
    return {column: (None if row.get(column) is None else str(row.get(column))) for column in columns}


def _validate_tagging_experiment(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
    scope: DocumentAcceptanceScope | None = None,
) -> dict[str, Any]:
    """Fail closed on sample, grounding, metrics, provider, and hashes."""
    manifest = _load_json_object(output_dir / "segmentation-tagging-manifest.json")
    base_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    failures: list[str] = []
    if base_receipt.get("status") != "pass":
        failures.append("base segmentation experiment did not validate")
    if manifest.get("format_version") != FORMAT_VERSION:
        failures.append("manifest format version differs")
    if manifest.get("experiment_version") != EXPERIMENT_VERSION:
        failures.append("tagging experiment version differs")
    model_configuration_value = manifest.get("model_configuration")
    if not isinstance(model_configuration_value, dict):
        failures.append("model run configuration is invalid")
        model_configuration: dict[str, Any] = {}
    else:
        model_configuration = cast(
            dict[str, Any],
            model_configuration_value,
        )
    if model_configuration.get("model_id") != manifest.get("model_id"):
        failures.append("model run configuration ID differs")
    if manifest.get("tag_max_output_tokens") != TAG_MAX_OUTPUT_TOKENS:
        failures.append("tag output-token cap differs")
    if manifest.get("validation_max_output_tokens") != VALIDATION_MAX_OUTPUT_TOKENS:
        failures.append("validation output-token cap differs")
    if manifest.get("evidence_alignment_policy") != EVIDENCE_ALIGNMENT_POLICY:
        failures.append("evidence alignment policy differs")
    if manifest.get("base_experiment_id") != base_receipt.get("experiment_id"):
        failures.append("base experiment ID differs")
    if manifest.get("base_experiment_manifest_sha256") != _sha256(
        experiment_dir / "segmentation-experiment-manifest.json"
    ):
        failures.append("base experiment manifest digest differs")
    if scope_dir is not None:
        if scope is None:
            try:
                scope = load_document_acceptance_scope(
                    dataset_dir,
                    scope_dir,
                )
            except (OSError, RuntimeError, ValueError) as exc:
                failures.append(f"document acceptance scope is invalid: {exc}")
        if scope is not None:
            if manifest.get("document_scope_id") != scope.scope_id:
                failures.append("document scope ID differs")
            if manifest.get("document_scope_policy_version") != scope.scope_policy_version:
                failures.append("document scope policy version differs")
            scope_manifest = scope_dir / "document-acceptance-manifest.json"
            if not scope_manifest.is_file() or manifest.get("document_scope_manifest_sha256") != _sha256(
                scope_manifest
            ):
                failures.append("document scope manifest digest differs")
    elif manifest.get("document_scope_id") is not None:
        failures.append("document scope directory is required")
    elif any(
        manifest.get(field) is not None
        for field in (
            "document_scope_policy_version",
            "document_scope_manifest_sha256",
        )
    ):
        failures.append("unscoped manifest contains document scope metadata")
    artifacts = _artifact_hashes(output_dir)
    if artifacts != manifest.get("artifacts"):
        failures.append("tagging artifact hashes or rows differ")
    tagging_id = (
        "segmentation_tagging_"
        + hashlib.sha256(
            canonical_json({name: value["sha256"] for name, value in sorted(artifacts.items())}).encode()
        ).hexdigest()[:24]
    )
    if manifest.get("tagging_id") != tagging_id:
        failures.append("tagging ID differs")

    budget = int(str(manifest.get("budget") or 0))
    negatives = int(str(manifest.get("negative_segments_per_artifact") or 0))
    configs = [cast(dict[str, Any], config) for config in manifest.get("configs", []) if isinstance(config, dict)]
    declared_config_ids = manifest.get("config_ids")
    if not isinstance(declared_config_ids, list):
        failures.append("tagging config IDs are invalid")
        requested_config_ids: Sequence[str] | None = None
    else:
        requested_config_ids = [str(config_id) for config_id in declared_config_ids]
    try:
        expected_configs = _selected_tagging_configs(
            experiment_dir,
            budget=budget,
            config_ids=requested_config_ids,
        )
    except (RuntimeError, ValueError) as exc:
        failures.append(str(exc))
        expected_configs = _selected_tagging_configs(
            experiment_dir,
            budget=budget,
            config_ids=None,
        )
    expected_config_ids_list = [str(config["config_id"]) for config in expected_configs]
    if configs != expected_configs:
        failures.append("tagging configs differ from base experiment")
    if manifest.get("config_ids") != expected_config_ids_list:
        failures.append("tagging config IDs differ")
    if manifest.get("config_count") != len(expected_config_ids_list):
        failures.append("tagging config count differs")
    selected_rows = _select_tagging_segments(
        dataset_dir,
        experiment_dir,
        budget=budget,
        negative_segments_per_artifact=negatives,
        scope=scope,
        config_ids=expected_config_ids_list,
    )
    subjects = _subjects_from_selected(
        dataset_dir,
        experiment_dir,
        selected_rows,
    )
    segment_rows = read_parquet_rows(output_dir / "tagging_segments.parquet")
    raw_assignment_rows = read_parquet_rows(output_dir / "tagging_raw_assignments.parquet")
    assignment_rows = read_parquet_rows(output_dir / "tagging_assignments.parquet")
    validation_rows = read_parquet_rows(output_dir / "tagging_validations.parquet")
    metric_rows = read_parquet_rows(output_dir / "tagging_metrics.parquet")
    transition_rows = read_parquet_rows(output_dir / "tagging_provider_transitions.parquet")
    concepts = read_parquet_rows(output_dir / "tagging_concepts.parquet")
    registry_artifact = output_dir / "tagging_input_registry.parquet"
    if manifest.get("registry_artifact") != registry_artifact.name:
        failures.append("input registry artifact name differs")
    if not registry_artifact.is_file() or manifest.get("registry_sha256") != _sha256(registry_artifact):
        failures.append("input registry digest differs")
    expected_segment_keys = {(str(row["config_id"]), str(row["segment_id"])) for row in selected_rows}
    stored_segment_keys = {(str(row["config_id"]), str(row["segment_id"])) for row in segment_rows}
    if stored_segment_keys != expected_segment_keys:
        failures.append("tagging segment sample differs")
    if len(stored_segment_keys) != len(segment_rows):
        failures.append("tagging segment rows are duplicated")
    static_segment_columns = (
        "config_id",
        "arm",
        "max_tokens",
        "profile_id",
        "subject_type",
        "subject_id",
        "artifact_digest",
        "segment_id",
        "ordinal",
        "segment_count",
        "selection_role",
        "gold_ids_json",
        "adversarial_case_ids_json",
    )
    expected_static_segments = sorted(
        (_stringified_row(row, static_segment_columns) for row in selected_rows),
        key=lambda row: (
            str(row["config_id"]),
            str(row["segment_id"]),
        ),
    )
    stored_static_segments = sorted(
        (_stringified_row(row, static_segment_columns) for row in segment_rows),
        key=lambda row: (
            str(row["config_id"]),
            str(row["segment_id"]),
        ),
    )
    if stored_static_segments != expected_static_segments:
        failures.append("tagging segment metadata differs")
    if any(str(row.get("status")) not in FINAL_GENERATION_STATUSES for row in segment_rows):
        failures.append("tagging segments include a non-final status")

    initial_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    validated_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    invalid_assignment_stages: set[str] = set()
    for row in assignment_rows:
        stage = str(row.get("assignment_stage"))
        if stage == "aggregated":
            initial_by_config[str(row["config_id"])].append(row)
        elif stage == "validated":
            validated_by_config[str(row["config_id"])].append(row)
        else:
            invalid_assignment_stages.add(stage)
    if invalid_assignment_stages:
        failures.append("tagging assignments include an invalid stage")
    ungrounded = [
        str(row.get("assignment_id") or "")
        for config_id, rows in initial_by_config.items()
        for row in rows
        if not _assignment_is_grounded(row, subjects, config_id)
    ]
    if ungrounded:
        failures.append(f"{len(ungrounded)} aggregated assignments are ungrounded")
    expected_validation = {
        (
            config_id,
            str(row["assignment_id"]),
            str(span.get("segment_id") or ""),
            str(span.get("source_field") or ""),
            int(str(span.get("start_char"))),
            int(str(span.get("end_char"))),
        )
        for config_id, rows in initial_by_config.items()
        for row in rows
        for span in _evidence(row).get("spans") or []
        if isinstance(span, dict)
    }
    stored_validation = set()
    for row in validation_rows:
        try:
            stored_validation.add(
                (
                    str(row["config_id"]),
                    str(row["assignment_id"]),
                    str(row["segment_id"]),
                    str(row["source_field"]),
                    int(str(row["start_char"])),
                    int(str(row["end_char"])),
                )
            )
        except (TypeError, ValueError):
            failures.append("validation coordinates are invalid")
    if stored_validation != expected_validation:
        failures.append("validation rows do not exactly cover evidence")
    if any(_stored_bool(row.get("agrees")) is None for row in validation_rows):
        failures.append("validation agreement values are invalid")

    config_by_id = {str(config.get("config_id") or ""): config for config in configs if config.get("config_id")}
    expected_config_ids = set(config_by_id)
    for table_name, rows in (
        ("segments", segment_rows),
        ("raw assignments", raw_assignment_rows),
        ("assignments", assignment_rows),
        ("validations", validation_rows),
    ):
        if any(
            str(row.get("config_id") or "") not in expected_config_ids
            or str(row.get("arm") or "")
            != str(
                config_by_id.get(
                    str(row.get("config_id") or ""),
                    {},
                ).get("arm")
                or ""
            )
            or int(str(row.get("max_tokens") or 0)) != budget
            for row in rows
        ):
            failures.append(f"tagging {table_name} config metadata differs")
    selected_adversarial_case_ids = sorted(
        {case_id for row in selected_rows for case_id in _string_list(row.get("adversarial_case_ids_json"))}
    )
    selected_profile_ids = sorted({str(row["profile_id"]) for row in selected_rows})
    if manifest.get("selected_profile_ids") != selected_profile_ids:
        failures.append("selected tagging profiles differ")
    if manifest.get("selected_profile_count") != len(selected_profile_ids):
        failures.append("selected tagging profile count differs")
    gold_rows, adversarial_rows = _scoped_evaluation_rows(
        dataset_dir,
        scope,
    )
    if manifest.get("adversarial_case_count") != len(adversarial_rows):
        failures.append("adversarial case count differs")
    if manifest.get("selected_adversarial_case_ids") != selected_adversarial_case_ids:
        failures.append("selected adversarial cases differ")
    if manifest.get("gold_span_count") != len(gold_rows):
        failures.append("gold span count differs")
    if manifest.get("gold_artifact_count") != len({str(row["artifact_digest"]) for row in gold_rows}):
        failures.append("gold artifact count differs")
    registry_context = RunContext(
        f"{manifest.get('run_id')}-gold-registry",
        str(manifest.get("asserted_at")),
    )
    expected_registry, gold_concept_ids = _gold_registry(
        read_parquet_rows(registry_artifact),
        gold_rows,
        context=registry_context,
    )
    concepts_by_id = {str(row.get("concept_id") or ""): row for row in concepts if row.get("concept_id")}
    if len(concepts_by_id) != len(concepts):
        failures.append("tagging concepts contain missing or duplicate IDs")
    for expected in expected_registry:
        stored = concepts_by_id.get(str(expected["concept_id"]))
        if stored is None or _stringified_row(stored, CONCEPT_COLUMNS) != _stringified_row(expected, CONCEPT_COLUMNS):
            failures.append("seeded input registry differs")
            break
    referenced_concepts = {str(row.get("concept_id") or "") for row in [*raw_assignment_rows, *assignment_rows]}
    if not referenced_concepts <= set(concepts_by_id):
        failures.append("an assignment references an absent concept")
    selected_lookup = {
        (
            str(row["config_id"]),
            str(row["segment_id"]),
        ): row
        for row in selected_rows
    }
    normalized_segment_rows = [
        {
            **row,
            "status": str(row["status"]),
        }
        for row in segment_rows
    ]
    raw_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw_assignment_rows:
        raw_by_config[str(row["config_id"])].append(row)
    raw_keys = {(str(row["config_id"]), str(row["assignment_id"])) for row in raw_assignment_rows}
    if len(raw_keys) != len(raw_assignment_rows):
        failures.append("raw tagging assignments are duplicated")
    if len(raw_assignment_rows) != sum(int(str(row.get("proposal_count") or 0)) for row in normalized_segment_rows):
        failures.append("raw assignment count differs from segment proposals")
    ungrounded_raw = [
        str(row.get("assignment_id") or "")
        for config_id, rows in raw_by_config.items()
        for row in rows
        if not _assignment_is_grounded(row, subjects, config_id)
    ]
    if ungrounded_raw:
        failures.append(f"{len(ungrounded_raw)} raw assignments are ungrounded")
    context = RunContext(
        str(manifest.get("run_id")),
        str(manifest.get("asserted_at")),
    )
    aggregate_by_config = {
        str(config["config_id"]): aggregate_segment_assignments(
            raw_by_config[str(config["config_id"])],
            context=context,
            actor_id=str(manifest.get("model_id")),
        )
        for config in configs
    }
    stored_aggregate_rows = sorted(
        (
            {
                "config_id": config_id,
                **_stringified_row(row, ASSIGNMENT_COLUMNS),
            }
            for config_id, rows in initial_by_config.items()
            for row in rows
        ),
        key=lambda row: (
            str(row["config_id"]),
            str(row["assignment_id"]),
        ),
    )
    expected_aggregate_rows = sorted(
        (
            {
                "config_id": config_id,
                **_stringified_row(row, ASSIGNMENT_COLUMNS),
            }
            for config_id, rows in aggregate_by_config.items()
            for row in rows
        ),
        key=lambda row: (
            str(row["config_id"]),
            str(row["assignment_id"]),
        ),
    )
    if stored_aggregate_rows != expected_aggregate_rows:
        failures.append("artifact aggregation differs from raw proposals")

    validation_by_assignment: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in validation_rows:
        serialized_call = row.get("model_call_json")
        try:
            model_call = json.loads(str(serialized_call)) if serialized_call else None
        except json.JSONDecodeError:
            model_call = None
        validation_by_assignment[(str(row["config_id"]), str(row["assignment_id"]))].append(
            {
                "work_id": row.get("work_id"),
                "assignment_id": row.get("assignment_id"),
                "segment_id": row.get("segment_id"),
                "source_field": row.get("source_field"),
                "start_char": int(str(row.get("start_char"))),
                "end_char": int(str(row.get("end_char"))),
                "agrees": _stored_bool(row.get("agrees")) is True,
                "confidence": float(str(row.get("confidence") or 0)),
                "rationale": row.get("rationale"),
                "actor_id": row.get("actor_id"),
                "model_call": model_call,
            }
        )
    expected_validated_by_config: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for config_id, rows in aggregate_by_config.items():
        for row in rows:
            values = validation_by_assignment[(config_id, str(row["assignment_id"]))]
            if values:
                expected_validated_by_config[config_id].append(
                    supersede_assignment_with_validation(
                        row,
                        validations=values,
                        context=context,
                        actor_id=str(manifest.get("model_id")),
                    )
                )
    stored_validated_rows = sorted(
        (
            {
                "config_id": config_id,
                **_stringified_row(row, ASSIGNMENT_COLUMNS),
            }
            for config_id, rows in validated_by_config.items()
            for row in rows
        ),
        key=lambda row: (
            str(row["config_id"]),
            str(row["assignment_id"]),
        ),
    )
    expected_validated_rows = sorted(
        (
            {
                "config_id": config_id,
                **_stringified_row(row, ASSIGNMENT_COLUMNS),
            }
            for config_id, rows in expected_validated_by_config.items()
            for row in rows
        ),
        key=lambda row: (
            str(row["config_id"]),
            str(row["assignment_id"]),
        ),
    )
    if stored_validated_rows != expected_validated_rows:
        failures.append("validated assignments differ from provider judgments")
    expected_metrics = _metric_rows(
        configs=configs,
        gold_rows=gold_rows,
        gold_concept_ids=gold_concept_ids,
        selected_rows=list(selected_lookup.values()),
        segment_results=normalized_segment_rows,
        raw_by_config=raw_by_config,
        aggregate_by_config=aggregate_by_config,
        validations=[
            {
                **row,
                "agrees": _stored_bool(row.get("agrees")) is True,
            }
            for row in validation_rows
        ],
        subjects=subjects,
    )
    metric_columns = METRIC_COLUMNS
    stored_metrics = sorted(
        (_stringified_row(row, metric_columns) for row in metric_rows),
        key=lambda row: (
            str(row["config_id"]),
            str(row["scope"]),
            str(row["profile_id"]),
        ),
    )
    expected_metrics_normalized = sorted(
        (_stringified_row(row, metric_columns) for row in expected_metrics),
        key=lambda row: (
            str(row["config_id"]),
            str(row["scope"]),
            str(row["profile_id"]),
        ),
    )
    if stored_metrics != expected_metrics_normalized:
        failures.append("tagging metrics differ from recomputation")

    production_provider = manifest.get("production_provider") is True
    completed_model_calls = 0
    invalid_model_calls = 0
    failed_transitions = 0
    invalid_transitions = 0
    successful_transitions = 0
    segment_results_by_key = {
        (str(row["config_id"]), str(row["segment_id"])): row
        for row in segment_rows
    }
    for row in transition_rows:
        phase = str(row.get("phase") or "")
        status = str(row.get("status") or "")
        allowed_statuses = (
            {*FINAL_GENERATION_STATUSES, "retry_exhausted"}
            if phase == "generation"
            else ({"completed", "retry_exhausted"} if phase == "validation" else set())
        )
        if status not in allowed_statuses:
            invalid_transitions += 1
            continue
        if status == "retry_exhausted":
            failed_transitions += 1
            continue
        successful_transitions += 1
        try:
            metadata = json.loads(str(row.get("model_call_json") or "{}"))
        except json.JSONDecodeError:
            metadata = {}
        if isinstance(metadata, dict) and metadata:
            completed_model_calls += 1
            expected_output_cap = TAG_MAX_OUTPUT_TOKENS if phase == "generation" else VALIDATION_MAX_OUTPUT_TOKENS
            metadata_valid = metadata.get("max_output_tokens") == expected_output_cap
            for metadata_field, configuration_field in (
                ("reasoning_effort", "reasoning_effort"),
                ("requested_service_tier", "service_tier"),
                ("timeout_seconds", "timeout_seconds"),
                ("max_retries", "max_retries"),
                ("sdk_max_retries", "sdk_max_retries"),
                ("store", "store"),
            ):
                if metadata.get(metadata_field) != model_configuration.get(configuration_field):
                    metadata_valid = False
            if phase == "generation":
                segment_result = segment_results_by_key.get(
                    (
                        str(row.get("config_id") or ""),
                        str(row.get("segment_id") or ""),
                    )
                )
                count_fields = (
                    "tag_output_item_count",
                    "tag_accepted_item_count",
                    "tag_rejection_count",
                    "evidence_offset_repair_count",
                )
                counts = {
                    field: metadata.get(field)
                    for field in count_fields
                }
                if (
                    segment_result is None
                    or any(
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or value < 0
                        for value in counts.values()
                    )
                ):
                    metadata_valid = False
                else:
                    output_count = cast(
                        int,
                        counts["tag_output_item_count"],
                    )
                    accepted_count = cast(
                        int,
                        counts["tag_accepted_item_count"],
                    )
                    rejection_count = cast(
                        int,
                        counts["tag_rejection_count"],
                    )
                    repair_count = cast(
                        int,
                        counts["evidence_offset_repair_count"],
                    )
                    if (
                        output_count
                        != accepted_count + rejection_count
                        or repair_count > accepted_count
                        or accepted_count
                        != int(str(segment_result["proposal_count"]))
                        or rejection_count
                        != int(str(segment_result["rejection_count"]))
                    ):
                        metadata_valid = False
            if production_provider and not _valid_completed_model_call(cast(dict[str, Any], metadata)):
                metadata_valid = False
            if not metadata_valid:
                invalid_model_calls += 1
    if invalid_transitions:
        failures.append(f"{invalid_transitions} provider transitions are invalid")
    if successful_transitions != (len(segment_rows) + len(validation_rows)):
        failures.append("provider transitions do not cover all model work")
    if production_provider and completed_model_calls != (len(segment_rows) + len(validation_rows)):
        failures.append("provider calls do not cover all model work")
    if invalid_model_calls:
        failures.append(f"{invalid_model_calls} OpenAI model-call receipts are invalid")
    secret_matches = _secret_matches(output_dir)
    if secret_matches:
        failures.append("secret-like content appears in tagging artifacts")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "tagging_id": tagging_id,
        "base_experiment_id": base_receipt.get("experiment_id"),
        "document_scope_id": (scope.scope_id if scope is not None else None),
        "model_id": manifest.get("model_id"),
        "model_configuration": model_configuration,
        "production_provider": production_provider,
        "evidence_alignment_policy": manifest.get(
            "evidence_alignment_policy",
        ),
        "config_ids": expected_config_ids_list,
        "config_count": len(configs),
        "selected_profile_ids": selected_profile_ids,
        "selected_profile_count": len(selected_profile_ids),
        "artifact_count": len({str(row["artifact_digest"]) for row in selected_rows}),
        "selected_segment_count": len(segment_rows),
        "aggregated_assignment_count": sum(len(rows) for rows in initial_by_config.values()),
        "validation_count": len(validation_rows),
        "provider_call_count": completed_model_calls,
        "provider_transition_count": len(transition_rows),
        "provider_call_failure_count": failed_transitions,
        "provider_invalid_call_count": invalid_model_calls,
        "metric_row_count": len(metric_rows),
        "secret_match_count": len(secret_matches),
        "artifacts": artifacts,
        "failures": failures,
    }


def validate_tagging_experiment(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Validate sample, scope, grounding, metrics, calls, and artifact hashes."""
    return _validate_tagging_experiment(
        dataset_dir,
        experiment_dir,
        output_dir,
        scope_dir=scope_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("preflight")
    preflight.add_argument("dataset_dir", type=Path)
    preflight.add_argument("experiment_dir", type=Path)
    preflight.add_argument("--registry-path", type=Path)
    preflight.add_argument("--budget", type=int, default=DEFAULT_BUDGET)
    preflight.add_argument(
        "--negative-segments",
        type=int,
        default=DEFAULT_NEGATIVE_SEGMENTS,
    )
    preflight.add_argument("--scope-dir", type=Path)
    preflight.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Upstream config to tag; repeat for a subset (default: five arms)",
    )
    build = commands.add_parser("build")
    build.add_argument("dataset_dir", type=Path)
    build.add_argument("experiment_dir", type=Path)
    build.add_argument("registry_path", type=Path)
    build.add_argument("output_dir", type=Path)
    build.add_argument("--budget", type=int, default=DEFAULT_BUDGET)
    build.add_argument(
        "--negative-segments",
        type=int,
        default=DEFAULT_NEGATIVE_SEGMENTS,
    )
    build.add_argument("--scope-dir", type=Path)
    build.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Upstream config to tag; repeat for a subset (default: five arms)",
    )
    build.add_argument("--run-id")
    build.add_argument("--asserted-at")
    validate = commands.add_parser("validate")
    validate.add_argument("dataset_dir", type=Path)
    validate.add_argument("experiment_dir", type=Path)
    validate.add_argument("output_dir", type=Path)
    validate.add_argument("--scope-dir", type=Path)
    args = parser.parse_args()
    if args.command == "preflight":
        result = tagging_preflight(
            args.dataset_dir,
            args.experiment_dir,
            registry_path=args.registry_path,
            budget=args.budget,
            negative_segments_per_artifact=args.negative_segments,
            scope_dir=args.scope_dir,
            config_ids=args.config_ids,
        )
    elif args.command == "build":
        load_dotenv()
        model = OpenAIOntologyModel.from_environment()
        if model is None:
            raise RuntimeError("OPENAI_API_KEY is required for the tagging experiment")
        result = build_tagging_experiment(
            args.dataset_dir,
            args.experiment_dir,
            args.registry_path,
            args.output_dir,
            model=model,
            budget=args.budget,
            negative_segments_per_artifact=args.negative_segments,
            scope_dir=args.scope_dir,
            config_ids=args.config_ids,
            run_id=args.run_id,
            asserted_at=args.asserted_at,
        )
    else:
        result = validate_tagging_experiment(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            scope_dir=args.scope_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

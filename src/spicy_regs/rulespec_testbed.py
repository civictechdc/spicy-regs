"""Run the small real-data Rulespec tagging diagnostic through v3 code.

The reader in this module is deliberately evaluation-only. It translates three
stored benchmark tables into current ``SourceArtifact``, ``ProcessingSegment``,
and ``ExtractionUnit`` objects. The production tag task never learns the old
experiment's run identity, manifest, or storage layout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.adapters import StructuredTextModel
from spicy_regs.docpipeline.adapters.openai import (
    OpenAIStructuredTextModel,
    PROMPT_INPUT_TOKEN_BUDGET,
    PROMPT_SAFETY_MARGIN_TOKENS,
    TiktokenCounter,
)
from spicy_regs.docpipeline.extraction import (
    ExtractionOutcome,
    ExtractionUnit,
    extraction_plan_facts,
    plan_extraction_items,
    run_extraction,
)
from spicy_regs.docpipeline.runtime import RunPlan, sha256_file
from spicy_regs.docpipeline.segments import (
    ProcessingSegment,
    SegmentSettings,
    segment_artifact,
)
from spicy_regs.docpipeline.source import (
    SOURCE_PROFILES,
    SourceArtifact,
    build_source_artifact,
    iter_source_records,
)
from spicy_regs.docpipeline.tag_task import TagExtractionTask, tag_unit
from spicy_regs.ontology.common import canonical_json, read_parquet_rows
from spicy_regs.ontology.concepts import (
    concept_aliases,
    normalize_label,
    select_candidate_concepts_for_text,
)
from spicy_regs.ontology.llm import resolve_exact_evidence_offsets

GOLD_FILE = "gold_spans.parquet"
EXPECTED_GOLD_ARTIFACTS = 35
EXPECTED_SELECTED_SEGMENTS = 109
PROMPT_CONCEPT_LIMIT = 12


class DiagnosticInputError(RuntimeError):
    """The stored sample cannot map safely onto current source coordinates."""


@dataclass(frozen=True)
class TestbedInputs:
    """Provider-ready units plus the separately held diagnostic answers."""

    units: tuple[ExtractionUnit, ...]
    answers: dict[str, Any]
    source_facts: dict[str, Any]
    segmentation_facts: dict[str, Any]
    vocabulary_facts: dict[str, Any]
    profile_facts: dict[str, Any]

    @property
    def selected_segment_count(self) -> int:
        return len(self.units)

    @property
    def gold_artifact_count(self) -> int:
        return len(self.answers["artifacts"])


def _json_string_array(value: object, label: str) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except json.JSONDecodeError as exc:
        raise DiagnosticInputError(f"{label} is not valid JSON") from exc
    if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
        raise DiagnosticInputError(f"{label} must be a JSON string array")
    return [str(item) for item in parsed]


def _selection_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, int]:
    try:
        return (
            str(row["profile_id"]),
            str(row["subject_type"]),
            str(row["subject_id"]),
            str(row["artifact_digest"]),
            int(str(row["ordinal"])),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DiagnosticInputError("a selected-segment row lacks a usable identity") from exc


def _artifact_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    key = _selection_key({**row, "ordinal": 0})
    return key[:4]


def _active_source_tables(profile_ids: set[str]) -> set[str]:
    by_profile = {profile.profile_id: profile.source_table for profile in SOURCE_PROFILES}
    missing = sorted(profile_ids - set(by_profile))
    if missing:
        raise DiagnosticInputError(f"selected profiles have no current source mapping: {missing}")
    return {by_profile[profile_id] for profile_id in profile_ids}


def _load_artifacts(
    dataset_dir: Path,
    selected_artifact_keys: set[tuple[str, str, str, str]],
) -> dict[tuple[str, str, str, str], SourceArtifact]:
    profile_ids = {key[0] for key in selected_artifact_keys}
    active_tables = _active_source_tables(profile_ids)
    artifacts: dict[tuple[str, str, str, str], SourceArtifact] = {}
    for record in iter_source_records(dataset_dir, active_source_tables=active_tables):
        outcome = build_source_artifact(record)
        artifact = outcome.artifact
        if artifact is None:
            continue
        key = (
            artifact.profile_id,
            artifact.subject_type,
            artifact.subject_id,
            artifact.content_sha256,
        )
        if key not in selected_artifact_keys:
            continue
        if key in artifacts:
            raise DiagnosticInputError(f"source parsing produced selected artifact {key} twice")
        if outcome.state != "completed":
            raise DiagnosticInputError(f"selected artifact {key} ended source parsing as {outcome.state}")
        artifacts[key] = artifact
    missing = sorted(selected_artifact_keys - set(artifacts))
    if missing:
        raise DiagnosticInputError(
            f"{len(missing)} selected artifacts did not reach a completed source outcome: {missing}"
        )
    return artifacts


def _segment_selection(
    artifacts: Mapping[tuple[str, str, str, str], SourceArtifact],
    selected_rows: Sequence[Mapping[str, Any]],
    *,
    counter: TiktokenCounter,
) -> tuple[dict[tuple[str, str, str, str, int], ProcessingSegment], SegmentSettings]:
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    all_segments: dict[tuple[str, str, str, str, int], ProcessingSegment] = {}
    for artifact_key, artifact in artifacts.items():
        outcome = segment_artifact(artifact, settings=settings, counter=counter)
        if outcome.state not in {"completed", "completed_empty"}:
            raise DiagnosticInputError(f"selected artifact {artifact_key} ended segmentation as {outcome.state}")
        for segment in outcome.segments:
            all_segments[(*artifact_key, segment.ordinal)] = segment

    selected: dict[tuple[str, str, str, str, int], ProcessingSegment] = {}
    for row in selected_rows:
        key = _selection_key(row)
        if key in selected:
            raise DiagnosticInputError(f"selected segment identity {key} appears twice")
        segment = all_segments.get(key)
        if segment is None:
            raise DiagnosticInputError(f"selected ordinal no longer exists in current segmentation: {key}")
        try:
            recorded_count = int(str(row["segment_count"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise DiagnosticInputError(f"selected segment {key} has no usable segment count") from exc
        if segment.segment_count != recorded_count:
            raise DiagnosticInputError(
                f"selected segment {key} moved from {recorded_count} total segments to {segment.segment_count}"
            )
        selected[key] = segment
    return selected, settings


def _registry_aliases(
    concepts: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], str]:
    candidates: dict[tuple[str, str], set[str]] = defaultdict(set)
    for concept in concepts:
        concept_id = str(concept.get("concept_id") or "")
        scheme = str(concept.get("scheme") or "")
        if not concept_id or not scheme or concept.get("status") == "deprecated":
            continue
        for alias in concept_aliases(dict(concept)):
            candidates[(scheme, alias)].add(concept_id)
    return {
        key: next(iter(concept_ids))
        for key, concept_ids in candidates.items()
        if len(concept_ids) == 1
    }


def _containing_segments(
    segments: Sequence[ProcessingSegment],
    *,
    source_field: str,
    start: int,
    end: int,
) -> list[str]:
    return sorted(
        {
            segment.segment_id
            for segment in segments
            if any(
                source_slice.source_field == source_field
                and source_slice.start_char <= start
                and source_slice.end_char >= end
                for source_slice in segment.slices
            )
        }
    )


def _answers(
    gold_rows: Sequence[Mapping[str, Any]],
    artifacts: Mapping[tuple[str, str, str, str], SourceArtifact],
    selected: Mapping[tuple[str, str, str, str, int], ProcessingSegment],
    selected_rows: Sequence[Mapping[str, Any]],
    concepts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    aliases = _registry_aliases(concepts)
    selected_by_artifact: dict[tuple[str, str, str, str], list[ProcessingSegment]] = defaultdict(list)
    for key, segment in selected.items():
        selected_by_artifact[key[:4]].append(segment)

    expected_by_artifact: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in gold_rows:
        key = (
            str(raw.get("profile_id") or ""),
            str(raw.get("subject_type") or ""),
            str(raw.get("subject_id") or ""),
            str(raw.get("artifact_digest") or ""),
        )
        artifact = artifacts.get(key)
        if artifact is None:
            raise DiagnosticInputError(
                f"gold row {raw.get('gold_id')} names an artifact outside the selected sample"
            )
        source_field = str(raw.get("source_field") or "")
        field_text = artifact.raw_fields.get(source_field)
        if field_text is None:
            raise DiagnosticInputError(
                f"gold row {raw.get('gold_id')} names missing source field {source_field!r}"
            )
        try:
            start = int(str(raw["start_char"]))
            end = int(str(raw["end_char"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise DiagnosticInputError(f"gold row {raw.get('gold_id')} has invalid offsets") from exc
        exact_text = str(raw.get("exact_text") or "")
        recorded_text_digest = str(raw.get("exact_text_sha256") or "")
        actual_text_digest = hashlib.sha256(exact_text.encode()).hexdigest()
        if recorded_text_digest != actual_text_digest:
            raise DiagnosticInputError(
                f"gold row {raw.get('gold_id')} has an invalid exact-text digest"
            )
        resolution = resolve_exact_evidence_offsets(field_text, exact_text, start, end)
        if resolution is None:
            raise DiagnosticInputError(
                f"gold row {raw.get('gold_id')} is absent or ambiguous in {source_field!r}"
            )
        containing = _containing_segments(
            selected_by_artifact.get(key, []),
            source_field=source_field,
            start=resolution.start,
            end=resolution.end,
        )
        if not containing:
            raise DiagnosticInputError(
                f"gold row {raw.get('gold_id')} is not fully contained in a selected evidence segment"
            )
        scheme = str(raw.get("concept_scheme") or "")
        label = str(raw.get("concept_label") or "")
        expected_by_artifact[key].append(
            {
                "gold_id": str(raw.get("gold_id") or ""),
                "scheme": scheme,
                "label": label,
                "concept_id": aliases.get((scheme, normalize_label(label))),
                "source_field": source_field,
                "start_char": resolution.start,
                "end_char": resolution.end,
                "exact_text": exact_text,
                "coordinate_resolution": resolution.method,
                "containing_segment_ids": containing,
            }
        )

    artifacts_answer = [
        {
            "profile_id": key[0],
            "subject_type": key[1],
            "subject_id": key[2],
            "artifact_digest": key[3],
            "expected_tags": sorted(
                values,
                key=lambda item: (
                    str(item["scheme"]),
                    normalize_label(item["label"]),
                    str(item["gold_id"]),
                ),
            ),
        }
        for key, values in sorted(expected_by_artifact.items())
    ]
    segment_answers: list[dict[str, Any]] = []
    for row in selected_rows:
        key = _selection_key(row)
        segment = selected[key]
        segment_answers.append(
            {
                "profile_id": key[0],
                "subject_type": key[1],
                "subject_id": key[2],
                "artifact_digest": key[3],
                "segment_id": segment.segment_id,
                "segment_ordinal": segment.ordinal,
                "adversarial_case_ids": _json_string_array(
                    row.get("adversarial_case_ids_json"),
                    f"{key} adversarial_case_ids_json",
                ),
            }
        )
    return {
        "artifacts": artifacts_answer,
        "segments": sorted(
            segment_answers,
            key=lambda item: (
                str(item["profile_id"]),
                str(item["subject_type"]),
                str(item["subject_id"]),
                int(item["segment_ordinal"]),
            ),
        ),
    }


def _source_file_facts(dataset_dir: Path, active_tables: set[str]) -> dict[str, str]:
    names = set(active_tables)
    if "documents" in names:
        names.add("federal_register")
    return {
        f"{name}.parquet": sha256_file(dataset_dir / f"{name}.parquet")
        for name in sorted(names)
        if (dataset_dir / f"{name}.parquet").is_file()
    }


def load_testbed_inputs(
    dataset_dir: Path,
    selection_file: Path,
    registry_file: Path,
    *,
    prompt_input_token_budget: int = PROMPT_INPUT_TOKEN_BUDGET,
    prompt_safety_margin_tokens: int = PROMPT_SAFETY_MARGIN_TOKENS,
) -> TestbedInputs:
    """Translate the frozen sample into current source, segment, and tag inputs."""
    if prompt_input_token_budget <= 0:
        raise ValueError("prompt_input_token_budget must be positive")
    if prompt_safety_margin_tokens < 0:
        raise ValueError("prompt_safety_margin_tokens must be nonnegative")
    dataset_dir = Path(dataset_dir)
    selection_file = Path(selection_file)
    registry_file = Path(registry_file)
    gold_file = dataset_dir / GOLD_FILE
    for path in (gold_file, selection_file, registry_file):
        if not path.is_file():
            raise FileNotFoundError(path)

    selected_rows = read_parquet_rows(selection_file)
    gold_rows = read_parquet_rows(gold_file)
    concepts = read_parquet_rows(registry_file)
    if len(selected_rows) != EXPECTED_SELECTED_SEGMENTS:
        raise DiagnosticInputError(
            f"the selected sample has {len(selected_rows)} segments, expected {EXPECTED_SELECTED_SEGMENTS}"
        )
    selected_artifact_keys = {_artifact_key(row) for row in selected_rows}
    gold_artifact_keys = {
        (
            str(row.get("profile_id") or ""),
            str(row.get("subject_type") or ""),
            str(row.get("subject_id") or ""),
            str(row.get("artifact_digest") or ""),
        )
        for row in gold_rows
    }
    if len(gold_artifact_keys) != EXPECTED_GOLD_ARTIFACTS:
        raise DiagnosticInputError(
            f"the gold sample has {len(gold_artifact_keys)} artifacts, expected {EXPECTED_GOLD_ARTIFACTS}"
        )
    if not gold_artifact_keys <= selected_artifact_keys:
        raise DiagnosticInputError("the selected segment sample does not cover every gold artifact")

    artifacts = _load_artifacts(dataset_dir, selected_artifact_keys)
    counter = TiktokenCounter()
    selected, settings = _segment_selection(
        artifacts,
        selected_rows,
        counter=counter,
    )
    answers = _answers(
        gold_rows,
        artifacts,
        selected,
        selected_rows,
        concepts,
    )
    units: list[ExtractionUnit] = []
    for row in selected_rows:
        key = _selection_key(row)
        artifact = artifacts[key[:4]]
        segment = selected[key]
        prompt_concepts = select_candidate_concepts_for_text(
            segment.text,
            artifact.allowed_schemes,
            concepts,
            limit=PROMPT_CONCEPT_LIMIT,
        )
        units.append(tag_unit(artifact, segment, prompt_concepts))

    task = TagExtractionTask()
    prompt_totals = [
        (
            counter.count(task.instructions + "\n" + canonical_json(task.build_payload(unit.input)))
            + counter.count(canonical_json(task.build_schema(task.build_payload(unit.input))))
            + prompt_safety_margin_tokens,
            unit.unit_id,
        )
        for unit in units
    ]
    over_budget = [
        (total, unit_id)
        for total, unit_id in prompt_totals
        if total > prompt_input_token_budget
    ]
    if over_budget:
        total, unit_id = max(over_budget)
        raise DiagnosticInputError(
            f"{len(over_budget)} tag prompts exceed the {prompt_input_token_budget}-token "
            f"input budget; {unit_id} needs {total}"
        )

    active_tables = _active_source_tables({key[0] for key in selected_artifact_keys})
    profile_counts = Counter(key[0] for key in gold_artifact_keys)
    return TestbedInputs(
        units=tuple(units),
        answers=answers,
        source_facts={
            "dataset_files": _source_file_facts(dataset_dir, active_tables),
            "selection_sha256": sha256_file(selection_file),
            "gold_sha256": sha256_file(gold_file),
            "selected_artifact_count": len(selected_artifact_keys),
            "gold_artifact_count": len(gold_artifact_keys),
        },
        segmentation_facts={
            **settings.identity(),
            "settings_sha256": settings.digest,
            "selected_segment_count": len(selected),
            "gold_span_count": len(gold_rows),
            "gold_coordinate_resolution_counts": dict(
                sorted(
                    Counter(
                        str(expected["coordinate_resolution"])
                        for artifact in answers["artifacts"]
                        for expected in artifact["expected_tags"]
                    ).items()
                )
            ),
            "prompt_input_token_budget": prompt_input_token_budget,
            "prompt_safety_margin_tokens": prompt_safety_margin_tokens,
            "prompt_input_token_max": max((total for total, _ in prompt_totals), default=0),
        },
        vocabulary_facts={
            "registry_sha256": sha256_file(registry_file),
            "registry_concept_count": len(concepts),
            "selection_method": f"lexical-overlap-v1-limit-{PROMPT_CONCEPT_LIMIT}",
        },
        profile_facts={
            "gold_profile_count": len(profile_counts),
            "gold_artifacts_by_profile": dict(sorted(profile_counts.items())),
        },
    )


def _git_commit() -> str:
    repository_root = Path(__file__).resolve().parents[2]
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repository_root,
        )
        if status.stdout.strip():
            return ""
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repository_root,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return result.stdout.strip()


def run_testbed(
    inputs: TestbedInputs,
    output_dir: Path,
    *,
    model: StructuredTextModel,
    run_id: str | None = None,
    task: TagExtractionTask | None = None,
) -> ExtractionOutcome:
    """Run one non-authorizing diagnostic over prepared real-data inputs."""
    selected_task = task or TagExtractionTask()
    items = plan_extraction_items(selected_task, model, inputs.units)
    provider = getattr(model, "run_configuration", None)
    plan = RunPlan(
        run_id=run_id or Path(output_dir).name,
        mode="diagnostic",
        steps=("extract",),
        source_snapshot=inputs.source_facts,
        profiles=inputs.profile_facts,
        vocabulary=inputs.vocabulary_facts,
        segmentation=inputs.segmentation_facts,
        extraction=extraction_plan_facts(
            selected_task,
            inputs.units,
            answers=inputs.answers,
        ),
        provider=(
            dict(provider)
            if isinstance(provider, Mapping)
            else {"model_id": str(getattr(model, "model_id", ""))}
        ),
        code_commit=_git_commit(),
        required_work=tuple(item.work_id for item in items),
    )
    return run_extraction(
        plan,
        output_dir,
        task=selected_task,
        model=model,
        units=inputs.units,
        answers=inputs.answers,
    )


def _preflight_summary(inputs: TestbedInputs) -> dict[str, Any]:
    return {
        "status": "pass",
        "selected_segment_count": inputs.selected_segment_count,
        "selected_artifact_count": inputs.source_facts["selected_artifact_count"],
        "gold_artifact_count": inputs.gold_artifact_count,
        "gold_span_count": inputs.segmentation_facts["gold_span_count"],
        "gold_profile_count": inputs.profile_facts["gold_profile_count"],
        "gold_coordinate_resolution_counts": inputs.segmentation_facts[
            "gold_coordinate_resolution_counts"
        ],
        "prompt_input_token_max": inputs.segmentation_facts["prompt_input_token_max"],
        "prompt_input_token_budget": inputs.segmentation_facts[
            "prompt_input_token_budget"
        ],
        "segmentation_settings_sha256": inputs.segmentation_facts["settings_sha256"],
        "registry_sha256": inputs.vocabulary_facts["registry_sha256"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the current source, segmentation, and tag path on the frozen Rulespec sample."
    )
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--selection-file", type=Path, required=True)
    parser.add_argument("--registry-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate all source, segment, gold, and registry mappings without calling a model.",
    )
    args = parser.parse_args(argv)
    model: OpenAIStructuredTextModel | None = None
    prompt_input_token_budget = PROMPT_INPUT_TOKEN_BUDGET
    prompt_safety_margin_tokens = PROMPT_SAFETY_MARGIN_TOKENS
    if not args.preflight_only:
        if args.output_dir is None:
            parser.error("--output-dir is required unless --preflight-only is used")
        model = OpenAIStructuredTextModel.from_environment()
        if model is None:
            parser.error("OPENAI_API_KEY is required for a real diagnostic run")
        prompt_input_token_budget = model.prompt_input_token_budget
        prompt_safety_margin_tokens = model.prompt_safety_margin_tokens
    inputs = load_testbed_inputs(
        args.dataset_dir,
        args.selection_file,
        args.registry_file,
        prompt_input_token_budget=prompt_input_token_budget,
        prompt_safety_margin_tokens=prompt_safety_margin_tokens,
    )
    if args.preflight_only:
        print(json.dumps(_preflight_summary(inputs), indent=2, sort_keys=True))
        return 0
    assert args.output_dir is not None
    assert model is not None
    outcome = run_testbed(
        inputs,
        args.output_dir,
        model=model,
        run_id=args.run_id,
    )
    summary = {
        "final_state": outcome.outcome.final_state,
        "run_directory": str(outcome.outcome.run_directory),
        "selected_segment_count": inputs.selected_segment_count,
        "gold_artifact_count": inputs.gold_artifact_count,
        "metrics": outcome.metrics,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if outcome.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

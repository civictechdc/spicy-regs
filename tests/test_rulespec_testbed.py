"""Evaluation-side checks for the small Rulespec tagging diagnostic."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters.openai import TiktokenCounter
from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifact
from spicy_regs.docpipeline.source import SourceRecord, build_source_artifact, profile_for_table
from spicy_regs.ontology.common import canonical_json
from spicy_regs.rulespec_testbed import (
    GOLD_FILE,
    DiagnosticInputError,
    _answers,
    _git_commit,
    load_testbed_inputs,
)


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()
SETTINGS = SegmentSettings(
    max_tokens=10_000,
    min_tokens=1,
    overlap_tokens=0,
    tokenizer=COUNTER.name,
    tokenizer_version=COUNTER.version,
)


def _artifact_and_segment(*, heading: str, text: str) -> tuple[Any, Any]:
    outcome = build_source_artifact(
        SourceRecord(
            profile=profile_for_table("cfr_sections"),
            row={
                "granule_id": "CFR-rulespec-testbed",
                "heading": heading,
                "text": text,
            },
        )
    )
    assert outcome.artifact is not None
    segmented = segment_artifact(outcome.artifact, settings=SETTINGS, counter=COUNTER)
    assert len(segmented.segments) == 1
    return outcome.artifact, segmented.segments[0]


def _gold(
    artifact: Any,
    *,
    source_field: str,
    exact_text: str,
    start: int,
    gold_id: str = "gold-1",
    concept_label: str | None = None,
    split: str | None = None,
) -> dict[str, Any]:
    row = {
        "gold_id": gold_id,
        "profile_id": artifact.profile_id,
        "subject_type": artifact.subject_type,
        "subject_id": artifact.subject_id,
        "artifact_digest": artifact.content_sha256,
        "source_field": source_field,
        "start_char": start,
        "end_char": start + len(exact_text),
        "exact_text": exact_text,
        "exact_text_sha256": hashlib.sha256(exact_text.encode()).hexdigest(),
        "concept_scheme": "subject",
        "concept_label": concept_label or exact_text,
    }
    return row if split is None else {**row, "split": split}


def _selection(artifact: Any, segment: Any) -> dict[str, Any]:
    return {
        "profile_id": artifact.profile_id,
        "subject_type": artifact.subject_type,
        "subject_id": artifact.subject_id,
        "artifact_digest": artifact.content_sha256,
        "ordinal": str(segment.ordinal),
        "segment_count": str(segment.segment_count),
        "adversarial_case_ids_json": "[]",
    }


def _concept(label: str) -> dict[str, Any]:
    return {
        "concept_id": f"concept:{label.replace(' ', '-')}",
        "scheme": "subject",
        "pref_label": label,
        "alt_labels_json": "[]",
        "definition": f"Rules concerning {label}.",
        "status": "active",
    }


def test_gold_coordinates_map_to_exact_heading_processing_slices() -> None:
    artifact, segment = _artifact_and_segment(
        heading="Aviation cybersecurity",
        text="The report discusses implementation details.",
    )
    key = (
        artifact.profile_id,
        artifact.subject_type,
        artifact.subject_id,
        artifact.content_sha256,
    )
    gold = _gold(
        artifact,
        source_field="cfr_sections.heading",
        exact_text="Aviation cybersecurity",
        start=0,
    )

    answers = _answers(
        [gold],
        {key: artifact},
        {(*key, segment.ordinal): segment},
        [_selection(artifact, segment)],
        [_concept("Aviation cybersecurity")],
    )

    expected = answers["artifacts"][0]["expected_tags"][0]
    assert expected["coordinate_resolution"] == "provided-offsets"
    assert expected["containing_segment_ids"] == [segment.segment_id]
    assert expected["concept_id"] == "concept:Aviation-cybersecurity"


def test_gold_mapping_repairs_one_unique_match_but_refuses_ambiguity() -> None:
    artifact, segment = _artifact_and_segment(
        heading="Topic",
        text="Clean water applies here.",
    )
    key = (
        artifact.profile_id,
        artifact.subject_type,
        artifact.subject_id,
        artifact.content_sha256,
    )
    repaired = _gold(
        artifact,
        source_field="cfr_sections.text",
        exact_text="Clean water",
        start=1,
    )

    answers = _answers(
        [repaired],
        {key: artifact},
        {(*key, segment.ordinal): segment},
        [_selection(artifact, segment)],
        [_concept("Clean water")],
    )

    expected = answers["artifacts"][0]["expected_tags"][0]
    assert expected["start_char"] == 0
    assert expected["coordinate_resolution"] == "unique-exact-match"

    repeated_artifact, repeated_segment = _artifact_and_segment(
        heading="Topic",
        text="PFAS appears, and PFAS appears again.",
    )
    repeated_key = (
        repeated_artifact.profile_id,
        repeated_artifact.subject_type,
        repeated_artifact.subject_id,
        repeated_artifact.content_sha256,
    )
    ambiguous = _gold(
        repeated_artifact,
        source_field="cfr_sections.text",
        exact_text="PFAS",
        start=1,
    )
    with pytest.raises(DiagnosticInputError, match="absent or ambiguous"):
        _answers(
            [ambiguous],
            {repeated_key: repeated_artifact},
            {(*repeated_key, repeated_segment.ordinal): repeated_segment},
            [_selection(repeated_artifact, repeated_segment)],
            [_concept("PFAS")],
        )


def test_gold_mapping_refuses_bad_digests_and_uncontained_spans() -> None:
    artifact, segment = _artifact_and_segment(
        heading="Topic",
        text="Clean water applies here.",
    )
    key = (
        artifact.profile_id,
        artifact.subject_type,
        artifact.subject_id,
        artifact.content_sha256,
    )
    gold = _gold(
        artifact,
        source_field="cfr_sections.text",
        exact_text="Clean water",
        start=0,
    )
    bad_digest = {**gold, "exact_text_sha256": "0" * 64}
    with pytest.raises(DiagnosticInputError, match="invalid exact-text digest"):
        _answers(
            [bad_digest],
            {key: artifact},
            {(*key, segment.ordinal): segment},
            [_selection(artifact, segment)],
            [_concept("Clean water")],
        )

    with pytest.raises(DiagnosticInputError, match="not fully contained"):
        _answers(
            [gold],
            {key: artifact},
            {},
            [_selection(artifact, segment)],
            [_concept("Clean water")],
        )


def test_gold_split_defaults_to_train_and_travels_with_every_answer() -> None:
    artifact, segment = _artifact_and_segment(
        heading="Aviation cybersecurity",
        text="The report discusses implementation details.",
    )
    key = (
        artifact.profile_id,
        artifact.subject_type,
        artifact.subject_id,
        artifact.content_sha256,
    )
    gold = _gold(
        artifact,
        source_field="cfr_sections.heading",
        exact_text="Aviation cybersecurity",
        start=0,
    )

    def answers_for(row: dict[str, Any]) -> dict[str, Any]:
        return _answers(
            [row],
            {key: artifact},
            {(*key, segment.ordinal): segment},
            [_selection(artifact, segment)],
            [_concept("Aviation cybersecurity")],
        )

    absent = answers_for(gold)
    assert absent["artifacts"][0]["split"] == "train"
    assert absent["artifacts"][0]["expected_tags"][0]["split"] == "train"

    holdout = answers_for({**gold, "split": "holdout"})
    assert holdout["artifacts"][0]["split"] == "holdout"
    assert holdout["artifacts"][0]["expected_tags"][0]["split"] == "holdout"

    with pytest.raises(DiagnosticInputError, match="unknown split"):
        answers_for({**gold, "split": "validation"})


def test_gold_refuses_one_artifact_split_across_two_partitions() -> None:
    artifact, segment = _artifact_and_segment(
        heading="Aviation cybersecurity",
        text="Water policy is also discussed here.",
    )
    key = (
        artifact.profile_id,
        artifact.subject_type,
        artifact.subject_id,
        artifact.content_sha256,
    )
    heading_gold = _gold(
        artifact,
        source_field="cfr_sections.heading",
        exact_text="Aviation cybersecurity",
        start=0,
        split="train",
    )
    text_gold = _gold(
        artifact,
        source_field="cfr_sections.text",
        exact_text="Water policy",
        start=0,
        gold_id="gold-2",
        split="holdout",
    )

    with pytest.raises(DiagnosticInputError, match="after an earlier row placed it"):
        _answers(
            [heading_gold, text_gold],
            {key: artifact},
            {(*key, segment.ordinal): segment},
            [_selection(artifact, segment)],
            [_concept("Aviation cybersecurity"), _concept("Water policy")],
        )


def test_git_commit_checks_the_implementation_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Path] = []

    def fake_run(*args: Any, **kwargs: Any) -> SimpleNamespace:
        del args
        calls.append(Path(kwargs["cwd"]))
        return SimpleNamespace(stdout=" M src/spicy_regs/rulespec_testbed.py\n")

    monkeypatch.setattr("spicy_regs.rulespec_testbed.subprocess.run", fake_run)

    assert _git_commit() == ""
    assert calls == [Path(__file__).resolve().parents[1]]


HOLDOUT_ONLY_LABEL = "quantum ferret doctrine"
_MINI_ROWS = (
    {
        "granule_id": "CFR-mini-aviation",
        "heading": "Aviation cybersecurity",
        "text": "This section governs aviation cybersecurity reporting by certificate holders.",
    },
    {
        "granule_id": "CFR-mini-water",
        "heading": "Drinking water standards",
        "text": "This section sets drinking water standards for public water systems.",
    },
)


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _mini_dataset(root: Path) -> tuple[Path, Path, Path, list[Any], int]:
    """Write a two-artifact sample whose declared counts are not the defaults."""
    dataset_dir = root / "dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(dataset_dir / "cfr_sections.parquet", [dict(row) for row in _MINI_ROWS])

    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    artifacts: list[Any] = []
    selection_rows: list[dict[str, Any]] = []
    for row in _MINI_ROWS:
        outcome = build_source_artifact(
            SourceRecord(profile=profile_for_table("cfr_sections"), row=dict(row))
        )
        assert outcome.state == "completed" and outcome.artifact is not None
        artifact = outcome.artifact
        artifacts.append(artifact)
        segmented = segment_artifact(artifact, settings=settings, counter=counter)
        for segment in segmented.segments:
            selection_rows.append(
                {
                    "profile_id": artifact.profile_id,
                    "subject_type": artifact.subject_type,
                    "subject_id": artifact.subject_id,
                    "artifact_digest": artifact.content_sha256,
                    "ordinal": segment.ordinal,
                    "segment_count": segment.segment_count,
                    "adversarial_case_ids_json": "[]",
                }
            )
    selection_file = root / "selected_segments.parquet"
    _write_parquet(selection_file, selection_rows)

    registry_file = root / "registry.parquet"
    _write_parquet(
        registry_file,
        [_concept("Aviation cybersecurity"), _concept("Drinking water standards"), _concept("Noise control")],
    )

    # Two gold tables over the same source: they differ only in what the
    # holdout artifact's gold concept is, so any prompt that changes between
    # them is reading gold.
    for name, holdout_label in (
        (GOLD_FILE, HOLDOUT_ONLY_LABEL),
        ("gold_spans_variant.parquet", "acoustic emissions doctrine"),
    ):
        _write_parquet(
            dataset_dir / name,
            [
                _gold(
                    artifacts[0],
                    source_field="cfr_sections.heading",
                    exact_text="Aviation cybersecurity",
                    start=0,
                    gold_id="mini-gold-train",
                    split="train",
                ),
                _gold(
                    artifacts[1],
                    source_field="cfr_sections.heading",
                    exact_text="Drinking water standards",
                    start=0,
                    gold_id="mini-gold-holdout",
                    concept_label=holdout_label,
                    split="holdout",
                ),
            ],
        )
    return dataset_dir, selection_file, registry_file, artifacts, len(selection_rows)


def test_declared_expectations_replace_the_hard_coded_sample_constants(tmp_path: Path) -> None:
    dataset_dir, selection_file, registry_file, _, segment_count = _mini_dataset(tmp_path)

    inputs = load_testbed_inputs(
        dataset_dir,
        selection_file,
        registry_file,
        expected_gold_artifacts=2,
        expected_selected_segments=segment_count,
        gold_file="gold_spans_variant.parquet",
    )

    assert inputs.gold_artifact_count == 2
    assert inputs.selected_segment_count == segment_count
    assert inputs.source_facts["gold_file"] == "gold_spans_variant.parquet"
    assert inputs.source_facts["gold_artifacts_by_split"] == {"holdout": 1, "train": 1}

    with pytest.raises(DiagnosticInputError, match="expected 3"):
        load_testbed_inputs(
            dataset_dir,
            selection_file,
            registry_file,
            expected_gold_artifacts=3,
            expected_selected_segments=segment_count,
            gold_file="gold_spans_variant.parquet",
        )
    # The frozen-sample defaults still apply when the caller declares nothing.
    with pytest.raises(DiagnosticInputError, match="expected 109"):
        load_testbed_inputs(dataset_dir, selection_file, registry_file)
    with pytest.raises(FileNotFoundError):
        load_testbed_inputs(
            dataset_dir,
            selection_file,
            registry_file,
            expected_gold_artifacts=2,
            expected_selected_segments=segment_count,
            gold_file="gold_spans_absent.parquet",
        )


def test_gold_never_reaches_a_prompt_payload_or_the_prompt_registry(tmp_path: Path) -> None:
    dataset_dir, selection_file, registry_file, _, segment_count = _mini_dataset(tmp_path)

    def load(gold_file: str) -> Any:
        return load_testbed_inputs(
            dataset_dir,
            selection_file,
            registry_file,
            expected_gold_artifacts=2,
            expected_selected_segments=segment_count,
            gold_file=gold_file,
        )

    first = load(GOLD_FILE)
    second = load("gold_spans_variant.parquet")

    # Nothing that reaches a model may move when only gold moves.
    assert [unit.unit_id for unit in first.units] == [unit.unit_id for unit in second.units]
    assert canonical_json([unit.input for unit in first.units]) == canonical_json(
        [unit.input for unit in second.units]
    )
    assert first.source_facts["gold_sha256"] != second.source_facts["gold_sha256"]

    prompts = canonical_json([unit.input for unit in first.units])
    for leaked in (HOLDOUT_ONLY_LABEL, "mini-gold-holdout", "mini-gold-train", "holdout"):
        assert leaked not in prompts
    # The oracle really does carry what the prompts must not.
    assert HOLDOUT_ONLY_LABEL in canonical_json(first.answers)
    assert "holdout" in canonical_json(first.answers)

    registry_ids = {str(row["concept_id"]) for row in pq.read_table(registry_file).to_pylist()}
    offered = {
        str(concept["concept_id"])
        for unit in first.units
        for concept in unit.input["available_concepts"]
    }
    assert offered and offered <= registry_ids
    # Gold cannot register a concept either: the registry the run reports is
    # still the file the caller supplied.
    assert first.vocabulary_facts["registry_sha256"] == hashlib.sha256(
        registry_file.read_bytes()
    ).hexdigest()
    assert first.vocabulary_facts == second.vocabulary_facts


@pytest.mark.integration
def test_canonical_sample_preflight_maps_every_gold_span_without_a_model() -> None:
    output_root_value = os.environ.get("SPICY_REGS_TESTBED_OUTPUT")
    if not output_root_value:
        pytest.skip("SPICY_REGS_TESTBED_OUTPUT does not name the local frozen outputs")
    output_root = Path(output_root_value)
    baseline = output_root / "segmentation-tagging-document-openai-structure-overlap-1800-v4"

    inputs = load_testbed_inputs(
        output_root / "segmented-real-data-evaluation-v2-rerun",
        baseline / "tagging_segments.parquet",
        baseline / "tagging_input_registry.parquet",
    )

    assert inputs.gold_artifact_count == 35
    assert inputs.selected_segment_count == 109
    assert inputs.source_facts["selected_artifact_count"] == 44
    assert inputs.segmentation_facts["gold_span_count"] == 35
    assert inputs.segmentation_facts["gold_coordinate_resolution_counts"] == {
        "provided-offsets": 35
    }

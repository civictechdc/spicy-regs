"""Evaluation-side checks for the small Rulespec tagging diagnostic."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifact
from spicy_regs.docpipeline.source import SourceRecord, build_source_artifact, profile_for_table
from spicy_regs.rulespec_testbed import (
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


def _gold(artifact: Any, *, source_field: str, exact_text: str, start: int) -> dict[str, Any]:
    return {
        "gold_id": "gold-1",
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
        "concept_label": exact_text,
    }


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

"""Regression tests for citable slices and reusable segment content identity."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.segments import (
    SEGMENT_COLUMNS,
    ProcessingSegment,
    SegmentSettings,
    contains_evidence_span,
    contains_span,
    segment_artifact,
)
from spicy_regs.docpipeline.source import SourceRecord, build_source_artifact, profile_for_table


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()
SETTINGS = SegmentSettings(
    max_tokens=1000,
    min_tokens=1,
    overlap_tokens=0,
    tokenizer=COUNTER.name,
    tokenizer_version=COUNTER.version,
)


def _cfr(**changes: Any) -> Any:
    row = {
        "granule_id": "CFR-REVIEW",
        "heading": None,
        "cfr_ref": None,
        "title": None,
        "part": None,
        "section": None,
        "text": "§ 1 Scope\n\nFacilities must sample water.",
        "full_text": None,
        "xml_text": None,
    }
    row.update(changes)
    outcome = build_source_artifact(SourceRecord(profile=profile_for_table("cfr_sections"), row=row))
    assert outcome.artifact is not None
    return outcome.artifact


def test_segment_slices_carry_evidence_context_layer_and_coordinate_facts() -> None:
    outcome = segment_artifact(_cfr(), settings=SETTINGS, counter=COUNTER)
    slices = tuple(one for segment in outcome.segments for one in segment.slices)

    assert slices
    assert all(one.evidence_grade == "source-exact" for one in slices)
    assert all(one.content_layer == "body" for one in slices)
    assert all(one.coordinate_grade == "source-exact" for one in slices)
    assert {one.context_only for one in slices} == {False, True}


def test_processing_text_keeps_context_slices_but_evidence_slices_exclude_them() -> None:
    segment = segment_artifact(_cfr(), settings=SETTINGS, counter=COUNTER).segments[0]
    heading = next(one for one in segment.slices if one.context_only)
    body = next(one for one in segment.slices if not one.context_only)

    assert heading.text in segment.text
    assert segment.evidence_slices == (body,)
    assert contains_span(segment, heading.source_field, heading.start_char, heading.end_char)
    assert not contains_evidence_span(segment, heading.source_field, heading.start_char, heading.end_char)
    assert contains_evidence_span(segment, body.source_field, body.start_char, body.end_char)


def test_content_identity_is_not_a_provider_work_identity() -> None:
    segment = segment_artifact(_cfr(), settings=SETTINGS, counter=COUNTER).segments[0]
    fields = {one.name for one in dataclasses.fields(ProcessingSegment)}
    columns = {name for name, _ in SEGMENT_COLUMNS}
    production = (Path(__file__).parents[1] / "src" / "spicy_regs" / "docpipeline" / "segments.py").read_text(
        encoding="utf-8"
    )
    retired_digest_name = "_".join(("work", "digest"))
    retired_identity_name = "_".join(("work", "identity"))

    assert segment.content_digest
    assert "artifact_id" not in segment.content_identity()
    assert "artifact_sha256" not in segment.content_identity()
    assert "context" not in segment.content_identity()
    assert "content_digest" in fields
    assert "content_digest" in columns
    assert not hasattr(segment, retired_digest_name)
    assert not hasattr(segment, retired_identity_name)
    assert retired_digest_name not in production
    assert retired_identity_name not in production


def test_later_provider_reuse_identity_requirements_are_explicit() -> None:
    documentation = ProcessingSegment.content_identity.__doc__ or ""

    for required in (
        "prompt",
        "schema",
        "provider",
        "model",
        "revision",
        "settings",
        "context digest",
        "policies",
        "earlier run",
    ):
        assert required in documentation

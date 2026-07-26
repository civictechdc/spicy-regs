"""Regression tests for the Step 4 source-review correction set."""

from __future__ import annotations

import dataclasses
import hashlib
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifact
from spicy_regs.docpipeline.source import (
    GATE_COMPLETED,
    GATE_EXIT,
    GATE_EXTRA_UNAVAILABLE,
    GATE_INPUT_OVER_LIMIT,
    GATE_MALFORMED_RESULT,
    GATE_RESULT_OVER_LIMIT,
    GATE_SIGNAL,
    GATE_TIMEOUT,
    OBSERVED_LIMITS,
    PARSED_TEXT_COORDINATES,
    PARSER_ATTEMPT_COLUMNS,
    PARSER_ATTEMPT_TABLE,
    PARSER_DERIVED_EVIDENCE,
    STEP4_ACTIVE_SOURCE_TABLES,
    UNENFORCED_LIMITS,
    ContainedParseResult,
    ParsedOfficeElement,
    ParsedOfficeText,
    ProcessGateLimits,
    ProcessGateReceipt,
    SourceAttachment,
    SourceRecord,
    artifact_fragments,
    build_source_artifact,
    build_source_artifacts,
    coverage_rows,
    iter_source_records,
    processing_regions,
    profile_for_table,
    run_contained_parse,
    write_source_tables,
)
from spicy_regs.ontology.common import read_parquet_rows, write_parquet_rows

OFFICE_BYTES = b"PK\x03\x04same office bytes"
OFFICE_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
COMMENTS = profile_for_table("comments")


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


def _receipt(classification: str = GATE_COMPLETED, *, parser_status: str = "completed") -> ProcessGateReceipt:
    return ProcessGateReceipt(
        worker_module="spicy_regs.docpipeline.adapters.docling",
        classification=classification,
        parser_status=parser_status,
        parser_failure_reason=None,
        exit_status=0,
        signal_number=None,
        process_group_terminated=True,
        duration_ms=1.0,
        result_bytes=100,
        result_over_limit=False,
        stderr_bytes=0,
        stderr_over_limit=False,
        limits=ProcessGateLimits(),
    )


def _element(
    ordinal: int,
    text: str,
    start: int,
    *,
    layer: str = "body",
    kind: str = "text",
    coordinate_grade: str = "none",
) -> ParsedOfficeElement:
    return ParsedOfficeElement(
        ordinal=ordinal,
        kind=kind,
        text=text,
        start_char=start,
        end_char=start + len(text),
        content_layer=layer,
        coordinate_grade=coordinate_grade,
        text_usable=True,
        heading_path=(),
    )


def _parsed_result(
    parser_id: str,
    *,
    text: str = "Body",
    elements: tuple[ParsedOfficeElement, ...] | None = None,
    classification: str = GATE_COMPLETED,
    parser_status: str = "completed",
    call: dict[str, Any] | None = None,
) -> ContainedParseResult:
    parsed = (
        None
        if parser_status != "completed"
        else ParsedOfficeText(
            text=text,
            elements=elements or (_element(0, text, 0),),
            parser_id=parser_id,
            input_format="docx",
            source_sha256=hashlib.sha256(OFFICE_BYTES).hexdigest(),
            source_bytes=len(OFFICE_BYTES),
            evidence_grade=PARSER_DERIVED_EVIDENCE,
            offsets=PARSED_TEXT_COORDINATES,
        )
    )
    return ContainedParseResult(
        receipt=_receipt(classification, parser_status=parser_status),
        parsed=parsed,
        call=call
        or {
            "provider": "docling",
            "operation": "document-parse",
            "parser_id": parser_id,
            "status": parser_status,
            "input_format": "docx",
            "source_sha256": hashlib.sha256(OFFICE_BYTES).hexdigest(),
            "source_bytes": len(OFFICE_BYTES),
        },
    )


def _record(*attachments: SourceAttachment) -> SourceRecord:
    return SourceRecord(
        profile=COMMENTS,
        row={
            "comment_id": "COMMENT-PARSER",
            "title": None,
            "comment": None,
            "text_content": None,
            "organization": None,
            "category": None,
        },
        attachments=attachments
        or (
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="rule.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )


class _Parser:
    def __init__(self, result: ContainedParseResult) -> None:
        self.result = result

    def __call__(self, content: bytes, *, source_name: str, media_type: str | None) -> ContainedParseResult:
        return self.result


def _artifact(result: ContainedParseResult) -> tuple[Any, Any]:
    outcome = build_source_artifact(_record(), parser=_Parser(result))
    assert outcome.artifact is not None, outcome.reason or outcome.error
    return outcome, outcome.artifact


def test_parser_region_identity_binds_mapping_revision_and_parsed_field_digest() -> None:
    _, first = _artifact(_parsed_result("docling:office-mapping-6:aaaa"))
    _, second = _artifact(_parsed_result("docling:office-mapping-7:bbbb"))

    assert first.content_sha256 == second.content_sha256
    assert first.field_sha256 == second.field_sha256
    assert [one.region_id for one in first.regions] != [one.region_id for one in second.regions]
    assert [one.fragment_id for one in artifact_fragments(first)] != [
        one.fragment_id for one in artifact_fragments(second)
    ]


def test_parser_layers_are_closed_and_keep_context_out_of_processing_and_evidence() -> None:
    pieces = ("Body", "Header", "Note", "Hidden")
    text = "\n\n".join(pieces)
    starts = (0, 6, 14, 20)
    elements = (
        _element(0, pieces[0], starts[0], layer="body"),
        _element(1, pieces[1], starts[1], layer="furniture"),
        _element(2, pieces[2], starts[2], layer="notes"),
        _element(3, pieces[3], starts[3], layer="background"),
    )
    _, artifact = _artifact(_parsed_result("docling:office-mapping-6:layers", text=text, elements=elements))

    by_layer = {one.content_layer: one for one in artifact.regions}
    assert set(by_layer) == {"body", "furniture", "notes", "background"}
    assert by_layer["body"].evidence_eligible and not by_layer["body"].context_only
    assert not by_layer["furniture"].evidence_eligible and by_layer["furniture"].context_only
    assert not by_layer["notes"].evidence_eligible and by_layer["notes"].context_only
    assert by_layer["background"].quarantine_reason
    assert {one.content_layer for one in artifact_fragments(artifact)} == {"body", "furniture", "notes"}
    assert {one.content_layer for one in processing_regions(artifact)} == {"body"}
    assert artifact.uncovered_chars == 0
    held_rows = [
        one
        for one in coverage_rows(
            build_source_artifact(
                _record(),
                parser=_Parser(
                    _parsed_result(
                        "docling:office-mapping-6:layers",
                        text=text,
                        elements=elements,
                    )
                ),
            )
        )
        if one["reason"] == "parser-content-layer-held"
    ]
    assert held_rows and {one["uncovered_chars"] for one in held_rows} == {0}

    segmented = segment_artifact(artifact, settings=SETTINGS, counter=COUNTER)
    assert segmented.coverage.uncovered_chars == 0
    assert {one.content_layer for segment in segmented.segments for one in segment.slices} == {"body"}
    assert all(segment.evidence_slices == segment.slices for segment in segmented.segments)


def test_parser_coordinates_target_the_adapter_text_and_reach_rows() -> None:
    _, artifact = _artifact(_parsed_result("docling:office-mapping-6:coordinates"))
    region = artifact.regions[0]
    fragment = artifact_fragments(artifact)[0]

    assert region.coordinates == fragment.coordinates == PARSED_TEXT_COORDINATES
    assert region.content_layer == fragment.content_layer == "body"
    assert region.coordinate_grade == fragment.coordinate_grade == "none"


def test_multiple_fallback_renditions_are_quarantined_without_parser_choice() -> None:
    first = SourceAttachment("comments.first", "first.docx", OFFICE_MEDIA_TYPE, OFFICE_BYTES)
    second = SourceAttachment("comments.second", "second.docx", OFFICE_MEDIA_TYPE, OFFICE_BYTES)
    outcome = build_source_artifact(_record(first, second), parser=_Parser(_parsed_result("unused")))

    assert outcome.state == "rejected"
    assert outcome.reason == "multiple_renditions_not_implemented"
    assert outcome.artifact is None
    assert {one.terminal_state for one in outcome.parser_attempts} == {"quarantine"}
    assert len(outcome.parser_attempts) == 2


def test_every_parse_attempt_is_immutable_and_written_with_full_gate_facts(tmp_path: Path) -> None:
    call = {
        "provider": "docling",
        "operation": "document-parse",
        "parser_id": "docling:office-mapping-6:attempt",
        "status": "completed",
        "input_format": "docx",
        "source_sha256": hashlib.sha256(OFFICE_BYTES).hexdigest(),
        "source_bytes": len(OFFICE_BYTES),
        "note": "OPENAI_API_KEY=secret",
    }
    outcome, _ = _artifact(_parsed_result(call["parser_id"], call=call))
    attempt = outcome.parser_attempts[0]

    assert dataclasses.is_dataclass(attempt)
    with pytest.raises(dataclasses.FrozenInstanceError):
        attempt.terminal_state = "changed"  # type: ignore[misc]
    assert attempt.terminal_state == "success"
    assert attempt.source_sha256 == outcome.artifact.content_sha256
    assert attempt.attachment_sha256 == hashlib.sha256(OFFICE_BYTES).hexdigest()
    assert attempt.gate_classification == GATE_COMPLETED
    assert "OPENAI_API_KEY=secret" not in attempt.call_json

    paths = write_source_tables(tmp_path, [outcome])
    assert set(paths) >= {PARSER_ATTEMPT_TABLE}
    assert [field.name for field in __import__("pyarrow.parquet").parquet.read_schema(paths[PARSER_ATTEMPT_TABLE])] == [
        name for name, _ in PARSER_ATTEMPT_COLUMNS
    ]
    rows = read_parquet_rows(paths[PARSER_ATTEMPT_TABLE])
    assert len(rows) == 1
    assert rows[0]["attempt_id"] == attempt.attempt_id
    assert rows[0]["terminal_state"] == "success"
    assert rows[0]["limits_json"]
    assert rows[0]["enforced_limits"]
    assert rows[0]["observed_limits"]
    assert rows[0]["unenforced_limits"]


def _script_command(script: str) -> Callable[[Path], Sequence[str]]:
    def command(job_path: Path) -> Sequence[str]:
        return (sys.executable, "-I", "-c", script, str(job_path))

    return command


def test_input_over_limit_is_a_receipted_result_not_a_run_crash() -> None:
    result = run_contained_parse(
        OFFICE_BYTES,
        source_name="rule.docx",
        media_type=OFFICE_MEDIA_TYPE,
        limits=ProcessGateLimits(max_input_bytes=3),
        worker_command=_script_command("raise AssertionError('must not launch')"),
    )

    assert result.parsed is None
    assert result.receipt.classification == GATE_INPUT_OVER_LIMIT
    assert result.receipt.result_bytes == result.receipt.stderr_bytes == 0

    with pytest.raises(Exception, match="bytes"):
        run_contained_parse(
            cast(Any, "not bytes"),
            source_name="rule.docx",
            media_type=OFFICE_MEDIA_TYPE,
        )


@pytest.mark.parametrize(
    ("classification", "parser_status", "terminal_state"),
    [
        (GATE_EXTRA_UNAVAILABLE, "", "unavailable"),
        (GATE_COMPLETED, "failed", "declared_failure"),
        (GATE_MALFORMED_RESULT, "", "malformed_result"),
        (GATE_TIMEOUT, "", "timeout"),
        (GATE_EXIT, "", "exit"),
        (GATE_SIGNAL, "", "signal"),
        (GATE_RESULT_OVER_LIMIT, "", "result_oversize"),
        (GATE_INPUT_OVER_LIMIT, "", "input_oversize"),
    ],
)
def test_every_process_terminal_state_survives_on_the_source_outcome(
    classification: str,
    parser_status: str,
    terminal_state: str,
) -> None:
    result = _parsed_result(
        "docling:office-mapping-6:failed",
        classification=classification,
        parser_status=parser_status,
    )
    outcome = build_source_artifact(_record(), parser=_Parser(result))

    assert outcome.state == "rejected"
    assert len(outcome.parser_attempts) == 1
    assert outcome.parser_attempts[0].terminal_state == terminal_state
    assert outcome.parser_attempts[0].gate_classification == classification


def test_limit_claims_are_three_exact_disjoint_lists() -> None:
    receipt = _receipt()
    groups = (set(receipt.enforced_limits), set(receipt.observed_limits), set(receipt.unenforced_limits))

    assert OBSERVED_LIMITS == ("stderr_bytes",)
    assert not groups[0] & groups[1]
    assert not groups[0] & groups[2]
    assert not groups[1] & groups[2]
    assert "stderr_bytes" not in receipt.enforced_limits
    assert "stderr_bytes" not in UNENFORCED_LIMITS


def test_explicit_active_source_selection_records_inactive_present_tables(tmp_path: Path) -> None:
    write_parquet_rows(
        tmp_path / "comments.parquet",
        columns=("comment_id", "comment"),
        rows=({"comment_id": "COMMENT-INACTIVE", "comment": "not active"},),
    )
    write_parquet_rows(
        tmp_path / "gao_reports.parquet",
        columns=("report_id", "title"),
        rows=({"report_id": "GAO-ACTIVE", "title": "active"},),
    )

    records = list(iter_source_records(tmp_path, active_source_tables={"gao_reports"}))
    outcomes = build_source_artifacts(tmp_path, active_source_tables={"gao_reports"})

    assert [one.profile.source_table for one in records] == ["gao_reports"]
    assert [one.artifact.source_table for one in outcomes if one.artifact is not None] == ["gao_reports"]
    inactive = [one for outcome in outcomes for one in outcome.source_table_exclusions]
    assert [(one.source_table, one.reason) for one in inactive] == [("comments", "inactive-source-table")]
    assert "comments" not in STEP4_ACTIVE_SOURCE_TABLES


def test_none_active_selection_preserves_the_existing_all_present_behavior(tmp_path: Path) -> None:
    write_parquet_rows(
        tmp_path / "comments.parquet",
        columns=("comment_id", "comment"),
        rows=({"comment_id": "COMMENT-CURRENT", "comment": "included under None"},),
    )

    outcomes = build_source_artifacts(tmp_path, active_source_tables=None)

    assert [one.artifact.source_table for one in outcomes if one.artifact is not None] == ["comments"]
    assert not [one for outcome in outcomes for one in outcome.source_table_exclusions]

"""End-to-end tests for segment processing, aggregation, and resume."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import RunContext, write_parquet_rows
from spicy_regs.ontology.concepts import (
    CONCEPT_COLUMNS,
    latest_assignments,
    seed_concept,
)
from spicy_regs.ontology.ledger import SEGMENT_LEDGER_COLUMNS
from spicy_regs.ontology.llm import (
    OpenAIOntologyModel,
    TagProposal,
    ValidationProposal,
)
from spicy_regs.ontology.subjects import Subject
from spicy_regs.transforms.build_concept_assignments import (
    build_concept_assignments,
)

_CONTEXT = RunContext(
    "segment-pipeline-fixture",
    "2026-07-24T12:00:00Z",
)


def _write_long_comment_fixture(root: Path) -> str:
    concept = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert concept is not None
    write_parquet_rows(
        root / "concepts.parquet",
        columns=CONCEPT_COLUMNS,
        rows=[concept],
    )
    body = "\n\n".join(
        f"PFAS paragraph {index} describes a distinct water policy detail. "
        + ("supporting evidence " * 80)
        for index in range(36)
    )
    write_parquet_rows(
        root / "comments.parquet",
        columns=(
            "comment_id",
            "title",
            "comment",
            "text_content",
            "organization",
            "category",
        ),
        rows=[
            {
                "comment_id": "COMMENT-LONG-1",
                "title": "PFAS water policy comment",
                "comment": body,
                "text_content": None,
                "organization": "Public commenter",
                "category": "Public Submission",
            }
        ],
    )
    return str(concept["concept_id"])


class _SegmentModel:
    model_id = "test-segment-model:v1"
    production_provider = False

    def __init__(self, concept_id: str, *, zero: bool = False) -> None:
        self.concept_id = concept_id
        self.zero = zero
        self.tag_calls = 0
        self.validation_calls = 0
        self.segment_calls: list[str] = []

    def tag(self, subject, concepts):
        del concepts
        self.tag_calls += 1
        self.segment_calls.append(subject.segment_id)
        if self.zero:
            return []
        field, text = next(
            (
                (field_name, value)
                for field_name, value in subject.fields.items()
                if "PFAS" in value
            ),
            next(iter(subject.fields.items())),
        )
        evidence = "PFAS" if "PFAS" in text else text[:12]
        start = text.index(evidence)
        return [
            TagProposal(
                concept_id=self.concept_id,
                proposed_label=None,
                scheme="subject",
                definition=None,
                confidence=0.9,
                evidence_text=evidence,
                evidence_field=field,
                evidence_start=start,
                evidence_end=start + len(evidence),
                justification="The exact segment span supports the tag.",
            )
        ]

    def validate(self, *, subject, concept, assignment):
        del subject, concept, assignment
        self.validation_calls += 1
        return ValidationProposal(
            agrees=True,
            confidence=0.95,
            rationale="The exact source span supports the concept.",
        )


def test_long_artifact_processes_every_segment_and_aggregates_one_assignment(
    tmp_path: Path,
) -> None:
    concept_id = _write_long_comment_fixture(tmp_path)
    model = _SegmentModel(concept_id)

    output = build_concept_assignments(
        tmp_path,
        model=model,
        run_id="segment-run",
        asserted_at="2026-07-24T12:00:00Z",
        generation_limit=1,
        validation_percent=100,
    )

    ledger = pq.read_table(
        tmp_path / "ontology_segment_ledger.parquet"
    ).to_pylist()
    assert pq.ParquetFile(
        tmp_path / "ontology_segment_ledger.parquet"
    ).schema_arrow.names == list(SEGMENT_LEDGER_COLUMNS)
    assert len(ledger) > 1
    assert model.tag_calls == len(ledger)
    assert model.validation_calls == len(ledger)
    assert {row["status"] for row in ledger} == {"tagged"}
    assert all(int(row["token_count"]) <= 1_200 for row in ledger)

    current = latest_assignments(pq.read_table(output).to_pylist())
    assert len(current) == 1
    evidence = json.loads(current[0]["evidence_json"])
    assert evidence["artifact_sha256"]
    assert len(evidence["spans"]) == len(ledger)
    assert len(evidence["segment_ids"]) == len(ledger)
    assert evidence["validation"]["agrees"] is True
    assert evidence["validation"]["accepted_span_count"] == len(ledger)

    comment_parts = []
    for row in ledger:
        fields = json.loads(row["fields_json"])
        field_sources = json.loads(row["field_sources_json"])
        source_spans = json.loads(row["source_spans_json"])
        comment_parts.extend(
            (source_spans[field_ref][0], value)
            for field_ref, value in fields.items()
            if field_sources[field_ref] == "comments.comment"
        )
    reconstructed = "".join(
        value for _, value in sorted(comment_parts)
    )
    source_body = pq.read_table(
        tmp_path / "comments.parquet"
    ).to_pylist()[0]["comment"]
    assert reconstructed == source_body


def test_zero_tag_segment_is_durable_and_prevents_repeat_work(
    tmp_path: Path,
) -> None:
    concept_id = _write_long_comment_fixture(tmp_path)
    model = _SegmentModel(concept_id, zero=True)

    build_concept_assignments(
        tmp_path,
        model=model,
        run_id="zero-run",
        asserted_at="2026-07-24T12:00:00Z",
        generation_limit=1,
        validation_percent=100,
    )
    first_call_count = model.tag_calls
    ledger_before = pq.read_table(
        tmp_path / "ontology_segment_ledger.parquet"
    ).to_pylist()
    assert first_call_count > 1
    assert {row["status"] for row in ledger_before} == {"zero_tags"}

    build_concept_assignments(
        tmp_path,
        model=model,
        run_id="zero-run",
        asserted_at="2026-07-24T12:00:00Z",
        generation_limit=1,
        validation_percent=100,
    )

    assert model.tag_calls == first_call_count
    assert pq.read_table(
        tmp_path / "ontology_segment_ledger.parquet"
    ).to_pylist() == ledger_before


def test_resume_reuses_successful_exact_segments_after_failure(
    tmp_path: Path,
) -> None:
    concept_id = _write_long_comment_fixture(tmp_path)

    class _FailOnceModel(_SegmentModel):
        def __init__(self, value: str) -> None:
            super().__init__(value)
            self.failed = False

        def tag(self, subject, concepts):
            if self.tag_calls == 1 and not self.failed:
                self.tag_calls += 1
                self.segment_calls.append(subject.segment_id)
                self.failed = True
                raise TimeoutError("controlled provider timeout")
            return super().tag(subject, concepts)

    model = _FailOnceModel(concept_id)
    with pytest.raises(RuntimeError, match="exact segment checkpoint"):
        build_concept_assignments(
            tmp_path,
            model=model,
            run_id="resume-segment-run",
            asserted_at="2026-07-24T12:00:00Z",
            generation_limit=1,
            validation_percent=0,
        )
    first_segment = model.segment_calls[0]
    assert model.segment_calls.count(first_segment) == 1

    build_concept_assignments(
        tmp_path,
        model=model,
        run_id="resume-segment-run",
        asserted_at="2026-07-24T12:00:00Z",
        generation_limit=1,
        validation_percent=0,
    )

    assert model.segment_calls.count(first_segment) == 1
    ledger = pq.read_table(
        tmp_path / "ontology_segment_ledger.parquet"
    ).to_pylist()
    assert all(row["status"] == "tagged" for row in ledger)
    failed_row = next(
        row
        for row in ledger
        if row["segment_id"] == model.segment_calls[1]
    )
    attempts = json.loads(failed_row["attempts_json"])
    assert attempts[0]["error_code"] == "TimeoutError"


def test_checkpoint_keys_segment_and_artifact_version_independently(
    tmp_path: Path,
) -> None:
    checkpoint = BatchCheckpoint(
        tmp_path,
        run_id="checkpoint-test",
        phase="generation",
    )
    for artifact_digest, segment_id in (
        ("artifact-v1", "segment-1"),
        ("artifact-v1", "segment-2"),
        ("artifact-v2", "segment-1"),
    ):
        checkpoint.append(
            {
                "subject_type": "comment",
                "subject_id": "COMMENT-1",
                "artifact_digest": artifact_digest,
                "segment_id": segment_id,
                "status": "zero_tags",
            }
        )

    assert len(checkpoint.records()) == 3
    cached = checkpoint.get(
        "comment",
        "COMMENT-1",
        artifact_digest="artifact-v1",
        segment_id="segment-2",
    )
    assert cached is not None
    assert cached["status"] == "zero_tags"
    assert checkpoint.get("comment", "COMMENT-1") is None


def test_openai_provider_cannot_ground_evidence_in_context_only() -> None:
    subject = Subject(
        subject_type="document",
        subject_id="DOC-CONTEXT",
        text="Clean water requirements.",
        fields={"documents.text_content": "Clean water requirements."},
        digest="segment-digest",
        artifact_digest="artifact-digest",
        segment_id="segment-context-test",
        context_fields={"artifact_title": "PFAS rule"},
    )

    class _Responses:
        def create(self, **kwargs):
            del kwargs
            return SimpleNamespace(
                status="completed",
                id="response-context",
                model="gpt-test",
                usage=SimpleNamespace(
                    input_tokens=10,
                    output_tokens=10,
                    total_tokens=20,
                ),
                output_text=json.dumps(
                    {
                        "tags": [
                            {
                                "concept_id": None,
                                "proposed_label": "PFAS",
                                "scheme": "subject",
                                "definition": "Rules concerning PFAS.",
                                "confidence": 0.9,
                                "evidence_text": "PFAS",
                                "evidence_field": "artifact_title",
                                "evidence_start": 0,
                                "evidence_end": 4,
                                "justification": "The title names PFAS.",
                                "external_ids": [],
                            }
                        ]
                    }
                ),
            )

    model = cast(Any, object.__new__(OpenAIOntologyModel))
    model.model = "gpt-test"
    model.model_id = "openai:gpt-test"
    model.timeout_seconds = 10
    model.max_retries = 0
    model._client = SimpleNamespace(responses=_Responses())

    assert model.tag(subject, []) == []
    assert model.last_tag_rejections == [
        {
            "reason": "ungrounded_evidence",
            "source_field": "artifact_title",
        }
    ]

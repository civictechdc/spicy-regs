"""Strict receipt tests for segment-backed OpenAI ontology runs."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.corpora.mixed_real_data import (
    build_openai_run_receipt,
)
from spicy_regs.ontology.common import (
    RunContext,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import CONCEPT_COLUMNS, seed_concept
from spicy_regs.ontology.ledger import (
    SEGMENT_LEDGER_COLUMNS,
)
from spicy_regs.ontology.llm import TagProposal, ValidationProposal
from spicy_regs.transforms.build_concept_assignments import (
    build_concept_assignments,
)

_RUN_ID = "strict-openai-receipt-test"
_ASSERTED_AT = "2026-07-24T12:00:00Z"


class _ReceiptModel:
    model_id = "openai:gpt-test"
    production_provider = True

    def __init__(self, concept_id: str) -> None:
        self.concept_id = concept_id
        self.call_count = 0
        self.last_call_metadata: dict[str, object] | None = None
        self.last_tag_rejections: list[dict[str, str]] = []

    def _record_call(self) -> None:
        self.call_count += 1
        response_id = f"response-{self.call_count}"
        self.last_call_metadata = {
            "response_id": response_id,
            "response_model": "gpt-test",
            "status": "completed",
            "duration_ms": 10.0,
            "input_tokens": 100,
            "output_tokens": 10,
            "total_tokens": 110,
            "attempt_count": 1,
            "retry_count": 0,
            "attempts": [
                {
                    "attempt": 1,
                    "status": "completed",
                    "duration_ms": 10.0,
                    "response_id": response_id,
                    "input_tokens": 100,
                    "output_tokens": 10,
                    "total_tokens": 110,
                }
            ],
            "prompt_sha256": "a" * 64,
            "request_sha256": "b" * 64,
            "prompt_token_estimate": 500,
            "prompt_input_token_budget": 8_192,
            "prompt_safety_margin_tokens": 1_024,
            "tokenizer": "o200k_base",
            "tokenizer_version": "0.13.0",
            "max_output_tokens": 4_096,
            "reasoning_effort": "medium",
            "store": False,
            "timeout_seconds": 120.0,
            "max_retries": 3,
            "sdk_max_retries": 0,
        }

    def tag(self, subject, concepts):
        del concepts
        self._record_call()
        field, text = next(iter(subject.fields.items()))
        evidence = "PFAS"
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
                justification="The exact segment span names PFAS.",
            )
        ]

    def validate(self, *, subject, concept, assignment):
        del subject, concept, assignment
        self._record_call()
        return ValidationProposal(
            agrees=True,
            confidence=0.95,
            rationale="The exact source span supports the concept.",
        )


def _build_receipted_run(root: Path) -> dict:
    context = RunContext(_RUN_ID, _ASSERTED_AT)
    concept = seed_concept({"name": "PFAS"}, context)
    assert concept is not None
    write_parquet_rows(
        root / "concepts.parquet",
        columns=CONCEPT_COLUMNS,
        rows=[concept],
    )
    body = "\n\n".join(
        f"PFAS section {index}. "
        + ("Source-backed water policy evidence. " * 100)
        for index in range(20)
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
                "comment_id": "COMMENT-STRICT-1",
                "title": "PFAS source comment",
                "comment": body,
                "text_content": None,
                "organization": "Public commenter",
                "category": "Public Submission",
            }
        ],
    )
    write_parquet_rows(
        root / "documents.parquet",
        columns=("document_id", "fr_doc_num", "title"),
        rows=[],
    )
    write_parquet_rows(
        root / "federal_register.parquet",
        columns=(
            "document_number",
            "topics_json",
            "title",
            "abstract",
            "document_type",
            "agency_slugs",
        ),
        rows=[],
    )
    model = _ReceiptModel(str(concept["concept_id"]))
    build_concept_assignments(
        root,
        model=model,
        run_id=_RUN_ID,
        asserted_at=_ASSERTED_AT,
        generation_limit=1,
        validation_percent=100,
    )
    (root / "ontology-receipt.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "snapshot_id": "snapshot-test",
            }
        ),
        encoding="utf-8",
    )
    return build_openai_run_receipt(
        root,
        minimum_f1=0,
        require_call_metadata=True,
    )


def test_strict_openai_receipt_counts_artifacts_segments_and_attempts(
    tmp_path: Path,
) -> None:
    receipt = _build_receipted_run(tmp_path)

    assert receipt["status"] == "pass"
    assert receipt["format_version"] == 2
    assert receipt["generation_artifacts"] == 1
    assert receipt["generation_segments"] > 1
    assert receipt["generation_assignments"] == 1
    assert (
        receipt["raw_segment_assignments"]
        == receipt["generation_segments"]
    )
    assert (
        receipt["provider_usage"]["physical_attempts"]
        == receipt["generation_segments"]
        + receipt["validation_calls"]
    )
    assert receipt["provider_usage"]["retries"] == 0
    assert receipt["validation_span_coverage"] == 1.0
    assert receipt["grounding_failure_count"] == 0
    assert receipt["api_key_persisted"] is False


def test_strict_openai_receipt_rejects_missing_segment_ledger_row(
    tmp_path: Path,
) -> None:
    assert _build_receipted_run(tmp_path)["status"] == "pass"
    ledger_path = tmp_path / "ontology_segment_ledger.parquet"
    rows = pq.read_table(ledger_path).to_pylist()
    write_parquet_rows(
        ledger_path,
        columns=SEGMENT_LEDGER_COLUMNS,
        rows=rows[1:],
    )

    receipt = build_openai_run_receipt(
        tmp_path,
        minimum_f1=0,
        require_call_metadata=True,
    )

    assert receipt["status"] == "fail"
    assert any(
        "segments lack ledger rows" in failure
        for failure in receipt["failures"]
    )


def test_strict_openai_receipt_rejects_payload_only_prompt_hash(
    tmp_path: Path,
) -> None:
    assert _build_receipted_run(tmp_path)["status"] == "pass"
    checkpoint_path = (
        tmp_path
        / ".ontology-checkpoints"
        / f"{_RUN_ID}-assignment-generation.jsonl"
    )
    rows = [
        json.loads(line)
        for line in checkpoint_path.read_text(encoding="utf-8").splitlines()
    ]
    for row in rows:
        metadata = row["model_call"]
        metadata.pop("request_sha256", None)
    checkpoint_path.write_text(
        "".join(
            json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )

    receipt = build_openai_run_receipt(
        tmp_path,
        minimum_f1=0,
        require_call_metadata=True,
    )

    assert receipt["status"] == "fail"
    assert any(
        "model-call telemetry rows are invalid" in failure
        for failure in receipt["failures"]
    )

"""Adversarial parent-side validation of contained parser results."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.adapters.docling import (
    ADAPTER_MAPPING_REVISION,
    DOCLING_CORE_VERSION,
    DOCLING_VERSION,
)
from spicy_regs.docpipeline.source import (
    GATE_COMPLETED,
    GATE_MALFORMED_RESULT,
    ProcessGateLimits,
    run_contained_parse,
)

CONTENT = b"PK\x03\x04worker-boundary"
SOURCE_NAME = "rule.docx"
MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def _command(record: dict[str, Any], *, stderr_bytes: int = 0) -> Callable[[Path], Sequence[str]]:
    encoded = json.dumps(record, ensure_ascii=False, sort_keys=True)
    script = (
        "import json,pathlib,sys\n"
        "job=json.loads(pathlib.Path(sys.argv[1]).read_text())\n"
        f"sys.stderr.write('x' * {stderr_bytes})\n"
        f"pathlib.Path(job['result_path']).write_text({encoded!r})\n"
    )

    def build(job_path: Path) -> Sequence[str]:
        return (sys.executable, "-I", "-c", script, str(job_path))

    return build


def _record() -> dict[str, Any]:
    text = "Title\n\nBody"
    digest = hashlib.sha256(CONTENT).hexdigest()
    policy = {"mapping_revision": ADAPTER_MAPPING_REVISION}
    policy_digest = hashlib.sha256(
        json.dumps(policy, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    parser_id = (
        f"docling:{DOCLING_VERSION}:docling-core:{DOCLING_CORE_VERSION}:{ADAPTER_MAPPING_REVISION}:{policy_digest[:16]}"
    )
    offsets = {
        "target": "adapter-parsed-text",
        "unit": "unicode-codepoints",
        "interval": "half-open",
    }
    return {
        "status": "completed",
        "call": {
            "provider": "docling",
            "operation": "document-parse",
            "parser_id": parser_id,
            "policy": policy,
            "policy_digest": policy_digest,
            "status": "completed",
            "input_format": "docx",
            "source_sha256": digest,
            "source_bytes": len(CONTENT),
            "source_name_sha256": hashlib.sha256(SOURCE_NAME.encode()).hexdigest(),
            "media_type": MEDIA_TYPE,
            "evidence_grade": "parser-derived",
            "offsets": offsets,
            "element_count": 2,
            "usable_element_count": 2,
            "usable_character_count": 9,
            "character_count": len(text),
            "content_layers_present": ["body"],
            "coordinate_grade": "none",
        },
        "document": {
            "text": text,
            "input_format": "docx",
            "source_sha256": digest,
            "source_bytes": len(CONTENT),
            "evidence_grade": "parser-derived",
            "offsets": offsets,
            "elements": [
                {
                    "ordinal": 0,
                    "kind": "title",
                    "text": "Title",
                    "start_char": 0,
                    "end_char": 5,
                    "content_layer": "body",
                    "coordinate_grade": "none",
                    "text_usable": True,
                    "heading_path": [],
                },
                {
                    "ordinal": 1,
                    "kind": "text",
                    "text": "Body",
                    "start_char": 7,
                    "end_char": 11,
                    "content_layer": "body",
                    "coordinate_grade": "none",
                    "text_usable": True,
                    "heading_path": ["Title"],
                },
            ],
        },
    }


def _mutated(case: str) -> dict[str, Any]:
    record = copy.deepcopy(_record())
    call = record["call"]
    document = record["document"]
    elements = document["elements"]
    if case == "document_source_sha256":
        document["source_sha256"] = "0" * 64
    elif case == "document_source_bytes":
        document["source_bytes"] += 1
    elif case == "call_source_sha256":
        call["source_sha256"] = "0" * 64
    elif case == "call_source_bytes":
        call["source_bytes"] += 1
    elif case == "document_input_format":
        document["input_format"] = "pptx"
    elif case == "call_input_format":
        call["input_format"] = "pptx"
    elif case == "content_layer":
        elements[0]["content_layer"] = "secret-layer"
    elif case == "coordinate_grade":
        elements[0]["coordinate_grade"] = "invented"
    elif case == "ordinal":
        elements[1]["ordinal"] = 7
    elif case == "order":
        document["elements"] = list(reversed(elements))
    elif case == "span":
        elements[0]["end_char"] = 99
    elif case == "text_round_trip":
        elements[0]["text"] = "Other"
    elif case == "text_usable":
        elements[0]["text_usable"] = False
    elif case == "completed_empty":
        document["text"] = ""
        document["elements"] = []
        call["element_count"] = 0
        call["usable_element_count"] = 0
        call["usable_character_count"] = 0
        call["character_count"] = 0
    elif case == "parser_identity":
        call["parser_id"] = "docling:another-mapping"
    elif case == "call_status":
        call["status"] = "failed"
    elif case == "call_provider":
        call["provider"] = "other"
    elif case == "call_element_count":
        call["element_count"] = 3
    elif case == "call_content_layers":
        call["content_layers_present"] = ["body", "notes"]
    else:  # pragma: no cover - the parameter list is closed below
        raise AssertionError(case)
    return record


@pytest.mark.parametrize(
    "case",
    [
        "document_source_sha256",
        "document_source_bytes",
        "call_source_sha256",
        "call_source_bytes",
        "document_input_format",
        "call_input_format",
        "content_layer",
        "coordinate_grade",
        "ordinal",
        "order",
        "span",
        "text_round_trip",
        "text_usable",
        "completed_empty",
        "parser_identity",
        "call_status",
        "call_provider",
        "call_element_count",
        "call_content_layers",
    ],
)
def test_every_forged_worker_semantic_is_one_malformed_result(case: str) -> None:
    result = run_contained_parse(
        CONTENT,
        source_name=SOURCE_NAME,
        media_type=MEDIA_TYPE,
        limits=ProcessGateLimits(wall_timeout_seconds=2, terminate_grace_seconds=0.2),
        worker_command=_command(_mutated(case)),
    )

    assert result.receipt.classification == GATE_MALFORMED_RESULT
    assert result.parsed is None
    assert result.call is None


def test_a_semantically_valid_worker_result_is_accepted() -> None:
    result = run_contained_parse(
        CONTENT,
        source_name=SOURCE_NAME,
        media_type=MEDIA_TYPE,
        limits=ProcessGateLimits(wall_timeout_seconds=2, terminate_grace_seconds=0.2),
        worker_command=_command(_record()),
    )

    assert result.receipt.classification == GATE_COMPLETED
    assert result.parsed is not None
    assert result.call is not None


def test_stderr_is_observed_without_becoming_a_success_gate() -> None:
    result = run_contained_parse(
        CONTENT,
        source_name=SOURCE_NAME,
        media_type=MEDIA_TYPE,
        limits=ProcessGateLimits(
            wall_timeout_seconds=2,
            terminate_grace_seconds=0.2,
            max_stderr_bytes=8,
        ),
        worker_command=_command(_record(), stderr_bytes=100),
    )

    assert result.receipt.classification == GATE_COMPLETED
    assert result.receipt.stderr_bytes == 100
    assert result.receipt.stderr_over_limit is True
    assert result.parsed is not None

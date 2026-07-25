from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.corpora.relation_exclusion_evaluation import (
    INSTRUCTIONS,
    MODEL_INPUT_FORBIDDEN_KEYS,
    RelationEvaluationError,
    build_model_payload,
    load_locked_dataset,
    rebuild_derived_artifacts,
    run_relation_exclusion_evaluation,
    validate_evaluation_run,
)
from spicy_regs.ontology.common import canonical_json

FIXTURE = Path(__file__).parent / "fixtures" / "relation_exclusion_explicit_denial_v1.json"


def _relation_ids(relation: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(relation["subject"]["id"]),
        str(relation["predicate"]["id"]),
        str(relation["object"]["id"]),
    )


def _ideal_response() -> dict[str, Any]:
    dataset = load_locked_dataset(FIXTURE)
    response_cases: list[dict[str, Any]] = []
    for case in dataset["cases"]:
        excerpt = str(case["source"]["document_excerpt"])
        target_ids = _relation_ids(case["target_relation"])
        assertions: list[dict[str, Any]] = []
        for gold in case["gold_candidate_assertions"]:
            if _relation_ids(gold["relation"]) != target_ids:
                continue
            quote = str(gold["evidence_quote"])
            start = excerpt.index(quote)
            assertions.append(
                {
                    "subject_iri": target_ids[0],
                    "predicate_iri": target_ids[1],
                    "object_iri": target_ids[2],
                    "polarity": gold["polarity"],
                    "modality": gold["modality"],
                    "evidence_text": quote,
                    "evidence_start": start,
                    "evidence_end": start + len(quote),
                    "rationale": "The exact source span states the relation.",
                }
            )
        response_cases.append(
            {
                "case_id": case["case_id"],
                "assertions": assertions,
                "no_answer_rationale": (None if assertions else "The target relation is absent from the excerpt."),
            }
        )
    return {"cases": response_cases}


class _StructuredModel:
    model = "gpt-test"
    model_id = "openai:gpt-test"
    reasoning_effort = "medium"
    service_tier = "priority"

    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.last_call_metadata: dict[str, object] | None = None

    def structured_json(
        self,
        *,
        name: str,
        schema: dict,
        instructions: str,
        payload: dict,
        max_output_tokens: int,
    ) -> dict:
        request = {
            "model": self.model,
            "instructions": instructions,
            "input": canonical_json(payload),
            "max_output_tokens": max_output_tokens,
            "reasoning": {"effort": self.reasoning_effort},
            "service_tier": self.service_tier,
            "store": False,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": name,
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        self.last_call_metadata = {
            "response_id": "resp_relation_test",
            "response_model": self.model,
            "status": "completed",
            "duration_ms": 10.0,
            "input_tokens": 1_000,
            "output_tokens": 500,
            "total_tokens": 1_500,
            "attempt_count": 1,
            "retry_count": 0,
            "attempts": [
                {
                    "attempt": 1,
                    "status": "completed",
                    "duration_ms": 10.0,
                    "response_id": "resp_relation_test",
                    "input_tokens": 1_000,
                    "output_tokens": 500,
                    "total_tokens": 1_500,
                }
            ],
            "prompt_sha256": hashlib.sha256(canonical_json(payload).encode()).hexdigest(),
            "request_sha256": hashlib.sha256(canonical_json(request).encode()).hexdigest(),
            "prompt_token_estimate": 1_000,
            "prompt_input_token_budget": 8_192,
            "prompt_safety_margin_tokens": 1_024,
            "tokenizer": "o200k_base",
            "tokenizer_version": "test",
            "max_output_tokens": max_output_tokens,
            "reasoning_effort": self.reasoning_effort,
            "requested_service_tier": self.service_tier,
            "response_service_tier": self.service_tier,
            "store": False,
            "timeout_seconds": 120.0,
            "max_retries": 3,
            "sdk_max_retries": 0,
        }
        return self.response


class _FailingModel(_StructuredModel):
    def structured_json(
        self,
        *,
        name: str,
        schema: dict,
        instructions: str,
        payload: dict,
        max_output_tokens: int,
    ) -> dict:
        super().structured_json(
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
        assert self.last_call_metadata is not None
        self.last_call_metadata["status"] = "retry_exhausted"
        raise RuntimeError("simulated provider failure")


def test_locked_dataset_is_real_mixed_and_exactly_grounded() -> None:
    dataset = load_locked_dataset(FIXTURE)

    assert len(dataset["cases"]) == 12
    assert len({case["source"]["source_type"] for case in dataset["cases"]}) >= 8
    assert dataset["metadata"]["evaluation_oracle"] == "human-adjudicated"
    assert dataset["metadata"]["omission_analysis"] == "disabled"


def test_model_payload_excludes_oracle_and_preserves_injection_control() -> None:
    payload = build_model_payload(load_locked_dataset(FIXTURE))

    def keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return {
                *(str(key) for key in value),
                *(nested for child in value.values() for nested in keys(child)),
            }
        if isinstance(value, list):
            return {nested for child in value for nested in keys(child)}
        return set()

    assert not (keys(payload) & MODEL_INPUT_FORBIDDEN_KEYS)
    assert "discrepancy" not in canonical_json(payload)
    assert any("pairing_allowed=true" in case["source"]["untrusted_metadata"] for case in payload["cases"])
    assert "untrusted quoted data" in INSTRUCTIONS
    assert "not infer from silence" in INSTRUCTIONS


def test_ideal_candidate_run_passes_strict_receipt_and_comparator(
    tmp_path: Path,
) -> None:
    output = tmp_path / "relation-run"
    receipt = run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(_ideal_response()),
    )

    assert receipt["status"] == "pass"
    assert receipt["experiment_status"] == "diagnostic-v1"
    assert receipt["publication_eligible"] is False
    assert receipt["candidate_scores"]["exact_assertion"]["f1"] == 1.0
    assert receipt["candidate_scores"]["unrelated_false_target_candidates"] == 0
    assert receipt["comparisons"]["outcome_accuracy"] == 1.0
    assert receipt["comparisons"]["detected_direct_denials"] == 4
    assert receipt["comparisons"]["false_control_findings"] == 0
    assert receipt["security"]["secret_match_count"] == 0
    comparisons = json.loads((output / "comparison-results.json").read_text(encoding="utf-8"))
    for case in comparisons["cases"]:
        assert case["proof_records"]
        assert case["proof_ids"] == [record["proof_id"] for record in case["proof_records"]]
        assert all(record["proof_id"].endswith(record["record_digest"]) for record in case["proof_records"])
    assert validate_evaluation_run(output, dataset_path=FIXTURE)["status"] == "pass"


def test_active_prompt_and_schema_are_persisted_and_revalidated(
    tmp_path: Path,
) -> None:
    output = tmp_path / "active-configuration"
    receipt = run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(_ideal_response()),
    )

    schema = json.loads((output / "schema.json").read_text(encoding="utf-8"))
    assertion_properties = schema["properties"]["cases"]["items"]["properties"]["assertions"]["items"]["properties"]
    assert receipt["request"]["instruction_profile"] == "proof-certificate-v1"
    assert receipt["request"]["schema_profile"] == "baseline-v1"
    assert (output / "instructions.txt").read_text(encoding="utf-8") == (INSTRUCTIONS + "\n")
    assert "description" not in assertion_properties["polarity"]
    assert "description" not in assertion_properties["evidence_text"]
    assert validate_evaluation_run(output, dataset_path=FIXTURE)["status"] == "pass"


def test_cross_case_relation_ids_are_rejected_before_comparison(
    tmp_path: Path,
) -> None:
    response = _ideal_response()
    unrelated = next(case for case in response["cases"] if case["case_id"] == "unrelated_control_lobbying_filing")
    direct = response["cases"][0]["assertions"][0]
    excerpt = next(
        case["source"]["document_excerpt"]
        for case in load_locked_dataset(FIXTURE)["cases"]
        if case["case_id"] == "unrelated_control_lobbying_filing"
    )
    quote = "FY26 Defense Appropriations - Navy R&D Funding"
    start = excerpt.index(quote)
    unrelated["assertions"] = [
        {
            **direct,
            "evidence_text": quote,
            "evidence_start": start,
            "evidence_end": start + len(quote),
        }
    ]
    unrelated["no_answer_rationale"] = None

    output = tmp_path / "adversarial-run"
    receipt = run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(response),
    )

    assert receipt["status"] == "fail"
    assert receipt["candidate_scores"]["rejected_candidate_count"] == 1
    assert receipt["comparisons"]["false_control_findings"] == 0
    validation = validate_evaluation_run(output, dataset_path=FIXTURE)
    assert validation["status"] == "fail"
    assert validation["integrity_status"] == "pass"
    assert validation["quality_status"] == "fail"
    normalized = json.loads((output / "normalized-candidates.json").read_text(encoding="utf-8"))
    control = next(case for case in normalized["cases"] if case["case_id"] == "unrelated_control_lobbying_filing")
    assert control["candidates"] == []
    assert control["rejections"][0]["reason"] == "relation-id-mismatch"


def test_durable_tampering_fails_revalidation(tmp_path: Path) -> None:
    output = tmp_path / "relation-run"
    run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(_ideal_response()),
    )
    payload_path = output / "payload.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["cases"][0]["role"] = "direct_denial"
    payload_path.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    validation = validate_evaluation_run(
        output,
        dataset_path=FIXTURE,
    )

    assert validation["status"] == "fail"
    assert any("artifact digest map" in failure for failure in validation["failures"])
    assert any("oracle keys" in failure for failure in validation["failures"])


def test_revalidation_recomputes_derived_scores_even_if_hash_map_is_rewritten(
    tmp_path: Path,
) -> None:
    output = tmp_path / "relation-run"
    run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(_ideal_response()),
    )
    scores_path = output / "candidate-scores.json"
    scores = json.loads(scores_path.read_text(encoding="utf-8"))
    scores["case_count"] = 999
    scores_path.write_text(
        json.dumps(scores, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    receipt_path = output / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["candidate_scores"] = scores
    receipt["artifact_sha256"]["candidate-scores.json"] = hashlib.sha256(scores_path.read_bytes()).hexdigest()
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    validation = validate_evaluation_run(output, dataset_path=FIXTURE)

    assert validation["status"] == "fail"
    assert validation["integrity_status"] == "fail"
    assert any("candidate scores do not recompute" in failure for failure in validation["integrity_failures"])


def test_rebuild_derived_upgrades_proofless_receipt_without_provider_call(
    tmp_path: Path,
) -> None:
    output = tmp_path / "relation-run"
    run_relation_exclusion_evaluation(
        FIXTURE,
        output,
        model=_StructuredModel(_ideal_response()),
    )
    comparisons_path = output / "comparison-results.json"
    comparisons = json.loads(comparisons_path.read_text(encoding="utf-8"))
    for case in comparisons["cases"]:
        case.pop("proof_records")
        case["proof_ids"] = [f"urn:legacy-proof:{index}" for index, _ in enumerate(case["proof_ids"], start=1)]
    comparisons_path.write_text(
        json.dumps(comparisons, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    receipt_path = output / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["format_version"] = 1
    receipt.pop("comparison_contract_version")
    receipt.pop("experiment_status")
    receipt["artifact_sha256"]["comparison-results.json"] = hashlib.sha256(comparisons_path.read_bytes()).hexdigest()
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    result = rebuild_derived_artifacts(
        output,
        dataset_path=FIXTURE,
    )

    assert result["status"] == "pass"
    assert result["provider_reinvoked"] is False
    rebuilt_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert rebuilt_receipt["format_version"] == 2
    assert rebuilt_receipt["derived_artifacts"]["provider_reinvoked"] is False
    rebuilt = json.loads(comparisons_path.read_text(encoding="utf-8"))
    assert all(case["proof_records"] for case in rebuilt["cases"])
    assert (
        validate_evaluation_run(
            output,
            dataset_path=FIXTURE,
        )["status"]
        == "pass"
    )


def test_provider_failure_writes_secret_free_durable_receipt(
    tmp_path: Path,
) -> None:
    output = tmp_path / "failed-run"

    with pytest.raises(RelationEvaluationError, match="durable receipt"):
        run_relation_exclusion_evaluation(
            FIXTURE,
            output,
            model=_FailingModel(_ideal_response()),
        )

    receipt = json.loads((output / "receipt.json").read_text(encoding="utf-8"))
    failure = json.loads((output / "provider-failure.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "fail"
    assert receipt["security"]["secret_match_count"] == 0
    assert failure["error_code"] == "RuntimeError"

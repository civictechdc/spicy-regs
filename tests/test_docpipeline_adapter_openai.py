"""Tests for the v3 OpenAI structured-text-model adapter.

The adapter implements the "Structured text model" interface from the v3
design: instructions, strict JSON schema, source payload, and an output token
limit go in; checked JSON and secret-free call details come back together in
one immutable result. V3 removes the mutable ``last_call_metadata`` channel, so
every test reads the returned result or the raised error, never the adapter.
"""

from __future__ import annotations

import dataclasses
import json
import re
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from spicy_regs.docpipeline.adapters import (
    SHARED_CALL_DETAIL_KEYS,
    StructuredTextModel,
    StructuredTextResult,
)
from spicy_regs.docpipeline.adapters.openai import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    PROMPT_INPUT_TOKEN_BUDGET,
    PROMPT_SAFETY_MARGIN_TOKENS,
    IncompleteStructuredResponseError,
    InvalidOutputSchemaError,
    OpenAIProviderCallError,
    OpenAIProviderExhaustedError,
    OpenAIStructuredTextModel,
    PromptBudgetExceededError,
    StructuredOutputSchemaError,
)

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}
INSTRUCTIONS = "Answer the supplied payload. Return only schema-valid JSON."
PAYLOAD: dict[str, Any] = {"question": "test"}
FAKE_API_KEY = "sk-proj-000000000000000000000000FAKEKEYFORTESTS"
SECRET_PATTERN = re.compile(r"sk-(proj-)?[A-Za-z0-9_-]{8,}")
INVALID_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "not-a-json-schema-type"}},
}


def _wide_schema(fields: int) -> dict[str, Any]:
    """A valid schema whose serialized form alone is expensive to send."""
    return {
        "type": "object",
        "properties": {f"field_{index}": {"type": "string"} for index in range(fields)},
        "required": [f"field_{index}" for index in range(fields)],
        "additionalProperties": False,
    }


class _FakeResponses:
    """Record every physical request and replay scripted provider outcomes."""

    def __init__(self, outcomes: list[BaseException | SimpleNamespace]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _ReconfiguringResponses(_FakeResponses):
    """Change the adapter's effort setting after the request has been built."""

    def __init__(self, outcomes: list[BaseException | SimpleNamespace]) -> None:
        super().__init__(outcomes)
        self.model: OpenAIStructuredTextModel | None = None

    def create(self, **kwargs: Any) -> SimpleNamespace:
        response = super().create(**kwargs)
        if self.model is not None:
            self.model.reasoning_effort = "xhigh"
        return response


def _response(
    *,
    output_text: str | None = None,
    status: str = "completed",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=f"response-{status}",
        model="gpt-test",
        status=status,
        service_tier="default",
        usage=SimpleNamespace(input_tokens=100, output_tokens=10, total_tokens=110),
        output_text=(json.dumps({"answer": "ok"}) if output_text is None else output_text),
    )


def _model(
    outcomes: list[BaseException | SimpleNamespace],
    *,
    max_retries: int = 0,
    prompt_input_token_budget: int = 8_192,
    prompt_safety_margin_tokens: int = 1_024,
) -> tuple[OpenAIStructuredTextModel, _FakeResponses]:
    responses = _FakeResponses(outcomes)
    model = OpenAIStructuredTextModel(
        api_key=FAKE_API_KEY,
        client=SimpleNamespace(responses=responses),
        model="gpt-test",
        max_retries=max_retries,
        retry_base_seconds=0.0,
        timeout_seconds=10.0,
        prompt_input_token_budget=prompt_input_token_budget,
        prompt_safety_margin_tokens=prompt_safety_margin_tokens,
    )
    return model, responses


def _call(model: OpenAIStructuredTextModel, **overrides: Any) -> StructuredTextResult:
    request = {
        "name": "test_answer",
        "schema": SCHEMA,
        "instructions": INSTRUCTIONS,
        "payload": PAYLOAD,
        "max_output_tokens": 256,
    }
    request.update(overrides)
    return model.structured_json(**request)  # type: ignore[arg-type]


class _RateLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-rate-limit"


class _QuotaLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-quota-limit"
    body = {"error": {"code": "insufficient_quota", "type": "insufficient_quota"}}


def test_success_returns_checked_output_and_call_details_together() -> None:
    model, responses = _model([_response()])

    result = _call(model)

    assert isinstance(result, StructuredTextResult)
    assert result.output == {"answer": "ok"}
    assert result.call["provider"] == "openai"
    assert result.call["model_id"] == "openai:gpt-test"
    assert result.call["status"] == "completed"
    assert result.call["schema_validated_locally"] is True
    assert result.call["attempt_count"] == 1
    assert result.call["retry_count"] == 0
    assert result.call["input_tokens"] == 100
    assert result.call["output_tokens"] == 10
    assert result.call["total_tokens"] == 110
    assert result.call["tokenizer"] == "o200k_base"
    assert result.call["tokenizer_version"]
    assert len(str(result.call["prompt_sha256"])) == 64
    assert len(str(result.call["request_sha256"])) == 64
    assert isinstance(result.call["duration_ms"], float)
    assert result.call["sdk_max_retries"] == 0
    assert result.call["store"] is False
    assert len(responses.calls) == 1


def test_result_is_immutable_and_adapter_keeps_no_last_call_state() -> None:
    model, _ = _model([_response(), _response(output_text=json.dumps({"answer": "two"}))])

    first = _call(model)
    second = _call(model, payload={"question": "second"})

    with pytest.raises(dataclasses.FrozenInstanceError):
        first.output = {"answer": "mutated"}  # ty: ignore[invalid-assignment]
    assert not hasattr(model, "last_call_metadata")
    assert not any("last_call" in name for name in dir(model))
    assert first.output == {"answer": "ok"}
    assert second.output == {"answer": "two"}
    assert first.call["prompt_sha256"] != second.call["prompt_sha256"]


def test_request_declares_strict_schema_payload_and_output_limit() -> None:
    model, responses = _model([_response()])

    _call(model, max_output_tokens=512)

    sent = responses.calls[0]
    assert sent["model"] == "gpt-test"
    assert sent["instructions"] == INSTRUCTIONS
    assert json.loads(sent["input"]) == PAYLOAD
    assert sent["max_output_tokens"] == 512
    assert sent["store"] is False
    assert sent["reasoning"] == {"effort": "medium"}
    assert sent["service_tier"] == "auto"
    assert sent["text"]["format"] == {
        "type": "json_schema",
        "name": "test_answer",
        "strict": True,
        "schema": SCHEMA,
    }
    assert model.run_configuration["sdk_max_retries"] == 0
    assert model.secret_free_request(
        name="test_answer",
        schema=SCHEMA,
        instructions=INSTRUCTIONS,
        payload=PAYLOAD,
        max_output_tokens=512,
    ) == sent


def test_module_defaults_are_pinned_to_the_ontology_provider_values() -> None:
    assert PROMPT_INPUT_TOKEN_BUDGET == 8_192
    assert PROMPT_SAFETY_MARGIN_TOKENS == 1_024
    assert DEFAULT_MAX_RETRIES == 3
    assert DEFAULT_TIMEOUT_SECONDS == 120.0


def test_call_details_record_the_reasoning_effort_actually_sent() -> None:
    """The receipt describes the request that went out, not a later setting."""
    responses = _ReconfiguringResponses([_response()])
    model = OpenAIStructuredTextModel(
        api_key=FAKE_API_KEY,
        client=SimpleNamespace(responses=responses),
        model="gpt-test",
        max_retries=0,
        retry_base_seconds=0.0,
        timeout_seconds=10.0,
    )
    responses.model = model

    result = _call(model)

    sent = responses.calls[0]
    assert sent["reasoning"] == {"effort": "medium"}
    assert model.reasoning_effort == "xhigh"
    assert result.call["reasoning_effort"] == "medium"


def test_schema_violating_response_fails_with_safe_failure_metadata() -> None:
    model, responses = _model([_response(output_text=json.dumps({"wrong": "shape"}))])

    with pytest.raises(StructuredOutputSchemaError) as failure:
        _call(model)

    assert failure.value.call["status"] == "failed"
    assert failure.value.call["schema_validated_locally"] is False
    assert failure.value.call["attempt_count"] == 1
    assert failure.value.call["retry_count"] == 0
    assert len(responses.calls) == 1


@pytest.mark.parametrize(
    "first_outcome,expected_error",
    [
        (TimeoutError("controlled timeout"), "TimeoutError"),
        (_RateLimitEquivalent("controlled rate limit"), "_RateLimitEquivalent"),
        (_response(output_text="{"), "IncompleteStructuredResponseError"),
        (_response(status="incomplete"), "IncompleteStructuredResponseError"),
    ],
)
def test_retryable_failures_are_retried_and_reported(
    first_outcome: BaseException | SimpleNamespace,
    expected_error: str,
) -> None:
    model, responses = _model([first_outcome, _response()], max_retries=1)

    result = _call(model)

    assert result.output == {"answer": "ok"}
    assert len(responses.calls) == 2
    assert result.call["status"] == "completed"
    assert result.call["attempt_count"] == 2
    assert result.call["retry_count"] == 1
    attempts = result.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["error_code"] == expected_error
    assert attempts[1]["status"] == "completed"


def test_exhausted_retries_raise_the_exhaustion_error_with_call_details() -> None:
    model, responses = _model(
        [
            TimeoutError("controlled timeout one"),
            TimeoutError("controlled timeout two"),
            TimeoutError("controlled timeout three"),
        ],
        max_retries=2,
    )

    with pytest.raises(OpenAIProviderExhaustedError, match="exhausted 3 attempts") as failure:
        _call(model)

    assert len(responses.calls) == 3
    assert failure.value.call["status"] == "retry_exhausted"
    assert failure.value.call["attempt_count"] == 3
    assert failure.value.call["retry_count"] == 2
    assert isinstance(failure.value.__cause__, TimeoutError)


def test_non_retryable_provider_error_fails_immediately_with_call_details() -> None:
    model, responses = _model([_QuotaLimitEquivalent("controlled quota limit")], max_retries=3)

    with pytest.raises(OpenAIProviderCallError) as failure:
        _call(model)

    assert len(responses.calls) == 1
    assert failure.value.call["status"] == "failed"
    assert failure.value.call["attempt_count"] == 1
    attempts = failure.value.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["provider_error_code"] == "insufficient_quota"
    assert attempts[0]["status_code"] == 429
    assert isinstance(failure.value.__cause__, _QuotaLimitEquivalent)


def test_prompt_budget_exceeded_raises_before_any_provider_call() -> None:
    model, responses = _model(
        [_response()],
        prompt_input_token_budget=64,
        prompt_safety_margin_tokens=16,
    )

    with pytest.raises(PromptBudgetExceededError) as failure:
        _call(model, payload={"question": "budget " * 200})

    assert responses.calls == []
    assert failure.value.call["status"] == "prompt_budget_exceeded"
    assert failure.value.call["attempt_count"] == 0
    assert failure.value.call["attempts"] == []
    assert failure.value.call["prompt_input_token_budget"] == 64
    assert failure.value.call["prompt_safety_margin_tokens"] == 16
    assert int(failure.value.call["prompt_token_estimate"]) + 16 > 64


def test_output_schema_tokens_count_against_the_prompt_budget() -> None:
    """The schema ships with the request and is billed, so it must be counted."""
    model, responses = _model(
        [_response()],
        prompt_input_token_budget=512,
        prompt_safety_margin_tokens=16,
    )

    with pytest.raises(PromptBudgetExceededError) as failure:
        _call(model, schema=_wide_schema(200))

    assert responses.calls == []
    call = failure.value.call
    prompt_estimate = int(call["prompt_token_estimate"])
    schema_estimate = int(call["schema_token_estimate"])
    assert call["status"] == "prompt_budget_exceeded"
    assert prompt_estimate + 16 <= 512
    assert prompt_estimate + schema_estimate + 16 > 512


def test_success_call_details_report_the_schema_token_estimate() -> None:
    model, _ = _model([_response()])

    result = _call(model)

    assert int(result.call["schema_token_estimate"]) > 0
    assert int(result.call["prompt_token_estimate"]) > 0


def test_invalid_caller_schema_fails_locally_before_any_provider_call() -> None:
    model, responses = _model([_response()])

    with pytest.raises(InvalidOutputSchemaError) as failure:
        _call(model, schema=INVALID_SCHEMA)

    assert responses.calls == []
    assert failure.value.call["status"] == "invalid_output_schema"
    assert failure.value.call["attempt_count"] == 0
    assert failure.value.call["attempts"] == []
    assert failure.value.call["schema_validated_locally"] is False


def test_call_details_and_request_carry_no_api_key_material() -> None:
    model, _ = _model([_response()])

    result = _call(model)

    serialized_call = json.dumps(result.call, sort_keys=True, default=str)
    serialized_request = json.dumps(
        model.secret_free_request(
            name="test_answer",
            schema=SCHEMA,
            instructions=INSTRUCTIONS,
            payload=PAYLOAD,
            max_output_tokens=256,
        ),
        sort_keys=True,
    )
    serialized_configuration = json.dumps(model.run_configuration, sort_keys=True, default=str)

    assert SECRET_PATTERN.search(serialized_call) is None
    assert SECRET_PATTERN.search(serialized_request) is None
    assert SECRET_PATTERN.search(serialized_configuration) is None
    assert not any(isinstance(value, str) and value.startswith("sk-") for value in vars(model).values())


def test_failure_call_details_carry_no_api_key_material() -> None:
    class _AuthEquivalent(RuntimeError):
        status_code = 401
        body = {"error": {"code": "invalid_api_key"}}

    model, _ = _model([_AuthEquivalent(f"Incorrect API key provided: {FAKE_API_KEY}")])

    with pytest.raises(OpenAIProviderCallError) as failure:
        _call(model)

    serialized = json.dumps(failure.value.call, sort_keys=True, default=str)
    assert SECRET_PATTERN.search(serialized) is None
    assert FAKE_API_KEY not in str(failure.value)


def test_client_construction_disables_sdk_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_openai(**kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(responses=_FakeResponses([_response()]))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=_fake_openai))
    monkeypatch.setenv("OPENAI_API_KEY", FAKE_API_KEY)

    model = OpenAIStructuredTextModel.from_environment()

    assert model is not None
    assert captured["max_retries"] == 0
    assert captured["api_key"] == FAKE_API_KEY
    assert _call(model).output == {"answer": "ok"}


def test_from_environment_returns_none_without_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert OpenAIStructuredTextModel.from_environment() is None


def test_constructor_rejects_missing_credentials_and_bad_settings() -> None:
    with pytest.raises(ValueError, match="api_key"):
        OpenAIStructuredTextModel(model="gpt-test")
    with pytest.raises(ValueError, match="max_retries"):
        OpenAIStructuredTextModel(client=SimpleNamespace(), model="gpt-test", max_retries=-1)
    with pytest.raises(ValueError, match="reasoning_effort"):
        OpenAIStructuredTextModel(client=SimpleNamespace(), model="gpt-test", reasoning_effort="turbo")
    with pytest.raises(ValueError, match="service_tier"):
        OpenAIStructuredTextModel(client=SimpleNamespace(), model="gpt-test", service_tier="platinum")


def test_every_success_and_failure_path_emits_the_shared_call_detail_keys() -> None:
    """A receipt reader must not need to know which arm or path produced it."""
    calls: list[dict[str, Any]] = []

    success_model, _ = _model([_response()])
    calls.append(_call(success_model).call)

    schema_model, _ = _model([_response(output_text=json.dumps({"wrong": "shape"}))])
    with pytest.raises(StructuredOutputSchemaError) as schema_failure:
        _call(schema_model)
    calls.append(schema_failure.value.call)

    quota_model, _ = _model([_QuotaLimitEquivalent("controlled quota limit")], max_retries=3)
    with pytest.raises(OpenAIProviderCallError) as quota_failure:
        _call(quota_model)
    calls.append(quota_failure.value.call)

    exhausted_model, _ = _model(
        [TimeoutError("controlled timeout one"), TimeoutError("controlled timeout two")],
        max_retries=1,
    )
    with pytest.raises(OpenAIProviderExhaustedError) as exhausted_failure:
        _call(exhausted_model)
    calls.append(exhausted_failure.value.call)

    budget_model, _ = _model(
        [_response()],
        prompt_input_token_budget=64,
        prompt_safety_margin_tokens=16,
    )
    with pytest.raises(PromptBudgetExceededError) as budget_failure:
        _call(budget_model, payload={"question": "budget " * 200})
    calls.append(budget_failure.value.call)

    invalid_model, _ = _model([_response()])
    with pytest.raises(InvalidOutputSchemaError) as invalid_failure:
        _call(invalid_model, schema=INVALID_SCHEMA)
    calls.append(invalid_failure.value.call)

    assert [call["status"] for call in calls] == [
        "completed",
        "failed",
        "failed",
        "retry_exhausted",
        "prompt_budget_exceeded",
        "invalid_output_schema",
    ]
    for call in calls:
        assert set(SHARED_CALL_DETAIL_KEYS) <= set(call)
        assert isinstance(call["schema_validated_locally"], bool)
    assert [call["schema_validated_locally"] for call in calls] == [
        True,
        False,
        False,
        False,
        False,
        False,
    ]


def test_adapter_satisfies_the_shared_structured_text_model_protocol() -> None:
    model, _ = _model([_response()])

    assert isinstance(model, StructuredTextModel)
    assert model.model_id == "openai:gpt-test"


def test_incomplete_response_error_is_retryable_and_typed() -> None:
    model, responses = _model([_response(output_text="")], max_retries=0)

    with pytest.raises(OpenAIProviderExhaustedError) as failure:
        _call(model)

    assert len(responses.calls) == 1
    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)
    assert failure.value.call["status"] == "retry_exhausted"

"""Tests for the v3 native Anthropic structured-text-model adapter.

This is the enforced-schema arm of the same interface the OpenAI, Codex, and
OpenAI-compatible arms implement: instructions, a strict JSON schema, a source
payload, and an output token limit go in; checked JSON and secret-free call
details come back together in one immutable result. Every test is hermetic — the
client is injected, and no test reaches a network, a credential, or provider
internals.

What this arm owes on top of interface parity, and therefore what is pinned
here: the request really carries the Messages API's native
``output_config.format`` json_schema (the mechanism the compat endpoint ignores),
an unenforceable schema is refused before any paid call, a response that fails
local validation is rejected rather than repaired, token counts come from the
provider and are not labeled estimates, and an unconfigured credential is a
refusal rather than a silent ``None``.
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
from spicy_regs.docpipeline.adapters.anthropic import (
    API_KEY_ENVIRONMENT_VARIABLE,
    SCHEMA_ENFORCEMENT,
    TOKEN_COUNT_METHOD,
    AnthropicProviderCallError,
    AnthropicProviderExhaustedError,
    AnthropicRefusalError,
    AnthropicStructuredTextModel,
    IncompleteStructuredResponseError,
    InvalidOutputSchemaError,
    OutputBudgetExceededError,
    PromptBudgetExceededError,
    ProviderConfigurationError,
    StructuredOutputSchemaError,
    TokenCountUnavailableError,
    UnenforceableOutputSchemaError,
    unenforceable_schema_findings,
)

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}
INSTRUCTIONS = "Answer the supplied payload. Return only schema-valid JSON."
PAYLOAD: dict[str, Any] = {"question": "test"}
MODEL = "claude-opus-5"
FAKE_API_KEY = "sk-ant-000000000000000000000000FAKEKEYFORTESTS"
SECRET_PATTERN = re.compile(r"sk-(ant-|proj-|or-)?[A-Za-z0-9_-]{8,}")
INVALID_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "not-a-json-schema-type"}},
    "additionalProperties": False,
}
#: Valid JSON Schema, but built from vocabulary the endpoint cannot enforce.
UNENFORCEABLE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "answer": {"type": "string", "minLength": 3},
        "score": {"type": "integer", "minimum": 0, "maximum": 10},
    },
    "required": ["answer", "score"],
    "additionalProperties": False,
}


class _FakeMessages:
    """Record every physical request and replay scripted provider outcomes."""

    def __init__(
        self,
        outcomes: list[BaseException | SimpleNamespace],
        *,
        input_tokens: int = 120,
        count_error: BaseException | None = None,
    ) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []
        self.counts: list[dict[str, Any]] = []
        self._input_tokens = input_tokens
        self._count_error = count_error

    def create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def count_tokens(self, **kwargs: Any) -> SimpleNamespace:
        self.counts.append(kwargs)
        if self._count_error is not None:
            raise self._count_error
        return SimpleNamespace(input_tokens=self._input_tokens)


def _client(messages: _FakeMessages) -> SimpleNamespace:
    return SimpleNamespace(messages=messages)


def _response(
    *,
    text: str | None = None,
    stop_reason: str = "end_turn",
    model: str = MODEL,
    stop_details: SimpleNamespace | None = None,
    blocks: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    content = (
        blocks
        if blocks is not None
        else [
            SimpleNamespace(
                type="text",
                text=(json.dumps({"answer": "ok"}) if text is None else text),
            )
        ]
    )
    return SimpleNamespace(
        id="msg_test",
        type="message",
        role="assistant",
        model=model,
        content=content,
        stop_reason=stop_reason,
        stop_details=stop_details,
        usage=SimpleNamespace(
            input_tokens=100,
            output_tokens=10,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )


def _model(
    outcomes: list[BaseException | SimpleNamespace],
    *,
    model: str = MODEL,
    max_retries: int = 0,
    prompt_input_token_budget: int = 8_192,
    prompt_safety_margin_tokens: int = 1_024,
    input_tokens: int = 120,
    count_error: BaseException | None = None,
    **overrides: Any,
) -> tuple[AnthropicStructuredTextModel, _FakeMessages]:
    messages = _FakeMessages(outcomes, input_tokens=input_tokens, count_error=count_error)
    built = AnthropicStructuredTextModel(
        model=model,
        client=_client(messages),
        max_retries=max_retries,
        retry_base_seconds=0.0,
        timeout_seconds=10.0,
        prompt_input_token_budget=prompt_input_token_budget,
        prompt_safety_margin_tokens=prompt_safety_margin_tokens,
        **overrides,
    )
    return built, messages


def _call(model: AnthropicStructuredTextModel, **overrides: Any) -> StructuredTextResult:
    request: dict[str, Any] = {
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
    type = "rate_limit_error"


class _BillingLimitEquivalent(RuntimeError):
    status_code = 400
    request_id = "request-billing-limit"
    body = {"type": "error", "error": {"type": "billing_error", "message": "credit balance too low"}}


class _BadRequestEquivalent(RuntimeError):
    status_code = 400
    request_id = "request-bad-request"
    body = {"type": "error", "error": {"type": "invalid_request_error", "message": "schema is not supported"}}


# ---------------------------------------------------------------------------
# the SDK surface this arm exists for
# ---------------------------------------------------------------------------


def test_the_installed_sdk_exposes_the_native_enforced_schema_surface() -> None:
    """The reason this arm exists must hold in the pinned SDK, not from memory."""
    from anthropic.types.json_output_format_param import JSONOutputFormatParam
    from anthropic.types.message_create_params import MessageCreateParamsBase
    from anthropic.types.output_config_param import OutputConfigParam

    assert "output_config" in MessageCreateParamsBase.__annotations__
    assert "format" in OutputConfigParam.__annotations__
    assert set(JSONOutputFormatParam.__annotations__) == {"schema", "type"}


def test_the_adapter_module_does_not_shadow_the_installed_sdk() -> None:
    """``adapters/anthropic.py`` must not resolve as the ``anthropic`` package."""
    import anthropic

    import spicy_regs.docpipeline.adapters.anthropic as arm

    assert anthropic is not arm
    assert hasattr(anthropic, "Anthropic")


def test_the_request_carries_the_native_output_config_json_schema() -> None:
    model, messages = _model([_response()])

    result = _call(model, max_output_tokens=512)

    sent = messages.calls[0]
    assert sent["model"] == MODEL
    assert sent["max_tokens"] == 512
    assert sent["system"] == INSTRUCTIONS
    assert sent["messages"] == [{"role": "user", "content": json.dumps(PAYLOAD, separators=(",", ":"))}]
    assert sent["output_config"] == {"format": {"type": "json_schema", "schema": SCHEMA}}
    # No compat vocabulary leaks into a native request.
    assert "response_format" not in sent
    assert result.output == {"answer": "ok"}
    assert result.call["schema_enforcement"] == SCHEMA_ENFORCEMENT
    assert result.call["schema_enforced_by_provider"] is True
    assert result.call["schema_validated_locally"] is True
    assert result.call["stop_reason"] == "end_turn"
    assert (
        model.secret_free_request(
            name="test_answer",
            schema=SCHEMA,
            instructions=INSTRUCTIONS,
            payload=PAYLOAD,
            max_output_tokens=512,
        )
        == sent
    )


def test_an_effort_level_rides_in_the_same_output_config() -> None:
    model, messages = _model([_response()], reasoning_effort="high")

    result = _call(model)

    assert messages.calls[0]["output_config"]["effort"] == "high"
    assert result.call["reasoning_effort"] == "high"


def test_max_output_tokens_is_the_transports_enforced_ceiling() -> None:
    model, messages = _model([_response()])

    call = _call(model, max_output_tokens=333).call

    assert messages.calls[0]["max_tokens"] == 333
    assert call["max_output_tokens"] == 333
    assert call["max_output_tokens_enforced"] is True


# ---------------------------------------------------------------------------
# schema enforceability: refused before any paid call
# ---------------------------------------------------------------------------


def test_unenforceable_schema_findings_name_the_offending_pointers() -> None:
    findings = unenforceable_schema_findings(UNENFORCEABLE_SCHEMA)

    assert findings == [
        "#/properties/answer/minLength: keyword is not enforced by this endpoint",
        "#/properties/score/maximum: keyword is not enforced by this endpoint",
        "#/properties/score/minimum: keyword is not enforced by this endpoint",
    ]
    assert unenforceable_schema_findings(SCHEMA) == []


def test_an_open_object_is_not_an_enforced_object() -> None:
    findings = unenforceable_schema_findings({"type": "object", "properties": {}})

    assert findings == ["#/additionalProperties: must be false for an enforced object"]


def test_a_recursive_schema_is_named_as_unenforceable() -> None:
    recursive: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"child": {"$ref": "#/$defs/node"}},
        "$defs": {
            "node": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"child": {"$ref": "#/$defs/node"}},
            }
        },
    }

    findings = unenforceable_schema_findings(recursive)

    assert findings == ["#/$defs/node/properties/child/$ref: recursive reference to 'node' is not enforced"]


def test_an_unsupported_string_format_is_named_as_unenforceable() -> None:
    findings = unenforceable_schema_findings(
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {"when": {"type": "string", "format": "week"}},
        }
    )

    assert findings == ["#/properties/when/format: format is not enforced by this endpoint"]


def test_an_unenforceable_schema_is_refused_before_any_provider_call() -> None:
    model, messages = _model([_response()])

    with pytest.raises(UnenforceableOutputSchemaError) as failure:
        _call(model, schema=UNENFORCEABLE_SCHEMA)

    # Not even the free token count runs: the refusal is purely local.
    assert messages.calls == []
    assert messages.counts == []
    assert failure.value.call["status"] == "unenforceable_output_schema"
    assert failure.value.call["attempts"] == []
    assert failure.value.call["schema_enforcement_findings"] == unenforceable_schema_findings(UNENFORCEABLE_SCHEMA)
    assert failure.value.call["schema_validated_locally"] is False


def test_invalid_caller_schema_fails_locally_before_any_provider_call() -> None:
    model, messages = _model([_response()])

    with pytest.raises(InvalidOutputSchemaError) as failure:
        _call(model, schema=INVALID_SCHEMA)

    assert messages.calls == []
    assert messages.counts == []
    assert failure.value.call["status"] == "invalid_output_schema"
    assert failure.value.call["attempts"] == []


# ---------------------------------------------------------------------------
# budgets and token counting
# ---------------------------------------------------------------------------


def test_token_counts_come_from_the_provider_and_are_not_estimates() -> None:
    model, messages = _model([_response()], input_tokens=421)

    call = _call(model).call

    counted = messages.counts[0]
    assert counted["model"] == MODEL
    assert counted["system"] == INSTRUCTIONS
    # The schema is counted too, because it ships inside ``output_config``.
    assert counted["output_config"] == {"format": {"type": "json_schema", "schema": SCHEMA}}
    assert "max_tokens" not in counted
    assert call["token_count_method"] == TOKEN_COUNT_METHOD == "anthropic-count-tokens"
    assert call["token_count_is_estimate"] is False
    assert call["prompt_token_count"] == 421
    # The counts the provider reported for the call itself stay separate.
    assert call["input_tokens"] == 100
    assert call["output_tokens"] == 10
    assert call["total_tokens"] == 110


def test_prompt_budget_exceeded_raises_before_any_paid_call() -> None:
    model, messages = _model(
        [_response()],
        input_tokens=900,
        prompt_input_token_budget=1_000,
        prompt_safety_margin_tokens=200,
    )

    with pytest.raises(PromptBudgetExceededError) as failure:
        _call(model)

    assert messages.calls == []
    assert len(messages.counts) == 1
    assert failure.value.call["status"] == "prompt_budget_exceeded"
    assert failure.value.call["attempt_count"] == 0
    assert failure.value.call["attempts"] == []
    assert failure.value.call["prompt_token_count"] == 900
    assert failure.value.call["prompt_input_token_budget"] == 1_000


def test_an_uncountable_prompt_refuses_rather_than_calling_unbudgeted() -> None:
    model, messages = _model([_response()], count_error=TimeoutError("controlled count timeout"))

    with pytest.raises(TokenCountUnavailableError) as failure:
        _call(model)

    assert messages.calls == []
    assert failure.value.call["status"] == "token_count_unavailable"
    assert failure.value.call["prompt_token_count"] is None
    assert isinstance(failure.value.__cause__, TimeoutError)


# ---------------------------------------------------------------------------
# what an enforced route does when enforcement does not hold
# ---------------------------------------------------------------------------


def test_a_schema_violating_response_is_rejected_and_never_repaired() -> None:
    model, messages = _model([_response(text=json.dumps({"wrong": "shape"}))], max_retries=3)

    with pytest.raises(StructuredOutputSchemaError) as failure:
        _call(model)

    # One physical call: a settled, schema-invalid answer is not retried.
    assert len(messages.calls) == 1
    assert failure.value.call["status"] == "failed"
    assert failure.value.call["schema_validated_locally"] is False
    assert failure.value.call["attempt_count"] == 1
    assert len(str(failure.value.call["response_sha256"])) == 64


def test_a_degenerate_but_json_valid_answer_is_rejected() -> None:
    """``[]`` parses and is even schema-shaped nonsense; it is not an object."""
    model, _ = _model([_response(text="[]")], max_retries=0)

    with pytest.raises(AnthropicProviderExhaustedError) as failure:
        _call(model)

    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)


def test_a_fenced_answer_is_rejected_rather_than_unwrapped() -> None:
    """On an enforced route a code fence means enforcement did not hold."""
    fenced = "```json\n" + json.dumps({"answer": "ok"}) + "\n```"
    model, _ = _model([_response(text=fenced)], max_retries=0)

    with pytest.raises(AnthropicProviderExhaustedError) as failure:
        _call(model)

    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)


def test_an_empty_answer_is_incomplete_rather_than_accepted() -> None:
    model, _ = _model([_response(blocks=[])], max_retries=0)

    with pytest.raises(AnthropicProviderExhaustedError) as failure:
        _call(model)

    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)


def test_only_text_blocks_contribute_to_the_answer() -> None:
    model, _ = _model(
        [
            _response(
                blocks=[
                    SimpleNamespace(type="thinking", thinking="ignored"),
                    SimpleNamespace(type="text", text=json.dumps({"answer": "ok"})),
                ]
            )
        ]
    )

    assert _call(model).output == {"answer": "ok"}


def test_a_refusal_is_reported_with_its_category_and_never_its_prose() -> None:
    model, messages = _model(
        [
            _response(
                text="",
                stop_reason="refusal",
                stop_details=SimpleNamespace(
                    type="refusal",
                    category="cyber",
                    explanation="a provider-written sentence that must never reach a receipt",
                ),
            )
        ],
        max_retries=3,
    )

    with pytest.raises(AnthropicRefusalError) as failure:
        _call(model)

    # A refusal is settled: repeating the same request cannot clear it.
    assert len(messages.calls) == 1
    assert failure.value.call["status"] == "refused"
    assert failure.value.call["stop_reason"] == "refusal"
    assert failure.value.call["refusal_category"] == "cyber"
    serialized = json.dumps(failure.value.call, sort_keys=True, default=str)
    assert "provider-written sentence" not in serialized
    assert "provider-written sentence" not in str(failure.value)


@pytest.mark.parametrize("stop_reason", ["max_tokens", "model_context_window_exceeded"])
def test_an_answer_that_did_not_fit_the_budget_is_not_retried(stop_reason: str) -> None:
    model, messages = _model([_response(text='{"answer": "trunc', stop_reason=stop_reason)], max_retries=3)

    with pytest.raises(OutputBudgetExceededError) as failure:
        _call(model)

    assert len(messages.calls) == 1
    assert failure.value.call["status"] == "output_budget_exceeded"
    assert failure.value.call["stop_reason"] == stop_reason


def test_a_paused_turn_is_incomplete_and_retried() -> None:
    model, messages = _model([_response(stop_reason="pause_turn"), _response()], max_retries=1)

    result = _call(model)

    assert len(messages.calls) == 2
    assert result.call["attempt_count"] == 2
    assert result.call["retry_count"] == 1


# ---------------------------------------------------------------------------
# retries and receipt-safe error paths
# ---------------------------------------------------------------------------


def test_retryable_failures_are_retried_and_reported() -> None:
    model, messages = _model([_RateLimitEquivalent("controlled rate limit"), _response()], max_retries=1)

    result = _call(model)

    assert len(messages.calls) == 2
    assert result.call["attempt_count"] == 2
    assert result.call["retry_count"] == 1
    attempts = result.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["error_code"] == "_RateLimitEquivalent"
    assert attempts[0]["status_code"] == 429
    assert attempts[0]["provider_error_type"] == "rate_limit_error"


def test_exhausted_retries_raise_the_exhaustion_error_with_call_details() -> None:
    model, messages = _model(
        [TimeoutError("controlled timeout one"), TimeoutError("controlled timeout two")],
        max_retries=1,
    )

    with pytest.raises(AnthropicProviderExhaustedError, match="exhausted 2 attempts") as failure:
        _call(model)

    assert len(messages.calls) == 2
    assert failure.value.call["status"] == "retry_exhausted"
    assert failure.value.call["retry_count"] == 1


def test_a_billing_limit_is_not_retried() -> None:
    model, messages = _model([_BillingLimitEquivalent("controlled billing limit")], max_retries=3)

    with pytest.raises(AnthropicProviderCallError) as failure:
        _call(model)

    assert len(messages.calls) == 1
    attempts = failure.value.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["provider_error_type"] == "billing_error"


def test_a_rejected_schema_fails_immediately_with_call_details() -> None:
    """Whatever the local check misses, the API's own rejection is terminal."""
    model, messages = _model([_BadRequestEquivalent("controlled schema rejection")], max_retries=3)

    with pytest.raises(AnthropicProviderCallError) as failure:
        _call(model)

    assert len(messages.calls) == 1
    assert failure.value.call["status"] == "failed"
    attempts = failure.value.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["provider_error_type"] == "invalid_request_error"


def test_failure_call_details_and_messages_carry_no_provider_text_or_key() -> None:
    class _AuthEquivalent(RuntimeError):
        status_code = 401
        body = {"type": "error", "error": {"type": "authentication_error"}}

    model, _ = _model([_AuthEquivalent(f"invalid x-api-key: {FAKE_API_KEY} at api.anthropic.com")])

    with pytest.raises(AnthropicProviderCallError) as failure:
        _call(model)

    serialized = json.dumps(failure.value.call, sort_keys=True, default=str)
    assert SECRET_PATTERN.search(serialized) is None
    assert FAKE_API_KEY not in str(failure.value)
    assert "invalid x-api-key" not in str(failure.value)
    assert failure.value.call["attempts"][0]["provider_error_type"] == "authentication_error"


def test_call_details_and_request_carry_no_api_key_material() -> None:
    model = AnthropicStructuredTextModel(
        model=MODEL,
        api_key=FAKE_API_KEY,
        client=_client(_FakeMessages([_response()])),
    )

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


# ---------------------------------------------------------------------------
# construction, environment, and interface parity
# ---------------------------------------------------------------------------


def test_an_unconfigured_credential_is_a_refusal_not_a_silent_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)

    with pytest.raises(ProviderConfigurationError) as failure:
        AnthropicStructuredTextModel.from_environment(model=MODEL)

    message = str(failure.value)
    assert "ANTHROPIC_API_KEY is unset" in message
    assert SECRET_PATTERN.search(message) is None


def test_from_environment_builds_the_sdk_client_with_sdk_retries_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_anthropic(**kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return _client(_FakeMessages([_response()]))

    monkeypatch.setitem(sys.modules, "anthropic", SimpleNamespace(Anthropic=_fake_anthropic))
    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, FAKE_API_KEY)

    model = AnthropicStructuredTextModel.from_environment(model=MODEL, retry_base_seconds=0.0)

    assert captured["api_key"] == FAKE_API_KEY
    assert captured["max_retries"] == 0
    assert captured["timeout"] == model.timeout_seconds
    assert model.model_id == "anthropic:claude-opus-5"
    assert _call(model).output == {"answer": "ok"}


def test_constructor_rejects_a_defaulted_model_and_bad_settings() -> None:
    with pytest.raises(ValueError, match="model is required"):
        AnthropicStructuredTextModel(model="", api_key=FAKE_API_KEY)
    with pytest.raises(ProviderConfigurationError, match=API_KEY_ENVIRONMENT_VARIABLE):
        AnthropicStructuredTextModel(model=MODEL)
    with pytest.raises(ValueError, match="max_retries"):
        AnthropicStructuredTextModel(model=MODEL, client=_client(_FakeMessages([])), max_retries=-1)
    with pytest.raises(ValueError, match="timeout_seconds"):
        AnthropicStructuredTextModel(model=MODEL, client=_client(_FakeMessages([])), timeout_seconds=0)
    with pytest.raises(ValueError, match="reasoning_effort"):
        AnthropicStructuredTextModel(model=MODEL, client=_client(_FakeMessages([])), reasoning_effort="none")


def test_result_is_immutable_and_adapter_keeps_no_last_call_state() -> None:
    model, _ = _model([_response(), _response(text=json.dumps({"answer": "two"}))], max_retries=0)

    first = _call(model)
    second = _call(model, payload={"question": "second"})

    with pytest.raises(dataclasses.FrozenInstanceError):
        first.output = {"answer": "mutated"}  # ty: ignore[invalid-assignment]
    assert not hasattr(model, "last_call_metadata")
    assert not any("last_call" in name for name in dir(model))
    assert first.call["prompt_sha256"] != second.call["prompt_sha256"]


def test_adapter_satisfies_the_shared_structured_text_model_protocol() -> None:
    model, _ = _model([_response()])

    assert isinstance(model, StructuredTextModel)
    assert model.model_id == "anthropic:claude-opus-5"


def test_every_success_and_failure_path_emits_the_shared_call_detail_keys() -> None:
    """A receipt reader must not need to know which arm or path produced it."""
    calls: list[dict[str, Any]] = []

    success_model, _ = _model([_response()])
    calls.append(_call(success_model).call)

    schema_model, _ = _model([_response(text=json.dumps({"wrong": "shape"}))])
    with pytest.raises(StructuredOutputSchemaError) as schema_failure:
        _call(schema_model)
    calls.append(schema_failure.value.call)

    refusal_model, _ = _model(
        [_response(text="", stop_reason="refusal", stop_details=SimpleNamespace(type="refusal", category="bio"))]
    )
    with pytest.raises(AnthropicRefusalError) as refusal_failure:
        _call(refusal_model)
    calls.append(refusal_failure.value.call)

    truncated_model, _ = _model([_response(text="{", stop_reason="max_tokens")])
    with pytest.raises(OutputBudgetExceededError) as truncated_failure:
        _call(truncated_model)
    calls.append(truncated_failure.value.call)

    billing_model, _ = _model([_BillingLimitEquivalent("controlled billing limit")], max_retries=3)
    with pytest.raises(AnthropicProviderCallError) as billing_failure:
        _call(billing_model)
    calls.append(billing_failure.value.call)

    exhausted_model, _ = _model(
        [TimeoutError("controlled timeout one"), TimeoutError("controlled timeout two")],
        max_retries=1,
    )
    with pytest.raises(AnthropicProviderExhaustedError) as exhausted_failure:
        _call(exhausted_model)
    calls.append(exhausted_failure.value.call)

    budget_model, _ = _model(
        [_response()],
        input_tokens=900,
        prompt_input_token_budget=1_000,
        prompt_safety_margin_tokens=200,
    )
    with pytest.raises(PromptBudgetExceededError) as budget_failure:
        _call(budget_model)
    calls.append(budget_failure.value.call)

    count_model, _ = _model([_response()], count_error=TimeoutError("controlled count timeout"))
    with pytest.raises(TokenCountUnavailableError) as count_failure:
        _call(count_model)
    calls.append(count_failure.value.call)

    unenforceable_model, _ = _model([_response()])
    with pytest.raises(UnenforceableOutputSchemaError) as unenforceable_failure:
        _call(unenforceable_model, schema=UNENFORCEABLE_SCHEMA)
    calls.append(unenforceable_failure.value.call)

    invalid_model, _ = _model([_response()])
    with pytest.raises(InvalidOutputSchemaError) as invalid_failure:
        _call(invalid_model, schema=INVALID_SCHEMA)
    calls.append(invalid_failure.value.call)

    assert [call["status"] for call in calls] == [
        "completed",
        "failed",
        "refused",
        "output_budget_exceeded",
        "failed",
        "retry_exhausted",
        "prompt_budget_exceeded",
        "token_count_unavailable",
        "unenforceable_output_schema",
        "invalid_output_schema",
    ]
    for call in calls:
        assert set(SHARED_CALL_DETAIL_KEYS) <= set(call)
        assert isinstance(call["schema_validated_locally"], bool)
        assert call["provider"] == "anthropic"
        assert call["transport"] == "anthropic-messages"
        assert call["model_id"] == "anthropic:claude-opus-5"
        assert call["schema_enforcement"] == SCHEMA_ENFORCEMENT
        assert call["schema_enforced_by_provider"] is True
        assert call["token_count_is_estimate"] is False
        assert call["store"] is False
        assert call["sdk_max_retries"] == 0
    assert [call["schema_validated_locally"] for call in calls] == [True] + [False] * 9

"""Tests for the v3 OpenAI-compatible chat structured-text-model adapter.

This is the multi-provider arm of the same interface the OpenAI and Codex arms
implement: instructions, a strict JSON schema, a source payload, and an output
token limit go in; checked JSON and secret-free call details come back together
in one immutable result. Every test is hermetic — the client is injected, and no
test reaches a network, a credential, or provider internals.

What this arm owes on top of interface parity, and therefore what is pinned
here: the structured mechanism it actually used, the routing constraint a
brokering endpoint needs so a strict-schema request cannot be served by a
provider that ignores it, the honesty of its token counts, unambiguous provider
identity in the receipt, and a registry that refuses an unknown label or a
missing credential before any call.
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
from spicy_regs.docpipeline.adapters.openai_compatible import (
    PROMPTED_SCHEMA_INSTRUCTIONS,
    PROVIDER_PROFILES,
    PROVIDER_ROUTING_REQUIRE_PARAMETERS,
    TOKEN_COUNT_METHOD,
    IncompleteStructuredResponseError,
    InvalidOutputSchemaError,
    OpenAICompatibleProviderCallError,
    OpenAICompatibleProviderExhaustedError,
    OpenAICompatibleStructuredTextModel,
    PromptBudgetExceededError,
    ProviderConfigurationError,
    StructuredOutputSchemaError,
    UnknownProviderProfileError,
    resolve_base_url,
)

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}
INSTRUCTIONS = "Answer the supplied payload. Return only schema-valid JSON."
PAYLOAD: dict[str, Any] = {"question": "test"}
FAKE_API_KEY = "sk-ant-000000000000000000000000FAKEKEYFORTESTS"
SECRET_PATTERN = re.compile(r"sk-(ant-|proj-|or-)?[A-Za-z0-9_-]{8,}")
INVALID_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "not-a-json-schema-type"}},
}
LOCAL_BASE_URL = "http://127.0.0.1:8012/v1"

# Registry pins. Written out in full, never derived from the module under test:
# a constant that supplies its own expectation pins nothing. Adding a provider,
# changing a base URL, changing which endpoint is trusted with strict schema
# output, or changing which endpoint brokers to upstream providers must fail
# here first and be reviewed deliberately.
#
# There is deliberately no first-party ``anthropic`` profile: that endpoint
# accepts ``response_format`` and ignores it, and Claude via ``openrouter``
# covers the cross-route comparison. The enforced route is the native arm,
# ``adapters/anthropic.py``.
EXPECTED_PROFILES: dict[str, tuple[str | None, str | None, bool, bool, bool]] = {
    "openrouter": ("https://openrouter.ai/api/v1", "OPENROUTER_API_KEY", True, True, True),
    "gemini": (
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        "GEMINI_API_KEY",
        True,
        True,
        False,
    ),
    "local": (None, None, False, False, False),
}


class _FakeCompletions:
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


def _client(completions: _FakeCompletions) -> SimpleNamespace:
    return SimpleNamespace(chat=SimpleNamespace(completions=completions))


def _response(
    *,
    content: str | None = None,
    finish_reason: str = "stop",
    model: str = "test-model",
) -> SimpleNamespace:
    return SimpleNamespace(
        id="chatcmpl-test",
        model=model,
        choices=[
            SimpleNamespace(
                index=0,
                finish_reason=finish_reason,
                message=SimpleNamespace(
                    role="assistant",
                    content=(json.dumps({"answer": "ok"}) if content is None else content),
                ),
            )
        ],
        usage=SimpleNamespace(prompt_tokens=100, completion_tokens=10, total_tokens=110),
    )


def _model(
    outcomes: list[BaseException | SimpleNamespace],
    *,
    provider: str = "openrouter",
    model: str = "anthropic/claude-sonnet-5",
    max_retries: int = 0,
    prompt_input_token_budget: int = 8_192,
    prompt_safety_margin_tokens: int = 1_024,
    **overrides: Any,
) -> tuple[OpenAICompatibleStructuredTextModel, _FakeCompletions]:
    completions = _FakeCompletions(outcomes)
    built = OpenAICompatibleStructuredTextModel(
        provider=provider,
        model=model,
        client=_client(completions),
        max_retries=max_retries,
        retry_base_seconds=0.0,
        timeout_seconds=10.0,
        prompt_input_token_budget=prompt_input_token_budget,
        prompt_safety_margin_tokens=prompt_safety_margin_tokens,
        **overrides,
    )
    return built, completions


def _prompted_model(
    outcomes: list[BaseException | SimpleNamespace],
    **overrides: Any,
) -> tuple[OpenAICompatibleStructuredTextModel, _FakeCompletions]:
    """Build a model on a profile known not to honor json_schema."""
    return _model(
        outcomes,
        provider="local",
        model="mlx-community/Qwen3-30B",
        base_url=LOCAL_BASE_URL,
        **overrides,
    )


def _call(model: OpenAICompatibleStructuredTextModel, **overrides: Any) -> StructuredTextResult:
    request = {
        "name": "test_answer",
        "schema": SCHEMA,
        "instructions": INSTRUCTIONS,
        "payload": PAYLOAD,
        "max_output_tokens": 256,
    }
    request.update(overrides)
    return model.structured_json(**request)  # type: ignore[arg-type]


class _BadRequestEquivalent(RuntimeError):
    """What an endpoint raises when it refuses ``response_format``."""

    status_code = 400
    request_id = "request-bad-request"


class _RateLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-rate-limit"


class _QuotaLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-quota-limit"
    body = {"error": {"code": "insufficient_quota", "type": "insufficient_quota"}}


# ---------------------------------------------------------------------------
# provider registry
# ---------------------------------------------------------------------------


def test_the_provider_registry_holds_exactly_the_reviewed_profiles() -> None:
    assert set(PROVIDER_PROFILES) == set(EXPECTED_PROFILES)
    for label, expected in EXPECTED_PROFILES.items():
        base_url, key_variable, response_format, requires_key, routes_upstream = expected
        profile = PROVIDER_PROFILES[label]
        assert profile.base_url == base_url
        assert profile.api_key_environment_variable == key_variable
        assert profile.supports_response_format is response_format
        assert profile.requires_api_key is requires_key
        assert profile.routes_to_upstream_providers is routes_upstream
    assert PROVIDER_PROFILES["local"].base_url_environment_variable == "SPICY_REGS_LOCAL_LLM_BASE_URL"
    assert PROVIDER_PROFILES["local"].loopback_only is True


def test_the_retired_anthropic_compat_profile_is_gone() -> None:
    """Claude has a native enforced arm; the compat workaround is retired."""
    assert "anthropic" not in PROVIDER_PROFILES

    with pytest.raises(UnknownProviderProfileError, match="unknown provider 'anthropic'"):
        OpenAICompatibleStructuredTextModel(provider="anthropic", model="claude-opus-5", api_key=FAKE_API_KEY)


def test_the_routing_constraint_is_openrouters_documented_field() -> None:
    """``provider.require_parameters`` is the field name; nothing else routes."""
    assert dict(PROVIDER_ROUTING_REQUIRE_PARAMETERS) == {"require_parameters": True}


def test_an_unknown_provider_label_is_refused_before_anything_is_built() -> None:
    with pytest.raises(UnknownProviderProfileError, match="unknown provider 'made-up'"):
        OpenAICompatibleStructuredTextModel(provider="made-up", model="some-model", api_key=FAKE_API_KEY)


def test_a_missing_credential_is_refused_and_names_only_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    with pytest.raises(ProviderConfigurationError) as failure:
        OpenAICompatibleStructuredTextModel.from_environment(
            provider="openrouter",
            model="anthropic/claude-sonnet-5",
        )

    message = str(failure.value)
    assert "OPENROUTER_API_KEY is unset" in message
    assert SECRET_PATTERN.search(message) is None


def test_a_missing_local_base_url_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SPICY_REGS_LOCAL_LLM_BASE_URL", raising=False)

    with pytest.raises(ProviderConfigurationError, match="SPICY_REGS_LOCAL_LLM_BASE_URL"):
        OpenAICompatibleStructuredTextModel.from_environment(provider="local", model="local-model")


def test_a_local_label_may_not_describe_a_call_that_leaves_the_machine() -> None:
    with pytest.raises(ProviderConfigurationError, match="loopback"):
        OpenAICompatibleStructuredTextModel(
            provider="local",
            model="local-model",
            base_url="https://models.example.com/v1",
            client=_client(_FakeCompletions([])),
        )


def test_base_url_resolution_drops_userinfo_query_and_trailing_slash() -> None:
    resolved = resolve_base_url(
        PROVIDER_PROFILES["openrouter"],
        "https://user:secret@openrouter.ai/api/v1/?key=leaked",
    )

    assert resolved == "https://openrouter.ai/api/v1"
    assert resolve_base_url(PROVIDER_PROFILES["gemini"]) == ("https://generativelanguage.googleapis.com/v1beta/openai")


def test_local_reads_its_base_url_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPICY_REGS_LOCAL_LLM_BASE_URL", LOCAL_BASE_URL)
    completions = _FakeCompletions([_response()])

    model = OpenAICompatibleStructuredTextModel(
        provider="local",
        model="mlx-community/Qwen3-30B",
        client=_client(completions),
    )

    assert model.base_url == LOCAL_BASE_URL
    assert model.base_url_host == "127.0.0.1"
    assert _call(model).call["base_url_host"] == "127.0.0.1"


# ---------------------------------------------------------------------------
# structured output: both mechanisms
# ---------------------------------------------------------------------------


def test_response_format_mode_sends_a_strict_schema_and_reports_the_mechanism() -> None:
    model, completions = _model([_response()])

    result = _call(model, max_output_tokens=512)

    sent = completions.calls[0]
    assert sent["model"] == "anthropic/claude-sonnet-5"
    assert sent["max_tokens"] == 512
    assert sent["messages"][0] == {"role": "system", "content": INSTRUCTIONS}
    assert json.loads(sent["messages"][1]["content"]) == PAYLOAD
    assert sent["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "test_answer", "strict": True, "schema": SCHEMA},
    }
    # A broker must not be free to serve this from a provider that ignores it.
    assert sent["extra_body"] == {"provider": {"require_parameters": True}}
    assert "reasoning_effort" not in sent
    assert result.output == {"answer": "ok"}
    assert result.call["structured_mode"] == "response_format"
    assert result.call["structured_mode_requested"] == "auto"
    assert result.call["structured_mode_fallback"] is False
    assert result.call["provider_routing"] == {"require_parameters": True}
    assert result.call["schema_validated_locally"] is True
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


def test_a_provider_known_to_ignore_json_schema_starts_in_prompted_mode() -> None:
    model, completions = _prompted_model([_response()])

    result = _call(model)

    sent = completions.calls[0]
    system = sent["messages"][0]["content"]
    assert "response_format" not in sent
    # No strict parameter is being sent, so nothing needs routing around.
    assert "extra_body" not in sent
    assert result.call["provider_routing"] is None
    assert system.startswith(INSTRUCTIONS)
    assert PROMPTED_SCHEMA_INSTRUCTIONS in system
    assert json.dumps(SCHEMA, sort_keys=True, separators=(",", ":")) in system
    assert result.output == {"answer": "ok"}
    assert result.call["structured_mode"] == "prompted"
    assert result.call["structured_mode_fallback"] is False
    assert result.call["schema_validated_locally"] is True


def test_a_rejected_response_format_falls_back_to_prompted_and_says_so() -> None:
    """A refused mechanism is not a refused call, and the receipt shows both."""
    model, completions = _model(
        [_BadRequestEquivalent("controlled response_format refusal"), _response()],
        max_retries=0,
    )

    result = _call(model)

    assert len(completions.calls) == 2
    assert "response_format" in completions.calls[0]
    assert "response_format" not in completions.calls[1]
    # The routing constraint is dropped with the mechanism it was protecting.
    assert completions.calls[0]["extra_body"] == {"provider": {"require_parameters": True}}
    assert "extra_body" not in completions.calls[1]
    assert PROMPTED_SCHEMA_INSTRUCTIONS in completions.calls[1]["messages"][0]["content"]
    assert result.output == {"answer": "ok"}
    assert result.call["structured_mode"] == "prompted"
    assert result.call["structured_mode_requested"] == "auto"
    assert result.call["structured_mode_fallback"] is True
    attempts = result.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["status"] == "response_format_rejected"
    assert attempts[0]["structured_mode"] == "response_format"
    assert attempts[1]["structured_mode"] == "prompted"
    # Each attempt records the hash of exactly the body that went out.
    assert attempts[0]["request_sha256"] != attempts[1]["request_sha256"]
    assert attempts[0]["request_sha256"] == result.call["request_sha256"]


def test_a_pinned_mechanism_is_never_silently_downgraded() -> None:
    model, completions = _model(
        [_BadRequestEquivalent("controlled response_format refusal")],
        structured_mode="response_format",
    )

    with pytest.raises(OpenAICompatibleProviderCallError) as failure:
        _call(model)

    assert len(completions.calls) == 1
    assert failure.value.call["structured_mode"] == "response_format"
    assert failure.value.call["structured_mode_requested"] == "response_format"


def test_the_fallback_happens_at_most_once_per_call() -> None:
    model, completions = _model(
        [
            _BadRequestEquivalent("controlled response_format refusal"),
            _BadRequestEquivalent("controlled second refusal"),
        ],
        max_retries=0,
    )

    with pytest.raises(OpenAICompatibleProviderCallError) as failure:
        _call(model)

    assert len(completions.calls) == 2
    assert failure.value.call["structured_mode"] == "prompted"
    assert failure.value.call["attempt_count"] == 2


def test_a_whole_response_code_fence_is_unwrapped_and_recorded() -> None:
    fenced = "```json\n" + json.dumps({"answer": "ok"}) + "\n```"
    model, _ = _prompted_model([_response(content=fenced)])

    result = _call(model)

    assert result.output == {"answer": "ok"}
    assert result.call["response_unfenced"] is True


def test_json_wrapped_in_prose_is_rejected_rather_than_scavenged() -> None:
    model, _ = _prompted_model([_response(content='Here is the answer: {"answer": "ok"}')])

    with pytest.raises(OpenAICompatibleProviderExhaustedError) as failure:
        _call(model)

    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)


def test_a_schema_violating_response_is_rejected_and_never_repaired() -> None:
    model, completions = _model([_response(content=json.dumps({"wrong": "shape"}))], max_retries=3)

    with pytest.raises(StructuredOutputSchemaError) as failure:
        _call(model)

    # One physical call: a settled, schema-invalid answer is not retried.
    assert len(completions.calls) == 1
    assert failure.value.call["status"] == "failed"
    assert failure.value.call["schema_validated_locally"] is False
    assert failure.value.call["attempt_count"] == 1
    assert len(str(failure.value.call["response_sha256"])) == 64


def test_prompted_mode_validates_locally_exactly_as_response_format_mode_does() -> None:
    model, _ = _prompted_model([_response(content=json.dumps({"wrong": "shape"}))])

    with pytest.raises(StructuredOutputSchemaError) as failure:
        _call(model)

    assert failure.value.call["structured_mode"] == "prompted"
    assert failure.value.call["schema_validated_locally"] is False


# ---------------------------------------------------------------------------
# budgets, retries, and refusals
# ---------------------------------------------------------------------------


def test_prompt_budget_exceeded_raises_before_any_provider_call() -> None:
    model, completions = _model(
        [_response()],
        prompt_input_token_budget=64,
        prompt_safety_margin_tokens=16,
    )

    with pytest.raises(PromptBudgetExceededError) as failure:
        _call(model, payload={"question": "budget " * 200})

    assert completions.calls == []
    assert failure.value.call["status"] == "prompt_budget_exceeded"
    assert failure.value.call["attempt_count"] == 0
    assert failure.value.call["attempts"] == []
    assert failure.value.call["prompt_input_token_budget"] == 64
    assert int(failure.value.call["prompt_token_estimate"]) + 16 > 64


def test_token_counts_are_enforced_but_labeled_as_estimates() -> None:
    """tiktoken does not tokenize a Claude or Gemini model; the receipt says so."""
    model, _ = _model([_response()])

    call = _call(model).call

    assert call["token_count_method"] == TOKEN_COUNT_METHOD == "tiktoken-estimate"
    assert call["token_count_is_estimate"] is True
    assert call["tokenizer"] == "o200k_base"
    assert call["tokenizer_version"]
    assert int(call["prompt_token_estimate"]) > 0
    assert int(call["schema_token_estimate"]) > 0
    # The counts the provider itself reported are recorded separately.
    assert call["input_tokens"] == 100
    assert call["output_tokens"] == 10
    assert call["total_tokens"] == 110


def test_invalid_caller_schema_fails_locally_before_any_provider_call() -> None:
    model, completions = _model([_response()])

    with pytest.raises(InvalidOutputSchemaError) as failure:
        _call(model, schema=INVALID_SCHEMA)

    assert completions.calls == []
    assert failure.value.call["status"] == "invalid_output_schema"
    assert failure.value.call["attempts"] == []


def test_retryable_failures_are_retried_and_reported() -> None:
    model, completions = _model(
        [_RateLimitEquivalent("controlled rate limit"), _response()],
        max_retries=1,
    )

    result = _call(model)

    assert len(completions.calls) == 2
    assert result.call["attempt_count"] == 2
    assert result.call["retry_count"] == 1
    attempts = result.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["error_code"] == "_RateLimitEquivalent"
    assert attempts[0]["status_code"] == 429


def test_exhausted_retries_raise_the_exhaustion_error_with_call_details() -> None:
    model, completions = _model(
        [TimeoutError("controlled timeout one"), TimeoutError("controlled timeout two")],
        max_retries=1,
    )

    with pytest.raises(OpenAICompatibleProviderExhaustedError, match="exhausted 2 attempts") as failure:
        _call(model)

    assert len(completions.calls) == 2
    assert failure.value.call["status"] == "retry_exhausted"
    assert failure.value.call["retry_count"] == 1


def test_non_retryable_provider_error_fails_immediately_with_call_details() -> None:
    model, completions = _model([_QuotaLimitEquivalent("controlled quota limit")], max_retries=3)

    with pytest.raises(OpenAICompatibleProviderCallError) as failure:
        _call(model)

    # A spend limit is not a refused mechanism: no fallback, no retry.
    assert len(completions.calls) == 1
    assert failure.value.call["structured_mode"] == "response_format"
    attempts = failure.value.call["attempts"]
    assert isinstance(attempts, list)
    assert attempts[0]["provider_error_code"] == "insufficient_quota"


def test_a_truncated_response_is_incomplete_rather_than_accepted() -> None:
    model, _ = _model([_response(finish_reason="length")], max_retries=0)

    with pytest.raises(OpenAICompatibleProviderExhaustedError) as failure:
        _call(model)

    assert isinstance(failure.value.__cause__, IncompleteStructuredResponseError)


# ---------------------------------------------------------------------------
# receipts: identity, secrets, and interface parity
# ---------------------------------------------------------------------------


def test_call_details_distinguish_the_same_model_reached_through_two_providers() -> None:
    through_openrouter, _ = _model([_response()], provider="openrouter", model="gemini-3-pro")
    through_gemini, _ = _model([_response()], provider="gemini", model="gemini-3-pro")

    first = _call(through_openrouter).call
    second = _call(through_gemini).call

    assert (first["provider"], first["model_id"], first["base_url_host"]) == (
        "openrouter",
        "openrouter:gemini-3-pro",
        "openrouter.ai",
    )
    assert (second["provider"], second["model_id"], second["base_url_host"]) == (
        "gemini",
        "gemini:gemini-3-pro",
        "generativelanguage.googleapis.com",
    )
    # Same mechanism, but only the broker needs the routing constraint.
    assert first["structured_mode"] == second["structured_mode"] == "response_format"
    assert first["provider_routing"] == {"require_parameters": True}
    assert second["provider_routing"] is None
    assert first["model"] == second["model"] == "gemini-3-pro"
    assert first["provider_family"] == second["provider_family"] == "openai-compatible"
    assert first["transport"] == second["transport"] == "openai-chat-completions"


def test_call_details_and_request_carry_no_api_key_material() -> None:
    model = OpenAICompatibleStructuredTextModel(
        provider="openrouter",
        model="anthropic/claude-sonnet-5",
        api_key=FAKE_API_KEY,
        client=_client(_FakeCompletions([_response()])),
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


def test_failure_call_details_and_messages_carry_no_provider_text_or_key() -> None:
    class _AuthEquivalent(RuntimeError):
        status_code = 401
        body = {"error": {"code": "invalid_api_key"}}

    model, _ = _model([_AuthEquivalent(f"Incorrect API key provided: {FAKE_API_KEY} at openrouter.ai")])

    with pytest.raises(OpenAICompatibleProviderCallError) as failure:
        _call(model)

    serialized = json.dumps(failure.value.call, sort_keys=True, default=str)
    assert SECRET_PATTERN.search(serialized) is None
    assert FAKE_API_KEY not in str(failure.value)
    assert "Incorrect API key" not in str(failure.value)
    assert failure.value.call["attempts"][0]["provider_error_code"] == "invalid_api_key"


def test_result_is_immutable_and_adapter_keeps_no_last_call_state() -> None:
    model, _ = _model([_response(), _response(content=json.dumps({"answer": "two"}))], max_retries=0)

    first = _call(model)
    second = _call(model, payload={"question": "second"})

    with pytest.raises(dataclasses.FrozenInstanceError):
        first.output = {"answer": "mutated"}  # ty: ignore[invalid-assignment]
    assert not hasattr(model, "last_call_metadata")
    assert not any("last_call" in name for name in dir(model))
    assert first.call["prompt_sha256"] != second.call["prompt_sha256"]


def test_every_success_and_failure_path_emits_the_shared_call_detail_keys() -> None:
    """A receipt reader must not need to know which arm or path produced it."""
    calls: list[dict[str, Any]] = []

    success_model, _ = _model([_response()])
    calls.append(_call(success_model).call)

    prompted_model, _ = _prompted_model([_response()])
    calls.append(_call(prompted_model).call)

    fallback_model, _ = _model([_BadRequestEquivalent("controlled refusal"), _response()])
    calls.append(_call(fallback_model).call)

    schema_model, _ = _model([_response(content=json.dumps({"wrong": "shape"}))])
    with pytest.raises(StructuredOutputSchemaError) as schema_failure:
        _call(schema_model)
    calls.append(schema_failure.value.call)

    quota_model, _ = _model([_QuotaLimitEquivalent("controlled quota limit")], max_retries=3)
    with pytest.raises(OpenAICompatibleProviderCallError) as quota_failure:
        _call(quota_model)
    calls.append(quota_failure.value.call)

    exhausted_model, _ = _model(
        [TimeoutError("controlled timeout one"), TimeoutError("controlled timeout two")],
        max_retries=1,
    )
    with pytest.raises(OpenAICompatibleProviderExhaustedError) as exhausted_failure:
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
        "completed",
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
        assert call["structured_mode"] in {"response_format", "prompted"}
        assert call["token_count_is_estimate"] is True
        assert call["base_url_host"]
    assert [call["schema_validated_locally"] for call in calls] == [
        True,
        True,
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
    assert model.model_id == "openrouter:anthropic/claude-sonnet-5"


def test_constructor_rejects_a_defaulted_model_and_bad_settings() -> None:
    with pytest.raises(ValueError, match="model is required"):
        OpenAICompatibleStructuredTextModel(provider="openrouter", model="", api_key=FAKE_API_KEY)
    with pytest.raises(ValueError, match="structured_mode"):
        OpenAICompatibleStructuredTextModel(
            provider="openrouter",
            model="m",
            client=_client(_FakeCompletions([])),
            structured_mode="strict-ish",
        )
    with pytest.raises(ValueError, match="max_retries"):
        OpenAICompatibleStructuredTextModel(
            provider="openrouter",
            model="m",
            client=_client(_FakeCompletions([])),
            max_retries=-1,
        )
    with pytest.raises(ValueError, match="reasoning_effort"):
        OpenAICompatibleStructuredTextModel(
            provider="openrouter",
            model="m",
            client=_client(_FakeCompletions([])),
            reasoning_effort="turbo",
        )


def test_from_environment_injects_the_base_url_and_disables_sdk_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_openai(**kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return _client(_FakeCompletions([_response()]))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=_fake_openai))
    monkeypatch.setenv("OPENROUTER_API_KEY", FAKE_API_KEY)

    model = OpenAICompatibleStructuredTextModel.from_environment(
        provider="openrouter",
        model="google/gemini-3-pro",
        retry_base_seconds=0.0,
    )

    assert captured["base_url"] == "https://openrouter.ai/api/v1"
    assert captured["max_retries"] == 0
    assert captured["api_key"] == FAKE_API_KEY
    assert model.model_id == "openrouter:google/gemini-3-pro"
    assert _call(model).output == {"answer": "ok"}


def test_a_keyless_local_endpoint_needs_no_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_openai(**kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return _client(_FakeCompletions([_response()]))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=_fake_openai))
    monkeypatch.setenv("SPICY_REGS_LOCAL_LLM_BASE_URL", LOCAL_BASE_URL)

    model = OpenAICompatibleStructuredTextModel.from_environment(
        provider="local",
        model="mlx-community/Qwen3-30B",
    )

    assert captured["base_url"] == LOCAL_BASE_URL
    assert captured["api_key"] == "not-required"
    assert model.structured_mode == "prompted"

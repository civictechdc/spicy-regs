"""OpenAI-compatible chat arm of the v3 structured-text-model interface.

This is the third provider arm for the same interface as ``adapters/openai.py``
and ``adapters/codex_cli.py``: the same (instructions, schema, payload,
``max_output_tokens``) call, and checked JSON plus secret-free call details
returned together in one immutable :class:`StructuredTextResult`. There is no
mutable ``last_call_metadata`` side channel; a failure carries its own call
details on the raised error.

Why it exists: the project must run and compare *different model families* —
immediately so blind adjudication can use judges from a family other than the
tagger's, and generally for model comparison. Every provider reached here speaks
the OpenAI chat-completions wire format, so the already-present ``openai`` SDK is
the transport and no new dependency enters the project. Only ``base_url`` and the
credential differ per provider, and both come from a small registry of profiles
(:data:`PROVIDER_PROFILES`). Model IDs are always caller-pinned strings: this
module never defaults a model for a provider, because "which model ran" is the
one fact a comparison receipt may not guess.

Two honesty properties this arm owes its receipts, because it talks to endpoints
it does not own:

* **Structured output.** ``response_format`` with a strict JSON schema is used
  when the endpoint accepts it, and schema-embedded instructions otherwise. Both
  ways the parsed response is validated locally against the caller's schema, and
  a response that fails is *rejected* — never repaired. ``structured_mode`` in
  the call details says which mechanism actually produced the answer.
* **Token counting.** ``tiktoken`` counts are exact only for OpenAI models; for
  every provider here they are estimates. The budget is still enforced, and
  ``token_count_method``/``token_count_is_estimate`` record what the number is.

Only ``StructuredTextCallError.call`` is receipt-safe. A raised error's message
and its ``__cause__`` chain may contain provider-supplied text — including key
fragments a provider echoes back in an authentication failure — so neither may
be written into a receipt, ledger, or checkpoint. No provider response text, and
no credential, reaches a message raised by this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version as installed_version
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse, urlunparse

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from loguru import logger

from spicy_regs.docpipeline.adapters import (
    SUPPORTED_REASONING_EFFORTS,
    StructuredTextCallError,
    StructuredTextResult,
)
from spicy_regs.ontology.common import canonical_json

PROVIDER_FAMILY = "openai-compatible"
TRANSPORT = "openai-chat-completions"

DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_SECONDS = 1.0
DEFAULT_TOKENIZER = "o200k_base"
PROMPT_INPUT_TOKEN_BUDGET = 8_192
PROMPT_SAFETY_MARGIN_TOKENS = 1_024

#: These counts are estimates: only OpenAI models tokenize as ``tiktoken`` does.
#: The budget is enforced anyway, and the receipt says the number is an estimate.
TOKEN_COUNT_METHOD = "tiktoken-estimate"

STRUCTURED_MODE_RESPONSE_FORMAT = "response_format"
STRUCTURED_MODE_PROMPTED = "prompted"
STRUCTURED_MODE_AUTO = "auto"
SUPPORTED_STRUCTURED_MODES = frozenset(
    {STRUCTURED_MODE_AUTO, STRUCTURED_MODE_RESPONSE_FORMAT, STRUCTURED_MODE_PROMPTED}
)

#: Appended to the caller's instructions in prompted mode. The schema itself is
#: appended after it, so the model sees the same contract the local validator
#: enforces. The wording states the rejection rule the adapter actually applies.
PROMPTED_SCHEMA_INSTRUCTIONS = (
    "Return exactly one JSON object that validates against the JSON Schema below. "
    "Return no prose, no explanation, and no Markdown code fences. "
    "A response that does not validate is rejected, never repaired."
)

#: The OpenAI SDK requires a key string even for a server that ignores it. This
#: placeholder is not a credential and never reaches call details or a receipt.
KEYLESS_PLACEHOLDER_API_KEY = "not-required"

LOCAL_BASE_URL_ENVIRONMENT_VARIABLE = "SPICY_REGS_LOCAL_LLM_BASE_URL"
TIMEOUT_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_TIMEOUT_SECONDS"
MAX_RETRIES_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_MAX_RETRIES"
RETRY_BASE_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_RETRY_BASE_SECONDS"
PROMPT_BUDGET_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_PROMPT_INPUT_TOKEN_BUDGET"

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True)
class ProviderProfile:
    """Everything that differs between two OpenAI-compatible endpoints.

    A profile carries no model: model IDs are caller-pinned. It carries the
    *name* of the credential environment variable, never a credential.
    """

    label: str
    base_url: str | None
    api_key_environment_variable: str | None
    base_url_environment_variable: str | None = None
    #: ``False`` when the endpoint is known not to honor ``response_format``
    #: json_schema. Anthropic's OpenAI-compatibility layer is the case that
    #: matters: it accepts the field and ignores it, which would silently
    #: downgrade a strict-schema call into an unconstrained one.
    supports_response_format: bool = True
    requires_api_key: bool = True
    #: ``True`` for a profile whose name promises a local endpoint. A receipt
    #: that says "local" must not describe a call that left the machine.
    loopback_only: bool = False


PROVIDER_PROFILES: Mapping[str, ProviderProfile] = MappingProxyType(
    {
        profile.label: profile
        for profile in (
            ProviderProfile(
                label="openrouter",
                base_url="https://openrouter.ai/api/v1",
                api_key_environment_variable="OPENROUTER_API_KEY",
                supports_response_format=True,
            ),
            ProviderProfile(
                label="anthropic",
                base_url="https://api.anthropic.com/v1/",
                api_key_environment_variable="ANTHROPIC_API_KEY",
                supports_response_format=False,
            ),
            ProviderProfile(
                label="gemini",
                base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                api_key_environment_variable="GEMINI_API_KEY",
                supports_response_format=True,
            ),
            ProviderProfile(
                label="local",
                base_url=None,
                api_key_environment_variable=None,
                base_url_environment_variable=LOCAL_BASE_URL_ENVIRONMENT_VARIABLE,
                supports_response_format=False,
                requires_api_key=False,
                loopback_only=True,
            ),
        )
    }
)


class UnknownProviderProfileError(ValueError):
    """The caller named a provider label the registry does not define."""


class ProviderConfigurationError(ValueError):
    """A named provider cannot be built: missing credential or base URL.

    The message names the environment variable, never its value, so a refusal
    can be logged safely.
    """


class PromptBudgetExceededError(StructuredTextCallError, ValueError):
    """A deterministic prompt exceeded the declared input budget."""


class InvalidOutputSchemaError(StructuredTextCallError, ValueError):
    """The caller's schema is not a valid JSON Schema, so no call was made."""


class IncompleteStructuredResponseError(StructuredTextCallError):
    """A provider response ended before a usable structured value."""


class StructuredOutputSchemaError(StructuredTextCallError):
    """A complete provider response violated the declared output schema."""


class OpenAICompatibleProviderCallError(StructuredTextCallError):
    """A non-retryable provider failure ended the call."""


class OpenAICompatibleProviderExhaustedError(StructuredTextCallError):
    """All application-owned provider attempts failed."""


class TiktokenCounter:
    """Pinned token counter, deliberately not shared with the OpenAI arm.

    The arms do not import one another, so this arm owns its counter and its
    honesty about it: for every provider reached here the count is an estimate,
    because only OpenAI models tokenize with these encodings.
    """

    def __init__(self, encoding_name: str = DEFAULT_TOKENIZER) -> None:
        import tiktoken

        self.name = encoding_name
        self.version = installed_version("tiktoken")
        self._encoding = tiktoken.get_encoding(encoding_name)

    def count(self, text: str) -> int:
        return len(self._encoding.encode(text, disallowed_special=()))


class OpenAICompatibleStructuredTextModel:
    """Chat-completions provider reached through an injected ``base_url``.

    The SDK import is lazy, so deterministic rollups and keyless CI never
    initialize an API client. Tests inject a client through ``client=`` instead
    of reaching into provider internals. The application owns retries: the SDK
    is configured with ``max_retries=0`` so every physical call appears in the
    returned call details.
    """

    production_provider = True

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any | None = None,
        structured_mode: str = STRUCTURED_MODE_AUTO,
        reasoning_effort: str | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
        prompt_input_token_budget: int = PROMPT_INPUT_TOKEN_BUDGET,
        prompt_safety_margin_tokens: int = PROMPT_SAFETY_MARGIN_TOKENS,
        tokenizer: str = DEFAULT_TOKENIZER,
    ) -> None:
        profile = resolve_provider_profile(provider)
        if not model:
            raise ValueError("model is required: this arm never defaults a model for a provider")
        if structured_mode not in SUPPORTED_STRUCTURED_MODES:
            raise ValueError("structured_mode must be one of " + ", ".join(sorted(SUPPORTED_STRUCTURED_MODES)))
        if max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        if retry_base_seconds < 0:
            raise ValueError("retry_base_seconds must be nonnegative")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if prompt_input_token_budget <= 0:
            raise ValueError("prompt_input_token_budget must be positive")
        if prompt_safety_margin_tokens < 0:
            raise ValueError("prompt_safety_margin_tokens must be nonnegative")
        if reasoning_effort is not None and reasoning_effort not in SUPPORTED_REASONING_EFFORTS:
            raise ValueError("reasoning_effort must be one of " + ", ".join(sorted(SUPPORTED_REASONING_EFFORTS)))
        self.profile = profile
        self.provider = profile.label
        self.base_url = resolve_base_url(profile, base_url)
        self.base_url_host = str(urlparse(self.base_url).hostname or "")
        self.model = model
        self.model_id = f"{profile.label}:{model}"
        self.requested_structured_mode = structured_mode
        self.structured_mode = (
            structured_mode
            if structured_mode != STRUCTURED_MODE_AUTO
            else (STRUCTURED_MODE_RESPONSE_FORMAT if profile.supports_response_format else STRUCTURED_MODE_PROMPTED)
        )
        # Only ``auto`` may change mechanism mid-call. A caller who pinned a
        # mechanism gets a failure instead of a silent downgrade.
        self.structured_mode_fallback_allowed = (
            structured_mode == STRUCTURED_MODE_AUTO and self.structured_mode == STRUCTURED_MODE_RESPONSE_FORMAT
        )
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_base_seconds = retry_base_seconds
        self.prompt_input_token_budget = prompt_input_token_budget
        self.prompt_safety_margin_tokens = prompt_safety_margin_tokens
        self.tokenizer = tokenizer
        self.run_configuration: dict[str, Any] = {
            "provider": self.provider,
            "provider_family": PROVIDER_FAMILY,
            "transport": TRANSPORT,
            "base_url": self.base_url,
            "base_url_host": self.base_url_host,
            "model": model,
            "model_id": self.model_id,
            "structured_mode": self.structured_mode,
            "structured_mode_requested": self.requested_structured_mode,
            "structured_mode_fallback_allowed": self.structured_mode_fallback_allowed,
            "reasoning_effort": reasoning_effort,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "retry_base_seconds": retry_base_seconds,
            "sdk_max_retries": 0,
            "store": False,
            "prompt_input_token_budget": prompt_input_token_budget,
            "prompt_safety_margin_tokens": prompt_safety_margin_tokens,
            "tokenizer": tokenizer,
            "token_count_method": TOKEN_COUNT_METHOD,
            "token_count_is_estimate": True,
        }
        self._token_counter: TiktokenCounter | None = None
        if client is not None:
            self._client = client
            return
        if profile.requires_api_key and not api_key:
            raise ProviderConfigurationError(
                f"{profile.label}: an API key is required when no client is injected "
                f"(set {profile.api_key_environment_variable})"
            )
        from openai import OpenAI

        self._client = OpenAI(
            api_key=api_key or KEYLESS_PLACEHOLDER_API_KEY,
            base_url=self.base_url,
            timeout=timeout_seconds,
            # The application owns retries so every physical call is visible in
            # checkpoint, ledger, and receipt telemetry.
            max_retries=0,
        )

    @classmethod
    def from_environment(
        cls,
        *,
        provider: str,
        model: str,
        **overrides: Any,
    ) -> OpenAICompatibleStructuredTextModel:
        """Build a named provider from the environment.

        Unlike the single-arm OpenAI provider, a missing credential here is a
        refusal rather than ``None``: the caller named one provider on purpose,
        usually to compare it against another, and silently dropping it would
        publish a comparison that never ran.
        """
        profile = resolve_provider_profile(provider)
        api_key: str | None = None
        if profile.api_key_environment_variable:
            api_key = os.environ.get(profile.api_key_environment_variable)
            if profile.requires_api_key and not api_key:
                raise ProviderConfigurationError(f"{profile.label}: {profile.api_key_environment_variable} is unset")
        settings: dict[str, Any] = {
            "timeout_seconds": _float_environment(
                TIMEOUT_ENVIRONMENT_VARIABLE,
                DEFAULT_TIMEOUT_SECONDS,
                positive=True,
            ),
            "max_retries": _int_environment(
                MAX_RETRIES_ENVIRONMENT_VARIABLE,
                DEFAULT_MAX_RETRIES,
                minimum=0,
            ),
            "retry_base_seconds": _float_environment(
                RETRY_BASE_ENVIRONMENT_VARIABLE,
                DEFAULT_RETRY_BASE_SECONDS,
            ),
            "prompt_input_token_budget": _int_environment(
                PROMPT_BUDGET_ENVIRONMENT_VARIABLE,
                PROMPT_INPUT_TOKEN_BUDGET,
                minimum=1,
            ),
        }
        settings.update(overrides)
        return cls(provider=profile.label, model=model, api_key=api_key, **settings)

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
        structured_mode: str | None = None,
    ) -> dict[str, Any]:
        """Return the exact request body, which never contains credentials.

        The body is the one this model sends first. ``structured_mode`` renders
        the body for a different mechanism, which is how the exact payload of a
        fallback attempt stays reproducible from its recorded hash.
        """
        mode = structured_mode or self.structured_mode
        if mode not in {STRUCTURED_MODE_RESPONSE_FORMAT, STRUCTURED_MODE_PROMPTED}:
            raise ValueError("structured_mode must be 'response_format' or 'prompted'")
        system = (
            instructions
            if mode == STRUCTURED_MODE_RESPONSE_FORMAT
            else prompted_instructions(instructions=instructions, name=name, schema=schema)
        )
        request: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": canonical_json(payload)},
            ],
            "max_tokens": max_output_tokens,
        }
        if mode == STRUCTURED_MODE_RESPONSE_FORMAT:
            request["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": name, "strict": True, "schema": dict(schema)},
            }
        if self.reasoning_effort is not None:
            request["reasoning_effort"] = self.reasoning_effort
        return request

    def structured_json(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> StructuredTextResult:
        """Run one strict-schema call and return checked JSON with call details.

        The caller owns the instructions, the schema, and the payload. This
        method owns the transport: schema validity, prompt budget, structured
        mechanism, retries, local schema validation, and secret-free telemetry.
        """
        request = self.secret_free_request(
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
        prompt = str(request["messages"][1]["content"])
        counter = self._counter()
        prompt_sha256 = _sha256_text(prompt)
        # The declared request identity is the body this model sends first; it is
        # what ``secret_free_request`` returns and what a run stores next to the
        # response. Every attempt additionally records the hash of exactly what
        # went out on that attempt, so a mid-call mechanism change stays visible.
        request_sha256 = _sha256_text(canonical_json(request))
        prompt_tokens = counter.count(instructions + "\n" + prompt)
        # The schema is billed either way — inside ``response_format`` or inside
        # the prompted instructions — so it belongs in the budget comparison.
        schema_tokens = counter.count(canonical_json(dict(schema)))
        identity = _CallIdentity(
            schema_name=name,
            prompt_sha256=prompt_sha256,
            request_sha256=request_sha256,
            prompt_tokens=prompt_tokens,
            schema_tokens=schema_tokens,
            counter=counter,
            max_output_tokens=max_output_tokens,
            call_started=time.monotonic(),
        )
        try:
            Draft202012Validator.check_schema(dict(schema))
        except SchemaError as error:
            raise InvalidOutputSchemaError(
                "Structured-text output schema is not a valid JSON Schema",
                call=self._call_details(
                    identity=identity,
                    attempts=[],
                    response=None,
                    status="invalid_output_schema",
                    structured_mode=self.structured_mode,
                ),
            ) from error
        if prompt_tokens + schema_tokens + self.prompt_safety_margin_tokens > self.prompt_input_token_budget:
            raise PromptBudgetExceededError(
                "Structured-text prompt exceeds the declared input-token budget",
                call=self._call_details(
                    identity=identity,
                    attempts=[],
                    response=None,
                    status="prompt_budget_exceeded",
                    structured_mode=self.structured_mode,
                ),
            )

        attempts: list[dict[str, Any]] = []
        last_error: BaseException | None = None
        mode = self.structured_mode
        fallback_used = False
        extra_attempts = 0
        attempt_index = 0
        while True:
            sent = (
                request
                if mode == self.structured_mode
                else self.secret_free_request(
                    name=name,
                    schema=schema,
                    instructions=instructions,
                    payload=payload,
                    max_output_tokens=max_output_tokens,
                    structured_mode=mode,
                )
            )
            attempt_started = time.monotonic()
            attempt: dict[str, Any] = {
                "attempt": attempt_index + 1,
                "status": "started",
                "structured_mode": mode,
                "request_sha256": _sha256_text(canonical_json(sent)),
            }
            try:
                response = self._client.chat.completions.create(**sent)
                usage = getattr(response, "usage", None)
                finish_reason = _finish_reason(response)
                attempt.update(
                    {
                        "status": str(finish_reason or "completed"),
                        "duration_ms": round((time.monotonic() - attempt_started) * 1_000, 3),
                        "response_id": getattr(response, "id", None),
                        "request_id": getattr(response, "_request_id", None),
                        "finish_reason": finish_reason,
                        "input_tokens": _usage_tokens(usage, "prompt_tokens", "input_tokens"),
                        "output_tokens": _usage_tokens(usage, "completion_tokens", "output_tokens"),
                        "total_tokens": _usage_tokens(usage, "total_tokens"),
                    }
                )
                text = _message_text(response)
                attempt["response_sha256"] = _sha256_text(text or "")
                if finish_reason not in {None, "stop", "end_turn", "completed"}:
                    raise IncompleteStructuredResponseError(
                        f"OpenAI-compatible response ended with finish reason {finish_reason!r}"
                    )
                if not text:
                    raise IncompleteStructuredResponseError("OpenAI-compatible response had no message text")
                value, unfenced = _parse_json_object(text)
                attempt["response_unfenced"] = unfenced
                if _schema_violations(schema, value):
                    attempt["status"] = "schema_invalid"
                    attempts.append(attempt)
                    raise StructuredOutputSchemaError(
                        "OpenAI-compatible response violated the output schema",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=response,
                            status="failed",
                            structured_mode=mode,
                            response_text=text,
                            response_unfenced=unfenced,
                            schema_validated_locally=False,
                        ),
                    )
                attempt["status"] = "completed"
                attempts.append(attempt)
                return StructuredTextResult(
                    output=value,
                    call=self._call_details(
                        identity=identity,
                        attempts=attempts,
                        response=response,
                        status="completed",
                        structured_mode=mode,
                        response_text=text,
                        response_unfenced=unfenced,
                        schema_validated_locally=True,
                    ),
                )
            except StructuredOutputSchemaError:
                raise
            except Exception as error:
                last_error = error
                attempt.setdefault(
                    "duration_ms",
                    round((time.monotonic() - attempt_started) * 1_000, 3),
                )
                attempt.update(_safe_error_details(error))
                attempt["status"] = "error"
                if (
                    mode == STRUCTURED_MODE_RESPONSE_FORMAT
                    and self.structured_mode_fallback_allowed
                    and not fallback_used
                    and _response_format_rejected(error)
                ):
                    # The endpoint refused the strict-schema field. Falling back
                    # to schema-embedded instructions changes the mechanism, not
                    # the contract: the response is still checked locally.
                    attempt["status"] = "response_format_rejected"
                    attempt["structured_mode_fallback"] = True
                    attempts.append(attempt)
                    fallback_used = True
                    extra_attempts += 1
                    mode = STRUCTURED_MODE_PROMPTED
                    attempt_index += 1
                    continue
                attempts.append(attempt)
                if not _retryable_error(error):
                    raise OpenAICompatibleProviderCallError(
                        f"OpenAI-compatible structured call failed with {type(error).__name__}",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=None,
                            status="failed",
                            structured_mode=mode,
                        ),
                    ) from error
                if attempt_index >= self.max_retries + extra_attempts:
                    break
                delay = self.retry_base_seconds * (2**attempt_index)
                if delay:
                    time.sleep(delay)
                attempt_index += 1
        assert last_error is not None
        raise OpenAICompatibleProviderExhaustedError(
            f"OpenAI-compatible structured call exhausted {len(attempts)} attempts; "
            f"last error was {type(last_error).__name__}",
            call=self._call_details(
                identity=identity,
                attempts=attempts,
                response=None,
                status="retry_exhausted",
                structured_mode=mode,
            ),
        ) from last_error

    def _counter(self) -> TiktokenCounter:
        if self._token_counter is None:
            self._token_counter = TiktokenCounter(self.tokenizer)
        return self._token_counter

    def _call_details(
        self,
        *,
        identity: _CallIdentity,
        attempts: list[dict[str, Any]],
        response: object | None,
        status: str,
        structured_mode: str,
        response_text: str | None = None,
        response_unfenced: bool = False,
        schema_validated_locally: bool = False,
    ) -> dict[str, Any]:
        """Build the secret-free record of one logical call.

        Every key in ``SHARED_CALL_DETAIL_KEYS`` is emitted on every path, with
        ``None`` or an empty value where the call died before the value existed,
        so this arm's receipts stay interchangeable with the other arms'. The
        arm-specific keys — provider identity, structured mechanism, and the
        honesty of the token counts — sit alongside them, the way the Codex arm
        carries its own transport facts.
        """
        usage = getattr(response, "usage", None)
        return {
            "provider": self.provider,
            "provider_family": PROVIDER_FAMILY,
            "transport": TRANSPORT,
            "base_url": self.base_url,
            "base_url_host": self.base_url_host,
            "model_id": self.model_id,
            "model": self.model,
            "schema_name": identity.schema_name,
            "response_id": getattr(response, "id", None),
            "response_model": str(getattr(response, "model", None) or self.model),
            "status": status,
            "duration_ms": round((time.monotonic() - identity.call_started) * 1_000, 3),
            "input_tokens": _usage_tokens(usage, "prompt_tokens", "input_tokens"),
            "output_tokens": _usage_tokens(usage, "completion_tokens", "output_tokens"),
            "total_tokens": _usage_tokens(usage, "total_tokens"),
            "attempt_count": len(attempts),
            "retry_count": max(0, len(attempts) - 1),
            "attempts": [dict(attempt) for attempt in attempts],
            "prompt_sha256": identity.prompt_sha256,
            "request_sha256": identity.request_sha256,
            "response_sha256": _sha256_text(response_text) if response_text is not None else None,
            "prompt_token_estimate": identity.prompt_tokens,
            "schema_token_estimate": identity.schema_tokens,
            "prompt_input_token_budget": self.prompt_input_token_budget,
            "prompt_safety_margin_tokens": self.prompt_safety_margin_tokens,
            "tokenizer": identity.counter.name,
            "tokenizer_version": identity.counter.version,
            "token_count_method": TOKEN_COUNT_METHOD,
            "token_count_is_estimate": True,
            "max_output_tokens": identity.max_output_tokens,
            "reasoning_effort": self.reasoning_effort,
            "structured_mode": structured_mode,
            "structured_mode_requested": self.requested_structured_mode,
            "structured_mode_fallback": structured_mode != self.structured_mode,
            "response_unfenced": response_unfenced,
            "store": False,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "sdk_max_retries": 0,
            "schema_validated_locally": schema_validated_locally,
        }


@dataclass(frozen=True)
class _CallIdentity:
    """Values that identify one logical call across all of its attempts."""

    schema_name: str
    prompt_sha256: str
    request_sha256: str
    prompt_tokens: int
    schema_tokens: int
    counter: TiktokenCounter
    max_output_tokens: int
    call_started: float


def resolve_provider_profile(provider: str) -> ProviderProfile:
    """Return the named profile, refusing an unregistered label."""
    profile = PROVIDER_PROFILES.get(provider)
    if profile is None:
        raise UnknownProviderProfileError(
            f"unknown provider {provider!r}; known providers are " + ", ".join(sorted(PROVIDER_PROFILES))
        )
    return profile


def resolve_base_url(profile: ProviderProfile, base_url: str | None = None) -> str:
    """Resolve and normalize a profile's base URL without keeping credentials.

    Any userinfo, query, or fragment is dropped: a base URL reaches receipts, so
    it must not be able to carry a token someone embedded in it.
    """
    resolved = base_url or profile.base_url
    if not resolved and profile.base_url_environment_variable:
        resolved = os.environ.get(profile.base_url_environment_variable)
    if not resolved:
        variable = profile.base_url_environment_variable
        hint = f" (set {variable})" if variable else ""
        raise ProviderConfigurationError(f"{profile.label}: no base URL{hint}")
    parsed = urlparse(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ProviderConfigurationError(f"{profile.label}: base URL must be an http(s) URL with a host")
    if profile.loopback_only and parsed.hostname not in LOOPBACK_HOSTS:
        raise ProviderConfigurationError(f"{profile.label}: base URL must be loopback for a provider labeled local")
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{parsed.hostname}{port}"
    return urlunparse((parsed.scheme, netloc, parsed.path.rstrip("/"), "", "", ""))


def prompted_instructions(*, instructions: str, name: str, schema: Mapping[str, Any]) -> str:
    """Build the schema-embedded instructions used when json_schema is refused."""
    return "\n\n".join(
        (
            instructions,
            PROMPTED_SCHEMA_INSTRUCTIONS,
            f"JSON Schema (name: {name}):",
            canonical_json(dict(schema)),
        )
    )


def _parse_json_object(text: str) -> tuple[dict[str, Any], bool]:
    """Parse one JSON object, unwrapping a whole-response code fence.

    Removing a fence that wraps the entire response is a framing correction, not
    a content repair: nothing inside the fence is edited, and the parsed value
    still has to validate against the caller's schema or be rejected. Anything
    less mechanical — JSON embedded in prose, two objects, a trailing sentence —
    is a rejection.
    """
    stripped = text.strip()
    unfenced = False
    if stripped.startswith("```") and stripped.endswith("```") and len(stripped) > 6:
        body = stripped[3:-3]
        newline = body.find("\n")
        # Drop the info string ("json") that follows the opening fence.
        if newline != -1 and not body[:newline].strip().startswith("{"):
            body = body[newline + 1 :]
        stripped = body.strip()
        unfenced = True
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError as error:
        raise IncompleteStructuredResponseError("OpenAI-compatible response was not valid JSON") from error
    if not isinstance(value, dict):
        raise IncompleteStructuredResponseError("OpenAI-compatible response root was not an object")
    return value, unfenced


def _schema_violations(schema: Mapping[str, Any], value: Mapping[str, Any]) -> list[str]:
    """Check the parsed response against the declared schema, locally."""
    validator = Draft202012Validator(dict(schema))
    return sorted("/".join(str(part) for part in error.path) for error in validator.iter_errors(value))


def _first_choice(response: object | None) -> object | None:
    choices = getattr(response, "choices", None)
    if isinstance(choices, Sequence) and choices:
        return choices[0]
    return None


def _finish_reason(response: object | None) -> str | None:
    choice = _first_choice(response)
    reason = getattr(choice, "finish_reason", None)
    return str(reason) if reason else None


def _message_text(response: object | None) -> str:
    """Return the assistant text, accepting the string and part-list shapes."""
    message = getattr(_first_choice(response), "message", None)
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, Sequence):
        parts: list[str] = []
        for part in content:
            text = getattr(part, "text", None)
            if text is None and isinstance(part, Mapping):
                text = part.get("text")
            if isinstance(text, str):
                parts.append(text)
        return "".join(parts)
    return ""


def _usage_tokens(usage: object | None, *names: str) -> int:
    """Read a usage count under either the object or the plain-dict shape."""
    fields: Mapping[str, Any] = cast("Mapping[str, Any]", usage) if isinstance(usage, Mapping) else {}
    for name in names:
        value = getattr(usage, name, None)
        if value is None:
            value = fields.get(name)
        if value:
            return int(value)
    return 0


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _provider_error_code(error: BaseException) -> str | None:
    direct = getattr(error, "code", None)
    if direct:
        return str(direct)
    body = getattr(error, "body", None)
    if not isinstance(body, dict):
        return None
    nested = body.get("error")
    if isinstance(nested, dict) and nested.get("code"):
        return str(nested["code"])
    if body.get("code"):
        return str(body["code"])
    return None


def _response_format_rejected(error: BaseException) -> bool:
    """Did the endpoint refuse the strict-schema request field itself?

    Decided from the status code alone. Provider prose is not read for this — it
    is not stable across providers, and reading it invites copying it somewhere.
    """
    if _provider_error_code(error) == "insufficient_quota":
        return False
    status_code = getattr(error, "status_code", None)
    return status_code in {400, 404, 415, 422, 501}


def _retryable_error(error: BaseException) -> bool:
    # ``insufficient_quota`` is a spend/credit hard-limit condition. Repeating
    # the same request cannot clear it; traffic resumes only after the
    # applicable limit or billing state changes.
    if _provider_error_code(error) == "insufficient_quota":
        return False
    if isinstance(error, (TimeoutError, ConnectionError, IncompleteStructuredResponseError)):
        return True
    if type(error).__name__ in {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
    }:
        return True
    status_code = getattr(error, "status_code", None)
    return status_code in {408, 409, 429} or (isinstance(status_code, int) and status_code >= 500)


def _safe_error_details(error: BaseException) -> dict[str, Any]:
    """Describe a failure without copying provider text, prompts, or keys."""
    details: dict[str, Any] = {"error_code": type(error).__name__}
    status_code = getattr(error, "status_code", None)
    if isinstance(status_code, int):
        details["status_code"] = status_code
    provider_error_code = _provider_error_code(error)
    if provider_error_code:
        details["provider_error_code"] = provider_error_code
    request_id = getattr(error, "request_id", None)
    if request_id:
        details["request_id"] = str(request_id)
    return details


def _float_environment(name: str, default: float, *, positive: bool = False) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not a number; using {}", name, default)
        return default
    if value < 0 or (positive and value == 0):
        logger.warning("{} must be {}; using {}", name, "positive" if positive else "nonnegative", default)
        return default
    return value


def _int_environment(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default
    if value < minimum:
        logger.warning("{} must be at least {}; using {}", name, minimum, default)
        return default
    return value

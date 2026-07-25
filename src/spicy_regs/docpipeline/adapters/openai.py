"""OpenAI arm of the v3 structured-text-model interface.

The v3 design keeps exactly four small provider interfaces. This module
implements the first one: instructions, a strict JSON schema, a source payload,
and an output token limit go in; checked JSON and secret-free call details come
back together in one immutable :class:`StructuredTextResult`. There is no
mutable ``last_call_metadata`` side channel — a failure carries its own call
details on the raised error.

The transport, application-owned retry loop, prompt budget, and secret-free
telemetry are copied from the ontology OpenAI provider. Tag-specific behavior
(tag proposals, evidence alignment, ontology payloads) deliberately stays out:
prompts and schemas belong to the pipeline step, not to the provider.

Only ``StructuredTextCallError.call`` is receipt-safe. A raised error's message
and its ``__cause__`` chain may contain provider-supplied text — including key
fragments the provider echoes back in an authentication failure — so neither may
be written into a receipt, ledger, or checkpoint.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import version as installed_version
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from loguru import logger

from spicy_regs.docpipeline.adapters import (
    DEFAULT_MODEL,
    DEFAULT_REASONING_EFFORT,
    SUPPORTED_REASONING_EFFORTS,
    StructuredTextCallError,
    StructuredTextResult,
)
from spicy_regs.ontology.common import canonical_json

PROVIDER = "openai"
TRANSPORT = "openai-responses"
DEFAULT_SERVICE_TIER = "auto"
SUPPORTED_SERVICE_TIERS = frozenset({"auto", "default", "flex", "scale", "priority"})
DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_SECONDS = 1.0
DEFAULT_TOKENIZER = "o200k_base"
PROMPT_INPUT_TOKEN_BUDGET = 8_192
PROMPT_SAFETY_MARGIN_TOKENS = 1_024

API_KEY_ENVIRONMENT_VARIABLE = "OPENAI_API_KEY"
MODEL_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_MODEL"
REASONING_EFFORT_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_REASONING_EFFORT"
SERVICE_TIER_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_SERVICE_TIER"
TIMEOUT_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_TIMEOUT_SECONDS"
MAX_RETRIES_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_MAX_RETRIES"
RETRY_BASE_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_RETRY_BASE_SECONDS"
PROMPT_BUDGET_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_PROMPT_INPUT_TOKEN_BUDGET"


class PromptBudgetExceededError(StructuredTextCallError, ValueError):
    """A deterministic prompt exceeded the declared input budget."""


class InvalidOutputSchemaError(StructuredTextCallError, ValueError):
    """The caller's schema is not a valid JSON Schema, so no call was made."""


class IncompleteStructuredResponseError(StructuredTextCallError):
    """A provider response ended before a usable structured value."""


class StructuredOutputSchemaError(StructuredTextCallError):
    """A complete provider response violated the declared output schema."""


class OpenAIProviderCallError(StructuredTextCallError):
    """A non-retryable provider failure ended the call."""


class OpenAIProviderExhaustedError(StructuredTextCallError):
    """All application-owned provider attempts failed."""


class TiktokenCounter:
    """Pinned OpenAI-compatible token counter.

    ``tiktoken`` is a provider library, so it lives in ``adapters/`` and is
    imported lazily: keyless CI and deterministic rebuilds never need it.
    """

    def __init__(self, encoding_name: str = DEFAULT_TOKENIZER) -> None:
        import tiktoken

        self.name = encoding_name
        self.version = installed_version("tiktoken")
        self._encoding = tiktoken.get_encoding(encoding_name)

    def count(self, text: str) -> int:
        return len(self._encoding.encode(text, disallowed_special=()))


class OpenAIStructuredTextModel:
    """Responses API provider using strict JSON-schema output.

    The SDK import is lazy, so deterministic rollups and keyless CI do not need
    to initialize an API client. Tests inject a client through ``client=``
    instead of reaching into provider internals. The application owns retries:
    the SDK is configured with ``max_retries=0`` so every physical call appears
    in the returned call details.
    """

    production_provider = True

    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: Any | None = None,
        model: str = DEFAULT_MODEL,
        reasoning_effort: str = DEFAULT_REASONING_EFFORT,
        service_tier: str = DEFAULT_SERVICE_TIER,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
        prompt_input_token_budget: int = PROMPT_INPUT_TOKEN_BUDGET,
        prompt_safety_margin_tokens: int = PROMPT_SAFETY_MARGIN_TOKENS,
        tokenizer: str = DEFAULT_TOKENIZER,
    ) -> None:
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
        if reasoning_effort not in SUPPORTED_REASONING_EFFORTS:
            raise ValueError("reasoning_effort must be one of " + ", ".join(sorted(SUPPORTED_REASONING_EFFORTS)))
        if service_tier not in SUPPORTED_SERVICE_TIERS:
            raise ValueError("service_tier must be one of " + ", ".join(sorted(SUPPORTED_SERVICE_TIERS)))
        self.model = model
        self.model_id = f"{PROVIDER}:{model}"
        self.reasoning_effort = reasoning_effort
        self.service_tier = service_tier
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_base_seconds = retry_base_seconds
        self.prompt_input_token_budget = prompt_input_token_budget
        self.prompt_safety_margin_tokens = prompt_safety_margin_tokens
        self.tokenizer = tokenizer
        self.run_configuration: dict[str, Any] = {
            "provider": PROVIDER,
            "transport": TRANSPORT,
            "model": model,
            "model_id": self.model_id,
            "reasoning_effort": reasoning_effort,
            "service_tier": service_tier,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "retry_base_seconds": retry_base_seconds,
            "sdk_max_retries": 0,
            "store": False,
            "prompt_input_token_budget": prompt_input_token_budget,
            "prompt_safety_margin_tokens": prompt_safety_margin_tokens,
            "tokenizer": tokenizer,
        }
        self._token_counter: TiktokenCounter | None = None
        if client is not None:
            self._client = client
            return
        if not api_key:
            raise ValueError("api_key is required when no client is injected")
        from openai import OpenAI

        self._client = OpenAI(
            api_key=api_key,
            timeout=timeout_seconds,
            # The application owns retries so every physical call is visible in
            # checkpoint, ledger, and receipt telemetry.
            max_retries=0,
        )

    @classmethod
    def from_environment(cls) -> OpenAIStructuredTextModel | None:
        """Build the provider from the environment, or ``None`` without a key."""
        api_key = os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
        if not api_key:
            logger.warning(
                "Document pipeline: {} is unset — the OpenAI arm is unavailable",
                API_KEY_ENVIRONMENT_VARIABLE,
            )
            return None
        return cls(
            api_key=api_key,
            model=os.environ.get(MODEL_ENVIRONMENT_VARIABLE, DEFAULT_MODEL),
            reasoning_effort=os.environ.get(
                REASONING_EFFORT_ENVIRONMENT_VARIABLE,
                DEFAULT_REASONING_EFFORT,
            ),
            service_tier=os.environ.get(
                SERVICE_TIER_ENVIRONMENT_VARIABLE,
                DEFAULT_SERVICE_TIER,
            ),
            timeout_seconds=_positive_float_environment(
                TIMEOUT_ENVIRONMENT_VARIABLE,
                DEFAULT_TIMEOUT_SECONDS,
            ),
            max_retries=_nonnegative_int_environment(
                MAX_RETRIES_ENVIRONMENT_VARIABLE,
                DEFAULT_MAX_RETRIES,
            ),
            retry_base_seconds=_nonnegative_float_environment(
                RETRY_BASE_ENVIRONMENT_VARIABLE,
                DEFAULT_RETRY_BASE_SECONDS,
            ),
            prompt_input_token_budget=_positive_int_environment(
                PROMPT_BUDGET_ENVIRONMENT_VARIABLE,
                PROMPT_INPUT_TOKEN_BUDGET,
            ),
        )

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        """Return the exact request body, which never contains credentials."""
        return {
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
                    "schema": dict(schema),
                }
            },
        }

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
        method owns the transport: schema validity, prompt budget, retries,
        response checking, local schema validation, and secret-free telemetry.
        """
        request = self.secret_free_request(
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
        prompt = str(request["input"])
        counter = self._counter()
        request_sha256 = _sha256_text(canonical_json(request))
        prompt_sha256 = _sha256_text(prompt)
        prompt_tokens = counter.count(instructions + "\n" + prompt)
        # The schema ships inside ``text.format`` and is billed as input, so it
        # belongs in the budget comparison alongside the instructions and prompt.
        schema_tokens = counter.count(canonical_json(dict(schema)))
        call_started = time.monotonic()
        details = _CallIdentity(
            schema_name=name,
            prompt_sha256=prompt_sha256,
            request_sha256=request_sha256,
            prompt_tokens=prompt_tokens,
            schema_tokens=schema_tokens,
            counter=counter,
            max_output_tokens=max_output_tokens,
            reasoning_effort=str(request["reasoning"]["effort"]),
            call_started=call_started,
        )
        try:
            Draft202012Validator.check_schema(dict(schema))
        except SchemaError as error:
            raise InvalidOutputSchemaError(
                "Structured-text output schema is not a valid JSON Schema",
                call=self._call_details(
                    identity=details,
                    attempts=[],
                    response=None,
                    status="invalid_output_schema",
                ),
            ) from error
        if prompt_tokens + schema_tokens + self.prompt_safety_margin_tokens > self.prompt_input_token_budget:
            raise PromptBudgetExceededError(
                "Structured-text prompt exceeds the declared input-token budget",
                call=self._call_details(
                    identity=details,
                    attempts=[],
                    response=None,
                    status="prompt_budget_exceeded",
                ),
            )

        attempts: list[dict[str, Any]] = []
        last_error: BaseException | None = None
        for attempt_index in range(self.max_retries + 1):
            attempt_started = time.monotonic()
            attempt: dict[str, Any] = {"attempt": attempt_index + 1, "status": "started"}
            try:
                response = self._client.responses.create(**request)
                status = str(getattr(response, "status", None) or "completed")
                usage = getattr(response, "usage", None)
                attempt = {
                    "attempt": attempt_index + 1,
                    "status": status,
                    "duration_ms": round((time.monotonic() - attempt_started) * 1_000, 3),
                    "response_id": getattr(response, "id", None),
                    "request_id": getattr(response, "_request_id", None),
                    "input_tokens": _usage_tokens(usage, "input_tokens"),
                    "output_tokens": _usage_tokens(usage, "output_tokens"),
                    "total_tokens": _usage_tokens(usage, "total_tokens"),
                }
                if status != "completed":
                    raise IncompleteStructuredResponseError(
                        f"OpenAI structured response ended with status {status!r}"
                    )
                output_text = getattr(response, "output_text", None)
                if not output_text:
                    raise IncompleteStructuredResponseError("OpenAI structured response had no output text")
                try:
                    value = json.loads(output_text)
                except json.JSONDecodeError as error:
                    raise IncompleteStructuredResponseError(
                        "OpenAI structured response was not valid JSON"
                    ) from error
                if not isinstance(value, dict):
                    raise IncompleteStructuredResponseError("OpenAI structured response root was not an object")
                if _schema_violations(schema, value):
                    attempt["status"] = "schema_invalid"
                    attempts.append(attempt)
                    raise StructuredOutputSchemaError(
                        "OpenAI structured response violated the output schema",
                        call=self._call_details(
                            identity=details,
                            attempts=attempts,
                            response=response,
                            status="failed",
                            schema_validated_locally=False,
                        ),
                    )
                attempt["status"] = "completed"
                attempts.append(attempt)
                return StructuredTextResult(
                    output=value,
                    call=self._call_details(
                        identity=details,
                        attempts=attempts,
                        response=response,
                        status="completed",
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
                attempts.append(attempt)
                retryable = _retryable_error(error)
                exhausted = attempt_index >= self.max_retries
                if not retryable:
                    raise OpenAIProviderCallError(
                        f"OpenAI structured call failed with {type(error).__name__}",
                        call=self._call_details(
                            identity=details,
                            attempts=attempts,
                            response=None,
                            status="failed",
                        ),
                    ) from error
                if exhausted:
                    break
                delay = self.retry_base_seconds * (2**attempt_index)
                if delay:
                    time.sleep(delay)
        assert last_error is not None
        raise OpenAIProviderExhaustedError(
            f"OpenAI structured call exhausted {len(attempts)} attempts; "
            f"last error was {type(last_error).__name__}",
            call=self._call_details(
                identity=details,
                attempts=attempts,
                response=None,
                status="retry_exhausted",
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
        schema_validated_locally: bool = False,
    ) -> dict[str, Any]:
        """Build the secret-free record of one logical call.

        ``schema_validated_locally`` is emitted on every path, ``False`` when no
        local check ran, so a receipt reader never has to infer a missing key.
        """
        usage = getattr(response, "usage", None)
        details: dict[str, Any] = {
            "provider": PROVIDER,
            "transport": TRANSPORT,
            "model_id": self.model_id,
            "schema_name": identity.schema_name,
            "response_id": getattr(response, "id", None),
            "response_model": str(getattr(response, "model", None) or self.model),
            "status": status,
            "duration_ms": round((time.monotonic() - identity.call_started) * 1_000, 3),
            "input_tokens": _usage_tokens(usage, "input_tokens"),
            "output_tokens": _usage_tokens(usage, "output_tokens"),
            "total_tokens": _usage_tokens(usage, "total_tokens"),
            "attempt_count": len(attempts),
            "retry_count": max(0, len(attempts) - 1),
            "attempts": [dict(attempt) for attempt in attempts],
            "prompt_sha256": identity.prompt_sha256,
            "request_sha256": identity.request_sha256,
            "prompt_token_estimate": identity.prompt_tokens,
            "schema_token_estimate": identity.schema_tokens,
            "prompt_input_token_budget": self.prompt_input_token_budget,
            "prompt_safety_margin_tokens": self.prompt_safety_margin_tokens,
            "tokenizer": identity.counter.name,
            "tokenizer_version": identity.counter.version,
            "max_output_tokens": identity.max_output_tokens,
            "reasoning_effort": identity.reasoning_effort,
            "requested_service_tier": self.service_tier,
            "response_service_tier": getattr(response, "service_tier", None),
            "store": False,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "sdk_max_retries": 0,
            "schema_validated_locally": schema_validated_locally,
        }
        return details


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
    reasoning_effort: str
    call_started: float


def _schema_violations(schema: Mapping[str, Any], value: Mapping[str, Any]) -> list[str]:
    """Check the parsed response against the declared schema, locally."""
    validator = Draft202012Validator(dict(schema))
    return sorted("/".join(str(part) for part in error.path) for error in validator.iter_errors(value))


def _usage_tokens(usage: object | None, key: str) -> int:
    value = getattr(usage, key, 0)
    return int(value or 0)


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


def _retryable_error(error: BaseException) -> bool:
    # OpenAI documents ``insufficient_quota`` as a spend/credit hard-limit
    # condition. Repeating the same request cannot clear it; traffic resumes
    # only after the applicable limit or billing state changes.
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


def _positive_float_environment(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not a number; using {}", name, default)
        return default
    if value <= 0:
        logger.warning("{} must be positive; using {}", name, default)
        return default
    return value


def _positive_int_environment(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default
    if value <= 0:
        logger.warning("{} must be positive; using {}", name, default)
        return default
    return value


def _nonnegative_int_environment(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default
    if value < 0:
        logger.warning("{} must be nonnegative; using {}", name, default)
        return default
    return value


def _nonnegative_float_environment(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        logger.warning("{} is not a number; using {}", name, default)
        return default
    if value < 0:
        logger.warning("{} must be nonnegative; using {}", name, default)
        return default
    return value

"""Native Anthropic arm of the v3 structured-text-model interface.

This is the fourth provider arm for the same interface as ``adapters/openai.py``,
``adapters/codex_cli.py``, and ``adapters/openai_compatible.py``: the same
(instructions, schema, payload, ``max_output_tokens``) call, and checked JSON
plus secret-free call details returned together in one immutable
:class:`StructuredTextResult`. There is no mutable ``last_call_metadata`` side
channel; a failure carries its own call details on the raised error.

Why it exists, and what makes it different from the compat arm: Anthropic's
OpenAI-compatibility endpoint accepts ``response_format`` json_schema and
*ignores* it, which silently downgrades a strict-schema call into an
unconstrained one. The native Messages API has real schema enforcement —
``output_config.format`` with ``{"type": "json_schema", "schema": ...}`` — so
Claude-family calls made through this arm are enforced by the provider, not
merely requested. That is the whole reason to take the ``anthropic`` dependency,
and it is why this arm has **one mode only**: there is no prompted fallback. A
schema the endpoint cannot enforce is a refusal — before any paid call when the
unenforceable construct is detectable locally, and on the API's own rejection
otherwise.

Two honesty properties this arm owes its receipts:

* **Enforcement.** ``schema_enforcement`` names the mechanism that actually
  constrained the answer, and ``schema_enforced_by_provider`` is ``True`` on
  every path that reached the provider. The response is *still* validated
  locally against the caller's schema, and a response that fails is rejected —
  never repaired, and never unfenced or scavenged out of prose. On an enforced
  route a malformed answer means enforcement did not hold, which is a fact to
  report, not a mess to clean up.
* **Token counting.** This arm uses the SDK's native ``messages.count_tokens``
  surface, so the number in the receipt is the provider's own count of the exact
  request (system, message, and the schema inside ``output_config``) rather than
  a foreign-tokenizer estimate. ``token_count_method`` and
  ``token_count_is_estimate`` record that. Tests stay network-free because the
  count goes through the same injected client as the call.

The SDK import is lazy and absolute: this module is named ``anthropic`` inside
``spicy_regs.docpipeline.adapters``, and ``from anthropic import Anthropic``
resolves to the installed top-level package, never to this file.

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
from typing import Any, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from loguru import logger

from spicy_regs.docpipeline.adapters import (
    StructuredTextCallError,
    StructuredTextResult,
)
from spicy_regs.ontology.common import canonical_json

PROVIDER = "anthropic"
PROVIDER_FAMILY = "anthropic-native"
TRANSPORT = "anthropic-messages"

#: The schema-enforcement mechanism this arm uses, recorded on every receipt.
#: Verified against the installed SDK: ``anthropic.types.OutputConfigParam`` has
#: a ``format`` member typed ``JSONOutputFormatParam``
#: (``{"type": "json_schema", "schema": {...}}``), accepted by the GA
#: ``client.messages.create`` — no beta flag is required at the pinned version.
SCHEMA_ENFORCEMENT = "anthropic-output-config-json-schema"

#: Counts come from ``client.messages.count_tokens``, the provider's own
#: tokenizer, over the same shaping fields the call sends. Not an estimate.
TOKEN_COUNT_METHOD = "anthropic-count-tokens"

DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_SECONDS = 1.0
PROMPT_INPUT_TOKEN_BUDGET = 8_192
PROMPT_SAFETY_MARGIN_TOKENS = 1_024

API_KEY_ENVIRONMENT_VARIABLE = "ANTHROPIC_API_KEY"
TIMEOUT_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_TIMEOUT_SECONDS"
MAX_RETRIES_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_MAX_RETRIES"
RETRY_BASE_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_RETRY_BASE_SECONDS"
PROMPT_BUDGET_ENVIRONMENT_VARIABLE = "SPICY_REGS_DOCPIPELINE_PROMPT_INPUT_TOKEN_BUDGET"

#: The effort ladder ``output_config.effort`` accepts, read off the pinned SDK's
#: ``OutputConfigParam.effort`` literal. The shared interface also names
#: ``"none"``; Anthropic has no such level, so omitting the effort (``None``) is
#: how a caller asks for the model's own default.
SUPPORTED_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})

#: A completed turn that produced a final answer. Every other stop reason is a
#: failure for a structured call, and the receipt names which one it was.
COMPLETED_STOP_REASONS = frozenset({"end_turn", "stop_sequence"})

#: Stop reasons a repeat of the same request cannot clear: the answer did not
#: fit the declared output budget or the model's window. Retrying burns money
#: for the same outcome, so these end the call the way a spend limit does.
OUTPUT_BUDGET_STOP_REASONS = frozenset({"max_tokens", "model_context_window_exceeded"})

#: Anthropic's spend/credit hard-limit condition, the analog of OpenAI's
#: ``insufficient_quota``. Traffic resumes only after the billing state changes.
NON_RETRYABLE_PROVIDER_ERROR_TYPES = frozenset({"billing_error"})

# --- what ``output_config.format`` can actually enforce -----------------------
#
# These sets are read off the pinned SDK's own schema converter,
# ``anthropic/lib/_parse/_transform.py``, which is the SDK's statement of what
# survives to the endpoint: anything it drops into a description is a hint the
# model *might* follow, not a constraint the endpoint enforces. This arm does
# not use that converter — the caller's schema goes to the provider verbatim, so
# the enforced schema and the locally validated schema are the same object — and
# instead refuses, before any paid call, a schema built out of vocabulary the
# endpoint cannot enforce.

SUPPORTED_SCHEMA_TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
SUPPORTED_STRING_FORMATS = frozenset(
    {"date-time", "time", "date", "duration", "email", "hostname", "uri", "ipv4", "ipv6", "uuid"}
)
COMMON_SCHEMA_KEYWORDS = frozenset({"$defs", "$ref", "type", "anyOf", "oneOf", "allOf", "enum", "description", "title"})
OBJECT_SCHEMA_KEYWORDS = frozenset({"properties", "required", "additionalProperties"})
ARRAY_SCHEMA_KEYWORDS = frozenset({"items", "minItems"})
STRING_SCHEMA_KEYWORDS = frozenset({"format"})
#: ``minItems`` is enforced only for these values; anything else is a hint.
SUPPORTED_MINIMUM_ITEMS = frozenset({0, 1})
DEFINITION_POINTER_PREFIX = "#/$defs/"


class ProviderConfigurationError(ValueError):
    """The provider cannot be built: no credential and no injected client.

    The message names the environment variable, never its value, so a refusal
    can be logged safely.
    """


class PromptBudgetExceededError(StructuredTextCallError, ValueError):
    """A deterministic prompt exceeded the declared input budget."""


class InvalidOutputSchemaError(StructuredTextCallError, ValueError):
    """The caller's schema is not a valid JSON Schema, so no call was made."""


class UnenforceableOutputSchemaError(StructuredTextCallError, ValueError):
    """The caller's schema uses vocabulary this endpoint cannot enforce.

    This arm exists to be the enforced route, so an unenforceable schema is a
    refusal rather than a downgrade. The refusal happens before any paid call.
    """


class TokenCountUnavailableError(StructuredTextCallError):
    """The provider's token count could not be obtained, so no budget held."""


class IncompleteStructuredResponseError(StructuredTextCallError):
    """A provider response ended before a usable structured value."""


class OutputBudgetExceededError(StructuredTextCallError):
    """The answer did not fit the declared output budget or the context window."""


class StructuredOutputSchemaError(StructuredTextCallError):
    """A complete provider response violated the declared output schema."""


class AnthropicRefusalError(StructuredTextCallError):
    """The provider's safety classifiers declined the request."""


class AnthropicProviderCallError(StructuredTextCallError):
    """A non-retryable provider failure ended the call."""


class AnthropicProviderExhaustedError(StructuredTextCallError):
    """All application-owned provider attempts failed."""


class AnthropicStructuredTextModel:
    """Messages API provider using native, enforced JSON-schema output.

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
        model: str,
        api_key: str | None = None,
        client: Any | None = None,
        reasoning_effort: str | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
        prompt_input_token_budget: int = PROMPT_INPUT_TOKEN_BUDGET,
        prompt_safety_margin_tokens: int = PROMPT_SAFETY_MARGIN_TOKENS,
    ) -> None:
        if not model:
            raise ValueError("model is required: this arm never defaults a model")
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
        if reasoning_effort is not None and reasoning_effort not in SUPPORTED_EFFORTS:
            raise ValueError("reasoning_effort must be None or one of " + ", ".join(sorted(SUPPORTED_EFFORTS)))
        self.model = model
        self.model_id = f"{PROVIDER}:{model}"
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_base_seconds = retry_base_seconds
        self.prompt_input_token_budget = prompt_input_token_budget
        self.prompt_safety_margin_tokens = prompt_safety_margin_tokens
        self.run_configuration: dict[str, Any] = {
            "provider": PROVIDER,
            "provider_family": PROVIDER_FAMILY,
            "transport": TRANSPORT,
            "model": model,
            "model_id": self.model_id,
            "schema_enforcement": SCHEMA_ENFORCEMENT,
            "schema_enforced_by_provider": True,
            "reasoning_effort": reasoning_effort,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "retry_base_seconds": retry_base_seconds,
            "sdk_max_retries": 0,
            "store": False,
            "prompt_input_token_budget": prompt_input_token_budget,
            "prompt_safety_margin_tokens": prompt_safety_margin_tokens,
            "token_count_method": TOKEN_COUNT_METHOD,
            "token_count_is_estimate": False,
            "sdk_version": _installed_sdk_version(),
        }
        if client is not None:
            self._client = client
            return
        if not api_key:
            raise ProviderConfigurationError(
                f"{PROVIDER}: an API key is required when no client is injected (set {API_KEY_ENVIRONMENT_VARIABLE})"
            )
        # Absolute import: resolves to the installed ``anthropic`` distribution,
        # not to this same-named module inside the adapters package.
        from anthropic import Anthropic

        self._client = Anthropic(
            api_key=api_key,
            timeout=timeout_seconds,
            # The application owns retries so every physical call is visible in
            # checkpoint, ledger, and receipt telemetry.
            max_retries=0,
        )

    @classmethod
    def from_environment(cls, *, model: str, **overrides: Any) -> AnthropicStructuredTextModel:
        """Build the native Anthropic provider from the environment.

        Unlike the single-arm OpenAI provider, a missing credential here is a
        refusal rather than ``None``: this arm is selected deliberately — for a
        cross-family judge, or to compare an enforced route against a compat
        one — and silently dropping it would publish a comparison that never
        ran. The refusal names the environment variable, never its value.
        """
        api_key = os.environ.get(API_KEY_ENVIRONMENT_VARIABLE)
        if not api_key:
            raise ProviderConfigurationError(f"{PROVIDER}: {API_KEY_ENVIRONMENT_VARIABLE} is unset")
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
        return cls(model=model, api_key=api_key, **settings)

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        """Return the exact request body, which never contains credentials.

        The returned mapping is the literal keyword set handed to
        ``client.messages.create``, so ``request_sha256`` is a hash of exactly
        what went out. ``name`` does not appear: the Messages API's output
        format carries no schema name, so two calls whose schemas match really
        are the same request, and the name is recorded in the receipt instead.

        ``max_tokens`` is required by the Messages API and comes straight from
        the caller's output budget, so the transport ceiling and the declared
        budget can never drift apart.
        """
        output_config: dict[str, Any] = {"format": {"type": "json_schema", "schema": dict(schema)}}
        if self.reasoning_effort is not None:
            output_config["effort"] = self.reasoning_effort
        return {
            "model": self.model,
            "max_tokens": max_output_tokens,
            "system": instructions,
            "messages": [{"role": "user", "content": canonical_json(payload)}],
            "output_config": output_config,
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
        """Run one enforced-schema call and return checked JSON with call details.

        The caller owns the instructions, the schema, and the payload. This
        method owns the transport: schema validity, schema enforceability, the
        prompt budget, retries, local schema validation, and secret-free
        telemetry.
        """
        request = self.secret_free_request(
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
        prompt = str(request["messages"][0]["content"])
        identity = _CallIdentity(
            schema_name=name,
            prompt_sha256=_sha256_text(prompt),
            request_sha256=_sha256_text(canonical_json(request)),
            max_output_tokens=max_output_tokens,
            prompt_tokens=None,
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
                ),
            ) from error
        findings = unenforceable_schema_findings(schema)
        if findings:
            raise UnenforceableOutputSchemaError(
                "Structured-text output schema uses vocabulary this endpoint cannot enforce",
                call=self._call_details(
                    identity=identity,
                    attempts=[],
                    response=None,
                    status="unenforceable_output_schema",
                    schema_enforcement_findings=findings,
                ),
            )
        try:
            identity = identity.with_prompt_tokens(self._count_input_tokens(request))
        except Exception as error:
            raise TokenCountUnavailableError(
                f"Anthropic token count failed with {type(error).__name__}",
                call=self._call_details(
                    identity=identity,
                    attempts=[],
                    response=None,
                    status="token_count_unavailable",
                ),
            ) from error
        counted = identity.prompt_tokens or 0
        if counted + self.prompt_safety_margin_tokens > self.prompt_input_token_budget:
            raise PromptBudgetExceededError(
                "Structured-text prompt exceeds the declared input-token budget",
                call=self._call_details(
                    identity=identity,
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
                response = self._client.messages.create(**request)
                stop_reason = _stop_reason(response)
                usage = getattr(response, "usage", None)
                text = _message_text(response)
                attempt.update(
                    {
                        "status": stop_reason or "completed",
                        "duration_ms": round((time.monotonic() - attempt_started) * 1_000, 3),
                        "response_id": getattr(response, "id", None),
                        "request_id": getattr(response, "_request_id", None),
                        "stop_reason": stop_reason,
                        "input_tokens": _usage_tokens(usage, "input_tokens"),
                        "output_tokens": _usage_tokens(usage, "output_tokens"),
                        "total_tokens": _total_tokens(usage),
                        "response_sha256": _sha256_text(text),
                    }
                )
                if stop_reason == "refusal":
                    attempt["status"] = "refusal"
                    attempts.append(attempt)
                    raise AnthropicRefusalError(
                        "Anthropic declined the structured request",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=response,
                            status="refused",
                            response_text=text,
                        ),
                    )
                if stop_reason in OUTPUT_BUDGET_STOP_REASONS:
                    attempt["status"] = "output_budget_exceeded"
                    attempts.append(attempt)
                    raise OutputBudgetExceededError(
                        f"Anthropic structured response ended with stop reason {stop_reason!r}",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=response,
                            status="output_budget_exceeded",
                            response_text=text,
                        ),
                    )
                if stop_reason is not None and stop_reason not in COMPLETED_STOP_REASONS:
                    raise IncompleteStructuredResponseError(
                        f"Anthropic structured response ended with stop reason {stop_reason!r}"
                    )
                value = _parse_json_object(text)
                if _schema_violations(schema, value):
                    # The endpoint enforced this schema, so a violation means
                    # enforcement did not hold. That is a fact to report, not a
                    # response to repair.
                    attempt["status"] = "schema_invalid"
                    attempts.append(attempt)
                    raise StructuredOutputSchemaError(
                        "Anthropic structured response violated the enforced output schema",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=response,
                            status="failed",
                            response_text=text,
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
                        response_text=text,
                        schema_validated_locally=True,
                    ),
                )
            except (StructuredOutputSchemaError, AnthropicRefusalError, OutputBudgetExceededError):
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
                if not _retryable_error(error):
                    raise AnthropicProviderCallError(
                        f"Anthropic structured call failed with {type(error).__name__}",
                        call=self._call_details(
                            identity=identity,
                            attempts=attempts,
                            response=None,
                            status="failed",
                        ),
                    ) from error
                if attempt_index >= self.max_retries:
                    break
                delay = self.retry_base_seconds * (2**attempt_index)
                if delay:
                    time.sleep(delay)
        assert last_error is not None
        raise AnthropicProviderExhaustedError(
            f"Anthropic structured call exhausted {len(attempts)} attempts; last error was {type(last_error).__name__}",
            call=self._call_details(
                identity=identity,
                attempts=attempts,
                response=None,
                status="retry_exhausted",
            ),
        ) from last_error

    def _count_input_tokens(self, request: Mapping[str, Any]) -> int:
        """Ask the provider to count the exact request it is about to receive.

        The shaping fields are taken from the request itself, so the counted
        body and the sent body cannot drift. ``max_tokens`` is an output ceiling
        and is not an input to the count.
        """
        counted = self._client.messages.count_tokens(
            model=request["model"],
            system=request["system"],
            messages=request["messages"],
            output_config=request["output_config"],
        )
        return int(getattr(counted, "input_tokens", 0) or 0)

    def _call_details(
        self,
        *,
        identity: _CallIdentity,
        attempts: list[dict[str, Any]],
        response: object | None,
        status: str,
        response_text: str | None = None,
        schema_validated_locally: bool = False,
        schema_enforcement_findings: list[str] | None = None,
    ) -> dict[str, Any]:
        """Build the secret-free record of one logical call.

        Every key in ``SHARED_CALL_DETAIL_KEYS`` is emitted on every path, with
        ``None`` or an empty value where the call died before the value existed,
        so this arm's receipts stay interchangeable with the other arms'. The
        arm-specific keys — provider identity, the enforcement mechanism, and
        the honesty of the token counts — sit alongside them, the way the Codex
        arm carries its own transport facts.
        """
        usage = getattr(response, "usage", None)
        return {
            "provider": PROVIDER,
            "provider_family": PROVIDER_FAMILY,
            "transport": TRANSPORT,
            "model_id": self.model_id,
            "model": self.model,
            "schema_name": identity.schema_name,
            "response_id": getattr(response, "id", None),
            "response_model": str(getattr(response, "model", None) or self.model),
            "status": status,
            "duration_ms": round((time.monotonic() - identity.call_started) * 1_000, 3),
            "input_tokens": _usage_tokens(usage, "input_tokens"),
            "output_tokens": _usage_tokens(usage, "output_tokens"),
            "total_tokens": _total_tokens(usage),
            "cache_read_input_tokens": _usage_tokens(usage, "cache_read_input_tokens"),
            "cache_creation_input_tokens": _usage_tokens(usage, "cache_creation_input_tokens"),
            "attempt_count": len(attempts),
            "retry_count": max(0, len(attempts) - 1),
            "attempts": [dict(attempt) for attempt in attempts],
            "prompt_sha256": identity.prompt_sha256,
            "request_sha256": identity.request_sha256,
            "response_sha256": _sha256_text(response_text) if response_text is not None else None,
            "prompt_token_count": identity.prompt_tokens,
            "prompt_input_token_budget": self.prompt_input_token_budget,
            "prompt_safety_margin_tokens": self.prompt_safety_margin_tokens,
            "token_count_method": TOKEN_COUNT_METHOD,
            "token_count_is_estimate": False,
            "max_output_tokens": identity.max_output_tokens,
            "max_output_tokens_enforced": True,
            "reasoning_effort": self.reasoning_effort,
            "schema_enforcement": SCHEMA_ENFORCEMENT,
            "schema_enforced_by_provider": True,
            "schema_enforcement_findings": list(schema_enforcement_findings or []),
            "stop_reason": _stop_reason(response),
            "refusal_category": _refusal_category(response),
            # The Messages API is stateless: there is no server-side transcript
            # to opt out of, so nothing this arm sends is stored by the provider.
            "store": False,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "sdk_max_retries": 0,
            "sdk_version": _installed_sdk_version(),
            "schema_validated_locally": schema_validated_locally,
        }


@dataclass(frozen=True)
class _CallIdentity:
    """Values that identify one logical call across all of its attempts."""

    schema_name: str
    prompt_sha256: str
    request_sha256: str
    max_output_tokens: int
    prompt_tokens: int | None
    call_started: float

    def with_prompt_tokens(self, prompt_tokens: int) -> _CallIdentity:
        """Return the identity once the provider has counted the request."""
        return _CallIdentity(
            schema_name=self.schema_name,
            prompt_sha256=self.prompt_sha256,
            request_sha256=self.request_sha256,
            max_output_tokens=self.max_output_tokens,
            prompt_tokens=prompt_tokens,
            call_started=self.call_started,
        )


def unenforceable_schema_findings(schema: Mapping[str, Any]) -> list[str]:
    """Name the parts of a schema ``output_config.format`` cannot enforce.

    Each finding is ``"<pointer>: <reason>"``, built only from the caller's own
    schema and this module's vocabulary — no provider text — so a finding is
    safe to put in a receipt. An empty list means the whole schema is enforced
    by the endpoint exactly as the local validator will check it.
    """
    definitions = schema.get("$defs")
    definitions = definitions if isinstance(definitions, Mapping) else {}
    findings: list[str] = []
    _visit_schema_node(schema, "#", (), definitions, findings)
    return sorted(set(findings))


def _visit_schema_node(
    node: Any,
    pointer: str,
    reference_chain: tuple[str, ...],
    definitions: Mapping[str, Any],
    findings: list[str],
) -> None:
    if not isinstance(node, Mapping):
        findings.append(f"{pointer}: schema node is not an object")
        return
    fields = cast("Mapping[str, Any]", node)
    reference = fields.get("$ref")
    if reference is not None:
        _visit_reference(reference, pointer, reference_chain, definitions, findings)
        return
    allowed = set(COMMON_SCHEMA_KEYWORDS)
    node_type = fields.get("type")
    if isinstance(node_type, str):
        if node_type not in SUPPORTED_SCHEMA_TYPES:
            findings.append(f"{pointer}/type: unsupported type '{node_type}'")
        allowed |= _type_keywords(node_type)
    elif node_type is not None:
        findings.append(f"{pointer}/type: only a single string type is enforced")
    for keyword in sorted(set(fields) - allowed):
        findings.append(f"{pointer}/{keyword}: keyword is not enforced by this endpoint")
    if node_type == "object":
        _visit_object_node(fields, pointer, reference_chain, definitions, findings)
    elif node_type == "array":
        _visit_array_node(fields, pointer, reference_chain, definitions, findings)
    elif node_type == "string":
        _visit_string_node(fields, pointer, findings)
    for keyword in ("anyOf", "oneOf", "allOf"):
        branches = fields.get(keyword)
        if isinstance(branches, Sequence) and not isinstance(branches, (str, bytes)):
            for index, branch in enumerate(branches):
                _visit_schema_node(branch, f"{pointer}/{keyword}/{index}", reference_chain, definitions, findings)
        elif branches is not None:
            findings.append(f"{pointer}/{keyword}: value is not a list of schemas")
    if pointer == "#":
        for name, definition in sorted(definitions.items()):
            _visit_schema_node(definition, f"#/$defs/{name}", (name,), definitions, findings)


def _visit_reference(
    reference: Any,
    pointer: str,
    reference_chain: tuple[str, ...],
    definitions: Mapping[str, Any],
    findings: list[str],
) -> None:
    if not isinstance(reference, str) or not reference.startswith(DEFINITION_POINTER_PREFIX):
        findings.append(f"{pointer}/$ref: only local '#/$defs/' references are enforced")
        return
    name = reference[len(DEFINITION_POINTER_PREFIX) :]
    if name not in definitions:
        findings.append(f"{pointer}/$ref: no definition named '{name}'")
        return
    if name in reference_chain:
        findings.append(f"{pointer}/$ref: recursive reference to '{name}' is not enforced")
        return
    _visit_schema_node(definitions[name], f"#/$defs/{name}", (*reference_chain, name), definitions, findings)


def _visit_object_node(
    fields: Mapping[str, Any],
    pointer: str,
    reference_chain: tuple[str, ...],
    definitions: Mapping[str, Any],
    findings: list[str],
) -> None:
    if fields.get("additionalProperties") is not False:
        findings.append(f"{pointer}/additionalProperties: must be false for an enforced object")
    properties = fields.get("properties")
    if isinstance(properties, Mapping):
        for key, value in sorted(cast("Mapping[str, Any]", properties).items()):
            _visit_schema_node(value, f"{pointer}/properties/{key}", reference_chain, definitions, findings)
    elif properties is not None:
        findings.append(f"{pointer}/properties: value is not an object")


def _visit_array_node(
    fields: Mapping[str, Any],
    pointer: str,
    reference_chain: tuple[str, ...],
    definitions: Mapping[str, Any],
    findings: list[str],
) -> None:
    minimum_items = fields.get("minItems")
    if minimum_items is not None and minimum_items not in SUPPORTED_MINIMUM_ITEMS:
        findings.append(f"{pointer}/minItems: only 0 and 1 are enforced")
    items = fields.get("items")
    if items is not None:
        _visit_schema_node(items, f"{pointer}/items", reference_chain, definitions, findings)


def _visit_string_node(fields: Mapping[str, Any], pointer: str, findings: list[str]) -> None:
    string_format = fields.get("format")
    if string_format is not None and string_format not in SUPPORTED_STRING_FORMATS:
        findings.append(f"{pointer}/format: format is not enforced by this endpoint")


def _type_keywords(node_type: str) -> frozenset[str]:
    if node_type == "object":
        return OBJECT_SCHEMA_KEYWORDS
    if node_type == "array":
        return ARRAY_SCHEMA_KEYWORDS
    if node_type == "string":
        return STRING_SCHEMA_KEYWORDS
    return frozenset()


def _parse_json_object(text: str) -> dict[str, Any]:
    """Parse the enforced answer, refusing anything that is not one JSON object.

    Nothing is unwrapped, unfenced, or scavenged out of prose here: on a route
    the endpoint constrains, a response that is not exactly one JSON object
    means enforcement did not hold.
    """
    if not text:
        raise IncompleteStructuredResponseError("Anthropic structured response had no text content")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise IncompleteStructuredResponseError("Anthropic structured response was not valid JSON") from error
    if not isinstance(value, dict):
        raise IncompleteStructuredResponseError("Anthropic structured response root was not an object")
    return cast("dict[str, Any]", value)


def _schema_violations(schema: Mapping[str, Any], value: Mapping[str, Any]) -> list[str]:
    """Check the parsed response against the declared schema, locally."""
    validator = Draft202012Validator(dict(schema))
    return sorted("/".join(str(part) for part in error.path) for error in validator.iter_errors(value))


def _message_text(response: object | None) -> str:
    """Return the assistant text, concatenating every ``text`` content block."""
    blocks = getattr(response, "content", None)
    if not isinstance(blocks, Sequence) or isinstance(blocks, (str, bytes)):
        return ""
    parts: list[str] = []
    for block in blocks:
        block_type = getattr(block, "type", None)
        text = getattr(block, "text", None)
        if block_type is None and isinstance(block, Mapping):
            fields = cast("Mapping[str, Any]", block)
            block_type = fields.get("type")
            text = fields.get("text")
        if block_type == "text" and isinstance(text, str):
            parts.append(text)
    return "".join(parts)


def _stop_reason(response: object | None) -> str | None:
    reason = getattr(response, "stop_reason", None)
    return str(reason) if reason else None


def _refusal_category(response: object | None) -> str | None:
    """Return the refusal policy category, which is an enum, never prose.

    ``stop_details.explanation`` is provider-written text and is deliberately
    not read: only the stable category label reaches a receipt.
    """
    details = getattr(response, "stop_details", None)
    category = getattr(details, "category", None)
    if category is None and isinstance(details, Mapping):
        category = cast("Mapping[str, Any]", details).get("category")
    return str(category) if category else None


def _usage_tokens(usage: object | None, name: str) -> int:
    """Read a usage count under either the object or the plain-dict shape."""
    value = getattr(usage, name, None)
    if value is None and isinstance(usage, Mapping):
        value = cast("Mapping[str, Any]", usage).get(name)
    return int(value or 0)


def _total_tokens(usage: object | None) -> int:
    """Anthropic reports no total, so the receipt states how one is derived."""
    return _usage_tokens(usage, "input_tokens") + _usage_tokens(usage, "output_tokens")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _installed_sdk_version() -> str:
    try:
        return installed_version("anthropic")
    except Exception:
        return "unknown"


def _provider_error_type(error: BaseException) -> str | None:
    """Read the provider's stable ``error.type`` label, never its message."""
    direct = getattr(error, "type", None)
    if isinstance(direct, str) and direct:
        return direct
    body = getattr(error, "body", None)
    if not isinstance(body, dict):
        return None
    nested = body.get("error")
    if isinstance(nested, dict) and nested.get("type"):
        return str(nested["type"])
    if body.get("type"):
        return str(body["type"])
    return None


def _retryable_error(error: BaseException) -> bool:
    if _provider_error_type(error) in NON_RETRYABLE_PROVIDER_ERROR_TYPES:
        return False
    if isinstance(error, (TimeoutError, ConnectionError, IncompleteStructuredResponseError)):
        return True
    if type(error).__name__ in {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
        "ServiceUnavailableError",
        "OverloadedError",
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
    provider_error_type = _provider_error_type(error)
    if provider_error_type:
        details["provider_error_type"] = provider_error_type
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

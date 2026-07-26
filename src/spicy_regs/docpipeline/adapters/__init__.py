"""Shared home for the v3 structured-text-model interface.

The v3 design keeps four small provider interfaces. The first one — a prompt, a
strict JSON schema, source data, and an output limit in; checked JSON plus call
details out — has two arms: ``adapters/openai.py`` and ``adapters/codex_cli.py``.
The interface itself lives here, at the package root, so neither arm imports the
other and neither arm owns the shape the other must return.

What lives here:

* :class:`StructuredTextResult` — the immutable (output, call details) pair both
  arms return. V3 has no mutable ``last_call_metadata`` side channel.
* :class:`StructuredTextCallError` — the base failure that carries its own call
  details. Provider-specific subclasses stay in their provider modules.
* :class:`StructuredTextModel` — the shared surface a caller may depend on.
* The defaults and the shared call-detail key list both arms honor.

The other three interfaces (dense embedder, sparse encoder, reranker) keep their
protocols in ``adapters/sentence_transformers.py`` for now. ``adapters/docling.py``
is not one of the four: it is the fallback document parser, used only by
``source.py``, and it keeps its own narrow interface and records so no Docling
type reaches this shared boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

DEFAULT_MODEL = "gpt-5.6-sol"
DEFAULT_REASONING_EFFORT = "medium"
SUPPORTED_REASONING_EFFORTS = frozenset({"none", "low", "medium", "high", "xhigh", "max"})

SHARED_CALL_DETAIL_KEYS: tuple[str, ...] = (
    "provider",
    "transport",
    "model_id",
    "schema_name",
    "response_id",
    "response_model",
    "status",
    "duration_ms",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "attempt_count",
    "retry_count",
    "attempts",
    "prompt_sha256",
    "request_sha256",
    "reasoning_effort",
    "max_output_tokens",
    "timeout_seconds",
    "max_retries",
    "sdk_max_retries",
    "store",
    "schema_validated_locally",
)
"""Call-detail keys every arm emits on every path, success or failure.

A reader of a receipt must not have to know which arm produced it. Keys that do
not apply to a given path are emitted with ``None`` or the appropriate empty
value rather than omitted, so the two arms stay interchangeable.
"""


@dataclass(frozen=True)
class StructuredTextResult:
    """One structured-text-model call: checked output plus its call details.

    ``output`` is the parsed JSON object after local schema validation.
    ``call`` is a plain, secret-free dictionary describing the physical call:
    provider, model, tokenizer, request hashes, token usage, attempt and retry
    counts, timing, and failure metadata when applicable. Both arms return this
    shape, and both populate at least :data:`SHARED_CALL_DETAIL_KEYS`.
    """

    output: dict[str, Any]
    call: dict[str, Any]


class StructuredTextCallError(RuntimeError):
    """A structured-text-model call failed and carries its own call details.

    Only ``.call`` is receipt-safe. The message and any ``__cause__`` chain may
    carry provider-supplied text and must never be copied into a receipt.
    """

    def __init__(self, message: str, *, call: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.call: dict[str, Any] = dict(call or {})


@runtime_checkable
class StructuredTextModel(Protocol):
    """The surface a pipeline step may depend on, shared by both arms."""

    model_id: str

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        """Return the exact request identity, which never contains credentials."""
        ...

    def structured_json(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> StructuredTextResult:
        """Run one strict-schema call and return checked JSON with call details."""
        ...

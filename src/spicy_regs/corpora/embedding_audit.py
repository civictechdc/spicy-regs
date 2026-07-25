"""Model-native embedding input audit contracts and package adapters."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any, Literal, Protocol

from spicy_regs.ontology.common import canonical_json

EMBEDDING_INPUT_AUDIT_POLICY_VERSION = "model-native-input-audit-v1"
OverflowPolicy = Literal["reject", "truncate", "unbounded"]


@dataclass(frozen=True)
class EmbeddingInputAudit:
    """Evidence about the exact token sequence presented to an embedder."""

    tokenizer_id: str
    token_count: int
    token_sequence_sha256: str
    max_input_tokens: int | None
    overflow_policy: OverflowPolicy
    input_over_limit: bool
    input_truncated: bool


class EmbeddingInputAuditor(Protocol):
    """Project-owned boundary for model-specific tokenization evidence."""

    policy_version: str
    tokenizer_id: str
    max_input_tokens: int | None
    overflow_policy: OverflowPolicy

    def audit_inputs(
        self,
        texts: Sequence[str],
    ) -> tuple[EmbeddingInputAudit, ...]: ...


def _sequence_sha256(tokens: Sequence[int | str]) -> str:
    return hashlib.sha256(canonical_json(list(tokens)).encode()).hexdigest()


def _audit(
    auditor: EmbeddingInputAuditor,
    tokens: Sequence[int | str],
) -> EmbeddingInputAudit:
    token_count = len(tokens)
    maximum = auditor.max_input_tokens
    over_limit = maximum is not None and token_count > maximum
    return EmbeddingInputAudit(
        tokenizer_id=auditor.tokenizer_id,
        token_count=token_count,
        token_sequence_sha256=_sequence_sha256(tokens),
        max_input_tokens=maximum,
        overflow_policy=auditor.overflow_policy,
        input_over_limit=over_limit,
        input_truncated=(over_limit and auditor.overflow_policy == "truncate"),
    )


def audit_embedding_inputs(
    auditor: EmbeddingInputAuditor,
    texts: Sequence[str],
) -> tuple[EmbeddingInputAudit, ...]:
    """Run and strictly validate one injected model-native auditor."""
    if auditor.policy_version != EMBEDDING_INPUT_AUDIT_POLICY_VERSION:
        raise ValueError("embedding input audit policy version differs")
    if not auditor.tokenizer_id:
        raise ValueError("embedding input tokenizer identity is empty")
    maximum = auditor.max_input_tokens
    if maximum is not None and maximum <= 0:
        raise ValueError("embedding input token limit is invalid")
    if auditor.overflow_policy == "unbounded":
        if maximum is not None:
            raise ValueError("unbounded embedding auditor declares a limit")
    elif maximum is None:
        raise ValueError("bounded embedding auditor omitted its limit")
    audits = auditor.audit_inputs(texts)
    if len(audits) != len(texts):
        raise ValueError("embedding input audit count differs from input")
    for item in audits:
        expected_over_limit = maximum is not None and item.token_count > maximum
        if (
            item.tokenizer_id != auditor.tokenizer_id
            or item.max_input_tokens != maximum
            or item.overflow_policy != auditor.overflow_policy
            or item.token_count < 0
            or len(item.token_sequence_sha256) != 64
            or item.input_over_limit != expected_over_limit
            or item.input_truncated != (expected_over_limit and auditor.overflow_policy == "truncate")
        ):
            raise ValueError("embedding input audit contract differs")
    return audits


def reject_unsupported_embedding_inputs(
    audits: Sequence[EmbeddingInputAudit],
) -> None:
    """Fail before a provider call when its declared policy is rejection."""
    rejected = [
        index for index, item in enumerate(audits) if item.input_over_limit and item.overflow_policy == "reject"
    ]
    if rejected:
        raise ValueError(
            "embedding inputs exceed the model token limit at indexes "
            + ", ".join(str(index) for index in rejected[:20])
        )


class TiktokenEmbeddingInputAuditor:
    """Exact tiktoken adapter for OpenAI embedding models."""

    policy_version = EMBEDDING_INPUT_AUDIT_POLICY_VERSION

    def __init__(
        self,
        *,
        encoding_name: str,
        max_input_tokens: int,
    ) -> None:
        import tiktoken

        if max_input_tokens <= 0:
            raise ValueError("tiktoken embedding input limit is invalid")
        self.encoding_name = encoding_name
        self.max_input_tokens: int | None = max_input_tokens
        self.overflow_policy: OverflowPolicy = "reject"
        self.tokenizer_id = f"tiktoken:{encoding_name}@{version('tiktoken')}"
        self._encoding = tiktoken.get_encoding(encoding_name)

    def audit_inputs(
        self,
        texts: Sequence[str],
    ) -> tuple[EmbeddingInputAudit, ...]:
        return tuple(
            _audit(
                self,
                self._encoding.encode(text, disallowed_special=()),
            )
            for text in texts
        )


class HuggingFaceEmbeddingInputAuditor:
    """Exact adapter around the tokenizer package used by BGE providers."""

    policy_version = EMBEDDING_INPUT_AUDIT_POLICY_VERSION

    def __init__(
        self,
        *,
        tokenizer: Any,
        tokenizer_id: str,
        max_input_tokens: int,
        overflow_policy: Literal["reject", "truncate"],
    ) -> None:
        if max_input_tokens <= 0:
            raise ValueError("Hugging Face embedding input limit is invalid")
        if not tokenizer_id:
            raise ValueError("Hugging Face tokenizer identity is empty")
        self._tokenizer = tokenizer
        self.max_input_tokens: int | None = max_input_tokens
        self.overflow_policy: OverflowPolicy = overflow_policy
        self.tokenizer_id = f"{tokenizer_id}@transformers-{version('transformers')}"

    def audit_inputs(
        self,
        texts: Sequence[str],
    ) -> tuple[EmbeddingInputAudit, ...]:
        rows: list[EmbeddingInputAudit] = []
        for text in texts:
            encoded = self._tokenizer(
                text,
                add_special_tokens=True,
                truncation=False,
                return_attention_mask=False,
                return_token_type_ids=False,
            )
            if not isinstance(encoded, Mapping):
                raise ValueError("Hugging Face tokenizer returned a non-object")
            input_ids = encoded.get("input_ids")
            if not isinstance(input_ids, list) or any(not isinstance(token, int) for token in input_ids):
                raise ValueError("Hugging Face tokenizer returned invalid input IDs")
            rows.append(_audit(self, input_ids))
        return tuple(rows)


class HashEmbeddingInputAuditor:
    """Exact audit adapter for the deterministic hash test double."""

    policy_version = EMBEDDING_INPUT_AUDIT_POLICY_VERSION
    tokenizer_id = "deterministic:casefold-whitespace-v1"
    max_input_tokens: int | None = None
    overflow_policy: OverflowPolicy = "unbounded"

    def audit_inputs(
        self,
        texts: Sequence[str],
    ) -> tuple[EmbeddingInputAudit, ...]:
        return tuple(_audit(self, text.casefold().split() or [text]) for text in texts)

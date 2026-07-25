"""Deterministic, token-bounded segmentation with reversible source spans."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from importlib.metadata import version
from typing import Literal, Protocol

from spicy_regs.ontology.common import canonical_json

DEFAULT_MAX_SEGMENT_TOKENS = 1_200
DEFAULT_MIN_SEGMENT_TOKENS = 480
DEFAULT_TOKENIZER = "o200k_base"
SEGMENT_POLICY_VERSION = "source-aware-o200k-v1"

BoundaryKind = Literal[
    "paragraph",
    "line",
    "sentence",
    "word",
    "hard",
    "eof",
]


class TokenCounter(Protocol):
    """Small adapter that makes token-budget behavior testable."""

    name: str
    version: str

    def count(self, text: str) -> int: ...


class TiktokenCounter:
    """Pinned OpenAI-compatible token counter."""

    def __init__(self, encoding_name: str = DEFAULT_TOKENIZER) -> None:
        import tiktoken

        self.name = encoding_name
        self.version = version("tiktoken")
        self._encoding = tiktoken.get_encoding(encoding_name)

    def count(self, text: str) -> int:
        return len(self._encoding.encode(text, disallowed_special=()))


@dataclass(frozen=True)
class TextSegment:
    """One contiguous, reversible span from a source field."""

    segment_id: str
    source_field: str
    ordinal: int
    start_char: int
    end_char: int
    text: str
    boundary: BoundaryKind
    source_sha256: str
    token_count: int
    tokenizer: str
    tokenizer_version: str
    policy_version: str


@dataclass(frozen=True)
class RecordSegment:
    """One prompt-sized group of non-overlapping source-field spans."""

    segment_id: str
    ordinal: int
    fields: dict[str, str]
    source_spans: dict[str, tuple[int, int]]
    boundaries: dict[str, BoundaryKind]
    source_sha256: dict[str, str]
    token_count: int
    tokenizer: str
    tokenizer_version: str
    policy_version: str
    previous_segment_id: str | None = None
    next_segment_id: str | None = None
    parent_segment_id: str | None = None


def _counter(counter: TokenCounter | None) -> TokenCounter:
    return counter if counter is not None else TiktokenCounter()


def _sentence_break(
    text: str,
    *,
    lower: int,
    upper: int,
) -> int | None:
    for index in range(upper - 1, lower - 1, -1):
        if (
            text[index] in ".!?"
            and index + 1 < len(text)
            and text[index + 1].isspace()
        ):
            return index + 1
    return None


def _last_break(
    text: str,
    *,
    start: int,
    lower: int,
    upper: int,
) -> tuple[int, BoundaryKind]:
    paragraph = text.rfind("\n\n", lower, upper)
    if paragraph >= lower:
        return paragraph + 2, "paragraph"
    line = text.rfind("\n", lower, upper)
    if line >= lower:
        return line + 1, "line"
    sentence = _sentence_break(text, lower=lower, upper=upper)
    if sentence is not None:
        return sentence, "sentence"
    for index in range(upper - 1, lower - 1, -1):
        if text[index].isspace():
            return index + 1, "word"
    if upper <= start:
        raise RuntimeError("Segment boundary did not advance")
    return upper, "hard"


def _largest_end_within_budget(
    text: str,
    *,
    start: int,
    max_tokens: int,
    counter: TokenCounter,
) -> int:
    """Find a safe character boundary for the token budget.

    BPE token counts are nearly monotone but can change at a new suffix. The
    exponential probe avoids repeatedly tokenizing the entire unprocessed tail
    of a multi-megabyte document. The binary search then finds a candidate
    quickly; the final loop proves the returned slice itself satisfies the
    budget.
    """
    if start >= len(text):
        return len(text)
    window = max(64, max_tokens * 4)
    high = min(len(text), start + window)
    safe = start
    while True:
        if counter.count(text[start:high]) <= max_tokens:
            safe = high
            if high == len(text):
                return high
            window *= 2
            high = min(len(text), start + window)
            continue
        break
    low = safe + 1
    while low <= high:
        middle = (low + high) // 2
        if counter.count(text[start:middle]) <= max_tokens:
            safe = middle
            low = middle + 1
        else:
            high = middle - 1
    while safe > start and counter.count(text[start:safe]) > max_tokens:
        safe -= 1
    if safe == start:
        first_end = min(len(text), start + 1)
        first_count = counter.count(text[start:first_end])
        raise ValueError(
            "max_tokens cannot contain one source character "
            f"({first_count} tokens required)"
        )
    return safe


def _smallest_end_at_budget(
    text: str,
    *,
    start: int,
    upper: int,
    min_tokens: int,
    counter: TokenCounter,
) -> int:
    if min_tokens <= 1:
        return min(start + 1, upper)
    low = start + 1
    high = upper
    result = upper
    while low <= high:
        middle = (low + high) // 2
        if counter.count(text[start:middle]) >= min_tokens:
            result = middle
            high = middle - 1
        else:
            low = middle + 1
    return result


def segment_text(
    source_field: str,
    text: str,
    *,
    max_tokens: int = DEFAULT_MAX_SEGMENT_TOKENS,
    min_tokens: int = DEFAULT_MIN_SEGMENT_TOKENS,
    token_counter: TokenCounter | None = None,
    policy_version: str = SEGMENT_POLICY_VERSION,
    identity_scope: dict[str, str] | None = None,
) -> list[TextSegment]:
    """Split raw text at structural boundaries without changing source text."""
    if max_tokens <= 0:
        raise ValueError("max_tokens must be positive")
    if min_tokens <= 0 or min_tokens > max_tokens:
        raise ValueError(
            "min_tokens must be positive and no larger than max_tokens"
        )
    if not text:
        return []

    counter = _counter(token_counter)
    source_sha256 = hashlib.sha256(text.encode()).hexdigest()
    result: list[TextSegment] = []
    start = 0
    while start < len(text):
        upper = _largest_end_within_budget(
            text,
            start=start,
            max_tokens=max_tokens,
            counter=counter,
        )
        if upper == len(text):
            end: int = upper
            boundary: BoundaryKind = "eof"
        else:
            lower = _smallest_end_at_budget(
                text,
                start=start,
                upper=upper,
                min_tokens=min_tokens,
                counter=counter,
            )
            end, boundary = _last_break(
                text,
                start=start,
                lower=lower,
                upper=upper,
            )
            while (
                end > start
                and counter.count(text[start:end]) > max_tokens
            ):
                end -= 1
                boundary = "hard"
        identity = canonical_json(
            {
                "identity_scope": identity_scope or {},
                "policy_version": policy_version,
                "source_field": source_field,
                "source_sha256": source_sha256,
                "start_char": start,
                "end_char": end,
            }
        )
        result.append(
            TextSegment(
                segment_id=(
                    "text_segment_"
                    + hashlib.sha256(identity.encode()).hexdigest()[:24]
                ),
                source_field=source_field,
                ordinal=len(result),
                start_char=start,
                end_char=end,
                text=text[start:end],
                boundary=boundary,
                source_sha256=source_sha256,
                token_count=counter.count(text[start:end]),
                tokenizer=counter.name,
                tokenizer_version=counter.version,
                policy_version=policy_version,
            )
        )
        start = end
    return result


def segment_fields(
    fields: dict[str, str],
    *,
    max_tokens: int = DEFAULT_MAX_SEGMENT_TOKENS,
    min_tokens: int = DEFAULT_MIN_SEGMENT_TOKENS,
    token_counter: TokenCounter | None = None,
    policy_version: str = SEGMENT_POLICY_VERSION,
    identity_scope: dict[str, str] | None = None,
) -> list[RecordSegment]:
    """Pack exact source spans within one hard leaf-text token budget."""
    counter = _counter(token_counter)
    atoms = [
        atom
        for source_field, text in fields.items()
        for atom in segment_text(
            source_field,
            text,
            max_tokens=max_tokens,
            min_tokens=min_tokens,
            token_counter=counter,
            policy_version=policy_version,
            identity_scope=identity_scope,
        )
    ]
    groups: list[list[TextSegment]] = []
    current: list[TextSegment] = []
    current_fields: set[str] = set()
    for atom in atoms:
        proposed = [*current, atom]
        proposed_text = "\n".join(item.text for item in proposed)
        if (
            current
            and (
                atom.source_field in current_fields
                or counter.count(proposed_text) > max_tokens
            )
        ):
            groups.append(current)
            current = []
            current_fields = set()
        current.append(atom)
        current_fields.add(atom.source_field)
    if current:
        groups.append(current)

    provisional: list[RecordSegment] = []
    for ordinal, group in enumerate(groups):
        spans = {
            atom.source_field: (atom.start_char, atom.end_char)
            for atom in group
        }
        identity = canonical_json(
            {
                "identity_scope": identity_scope or {},
                "policy_version": policy_version,
                "spans": [
                    {
                        "field": atom.source_field,
                        "source_sha256": atom.source_sha256,
                        "start": atom.start_char,
                        "end": atom.end_char,
                    }
                    for atom in group
                ],
            }
        )
        fields_in_segment = {
            atom.source_field: atom.text
            for atom in group
        }
        provisional.append(
            RecordSegment(
                segment_id=(
                    "record_segment_"
                    + hashlib.sha256(identity.encode()).hexdigest()[:24]
                ),
                ordinal=ordinal,
                fields=fields_in_segment,
                source_spans=spans,
                boundaries={
                    atom.source_field: atom.boundary
                    for atom in group
                },
                source_sha256={
                    atom.source_field: atom.source_sha256
                    for atom in group
                },
                token_count=counter.count(
                    "\n".join(fields_in_segment.values())
                ),
                tokenizer=counter.name,
                tokenizer_version=counter.version,
                policy_version=policy_version,
            )
        )

    return [
        replace(
            segment,
            previous_segment_id=(
                provisional[index - 1].segment_id if index else None
            ),
            next_segment_id=(
                provisional[index + 1].segment_id
                if index + 1 < len(provisional)
                else None
            ),
        )
        for index, segment in enumerate(provisional)
    ]

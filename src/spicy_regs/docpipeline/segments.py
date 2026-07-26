"""The v3 ``segment`` step: model-sized ProcessingSegments over exact source.

What goes in is one :class:`~spicy_regs.docpipeline.source.SourceArtifact` and
its processing-region stream. What comes out is the selected
``structure-overlap-1800`` segmentation of that stream: every segment a group of
exact half-open source slices, every slice bound to the region and durable
fragment it came from, and a complete account of what each field's characters
were covered by.

The policy is not a new idea. It is the arm the bounded fair comparison chose
(``docs/evidence/document-segmentation-fair-comparison-2026-07-24.md``): the
strongest lossless direct arm under the declared Recall@50 → Recall@10 → MRR
ordering, containing all 35 gold spans. This module reproduces that behavior
exactly, so the frozen 1,302-segment baseline still holds.

The rules this module keeps, and where each one lives:

* **The processing stream is not re-decided here.**
  :func:`~spicy_regs.docpipeline.source.processing_regions` is the whole input,
  in its order. This step skips exactly the regions that stream already holds
  back — an ineligible container, an empty region — and records each one in
  :attr:`SegmentOutcome.excluded` rather than dropping it quietly.
* **A region within budget stays whole.** Only a region that is itself oversized
  is split, into leaves of :attr:`SegmentSettings.leaf_budget` tokens. Each later
  leaf reaches backward at most :attr:`SegmentSettings.overlap_tokens` tokens and
  never past the start of its own region — that is what "limited overlap only
  when one structural element is itself oversized" means.
* **A split leaf occupies its own segment.** Whole regions pack greedily, by
  counting the newline-joined processing text against the hard budget. There is no
  same-field break: the selected path did not have one, and adding one would
  move every boundary in the frozen data.
* **Coordinates are Python unicode codepoints, half-open**, and every slice
  carries its own :class:`~spicy_regs.docpipeline.source.CoordinateSystem` saying
  so. :func:`check_segment_slices` proves ``slice.text ==
  field_text[start:end]`` against the artifact.
* **Context is not evidence.** Parent heading paths and the artifact title live
  on :class:`SegmentContext`. They are not slices, they are not in
  :attr:`ProcessingSegment.text`, they are not in the token count, and they are
  in neither identity. A heading *region* remains in processing text for
  migration parity, but its slice is marked context-only and excluded from
  :attr:`ProcessingSegment.evidence_slices`.
* **Two identities, because they answer two questions.**
  :attr:`ProcessingSegment.segment_id` is Artifact-scoped: it says which exact
  source state this segment belongs to, so provenance and migration parity stay
  clear. :attr:`ProcessingSegment.content_digest` is content-addressed over the
  slice texts and the settings alone, so a new Artifact version whose fragments
  did not change does not pay a provider again.

Deliberately unchanged for this migration: a heading region is still a
processing slice, and the ``markup-prolog`` region the source step keeps in the
stream is still segmented. Both are recorded follow-ups for after step 8 —
changing either would move the selected boundaries this step exists to preserve.

The token counter is injected. This module names no tokenizer package: a
provider library lives in ``adapters/``, and a step that hard-wired one could
not be run against a test counter with an exact budget.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from spicy_regs.docpipeline.runtime import CheckResult
from spicy_regs.docpipeline.source import (
    CoordinateSystem,
    SourceArtifact,
    SourceRegion,
    artifact_fragments,
    processing_regions,
    write_table,
)
from spicy_regs.ontology.common import canonical_json, stable_id, text_digest

SEGMENT_STEP = "segment"

# --- the selected policy ----------------------------------------------------

SELECTED_POLICY = "structure-overlap"
"""The arm the bounded fair comparison selected on 2026-07-24."""

SEGMENT_POLICY_VERSION = "structure-overlap-v1"
"""This module's own semantics version, hashed into the settings digest.

Bump it when a boundary rule, a packing rule, or a recorded segment fact
changes. It is not the budget: that is :data:`SELECTED_MAX_TOKENS`.
"""

SELECTED_MAX_TOKENS = 1_800
SELECTED_MIN_TOKENS = 720
SELECTED_OVERLAP_TOKENS = 80
SELECTED_TOKENIZER = "o200k_base"

BOUNDARY_METHOD = "source-native-oversized-overlap"
"""Boundaries come from the source's own regions; overlap appears only inside
one region that did not fit."""

SEGMENT_ID_PREFIX = "processing_segment"

# --- exclusion reasons ------------------------------------------------------

EXCLUDED_NOT_ELIGIBLE = "region-not-evidence-eligible"
"""A container region: its children carry the meaning, so it is not segmented."""

EXCLUDED_EMPTY = "region-empty"
"""A region with no text. Success with nothing to do, recorded rather than lost."""

# --- tables -----------------------------------------------------------------

SEGMENT_TABLE = "processing/segments.parquet"

SEGMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("segment_id", "string"),
    ("content_digest", "string"),
    ("artifact_id", "string"),
    ("artifact_sha256", "string"),
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("profile_id", "string"),
    ("source_table", "string"),
    ("ordinal", "int64"),
    ("segment_count", "int64"),
    ("previous_segment_id", "string"),
    ("next_segment_id", "string"),
    ("slice_count", "int64"),
    ("char_count", "int64"),
    ("overlap_chars", "int64"),
    ("token_count", "int64"),
    ("input_limit", "int64"),
    ("truncated", "bool"),
    ("tokenizer", "string"),
    ("tokenizer_version", "string"),
    ("policy", "string"),
    ("policy_version", "string"),
    ("max_tokens", "int64"),
    ("min_tokens", "int64"),
    ("overlap_tokens", "int64"),
    ("boundary_method", "string"),
    ("settings_sha256", "string"),
    ("coordinate_target", "string"),
    ("coordinate_unit", "string"),
    ("coordinate_interval", "string"),
    ("text_sha256", "string"),
    ("text", "string"),
    ("headings", "string"),
    ("artifact_context", "string"),
    ("slices_json", "string"),
)


class SegmentError(Exception):
    """One of this module's own invariants did not hold."""


# --------------------------------------------------------------------------
# the injected token counter
# --------------------------------------------------------------------------


class TokenCounter(Protocol):
    """The whole tokenizer contract this step needs: a name, a version, a count.

    Injected, never constructed here. ``adapters/openai.py`` supplies the pinned
    ``tiktoken`` counter for production; a test supplies an exact one.
    """

    name: str
    version: str

    def count(self, text: str) -> int: ...


# --------------------------------------------------------------------------
# settings identity
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SegmentSettings:
    """The complete, versioned identity of one segmentation.

    Everything that can move a boundary or change a recorded fact is here, and
    :attr:`digest` covers all of it. Two runs that agree on this digest selected
    the same segments from the same regions.
    """

    policy: str = SELECTED_POLICY
    policy_version: str = SEGMENT_POLICY_VERSION
    max_tokens: int = SELECTED_MAX_TOKENS
    min_tokens: int = SELECTED_MIN_TOKENS
    overlap_tokens: int = SELECTED_OVERLAP_TOKENS
    tokenizer: str = SELECTED_TOKENIZER
    tokenizer_version: str = ""
    boundary_method: str = BOUNDARY_METHOD

    def __post_init__(self) -> None:
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if self.min_tokens <= 0 or self.min_tokens > self.max_tokens:
            raise ValueError("min_tokens must be positive and no larger than max_tokens")
        if self.overlap_tokens < 0:
            raise ValueError("overlap_tokens cannot be negative")
        if self.max_tokens - self.overlap_tokens <= 0:
            raise ValueError("the overlap budget leaves no room for evidence")
        for name, value in (
            ("policy", self.policy),
            ("policy_version", self.policy_version),
            ("tokenizer", self.tokenizer),
            ("tokenizer_version", self.tokenizer_version),
            ("boundary_method", self.boundary_method),
        ):
            if not str(value).strip():
                raise ValueError(f"a segmentation names its {name}")

    @classmethod
    def selected(cls, *, tokenizer_version: str) -> SegmentSettings:
        """The exact ``structure-overlap-1800`` settings, bound to a tokenizer build."""
        return cls(tokenizer_version=tokenizer_version)

    @classmethod
    def for_counter(cls, counter: TokenCounter, **overrides: Any) -> SegmentSettings:
        """Settings that name the counter really doing the counting."""
        return cls(tokenizer=counter.name, tokenizer_version=counter.version, **overrides)

    @property
    def leaf_budget(self) -> int:
        """The budget one leaf of an oversized region may fill, overlap reserved."""
        return self.max_tokens - self.overlap_tokens

    def identity(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "policy_version": self.policy_version,
            "max_tokens": self.max_tokens,
            "min_tokens": self.min_tokens,
            "overlap_tokens": self.overlap_tokens,
            "tokenizer": self.tokenizer,
            "tokenizer_version": self.tokenizer_version,
            "boundary_method": self.boundary_method,
        }

    @property
    def digest(self) -> str:
        return text_digest(canonical_json(self.identity()))


# --------------------------------------------------------------------------
# result records
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SegmentSlice:
    """One exact half-open source span inside one segment.

    ``field_sha256`` covers the whole source field the offsets address;
    ``text_sha256`` covers this slice's own text. ``fragment_id`` is present when
    the region behind this slice is durable, and ``None`` when it is not — a
    container child's parent, for instance, never becomes a fragment.
    """

    artifact_id: str
    artifact_sha256: str
    region_id: str
    fragment_id: str | None
    region_kind: str
    source_field: str
    field_sha256: str
    start_char: int
    end_char: int
    text: str
    text_sha256: str
    coordinates: CoordinateSystem
    evidence_grade: str
    content_layer: str
    coordinate_grade: str
    context_only: bool
    overlap_chars: int = 0

    @property
    def char_count(self) -> int:
        return self.end_char - self.start_char

    def identity(self) -> dict[str, Any]:
        """What makes this slice this exact source span, and nothing more."""
        return {
            "source_field": self.source_field,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "source_sha256": self.field_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "region_id": self.region_id,
            "fragment_id": self.fragment_id,
            "region_kind": self.region_kind,
            "source_field": self.source_field,
            "field_sha256": self.field_sha256,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "overlap_chars": self.overlap_chars,
            "evidence_grade": self.evidence_grade,
            "content_layer": self.content_layer,
            "coordinate_grade": self.coordinate_grade,
            "context_only": self.context_only,
            "text_sha256": self.text_sha256,
            "text": self.text,
        }


@dataclass(frozen=True)
class SegmentContext:
    """Everything a prompt may use as context and may never cite as evidence.

    Kept beside the slices, never inside them: context is not a source slice, so
    it changes no boundary, no token count, no containment, and no identity.
    """

    headings: tuple[str, ...] = ()
    artifact_context: Mapping[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"headings": list(self.headings), "artifact_context": dict(self.artifact_context)}


@dataclass(frozen=True)
class ProcessingSegment:
    """One model-sized group of exact source slices, with its two identities."""

    segment_id: str
    content_digest: str
    artifact_id: str
    artifact_sha256: str
    subject_type: str
    subject_id: str
    profile_id: str
    source_table: str
    ordinal: int
    segment_count: int
    slices: tuple[SegmentSlice, ...]
    context: SegmentContext
    token_count: int
    settings: SegmentSettings
    previous_segment_id: str | None = None
    next_segment_id: str | None = None

    @property
    def text(self) -> str:
        """Processing text: every migration-compatible slice, newline-joined."""
        return "\n".join(one.text for one in self.slices)

    @property
    def evidence_slices(self) -> tuple[SegmentSlice, ...]:
        """Citable body slices, excluding headings, syntax, and non-body context."""
        return tuple(
            one
            for one in self.slices
            if one.fragment_id is not None and not one.context_only and one.content_layer == "body"
        )

    @property
    def text_sha256(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()

    @property
    def char_count(self) -> int:
        return sum(one.char_count for one in self.slices)

    @property
    def overlap_chars(self) -> int:
        return sum(one.overlap_chars for one in self.slices)

    @property
    def input_limit(self) -> int:
        """The hard token budget this segment was built to fit."""
        return self.settings.max_tokens

    @property
    def truncated(self) -> bool:
        """Whether any source text was dropped to fit. This policy never truncates."""
        return self.token_count > self.input_limit

    @property
    def boundary_method(self) -> str:
        return self.settings.boundary_method

    def identity(self) -> dict[str, Any]:
        """Artifact-scoped identity: which exact source state this segment is of."""
        return {
            "settings_sha256": self.settings.digest,
            "artifact_sha256": self.artifact_sha256,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "profile_id": self.profile_id,
            "slices": [one.identity() for one in self.slices],
        }

    def content_identity(self) -> dict[str, Any]:
        """Reusable content identity for these slices and segment settings.

        Deliberately free of the Artifact version, the field names, and the
        offsets. A new Artifact version whose fragment content did not change
        produces the same content digest. This is not a provider-work identity:
        later provider reuse must also bind the prompt, schema, provider, model,
        revision, provider settings, context digest, approval and evidence
        policies, and the earlier run before it may reuse a result.
        """
        return {
            "settings_sha256": self.settings.digest,
            "slices": [{"text_sha256": one.text_sha256} for one in self.slices],
        }


@dataclass(frozen=True)
class FieldSegmentCoverage:
    """What one source field's characters were covered by, and what they were not."""

    source_field: str
    field_chars: int
    covered_chars: int
    duplicated_chars: int
    excluded_chars: int
    uncovered_chars: int
    slice_count: int


@dataclass(frozen=True)
class ArtifactSegmentCoverage:
    """One artifact's segment coverage, recomputed rather than remembered."""

    fields: tuple[FieldSegmentCoverage, ...]

    @property
    def field_chars(self) -> int:
        return sum(one.field_chars for one in self.fields)

    @property
    def covered_chars(self) -> int:
        return sum(one.covered_chars for one in self.fields)

    @property
    def duplicated_chars(self) -> int:
        return sum(one.duplicated_chars for one in self.fields)

    @property
    def excluded_chars(self) -> int:
        return sum(one.excluded_chars for one in self.fields)

    @property
    def uncovered_chars(self) -> int:
        return sum(one.uncovered_chars for one in self.fields)


@dataclass(frozen=True)
class ExcludedRegion:
    """One processing region this step did not segment, and why."""

    region_id: str
    source_field: str
    kind: str
    reason: str
    start_char: int
    end_char: int


@dataclass(frozen=True)
class SegmentOutcome:
    """One artifact's segmentation, in the runtime's item vocabulary.

    ``coverage`` is derived from ``segments`` and ``field_chars`` rather than
    stored, so a check cannot pass by trusting a number that no longer describes
    the segments beside it.
    """

    state: str
    artifact_id: str
    artifact_sha256: str
    subject_type: str
    subject_id: str
    profile_id: str
    source_table: str
    field_chars: tuple[tuple[str, int], ...]
    segments: tuple[ProcessingSegment, ...]
    excluded: tuple[ExcludedRegion, ...]
    settings: SegmentSettings
    error: str = ""

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty", "failed"}:
            raise SegmentError(f"unknown segment state {self.state!r}")

    @property
    def coverage(self) -> ArtifactSegmentCoverage:
        return ArtifactSegmentCoverage(
            fields=tuple(
                _field_coverage(source_field, chars, self.segments, self.excluded)
                for source_field, chars in self.field_chars
            )
        )


@dataclass(frozen=True)
class SegmentResult:
    """What one segment step produced, refused, duplicated, and left undone."""

    artifact_count: int
    segment_count: int
    slice_count: int
    field_chars: int
    covered_chars: int
    uncovered_chars: int
    duplicated_chars: int
    overflow_count: int
    excluded_count: int
    zero_work_count: int
    failed_count: int
    checks: tuple[CheckResult, ...]


# --------------------------------------------------------------------------
# boundary machinery
#
# Behaviour copied from the selected path, not its private names. These four
# functions decide where an oversized region is cut, and a change to any of them
# moves the frozen 1,302-segment baseline.
# --------------------------------------------------------------------------


def _sentence_break(text: str, *, lower: int, upper: int) -> int | None:
    for index in range(upper - 1, lower - 1, -1):
        if text[index] in ".!?" and index + 1 < len(text) and text[index + 1].isspace():
            return index + 1
    return None


def _last_break(text: str, *, start: int, lower: int, upper: int) -> int:
    """The latest structural break in ``[lower, upper)``, preferring larger units."""
    paragraph = text.rfind("\n\n", lower, upper)
    if paragraph >= lower:
        return paragraph + 2
    line = text.rfind("\n", lower, upper)
    if line >= lower:
        return line + 1
    sentence = _sentence_break(text, lower=lower, upper=upper)
    if sentence is not None:
        return sentence
    for index in range(upper - 1, lower - 1, -1):
        if text[index].isspace():
            return index + 1
    if upper <= start:
        raise SegmentError("segment boundary did not advance")
    return upper


def _largest_end_within_budget(text: str, *, start: int, max_tokens: int, counter: TokenCounter) -> int:
    """The furthest character boundary whose slice still fits the budget.

    BPE counts are nearly monotone but can change at a new suffix, so the
    exponential probe avoids tokenizing the whole tail of a large document, the
    binary search finds a candidate, and the final loop proves the returned slice
    itself fits.
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
        raise SegmentError(
            f"max_tokens cannot contain one source character ({counter.count(text[start:first_end])} tokens required)"
        )
    return safe


def _smallest_end_at_budget(text: str, *, start: int, upper: int, min_tokens: int, counter: TokenCounter) -> int:
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


def _leaf_spans(text: str, *, max_tokens: int, min_tokens: int, counter: TokenCounter) -> list[tuple[int, int]]:
    """Split one oversized region's text into gap-free leaves within the budget."""
    if max_tokens <= 0:
        raise SegmentError("max_tokens must be positive")
    if min_tokens <= 0 or min_tokens > max_tokens:
        raise SegmentError("min_tokens must be positive and no larger than max_tokens")
    if not text:
        return []
    spans: list[tuple[int, int]] = []
    start = 0
    while start < len(text):
        upper = _largest_end_within_budget(text, start=start, max_tokens=max_tokens, counter=counter)
        if upper == len(text):
            end = upper
        else:
            lower = _smallest_end_at_budget(text, start=start, upper=upper, min_tokens=min_tokens, counter=counter)
            end = _last_break(text, start=start, lower=lower, upper=upper)
            while end > start and counter.count(text[start:end]) > max_tokens:
                end -= 1
        spans.append((start, end))
        start = end
    return spans


def _overlap_start(text: str, *, lower: int, end: int, overlap_tokens: int, counter: TokenCounter) -> int:
    """The earliest start at or after ``lower`` whose tail still fits the overlap."""
    low = lower
    high = end
    while low < high:
        middle = (low + high) // 2
        if counter.count(text[middle:end]) <= overlap_tokens:
            high = middle
        else:
            low = middle + 1
    start = low
    while start < end and counter.count(text[start:end]) > overlap_tokens:
        start += 1
    while start > lower and counter.count(text[start - 1 : end]) <= overlap_tokens:
        start -= 1
    return start


# --------------------------------------------------------------------------
# units
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class _Unit:
    """One packable piece of the processing stream: a whole region, or one leaf."""

    region: SourceRegion
    start_char: int
    end_char: int
    text: str
    overlap_chars: int
    split_region: bool


def _units(artifact: SourceArtifact, *, settings: SegmentSettings, counter: TokenCounter) -> list[_Unit]:
    """Turn the processing stream into units, splitting only what does not fit."""
    units: list[_Unit] = []
    for region in processing_regions(artifact):
        if counter.count(region.text) <= settings.max_tokens:
            units.append(
                _Unit(
                    region=region,
                    start_char=region.start_char,
                    end_char=region.end_char,
                    text=region.text,
                    overlap_chars=0,
                    split_region=False,
                )
            )
            continue
        spans = _leaf_spans(
            region.text,
            max_tokens=settings.leaf_budget,
            min_tokens=min(settings.min_tokens, settings.leaf_budget),
            counter=counter,
        )
        for index, (leaf_start, leaf_end) in enumerate(spans):
            # ``lower=0`` is the region's own start: the overlap reaches back
            # into this region and never into the one before it.
            relative_start = leaf_start
            overlap_chars = 0
            if index:
                relative_start = _overlap_start(
                    region.text,
                    lower=0,
                    end=leaf_start,
                    overlap_tokens=settings.overlap_tokens,
                    counter=counter,
                )
                overlap_chars = leaf_start - relative_start
            units.append(
                _Unit(
                    region=region,
                    start_char=region.start_char + relative_start,
                    end_char=region.start_char + leaf_end,
                    text=region.text[relative_start:leaf_end],
                    overlap_chars=overlap_chars,
                    split_region=True,
                )
            )
    return units


def _pack(units: Sequence[_Unit], *, settings: SegmentSettings, counter: TokenCounter) -> list[list[_Unit]]:
    """Group units into segments: split leaves alone, whole regions greedily.

    There is no same-field break. The selected path did not have one, and adding
    one here would move boundaries the frozen baseline depends on.
    """
    groups: list[list[_Unit]] = []
    current: list[_Unit] = []
    for unit in units:
        if unit.split_region:
            if current:
                groups.append(current)
                current = []
            groups.append([unit])
            continue
        proposed = counter.count("\n".join(item.text for item in [*current, unit]))
        if current and proposed > settings.max_tokens:
            groups.append(current)
            current = []
        current.append(unit)
    if current:
        groups.append(current)
    return groups


# --------------------------------------------------------------------------
# building one artifact's segments
# --------------------------------------------------------------------------


def _require_matching_counter(settings: SegmentSettings, counter: TokenCounter) -> None:
    if settings.tokenizer != counter.name or settings.tokenizer_version != counter.version:
        raise SegmentError(
            f"settings name tokenizer {settings.tokenizer}@{settings.tokenizer_version} "
            f"but the counter is {counter.name}@{counter.version}"
        )


def _context_for(artifact: SourceArtifact, units: Sequence[_Unit]) -> SegmentContext:
    headings: list[str] = []
    for unit in units:
        for heading in unit.region.heading_path:
            if heading not in headings:
                headings.append(heading)
    return SegmentContext(headings=tuple(headings), artifact_context=dict(artifact.context_fields))


def segment_artifact(
    artifact: SourceArtifact,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
) -> SegmentOutcome:
    """Segment one artifact's processing stream, or say it had nothing to do."""
    _require_matching_counter(settings, counter)

    stream = processing_regions(artifact)
    kept = {region.region_id for region in stream}
    excluded = tuple(
        ExcludedRegion(
            region_id=region.region_id,
            source_field=region.source_field,
            kind=region.kind,
            reason=EXCLUDED_NOT_ELIGIBLE if not region.evidence_eligible else EXCLUDED_EMPTY,
            start_char=region.start_char,
            end_char=region.end_char,
        )
        for region in artifact.regions
        if region.region_id not in kept
    )
    fragment_ids = {fragment.region_id: fragment.fragment_id for fragment in artifact_fragments(artifact)}

    groups = _pack(_units(artifact, settings=settings, counter=counter), settings=settings, counter=counter)
    built: list[ProcessingSegment] = []
    for ordinal, group in enumerate(groups):
        slices = tuple(
            SegmentSlice(
                artifact_id=artifact.artifact_id,
                artifact_sha256=artifact.content_sha256,
                region_id=unit.region.region_id,
                fragment_id=fragment_ids.get(unit.region.region_id),
                region_kind=unit.region.kind,
                source_field=unit.region.source_field,
                field_sha256=unit.region.field_sha256,
                start_char=unit.start_char,
                end_char=unit.end_char,
                text=unit.text,
                text_sha256=hashlib.sha256(unit.text.encode()).hexdigest(),
                coordinates=unit.region.coordinates,
                evidence_grade=unit.region.evidence_grade,
                content_layer=unit.region.content_layer,
                coordinate_grade=unit.region.coordinate_grade,
                context_only=unit.region.context_only,
                overlap_chars=unit.overlap_chars,
            )
            for unit in group
        )
        segment = ProcessingSegment(
            segment_id="",
            content_digest="",
            artifact_id=artifact.artifact_id,
            artifact_sha256=artifact.content_sha256,
            subject_type=artifact.subject_type,
            subject_id=artifact.subject_id,
            profile_id=artifact.profile_id,
            source_table=artifact.source_table,
            ordinal=ordinal,
            segment_count=len(groups),
            slices=slices,
            context=_context_for(artifact, group),
            token_count=counter.count("\n".join(one.text for one in slices)),
            settings=settings,
        )
        if segment.token_count > settings.max_tokens:
            raise SegmentError(
                f"segment {ordinal} of {artifact.subject_id} needs {segment.token_count} tokens, "
                f"over the hard budget of {settings.max_tokens}"
            )
        built.append(
            _identified(
                segment,
                segment_id=stable_id(SEGMENT_ID_PREFIX, canonical_json(segment.identity()), length=24),
                content_digest=text_digest(canonical_json(segment.content_identity())),
            )
        )

    linked = tuple(
        _linked(
            segment,
            previous_segment_id=built[index - 1].segment_id if index else None,
            next_segment_id=built[index + 1].segment_id if index + 1 < len(built) else None,
        )
        for index, segment in enumerate(built)
    )
    outcome = SegmentOutcome(
        state="completed" if linked else "completed_empty",
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.content_sha256,
        subject_type=artifact.subject_type,
        subject_id=artifact.subject_id,
        profile_id=artifact.profile_id,
        source_table=artifact.source_table,
        field_chars=tuple((source_field, len(text)) for source_field, text in artifact.raw_fields.items()),
        segments=linked,
        excluded=excluded,
        settings=settings,
    )
    check_segment_slices(artifact, outcome)
    return outcome


def _identified(segment: ProcessingSegment, *, segment_id: str, content_digest: str) -> ProcessingSegment:
    return replace(segment, segment_id=segment_id, content_digest=content_digest)


def _linked(
    segment: ProcessingSegment, *, previous_segment_id: str | None, next_segment_id: str | None
) -> ProcessingSegment:
    return replace(
        segment,
        previous_segment_id=previous_segment_id,
        next_segment_id=next_segment_id,
    )


def segment_artifacts(
    artifacts: Iterable[SourceArtifact],
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
) -> list[SegmentOutcome]:
    """Segment every artifact, keeping a failure visible instead of fatal."""
    _require_matching_counter(settings, counter)
    outcomes: list[SegmentOutcome] = []
    for artifact in artifacts:
        try:
            outcomes.append(segment_artifact(artifact, settings=settings, counter=counter))
        except SegmentError as error:
            outcomes.append(
                SegmentOutcome(
                    state="failed",
                    artifact_id=artifact.artifact_id,
                    artifact_sha256=artifact.content_sha256,
                    subject_type=artifact.subject_type,
                    subject_id=artifact.subject_id,
                    profile_id=artifact.profile_id,
                    source_table=artifact.source_table,
                    field_chars=(),
                    segments=(),
                    excluded=(),
                    settings=settings,
                    error=str(error),
                )
            )
    return outcomes


# --------------------------------------------------------------------------
# coverage, containment, and checks
# --------------------------------------------------------------------------


def _field_coverage(
    source_field: str,
    field_chars: int,
    segments: Sequence[ProcessingSegment],
    excluded_regions: Sequence[ExcludedRegion],
) -> FieldSegmentCoverage:
    """Sweep one field's slice spans, counting covered, duplicated, and missing.

    Overlap duplicates characters on purpose, so duplication is reported rather
    than treated as a defect. An uncovered character is a real gap: the frozen
    baseline has none.
    """
    spans = [
        (one.start_char, one.end_char)
        for segment in segments
        for one in segment.slices
        if one.source_field == source_field
    ]
    events: list[tuple[int, int]] = []
    for start, end in spans:
        events.append((start, 1))
        events.append((end, -1))
    covered = 0
    duplicated = 0
    active = 0
    previous = 0
    for position, delta in sorted(events, key=lambda value: (value[0], -value[1])):
        width = position - previous
        if active:
            covered += width
            if active > 1:
                duplicated += width * (active - 1)
        active += delta
        previous = position
    excluded_spans = [(one.start_char, one.end_char) for one in excluded_regions if one.source_field == source_field]

    def union_length(values: Sequence[tuple[int, int]]) -> int:
        merged: list[tuple[int, int]] = []
        for start, end in sorted(values):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        return sum(end - start for start, end in merged)

    accounted = union_length([*spans, *excluded_spans])
    excluded_chars = accounted - union_length(spans)
    uncovered = field_chars - accounted
    return FieldSegmentCoverage(
        source_field=source_field,
        field_chars=field_chars,
        covered_chars=covered,
        duplicated_chars=duplicated,
        excluded_chars=excluded_chars,
        uncovered_chars=uncovered,
        slice_count=len(spans),
    )


def contains_span(segment: ProcessingSegment, source_field: str, start_char: int, end_char: int) -> bool:
    """Whether one slice of this segment encloses the whole named span.

    Enclosure, never overlap: a segment that merely touches an evidence span does
    not contain it, and the gold-containment metric depends on that difference.
    """
    return any(
        one.source_field == source_field and one.start_char <= start_char and one.end_char >= end_char
        for one in segment.slices
    )


def contains_evidence_span(
    segment: ProcessingSegment,
    source_field: str,
    start_char: int,
    end_char: int,
) -> bool:
    """Whether one citable evidence slice encloses the whole named span."""
    return any(
        one.source_field == source_field and one.start_char <= start_char and one.end_char >= end_char
        for one in segment.evidence_slices
    )


def overlaps_span(segment: ProcessingSegment, source_field: str, start_char: int, end_char: int) -> bool:
    """Whether any slice of this segment touches the named span at all."""
    return any(
        one.source_field == source_field and one.start_char < end_char and one.end_char > start_char
        for one in segment.slices
    )


def check_segment_slices(artifact: SourceArtifact, outcome: SegmentOutcome) -> None:
    """Prove every slice is the exact half-open codepoint span it claims."""
    for segment in outcome.segments:
        for one in segment.slices:
            field_text = artifact.raw_fields.get(one.source_field)
            if field_text is None:
                raise SegmentError(f"segment {segment.ordinal} names a field the artifact does not carry")
            if not 0 <= one.start_char <= one.end_char <= len(field_text):
                raise SegmentError(f"slice of {one.source_field} leaves the exact source span")
            if one.text != field_text[one.start_char : one.end_char]:
                raise SegmentError(f"slice of {one.source_field} is not the exact source span")


def _check(name: str, status: str, detail: str = "") -> CheckResult:
    return CheckResult(step=SEGMENT_STEP, name=name, status=status, detail=detail)


def summarize_segments(outcomes: Sequence[SegmentOutcome]) -> SegmentResult:
    """Count what the step produced, duplicated, excluded, and left undone."""
    segments = [segment for outcome in outcomes for segment in outcome.segments]
    coverage = [outcome.coverage for outcome in outcomes]
    return SegmentResult(
        artifact_count=len(outcomes),
        segment_count=len(segments),
        slice_count=sum(len(segment.slices) for segment in segments),
        field_chars=sum(one.field_chars for one in coverage),
        covered_chars=sum(one.covered_chars for one in coverage),
        uncovered_chars=sum(one.uncovered_chars for one in coverage),
        duplicated_chars=sum(one.duplicated_chars for one in coverage),
        overflow_count=sum(segment.truncated for segment in segments),
        excluded_count=sum(len(outcome.excluded) for outcome in outcomes),
        zero_work_count=sum(outcome.state == "completed_empty" for outcome in outcomes),
        failed_count=sum(outcome.state == "failed" for outcome in outcomes),
        checks=tuple(segment_checks(outcomes)),
    )


def segment_checks(outcomes: Sequence[SegmentOutcome]) -> list[CheckResult]:
    """Report what the segment step proved, duplicated, and left undecided."""
    uncovered = [
        f"{outcome.subject_id}:{one.source_field}"
        for outcome in outcomes
        for one in outcome.coverage.fields
        if one.uncovered_chars
    ]
    segments = [segment for outcome in outcomes for segment in outcome.segments]
    overflow = [segment.segment_id for segment in segments if segment.truncated]
    duplicated = sum(outcome.coverage.duplicated_chars for outcome in outcomes)
    excluded = sum(len(outcome.excluded) for outcome in outcomes)
    empty = [outcome for outcome in outcomes if outcome.state == "completed_empty"]
    failed = [outcome for outcome in outcomes if outcome.state == "failed"]

    return [
        _check(
            "source_coverage_gap_free",
            "pass" if not uncovered else "fail",
            "every processing character reaches a segment"
            if not uncovered
            else f"uncovered fields: {sorted(uncovered)[:5]}",
        ),
        _check(
            "no_token_overflow",
            "pass" if not overflow else "fail",
            f"{len(segments)} segments fit the declared input limit"
            if not overflow
            else f"{len(overflow)} segments exceed the declared input limit",
        ),
        _check(
            "duplicated_characters",
            "pass",
            f"{duplicated} characters are duplicated by the declared overlap budget",
        ),
        _check(
            "regions_excluded",
            "pass",
            f"{excluded} processing regions were excluded by the source stream",
        ),
        _check(
            "completed_empty",
            "pass",
            f"{len(empty)} artifacts carried no processing region and succeeded with no segment",
        ),
        _check(
            "no_failed_work",
            "pass" if not failed else "fail",
            f"{len(outcomes)} segment items settled"
            if not failed
            else f"{len(failed)} segment items failed: {sorted({one.error for one in failed})}",
        ),
    ]


# --------------------------------------------------------------------------
# tables
# --------------------------------------------------------------------------


def segment_rows(outcome: SegmentOutcome) -> list[dict[str, Any]]:
    """``processing/segments.parquet`` rows: one per ProcessingSegment."""
    return [
        {
            "segment_id": segment.segment_id,
            "content_digest": segment.content_digest,
            "artifact_id": segment.artifact_id,
            "artifact_sha256": segment.artifact_sha256,
            "subject_type": segment.subject_type,
            "subject_id": segment.subject_id,
            "profile_id": segment.profile_id,
            "source_table": segment.source_table,
            "ordinal": segment.ordinal,
            "segment_count": segment.segment_count,
            "previous_segment_id": segment.previous_segment_id,
            "next_segment_id": segment.next_segment_id,
            "slice_count": len(segment.slices),
            "char_count": segment.char_count,
            "overlap_chars": segment.overlap_chars,
            "token_count": segment.token_count,
            "input_limit": segment.input_limit,
            "truncated": segment.truncated,
            "tokenizer": segment.settings.tokenizer,
            "tokenizer_version": segment.settings.tokenizer_version,
            "policy": segment.settings.policy,
            "policy_version": segment.settings.policy_version,
            "max_tokens": segment.settings.max_tokens,
            "min_tokens": segment.settings.min_tokens,
            "overlap_tokens": segment.settings.overlap_tokens,
            "boundary_method": segment.boundary_method,
            "settings_sha256": segment.settings.digest,
            "coordinate_target": segment.slices[0].coordinates.target if segment.slices else None,
            "coordinate_unit": segment.slices[0].coordinates.unit if segment.slices else None,
            "coordinate_interval": segment.slices[0].coordinates.interval if segment.slices else None,
            "text_sha256": segment.text_sha256,
            "text": segment.text,
            "headings": canonical_json(list(segment.context.headings)),
            "artifact_context": canonical_json(dict(segment.context.artifact_context)),
            "slices_json": canonical_json([one.as_dict() for one in segment.slices]),
        }
        for segment in outcome.segments
    ]


def write_segment_table(run_directory: Any, outcomes: Sequence[SegmentOutcome]) -> Any:
    """Write ``processing/segments.parquet``, correctly shaped when it has no rows."""
    return write_table(
        run_directory / SEGMENT_TABLE,
        SEGMENT_COLUMNS,
        [row for outcome in outcomes for row in segment_rows(outcome)],
    )

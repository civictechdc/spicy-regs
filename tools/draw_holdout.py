"""Draw the untouched Rulespec holdout and build its blind drafting input.

Track A steps 1–2 of ``docs/rulespec-testbed-path-forward.md``. This tool does
two things and refuses to do a third:

1. **Draw.** Select artifacts from the frozen mixed corpus snapshot, stratified
   by source profile, by a *seeded deterministic order* — never by hand. Every
   drawn artifact is proved disjoint from the 44 development artifacts and the
   35 gold artifacts on three independent keys: subject identity, artifact
   digest, and the sha256 of its extracted text. Membership, digests, strata,
   and the selection procedure constant are pinned into the tracked evaluation
   boundary record at draw time (``pending_holdout``).

2. **Build blind drafting inputs.** Emit each drawn artifact's identity, title,
   and its segments' exact text through the *production* source and segmentation
   path (``docpipeline/source.py`` + ``docpipeline/segments.py``) under the same
   settings the development run used. Nothing else.

**What must never reach the drafting input** — and what :func:`assert_blind`
proves is absent: registry candidates, concept ids, concept labels, schemes or
aliases, tagger output of any kind, scores, ranks, confidences, and every gold
value from the development set. The gold drafter annotates *free form* — labels
as free text plus evidence offsets — precisely so registry framing cannot anchor
it. Layer 4 of ``docs/evidence/failure-analysis-2026-07-27.md`` (gold encodes
the annotator's frame) and layer 5 (information flows downhill from gold into
every artifact it touches) are the reasons this file exists in this shape.

**No labels are created here.** The semantic vocabulary universe—registry
releases, mapping releases, imports, coverage reports, and output profile—is
frozen before selection. Model-facing implementation pins such as selector,
prompt, schema, index build, and token budget remain ``RESERVED`` until the
later pre-label-exposure freeze. ``--require-adoption-ready`` stays red after
this tool runs: a drawn-but-unadjudicated holdout is not adoption-ready.

Selection procedure ``holdout-seeded-stratified-v1``, in full:

* Strata are declared in :data:`PROFILE_STRATA` — one source profile each, with
  a quota, a minimum extracted-text length, and a scan bound. The declaration is
  the whole of the human input; no row is ever named.
* Within a stratum, every corpus row gets ``rank_key = sha256(seed | procedure |
  profile_id | subject_key)`` where ``subject_key`` is the canonical JSON of the
  profile's identity columns. Rows are visited in ascending
  ``(rank_key, subject_key)`` order — a total order that depends on identity
  only, so no row's *content* can influence its position.
* A visited row is accepted when it builds a completed artifact, segments to at
  least one segment, carries at least ``min_text_chars`` of extracted text, is
  disjoint from the exclusion universe on all three keys, and does not duplicate
  an already-accepted artifact's text. Scanning stops at the quota or at
  ``max_rows_scanned``, whichever comes first.

Re-running with the same seed, corpus, and strata reproduces the same draw.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.segments import (
    ProcessingSegment,
    SegmentSettings,
    TokenCounter,
    segment_artifact,
)
from spicy_regs.docpipeline.source import (
    STEP4_ACTIVE_SOURCE_TABLES,
    SourceArtifact,
    SourceRecord,
    build_source_artifact,
    iter_source_records,
    profile_for_table,
)
from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.evaluation_boundary import (
    BOUNDARY_SCHEMA_VERSION,
    DEFAULT_BOUNDARY_MANIFEST,
    DEVELOPMENT_DATASET_ID,
)
from refspec import (
    ReferenceRuntimeError,
    require_vocabulary_universe_freeze,
)
from spicy_regs.ontology.common import canonical_json, iter_parquet_rows, read_parquet_rows

REPO_ROOT = Path(__file__).resolve().parents[1]

DRAW_SCHEMA_VERSION = "rulespec-holdout-draw-v1"
DRAFTING_SCHEMA_VERSION = "rulespec-holdout-drafting-input-v1"

#: The recorded selection-procedure constant. Changing it changes every draw.
SELECTION_PROCEDURE = "holdout-seeded-stratified-v1"
#: The recorded seed constant. Changing it changes every draw.
SELECTION_SEED = "rulespec-holdout-draw-2026-07-28"

HOLDOUT_DATASET_ID = "rulespec-holdout-28-v1"

DEFAULT_CORPUS_DIR = REPO_ROOT / "output" / "mixed-real-data-corpus-v2"
DEFAULT_DATASET_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
DEFAULT_SELECTION_FILE = (
    REPO_ROOT / "output" / "segmentation-tagging-document-openai-structure-overlap-1800-v4" / "tagging_segments.parquet"
)
GOLD_FILE_NAME = "gold_spans.parquet"

BLINDNESS_STATEMENT = (
    "blind: contains no registry candidates, no concept ids, labels, schemes or aliases, no "
    "tagger output, no scores, ranks or confidences, and no gold value from the development "
    "set. Every field is derived from the corpus snapshot and the production source and "
    "segmentation path. The drafter annotates free form."
)

DRAFTING_PROTOCOL = {
    "annotation_form": "free-text",
    "instructions": [
        "Read each artifact's segments in ordinal order.",
        "Record every topic or regulated-entity subject the text actually supports, as free "
        "text in your own words. Do not consult, and do not try to guess, any controlled "
        "vocabulary — none is supplied on purpose.",
        "Multiple labels per artifact are expected. A two-frame document gets two labels.",
        "Give each label a role and a frame in your own words, and say whether the artifact is "
        "about the topic or merely mentions it.",
        "Support each label with evidence: the segment_id, the character offsets inside that "
        "segment's text (half-open, unicode codepoints), and the exact quoted span. Each "
        "segment's slices carry the source field and source offsets those segment offsets map "
        "onto.",
        "Record explicit denials ('this rule does not apply to X') as denials, not as labels.",
        "Record forbidden results: subjects a reader might plausibly assign that the text does not support.",
        "Record ambiguity rather than resolving it.",
        "Abstain where the text supports nothing; abstention is a real answer.",
    ],
    "evidence_coordinates": {
        "target": "segment text",
        "unit": "unicode-codepoints",
        "interval": "half-open",
    },
}

#: Keys that may never appear anywhere in the drafting input, as substrings.
#: ``selection`` is deliberately not banned; ``selector`` is.
BANNED_OUTPUT_KEY_SUBSTRINGS: tuple[str, ...] = (
    "alias",
    "assignment",
    "candidate",
    "concept",
    "confidence",
    "expected_tag",
    "gold",
    "label",
    "predict",
    "registry",
    "score",
    "selector",
    "tagger",
    "taxonomy",
    "vocabulary",
)


class HoldoutDrawError(RuntimeError):
    """The corpus or the boundary record cannot support a holdout draw."""


class HoldoutDisjointnessError(HoldoutDrawError):
    """A drawn artifact is not disjoint from development or gold data."""


class HoldoutBlindnessError(HoldoutDrawError):
    """A drafting input would carry registry, tagger, or gold information."""


# --------------------------------------------------------------------------
# strata
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ProfileStratum:
    """One source profile's share of the draw, and its acceptance floor.

    ``min_text_chars`` is a declared, per-profile floor rather than one global
    number because the corpus snapshot carries metadata-depth text for some
    families and abstract-depth text for others. It is part of the recorded
    procedure, so it cannot be tuned per row.
    """

    profile_id: str
    source_table: str
    quota: int
    min_text_chars: int
    max_rows_scanned: int
    note: str = ""

    def identity(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "source_table": self.source_table,
            "quota": self.quota,
            "min_text_chars": self.min_text_chars,
            "max_rows_scanned": self.max_rows_scanned,
        }


PROFILE_STRATA: tuple[ProfileStratum, ...] = (
    ProfileStratum("federal-register-document-v1", "federal_register", 6, 600, 4000),
    ProfileStratum("congress-bill-v1", "congress_bills", 5, 300, 4000),
    ProfileStratum("unified-agenda-observation-v1", "unified_agenda", 5, 600, 4000),
    ProfileStratum("gao-report-v1", "gao_reports", 4, 1000, 4000),
    ProfileStratum("lobbying-filing-v1", "lobbying_filings", 4, 600, 4000),
    ProfileStratum(
        "court-docket-v1",
        "court_dockets",
        4,
        150,
        4000,
        note=(
            "The judicial family. court_opinions is absent from the corpus snapshot: the only "
            "ten local opinions are all development artifacts, so no disjoint opinion exists. "
            "court_dockets carries case name, nature of suit, and cause at metadata depth."
        ),
    ),
)

MINIMUM_PROFILE_STRATA = 4


# --------------------------------------------------------------------------
# drawn artifacts
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class DrawnSlice:
    """One exact source span inside a segment, positioned in the segment text."""

    source_field: str
    start_char: int
    end_char: int
    segment_start_char: int
    segment_end_char: int
    char_count: int
    text_sha256: str
    content_layer: str
    context_only: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_field": self.source_field,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "segment_start_char": self.segment_start_char,
            "segment_end_char": self.segment_end_char,
            "char_count": self.char_count,
            "text_sha256": self.text_sha256,
            "content_layer": self.content_layer,
            "context_only": self.context_only,
        }


@dataclass(frozen=True)
class DrawnSegment:
    """One processing segment's exact text, as the drafter will read it."""

    segment_id: str
    ordinal: int
    segment_count: int
    text: str
    text_sha256: str
    token_count: int
    headings: tuple[str, ...]
    slices: tuple[DrawnSlice, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "ordinal": self.ordinal,
            "segment_count": self.segment_count,
            "token_count": self.token_count,
            "char_count": len(self.text),
            "text_sha256": self.text_sha256,
            "headings": list(self.headings),
            "slices": [one.as_dict() for one in self.slices],
            "text": self.text,
        }


@dataclass(frozen=True)
class DrawnArtifact:
    """One drawn holdout artifact: identity, title, and its segments' text."""

    profile_id: str
    source_table: str
    subject_type: str
    subject_id: str
    artifact_id: str
    artifact_digest: str
    title: str | None
    extracted_text_sha256: str
    extracted_text_chars: int
    segments: tuple[DrawnSegment, ...]

    @property
    def subject_identity(self) -> tuple[str, str, str]:
        return (self.profile_id, self.subject_type, self.subject_id)

    def membership_row(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "source_table": self.source_table,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "artifact_id": self.artifact_id,
            "artifact_digest": self.artifact_digest,
            "extracted_text_sha256": self.extracted_text_sha256,
            "extracted_text_chars": self.extracted_text_chars,
            "segment_count": len(self.segments),
        }

    def drafting_row(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "source_table": self.source_table,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "artifact_id": self.artifact_id,
            "artifact_digest": self.artifact_digest,
            "title": self.title,
            "extracted_text_sha256": self.extracted_text_sha256,
            "segment_count": len(self.segments),
            "segments": [one.as_dict() for one in self.segments],
        }


def extracted_text(segments: Sequence[ProcessingSegment]) -> str:
    """The exact text a drafter would read, newline-joined in ordinal order."""
    return "\n".join(segment.text for segment in segments)


def _drawn_slices(segment: ProcessingSegment) -> tuple[DrawnSlice, ...]:
    """Position each slice inside ``segment.text``, which joins them with \\n."""
    slices: list[DrawnSlice] = []
    cursor = 0
    for one in segment.slices:
        start = cursor
        end = start + len(one.text)
        cursor = end + 1
        slices.append(
            DrawnSlice(
                source_field=one.source_field,
                start_char=one.start_char,
                end_char=one.end_char,
                segment_start_char=start,
                segment_end_char=end,
                char_count=one.char_count,
                text_sha256=one.text_sha256,
                content_layer=one.content_layer,
                context_only=one.context_only,
            )
        )
    return tuple(slices)


def build_drawn_artifact(
    record: SourceRecord,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
) -> DrawnArtifact | None:
    """Build and segment one record through the production path, or return None."""
    outcome = build_source_artifact(record)
    if outcome.state != "completed" or outcome.artifact is None:
        return None
    return drawn_artifact_from(outcome.artifact, settings=settings, counter=counter)


def drawn_artifact_from(
    artifact: SourceArtifact,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
) -> DrawnArtifact | None:
    """Segment one already-built artifact and project it for the drafter."""
    segmented = segment_artifact(artifact, settings=settings, counter=counter)
    if not segmented.segments:
        return None
    text = extracted_text(segmented.segments)
    title = artifact.context_fields.get("artifact_title")
    return DrawnArtifact(
        profile_id=artifact.profile_id,
        source_table=artifact.source_table,
        subject_type=artifact.subject_type,
        subject_id=artifact.subject_id,
        artifact_id=artifact.artifact_id,
        artifact_digest=artifact.content_sha256,
        title=str(title) if title else None,
        extracted_text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        extracted_text_chars=len(text),
        segments=tuple(
            DrawnSegment(
                segment_id=segment.segment_id,
                ordinal=segment.ordinal,
                segment_count=segment.segment_count,
                text=segment.text,
                text_sha256=segment.text_sha256,
                token_count=segment.token_count,
                headings=tuple(str(heading) for heading in segment.context.headings),
                slices=_drawn_slices(segment),
            )
            for segment in segmented.segments
        ),
    )


# --------------------------------------------------------------------------
# the exclusion universe
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ExclusionUniverse:
    """Everything a drawn artifact must not be, on three independent keys."""

    subject_identities: frozenset[tuple[str, str, str]]
    artifact_digests: frozenset[str]
    extracted_text_sha256: frozenset[str]
    forbidden_values: frozenset[str]
    counts: Mapping[str, int] = field(default_factory=dict)

    def excludes(self, artifact: DrawnArtifact) -> str | None:
        """Name the key that rejects this artifact, or None when it is disjoint."""
        if artifact.subject_identity in self.subject_identities:
            return "subject_identity"
        if artifact.artifact_digest in self.artifact_digests:
            return "artifact_digest"
        if artifact.extracted_text_sha256 in self.extracted_text_sha256:
            return "extracted_text_sha256"
        return None


def _identity_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[set[tuple[str, str, str]], set[str], set[str]]:
    identities: set[tuple[str, str, str]] = set()
    digests: set[str] = set()
    subject_ids: set[str] = set()
    for row in rows:
        profile_id = str(row.get("profile_id") or "")
        subject_type = str(row.get("subject_type") or "")
        subject_id = str(row.get("subject_id") or "")
        digest = str(row.get("artifact_digest") or "")
        if profile_id and subject_type and subject_id:
            identities.add((profile_id, subject_type, subject_id))
            subject_ids.add(subject_id)
        if digest:
            digests.add(digest)
    return identities, digests, subject_ids


def build_exclusions(
    *,
    development_rows: Sequence[Mapping[str, Any]],
    gold_rows: Sequence[Mapping[str, Any]],
    dataset_artifacts: Sequence[DrawnArtifact] = (),
) -> ExclusionUniverse:
    """Assemble the exclusion universe from already-loaded rows and artifacts."""
    dev_identities, dev_digests, dev_subject_ids = _identity_rows(development_rows)
    gold_identities, gold_digests, gold_subject_ids = _identity_rows(gold_rows)

    identities = set(dev_identities) | set(gold_identities)
    digests = set(dev_digests) | set(gold_digests)
    text_digests: set[str] = set()
    for artifact in dataset_artifacts:
        identities.add(artifact.subject_identity)
        digests.add(artifact.artifact_digest)
        text_digests.add(artifact.extracted_text_sha256)

    forbidden = set(dev_digests) | set(gold_digests) | set(dev_subject_ids) | set(gold_subject_ids)
    forbidden |= {str(artifact.artifact_digest) for artifact in dataset_artifacts}
    # ``concept_scheme`` is deliberately absent: its two values are a fixed
    # enum every source profile declares, so banning them would reject honest
    # text without revealing anything a drafter could be anchored by.
    for row in gold_rows:
        for key in ("gold_id", "concept_label", "exact_text", "exact_text_sha256"):
            value = str(row.get(key) or "").strip()
            if value:
                forbidden.add(value)
    forbidden.discard("")

    return ExclusionUniverse(
        subject_identities=frozenset(identities),
        artifact_digests=frozenset(digests),
        extracted_text_sha256=frozenset(text_digests),
        forbidden_values=frozenset(forbidden),
        counts={
            "development_artifacts": len(dev_identities),
            "gold_artifacts": len(gold_identities),
            "evaluation_dataset_artifacts": len(dataset_artifacts),
            "excluded_subject_identities": len(identities),
            "excluded_artifact_digests": len(digests),
            "excluded_extracted_text_digests": len(text_digests),
        },
    )


def load_exclusions(
    dataset_dir: Path,
    selection_file: Path,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    gold_file: str = GOLD_FILE_NAME,
) -> ExclusionUniverse:
    """Read the development selection and gold table, and rebuild the dataset."""
    development_rows = read_parquet_rows(Path(selection_file))
    gold_rows = read_parquet_rows(Path(dataset_dir) / gold_file)
    dataset_artifacts: list[DrawnArtifact] = []
    for record in iter_source_records(
        Path(dataset_dir),
        active_source_tables=STEP4_ACTIVE_SOURCE_TABLES,
    ):
        drawn = build_drawn_artifact(record, settings=settings, counter=counter)
        if drawn is not None:
            dataset_artifacts.append(drawn)
    return build_exclusions(
        development_rows=development_rows,
        gold_rows=gold_rows,
        dataset_artifacts=dataset_artifacts,
    )


# --------------------------------------------------------------------------
# the draw
# --------------------------------------------------------------------------


def subject_key(profile_id: str, id_columns: Sequence[str], row: Mapping[str, Any]) -> str:
    """The identity-only key a row is ranked by. Content never enters it."""
    return canonical_json(
        {"profile_id": profile_id, "identity": {column: str(row.get(column) or "") for column in id_columns}}
    )


def rank_key(key: str, *, seed: str = SELECTION_SEED, procedure: str = SELECTION_PROCEDURE) -> str:
    """The seeded deterministic rank of one identity key."""
    return hashlib.sha256(f"{seed}\x1f{procedure}\x1f{key}".encode("utf-8")).hexdigest()


def ranked_records(
    records: Iterable[SourceRecord],
    *,
    seed: str = SELECTION_SEED,
    procedure: str = SELECTION_PROCEDURE,
) -> list[tuple[str, str, SourceRecord]]:
    """Order records by ``(rank_key, subject_key)`` — a content-blind total order."""
    ranked: list[tuple[str, str, SourceRecord]] = []
    for record in records:
        key = subject_key(record.profile.profile_id, record.profile.id_columns, record.row)
        ranked.append((rank_key(key, seed=seed, procedure=procedure), key, record))
    ranked.sort(key=lambda item: (item[0], item[1]))
    return ranked


@dataclass(frozen=True)
class StratumDraw:
    """One stratum's accepted artifacts and its scan accounting."""

    stratum: ProfileStratum
    artifacts: tuple[DrawnArtifact, ...]
    rows_available: int
    rows_visited: int
    rejected: Mapping[str, int]

    def facts(self) -> dict[str, Any]:
        return {
            **self.stratum.identity(),
            "drawn": len(self.artifacts),
            "rows_available": self.rows_available,
            "rows_visited": self.rows_visited,
            "rejected": dict(sorted(self.rejected.items())),
        }


def draw_stratum(
    stratum: ProfileStratum,
    records: Iterable[SourceRecord],
    exclusions: ExclusionUniverse,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    seed: str = SELECTION_SEED,
    procedure: str = SELECTION_PROCEDURE,
) -> StratumDraw:
    """Walk one stratum in seeded order and accept until its quota is met."""
    ordered = ranked_records(records, seed=seed, procedure=procedure)
    accepted: list[DrawnArtifact] = []
    seen_text: set[str] = set()
    rejected: dict[str, int] = {}
    visited = 0

    def reject(reason: str) -> None:
        rejected[reason] = rejected.get(reason, 0) + 1

    for _, _, record in ordered:
        if len(accepted) >= stratum.quota or visited >= stratum.max_rows_scanned:
            break
        visited += 1
        drawn = build_drawn_artifact(record, settings=settings, counter=counter)
        if drawn is None:
            reject("unbuildable_or_unsegmented")
            continue
        if drawn.extracted_text_chars < stratum.min_text_chars:
            reject("below_min_text_chars")
            continue
        excluded_by = exclusions.excludes(drawn)
        if excluded_by is not None:
            reject(f"excluded_by_{excluded_by}")
            continue
        if drawn.extracted_text_sha256 in seen_text:
            reject("duplicate_within_draw")
            continue
        seen_text.add(drawn.extracted_text_sha256)
        accepted.append(drawn)

    return StratumDraw(
        stratum=stratum,
        artifacts=tuple(accepted),
        rows_available=len(ordered),
        rows_visited=visited,
        rejected=rejected,
    )


@dataclass(frozen=True)
class HoldoutDraw:
    """Every stratum's result, plus the constants that produced it."""

    dataset_id: str
    seed: str
    procedure: str
    settings: SegmentSettings
    strata: tuple[StratumDraw, ...]
    vocabulary_universe_freeze: Mapping[str, Any]

    @property
    def artifacts(self) -> tuple[DrawnArtifact, ...]:
        return tuple(artifact for stratum in self.strata for artifact in stratum.artifacts)

    @property
    def artifacts_by_profile(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for artifact in self.artifacts:
            counts[artifact.profile_id] = counts.get(artifact.profile_id, 0) + 1
        return dict(sorted(counts.items()))

    def membership(self) -> list[dict[str, Any]]:
        return [artifact.membership_row() for artifact in self.artifacts]

    def membership_sha256(self) -> str:
        return hashlib.sha256(canonical_json(self.membership()).encode("utf-8")).hexdigest()

    def selection_sha256(self) -> str:
        """One digest over the procedure, the strata, the settings, and membership."""
        return hashlib.sha256(
            canonical_json(
                {
                    "dataset_id": self.dataset_id,
                    "draw_schema_version": DRAW_SCHEMA_VERSION,
                    "selection_procedure": self.procedure,
                    "selection_seed": self.seed,
                    "profile_strata": [stratum.stratum.identity() for stratum in self.strata],
                    "segmentation": self.settings.identity(),
                    "vocabulary_universe_freeze": {
                        "id": self.vocabulary_universe_freeze["id"],
                        "canonicalPayloadDigest": self.vocabulary_universe_freeze[
                            "canonicalPayloadDigest"
                        ],
                    },
                    "membership": self.membership(),
                }
            ).encode("utf-8")
        ).hexdigest()


def stratum_records(corpus_dir: Path, stratum: ProfileStratum) -> Iterator[SourceRecord]:
    """Yield one stratum's corpus rows as source records, unordered."""
    profile = profile_for_table(stratum.source_table)
    if profile.profile_id != stratum.profile_id:
        raise HoldoutDrawError(f"stratum {stratum.profile_id!r} does not map source table {stratum.source_table!r}")
    path = Path(corpus_dir) / f"{stratum.source_table}.parquet"
    if not path.exists():
        raise HoldoutDrawError(f"corpus is missing {path}")
    for row in iter_parquet_rows(path):
        yield SourceRecord(profile=profile, row=row)


def draw_holdout(
    corpus_dir: Path,
    exclusions: ExclusionUniverse,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    vocabulary_universe_freeze: Mapping[str, Any],
    strata: Sequence[ProfileStratum] = PROFILE_STRATA,
    seed: str = SELECTION_SEED,
    procedure: str = SELECTION_PROCEDURE,
    dataset_id: str = HOLDOUT_DATASET_ID,
) -> HoldoutDraw:
    """Draw every stratum after freezing the registry and mapping universe."""
    try:
        require_vocabulary_universe_freeze(vocabulary_universe_freeze)
    except ReferenceRuntimeError as exc:
        raise HoldoutDrawError(
            f"vocabulary universe is not frozen: {exc}"
        ) from exc
    drawn = tuple(
        draw_stratum(
            stratum,
            stratum_records(corpus_dir, stratum),
            exclusions,
            settings=settings,
            counter=counter,
            seed=seed,
            procedure=procedure,
        )
        for stratum in strata
    )
    return HoldoutDraw(
        dataset_id=dataset_id,
        seed=seed,
        procedure=procedure,
        settings=settings,
        strata=drawn,
        vocabulary_universe_freeze=dict(vocabulary_universe_freeze),
    )


def verify_draw(
    draw: HoldoutDraw,
    exclusions: ExclusionUniverse,
    *,
    minimum_profiles: int = MINIMUM_PROFILE_STRATA,
) -> dict[str, Any]:
    """Prove disjointness and stratum coverage, or refuse the draw."""
    artifacts = draw.artifacts
    shared_identities = sorted(
        "|".join(artifact.subject_identity)
        for artifact in artifacts
        if artifact.subject_identity in exclusions.subject_identities
    )
    shared_digests = sorted(
        artifact.artifact_digest for artifact in artifacts if artifact.artifact_digest in exclusions.artifact_digests
    )
    shared_text = sorted(
        artifact.extracted_text_sha256
        for artifact in artifacts
        if artifact.extracted_text_sha256 in exclusions.extracted_text_sha256
    )
    duplicate_digests = sorted(
        digest
        for digest in {artifact.artifact_digest for artifact in artifacts}
        if sum(1 for artifact in artifacts if artifact.artifact_digest == digest) > 1
    )
    short_strata = sorted(
        stratum.stratum.profile_id for stratum in draw.strata if len(stratum.artifacts) < stratum.stratum.quota
    )
    profiles = sorted({artifact.profile_id for artifact in artifacts})

    facts = {
        "artifact_count": len(artifacts),
        "profile_count": len(profiles),
        "profiles": profiles,
        "artifacts_by_profile": draw.artifacts_by_profile,
        "compared_on": ["subject_identity", "artifact_digest", "extracted_text_sha256"],
        "shared_subject_identities": shared_identities,
        "shared_artifact_digests": shared_digests,
        "shared_extracted_text_sha256": shared_text,
        "duplicate_artifact_digests": duplicate_digests,
        "understaffed_strata": short_strata,
        "minimum_profiles": minimum_profiles,
        "exclusion_counts": dict(sorted(exclusions.counts.items())),
        "passed": not (shared_identities or shared_digests or shared_text or duplicate_digests),
    }
    if not facts["passed"]:
        raise HoldoutDisjointnessError(
            "holdout is not disjoint from development or gold data: "
            + canonical_json(
                {
                    "shared_subject_identities": shared_identities,
                    "shared_artifact_digests": shared_digests,
                    "shared_extracted_text_sha256": shared_text,
                    "duplicate_artifact_digests": duplicate_digests,
                }
            )
        )
    if short_strata:
        raise HoldoutDrawError("these strata could not be filled from the corpus: " + ", ".join(short_strata))
    if len(profiles) < minimum_profiles:
        raise HoldoutDrawError(
            f"the draw spans {len(profiles)} source profiles; at least {minimum_profiles} are required"
        )
    return facts


# --------------------------------------------------------------------------
# the blind drafting input
# --------------------------------------------------------------------------


def drafting_document(
    draw: HoldoutDraw,
    *,
    generated_at: str,
    corpus: Mapping[str, Any],
) -> dict[str, Any]:
    """Assemble the blind drafting input from an already-verified draw."""
    return {
        "schema_version": DRAFTING_SCHEMA_VERSION,
        "blind": BLINDNESS_STATEMENT,
        "generated_at": generated_at,
        "holdout": {
            "dataset_id": draw.dataset_id,
            "draw_schema_version": DRAW_SCHEMA_VERSION,
            "selection_procedure": draw.procedure,
            "selection_seed": draw.seed,
            "selection_sha256": draw.selection_sha256(),
            "membership_sha256": draw.membership_sha256(),
            "profile_strata": [stratum.facts() for stratum in draw.strata],
            "segmentation": draw.settings.identity(),
            "segmentation_settings_sha256": draw.settings.digest,
        },
        "corpus": dict(corpus),
        "drafting_protocol": DRAFTING_PROTOCOL,
        "artifact_count": len(draw.artifacts),
        "artifacts_by_profile": draw.artifacts_by_profile,
        "artifacts": [artifact.drafting_row() for artifact in draw.artifacts],
    }


def _walk(value: Any, path: str = "") -> Iterator[tuple[str, str | None, Any]]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}" if path else str(key)
            yield child, str(key), item
            yield from _walk(item, child)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child = f"{path}[{index}]"
            yield child, None, item
            yield from _walk(item, child)


def assert_blind(
    document: Mapping[str, Any],
    *,
    forbidden_values: Iterable[str] = (),
    banned_key_substrings: Sequence[str] = BANNED_OUTPUT_KEY_SUBSTRINGS,
) -> dict[str, Any]:
    """Prove no registry, tagger, or gold information reached the document.

    Two independent checks, because a whitelist alone is only as good as the
    person who wrote it: no key anywhere may carry a banned substring, and no
    scalar string anywhere may equal a known development or gold value.
    """
    forbidden = {str(value) for value in forbidden_values if str(value).strip()}
    banned_keys: list[str] = []
    leaked_values: list[str] = []
    scalars = 0
    for path, key, item in _walk(document):
        if key is not None:
            folded = key.casefold()
            if any(banned in folded for banned in banned_key_substrings):
                banned_keys.append(path)
        if isinstance(item, str):
            scalars += 1
            if item in forbidden:
                leaked_values.append(path)
    facts = {
        "banned_key_paths": sorted(banned_keys),
        "leaked_value_paths": sorted(leaked_values),
        "banned_key_substrings": list(banned_key_substrings),
        "forbidden_value_count": len(forbidden),
        "string_values_checked": scalars,
        "passed": not banned_keys and not leaked_values,
    }
    if not facts["passed"]:
        raise HoldoutBlindnessError(
            "drafting input is not blind: "
            + canonical_json({"banned_key_paths": sorted(banned_keys), "leaked_value_paths": sorted(leaked_values)})
        )
    return facts


# --------------------------------------------------------------------------
# the evaluation boundary record
# --------------------------------------------------------------------------

RESERVED = "RESERVED"

PENDING_REASON = (
    "A holdout has been drawn and pinned, and no label exists for it. It is not adjudicated, "
    "the semantic vocabulary universe is frozen, but the evaluated implementation configuration "
    "is not yet frozen, so it can authorize nothing. Remaining implementation pins freeze at "
    "label-exposure time because the exit bar's trivial baselines are computed on this same set."
)

STEPS_COMPLETED = (
    "Draw new artifacts from authoritative corpus data without consulting tagger output.",
    "Freeze membership and source, selection, and gold digests before scoring. "
    "(Membership, source, and selection digests are pinned; gold does not exist yet.)",
)


def pending_holdout_record(
    draw: HoldoutDraw,
    *,
    drawn_at: str,
    corpus: Mapping[str, Any],
    disjointness: Mapping[str, Any],
    drafting_input: Mapping[str, Any],
    development_dataset_id: str = DEVELOPMENT_DATASET_ID,
    stratum_notes: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build the ``pending_holdout`` section for a drawn, unlabelled holdout."""
    return {
        "status": "drawn_unadjudicated",
        "reason": PENDING_REASON,
        "steps_completed": list(STEPS_COMPLETED),
        "required_before_adoption": [
            "Draw new artifacts from authoritative corpus data without consulting tagger output.",
            "Freeze membership and source, selection, and gold digests before scoring.",
            "Keep target concepts and every registered alias disjoint from train.",
            "Complete blind adjudication with at least two independent model families or humans; "
            "publish agreement and resolve or exclude every disagreement.",
            "Freeze and pin the selector, index build, prompt, schema, and token-budget configuration "
            "before revealing holdout labels.",
        ],
        "draw": {
            "dataset_id": draw.dataset_id,
            "draw_schema_version": DRAW_SCHEMA_VERSION,
            "drawn_at": drawn_at,
            "drawn_by": "tools/draw_holdout.py",
            "selection_procedure": draw.procedure,
            "selection_seed": draw.seed,
            "selection_sha256": draw.selection_sha256(),
            "membership_sha256": draw.membership_sha256(),
            "artifact_count": len(draw.artifacts),
            "artifacts_by_profile": draw.artifacts_by_profile,
            "profile_strata": [stratum.facts() for stratum in draw.strata],
            "profile_stratum_notes": [dict(note) for note in stratum_notes],
            "segmentation": {
                **draw.settings.identity(),
                "settings_sha256": draw.settings.digest,
            },
            "vocabulary_universe_freeze": {
                "id": draw.vocabulary_universe_freeze["id"],
                "canonicalPayloadDigest": draw.vocabulary_universe_freeze[
                    "canonicalPayloadDigest"
                ],
                "registryReleases": draw.vocabulary_universe_freeze[
                    "registryReleases"
                ],
                "mappingReleases": draw.vocabulary_universe_freeze[
                    "mappingReleases"
                ],
                "outputProfile": draw.vocabulary_universe_freeze[
                    "outputProfile"
                ],
            },
            "corpus": dict(corpus),
            "membership": draw.membership(),
            "disjointness": {
                "development_dataset_id": development_dataset_id,
                **dict(disjointness),
            },
            "drafting_input": dict(drafting_input),
        },
        "labels": {
            "status": "not_drafted",
            "gold_file": None,
            "gold_sha256": None,
            "gold_row_count": None,
            "note": "No label exists. Nothing in this record may be scored.",
        },
        "frozen_configuration": {
            "status": RESERVED,
            "freeze_point": "before holdout label exposure",
            "candidate_selector": RESERVED,
            "prompt_concept_limit": RESERVED,
            "registry_sha256": RESERVED,
            "tag_instructions_sha256": RESERVED,
            "tag_schema_sha256": RESERVED,
            "prompt_input_token_budget": RESERVED,
            "prompt_safety_margin_tokens": RESERVED,
        },
        "holdout_controls": {
            "status": RESERVED,
            "configuration_frozen_before_labels": False,
            "tuning_access": False,
        },
        "adjudication": {
            "status": "not_started",
            "scope": "holdout",
            "blind_to_tagger_output": True,
            "agreement_published": False,
            "disagreement_resolution_status": "not_started",
            "reviewers": [],
            "cross_family": False,
        },
    }


def update_boundary_manifest(path: Path, section: Mapping[str, Any]) -> dict[str, Any]:
    """Replace ``pending_holdout`` in the tracked record, changing nothing else."""
    path = Path(path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != BOUNDARY_SCHEMA_VERSION:
        raise HoldoutDrawError(f"evaluation boundary must use schema {BOUNDARY_SCHEMA_VERSION!r}")
    if "pending_holdout" not in manifest:
        raise HoldoutDrawError("evaluation boundary has no pending_holdout section to populate")
    manifest["pending_holdout"] = dict(section)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return manifest


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _relative(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _corpus_facts(corpus_dir: Path, strata: Sequence[ProfileStratum]) -> dict[str, Any]:
    manifest_path = Path(corpus_dir) / "corpus-manifest.json"
    corpus_id = None
    if manifest_path.exists():
        try:
            corpus_id = json.loads(manifest_path.read_text(encoding="utf-8")).get("dataset_id")
        except json.JSONDecodeError:
            corpus_id = None
    return {
        "corpus_dir": _relative(corpus_dir),
        "corpus_dataset_id": corpus_id,
        "corpus_manifest_sha256": sha256_file(manifest_path) if manifest_path.exists() else None,
        "source_files": {
            f"{stratum.source_table}.parquet": sha256_file(Path(corpus_dir) / f"{stratum.source_table}.parquet")
            for stratum in strata
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Draw the untouched Rulespec holdout, pin it in the evaluation boundary, and emit "
            "its blind drafting input. Creates no labels."
        )
    )
    parser.add_argument("--corpus-dir", type=Path, default=DEFAULT_CORPUS_DIR)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--selection-file", type=Path, default=DEFAULT_SELECTION_FILE)
    parser.add_argument("--drafting-output", type=Path, required=True)
    parser.add_argument(
        "--vocabulary-freeze",
        type=Path,
        required=True,
        help=(
            "Sealed VocabularyUniverseFreeze JSON. Registry and mapping "
            "releases must be pinned before selection."
        ),
    )
    parser.add_argument("--boundary", type=Path, default=DEFAULT_BOUNDARY_MANIFEST)
    parser.add_argument("--seed", default=SELECTION_SEED)
    parser.add_argument("--dataset-id", default=HOLDOUT_DATASET_ID)
    parser.add_argument(
        "--drawn-at",
        default=None,
        help="Draw timestamp; defaults to now, UTC, second resolution.",
    )
    parser.add_argument(
        "--skip-boundary-update",
        action="store_true",
        help="Emit the drafting input and report without touching the tracked boundary record.",
    )
    args = parser.parse_args(argv)

    from spicy_regs.docpipeline.adapters.openai import TiktokenCounter

    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)

    exclusions = load_exclusions(
        args.dataset_dir,
        args.selection_file,
        settings=settings,
        counter=counter,
    )
    try:
        vocabulary_freeze = json.loads(
            args.vocabulary_freeze.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise HoldoutDrawError(
            f"cannot read vocabulary freeze {args.vocabulary_freeze}"
        ) from exc
    if not isinstance(vocabulary_freeze, dict):
        raise HoldoutDrawError("vocabulary freeze must be a JSON object")

    draw = draw_holdout(
        args.corpus_dir,
        exclusions,
        settings=settings,
        counter=counter,
        vocabulary_universe_freeze=vocabulary_freeze,
        seed=args.seed,
        dataset_id=args.dataset_id,
    )
    disjointness = verify_draw(draw, exclusions)

    drawn_at = args.drawn_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    corpus = _corpus_facts(args.corpus_dir, [stratum.stratum for stratum in draw.strata])
    document = drafting_document(draw, generated_at=drawn_at, corpus=corpus)
    blindness = assert_blind(document, forbidden_values=exclusions.forbidden_values)

    output = Path(args.drafting_output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    drafting_sha256 = sha256_file(output)

    boundary_updated = False
    if not args.skip_boundary_update:
        section = pending_holdout_record(
            draw,
            drawn_at=drawn_at,
            corpus=corpus,
            disjointness=disjointness,
            drafting_input={
                "path": str(output),
                "sha256": drafting_sha256,
                "schema_version": DRAFTING_SCHEMA_VERSION,
                "blind": True,
                "blindness_checks": {
                    "banned_key_substrings": blindness["banned_key_substrings"],
                    "forbidden_value_count": blindness["forbidden_value_count"],
                    "string_values_checked": blindness["string_values_checked"],
                    "passed": blindness["passed"],
                },
                "tracked": False,
                "note": "Held outside the repository: it carries the holdout's full source text.",
            },
            stratum_notes=[
                {"profile_id": stratum.stratum.profile_id, "note": stratum.stratum.note}
                for stratum in draw.strata
                if stratum.stratum.note
            ],
        )
        update_boundary_manifest(args.boundary, section)
        boundary_updated = True

    print(
        json.dumps(
            {
                "dataset_id": draw.dataset_id,
                "selection_procedure": draw.procedure,
                "selection_seed": draw.seed,
                "selection_sha256": draw.selection_sha256(),
                "membership_sha256": draw.membership_sha256(),
                "artifact_count": len(draw.artifacts),
                "artifacts_by_profile": draw.artifacts_by_profile,
                "profile_strata": [stratum.facts() for stratum in draw.strata],
                "disjointness": disjointness,
                "blindness": blindness,
                "drafting_input": {"path": str(output), "sha256": drafting_sha256},
                "segmentation_settings_sha256": settings.digest,
                "boundary_record": _relative(args.boundary),
                "boundary_updated": boundary_updated,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""The bridge from a sealed ``DocumentRelease`` to the measured segmenter.

SpicyRegs owns both halves of document processing: the durable structural
passages a :class:`DocumentRelease` seals, and the temporary model-input
segmentation the Rulespec Extrapolator consumes. Until this module existed the
two halves could not meet — the v2 file pipeline emits
:class:`~spicy_regs.document_release.StructuralPassage` records over one exact
:class:`~spicy_regs.document_release.TextRepresentation`, while
:mod:`spicy_regs.docpipeline.segments` consumes a
:class:`~spicy_regs.docpipeline.source.SourceArtifact` and the processing-region
stream :func:`~spicy_regs.docpipeline.source.processing_regions` derives from it.

This module adapts the first shape into the second and nothing else. It does not
re-decide a boundary, does not touch the pinned ``structure-overlap-1800``
constants, and does not add a policy: the segmenter runs exactly as the bounded
fair comparison froze it.

The mapping, and why each half is what it is:

* **One text representation is one artifact.** Two representations of the same
  document (a PDF text and an HTML text, say) are two different exact sources; a
  segment that mixed them would be a model input nobody sealed. The artifact
  carries a single source field whose text is the representation's exact
  ``unicode_text``, so a slice offset *is* a representation offset and needs no
  translation to be verified.
* **One sealed passage is one processing region.** Passage coordinates are
  already Python unicode codepoints, half-open — the same coordinate system
  :mod:`~spicy_regs.docpipeline.segments` writes down — so the offsets pass
  through unchanged and reversibility is preserved by construction rather than by
  arithmetic.
* **Text outside every sealed passage is an ineligible region, not a gap.** The
  release records those spans itself, as ``passage_coverage`` regions in state
  ``excluded`` with reason ``outside-sealed-structural-passages``. The source
  step's own vocabulary for "present in the exact source, deliberately not in the
  processing stream" is a region that is not evidence-eligible, which
  :func:`~spicy_regs.docpipeline.segments.segment_artifact` records in
  :attr:`SegmentOutcome.excluded` rather than dropping. Mirroring it that way
  keeps ``source_coverage_gap_free`` honest: every processing character reaches a
  segment, and every excluded character says which sealed record excluded it.
* **The adapter's frame is the sealed representation.** ``field_origin`` is
  source-native and ``coordinate_grade`` is source-exact because, inside this
  artifact, the representation text *is* the exact source and the offsets into it
  are exact. How that text came to exist — ``pypdf``, ``raw-utf8``, an OCR pass —
  is not erased: it rides on every region's ``evidence_grade`` and is written in
  full (method, version, configuration digest) into the model-input file and the
  receipt.

``parser_invoked`` on the adapted artifact is always ``False``: it records
whether the source step's *contained Office parser* ran, and it did not run here.
Whatever produced the representation text ran inside the release, before this
module was reached, and is reported on ``evidence_grade`` and the method fields.

What comes out is a directory of temporary model-input segment files plus a
sealed receipt. Those files are the whole seam to the Rulespec Extrapolator:
file-only, no import, no Rulespec-side consumer built here. Every slice in them
names the sealed passage and source fragment it came from, so evidence a model
cites can be resolved back to the release without this module in the loop.

**Reading one model-input file — the rule a consumer must not get wrong.** Only
``segments[].slices[]`` are citable spans. Each slice carries ``start``, ``end``,
``text``, ``text_sha256``, and the ``passage_id`` and ``fragment_id`` it came
from, and its offsets address the sealed representation text directly. A
segment's ``text`` is those slices joined with ``join_separator``, so when
``contiguous`` is false the segment spans a sealed passage boundary and the
characters between those passages are *not* in it. An offset into a segment's
``text`` is therefore not a source offset, and evidence must never be recorded
against one.

Everything fails closed: an unknown ``format_version``, a release whose seal does
not cover its body, a digest that does not cover the text it names, a passage
that overlaps another or leaves its representation, and a coverage record that
disagrees with the passages beside it all refuse with a typed error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from spicy_regs.docpipeline.runtime import scan_text_for_secrets
from spicy_regs.docpipeline.segments import (
    SegmentOutcome,
    SegmentSettings,
    TokenCounter,
    segment_artifact,
    segment_checks,
)
from spicy_regs.docpipeline.source import (
    BODY_CONTENT_LAYER,
    DURABLE_MEANINGFUL,
    SOURCE_COORDINATE_GRADE,
    SOURCE_FIELD_COORDINATES,
    SOURCE_NATIVE_FIELD,
    SYNTAX_REGION,
    AccessScope,
    FieldCoverage,
    SourceArtifact,
    SourceRegion,
    region_id_for,
)
from spicy_regs.document_release import (
    ACTUAL_FILE_FORMAT_VERSION,
    COORDINATE_SYSTEM,
    DEFAULT_RULESPEC_CORE_PATH,
    FORMAT_VERSION,
    DocumentReleaseError,
    canonical_digest,
    canonical_json,
    validate_document_release,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
"""The repository root, used to keep absolute scratch paths out of receipts."""

ADAPTER_STEP = "document-release-segment"

ADAPTER_VERSION = "document-release-segment-adapter-v1"
"""This module's own semantics version, recorded on every receipt it writes."""

REGION_ADAPTER_ID = "document-release-passages-v1"
"""The region adapter identity hashed into every region id this module mints."""

MODEL_INPUT_FORMAT_VERSION = "spicyregs-model-input-segments/v1"
"""The format marker on every temporary model-input segment file."""

RECEIPT_NAME = "receipt.json"

SEGMENT_DIRECTORY = "segments"

SUPPORTED_FORMAT_VERSIONS: frozenset[str] = frozenset({FORMAT_VERSION, ACTUAL_FILE_FORMAT_VERSION})
"""The release formats this adapter reads. Anything else refuses by name."""

SELECTED_SETTINGS_ID = "structure-overlap-1800"
"""The frozen configuration name the fair comparison selected on 2026-07-24."""

RELEASE_SUBJECT_TYPE = "document_version"
RELEASE_PROFILE_ID = "document-release-text-representation"
RELEASE_SOURCE_TABLE = "document_release/text_representations"
RELEASE_ACCESS = AccessScope(scope="published", basis="sealed-document-release")
"""What a sealed release says about access: it is already a published record."""

DISPATCH_SEALED_PASSAGES = "sealed-structural-passages"

PASSAGE_REGION_KIND = "structural-passage"
OUTSIDE_PASSAGE_REGION_KIND = "outside-sealed-structural-passages"
OUTSIDE_PASSAGE_CONTENT_LAYER = "furniture"

EXCLUDED_COVERAGE_REASON = "outside-sealed-structural-passages"
"""The release's own word for text no sealed passage selected."""

JOIN_SEPARATOR = "\n"
"""What ``ProcessingSegment.text`` puts between slices. See ``contiguous`` below."""

_SAFE_FILE_STEM = re.compile(r"[0-9a-z-]{1,120}")
"""A record-id tail safe to use as a file name, by the shape ids actually take."""


# --------------------------------------------------------------------------
# typed refusals
# --------------------------------------------------------------------------


class DocumentReleaseSegmentError(Exception):
    """This adapter refused to bridge a release to the segmenter."""


class ReleaseSealError(DocumentReleaseSegmentError):
    """The release digest does not cover the release body beside it."""


class UnknownFormatVersionError(DocumentReleaseSegmentError):
    """The release names a format version this adapter does not read."""


class UnsupportedCoordinateSystemError(DocumentReleaseSegmentError):
    """Offsets are not the unicode-codepoint half-open coordinates required."""


class PassageBindingError(DocumentReleaseSegmentError):
    """A record points at something the release does not carry."""


class PassageDigestMismatchError(DocumentReleaseSegmentError):
    """A digest does not cover the exact text it claims to address."""


class PassageBoundaryError(DocumentReleaseSegmentError):
    """Passage boundaries would make a produced segment irreversible."""


class PassageCoverageMismatchError(DocumentReleaseSegmentError):
    """The sealed coverage record disagrees with the passages beside it."""


class ReleaseValidationError(DocumentReleaseSegmentError):
    """The release contract itself refused this release.

    Raised when :func:`~spicy_regs.document_release.validate_document_release`
    rejects a release that is sealed and structurally adaptable. The release
    validator rebuilds every record and byte-compares, so it catches what an
    adapter reading fields cannot: duplicate record identifiers, a fragment
    projection whose selector cites a different span than its passage, and
    coverage that names a passage nobody sealed.
    """


class ModelInputWriteError(DocumentReleaseSegmentError):
    """The model-input files could not be published as written."""


# --------------------------------------------------------------------------
# what one adaptation produced
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class PassageBinding:
    """One sealed passage and the processing region this adapter minted for it."""

    passage_id: str
    fragment_id: str
    region_id: str
    start: int
    end: int
    selected_text_digest: str
    evidence_grade: str
    passage_policy_version: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "end": self.end,
            "evidence_grade": self.evidence_grade,
            "fragment_id": self.fragment_id,
            "passage_id": self.passage_id,
            "passage_policy_version": self.passage_policy_version,
            "region_id": self.region_id,
            "selected_text_digest": self.selected_text_digest,
            "start": self.start,
        }


@dataclass(frozen=True)
class AdaptedRepresentation:
    """One sealed text representation, ready for the segmenter, with its lineage."""

    artifact: SourceArtifact
    representation_id: str
    representation_digest: str
    representation_kind_and_path: str
    document_version_ref: str
    coordinate_system: str
    evidence_grade: str
    method: str | None
    method_version: str | None
    method_config_digest: str | None
    source_rendition_ref: str | None
    text: str
    source_field: str
    bindings: tuple[PassageBinding, ...]
    context_fields: Mapping[str, str]

    def passage_for_region(self, region_id: str) -> PassageBinding:
        """The sealed passage one processing region came from."""
        for binding in self.bindings:
            if binding.region_id == region_id:
                return binding
        raise PassageBindingError(f"region {region_id!r} belongs to no sealed passage")

    @property
    def file_stem(self) -> str:
        """A deterministic, filesystem-safe name for this representation."""
        return model_input_file_stem(self.representation_id)


@dataclass(frozen=True)
class RepresentationSegmentation:
    """One representation's adaptation and the segmentation it produced."""

    adapted: AdaptedRepresentation
    outcome: SegmentOutcome

    def passage_for_region(self, region_id: str) -> PassageBinding:
        return self.adapted.passage_for_region(region_id)


# --------------------------------------------------------------------------
# settings identity
# --------------------------------------------------------------------------


def settings_id(settings: SegmentSettings) -> str:
    """The configuration name a receipt records: policy and hard budget."""
    return f"{settings.policy}-{settings.max_tokens}"


def _settings_facts(settings: SegmentSettings) -> dict[str, Any]:
    return {"settings_id": settings_id(settings), "settings_sha256": settings.digest, **settings.identity()}


# --------------------------------------------------------------------------
# names and paths
# --------------------------------------------------------------------------


def model_input_file_stem(representation_id: str) -> str:
    """A file name for one representation that cannot leave its directory.

    A record identifier is data, not a name this module chose: nothing stops a
    release from carrying ``…:../../escaped``. The tail is used only when it has
    the shape identifiers actually take; anything else is replaced by a digest of
    the whole identifier, which is deterministic, collision-free, and inert.
    """
    tail = representation_id.rsplit(":", 1)[-1]
    if _SAFE_FILE_STEM.fullmatch(tail):
        return tail
    return hashlib.sha256(representation_id.encode("utf-8")).hexdigest()


def _contained_output_path(output_dir: Path, relative: str) -> Path:
    """Resolve one published path, refusing anything that leaves ``output_dir``."""
    root = Path(output_dir).resolve()
    target = (root / Path(*relative.split("/"))).resolve()
    if target != root and root not in target.parents:
        raise ModelInputWriteError(f"published path {relative!r} leaves the output directory")
    return target


def _pin_path(path: Path) -> str:
    """Record a repo-relative path when possible, else the basename.

    Keeping absolute scratch paths out of the receipt keeps the same input
    byte-identical from any working directory. Ported from the same idiom in
    ``tools/build_date_event_artifact.py``.
    """
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return resolved.name


# --------------------------------------------------------------------------
# reading the release
# --------------------------------------------------------------------------


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PassageBindingError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _require_list(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise PassageBindingError(f"{label} must be an array")
    return value


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise PassageBindingError(f"{label} must be a non-empty string")
    return value


def _require_offset(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PassageBoundaryError(f"{label} must be an integer")
    return value


def _prefixed_digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _bare_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _check_seal(release: Mapping[str, Any]) -> None:
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    try:
        digest = canonical_digest(body)
    except Exception as error:  # pragma: no cover - canonical_json raises its own type
        raise ReleaseSealError("release body is not canonical JSON") from error
    if release.get("release_digest") != digest:
        raise ReleaseSealError("release digest does not cover the release body")
    if release.get("release_id") != "urn:spicy-regs:document-release:" + digest.removeprefix("sha256:"):
        raise ReleaseSealError("release id does not name the release digest")


def _check_format_version(release: Mapping[str, Any]) -> str:
    format_version = release.get("format_version")
    if format_version not in SUPPORTED_FORMAT_VERSIONS:
        raise UnknownFormatVersionError(
            f"this adapter reads {sorted(SUPPORTED_FORMAT_VERSIONS)}, not {format_version!r}"
        )
    if release.get("record_type") != "DocumentRelease":
        raise UnknownFormatVersionError(f"expected a DocumentRelease, not {release.get('record_type')!r}")
    return str(format_version)


def _check_coordinate_system(value: object, label: str) -> str:
    if value != COORDINATE_SYSTEM:
        raise UnsupportedCoordinateSystemError(
            f"{label} declares {value!r}; this adapter reads only {COORDINATE_SYSTEM!r}"
        )
    return str(value)


def _document_context(release: Mapping[str, Any], document_version_ref: str) -> dict[str, str]:
    for record in _require_list(release.get("document_versions"), "document_versions"):
        record = _require_mapping(record, "document version")
        if record.get("document_version_id") == document_version_ref:
            return {
                "document_type": str(record.get("document_type", "")),
                "document_version_ref": document_version_ref,
                "publisher": str(record.get("publisher", "")),
                "source_issued_version_id": str(record.get("source_issued_version_id", "")),
                "source_record_id": str(record.get("source_record_id", "")),
            }
    raise PassageBindingError(f"no document version {document_version_ref!r} in this release")


def _sealed_coverage(release: Mapping[str, Any], representation_id: str) -> list[tuple[int, int, str]]:
    """The release's own account of what its passages cover, in offset order."""
    for record in _require_list(release.get("passage_coverage"), "passage_coverage"):
        record = _require_mapping(record, "passage coverage")
        if record.get("text_representation_ref") != representation_id:
            continue
        regions: list[tuple[int, int, str]] = []
        for region in _require_list(record.get("regions"), "passage coverage regions"):
            region = _require_mapping(region, "passage coverage region")
            state = region.get("state")
            if state not in {"processed", "excluded"}:
                raise PassageCoverageMismatchError(f"unknown passage coverage state {state!r}")
            regions.append(
                (
                    _require_offset(region.get("start"), "coverage region start"),
                    _require_offset(region.get("end"), "coverage region end"),
                    str(state),
                )
            )
        return regions
    raise PassageCoverageMismatchError(f"no sealed passage coverage for representation {representation_id!r}")


def _bind_passages(
    release: Mapping[str, Any],
    representation: Mapping[str, Any],
    text: str,
) -> list[Mapping[str, Any]]:
    representation_id = _require_text(representation.get("representation_id"), "representation id")
    representation_digest = _require_text(representation.get("text_digest"), "representation text digest")
    representation_artifact_id = _require_text(
        _require_mapping(representation.get("artifact_projection"), "representation artifact projection").get(
            "artifact_id"
        ),
        "representation artifact id",
    )
    selected = [
        _require_mapping(record, "structural passage")
        for record in _require_list(release.get("structural_passages"), "structural_passages")
        if _require_mapping(record, "structural passage").get("text_representation_ref") == representation_id
    ]
    known = {
        _require_mapping(record, "text representation")["representation_id"]
        for record in _require_list(release.get("text_representations"), "text_representations")
    }
    for record in _require_list(release.get("structural_passages"), "structural_passages"):
        record = _require_mapping(record, "structural passage")
        if record.get("text_representation_ref") not in known:
            raise PassageBindingError(
                f"passage {record.get('passage_id')!r} names a representation this release does not carry"
            )
    ordered = sorted(selected, key=lambda record: _require_offset(record.get("start"), "passage start"))
    cursor = 0
    for record in ordered:
        start = _require_offset(record.get("start"), "passage start")
        end = _require_offset(record.get("end"), "passage end")
        if start < 0 or end <= start:
            raise PassageBoundaryError(f"passage [{start}, {end}) is not a forward span")
        if end > len(text):
            raise PassageBoundaryError(f"passage [{start}, {end}) leaves the representation of length {len(text)}")
        if start < cursor:
            raise PassageBoundaryError(f"passage [{start}, {end}) overlaps the passage that ends at {cursor}")
        cursor = end
        _check_coordinate_system(record.get("coordinate_system"), f"passage {record.get('passage_id')!r}")
        _require_text(record.get("passage_id"), "passage id")
        _require_text(record.get("evidence_grade"), "passage evidence grade")
        _require_text(record.get("passage_policy_version"), "passage policy version")
        _require_text(record.get("selected_text_digest"), "passage selected text digest")
        if record.get("representation_digest") != representation_digest:
            raise PassageDigestMismatchError(
                f"passage {record.get('passage_id')!r} names another representation digest"
            )
        if record.get("selected_text_digest") != _prefixed_digest(text[start:end]):
            raise PassageDigestMismatchError(
                f"passage {record.get('passage_id')!r} digest does not cover its exact text"
            )
        projection = _require_mapping(record.get("source_fragment_projection"), "passage source fragment projection")
        _require_text(projection.get("fragment_id"), "passage fragment id")
        if projection.get("source_artifact_ref") != representation_artifact_id:
            raise PassageBindingError(f"passage {record.get('passage_id')!r} fragment names another artifact")
        if projection.get("source_artifact_digest") != representation_digest:
            raise PassageBindingError(f"passage {record.get('passage_id')!r} fragment names another artifact digest")
        if projection.get("selected_text_digest") != record.get("selected_text_digest"):
            raise PassageDigestMismatchError(
                f"passage {record.get('passage_id')!r} fragment digest differs from its passage"
            )
    return ordered


def _tiled_regions(
    ordered: Sequence[Mapping[str, Any]],
    text: str,
) -> list[tuple[int, int, Mapping[str, Any] | None]]:
    """Passages and the spans between them, tiling the exact text with no gap."""
    tiles: list[tuple[int, int, Mapping[str, Any] | None]] = []
    cursor = 0
    for record in ordered:
        start = int(record["start"])
        end = int(record["end"])
        if start > cursor:
            tiles.append((cursor, start, None))
        tiles.append((start, end, record))
        cursor = end
    if cursor < len(text):
        tiles.append((cursor, len(text), None))
    if not tiles and not text:
        return []
    return tiles


def _check_against_sealed_coverage(
    tiles: Sequence[tuple[int, int, Mapping[str, Any] | None]],
    sealed: Sequence[tuple[int, int, str]],
    representation_id: str,
) -> None:
    derived = [(start, end, "processed" if record is not None else "excluded") for start, end, record in tiles]
    if derived != list(sealed):
        raise PassageCoverageMismatchError(
            f"derived coverage for {representation_id!r} differs from the sealed passage coverage"
        )


def _check_release_contract(release: Mapping[str, Any], rulespec_core_path: Path) -> None:
    """Run the release's own validator and re-raise its refusal as a typed one.

    Deliberately after this module's structural checks and before anything is
    returned, so a caller gets the precise, seam-shaped refusal when this adapter
    can name the problem, and the release contract's verdict when it cannot.
    Nothing is returned or written until both agree.
    """
    try:
        validate_document_release(release, rulespec_core_path=Path(rulespec_core_path))
    except DocumentReleaseError as error:
        raise ReleaseValidationError(f"the release contract refused this release: {error}") from error


def adapt_document_release(
    release: Mapping[str, Any],
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> tuple[AdaptedRepresentation, ...]:
    """Adapt every sealed text representation into a segmenter-ready artifact."""
    release = _require_mapping(release, "DocumentRelease")
    _check_format_version(release)
    _check_seal(release)

    adapted: list[AdaptedRepresentation] = []
    for record in _require_list(release.get("text_representations"), "text_representations"):
        representation = dict(_require_mapping(record, "text representation"))
        representation_id = _require_text(representation.get("representation_id"), "representation id")
        text = representation.get("unicode_text")
        if not isinstance(text, str):
            raise PassageBindingError(f"representation {representation_id!r} carries no exact text")
        _check_coordinate_system(representation.get("coordinate_system"), f"representation {representation_id!r}")
        if representation.get("text_digest") != _prefixed_digest(text):
            raise PassageDigestMismatchError(
                f"representation {representation_id!r} digest does not cover its exact text"
            )
        projection = _require_mapping(representation.get("artifact_projection"), "representation artifact projection")
        if projection.get("content_digest") != representation["text_digest"]:
            raise PassageDigestMismatchError(
                f"representation {representation_id!r} artifact digest does not cover its exact text"
            )
        document_version_ref = _require_text(
            representation.get("document_version_ref"), "representation document version"
        )

        ordered = _bind_passages(release, representation, text)
        tiles = _tiled_regions(ordered, text)
        _check_against_sealed_coverage(tiles, _sealed_coverage(release, representation_id), representation_id)

        source_field = _require_text(representation.get("representation_kind_and_path"), "representation kind and path")
        content_sha256 = _bare_digest(text)
        context_fields = _document_context(release, document_version_ref)
        context_fields["representation_kind_and_path"] = source_field

        regions: list[SourceRegion] = []
        bindings: list[PassageBinding] = []
        for ordinal, (start, end, record) in enumerate(tiles):
            passage = record
            region_id = region_id_for(
                region_adapter_id=REGION_ADAPTER_ID,
                subject_type=RELEASE_SUBJECT_TYPE,
                subject_id=document_version_ref,
                content_sha256=content_sha256,
                source_field=source_field,
                start_char=start,
                end_char=end,
                kind=PASSAGE_REGION_KIND if passage is not None else OUTSIDE_PASSAGE_REGION_KIND,
                ordinal=ordinal,
            )
            regions.append(
                SourceRegion(
                    region_id=region_id,
                    kind=PASSAGE_REGION_KIND if passage is not None else OUTSIDE_PASSAGE_REGION_KIND,
                    ordinal=ordinal,
                    parent_region_id=None,
                    heading_path=(),
                    source_field=source_field,
                    field_origin=SOURCE_NATIVE_FIELD,
                    start_char=start,
                    end_char=end,
                    text=text[start:end],
                    text_sha256=_bare_digest(text[start:end]),
                    field_sha256=content_sha256,
                    artifact_sha256=content_sha256,
                    coordinates=SOURCE_FIELD_COORDINATES,
                    evidence_grade=(
                        _require_text(passage.get("evidence_grade"), "passage evidence grade")
                        if passage is not None
                        else str(representation.get("evidence_grade", ""))
                    ),
                    content_layer=(BODY_CONTENT_LAYER if passage is not None else OUTSIDE_PASSAGE_CONTENT_LAYER),
                    coordinate_grade=SOURCE_COORDINATE_GRADE,
                    evidence_eligible=passage is not None,
                    durability=DURABLE_MEANINGFUL if passage is not None else SYNTAX_REGION,
                    context_only=passage is None,
                )
            )
            if passage is not None:
                bindings.append(
                    PassageBinding(
                        passage_id=_require_text(passage.get("passage_id"), "passage id"),
                        fragment_id=_require_text(
                            _require_mapping(
                                passage.get("source_fragment_projection"), "passage source fragment projection"
                            ).get("fragment_id"),
                            "passage fragment id",
                        ),
                        region_id=region_id,
                        start=start,
                        end=end,
                        selected_text_digest=_require_text(
                            passage.get("selected_text_digest"), "passage selected text digest"
                        ),
                        evidence_grade=_require_text(passage.get("evidence_grade"), "passage evidence grade"),
                        passage_policy_version=_require_text(
                            passage.get("passage_policy_version"), "passage policy version"
                        ),
                    )
                )

        covered_chars = sum(end - start for start, end, _ in tiles)
        durable_chars = sum(end - start for start, end, record in tiles if record is not None)
        if covered_chars != len(text):
            # The tiling is built to span the text end to end; proving it here
            # means the coverage numbers below are checked, not assumed.
            raise PassageCoverageMismatchError(
                f"representation {representation_id!r} tiles cover {covered_chars} of {len(text)} characters"
            )
        artifact = SourceArtifact(
            artifact_id=_require_text(projection.get("artifact_id"), "representation artifact id"),
            content_sha256=content_sha256,
            subject_type=RELEASE_SUBJECT_TYPE,
            subject_id=document_version_ref,
            profile_id=RELEASE_PROFILE_ID,
            source_table=RELEASE_SOURCE_TABLE,
            allowed_schemes=(),
            access=RELEASE_ACCESS,
            coordinates=SOURCE_FIELD_COORDINATES,
            region_adapter_id=REGION_ADAPTER_ID,
            source_policy_version=ADAPTER_VERSION,
            raw_fields={source_field: text},
            field_sha256={source_field: content_sha256},
            field_origins={source_field: SOURCE_NATIVE_FIELD},
            field_dispatch={source_field: DISPATCH_SEALED_PASSAGES},
            context_fields=context_fields,
            dispatch=(DISPATCH_SEALED_PASSAGES,),
            parser_invoked=False,
            regions=tuple(regions),
            exclusions=(),
            coverage=(
                FieldCoverage(
                    source_field=source_field,
                    field_origin=SOURCE_NATIVE_FIELD,
                    field_chars=len(text),
                    covered_chars=covered_chars,
                    durable_chars=durable_chars,
                    syntax_chars=covered_chars - durable_chars,
                    container_chars=0,
                    # Derived from the tiling, never asserted: the tiles cover the
                    # text end to end by construction, and a construction that
                    # stopped doing so would say so here instead of claiming zero.
                    uncovered_chars=len(text) - covered_chars,
                    gaps=(),
                    region_count=len(regions),
                    fragment_count=len(bindings),
                ),
            ),
            secret_rules=tuple(sorted(scan_text_for_secrets(text))),
        )
        adapted.append(
            AdaptedRepresentation(
                artifact=artifact,
                representation_id=representation_id,
                representation_digest=str(representation["text_digest"]),
                representation_kind_and_path=source_field,
                document_version_ref=document_version_ref,
                coordinate_system=str(representation["coordinate_system"]),
                evidence_grade=str(representation.get("evidence_grade", "")),
                method=representation.get("method"),
                method_version=representation.get("method_version"),
                method_config_digest=representation.get("method_config_digest"),
                source_rendition_ref=representation.get("source_rendition_ref"),
                text=text,
                source_field=source_field,
                bindings=tuple(bindings),
                context_fields=context_fields,
            )
        )
    _check_release_contract(release, rulespec_core_path)
    return tuple(adapted)


# --------------------------------------------------------------------------
# segmentation
# --------------------------------------------------------------------------


def segment_document_release(
    release: Mapping[str, Any],
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> tuple[RepresentationSegmentation, ...]:
    """Adapt one sealed release and segment it with the settings as supplied.

    Nothing about the segmentation is decided here: ``settings`` and ``counter``
    go straight to :func:`~spicy_regs.docpipeline.segments.segment_artifact`.
    """
    results = tuple(
        RepresentationSegmentation(
            adapted=adapted,
            outcome=segment_artifact(adapted.artifact, settings=settings, counter=counter),
        )
        for adapted in adapt_document_release(release, rulespec_core_path=rulespec_core_path)
    )
    check_release_reversibility(release, results)
    return results


def check_release_reversibility(
    release: Mapping[str, Any],
    results: Sequence[RepresentationSegmentation],
) -> None:
    """Prove every produced slice is exact release bytes inside a sealed passage.

    Stronger than :func:`~spicy_regs.docpipeline.segments.check_segment_slices`,
    which proves a slice against the artifact this module built. This proves it
    against the release's own text and passage digests, so an adapter bug cannot
    hide behind an artifact that agrees with itself.
    """
    exact_by_id = {
        _require_mapping(record, "text representation")["representation_id"]: _require_mapping(
            record, "text representation"
        )["unicode_text"]
        for record in _require_list(release.get("text_representations"), "text_representations")
    }
    for result in results:
        text = exact_by_id.get(result.adapted.representation_id)
        if not isinstance(text, str):
            raise PassageBindingError(f"representation {result.adapted.representation_id!r} is not in this release")
        for segment in result.outcome.segments:
            for one in segment.slices:
                binding = result.passage_for_region(one.region_id)
                if not binding.start <= one.start_char <= one.end_char <= binding.end:
                    raise PassageBoundaryError(
                        f"slice [{one.start_char}, {one.end_char}) leaves sealed passage {binding.passage_id!r}"
                    )
                if one.text != text[one.start_char : one.end_char]:
                    raise PassageDigestMismatchError(
                        f"slice [{one.start_char}, {one.end_char}) is not the exact release text"
                    )
                if one.text_sha256 != _bare_digest(one.text):
                    raise PassageDigestMismatchError(
                        f"slice [{one.start_char}, {one.end_char}) carries a digest that does not cover its text"
                    )
                if binding.selected_text_digest != _prefixed_digest(text[binding.start : binding.end]):
                    raise PassageDigestMismatchError(
                        f"sealed passage {binding.passage_id!r} digest no longer covers its exact text"
                    )


# --------------------------------------------------------------------------
# the temporary model-input files
# --------------------------------------------------------------------------


def _slice_record(result: RepresentationSegmentation, one: Any) -> dict[str, Any]:
    binding = result.passage_for_region(one.region_id)
    return {
        "content_layer": one.content_layer,
        "coordinate_system": COORDINATE_SYSTEM,
        "end": one.end_char,
        "evidence_grade": one.evidence_grade,
        "fragment_id": binding.fragment_id,
        "overlap_chars": one.overlap_chars,
        "passage_id": binding.passage_id,
        "passage_policy_version": binding.passage_policy_version,
        "region_id": one.region_id,
        "source_field": one.source_field,
        "start": one.start_char,
        "text": one.text,
        "text_sha256": one.text_sha256,
    }


def _segment_record(result: RepresentationSegmentation, segment: Any) -> dict[str, Any]:
    return {
        "char_count": segment.char_count,
        "content_digest": segment.content_digest,
        # ``text`` is the newline JOIN of ``slices``. When a segment carries more
        # than one slice it spans a sealed passage boundary, and the characters
        # between those passages — which no passage selected — are not in it. A
        # consumer must cite ``slices[]``, never an offset into ``text``.
        "contiguous": len(segment.slices) <= 1,
        "context": {
            "artifact_context": dict(segment.context.artifact_context),
            "headings": list(segment.context.headings),
        },
        "input_limit": segment.input_limit,
        "join_separator": JOIN_SEPARATOR,
        "next_segment_id": segment.next_segment_id,
        "ordinal": segment.ordinal,
        "overlap_chars": segment.overlap_chars,
        "previous_segment_id": segment.previous_segment_id,
        "segment_count": segment.segment_count,
        "segment_id": segment.segment_id,
        "slices": [_slice_record(result, one) for one in segment.slices],
        "text": segment.text,
        "text_sha256": segment.text_sha256,
        "token_count": segment.token_count,
        "truncated": segment.truncated,
    }


def model_input_document(
    release: Mapping[str, Any],
    result: RepresentationSegmentation,
    *,
    settings: SegmentSettings,
) -> dict[str, Any]:
    """One representation's temporary model input, as the Extrapolator reads it."""
    adapted = result.adapted
    return {
        "adapter_version": ADAPTER_VERSION,
        "format_version": MODEL_INPUT_FORMAT_VERSION,
        "release": {
            "format_version": release["format_version"],
            "release_digest": release["release_digest"],
            "release_id": release["release_id"],
        },
        "segments": [_segment_record(result, segment) for segment in result.outcome.segments],
        "settings": _settings_facts(settings),
        "state": result.outcome.state,
        "structural_passages": [binding.as_dict() for binding in adapted.bindings],
        "text_representation": {
            "artifact_id": adapted.artifact.artifact_id,
            "coordinate_system": adapted.coordinate_system,
            "document_version_ref": adapted.document_version_ref,
            "evidence_grade": adapted.evidence_grade,
            "method": adapted.method,
            "method_config_digest": adapted.method_config_digest,
            "method_version": adapted.method_version,
            "representation_digest": adapted.representation_digest,
            "representation_id": adapted.representation_id,
            "representation_kind_and_path": adapted.representation_kind_and_path,
            "source_rendition_ref": adapted.source_rendition_ref,
            "text_chars": len(adapted.text),
        },
    }


def _sealed_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    body["receipt_sha256"] = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
    return body


def _write_canonical(path: Path, value: Mapping[str, Any]) -> str:
    payload = (canonical_json(value) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def write_model_input_segments(
    release: Mapping[str, Any],
    output_dir: Path,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    release_path: Path | None = None,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> dict[str, Any]:
    """Segment one sealed release into temporary model-input files and a receipt.

    The files are the whole seam to the Rulespec Extrapolator: it reads them, it
    never imports this module, and nothing here knows what it does with them.
    """
    output_dir = Path(output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ModelInputWriteError(f"output directory is not empty: {output_dir}")

    results = segment_document_release(
        release, settings=settings, counter=counter, rulespec_core_path=rulespec_core_path
    )

    inputs: list[dict[str, Any]] = []
    documents: list[tuple[str, dict[str, Any]]] = []
    seen_paths: set[str] = set()
    for result in results:
        adapted = result.adapted
        inputs.append(
            {
                "artifact_id": adapted.artifact.artifact_id,
                "document_version_ref": adapted.document_version_ref,
                "evidence_grade": adapted.evidence_grade,
                "passage_count": len(adapted.bindings),
                "representation_digest": adapted.representation_digest,
                "representation_id": adapted.representation_id,
                "representation_kind_and_path": adapted.representation_kind_and_path,
                "text_chars": len(adapted.text),
            }
        )
        relative = f"{SEGMENT_DIRECTORY}/{adapted.file_stem}.json"
        if relative in seen_paths:
            raise ModelInputWriteError(f"two representations would publish to one path: {relative}")
        seen_paths.add(relative)
        documents.append((relative, model_input_document(release, result, settings=settings)))

    outcomes = [result.outcome for result in results]
    segments = [segment for outcome in outcomes for segment in outcome.segments]
    coverage = [outcome.coverage for outcome in outcomes]
    checks = [
        {"detail": check.detail, "name": check.name, "status": check.status, "step": check.step}
        for check in segment_checks(outcomes)
    ]

    release_facts: dict[str, Any] = {
        "format_version": release["format_version"],
        "release_digest": release["release_digest"],
        "release_id": release["release_id"],
    }
    if release_path is not None:
        release_path = Path(release_path)
        release_facts["release_path"] = _pin_path(release_path)
        release_facts["release_path_sha256"] = hashlib.sha256(release_path.read_bytes()).hexdigest()

    failures: list[str] = [
        f"check {check['name']} failed: {check['detail']}" for check in checks if check["status"] == "fail"
    ]
    if not results:
        failures.append("the release carried no text representation to segment")
    if not segments:
        failures.append("segmentation produced no model input")

    receipt = _sealed_receipt(
        {
            "adapter_version": ADAPTER_VERSION,
            "checks": checks,
            "counts": {
                "covered_chars": sum(one.covered_chars for one in coverage),
                "duplicated_chars": sum(one.duplicated_chars for one in coverage),
                "excluded_chars": sum(one.excluded_chars for one in coverage),
                "excluded_region_count": sum(len(outcome.excluded) for outcome in outcomes),
                "field_chars": sum(one.field_chars for one in coverage),
                "passage_count": sum(len(result.adapted.bindings) for result in results),
                "representation_count": len(results),
                "segment_count": len(segments),
                "slice_count": sum(len(segment.slices) for segment in segments),
                "uncovered_chars": sum(one.uncovered_chars for one in coverage),
            },
            "failures": failures,
            "final_state": "fail" if failures else "pass",
            "format_version": MODEL_INPUT_FORMAT_VERSION,
            "inputs": inputs,
            "outputs": [
                {
                    "path": path,
                    "representation_id": document["text_representation"]["representation_id"],
                    "segment_count": len(document["segments"]),
                    "sha256": hashlib.sha256((canonical_json(document) + "\n").encode("utf-8")).hexdigest(),
                }
                for path, document in documents
            ],
            "release": release_facts,
            "settings": _settings_facts(settings),
            "step": ADAPTER_STEP,
        }
    )
    # Scan what is actually published, not only the receipt. The receipt carries
    # identifiers and digests; the model-input files carry the whole document
    # body, which is where secret-like content in a sealed release would be.
    # Reported by relative path and rule, the same shape ``scan_tree_for_secrets``
    # reports, so a refusal names the file a reader would have to go look at.
    scanned = {
        path: sorted(matched)
        for path, document in (*documents, (RECEIPT_NAME, receipt))
        if (matched := scan_text_for_secrets(canonical_json(document)))
    }
    if scanned:
        raise ModelInputWriteError(f"refusing to publish secret-like content: {scanned}")

    output_dir.mkdir(parents=True, exist_ok=True)
    for path, document in documents:
        written = _write_canonical(_contained_output_path(output_dir, path), document)
        expected = next(one["sha256"] for one in receipt["outputs"] if one["path"] == path)
        if written != expected:
            raise ModelInputWriteError(f"published model input {path} differs from its receipt digest")
    _write_canonical(_contained_output_path(output_dir, RECEIPT_NAME), receipt)
    return receipt


def read_document_release(path: Path) -> dict[str, Any]:
    """Read one sealed release from disk, refusing anything but canonical JSON."""
    path = Path(path)
    try:
        payload = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ReleaseSealError(f"cannot read DocumentRelease {path}") from error
    try:
        release = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ReleaseSealError(f"DocumentRelease {path} is not JSON") from error
    if not isinstance(release, dict):
        raise ReleaseSealError(f"DocumentRelease {path} is not an object")
    return release


def segment_release_path(
    release_path: Path,
    output_dir: Path,
    *,
    settings: SegmentSettings,
    counter: TokenCounter,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> dict[str, Any]:
    """DocumentRelease path in; temporary model-input files and a receipt out."""
    release_path = Path(release_path)
    return write_model_input_segments(
        read_document_release(release_path),
        Path(output_dir),
        settings=settings,
        counter=counter,
        release_path=release_path,
        rulespec_core_path=rulespec_core_path,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Segment a sealed DocumentRelease into temporary model-input files")
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rulespec-core", type=Path, default=DEFAULT_RULESPEC_CORE_PATH)
    args = parser.parse_args(argv)

    from spicy_regs.ontology.segmentation import TiktokenCounter

    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    receipt = segment_release_path(
        args.release,
        args.output_dir,
        settings=settings,
        counter=counter,
        rulespec_core_path=args.rulespec_core,
    )
    print(
        canonical_json(
            {
                "failures": receipt["failures"],
                "output_dir": str(args.output_dir),
                "receipt_sha256": receipt["receipt_sha256"],
                "release_digest": receipt["release"]["release_digest"],
                "segment_count": receipt["counts"]["segment_count"],
                "settings_id": receipt["settings"]["settings_id"],
                "status": receipt["final_state"],
            }
        )
    )
    return 0 if receipt["final_state"] == "pass" else 1


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

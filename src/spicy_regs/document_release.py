"""Build and validate immutable SpicyRegs ``DocumentRelease`` records.

SpicyRegs owns source capture and source-addressable structure.  This module
publishes that state as canonical JSON so consumers can validate a release
without importing SpicyRegs, RefSpec, or Rulespec source code.

The release identity recipe is intentionally small and public:

* JSON is UTF-8, sorted by key, compact, non-ASCII preserving, and rejects
  non-finite numbers.
* ``release_digest`` hashes the complete root object after removing only the
  root ``release_id`` and ``release_digest`` fields.
* ``release_id`` is ``urn:spicyregs:document-release:<digest hex>``.
* Every other record identifier hashes the fields that define that fact.

The bundled M1 fixture is synthetic but source-shaped.  It seals the exact
document and link facts needed by the first search slice, including the exact
case-sensitive phrase at Unicode codepoint range ``[2282, 2307)``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast


FORMAT_VERSION = "spicyregs-document-release/v1"
ACTUAL_FILE_FORMAT_VERSION = "spicyregs-document-release/v2"
FIXTURE_FORMAT_VERSION = "spicyregs-m1-source-fixture/v1"
DOCUMENT_ELIGIBILITY_POLICY_VERSION = "spicyregs-document-only/v1"
PASSAGE_POLICY_VERSION = "spicyregs-structural-passages/v1"
ACTUAL_DOCUMENT_ELIGIBILITY_POLICY_VERSION = "spicyregs-captured-documents/v2"
PDF_PASSAGE_POLICY_VERSION = "spicyregs-pdf-page-text-passages/v1"
MARKUP_PASSAGE_POLICY_VERSION = "spicyregs-visible-native-markup-passages/v1"
LINK_VERIFICATION_METHOD = "federal-register-document-number-exact-match"
LINK_VERIFICATION_METHOD_VERSION = "1"
COORDINATE_SYSTEM = "unicode-codepoints-half-open"
EVIDENCE_GRADES = frozenset({"source-exact", "parser-derived", "ocr-derived"})
SOURCE_EXACT_DECODING_METHODS = frozenset(
    {
        "json-field-decoding",
        "raw-utf8",
        "source-native-unicode",
        "utf-8-decoding",
    }
)

DEFAULT_FIXTURE_PATH = Path(__file__).with_name("fixtures") / "spicyregs-m1-source-fixture-v1.json"
DEFAULT_RULESPEC_CORE_PATH = Path(__file__).with_name("fixtures") / "rulespec-core-release-v1.json"
DEFAULT_DOCUMENT_RELEASE_FIXTURE_PATH = Path(__file__).with_name("fixtures") / "spicyregs-m1-document-release-v1.json"

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_FR_DOCUMENT_NUMBER = re.compile(r"^(?:19|20)[0-9]{2}-[0-9]{5}$")

JsonObject = dict[str, Any]
RecordRole = Literal["document", "relationship-context", "public-comment"]


class DocumentReleaseError(ValueError):
    """A source fixture or document release failed closed validation."""


def canonical_json(value: object) -> str:
    """Return the one canonical JSON representation used by this release."""

    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise DocumentReleaseError(f"value is not canonical JSON: {error}") from error


def canonical_digest(value: object) -> str:
    """Return a prefixed SHA-256 digest of canonical UTF-8 JSON."""

    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_digest(value: str) -> str:
    """Return a prefixed SHA-256 digest of exact UTF-8 Unicode text."""

    if not isinstance(value, str):
        raise DocumentReleaseError("addressable text must be a Unicode string")
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_record_id(record_type: str, identity: Mapping[str, Any]) -> str:
    """Hash the complete identity fields for one immutable record fact."""

    if not record_type or not re.fullmatch(r"[a-z0-9-]+", record_type):
        raise DocumentReleaseError("record type must use lowercase letters, digits, and hyphens")
    digest = canonical_digest(dict(identity)).removeprefix("sha256:")
    return f"urn:spicyregs:{record_type}:{digest}"


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DocumentReleaseError(f"{label} must be sha256:<64 lowercase hex>")
    return value


def _require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DocumentReleaseError(f"{label} must be a non-empty string")
    return value


def _requested_source_base(value: str, *, require_selection: bool) -> str:
    base, separator, selection = value.partition("#selection=")
    publisher, source_separator, collection = base.partition(":")
    if not publisher or source_separator != ":" or not collection or ":" in collection:
        raise DocumentReleaseError("requested source must be publisher:collection")
    if require_selection:
        if separator != "#selection=":
            raise DocumentReleaseError("actual-file requested source must pin its selected input")
        _require_digest(selection, "requested source selection digest")
    elif separator:
        raise DocumentReleaseError("legacy requested source cannot add a selection suffix")
    return base


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise DocumentReleaseError(f"{label} keys differ; missing={missing}, unexpected={unexpected}")


def _canonical_clone(value: object) -> Any:
    return json.loads(canonical_json(value))


def _resolve_path(value: object, path: str) -> object:
    current = value
    for part in path.split("."):
        if isinstance(current, Mapping):
            current_mapping = cast(Mapping[str, Any], current)
            if part not in current_mapping:
                raise DocumentReleaseError(f"source-native path {path!r} is missing component {part!r}")
            current = current_mapping[part]
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                raise DocumentReleaseError(f"source-native path {path!r} has an out-of-range ordinal")
            current = current[index]
        else:
            raise DocumentReleaseError(f"source-native path {path!r} cannot traverse {part!r}")
    return current


_LEGACY_SOURCE_RECORD_ROLES: dict[tuple[str, str], RecordRole] = {
    ("federal-register", "documents"): "document",
    ("regulations.gov", "documents"): "document",
    ("regulations.gov", "dockets"): "relationship-context",
    ("regulations.gov", "comments"): "public-comment",
}

_ACTUAL_SOURCE_RECORD_ROLES: dict[tuple[str, str], RecordRole] = {
    **_LEGACY_SOURCE_RECORD_ROLES,
    ("cfr", "sections"): "document",
    ("congress", "bills"): "document",
    ("congressional-research-service", "reports"): "document",
    ("gao", "reports"): "document",
    ("supreme-court", "opinions"): "document",
}


def classify_source_record(
    publisher: str,
    collection: str,
    *,
    policy_version: str = DOCUMENT_ELIGIBILITY_POLICY_VERSION,
) -> RecordRole:
    """Classify a known source record; unknown kinds fail closed."""

    if policy_version == DOCUMENT_ELIGIBILITY_POLICY_VERSION:
        roles = _LEGACY_SOURCE_RECORD_ROLES
    elif policy_version == ACTUAL_DOCUMENT_ELIGIBILITY_POLICY_VERSION:
        roles = _ACTUAL_SOURCE_RECORD_ROLES
    else:
        raise DocumentReleaseError(f"unknown document eligibility policy: {policy_version!r}")
    try:
        return roles[(publisher, collection)]
    except KeyError as error:
        raise DocumentReleaseError(
            f"unclassified source record kind: publisher={publisher!r}, collection={collection!r}"
        ) from error


@dataclass(frozen=True, slots=True)
class SourceRecordVersion:
    source_record_version_id: str
    publisher: str
    collection: str
    source_record_id: str
    source_url: str
    source_record_digest: str
    content_json: str

    @classmethod
    def create(
        cls,
        *,
        publisher: str,
        collection: str,
        source_record_id: str,
        source_url: str,
        content: Mapping[str, Any],
        eligibility_policy_version: str = DOCUMENT_ELIGIBILITY_POLICY_VERSION,
    ) -> "SourceRecordVersion":
        classify_source_record(
            publisher,
            collection,
            policy_version=eligibility_policy_version,
        )
        content_json = canonical_json(dict(content))
        digest = canonical_digest(dict(content))
        identity = {
            "collection": collection,
            "publisher": publisher,
            "source_record_digest": digest,
            "source_record_id": source_record_id,
        }
        return cls(
            source_record_version_id=stable_record_id("source-record-version", identity),
            publisher=publisher,
            collection=collection,
            source_record_id=source_record_id,
            source_url=source_url,
            source_record_digest=digest,
            content_json=content_json,
        )

    @property
    def content(self) -> JsonObject:
        value = json.loads(self.content_json)
        if not isinstance(value, dict):
            raise DocumentReleaseError("source record content must be an object")
        return value

    def as_record(self) -> JsonObject:
        return {
            "collection": self.collection,
            "content": self.content,
            "publisher": self.publisher,
            "source_record_digest": self.source_record_digest,
            "source_record_id": self.source_record_id,
            "source_record_version_id": self.source_record_version_id,
            "source_url": self.source_url,
        }


@dataclass(frozen=True, slots=True)
class DocumentVersion:
    document_version_id: str
    artifact_id: str
    publisher: str
    source_record_id: str
    source_issued_version_id: str
    document_type: str
    content_digest: str
    coordinate_system: str
    evidence_grade: str
    media_type: str

    @classmethod
    def create(
        cls,
        *,
        publisher: str,
        source_record_id: str,
        source_issued_version_id: str,
        document_type: str,
        content_digest: str,
        coordinate_system: str = "document-version",
        evidence_grade: str = "source-exact",
        media_type: str = "text/plain; charset=utf-8",
    ) -> "DocumentVersion":
        _require_digest(content_digest, "document content digest")
        if evidence_grade != "source-exact":
            raise DocumentReleaseError("a source document version must remain source-exact")
        if not coordinate_system or not media_type:
            raise DocumentReleaseError("document Artifact coordinates and media type must be named")
        identity = {
            "content_digest": content_digest,
            "document_type": document_type,
            "publisher": publisher,
            "source_issued_version_id": source_issued_version_id,
            "source_record_id": source_record_id,
        }
        document_version_id = stable_record_id("document-version", identity)
        artifact_identity = {
            "content_digest": content_digest,
            "coordinate_system": coordinate_system,
            "document_version_id": document_version_id,
            "evidence_grade": "source-exact",
            "media_type": media_type,
        }
        return cls(
            document_version_id=document_version_id,
            artifact_id=stable_record_id("artifact", artifact_identity),
            publisher=publisher,
            source_record_id=source_record_id,
            source_issued_version_id=source_issued_version_id,
            document_type=document_type,
            content_digest=content_digest,
            coordinate_system=coordinate_system,
            evidence_grade=evidence_grade,
            media_type=media_type,
        )

    @property
    def artifact_projection(self) -> JsonObject:
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": "Artifact",
            "content_digest": self.content_digest,
            "coordinate_system": self.coordinate_system,
            "evidence_grade": self.evidence_grade,
            "media_type": self.media_type,
        }

    def as_record(self) -> JsonObject:
        return {
            "artifact_projection": self.artifact_projection,
            "content_digest": self.content_digest,
            "document_type": self.document_type,
            "document_version_id": self.document_version_id,
            "publisher": self.publisher,
            "source_issued_version_id": self.source_issued_version_id,
            "source_record_id": self.source_record_id,
        }


@dataclass(frozen=True, slots=True)
class SourceRendition:
    rendition_id: str
    document_version_ref: str
    source_native_path: str | None
    source_url: str | None
    media_type: str
    bytes_digest: str

    @classmethod
    def create(
        cls,
        *,
        document_version_ref: str,
        source_native_path: str | None,
        source_url: str | None,
        media_type: str,
        bytes_digest: str,
    ) -> "SourceRendition":
        if source_native_path is None and source_url is None:
            raise DocumentReleaseError("rendition requires a source-native path or source URL")
        _require_digest(bytes_digest, "rendition bytes digest")
        identity = {
            "bytes_digest": bytes_digest,
            "document_version_ref": document_version_ref,
            "media_type": media_type,
            "source_native_path": source_native_path,
            "source_url": source_url,
        }
        return cls(
            rendition_id=stable_record_id("source-rendition", identity),
            document_version_ref=document_version_ref,
            source_native_path=source_native_path,
            source_url=source_url,
            media_type=media_type,
            bytes_digest=bytes_digest,
        )

    def as_record(self) -> JsonObject:
        return {
            "bytes_digest": self.bytes_digest,
            "document_version_ref": self.document_version_ref,
            "media_type": self.media_type,
            "rendition_id": self.rendition_id,
            "source_native_path": self.source_native_path,
            "source_url": self.source_url,
        }


@dataclass(frozen=True, slots=True)
class SourceRenditionCapture:
    capture_id: str
    source_rendition_ref: str
    observed_at: str
    retrieval_receipt_ref: str
    acquisition_release_ref: str

    @classmethod
    def create(
        cls,
        *,
        source_rendition_ref: str,
        observed_at: str,
        retrieval_receipt_ref: str,
        acquisition_release_ref: str,
    ) -> "SourceRenditionCapture":
        identity = {
            "acquisition_release_ref": acquisition_release_ref,
            "observed_at": observed_at,
            "retrieval_receipt_ref": retrieval_receipt_ref,
            "source_rendition_ref": source_rendition_ref,
        }
        return cls(
            capture_id=stable_record_id("source-rendition-capture", identity),
            source_rendition_ref=source_rendition_ref,
            observed_at=observed_at,
            retrieval_receipt_ref=retrieval_receipt_ref,
            acquisition_release_ref=acquisition_release_ref,
        )

    def as_record(self) -> JsonObject:
        return {
            "acquisition_release_ref": self.acquisition_release_ref,
            "capture_id": self.capture_id,
            "observed_at": self.observed_at,
            "retrieval_receipt_ref": self.retrieval_receipt_ref,
            "source_rendition_ref": self.source_rendition_ref,
        }


@dataclass(frozen=True, slots=True)
class TextRepresentation:
    representation_id: str
    document_version_ref: str
    representation_kind_and_path: str
    unicode_text: str
    text_digest: str
    coordinate_system: str
    evidence_grade: str
    source_rendition_ref: str | None
    method: str | None
    method_version: str | None
    method_config_digest: str | None
    artifact_id: str

    @classmethod
    def create(
        cls,
        *,
        document_version_ref: str,
        representation_kind_and_path: str,
        unicode_text: str,
        evidence_grade: str,
        source_rendition_ref: str | None = None,
        method: str | None = None,
        method_version: str | None = None,
        method_config_digest: str | None = None,
        coordinate_system: str = COORDINATE_SYSTEM,
    ) -> "TextRepresentation":
        if evidence_grade not in EVIDENCE_GRADES:
            raise DocumentReleaseError(f"unknown evidence grade: {evidence_grade!r}")
        method_fields = (method, method_version, method_config_digest)
        if any(value is not None for value in method_fields) and not all(
            isinstance(value, str) and value for value in method_fields
        ):
            raise DocumentReleaseError("text method, version, and configuration digest must be supplied together")
        if evidence_grade in {"parser-derived", "ocr-derived"}:
            if source_rendition_ref is None:
                raise DocumentReleaseError("derived text must reference its source rendition")
            if not method or not method_version or not method_config_digest:
                raise DocumentReleaseError("derived text must pin method, version, and configuration digest")
        elif method is not None and method not in SOURCE_EXACT_DECODING_METHODS:
            raise DocumentReleaseError(
                "source-exact text may name only a declared source decoding method, not derived extraction provenance"
            )
        if method_config_digest is not None:
            _require_digest(method_config_digest, "text representation method configuration digest")
        digest = text_digest(unicode_text)
        identity = {
            "coordinate_system": coordinate_system,
            "document_version_ref": document_version_ref,
            "evidence_grade": evidence_grade,
            "method": method,
            "method_config_digest": method_config_digest,
            "method_version": method_version,
            "representation_kind_and_path": representation_kind_and_path,
            "source_rendition_ref": source_rendition_ref,
            "text_digest": digest,
        }
        representation_id = stable_record_id("text-representation", identity)
        artifact_identity = {
            "content_digest": digest,
            "coordinate_system": coordinate_system,
            "evidence_grade": evidence_grade,
            "representation_id": representation_id,
        }
        return cls(
            representation_id=representation_id,
            document_version_ref=document_version_ref,
            representation_kind_and_path=representation_kind_and_path,
            unicode_text=unicode_text,
            text_digest=digest,
            coordinate_system=coordinate_system,
            evidence_grade=evidence_grade,
            source_rendition_ref=source_rendition_ref,
            method=method,
            method_version=method_version,
            method_config_digest=method_config_digest,
            artifact_id=stable_record_id("artifact", artifact_identity),
        )

    @property
    def artifact_projection(self) -> JsonObject:
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": "Artifact",
            "content_digest": self.text_digest,
            "coordinate_system": self.coordinate_system,
            "evidence_grade": self.evidence_grade,
            "media_type": "text/plain; charset=utf-8",
        }

    def as_record(self) -> JsonObject:
        return {
            "artifact_projection": self.artifact_projection,
            "coordinate_system": self.coordinate_system,
            "document_version_ref": self.document_version_ref,
            "evidence_grade": self.evidence_grade,
            "method": self.method,
            "method_config_digest": self.method_config_digest,
            "method_version": self.method_version,
            "representation_id": self.representation_id,
            "representation_kind_and_path": self.representation_kind_and_path,
            "source_rendition_ref": self.source_rendition_ref,
            "text_digest": self.text_digest,
            "unicode_text": self.unicode_text,
        }


@dataclass(frozen=True, slots=True)
class StructuralPassage:
    passage_id: str
    document_version_ref: str
    text_representation_ref: str
    representation_digest: str
    passage_policy_version: str
    selector_kind: str
    coordinate_system: str
    start: int
    end: int
    selected_text_digest: str
    evidence_grade: str
    fragment_id: str

    @classmethod
    def create(
        cls,
        *,
        representation: TextRepresentation,
        start: int,
        end: int,
        passage_policy_version: str = PASSAGE_POLICY_VERSION,
        selector_kind: str = "TextPositionSelector",
    ) -> "StructuralPassage":
        if isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, int) or not isinstance(end, int):
            raise DocumentReleaseError("passage coordinates must be integers")
        if start < 0 or end <= start or end > len(representation.unicode_text):
            raise DocumentReleaseError(
                f"passage coordinates [{start}, {end}) are outside representation length {len(representation.unicode_text)}"
            )
        selected_digest = text_digest(representation.unicode_text[start:end])
        selector = {
            "coordinate_system": representation.coordinate_system,
            "end": end,
            "selector_type": selector_kind,
            "start": start,
        }
        identity = {
            "document_version_ref": representation.document_version_ref,
            "passage_policy_version": passage_policy_version,
            "selected_text_digest": selected_digest,
            "selector": selector,
            "text_representation_ref": representation.representation_id,
        }
        passage_id = stable_record_id("structural-passage", identity)
        fragment_identity = {
            "passage_id": passage_id,
            "selected_text_digest": selected_digest,
            "source_artifact_ref": representation.artifact_id,
        }
        return cls(
            passage_id=passage_id,
            document_version_ref=representation.document_version_ref,
            text_representation_ref=representation.representation_id,
            representation_digest=representation.text_digest,
            passage_policy_version=passage_policy_version,
            selector_kind=selector_kind,
            coordinate_system=representation.coordinate_system,
            start=start,
            end=end,
            selected_text_digest=selected_digest,
            evidence_grade=representation.evidence_grade,
            fragment_id=stable_record_id("source-fragment", fragment_identity),
        )

    @property
    def selector(self) -> JsonObject:
        return {
            "coordinate_system": self.coordinate_system,
            "end": self.end,
            "selector_type": self.selector_kind,
            "start": self.start,
        }

    def source_fragment_projection(self, artifact_id: str) -> JsonObject:
        return {
            "fragment_id": self.fragment_id,
            "fragment_type": "SourceFragment",
            "selected_text_digest": self.selected_text_digest,
            "selector": self.selector,
            "source_artifact_digest": self.representation_digest,
            "source_artifact_ref": artifact_id,
        }

    def as_record(self, artifact_id: str) -> JsonObject:
        return {
            "coordinate_system": self.coordinate_system,
            "document_version_ref": self.document_version_ref,
            "end": self.end,
            "evidence_grade": self.evidence_grade,
            "passage_id": self.passage_id,
            "passage_policy_version": self.passage_policy_version,
            "representation_digest": self.representation_digest,
            "selected_text_digest": self.selected_text_digest,
            "selector_kind": self.selector_kind,
            "source_fragment_projection": self.source_fragment_projection(artifact_id),
            "start": self.start,
            "text_representation_ref": self.text_representation_ref,
        }


@dataclass(frozen=True, slots=True)
class SourceObservation:
    observation_id: str
    document_version_ref: str
    source_record_version_ref: str
    source_native_path: str
    source_native_key_or_ordinal: str
    raw_value_json: str
    source_record_digest: str
    observation_kind: str

    @classmethod
    def create(
        cls,
        *,
        document_version_ref: str,
        source_record_version_ref: str,
        source_native_path: str,
        source_native_key_or_ordinal: str,
        raw_value: object,
        source_record_digest: str,
        observation_kind: str,
    ) -> "SourceObservation":
        _require_digest(source_record_digest, "source observation record digest")
        raw_json = canonical_json(raw_value)
        identity = {
            "observation_kind": observation_kind,
            "raw_value": json.loads(raw_json),
            "source_native_key_or_ordinal": source_native_key_or_ordinal,
            "source_native_path": source_native_path,
            "source_record_digest": source_record_digest,
            "source_record_version_ref": source_record_version_ref,
        }
        return cls(
            observation_id=stable_record_id("source-observation", identity),
            document_version_ref=document_version_ref,
            source_record_version_ref=source_record_version_ref,
            source_native_path=source_native_path,
            source_native_key_or_ordinal=source_native_key_or_ordinal,
            raw_value_json=raw_json,
            source_record_digest=source_record_digest,
            observation_kind=observation_kind,
        )

    @property
    def raw_value(self) -> object:
        return json.loads(self.raw_value_json)

    def as_record(self) -> JsonObject:
        return {
            "document_version_ref": self.document_version_ref,
            "observation_id": self.observation_id,
            "observation_kind": self.observation_kind,
            "raw_value": self.raw_value,
            "source_native_key_or_ordinal": self.source_native_key_or_ordinal,
            "source_native_path": self.source_native_path,
            "source_record_digest": self.source_record_digest,
            "source_record_version_ref": self.source_record_version_ref,
        }


@dataclass(frozen=True, slots=True)
class SourceObservationCapture:
    capture_id: str
    source_observation_ref: str
    observed_at: str
    retrieval_receipt_ref: str
    acquisition_release_ref: str

    @classmethod
    def create(
        cls,
        *,
        source_observation_ref: str,
        observed_at: str,
        retrieval_receipt_ref: str,
        acquisition_release_ref: str,
    ) -> "SourceObservationCapture":
        identity = {
            "acquisition_release_ref": acquisition_release_ref,
            "observed_at": observed_at,
            "retrieval_receipt_ref": retrieval_receipt_ref,
            "source_observation_ref": source_observation_ref,
        }
        return cls(
            capture_id=stable_record_id("source-observation-capture", identity),
            source_observation_ref=source_observation_ref,
            observed_at=observed_at,
            retrieval_receipt_ref=retrieval_receipt_ref,
            acquisition_release_ref=acquisition_release_ref,
        )

    def as_record(self) -> JsonObject:
        return {
            "acquisition_release_ref": self.acquisition_release_ref,
            "capture_id": self.capture_id,
            "observed_at": self.observed_at,
            "retrieval_receipt_ref": self.retrieval_receipt_ref,
            "source_observation_ref": self.source_observation_ref,
        }


@dataclass(frozen=True, slots=True)
class SourceLink:
    source_link_id: str
    source_record_version_ref: str
    target_source_record_version_ref: str
    source_field: str
    raw_value_json: str
    origin: str = "source-stated"

    @classmethod
    def create(
        cls,
        *,
        source_record_version_ref: str,
        target_source_record_version_ref: str,
        source_field: str,
        raw_value: object,
    ) -> "SourceLink":
        raw_json = canonical_json(raw_value)
        identity = {
            "origin": "source-stated",
            "raw_value": json.loads(raw_json),
            "source_field": source_field,
            "source_record_version_ref": source_record_version_ref,
            "target_source_record_version_ref": target_source_record_version_ref,
        }
        return cls(
            source_link_id=stable_record_id("source-link", identity),
            source_record_version_ref=source_record_version_ref,
            target_source_record_version_ref=target_source_record_version_ref,
            source_field=source_field,
            raw_value_json=raw_json,
        )

    @property
    def raw_value(self) -> object:
        return json.loads(self.raw_value_json)

    def as_record(self) -> JsonObject:
        return {
            "origin": self.origin,
            "raw_value": self.raw_value,
            "source_field": self.source_field,
            "source_link_id": self.source_link_id,
            "source_record_version_ref": self.source_record_version_ref,
            "target_source_record_version_ref": self.target_source_record_version_ref,
        }


LinkFailure = Literal[
    "missing-source-record",
    "missing-target-record",
    "missing-source-digest",
    "missing-target-digest",
    "malformed-fr-doc-num",
    "document-number-mismatch",
]


def _verification_payload(
    link: SourceLink,
    source: SourceRecordVersion | None,
    target: SourceRecordVersion | None,
) -> tuple[list[JsonObject], str, LinkFailure | None]:
    source_resolves = source is not None
    target_resolves = target is not None
    source_digest = source.source_record_digest if source is not None else None
    target_digest = target.source_record_digest if target is not None else None
    grammar_passes = isinstance(link.raw_value, str) and _FR_DOCUMENT_NUMBER.fullmatch(link.raw_value) is not None
    target_value = target.content.get("document_number") if target is not None else None
    exact_match = grammar_passes and isinstance(target_value, str) and link.raw_value == target_value
    checks = [
        {"check": "source-record-resolves", "passed": source_resolves},
        {"check": "target-record-resolves", "passed": target_resolves},
        {"check": "source-digest-resolves", "passed": source_digest is not None},
        {"check": "target-digest-resolves", "passed": target_digest is not None},
        {"check": "fr-doc-num-grammar", "passed": grammar_passes},
        {"check": "exact-document-number-match", "passed": exact_match},
    ]
    if source is None:
        return checks, "failed", "missing-source-record"
    if target is None:
        return checks, "failed", "missing-target-record"
    if source_digest is None:
        return checks, "failed", "missing-source-digest"
    if target_digest is None:
        return checks, "failed", "missing-target-digest"
    if not grammar_passes:
        return checks, "failed", "malformed-fr-doc-num"
    if not exact_match:
        return checks, "failed", "document-number-mismatch"
    return checks, "verified", None


def make_link_verification_receipt(
    link: SourceLink,
    *,
    source: SourceRecordVersion | None,
    target: SourceRecordVersion | None,
) -> JsonObject:
    """Verify one source-stated Regulations.gov cross-post deterministically."""

    checks, outcome, failure_reason = _verification_payload(link, source, target)
    payload: JsonObject = {
        "checks": checks,
        "comparison_method": LINK_VERIFICATION_METHOD,
        "comparison_method_version": LINK_VERIFICATION_METHOD_VERSION,
        "failure_reason": failure_reason,
        "outcome": outcome,
        "raw_value": link.raw_value,
        "source_field": link.source_field,
        "source_link_ref": link.source_link_id,
        "source_record_digest": source.source_record_digest if source is not None else None,
        "source_record_ref": source.source_record_version_id if source is not None else None,
        "target_record_digest": target.source_record_digest if target is not None else None,
        "target_record_ref": target.source_record_version_id if target is not None else None,
    }
    payload["verification_id"] = stable_record_id("link-verification-receipt", payload)
    return payload


def _expand_layout(parts: object, label: str) -> str:
    if not isinstance(parts, list) or not parts:
        raise DocumentReleaseError(f"{label} must be a non-empty layout list")
    output: list[str] = []
    for index, part in enumerate(parts):
        if not isinstance(part, Mapping):
            raise DocumentReleaseError(f"{label}[{index}] must be an object")
        part_mapping = cast(Mapping[str, Any], part)
        if set(part_mapping) == {"text"} and isinstance(part_mapping["text"], str):
            output.append(part_mapping["text"])
        elif set(part_mapping) == {"repeat", "count"}:
            repeated = part_mapping["repeat"]
            count = part_mapping["count"]
            if not isinstance(repeated, str) or len(repeated) != 1:
                raise DocumentReleaseError(f"{label}[{index}].repeat must be one Unicode codepoint")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise DocumentReleaseError(f"{label}[{index}].count must be a non-negative integer")
            output.append(repeated * count)
        else:
            raise DocumentReleaseError(f"{label}[{index}] must contain text or repeat/count")
    return "".join(output)


def _expand_content(content: object) -> JsonObject:
    if not isinstance(content, Mapping):
        raise DocumentReleaseError("fixture source record content must be an object")
    expanded: JsonObject = {}
    for key, value in content.items():
        if not isinstance(key, str):
            raise DocumentReleaseError("fixture source record keys must be strings")
        if key.endswith("_layout"):
            target = key.removesuffix("_layout")
            if target in content or target in expanded:
                raise DocumentReleaseError(f"fixture declares both {target!r} and its layout")
            expanded[target] = _expand_layout(value, key)
        else:
            expanded[key] = _canonical_clone(value)
    return expanded


def _validate_fixture(fixture: object) -> JsonObject:
    if not isinstance(fixture, dict):
        raise DocumentReleaseError("source fixture root must be an object")
    _require_exact_keys(
        fixture,
        {
            "acquisition_release_ref",
            "coverage_gaps",
            "fixture_digest",
            "fixture_id",
            "format_version",
            "links",
            "records",
            "released_at",
            "requested_sources",
        },
        "source fixture",
    )
    if fixture["format_version"] != FIXTURE_FORMAT_VERSION:
        raise DocumentReleaseError("source fixture format version differs")
    expected = canonical_digest({key: value for key, value in fixture.items() if key != "fixture_digest"})
    if fixture["fixture_digest"] != expected:
        raise DocumentReleaseError("source fixture digest differs")
    return fixture


def _load_fixture(path: Path) -> JsonObject:
    try:
        fixture = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DocumentReleaseError(f"cannot read source fixture {path}: {error}") from error
    return _validate_fixture(fixture)


def _fixture_representation_text(
    content: Mapping[str, Any],
    spec: Mapping[str, Any],
    *,
    label: str,
) -> tuple[str, str, str]:
    """Resolve one legacy source field or one prepared actual-file text value."""

    inline_text = spec.get("unicode_text")
    source_path = spec.get("source_native_path")
    if inline_text is not None:
        if not isinstance(inline_text, str):
            raise DocumentReleaseError(f"{label} unicode_text must be a Unicode string")
        if source_path is not None:
            raise DocumentReleaseError(f"{label} cannot declare both unicode_text and source_native_path")
        key = _require_string(spec.get("key"), f"{label} key")
        kind_and_path = _require_string(
            spec.get("representation_kind_and_path"),
            f"{label} representation_kind_and_path",
        )
        return key, kind_and_path, inline_text
    path = _require_string(source_path, f"{label} path")
    text = _resolve_path(content, path)
    if not isinstance(text, str):
        raise DocumentReleaseError(f"{label} path must resolve to Unicode text")
    return path, f"source-record-field:{path}", text


def _load_rulespec_core_release(
    path: Path,
    *,
    allowed_statuses: frozenset[str],
) -> JsonObject:
    try:
        fixture = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DocumentReleaseError(f"cannot read Rulespec Core release {path}: {error}") from error
    if not isinstance(fixture, dict):
        raise DocumentReleaseError("Rulespec Core release root must be an object")
    required_root = {
        "conformance_fixture_artifacts",
        "record_type",
        "release_digest",
        "release_id",
        "release_status",
        "schema_artifacts",
        "validator_artifacts",
        "version",
    }
    _require_exact_keys(fixture, required_root, "Rulespec Core release")
    if fixture["record_type"] != "RulespecCoreRelease":
        raise DocumentReleaseError("Rulespec Core release has the wrong record type")
    if fixture["release_status"] not in allowed_statuses:
        raise DocumentReleaseError(
            "Rulespec Core release status is not allowed here: "
            f"{fixture['release_status']!r}"
        )
    body = {key: value for key, value in fixture.items() if key not in {"release_id", "release_digest"}}
    expected_digest = canonical_digest(body)
    expected_id = "urn:rulespec:core:" + expected_digest.removeprefix("sha256:")
    if fixture["release_digest"] != expected_digest or fixture["release_id"] != expected_id:
        raise DocumentReleaseError("Rulespec Core release identity differs from its canonical bytes")
    for field in ("schema_artifacts", "validator_artifacts", "conformance_fixture_artifacts"):
        artifacts = fixture[field]
        if not isinstance(artifacts, list) or not artifacts:
            raise DocumentReleaseError(f"Rulespec Core {field} must be a non-empty array")
        for artifact in artifacts:
            if not isinstance(artifact, Mapping):
                raise DocumentReleaseError(f"Rulespec Core {field} entries must be objects")
            _require_exact_keys(artifact, {"artifact_digest", "media_type", "name"}, f"Rulespec Core {field} entry")
            _require_digest(artifact["artifact_digest"], f"Rulespec Core {field} artifact digest")
    return fixture


def _sorted_records(records: Sequence[JsonObject], id_field: str) -> list[JsonObject]:
    return sorted((_canonical_clone(record) for record in records), key=lambda row: str(row[id_field]))


def _passage_coverage(
    representations: Sequence[TextRepresentation],
    passages: Sequence[StructuralPassage],
    *,
    default_policy_version: str,
) -> list[JsonObject]:
    by_representation: dict[str, list[StructuralPassage]] = {}
    for passage in passages:
        by_representation.setdefault(passage.text_representation_ref, []).append(passage)
    output: list[JsonObject] = []
    for representation in representations:
        representation_passages = by_representation.get(representation.representation_id, [])
        policy_versions = {passage.passage_policy_version for passage in representation_passages}
        if len(policy_versions) > 1:
            raise DocumentReleaseError("one text representation cannot mix passage policies")
        passage_policy_version = next(iter(policy_versions), default_policy_version)
        cursor = 0
        regions: list[JsonObject] = []
        for passage in sorted(representation_passages, key=lambda item: item.start):
            if passage.start < cursor:
                raise DocumentReleaseError("structural passages overlap in one text representation")
            if passage.start > cursor:
                regions.append(
                    {
                        "end": passage.start,
                        "passage_ref": None,
                        "reason": "outside-sealed-structural-passages",
                        "start": cursor,
                        "state": "excluded",
                    }
                )
            regions.append(
                {
                    "end": passage.end,
                    "passage_ref": passage.passage_id,
                    "reason": None,
                    "start": passage.start,
                    "state": "processed",
                }
            )
            cursor = passage.end
        if cursor < len(representation.unicode_text):
            regions.append(
                {
                    "end": len(representation.unicode_text),
                    "passage_ref": None,
                    "reason": "outside-sealed-structural-passages",
                    "start": cursor,
                    "state": "excluded",
                }
            )
        if not regions and not representation.unicode_text:
            regions.append(
                {"end": 0, "passage_ref": None, "reason": "empty-representation", "start": 0, "state": "excluded"}
            )
        identity = {
            "passage_policy_version": passage_policy_version,
            "regions": regions,
            "text_representation_ref": representation.representation_id,
        }
        output.append(
            {
                "coverage_id": stable_record_id("passage-coverage", identity),
                "passage_policy_version": passage_policy_version,
                "regions": regions,
                "text_representation_ref": representation.representation_id,
            }
        )
    return _sorted_records(output, "coverage_id")


def seal_document_release(body: Mapping[str, Any]) -> JsonObject:
    """Seal a release body, overwriting no nested values."""

    if "release_id" in body or "release_digest" in body:
        raise DocumentReleaseError("unsealed release body must not contain release identity fields")
    sealed = _canonical_clone(dict(body))
    digest = canonical_digest(sealed)
    sealed["release_digest"] = digest
    sealed["release_id"] = "urn:spicyregs:document-release:" + digest.removeprefix("sha256:")
    return _canonical_clone(sealed)


def _actual_file_release_status(core_status: object, source_input_type: object) -> str:
    """Keep evaluation locks non-promotable regardless of dependency status."""

    if source_input_type == "EvaluationSourceLock" or core_status == "fixture":
        return "conformance"
    if source_input_type == "CapturedFileManifest" and core_status in {
        "candidate",
        "published",
    }:
        return "candidate"
    raise DocumentReleaseError("actual-file release status inputs are unsupported")


def build_document_release(
    fixture_path: Path | Mapping[str, Any] = DEFAULT_FIXTURE_PATH,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
    *,
    format_version: str = FORMAT_VERSION,
    source_input_digest: str | None = None,
    source_input_id: str | None = None,
    source_input_path: str | None = None,
    source_input_type: str | None = None,
) -> JsonObject:
    """Build a sealed ``DocumentRelease`` from a validated prepared source fixture."""

    if format_version == FORMAT_VERSION:
        document_policy_version = DOCUMENT_ELIGIBILITY_POLICY_VERSION
        allowed_passage_policies = frozenset({PASSAGE_POLICY_VERSION})
        default_passage_policy = PASSAGE_POLICY_VERSION
        allowed_core_statuses = frozenset({"fixture"})
        if any(
            value is not None
            for value in (source_input_digest, source_input_id, source_input_path, source_input_type)
        ):
            raise DocumentReleaseError("legacy DocumentRelease cannot declare a source input pin")
    elif format_version == ACTUAL_FILE_FORMAT_VERSION:
        document_policy_version = ACTUAL_DOCUMENT_ELIGIBILITY_POLICY_VERSION
        allowed_passage_policies = frozenset(
            {PDF_PASSAGE_POLICY_VERSION, MARKUP_PASSAGE_POLICY_VERSION}
        )
        default_passage_policy = MARKUP_PASSAGE_POLICY_VERSION
        allowed_core_statuses = frozenset({"candidate", "fixture", "published"})
        source_input_type = _require_string(
            source_input_type,
            "actual-file DocumentRelease source input type",
        )
        source_input_id = _require_string(
            source_input_id,
            "actual-file DocumentRelease source input ID",
        )
        source_input_digest = _require_digest(
            source_input_digest,
            "actual-file DocumentRelease source input digest",
        )
        source_input_path = _require_string(
            source_input_path,
            "actual-file DocumentRelease source input path",
        )
    else:
        raise DocumentReleaseError(f"unknown DocumentRelease format version: {format_version!r}")

    fixture = (
        _validate_fixture(_canonical_clone(dict(fixture_path)))
        if isinstance(fixture_path, Mapping)
        else _load_fixture(fixture_path)
    )
    rulespec_core = _load_rulespec_core_release(
        rulespec_core_path,
        allowed_statuses=allowed_core_statuses,
    )
    source_records: list[SourceRecordVersion] = []
    documents: list[DocumentVersion] = []
    renditions: list[SourceRendition] = []
    rendition_captures: list[SourceRenditionCapture] = []
    representations: list[TextRepresentation] = []
    passages: list[StructuralPassage] = []
    observations: list[SourceObservation] = []
    observation_captures: list[SourceObservationCapture] = []
    fixture_records = fixture["records"]
    if not isinstance(fixture_records, list):
        raise DocumentReleaseError("source fixture records must be an array")
    records_by_key: dict[str, SourceRecordVersion] = {}
    documents_by_key: dict[str, DocumentVersion] = {}
    representations_by_key: dict[tuple[str, str], TextRepresentation] = {}
    record_capture_refs: dict[str, list[str]] = {}
    coverage_entries: list[JsonObject] = []

    for index, spec in enumerate(fixture_records):
        if not isinstance(spec, Mapping):
            raise DocumentReleaseError(f"fixture record {index} must be an object")
        spec = cast(Mapping[str, Any], spec)
        key = _require_string(spec.get("key"), f"fixture record {index}.key")
        if key in records_by_key:
            raise DocumentReleaseError(f"duplicate fixture record key: {key}")
        publisher = _require_string(spec.get("publisher"), f"fixture record {key}.publisher")
        collection = _require_string(spec.get("collection"), f"fixture record {key}.collection")
        role = classify_source_record(
            publisher,
            collection,
            policy_version=document_policy_version,
        )
        content = _expand_content(spec.get("content"))
        source_record = SourceRecordVersion.create(
            publisher=publisher,
            collection=collection,
            source_record_id=_require_string(spec.get("source_record_id"), f"fixture record {key}.source_record_id"),
            source_url=_require_string(spec.get("source_url"), f"fixture record {key}.source_url"),
            content=content,
            eligibility_policy_version=document_policy_version,
        )
        records_by_key[key] = source_record
        source_records.append(source_record)
        record_capture_refs[key] = []
        if role != "document":
            if spec.get("document") is not None:
                raise DocumentReleaseError(f"excluded record {key} cannot declare a document version")
            coverage_entries.append(
                {
                    "collection": collection,
                    "reason": f"document-only policy classifies record as {role}",
                    "record_ref": source_record.source_record_version_id,
                    "source": publisher,
                    "state": "excluded",
                }
            )
            continue
        document_spec = spec.get("document")
        if not isinstance(document_spec, Mapping):
            raise DocumentReleaseError(f"document record {key} must declare document metadata")
        representation_specs = spec.get("representations")
        if not isinstance(representation_specs, list) or not representation_specs:
            raise DocumentReleaseError(f"fixture document {key} must declare a text representation")
        prepared_representations: list[tuple[str, str, str, Mapping[str, Any]]] = []
        for representation_spec in representation_specs:
            if not isinstance(representation_spec, Mapping):
                raise DocumentReleaseError(f"fixture document {key} representation must be an object")
            representation_key, kind_and_path, text = _fixture_representation_text(
                content,
                representation_spec,
                label=f"fixture document {key} representation",
            )
            if any(existing[0] == representation_key for existing in prepared_representations):
                raise DocumentReleaseError(f"fixture document {key} has duplicate representation key")
            prepared_representations.append((representation_key, kind_and_path, text, representation_spec))
        declared_content_digest = document_spec.get("content_digest")
        content_representation_key = document_spec.get("content_representation_key")
        if declared_content_digest is not None:
            if content_representation_key is not None or document_spec.get("content_path") is not None:
                raise DocumentReleaseError(
                    f"fixture record {key} document content must choose bytes or a text representation"
                )
            document_content_digest = _require_digest(
                declared_content_digest,
                f"fixture record {key}.content_digest",
            )
            document_coordinate_system = _require_string(
                document_spec.get("content_coordinate_system"),
                f"fixture record {key}.content_coordinate_system",
            )
            document_media_type = _require_string(
                document_spec.get("content_media_type"),
                f"fixture record {key}.content_media_type",
            )
        elif content_representation_key is None:
            content_path = _require_string(document_spec.get("content_path"), f"fixture record {key}.content_path")
            document_text = _resolve_path(content, content_path)
            if not isinstance(document_text, str):
                raise DocumentReleaseError(f"fixture record {key} document content must resolve to Unicode text")
            document_content_digest = text_digest(document_text)
            document_coordinate_system = "document-version"
            document_media_type = "text/plain; charset=utf-8"
        else:
            content_representation_key = _require_string(
                content_representation_key,
                f"fixture record {key}.content_representation_key",
            )
            matching_text = [
                text for representation_key, _, text, _ in prepared_representations
                if representation_key == content_representation_key
            ]
            if len(matching_text) != 1:
                raise DocumentReleaseError(
                    f"fixture record {key} document names an unknown content representation"
                )
            document_text = matching_text[0]
            document_content_digest = text_digest(document_text)
            document_coordinate_system = "document-version"
            document_media_type = "text/plain; charset=utf-8"
        document = DocumentVersion.create(
            publisher=publisher,
            source_record_id=source_record.source_record_id,
            source_issued_version_id=_require_string(
                document_spec.get("source_issued_version_id"), f"fixture record {key}.source_issued_version_id"
            ),
            document_type=_require_string(document_spec.get("document_type"), f"fixture record {key}.document_type"),
            content_digest=document_content_digest,
            coordinate_system=document_coordinate_system,
            media_type=document_media_type,
        )
        documents.append(document)
        documents_by_key[key] = document
        capture_specs = spec.get("captures")
        if not isinstance(capture_specs, list) or not capture_specs:
            raise DocumentReleaseError(f"fixture document {key} must declare at least one capture")
        rendition_specs = spec.get("renditions")
        if rendition_specs is None:
            rendition_specs = [
                {
                    "bytes_digest": source_record.source_record_digest,
                    "key": "source-record",
                    "media_type": "application/json",
                    "source_native_path": f"{collection}:{source_record.source_record_id}",
                    "source_url": source_record.source_url,
                }
            ]
        if not isinstance(rendition_specs, list) or not rendition_specs:
            raise DocumentReleaseError(f"fixture document {key} must declare at least one rendition")
        renditions_by_key: dict[str, SourceRendition] = {}
        for rendition_spec in rendition_specs:
            if not isinstance(rendition_spec, Mapping):
                raise DocumentReleaseError(f"fixture document {key} rendition must be an object")
            rendition_key = _require_string(
                rendition_spec.get("key"),
                f"fixture document {key} rendition key",
            )
            if rendition_key in renditions_by_key:
                raise DocumentReleaseError(f"fixture document {key} has duplicate rendition key")
            source_native_path = rendition_spec.get("source_native_path")
            source_url = rendition_spec.get("source_url")
            if source_native_path is not None:
                source_native_path = _require_string(
                    source_native_path,
                    f"fixture document {key} rendition source_native_path",
                )
            if source_url is not None:
                source_url = _require_string(source_url, f"fixture document {key} rendition source_url")
            rendition = SourceRendition.create(
                document_version_ref=document.document_version_id,
                source_native_path=source_native_path,
                source_url=source_url,
                media_type=_require_string(
                    rendition_spec.get("media_type"),
                    f"fixture document {key} rendition media_type",
                ),
                bytes_digest=_require_digest(
                    rendition_spec.get("bytes_digest"),
                    f"fixture document {key} rendition bytes digest",
                ),
            )
            renditions_by_key[rendition_key] = rendition
            renditions.append(rendition)
            for capture_spec in capture_specs:
                if not isinstance(capture_spec, Mapping):
                    raise DocumentReleaseError(f"fixture document {key} capture must be an object")
                capture = SourceRenditionCapture.create(
                    source_rendition_ref=rendition.rendition_id,
                    observed_at=_require_string(capture_spec.get("observed_at"), f"fixture record {key} observed_at"),
                    retrieval_receipt_ref=_require_string(
                        capture_spec.get("retrieval_receipt_ref"), f"fixture record {key} retrieval_receipt_ref"
                    ),
                    acquisition_release_ref=_require_string(
                        fixture.get("acquisition_release_ref"), "fixture acquisition_release_ref"
                    ),
                )
                rendition_captures.append(capture)
                record_capture_refs[key].append(capture.capture_id)
        for representation_key, kind_and_path, text, representation_spec in prepared_representations:
            method_config = representation_spec.get("method_config", {})
            if not isinstance(method_config, Mapping):
                raise DocumentReleaseError(f"fixture document {key} method_config must be an object")
            rendition_key = representation_spec.get("source_rendition_key")
            if rendition_key is None:
                if len(renditions_by_key) != 1:
                    raise DocumentReleaseError(
                        f"fixture document {key} representation must name its source rendition"
                    )
                source_rendition = next(iter(renditions_by_key.values()))
            else:
                source_rendition = renditions_by_key.get(
                    _require_string(rendition_key, f"fixture document {key} source_rendition_key")
                )
                if source_rendition is None:
                    raise DocumentReleaseError(
                        f"fixture document {key} representation names an unknown source rendition"
                    )
            representation = TextRepresentation.create(
                document_version_ref=document.document_version_id,
                representation_kind_and_path=kind_and_path,
                unicode_text=text,
                evidence_grade=_require_string(
                    representation_spec.get("evidence_grade"), f"fixture document {key} evidence_grade"
                ),
                source_rendition_ref=source_rendition.rendition_id,
                method=(str(representation_spec["method"]) if representation_spec.get("method") is not None else None),
                method_version=(
                    str(representation_spec["method_version"])
                    if representation_spec.get("method_version") is not None
                    else None
                ),
                method_config_digest=canonical_digest(dict(method_config)),
            )
            representations.append(representation)
            representations_by_key[(key, representation_key)] = representation
        passage_specs = spec.get("passages")
        if not isinstance(passage_specs, list) or not passage_specs:
            raise DocumentReleaseError(f"fixture document {key} must declare structural passages")
        for passage_spec in passage_specs:
            if not isinstance(passage_spec, Mapping):
                raise DocumentReleaseError(f"fixture document {key} passage must be an object")
            representation_key = passage_spec.get("representation_key", passage_spec.get("representation_path"))
            representation_key = _require_string(
                representation_key,
                f"fixture document {key} passage representation",
            )
            representation = representations_by_key.get((key, representation_key))
            if representation is None:
                raise DocumentReleaseError(f"fixture document {key} passage references an unknown representation")
            end = passage_spec.get("end")
            if end == "full":
                end = len(representation.unicode_text)
            start = passage_spec.get("start")
            if isinstance(start, bool) or not isinstance(start, int):
                raise DocumentReleaseError(f"fixture document {key} passage start must be an integer")
            if isinstance(end, bool) or not isinstance(end, int):
                raise DocumentReleaseError(f"fixture document {key} passage end must be an integer or full")
            passage = StructuralPassage.create(
                representation=representation,
                start=start,
                end=end,
                passage_policy_version=_require_string(
                    passage_spec.get("passage_policy_version", default_passage_policy),
                    f"fixture document {key} passage policy version",
                ),
            )
            if passage.passage_policy_version not in allowed_passage_policies:
                raise DocumentReleaseError(
                    f"fixture document {key} uses a passage policy outside the release profile"
                )
            expected_text = passage_spec.get("expected_text")
            if expected_text is not None and representation.unicode_text[passage.start : passage.end] != expected_text:
                raise DocumentReleaseError(f"fixture document {key} passage text differs from its sealed expectation")
            passages.append(passage)
        observation_specs = spec.get("observations", [])
        if not isinstance(observation_specs, list):
            raise DocumentReleaseError(f"fixture document {key} observations must be an array")
        for observation_spec in observation_specs:
            if not isinstance(observation_spec, Mapping):
                raise DocumentReleaseError(f"fixture document {key} observation must be an object")
            path = _require_string(
                observation_spec.get("source_native_path"), f"fixture document {key} observation path"
            )
            ordinal = _require_string(
                observation_spec.get("source_native_key_or_ordinal"),
                f"fixture document {key} observation ordinal",
            )
            raw_value = _resolve_path(content, f"{path}.{ordinal}" if ordinal.isdigit() else path)
            observation = SourceObservation.create(
                document_version_ref=document.document_version_id,
                source_record_version_ref=source_record.source_record_version_id,
                source_native_path=path,
                source_native_key_or_ordinal=ordinal,
                raw_value=raw_value,
                source_record_digest=source_record.source_record_digest,
                observation_kind=_require_string(
                    observation_spec.get("observation_kind"), f"fixture document {key} observation_kind"
                ),
            )
            observations.append(observation)
            for capture_spec in capture_specs:
                observation_capture = SourceObservationCapture.create(
                    source_observation_ref=observation.observation_id,
                    observed_at=str(capture_spec["observed_at"]),
                    retrieval_receipt_ref=str(capture_spec["retrieval_receipt_ref"]),
                    acquisition_release_ref=str(fixture["acquisition_release_ref"]),
                )
                observation_captures.append(observation_capture)
                record_capture_refs[key].append(observation_capture.capture_id)
        coverage_entries.append(
            {
                "collection": collection,
                "reason": None,
                "record_ref": source_record.source_record_version_id,
                "source": publisher,
                "state": "captured",
            }
        )

    source_links: list[JsonObject] = []
    verification_receipts: list[JsonObject] = []
    link_specs = fixture["links"]
    if not isinstance(link_specs, list):
        raise DocumentReleaseError("source fixture links must be an array")
    for link_spec in link_specs:
        if not isinstance(link_spec, Mapping):
            raise DocumentReleaseError("source fixture link must be an object")
        source_key = _require_string(link_spec.get("source_record_key"), "link source_record_key")
        target_key = _require_string(link_spec.get("target_record_key"), "link target_record_key")
        source = records_by_key.get(source_key)
        target = records_by_key.get(target_key)
        if source is None or target is None:
            raise DocumentReleaseError("sealed fixture links must name existing source records")
        source_field = _require_string(link_spec.get("source_field"), "link source_field")
        raw_value = _resolve_path(source.content, source_field.split(".", maxsplit=1)[-1])
        link = SourceLink.create(
            source_record_version_ref=source.source_record_version_id,
            target_source_record_version_ref=target.source_record_version_id,
            source_field=source_field,
            raw_value=raw_value,
        )
        receipt = make_link_verification_receipt(link, source=source, target=target)
        if receipt["outcome"] != link_spec.get("expected_outcome"):
            raise DocumentReleaseError("link verification outcome differs from sealed fixture")
        if receipt["failure_reason"] != link_spec.get("expected_failure_reason"):
            raise DocumentReleaseError("link verification failure reason differs from sealed fixture")
        source_links.append(link.as_record())
        verification_receipts.append(receipt)

    coverage_gaps = fixture["coverage_gaps"]
    if not isinstance(coverage_gaps, list):
        raise DocumentReleaseError("source fixture coverage_gaps must be an array")
    for gap in coverage_gaps:
        if not isinstance(gap, Mapping):
            raise DocumentReleaseError("source fixture coverage gap must be an object")
        state = gap.get("state")
        if state not in {"failed", "restricted", "stale", "unavailable", "unprocessed"}:
            raise DocumentReleaseError(f"unknown acquisition coverage gap state: {state!r}")
        coverage_entries.append(
            {
                "collection": _require_string(gap.get("collection"), "coverage gap collection"),
                "reason": _require_string(gap.get("reason"), "coverage gap reason"),
                "record_ref": gap.get("record_ref"),
                "source": _require_string(gap.get("source"), "coverage gap source"),
                "state": state,
            }
        )
    capture_refs = sorted(capture_id for references in record_capture_refs.values() for capture_id in references)
    coverage_entries = sorted(coverage_entries, key=canonical_json)
    acquisition_identity = {
        "capture_refs": capture_refs,
        "entries": coverage_entries,
        "policy_version": document_policy_version,
        "requested_sources": fixture["requested_sources"],
    }
    acquisition_coverage = {
        "capture_refs": capture_refs,
        "coverage_id": stable_record_id("acquisition-coverage", acquisition_identity),
        "entries": coverage_entries,
        "policy_version": document_policy_version,
        "requested_sources": fixture["requested_sources"],
    }
    representation_by_id = {item.representation_id: item for item in representations}
    used_passage_policies = sorted(
        {passage.passage_policy_version for passage in passages}
    )
    body: JsonObject = {
        "acquisition_coverage": acquisition_coverage,
        "document_versions": _sorted_records([item.as_record() for item in documents], "document_version_id"),
        "format_version": format_version,
        "link_verification_receipts": _sorted_records(verification_receipts, "verification_id"),
        "passage_coverage": _passage_coverage(
            representations,
            passages,
            default_policy_version=default_passage_policy,
        ),
        "policies": {
            "document_eligibility": document_policy_version,
            "passage_generation": (
                PASSAGE_POLICY_VERSION
                if format_version == FORMAT_VERSION
                else used_passage_policies
            ),
        },
        "record_type": "DocumentRelease",
        "released_at": fixture["released_at"],
        "rulespec_core_release": {
            "release_digest": rulespec_core["release_digest"],
            "release_id": rulespec_core["release_id"],
        },
        "source_links": _sorted_records(source_links, "source_link_id"),
        "source_observation_captures": _sorted_records(
            [item.as_record() for item in observation_captures], "capture_id"
        ),
        "source_observations": _sorted_records([item.as_record() for item in observations], "observation_id"),
        "source_record_versions": _sorted_records(
            [item.as_record() for item in source_records], "source_record_version_id"
        ),
        "source_rendition_captures": _sorted_records([item.as_record() for item in rendition_captures], "capture_id"),
        "source_renditions": _sorted_records([item.as_record() for item in renditions], "rendition_id"),
        "structural_passages": _sorted_records(
            [item.as_record(representation_by_id[item.text_representation_ref].artifact_id) for item in passages],
            "passage_id",
        ),
        "text_representations": _sorted_records([item.as_record() for item in representations], "representation_id"),
    }
    if format_version == FORMAT_VERSION:
        body["source_fixture"] = {
            "fixture_digest": fixture["fixture_digest"],
            "fixture_id": fixture["fixture_id"],
        }
    else:
        body["release_status"] = _actual_file_release_status(
            rulespec_core["release_status"],
            source_input_type,
        )
        body["source_input"] = {
            "input_digest": source_input_digest,
            "input_id": source_input_id,
            "input_path": source_input_path,
            "input_type": _require_string(source_input_type, "source input type"),
        }
    release = seal_document_release(body)
    validate_document_release(release, rulespec_core_path=rulespec_core_path)
    return release


def _record_index(release: Mapping[str, Any], field: str, id_field: str) -> dict[str, JsonObject]:
    records = release.get(field)
    if not isinstance(records, list):
        raise DocumentReleaseError(f"{field} must be an array")
    output: dict[str, JsonObject] = {}
    if records != sorted(records, key=lambda row: str(row.get(id_field)) if isinstance(row, Mapping) else ""):
        raise DocumentReleaseError(f"{field} must be sorted by {id_field}")
    for record in records:
        if not isinstance(record, dict):
            raise DocumentReleaseError(f"{field} records must be objects")
        identifier = _require_string(record.get(id_field), f"{field}.{id_field}")
        if identifier in output:
            raise DocumentReleaseError(f"{field} contains duplicate identifier {identifier}")
        output[identifier] = record
    return output


def validate_document_release(
    release: Mapping[str, Any],
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> None:
    """Validate canonical identity, record facts, coordinates, and closure."""

    format_version = release.get("format_version")
    expected_acquisition_release_ref: str | None = None
    source_input_digest: str | None = None
    if format_version == FORMAT_VERSION:
        document_policy_version = DOCUMENT_ELIGIBILITY_POLICY_VERSION
        allowed_passage_policies = frozenset({PASSAGE_POLICY_VERSION})
        allowed_core_statuses = frozenset({"fixture"})
        source_pin_field = "source_fixture"
        declared_passage_policies = frozenset({PASSAGE_POLICY_VERSION})
    elif format_version == ACTUAL_FILE_FORMAT_VERSION:
        document_policy_version = ACTUAL_DOCUMENT_ELIGIBILITY_POLICY_VERSION
        allowed_passage_policies = frozenset(
            {PDF_PASSAGE_POLICY_VERSION, MARKUP_PASSAGE_POLICY_VERSION}
        )
        allowed_core_statuses = frozenset({"candidate", "fixture", "published"})
        source_pin_field = "source_input"
        passage_policy_value = (
            release.get("policies", {}).get("passage_generation")
            if isinstance(release.get("policies"), Mapping)
            else None
        )
        if (
            not isinstance(passage_policy_value, list)
            or not passage_policy_value
            or passage_policy_value != sorted(set(passage_policy_value))
            or not all(isinstance(value, str) for value in passage_policy_value)
        ):
            raise DocumentReleaseError("actual-file passage policies must be sorted unique strings")
        declared_passage_policies = frozenset(passage_policy_value)
        if not declared_passage_policies.issubset(allowed_passage_policies):
            raise DocumentReleaseError("actual-file release names an unknown passage policy")
    else:
        raise DocumentReleaseError("DocumentRelease type or format version differs")

    expected_root = {
        "acquisition_coverage",
        "document_versions",
        "format_version",
        "link_verification_receipts",
        "passage_coverage",
        "policies",
        "record_type",
        "release_digest",
        "release_id",
        "released_at",
        "rulespec_core_release",
        source_pin_field,
        "source_links",
        "source_observation_captures",
        "source_observations",
        "source_record_versions",
        "source_rendition_captures",
        "source_renditions",
        "structural_passages",
        "text_representations",
    }
    if format_version == ACTUAL_FILE_FORMAT_VERSION:
        expected_root.add("release_status")
    _require_exact_keys(release, expected_root, "DocumentRelease")
    if release["record_type"] != "DocumentRelease":
        raise DocumentReleaseError("DocumentRelease type or format version differs")
    expected_policy_value: str | list[str] = (
        PASSAGE_POLICY_VERSION
        if format_version == FORMAT_VERSION
        else sorted(declared_passage_policies)
    )
    if release["policies"] != {
        "document_eligibility": document_policy_version,
        "passage_generation": expected_policy_value,
    }:
        raise DocumentReleaseError("DocumentRelease policy pins differ")
    core = _load_rulespec_core_release(
        rulespec_core_path,
        allowed_statuses=allowed_core_statuses,
    )
    if release["rulespec_core_release"] != {
        "release_digest": core["release_digest"],
        "release_id": core["release_id"],
    }:
        raise DocumentReleaseError("DocumentRelease Rulespec Core pin differs")
    _require_string(release["released_at"], "DocumentRelease released_at")
    source_pin = release[source_pin_field]
    if not isinstance(source_pin, Mapping):
        raise DocumentReleaseError(f"DocumentRelease {source_pin_field} must be an object")
    if format_version == FORMAT_VERSION:
        _require_exact_keys(source_pin, {"fixture_digest", "fixture_id"}, "DocumentRelease source fixture pin")
        _require_string(source_pin["fixture_id"], "DocumentRelease source fixture ID")
        _require_digest(source_pin["fixture_digest"], "DocumentRelease source fixture digest")
    else:
        _require_exact_keys(
            source_pin,
            {"input_digest", "input_id", "input_path", "input_type"},
            "DocumentRelease source input pin",
        )
        _require_digest(source_pin["input_digest"], "DocumentRelease source input digest")
        _require_string(source_pin["input_id"], "DocumentRelease source input ID")
        _require_string(source_pin["input_path"], "DocumentRelease source input path")
        _require_string(source_pin["input_type"], "DocumentRelease source input type")
        source_input_digest = str(source_pin["input_digest"])
        input_kind = str(source_pin["input_type"])
        acquisition_kind = {
            "CapturedFileManifest": "file-manifest",
            "EvaluationSourceLock": "source-lock",
        }.get(input_kind)
        if acquisition_kind is None:
            raise DocumentReleaseError("DocumentRelease source input type has no acquisition binding")
        expected_acquisition_release_ref = (
            f"urn:spicyregs:acquisition-release:{acquisition_kind}:"
            + source_input_digest.removeprefix("sha256:")
        )
        expected_status = _actual_file_release_status(
            core["release_status"],
            input_kind,
        )
        if release["release_status"] != expected_status:
            raise DocumentReleaseError("DocumentRelease status contradicts its Rulespec Core input")

    source_records = _record_index(release, "source_record_versions", "source_record_version_id")
    source_objects: dict[str, SourceRecordVersion] = {}
    source_records_by_natural_key: set[tuple[str, str]] = set()
    for identifier, record in source_records.items():
        _require_exact_keys(
            record,
            {
                "collection",
                "content",
                "publisher",
                "source_record_digest",
                "source_record_id",
                "source_record_version_id",
                "source_url",
            },
            "SourceRecordVersion",
        )
        if not isinstance(record["content"], Mapping):
            raise DocumentReleaseError("SourceRecordVersion content must be an object")
        rebuilt = SourceRecordVersion.create(
            publisher=str(record["publisher"]),
            collection=str(record["collection"]),
            source_record_id=str(record["source_record_id"]),
            source_url=str(record["source_url"]),
            content=record["content"],
            eligibility_policy_version=document_policy_version,
        )
        if rebuilt.as_record() != record or rebuilt.source_record_version_id != identifier:
            raise DocumentReleaseError(f"SourceRecordVersion {identifier} identity or digest differs")
        source_objects[identifier] = rebuilt
        source_records_by_natural_key.add((rebuilt.publisher, rebuilt.source_record_id))

    documents = _record_index(release, "document_versions", "document_version_id")
    for identifier, record in documents.items():
        _require_exact_keys(
            record,
            {
                "artifact_projection",
                "content_digest",
                "document_type",
                "document_version_id",
                "publisher",
                "source_issued_version_id",
                "source_record_id",
            },
            "DocumentVersion",
        )
        if (str(record["publisher"]), str(record["source_record_id"])) not in source_records_by_natural_key:
            raise DocumentReleaseError(f"DocumentVersion {identifier} does not resolve to a source record")
        matching = [
            source
            for source in source_objects.values()
            if source.publisher == record["publisher"] and source.source_record_id == record["source_record_id"]
        ]
        if not matching or any(
            classify_source_record(
                source.publisher,
                source.collection,
                policy_version=document_policy_version,
            )
            != "document"
            for source in matching
        ):
            raise DocumentReleaseError(f"DocumentVersion {identifier} is not document-only eligible")
        projection = record["artifact_projection"]
        if not isinstance(projection, Mapping):
            raise DocumentReleaseError(f"DocumentVersion {identifier} Artifact projection must be an object")
        _require_exact_keys(
            projection,
            {
                "artifact_id",
                "artifact_type",
                "content_digest",
                "coordinate_system",
                "evidence_grade",
                "media_type",
            },
            "DocumentVersion Artifact projection",
        )
        rebuilt = DocumentVersion.create(
            publisher=str(record["publisher"]),
            source_record_id=str(record["source_record_id"]),
            source_issued_version_id=str(record["source_issued_version_id"]),
            document_type=str(record["document_type"]),
            content_digest=str(record["content_digest"]),
            coordinate_system=str(projection["coordinate_system"]),
            evidence_grade=str(projection["evidence_grade"]),
            media_type=str(projection["media_type"]),
        )
        if rebuilt.as_record() != record or rebuilt.document_version_id != identifier:
            raise DocumentReleaseError(f"DocumentVersion {identifier} identity differs")

    renditions = _record_index(release, "source_renditions", "rendition_id")
    for identifier, record in renditions.items():
        _require_exact_keys(
            record,
            {"bytes_digest", "document_version_ref", "media_type", "rendition_id", "source_native_path", "source_url"},
            "SourceRendition",
        )
        if record["document_version_ref"] not in documents:
            raise DocumentReleaseError(f"SourceRendition {identifier} has a missing document version")
        rebuilt = SourceRendition.create(
            document_version_ref=str(record["document_version_ref"]),
            source_native_path=record["source_native_path"],
            source_url=record["source_url"],
            media_type=str(record["media_type"]),
            bytes_digest=str(record["bytes_digest"]),
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"SourceRendition {identifier} identity differs")

    rendition_captures = _record_index(release, "source_rendition_captures", "capture_id")
    for identifier, record in rendition_captures.items():
        _require_exact_keys(
            record,
            {"acquisition_release_ref", "capture_id", "observed_at", "retrieval_receipt_ref", "source_rendition_ref"},
            "SourceRenditionCapture",
        )
        if record["source_rendition_ref"] not in renditions:
            raise DocumentReleaseError(f"SourceRenditionCapture {identifier} has a missing rendition")
        rebuilt = SourceRenditionCapture.create(
            source_rendition_ref=str(record["source_rendition_ref"]),
            observed_at=str(record["observed_at"]),
            retrieval_receipt_ref=str(record["retrieval_receipt_ref"]),
            acquisition_release_ref=str(record["acquisition_release_ref"]),
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"SourceRenditionCapture {identifier} identity differs")
        if (
            expected_acquisition_release_ref is not None
            and rebuilt.acquisition_release_ref != expected_acquisition_release_ref
        ):
            raise DocumentReleaseError(
                f"SourceRenditionCapture {identifier} is not bound to the source input"
            )
    if {str(record["source_rendition_ref"]) for record in rendition_captures.values()} != set(renditions):
        raise DocumentReleaseError("every SourceRendition must have at least one immutable capture event")

    representations = _record_index(release, "text_representations", "representation_id")
    representation_objects: dict[str, TextRepresentation] = {}
    artifact_ids: set[str] = set()
    for identifier, record in representations.items():
        _require_exact_keys(
            record,
            {
                "artifact_projection",
                "coordinate_system",
                "document_version_ref",
                "evidence_grade",
                "method",
                "method_config_digest",
                "method_version",
                "representation_id",
                "representation_kind_and_path",
                "source_rendition_ref",
                "text_digest",
                "unicode_text",
            },
            "TextRepresentation",
        )
        if record["document_version_ref"] not in documents:
            raise DocumentReleaseError(f"TextRepresentation {identifier} has a missing document version")
        if record["source_rendition_ref"] is not None and record["source_rendition_ref"] not in renditions:
            raise DocumentReleaseError(f"TextRepresentation {identifier} has a missing source rendition")
        if (
            record["source_rendition_ref"] is not None
            and renditions[str(record["source_rendition_ref"])]["document_version_ref"]
            != record["document_version_ref"]
        ):
            raise DocumentReleaseError(f"TextRepresentation {identifier} source rendition belongs to another document")
        rebuilt = TextRepresentation.create(
            document_version_ref=str(record["document_version_ref"]),
            representation_kind_and_path=str(record["representation_kind_and_path"]),
            unicode_text=record["unicode_text"],
            evidence_grade=str(record["evidence_grade"]),
            source_rendition_ref=record["source_rendition_ref"],
            method=record["method"],
            method_version=record["method_version"],
            method_config_digest=record["method_config_digest"],
            coordinate_system=str(record["coordinate_system"]),
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"TextRepresentation {identifier} identity, text digest, or Artifact differs")
        representation_objects[identifier] = rebuilt
        artifact_ids.add(rebuilt.artifact_id)
    for document_id, document in documents.items():
        document_representations = [
            representation
            for representation in representation_objects.values()
            if representation.document_version_ref == document_id
        ]
        if not document_representations:
            raise DocumentReleaseError(f"DocumentVersion {document_id} has no text representation")
        projection = cast(Mapping[str, Any], document["artifact_projection"])
        if projection["media_type"] == "text/plain; charset=utf-8":
            if not any(
                representation.text_digest == document["content_digest"]
                for representation in document_representations
            ):
                raise DocumentReleaseError(f"DocumentVersion {document_id} has no exact content representation")
        elif not any(
            rendition["document_version_ref"] == document_id
            and rendition["bytes_digest"] == document["content_digest"]
            and rendition["media_type"] == projection["media_type"]
            for rendition in renditions.values()
        ):
            raise DocumentReleaseError(f"DocumentVersion {document_id} has no exact source-byte rendition")

    passages = _record_index(release, "structural_passages", "passage_id")
    passage_objects: dict[str, StructuralPassage] = {}
    for identifier, record in passages.items():
        _require_exact_keys(
            record,
            {
                "coordinate_system",
                "document_version_ref",
                "end",
                "evidence_grade",
                "passage_id",
                "passage_policy_version",
                "representation_digest",
                "selected_text_digest",
                "selector_kind",
                "source_fragment_projection",
                "start",
                "text_representation_ref",
            },
            "StructuralPassage",
        )
        representation = representation_objects.get(str(record["text_representation_ref"]))
        if representation is None:
            raise DocumentReleaseError(f"StructuralPassage {identifier} has a missing text representation")
        rebuilt = StructuralPassage.create(
            representation=representation,
            start=record["start"],
            end=record["end"],
            passage_policy_version=str(record["passage_policy_version"]),
            selector_kind=str(record["selector_kind"]),
        )
        if rebuilt.passage_policy_version not in allowed_passage_policies:
            raise DocumentReleaseError(
                f"StructuralPassage {identifier} names a policy outside the release profile"
            )
        if rebuilt.as_record(representation.artifact_id) != record:
            raise DocumentReleaseError(f"StructuralPassage {identifier} coordinate, digest, or projection differs")
        passage_objects[identifier] = rebuilt

    coverage = _record_index(release, "passage_coverage", "coverage_id")
    covered_representations: set[str] = set()
    covered_passages: set[str] = set()
    for identifier, record in coverage.items():
        _require_exact_keys(
            record,
            {"coverage_id", "passage_policy_version", "regions", "text_representation_ref"},
            "PassageCoverage",
        )
        representation = representation_objects.get(str(record["text_representation_ref"]))
        if representation is None or representation.representation_id in covered_representations:
            raise DocumentReleaseError(f"PassageCoverage {identifier} has a missing or duplicate representation")
        covered_representations.add(representation.representation_id)
        regions = record["regions"]
        if not isinstance(regions, list) or not regions:
            raise DocumentReleaseError(f"PassageCoverage {identifier} must contain regions")
        cursor = 0
        for region in regions:
            if not isinstance(region, Mapping):
                raise DocumentReleaseError("passage coverage regions must be objects")
            _require_exact_keys(region, {"end", "passage_ref", "reason", "start", "state"}, "passage coverage region")
            if (
                isinstance(region["start"], bool)
                or isinstance(region["end"], bool)
                or region["start"] != cursor
                or not isinstance(region["end"], int)
                or region["end"] < cursor
                or (region["end"] == cursor and (representation.unicode_text or len(regions) != 1))
            ):
                raise DocumentReleaseError(f"PassageCoverage {identifier} is not contiguous")
            if region["state"] == "processed":
                passage = passage_objects.get(str(region["passage_ref"]))
                if passage is None or passage.text_representation_ref != representation.representation_id:
                    raise DocumentReleaseError(f"PassageCoverage {identifier} has an invalid processed passage")
                if (region["start"], region["end"], region["reason"]) != (passage.start, passage.end, None):
                    raise DocumentReleaseError(f"PassageCoverage {identifier} processed coordinates differ")
                if passage.passage_id in covered_passages:
                    raise DocumentReleaseError(f"PassageCoverage {identifier} repeats a processed passage")
                covered_passages.add(passage.passage_id)
            elif (
                region["state"] not in {"excluded", "failed"}
                or region["passage_ref"] is not None
                or not isinstance(region["reason"], str)
                or not region["reason"].strip()
            ):
                raise DocumentReleaseError(f"PassageCoverage {identifier} has an invalid region state")
            cursor = region["end"]
        if cursor != len(representation.unicode_text):
            raise DocumentReleaseError(f"PassageCoverage {identifier} does not cover the complete representation")
        identity = {
            "passage_policy_version": record["passage_policy_version"],
            "regions": regions,
            "text_representation_ref": representation.representation_id,
        }
        if identifier != stable_record_id("passage-coverage", identity):
            raise DocumentReleaseError(f"PassageCoverage {identifier} identity differs")
    if covered_representations != set(representation_objects):
        raise DocumentReleaseError("passage coverage does not account for every text representation")
    if covered_passages != set(passage_objects):
        raise DocumentReleaseError("passage coverage does not account for every structural passage exactly once")
    actual_passage_policies = frozenset(
        passage.passage_policy_version for passage in passage_objects.values()
    )
    if actual_passage_policies != declared_passage_policies:
        raise DocumentReleaseError("DocumentRelease passage policy pins do not match its passages")

    observations = _record_index(release, "source_observations", "observation_id")
    observation_objects: dict[str, SourceObservation] = {}
    for identifier, record in observations.items():
        _require_exact_keys(
            record,
            {
                "document_version_ref",
                "observation_id",
                "observation_kind",
                "raw_value",
                "source_native_key_or_ordinal",
                "source_native_path",
                "source_record_digest",
                "source_record_version_ref",
            },
            "SourceObservation",
        )
        source = source_objects.get(str(record["source_record_version_ref"]))
        if source is None or record["document_version_ref"] not in documents:
            raise DocumentReleaseError(f"SourceObservation {identifier} has a missing source or document")
        document = documents[str(record["document_version_ref"])]
        if (source.publisher, source.source_record_id) != (
            document["publisher"],
            document["source_record_id"],
        ):
            raise DocumentReleaseError(f"SourceObservation {identifier} names a different document source record")
        if source.source_record_digest != record["source_record_digest"]:
            raise DocumentReleaseError(f"SourceObservation {identifier} source digest differs")
        path = str(record["source_native_path"])
        ordinal = str(record["source_native_key_or_ordinal"])
        actual = _resolve_path(source.content, f"{path}.{ordinal}" if ordinal.isdigit() else path)
        if canonical_json(actual) != canonical_json(record["raw_value"]):
            raise DocumentReleaseError(f"SourceObservation {identifier} raw source value differs")
        rebuilt = SourceObservation.create(
            document_version_ref=str(record["document_version_ref"]),
            source_record_version_ref=source.source_record_version_id,
            source_native_path=path,
            source_native_key_or_ordinal=ordinal,
            raw_value=actual,
            source_record_digest=source.source_record_digest,
            observation_kind=str(record["observation_kind"]),
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"SourceObservation {identifier} identity differs")
        observation_objects[identifier] = rebuilt

    observation_captures = _record_index(release, "source_observation_captures", "capture_id")
    for identifier, record in observation_captures.items():
        _require_exact_keys(
            record,
            {"acquisition_release_ref", "capture_id", "observed_at", "retrieval_receipt_ref", "source_observation_ref"},
            "SourceObservationCapture",
        )
        if record["source_observation_ref"] not in observations:
            raise DocumentReleaseError(f"SourceObservationCapture {identifier} has a missing observation")
        rebuilt = SourceObservationCapture.create(
            source_observation_ref=str(record["source_observation_ref"]),
            observed_at=str(record["observed_at"]),
            retrieval_receipt_ref=str(record["retrieval_receipt_ref"]),
            acquisition_release_ref=str(record["acquisition_release_ref"]),
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"SourceObservationCapture {identifier} identity differs")
        if (
            expected_acquisition_release_ref is not None
            and rebuilt.acquisition_release_ref != expected_acquisition_release_ref
        ):
            raise DocumentReleaseError(
                f"SourceObservationCapture {identifier} is not bound to the source input"
            )
    if {str(record["source_observation_ref"]) for record in observation_captures.values()} != set(observations):
        raise DocumentReleaseError("every SourceObservation must have at least one immutable capture event")

    links = _record_index(release, "source_links", "source_link_id")
    link_objects: dict[str, SourceLink] = {}
    for identifier, record in links.items():
        _require_exact_keys(
            record,
            {
                "origin",
                "raw_value",
                "source_field",
                "source_link_id",
                "source_record_version_ref",
                "target_source_record_version_ref",
            },
            "SourceLink",
        )
        source = source_objects.get(str(record["source_record_version_ref"]))
        target = source_objects.get(str(record["target_source_record_version_ref"]))
        if source is None or target is None:
            raise DocumentReleaseError(f"SourceLink {identifier} has a missing source record")
        source_field = str(record["source_field"])
        field_collection, separator, source_path = source_field.partition(".")
        if separator != "." or field_collection != source.collection:
            raise DocumentReleaseError(f"SourceLink {identifier} source field does not name its source collection")
        raw_value = _resolve_path(source.content, source_path)
        rebuilt = SourceLink.create(
            source_record_version_ref=source.source_record_version_id,
            target_source_record_version_ref=target.source_record_version_id,
            source_field=str(record["source_field"]),
            raw_value=raw_value,
        )
        if rebuilt.as_record() != record:
            raise DocumentReleaseError(f"SourceLink {identifier} identity or raw value differs")
        link_objects[identifier] = rebuilt

    receipts = _record_index(release, "link_verification_receipts", "verification_id")
    verified_links: set[str] = set()
    for identifier, record in receipts.items():
        link = link_objects.get(str(record.get("source_link_ref")))
        if link is None:
            raise DocumentReleaseError(f"LinkVerificationReceipt {identifier} has a missing source link")
        source = source_objects.get(link.source_record_version_ref)
        target = source_objects.get(link.target_source_record_version_ref)
        expected = make_link_verification_receipt(link, source=source, target=target)
        if record != expected:
            raise DocumentReleaseError(f"LinkVerificationReceipt {identifier} checks or outcome differ")
        if record["outcome"] == "verified":
            verified_links.add(link.source_link_id)
    if set(link_objects) != {str(record["source_link_ref"]) for record in receipts.values()}:
        raise DocumentReleaseError("every source link must have exactly one verification receipt")

    acquisition = release["acquisition_coverage"]
    if not isinstance(acquisition, Mapping):
        raise DocumentReleaseError("acquisition_coverage must be an object")
    _require_exact_keys(
        acquisition,
        {"capture_refs", "coverage_id", "entries", "policy_version", "requested_sources"},
        "AcquisitionCoverage",
    )
    if acquisition["policy_version"] != document_policy_version:
        raise DocumentReleaseError("acquisition coverage policy differs")
    requested_sources = acquisition["requested_sources"]
    if (
        not isinstance(requested_sources, list)
        or not requested_sources
        or requested_sources != sorted(set(requested_sources))
        or not all(isinstance(source, str) and source for source in requested_sources)
    ):
        raise DocumentReleaseError("acquisition coverage requested sources must be sorted unique strings")
    requested_source_bases = {
        _requested_source_base(
            source,
            require_selection=format_version == ACTUAL_FILE_FORMAT_VERSION,
        )
        for source in requested_sources
    }
    if source_input_digest is not None and any(
        source.partition("#selection=")[2] != source_input_digest
        for source in requested_sources
    ):
        raise DocumentReleaseError("requested source selection differs from the source input")
    if len(requested_source_bases) != len(requested_sources):
        raise DocumentReleaseError("requested source selections must have unique source bases")
    all_capture_ids = set(rendition_captures) | set(observation_captures)
    if acquisition["capture_refs"] != sorted(all_capture_ids):
        raise DocumentReleaseError("acquisition coverage capture references are incomplete")
    entries = acquisition["entries"]
    if not isinstance(entries, list) or entries != sorted(entries, key=canonical_json):
        raise DocumentReleaseError("acquisition coverage entries must be a sorted array")
    covered_record_refs: set[str] = set()
    covered_source_bases: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise DocumentReleaseError("acquisition coverage entries must be objects")
        _require_exact_keys(entry, {"collection", "reason", "record_ref", "source", "state"}, "coverage entry")
        state = entry["state"]
        record_ref = entry["record_ref"]
        reason = entry["reason"]
        source_key = f"{entry['source']}:{entry['collection']}"
        covered_source_bases.add(source_key)
        if source_key not in requested_source_bases:
            raise DocumentReleaseError("acquisition coverage entry names an unrequested source")
        if state in {"captured", "excluded"}:
            if record_ref not in source_records:
                raise DocumentReleaseError("captured or excluded acquisition entry has a missing source record")
            if record_ref in covered_record_refs:
                raise DocumentReleaseError("source record appears more than once in acquisition coverage")
            covered_record_refs.add(str(record_ref))
            source_record = source_records[str(record_ref)]
            if (entry["source"], entry["collection"]) != (
                source_record["publisher"],
                source_record["collection"],
            ):
                raise DocumentReleaseError("acquisition coverage entry names the wrong source record tuple")
            role = classify_source_record(
                str(entry["source"]),
                str(entry["collection"]),
                policy_version=document_policy_version,
            )
            if state == "captured" and role != "document":
                raise DocumentReleaseError("non-document source record cannot be captured into document scope")
            if state == "excluded" and role == "document":
                raise DocumentReleaseError("eligible document source record cannot be silently excluded")
            if state == "captured" and reason is not None:
                raise DocumentReleaseError("captured acquisition entry cannot have a failure reason")
            if state == "excluded" and (not isinstance(reason, str) or not reason.strip()):
                raise DocumentReleaseError("excluded acquisition entry requires a reason")
        elif state not in {"failed", "restricted", "stale", "unavailable", "unprocessed"}:
            raise DocumentReleaseError(f"unknown acquisition coverage state: {state!r}")
        elif record_ref is not None or not isinstance(reason, str) or not reason.strip():
            raise DocumentReleaseError(f"{state} acquisition entry requires no record and a typed reason")
    if covered_record_refs != set(source_records):
        raise DocumentReleaseError("acquisition coverage does not account for every source record")
    if covered_source_bases != requested_source_bases:
        raise DocumentReleaseError("acquisition coverage does not account for every requested source")
    acquisition_identity = {
        "capture_refs": acquisition["capture_refs"],
        "entries": entries,
        "policy_version": acquisition["policy_version"],
        "requested_sources": acquisition["requested_sources"],
    }
    if acquisition["coverage_id"] != stable_record_id("acquisition-coverage", acquisition_identity):
        raise DocumentReleaseError("acquisition coverage identity differs")

    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    expected_digest = canonical_digest(body)
    expected_id = "urn:spicyregs:document-release:" + expected_digest.removeprefix("sha256:")
    if release["release_digest"] != expected_digest or release["release_id"] != expected_id:
        raise DocumentReleaseError("DocumentRelease canonical identity differs")


def write_document_release(
    path: Path,
    release: Mapping[str, Any],
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> None:
    """Write one canonical UTF-8 release with a trailing newline."""

    validate_document_release(release, rulespec_core_path=rulespec_core_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(dict(release)) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the sealed SpicyRegs M1 DocumentRelease fixture")
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE_PATH)
    parser.add_argument("--rulespec-core", type=Path, default=DEFAULT_RULESPEC_CORE_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    release = build_document_release(args.fixture, args.rulespec_core)
    validate_document_release(release, rulespec_core_path=args.rulespec_core)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(canonical_json(release) + "\n", encoding="utf-8")
    print(
        canonical_json(
            {
                "document_count": len(release["document_versions"]),
                "output": str(args.output),
                "passage_count": len(release["structural_passages"]),
                "release_digest": release["release_digest"],
                "release_id": release["release_id"],
                "status": "pass",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

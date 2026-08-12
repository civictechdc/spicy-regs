"""Build a ``DocumentRelease`` from digest-pinned captured source files.

This is the production-shaped bridge between files SpicyRegs actually captured
and the immutable release model. It supports embedded-text PDF and
source-native HTML/XML when a capture manifest supplies complete source facts.
Network acquisition remains a separate concern: every input file and its
expected SHA-256 are declared before this module reads a byte.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

from spicy_regs.document_release import (
    ACTUAL_FILE_FORMAT_VERSION,
    DEFAULT_RULESPEC_CORE_PATH,
    FIXTURE_FORMAT_VERSION,
    MARKUP_PASSAGE_POLICY_VERSION,
    PDF_PASSAGE_POLICY_VERSION,
    DocumentReleaseError,
    build_document_release,
    canonical_digest,
    canonical_json,
    validate_document_release,
    write_document_release,
)
from spicy_regs.docpipeline.source import (
    SourceRetentionError,
    check_extraction_retention,
    native_structural_passage_spans,
    retention_format_for,
    visible_retention,
)
from spicy_regs.schemas import DOCUMENT
from spicy_regs.transforms.pdf_text import PAGE_SEPARATOR, PdfTextResult, PdfTextStatus, extract_pdf_text
from spicy_regs.transforms.pdf_text_pymupdf import (
    PYMUPDF_EXTRACTION_METHOD,
    extract_pdf_text_pymupdf,
    pymupdf_version,
)


FILE_MANIFEST_FORMAT_VERSION = "spicyregs-document-files/v1"
PDF_EXTRACTION_METHOD = "pypdf"
DEFAULT_PDF_EXTRACTION_METHOD = PYMUPDF_EXTRACTION_METHOD
"""What a *new* extraction uses.

Adopted 2026-08-02 on ``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``.
Changing this never moves an already-sealed document: a lock records the parser
it was sealed under and :func:`_extract_pdf_with` is handed that name, so a
pypdf-sealed record keeps reproducing under pypdf byte for byte. What changes is
which parser fills the ``primary-text`` slot for documents captured from now on,
and the release that results is a new immutable object with its own digest —
never an edit of an existing one.
"""
PDF_EXTRACTION_CONFIG = {
    "mode": "embedded-text-only",
    "page_separator": PAGE_SEPARATOR,
    "page_whitespace": "strip",
}
LOCKED_PDF_PAGE_SEPARATOR = "\n\f\n"
LOCKED_PDF_EXTRACTION_CONFIG = {
    "mode": "embedded-text-only",
    "page_separator": LOCKED_PDF_PAGE_SEPARATOR,
    "page_whitespace": "preserve",
}
DEFAULT_FILE_MANIFEST_PATH = (
    Path(__file__).resolve().parents[2] / "sample-data" / "mirrulations" / "document-release-file-manifest-v1.json"
)

_SUPPORTED_PRIMARY_MEDIA_TYPES = frozenset({"application/pdf"})
_SUPPORTED_CAPTURED_TEXT_MEDIA_TYPES = frozenset({"application/pdf", "application/xml", "text/html", "text/xml"})
_SOURCE_CACHE_DOCUMENT_TYPES: dict[str, tuple[str, str, str]] = {
    "cfr-section-v1": ("cfr", "sections", "CFR section"),
    "congress-bill-v1": ("congress", "bills", "Congressional bill"),
    "court-opinion-v1": ("supreme-court", "opinions", "Supreme Court opinion"),
    "crs-report-v1": ("congressional-research-service", "reports", "CRS report"),
    "federal-register-document-v1": ("federal-register", "documents", "Federal Register document"),
    "gao-report-v1": ("gao", "reports", "GAO report"),
    "regulations-document-v2": ("regulations.gov", "documents", "Regulations.gov document"),
}


class DocumentFilePipelineError(DocumentReleaseError):
    """A captured-file manifest or one of its exact files failed closed."""


@dataclass(frozen=True, slots=True)
class _PreparedFileRelease:
    release: dict[str, Any]
    rendition_bytes: Mapping[str, bytes]
    supporting_bytes: Mapping[str, bytes]


def _require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DocumentFilePipelineError(f"{label} must be a non-empty string")
    return value


def _require_capture_observation(value: object, label: str) -> str:
    """Accept only an exact calendar date or a timezone-aware ISO instant."""

    observation = _require_string(value, label)
    if len(observation) == 10:
        try:
            parsed_date = date.fromisoformat(observation)
        except ValueError as error:
            raise DocumentFilePipelineError(f"{label} must be an exact ISO date or timezone-aware instant") from error
        if parsed_date.isoformat() != observation:
            raise DocumentFilePipelineError(f"{label} must be an exact ISO date or timezone-aware instant")
        return observation
    normalized = observation[:-1] + "+00:00" if observation.endswith("Z") else observation
    try:
        parsed_instant = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise DocumentFilePipelineError(f"{label} must be an exact ISO date or timezone-aware instant") from error
    if parsed_instant.tzinfo is None or parsed_instant.utcoffset() is None:
        raise DocumentFilePipelineError(f"{label} must be an exact ISO date or timezone-aware instant")
    return observation


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DocumentFilePipelineError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise DocumentFilePipelineError(
            f"{label} keys differ; missing={sorted(expected - set(value))}, unexpected={sorted(set(value) - expected)}"
        )


def _resolve_path(value: object, path: str) -> object:
    current = value
    for part in path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                raise DocumentFilePipelineError(f"source path {path!r} is missing component {part!r}")
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                raise DocumentFilePipelineError(f"source path {path!r} has an out-of-range ordinal")
            current = current[index]
        else:
            raise DocumentFilePipelineError(f"source path {path!r} cannot traverse {part!r}")
    return current


def _parse_manifest_bytes(payload: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise DocumentFilePipelineError(f"cannot read file manifest {label}: {error}") from error
    manifest = dict(_require_mapping(value, "file manifest"))
    _require_exact_keys(
        manifest,
        {
            "coverage_gaps",
            "documents",
            "format_version",
            "manifest_id",
            "released_at",
            "requested_sources",
        },
        "file manifest",
    )
    if manifest["format_version"] != FILE_MANIFEST_FORMAT_VERSION:
        raise DocumentFilePipelineError("file manifest format version differs")
    if not isinstance(manifest["documents"], list) or not manifest["documents"]:
        raise DocumentFilePipelineError("file manifest documents must be a non-empty array")
    return manifest


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise DocumentFilePipelineError(f"cannot read file manifest {path}: {error}") from error
    return _parse_manifest_bytes(payload, label=str(path))


def _manifest_file(
    manifest_path: Path,
    relative_name: str,
    *,
    file_overrides: Mapping[str, Path],
) -> Path:
    relative = PurePosixPath(relative_name)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise DocumentFilePipelineError(f"manifest file path is not contained: {relative_name!r}")
    override = file_overrides.get(relative_name)
    if override is not None:
        return Path(override)
    root = manifest_path.resolve().parent
    candidate = (root / Path(*relative.parts)).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise DocumentFilePipelineError(f"manifest file path escapes its directory: {relative_name!r}") from error
    return candidate


def _read_exact_bytes(path: Path, expected_digest: str, *, label: str) -> bytes:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise DocumentFilePipelineError(f"cannot read {label}: {path}") from error
    actual = "sha256:" + hashlib.sha256(payload).hexdigest()
    if actual != expected_digest:
        raise DocumentFilePipelineError(f"{label} bytes digest differs: expected {expected_digest}, observed {actual}")
    return payload


def _distribution_path(expected_digest: str, source_name: str) -> str:
    suffix = PurePosixPath(source_name).suffix.lower()
    digest = expected_digest.removeprefix("sha256:")
    return f"renditions/{digest}{suffix}"


def _pypdf_version() -> str:
    try:
        return version("pypdf")
    except PackageNotFoundError as error:
        raise DocumentFilePipelineError("pypdf is required to extract captured PDF files") from error


def _extract_pdf_with(
    method: str,
    payload: bytes,
    *,
    page_separator: str,
    page_whitespace: Literal["preserve", "strip"],
) -> tuple[PdfTextResult, str, str]:
    """Extract with the *named* parser, and report the version actually imported.

    The name is an input rather than a constant because a sealed record must
    reproduce under the parser it was sealed with. Adopting a better parser
    changes what new captures use; it may not reach backwards and move a
    document that is already sealed.

    An unknown name fails closed rather than falling back to a default — a
    silent fallback would let a lock naming a parser this build does not have
    verify against a different one, which is the one way this check could lie.
    """
    if method == PDF_EXTRACTION_METHOD:
        result = extract_pdf_text(payload, page_separator=page_separator, page_whitespace=page_whitespace)
        return result, method, _pypdf_version()
    if method == PYMUPDF_EXTRACTION_METHOD:
        result = extract_pdf_text_pymupdf(payload, page_separator=page_separator, page_whitespace=page_whitespace)
        return result, method, pymupdf_version()
    raise DocumentFilePipelineError(
        f"PDF extraction method {method!r} is not one this build can run "
        f"({PDF_EXTRACTION_METHOD!r}, {PYMUPDF_EXTRACTION_METHOD!r})"
    )


def _pdf_extraction_config(method: str, *, locked: bool) -> dict[str, str]:
    """The effective configuration recorded beside the parser and its version."""
    base = dict(LOCKED_PDF_EXTRACTION_CONFIG if locked else PDF_EXTRACTION_CONFIG)
    if method == PYMUPDF_EXTRACTION_METHOD:
        base["text_flags"] = "default"
        base["reading_order"] = "content-stream"
    return base


def _refuse_thin_markup_parse(
    text: str,
    *,
    media_type: str,
    source_field: str,
    subject_id: str,
) -> None:
    """Refuse a markup parse that did not account for its own document.

    The gate sits here, at the boundary where a representation is about to be
    sealed, rather than inside any one extractor — so it constrains the native
    path today and anything swapped in behind it later. Docling's HTML backend
    retained 0.27% of a Federal Register rule and reported success; nothing
    before this forced a parse to justify its own volume.
    """
    source_format = retention_format_for(media_type)
    measured = visible_retention(source_field, text, media_type=media_type)
    try:
        check_extraction_retention("native", source_format, measured, subject_id=subject_id)
    except SourceRetentionError as error:
        raise DocumentFilePipelineError(str(error)) from error


def _refuse_thin_pdf_parse(text: str, payload: bytes, *, subject_id: str, method: str) -> None:
    """Refuse a PDF parse whose text density is below the measured floor.

    A PDF carries no source text to compare against, so the measurement is
    characters per source byte and its floor is derived from a different
    population than the markup floors. A scanned page with no OCR lands near
    zero and is refused here rather than sealed as an empty document.
    """
    if not payload:
        raise DocumentFilePipelineError(f"{subject_id} has no PDF bytes to extract")
    try:
        check_extraction_retention(
            method,
            "application/pdf",
            len(text) / len(payload),
            subject_id=subject_id,
        )
    except SourceRetentionError as error:
        raise DocumentFilePipelineError(str(error)) from error


def _pdf_passages(
    text: str,
    pages: Sequence[str],
    *,
    page_separator: str = PAGE_SEPARATOR,
) -> list[dict[str, Any]]:
    joined = page_separator.join(pages)
    if text == joined:
        leading_trim = 0
    elif text == joined.strip():
        leading_trim = len(joined) - len(joined.lstrip())
    else:
        raise DocumentFilePipelineError("PDF combined text does not close against page text")
    passages: list[dict[str, Any]] = []
    cursor = 0
    for page in pages:
        start = cursor - leading_trim
        end = start + len(page)
        if page and 0 <= start < end <= len(text):
            passages.append(
                {
                    "end": end,
                    "passage_policy_version": PDF_PASSAGE_POLICY_VERSION,
                    "representation_key": "primary-text",
                    "start": start,
                }
            )
        cursor += len(page) + len(page_separator)
    if not passages:
        raise DocumentFilePipelineError("PDF extraction produced no addressable page passages")
    return passages


def _prepare_regulations_document(
    manifest_path: Path,
    document_spec: object,
    *,
    file_overrides: Mapping[str, Path],
    retrieval_receipt_ref: str,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    document = _require_mapping(document_spec, "file manifest document")
    _require_exact_keys(
        document,
        {
            "adapter",
            "captures",
            "collection",
            "document",
            "key",
            "publisher",
            "renditions",
            "source_record",
        },
        "file manifest document",
    )
    publisher = _require_string(document["publisher"], "document publisher")
    collection = _require_string(document["collection"], "document collection")
    captures = document["captures"]
    if not isinstance(captures, list) or not captures:
        raise DocumentFilePipelineError("document captures must be a non-empty array")
    if (publisher, collection) != ("regulations.gov", "documents"):
        raise DocumentFilePipelineError("the first actual-file adapter supports regulations.gov documents only")
    if document["adapter"] != "regulations-json-pdf/v1":
        raise DocumentFilePipelineError("Regulations.gov document adapter differs")
    source_spec = _require_mapping(document["source_record"], "document source_record")
    _require_exact_keys(source_spec, {"id_path", "path", "url_path"}, "document source_record")
    source_path_name = _require_string(source_spec["path"], "source record path")
    source_path = _manifest_file(manifest_path, source_path_name, file_overrides=file_overrides)

    rendition_specs = document["renditions"]
    if not isinstance(rendition_specs, list) or not rendition_specs:
        raise DocumentFilePipelineError("document renditions must be a non-empty array")
    source_rendition_spec = next(
        (
            _require_mapping(item, "document rendition")
            for item in rendition_specs
            if isinstance(item, Mapping) and item.get("path") == source_path_name
        ),
        None,
    )
    if source_rendition_spec is None:
        raise DocumentFilePipelineError("source record JSON must also be declared as an exact rendition")
    source_bytes = _read_exact_bytes(
        source_path,
        _require_string(source_rendition_spec.get("bytes_digest"), "source record bytes digest"),
        label="source record",
    )
    try:
        source_content = json.loads(source_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise DocumentFilePipelineError("source record is not UTF-8 JSON") from error
    source_content = dict(_require_mapping(source_content, "source record"))
    flattened = DOCUMENT.extract(source_content)
    flattened_attachments: list[Mapping[str, Any]] = []
    attachments_json = flattened.get("attachments_json")
    if isinstance(attachments_json, str):
        try:
            decoded_attachments = json.loads(attachments_json)
        except json.JSONDecodeError as error:
            raise DocumentFilePipelineError("Regulations.gov attachments_json is invalid") from error
        if isinstance(decoded_attachments, list):
            flattened_attachments = [item for item in decoded_attachments if isinstance(item, Mapping)]

    source_record_id = _require_string(
        _resolve_path(source_content, _require_string(source_spec["id_path"], "source record id_path")),
        "source record ID",
    )
    if flattened.get("document_id") != source_record_id:
        raise DocumentFilePipelineError("Regulations.gov extraction returned a different document ID")
    source_url = _require_string(
        _resolve_path(source_content, _require_string(source_spec["url_path"], "source record url_path")),
        "source record URL",
    )

    document_metadata = _require_mapping(document["document"], "document metadata")
    _require_exact_keys(
        document_metadata,
        {"document_type_path", "primary_rendition_key", "source_issued_version_id_path"},
        "document metadata",
    )
    primary_key = _require_string(document_metadata["primary_rendition_key"], "primary rendition key")

    prepared_renditions: list[dict[str, Any]] = []
    rendition_bytes: dict[str, bytes] = {}
    primary_payload: bytes | None = None
    primary_media_type: str | None = None
    primary_digest: str | None = None
    for raw_rendition in rendition_specs:
        rendition = _require_mapping(raw_rendition, "document rendition")
        allowed = {"bytes_digest", "key", "media_type", "path", "source_url_path"}
        if "expected_bytes_path" in rendition:
            allowed.add("expected_bytes_path")
        _require_exact_keys(rendition, allowed, "document rendition")
        rendition_key = _require_string(rendition["key"], "rendition key")
        relative_name = _require_string(rendition["path"], "rendition path")
        expected_digest = _require_string(rendition["bytes_digest"], "rendition bytes digest")
        payload = _read_exact_bytes(
            _manifest_file(manifest_path, relative_name, file_overrides=file_overrides),
            expected_digest,
            label=f"rendition {rendition_key}",
        )
        expected_bytes_path = rendition.get("expected_bytes_path")
        if expected_bytes_path is not None:
            expected_size = _resolve_path(
                source_content,
                _require_string(expected_bytes_path, "rendition expected_bytes_path"),
            )
            if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size != len(payload):
                raise DocumentFilePipelineError(f"rendition {rendition_key} byte count differs from its source record")
        source_url_value = _resolve_path(
            source_content,
            _require_string(rendition["source_url_path"], "rendition source_url_path"),
        )
        source_rendition_url = _require_string(source_url_value, "rendition source URL")
        media_type = _require_string(rendition["media_type"], "rendition media type")
        if relative_name != source_path_name:
            expected_format = PurePosixPath(relative_name).suffix.removeprefix(".").casefold()
            if not any(
                attachment.get("url") == source_rendition_url
                and str(attachment.get("format") or "").casefold() == expected_format
                and (expected_bytes_path is None or attachment.get("size") == len(payload))
                for attachment in flattened_attachments
            ):
                raise DocumentFilePipelineError(
                    f"rendition {rendition_key} does not resolve to a declared Regulations.gov attachment"
                )
        distribution_path = _distribution_path(expected_digest, relative_name)
        if distribution_path in rendition_bytes and rendition_bytes[distribution_path] != payload:
            raise DocumentFilePipelineError("two source renditions resolve to one distribution path")
        rendition_bytes[distribution_path] = payload
        prepared_renditions.append(
            {
                "bytes_digest": expected_digest,
                "key": rendition_key,
                "media_type": media_type,
                "source_native_path": distribution_path,
                "source_url": source_rendition_url,
            }
        )
        if rendition_key == primary_key:
            primary_payload = payload
            primary_media_type = media_type
            primary_digest = expected_digest

    if primary_payload is None or primary_media_type is None or primary_digest is None:
        raise DocumentFilePipelineError("document primary rendition does not exist")
    if primary_media_type not in _SUPPORTED_PRIMARY_MEDIA_TYPES:
        raise DocumentFilePipelineError(f"unsupported primary rendition media type: {primary_media_type}")
    extraction = extract_pdf_text(primary_payload)
    if extraction.status is not PdfTextStatus.OK:
        raise DocumentFilePipelineError(f"PDF text extraction failed closed with status {extraction.status.value}")
    if len(extraction.pages) != extraction.page_count:
        raise DocumentFilePipelineError("PDF page text does not close against the parser page count")
    if extraction.failed_page_ordinals:
        raise DocumentFilePipelineError(
            f"PDF extraction has failed page ordinals: {list(extraction.failed_page_ordinals)}"
        )
    passages = _pdf_passages(extraction.text, extraction.pages)

    document_type = _require_string(
        _resolve_path(
            source_content,
            _require_string(document_metadata["document_type_path"], "document_type_path"),
        ),
        "document type",
    )
    if flattened.get("document_type") != document_type:
        raise DocumentFilePipelineError("Regulations.gov extraction returned a different document type")
    source_issued_version_id = _require_string(
        _resolve_path(
            source_content,
            _require_string(
                document_metadata["source_issued_version_id_path"],
                "source_issued_version_id_path",
            ),
        ),
        "source-issued version ID",
    )
    if flattened.get("modify_date") != source_issued_version_id:
        raise DocumentFilePipelineError("Regulations.gov extraction returned a different source version")

    return (
        {
            "captures": [
                {
                    "observed_at": _require_capture_observation(
                        _require_mapping(capture, "document capture").get("observed_at"),
                        "document capture observed_at",
                    ),
                    "retrieval_receipt_ref": retrieval_receipt_ref,
                }
                for capture in captures
            ],
            "collection": collection,
            "content": source_content,
            "document": {
                "content_coordinate_system": "source-bytes",
                "content_digest": primary_digest,
                "content_media_type": primary_media_type,
                "document_type": document_type,
                "source_issued_version_id": source_issued_version_id,
            },
            "key": _require_string(document["key"], "document key"),
            "observations": [],
            "passages": passages,
            "publisher": publisher,
            "renditions": prepared_renditions,
            "representations": [
                {
                    "evidence_grade": "parser-derived",
                    "key": "primary-text",
                    "method": PDF_EXTRACTION_METHOD,
                    "method_config": PDF_EXTRACTION_CONFIG,
                    "method_version": _pypdf_version(),
                    "representation_kind_and_path": f"derived-from-rendition:{primary_key}",
                    "source_rendition_key": primary_key,
                    "unicode_text": extraction.text,
                }
            ],
            "source_record_id": source_record_id,
            "source_url": source_url,
        },
        rendition_bytes,
    )


def _prepare_captured_rendition_document(
    manifest_path: Path,
    document_spec: object,
    *,
    file_overrides: Mapping[str, Path],
    retrieval_receipt_ref: str,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Normalize one exact publisher file declared by a capture manifest."""

    document = _require_mapping(document_spec, "captured rendition document")
    _require_exact_keys(
        document,
        {
            "adapter",
            "captures",
            "collection",
            "document",
            "key",
            "publisher",
            "renditions",
            "source_record",
        },
        "captured rendition document",
    )
    if document["adapter"] != "captured-rendition/v1":
        raise DocumentFilePipelineError("captured rendition adapter differs")
    publisher = _require_string(document["publisher"], "captured document publisher")
    collection = _require_string(document["collection"], "captured document collection")
    key = _require_string(document["key"], "captured document key")
    captures = document["captures"]
    if not isinstance(captures, list) or not captures:
        raise DocumentFilePipelineError("captured document captures must be a non-empty array")

    source_spec = _require_mapping(document["source_record"], "captured source record")
    _require_exact_keys(
        source_spec,
        {"content", "source_record_id", "source_url"},
        "captured source record",
    )
    source_record_id = _require_string(
        source_spec["source_record_id"],
        "captured source record ID",
    )
    source_url = _require_string(source_spec["source_url"], "captured source URL")
    source_content = dict(_require_mapping(source_spec["content"], "captured source content"))

    document_metadata = _require_mapping(document["document"], "captured document metadata")
    _require_exact_keys(
        document_metadata,
        {"document_type", "primary_rendition_key", "source_issued_version_id"},
        "captured document metadata",
    )
    primary_key = _require_string(
        document_metadata["primary_rendition_key"],
        "captured primary rendition key",
    )

    rendition_specs = document["renditions"]
    if not isinstance(rendition_specs, list) or not rendition_specs:
        raise DocumentFilePipelineError("captured document renditions must be a non-empty array")
    prepared_renditions: list[dict[str, Any]] = []
    rendition_bytes: dict[str, bytes] = {}
    primary_payload: bytes | None = None
    primary_media_type: str | None = None
    primary_digest: str | None = None
    for raw_rendition in rendition_specs:
        rendition = _require_mapping(raw_rendition, "captured document rendition")
        _require_exact_keys(
            rendition,
            {"bytes_digest", "key", "media_type", "path", "source_url"},
            "captured document rendition",
        )
        rendition_key = _require_string(rendition["key"], "captured rendition key")
        relative_name = _require_string(rendition["path"], "captured rendition path")
        expected_digest = _require_string(
            rendition["bytes_digest"],
            "captured rendition bytes digest",
        )
        payload = _read_exact_bytes(
            _manifest_file(manifest_path, relative_name, file_overrides=file_overrides),
            expected_digest,
            label=f"captured rendition {rendition_key}",
        )
        media_type = _require_string(rendition["media_type"], "captured rendition media type")
        distribution_path = _distribution_path(expected_digest, relative_name)
        if distribution_path in rendition_bytes and rendition_bytes[distribution_path] != payload:
            raise DocumentFilePipelineError("captured rendition distribution path collision")
        rendition_bytes[distribution_path] = payload
        prepared_renditions.append(
            {
                "bytes_digest": expected_digest,
                "key": rendition_key,
                "media_type": media_type,
                "source_native_path": distribution_path,
                "source_url": _require_string(
                    rendition["source_url"],
                    "captured rendition source URL",
                ),
            }
        )
        if rendition_key == primary_key:
            primary_payload = payload
            primary_media_type = media_type
            primary_digest = expected_digest

    if primary_payload is None or primary_media_type is None or primary_digest is None:
        raise DocumentFilePipelineError("captured document primary rendition does not exist")
    if primary_media_type not in _SUPPORTED_CAPTURED_TEXT_MEDIA_TYPES:
        raise DocumentFilePipelineError(f"unsupported captured primary rendition media type: {primary_media_type}")

    if primary_media_type == "application/pdf":
        extraction, pdf_method, pdf_method_version = _extract_pdf_with(
            DEFAULT_PDF_EXTRACTION_METHOD, primary_payload, page_separator=PAGE_SEPARATOR, page_whitespace="strip"
        )
        if extraction.status is not PdfTextStatus.OK:
            raise DocumentFilePipelineError(f"PDF text extraction failed closed with status {extraction.status.value}")
        if len(extraction.pages) != extraction.page_count:
            raise DocumentFilePipelineError("PDF page text does not close against the parser page count")
        if extraction.failed_page_ordinals:
            raise DocumentFilePipelineError(
                f"PDF extraction has failed page ordinals: {list(extraction.failed_page_ordinals)}"
            )
        text = extraction.text
        _refuse_thin_pdf_parse(text, primary_payload, subject_id=f"{collection}.source-file", method=pdf_method)
        passages = _pdf_passages(text, extraction.pages)
        evidence_grade = "parser-derived"
        method = pdf_method
        method_version = pdf_method_version
        method_config = _pdf_extraction_config(pdf_method, locked=False)
    else:
        try:
            text = primary_payload.decode("utf-8-sig")
        except UnicodeError as error:
            raise DocumentFilePipelineError("captured markup is not UTF-8") from error
        spans = native_structural_passage_spans(
            f"{collection}.source-file",
            text,
            media_type=primary_media_type,
        )
        if not spans:
            raise DocumentFilePipelineError("captured markup has no visible structural passages")
        _refuse_thin_markup_parse(
            text,
            media_type=primary_media_type,
            source_field=f"{collection}.source-file",
            subject_id=f"{collection}.source-file",
        )
        passages = [
            {
                "end": end,
                "passage_policy_version": MARKUP_PASSAGE_POLICY_VERSION,
                "representation_key": "primary-text",
                "start": start,
            }
            for start, end in spans
        ]
        evidence_grade = "source-exact"
        method = "raw-utf8"
        method_version = "1"
        method_config = {"encoding": "utf-8-sig", "input_media_type": primary_media_type}

    return (
        {
            "captures": [
                {
                    "observed_at": _require_capture_observation(
                        _require_mapping(capture, "captured document capture").get("observed_at"),
                        "captured document observed_at",
                    ),
                    "retrieval_receipt_ref": retrieval_receipt_ref,
                }
                for capture in captures
            ],
            "collection": collection,
            "content": source_content,
            "document": {
                "content_coordinate_system": "source-bytes",
                "content_digest": primary_digest,
                "content_media_type": primary_media_type,
                "document_type": _require_string(
                    document_metadata["document_type"],
                    "captured document type",
                ),
                "source_issued_version_id": _require_string(
                    document_metadata["source_issued_version_id"],
                    "captured source-issued version ID",
                ),
            },
            "key": key,
            "observations": [],
            "passages": passages,
            "publisher": publisher,
            "renditions": prepared_renditions,
            "representations": [
                {
                    "evidence_grade": evidence_grade,
                    "key": "primary-text",
                    "method": method,
                    "method_config": method_config,
                    "method_version": method_version,
                    "representation_kind_and_path": f"decoded-rendition:{primary_key}",
                    "source_rendition_key": primary_key,
                    "unicode_text": text,
                }
            ],
            "source_record_id": source_record_id,
            "source_url": source_url,
        },
        rendition_bytes,
    )


def _prepare_document(
    manifest_path: Path,
    document_spec: object,
    *,
    file_overrides: Mapping[str, Path],
    retrieval_receipt_ref: str,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    document = _require_mapping(document_spec, "file manifest document")
    adapter = document.get("adapter")
    if adapter == "regulations-json-pdf/v1":
        return _prepare_regulations_document(
            manifest_path,
            document,
            file_overrides=file_overrides,
            retrieval_receipt_ref=retrieval_receipt_ref,
        )
    if adapter == "captured-rendition/v1":
        return _prepare_captured_rendition_document(
            manifest_path,
            document,
            file_overrides=file_overrides,
            retrieval_receipt_ref=retrieval_receipt_ref,
        )
    raise DocumentFilePipelineError(f"unsupported captured-file adapter: {adapter!r}")


def _prepare_file_release(
    manifest_path: Path,
    *,
    rulespec_core_path: Path,
    file_overrides: Mapping[str, Path],
) -> _PreparedFileRelease:
    try:
        manifest_bytes = manifest_path.read_bytes()
    except OSError as error:
        raise DocumentFilePipelineError(f"cannot read file manifest {manifest_path}") from error
    manifest_digest = "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
    receipt_path = f"receipts/{manifest_digest.removeprefix('sha256:')}.json"
    manifest = _parse_manifest_bytes(manifest_bytes, label=str(manifest_path))
    records: list[dict[str, Any]] = []
    rendition_bytes: dict[str, bytes] = {}
    for document_spec in manifest["documents"]:
        record, document_bytes = _prepare_document(
            manifest_path,
            document_spec,
            file_overrides=file_overrides,
            retrieval_receipt_ref=receipt_path,
        )
        records.append(record)
        for relative_path, payload in document_bytes.items():
            if relative_path in rendition_bytes and rendition_bytes[relative_path] != payload:
                raise DocumentFilePipelineError("rendition distribution path collision")
            rendition_bytes[relative_path] = payload

    fixture: dict[str, Any] = {
        "acquisition_release_ref": (
            "urn:spicy-regs:acquisition-release:file-manifest:" + manifest_digest.removeprefix("sha256:")
        ),
        "coverage_gaps": manifest["coverage_gaps"],
        "fixture_id": (
            _require_string(manifest["manifest_id"], "manifest ID") + "@" + manifest_digest.removeprefix("sha256:")
        ),
        "format_version": FIXTURE_FORMAT_VERSION,
        "links": [],
        "records": records,
        "released_at": manifest["released_at"],
        "requested_sources": [f"{source}#selection={manifest_digest}" for source in manifest["requested_sources"]],
    }
    fixture["fixture_digest"] = canonical_digest(fixture)
    release = build_document_release(
        fixture,
        rulespec_core_path,
        format_version=ACTUAL_FILE_FORMAT_VERSION,
        source_input_digest=manifest_digest,
        source_input_id=_require_string(manifest["manifest_id"], "manifest ID"),
        source_input_path=receipt_path,
        source_input_type="CapturedFileManifest",
    )
    validate_document_release(release, rulespec_core_path=rulespec_core_path)
    declared_paths = {str(rendition["source_native_path"]) for rendition in release["source_renditions"]}
    if declared_paths != set(rendition_bytes):
        raise DocumentFilePipelineError("release rendition paths do not close against captured bytes")
    return _PreparedFileRelease(
        release=release,
        rendition_bytes=rendition_bytes,
        supporting_bytes={receipt_path: manifest_bytes},
    )


def _source_cache_record(
    *,
    cache_dir: Path,
    lock_record: Mapping[str, Any],
    source_spec: Any,
    lock_digest: str,
    document_type_override: str | None = None,
) -> tuple[dict[str, Any], str, bytes]:
    profile_id = _require_string(source_spec.profile_id, "source cache profile ID")
    try:
        publisher, collection, document_type = _SOURCE_CACHE_DOCUMENT_TYPES[profile_id]
    except KeyError as error:
        raise DocumentFilePipelineError(f"unclassified source-cache profile: {profile_id}") from error
    # The per-profile constant names the *kind* of thing captured, which is all
    # a fixture corpus needs. A real corpus must carry the publisher's own
    # document type instead: a constant makes every downstream type allowlist a
    # no-op, so "Rule" and "Proposed Rule" are excluded identically and the
    # corpus admits nothing at all.
    if document_type_override is not None:
        document_type = _require_string(document_type_override, f"{source_spec.case_id} document type")
    case_id = _require_string(source_spec.case_id, "source cache case ID")
    if lock_record.get("case_id") != case_id:
        raise DocumentFilePipelineError(f"source lock record differs for case {case_id}")
    cache_file = _require_string(lock_record.get("cache_file"), f"{case_id} cache file")
    expected_digest = "sha256:" + _require_string(
        lock_record.get("source_sha256"),
        f"{case_id} source SHA-256",
    )
    payload = _read_exact_bytes(
        _manifest_file(cache_dir / "source-lock.json", cache_file, file_overrides={}),
        expected_digest,
        label=f"source cache document {case_id}",
    )
    declared_bytes = lock_record.get("source_bytes")
    if isinstance(declared_bytes, bool) or not isinstance(declared_bytes, int) or declared_bytes != len(payload):
        raise DocumentFilePipelineError(f"{case_id} source byte count differs")
    media_type = _require_string(lock_record.get("media_type"), f"{case_id} media type")
    distribution_path = _distribution_path(expected_digest, cache_file)

    if source_spec.representation == "pdf":
        locked_method = lock_record.get("extraction_method")
        extraction, pdf_method, pdf_method_version = _extract_pdf_with(
            locked_method if isinstance(locked_method, str) and locked_method else DEFAULT_PDF_EXTRACTION_METHOD,
            payload,
            page_separator=LOCKED_PDF_PAGE_SEPARATOR,
            page_whitespace="preserve",
        )
        if extraction.status is not PdfTextStatus.OK:
            raise DocumentFilePipelineError(
                f"{case_id} PDF text extraction failed closed with status {extraction.status.value}"
            )
        if extraction.failed_page_ordinals:
            raise DocumentFilePipelineError(
                f"{case_id} PDF extraction has failed page ordinals: {list(extraction.failed_page_ordinals)}"
            )
        text = extraction.text
        _refuse_thin_pdf_parse(text, payload, subject_id=case_id, method=pdf_method)
        passage_specs = _pdf_passages(
            text,
            extraction.pages,
            page_separator=LOCKED_PDF_PAGE_SEPARATOR,
        )
        evidence_grade = "parser-derived"
        method = pdf_method
        method_version = pdf_method_version
        method_config = _pdf_extraction_config(pdf_method, locked=True)
    else:
        try:
            text = payload.decode("utf-8-sig")
        except UnicodeError as error:
            raise DocumentFilePipelineError(f"{case_id} markup is not UTF-8") from error
        spans = native_structural_passage_spans(
            source_spec.target_field,
            text,
            media_type=media_type,
        )
        if not spans:
            raise DocumentFilePipelineError(f"{case_id} markup has no visible structural passages")
        _refuse_thin_markup_parse(
            text,
            media_type=media_type,
            source_field=source_spec.target_field,
            subject_id=case_id,
        )
        passage_specs = [
            {
                "end": end,
                "passage_policy_version": MARKUP_PASSAGE_POLICY_VERSION,
                "representation_key": "primary-text",
                "start": start,
            }
            for start, end in spans
        ]
        evidence_grade = "source-exact"
        method = "raw-utf8"
        method_version = "1"
        method_config = {"encoding": "utf-8-sig", "input_media_type": media_type}

    expected_chars = lock_record.get("extracted_chars")
    expected_text_digest = lock_record.get("extracted_sha256")
    expected_method = lock_record.get("extraction_method")
    expected_method_version = lock_record.get("extraction_version")
    if (
        isinstance(expected_chars, bool)
        or not isinstance(expected_chars, int)
        or len(text) != expected_chars
        or "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
        != "sha256:" + _require_string(expected_text_digest, f"{case_id} extracted SHA-256")
        or method != expected_method
        or method_version != expected_method_version
    ):
        raise DocumentFilePipelineError(f"{case_id} extraction differs from the sealed source lock")

    # These fields describe the captured publisher record itself. Retrieval,
    # selection, rights, and whole-lock facts belong to capture/acquisition
    # evidence so they cannot change an unchanged record's identity.
    source_record_content = {
        "document_type": document_type,
        "media_type": media_type,
        "rendition_bytes_digest": expected_digest,
        "source_record_id": source_spec.key_value,
        "source_url": source_spec.source_url,
    }
    return (
        {
            "captures": [
                {
                    "observed_at": _require_capture_observation(
                        lock_record.get("retrieved_on"),
                        f"{case_id} retrieved_on",
                    ),
                    "retrieval_receipt_ref": ("receipts/" + lock_digest.removeprefix("sha256:") + ".json"),
                }
            ],
            "collection": collection,
            "content": source_record_content,
            "document": {
                "content_coordinate_system": "source-bytes",
                "content_digest": expected_digest,
                "content_media_type": media_type,
                "document_type": document_type,
                "source_issued_version_id": source_spec.key_value,
            },
            "key": case_id,
            "observations": [],
            "passages": passage_specs,
            "publisher": publisher,
            "renditions": [
                {
                    "bytes_digest": expected_digest,
                    "key": "source-file",
                    "media_type": media_type,
                    "source_native_path": distribution_path,
                    "source_url": _require_string(
                        lock_record.get("resolved_url"),
                        f"{case_id} resolved URL",
                    ),
                }
            ],
            "representations": [
                {
                    "evidence_grade": evidence_grade,
                    "key": "primary-text",
                    "method": method,
                    "method_config": method_config,
                    "method_version": method_version,
                    "representation_kind_and_path": f"derived-from-rendition:{source_spec.representation}",
                    "source_rendition_key": "source-file",
                    "unicode_text": text,
                }
            ],
            "source_record_id": source_spec.key_value,
            "source_url": source_spec.source_url,
        },
        distribution_path,
        payload,
    )


def _prepare_source_cache_release(
    cache_dir: Path,
    *,
    released_at: str,
    rulespec_core_path: Path,
    source_specs: Sequence[Any] | None = None,
    document_types: Mapping[str, str] | None = None,
) -> _PreparedFileRelease:
    # The source list and byte-lock validator are reused from the measured
    # segmentation corpus. The release model and publication path remain here.
    from spicy_regs.corpora.segmentation_evaluation import FULL_DOCUMENT_SPECS, validate_source_cache

    specs = tuple(FULL_DOCUMENT_SPECS if source_specs is None else source_specs)
    cache_dir = Path(cache_dir)
    validation = validate_source_cache(cache_dir, specs=specs)
    if validation.get("status") != "pass":
        failures = validation.get("failures")
        raise DocumentFilePipelineError(f"source cache validation failed: {failures}")
    lock_path = cache_dir / "source-lock.json"
    try:
        lock_bytes = lock_path.read_bytes()
        lock = json.loads(lock_bytes.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DocumentFilePipelineError(f"cannot read source cache lock {lock_path}") from error
    lock = dict(_require_mapping(lock, "source cache lock"))
    lock_digest = "sha256:" + hashlib.sha256(lock_bytes).hexdigest()
    source_rows = lock.get("sources")
    if not isinstance(source_rows, list):
        raise DocumentFilePipelineError("source cache lock sources must be an array")
    by_case = {
        _require_string(row.get("case_id"), "source cache case ID"): row
        for row in source_rows
        if isinstance(row, Mapping)
    }
    if len(by_case) != len(source_rows):
        raise DocumentFilePipelineError("source cache case IDs must be unique objects")

    records: list[dict[str, Any]] = []
    rendition_bytes: dict[str, bytes] = {}
    requested_sources: set[str] = set()
    for spec in specs:
        lock_record = by_case.get(spec.case_id)
        if lock_record is None:
            raise DocumentFilePipelineError(f"source cache is missing case {spec.case_id}")
        record, distribution_path, payload = _source_cache_record(
            cache_dir=cache_dir,
            lock_record=lock_record,
            source_spec=spec,
            lock_digest=lock_digest,
            document_type_override=(None if document_types is None else document_types.get(spec.case_id)),
        )
        records.append(record)
        requested_sources.add(f"{record['publisher']}:{record['collection']}#selection={lock_digest}")
        if distribution_path in rendition_bytes and rendition_bytes[distribution_path] != payload:
            raise DocumentFilePipelineError("source cache rendition distribution path collision")
        rendition_bytes[distribution_path] = payload

    _require_string(lock.get("retrieved_on"), "source cache retrieved_on")
    fixture: dict[str, Any] = {
        "acquisition_release_ref": (
            "urn:spicy-regs:acquisition-release:source-lock:" + lock_digest.removeprefix("sha256:")
        ),
        "coverage_gaps": [],
        "fixture_id": "source-lock:" + lock_digest.removeprefix("sha256:"),
        "format_version": FIXTURE_FORMAT_VERSION,
        "links": [],
        "records": records,
        "released_at": _require_string(released_at, "DocumentRelease released_at"),
        "requested_sources": sorted(requested_sources),
    }
    fixture["fixture_digest"] = canonical_digest(fixture)
    receipt_path = "receipts/" + lock_digest.removeprefix("sha256:") + ".json"
    release = build_document_release(
        fixture,
        rulespec_core_path,
        format_version=ACTUAL_FILE_FORMAT_VERSION,
        source_input_digest=lock_digest,
        source_input_id="source-lock:" + lock_digest.removeprefix("sha256:"),
        source_input_path=receipt_path,
        source_input_type="EvaluationSourceLock",
    )
    validate_document_release(release, rulespec_core_path=rulespec_core_path)
    return _PreparedFileRelease(
        release=release,
        rendition_bytes=rendition_bytes,
        supporting_bytes={receipt_path: lock_bytes},
    )


def _contained_distribution_file(root: Path, relative_name: object, *, label: str) -> Path:
    name = _require_string(relative_name, label)
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise DocumentFilePipelineError(f"{label} is not a contained relative path")
    resolved_root = root.resolve()
    candidate = (resolved_root / Path(*relative.parts)).resolve()
    try:
        candidate.relative_to(resolved_root)
    except ValueError as error:
        raise DocumentFilePipelineError(f"{label} escapes the distribution") from error
    if not candidate.is_file():
        raise DocumentFilePipelineError(f"{label} is missing from the distribution")
    return candidate


def _captured_manifest_file_overrides(
    manifest: Mapping[str, Any],
    distribution_dir: Path,
) -> dict[str, Path]:
    """Resolve every captured manifest input to its content-addressed copy."""

    documents = manifest.get("documents")
    if not isinstance(documents, list):
        raise DocumentFilePipelineError("captured manifest documents must be an array")
    overrides: dict[str, Path] = {}
    for document_index, raw_document in enumerate(documents):
        document = _require_mapping(
            raw_document,
            f"captured manifest document {document_index}",
        )
        renditions = document.get("renditions")
        if not isinstance(renditions, list) or not renditions:
            raise DocumentFilePipelineError(f"captured manifest document {document_index} has no renditions")
        for rendition_index, raw_rendition in enumerate(renditions):
            rendition = _require_mapping(
                raw_rendition,
                f"captured manifest document {document_index} rendition {rendition_index}",
            )
            source_path = _require_string(
                rendition.get("path"),
                "captured manifest rendition path",
            )
            digest = _require_string(
                rendition.get("bytes_digest"),
                "captured manifest rendition digest",
            )
            target = _contained_distribution_file(
                distribution_dir,
                _distribution_path(digest, source_path),
                label="captured manifest rendition distribution path",
            )
            previous = overrides.get(source_path)
            if previous is not None and previous != target:
                raise DocumentFilePipelineError("captured manifest reuses one source path for different renditions")
            overrides[source_path] = target
    return overrides


def validate_document_release_distribution(
    distribution_dir: Path,
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> dict[str, Any]:
    """Validate release JSON and every file reference in a published distribution."""

    distribution_dir = Path(distribution_dir)
    release_path = distribution_dir / "document-release.json"
    try:
        release_text = release_path.read_text(encoding="utf-8")
        release = json.loads(release_text)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DocumentFilePipelineError(f"cannot read distribution release {release_path}") from error
    release = dict(_require_mapping(release, "distribution DocumentRelease"))
    if release_text != canonical_json(release) + "\n":
        raise DocumentFilePipelineError("distribution DocumentRelease is not canonical JSON")
    validate_document_release(release, rulespec_core_path=Path(rulespec_core_path))

    for rendition in release["source_renditions"]:
        rendition = _require_mapping(rendition, "distribution SourceRendition")
        target = _contained_distribution_file(
            distribution_dir,
            rendition.get("source_native_path"),
            label="source rendition path",
        )
        rendition_payload = target.read_bytes()
        actual = "sha256:" + hashlib.sha256(rendition_payload).hexdigest()
        if actual != rendition.get("bytes_digest"):
            raise DocumentFilePipelineError("published rendition bytes digest differs")

    receipt_refs = {
        _require_string(capture.get("retrieval_receipt_ref"), "capture receipt reference")
        for capture in (
            _require_mapping(item, "distribution SourceRenditionCapture")
            for item in release["source_rendition_captures"]
        )
    }
    source_input = release.get("source_input")
    if source_input is not None:
        source_input = _require_mapping(source_input, "distribution source input")
        input_path = _require_string(
            source_input.get("input_path"),
            "distribution source input path",
        )
        if input_path not in receipt_refs:
            raise DocumentFilePipelineError("published source input is not one of the capture evidence files")
        target = _contained_distribution_file(
            distribution_dir,
            input_path,
            label="source input path",
        )
        input_bytes = target.read_bytes()
        actual = "sha256:" + hashlib.sha256(input_bytes).hexdigest()
        if actual != source_input.get("input_digest"):
            raise DocumentFilePipelineError("source input bytes digest differs")
        input_type = source_input.get("input_type")
        if input_type == "CapturedFileManifest":
            manifest = _parse_manifest_bytes(
                input_bytes,
                label=input_path,
            )
            if manifest.get("manifest_id") != source_input.get("input_id"):
                raise DocumentFilePipelineError("source input ID differs from the captured manifest")
            declared_sources = manifest.get("requested_sources")
            release_sources = release["acquisition_coverage"]["requested_sources"]
            if not isinstance(declared_sources, list) or {
                str(source).partition("#selection=")[0] for source in release_sources
            } != set(declared_sources):
                raise DocumentFilePipelineError("release requested sources differ from the captured manifest")
            expected = _prepare_file_release(
                target,
                rulespec_core_path=Path(rulespec_core_path),
                file_overrides=_captured_manifest_file_overrides(
                    manifest,
                    distribution_dir,
                ),
            ).release
            if expected != release:
                raise DocumentFilePipelineError(
                    "published release is not reproducible from its captured manifest and renditions"
                )
        elif input_type == "EvaluationSourceLock":
            expected_id = "source-lock:" + actual.removeprefix("sha256:")
            if source_input.get("input_id") != expected_id:
                raise DocumentFilePipelineError("source input ID differs from the source lock")
        else:
            raise DocumentFilePipelineError("distribution source input type is unsupported")
    for receipt_ref in receipt_refs:
        target = _contained_distribution_file(
            distribution_dir,
            receipt_ref,
            label="capture receipt path",
        )
        expected = PurePosixPath(receipt_ref).stem
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual != expected:
            raise DocumentFilePipelineError("published capture receipt bytes digest differs")
    return release


def _publish_prepared_release(
    prepared: _PreparedFileRelease,
    output_dir: Path,
    *,
    rulespec_core_path: Path,
) -> dict[str, Any]:
    output_dir = Path(output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise DocumentFilePipelineError(f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    published_bytes = dict(prepared.rendition_bytes)
    published_bytes.update(prepared.supporting_bytes)
    for relative_path, payload in sorted(published_bytes.items()):
        target = output_dir / Path(*PurePosixPath(relative_path).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    write_document_release(
        output_dir / "document-release.json",
        prepared.release,
        rulespec_core_path=rulespec_core_path,
    )
    receipt_refs = {str(capture["retrieval_receipt_ref"]) for capture in prepared.release["source_rendition_captures"]}
    if receipt_refs != set(prepared.supporting_bytes):
        raise DocumentFilePipelineError("capture receipt references do not close against published files")
    validated = validate_document_release_distribution(
        output_dir,
        rulespec_core_path=rulespec_core_path,
    )
    if validated != prepared.release:
        raise DocumentFilePipelineError("published release differs from the prepared release")
    return validated


def build_document_release_from_file_manifest(
    manifest_path: Path = DEFAULT_FILE_MANIFEST_PATH,
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
    file_overrides: Mapping[str, Path] | None = None,
) -> dict[str, Any]:
    """Read verified captured files and return their sealed ``DocumentRelease``."""

    prepared = _prepare_file_release(
        Path(manifest_path),
        rulespec_core_path=Path(rulespec_core_path),
        file_overrides={} if file_overrides is None else file_overrides,
    )
    return prepared.release


def build_document_release_from_source_cache(
    cache_dir: Path,
    *,
    released_at: str,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
    source_specs: Sequence[Any] | None = None,
    document_types: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build a local conformance release from a source cache.

    The cache lock does not package the code-defined source specifications or
    complete source-issued version metadata.  This helper therefore exercises
    parser depth and addressability only; it is intentionally absent from the
    publication CLI.

    ``source_specs`` defaults to the 34-file segmentation evaluation cache.
    Supplying it lets a differently-drawn corpus over the same lock contract
    build a release without copying this path; the ESA body-retrieval corpus
    is the first such caller.
    """

    return _prepare_source_cache_release(
        Path(cache_dir),
        released_at=released_at,
        rulespec_core_path=Path(rulespec_core_path),
        source_specs=source_specs,
        document_types=document_types,
    ).release


def publish_document_release_from_source_cache(
    cache_dir: Path,
    output_dir: Path,
    *,
    released_at: str,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
    source_specs: Sequence[Any] | None = None,
    document_types: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write the release JSON, its exact source bytes, and its capture receipt.

    A consumer of a v2 release needs the published *distribution*, not a bare
    release file -- spicysearch refuses the latter with "build from the
    directory, not the file". Only the file-manifest path had a publish
    counterpart, so a source-cache release could be built but never admitted.
    """

    prepared = _prepare_source_cache_release(
        Path(cache_dir),
        released_at=released_at,
        rulespec_core_path=Path(rulespec_core_path),
        source_specs=source_specs,
        document_types=document_types,
    )
    return _publish_prepared_release(
        prepared,
        Path(output_dir),
        rulespec_core_path=Path(rulespec_core_path),
    )


def publish_document_release_from_file_manifest(
    manifest_path: Path,
    output_dir: Path,
    *,
    rulespec_core_path: Path = DEFAULT_RULESPEC_CORE_PATH,
) -> dict[str, Any]:
    """Write release JSON, exact source bytes, and capture input as one distribution."""

    prepared = _prepare_file_release(
        Path(manifest_path),
        rulespec_core_path=Path(rulespec_core_path),
        file_overrides={},
    )
    return _publish_prepared_release(
        prepared,
        Path(output_dir),
        rulespec_core_path=Path(rulespec_core_path),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a DocumentRelease from exact captured files")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--rulespec-core", type=Path, default=DEFAULT_RULESPEC_CORE_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    release = publish_document_release_from_file_manifest(
        args.manifest,
        args.output_dir,
        rulespec_core_path=args.rulespec_core,
    )
    print(
        canonical_json(
            {
                "document_count": len(release["document_versions"]),
                "output_dir": str(args.output_dir),
                "passage_count": len(release["structural_passages"]),
                "release_digest": release["release_digest"],
                "status": "pass",
            }
        )
    )
    return 0


def validate_distribution_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a complete DocumentRelease distribution")
    parser.add_argument("--distribution", type=Path, required=True)
    parser.add_argument("--rulespec-core", type=Path, default=DEFAULT_RULESPEC_CORE_PATH)
    args = parser.parse_args(argv)
    release = validate_document_release_distribution(
        args.distribution,
        rulespec_core_path=args.rulespec_core,
    )
    print(
        canonical_json(
            {
                "document_count": len(release["document_versions"]),
                "release_digest": release["release_digest"],
                "status": "pass",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

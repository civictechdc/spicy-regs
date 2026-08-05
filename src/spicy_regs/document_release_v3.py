"""Protocol primitives for the sealed ``DocumentRelease`` version 3 format.

This module deliberately contains no pipeline or search code.  It defines the
closed manifest shapes, canonical identity recipe, Arrow table schemas, and
small value objects shared by the SpicyRegs writer and producer verifier.
Consumers in other products implement the same public file protocol without
importing this module.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

import pyarrow as pa


FORMAT = "spicyregs-document-release"
FORMAT_VERSION = "3.0"
CANONICAL_JSON_PROFILE = "spicy-canonical-json-v1"
MEMBER_MANIFEST_FORMAT = "spicy-artifact-member-manifest"
MEMBER_MANIFEST_VERSION = "1.0"
RELEASE_ID_PREFIX = "urn:spicyregs:document-release:v3:"
SCHEMA_SET_ID_PREFIX = "urn:spicy:schema-set:v1:"
MAX_JSON_SAFE_INTEGER = (1 << 53) - 1

SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
DIAGNOSTIC_CODE_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9-]*){2,}\Z")
MEMORY_LIMIT_PATTERN = re.compile(r"[1-9][0-9]*(?:\.[0-9]+)?(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)\Z", re.IGNORECASE)

ROOT_KEYS = frozenset({"format", "formatVersion", "releaseId", "content", "annotations"})
CONTENT_KEYS = frozenset(
    {
        "schemaSet",
        "producer",
        "processingPolicy",
        "sourceSelection",
        "previousRelease",
        "globalManifest",
        "partitionManifests",
        "counts",
        "coverage",
    }
)
MEMBER_KEYS = frozenset(
    {
        "objectKey",
        "role",
        "mediaType",
        "byteSize",
        "sha256",
        "recordCount",
        "schemaId",
        "partitionId",
        "servingShardId",
    }
)
MANIFEST_REFERENCE_KEYS = frozenset({"manifestId", "scopeKind", "scopeId", "objectKey", "byteSize", "sha256"})
SUBORDINATE_MANIFEST_KEYS = frozenset({"format", "formatVersion", "manifestId", "scope", "members", "counts"})

ALLOWED_MEMBER_ROLES = frozenset(
    {
        "schema",
        "current-documents",
        "documents",
        "eligibility-evidence",
        "passages",
        "source-dispositions",
        "changes",
        "failures",
        "coverage",
        "rendition",
        "rendition-pack",
        "rendition-pack-index",
        "build-receipt",
        "partition-receipt",
    }
)

COUNT_FIELDS = (
    "selectedDocumentCount",
    "previousActiveDocumentCount",
    "reconciliationUniverseCount",
    "activeDocumentCount",
    "deletedDocumentCount",
    "excludedDocumentCount",
    "acceptedTerminalFailureCount",
    "documentVersionCount",
    "passageCount",
    "renditionCount",
    "eligibilityEvidenceCount",
    "sourceDispositionCount",
    "failureRecordCount",
    "partitionManifestCount",
    "memberCount",
    "totalMemberByteSize",
)
COVERAGE_FIELDS = (
    "sourceCount",
    "eligibleActiveDocumentCount",
    "ineligibleActiveDocumentCount",
    "unverifiedActiveDocumentCount",
    "normalizedTextUtf8ByteCount",
    "passageTextUtf8ByteCount",
    "renditionByteCount",
)


class VerificationCode(StrEnum):
    """Registered first-result codes shared with independent consumers."""

    VALID = "valid"
    ROOT_SYNTAX = "invalid.root-syntax"
    FORMAT = "invalid.format"
    IDENTITY = "invalid.identity"
    MEMBERSHIP_MISSING = "invalid.membership-missing"
    MEMBERSHIP_EXTRA = "invalid.membership-extra"
    MEMBER_DIGEST = "invalid.member-digest"
    PATH = "invalid.path"
    SCHEMA = "invalid.schema"
    DUPLICATE_IDENTITY = "invalid.duplicate-identity"
    FOREIGN_KEY = "invalid.foreign-key"
    COORDINATE = "invalid.coordinate"
    ELIGIBILITY_EVIDENCE = "invalid.eligibility-evidence"
    RECONCILIATION = "invalid.reconciliation"


class DocumentReleaseV3Error(ValueError):
    """A v3 protocol value cannot be represented safely."""


class DocumentReleaseV3VerificationError(DocumentReleaseV3Error):
    """A materialized release failed closed verification."""

    def __init__(self, code: VerificationCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def fail(code: VerificationCode, detail: str) -> NoReturn:
    """Raise a verification failure with a stable first-result code."""

    raise DocumentReleaseV3VerificationError(code, detail)


def require_exact_keys(value: Mapping[str, Any], expected: set[str] | frozenset[str], label: str) -> None:
    """Require one closed JSON object."""

    actual = set(value)
    if actual != set(expected):
        missing = sorted(set(expected) - actual)
        unexpected = sorted(actual - set(expected))
        raise DocumentReleaseV3Error(f"{label} keys differ; missing={missing}, unexpected={unexpected}")


def _validate_json_value(value: object, path: str = "$", *, unsigned_integers: bool = False) -> None:
    if value is None or isinstance(value, (str, bool)):
        if isinstance(value, str) and any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise DocumentReleaseV3Error(f"{path} contains a lone Unicode surrogate")
        return
    if isinstance(value, int):
        minimum = 0 if unsigned_integers else -MAX_JSON_SAFE_INTEGER
        if not minimum <= value <= MAX_JSON_SAFE_INTEGER:
            raise DocumentReleaseV3Error(f"{path} is outside the JSON safe integer range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DocumentReleaseV3Error(f"{path} contains a non-finite number")
        raise DocumentReleaseV3Error(f"{path} contains a floating-point value")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DocumentReleaseV3Error(f"{path} has a non-string object key")
            _validate_json_value(key, f"{path}.<key>")
            _validate_json_value(item, f"{path}.{key}", unsigned_integers=unsigned_integers)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]", unsigned_integers=unsigned_integers)
        return
    raise DocumentReleaseV3Error(f"{path} contains unsupported JSON value {type(value).__name__}")


def canonical_json_bytes(value: object) -> bytes:
    """Encode the integer-only RFC 8785 profile used by v3 identities.

    RFC 8785 and Python's JSON encoder agree for the permitted value domain:
    objects, arrays, strings, booleans, null, and JSON-safe integers.  The
    profile rejects floats before serialization, avoiding implementation-
    dependent numeric spellings.
    """

    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_text(value: object) -> str:
    """Return canonical JSON as UTF-8 text."""

    return canonical_json_bytes(value).decode("utf-8")


def _reject_float(value: str) -> NoReturn:
    raise DocumentReleaseV3Error(f"floating-point JSON value is forbidden: {value}")


def _reject_constant(value: str) -> NoReturn:
    raise DocumentReleaseV3Error(f"non-finite JSON value is forbidden: {value}")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DocumentReleaseV3Error(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def parse_canonical_json(data: bytes, *, label: str) -> Any:
    """Parse JSON and require the exact canonical byte representation."""

    if data.startswith(b"\xef\xbb\xbf"):
        raise DocumentReleaseV3Error(f"{label} starts with a byte order mark")
    try:
        value = json.loads(
            data,
            object_pairs_hook=_object_without_duplicates,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DocumentReleaseV3Error(f"{label} is not valid UTF-8 JSON: {error}") from error
    _validate_json_value(value)
    if canonical_json_bytes(value) != data:
        raise DocumentReleaseV3Error(f"{label} is not canonical {CANONICAL_JSON_PROFILE}")
    return value


def sha256_bytes(data: bytes) -> str:
    """Return the lowercase SHA-256 digest of bytes."""

    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = 1 << 20) -> tuple[str, int]:
    """Hash one file in bounded memory and return digest plus byte size."""

    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def require_sha256(value: object, label: str) -> str:
    """Return a validated lowercase hexadecimal SHA-256 value."""

    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise DocumentReleaseV3Error(f"{label} must be 64 lowercase hexadecimal characters")
    return value


def require_non_empty_string(value: object, label: str) -> str:
    """Return a validated non-empty Unicode string."""

    if not isinstance(value, str) or not value:
        raise DocumentReleaseV3Error(f"{label} must be a non-empty string")
    _validate_json_value(value, label)
    return value


def require_memory_limit(value: object) -> str:
    """Validate a DuckDB memory limit before using it in a ``SET`` statement."""

    limit = require_non_empty_string(value, "memory limit")
    if MEMORY_LIMIT_PATTERN.fullmatch(limit) is None:
        raise DocumentReleaseV3Error("memory limit must be a positive size such as 512MB or 2GiB")
    return limit


def require_safe_unsigned_integer(value: object, label: str) -> int:
    """Return a JSON-safe unsigned integer, excluding booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_JSON_SAFE_INTEGER:
        raise DocumentReleaseV3Error(f"{label} must be a JSON safe unsigned integer")
    return value


def validate_object_key(value: object, label: str = "objectKey") -> str:
    """Validate a portable relative member key without resolving it."""

    key = require_non_empty_string(value, label)
    if "\x00" in key or "\\" in key:
        raise DocumentReleaseV3Error(f"{label} contains a forbidden platform-specific escape")
    path = PurePosixPath(key)
    if path.is_absolute() or key.startswith("/"):
        raise DocumentReleaseV3Error(f"{label} must be relative")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise DocumentReleaseV3Error(f"{label} contains an unsafe path segment")
    if path.as_posix() != key:
        raise DocumentReleaseV3Error(f"{label} is not a normalized POSIX key")
    return key


def identity_payload(content: Mapping[str, Any]) -> dict[str, Any]:
    """Construct the closed identity-bearing root payload."""

    return {"format": FORMAT, "formatVersion": FORMAT_VERSION, "content": dict(content)}


def artifact_digest(content: Mapping[str, Any]) -> str:
    """Derive the release artifact digest from semantic content."""

    return sha256_bytes(canonical_json_bytes(identity_payload(content)))


def release_id(content: Mapping[str, Any]) -> str:
    """Derive the required v3 URN from semantic content."""

    return RELEASE_ID_PREFIX + artifact_digest(content)


def schema_set_id(descriptors: Sequence[Mapping[str, Any]]) -> str:
    """Derive the schema-set identity from its sorted descriptor array."""

    return SCHEMA_SET_ID_PREFIX + sha256_bytes(canonical_json_bytes(list(descriptors)))


@dataclass(frozen=True, slots=True)
class MemberDescriptor:
    """Closed descriptor for one immutable release member."""

    object_key: str
    role: str
    media_type: str
    byte_size: int
    sha256: str
    record_count: int | None
    schema_id: str | None
    partition_id: str | None = None
    serving_shard_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "objectKey": validate_object_key(self.object_key),
            "role": require_non_empty_string(self.role, "member role"),
            "mediaType": require_non_empty_string(self.media_type, "member media type"),
            "byteSize": require_safe_unsigned_integer(self.byte_size, "member byte size"),
            "sha256": require_sha256(self.sha256, "member digest"),
            "recordCount": (
                None
                if self.record_count is None
                else require_safe_unsigned_integer(self.record_count, "member record count")
            ),
            "schemaId": self.schema_id,
            "partitionId": self.partition_id,
            "servingShardId": self.serving_shard_id,
        }


@dataclass(frozen=True, slots=True)
class ManifestReference:
    """Root reference to one subordinate member manifest."""

    manifest_id: str
    scope_kind: str
    scope_id: str
    object_key: str
    byte_size: int
    sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifestId": require_non_empty_string(self.manifest_id, "manifest id"),
            "scopeKind": require_non_empty_string(self.scope_kind, "manifest scope kind"),
            "scopeId": require_non_empty_string(self.scope_id, "manifest scope id"),
            "objectKey": validate_object_key(self.object_key, "manifest object key"),
            "byteSize": require_safe_unsigned_integer(self.byte_size, "manifest byte size"),
            "sha256": require_sha256(self.sha256, "manifest digest"),
        }


def make_subordinate_manifest(*, scope_kind: str, scope_id: str, members: Sequence[MemberDescriptor]) -> dict[str, Any]:
    """Build one canonical subordinate manifest with exact rollups."""

    manifest_id = f"{scope_kind}:{scope_id}"
    encoded_members = sorted((member.as_dict() for member in members), key=lambda item: item["objectKey"])
    return {
        "format": MEMBER_MANIFEST_FORMAT,
        "formatVersion": MEMBER_MANIFEST_VERSION,
        "manifestId": manifest_id,
        "scope": {"kind": scope_kind, "id": scope_id},
        "members": encoded_members,
        "counts": {
            "memberCount": len(encoded_members),
            "totalByteSize": sum(member["byteSize"] for member in encoded_members),
            "totalRecordCount": sum(member["recordCount"] or 0 for member in encoded_members),
        },
    }


CURRENT_DOCUMENTS_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:current-documents:1.0"
DOCUMENTS_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:documents:1.0"
ELIGIBILITY_EVIDENCE_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:eligibility-evidence:1.0"
ELIGIBILITY_DATA_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:eligibility-data:1.0"
RENDITION_UTF8_COORDINATE_SCHEMA_ID = "urn:spicyregs:coordinate:rendition-utf8-byte-slice:1.0"
PASSAGES_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:passages:1.0"
SOURCE_DISPOSITIONS_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:source-dispositions:1.0"
CHANGES_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:changes:1.0"
FAILURES_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:failures:1.0"
COVERAGE_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:coverage:1.0"
RENDITION_PACK_INDEX_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:rendition-pack-index:1.0"
PARTITION_RECEIPT_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:partition-receipt:1.0"
BUILD_RECEIPT_SCHEMA_ID = "urn:spicyregs:schema:document-release-v3:build-receipt:1.0"

UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")
TABLE_SCHEMAS: dict[str, pa.Schema] = {
    CURRENT_DOCUMENTS_SCHEMA_ID: pa.schema(
        [
            ("document_id", pa.string(), False),
            ("document_version_id", pa.string()),
            ("state", pa.string(), False),
            ("source_id", pa.string(), False),
            ("source_partition", pa.string()),
            ("eligibility_state", pa.string()),
        ]
    ),
    DOCUMENTS_SCHEMA_ID: pa.schema(
        [
            ("document_id", pa.string(), False),
            ("document_version_id", pa.string(), False),
            ("rendition_digest", pa.binary(32), False),
            ("source_record_id", pa.string(), False),
            ("source_version", pa.string(), False),
            ("title", pa.string()),
            ("published_at", UTC_TIMESTAMP),
            ("updated_at", UTC_TIMESTAMP),
            ("document_type", pa.string(), False),
            ("language", pa.string(), False),
            ("normalized_text", pa.large_string(), False),
            ("normalized_text_digest", pa.binary(32), False),
            ("normalizer_id", pa.string(), False),
            ("segmenter_id", pa.string(), False),
            ("processing_policy_id", pa.string(), False),
            ("eligibility_state", pa.string(), False),
            ("eligibility_evidence_id", pa.string(), False),
        ]
    ),
    ELIGIBILITY_EVIDENCE_SCHEMA_ID: pa.schema(
        [
            ("eligibility_evidence_id", pa.string(), False),
            ("document_version_id", pa.string(), False),
            ("policy_id", pa.string(), False),
            ("authority_id", pa.string(), False),
            ("evidence_kind", pa.string(), False),
            ("verdict", pa.string(), False),
            ("evidence_schema_id", pa.string(), False),
            ("evidence_data", pa.large_string(), False),
            ("evidence_digest", pa.binary(32), False),
            ("reason_code", pa.string(), False),
        ]
    ),
    PASSAGES_SCHEMA_ID: pa.schema(
        [
            ("passage_id", pa.string(), False),
            ("document_id", pa.string(), False),
            ("document_version_id", pa.string(), False),
            ("ordinal", pa.uint64(), False),
            ("text", pa.large_string(), False),
            ("text_digest", pa.binary(32), False),
            ("normalized_start_utf8_byte", pa.uint64(), False),
            ("normalized_end_utf8_byte", pa.uint64(), False),
            ("coordinate_scheme", pa.string(), False),
            ("coordinate_data", pa.large_string(), False),
            ("processing_steps", pa.list_(pa.string()), False),
        ]
    ),
    SOURCE_DISPOSITIONS_SCHEMA_ID: pa.schema(
        [
            ("document_id", pa.string(), False),
            ("source_input_id", pa.string()),
            ("selected_current", pa.bool_(), False),
            ("previous_active", pa.bool_(), False),
            ("disposition", pa.string(), False),
            ("document_version_id", pa.string()),
            ("exclusion_policy_id", pa.string()),
            ("failure_id", pa.string()),
            ("disposition_code", pa.string(), False),
        ]
    ),
    CHANGES_SCHEMA_ID: pa.schema(
        [
            ("document_id", pa.string(), False),
            ("old_document_version_id", pa.string()),
            ("new_document_version_id", pa.string()),
            ("change_kind", pa.string(), False),
        ]
    ),
    FAILURES_SCHEMA_ID: pa.schema(
        [
            ("failure_id", pa.string(), False),
            ("source_input_id", pa.string(), False),
            ("document_id", pa.string(), False),
            ("stage", pa.string(), False),
            ("failure_class", pa.string(), False),
            ("retryable", pa.bool_(), False),
            ("attempt_count", pa.uint32(), False),
            ("diagnostic_code", pa.string(), False),
            ("final_disposition", pa.string(), False),
            ("failure_policy_id", pa.string(), False),
        ]
    ),
    COVERAGE_SCHEMA_ID: pa.schema(
        [
            ("source_id", pa.string(), False),
            ("selected_document_count", pa.uint64(), False),
            ("active_document_count", pa.uint64(), False),
            ("deleted_document_count", pa.uint64(), False),
            ("excluded_document_count", pa.uint64(), False),
            ("accepted_terminal_failure_count", pa.uint64(), False),
        ]
    ),
    RENDITION_PACK_INDEX_SCHEMA_ID: pa.schema(
        [
            ("rendition_digest", pa.binary(32), False),
            ("byte_offset", pa.uint64(), False),
            ("byte_length", pa.uint64(), False),
            ("media_type", pa.string(), False),
        ]
    ),
    PARTITION_RECEIPT_SCHEMA_ID: pa.schema(
        [
            ("task_key", pa.string(), False),
            ("attempt_id", pa.string(), False),
            ("partition_id", pa.string(), False),
            ("state", pa.string(), False),
            ("document_count", pa.uint64(), False),
            ("passage_count", pa.uint64(), False),
            ("rendition_byte_count", pa.uint64(), False),
            ("started_at", pa.string(), False),
            ("completed_at", pa.string(), False),
            ("member_digests_json", pa.large_string(), False),
        ]
    ),
}

ROLE_SCHEMA_IDS = {
    "current-documents": CURRENT_DOCUMENTS_SCHEMA_ID,
    "documents": DOCUMENTS_SCHEMA_ID,
    "eligibility-evidence": ELIGIBILITY_EVIDENCE_SCHEMA_ID,
    "passages": PASSAGES_SCHEMA_ID,
    "source-dispositions": SOURCE_DISPOSITIONS_SCHEMA_ID,
    "changes": CHANGES_SCHEMA_ID,
    "failures": FAILURES_SCHEMA_ID,
    "coverage": COVERAGE_SCHEMA_ID,
    "rendition-pack-index": RENDITION_PACK_INDEX_SCHEMA_ID,
    "partition-receipt": PARTITION_RECEIPT_SCHEMA_ID,
    "build-receipt": BUILD_RECEIPT_SCHEMA_ID,
}


def arrow_schema_document(schema_id: str, schema: pa.Schema) -> dict[str, Any]:
    """Describe an exact Arrow schema in canonical, implementation-neutral JSON."""

    return {
        "format": "spicy-arrow-schema",
        "formatVersion": "1.0",
        "schemaId": schema_id,
        "fields": [
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
            for field in schema
        ],
    }


def schema_documents() -> dict[str, dict[str, Any]]:
    """Return every closed schema document emitted by the reference writer."""

    documents = {schema_id: arrow_schema_document(schema_id, schema) for schema_id, schema in TABLE_SCHEMAS.items()}
    documents[ELIGIBILITY_DATA_SCHEMA_ID] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": ELIGIBILITY_DATA_SCHEMA_ID,
        "type": "object",
        "additionalProperties": False,
        "required": ["basis", "sourceInputId"],
        "properties": {
            "basis": {"type": "string", "minLength": 1},
            "sourceInputId": {"type": "string", "minLength": 1},
        },
    }
    documents[RENDITION_UTF8_COORDINATE_SCHEMA_ID] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RENDITION_UTF8_COORDINATE_SCHEMA_ID,
        "type": "object",
        "additionalProperties": False,
        "required": ["endUtf8Byte", "mediaType", "renditionSha256", "startUtf8Byte"],
        "properties": {
            "endUtf8Byte": {"type": "integer", "minimum": 0},
            "mediaType": {"type": "string", "minLength": 1},
            "renditionSha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "startUtf8Byte": {"type": "integer", "minimum": 0},
        },
    }
    documents[BUILD_RECEIPT_SCHEMA_ID] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": BUILD_RECEIPT_SCHEMA_ID,
        "type": "object",
        "additionalProperties": False,
        "required": [
            "conformanceClass",
            "specificationVersion",
            "implementation",
            "configurationIdentity",
            "inputIdentities",
            "outputIdentities",
            "counts",
            "coverage",
            "failureTotal",
            "retryTotal",
            "startedAt",
            "completedAt",
            "verifier",
            "verdict",
            "verificationCode",
        ],
        "properties": {
            "conformanceClass": {"const": "DocumentRelease Producer"},
            "specificationVersion": {"const": "2026-08-04"},
            "implementation": {"type": "object"},
            "configurationIdentity": {"type": "string"},
            "inputIdentities": {"type": "array"},
            "outputIdentities": {"type": "array"},
            "counts": {"type": "object"},
            "coverage": {"type": "object"},
            "failureTotal": {"type": "integer", "minimum": 0},
            "retryTotal": {"type": "integer", "minimum": 0},
            "startedAt": {"type": "string"},
            "completedAt": {"type": "string"},
            "verifier": {"type": "object"},
            "verdict": {"enum": ["pass", "fail"]},
            "verificationCode": {"type": "string"},
        },
    }
    return documents


def role_for_schema(schema_id: str) -> tuple[str, ...]:
    """Return the sorted semantic roles covered by one schema document."""

    roles = sorted(role for role, candidate in ROLE_SCHEMA_IDS.items() if candidate == schema_id)
    if schema_id == ELIGIBILITY_DATA_SCHEMA_ID:
        roles = ["eligibility-evidence-data"]
    if schema_id == RENDITION_UTF8_COORDINATE_SCHEMA_ID:
        roles = ["passage-coordinate-data"]
    if not roles:
        raise DocumentReleaseV3Error(f"schema {schema_id!r} has no registered role")
    return tuple(roles)

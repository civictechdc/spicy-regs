"""Reference implementation of the RefSpec vocabulary-enrichment boundary.

This module deliberately sits beside the historical fused-registry
experiments.  It reuses their source parsers and ranked candidate identifiers,
but it does not treat their flat rows as semantic authority.  Conforming data
has one row per authored label, hierarchy relation, and lifecycle participant,
plus immutable operational records with canonical payload digests.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from spicy_regs.ontology.common import (
    canonical_json,
    parse_json_list,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)

CANONICAL_JSON_POLICY = "urn:ref:canonical-json:v1"
DEVELOPMENT_DATASET_ID = "rulespec-development-35-v1"

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANONICAL_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_BCP47 = re.compile(
    r"^(?:(?:[A-Za-z]{2,3}(?:-[A-Za-z]{3}){0,3}|[A-Za-z]{4}|"
    r"[A-Za-z]{5,8})(?:-[A-Za-z]{4})?(?:-(?:[A-Za-z]{2}|[0-9]{3}))?"
    r"(?:-(?:[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*"
    r"(?:-[0-9A-WY-Za-wy-z](?:-[A-Za-z0-9]{2,8})+)*"
    r"(?:-[xX](?:-[A-Za-z0-9]{1,8})+)?|"
    r"[xX](?:-[A-Za-z0-9]{1,8})+|[eE][nN]-[gG][bB]-[oO][eE][dD]|"
    r"[iI]-(?:[aA][mM][iI]|[bB][nN][nN]|[dD][eE][fF][aA][uU][lL][tT]|"
    r"[eE][nN][oO][cC][hH][iI][aA][nN]|[hH][aA][kK]|"
    r"[kK][lL][iI][nN][gG][oO][nN]|[lL][uU][xX]|"
    r"[mM][iI][nN][gG][oO]|[nN][aA][vV][aA][jJ][oO]|"
    r"[pP][wW][nN]|[tT][aA][oO]|[tT][aA][yY]|[tT][sS][uU])|"
    r"[sS][gG][nN]-(?:[bB][eE]-[fF][rR]|[bB][eE]-[nN][lL]|"
    r"[cC][hH]-[dD][eE])|[aA][rR][tT]-[lL][oO][jJ][bB][aA][nN]|"
    r"[cC][eE][lL]-[gG][aA][uU][lL][iI][sS][hH]|"
    r"[nN][oO]-(?:[bB][oO][kK]|[nN][yY][nN])|"
    r"[zZ][hH]-(?:[gG][uU][oO][yY][uU]|[hH][aA][kK][kK][aA]|"
    r"[mM][iI][nN]|[mM][iI][nN]-[nN][aA][nN]|"
    r"[xX][iI][aA][nN][gG]))$"
)
_LEGACY_FIELDS = frozenset(
    {
        "broader_id",
        "replaced_by",
        "alt_labels_json",
        "hidden_labels_json",
    }
)
_LEGACY_NORMALIZATION_POLICIES = frozenset(
    {
        "ascii",
        "ascii-only",
        "ascii-only-v1",
        "legacy-ascii-v1",
    }
)
_LEGACY_AUTHORITIES = frozenset(
    {
        "fused-registry",
        "fused-registry-v1",
        "legacyFusedRegistry",
    }
)

CONCEPT_LABEL_COLUMNS = (
    "label_id",
    "concept_iri",
    "scheme_iri",
    "release_iri",
    "import_snapshot_id",
    "distribution_artifact_id",
    "source_property_iri",
    "label_role",
    "original_literal",
    "language_tag",
    "status",
    "expression_id",
    "migration_only",
)
CONCEPT_RELATION_COLUMNS = (
    "relation_id",
    "release_iri",
    "import_snapshot_id",
    "distribution_artifact_id",
    "subject_concept_iri",
    "subject_scheme_iri",
    "predicate_iri",
    "object_concept_iri",
    "object_scheme_iri",
    "source_property_or_path",
    "migration_only",
)
CONCEPT_EVENT_PARTICIPANT_COLUMNS = (
    "event_id",
    "operation",
    "participant_role",
    "concept_iri",
    "concept_kind",
    "release_iri",
    "complete_membership",
    "ordinal",
    "migration_only",
)

LABEL_ROLES = frozenset({"preferred", "alternate", "hidden"})
LABEL_STATUSES = frozenset({"current", "deprecated"})
HIERARCHY_PREDICATES = frozenset(
    {
        "http://www.w3.org/2004/02/skos/core#broader",
        "http://www.w3.org/2004/02/skos/core#narrower",
        "http://www.w3.org/2004/02/skos/core#related",
    }
)
LIFECYCLE_OPERATIONS = frozenset(
    {
        "deprecation",
        "withdrawal",
        "replacement",
        "split",
        "merge",
        "promotion",
        "demotion",
    }
)
LIFECYCLE_CARDINALITIES: Mapping[str, tuple[tuple[int, int | None], tuple[int, int | None]]] = {
    "deprecation": ((1, 1), (0, 0)),
    "withdrawal": ((1, 1), (0, 0)),
    "replacement": ((1, 1), (1, 1)),
    "promotion": ((1, 1), (1, 1)),
    "demotion": ((1, 1), (1, 1)),
    "split": ((1, 1), (2, None)),
    "merge": ((2, None), (1, 1)),
}

REQUIRED_IMPORT_FEATURES = frozenset(
    {
        "labels",
        "languages",
        "notation",
        "notes",
        "hierarchy",
        "mappings",
        "status",
        "replacements",
        "identifiers",
        "membership",
    }
)
MAPPING_IMPORT_REQUIRED_FEATURES = frozenset(
    {
        "mappings",
        "identifiers",
        "membership",
    }
)
ENRICHMENT_RESOURCE_ROUTES = frozenset(
    {
        "document",
        "participation",
        "container",
        "entity",
        "observation",
        "event",
        "externalReference",
    }
)
EVALUATION_GATE_DIMENSIONS = frozenset(
    {
        "stage",
        "source",
        "subtype",
        "facet",
        "role",
        "predicate",
        "privacy",
        "risk",
        "latency",
        "cost",
        "product",
    }
)
GOLD_PARTITION_DIMENSIONS = frozenset(
    {
        "conceptIdentity",
        "exactMatchCluster",
        "alias",
        "sourceIdentity",
        "artifactDigest",
        "textDigest",
        "nearDuplicateCluster",
    }
)
REF_RECORD_TYPES: Mapping[str, str] = {
    "enrichment-profile": "urn:ref:type:EnrichmentProfile",
    "output-profile": "urn:ref:type:OutputProfile",
    "registry-import-coverage-report": ("urn:ref:type:RegistryImportCoverageReport"),
    "indexed-vocabulary-expression": ("urn:ref:type:IndexedVocabularyExpression"),
    "registry-reconciliation-report": ("urn:ref:type:RegistryReconciliationReport"),
    "registry-deployment-decision": ("urn:ref:type:RegistryDeploymentDecision"),
    "sealed-gold-manifest": "urn:ref:type:SealedGoldManifest",
    "enrichment-configuration": "urn:ref:type:EnrichmentConfiguration",
    "enrichment-evaluation-result": ("urn:ref:type:EnrichmentEvaluationResult"),
    "enrichment-deployment-decision": ("urn:ref:type:EnrichmentDeploymentDecision"),
}

GovernanceAuthorizationValidator = Callable[[Mapping[str, Any]], bool]


class ReferenceRuntimeError(ValueError):
    """A record would violate the RefSpec reference-runtime boundary."""


def _require_text(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ReferenceRuntimeError(f"{label} is required")
    return text


def _require_iri(value: object, label: str) -> str:
    iri = _require_text(value, label)
    parsed = urlsplit(iri)
    if not parsed.scheme:
        raise ReferenceRuntimeError(f"{label} must be an absolute IRI")
    return iri


def _require_language_tag(value: object, label: str = "languageTag") -> str:
    tag = _require_text(value, label)
    if tag == "@none" or not _BCP47.fullmatch(tag):
        raise ReferenceRuntimeError(f"{label} must be a BCP 47 language tag")
    return tag


def _require_digest(value: object, label: str) -> str:
    digest = _require_text(value, label)
    if not _SHA256.fullmatch(digest):
        raise ReferenceRuntimeError(f"{label} must be sha256:<lowercase hex>")
    return digest


def _require_decimal(value: object, label: str) -> Decimal:
    text = _require_text(value, label)
    if not _CANONICAL_DECIMAL.fullmatch(text):
        raise ReferenceRuntimeError(f"{label} must be a canonical finite decimal string")
    try:
        decimal = Decimal(text)
    except InvalidOperation as exc:
        raise ReferenceRuntimeError(f"{label} must be a canonical finite decimal string") from exc
    if not decimal.is_finite():
        raise ReferenceRuntimeError(f"{label} must be finite")
    return decimal


def _require_datetime(value: object, label: str) -> str:
    text = _require_text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReferenceRuntimeError(f"{label} must be an ISO-8601 date-time") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReferenceRuntimeError(f"{label} must include an RFC 3339 timezone offset")
    return text


def _require_reference(
    value: Mapping[str, Any],
    label: str,
    *,
    versioned: bool,
) -> None:
    required = {"id", "digest"} | ({"version"} if versioned else set())
    _require_exact_fields(value, frozenset(required), label)
    _require_iri(value.get("id"), f"{label}.id")
    _require_digest(value.get("digest"), f"{label}.digest")
    if versioned:
        _require_text(value.get("version"), f"{label}.version")


def _require_component_pin(
    value: Mapping[str, Any],
    label: str,
) -> None:
    _require_exact_fields(
        value,
        frozenset({"id", "revision", "digest"}),
        label,
    )
    _require_iri(value.get("id"), f"{label}.id")
    _require_text(value.get("revision"), f"{label}.revision")
    _require_digest(value.get("digest"), f"{label}.digest")


def _record_base(
    *,
    record_id: str,
    record_type: str,
    recorded_at: str,
    recorded_by: str,
    operational_state: str,
) -> dict[str, Any]:
    return {
        "id": _require_iri(record_id, "id"),
        "type": _require_iri(record_type, "type"),
        "recordedAt": _require_datetime(recorded_at, "recordedAt"),
        "recordedBy": _require_iri(recorded_by, "recordedBy"),
        "schemaVersion": "1.0",
        "operationalState": _require_text(
            operational_state,
            "operationalState",
        ),
    }


def _json_copy(value: Any) -> Any:
    """Copy Python containers into JSON object/array container types."""
    if isinstance(value, Mapping):
        return {key: _json_copy(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_copy(child) for child in value]
    if isinstance(value, (set, frozenset)):
        return [_json_copy(child) for child in sorted(value, key=lambda item: canonical_json(item))]
    return value


def _assert_finite_json(value: object, path: str = "$") -> None:
    if value is None:
        raise ReferenceRuntimeError(f"{path} contains null; omit optional fields")
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > 9_007_199_254_740_991:
            raise ReferenceRuntimeError(f"{path} exceeds the interoperable JSON integer range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ReferenceRuntimeError(f"{path} contains a non-finite number")
        raise ReferenceRuntimeError(f"{path} contains a JSON float; use a canonical decimal string")
    if isinstance(value, str):
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ReferenceRuntimeError(f"{path} contains a non-string object key")
            _assert_finite_json(child, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _assert_finite_json(child, f"{path}[{index}]")
        return
    raise ReferenceRuntimeError(f"{path} contains unsupported JSON value {type(value).__name__}")


def canonical_payload_digest(
    payload: Mapping[str, Any],
    *,
    digest_field: str | None = None,
) -> str:
    """Hash the REF canonical JSON payload, omitting one top-level digest.

    The shared :func:`canonical_json` helper supplies UTF-8-safe sorted compact
    JSON.  REF adds the finite-number rule and the ``sha256:`` spelling.
    """
    resolved_digest_field = digest_field or (
        "contentDigest"
        if payload.get("type")
        in {
            "urn:ref:type:EnrichmentProfile",
            "urn:ref:type:OutputProfile",
        }
        else "canonicalPayloadDigest"
    )
    canonical_payload = {key: value for key, value in payload.items() if key != resolved_digest_field}
    _assert_finite_json(canonical_payload)
    encoded = canonical_json(canonical_payload).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def seal_payload(
    payload: Mapping[str, Any],
    *,
    digest_field: str | None = None,
) -> dict[str, Any]:
    """Return a copy carrying its canonical REF digest."""
    sealed = dict(payload)
    resolved_digest_field = digest_field or (
        "contentDigest"
        if payload.get("type")
        in {
            "urn:ref:type:EnrichmentProfile",
            "urn:ref:type:OutputProfile",
        }
        else "canonicalPayloadDigest"
    )
    sealed[resolved_digest_field] = canonical_payload_digest(
        sealed,
        digest_field=resolved_digest_field,
    )
    return sealed


def require_payload_digest(
    payload: Mapping[str, Any],
    *,
    digest_field: str | None = None,
) -> None:
    resolved_digest_field = digest_field or (
        "contentDigest"
        if payload.get("type")
        in {
            "urn:ref:type:EnrichmentProfile",
            "urn:ref:type:OutputProfile",
        }
        else "canonicalPayloadDigest"
    )
    expected = canonical_payload_digest(
        payload,
        digest_field=resolved_digest_field,
    )
    actual = _require_digest(
        payload.get(resolved_digest_field),
        resolved_digest_field,
    )
    if actual != expected:
        raise ReferenceRuntimeError(f"{resolved_digest_field} mismatch: expected {expected}, got {actual}")


def normalize_unicode_text(value: object) -> str:
    """Normalize search text without discarding non-ASCII characters."""
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return " ".join(normalized.split())


@dataclass(frozen=True)
class ConceptLabel:
    """One language-preserving authored SKOS label expression."""

    label_id: str
    concept_iri: str
    scheme_iri: str
    release_iri: str
    import_snapshot_id: str
    distribution_artifact_id: str
    source_property_iri: str
    label_role: str
    original_literal: str
    language_tag: str
    status: str = "current"
    expression_id: str | None = None
    migration_only: bool = False

    def __post_init__(self) -> None:
        _require_text(self.label_id, "label_id")
        _require_iri(self.concept_iri, "concept_iri")
        _require_iri(self.scheme_iri, "scheme_iri")
        _require_iri(self.release_iri, "release_iri")
        _require_text(self.import_snapshot_id, "import_snapshot_id")
        _require_text(
            self.distribution_artifact_id,
            "distribution_artifact_id",
        )
        _require_iri(self.source_property_iri, "source_property_iri")
        if self.label_role not in LABEL_ROLES:
            raise ReferenceRuntimeError(f"unknown label_role {self.label_role!r}")
        _require_text(self.original_literal, "original_literal")
        _require_language_tag(self.language_tag, "language_tag")
        if self.status not in LABEL_STATUSES:
            raise ReferenceRuntimeError(f"unknown label status {self.status!r}")
        if not isinstance(self.migration_only, bool):
            raise ReferenceRuntimeError("migration_only must be a boolean")

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConceptRelation:
    """One source-authored hierarchy edge; multiple parents are multiple rows."""

    relation_id: str
    release_iri: str
    import_snapshot_id: str
    distribution_artifact_id: str
    subject_concept_iri: str
    subject_scheme_iri: str
    predicate_iri: str
    object_concept_iri: str
    object_scheme_iri: str
    source_property_or_path: str
    migration_only: bool = False

    def __post_init__(self) -> None:
        _require_text(self.relation_id, "relation_id")
        _require_iri(self.release_iri, "release_iri")
        _require_text(self.import_snapshot_id, "import_snapshot_id")
        _require_text(
            self.distribution_artifact_id,
            "distribution_artifact_id",
        )
        _require_iri(self.subject_concept_iri, "subject_concept_iri")
        _require_iri(self.subject_scheme_iri, "subject_scheme_iri")
        predicate = _require_iri(self.predicate_iri, "predicate_iri")
        if predicate.startswith("skos:"):
            raise ReferenceRuntimeError(
                "predicate_iri must be an absolute IRI in the SKOS namespace, not a compact name"
            )
        if predicate not in HIERARCHY_PREDICATES:
            raise ReferenceRuntimeError("concept_relations may contain only source-authored hierarchy relations")
        _require_iri(self.object_concept_iri, "object_concept_iri")
        _require_iri(self.object_scheme_iri, "object_scheme_iri")
        _require_text(
            self.source_property_or_path,
            "source_property_or_path",
        )
        if self.subject_scheme_iri != self.object_scheme_iri:
            raise ReferenceRuntimeError("hierarchy relations must remain scheme-internal")
        if not isinstance(self.migration_only, bool):
            raise ReferenceRuntimeError("migration_only must be a boolean")

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConceptEventParticipant:
    """One predecessor or successor in a concept lifecycle event."""

    event_id: str
    operation: str
    participant_role: str
    concept_iri: str
    concept_kind: str
    release_iri: str
    complete_membership: bool
    ordinal: int
    migration_only: bool = False

    def __post_init__(self) -> None:
        _require_text(self.event_id, "event_id")
        if self.operation not in LIFECYCLE_OPERATIONS:
            raise ReferenceRuntimeError(f"unknown concept lifecycle operation {self.operation!r}")
        if self.participant_role not in {"predecessor", "successor"}:
            raise ReferenceRuntimeError("participant_role must be predecessor or successor")
        _require_iri(self.concept_iri, "concept_iri")
        if self.concept_kind not in {"local", "registered"}:
            raise ReferenceRuntimeError("concept_kind must be local or registered")
        _require_iri(self.release_iri, "release_iri")
        if self.complete_membership is not True:
            raise ReferenceRuntimeError("concept lifecycle release pins require complete membership")
        if not isinstance(self.ordinal, int) or isinstance(self.ordinal, bool) or self.ordinal < 0:
            raise ReferenceRuntimeError("ordinal must be a non-negative integer")
        if not isinstance(self.migration_only, bool):
            raise ReferenceRuntimeError("migration_only must be a boolean")

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def _in_range(
    count: int,
    bounds: tuple[int, int | None],
) -> bool:
    lower, upper = bounds
    return count >= lower and (upper is None or count <= upper)


def validate_lifecycle_participants(
    participants: Sequence[ConceptEventParticipant],
    *,
    release_membership: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Enforce lifecycle operation cardinality and release membership pins."""
    if participants and release_membership is None:
        raise ReferenceRuntimeError("conforming lifecycle validation requires exact release membership")
    for participant in participants:
        assert release_membership is not None
        membership = release_membership.get(participant.release_iri)
        if not isinstance(membership, Mapping):
            raise ReferenceRuntimeError(f"release {participant.release_iri!r} has no membership pin")
        _require_exact_fields(
            membership,
            frozenset({"completeMembership", "members"}),
            f"releaseMembership[{participant.release_iri}]",
        )
        if membership.get("completeMembership") is not True:
            raise ReferenceRuntimeError(f"release {participant.release_iri!r} is not complete-membership")
        members = membership.get("members")
        if not isinstance(members, (Sequence, set, frozenset)) or isinstance(
            members,
            (str, bytes),
        ):
            raise ReferenceRuntimeError(f"release {participant.release_iri!r} members must be an array")
        if participant.concept_iri not in members:
            raise ReferenceRuntimeError(
                f"concept {participant.concept_iri!r} is not a member of release {participant.release_iri!r}"
            )
    grouped: dict[str, list[ConceptEventParticipant]] = defaultdict(list)
    for participant in participants:
        grouped[participant.event_id].append(participant)
    for event_id, rows in grouped.items():
        operations = {row.operation for row in rows}
        if len(operations) != 1:
            raise ReferenceRuntimeError(f"event {event_id!r} has multiple lifecycle operations")
        operation = next(iter(operations))
        predecessor_bounds, successor_bounds = LIFECYCLE_CARDINALITIES[operation]
        predecessors = [row for row in rows if row.participant_role == "predecessor"]
        successors = [row for row in rows if row.participant_role == "successor"]
        if not _in_range(len(predecessors), predecessor_bounds) or not _in_range(
            len(successors),
            successor_bounds,
        ):
            raise ReferenceRuntimeError(f"event {event_id!r} violates {operation} participant cardinality")
        participant_iris = [row.concept_iri for row in rows]
        if len(set(participant_iris)) != len(participant_iris):
            raise ReferenceRuntimeError(f"event {event_id!r} contains a duplicate participant concept")
        predecessor_releases = {row.release_iri for row in predecessors}
        successor_releases = {row.release_iri for row in successors}
        if len(predecessor_releases) != 1:
            raise ReferenceRuntimeError(f"event {event_id!r} must pin exactly one predecessor release")
        if len(successor_releases) > 1:
            raise ReferenceRuntimeError(f"event {event_id!r} may pin at most one successor release")
        if successors and predecessor_releases == successor_releases:
            raise ReferenceRuntimeError(f"event {event_id!r} predecessor and successor releases must differ")
        if operation == "promotion" and (
            {row.concept_kind for row in predecessors} != {"local"}
            or {row.concept_kind for row in successors} != {"registered"}
        ):
            raise ReferenceRuntimeError(f"event {event_id!r} promotion must be local to registered")
        if operation == "demotion" and (
            {row.concept_kind for row in predecessors} != {"registered"}
            or {row.concept_kind for row in successors} != {"local"}
        ):
            raise ReferenceRuntimeError(f"event {event_id!r} demotion must be registered to local")
        for role_rows in (predecessors, successors):
            ordinals = [row.ordinal for row in role_rows]
            if sorted(ordinals) != list(range(len(role_rows))):
                raise ReferenceRuntimeError(f"event {event_id!r} participant ordinals are not contiguous")


def assert_conforming_vocabulary_rows(
    labels: Sequence[ConceptLabel],
    relations: Sequence[ConceptRelation],
    participants: Sequence[ConceptEventParticipant],
    *,
    release_membership: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Reject migration-only rows and structural data loss at the boundary."""
    all_rows: tuple[
        ConceptLabel | ConceptRelation | ConceptEventParticipant,
        ...,
    ] = (*labels, *relations, *participants)
    if any(row.migration_only for row in all_rows):
        raise ReferenceRuntimeError("legacy migration rows cannot be emitted as conforming output")
    label_ids = [row.label_id for row in labels]
    relation_ids = [row.relation_id for row in relations]
    if len(set(label_ids)) != len(label_ids):
        raise ReferenceRuntimeError("concept_labels contains duplicate label_id")
    if len(set(relation_ids)) != len(relation_ids):
        raise ReferenceRuntimeError("concept_relations contains duplicate relation_id")
    preferred_by_concept_language: set[tuple[str, str]] = set()
    label_values: set[tuple[str, str, str]] = set()
    for row in labels:
        preferred_key = (row.concept_iri, row.language_tag.casefold())
        if row.label_role == "preferred":
            if preferred_key in preferred_by_concept_language:
                raise ReferenceRuntimeError("a concept may have one preferred label per language")
            preferred_by_concept_language.add(preferred_key)
        collision_key = (
            row.concept_iri,
            row.language_tag.casefold(),
            row.original_literal,
        )
        if collision_key in label_values:
            raise ReferenceRuntimeError("preferred, alternate, and hidden label values must be disjoint")
        label_values.add(collision_key)
    if relations:
        if release_membership is None:
            raise ReferenceRuntimeError("conforming hierarchy validation requires exact release membership")
        concept_catalog: dict[
            str,
            set[tuple[str, str, str, str]],
        ] = defaultdict(set)
        for row in labels:
            concept_catalog[row.concept_iri].add(
                (
                    row.scheme_iri,
                    row.release_iri,
                    row.import_snapshot_id,
                    row.distribution_artifact_id,
                )
            )
        for relation in relations:
            record_key = (
                relation.subject_scheme_iri,
                relation.release_iri,
                relation.import_snapshot_id,
                relation.distribution_artifact_id,
            )
            if record_key not in concept_catalog.get(
                relation.subject_concept_iri,
                set(),
            ):
                raise ReferenceRuntimeError(
                    f"relation {relation.relation_id!r} subject does not resolve to an exact concept record"
                )
            target_key = (
                relation.object_scheme_iri,
                relation.release_iri,
                relation.import_snapshot_id,
                relation.distribution_artifact_id,
            )
            if target_key not in concept_catalog.get(
                relation.object_concept_iri,
                set(),
            ):
                raise ReferenceRuntimeError(
                    f"relation {relation.relation_id!r} target does not resolve to an exact concept record"
                )
            membership = release_membership.get(relation.release_iri)
            if not isinstance(membership, Mapping):
                raise ReferenceRuntimeError(f"relation {relation.relation_id!r} release has no membership pin")
            _require_exact_fields(
                membership,
                frozenset({"completeMembership", "members"}),
                f"releaseMembership[{relation.release_iri}]",
            )
            if membership.get("completeMembership") is not True:
                raise ReferenceRuntimeError(f"relation {relation.relation_id!r} release is not complete-membership")
            members = membership.get("members")
            if not isinstance(
                members,
                (Sequence, set, frozenset),
            ) or isinstance(members, (str, bytes)):
                raise ReferenceRuntimeError(f"relation {relation.relation_id!r} release members must be an array")
            for role, concept_iri in (
                ("subject", relation.subject_concept_iri),
                ("target", relation.object_concept_iri),
            ):
                if concept_iri not in members:
                    raise ReferenceRuntimeError(
                        f"relation {relation.relation_id!r} {role} is not a member of release {relation.release_iri!r}"
                    )
    validate_lifecycle_participants(
        participants,
        release_membership=release_membership,
    )


@dataclass(frozen=True)
class CoverageException:
    item_id: str
    stage: str
    count: int
    policy: Mapping[str, str]
    rationale: str

    def validate(self, *, exclusion: bool) -> None:
        del exclusion
        _require_iri(self.item_id, "coverage account item id")
        if self.stage not in {"parsing", "indexing"}:
            raise ReferenceRuntimeError("coverage account stage must be parsing or indexing")
        if not isinstance(self.count, int) or isinstance(self.count, bool) or self.count < 1:
            raise ReferenceRuntimeError("coverage exception count must be a positive integer")
        _require_reference(
            self.policy,
            "coverage account policy",
            versioned=True,
        )
        _require_text(self.rationale, "coverage account rationale")

    def payload(self) -> dict[str, Any]:
        return {
            "id": self.item_id,
            "stage": self.stage,
            "count": self.count,
            "policy": dict(self.policy),
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class ImportFeatureCoverage:
    """Source, parser, and index accounting for one vocabulary feature."""

    feature: str
    source_observed_count: int
    parsed_count: int
    indexed_count: int
    explicitly_excluded_count: int
    failed_count: int
    source_observed_digest: str
    parsed_digest: str
    indexed_digest: str
    exclusions: tuple[CoverageException, ...] = ()
    failures: tuple[CoverageException, ...] = ()
    required_for_candidate_or_output: bool = True
    parse_difference_explanation: str | None = None
    index_difference_explanation: str | None = None

    def validate(self) -> None:
        if self.feature not in REQUIRED_IMPORT_FEATURES:
            raise ReferenceRuntimeError(f"unknown registry-import feature {self.feature!r}")
        if not isinstance(self.required_for_candidate_or_output, bool):
            raise ReferenceRuntimeError(f"{self.feature}.requiredForCandidateOrOutput must be a boolean")
        counts = (
            self.source_observed_count,
            self.parsed_count,
            self.indexed_count,
            self.explicitly_excluded_count,
            self.failed_count,
        )
        if any(not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts):
            raise ReferenceRuntimeError(f"{self.feature}: coverage counts must be non-negative integers")
        if self.parsed_count > self.source_observed_count:
            raise ReferenceRuntimeError(f"{self.feature}: parsed count exceeds source count")
        if self.indexed_count > self.parsed_count:
            raise ReferenceRuntimeError(f"{self.feature}: indexed count exceeds parsed count")
        _require_digest(
            self.source_observed_digest,
            f"{self.feature}.sourceObservedDigest",
        )
        _require_digest(self.parsed_digest, f"{self.feature}.parsedDigest")
        _require_digest(self.indexed_digest, f"{self.feature}.indexedDigest")
        for item in self.exclusions:
            item.validate(exclusion=True)
        for item in self.failures:
            item.validate(exclusion=False)
        if sum(item.count for item in self.exclusions) != self.explicitly_excluded_count:
            raise ReferenceRuntimeError(f"{self.feature}: excluded count lacks itemized exclusions")
        if sum(item.count for item in self.failures) != self.failed_count:
            raise ReferenceRuntimeError(f"{self.feature}: failed count lacks itemized failures")
        parse_excluded = sum(item.count for item in self.exclusions if item.stage == "parsing")
        index_excluded = sum(item.count for item in self.exclusions if item.stage == "indexing")
        parse_failed = sum(item.count for item in self.failures if item.stage == "parsing")
        index_failed = sum(item.count for item in self.failures if item.stage == "indexing")
        if self.source_observed_count != (self.parsed_count + parse_excluded + parse_failed):
            raise ReferenceRuntimeError(f"{self.feature}: source-to-parsed counts do not reconcile")
        if self.parsed_count != (self.indexed_count + index_excluded + index_failed):
            raise ReferenceRuntimeError(f"{self.feature}: parsed-to-indexed counts do not reconcile")
        parse_differs = (
            self.source_observed_count != self.parsed_count or self.source_observed_digest != self.parsed_digest
        )
        index_differs = self.parsed_count != self.indexed_count or self.parsed_digest != self.indexed_digest
        if parse_differs and not str(self.parse_difference_explanation or "").strip():
            raise ReferenceRuntimeError(f"{self.feature}: source-to-parsed difference is unexplained")
        if index_differs and not str(self.index_difference_explanation or "").strip():
            raise ReferenceRuntimeError(f"{self.feature}: parsed-to-indexed difference is unexplained")
        if self.required_for_candidate_or_output and self.parsed_count > 0 and self.indexed_count != self.parsed_count:
            raise ReferenceRuntimeError(f"{self.feature}: a required parsed feature was not fully indexed")

    def payload(self) -> dict[str, Any]:
        self.validate()
        result: dict[str, Any] = {
            "feature": self.feature,
            "requiredForCandidateOrOutput": (self.required_for_candidate_or_output),
            "sourceObservedCount": self.source_observed_count,
            "parsedCount": self.parsed_count,
            "indexedCount": self.indexed_count,
            "excludedCount": self.explicitly_excluded_count,
            "failedCount": self.failed_count,
            "sourceObservedDigest": self.source_observed_digest,
            "parsedDigest": self.parsed_digest,
            "indexedDigest": self.indexed_digest,
            "exclusions": [item.payload() for item in self.exclusions],
            "failures": [item.payload() for item in self.failures],
        }
        if self.parse_difference_explanation is not None:
            result["parseDifferenceExplanation"] = _require_text(
                self.parse_difference_explanation,
                f"{self.feature}.parseDifferenceExplanation",
            )
        if self.index_difference_explanation is not None:
            result["indexDifferenceExplanation"] = _require_text(
                self.index_difference_explanation,
                f"{self.feature}.indexDifferenceExplanation",
            )
        return result


@dataclass(frozen=True)
class RegistryImportCoverageReport:
    report_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    output_profile: Mapping[str, str]
    import_snapshot: Mapping[str, str]
    reference_resource_release: Mapping[str, str]
    distribution_artifacts: tuple[Mapping[str, str], ...]
    import_profile: Mapping[str, str]
    parser_version: str
    index_snapshot: Mapping[str, str]
    activity: str
    receipt: str
    feature_rows: tuple[ImportFeatureCoverage, ...]
    report_status: str = "pass"

    def payload(self) -> dict[str, Any]:
        base = _record_base(
            record_id=self.report_id,
            record_type="urn:ref:type:RegistryImportCoverageReport",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_reference(
            self.output_profile,
            "outputProfile",
            versioned=True,
        )
        _require_reference(
            self.import_snapshot,
            "registryImportSnapshot",
            versioned=False,
        )
        _require_reference(
            self.reference_resource_release,
            "referenceResourceRelease",
            versioned=True,
        )
        if not self.distribution_artifacts:
            raise ReferenceRuntimeError("distributionArtifacts must not be empty")
        for value in self.distribution_artifacts:
            _require_reference(
                value,
                "distributionArtifact",
                versioned=False,
            )
        _require_reference(
            self.import_profile,
            "importProfile",
            versioned=True,
        )
        _require_text(self.parser_version, "parserVersion")
        _require_reference(
            self.index_snapshot,
            "indexSnapshot",
            versioned=False,
        )
        _require_iri(self.activity, "activity")
        _require_iri(self.receipt, "receipt")
        if self.report_status not in {"pass", "fail"}:
            raise ReferenceRuntimeError("reportStatus must be pass or fail")
        feature_names = [row.feature for row in self.feature_rows]
        if len(set(feature_names)) != len(feature_names):
            raise ReferenceRuntimeError("coverage report contains a duplicate feature row")
        missing = sorted(REQUIRED_IMPORT_FEATURES - set(feature_names))
        unexpected = sorted(set(feature_names) - REQUIRED_IMPORT_FEATURES)
        if missing or unexpected:
            raise ReferenceRuntimeError(
                f"coverage report feature set differs from the required set: missing={missing}, unexpected={unexpected}"
            )
        validation_errors: list[str] = []
        for row in self.feature_rows:
            try:
                row.validate()
            except ReferenceRuntimeError as exc:
                validation_errors.append(str(exc))
        has_failures = any(row.failed_count for row in self.feature_rows)
        if self.report_status == "pass" and (validation_errors or has_failures):
            detail = "; ".join(validation_errors) or "feature failures are present"
            raise ReferenceRuntimeError("a passing coverage report has unresolved loss: " + detail)
        return {
            **base,
            "outputProfile": dict(self.output_profile),
            "registryImportSnapshot": dict(self.import_snapshot),
            "referenceResourceRelease": dict(self.reference_resource_release),
            "distributionArtifacts": [dict(value) for value in self.distribution_artifacts],
            "importProfile": dict(self.import_profile),
            "parserVersion": self.parser_version,
            "indexSnapshot": dict(self.index_snapshot),
            "activity": self.activity,
            "receipt": self.receipt,
            "reportStatus": self.report_status,
            "features": [row.payload() for row in self.feature_rows],
        }

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload())


@dataclass(frozen=True)
class IndexedVocabularyExpression:
    expression_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    reference_resource_release: Mapping[str, str]
    registry_import_snapshot: Mapping[str, str]
    distribution_artifact: Mapping[str, str]
    scheme_iri: str
    member_iri: str
    source_property_or_path: str
    original_literal: str
    language_tag: str | None
    datatype_iri: str | None
    normalization_policy: Mapping[str, str]
    indexed_text: str
    indexed_text_digest: str
    indexed_representation_version: str
    index_snapshot: Mapping[str, str]
    activity: str
    receipt: str

    def payload(self) -> dict[str, Any]:
        base = _record_base(
            record_id=self.expression_id,
            record_type="urn:ref:type:IndexedVocabularyExpression",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_reference(
            self.reference_resource_release,
            "referenceResourceRelease",
            versioned=True,
        )
        _require_reference(
            self.registry_import_snapshot,
            "registryImportSnapshot",
            versioned=False,
        )
        _require_reference(
            self.distribution_artifact,
            "distributionArtifact",
            versioned=False,
        )
        _require_iri(self.scheme_iri, "scheme")
        _require_iri(self.member_iri, "member")
        source = _require_text(
            self.source_property_or_path,
            "sourcePropertyOrPath",
        )
        _require_text(self.original_literal, "originalLiteral")
        if (self.language_tag is None) == (self.datatype_iri is None):
            raise ReferenceRuntimeError("an indexed expression must carry exactly one of language or datatype")
        if self.language_tag is not None:
            _require_language_tag(self.language_tag)
        if self.datatype_iri is not None:
            _require_iri(self.datatype_iri, "datatype")
        _require_reference(
            self.normalization_policy,
            "normalizationPolicy",
            versioned=True,
        )
        policy = str(self.normalization_policy["id"])
        if policy in _LEGACY_NORMALIZATION_POLICIES or "ascii" in policy.casefold():
            raise ReferenceRuntimeError("ASCII-only normalization is not conforming")
        _require_text(self.indexed_text, "indexedText")
        expected_indexed_digest = canonical_text_digest(self.indexed_text)
        if self.indexed_text_digest != expected_indexed_digest:
            raise ReferenceRuntimeError("indexedTextDigest does not match")
        _require_text(
            self.indexed_representation_version,
            "indexedRepresentationVersion",
        )
        _require_reference(
            self.index_snapshot,
            "indexSnapshot",
            versioned=False,
        )
        _require_iri(self.activity, "activity")
        _require_iri(self.receipt, "receipt")
        expected_id = indexed_expression_id(
            reference_resource_release=self.reference_resource_release,
            registry_import_snapshot=self.registry_import_snapshot,
            distribution_artifact=self.distribution_artifact,
            scheme_iri=self.scheme_iri,
            member_iri=self.member_iri,
            source_property_or_path=self.source_property_or_path,
            original_literal=self.original_literal,
            language_tag=self.language_tag,
            datatype_iri=self.datatype_iri,
        )
        if self.expression_id != expected_id:
            raise ReferenceRuntimeError("indexed expression id does not match its exact source identity")
        result: dict[str, Any] = {
            **base,
            "referenceResourceRelease": dict(self.reference_resource_release),
            "registryImportSnapshot": dict(self.registry_import_snapshot),
            "distributionArtifact": dict(self.distribution_artifact),
            "scheme": self.scheme_iri,
            "member": self.member_iri,
            "originalLiteral": self.original_literal,
            "normalizationPolicy": dict(self.normalization_policy),
            "indexedText": self.indexed_text,
            "indexedTextDigest": self.indexed_text_digest,
            "indexedRepresentationVersion": self.indexed_representation_version,
            "indexSnapshot": dict(self.index_snapshot),
            "activity": self.activity,
            "receipt": self.receipt,
        }
        if urlsplit(source).scheme:
            result["sourceProperty"] = source
        else:
            result["sourcePath"] = source
        if self.language_tag is not None:
            result["language"] = self.language_tag
        if self.datatype_iri is not None:
            result["datatype"] = self.datatype_iri
        return result

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload())


def canonical_text_digest(value: str) -> str:
    """Digest exact UTF-8 text without JSON quoting."""
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def indexed_expression_id(
    *,
    reference_resource_release: Mapping[str, str],
    registry_import_snapshot: Mapping[str, str],
    distribution_artifact: Mapping[str, str],
    scheme_iri: str,
    member_iri: str,
    source_property_or_path: str,
    original_literal: str,
    language_tag: str | None,
    datatype_iri: str | None,
) -> str:
    """Mint the identity from every source distinction REF-CAND-009 keeps."""
    identity = {
        "referenceResourceRelease": dict(reference_resource_release),
        "registryImportSnapshot": dict(registry_import_snapshot),
        "distributionArtifact": dict(distribution_artifact),
        "scheme": scheme_iri,
        "member": member_iri,
        "sourcePropertyOrPath": source_property_or_path,
        "originalLiteral": original_literal,
    }
    if language_tag is not None:
        identity["language"] = language_tag
    if datatype_iri is not None:
        identity["datatype"] = datatype_iri
    _assert_finite_json(identity)
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return f"urn:ref:indexed-expression:{digest}"


@dataclass(frozen=True)
class RegistryReconciliationReport:
    report_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    inputs: tuple[Mapping[str, Any], ...]
    compared_items: tuple[Mapping[str, str], ...]
    differences: tuple[Mapping[str, Any], ...]
    concept_mappings: tuple[str, ...]
    precedence_policy: Mapping[str, str]
    rulespec_authority_refs: tuple[str, ...]
    attestation_refs: tuple[str, ...]
    local_adoption_refs: tuple[str, ...]
    authorization_validations: tuple[Mapping[str, Any], ...]
    unresolved_items: tuple[str, ...]
    activity: str
    outcome: str
    selected_input_release: Mapping[str, str] | None = None
    reconciled_release: Mapping[str, str] | None = None

    def payload(
        self,
        *,
        governance_validator: GovernanceAuthorizationValidator | None = None,
    ) -> dict[str, Any]:
        base = _record_base(
            record_id=self.report_id,
            record_type="urn:ref:type:RegistryReconciliationReport",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        if len(self.inputs) < 2:
            raise ReferenceRuntimeError("a reconciliation report requires at least two exact inputs")
        input_ids: set[str] = set()
        for index, value in enumerate(self.inputs):
            label = f"inputs[{index}]"
            _require_exact_fields(
                value,
                frozenset(
                    {
                        "id",
                        "referenceResourceRelease",
                        "distributionArtifacts",
                        "registryImportSnapshot",
                        "stageDigests",
                    }
                ),
                label,
            )
            input_id = _require_iri(value["id"], f"{label}.id")
            if input_id in input_ids:
                raise ReferenceRuntimeError(f"{label}.id duplicates an earlier reconciliation input")
            input_ids.add(input_id)
            _require_reference(
                value["referenceResourceRelease"],
                f"{label}.referenceResourceRelease",
                versioned=True,
            )
            distributions = value["distributionArtifacts"]
            stages = value["stageDigests"]
            if not isinstance(distributions, Sequence) or not distributions:
                raise ReferenceRuntimeError(f"{label}.distributionArtifacts must not be empty")
            if not isinstance(stages, Sequence) or not stages:
                raise ReferenceRuntimeError(f"{label}.stageDigests must not be empty")
            for item in distributions:
                _require_reference(
                    item,
                    f"{label}.distributionArtifact",
                    versioned=False,
                )
            _require_reference(
                value["registryImportSnapshot"],
                f"{label}.registryImportSnapshot",
                versioned=False,
            )
            for item in stages:
                _require_reference(
                    item,
                    f"{label}.stageDigest",
                    versioned=False,
                )
        if not self.compared_items or not self.differences:
            raise ReferenceRuntimeError("reconciliation must record compared items and differences")
        for index, item in enumerate(self.compared_items):
            _require_exact_fields(
                item,
                frozenset({"kind", "left", "right"}),
                f"comparedItems[{index}]",
            )
            if item["kind"] not in {
                "field",
                "member",
                "relation",
                "stageDigest",
            }:
                raise ReferenceRuntimeError("unknown compared-item kind")
            _require_text(item["left"], "compared item left")
            _require_text(item["right"], "compared item right")
        for index, difference in enumerate(self.differences):
            _require_exact_fields(
                difference,
                frozenset(
                    {
                        "id",
                        "kind",
                        "inputRefs",
                        "description",
                        "resolution",
                    }
                ),
                f"differences[{index}]",
            )
            _require_iri(difference["id"], "difference id")
            if difference["kind"] not in {
                "field",
                "member",
                "relation",
                "stageDigest",
            }:
                raise ReferenceRuntimeError("unknown difference kind")
            input_refs = difference["inputRefs"]
            if not isinstance(input_refs, Sequence) or len(input_refs) < 2:
                raise ReferenceRuntimeError("a reconciliation difference requires two input refs")
            resolved_input_refs = {_require_iri(value, "difference input ref") for value in input_refs}
            if len(resolved_input_refs) != len(input_refs):
                raise ReferenceRuntimeError("difference input refs must be unique")
            if not resolved_input_refs.issubset(input_ids):
                raise ReferenceRuntimeError("difference input refs must name exact reconciliation input identifiers")
            _require_text(difference["description"], "difference description")
            if difference["resolution"] not in {
                "selectedInput",
                "mapped",
                "reconciled",
                "unresolved",
            }:
                raise ReferenceRuntimeError("unknown difference resolution")
        difference_ids = [str(item["id"]) for item in self.differences]
        if len(set(difference_ids)) != len(difference_ids):
            raise ReferenceRuntimeError("reconciliation difference identifiers must be unique")
        expected_unresolved = {str(item["id"]) for item in self.differences if item["resolution"] == "unresolved"}
        if set(self.unresolved_items) != expected_unresolved:
            raise ReferenceRuntimeError("unresolved_items must exactly name unresolved differences")
        for value in self.concept_mappings:
            _require_iri(value, "conceptMapping")
        _require_reference(
            self.precedence_policy,
            "precedencePolicy",
            versioned=True,
        )
        for label, values in (
            ("rulespecAuthorityRefs", self.rulespec_authority_refs),
            ("attestationRefs", self.attestation_refs),
            ("localAdoptionRefs", self.local_adoption_refs),
        ):
            if not values:
                raise ReferenceRuntimeError(f"{label} must not be empty")
            for value in values:
                _require_iri(value, label)
        expected_authorizations = {
            **{value: "rulespecAuthority" for value in self.rulespec_authority_refs},
            **{value: "rulespecAttestation" for value in self.attestation_refs},
            **{value: "localAdoption" for value in self.local_adoption_refs},
        }
        if len(expected_authorizations) != (
            len(self.rulespec_authority_refs) + len(self.attestation_refs) + len(self.local_adoption_refs)
        ):
            raise ReferenceRuntimeError("governance authorization references must be distinct")
        validation_fields = frozenset(
            {
                "authorizationRef",
                "kind",
                "validationReceipt",
                "validator",
                "validatedAt",
                "effective",
            }
        )
        validated_authorizations: dict[str, str] = {}
        for index, raw in enumerate(self.authorization_validations):
            label = f"authorizationValidations[{index}]"
            value = _require_mapping(raw, label)
            _require_exact_fields(value, validation_fields, label)
            authorization_ref = _require_iri(
                value["authorizationRef"],
                f"{label}.authorizationRef",
            )
            kind = str(value["kind"])
            if kind not in {
                "rulespecAuthority",
                "rulespecAttestation",
                "localAdoption",
            }:
                raise ReferenceRuntimeError(f"{label}.kind is not a Rulespec governance kind")
            _require_reference(
                _require_mapping(
                    value["validationReceipt"],
                    f"{label}.validationReceipt",
                ),
                f"{label}.validationReceipt",
                versioned=False,
            )
            _require_iri(value["validator"], f"{label}.validator")
            _require_datetime(value["validatedAt"], f"{label}.validatedAt")
            if value["effective"] is not True:
                raise ReferenceRuntimeError(f"{label}.effective must be true")
            if authorization_ref in validated_authorizations:
                raise ReferenceRuntimeError("authorizationValidations contains a duplicate reference")
            validated_authorizations[authorization_ref] = kind
        if validated_authorizations != expected_authorizations:
            raise ReferenceRuntimeError(
                "authorizationValidations must exactly validate every "
                "Rulespec authority, attestation, and local adoption"
            )
        if self.outcome not in {
            "selectedInput",
            "reconciledReleaseAuthorized",
            "unresolved",
        }:
            raise ReferenceRuntimeError("unknown reconciliation outcome")
        if self.outcome == "unresolved":
            if not self.unresolved_items:
                raise ReferenceRuntimeError("an unresolved reconciliation must name unresolved items")
            if self.reconciled_release is not None:
                raise ReferenceRuntimeError("an unresolved report cannot authorize a synthesized union")
            if self.selected_input_release is not None:
                raise ReferenceRuntimeError("an unresolved report cannot select an input release")
        else:
            if self.unresolved_items:
                raise ReferenceRuntimeError("a resolved reconciliation cannot retain unresolved items")
        if self.outcome == "reconciledReleaseAuthorized":
            if self.reconciled_release is None:
                raise ReferenceRuntimeError("reconciledReleaseAuthorized requires reconciledRelease")
            _require_reference(
                self.reconciled_release,
                "reconciledRelease",
                versioned=True,
            )
            input_releases = [dict(value["referenceResourceRelease"]) for value in self.inputs]
            if dict(self.reconciled_release) in input_releases:
                raise ReferenceRuntimeError("a reconciled release must differ from every input release")
            if self.selected_input_release is not None:
                raise ReferenceRuntimeError("a reconciled release outcome cannot select an input")
            if governance_validator is None:
                raise ReferenceRuntimeError(
                    "a synthesized union requires externally validated Rulespec governance authorization"
                )
            authorization_request = {
                "reconciliationReport": self.report_id,
                "inputIds": sorted(input_ids),
                "reconciledRelease": dict(self.reconciled_release),
                "precedencePolicy": dict(self.precedence_policy),
                "rulespecAuthorityRefs": list(self.rulespec_authority_refs),
                "attestationRefs": list(self.attestation_refs),
                "localAdoptionRefs": list(self.local_adoption_refs),
                "authorizationValidations": [_json_copy(item) for item in self.authorization_validations],
            }
            try:
                authorized = governance_validator(authorization_request)
            except Exception as exc:
                raise ReferenceRuntimeError("Rulespec governance authorization validation failed") from exc
            if authorized is not True:
                raise ReferenceRuntimeError("Rulespec governance did not authorize the synthesized union")
        elif self.reconciled_release is not None:
            raise ReferenceRuntimeError("only reconciledReleaseAuthorized may name a reconciled release")
        if self.outcome == "selectedInput":
            if self.selected_input_release is None:
                raise ReferenceRuntimeError("selectedInput requires selectedInputRelease")
            _require_reference(
                self.selected_input_release,
                "selectedInputRelease",
                versioned=True,
            )
            input_releases = [dict(value["referenceResourceRelease"]) for value in self.inputs]
            if dict(self.selected_input_release) not in input_releases:
                raise ReferenceRuntimeError("selectedInputRelease must be one exact input release")
        elif self.selected_input_release is not None:
            raise ReferenceRuntimeError("only selectedInput may name selectedInputRelease")
        _require_iri(self.activity, "activity")
        result: dict[str, Any] = {
            **base,
            "inputs": [_json_copy(item) for item in self.inputs],
            "comparedItems": [dict(item) for item in self.compared_items],
            "differences": [_json_copy(item) for item in self.differences],
            "conceptMappings": list(self.concept_mappings),
            "precedencePolicy": dict(self.precedence_policy),
            "rulespecAuthorityRefs": list(self.rulespec_authority_refs),
            "attestationRefs": list(self.attestation_refs),
            "localAdoptionRefs": list(self.local_adoption_refs),
            "authorizationValidations": [_json_copy(item) for item in self.authorization_validations],
            "unresolvedItems": list(self.unresolved_items),
            "synthesizedUnionAuthorized": (self.outcome == "reconciledReleaseAuthorized"),
            "activity": self.activity,
            "outcome": self.outcome,
        }
        if self.reconciled_release is not None:
            result["reconciledRelease"] = dict(self.reconciled_release)
        if self.selected_input_release is not None:
            result["selectedInputRelease"] = dict(self.selected_input_release)
        return result

    def sealed_payload(
        self,
        *,
        governance_validator: GovernanceAuthorizationValidator | None = None,
    ) -> dict[str, Any]:
        return seal_payload(self.payload(governance_validator=governance_validator))


@dataclass(frozen=True)
class RegistryDeploymentDecision:
    """Select one exact, coverage-checked registry import for an environment."""

    decision_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    environment: Mapping[str, str]
    registry_import_snapshot: Mapping[str, str]
    reference_resource_release: Mapping[str, str]
    coverage_report: Mapping[str, str]
    output_profile: Mapping[str, str]
    selection_state: str
    effective_at: str
    reason: str
    activity: str
    rulespec_attestation_refs: tuple[str, ...]
    local_adoption_refs: tuple[str, ...]
    authorization_validations: tuple[Mapping[str, Any], ...]
    reconciliation_report: Mapping[str, str] | None = None
    predecessor: Mapping[str, str] | None = None

    def payload(
        self,
        *,
        coverage_report_record: Mapping[str, Any] | None = None,
        output_profile_record: Mapping[str, Any] | None = None,
        reconciliation_report_record: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        base = _record_base(
            record_id=self.decision_id,
            record_type="urn:ref:type:RegistryDeploymentDecision",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_exact_fields(
            self.environment,
            frozenset({"id", "classification"}),
            "environment",
        )
        _require_iri(self.environment.get("id"), "environment.id")
        if self.environment.get("classification") not in {
            "development",
            "staging",
            "production",
        }:
            raise ReferenceRuntimeError("environment.classification is invalid")
        _require_reference(
            self.registry_import_snapshot,
            "registryImportSnapshot",
            versioned=False,
        )
        _require_reference(
            self.reference_resource_release,
            "referenceResourceRelease",
            versioned=True,
        )
        _require_reference(
            self.coverage_report,
            "coverageReport",
            versioned=False,
        )
        _require_reference(
            self.output_profile,
            "outputProfile",
            versioned=True,
        )
        if self.reconciliation_report is not None:
            _require_reference(
                self.reconciliation_report,
                "reconciliationReport",
                versioned=False,
            )
        if self.predecessor is not None:
            _require_reference(
                self.predecessor,
                "predecessor",
                versioned=False,
            )
        if self.selection_state not in {
            "quarantined",
            "staged",
            "selected",
            "deselected",
            "failed",
        }:
            raise ReferenceRuntimeError("unknown registry deployment selection state")

        attestation_refs = tuple(
            _require_iri(value, "rulespecAttestationRef") for value in self.rulespec_attestation_refs
        )
        adoption_refs = tuple(_require_iri(value, "localAdoptionRef") for value in self.local_adoption_refs)
        if len(set((*attestation_refs, *adoption_refs))) != (len(attestation_refs) + len(adoption_refs)):
            raise ReferenceRuntimeError("registry deployment authorization references must be distinct")
        expected_authorizations = {
            **{value: "rulespecAttestation" for value in attestation_refs},
            **{value: "localAdoption" for value in adoption_refs},
        }
        if len(self.authorization_validations) < 2:
            raise ReferenceRuntimeError("registry deployment requires at least two authorization validations")
        validation_fields = frozenset(
            {
                "authorizationRef",
                "kind",
                "validationReceipt",
                "validator",
                "validatedAt",
                "effective",
            }
        )
        validated_authorizations: dict[str, str] = {}
        for index, raw in enumerate(self.authorization_validations):
            label = f"authorizationValidations[{index}]"
            value = _require_mapping(raw, label)
            _require_exact_fields(value, validation_fields, label)
            authorization_ref = _require_iri(
                value["authorizationRef"],
                f"{label}.authorizationRef",
            )
            kind = str(value["kind"])
            if kind not in {"rulespecAttestation", "localAdoption"}:
                raise ReferenceRuntimeError(f"{label}.kind is not a registry governance kind")
            _require_reference(
                _require_mapping(
                    value["validationReceipt"],
                    f"{label}.validationReceipt",
                ),
                f"{label}.validationReceipt",
                versioned=False,
            )
            _require_iri(value["validator"], f"{label}.validator")
            _require_datetime(value["validatedAt"], f"{label}.validatedAt")
            if value["effective"] is not True:
                raise ReferenceRuntimeError(f"{label}.effective must be true")
            if authorization_ref in validated_authorizations:
                raise ReferenceRuntimeError("authorizationValidations contains a duplicate reference")
            validated_authorizations[authorization_ref] = kind
        if validated_authorizations != expected_authorizations:
            raise ReferenceRuntimeError(
                "authorizationValidations must exactly validate every registry attestation and local adoption"
            )

        selected = self.selection_state == "selected"
        if coverage_report_record is None or output_profile_record is None:
            raise ReferenceRuntimeError(
                "registry deployment requires supplied exact coverage and OutputProfile records"
            )
        if selected and self.environment["classification"] == "production":
            if not attestation_refs or not adoption_refs:
                raise ReferenceRuntimeError("selected production registry requires attestation and adoption references")

        if coverage_report_record is not None:
            require_payload_digest(coverage_report_record)
            if (
                coverage_report_record.get("type") != "urn:ref:type:RegistryImportCoverageReport"
                or coverage_report_record.get("id") != self.coverage_report["id"]
                or coverage_report_record.get("canonicalPayloadDigest") != self.coverage_report["digest"]
            ):
                raise ReferenceRuntimeError("registry deployment coverage-report pin mismatch")
            if (
                dict(
                    _require_mapping(
                        coverage_report_record.get("registryImportSnapshot"),
                        "coverage.registryImportSnapshot",
                    )
                )
                != dict(self.registry_import_snapshot)
                or dict(
                    _require_mapping(
                        coverage_report_record.get("referenceResourceRelease"),
                        "coverage.referenceResourceRelease",
                    )
                )
                != dict(self.reference_resource_release)
                or dict(
                    _require_mapping(
                        coverage_report_record.get("outputProfile"),
                        "coverage.outputProfile",
                    )
                )
                != dict(self.output_profile)
            ):
                raise ReferenceRuntimeError("registry deployment pins differ from its coverage report")
            if selected and coverage_report_record.get("reportStatus") != "pass":
                raise ReferenceRuntimeError("selected registry deployment requires passing import coverage")

        if output_profile_record is not None:
            content_digest = output_profile_record.get("contentDigest")
            if (
                output_profile_record.get("type") != "urn:ref:type:OutputProfile"
                or output_profile_record.get("id") != self.output_profile["id"]
                or output_profile_record.get("version") != self.output_profile["version"]
                or content_digest != self.output_profile["digest"]
            ):
                raise ReferenceRuntimeError("registry deployment OutputProfile pin mismatch")
            require_payload_digest(
                output_profile_record,
                digest_field="contentDigest",
            )

        if self.reconciliation_report is not None and reconciliation_report_record is None:
            raise ReferenceRuntimeError("registry deployment requires its exact reconciliation record")
        if reconciliation_report_record is not None:
            require_payload_digest(reconciliation_report_record)
            if self.reconciliation_report is None or (
                reconciliation_report_record.get("type") != "urn:ref:type:RegistryReconciliationReport"
                or reconciliation_report_record.get("id") != self.reconciliation_report["id"]
                or reconciliation_report_record.get("canonicalPayloadDigest") != self.reconciliation_report["digest"]
            ):
                raise ReferenceRuntimeError("registry deployment reconciliation-report pin mismatch")
            if selected:
                outcome = reconciliation_report_record.get("outcome")
                if outcome == "unresolved":
                    raise ReferenceRuntimeError("selected registry deployment cannot use unresolved reconciliation")
                authorized_release = (
                    reconciliation_report_record.get("selectedInputRelease")
                    if outcome == "selectedInput"
                    else reconciliation_report_record.get("reconciledRelease")
                    if outcome == "reconciledReleaseAuthorized"
                    else None
                )
                if authorized_release != dict(self.reference_resource_release):
                    raise ReferenceRuntimeError("selected registry release is not authorized by reconciliation")

        _require_datetime(self.effective_at, "effectiveAt")
        _require_text(self.reason, "reason")
        _require_iri(self.activity, "activity")
        result: dict[str, Any] = {
            **base,
            "environment": dict(self.environment),
            "registryImportSnapshot": dict(self.registry_import_snapshot),
            "referenceResourceRelease": dict(self.reference_resource_release),
            "coverageReport": dict(self.coverage_report),
            "outputProfile": dict(self.output_profile),
            "selectionState": self.selection_state,
            "effectiveAt": self.effective_at,
            "reason": self.reason,
            "activity": self.activity,
            "rulespecAttestationRefs": list(attestation_refs),
            "localAdoptionRefs": list(adoption_refs),
            "authorizationValidations": [_json_copy(value) for value in self.authorization_validations],
        }
        if self.reconciliation_report is not None:
            result["reconciliationReport"] = dict(self.reconciliation_report)
        if self.predecessor is not None:
            result["predecessor"] = dict(self.predecessor)
        return result

    def sealed_payload(
        self,
        *,
        coverage_report_record: Mapping[str, Any] | None = None,
        output_profile_record: Mapping[str, Any] | None = None,
        reconciliation_report_record: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return seal_payload(
            self.payload(
                coverage_report_record=coverage_report_record,
                output_profile_record=output_profile_record,
                reconciliation_report_record=(reconciliation_report_record),
            )
        )


def _validate_permission_use(row: Mapping[str, Any], label: str) -> None:
    if not isinstance(row.get("candidateUse"), bool):
        raise ReferenceRuntimeError(f"{label}.candidateUse must be a boolean")
    if not isinstance(row.get("acceptedOutputUse"), bool):
        raise ReferenceRuntimeError(f"{label}.acceptedOutputUse must be a boolean")
    if row["acceptedOutputUse"] and not row["candidateUse"]:
        raise ReferenceRuntimeError(f"{label}: accepted-output permission requires candidate permission")


def _require_exact_fields(
    row: Mapping[str, Any],
    required: frozenset[str],
    label: str,
) -> None:
    missing = sorted(required - set(row))
    extra = sorted(set(row) - required)
    if missing or extra:
        raise ReferenceRuntimeError(f"{label} must be one complete permission row; missing={missing}, extra={extra}")


def _require_unique_json_values(
    values: Sequence[Any],
    label: str,
) -> None:
    serialized = [canonical_json(_json_copy(value)) for value in values]
    if len(set(serialized)) != len(serialized):
        raise ReferenceRuntimeError(f"{label} must contain unique values")


def _require_nonempty_string_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise ReferenceRuntimeError(f"{label} must be a non-empty array")
    result = tuple(_require_text(item, f"{label} item") for item in value)
    if len(set(result)) != len(result):
        raise ReferenceRuntimeError(f"{label} must contain unique values")
    return result


@dataclass(frozen=True)
class EnrichmentProfile:
    """Immutable facet definitions used to validate every permission tuple."""

    profile_id: str
    version: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    facets: tuple[Mapping[str, Any], ...]

    def payload(self) -> dict[str, Any]:
        base = _record_base(
            record_id=self.profile_id,
            record_type="urn:ref:type:EnrichmentProfile",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_text(self.version, "version")
        if not self.facets:
            raise ReferenceRuntimeError("EnrichmentProfile.facets must not be empty")
        facet_fields = frozenset(
            {
                "iri",
                "label",
                "definition",
                "inclusionCues",
                "exclusionCues",
                "compatibleResourceRoutes",
                "compatibleAssignmentPredicates",
            }
        )
        payload_facets: list[dict[str, Any]] = []
        facet_iris: list[str] = []
        for index, facet in enumerate(self.facets):
            label = f"facets[{index}]"
            if not isinstance(facet, Mapping) or not facet:
                raise ReferenceRuntimeError(f"{label} must be a non-empty facet record")
            _require_exact_fields(facet, facet_fields, label)
            facet_iri = _require_iri(facet["iri"], f"{label}.iri")
            facet_iris.append(facet_iri)
            _require_text(facet["label"], f"{label}.label")
            _require_text(facet["definition"], f"{label}.definition")
            _require_nonempty_string_list(
                facet["inclusionCues"],
                f"{label}.inclusionCues",
            )
            _require_nonempty_string_list(
                facet["exclusionCues"],
                f"{label}.exclusionCues",
            )
            routes = _require_nonempty_string_list(
                facet["compatibleResourceRoutes"],
                f"{label}.compatibleResourceRoutes",
            )
            unknown_routes = sorted(set(routes) - ENRICHMENT_RESOURCE_ROUTES)
            if unknown_routes:
                raise ReferenceRuntimeError(f"{label} contains unknown resource routes: " + ", ".join(unknown_routes))
            predicates = _require_nonempty_string_list(
                facet["compatibleAssignmentPredicates"],
                f"{label}.compatibleAssignmentPredicates",
            )
            for predicate in predicates:
                _require_iri(
                    predicate,
                    f"{label}.compatibleAssignmentPredicate",
                )
            payload_facets.append(_json_copy(facet))
        if len(set(facet_iris)) != len(facet_iris):
            raise ReferenceRuntimeError("EnrichmentProfile contains a duplicate facet IRI")
        return {
            **base,
            "version": self.version,
            "facets": payload_facets,
        }

    @property
    def content_digest(self) -> str:
        return canonical_payload_digest(
            self.payload(),
            digest_field="contentDigest",
        )

    @property
    def reference(self) -> Mapping[str, str]:
        return {
            "id": self.profile_id,
            "version": self.version,
            "digest": self.content_digest,
        }

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload(), digest_field="contentDigest")

    def require_compatible(
        self,
        *,
        facet: str,
        assignment_role: str,
        resource_route: str | None,
    ) -> None:
        """Require one exact facet whose role and optional route are allowed."""
        self.payload()
        matching = [row for row in self.facets if row["iri"] == facet]
        if len(matching) != 1:
            raise ReferenceRuntimeError(f"facet {facet!r} is not defined by the EnrichmentProfile")
        row = matching[0]
        if assignment_role not in row["compatibleAssignmentPredicates"]:
            raise ReferenceRuntimeError(f"assignment role {assignment_role!r} is incompatible with facet {facet!r}")
        if resource_route is not None:
            if resource_route not in ENRICHMENT_RESOURCE_ROUTES:
                raise ReferenceRuntimeError(f"unknown REF resource route {resource_route!r}")
            if resource_route not in row["compatibleResourceRoutes"]:
                raise ReferenceRuntimeError(f"resource route {resource_route!r} is incompatible with facet {facet!r}")


@dataclass(frozen=True)
class OutputProfile:
    """Immutable complete permission tuples for candidate and output use."""

    profile_id: str
    version: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    enrichment_profile: Mapping[str, str]
    acceptance_policies: tuple[Mapping[str, str], ...]
    publication_views: tuple[Mapping[str, str], ...]
    release_permissions: tuple[Mapping[str, Any], ...] = ()
    mapping_permissions: tuple[Mapping[str, Any], ...] = ()
    open_label_permissions: tuple[Mapping[str, Any], ...] = ()
    enrichment_profile_record: EnrichmentProfile | None = None

    def _resolved_enrichment_profile(self) -> EnrichmentProfile:
        profile = self.enrichment_profile_record
        if profile is None:
            raise ReferenceRuntimeError("OutputProfile requires the exact EnrichmentProfile record")
        if dict(self.enrichment_profile) != dict(profile.reference):
            raise ReferenceRuntimeError("OutputProfile EnrichmentProfile pin does not match the resolved record")
        return profile

    def payload(self) -> dict[str, Any]:
        base = _record_base(
            record_id=self.profile_id,
            record_type="urn:ref:type:OutputProfile",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_text(self.version, "version")
        _require_reference(
            self.enrichment_profile,
            "enrichmentProfile",
            versioned=True,
        )
        enrichment_profile = self._resolved_enrichment_profile()
        if not self.acceptance_policies or not self.publication_views:
            raise ReferenceRuntimeError("OutputProfile requires acceptance policies and publication views")
        for index, value in enumerate(self.acceptance_policies):
            _require_reference(
                value,
                f"acceptancePolicies[{index}]",
                versioned=True,
            )
        for index, value in enumerate(self.publication_views):
            _require_reference(
                value,
                f"publicationViews[{index}]",
                versioned=True,
            )
        release_fields = frozenset(
            {
                "facet",
                "assignmentRole",
                "referenceResourceRelease",
                "registryImportSnapshot",
                "requiredImportFeatures",
                "candidateUse",
                "acceptedOutputUse",
            }
        )
        mapping_fields = frozenset(
            {
                "facet",
                "assignmentRole",
                "mappingSnapshot",
                "sourceRelease",
                "targetRelease",
                "relation",
                "direction",
                "candidateUse",
                "acceptedOutputUse",
            }
        )
        open_fields = frozenset(
            {
                "facet",
                "assignmentRole",
                "mode",
                "candidateUse",
                "acceptedOutputUse",
            }
        )
        open_default_fields = open_fields | {"defaultLanguage"}
        all_rows: list[tuple[str, Mapping[str, Any]]] = []
        for index, row in enumerate(self.release_permissions):
            label = f"releasePermissions[{index}]"
            _require_exact_fields(row, release_fields, label)
            _validate_permission_use(row, label)
            _require_iri(row["facet"], f"{label}.facet")
            _require_iri(row["assignmentRole"], f"{label}.assignmentRole")
            enrichment_profile.require_compatible(
                facet=str(row["facet"]),
                assignment_role=str(row["assignmentRole"]),
                resource_route=None,
            )
            _require_reference(
                row["referenceResourceRelease"],
                f"{label}.referenceResourceRelease",
                versioned=True,
            )
            _require_reference(
                row["registryImportSnapshot"],
                f"{label}.registryImportSnapshot",
                versioned=False,
            )
            required_features = _require_nonempty_string_list(
                row["requiredImportFeatures"],
                f"{label}.requiredImportFeatures",
            )
            unknown_features = sorted(set(required_features) - REQUIRED_IMPORT_FEATURES)
            if unknown_features:
                raise ReferenceRuntimeError(
                    f"{label} contains unknown required import features: " + ", ".join(unknown_features)
                )
            all_rows.append(("release", row))
        for index, row in enumerate(self.mapping_permissions):
            label = f"mappingPermissions[{index}]"
            _require_exact_fields(row, mapping_fields, label)
            _validate_permission_use(row, label)
            _require_iri(row["facet"], f"{label}.facet")
            _require_iri(row["assignmentRole"], f"{label}.assignmentRole")
            enrichment_profile.require_compatible(
                facet=str(row["facet"]),
                assignment_role=str(row["assignmentRole"]),
                resource_route=None,
            )
            _require_reference(
                row["mappingSnapshot"],
                f"{label}.mappingSnapshot",
                versioned=False,
            )
            _require_reference(
                row["sourceRelease"],
                f"{label}.sourceRelease",
                versioned=True,
            )
            _require_reference(
                row["targetRelease"],
                f"{label}.targetRelease",
                versioned=True,
            )
            _require_iri(row["relation"], f"{label}.relation")
            if row["direction"] not in {"sourceToTarget", "targetToSource"}:
                raise ReferenceRuntimeError(f"{label}.direction is invalid")
            all_rows.append(("mapping", row))
        for index, row in enumerate(self.open_label_permissions):
            label = f"openLabelPermissions[{index}]"
            mode = row.get("mode")
            required = open_default_fields if mode == "declaredDefaultLanguage" else open_fields
            _require_exact_fields(row, required, label)
            _validate_permission_use(row, label)
            _require_iri(row["facet"], f"{label}.facet")
            _require_iri(row["assignmentRole"], f"{label}.assignmentRole")
            enrichment_profile.require_compatible(
                facet=str(row["facet"]),
                assignment_role=str(row["assignmentRole"]),
                resource_route=None,
            )
            if mode not in {"explicitLanguage", "declaredDefaultLanguage"}:
                raise ReferenceRuntimeError(f"{label}.mode is invalid")
            if mode == "declaredDefaultLanguage":
                _require_language_tag(
                    row["defaultLanguage"],
                    f"{label}.defaultLanguage",
                )
            all_rows.append(("openLabel", row))
        selector_fields = {
            "release": (
                "facet",
                "assignmentRole",
                "referenceResourceRelease",
                "registryImportSnapshot",
            ),
            "mapping": (
                "facet",
                "assignmentRole",
                "mappingSnapshot",
                "sourceRelease",
                "targetRelease",
                "relation",
                "direction",
            ),
            "openLabel": (
                "facet",
                "assignmentRole",
                "mode",
                "defaultLanguage",
            ),
        }
        serialized_selectors = [
            (
                kind,
                canonical_json({key: row.get(key) for key in selector_fields[kind]}),
            )
            for kind, row in all_rows
        ]
        if len(set(serialized_selectors)) != len(serialized_selectors):
            raise ReferenceRuntimeError("OutputProfile contains a duplicate permission selector tuple")
        return {
            **base,
            "version": self.version,
            "enrichmentProfile": dict(self.enrichment_profile),
            "acceptancePolicies": [dict(policy) for policy in self.acceptance_policies],
            "publicationViews": [dict(view) for view in self.publication_views],
            "releasePermissions": [dict(row) for row in self.release_permissions],
            "mappingPermissions": [dict(row) for row in self.mapping_permissions],
            "openLabelPermissions": [dict(row) for row in self.open_label_permissions],
        }

    @property
    def content_digest(self) -> str:
        return canonical_payload_digest(
            self.payload(),
            digest_field="contentDigest",
        )

    @property
    def reference(self) -> Mapping[str, str]:
        return {
            "id": self.profile_id,
            "version": self.version,
            "digest": self.content_digest,
        }

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload(), digest_field="contentDigest")

    def _authorize(
        self,
        rows: Sequence[Mapping[str, Any]],
        supplied: Mapping[str, Any],
        *,
        accepted_output: bool,
        kind: str,
    ) -> Mapping[str, Any]:
        self.payload()
        key = "acceptedOutputUse" if accepted_output else "candidateUse"
        matches = [row for row in rows if all(row.get(name) == value for name, value in supplied.items())]
        if len(matches) != 1 or matches[0].get(key) is not True:
            raise ReferenceRuntimeError(
                f"{kind} authorization must match exactly one complete permission row with {key}=true"
            )
        return matches[0]

    def authorize_release(
        self,
        *,
        facet: str,
        assignment_role: str,
        resource_route: str,
        reference_resource_release: Mapping[str, str],
        registry_import_snapshot: Mapping[str, str],
        coverage_report: RegistryImportCoverageReport,
        accepted_output: bool,
    ) -> Mapping[str, Any]:
        self._resolved_enrichment_profile().require_compatible(
            facet=facet,
            assignment_role=assignment_role,
            resource_route=resource_route,
        )
        row = self._authorize(
            self.release_permissions,
            {
                "facet": facet,
                "assignmentRole": assignment_role,
                "referenceResourceRelease": reference_resource_release,
                "registryImportSnapshot": registry_import_snapshot,
            },
            accepted_output=accepted_output,
            kind="release",
        )
        coverage_report.payload()
        if coverage_report.report_status != "pass":
            raise ReferenceRuntimeError("release authorization requires a passing coverage report")
        if dict(coverage_report.output_profile) != dict(self.reference):
            raise ReferenceRuntimeError("coverage report is not tied to this exact OutputProfile")
        if dict(coverage_report.reference_resource_release) != dict(reference_resource_release):
            raise ReferenceRuntimeError("coverage report release does not match the permission row")
        if dict(coverage_report.import_snapshot) != dict(registry_import_snapshot):
            raise ReferenceRuntimeError("coverage report import snapshot does not match the permission row")
        required_features = {
            feature
            for permission in self.release_permissions
            if (
                dict(permission["referenceResourceRelease"]) == dict(reference_resource_release)
                and dict(permission["registryImportSnapshot"]) == dict(registry_import_snapshot)
                and permission["candidateUse"] is True
            )
            for feature in permission["requiredImportFeatures"]
        }
        coverage_requirements = {
            feature.feature for feature in coverage_report.feature_rows if feature.required_for_candidate_or_output
        }
        if coverage_requirements != required_features:
            raise ReferenceRuntimeError("coverage requirement flags do not match the OutputProfile release permissions")
        return row

    def authorize_mapping(
        self,
        *,
        facet: str,
        assignment_role: str,
        resource_route: str,
        mapping_snapshot: Mapping[str, str],
        source_release: Mapping[str, str],
        target_release: Mapping[str, str],
        relation: str,
        direction: str,
        coverage_report: RegistryImportCoverageReport | None,
        accepted_output: bool,
    ) -> Mapping[str, Any]:
        self._resolved_enrichment_profile().require_compatible(
            facet=facet,
            assignment_role=assignment_role,
            resource_route=resource_route,
        )
        row = self._authorize(
            self.mapping_permissions,
            {
                "facet": facet,
                "assignmentRole": assignment_role,
                "mappingSnapshot": mapping_snapshot,
                "sourceRelease": source_release,
                "targetRelease": target_release,
                "relation": relation,
                "direction": direction,
            },
            accepted_output=accepted_output,
            kind="mapping",
        )
        if coverage_report is None:
            raise ReferenceRuntimeError("mapping authorization requires an exact coverage report")
        coverage_report.payload()
        if coverage_report.report_status != "pass":
            raise ReferenceRuntimeError("mapping authorization requires a passing coverage report")
        if dict(coverage_report.output_profile) != dict(self.reference):
            raise ReferenceRuntimeError("mapping coverage report is not tied to this exact OutputProfile")
        if dict(coverage_report.import_snapshot) != dict(mapping_snapshot):
            raise ReferenceRuntimeError("mapping coverage report import snapshot does not match the permission row")
        required_features = {
            feature.feature for feature in coverage_report.feature_rows if feature.required_for_candidate_or_output
        }
        if required_features != MAPPING_IMPORT_REQUIRED_FEATURES:
            raise ReferenceRuntimeError(
                "mapping coverage requirement flags must exactly match "
                "mappings, identifiers, and membership"
            )
        return row

    def authorize_open_label(
        self,
        *,
        facet: str,
        assignment_role: str,
        resource_route: str,
        mode: str,
        default_language: str | None,
        accepted_output: bool,
    ) -> Mapping[str, Any]:
        self._resolved_enrichment_profile().require_compatible(
            facet=facet,
            assignment_role=assignment_role,
            resource_route=resource_route,
        )
        supplied: dict[str, Any] = {
            "facet": facet,
            "assignmentRole": assignment_role,
            "mode": mode,
        }
        if mode == "declaredDefaultLanguage":
            supplied["defaultLanguage"] = default_language
        return self._authorize(
            self.open_label_permissions,
            supplied,
            accepted_output=accepted_output,
            kind="open-label",
        )


def materialize_open_label_value_assertion(
    *,
    output_profile: OutputProfile,
    facet: str,
    assignment_role: str,
    resource_route: str,
    mode: str,
    declared_default_language: str | None,
    literal: str,
    language_tag: str | None,
    assertion_id: str,
    subject_iri: str,
    extraction_activity_iri: str,
    asserted_at: str,
    evidence_binding_id: str,
    source_fragment_iris: Sequence[str],
    assertion_origin: str = "rkaf:deterministicExtraction",
    epistemic_basis: str = "rkaf:sourceExplicit",
    evidence_role: str = "rkaf:textualEvidence",
    ai_lineage_iri: str | None = None,
    usage_eligibility: str | None = None,
) -> Mapping[str, Mapping[str, Any]]:
    """Materialize one permission-bound, fragment-grounded open label.

    The result contains the portable Rulespec ``ValueAssertion`` and its
    separate supporting ``EvidenceBinding``. A declared default is copied into
    the final JSON-LD value before either record leaves this function.
    """
    permission = output_profile.authorize_open_label(
        facet=facet,
        assignment_role=assignment_role,
        resource_route=resource_route,
        mode=mode,
        default_language=declared_default_language,
        accepted_output=True,
    )
    wording = _require_text(literal, "open-label literal")
    if mode == "explicitLanguage":
        if declared_default_language is not None:
            raise ReferenceRuntimeError("explicitLanguage does not accept a declared default")
        language = _require_language_tag(
            language_tag,
            "open-label language",
        )
    elif mode == "declaredDefaultLanguage":
        configured_default = _require_language_tag(
            permission.get("defaultLanguage"),
            "open-label permission defaultLanguage",
        )
        if declared_default_language != configured_default:
            raise ReferenceRuntimeError("declared default language does not match the permission row")
        materialized_language = configured_default if language_tag is None else language_tag
        language = _require_language_tag(
            materialized_language,
            "open-label language",
        )
    else:
        raise ReferenceRuntimeError(f"unknown open-label mode {mode!r}")

    assertion_origins = {
        "rkaf:humanAsserted",
        "rkaf:aiSuggested",
        "rkaf:imported",
        "rkaf:deterministicExtraction",
    }
    epistemic_bases = {
        "rkaf:sourceExplicit",
        "rkaf:deterministicDerivation",
        "rkaf:statisticalInference",
        "rkaf:editorialAssertion",
        "rkaf:userAssertion",
    }
    evidence_roles = {
        "rkaf:textualEvidence",
        "rkaf:structuralEvidence",
        "rkaf:retrievalSignal",
        "rkaf:authorityCitation",
        "rkaf:officialSourceMetadata",
        "rkaf:reviewedAuthorityChain",
        "rkaf:formalAdoptionEvent",
        "rkaf:mappingRationale",
        "rkaf:registrationEvent",
        "rkaf:rescissionEvidence",
    }
    if assertion_origin not in assertion_origins:
        raise ReferenceRuntimeError("unknown Rulespec assertion origin")
    if epistemic_basis not in epistemic_bases:
        raise ReferenceRuntimeError("unknown Rulespec epistemic basis")
    if evidence_role not in evidence_roles:
        raise ReferenceRuntimeError("unknown Rulespec evidence role")

    assertion = {
        "@id": _require_iri(assertion_id, "assertion id"),
        "@type": "rkaf:ValueAssertion",
        "rkaf:assertionOrigin": assertion_origin,
        "rkaf:epistemicBasis": epistemic_basis,
        "rkaf:assertsSubject": _require_iri(subject_iri, "assertion subject"),
        "rkaf:assertsPredicate": "rkaf:openLabel",
        "rkaf:assertsValue": {
            "@value": wording,
            "@language": language,
        },
        "rkaf:assertionPolarity": "rkaf:affirmed",
        "rkaf:openLabelFacet": _require_iri(facet, "openLabelFacet"),
        "rkaf:openLabelRole": _require_iri(
            assignment_role,
            "openLabelRole",
        ),
        "rkaf:hasExtractionProvenance": _require_iri(
            extraction_activity_iri,
            "hasExtractionProvenance",
        ),
        "rkaf:assertedAt": _require_datetime(asserted_at, "assertedAt"),
    }
    if assertion_origin == "rkaf:aiSuggested":
        assertion["rkaf:hasAILineage"] = _require_iri(
            ai_lineage_iri,
            "hasAILineage",
        )
        if usage_eligibility not in {
            "rkaf:notEligible",
            "rkaf:searchOnly",
            "rkaf:reviewQueueOnly",
        }:
            raise ReferenceRuntimeError("aiSuggested open labels require provisional usage eligibility")
        assertion["rkaf:usageEligibility"] = usage_eligibility
    elif ai_lineage_iri is not None or usage_eligibility is not None:
        raise ReferenceRuntimeError("AI lineage and provisional eligibility require aiSuggested origin")

    fragments = _require_array(
        source_fragment_iris,
        "bindsSourceFragment",
        nonempty=True,
    )
    evidence = {
        "@id": _require_iri(evidence_binding_id, "evidence binding id"),
        "@type": "rkaf:EvidenceBinding",
        "rkaf:bindsAssertion": assertion["@id"],
        "rkaf:bindsSourceFragment": [_require_iri(value, "bindsSourceFragment") for value in fragments],
        "rkaf:evidenceRole": evidence_role,
        "rkaf:evidentiaryFunction": "rkaf:supports",
    }
    return {
        "assertion": assertion,
        "evidenceBinding": evidence,
    }


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise ReferenceRuntimeError(f"{label} must be a non-empty object")
    return cast(Mapping[str, Any], value)


def _require_array(
    value: object,
    label: str,
    *,
    nonempty: bool,
) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ReferenceRuntimeError(f"{label} must be an array")
    if nonempty and not value:
        raise ReferenceRuntimeError(f"{label} must not be empty")
    _require_unique_json_values(value, label)
    return value


def _require_budget_limit(value: object, label: str) -> None:
    if value == "unlimited":
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ReferenceRuntimeError(f"{label} must be a non-negative integer or 'unlimited'")


def _validate_configuration_pins(
    *,
    indexes: Sequence[Mapping[str, Any]],
    candidate_channels: Sequence[Mapping[str, Any]],
    models: Sequence[Mapping[str, Any]],
    prompts: Sequence[Mapping[str, Any]],
    tool_policies: Sequence[Mapping[str, Any]],
    budgets: Sequence[Mapping[str, Any]],
    determinism: Sequence[Mapping[str, Any]],
    other_behavior_pins: Sequence[Mapping[str, Any]],
) -> None:
    """Validate every nested EnrichmentConfiguration record shape."""
    for group, label in (
        (indexes, "indexes"),
        (candidate_channels, "candidateChannels"),
        (models, "models"),
        (prompts, "prompts"),
        (tool_policies, "toolPolicies"),
        (budgets, "budgets"),
        (determinism, "determinism"),
        (other_behavior_pins, "otherBehaviorPins"),
    ):
        _require_array(group, label, nonempty=False)

    index_fields = frozenset(
        {
            "indexSnapshot",
            "indexedExpressionCorpusDigest",
            "indexedRepresentationVersion",
            "normalizationPolicy",
        }
    )
    for index, raw in enumerate(indexes):
        label = f"indexes[{index}]"
        value = _require_mapping(raw, label)
        _require_exact_fields(value, index_fields, label)
        _require_reference(
            _require_mapping(value["indexSnapshot"], f"{label}.indexSnapshot"),
            f"{label}.indexSnapshot",
            versioned=False,
        )
        _require_digest(
            value["indexedExpressionCorpusDigest"],
            f"{label}.indexedExpressionCorpusDigest",
        )
        _require_text(
            value["indexedRepresentationVersion"],
            f"{label}.indexedRepresentationVersion",
        )
        _require_reference(
            _require_mapping(
                value["normalizationPolicy"],
                f"{label}.normalizationPolicy",
            ),
            f"{label}.normalizationPolicy",
            versioned=True,
        )

    channel_fields = frozenset(
        {
            "id",
            "retriever",
            "queryConstruction",
            "ordering",
            "fusion",
            "deduplication",
            "quota",
            "truncation",
            "fallbackPolicy",
        }
    )
    for index, raw in enumerate(candidate_channels):
        label = f"candidateChannels[{index}]"
        value = _require_mapping(raw, label)
        _require_exact_fields(value, channel_fields, label)
        _require_iri(value["id"], f"{label}.id")
        for name in (
            "retriever",
            "queryConstruction",
            "ordering",
            "fusion",
            "deduplication",
            "truncation",
            "fallbackPolicy",
        ):
            _require_component_pin(
                _require_mapping(value[name], f"{label}.{name}"),
                f"{label}.{name}",
            )
        quota = _require_mapping(value["quota"], f"{label}.quota")
        _require_exact_fields(
            quota,
            frozenset({"maximumCandidates", "policy"}),
            f"{label}.quota",
        )
        maximum = quota["maximumCandidates"]
        if not isinstance(maximum, int) or isinstance(maximum, bool) or maximum < 1:
            raise ReferenceRuntimeError(f"{label}.quota.maximumCandidates must be a positive integer")
        _require_component_pin(
            _require_mapping(
                quota["policy"],
                f"{label}.quota.policy",
            ),
            f"{label}.quota.policy",
        )

    model_fields = frozenset(
        {
            "id",
            "revision",
            "providerConfiguration",
            "endpointConfiguration",
            "inferenceParameters",
            "structuredOutputSchema",
        }
    )
    for index, raw in enumerate(models):
        label = f"models[{index}]"
        value = _require_mapping(raw, label)
        _require_exact_fields(value, model_fields, label)
        _require_iri(value["id"], f"{label}.id")
        _require_text(value["revision"], f"{label}.revision")
        for name in ("providerConfiguration", "endpointConfiguration"):
            _require_component_pin(
                _require_mapping(value[name], f"{label}.{name}"),
                f"{label}.{name}",
            )
        parameters = _require_mapping(
            value["inferenceParameters"],
            f"{label}.inferenceParameters",
        )
        for name, parameter in parameters.items():
            parameter_label = f"{label}.inferenceParameters.{name}"
            if isinstance(parameter, bool):
                continue
            if isinstance(parameter, int):
                continue
            if isinstance(parameter, str):
                _require_text(parameter, parameter_label)
                continue
            raise ReferenceRuntimeError(f"{parameter_label} must be a string, boolean, or integer")
        _require_reference(
            _require_mapping(
                value["structuredOutputSchema"],
                f"{label}.structuredOutputSchema",
            ),
            f"{label}.structuredOutputSchema",
            versioned=True,
        )

    for pin_group, label in (
        (prompts, "prompts"),
        (tool_policies, "toolPolicies"),
        (other_behavior_pins, "otherBehaviorPins"),
    ):
        for index, raw in enumerate(pin_group):
            _require_component_pin(
                _require_mapping(raw, f"{label}[{index}]"),
                f"{label}[{index}]",
            )

    budget_fields = frozenset(
        {
            "stage",
            "inputBytes",
            "outputBytes",
            "tokens",
            "milliseconds",
            "candidates",
            "costMicrounits",
        }
    )
    for index, raw in enumerate(budgets):
        label = f"budgets[{index}]"
        value = _require_mapping(raw, label)
        _require_exact_fields(value, budget_fields, label)
        _require_iri(value["stage"], f"{label}.stage")
        for name in budget_fields - {"stage"}:
            _require_budget_limit(value[name], f"{label}.{name}")

    determinism_required = frozenset({"stage", "status", "replayControls"})
    determinism_allowed = determinism_required | {"seed"}
    for index, raw in enumerate(determinism):
        label = f"determinism[{index}]"
        value = _require_mapping(raw, label)
        missing = sorted(determinism_required - set(value))
        extra = sorted(set(value) - determinism_allowed)
        if missing or extra:
            raise ReferenceRuntimeError(
                f"{label} must be one complete determinism record; missing={missing}, extra={extra}"
            )
        _require_iri(value["stage"], f"{label}.stage")
        if value["status"] not in {"deterministic", "nondeterministic"}:
            raise ReferenceRuntimeError(f"{label}.status must be deterministic or nondeterministic")
        if "seed" in value:
            seed = value["seed"]
            if isinstance(seed, bool) or not isinstance(seed, (int, str)):
                raise ReferenceRuntimeError(f"{label}.seed must be an integer or non-empty string")
            if isinstance(seed, str):
                _require_text(seed, f"{label}.seed")
        _require_component_pin(
            _require_mapping(
                value["replayControls"],
                f"{label}.replayControls",
            ),
            f"{label}.replayControls",
        )


@dataclass(frozen=True)
class EnrichmentConfiguration:
    configuration_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    implementation: Mapping[str, Any]
    enrichment_profile: Mapping[str, str]
    output_profile: Mapping[str, str]
    acceptance_policy: Mapping[str, str]
    schemas: tuple[Mapping[str, str], ...]
    input_corpora: tuple[Mapping[str, str], ...]
    vocabulary: Mapping[str, Any]
    indexes: tuple[Mapping[str, Any], ...]
    candidate_channels: tuple[Mapping[str, Any], ...]
    models: tuple[Mapping[str, Any], ...]
    prompts: tuple[Mapping[str, Any], ...]
    tool_policies: tuple[Mapping[str, Any], ...]
    budgets: tuple[Mapping[str, Any], ...]
    determinism: tuple[Mapping[str, Any], ...]
    other_behavior_pins: tuple[Mapping[str, Any], ...]
    secret_version_refs: tuple[str, ...]
    output_profile_record: OutputProfile | None = None

    def payload(self) -> dict[str, Any]:
        base = _record_base(
            record_id=self.configuration_id,
            record_type="urn:ref:type:EnrichmentConfiguration",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        implementation = _require_mapping(
            self.implementation,
            "implementation",
        )
        _require_exact_fields(
            implementation,
            frozenset(
                {
                    "id",
                    "revision",
                    "build",
                    "runtime",
                    "dependencyLockDigest",
                }
            ),
            "implementation",
        )
        _require_iri(implementation.get("id"), "implementation.id")
        _require_text(
            implementation.get("revision"),
            "implementation.revision",
        )
        _require_text(
            implementation.get("build"),
            "implementation.build",
        )
        runtime = implementation.get("runtime")
        if not isinstance(runtime, Mapping):
            raise ReferenceRuntimeError("implementation.runtime must be a component pin")
        _require_component_pin(runtime, "implementation.runtime")
        _require_digest(
            implementation.get("dependencyLockDigest"),
            "implementation.dependencyLockDigest",
        )
        for label, pin in (
            ("enrichmentProfile", self.enrichment_profile),
            ("outputProfile", self.output_profile),
            ("acceptancePolicy", self.acceptance_policy),
        ):
            _require_reference(
                _require_mapping(pin, label),
                label,
                versioned=True,
            )
        if not self.schemas or not self.input_corpora:
            raise ReferenceRuntimeError("configuration must pin schemas and input corpora")
        for index, value in enumerate(self.schemas):
            _require_reference(
                _require_mapping(value, f"schemas[{index}]"),
                f"schemas[{index}]",
                versioned=True,
            )
        for index, value in enumerate(self.input_corpora):
            _require_reference(
                _require_mapping(value, f"inputCorpora[{index}]"),
                f"inputCorpora[{index}]",
                versioned=True,
            )
        vocabulary_fields = frozenset(
            {
                "referenceResourceReleases",
                "registryImportSnapshots",
                "mappingReleases",
                "mappingSnapshots",
                "candidateTargetUniverseDigest",
                "registryDeploymentDecisions",
            }
        )
        _require_exact_fields(
            self.vocabulary,
            vocabulary_fields,
            "vocabulary",
        )
        if not self.vocabulary["referenceResourceReleases"] or not self.vocabulary["registryImportSnapshots"]:
            raise ReferenceRuntimeError("configuration must pin reference releases and registry imports")
        for key in ("referenceResourceReleases", "mappingReleases"):
            values = _require_array(
                self.vocabulary[key],
                f"vocabulary.{key}",
                nonempty=(key == "referenceResourceReleases"),
            )
            for index, value in enumerate(values):
                _require_reference(
                    _require_mapping(
                        value,
                        f"vocabulary.{key}[{index}]",
                    ),
                    f"vocabulary.{key}[{index}]",
                    versioned=True,
                )
        for key in ("registryImportSnapshots", "mappingSnapshots"):
            values = _require_array(
                self.vocabulary[key],
                f"vocabulary.{key}",
                nonempty=(key == "registryImportSnapshots"),
            )
            for index, value in enumerate(values):
                _require_reference(
                    _require_mapping(
                        value,
                        f"vocabulary.{key}[{index}]",
                    ),
                    f"vocabulary.{key}[{index}]",
                    versioned=False,
                )
        deployment_decisions = _require_array(
            self.vocabulary["registryDeploymentDecisions"],
            "vocabulary.registryDeploymentDecisions",
            nonempty=True,
        )
        for index, value in enumerate(deployment_decisions):
            _require_reference(
                _require_mapping(
                    value,
                    f"vocabulary.registryDeploymentDecisions[{index}]",
                ),
                f"vocabulary.registryDeploymentDecisions[{index}]",
                versioned=False,
            )
        _require_digest(
            self.vocabulary["candidateTargetUniverseDigest"],
            "vocabulary.candidateTargetUniverseDigest",
        )
        if not self.indexes or not self.candidate_channels:
            raise ReferenceRuntimeError("configuration must pin candidate indexes and channels")
        if not self.budgets or not self.determinism:
            raise ReferenceRuntimeError("configuration must pin budgets and deterministic controls")
        _validate_configuration_pins(
            indexes=self.indexes,
            candidate_channels=self.candidate_channels,
            models=self.models,
            prompts=self.prompts,
            tool_policies=self.tool_policies,
            budgets=self.budgets,
            determinism=self.determinism,
            other_behavior_pins=self.other_behavior_pins,
        )
        _require_unique_json_values(self.schemas, "schemas")
        _require_unique_json_values(self.input_corpora, "inputCorpora")
        if len(set(self.secret_version_refs)) != len(self.secret_version_refs):
            raise ReferenceRuntimeError("secretVersionRefs must contain unique values")
        for value in self.secret_version_refs:
            _require_iri(value, "secretVersionRef")
        if self.output_profile_record is not None:
            output_profile = self.output_profile_record
            output_profile.payload()
            if dict(self.output_profile) != dict(output_profile.reference):
                raise ReferenceRuntimeError("configuration OutputProfile pin does not match the resolved record")
            if dict(self.enrichment_profile) != dict(output_profile.enrichment_profile):
                raise ReferenceRuntimeError(
                    "configuration EnrichmentProfile pin differs from the resolved OutputProfile"
                )
            if dict(self.acceptance_policy) not in [dict(value) for value in output_profile.acceptance_policies]:
                raise ReferenceRuntimeError(
                    "configuration acceptance policy is not pinned by the resolved OutputProfile"
                )
            reference_releases = {canonical_json(value) for value in self.vocabulary["referenceResourceReleases"]}
            registry_snapshots = {canonical_json(value) for value in self.vocabulary["registryImportSnapshots"]}
            mapping_snapshots = {canonical_json(value) for value in self.vocabulary["mappingSnapshots"]}
            for row in output_profile.release_permissions:
                if (
                    canonical_json(row["referenceResourceRelease"]) not in reference_releases
                    or canonical_json(row["registryImportSnapshot"]) not in registry_snapshots
                ):
                    raise ReferenceRuntimeError(
                        "configuration vocabulary omits an OutputProfile release or registry-import permission pin"
                    )
            for row in output_profile.mapping_permissions:
                if (
                    canonical_json(row["sourceRelease"]) not in reference_releases
                    or canonical_json(row["targetRelease"]) not in reference_releases
                    or canonical_json(row["mappingSnapshot"]) not in mapping_snapshots
                ):
                    raise ReferenceRuntimeError(
                        "configuration vocabulary omits an OutputProfile mapping endpoint or snapshot permission pin"
                    )
        result = {
            **base,
            "implementation": dict(self.implementation),
            "enrichmentProfile": dict(self.enrichment_profile),
            "outputProfile": dict(self.output_profile),
            "acceptancePolicy": dict(self.acceptance_policy),
            "schemas": [dict(value) for value in self.schemas],
            "inputCorpora": [dict(value) for value in self.input_corpora],
            "vocabulary": {
                "referenceResourceReleases": [dict(value) for value in self.vocabulary["referenceResourceReleases"]],
                "registryImportSnapshots": [dict(value) for value in self.vocabulary["registryImportSnapshots"]],
                "mappingReleases": [dict(value) for value in self.vocabulary["mappingReleases"]],
                "mappingSnapshots": [dict(value) for value in self.vocabulary["mappingSnapshots"]],
                "candidateTargetUniverseDigest": self.vocabulary["candidateTargetUniverseDigest"],
                "registryDeploymentDecisions": [
                    dict(value) for value in self.vocabulary["registryDeploymentDecisions"]
                ],
            },
            "indexes": [dict(value) for value in self.indexes],
            "candidateChannels": [dict(value) for value in self.candidate_channels],
            "models": [dict(value) for value in self.models],
            "prompts": [dict(value) for value in self.prompts],
            "toolPolicies": [dict(value) for value in self.tool_policies],
            "budgets": [dict(value) for value in self.budgets],
            "determinism": [dict(value) for value in self.determinism],
            "otherBehaviorPins": [dict(value) for value in self.other_behavior_pins],
            "secretVersionRefs": list(self.secret_version_refs),
        }
        _assert_finite_json(result)
        return result

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload())

    @property
    def digest(self) -> str:
        return canonical_payload_digest(self.payload())


def _reference_set(values: object, label: str) -> set[str]:
    rows = _require_array(values, label, nonempty=False)
    result: set[str] = set()
    for index, raw in enumerate(rows):
        value = _require_mapping(raw, f"{label}[{index}]")
        identity = canonical_json(_json_copy(value))
        if identity in result:
            raise ReferenceRuntimeError(f"{label} contains a duplicate reference")
        result.add(identity)
    return result


def _validate_sealed_gold_partition_semantics(
    record: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate the authoritative seven-way development/holdout boundary."""

    universe = _require_mapping(
        record.get("vocabularyUniverse"),
        "sealedGold.vocabularyUniverse",
    )
    universe_fields = frozenset(
        {
            "referenceResourceReleases",
            "registryImportSnapshots",
            "mappingReleases",
            "mappingSnapshots",
            "indexedExpressionCorpusDigests",
            "enrichmentProfile",
            "outputProfile",
            "normalizationPolicy",
            "candidateTargetUniverseDigest",
        }
    )
    _require_exact_fields(
        universe,
        universe_fields,
        "sealedGold.vocabularyUniverse",
    )
    for field, versioned, nonempty in (
        ("referenceResourceReleases", True, True),
        ("registryImportSnapshots", False, True),
        ("mappingReleases", True, False),
        ("mappingSnapshots", False, False),
    ):
        values = _require_array(
            universe[field],
            f"sealedGold.vocabularyUniverse.{field}",
            nonempty=nonempty,
        )
        _require_unique_json_values(
            values,
            f"sealedGold.vocabularyUniverse.{field}",
        )
        for index, value in enumerate(values):
            _require_reference(
                _require_mapping(
                    value,
                    f"sealedGold.vocabularyUniverse.{field}[{index}]",
                ),
                f"sealedGold.vocabularyUniverse.{field}[{index}]",
                versioned=versioned,
            )
    for field in ("enrichmentProfile", "outputProfile", "normalizationPolicy"):
        _require_reference(
            _require_mapping(
                universe[field],
                f"sealedGold.vocabularyUniverse.{field}",
            ),
            f"sealedGold.vocabularyUniverse.{field}",
            versioned=True,
        )
    _require_digest(
        universe["candidateTargetUniverseDigest"],
        "sealedGold.vocabularyUniverse.candidateTargetUniverseDigest",
    )
    expression_corpus_digests = _require_nonempty_string_list(
        universe["indexedExpressionCorpusDigests"],
        "sealedGold.vocabularyUniverse.indexedExpressionCorpusDigests",
    )
    for digest in expression_corpus_digests:
        _require_digest(
            digest,
            "sealedGold.vocabularyUniverse.indexedExpressionCorpusDigest",
        )

    items = _require_array(record.get("items"), "sealedGold.items", nonempty=True)
    if len(items) < 2:
        raise ReferenceRuntimeError("sealedGold.items must contain development and holdout items")
    item_values: dict[str, dict[str, set[str]]] = {}
    split_values: dict[str, dict[str, set[str]]] = {
        dimension: {"development": set(), "holdout": set()} for dimension in GOLD_PARTITION_DIMENSIONS
    }
    evidence_by_item: dict[str, Mapping[str, Any]] = {}
    for index, raw_item in enumerate(items):
        item = _require_mapping(raw_item, f"sealedGold.items[{index}]")
        item_id = _require_iri(
            item.get("id"),
            f"sealedGold.items[{index}].id",
        )
        if item_id in item_values:
            raise ReferenceRuntimeError("sealedGold.items contains a duplicate identifier")
        split = item.get("split")
        if split not in {"development", "holdout"}:
            raise ReferenceRuntimeError(f"sealedGold.items[{index}].split is invalid")
        source_resource = _require_iri(
            item.get("sourceResource"),
            f"sealedGold.items[{index}].sourceResource",
        )
        rendition = _require_mapping(
            item.get("renditionArtifact"),
            f"sealedGold.items[{index}].renditionArtifact",
        )
        _require_reference(
            rendition,
            f"sealedGold.items[{index}].renditionArtifact",
            versioned=False,
        )
        keys = _require_mapping(
            item.get("partitionKeys"),
            f"sealedGold.items[{index}].partitionKeys",
        )
        _require_exact_fields(
            keys,
            GOLD_PARTITION_DIMENSIONS,
            f"sealedGold.items[{index}].partitionKeys",
        )
        values_by_dimension: dict[str, set[str]] = {}
        for dimension in GOLD_PARTITION_DIMENSIONS:
            values = _require_array(
                keys[dimension],
                (f"sealedGold.items[{index}].partitionKeys.{dimension}"),
                nonempty=dimension
                in {
                    "sourceIdentity",
                    "artifactDigest",
                    "textDigest",
                    "nearDuplicateCluster",
                },
            )
            resolved = {
                _require_text(
                    value,
                    (f"sealedGold.items[{index}].partitionKeys.{dimension} item"),
                )
                for value in values
            }
            if len(resolved) != len(values):
                raise ReferenceRuntimeError(f"sealedGold.items[{index}].partitionKeys.{dimension} contains a duplicate")
            values_by_dimension[dimension] = resolved
            split_values[dimension][str(split)].update(resolved)
        if values_by_dimension["sourceIdentity"] != {source_resource}:
            raise ReferenceRuntimeError(f"{item_id}: sourceIdentity must exactly match sourceResource")
        if values_by_dimension["artifactDigest"] != {str(rendition["digest"])}:
            raise ReferenceRuntimeError(f"{item_id}: artifactDigest must exactly match renditionArtifact.digest")
        if values_by_dimension["conceptIdentity"] and not values_by_dimension["exactMatchCluster"]:
            raise ReferenceRuntimeError(f"{item_id}: represented concepts require an exact-match cluster key")

        evidence = _require_mapping(
            item.get("partitionEvidence"),
            f"sealedGold.items[{index}].partitionEvidence",
        )
        _require_exact_fields(
            evidence,
            frozenset(
                {
                    "sourceTextDigest",
                    "vocabularyExpressionCorpusDigest",
                    "exactMatchGraphDigest",
                    "nearDuplicateAnalysisDigest",
                    "receipt",
                }
            ),
            f"sealedGold.items[{index}].partitionEvidence",
        )
        for field in (
            "sourceTextDigest",
            "vocabularyExpressionCorpusDigest",
            "exactMatchGraphDigest",
            "nearDuplicateAnalysisDigest",
        ):
            _require_digest(
                evidence[field],
                f"sealedGold.items[{index}].partitionEvidence.{field}",
            )
        _require_iri(
            evidence["receipt"],
            f"sealedGold.items[{index}].partitionEvidence.receipt",
        )
        if values_by_dimension["textDigest"] != {str(evidence["sourceTextDigest"])}:
            raise ReferenceRuntimeError(f"{item_id}: textDigest must exactly match partition evidence")
        if evidence["vocabularyExpressionCorpusDigest"] not in expression_corpus_digests:
            raise ReferenceRuntimeError(
                f"{item_id}: partition evidence expression corpus is outside the sealed vocabulary universe"
            )
        item_values[item_id] = values_by_dimension
        evidence_by_item[item_id] = evidence

    for dimension, by_split in split_values.items():
        crossing = by_split["development"] & by_split["holdout"]
        if crossing:
            raise ReferenceRuntimeError(
                f"sealed gold leaks {dimension} across development and holdout: {sorted(crossing)!r}"
            )

    report = _require_mapping(
        record.get("partitionReport"),
        "sealedGold.partitionReport",
    )
    input_digests = set(
        _require_nonempty_string_list(
            report.get("inputDigests"),
            "sealedGold.partitionReport.inputDigests",
        )
    )
    for digest in input_digests:
        _require_digest(digest, "sealedGold.partitionReport.inputDigest")
    dimensions = _require_array(
        report.get("dimensions"),
        "sealedGold.partitionReport.dimensions",
        nonempty=True,
    )
    reported: dict[str, Mapping[str, Any]] = {}
    for index, raw_dimension in enumerate(dimensions):
        dimension = _require_mapping(
            raw_dimension,
            f"sealedGold.partitionReport.dimensions[{index}]",
        )
        name = _require_text(
            dimension.get("dimension"),
            f"sealedGold.partitionReport.dimensions[{index}].dimension",
        )
        if name not in GOLD_PARTITION_DIMENSIONS or name in reported:
            raise ReferenceRuntimeError(
                "sealedGold.partitionReport must contain every partition dimension exactly once"
            )
        reported[name] = dimension
    if set(reported) != GOLD_PARTITION_DIMENSIONS:
        raise ReferenceRuntimeError("sealedGold.partitionReport must contain every partition dimension exactly once")
    for dimension, report_row in reported.items():
        item_keys = _require_array(
            report_row.get("itemKeys"),
            f"sealedGold.partitionReport.{dimension}.itemKeys",
            nonempty=True,
        )
        reported_items: set[str] = set()
        for index, raw_entry in enumerate(item_keys):
            entry = _require_mapping(
                raw_entry,
                f"sealedGold.partitionReport.{dimension}.itemKeys[{index}]",
            )
            item_id = _require_iri(
                entry.get("item"),
                f"sealedGold.partitionReport.{dimension}.item",
            )
            values = set(
                _require_array(
                    entry.get("values"),
                    f"sealedGold.partitionReport.{dimension}.values",
                    nonempty=False,
                )
            )
            if item_id in reported_items or item_id not in item_values:
                raise ReferenceRuntimeError(
                    f"sealedGold.partitionReport.{dimension} must account for every sealed item exactly once"
                )
            if values != item_values[item_id][dimension]:
                raise ReferenceRuntimeError(
                    f"{item_id}: partitionReport {dimension} keys differ from the authoritative item keys"
                )
            reported_items.add(item_id)
        if reported_items != set(item_values):
            raise ReferenceRuntimeError(
                f"sealedGold.partitionReport.{dimension} must account for every sealed item exactly once"
            )
    for item_id, evidence in evidence_by_item.items():
        required_inputs = {
            str(evidence["vocabularyExpressionCorpusDigest"]),
            str(evidence["exactMatchGraphDigest"]),
            str(evidence["nearDuplicateAnalysisDigest"]),
        }
        if not required_inputs.issubset(input_digests):
            raise ReferenceRuntimeError(
                f"{item_id}: partition evidence digests are not pinned by partitionReport.inputDigests"
            )
    return universe


@dataclass(frozen=True)
class EnrichmentEvaluationResult:
    """Evaluation of one exact configuration/gold pair.

    Each predeclared measure has exactly one threshold and one observation.
    A passing verdict also requires the point estimate to satisfy its declared
    threshold, every configured stratum, and every core gate dimension.
    """

    result_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    configuration: Mapping[str, str]
    sealed_gold_manifest: Mapping[str, str]
    evaluation_protocol: Mapping[str, str]
    predeclared_measures: tuple[str, ...]
    thresholds: tuple[Mapping[str, Any], ...]
    configured_strata: tuple[Mapping[str, Any], ...]
    exclusions: tuple[str, ...]
    uncertainty_method: Mapping[str, str]
    observed_measures: tuple[Mapping[str, Any], ...]
    measure_populations: tuple[Mapping[str, Any], ...]
    gates: tuple[Mapping[str, Any], ...]
    evaluator: str
    activity: str
    evaluated_at: str
    output_artifact_digests: tuple[str, ...]
    verdict: str
    configuration_record: EnrichmentConfiguration | None = None
    gold_manifest_record: Mapping[str, Any] | None = None

    def payload(
        self,
        *,
        configuration: EnrichmentConfiguration | None = None,
        gold_manifest: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        base = _record_base(
            record_id=self.result_id,
            record_type="urn:ref:type:EnrichmentEvaluationResult",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_reference(
            self.configuration,
            "configuration",
            versioned=False,
        )
        _require_reference(
            self.sealed_gold_manifest,
            "sealedGoldManifest",
            versioned=False,
        )
        _require_reference(
            self.evaluation_protocol,
            "evaluationProtocol",
            versioned=True,
        )
        if self.verdict not in {"pass", "fail", "developmentOnly"}:
            raise ReferenceRuntimeError("unknown evaluation verdict")
        if (
            not self.predeclared_measures
            or not self.thresholds
            or not self.configured_strata
            or not self.observed_measures
            or not self.measure_populations
            or not self.gates
        ):
            raise ReferenceRuntimeError(
                "evaluation result must contain measures, thresholds, strata, populations, and gates"
            )
        if len(set(self.predeclared_measures)) != len(self.predeclared_measures):
            raise ReferenceRuntimeError("predeclaredMeasures contains a duplicate measure")
        for value in self.predeclared_measures:
            _require_iri(value, "predeclaredMeasure")
        for value in self.exclusions:
            _require_iri(value, "exclusion")
        _require_reference(
            self.uncertainty_method,
            "uncertaintyMethod",
            versioned=True,
        )

        observed: dict[str, Decimal] = {}
        for index, item in enumerate(self.observed_measures):
            label = f"observedMeasures[{index}]"
            _require_exact_fields(
                item,
                frozenset(
                    {
                        "measure",
                        "value",
                        "uncertaintyLower",
                        "uncertaintyUpper",
                    }
                ),
                label,
            )
            measure = _require_iri(item.get("measure"), f"{label}.measure")
            if measure in observed:
                raise ReferenceRuntimeError(f"{label}.measure duplicates {measure!r}")
            value = _require_decimal(item.get("value"), f"{label}.value")
            lower = _require_decimal(
                item.get("uncertaintyLower"),
                f"{label}.uncertaintyLower",
            )
            upper = _require_decimal(
                item.get("uncertaintyUpper"),
                f"{label}.uncertaintyUpper",
            )
            if not lower <= value <= upper:
                raise ReferenceRuntimeError(f"{measure}: uncertainty interval must contain the observed value")
            observed[measure] = value

        threshold_measures: set[str] = set()
        threshold_failures: list[str] = []
        for index, item in enumerate(self.thresholds):
            label = f"thresholds[{index}]"
            _require_exact_fields(
                item,
                frozenset({"measure", "operator", "value"}),
                label,
            )
            measure = _require_iri(item.get("measure"), f"{label}.measure")
            if measure not in self.predeclared_measures:
                raise ReferenceRuntimeError(f"{label}.measure was not predeclared")
            operator = item.get("operator")
            if operator not in {"atLeast", "atMost"}:
                raise ReferenceRuntimeError(f"{label}.operator must be atLeast or atMost")
            threshold = _require_decimal(item.get("value"), f"{label}.value")
            if measure not in observed:
                raise ReferenceRuntimeError(f"threshold measure {measure!r} lacks an observation")
            if measure in threshold_measures:
                raise ReferenceRuntimeError(f"{measure!r} has more than one threshold")
            threshold_measures.add(measure)
            actual = observed[measure]
            if operator == "atLeast" and actual < threshold or operator == "atMost" and actual > threshold:
                threshold_failures.append(measure)
        declared_measures = set(self.predeclared_measures)
        observed_measures = set(observed)
        if not (declared_measures == threshold_measures == observed_measures):
            raise ReferenceRuntimeError(
                "predeclaredMeasures, threshold measures, and observed measures must form exact one-to-one sets"
            )

        stratum_rows: set[str] = set()
        for index, stratum in enumerate(self.configured_strata):
            label = f"configuredStrata[{index}]"
            _require_exact_fields(
                stratum,
                frozenset(
                    {
                        "stratum",
                        "minimumSampleSize",
                        "observedSampleSize",
                        "passed",
                    }
                ),
                label,
            )
            _require_iri(stratum.get("stratum"), f"{label}.stratum")
            for count_field, minimum in (
                ("minimumSampleSize", 1),
                ("observedSampleSize", 0),
            ):
                count = stratum.get(count_field)
                if not isinstance(count, int) or isinstance(count, bool) or count < minimum:
                    raise ReferenceRuntimeError(f"{label}.{count_field} must be an integer >= {minimum}")
            if not isinstance(stratum.get("passed"), bool):
                raise ReferenceRuntimeError(f"{label}.passed must be a boolean")
            row_identity = canonical_json(stratum)
            if row_identity in stratum_rows:
                raise ReferenceRuntimeError(f"{label} duplicates an earlier configured stratum")
            stratum_rows.add(row_identity)

        gate_rows: set[str] = set()
        gate_dimensions: set[str] = set()
        for index, gate in enumerate(self.gates):
            label = f"gates[{index}]"
            _require_exact_fields(
                gate,
                frozenset(
                    {
                        "id",
                        "dimension",
                        "subject",
                        "passed",
                        "reason",
                    }
                ),
                label,
            )
            _require_iri(gate.get("id"), f"{label}.id")
            dimension = _require_text(
                gate.get("dimension"),
                f"{label}.dimension",
            )
            if dimension not in EVALUATION_GATE_DIMENSIONS:
                raise ReferenceRuntimeError(f"{label}.dimension is not a core evaluation dimension")
            gate_dimensions.add(dimension)
            _require_iri(gate.get("subject"), f"{label}.subject")
            if not isinstance(gate.get("passed"), bool):
                raise ReferenceRuntimeError(f"{label}.passed must be a boolean")
            _require_text(gate.get("reason"), f"{label}.reason")
            row_identity = canonical_json(gate)
            if row_identity in gate_rows:
                raise ReferenceRuntimeError(f"{label} duplicates an earlier gate")
            gate_rows.add(row_identity)

        resolved_configuration = configuration or self.configuration_record
        resolved_gold = gold_manifest or self.gold_manifest_record
        if resolved_configuration is None or resolved_gold is None:
            raise ReferenceRuntimeError(
                "evaluation validation requires the exact configuration and sealed-gold records"
            )
        if self.configuration["id"] != resolved_configuration.configuration_id:
            raise ReferenceRuntimeError("evaluation configuration identifier mismatch")
        if self.configuration["digest"] != resolved_configuration.digest:
            raise ReferenceRuntimeError("evaluation configuration digest mismatch")
        require_payload_digest(resolved_gold)
        if resolved_gold.get("type") != "urn:ref:type:SealedGoldManifest":
            raise ReferenceRuntimeError("evaluation gold record must be a SealedGoldManifest")
        if self.sealed_gold_manifest["id"] != resolved_gold.get("id"):
            raise ReferenceRuntimeError("evaluation sealed-gold identifier mismatch")
        if self.sealed_gold_manifest["digest"] != resolved_gold.get("canonicalPayloadDigest"):
            raise ReferenceRuntimeError("evaluation sealed-gold digest mismatch")
        vocabulary_universe = _validate_sealed_gold_partition_semantics(resolved_gold)
        if dict(
            _require_mapping(
                vocabulary_universe.get("outputProfile"),
                "sealedGold.vocabularyUniverse.outputProfile",
            )
        ) != dict(resolved_configuration.output_profile):
            raise ReferenceRuntimeError("configuration and sealed gold use different output-profile pins")
        if dict(
            _require_mapping(
                vocabulary_universe.get("enrichmentProfile"),
                "sealedGold.vocabularyUniverse.enrichmentProfile",
            )
        ) != dict(resolved_configuration.enrichment_profile):
            raise ReferenceRuntimeError("configuration and sealed gold use different enrichment-profile pins")
        for field in (
            "referenceResourceReleases",
            "registryImportSnapshots",
            "mappingReleases",
            "mappingSnapshots",
        ):
            configured_values = _reference_set(
                resolved_configuration.vocabulary[field],
                f"configuration.vocabulary.{field}",
            )
            gold_values = _reference_set(
                vocabulary_universe[field],
                f"sealedGold.vocabularyUniverse.{field}",
            )
            if configured_values != gold_values:
                raise ReferenceRuntimeError(f"configuration and sealed gold use different {field}")
        if (
            resolved_configuration.vocabulary["candidateTargetUniverseDigest"]
            != vocabulary_universe["candidateTargetUniverseDigest"]
        ):
            raise ReferenceRuntimeError("configuration and sealed gold use different candidate target universes")
        configured_normalization_policies = {
            canonical_json(
                _json_copy(
                    _require_mapping(
                        index["normalizationPolicy"],
                        "configuration.index.normalizationPolicy",
                    )
                )
            )
            for index in resolved_configuration.indexes
        }
        gold_normalization_policy = canonical_json(
            _json_copy(
                _require_mapping(
                    vocabulary_universe["normalizationPolicy"],
                    "sealedGold.vocabularyUniverse.normalizationPolicy",
                )
            )
        )
        if configured_normalization_policies != {gold_normalization_policy}:
            raise ReferenceRuntimeError("configuration and sealed gold use different normalization policies")
        configured_expression_corpora = {
            _require_digest(
                index["indexedExpressionCorpusDigest"],
                "configuration.index.indexedExpressionCorpusDigest",
            )
            for index in resolved_configuration.indexes
        }
        if configured_expression_corpora != set(
            cast(
                Sequence[str],
                vocabulary_universe["indexedExpressionCorpusDigests"],
            )
        ):
            raise ReferenceRuntimeError("configuration and sealed gold use different indexed-expression corpora")
        gold_corpus_digest = _require_digest(
            resolved_gold.get("corpusDigest"),
            "sealedGold.corpusDigest",
        )
        configured_corpus_digests = {str(corpus["digest"]) for corpus in resolved_configuration.input_corpora}
        if gold_corpus_digest not in configured_corpus_digests:
            raise ReferenceRuntimeError("configuration input corpora omit the sealed gold corpus digest")

        expectations_raw = resolved_gold.get("expectations")
        if (
            not isinstance(expectations_raw, Sequence)
            or isinstance(
                expectations_raw,
                (str, bytes),
            )
            or not expectations_raw
        ):
            raise ReferenceRuntimeError("sealed gold must contain expectations")
        expectation_ids: set[str] = set()
        not_represented: set[str] = set()
        for index, expectation in enumerate(expectations_raw):
            if not isinstance(expectation, Mapping):
                raise ReferenceRuntimeError(f"sealedGold.expectations[{index}] must be an object")
            expectation_mapping = cast(Mapping[str, Any], expectation)
            expectation_id = _require_iri(
                expectation_mapping.get("id"),
                f"sealedGold.expectations[{index}].id",
            )
            if expectation_id in expectation_ids:
                raise ReferenceRuntimeError("sealed gold contains duplicate expectation identifiers")
            expectation_ids.add(expectation_id)
            targets = expectation_mapping.get("registeredTargets", ())
            if not isinstance(targets, Sequence) or isinstance(
                targets,
                (str, bytes),
            ):
                raise ReferenceRuntimeError(f"sealedGold.expectations[{index}].registeredTargets must be an array")
            if any(isinstance(target, Mapping) and target.get("grade") == "notRepresented" for target in targets):
                not_represented.add(expectation_id)

        populations: dict[str, Mapping[str, Any]] = {}
        population_rows: set[str] = set()
        for index, population in enumerate(self.measure_populations):
            label = f"measurePopulations[{index}]"
            _require_exact_fields(
                population,
                frozenset(
                    {
                        "measure",
                        "populationKind",
                        "includedExpectations",
                        "excludedExpectations",
                    }
                ),
                label,
            )
            _require_iri(population.get("measure"), f"{label}.measure")
            kind = _require_text(
                population.get("populationKind"),
                f"{label}.populationKind",
            )
            if kind not in {
                "reachableRegisteredCandidateRecall",
                "targetAvailability",
                "openSet",
            }:
                raise ReferenceRuntimeError(f"{label}.populationKind is invalid")
            if kind in populations:
                raise ReferenceRuntimeError(f"{label} duplicates population kind {kind!r}")
            populations[kind] = population
            row_identity = canonical_json(population)
            if row_identity in population_rows:
                raise ReferenceRuntimeError(f"{label} duplicates an earlier population")
            population_rows.add(row_identity)
            included_raw = population.get("includedExpectations")
            excluded_raw = population.get("excludedExpectations")
            for field_name, values in (
                ("includedExpectations", included_raw),
                ("excludedExpectations", excluded_raw),
            ):
                if not isinstance(values, Sequence) or isinstance(
                    values,
                    (str, bytes),
                ):
                    raise ReferenceRuntimeError(f"{label}.{field_name} must be an array")
                for value in values:
                    _require_iri(value, f"{label}.{field_name}")
            included = set(cast(Sequence[str], included_raw))
            excluded = set(cast(Sequence[str], excluded_raw))
            if included & excluded or included | excluded != expectation_ids:
                raise ReferenceRuntimeError(
                    f"{kind}: included and excluded expectations must partition the sealed expectations"
                )
            if kind == "reachableRegisteredCandidateRecall":
                if not_represented & included or not not_represented.issubset(excluded):
                    raise ReferenceRuntimeError(
                        "notRepresented expectations must be excluded from reachable registered-candidate recall"
                    )
            elif not not_represented.issubset(included):
                raise ReferenceRuntimeError(f"notRepresented expectations must remain in {kind} measures")
        if set(populations) != {
            "reachableRegisteredCandidateRecall",
            "targetAvailability",
            "openSet",
        }:
            raise ReferenceRuntimeError("evaluation must report all three notRepresented measure populations")

        failed_gates = [str(gate.get("id") or "unnamed") for gate in self.gates if gate.get("passed") is not True]
        failed_strata = [
            str(stratum.get("stratum") or "unnamed")
            for stratum in self.configured_strata
            if stratum.get("passed") is not True
            or int(stratum.get("observedSampleSize") or 0) < int(stratum.get("minimumSampleSize") or 0)
        ]
        if self.verdict == "pass" and (failed_gates or failed_strata):
            raise ReferenceRuntimeError(
                "a passing evaluation has failed gates or strata: " + ", ".join([*failed_gates, *failed_strata])
            )
        if self.verdict == "pass" and threshold_failures:
            raise ReferenceRuntimeError(
                "a passing evaluation misses thresholds for: " + ", ".join(sorted(threshold_failures))
            )
        if self.verdict == "pass" and gate_dimensions != EVALUATION_GATE_DIMENSIONS:
            missing = sorted(EVALUATION_GATE_DIMENSIONS - gate_dimensions)
            raise ReferenceRuntimeError(
                "a passing evaluation must report every core gate dimension; missing: " + ", ".join(missing)
            )
        _require_iri(self.evaluator, "evaluator")
        _require_iri(self.activity, "activity")
        _require_datetime(self.evaluated_at, "evaluatedAt")
        for digest in self.output_artifact_digests:
            _require_digest(digest, "outputArtifactDigest")
        return {
            **base,
            "configuration": dict(self.configuration),
            "sealedGoldManifest": dict(self.sealed_gold_manifest),
            "evaluationProtocol": dict(self.evaluation_protocol),
            "predeclaredMeasures": list(self.predeclared_measures),
            "thresholds": [dict(value) for value in self.thresholds],
            "configuredStrata": [dict(value) for value in self.configured_strata],
            "exclusions": list(self.exclusions),
            "uncertaintyMethod": dict(self.uncertainty_method),
            "observedMeasures": [_json_copy(value) for value in self.observed_measures],
            "measurePopulations": [_json_copy(value) for value in self.measure_populations],
            "gates": [_json_copy(value) for value in self.gates],
            "evaluator": self.evaluator,
            "activity": self.activity,
            "evaluatedAt": self.evaluated_at,
            "outputArtifactDigests": list(self.output_artifact_digests),
            "verdict": self.verdict,
        }

    def sealed_payload(
        self,
        *,
        configuration: EnrichmentConfiguration | None = None,
        gold_manifest: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return seal_payload(
            self.payload(
                configuration=configuration,
                gold_manifest=gold_manifest,
            )
        )

    @property
    def digest(self) -> str:
        return canonical_payload_digest(self.payload())


@dataclass(frozen=True)
class EnrichmentDeploymentDecision:
    decision_id: str
    recorded_at: str
    recorded_by: str
    operational_state: str
    environment: Mapping[str, str]
    configuration: Mapping[str, str]
    evaluation_result: Mapping[str, str]
    output_profile: Mapping[str, str]
    selection_state: str
    effective_at: str
    reason: str
    activity: str
    predecessor_decision: str | None
    superseding_decision: str | None
    rulespec_attestation_refs: tuple[str, ...]
    local_adoption_refs: tuple[str, ...]
    authorization_validations: tuple[Mapping[str, Any], ...]

    def payload(
        self,
        *,
        configuration: EnrichmentConfiguration | None = None,
        evaluation: EnrichmentEvaluationResult | None = None,
    ) -> dict[str, Any]:
        base = _record_base(
            record_id=self.decision_id,
            record_type="urn:ref:type:EnrichmentDeploymentDecision",
            recorded_at=self.recorded_at,
            recorded_by=self.recorded_by,
            operational_state=self.operational_state,
        )
        _require_exact_fields(
            self.environment,
            frozenset({"id", "classification"}),
            "environment",
        )
        _require_iri(self.environment.get("id"), "environment.id")
        if self.environment.get("classification") not in {
            "production",
            "nonProduction",
        }:
            raise ReferenceRuntimeError("environment.classification is invalid")
        _require_reference(
            self.configuration,
            "configuration",
            versioned=False,
        )
        _require_reference(
            self.evaluation_result,
            "evaluationResult",
            versioned=False,
        )
        _require_reference(
            self.output_profile,
            "outputProfile",
            versioned=True,
        )
        if self.selection_state not in {
            "staged",
            "selected",
            "deselected",
            "failed",
        }:
            raise ReferenceRuntimeError("unknown deployment selection state")
        if not self.rulespec_attestation_refs or not self.local_adoption_refs:
            raise ReferenceRuntimeError("deployment decisions require Rulespec authorization references")
        for value in (
            *self.rulespec_attestation_refs,
            *self.local_adoption_refs,
        ):
            _require_iri(value, "authorization reference")
        expected_authorizations = {
            **{value: "rulespecAttestation" for value in self.rulespec_attestation_refs},
            **{value: "localAdoption" for value in self.local_adoption_refs},
        }
        if len(expected_authorizations) != (len(self.rulespec_attestation_refs) + len(self.local_adoption_refs)):
            raise ReferenceRuntimeError("deployment authorization references must be distinct")
        validation_fields = frozenset(
            {
                "authorizationRef",
                "kind",
                "validationReceipt",
                "validator",
                "validatedAt",
                "effective",
            }
        )
        validated_authorizations: dict[str, str] = {}
        for index, raw in enumerate(self.authorization_validations):
            label = f"authorizationValidations[{index}]"
            value = _require_mapping(raw, label)
            _require_exact_fields(value, validation_fields, label)
            authorization_ref = _require_iri(
                value["authorizationRef"],
                f"{label}.authorizationRef",
            )
            kind = str(value["kind"])
            if kind not in {"rulespecAttestation", "localAdoption"}:
                raise ReferenceRuntimeError(f"{label}.kind is not a deployment governance kind")
            _require_reference(
                _require_mapping(
                    value["validationReceipt"],
                    f"{label}.validationReceipt",
                ),
                f"{label}.validationReceipt",
                versioned=False,
            )
            _require_iri(value["validator"], f"{label}.validator")
            _require_datetime(value["validatedAt"], f"{label}.validatedAt")
            if value["effective"] is not True:
                raise ReferenceRuntimeError(f"{label}.effective must be true")
            if authorization_ref in validated_authorizations:
                raise ReferenceRuntimeError("authorizationValidations contains a duplicate reference")
            validated_authorizations[authorization_ref] = kind
        if validated_authorizations != expected_authorizations:
            raise ReferenceRuntimeError(
                "authorizationValidations must exactly validate every deployment attestation and local adoption"
            )
        production_selection = self.selection_state == "selected" and self.environment["classification"] == "production"
        if production_selection and (configuration is None or evaluation is None):
            raise ReferenceRuntimeError(
                "production selection requires supplied exact configuration and evaluation records"
            )
        if configuration is not None:
            if self.configuration["id"] != configuration.configuration_id:
                raise ReferenceRuntimeError("deployment configuration identifier mismatch")
            if self.configuration["digest"] != configuration.digest:
                raise ReferenceRuntimeError("deployment configuration digest mismatch")
            if dict(self.output_profile) != dict(configuration.output_profile):
                raise ReferenceRuntimeError("deployment output-profile pin differs from configuration")
        if evaluation is not None:
            if self.evaluation_result["id"] != evaluation.result_id:
                raise ReferenceRuntimeError("deployment evaluation identifier mismatch")
            if self.evaluation_result["digest"] != evaluation.digest:
                raise ReferenceRuntimeError("deployment evaluation digest mismatch")
            if production_selection and evaluation.verdict != "pass":
                raise ReferenceRuntimeError("production selection requires a passing evaluation")
            if dict(evaluation.configuration) != dict(self.configuration):
                raise ReferenceRuntimeError("evaluated and deployed configuration pins differ")
        _require_datetime(self.effective_at, "effectiveAt")
        _require_text(self.reason, "reason")
        _require_iri(self.activity, "activity")
        result: dict[str, Any] = {
            **base,
            "environment": dict(self.environment),
            "configuration": dict(self.configuration),
            "evaluationResult": dict(self.evaluation_result),
            "outputProfile": dict(self.output_profile),
            "selectionState": self.selection_state,
            "effectiveAt": self.effective_at,
            "reason": self.reason,
            "activity": self.activity,
            "rulespecAttestationRefs": list(self.rulespec_attestation_refs),
            "localAdoptionRefs": list(self.local_adoption_refs),
            "authorizationValidations": [_json_copy(item) for item in self.authorization_validations],
        }
        if self.predecessor_decision is not None:
            result["predecessorDecision"] = _require_iri(
                self.predecessor_decision,
                "predecessorDecision",
            )
        if self.superseding_decision is not None:
            result["supersedingDecision"] = _require_iri(
                self.superseding_decision,
                "supersedingDecision",
            )
        return result

    def sealed_payload(
        self,
        *,
        configuration: EnrichmentConfiguration | None = None,
        evaluation: EnrichmentEvaluationResult | None = None,
    ) -> dict[str, Any]:
        return seal_payload(
            self.payload(
                configuration=configuration,
                evaluation=evaluation,
            )
        )


@dataclass(frozen=True)
class VocabularyUniverseFreeze:
    """Exact registry and mapping pins required before a holdout draw."""

    freeze_id: str
    registry_releases: tuple[Mapping[str, str], ...]
    mapping_releases: tuple[Mapping[str, str], ...]
    output_profile: Mapping[str, str]
    frozen_at: str
    frozen_by: str

    def payload(self) -> dict[str, Any]:
        _require_iri(self.freeze_id, "id")
        if not self.registry_releases:
            raise ReferenceRuntimeError("holdout vocabulary freeze requires registry releases")
        required_registry = {
            "release",
            "releaseDigest",
            "importSnapshot",
            "coverageReport",
            "coverageReportDigest",
        }
        required_mapping = {
            "release",
            "releaseDigest",
            "mappingSnapshot",
            "coverageReport",
            "coverageReportDigest",
        }
        for index, pin in enumerate(self.registry_releases):
            _require_exact_fields(
                pin,
                frozenset(required_registry),
                f"registryReleases[{index}]",
            )
            _require_iri(pin["release"], "registry release")
            _require_digest(pin["releaseDigest"], "registry release digest")
            _require_iri(pin["importSnapshot"], "registry import snapshot")
            _require_iri(pin["coverageReport"], "registry coverage report")
            _require_digest(
                pin["coverageReportDigest"],
                "registry coverage report digest",
            )
        for index, pin in enumerate(self.mapping_releases):
            _require_exact_fields(
                pin,
                frozenset(required_mapping),
                f"mappingReleases[{index}]",
            )
            _require_iri(pin["release"], "mapping release")
            _require_digest(pin["releaseDigest"], "mapping release digest")
            _require_iri(pin["mappingSnapshot"], "mapping snapshot")
            _require_iri(pin["coverageReport"], "mapping coverage report")
            _require_digest(
                pin["coverageReportDigest"],
                "mapping coverage report digest",
            )
        for key in ("id", "version", "digest"):
            _require_text(self.output_profile.get(key), f"outputProfile.{key}")
        _require_digest(
            self.output_profile["digest"],
            "outputProfile.digest",
        )
        _require_datetime(self.frozen_at, "frozenAt")
        _require_iri(self.frozen_by, "frozenBy")
        return {
            "id": self.freeze_id,
            "type": "urn:spicy-regs:type:VocabularyUniverseFreeze",
            "registryReleases": [dict(value) for value in self.registry_releases],
            "mappingReleases": [dict(value) for value in self.mapping_releases],
            "outputProfile": dict(self.output_profile),
            "frozenAt": self.frozen_at,
            "frozenBy": self.frozen_by,
            "canonicalizationPolicy": CANONICAL_JSON_POLICY,
        }

    def sealed_payload(self) -> dict[str, Any]:
        return seal_payload(self.payload())

    @property
    def digest(self) -> str:
        return canonical_payload_digest(self.payload())


def require_vocabulary_universe_freeze(
    payload: Mapping[str, Any],
) -> None:
    """Validate the exact sealed vocabulary universe used for a holdout draw."""
    require_payload_digest(payload)
    required = frozenset(
        {
            "id",
            "type",
            "registryReleases",
            "mappingReleases",
            "outputProfile",
            "frozenAt",
            "frozenBy",
            "canonicalizationPolicy",
            "canonicalPayloadDigest",
        }
    )
    _require_exact_fields(payload, required, "vocabularyUniverseFreeze")
    if payload.get("canonicalizationPolicy") != CANONICAL_JSON_POLICY:
        raise ReferenceRuntimeError("holdout vocabulary freeze uses an unknown canonical JSON policy")
    freeze = VocabularyUniverseFreeze(
        freeze_id=str(payload["id"]),
        registry_releases=tuple(payload["registryReleases"]),
        mapping_releases=tuple(payload["mappingReleases"]),
        output_profile=dict(payload["outputProfile"]),
        frozen_at=str(payload["frozenAt"]),
        frozen_by=str(payload["frozenBy"]),
    )
    freeze.payload()


def bind_ranked_candidates(
    ranked_concept_ids: Sequence[str],
    *,
    channel: str,
    concept_catalog: Mapping[str, Mapping[str, Any]],
    expression_ids_by_concept: Mapping[str, Sequence[str]],
) -> list[dict[str, Any]]:
    """Bind existing candidate-channel results to exact vocabulary lineage."""
    _require_text(channel, "channel")
    bound: list[dict[str, Any]] = []
    for rank, concept_id in enumerate(ranked_concept_ids, start=1):
        catalog = concept_catalog.get(concept_id)
        if catalog is None:
            raise ReferenceRuntimeError(f"candidate {concept_id!r} is absent from the pinned catalog")
        expressions = tuple(expression_ids_by_concept.get(concept_id, ()))
        if not expressions:
            raise ReferenceRuntimeError(f"candidate {concept_id!r} has no indexed expression lineage")
        required = (
            "conceptIri",
            "schemeIri",
            "facet",
            "referenceResourceRelease",
            "registryImportSnapshot",
            "indexSnapshot",
        )
        for key in required:
            _require_text(catalog.get(key), f"catalog[{concept_id}].{key}")
        bound.append(
            {
                "conceptId": concept_id,
                **{key: catalog[key] for key in required},
                "indexedExpressionIds": list(expressions),
                "channel": channel,
                "rank": rank,
            }
        )
    return bound


@dataclass(frozen=True)
class LegacyMigrationBatch:
    """Quarantined normalized rows read from a legacy flat registry."""

    labels: tuple[ConceptLabel, ...]
    relations: tuple[ConceptRelation, ...]
    participants: tuple[ConceptEventParticipant, ...]
    source_authority: str
    production_eligible: bool = False
    warnings: tuple[str, ...] = ()

    def assert_not_production(self) -> None:
        if self.production_eligible:
            raise ReferenceRuntimeError("legacy migration batches can never be production eligible")


def adapt_source_terms_for_migration(
    terms: Sequence[Any],
    *,
    term_identities: Sequence[Mapping[str, Any]],
    release_iri: str,
    import_snapshot_id: str,
    distribution_artifact_id: str,
) -> LegacyMigrationBatch:
    """Preserve existing ``SourceTerm`` parser output in quarantined rows.

    ``tools/fuse_concept_registries.py`` parsers remain useful source readers,
    but ``SourceTerm`` has already lost source language and, for some inputs,
    native parent identifiers.  The caller must therefore supply exact concept
    and scheme IRIs, a language for every available label, and an exact parent
    identity for every ``broader_labels`` entry.  The result always remains
    migration/development-only; a native re-import and passing coverage report
    are required before production use.
    """
    _require_iri(release_iri, "release_iri")
    _require_text(import_snapshot_id, "import_snapshot_id")
    _require_text(distribution_artifact_id, "distribution_artifact_id")
    if len(terms) != len(term_identities):
        raise ReferenceRuntimeError("every SourceTerm requires one externally supplied identity record")
    labels: list[ConceptLabel] = []
    relations: list[ConceptRelation] = []
    warnings = [
        "SourceTerm parser output is lossy and remains migration-only",
        "native source re-import and coverage reconciliation are required",
    ]
    for term_index, (term, identity) in enumerate(zip(terms, term_identities, strict=True)):
        _require_exact_fields(
            identity,
            frozenset(
                {
                    "conceptIri",
                    "schemeIri",
                    "prefLabelLanguage",
                    "altLabelLanguages",
                    "broaderConcepts",
                }
            ),
            f"termIdentities[{term_index}]",
        )
        concept_iri = _require_iri(
            identity["conceptIri"],
            f"termIdentities[{term_index}].conceptIri",
        )
        scheme_iri = _require_iri(
            identity["schemeIri"],
            f"termIdentities[{term_index}].schemeIri",
        )
        preferred = _require_text(
            getattr(term, "pref_label", None),
            f"terms[{term_index}].pref_label",
        )
        pref_language = _require_language_tag(
            identity["prefLabelLanguage"],
            f"termIdentities[{term_index}].prefLabelLanguage",
        )
        alt_language_map = identity["altLabelLanguages"]
        if not isinstance(alt_language_map, Mapping):
            raise ReferenceRuntimeError(f"termIdentities[{term_index}].altLabelLanguages must be an object")
        alt_values: list[str] = []
        for value in (
            *getattr(term, "alt_labels", ()),
            *getattr(term, "dropped_alt_labels", ()),
        ):
            literal = str(value).strip()
            if literal and literal not in alt_values and literal != preferred:
                alt_values.append(literal)
        missing_alt_languages = [value for value in alt_values if value not in alt_language_map]
        if missing_alt_languages:
            raise ReferenceRuntimeError(
                f"term {term_index} lacks authoritative language tags for "
                + ", ".join(repr(value) for value in missing_alt_languages)
            )
        authored = [
            ("preferred", preferred, pref_language),
            *[
                (
                    "alternate",
                    value,
                    _require_language_tag(
                        alt_language_map[value],
                        f"term {term_index} alternate language",
                    ),
                )
                for value in alt_values
            ],
        ]
        source_status = str(getattr(term, "source_status", "") or "").casefold()
        status = "deprecated" if source_status in {"deprecated", "inactive", "withdrawn"} else "current"
        for label_index, (role, literal, language) in enumerate(authored):
            labels.append(
                ConceptLabel(
                    label_id=stable_id(
                        "source_term_label",
                        concept_iri,
                        role,
                        label_index,
                        literal,
                        language,
                    ),
                    concept_iri=concept_iri,
                    scheme_iri=scheme_iri,
                    release_iri=release_iri,
                    import_snapshot_id=import_snapshot_id,
                    distribution_artifact_id=distribution_artifact_id,
                    source_property_iri={
                        "preferred": "http://www.w3.org/2004/02/skos/core#prefLabel",
                        "alternate": "http://www.w3.org/2004/02/skos/core#altLabel",
                    }[role],
                    label_role=role,
                    original_literal=literal,
                    language_tag=language,
                    status=status,
                    migration_only=True,
                )
            )
        broader_labels = [str(value).strip() for value in getattr(term, "broader_labels", ()) if str(value).strip()]
        broader_concepts = identity["broaderConcepts"]
        if not isinstance(broader_concepts, Sequence):
            raise ReferenceRuntimeError(f"termIdentities[{term_index}].broaderConcepts must be an array")
        supplied_by_label: dict[str, Mapping[str, Any]] = {}
        for parent in broader_concepts:
            if not isinstance(parent, Mapping):
                raise ReferenceRuntimeError("broaderConcepts entries must be objects")
            _require_exact_fields(
                parent,
                frozenset({"sourceLabel", "conceptIri", "schemeIri"}),
                f"termIdentities[{term_index}].broaderConcept",
            )
            source_label = _require_text(
                parent["sourceLabel"],
                "broaderConcept.sourceLabel",
            )
            if source_label in supplied_by_label:
                raise ReferenceRuntimeError(f"duplicate supplied parent label {source_label!r}")
            supplied_by_label[source_label] = parent
        if set(broader_labels) != set(supplied_by_label):
            raise ReferenceRuntimeError(f"term {term_index} broader labels lack exact supplied identities")
        for parent_index, source_label in enumerate(broader_labels):
            parent = supplied_by_label[source_label]
            parent_scheme = _require_iri(
                parent["schemeIri"],
                "broaderConcept.schemeIri",
            )
            relations.append(
                ConceptRelation(
                    relation_id=stable_id(
                        "source_term_relation",
                        concept_iri,
                        source_label,
                        parent["conceptIri"],
                        parent_index,
                    ),
                    release_iri=release_iri,
                    import_snapshot_id=import_snapshot_id,
                    distribution_artifact_id=distribution_artifact_id,
                    subject_concept_iri=concept_iri,
                    subject_scheme_iri=scheme_iri,
                    predicate_iri="http://www.w3.org/2004/02/skos/core#broader",
                    object_concept_iri=_require_iri(
                        parent["conceptIri"],
                        "broaderConcept.conceptIri",
                    ),
                    object_scheme_iri=parent_scheme,
                    source_property_or_path="SourceTerm.broader_labels",
                    migration_only=True,
                )
            )
    batch = LegacyMigrationBatch(
        labels=tuple(labels),
        relations=tuple(relations),
        participants=(),
        source_authority="sourceTermParserOutput",
        warnings=tuple(warnings),
    )
    batch.assert_not_production()
    return batch


def _legacy_iri(
    value: object,
    *,
    namespace: str,
) -> str:
    text = _require_text(value, "legacy identity")
    if urlsplit(text).scheme:
        return text
    # This is a quarantine identifier, not a newly minted registered concept.
    return f"{namespace}{text}"


def migrate_legacy_concepts(
    rows: Sequence[Mapping[str, Any]],
    *,
    release_iri: str,
    scheme_iri: str,
    import_snapshot_id: str,
    distribution_artifact_id: str,
    default_language: str,
    source_authority: str = "legacyFusedRegistry",
) -> LegacyMigrationBatch:
    """Read flat rows into quarantined normalized tables.

    The adapter preserves legacy identifiers and its recoverable single parent;
    it does not claim missing languages, aliases, or additional parents were
    recovered.  Every output row remains ``migration_only`` and therefore fails
    :func:`assert_conforming_vocabulary_rows`.
    """
    _require_iri(release_iri, "release_iri")
    _require_iri(scheme_iri, "scheme_iri")
    language = _require_language_tag(default_language, "default_language")
    namespace = "urn:spicy-regs:legacy-concept:"
    by_legacy_id: dict[str, str] = {}
    for row in rows:
        legacy_id = _require_text(row.get("concept_id"), "legacy concept_id")
        by_legacy_id[legacy_id] = _legacy_iri(
            legacy_id,
            namespace=namespace,
        )
    labels: list[ConceptLabel] = []
    relations: list[ConceptRelation] = []
    warnings: list[str] = ["legacy flat rows are quarantined and cannot authorize conforming output"]
    for row in rows:
        legacy_id = _require_text(row.get("concept_id"), "legacy concept_id")
        concept_iri = by_legacy_id[legacy_id]
        status = "deprecated" if str(row.get("status") or "") == "deprecated" else "current"
        authored: list[tuple[str, str]] = []
        preferred = str(row.get("pref_label") or "").strip()
        if preferred:
            authored.append(("preferred", preferred))
        for column, role in (
            ("alt_labels_json", "alternate"),
            ("hidden_labels_json", "hidden"),
        ):
            values = parse_json_list(row.get(column))
            if values is None:
                warnings.append(f"{legacy_id}: malformed {column} was not migrated")
                continue
            authored.extend((role, str(value).strip()) for value in values if str(value).strip())
        for ordinal, (role, literal) in enumerate(authored):
            label_id = stable_id(
                "legacy_label",
                legacy_id,
                role,
                ordinal,
                literal,
            )
            labels.append(
                ConceptLabel(
                    label_id=label_id,
                    concept_iri=concept_iri,
                    scheme_iri=scheme_iri,
                    release_iri=release_iri,
                    import_snapshot_id=import_snapshot_id,
                    distribution_artifact_id=distribution_artifact_id,
                    source_property_iri={
                        "preferred": "http://www.w3.org/2004/02/skos/core#prefLabel",
                        "alternate": "http://www.w3.org/2004/02/skos/core#altLabel",
                        "hidden": "http://www.w3.org/2004/02/skos/core#hiddenLabel",
                    }[role],
                    label_role=role,
                    original_literal=literal,
                    language_tag=language,
                    status=status,
                    migration_only=True,
                )
            )
        broader_id = str(row.get("broader_id") or "").strip()
        if broader_id:
            parent_iri = by_legacy_id.get(broader_id)
            if parent_iri is None:
                warnings.append(f"{legacy_id}: unresolved broader_id {broader_id!r}")
            else:
                relations.append(
                    ConceptRelation(
                        relation_id=stable_id(
                            "legacy_relation",
                            legacy_id,
                            "broader",
                            broader_id,
                        ),
                        release_iri=release_iri,
                        import_snapshot_id=import_snapshot_id,
                        distribution_artifact_id=distribution_artifact_id,
                        subject_concept_iri=concept_iri,
                        subject_scheme_iri=scheme_iri,
                        predicate_iri="http://www.w3.org/2004/02/skos/core#broader",
                        object_concept_iri=parent_iri,
                        object_scheme_iri=scheme_iri,
                        source_property_or_path="legacy.broader_id",
                        migration_only=True,
                    )
                )
        if row.get("replaced_by"):
            warnings.append(f"{legacy_id}: replaced_by requires source lifecycle reconstruction")
    batch = LegacyMigrationBatch(
        labels=tuple(labels),
        relations=tuple(relations),
        participants=(),
        source_authority=source_authority,
        warnings=tuple(warnings),
    )
    batch.assert_not_production()
    return batch


def reject_legacy_conforming_payload(payload: Mapping[str, Any]) -> None:
    """Reject legacy columns, policies, or fused authority recursively."""

    def walk(value: object, path: str) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                key_text = str(key)
                if key_text in _LEGACY_FIELDS:
                    raise ReferenceRuntimeError(f"{path}.{key_text} is a legacy read-only field")
                if (
                    key_text in {"normalizationPolicy", "normalizationPolicyId"}
                    and str(child) in _LEGACY_NORMALIZATION_POLICIES
                ):
                    raise ReferenceRuntimeError("ASCII-only normalization is not conforming")
                if (
                    key_text in {"authority", "registryAuthority", "sourceAuthority"}
                    and str(child) in _LEGACY_AUTHORITIES
                ):
                    raise ReferenceRuntimeError("the fused registry is not production authority")
                walk(child, f"{path}.{key_text}")
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(payload, "$")


@dataclass
class ReferenceRuntimeStore:
    """Small immutable file store for normalized rows and REF records."""

    root: Path

    def write_vocabulary_rows(
        self,
        *,
        labels: Sequence[ConceptLabel],
        relations: Sequence[ConceptRelation],
        participants: Sequence[ConceptEventParticipant],
        release_membership: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> Mapping[str, Path]:
        assert_conforming_vocabulary_rows(
            labels,
            relations,
            participants,
            release_membership=release_membership,
        )
        self.root.mkdir(parents=True, exist_ok=True)
        paths = {
            "concept_labels": self.root / "concept_labels.parquet",
            "concept_relations": self.root / "concept_relations.parquet",
            "concept_event_participants": (self.root / "concept_event_participants.parquet"),
        }
        write_parquet_rows(
            paths["concept_labels"],
            columns=CONCEPT_LABEL_COLUMNS,
            rows=(row.to_row() for row in labels),
        )
        write_parquet_rows(
            paths["concept_relations"],
            columns=CONCEPT_RELATION_COLUMNS,
            rows=(row.to_row() for row in relations),
        )
        write_parquet_rows(
            paths["concept_event_participants"],
            columns=CONCEPT_EVENT_PARTICIPANT_COLUMNS,
            rows=(row.to_row() for row in participants),
        )
        return paths

    def read_vocabulary_rows(self) -> Mapping[str, list[dict[str, Any]]]:
        rows = {
            name: read_parquet_rows(self.root / f"{name}.parquet")
            for name in (
                "concept_labels",
                "concept_relations",
                "concept_event_participants",
            )
        }
        for name in (
            "concept_labels",
            "concept_relations",
            "concept_event_participants",
        ):
            for row in rows[name]:
                migration = str(row.get("migration_only") or "").casefold()
                if migration not in {"true", "false"}:
                    raise ReferenceRuntimeError(f"{name}.migration_only must be true or false")
                row["migration_only"] = migration == "true"
        for row in rows["concept_event_participants"]:
            complete = str(row.get("complete_membership") or "").casefold()
            if complete not in {"true", "false"}:
                raise ReferenceRuntimeError("concept_event_participants.complete_membership must be true or false")
            row["complete_membership"] = complete == "true"
            ordinal = str(row.get("ordinal") or "").strip()
            if not ordinal.isdigit():
                raise ReferenceRuntimeError("concept_event_participants.ordinal must be a non-negative integer")
            row["ordinal"] = int(ordinal)
        return rows

    def put_record(
        self,
        record_type: str,
        payload: Mapping[str, Any],
        *,
        binding_validator: Callable[
            [Sequence[Mapping[str, Any]]],
            None,
        ]
        | None = None,
        linked_records: Sequence[Mapping[str, Any]] = (),
        digest_field: str | None = None,
    ) -> Path:
        """Persist one validated, content-addressed immutable REF record.

        The explicit validator receives the record plus every linked record
        needed for REF JSON Binding graph checks.  Requiring it here prevents a
        digest-correct but schema-invalid mapping from entering the immutable
        store.
        """
        expected_type = REF_RECORD_TYPES.get(record_type)
        if expected_type is None:
            raise ReferenceRuntimeError("record_type must name a supported REF JSON Binding record")
        actual_type = _require_iri(payload.get("type"), "record.type")
        if actual_type != expected_type:
            raise ReferenceRuntimeError(
                f"record_type {record_type!r} requires payload type {expected_type!r}, got {actual_type!r}"
            )
        if binding_validator is None:
            raise ReferenceRuntimeError("put_record requires an explicit REF JSON Binding validator")
        reject_legacy_conforming_payload(payload)
        require_payload_digest(payload, digest_field=digest_field)
        bundle = (*linked_records, payload)
        try:
            binding_validator(bundle)
        except ReferenceRuntimeError:
            raise
        except Exception as exc:
            raise ReferenceRuntimeError(f"REF JSON Binding validator rejected the record bundle: {exc}") from exc
        record_id = _require_text(payload.get("id"), "record.id")
        resolved_digest_field = digest_field or (
            "contentDigest"
            if payload.get("type")
            in {
                "urn:ref:type:EnrichmentProfile",
                "urn:ref:type:OutputProfile",
            }
            else "canonicalPayloadDigest"
        )
        digest = str(payload[resolved_digest_field]).removeprefix("sha256:")
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", record_id)
        directory = self.root / "records" / record_type
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{safe_id}-{digest}.json"
        encoded = (
            json.dumps(
                dict(payload),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        )
        if path.exists():
            if path.read_text(encoding="utf-8") != encoded:
                raise ReferenceRuntimeError(f"immutable record path already contains different bytes: {path}")
            return path
        path.write_text(encoded, encoding="utf-8")
        return path

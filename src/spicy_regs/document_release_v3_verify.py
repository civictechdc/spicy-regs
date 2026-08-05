"""Producer-side complete-distribution verifier for ``DocumentRelease`` v3."""

from __future__ import annotations

import hashlib
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, cast

import duckdb
import jsonschema
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.document_release_v3 import (
    ALLOWED_MEMBER_ROLES,
    BUILD_RECEIPT_SCHEMA_ID,
    CANONICAL_JSON_PROFILE,
    CONTENT_KEYS,
    COUNT_FIELDS,
    COVERAGE_FIELDS,
    DIAGNOSTIC_CODE_PATTERN,
    ELIGIBILITY_DATA_SCHEMA_ID,
    FORMAT,
    FORMAT_VERSION,
    MANIFEST_REFERENCE_KEYS,
    MEMBER_KEYS,
    MEMBER_MANIFEST_FORMAT,
    MEMBER_MANIFEST_VERSION,
    RELEASE_ID_PREFIX,
    RENDITION_UTF8_COORDINATE_SCHEMA_ID,
    ROLE_SCHEMA_IDS,
    ROOT_KEYS,
    SCHEMA_SET_ID_PREFIX,
    SUBORDINATE_MANIFEST_KEYS,
    TABLE_SCHEMAS,
    DocumentReleaseV3Error,
    DocumentReleaseV3VerificationError,
    VerificationCode,
    artifact_digest,
    canonical_json_bytes,
    fail,
    parse_canonical_json,
    require_non_empty_string,
    require_memory_limit,
    require_safe_unsigned_integer,
    require_sha256,
    schema_documents,
    schema_set_id,
    sha256_file,
    validate_object_key,
)


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Machine-readable result from one complete release verification."""

    verdict: str
    code: VerificationCode
    release_id: str | None
    artifact_digest: str | None
    diagnostics: tuple[str, ...]
    counts: Mapping[str, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "verificationCode": self.code.value,
            "releaseId": self.release_id,
            "artifactDigest": self.artifact_digest,
            "diagnostics": list(self.diagnostics),
            "counts": dict(self.counts),
        }


@dataclass(frozen=True, slots=True)
class _Member:
    object_key: str
    role: str
    media_type: str
    byte_size: int
    sha256: str
    record_count: int | None
    schema_id: str | None
    partition_id: str | None
    serving_shard_id: int | None
    scope_kind: str
    scope_id: str


def _schema_failure(error: Exception | str) -> NoReturn:
    fail(VerificationCode.SCHEMA, str(error))


def _path_failure(error: Exception | str) -> NoReturn:
    fail(VerificationCode.PATH, str(error))


def _closed_object(value: object, keys: set[str] | frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _schema_failure(f"{label} must be an object")
    assert isinstance(value, dict)
    actual = set(value)
    if actual != set(keys):
        _schema_failure(
            f"{label} keys differ; missing={sorted(set(keys) - actual)}, unexpected={sorted(actual - set(keys))}"
        )
    return cast(dict[str, Any], value)


def _safe_uint(value: object, label: str) -> int:
    try:
        return require_safe_unsigned_integer(value, label)
    except DocumentReleaseV3Error as error:
        _schema_failure(error)


def _string(value: object, label: str) -> str:
    try:
        return require_non_empty_string(value, label)
    except DocumentReleaseV3Error as error:
        _schema_failure(error)


def _digest(value: object, label: str) -> str:
    try:
        return require_sha256(value, label)
    except DocumentReleaseV3Error as error:
        _schema_failure(error)


def _key(value: object, label: str) -> str:
    try:
        return validate_object_key(value, label)
    except DocumentReleaseV3Error as error:
        _path_failure(error)


def _materialized_path(root: Path, key: str) -> Path:
    path = root.joinpath(*key.split("/"))
    current = root
    for part in key.split("/"):
        current = current / part
        if current.is_symlink():
            _path_failure(f"declared path uses a symlink: {key}")
    if not path.exists():
        fail(VerificationCode.MEMBERSHIP_MISSING, f"declared member is missing: {key}")
    if not path.is_file():
        _path_failure(f"declared member is not a regular file: {key}")
    return path


def _load_root(root: Path) -> Mapping[str, Any]:
    release_path = root / "release.json"
    if release_path.is_symlink():
        _path_failure("release.json must not be a symlink")
    if not release_path.is_file():
        fail(VerificationCode.MEMBERSHIP_MISSING, "release.json is missing")
    try:
        value = parse_canonical_json(release_path.read_bytes(), label="release.json")
    except DocumentReleaseV3Error as error:
        fail(VerificationCode.ROOT_SYNTAX, str(error))
    if not isinstance(value, dict):
        fail(VerificationCode.ROOT_SYNTAX, "release.json root must be an object")
    if value.get("format") != FORMAT or value.get("formatVersion") != FORMAT_VERSION:
        fail(
            VerificationCode.FORMAT,
            f"unsupported release format/version: {value.get('format')!r} {value.get('formatVersion')!r}",
        )
    content = value.get("content")
    if not isinstance(content, dict):
        _schema_failure("release content must be an object")
    expected_digest = artifact_digest(content)
    expected_id = RELEASE_ID_PREFIX + expected_digest
    if value.get("releaseId") != expected_id:
        fail(VerificationCode.IDENTITY, f"releaseId must be {expected_id}")
    _closed_object(value, ROOT_KEYS, "release root")
    _closed_object(content, CONTENT_KEYS, "release content")
    if not isinstance(value.get("annotations"), dict):
        _schema_failure("annotations must be an object")
    return value


def _validate_reference(value: object, *, expected_kind: str, expected_id: str | None = None) -> Mapping[str, Any]:
    reference = _closed_object(value, MANIFEST_REFERENCE_KEYS, "manifest reference")
    manifest_id = _string(reference["manifestId"], "manifestId")
    scope_kind = _string(reference["scopeKind"], "scopeKind")
    scope_id = _string(reference["scopeId"], "scopeId")
    if scope_kind not in {"global", "partition", "serving-shard", "semantic-tier"}:
        _schema_failure(f"unknown scopeKind: {scope_kind!r}")
    if scope_kind != expected_kind:
        _schema_failure(f"manifest scope must be {expected_kind}, got {scope_kind}")
    if expected_id is not None and scope_id != expected_id:
        _schema_failure(f"manifest scope id must be {expected_id!r}, got {scope_id!r}")
    if manifest_id != f"{scope_kind}:{scope_id}":
        _schema_failure("manifestId does not match scopeKind + ':' + scopeId")
    _key(reference["objectKey"], "manifest objectKey")
    _safe_uint(reference["byteSize"], "manifest byteSize")
    _digest(reference["sha256"], "manifest sha256")
    return reference


def _validate_root_content(root: Mapping[str, Any]) -> None:
    content = root["content"]
    assert isinstance(content, dict)
    producer = _closed_object(
        content["producer"],
        {"product", "implementationId", "implementationVersion", "sourceRevision", "runtimeProfileId"},
        "producer",
    )
    if producer["product"] != "spicyregs":
        _schema_failure("producer.product must be spicyregs")
    for field in ("implementationId", "implementationVersion", "runtimeProfileId"):
        _string(producer[field], f"producer.{field}")
    if producer["sourceRevision"] is not None:
        _string(producer["sourceRevision"], "producer.sourceRevision")

    policy = _closed_object(
        content["processingPolicy"],
        {
            "processingPolicyId",
            "normalizerId",
            "segmenterId",
            "renditionPolicyId",
            "eligibilityPolicyId",
            "failurePolicyId",
            "diagnosticRegistryId",
            "canonicalJsonProfile",
        },
        "processingPolicy",
    )
    for field, value in policy.items():
        _string(value, f"processingPolicy.{field}")
    if policy["canonicalJsonProfile"] != CANONICAL_JSON_PROFILE:
        _schema_failure("processingPolicy.canonicalJsonProfile is unsupported")

    selection = _closed_object(
        content["sourceSelection"],
        {"selectionId", "selectorType", "selectorDigest", "inventoryDigest", "effectiveAt", "selectedDocumentCount"},
        "sourceSelection",
    )
    _string(selection["selectionId"], "sourceSelection.selectionId")
    _string(selection["selectorType"], "sourceSelection.selectorType")
    _digest(selection["selectorDigest"], "sourceSelection.selectorDigest")
    _digest(selection["inventoryDigest"], "sourceSelection.inventoryDigest")
    effective_at = _string(selection["effectiveAt"], "sourceSelection.effectiveAt")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", effective_at) is None:
        _schema_failure("sourceSelection.effectiveAt must be an RFC 3339 UTC instant")
    _safe_uint(selection["selectedDocumentCount"], "sourceSelection.selectedDocumentCount")

    previous = content["previousRelease"]
    if previous is not None:
        previous_object = _closed_object(previous, {"releaseId", "artifactDigest"}, "previousRelease")
        previous_id = _string(previous_object["releaseId"], "previousRelease.releaseId")
        previous_digest = _digest(previous_object["artifactDigest"], "previousRelease.artifactDigest")
        if previous_id != RELEASE_ID_PREFIX + previous_digest:
            _schema_failure("previousRelease identity and digest disagree")

    _validate_reference(content["globalManifest"], expected_kind="global", expected_id="global")
    partitions = content["partitionManifests"]
    if not isinstance(partitions, list) or not partitions:
        _schema_failure("partitionManifests must be a non-empty array")
    validated_partitions = [_validate_reference(value, expected_kind="partition") for value in partitions]
    manifest_ids = [value["manifestId"] for value in validated_partitions]
    if manifest_ids != sorted(manifest_ids) or len(set(manifest_ids)) != len(manifest_ids):
        _schema_failure("partitionManifests must be sorted by distinct manifestId")

    counts = _closed_object(content["counts"], set(COUNT_FIELDS), "counts")
    for field in COUNT_FIELDS:
        _safe_uint(counts[field], f"counts.{field}")
    coverage = _closed_object(content["coverage"], set(COVERAGE_FIELDS), "coverage")
    for field in COVERAGE_FIELDS:
        _safe_uint(coverage[field], f"coverage.{field}")
    if (
        coverage["eligibleActiveDocumentCount"]
        + coverage["ineligibleActiveDocumentCount"]
        + coverage["unverifiedActiveDocumentCount"]
        != counts["activeDocumentCount"]
    ):
        _schema_failure("coverage eligibility counts do not partition activeDocumentCount")


def _parse_member(value: object, *, scope_kind: str, scope_id: str) -> _Member:
    member = _closed_object(value, MEMBER_KEYS, "member descriptor")
    object_key = _key(member["objectKey"], "member objectKey")
    role = _string(member["role"], "member role")
    if role not in ALLOWED_MEMBER_ROLES:
        _schema_failure(f"unknown member role: {role!r}")
    media_type = _string(member["mediaType"], "member mediaType")
    byte_size = _safe_uint(member["byteSize"], "member byteSize")
    digest = _digest(member["sha256"], "member sha256")
    record_count_value = member["recordCount"]
    record_count = None if record_count_value is None else _safe_uint(record_count_value, "member recordCount")
    schema_id_value = member["schemaId"]
    schema_id = None if schema_id_value is None else _string(schema_id_value, "member schemaId")
    partition_id_value = member["partitionId"]
    partition_id = None if partition_id_value is None else _string(partition_id_value, "member partitionId")
    serving_shard_value = member["servingShardId"]
    serving_shard_id = None if serving_shard_value is None else _safe_uint(serving_shard_value, "member servingShardId")
    if serving_shard_id is not None:
        _schema_failure("DocumentRelease member servingShardId must be null")
    if scope_kind == "partition":
        if partition_id != scope_id:
            _schema_failure(f"partition member {object_key!r} must declare partitionId={scope_id!r}")
    elif partition_id is not None:
        _schema_failure(f"global member {object_key!r} must have null partitionId")
    expected_schema_id = ROLE_SCHEMA_IDS.get(role)
    if expected_schema_id is not None and schema_id != expected_schema_id:
        _schema_failure(f"member role {role!r} must use schemaId {expected_schema_id!r}")
    if role in {"schema", "rendition", "rendition-pack"} and schema_id is not None:
        _schema_failure(f"member role {role!r} must have null schemaId")
    if role in {"rendition", "rendition-pack"} and record_count is not None:
        _schema_failure(f"member role {role!r} must have null recordCount")
    if role not in {"rendition", "rendition-pack"} and record_count is None:
        _schema_failure(f"record member role {role!r} must declare recordCount")
    return _Member(
        object_key=object_key,
        role=role,
        media_type=media_type,
        byte_size=byte_size,
        sha256=digest,
        record_count=record_count,
        schema_id=schema_id,
        partition_id=partition_id,
        serving_shard_id=serving_shard_id,
        scope_kind=scope_kind,
        scope_id=scope_id,
    )


def _load_subordinate_manifest(root: Path, reference: Mapping[str, Any]) -> tuple[list[_Member], str]:
    key = str(reference["objectKey"])
    path = _materialized_path(root, key)
    digest, size = sha256_file(path)
    if size != reference["byteSize"] or digest != reference["sha256"]:
        fail(VerificationCode.MEMBER_DIGEST, f"subordinate manifest digest or size differs: {key}")
    try:
        value = parse_canonical_json(path.read_bytes(), label=key)
    except DocumentReleaseV3Error as error:
        _schema_failure(error)
    manifest = _closed_object(value, SUBORDINATE_MANIFEST_KEYS, key)
    if manifest["format"] != MEMBER_MANIFEST_FORMAT or manifest["formatVersion"] != MEMBER_MANIFEST_VERSION:
        _schema_failure(f"unsupported subordinate manifest format: {key}")
    scope = _closed_object(manifest["scope"], {"kind", "id"}, f"{key}.scope")
    if scope["kind"] != reference["scopeKind"] or scope["id"] != reference["scopeId"]:
        _schema_failure(f"subordinate manifest scope differs from reference: {key}")
    if manifest["manifestId"] != reference["manifestId"]:
        _schema_failure(f"subordinate manifest id differs from reference: {key}")
    members_value = manifest["members"]
    if not isinstance(members_value, list):
        _schema_failure(f"{key}.members must be an array")
    members = [
        _parse_member(value, scope_kind=str(scope["kind"]), scope_id=str(scope["id"])) for value in members_value
    ]
    member_keys = [member.object_key for member in members]
    if member_keys != sorted(member_keys) or len(set(member_keys)) != len(member_keys):
        _schema_failure(f"{key}.members must be sorted by distinct objectKey")
    counts = _closed_object(manifest["counts"], {"memberCount", "totalByteSize", "totalRecordCount"}, f"{key}.counts")
    expected_counts = {
        "memberCount": len(members),
        "totalByteSize": sum(member.byte_size for member in members),
        "totalRecordCount": sum(member.record_count or 0 for member in members),
    }
    if dict(counts) != expected_counts:
        _schema_failure(f"subordinate manifest counts differ: {key}")
    return members, key


def _load_members(root: Path, content: Mapping[str, Any]) -> tuple[list[_Member], set[str]]:
    references = [content["globalManifest"], *content["partitionManifests"]]
    members: list[_Member] = []
    manifest_keys: set[str] = set()
    for reference_value in references:
        assert isinstance(reference_value, dict)
        loaded, manifest_key = _load_subordinate_manifest(root, reference_value)
        members.extend(loaded)
        manifest_keys.add(manifest_key)
    keys = [member.object_key for member in members]
    if len(set(keys)) != len(keys):
        _schema_failure("a member objectKey appears in more than one subordinate manifest")

    missing: list[str] = []
    for member in members:
        path = root.joinpath(*member.object_key.split("/"))
        if not path.exists():
            missing.append(member.object_key)
    if missing:
        fail(VerificationCode.MEMBERSHIP_MISSING, f"declared members are missing: {sorted(missing)}")

    declared = {"release.json", *manifest_keys, *keys}
    actual: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            _path_failure(f"materialized release contains a symlink: {path.relative_to(root).as_posix()}")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    extra = sorted(actual - declared)
    if extra:
        fail(VerificationCode.MEMBERSHIP_EXTRA, f"undeclared materialized members: {extra}")

    for member in members:
        path = _materialized_path(root, member.object_key)
        digest, size = sha256_file(path)
        if digest != member.sha256 or size != member.byte_size:
            fail(VerificationCode.MEMBER_DIGEST, f"member digest or size differs: {member.object_key}")
    return members, manifest_keys


def _validate_schema_set(root: Path, content: Mapping[str, Any], members: Sequence[_Member]) -> None:
    schema_set = _closed_object(content["schemaSet"], {"schemaSetId", "schemas"}, "schemaSet")
    schemas = schema_set["schemas"]
    if not isinstance(schemas, list) or not schemas:
        _schema_failure("schemaSet.schemas must be a non-empty array")
    schema_members = [member for member in members if member.role == "schema"]
    member_by_digest: dict[str, list[_Member]] = {}
    for member in schema_members:
        member_by_digest.setdefault(member.sha256, []).append(member)
    descriptor_ids: list[str] = []
    known_documents = schema_documents()
    for descriptor_value in schemas:
        descriptor = _closed_object(
            descriptor_value,
            {"schemaId", "schemaVersion", "schemaSha256", "roles"},
            "schema descriptor",
        )
        schema_id = _string(descriptor["schemaId"], "schema descriptor schemaId")
        descriptor_ids.append(schema_id)
        if descriptor["schemaVersion"] != "1.0":
            _schema_failure(f"unknown schema version for {schema_id!r}")
        digest = _digest(descriptor["schemaSha256"], "schema descriptor schemaSha256")
        roles = descriptor["roles"]
        if not isinstance(roles, list) or not roles or any(not isinstance(role, str) or not role for role in roles):
            _schema_failure(f"schema descriptor roles are invalid for {schema_id!r}")
        if roles != sorted(set(roles)):
            _schema_failure(f"schema descriptor roles must be sorted and distinct for {schema_id!r}")
        matching = member_by_digest.get(digest, [])
        if len(matching) != 1:
            _schema_failure(f"schema descriptor {schema_id!r} must resolve to exactly one schema member")
        if schema_id not in known_documents:
            _schema_failure(f"unrecognized schemaId: {schema_id!r}")
        path = root.joinpath(*matching[0].object_key.split("/"))
        try:
            document = parse_canonical_json(path.read_bytes(), label=matching[0].object_key)
        except DocumentReleaseV3Error as error:
            _schema_failure(error)
        if document != known_documents[schema_id]:
            _schema_failure(f"schema member bytes do not define declared schema {schema_id!r}")
    if descriptor_ids != sorted(descriptor_ids) or len(set(descriptor_ids)) != len(descriptor_ids):
        _schema_failure("schema descriptors must be sorted by distinct schemaId")
    expected_set_id = schema_set_id(schemas)
    if schema_set["schemaSetId"] != expected_set_id or not str(schema_set["schemaSetId"]).startswith(
        SCHEMA_SET_ID_PREFIX
    ):
        _schema_failure("schemaSetId differs from the canonical schemas array")
    if len(schema_members) != len(schemas):
        _schema_failure("every schema member must have exactly one schema descriptor")


def _validate_member_file_schemas(root: Path, members: Sequence[_Member]) -> None:
    schema_documents_by_id = schema_documents()
    for member in members:
        path = root.joinpath(*member.object_key.split("/"))
        if member.role in ROLE_SCHEMA_IDS and member.role not in {"build-receipt"}:
            assert member.schema_id is not None
            expected = TABLE_SCHEMAS[member.schema_id]
            try:
                parquet = pq.ParquetFile(path)
            except (OSError, pa.ArrowInvalid) as error:
                _schema_failure(f"cannot open Parquet member {member.object_key}: {error}")
            if not parquet.schema_arrow.equals(expected, check_metadata=False):
                _schema_failure(
                    f"Parquet schema differs for {member.object_key}; expected={expected}, actual={parquet.schema_arrow}"
                )
            actual_rows = parquet.metadata.num_rows
            if actual_rows != member.record_count:
                _schema_failure(
                    f"recordCount differs for {member.object_key}: declared={member.record_count}, actual={actual_rows}"
                )
        elif member.role == "build-receipt":
            try:
                value = parse_canonical_json(path.read_bytes(), label=member.object_key)
                jsonschema.Draft202012Validator(schema_documents_by_id[BUILD_RECEIPT_SCHEMA_ID]).validate(value)
            except (DocumentReleaseV3Error, jsonschema.ValidationError) as error:
                _schema_failure(f"build receipt schema differs: {error}")
            if value["verdict"] != "pass" or value["verificationCode"] != "valid":
                _schema_failure("build receipt must record a passing producer verification")


def _sql_paths(paths: Iterable[Path]) -> str:
    quoted = ["'" + str(path).replace("'", "''") + "'" for path in paths]
    if not quoted:
        raise DocumentReleaseV3Error("at least one Parquet member is required for each role")
    return "[" + ",".join(quoted) + "]"


def _create_views(connection: duckdb.DuckDBPyConnection, root: Path, members: Sequence[_Member]) -> None:
    for role, view_name in {
        "current-documents": "current_documents",
        "documents": "documents",
        "eligibility-evidence": "eligibility_evidence",
        "passages": "passages",
        "source-dispositions": "source_dispositions",
        "changes": "changes",
        "failures": "failures",
        "coverage": "coverage_rows",
        "partition-receipt": "partition_receipts",
    }.items():
        role_members = [member for member in members if member.role == role]
        if not role_members:
            _schema_failure(f"release has no required {role!r} member")
        paths = [root.joinpath(*member.object_key.split("/")) for member in role_members]
        connection.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet({_sql_paths(paths)})")


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str, parameters: Sequence[object] = ()) -> Any:
    row = connection.execute(sql, parameters).fetchone()
    if row is None:
        raise DocumentReleaseV3Error("aggregate query returned no row")
    return row[0]


def _require_zero(connection: duckdb.DuckDBPyConnection, sql: str, code: VerificationCode, detail: str) -> None:
    count = int(_scalar(connection, sql))
    if count:
        fail(code, f"{detail}: {count} row(s)")


def _validate_unique_and_foreign_keys(connection: duckdb.DuckDBPyConnection) -> None:
    for view, key in (
        ("current_documents", "document_id"),
        ("documents", "document_version_id"),
        ("eligibility_evidence", "eligibility_evidence_id"),
        ("eligibility_evidence", "document_version_id"),
        ("passages", "passage_id"),
        ("source_dispositions", "document_id"),
        ("failures", "failure_id"),
        ("changes", "document_id"),
        ("coverage_rows", "source_id"),
    ):
        _require_zero(
            connection,
            f"SELECT count(*) FROM (SELECT {key} FROM {view} GROUP BY {key} HAVING count(*) <> 1)",
            VerificationCode.DUPLICATE_IDENTITY,
            f"duplicate {view}.{key}",
        )
    _require_zero(
        connection,
        "SELECT count(*) FROM (SELECT document_version_id, ordinal FROM passages "
        "GROUP BY document_version_id, ordinal HAVING count(*) <> 1)",
        VerificationCode.DUPLICATE_IDENTITY,
        "duplicate passage ordinal",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM current_documents c LEFT JOIN documents d "
        "ON c.document_version_id=d.document_version_id "
        "WHERE c.state='active' AND (d.document_version_id IS NULL OR c.document_id<>d.document_id "
        "OR c.eligibility_state<>d.eligibility_state)",
        VerificationCode.FOREIGN_KEY,
        "active current-document row does not resolve to its matching document version",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM documents d LEFT JOIN eligibility_evidence e "
        "ON d.eligibility_evidence_id=e.eligibility_evidence_id "
        "WHERE e.eligibility_evidence_id IS NULL OR d.document_version_id<>e.document_version_id "
        "OR d.eligibility_state<>e.verdict",
        VerificationCode.ELIGIBILITY_EVIDENCE,
        "document eligibility evidence is missing or inconsistent",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM passages p LEFT JOIN documents d ON p.document_version_id=d.document_version_id "
        "WHERE d.document_version_id IS NULL OR p.document_id<>d.document_id",
        VerificationCode.FOREIGN_KEY,
        "passage document version does not resolve",
    )


def _validate_row_constraints(connection: duckdb.DuckDBPyConnection, content: Mapping[str, Any]) -> None:
    policy = content["processingPolicy"]
    assert isinstance(policy, dict)
    _require_zero(
        connection,
        "SELECT count(*) FROM current_documents WHERE state NOT IN ('active','deleted') "
        "OR (state='active' AND (document_version_id IS NULL OR eligibility_state NOT IN ('eligible','ineligible','unverified'))) "
        "OR (state='deleted' AND (document_version_id IS NOT NULL OR eligibility_state IS NOT NULL))",
        VerificationCode.SCHEMA,
        "invalid current-document state",
    )
    count = int(
        _scalar(
            connection,
            "SELECT count(*) FROM documents WHERE eligibility_state NOT IN ('eligible','ineligible','unverified') "
            "OR normalizer_id<>? OR segmenter_id<>? OR processing_policy_id<>?",
            [policy["normalizerId"], policy["segmenterId"], policy["processingPolicyId"]],
        )
    )
    if count:
        fail(VerificationCode.SCHEMA, f"document policy identity or eligibility state differs: {count} row(s)")
    count = int(
        _scalar(
            connection,
            "SELECT count(*) FROM eligibility_evidence WHERE policy_id<>? "
            "OR evidence_kind NOT IN ('source-assertion','deterministic-policy','sealed-qualification') "
            "OR verdict NOT IN ('eligible','ineligible','unverified')",
            [policy["eligibilityPolicyId"]],
        )
    )
    if count:
        fail(VerificationCode.ELIGIBILITY_EVIDENCE, f"eligibility evidence fields differ: {count} row(s)")
    _require_zero(
        connection,
        "SELECT count(*) FROM source_dispositions WHERE disposition NOT IN "
        "('active','deleted','excluded','accepted-failure') OR NOT (selected_current OR previous_active) "
        "OR (source_input_id IS NULL AND NOT (disposition='deleted' AND previous_active AND NOT selected_current)) "
        "OR (selected_current AND source_input_id IS NULL) "
        "OR (disposition='active' AND (NOT selected_current OR document_version_id IS NULL "
        "OR exclusion_policy_id IS NOT NULL OR failure_id IS NOT NULL)) "
        "OR (disposition='deleted' AND (document_version_id IS NOT NULL OR exclusion_policy_id IS NOT NULL "
        "OR failure_id IS NOT NULL OR NOT (previous_active OR selected_current))) "
        "OR (disposition='excluded' AND (NOT selected_current OR exclusion_policy_id IS NULL "
        "OR document_version_id IS NOT NULL OR failure_id IS NOT NULL)) "
        "OR (disposition='accepted-failure' AND (NOT selected_current OR failure_id IS NULL "
        "OR document_version_id IS NOT NULL OR exclusion_policy_id IS NOT NULL))",
        VerificationCode.RECONCILIATION,
        "source disposition row is inconsistent",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM source_dispositions s LEFT JOIN current_documents c USING(document_id) "
        "WHERE (s.disposition IN ('active','deleted') AND (c.document_id IS NULL OR c.state<>s.disposition "
        "OR coalesce(c.document_version_id,'')<>coalesce(s.document_version_id,''))) "
        "OR (s.disposition IN ('excluded','accepted-failure') AND c.document_id IS NOT NULL)",
        VerificationCode.RECONCILIATION,
        "source disposition does not match current-document ledger",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM current_documents c LEFT JOIN source_dispositions s USING(document_id) "
        "WHERE s.document_id IS NULL OR s.disposition NOT IN ('active','deleted')",
        VerificationCode.RECONCILIATION,
        "current-document row is outside the reconciliation disposition ledger",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM source_dispositions s LEFT JOIN failures f USING(failure_id) "
        "WHERE s.disposition='accepted-failure' AND (f.failure_id IS NULL OR f.final_disposition<>'accepted-terminal')",
        VerificationCode.FOREIGN_KEY,
        "accepted-failure disposition has a foreign or nonterminal failure",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM failures WHERE stage NOT IN "
        "('select','acquire','verify-rendition','normalize','segment','eligibility','write','finalize') "
        "OR failure_class NOT IN ('transient-external','transient-resource','deterministic-input',"
        "'policy-exclusion','artifact-integrity','implementation-defect') "
        "OR attempt_count=0 OR final_disposition NOT IN "
        "('retried-successfully','accepted-terminal','rejected-build')",
        VerificationCode.SCHEMA,
        "failure row enum or attempt count is invalid",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM changes WHERE change_kind NOT IN ('add','update','delete','eligibility') "
        "OR (change_kind='add' AND (old_document_version_id IS NOT NULL OR new_document_version_id IS NULL)) "
        "OR (change_kind='delete' AND new_document_version_id IS NOT NULL) "
        "OR (change_kind IN ('update','eligibility') AND "
        "(old_document_version_id IS NULL OR new_document_version_id IS NULL "
        "OR old_document_version_id=new_document_version_id))",
        VerificationCode.SCHEMA,
        "change row is invalid",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM changes c LEFT JOIN current_documents d USING(document_id) "
        "LEFT JOIN source_dispositions s USING(document_id) "
        "WHERE (c.change_kind='delete' AND (s.document_id IS NULL OR s.disposition='active')) "
        "OR (c.change_kind<>'delete' AND (d.document_id IS NULL OR d.state<>'active' "
        "OR d.document_version_id<>c.new_document_version_id))",
        VerificationCode.FOREIGN_KEY,
        "change row does not resolve to current-document state",
    )


def _validate_diagnostic_codes(connection: duckdb.DuckDBPyConnection) -> None:
    for view, field in (
        ("source_dispositions", "disposition_code"),
        ("eligibility_evidence", "reason_code"),
        ("failures", "diagnostic_code"),
    ):
        for (value,) in connection.execute(f"SELECT DISTINCT {field} FROM {view}").fetchall():
            if not isinstance(value, str) or DIAGNOSTIC_CODE_PATTERN.fullmatch(value) is None:
                _schema_failure(f"{view}.{field} has an invalid diagnostic code: {value!r}")


def _iter_query_batches(
    connection: duckdb.DuckDBPyConnection, sql: str, *, batch_size: int = 2_000
) -> Iterable[pa.RecordBatch]:
    reader = connection.execute(sql).fetch_record_batch(rows_per_batch=batch_size)
    yield from reader


def _validate_eligibility_evidence(connection: duckdb.DuckDBPyConnection) -> None:
    schema = schema_documents()[ELIGIBILITY_DATA_SCHEMA_ID]
    validator = jsonschema.Draft202012Validator(schema)
    for batch in _iter_query_batches(
        connection,
        "SELECT evidence_data, evidence_digest, evidence_schema_id FROM eligibility_evidence",
    ):
        for row in batch.to_pylist():
            if row["evidence_schema_id"] != ELIGIBILITY_DATA_SCHEMA_ID:
                fail(VerificationCode.ELIGIBILITY_EVIDENCE, "eligibility evidence names an unknown data schema")
            try:
                value = parse_canonical_json(row["evidence_data"].encode("utf-8"), label="eligibility evidence_data")
                validator.validate(value)
            except (DocumentReleaseV3Error, jsonschema.ValidationError) as error:
                fail(VerificationCode.ELIGIBILITY_EVIDENCE, f"eligibility evidence_data is invalid: {error}")
            digest = hashlib.sha256(row["evidence_data"].encode("utf-8")).digest()
            if digest != row["evidence_digest"]:
                fail(VerificationCode.ELIGIBILITY_EVIDENCE, "eligibility evidence digest differs")


def _validate_passages(connection: duckdb.DuckDBPyConnection) -> None:
    sql = (
        "SELECT p.passage_id,p.document_id,p.document_version_id,p.ordinal,p.text,p.text_digest,"
        "p.normalized_start_utf8_byte,p.normalized_end_utf8_byte,p.coordinate_scheme,p.coordinate_data,"
        "p.processing_steps,d.normalized_text,d.normalized_text_digest,d.rendition_digest "
        "FROM passages p JOIN documents d USING(document_version_id) "
        "ORDER BY p.document_version_id,p.ordinal"
    )
    current_version: str | None = None
    expected_ordinal = 0
    covered_end = 0
    normalized_length = 0

    def finish_version() -> None:
        if current_version is not None and covered_end != normalized_length:
            fail(
                VerificationCode.COORDINATE,
                f"passage intervals do not cover normalized text for {current_version}",
            )

    for batch in _iter_query_batches(connection, sql):
        for row in batch.to_pylist():
            version = row["document_version_id"]
            normalized = row["normalized_text"].encode("utf-8")
            if hashlib.sha256(normalized).digest() != row["normalized_text_digest"]:
                fail(VerificationCode.COORDINATE, f"normalized text digest differs for {version}")
            if version != current_version:
                finish_version()
                current_version = version
                expected_ordinal = 0
                covered_end = 0
                normalized_length = len(normalized)
            if row["ordinal"] != expected_ordinal:
                fail(VerificationCode.COORDINATE, f"passage ordinals are not contiguous for {version}")
            expected_ordinal += 1
            start = row["normalized_start_utf8_byte"]
            end = row["normalized_end_utf8_byte"]
            if start > end or end > len(normalized) or start > covered_end:
                fail(
                    VerificationCode.COORDINATE,
                    f"passage interval is out of range or leaves a gap: {row['passage_id']}",
                )
            try:
                selected = normalized[start:end].decode("utf-8")
            except UnicodeDecodeError as error:
                fail(VerificationCode.COORDINATE, f"passage interval splits a UTF-8 code point: {error}")
            if selected != row["text"] or hashlib.sha256(selected.encode("utf-8")).digest() != row["text_digest"]:
                fail(VerificationCode.COORDINATE, f"passage interval or text digest differs: {row['passage_id']}")
            covered_end = max(covered_end, end)
            if row["coordinate_scheme"] != RENDITION_UTF8_COORDINATE_SCHEMA_ID:
                fail(VerificationCode.COORDINATE, "unknown passage coordinate_scheme")
            try:
                coordinate = parse_canonical_json(
                    row["coordinate_data"].encode("utf-8"), label="passage coordinate_data"
                )
            except DocumentReleaseV3Error as error:
                fail(VerificationCode.COORDINATE, str(error))
            if not isinstance(coordinate, dict) or set(coordinate) != {
                "endUtf8Byte",
                "mediaType",
                "renditionSha256",
                "startUtf8Byte",
            }:
                fail(VerificationCode.COORDINATE, "passage coordinate_data has an invalid closed shape")
            if (
                coordinate["startUtf8Byte"] != start
                or coordinate["endUtf8Byte"] != end
                or coordinate["renditionSha256"] != row["rendition_digest"].hex()
                or not isinstance(coordinate["mediaType"], str)
                or not coordinate["mediaType"]
            ):
                fail(VerificationCode.COORDINATE, "passage rendition coordinate differs from document identity")
            if row["processing_steps"] is None or not row["processing_steps"]:
                fail(VerificationCode.SCHEMA, "passage processing_steps must be non-empty")
    finish_version()
    _require_zero(
        connection,
        "SELECT count(*) FROM documents d LEFT JOIN passages p USING(document_version_id) "
        "WHERE p.document_version_id IS NULL",
        VerificationCode.COORDINATE,
        "document has no passage coverage",
    )


def _pair_pack_members(members: Sequence[_Member]) -> list[tuple[_Member, _Member]]:
    packs = {
        Path(member.object_key).name.removeprefix("pack-").removesuffix(".bin"): member
        for member in members
        if member.role == "rendition-pack"
    }
    indexes = {
        Path(member.object_key).name.removeprefix("pack-index-").removesuffix(".parquet"): member
        for member in members
        if member.role == "rendition-pack-index"
    }
    if set(packs) != set(indexes):
        fail(VerificationCode.FOREIGN_KEY, "rendition packs and pack indexes do not pair by immutable key")
    return [(packs[key], indexes[key]) for key in sorted(packs)]


def _validate_rendition_packs(connection: duckdb.DuckDBPyConnection, root: Path, members: Sequence[_Member]) -> None:
    connection.execute(
        "CREATE TEMP TABLE rendition_locations (rendition_digest BLOB, byte_length UBIGINT, media_type VARCHAR, "
        "pack_path VARCHAR, byte_offset UBIGINT)"
    )
    for pack_member, index_member in _pair_pack_members(members):
        pack_path = root.joinpath(*pack_member.object_key.split("/"))
        index_path = root.joinpath(*index_member.object_key.split("/"))
        pack_size = pack_member.byte_size
        previous_end = 0
        with pack_path.open("rb") as pack:
            parquet = pq.ParquetFile(index_path)
            for batch in parquet.iter_batches(batch_size=2_000):
                rows = batch.to_pylist()
                for row in rows:
                    offset = row["byte_offset"]
                    length = row["byte_length"]
                    end = offset + length
                    if offset < previous_end or end > pack_size:
                        fail(
                            VerificationCode.COORDINATE,
                            f"invalid or overlapping rendition pack range: {index_member.object_key}",
                        )
                    pack.seek(offset)
                    digest = hashlib.sha256()
                    remaining = length
                    while remaining:
                        chunk = pack.read(min(remaining, 1 << 20))
                        if not chunk:
                            fail(VerificationCode.COORDINATE, "rendition pack range ended early")
                        digest.update(chunk)
                        remaining -= len(chunk)
                    if digest.digest() != row["rendition_digest"]:
                        fail(VerificationCode.COORDINATE, "rendition digest differs from indexed pack bytes")
                    previous_end = end
                if rows:
                    insert = pa.table(
                        {
                            "rendition_digest": [row["rendition_digest"] for row in rows],
                            "byte_length": [row["byte_length"] for row in rows],
                            "media_type": [row["media_type"] for row in rows],
                            "pack_path": [str(pack_path)] * len(rows),
                            "byte_offset": [row["byte_offset"] for row in rows],
                        }
                    )
                    connection.register("rendition_batch", insert)
                    connection.execute("INSERT INTO rendition_locations SELECT * FROM rendition_batch")
                    connection.unregister("rendition_batch")
    _require_zero(
        connection,
        "SELECT count(*) FROM (SELECT rendition_digest FROM rendition_locations "
        "GROUP BY rendition_digest HAVING count(*)<>1)",
        VerificationCode.DUPLICATE_IDENTITY,
        "rendition digest is indexed more than once",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM documents d LEFT JOIN rendition_locations r USING(rendition_digest) "
        "WHERE r.rendition_digest IS NULL",
        VerificationCode.FOREIGN_KEY,
        "document rendition digest does not resolve",
    )
    reader = connection.execute(
        "SELECT p.passage_id,p.text,p.coordinate_data,r.pack_path,r.byte_offset,r.byte_length,r.media_type "
        "FROM passages p JOIN documents d USING(document_version_id) "
        "JOIN rendition_locations r USING(rendition_digest) "
        "ORDER BY r.pack_path,r.byte_offset,p.ordinal"
    ).fetch_record_batch(rows_per_batch=2_000)
    open_path: str | None = None
    pack_stream: Any | None = None
    try:
        for batch in reader:
            for row in batch.to_pylist():
                coordinate = parse_canonical_json(
                    row["coordinate_data"].encode("utf-8"), label="passage coordinate_data"
                )
                assert isinstance(coordinate, dict)
                start = coordinate["startUtf8Byte"]
                end = coordinate["endUtf8Byte"]
                if start > end or end > row["byte_length"] or coordinate["mediaType"] != row["media_type"]:
                    fail(VerificationCode.COORDINATE, "passage rendition byte range or media type differs")
                if row["pack_path"] != open_path:
                    if pack_stream is not None:
                        pack_stream.close()
                    open_path = row["pack_path"]
                    pack_stream = Path(open_path).open("rb")
                assert pack_stream is not None
                pack_stream.seek(row["byte_offset"] + start)
                selected = pack_stream.read(end - start)
                if selected != row["text"].encode("utf-8"):
                    fail(
                        VerificationCode.COORDINATE,
                        f"passage does not reverse to rendition bytes: {row['passage_id']}",
                    )
    finally:
        if pack_stream is not None:
            pack_stream.close()


def _inventory_digest(connection: duckdb.DuckDBPyConnection) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    first = True
    for batch in _iter_query_batches(
        connection,
        "SELECT document_id,source_input_id FROM source_dispositions "
        "WHERE selected_current ORDER BY document_id,source_input_id",
    ):
        for row in batch.to_pylist():
            if not first:
                digest.update(b",")
            digest.update(
                canonical_json_bytes({"documentId": row["document_id"], "sourceInputId": row["source_input_id"]})
            )
            first = False
    digest.update(b"]")
    return digest.hexdigest()


def _computed_rollups(
    connection: duckdb.DuckDBPyConnection, members: Sequence[_Member], partition_count: int
) -> tuple[dict[str, int], dict[str, int]]:
    dispositions = dict(
        connection.execute("SELECT disposition,count(*) FROM source_dispositions GROUP BY disposition").fetchall()
    )
    eligibility = dict(
        connection.execute(
            "SELECT d.eligibility_state,count(*) FROM documents d JOIN current_documents c "
            "USING(document_version_id) WHERE c.state='active' GROUP BY d.eligibility_state"
        ).fetchall()
    )
    counts = {
        "selectedDocumentCount": int(
            _scalar(connection, "SELECT count(*) FROM source_dispositions WHERE selected_current")
        ),
        "previousActiveDocumentCount": int(
            _scalar(connection, "SELECT count(*) FROM source_dispositions WHERE previous_active")
        ),
        "reconciliationUniverseCount": int(_scalar(connection, "SELECT count(*) FROM source_dispositions")),
        "activeDocumentCount": int(dispositions.get("active", 0)),
        "deletedDocumentCount": int(dispositions.get("deleted", 0)),
        "excludedDocumentCount": int(dispositions.get("excluded", 0)),
        "acceptedTerminalFailureCount": int(dispositions.get("accepted-failure", 0)),
        "documentVersionCount": int(_scalar(connection, "SELECT count(*) FROM documents")),
        "passageCount": int(_scalar(connection, "SELECT count(*) FROM passages")),
        "renditionCount": int(_scalar(connection, "SELECT count(DISTINCT rendition_digest) FROM rendition_locations")),
        "eligibilityEvidenceCount": int(_scalar(connection, "SELECT count(*) FROM eligibility_evidence")),
        "sourceDispositionCount": int(_scalar(connection, "SELECT count(*) FROM source_dispositions")),
        "failureRecordCount": int(_scalar(connection, "SELECT count(*) FROM failures")),
        "partitionManifestCount": partition_count,
        "memberCount": len(members),
        "totalMemberByteSize": sum(member.byte_size for member in members),
    }
    coverage = {
        "sourceCount": int(_scalar(connection, "SELECT count(*) FROM coverage_rows")),
        "eligibleActiveDocumentCount": int(eligibility.get("eligible", 0)),
        "ineligibleActiveDocumentCount": int(eligibility.get("ineligible", 0)),
        "unverifiedActiveDocumentCount": int(eligibility.get("unverified", 0)),
        "normalizedTextUtf8ByteCount": int(
            _scalar(
                connection,
                "SELECT coalesce(sum(octet_length(encode(d.normalized_text))),0) FROM documents d "
                "JOIN current_documents c USING(document_version_id) WHERE c.state='active'",
            )
        ),
        "passageTextUtf8ByteCount": int(
            _scalar(
                connection,
                "SELECT coalesce(sum(octet_length(encode(p.text))),0) FROM passages p "
                "JOIN current_documents c USING(document_version_id) WHERE c.state='active'",
            )
        ),
        "renditionByteCount": int(
            _scalar(
                connection,
                "SELECT coalesce(sum(r.byte_length),0) FROM rendition_locations r JOIN "
                "(SELECT DISTINCT d.rendition_digest FROM documents d JOIN current_documents c "
                "USING(document_version_id) WHERE c.state='active') active_renditions USING(rendition_digest)",
            )
        ),
    }
    return counts, coverage


def _validate_rollups(
    connection: duckdb.DuckDBPyConnection,
    content: Mapping[str, Any],
    members: Sequence[_Member],
) -> dict[str, int]:
    partition_count = len(content["partitionManifests"])
    counts, coverage = _computed_rollups(connection, members, partition_count)
    if content["counts"] != counts:
        fail(
            VerificationCode.RECONCILIATION, f"root count rollups differ; expected={counts}, actual={content['counts']}"
        )
    if content["coverage"] != coverage:
        fail(
            VerificationCode.RECONCILIATION,
            f"root coverage rollups differ; expected={coverage}, actual={content['coverage']}",
        )
    selection = content["sourceSelection"]
    assert isinstance(selection, dict)
    if selection["selectedDocumentCount"] != counts["selectedDocumentCount"]:
        fail(VerificationCode.RECONCILIATION, "sourceSelection selectedDocumentCount differs")
    inventory_digest = _inventory_digest(connection)
    if selection["inventoryDigest"] != inventory_digest:
        fail(VerificationCode.RECONCILIATION, "sourceSelection inventoryDigest differs")
    coverage_sums = connection.execute(
        "SELECT coalesce(sum(selected_document_count),0),coalesce(sum(active_document_count),0),"
        "coalesce(sum(deleted_document_count),0),coalesce(sum(excluded_document_count),0),"
        "coalesce(sum(accepted_terminal_failure_count),0) FROM coverage_rows"
    ).fetchone()
    assert coverage_sums is not None
    expected_sums = (
        counts["selectedDocumentCount"],
        counts["activeDocumentCount"],
        counts["deletedDocumentCount"],
        counts["excludedDocumentCount"],
        counts["acceptedTerminalFailureCount"],
    )
    if tuple(int(value) for value in coverage_sums) != expected_sums:
        fail(VerificationCode.RECONCILIATION, "coverage member rollups do not balance disposition sets")
    return counts


def _verify_release(root: Path, *, memory_limit: str) -> VerificationResult:
    require_memory_limit(memory_limit)
    root = Path(root).resolve()
    if not root.is_dir():
        fail(VerificationCode.MEMBERSHIP_MISSING, f"release directory does not exist: {root}")
    root_value = _load_root(root)
    _validate_root_content(root_value)
    content = root_value["content"]
    assert isinstance(content, dict)
    members, _manifest_keys = _load_members(root, content)
    _validate_schema_set(root, content, members)
    _validate_member_file_schemas(root, members)

    with tempfile.TemporaryDirectory(prefix="spicyregs-v3-verify-") as temp_directory:
        connection = duckdb.connect()
        try:
            escaped_temp = temp_directory.replace("'", "''")
            connection.execute(f"SET temp_directory='{escaped_temp}'")
            connection.execute(f"SET memory_limit='{memory_limit}'")
            connection.execute("SET threads=2")
            _create_views(connection, root, members)
            _validate_unique_and_foreign_keys(connection)
            _validate_row_constraints(connection, content)
            _validate_diagnostic_codes(connection)
            _validate_eligibility_evidence(connection)
            _validate_passages(connection)
            _validate_rendition_packs(connection, root, members)
            counts = _validate_rollups(connection, content, members)
        finally:
            connection.close()
    digest = artifact_digest(content)
    return VerificationResult(
        verdict="pass",
        code=VerificationCode.VALID,
        release_id=str(root_value["releaseId"]),
        artifact_digest=digest,
        diagnostics=(),
        counts=counts,
    )


def verify_release(root: Path, *, memory_limit: str = "512MB") -> VerificationResult:
    """Verify a complete release and return a stable pass/fail result."""

    try:
        return _verify_release(root, memory_limit=memory_limit)
    except DocumentReleaseV3VerificationError as error:
        return VerificationResult(
            verdict="fail",
            code=error.code,
            release_id=None,
            artifact_digest=None,
            diagnostics=(error.detail,),
            counts={},
        )
    except (DocumentReleaseV3Error, duckdb.Error, pa.ArrowException, OSError) as error:
        return VerificationResult(
            verdict="fail",
            code=VerificationCode.SCHEMA,
            release_id=None,
            artifact_digest=None,
            diagnostics=(f"{type(error).__name__}: {error}",),
            counts={},
        )


def verify_release_or_raise(root: Path, *, memory_limit: str = "512MB") -> VerificationResult:
    """Verify a release and raise its registered first failure code."""

    result = verify_release(root, memory_limit=memory_limit)
    if result.code is not VerificationCode.VALID:
        raise DocumentReleaseV3VerificationError(result.code, result.diagnostics[0])
    return result

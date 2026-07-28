"""Manifest-bound corpus validation and paired Rulespec gate receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.ontology.citations import (
    canonical_cfr_iri,
    canonical_pl_iri,
    canonical_regsgov_iri,
    canonical_rin_iri,
    canonical_usc_iri,
    normalize_regsgov_identifier,
    parse_cfr_citation,
)
from spicy_regs.ontology.common import canonical_json, iter_parquet_rows
from spicy_regs.ontology.concepts import latest_assignments
from spicy_regs.ontology.ledger import (
    FINAL_STATUSES,
    KNOWN_STATUSES,
    SEGMENT_LEDGER_COLUMNS,
)
from spicy_regs.ontology.llm import SUPPORTED_REASONING_EFFORTS
from spicy_regs.ontology.subjects import (
    PROFILE_SEGMENTATION_POLICIES,
    iter_artifacts,
    segment_artifact,
)
from spicy_regs.pipelines.ontology_dataset import OntologyDatasetPipeline
from spicy_regs.transforms.build_proceedings import (
    STAGES,
    _current_stage_from_events,
)

RECEIPT_FORMAT_VERSION = 1
EXPECTED_SCHEMAS = expected_schemas()
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CFR_IRI = re.compile(
    r"^urn:rkaf:us:cfr:[1-9][0-9]*:[0-9]+"
    r"(?:\.[0-9]+[a-z]{0,3}(?:-[0-9a-z]+)*)?$"
)
_SAFE_FR_DOCUMENT_NUMBER = re.compile(r"^[A-Za-z0-9-]+$")
_SECRET_LIKE = re.compile(
    r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON object at {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON value at {path} is not an object")
    return value


def _json_list(
    value: object,
    *,
    table: str,
    row_id: object,
    column: str,
    failures: "FailureCollector",
) -> list[Any]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        failures.add(table, row_id, column, "malformed JSON list")
        return []
    if not isinstance(parsed, list):
        failures.add(table, row_id, column, "value is not a JSON list")
        return []
    return parsed


@dataclass
class FailureCollector:
    """Count all failures while retaining a bounded diagnostic sample."""

    total: int = 0
    by_check: Counter[str] = field(default_factory=Counter)
    examples: list[dict[str, str]] = field(default_factory=list)
    max_examples: int = 50

    def add(
        self,
        table: str,
        row_id: object,
        check: str,
        message: str,
    ) -> None:
        self.total += 1
        self.by_check[f"{table}.{check}"] += 1
        if len(self.examples) < self.max_examples:
            self.examples.append(
                {
                    "table": table,
                    "row_id": str(row_id),
                    "check": check,
                    "message": message,
                }
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "by_check": dict(sorted(self.by_check.items())),
            "examples": self.examples,
        }


def _artifact_records(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    check_schema: bool,
    failures: FailureCollector,
) -> dict[str, dict[str, Any]]:
    expected = set(OntologyDatasetPipeline.generation_outputs())
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        failures.add("manifest", manifest_path, "artifacts", "artifact map is missing")
        return {}
    actual = set(artifacts)
    if actual != expected:
        failures.add(
            "manifest",
            manifest_path,
            "artifacts",
            f"expected {sorted(expected)}, found {sorted(actual)}",
        )

    records: dict[str, dict[str, Any]] = {}
    snapshot_id = str(manifest.get("snapshot_id") or "")
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    for name in sorted(expected):
        record = artifacts.get(name)
        path = manifest_path.parent / name
        if not isinstance(record, dict):
            failures.add("manifest", name, "artifact_record", "record is missing")
            continue
        if not path.is_file():
            failures.add("manifest", name, "artifact_file", f"missing {path}")
            continue
        digest = _sha256(path)
        size = path.stat().st_size
        expected_digest = str(record.get("sha256") or "")
        expected_bytes = record.get("bytes")
        remote_key = str(record.get("remote_key") or "")
        if not _SHA256.fullmatch(expected_digest) or digest != expected_digest:
            failures.add("manifest", name, "sha256", "artifact digest mismatch")
        if expected_bytes != size:
            failures.add("manifest", name, "bytes", "artifact byte count mismatch")
        if remote_key != f"{prefix}{name}":
            failures.add("manifest", name, "remote_key", "artifact key leaves snapshot prefix")
        expected_visibility = (
            "public"
            if name in OntologyDatasetPipeline.published_outputs
            else "internal"
        )
        if record.get("visibility", expected_visibility) != expected_visibility:
            failures.add(
                "manifest",
                name,
                "visibility",
                f"expected {expected_visibility}",
            )

        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as exc:
            failures.add(
                "manifest",
                name,
                "parquet",
                f"artifact is not readable Parquet: {exc}",
            )
            continue
        columns = parquet.schema_arrow.names
        rows = parquet.metadata.num_rows
        if "rows" in record and record.get("rows") != rows:
            failures.add("manifest", name, "rows", "artifact row count mismatch")
        table = name.removesuffix(".parquet")
        if check_schema:
            expected_columns = (
                list(SEGMENT_LEDGER_COLUMNS)
                if name == "ontology_segment_ledger.parquet"
                else [
                    column
                    for column, _ in EXPECTED_SCHEMAS[table]
                ]
            )
            if columns != expected_columns:
                failures.add(
                    table,
                    name,
                    "schema",
                    f"expected {expected_columns}, found {columns}",
                )
        records[name] = {
            "bytes": size,
            "sha256": digest,
            "rows": rows,
            "columns": columns,
        }
    return records


def _source_membership(
    directory: Path,
    manifest: dict[str, Any],
    failures: FailureCollector,
) -> tuple[set[str], set[str], set[str], dict[str, int]]:
    sources = manifest.get("inputs", {}).get("sources", {})
    if not isinstance(sources, dict):
        failures.add("manifest", directory, "inputs.sources", "source map is missing")
        return set(), set(), set(), {}

    for name, record in sources.items():
        path = directory / name
        if not path.is_file() or not isinstance(record, dict):
            failures.add("manifest", name, "source", "source file or record is missing")
            continue
        if _sha256(path) != record.get("sha256") or path.stat().st_size != record.get("bytes"):
            failures.add("manifest", name, "source_hash", "source snapshot mismatch")

    dockets: set[str] = set()
    documents: set[str] = set()
    fr_documents: set[str] = set()
    metrics = Counter()
    docket_path = directory / "dockets.parquet"
    document_path = directory / "documents.parquet"
    fr_path = directory / "federal_register.parquet"
    if fr_path.exists():
        for row in iter_parquet_rows(fr_path, columns=("document_number",)):
            if row.get("document_number"):
                fr_documents.add(str(row["document_number"]))
    if docket_path.exists():
        for row in iter_parquet_rows(docket_path, columns=("docket_id",)):
            if normalized := normalize_regsgov_identifier(row.get("docket_id")):
                dockets.add(normalized)
    if document_path.exists():
        for row in iter_parquet_rows(
            document_path,
            columns=("document_id", "docket_id", "fr_doc_num"),
        ):
            row_id = row.get("document_id")
            document_id = normalize_regsgov_identifier(row_id)
            if document_id is None:
                failures.add("documents", row_id, "document_id", "invalid us-regsgov identifier")
            else:
                documents.add(document_id)
                try:
                    canonical_regsgov_iri(document_id)
                except ValueError as exc:
                    failures.add("documents", row_id, "document_transform", str(exc))
            if docket := normalize_regsgov_identifier(row.get("docket_id")):
                dockets.add(docket)
            if row.get("fr_doc_num"):
                # The L0 mapping's source_membership filter projects only exact
                # Federal Register source-of-record values. Raw nonmembers stay
                # in documents.parquet and are counted, not coerced into IRIs.
                value = str(row["fr_doc_num"])
                metrics["cross_posting_values"] += 1
                if value not in fr_documents:
                    metrics["cross_posting_values_filtered"] += 1
                elif not _SAFE_FR_DOCUMENT_NUMBER.fullmatch(value):
                    failures.add(
                        "documents",
                        row_id,
                        "fr_doc_num_transform",
                        f"source-backed value cannot form the declared URL: {value!r}",
                    )
                else:
                    metrics["cross_posting_links"] += 1
    return dockets, documents, fr_documents, dict(metrics)


def _json_object_value(
    value: object,
    *,
    table: str,
    row_id: object,
    column: str,
    failures: FailureCollector,
) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        failures.add(table, row_id, column, "malformed JSON object")
        return {}
    if not isinstance(parsed, dict):
        failures.add(table, row_id, column, "value is not a JSON object")
        return {}
    return parsed


def _valid_completed_model_call(metadata: dict[str, Any]) -> bool:
    """Validate safe, application-owned structured-model telemetry."""
    if metadata.get("transport") == "codex-cli":
        attempts = metadata.get("attempts")
        disabled_features = metadata.get("disabled_features")
        event_types = metadata.get("event_types")
        try:
            total_tokens = int(str(metadata.get("total_tokens") or 0))
        except (TypeError, ValueError):
            return False
        return (
            metadata.get("status") == "completed"
            and metadata.get("store") is False
            and metadata.get("session_persistence") == "ephemeral"
            and metadata.get("tools_enabled") is False
            and metadata.get("schema_validated_locally") is True
            and metadata.get("exit_code") == 0
            and bool(metadata.get("response_id"))
            and bool(metadata.get("thread_id"))
            and bool(metadata.get("cli_version"))
            and bool(_SHA256.fullmatch(str(metadata.get("prompt_sha256") or "")))
            and bool(_SHA256.fullmatch(str(metadata.get("request_sha256") or "")))
            and bool(_SHA256.fullmatch(str(metadata.get("command_sha256") or "")))
            and bool(_SHA256.fullmatch(str(metadata.get("event_stream_sha256") or "")))
            and isinstance(disabled_features, list)
            and "shell_tool" in disabled_features
            and "plugins" in disabled_features
            and "skill_search" in disabled_features
            and isinstance(event_types, list)
            and not metadata.get("unknown_event_types")
            and not metadata.get("forbidden_item_types")
            and not metadata.get("terminal_error_event_types")
            and isinstance(attempts, list)
            and len(attempts) == 1
            and isinstance(attempts[0], dict)
            and attempts[0].get("status") == "completed"
            and total_tokens > 0
        )
    try:
        attempt_count = int(str(metadata.get("attempt_count")))
        retry_count = int(str(metadata.get("retry_count")))
        prompt_estimate = int(
            str(metadata.get("prompt_token_estimate"))
        )
        safety_margin = int(
            str(metadata.get("prompt_safety_margin_tokens"))
        )
        input_budget = int(
            str(metadata.get("prompt_input_token_budget"))
        )
        timeout_seconds = float(
            str(metadata.get("timeout_seconds"))
        )
        max_retries = int(str(metadata.get("max_retries")))
        max_output_tokens = int(
            str(metadata.get("max_output_tokens"))
        )
        total_tokens = int(str(metadata.get("total_tokens")))
    except (TypeError, ValueError):
        return False
    attempts_value = metadata.get("attempts")
    if not isinstance(attempts_value, list) or not all(
        isinstance(attempt, dict)
        for attempt in attempts_value
    ):
        return False
    attempts = [
        cast(dict[str, Any], attempt)
        for attempt in attempts_value
    ]
    return (
        metadata.get("status") == "completed"
        and metadata.get("store") is False
        and bool(_SHA256.fullmatch(str(metadata.get("prompt_sha256") or "")))
        and bool(_SHA256.fullmatch(str(metadata.get("request_sha256") or "")))
        and metadata.get("reasoning_effort")
        in SUPPORTED_REASONING_EFFORTS
        and bool(metadata.get("response_id"))
        and total_tokens > 0
        and metadata.get("sdk_max_retries") == 0
        and attempt_count > 0
        and retry_count == attempt_count - 1
        and len(attempts) == attempt_count
        and all(
            attempt.get("attempt") == index
            for index, attempt in enumerate(attempts, start=1)
        )
        and bool(attempts)
        and attempts[-1].get("status") == "completed"
        and prompt_estimate > 0
        and safety_margin >= 0
        and prompt_estimate + safety_margin <= input_budget
        and timeout_seconds > 0
        and max_retries >= 0
        and max_output_tokens > 0
    )


def _validate_segment_ledger(
    path: Path,
    *,
    directory: Path,
    run_id: str,
    failures: FailureCollector,
) -> dict[str, Any]:
    """Validate segment completeness, token bounds, and exact evidence."""
    rows = list(iter_parquet_rows(path))
    current_rows = [
        row
        for row in rows
        if not run_id or str(row.get("run_id") or "") == run_id
    ]
    status_counts = Counter(
        str(row.get("status") or "") for row in current_rows
    )
    profile_counts = Counter(
        str(row.get("subject_profile") or "")
        for row in current_rows
    )
    result_ids: set[str] = set()
    rows_by_segment: dict[str, dict] = {}
    current_by_artifact: dict[str, list[dict]] = defaultdict(list)
    selected_artifact_keys: set[tuple[str, str, str]] = set()
    coverage: dict[
        tuple[str, str],
        list[tuple[int, int, str, str]],
    ] = defaultdict(list)

    token_counter = None
    if rows:
        from spicy_regs.ontology.segmentation import TiktokenCounter

        token_counter = TiktokenCounter()

    for row in rows:
        result_id = str(row.get("segment_result_id") or "")
        if not result_id or result_id in result_ids:
            failures.add(
                "ontology_segment_ledger",
                result_id,
                "segment_result_id",
                "missing or duplicate result id",
            )
        result_ids.add(result_id)
        status = str(row.get("status") or "")
        if status not in KNOWN_STATUSES:
            failures.add(
                "ontology_segment_ledger",
                result_id,
                "status",
                f"unknown status {status!r}",
            )
        segment_id = str(row.get("segment_id") or "")
        if segment_id:
            rows_by_segment[segment_id] = row
        if row not in current_rows:
            continue

        digest = str(row.get("artifact_digest") or "")
        subject_type = str(row.get("subject_type") or "")
        subject_id = str(row.get("subject_id") or "")
        profile_id = str(row.get("subject_profile") or "")
        current_by_artifact[digest].append(row)
        selected_artifact_keys.add(
            (subject_type, subject_id, digest)
        )
        policy = PROFILE_SEGMENTATION_POLICIES.get(profile_id)
        if policy is None:
            failures.add(
                "ontology_segment_ledger",
                result_id,
                "subject_profile",
                f"undeclared profile {profile_id!r}",
            )
        elif status != "skipped_non_content":
            if not str(row.get("segment_policy") or "").startswith(
                f"{policy.policy_version}:"
            ):
                failures.add(
                    "ontology_segment_ledger",
                    result_id,
                    "segment_policy",
                    "policy differs from the profile declaration",
                )
            try:
                token_count = int(str(row.get("token_count") or ""))
                max_tokens = int(str(row.get("max_tokens") or ""))
                min_tokens = int(str(row.get("min_tokens") or ""))
            except ValueError:
                token_count = -1
                max_tokens = -1
                min_tokens = -1
            if (
                token_count < 0
                or max_tokens <= 0
                or min_tokens <= 0
                or min_tokens > max_tokens
                or token_count > max_tokens
            ):
                failures.add(
                    "ontology_segment_ledger",
                    result_id,
                    "token_budget",
                    f"{token_count} violates {min_tokens}:{max_tokens}",
                )
            if token_counter is not None and (
                row.get("tokenizer") != token_counter.name
                or row.get("tokenizer_version")
                != token_counter.version
            ):
                failures.add(
                    "ontology_segment_ledger",
                    result_id,
                    "tokenizer",
                    "tokenizer name or installed version differs",
                )
            fields = _json_object_value(
                row.get("fields_json"),
                table="ontology_segment_ledger",
                row_id=result_id,
                column="fields_json",
                failures=failures,
            )
            spans = _json_object_value(
                row.get("source_spans_json"),
                table="ontology_segment_ledger",
                row_id=result_id,
                column="source_spans_json",
                failures=failures,
            )
            field_sources = _json_object_value(
                row.get("field_sources_json"),
                table="ontology_segment_ledger",
                row_id=result_id,
                column="field_sources_json",
                failures=failures,
            )
            source_hashes = _json_object_value(
                row.get("source_sha256_json"),
                table="ontology_segment_ledger",
                row_id=result_id,
                column="source_sha256_json",
                failures=failures,
            )
            if token_counter is not None:
                measured = token_counter.count(
                    "\n".join(
                        str(value) for value in fields.values()
                    )
                )
                if measured != token_count:
                    failures.add(
                        "ontology_segment_ledger",
                        result_id,
                        "token_count",
                        f"recorded {token_count}, measured {measured}",
                    )
            for field_name, field_text_value in fields.items():
                field_text = str(field_text_value)
                source_span = spans.get(field_name)
                if (
                    not isinstance(source_span, list)
                    or len(source_span) != 2
                    or not all(
                        isinstance(value, int)
                        and not isinstance(value, bool)
                        for value in source_span
                    )
                ):
                    failures.add(
                        "ontology_segment_ledger",
                        result_id,
                        "source_spans",
                        f"invalid span for {field_name}",
                    )
                    continue
                start, end = source_span
                if (
                    start < 0
                    or end <= start
                    or end - start != len(field_text)
                ):
                    failures.add(
                        "ontology_segment_ledger",
                        result_id,
                        "source_spans",
                        f"span length differs for {field_name}",
                    )
                    continue
                canonical_field = str(
                    field_sources.get(field_name) or field_name
                )
                coverage[(digest, canonical_field)].append(
                    (
                        start,
                        end,
                        field_text,
                        str(source_hashes.get(field_name) or ""),
                    )
                )
        if status == "retry_exhausted":
            failures.add(
                "ontology_segment_ledger",
                result_id,
                "processing_status",
                "selected segment exhausted its retry budget",
            )
        actor_id = str(row.get("actor_id") or "")
        if (
            actor_id.startswith("openai:")
            and status in FINAL_STATUSES
            and status != "skipped_non_content"
        ):
            metadata = _json_object_value(
                row.get("model_call_json"),
                table="ontology_segment_ledger",
                row_id=result_id,
                column="model_call_json",
                failures=failures,
            )
            if not _valid_completed_model_call(metadata):
                failures.add(
                    "ontology_segment_ledger",
                    result_id,
                    "model_call",
                    "OpenAI result lacks safe completed-call telemetry",
                )
        if _SECRET_LIKE.search(json.dumps(row, sort_keys=True)):
            failures.add(
                "ontology_segment_ledger",
                result_id,
                "secret_scan",
                "secret-like OpenAI key prefix appears in the ledger",
            )

    for digest, artifact_rows in current_by_artifact.items():
        statuses = {
            str(row.get("status") or "")
            for row in artifact_rows
        }
        if "skipped_non_content" in statuses:
            if len(artifact_rows) != 1:
                failures.add(
                    "ontology_segment_ledger",
                    digest,
                    "segment_completeness",
                    "non-content artifact has additional segment rows",
                )
            continue
        try:
            expected_count = max(
                int(str(row.get("segment_count") or ""))
                for row in artifact_rows
            )
            ordinals = {
                int(str(row.get("segment_ordinal") or ""))
                for row in artifact_rows
            }
        except ValueError:
            expected_count = -1
            ordinals = set()
        if (
            expected_count <= 0
            or len(artifact_rows) != expected_count
            or ordinals != set(range(expected_count))
            or any(
                str(row.get("status") or "") not in FINAL_STATUSES
                for row in artifact_rows
            )
        ):
            failures.add(
                "ontology_segment_ledger",
                digest,
                "segment_completeness",
                "selected artifact lacks one final row per segment",
            )

    for (digest, field_name), pieces in coverage.items():
        cursor = 0
        text_parts: list[str] = []
        source_hashes: set[str] = set()
        for start, end, text, source_hash in sorted(pieces):
            if start != cursor:
                failures.add(
                    "ontology_segment_ledger",
                    digest,
                    "source_coverage",
                    f"{field_name} has a gap or overlap at {cursor}",
                )
            cursor = end
            text_parts.append(text)
            source_hashes.add(source_hash)
        measured_hash = hashlib.sha256(
            "".join(text_parts).encode()
        ).hexdigest()
        if len(source_hashes) != 1 or measured_hash not in source_hashes:
            failures.add(
                "ontology_segment_ledger",
                digest,
                "source_digest",
                f"{field_name} does not reconstruct its source digest",
            )

    unresolved_artifacts = set(selected_artifact_keys)
    resolved_artifacts = {}
    if unresolved_artifacts:
        for artifact in iter_artifacts(directory):
            key = (
                artifact.subject_type,
                artifact.subject_id,
                artifact.digest,
            )
            if key in unresolved_artifacts:
                resolved_artifacts[key] = artifact
                unresolved_artifacts.discard(key)
            if not unresolved_artifacts:
                break
    for subject_type, subject_id, digest in sorted(unresolved_artifacts):
        failures.add(
            "ontology_segment_ledger",
            f"{subject_type}:{subject_id}",
            "artifact_version",
            f"artifact digest {digest} does not resolve to current inputs",
        )

    for artifact_key, artifact in resolved_artifacts.items():
        digest = artifact_key[2]
        artifact_rows = current_by_artifact[digest]
        expected_segments = segment_artifact(artifact)
        if not expected_segments:
            if (
                len(artifact_rows) != 1
                or artifact_rows[0].get("status")
                != "skipped_non_content"
            ):
                failures.add(
                    "ontology_segment_ledger",
                    digest,
                    "source_replay",
                    "non-content artifact does not replay to one skip row",
                )
            continue
        expected_by_id = {
            subject.segment_id: subject
            for subject in expected_segments
        }
        actual_by_id = {
            str(row.get("segment_id") or ""): row
            for row in artifact_rows
        }
        if set(actual_by_id) != set(expected_by_id):
            failures.add(
                "ontology_segment_ledger",
                digest,
                "segment_identity",
                "stored segment IDs differ from deterministic replay",
            )
            continue
        for segment_id, subject in expected_by_id.items():
            row = actual_by_id[segment_id]
            replay_values = {
                "segment_ordinal": str(subject.segment_ordinal),
                "segment_count": str(subject.segment_count),
                "segment_policy": subject.segment_policy,
                "tokenizer": subject.tokenizer,
                "tokenizer_version": subject.tokenizer_version,
                "token_count": str(subject.token_count),
                "max_tokens": str(subject.max_segment_tokens),
                "min_tokens": str(subject.min_segment_tokens),
                "fields_json": canonical_json(subject.fields),
                "context_fields_json": canonical_json(
                    subject.context_fields or {}
                ),
                "field_sources_json": canonical_json(
                    subject.field_sources or {}
                ),
                "source_spans_json": canonical_json(
                    subject.source_spans or {}
                ),
                "source_sha256_json": canonical_json(
                    subject.source_sha256 or {}
                ),
                "previous_segment_id": subject.previous_segment_id,
                "next_segment_id": subject.next_segment_id,
                "parent_segment_id": subject.parent_segment_id,
            }
            for column, expected_value in replay_values.items():
                if row.get(column) != expected_value:
                    failures.add(
                        "ontology_segment_ledger",
                        segment_id,
                        "source_replay",
                        f"{column} differs from deterministic replay",
                    )

    assignments_path = directory / "concept_assignments.parquet"
    if assignments_path.exists():
        assignments = latest_assignments(
            list(iter_parquet_rows(assignments_path))
        )
        for assignment in assignments:
            if assignment.get("method") != "llm":
                continue
            assignment_id = str(
                assignment.get("assignment_id") or ""
            )
            evidence = _json_object_value(
                assignment.get("evidence_json"),
                table="concept_assignments",
                row_id=assignment_id,
                column="evidence_json",
                failures=failures,
            )
            evidence_spans = evidence.get("spans")
            if not isinstance(evidence_spans, list) or not evidence_spans:
                failures.add(
                    "concept_assignments",
                    assignment_id,
                    "segment_evidence",
                    "LLM assignment has no segment-backed spans",
                )
                continue
            artifact_digest = str(
                evidence.get("artifact_sha256")
                or evidence.get("subject_sha256")
                or ""
            )
            for span_value in evidence_spans:
                if not isinstance(span_value, dict):
                    failures.add(
                        "concept_assignments",
                        assignment_id,
                        "segment_evidence",
                        "evidence span is not an object",
                    )
                    continue
                span = cast(dict[str, Any], span_value)
                segment_id = str(span.get("segment_id") or "")
                segment_row = rows_by_segment.get(segment_id)
                if segment_row is None:
                    failures.add(
                        "concept_assignments",
                        assignment_id,
                        "segment_range",
                        f"unknown segment {segment_id!r}",
                    )
                    continue
                if (
                    str(segment_row.get("artifact_digest") or "")
                    != artifact_digest
                ):
                    failures.add(
                        "concept_assignments",
                        assignment_id,
                        "artifact_digest",
                        "assignment and segment artifact digests differ",
                    )
                fields = _json_object_value(
                    segment_row.get("fields_json"),
                    table="ontology_segment_ledger",
                    row_id=segment_id,
                    column="fields_json",
                    failures=failures,
                )
                source_spans = _json_object_value(
                    segment_row.get("source_spans_json"),
                    table="ontology_segment_ledger",
                    row_id=segment_id,
                    column="source_spans_json",
                    failures=failures,
                )
                field_sources = _json_object_value(
                    segment_row.get("field_sources_json"),
                    table="ontology_segment_ledger",
                    row_id=segment_id,
                    column="field_sources_json",
                    failures=failures,
                )
                segment_source_hashes = _json_object_value(
                    segment_row.get("source_sha256_json"),
                    table="ontology_segment_ledger",
                    row_id=segment_id,
                    column="source_sha256_json",
                    failures=failures,
                )
                field_name = str(span.get("source_field") or "")
                evidence_field_key = str(
                    span.get("evidence_field_key") or field_name
                )
                segment_text = fields.get(evidence_field_key)
                segment_source_span = source_spans.get(
                    evidence_field_key
                )
                try:
                    start = int(str(span.get("start_char")))
                    end = int(str(span.get("end_char")))
                    if (
                        not isinstance(segment_source_span, list)
                        or len(segment_source_span) != 2
                    ):
                        raise ValueError
                    segment_source_start = int(segment_source_span[0])
                    segment_source_end = int(segment_source_span[1])
                except (TypeError, ValueError):
                    start = end = segment_source_start = (
                        segment_source_end
                    ) = -1
                if (
                    not isinstance(segment_text, str)
                    or not isinstance(segment_source_span, list)
                    or len(segment_source_span) != 2
                    or start < segment_source_start
                    or end > segment_source_end
                    or end <= start
                    or field_sources.get(evidence_field_key)
                    != field_name
                    or (
                        span.get("source_sha256")
                        and segment_source_hashes.get(
                            evidence_field_key
                        )
                        != span.get("source_sha256")
                    )
                ):
                    failures.add(
                        "concept_assignments",
                        assignment_id,
                        "evidence_coordinates",
                        "span coordinates leave the declared segment",
                    )
                    continue
                local_start = start - segment_source_start
                local_end = end - segment_source_start
                if (
                    segment_text[local_start:local_end]
                    != span.get("text")
                    or (
                        span.get("segment_start_char") is not None
                        and span.get("segment_start_char")
                        != local_start
                    )
                    or (
                        span.get("segment_end_char") is not None
                        and span.get("segment_end_char")
                        != local_end
                    )
                ):
                    failures.add(
                        "concept_assignments",
                        assignment_id,
                        "evidence_text",
                        "span text differs from exact source coordinates",
                    )

    return {
        "declared_profile_policies": len(
            PROFILE_SEGMENTATION_POLICIES
        ),
        "rows": len(rows),
        "current_run_rows": len(current_rows),
        "selected_artifacts": len(current_by_artifact),
        "profile_counts": dict(sorted(profile_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "zero_tag_segments": status_counts.get("zero_tags", 0),
        "rejected_output_segments": status_counts.get(
            "rejected_output",
            0,
        ),
        "retry_exhausted_segments": status_counts.get(
            "retry_exhausted",
            0,
        ),
    }


def _validate_rule_targets(
    path: Path,
    *,
    dockets: set[str],
    failures: FailureCollector,
) -> dict[str, int]:
    metrics = Counter(
        {
            "invalid_docket_syntax": 0,
            "docket_not_in_source": 0,
            "invalid_cfr_reference": 0,
        }
    )
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = f"{row.get('docket_id')}:{row.get('cfr_ref')}:{row.get('source')}"
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None:
            failures.add("rule_targets", row_id, "docket_id", "invalid us-regsgov identifier")
            metrics["invalid_docket_syntax"] += 1
        elif docket not in dockets:
            failures.add("rule_targets", row_id, "docket_membership", "identifier absent from source")
            metrics["docket_not_in_source"] += 1
        if row.get("rin"):
            try:
                canonical_rin_iri(row["rin"])
            except ValueError as exc:
                failures.add("rule_targets", row_id, "rin", str(exc))
        if row.get("cfr_ref"):
            citations = parse_cfr_citation(row["cfr_ref"])
            if len(citations) != 1 or citations[0].cfr_ref != row["cfr_ref"]:
                failures.add("rule_targets", row_id, "cfr_ref", "invalid normalized CFR reference")
                metrics["invalid_cfr_reference"] += 1
            else:
                citation = citations[0]
                expected_components = (citation.title, citation.part, citation.section)
                actual_components = (
                    str(row.get("cfr_title") or ""),
                    str(row.get("cfr_part") or ""),
                    row.get("cfr_section"),
                )
                if actual_components != expected_components:
                    failures.add(
                        "rule_targets",
                        row_id,
                        "cfr_components",
                        f"expected {expected_components}, found {actual_components}",
                    )
                try:
                    canonical_cfr_iri(*expected_components)
                except ValueError as exc:
                    failures.add("rule_targets", row_id, "cfr_transform", str(exc))
                metrics["citation_targets"] += 1
        metrics[f"source:{row.get('source')}"] += 1
    return dict(metrics)


def _validate_authorities(path: Path, failures: FailureCollector) -> dict[str, int]:
    metrics = Counter()
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = f"{row.get('rin')}:{row.get('authority_raw')}"
        if row.get("rin"):
            try:
                canonical_rin_iri(row["rin"])
            except ValueError as exc:
                failures.add("authority_edges", row_id, "rin", str(exc))
        if row.get("usc_title") or row.get("usc_section"):
            try:
                canonical_usc_iri(row.get("usc_title"), row.get("usc_section"))
                metrics["usc_identifiers"] += 1
            except ValueError as exc:
                failures.add("authority_edges", row_id, "usc_transform", str(exc))
        # A range's far endpoint is a section the source text names, so it has
        # to be expressible as one. Interior members are never rows and so are
        # never checked — there are none to check.
        if row.get("usc_section_end"):
            try:
                canonical_usc_iri(row.get("usc_title"), row.get("usc_section_end"))
                metrics["usc_section_ranges"] += 1
            except ValueError as exc:
                failures.add("authority_edges", row_id, "usc_range_transform", str(exc))
        if row.get("pl_number"):
            try:
                canonical_pl_iri(row["pl_number"])
                metrics["public_law_identifiers"] += 1
            except ValueError as exc:
                failures.add("authority_edges", row_id, "pl_transform", str(exc))
        metrics[f"parse_status:{row.get('parse_status')}"] += 1
    return dict(metrics)


def _validate_proceedings(
    path: Path,
    *,
    dockets: set[str],
    fr_documents: set[str],
    failures: FailureCollector,
) -> tuple[set[str], dict[str, int]]:
    proceeding_ids: set[str] = set()
    metrics = Counter(
        {
            "invalid_docket_syntax": 0,
            "docket_not_in_source": 0,
            "multi_docket_rows": 0,
            "self_predecessor_edges": 0,
        }
    )
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = str(row.get("proceeding_id") or "")
        if not row_id or row_id in proceeding_ids:
            failures.add("proceedings", row_id, "proceeding_id", "missing or duplicate id")
        proceeding_ids.add(row_id)
        if row.get("rin"):
            try:
                canonical_rin_iri(row["rin"])
                metrics["rin_evidence"] += 1
            except ValueError as exc:
                failures.add("proceedings", row_id, "rin", str(exc))

        docket_values = _json_list(
            row.get("docket_ids_json"),
            table="proceedings",
            row_id=row_id,
            column="docket_ids_json",
            failures=failures,
        )
        if len(docket_values) > 1:
            metrics["multi_docket_rows"] += 1
        for value in docket_values:
            docket = normalize_regsgov_identifier(value)
            if docket is None:
                failures.add("proceedings", row_id, "docket_id", f"invalid {value!r}")
                metrics["invalid_docket_syntax"] += 1
            elif docket not in dockets:
                failures.add("proceedings", row_id, "docket_membership", f"unknown {docket}")
                metrics["docket_not_in_source"] += 1

        fr_values = _json_list(
            row.get("fr_document_numbers_json"),
            table="proceedings",
            row_id=row_id,
            column="fr_document_numbers_json",
            failures=failures,
        )
        for value in fr_values:
            if str(value) not in fr_documents:
                failures.add(
                    "proceedings",
                    row_id,
                    "fr_document_membership",
                    f"unknown {value!r}",
                )

        refs = _json_list(
            row.get("cfr_refs_json"),
            table="proceedings",
            row_id=row_id,
            column="cfr_refs_json",
            failures=failures,
        )
        target_iris = _json_list(
            row.get("cfr_target_iris_json"),
            table="proceedings",
            row_id=row_id,
            column="cfr_target_iris_json",
            failures=failures,
        )
        expected_iris: set[str] = set()
        for ref in refs:
            parsed = parse_cfr_citation(ref)
            if len(parsed) == 1 and parsed[0].cfr_ref == ref:
                expected_iris.add(parsed[0].iri)
            else:
                metrics["unprojectable_cfr_refs"] += 1
        if set(map(str, target_iris)) != expected_iris:
            failures.add(
                "proceedings",
                row_id,
                "cfr_target_projection",
                "citation IRI set does not match valid compact references",
            )
        for iri in target_iris:
            if not _CFR_IRI.fullmatch(str(iri)):
                failures.add("proceedings", row_id, "cfr_target_iri", f"invalid {iri!r}")
        metrics["citation_target_iris"] += len(target_iris)
        metrics["unresolved_edition_targets"] += len(target_iris)

        events = _json_list(
            row.get("stage_events_json"),
            table="proceedings",
            row_id=row_id,
            column="stage_events_json",
            failures=failures,
        )
        if not all(isinstance(event, dict) for event in events):
            failures.add("proceedings", row_id, "stage_events", "event is not an object")
            events = []
        try:
            expected_stage = _current_stage_from_events(events)
        except ValueError as exc:
            failures.add("proceedings", row_id, "stage_event_kind", str(exc))
            expected_stage = None
        current_stage = row.get("current_stage")
        if current_stage is not None and current_stage not in STAGES:
            failures.add("proceedings", row_id, "current_stage", f"unknown {current_stage!r}")
        if current_stage != expected_stage:
            failures.add(
                "proceedings",
                row_id,
                "latest_stage_agreement",
                f"stored {current_stage!r}, derived {expected_stage!r}",
            )
        if current_stage:
            metrics[f"stage:{current_stage}"] += 1

        predecessors = _json_list(
            row.get("identity_predecessors_json"),
            table="proceedings",
            row_id=row_id,
            column="identity_predecessors_json",
            failures=failures,
        )
        valid_predecessors = all(
            isinstance(value, str) and value for value in predecessors
        )
        if not valid_predecessors:
            failures.add("proceedings", row_id, "predecessors", "invalid predecessor id")
        elif len(set(predecessors)) != len(predecessors):
            failures.add("proceedings", row_id, "predecessors", "duplicate predecessor id")
        if valid_predecessors and row_id in predecessors:
            metrics["self_predecessor_edges"] += 1
            failures.add(
                "proceedings",
                row_id,
                "predecessor_self_edge",
                "proceedingSupersedes cannot target the same Proceeding",
            )
        if valid_predecessors:
            metrics["distinct_predecessor_edges"] += len(predecessors)
        if row.get("agency_code"):
            metrics["agency_identified"] += 1
        authority_refs = _json_list(
            row.get("authority_refs_json"),
            table="proceedings",
            row_id=row_id,
            column="authority_refs_json",
            failures=failures,
        )
        if authority_refs:
            metrics["authority_evidence_present"] += 1
    # Agency identity is no longer projected as an Authority node.
    metrics["placeholder_authority_claims"] = 0
    return proceeding_ids, dict(metrics)


def _agenda_observations(directory: Path) -> dict[str, dict[str, Any]]:
    observations: dict[str, dict[str, Any]] = {}
    path = directory / "unified_agenda.parquet"
    if not path.exists():
        return observations
    for row in iter_parquet_rows(
        path,
        columns=("rin", "agenda_edition", "priority_category", "url"),
    ):
        rin = str(row.get("rin") or "").strip().upper()
        try:
            canonical_rin_iri(rin)
        except ValueError:
            continue
        edition = str(row.get("agenda_edition") or "").strip()
        url = str(row.get("url") or "").strip()
        record = observations.setdefault(
            rin,
            {"keys": set(), "latest_edition": "", "latest_priority": ""},
        )
        record["keys"].add((edition, url))
        if edition > record["latest_edition"]:
            record["latest_edition"] = edition
            record["latest_priority"] = " ".join(
                str(row.get("priority_category") or "").casefold().split()
            )
    return observations


def _validate_regulatory_agenda_items(
    path: Path,
    *,
    observations: dict[str, dict[str, Any]],
    failures: FailureCollector,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    items: dict[str, dict[str, Any]] = {}
    metrics = Counter()
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = str(row.get("agenda_item_id") or "")
        rin = str(row.get("rin") or "")
        if not row_id or row_id in items:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "agenda_item_id",
                "missing or duplicate id",
            )
        try:
            expected_id = canonical_rin_iri(rin)
        except ValueError as exc:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "rin",
                str(exc),
            )
            expected_id = None
        if expected_id is not None and row_id != expected_id:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "agenda_item_transform",
                f"expected {expected_id!r}",
            )
        scope_status = row.get("scope_status")
        if scope_status not in {"recurring", "single_observed", "unresolved"}:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "scope_status",
                f"unknown {scope_status!r}",
            )
        try:
            linked_count = int(str(row.get("linked_proceeding_count")))
            observation_count = int(str(row.get("observation_count")))
            if linked_count < 0 or observation_count < 0:
                raise ValueError
        except ValueError:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "counts",
                "counts must be non-negative integers",
            )
            linked_count = -1
            observation_count = -1

        observed = observations.get(rin)
        expected_observation_count = (
            0 if observed is None else len(observed["keys"])
        )
        expected_latest = (
            None
            if observed is None or not observed["latest_edition"]
            else observed["latest_edition"]
        )
        if observation_count != expected_observation_count:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "observation_count",
                f"expected {expected_observation_count}, found {observation_count}",
            )
        if row.get("latest_agenda_edition") != expected_latest:
            failures.add(
                "regulatory_agenda_items",
                row_id,
                "latest_agenda_edition",
                f"expected {expected_latest!r}",
            )
        items[row_id] = {
            **row,
            "_linked_count": linked_count,
            "_latest_priority": (
                "" if observed is None else observed["latest_priority"]
            ),
        }
        metrics[f"scope:{scope_status}"] += 1
    return items, dict(metrics)


def _validate_agenda_item_proceedings(
    path: Path,
    *,
    items: dict[str, dict[str, Any]],
    proceeding_ids: set[str],
    dockets: set[str],
    documents: set[str],
    fr_documents: set[str],
    failures: FailureCollector,
) -> dict[str, int]:
    metrics = Counter()
    seen_ids: set[str] = set()
    linked_by_item: dict[str, set[str]] = defaultdict(set)
    source_members = {
        "docket_rin": (dockets, "https://www.regulations.gov/docket/"),
        "document_rin": (
            documents,
            "https://www.regulations.gov/document/",
        ),
        "federal_register_rin": (
            fr_documents,
            "https://www.federalregister.gov/d/",
        ),
    }
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = str(row.get("relationship_id") or "")
        if not row_id or row_id in seen_ids:
            failures.add(
                "agenda_item_proceedings",
                row_id,
                "relationship_id",
                "missing or duplicate id",
            )
        seen_ids.add(row_id)
        item_id = str(row.get("agenda_item_id") or "")
        proceeding_id = str(row.get("proceeding_id") or "")
        if item_id not in items:
            failures.add(
                "agenda_item_proceedings",
                row_id,
                "agenda_item_range",
                f"unknown {item_id!r}",
            )
        else:
            if row.get("rin") != items[item_id].get("rin"):
                failures.add(
                    "agenda_item_proceedings",
                    row_id,
                    "rin_agreement",
                    "relationship RIN disagrees with agenda item",
                )
            linked_by_item[item_id].add(proceeding_id)
        if proceeding_id not in proceeding_ids:
            failures.add(
                "agenda_item_proceedings",
                row_id,
                "proceeding_range",
                f"unknown {proceeding_id!r}",
            )
        if row.get("relationship_role") != "agenda_tracks_proceeding":
            failures.add(
                "agenda_item_proceedings",
                row_id,
                "relationship_role",
                "role must be agenda_tracks_proceeding",
            )
        source = str(row.get("source") or "")
        source_spec = source_members.get(source)
        evidence_id = str(row.get("evidence_id") or "")
        if source_spec is None:
            failures.add(
                "agenda_item_proceedings",
                row_id,
                "source",
                f"unknown {source!r}",
            )
        else:
            members, prefix = source_spec
            if evidence_id not in members:
                failures.add(
                    "agenda_item_proceedings",
                    row_id,
                    "evidence_membership",
                    f"unknown source evidence {evidence_id!r}",
                )
            expected_uri = f"{prefix}{quote(evidence_id, safe='-._~')}"
            if row.get("evidence_uri") != expected_uri:
                failures.add(
                    "agenda_item_proceedings",
                    row_id,
                    "evidence_uri",
                    f"expected {expected_uri!r}",
                )
        if row.get("evidence_date"):
            try:
                date.fromisoformat(str(row["evidence_date"]))
            except ValueError:
                failures.add(
                    "agenda_item_proceedings",
                    row_id,
                    "evidence_date",
                    "invalid calendar date",
                )
        metrics[f"source:{source}"] += 1

    for item_id, item in items.items():
        linked_count = len(linked_by_item.get(item_id, ()))
        if item["_linked_count"] != linked_count:
            failures.add(
                "regulatory_agenda_items",
                item_id,
                "linked_proceeding_count",
                f"expected {linked_count}, found {item['_linked_count']}",
            )
        recurring = item["_latest_priority"] == "routine and frequent"
        expected_scope = (
            "recurring"
            if recurring
            else "single_observed"
            if linked_count == 1
            else "unresolved"
        )
        expected_basis = (
            "latest_agenda_priority_routine_and_frequent"
            if recurring
            else "one_evidence_linked_proceeding"
            if linked_count == 1
            else "zero_evidence_linked_proceedings"
            if linked_count == 0
            else "multiple_evidence_linked_proceedings"
        )
        if item.get("scope_status") != expected_scope:
            failures.add(
                "regulatory_agenda_items",
                item_id,
                "scope_derivation",
                f"expected {expected_scope!r}",
            )
        if item.get("scope_basis") != expected_basis:
            failures.add(
                "regulatory_agenda_items",
                item_id,
                "scope_basis",
                f"expected {expected_basis!r}",
            )
    metrics["linked_items"] = len(linked_by_item)
    return dict(metrics)


def _validate_comment_periods(
    path: Path,
    *,
    proceeding_ids: set[str],
    dockets: set[str],
    documents: set[str],
    fr_documents: set[str],
    failures: FailureCollector,
) -> dict[str, int]:
    metrics = Counter({"rows": 0, "unanchored_intervals": 0})
    seen_ids: set[str] = set()
    for row in iter_parquet_rows(path):
        metrics["rows"] += 1
        row_id = str(row.get("comment_period_id") or "")
        if not row_id or row_id in seen_ids:
            failures.add("comment_periods", row_id, "comment_period_id", "missing or duplicate id")
        seen_ids.add(row_id)
        proceedings = _json_list(
            row.get("proceeding_ids_json"),
            table="comment_periods",
            row_id=row_id,
            column="proceeding_ids_json",
            failures=failures,
        )
        docket_values = _json_list(
            row.get("docket_ids_json"),
            table="comment_periods",
            row_id=row_id,
            column="docket_ids_json",
            failures=failures,
        )
        if not proceedings and not docket_values:
            metrics["unanchored_intervals"] += 1
            failures.add("comment_periods", row_id, "anchors", "no Proceeding or Docket anchor")
        for value in proceedings:
            if value not in proceeding_ids:
                failures.add("comment_periods", row_id, "proceeding_range", f"unknown {value!r}")
        for value in docket_values:
            docket = normalize_regsgov_identifier(value)
            if docket is None or docket not in dockets:
                failures.add("comment_periods", row_id, "docket_range", f"unknown {value!r}")
        if not proceedings and docket_values:
            metrics["docket_only_intervals"] += 1
        if len(proceedings) > 1:
            metrics["joint_proceeding_intervals"] += 1
        if len(docket_values) > 1:
            metrics["multi_docket_intervals"] += 1

        rins = _json_list(
            row.get("rins_json"),
            table="comment_periods",
            row_id=row_id,
            column="rins_json",
            failures=failures,
        )
        for rin in rins:
            try:
                canonical_rin_iri(rin)
            except ValueError as exc:
                failures.add("comment_periods", row_id, "rin", str(exc))
        try:
            opened = date.fromisoformat(str(row.get("open_date")))
            closed = date.fromisoformat(str(row.get("close_date")))
            if closed < opened:
                failures.add("comment_periods", row_id, "date_order", "close precedes open")
        except ValueError:
            failures.add("comment_periods", row_id, "date", "invalid calendar date")

        evidence = _json_list(
            row.get("evidence_ids_json"),
            table="comment_periods",
            row_id=row_id,
            column="evidence_ids_json",
            failures=failures,
        )
        opened_by = _json_list(
            row.get("opened_by_artifact_ids_json"),
            table="comment_periods",
            row_id=row_id,
            column="opened_by_artifact_ids_json",
            failures=failures,
        )
        if not evidence:
            failures.add("comment_periods", row_id, "evidence", "empty evidence list")
        if not opened_by:
            failures.add("comment_periods", row_id, "opened_by", "empty opening Artifact list")
        evidence_strings = {str(value) for value in evidence}
        if len(evidence_strings) != len(evidence):
            failures.add("comment_periods", row_id, "evidence", "duplicate evidence id")
        for evidence_id in evidence_strings:
            if evidence_id not in documents and evidence_id not in fr_documents:
                failures.add(
                    "comment_periods",
                    row_id,
                    "evidence_membership",
                    f"unknown source Artifact {evidence_id!r}",
                )
        for iri in opened_by:
            parsed = urlparse(str(iri))
            evidence_id = unquote(parsed.path.rsplit("/", 1)[-1])
            source_members = (
                documents
                if parsed.netloc == "www.regulations.gov"
                else fr_documents
            )
            if (
                parsed.scheme != "https"
                or parsed.netloc not in {"www.regulations.gov", "www.federalregister.gov"}
                or evidence_id not in evidence_strings
                or evidence_id not in source_members
            ):
                failures.add(
                    "comment_periods",
                    row_id,
                    "opened_by_transform",
                    f"{iri!r} is not a canonical URL for row evidence",
                )
    return dict(metrics)


def _measure_baseline(
    directory: Path,
    *,
    dockets: set[str],
) -> dict[str, dict[str, int]]:
    """Measure pre-repair defects without applying candidate schema rules."""
    metrics: dict[str, Counter[str]] = {
        "rule_targets": Counter(),
        "proceedings": Counter(),
        "comment_periods": Counter(),
    }
    rule_targets_path = directory / "rule_targets.parquet"
    if rule_targets_path.exists():
        target_metrics = metrics["rule_targets"]
        for row in iter_parquet_rows(
            rule_targets_path,
            columns=("docket_id", "cfr_ref"),
        ):
            target_metrics["rows"] += 1
            docket = normalize_regsgov_identifier(row.get("docket_id"))
            if docket is None:
                target_metrics["invalid_docket_syntax"] += 1
            elif docket not in dockets:
                target_metrics["docket_not_in_source"] += 1
            cfr_ref = row.get("cfr_ref")
            if cfr_ref:
                parsed = parse_cfr_citation(cfr_ref)
                if len(parsed) != 1 or parsed[0].cfr_ref != cfr_ref:
                    target_metrics["invalid_cfr_reference"] += 1

    proceedings_path = directory / "proceedings.parquet"
    if proceedings_path.exists():
        proceeding_metrics = metrics["proceedings"]
        for row in iter_parquet_rows(
            proceedings_path,
            columns=(
                "proceeding_id",
                "docket_ids_json",
                "agency_code",
                "identity_predecessors_json",
            ),
        ):
            proceeding_metrics["rows"] += 1
            row_id = str(row.get("proceeding_id") or "")
            try:
                docket_values = json.loads(str(row.get("docket_ids_json") or "[]"))
            except json.JSONDecodeError:
                docket_values = []
                proceeding_metrics["malformed_docket_lists"] += 1
            if isinstance(docket_values, list):
                if len(docket_values) > 1:
                    proceeding_metrics["multi_docket_rows"] += 1
                for value in docket_values:
                    docket = normalize_regsgov_identifier(value)
                    if docket is None:
                        proceeding_metrics["invalid_docket_syntax"] += 1
                    elif docket not in dockets:
                        proceeding_metrics["docket_not_in_source"] += 1
            if row.get("agency_code"):
                proceeding_metrics["agency_code_values"] += 1
            try:
                predecessors = json.loads(
                    str(row.get("identity_predecessors_json") or "[]")
                )
            except json.JSONDecodeError:
                predecessors = []
                proceeding_metrics["malformed_predecessor_lists"] += 1
            if isinstance(predecessors, list) and row_id in predecessors:
                proceeding_metrics["self_predecessor_edges"] += 1

    comment_path = directory / "comment_periods.parquet"
    if comment_path.exists():
        comment_metrics = metrics["comment_periods"]
        columns = set(pq.ParquetFile(comment_path).schema_arrow.names)
        for row in iter_parquet_rows(comment_path):
            comment_metrics["rows"] += 1
            if "proceeding_ids_json" in columns:
                try:
                    proceedings = json.loads(
                        str(row.get("proceeding_ids_json") or "[]")
                    )
                    docket_values = json.loads(
                        str(row.get("docket_ids_json") or "[]")
                    )
                except json.JSONDecodeError:
                    continue
            else:
                proceedings = [row["proceeding_id"]] if row.get("proceeding_id") else []
                docket_values = [row["docket_id"]] if row.get("docket_id") else []
            if not proceedings and docket_values:
                comment_metrics["docket_only_intervals"] += 1
            if not proceedings and not docket_values:
                comment_metrics["unanchored_intervals"] += 1

    return {
        table: dict(table_metrics)
        for table, table_metrics in metrics.items()
    }


def validate_generation(
    manifest_path: Path,
    *,
    baseline_manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Validate every ontology artifact and return receipt-ready evidence."""
    manifest_path = manifest_path.resolve()
    manifest = _load_object(manifest_path)
    failures = FailureCollector()
    if manifest.get("format_version") != 1 or manifest.get("dataset") != "ontology":
        failures.add("manifest", manifest_path, "identity", "not an ontology v1 manifest")
    artifact_records = _artifact_records(
        manifest_path,
        manifest,
        check_schema=True,
        failures=failures,
    )
    dockets, documents, fr_documents, source_metrics = _source_membership(
        manifest_path.parent,
        manifest,
        failures,
    )

    metrics: dict[str, Any] = {
        "source_membership": {
            "dockets": len(dockets),
            "documents": len(documents),
            "federal_register_documents": len(fr_documents),
            **source_metrics,
        }
    }
    required_paths = {
        name: manifest_path.parent / name
        for name in OntologyDatasetPipeline.generation_outputs()
    }
    if all(path.exists() for path in required_paths.values()):
        metrics["rule_targets"] = _validate_rule_targets(
            required_paths["rule_targets.parquet"],
            dockets=dockets,
            failures=failures,
        )
        metrics["authority_edges"] = _validate_authorities(
            required_paths["authority_edges.parquet"],
            failures,
        )
        proceeding_ids, proceeding_metrics = _validate_proceedings(
            required_paths["proceedings.parquet"],
            dockets=dockets,
            fr_documents=fr_documents,
            failures=failures,
        )
        metrics["proceedings"] = proceeding_metrics
        observations = _agenda_observations(manifest_path.parent)
        agenda_items, agenda_metrics = _validate_regulatory_agenda_items(
            required_paths["regulatory_agenda_items.parquet"],
            observations=observations,
            failures=failures,
        )
        metrics["regulatory_agenda_items"] = agenda_metrics
        metrics["agenda_item_proceedings"] = (
            _validate_agenda_item_proceedings(
                required_paths["agenda_item_proceedings.parquet"],
                items=agenda_items,
                proceeding_ids=proceeding_ids,
                dockets=dockets,
                documents=documents,
                fr_documents=fr_documents,
                failures=failures,
            )
        )
        metrics["comment_periods"] = _validate_comment_periods(
            required_paths["comment_periods.parquet"],
            proceeding_ids=proceeding_ids,
            dockets=dockets,
            documents=documents,
            fr_documents=fr_documents,
            failures=failures,
        )
        metrics["ontology_segment_ledger"] = (
            _validate_segment_ledger(
                required_paths["ontology_segment_ledger.parquet"],
                directory=manifest_path.parent,
                run_id=str(manifest.get("run_id") or ""),
                failures=failures,
            )
        )

    baseline: dict[str, Any] | None = None
    if baseline_manifest_path is not None:
        baseline_path = baseline_manifest_path.resolve()
        baseline_manifest = _load_object(baseline_path)
        baseline_failures = FailureCollector()
        baseline_records = _artifact_records(
            baseline_path,
            baseline_manifest,
            check_schema=False,
            failures=baseline_failures,
        )
        inputs_match = baseline_manifest.get("inputs") == manifest.get("inputs")
        if not inputs_match:
            failures.add(
                "manifest",
                manifest_path,
                "baseline_inputs",
                "baseline and candidate inputs/prior state differ",
            )
        if baseline_failures.total:
            failures.add(
                "manifest",
                baseline_path,
                "baseline_integrity",
                f"{baseline_failures.total} baseline artifact failures",
            )
        baseline = {
            "manifest": str(baseline_path),
            "snapshot_id": baseline_manifest.get("snapshot_id"),
            "inputs_match": inputs_match,
            "artifacts": baseline_records,
            "metrics": _measure_baseline(
                baseline_path.parent,
                dockets=dockets,
            ),
            "failures": baseline_failures.as_dict(),
        }

    return {
        "status": "pass" if failures.total == 0 else "fail",
        "manifest": str(manifest_path),
        "snapshot_id": manifest.get("snapshot_id"),
        "inputs": manifest.get("inputs"),
        "artifacts": artifact_records,
        "metrics": metrics,
        "failures": failures.as_dict(),
        "baseline": baseline,
    }


def _git_info(repo: Path) -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    status = git("status", "--porcelain")
    return {
        "path": str(repo.resolve()),
        "commit": git("rev-parse", "HEAD"),
        "branch": git("branch", "--show-current") or None,
        "clean": not bool(status),
        "status_sha256": hashlib.sha256(status.encode()).hexdigest(),
    }


def _contract_digest(rulespec_repo: Path) -> str:
    result = subprocess.run(
        [
            sys.executable,
            str(rulespec_repo / "tools" / "l0_mapping_audit.py"),
            "--print-contract-version",
        ],
        cwd=rulespec_repo,
        check=True,
        capture_output=True,
        text=True,
    )
    digest = result.stdout.strip().splitlines()[-1]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise RuntimeError(f"Rulespec returned an invalid contract digest: {digest!r}")
    return digest


def _run_gate(
    *,
    name: str,
    command: list[str],
    cwd: Path,
    log_dir: Path,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    finished = datetime.now(timezone.utc)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}.log"
    log_text = (
        f"$ {' '.join(command)}\n"
        f"cwd: {cwd}\n"
        f"exit_code: {result.returncode}\n\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}\n"
    )
    log_path.write_text(log_text, encoding="utf-8")
    return {
        "name": name,
        "command": command,
        "cwd": str(cwd.resolve()),
        "exit_code": result.returncode,
        "status": "pass" if result.returncode == 0 else "fail",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "log": str(log_path.resolve()),
        "log_sha256": hashlib.sha256(log_text.encode()).hexdigest(),
    }


def run_paired_gates(
    *,
    spicy_repo: Path,
    rulespec_repo: Path,
    log_dir: Path,
) -> list[dict[str, Any]]:
    """Run and record the exact paired release-candidate gate commands."""
    commands = (
        (
            "rulespec-compile",
            [
                "make",
                "-C",
                str(rulespec_repo),
                (
                    "PYTHON=uv run --with-requirements requirements.txt "
                    "--with jsonschema --with pyld python"
                ),
                "compile",
            ],
            rulespec_repo,
        ),
        (
            "rulespec-test",
            [
                "make",
                "-C",
                str(rulespec_repo),
                (
                    "PYTHON=uv run --with-requirements requirements.txt "
                    "--with jsonschema --with pyld python"
                ),
                "test",
            ],
            rulespec_repo,
        ),
        (
            "spicy-pytest",
            ["env", "R2_PUBLIC_URL=", "uv", "run", "pytest"],
            spicy_repo,
        ),
        ("spicy-dictionary", ["uv", "run", "spicy-regs-dict", "check"], spicy_repo),
        ("spicy-ruff", ["uv", "run", "ruff", "check", "."], spicy_repo),
        ("spicy-types", ["uv", "run", "ty", "check", "src", "tests"], spicy_repo),
        (
            "spicy-docs",
            ["uv", "run", "--group", "docs", "mkdocs", "build", "--strict"],
            spicy_repo,
        ),
        (
            "rulespec-l0",
            [
                "uv",
                "run",
                "python",
                str(rulespec_repo / "tools" / "l0_mapping_audit.py"),
                str(spicy_repo / "conformance" / "rulespec-l0.yaml"),
            ],
            spicy_repo,
        ),
    )
    return [
        _run_gate(name=name, command=command, cwd=cwd, log_dir=log_dir)
        for name, command, cwd in commands
    ]


def run_materialization_gates(
    *,
    candidate_manifest_path: Path,
    baseline_manifest_path: Path,
    spicy_repo: Path,
    baseline_repo: Path,
    log_dir: Path,
) -> list[dict[str, Any]]:
    """Build baseline and candidate from their already-primed input snapshots."""
    commands = (
        (
            "baseline-materialize",
            [
                "uv",
                "run",
                "materialize-ontology",
                "--output-dir",
                str(baseline_manifest_path.parent.resolve()),
                "--no-full-refresh",
                "--skip-upload",
            ],
            baseline_repo,
        ),
        (
            "candidate-materialize",
            [
                "uv",
                "run",
                "materialize-ontology",
                "--output-dir",
                str(candidate_manifest_path.parent.resolve()),
                "--no-full-refresh",
                "--skip-upload",
            ],
            spicy_repo,
        ),
    )
    return [
        _run_gate(name=name, command=command, cwd=cwd, log_dir=log_dir)
        for name, command, cwd in commands
    ]


def build_receipt(
    manifest_path: Path,
    *,
    baseline_manifest_path: Path | None,
    baseline_repo: Path | None,
    spicy_repo: Path,
    rulespec_repo: Path,
    output_path: Path,
    run_materialization: bool,
    run_gates: bool,
    require_clean: bool,
) -> dict[str, Any]:
    spicy_git = _git_info(spicy_repo)
    rulespec_git = _git_info(rulespec_repo)
    baseline_git = _git_info(baseline_repo) if baseline_repo is not None else None
    materialization_gates = (
        run_materialization_gates(
            candidate_manifest_path=manifest_path,
            baseline_manifest_path=baseline_manifest_path,
            spicy_repo=spicy_repo,
            baseline_repo=baseline_repo,
            log_dir=output_path.parent / f"{output_path.stem}-logs",
        )
        if (
            run_materialization
            and baseline_manifest_path is not None
            and baseline_repo is not None
        )
        else []
    )
    try:
        generation = validate_generation(
            manifest_path,
            baseline_manifest_path=baseline_manifest_path,
        )
    except (OSError, ValueError) as exc:
        generation = {
            "status": "fail",
            "manifest": str(manifest_path.resolve()),
            "snapshot_id": None,
            "failures": {
                "total": 1,
                "by_check": {"manifest.load": 1},
                "examples": [
                    {
                        "table": "manifest",
                        "row_id": str(manifest_path),
                        "check": "load",
                        "message": str(exc),
                    }
                ],
            },
        }
    gates = (
        run_paired_gates(
            spicy_repo=spicy_repo,
            rulespec_repo=rulespec_repo,
            log_dir=output_path.parent / f"{output_path.stem}-logs",
        )
        if run_gates
        else []
    )
    spicy_git_after = _git_info(spicy_repo)
    rulespec_git_after = _git_info(rulespec_repo)
    baseline_git_after = (
        _git_info(baseline_repo) if baseline_repo is not None else None
    )
    baseline_proven = baseline_manifest_path is None or baseline_git is not None
    clean_ok = not require_clean or (
        spicy_git["clean"]
        and rulespec_git["clean"]
        and spicy_git_after["clean"]
        and rulespec_git_after["clean"]
        and baseline_proven
        and (baseline_git is None or baseline_git["clean"])
        and (baseline_git_after is None or baseline_git_after["clean"])
    )
    gates_ok = not run_gates or all(gate["status"] == "pass" for gate in gates)
    materialization_ok = not run_materialization or (
        len(materialization_gates) == 2
        and all(gate["status"] == "pass" for gate in materialization_gates)
    )
    status = (
        "pass"
        if (
            generation["status"] == "pass"
            and clean_ok
            and gates_ok
            and materialization_ok
        )
        else "fail"
    )
    receipt = {
        "format_version": RECEIPT_FORMAT_VERSION,
        "kind": "spicy-regs-rulespec-paired-corpus-receipt",
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repositories": {
            "spicy_regs": {**spicy_git, "after_gates": spicy_git_after},
            "rulespec": {**rulespec_git, "after_gates": rulespec_git_after},
            "baseline_spicy_regs": (
                {**baseline_git, "after_gates": baseline_git_after}
                if baseline_git is not None
                else None
            ),
        },
        "rulespec_contract_digest": _contract_digest(rulespec_repo),
        "generation": generation,
        "materialization_gates": materialization_gates,
        "gates": gates,
        "clean_repository_gate": {
            "required": require_clean,
            "baseline_worktree_required": (
                require_clean and baseline_manifest_path is not None
            ),
            "baseline_worktree_proven": baseline_proven,
            "status": "pass" if clean_ok else "fail",
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate an ontology manifest and emit a paired Rulespec receipt",
    )
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--baseline-manifest", type=Path)
    parser.add_argument(
        "--baseline-repo",
        type=Path,
        help="Clean worktree used to produce the baseline manifest",
    )
    parser.add_argument("--spicy-repo", type=Path, default=Path.cwd())
    parser.add_argument(
        "--rulespec-repo",
        type=Path,
        default=Path.cwd().parent / "rulespec",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-materialization", action="store_true")
    parser.add_argument("--run-gates", action="store_true")
    parser.add_argument("--require-clean", action="store_true")
    args = parser.parse_args(argv)
    if args.run_materialization and (
        args.baseline_manifest is None or args.baseline_repo is None
    ):
        parser.error(
            "--run-materialization requires --baseline-manifest and --baseline-repo"
        )
    receipt = build_receipt(
        args.manifest,
        baseline_manifest_path=args.baseline_manifest,
        baseline_repo=(
            args.baseline_repo.resolve()
            if args.baseline_repo is not None
            else None
        ),
        spicy_repo=args.spicy_repo.resolve(),
        rulespec_repo=args.rulespec_repo.resolve(),
        output_path=args.output.resolve(),
        run_materialization=args.run_materialization,
        run_gates=args.run_gates,
        require_clean=args.require_clean,
    )
    print(
        f"{receipt['status'].upper()}: {args.output} "
        f"(snapshot {receipt['generation']['snapshot_id']})"
    )
    return 0 if receipt["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

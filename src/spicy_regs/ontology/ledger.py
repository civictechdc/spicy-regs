"""Immutable segment-processing ledger for ontology generations."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from pathlib import Path

from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    RunContext,
    canonical_json,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)
from spicy_regs.ontology.subjects import Artifact, Subject

OUTPUT = "ontology_segment_ledger.parquet"

SEGMENT_LEDGER_COLUMNS = (
    "segment_result_id",
    "subject_type",
    "subject_id",
    "subject_profile",
    "source_table",
    "artifact_digest",
    "segment_id",
    "segment_ordinal",
    "segment_count",
    "segment_policy",
    "tokenizer",
    "tokenizer_version",
    "token_count",
    "max_tokens",
    "min_tokens",
    "source_character_count",
    "fields_json",
    "context_fields_json",
    "field_sources_json",
    "source_spans_json",
    "source_sha256_json",
    "element_ids_json",
    "element_kinds_json",
    "boundaries_json",
    "previous_segment_id",
    "next_segment_id",
    "parent_segment_id",
    "exclusions_json",
    "status",
    "proposal_count",
    "accepted_assignment_ids_json",
    "rejections_json",
    "validation_json",
    "model_call_json",
    "attempts_json",
    "error_code",
    "error_message",
    *ATTESTATION_COLUMNS,
)

FINAL_STATUSES = frozenset(
    {
        "tagged",
        "zero_tags",
        "rejected_output",
        "skipped_non_content",
    }
)
KNOWN_STATUSES = FINAL_STATUSES | {"retry_exhausted"}


def _safe_error_message(error: BaseException | None) -> str | None:
    if error is None:
        return None
    message = " ".join(str(error).split())
    return message[:500] or None


def segment_result_row(
    *,
    subject: Subject,
    context: RunContext,
    actor_id: str,
    status: str,
    assignments: Sequence[dict] = (),
    rejections: Sequence[dict] = (),
    validation: Sequence[dict] = (),
    model_call: dict[str, object] | None = None,
    attempts: Sequence[dict] = (),
    exclusions: Sequence[dict] = (),
    error: BaseException | None = None,
) -> dict:
    """Create one auditable result for an exact segment version."""
    if status not in KNOWN_STATUSES:
        raise ValueError(f"Unknown segment result status: {status}")
    result_id = stable_id(
        "segment-result",
        context.run_id,
        subject.subject_type,
        subject.subject_id,
        subject.version_digest,
        subject.segment_id,
    )
    return {
        "segment_result_id": result_id,
        "subject_type": subject.subject_type,
        "subject_id": subject.subject_id,
        "subject_profile": subject.profile_id,
        "source_table": subject.source_table,
        "artifact_digest": subject.version_digest,
        "segment_id": subject.segment_id,
        "segment_ordinal": subject.segment_ordinal,
        "segment_count": subject.segment_count,
        "segment_policy": subject.segment_policy,
        "tokenizer": subject.tokenizer,
        "tokenizer_version": subject.tokenizer_version,
        "token_count": subject.token_count,
        "max_tokens": subject.max_segment_tokens,
        "min_tokens": subject.min_segment_tokens,
        "source_character_count": sum(
            len(value) for value in subject.fields.values()
        ),
        "fields_json": canonical_json(subject.fields),
        "context_fields_json": canonical_json(
            subject.context_fields or {}
        ),
        "field_sources_json": canonical_json(
            subject.field_sources or {
                field: field for field in subject.fields
            }
        ),
        "source_spans_json": canonical_json(subject.source_spans or {}),
        "source_sha256_json": canonical_json(
            subject.source_sha256 or {}
        ),
        "element_ids_json": canonical_json(subject.element_ids or {}),
        "element_kinds_json": canonical_json(
            subject.element_kinds or {}
        ),
        "boundaries_json": canonical_json(subject.boundaries or {}),
        "previous_segment_id": subject.previous_segment_id,
        "next_segment_id": subject.next_segment_id,
        "parent_segment_id": subject.parent_segment_id,
        "exclusions_json": canonical_json(list(exclusions)),
        "status": status,
        "proposal_count": len(assignments),
        "accepted_assignment_ids_json": canonical_json(
            sorted(
                str(row["assignment_id"])
                for row in assignments
                if row.get("assignment_id")
            )
        ),
        "rejections_json": canonical_json(list(rejections)),
        "validation_json": canonical_json(list(validation)),
        "model_call_json": (
            canonical_json(model_call) if model_call is not None else None
        ),
        "attempts_json": canonical_json(list(attempts)),
        "error_code": type(error).__name__ if error is not None else None,
        "error_message": _safe_error_message(error),
        **context.provenance(method="llm", actor_id=actor_id),
    }


def non_content_result_row(
    *,
    artifact: Artifact,
    context: RunContext,
    actor_id: str,
) -> dict:
    """Represent an explicitly selected artifact with no eligible content."""
    segment_id = "non_content_segment_" + hashlib.sha256(
        canonical_json(
            {
                "subject_type": artifact.subject_type,
                "subject_id": artifact.subject_id,
                "artifact_digest": artifact.digest,
                "profile": artifact.profile_id,
            }
        ).encode()
    ).hexdigest()[:24]
    subject = Subject(
        subject_type=artifact.subject_type,
        subject_id=artifact.subject_id,
        text="",
        fields={},
        digest=artifact.digest,
        profile_id=artifact.profile_id,
        source_table=artifact.source_table,
        allowed_schemes=artifact.allowed_schemes,
        artifact_digest=artifact.digest,
        segment_id=segment_id,
        segment_count=0,
        segment_policy="explicit-non-content-v1",
        tokenizer="none",
        tokenizer_version="none",
        context_fields=artifact.context_fields,
    )
    return segment_result_row(
        subject=subject,
        context=context,
        actor_id=actor_id,
        status="skipped_non_content",
        exclusions=[
            {
                "source_field": exclusion.source_field,
                "reason": exclusion.reason,
                "start_char": exclusion.start_char,
                "end_char": exclusion.end_char,
                "raw_text_sha256": exclusion.raw_text_sha256,
            }
            for exclusion in artifact.exclusions
        ],
    )


def write_segment_ledger(
    output_dir: Path,
    *,
    new_rows: Iterable[dict],
    prior_path: Path | None = None,
) -> Path:
    """Append exact result IDs and write a deterministic immutable ledger."""
    prior = read_parquet_rows(prior_path) if prior_path else []
    rows_by_id = {
        str(row["segment_result_id"]): dict(row)
        for row in [*prior, *new_rows]
        if row.get("segment_result_id")
    }
    rows = sorted(
        rows_by_id.values(),
        key=lambda row: (
            str(row.get("subject_type") or ""),
            str(row.get("subject_id") or ""),
            str(row.get("artifact_digest") or ""),
            int(str(row.get("segment_ordinal") or 0)),
            str(row.get("run_id") or ""),
            str(row.get("segment_result_id") or ""),
        ),
    )
    return write_parquet_rows(
        output_dir / OUTPUT,
        columns=SEGMENT_LEDGER_COLUMNS,
        rows=rows,
    )

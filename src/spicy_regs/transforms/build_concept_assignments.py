"""Transform: generate segment-backed, artifact-level concept assertions."""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import (
    RunContext,
    canonical_json,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import (
    ASSIGNMENT_COLUMNS,
    CONCEPT_COLUMNS,
    EVENT_COLUMNS,
    aggregate_segment_assignments,
    assignment_subject_digest,
    generate_for_subject,
    latest_assignments,
    merge_seed_registry,
    supersede_assignment_with_validation,
)
from spicy_regs.ontology.invariants import (
    assert_append_only,
    assert_attestation_complete,
    assert_concept_graphs,
)
from spicy_regs.ontology.ledger import (
    FINAL_STATUSES,
    OUTPUT as LEDGER_OUTPUT,
    non_content_result_row,
    segment_result_row,
    write_segment_ledger,
)
from spicy_regs.ontology.llm import (
    OntologyModel,
    OpenAIOntologyModel,
    model_call_metadata,
    model_tag_rejections,
)
from spicy_regs.ontology.subjects import (
    Artifact,
    balanced_artifact_batch,
    iter_artifacts,
    segment_artifact,
    subjects_by_segment_id,
)

OUTPUT = "concept_assignments.parquet"


def _checkpoint_rows(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [
        cast(dict, row)
        for row in value
        if isinstance(row, dict)
    ]


def _integer_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default


def _validation_selected(assignment_id: str, percent: int) -> bool:
    if percent <= 0:
        return False
    bucket = (
        int(hashlib.sha256(assignment_id.encode()).hexdigest()[:8], 16)
        % 100
    )
    return bucket < min(percent, 100)


def _assignment_evidence(assignment: dict) -> dict[str, Any]:
    try:
        value = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _already_validated(assignment: dict) -> bool:
    return "validation" in _assignment_evidence(assignment)


def _completed_artifact_digests(
    ledger_rows: list[dict],
) -> tuple[set[str], set[str]]:
    """Return complete digests and every digest represented in the ledger."""
    by_digest: dict[str, list[dict]] = defaultdict(list)
    for row in ledger_rows:
        digest = str(row.get("artifact_digest") or "")
        if digest:
            by_digest[digest].append(row)
    complete: set[str] = set()
    for digest, rows in by_digest.items():
        current_by_segment: dict[str, dict] = {}
        for row in rows:
            segment_id = str(row.get("segment_id") or "")
            if segment_id:
                current_by_segment[segment_id] = row
        current = list(current_by_segment.values())
        if not current or any(
            str(row.get("status") or "") not in FINAL_STATUSES
            for row in current
        ):
            continue
        if any(
            row.get("status") == "skipped_non_content"
            for row in current
        ):
            complete.add(digest)
            continue
        expected = max(
            int(str(row.get("segment_count") or 0))
            for row in current
        )
        if expected and len(current) == expected:
            complete.add(digest)
    return complete, set(by_digest)


def _artifact_exclusions(artifact: Artifact) -> list[dict]:
    return [
        {
            "source_field": exclusion.source_field,
            "reason": exclusion.reason,
            "start_char": exclusion.start_char,
            "end_char": exclusion.end_char,
            "raw_text_sha256": exclusion.raw_text_sha256,
        }
        for exclusion in artifact.exclusions
    ]


def _failure_attempt(
    error: BaseException,
    *,
    model_call: dict[str, object] | None,
) -> dict:
    return {
        "status": "retry_exhausted",
        "error_code": type(error).__name__,
        "error_message": " ".join(str(error).split())[:500],
        "model_call": model_call,
    }


def _span_work_id(assignment_id: str, span: dict) -> str:
    return stable_id(
        "validation-work",
        assignment_id,
        span.get("segment_id"),
        span.get("source_field"),
        span.get("start_char"),
        span.get("end_char"),
    )


def _single_span_assignment(assignment: dict, span: dict) -> dict:
    evidence = _assignment_evidence(assignment)
    evidence["spans"] = [span]
    return {
        **assignment,
        "evidence_json": canonical_json(evidence),
    }


def _update_ledger_validation(
    ledger_rows_by_segment: dict[str, dict],
    validation: dict,
) -> None:
    row = ledger_rows_by_segment.get(
        str(validation.get("segment_id") or "")
    )
    if row is None:
        return
    try:
        values = json.loads(row.get("validation_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        values = []
    if not isinstance(values, list):
        values = []
    by_work_id = {
        str(value.get("work_id")): value
        for value in values
        if isinstance(value, dict) and value.get("work_id")
    }
    by_work_id[str(validation["work_id"])] = validation
    row["validation_json"] = canonical_json(
        [
            by_work_id[key]
            for key in sorted(by_work_id)
        ]
    )


def build_concept_assignments(
    output_dir: Path,
    *,
    model: OntologyModel | None = None,
    run_id: str | None = None,
    asserted_at: str | None = None,
    generation_limit: int | None = None,
    validation_percent: int | None = None,
) -> Path:
    """Generate every selected artifact segment and aggregate its evidence."""
    concepts_file = output_dir / "concepts.parquet"
    if not concepts_file.exists():
        raise FileNotFoundError(
            f"concepts.parquet not found in {output_dir}"
        )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="concept-assignments",
    )
    concepts = read_parquet_rows(concepts_file)
    prior_file = output_dir / "_concept_assignments_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior = read_parquet_rows(prior_file)
    assignments = [dict(row) for row in prior]
    events: list[dict] = []
    prior_ledger_path = output_dir / "_ontology_segment_ledger_prior.parquet"
    if not prior_ledger_path.exists() and (
        output_dir / LEDGER_OUTPUT
    ).exists():
        prior_ledger_path = output_dir / LEDGER_OUTPUT
    prior_ledger = read_parquet_rows(prior_ledger_path)
    complete_digests, represented_digests = _completed_artifact_digests(
        prior_ledger
    )
    legacy_assignment_digests = {
        digest
        for row in latest_assignments(assignments)
        if (digest := assignment_subject_digest(row))
    }

    if model is None:
        model = OpenAIOntologyModel.from_environment()
    limit = (
        generation_limit
        if generation_limit is not None
        else _integer_env("ONTOLOGY_GENERATION_LIMIT", 500)
    )
    new_ledger_rows: list[dict] = []
    raw_segment_assignments: list[dict] = []
    if model is not None and limit:
        pending = balanced_artifact_batch(
            (
                artifact
                for artifact in iter_artifacts(output_dir)
                if (
                    artifact.digest not in complete_digests
                    and not (
                        artifact.digest not in represented_digests
                        and artifact.digest in legacy_assignment_digests
                    )
                )
            ),
            limit,
        )
        generation_checkpoint = BatchCheckpoint(
            output_dir,
            run_id=context.run_id,
            phase="assignment-generation",
        )
        for artifact in pending:
            segments = segment_artifact(artifact)
            if not segments:
                new_ledger_rows.append(
                    non_content_result_row(
                        artifact=artifact,
                        context=context,
                        actor_id=model.model_id,
                    )
                )
                continue
            exclusions = _artifact_exclusions(artifact)
            for subject in segments:
                cached = generation_checkpoint.get(
                    subject.subject_type,
                    subject.subject_id,
                    artifact_digest=subject.version_digest,
                    segment_id=subject.segment_id,
                )
                if (
                    cached is not None
                    and cached.get("status") in FINAL_STATUSES
                ):
                    new_concepts = _checkpoint_rows(
                        cached.get("concepts")
                    )
                    new_assignments = _checkpoint_rows(
                        cached.get("assignments")
                    )
                    new_events = _checkpoint_rows(cached.get("events"))
                    ledger_row = cached.get("ledger_row")
                else:
                    attempts = (
                        list(cached.get("attempts") or [])
                        if cached is not None
                        else []
                    )
                    try:
                        (
                            new_concepts,
                            new_assignments,
                            new_events,
                        ) = generate_for_subject(
                            subject=subject,
                            concepts=concepts,
                            model=model,
                            context=context,
                        )
                    except Exception as exc:
                        call = model_call_metadata(model)
                        attempts.append(
                            _failure_attempt(exc, model_call=call)
                        )
                        failure_row = segment_result_row(
                            subject=subject,
                            context=context,
                            actor_id=model.model_id,
                            status="retry_exhausted",
                            model_call=call,
                            attempts=attempts,
                            exclusions=(
                                exclusions
                                if subject.segment_ordinal == 0
                                else ()
                            ),
                            error=exc,
                        )
                        generation_checkpoint.append(
                            {
                                "subject_type": subject.subject_type,
                                "subject_id": subject.subject_id,
                                "artifact_digest": (
                                    subject.version_digest
                                ),
                                "segment_id": subject.segment_id,
                                "status": "retry_exhausted",
                                "actor_id": model.model_id,
                                "attempts": attempts,
                                "ledger_row": failure_row,
                                "model_call": call,
                            }
                        )
                        raise RuntimeError(
                            "Concept tagging failed for segment "
                            f"{subject.segment_id} of "
                            f"{subject.subject_type} {subject.subject_id}; "
                            "the exact segment checkpoint is resumable"
                        ) from exc
                    rejections = model_tag_rejections(model)
                    status = (
                        "tagged"
                        if new_assignments
                        else (
                            "rejected_output"
                            if rejections
                            else "zero_tags"
                        )
                    )
                    ledger_row = segment_result_row(
                        subject=subject,
                        context=context,
                        actor_id=model.model_id,
                        status=status,
                        assignments=new_assignments,
                        rejections=rejections,
                        model_call=model_call_metadata(model),
                        attempts=attempts,
                        exclusions=(
                            exclusions
                            if subject.segment_ordinal == 0
                            else ()
                        ),
                    )
                    generation_checkpoint.append(
                        {
                            "subject_type": subject.subject_type,
                            "subject_id": subject.subject_id,
                            "artifact_digest": subject.version_digest,
                            "segment_id": subject.segment_id,
                            "subject_profile": subject.profile_id,
                            "source_table": subject.source_table,
                            "status": status,
                            "actor_id": model.model_id,
                            "concepts": new_concepts,
                            "assignments": new_assignments,
                            "events": new_events,
                            "attempts": attempts,
                            "ledger_row": ledger_row,
                            "model_call": model_call_metadata(model),
                        }
                    )
                concepts = merge_seed_registry(concepts, new_concepts)
                raw_segment_assignments.extend(new_assignments)
                events.extend(new_events)
                if isinstance(ledger_row, dict):
                    new_ledger_rows.append(cast(dict, ledger_row))

        prior_current = latest_assignments(assignments)
        supersedes_by_key = {
            (
                str(row.get("subject_type") or ""),
                str(row.get("subject_id") or ""),
                str(row.get("concept_id") or ""),
            ): str(row["assignment_id"])
            for row in prior_current
            if row.get("assignment_id")
        }
        assignments.extend(
            aggregate_segment_assignments(
                raw_segment_assignments,
                context=context,
                actor_id=model.model_id,
                supersedes_by_key=supersedes_by_key,
            )
        )
        assignments = list(
            {
                str(row["assignment_id"]): row
                for row in assignments
                if row.get("assignment_id")
            }.values()
        )

        percent = (
            validation_percent
            if validation_percent is not None
            else _integer_env("ONTOLOGY_VALIDATION_PERCENT", 10)
        )
        latest = latest_assignments(assignments)
        concepts_by_id = {
            str(row["concept_id"]): row
            for row in concepts
        }
        selected_assignments = [
            row
            for row in latest
            if (
                row.get("method") == "llm"
                and row.get("run_id") == context.run_id
                and not _already_validated(row)
                and _validation_selected(
                    str(row.get("assignment_id") or ""),
                    percent,
                )
            )
        ]
        segment_ids = {
            str(span.get("segment_id"))
            for row in selected_assignments
            for span in _assignment_evidence(row).get("spans") or []
            if isinstance(span, dict) and span.get("segment_id")
        }
        validation_subjects = subjects_by_segment_id(
            output_dir,
            segment_ids,
        )
        ledger_rows_by_segment = {
            str(row.get("segment_id") or ""): row
            for row in new_ledger_rows
            if row.get("segment_id")
        }
        validation_checkpoint = BatchCheckpoint(
            output_dir,
            run_id=context.run_id,
            phase="assignment-validation",
        )
        for row in selected_assignments:
            assignment_id = str(row.get("assignment_id") or "")
            validations: list[dict] = []
            spans = _assignment_evidence(row).get("spans") or []
            for span_value in spans:
                if not isinstance(span_value, dict):
                    continue
                span = cast(dict, span_value)
                segment_id = str(span.get("segment_id") or "")
                subject = validation_subjects.get(segment_id)
                concept = concepts_by_id.get(
                    str(row.get("concept_id") or "")
                )
                if subject is None or concept is None:
                    raise RuntimeError(
                        "Validation evidence does not resolve to its "
                        f"segment or concept: {assignment_id}"
                    )
                work_id = _span_work_id(assignment_id, span)
                cached = validation_checkpoint.get(
                    subject.subject_type,
                    subject.subject_id,
                    artifact_digest=subject.version_digest,
                    segment_id=subject.segment_id,
                    work_id=work_id,
                )
                if cached is None:
                    try:
                        result = model.validate(
                            subject=subject,
                            concept=concept,
                            assignment=_single_span_assignment(
                                row,
                                span,
                            ),
                        )
                    except Exception as exc:
                        validation_checkpoint.append(
                            {
                                "subject_type": subject.subject_type,
                                "subject_id": subject.subject_id,
                                "artifact_digest": (
                                    subject.version_digest
                                ),
                                "segment_id": subject.segment_id,
                                "work_id": work_id,
                                "assignment_id": assignment_id,
                                "status": "retry_exhausted",
                                "actor_id": model.model_id,
                                "error_code": type(exc).__name__,
                                "error_message": (
                                    " ".join(str(exc).split())[:500]
                                ),
                                "model_call": model_call_metadata(model),
                            }
                        )
                        raise RuntimeError(
                            "Concept validation failed for assignment "
                            f"{assignment_id}, segment "
                            f"{subject.segment_id}; the checkpoint is "
                            "resumable"
                        ) from exc
                    cached = {
                        "subject_type": subject.subject_type,
                        "subject_id": subject.subject_id,
                        "artifact_digest": subject.version_digest,
                        "segment_id": subject.segment_id,
                        "work_id": work_id,
                        "assignment_id": assignment_id,
                        "status": "completed",
                        "actor_id": model.model_id,
                        "source_field": span.get("source_field"),
                        "start_char": span.get("start_char"),
                        "end_char": span.get("end_char"),
                        "agrees": result.agrees,
                        "confidence": result.confidence,
                        "rationale": result.rationale,
                        "model_call": model_call_metadata(model),
                    }
                    validation_checkpoint.append(cached)
                if cached.get("status") != "completed":
                    continue
                validation = {
                    "work_id": work_id,
                    "assignment_id": assignment_id,
                    "segment_id": subject.segment_id,
                    "source_field": span.get("source_field"),
                    "start_char": span.get("start_char"),
                    "end_char": span.get("end_char"),
                    "agrees": cached.get("agrees") is True,
                    "confidence": float(
                        str(cached.get("confidence") or 0)
                    ),
                    "rationale": cached.get("rationale"),
                    "actor_id": model.model_id,
                    "model_call": cached.get("model_call"),
                }
                validations.append(validation)
                _update_ledger_validation(
                    ledger_rows_by_segment,
                    validation,
                )
            if validations:
                assignments.append(
                    supersede_assignment_with_validation(
                        row,
                        validations=validations,
                        context=context,
                        actor_id=model.model_id,
                    )
                )

    assignments = list(
        {
            str(row["assignment_id"]): row
            for row in assignments
            if row.get("assignment_id")
        }.values()
    )
    assignments.sort(
        key=lambda row: (
            row.get("subject_type") or "",
            row.get("subject_id") or "",
            row["assignment_id"],
        )
    )
    assert_append_only(
        prior,
        assignments,
        id_column="assignment_id",
    )
    assert_attestation_complete(assignments)
    assert_concept_graphs(concepts)

    concepts.sort(
        key=lambda row: (
            row.get("scheme") or "",
            row.get("pref_label") or "",
            row["concept_id"],
        )
    )
    write_parquet_rows(
        concepts_file,
        columns=CONCEPT_COLUMNS,
        rows=concepts,
    )
    if events:
        existing_events = read_parquet_rows(
            output_dir / "concept_events.parquet"
        )
        event_by_id = {
            str(row["event_id"]): row
            for row in [*existing_events, *events]
            if row.get("event_id")
        }
        write_parquet_rows(
            output_dir / "concept_events.parquet",
            columns=EVENT_COLUMNS,
            rows=sorted(
                event_by_id.values(),
                key=lambda row: (
                    row.get("asserted_at") or "",
                    row["event_id"],
                ),
            ),
        )

    write_segment_ledger(
        output_dir,
        new_rows=new_ledger_rows,
        prior_path=(
            prior_ledger_path
            if prior_ledger_path.exists()
            else None
        ),
    )
    out_file = write_parquet_rows(
        output_dir / OUTPUT,
        columns=ASSIGNMENT_COLUMNS,
        rows=assignments,
    )
    logger.info(
        "Concept assignments: {:,} append-only rows "
        "({:,} current after supersession); {:,} segment results",
        len(assignments),
        len(latest_assignments(assignments)),
        len(new_ledger_rows),
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(
        ASSIGNMENT_COLUMNS
    )
    return out_file

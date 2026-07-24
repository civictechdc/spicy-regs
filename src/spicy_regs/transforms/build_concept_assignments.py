"""Transform: append LLM-generated and validation-superseding concept assertions."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import RunContext, read_parquet_rows, write_parquet_rows
from spicy_regs.ontology.concepts import (
    ASSIGNMENT_COLUMNS,
    CONCEPT_COLUMNS,
    EVENT_COLUMNS,
    assignment_subject_digest,
    generate_for_subject,
    latest_assignments,
    make_assignment,
    merge_seed_registry,
)
from spicy_regs.ontology.invariants import (
    assert_append_only,
    assert_attestation_complete,
    assert_concept_graphs,
)
from spicy_regs.ontology.llm import OntologyModel, OpenAIOntologyModel, TagProposal
from spicy_regs.ontology.subjects import build_subjects

OUTPUT = "concept_assignments.parquet"


def _checkpoint_rows(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [cast(dict, row) for row in value if isinstance(row, dict)]


def _integer_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default


def _validation_selected(assignment_id: str, percent: int) -> bool:
    if percent <= 0:
        return False
    bucket = int(hashlib.sha256(assignment_id.encode()).hexdigest()[:8], 16) % 100
    return bucket < min(percent, 100)


def _proposal_from_assignment(assignment: dict, confidence: float, rationale: str) -> TagProposal:
    try:
        evidence = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        evidence = {}
    span = (evidence.get("spans") or [{}])[0]
    return TagProposal(
        concept_id=str(assignment.get("concept_id")),
        proposed_label=None,
        scheme="subject",
        definition=None,
        confidence=confidence,
        evidence_text=str(span.get("text") or ""),
        evidence_field=str(span.get("source_field") or ""),
        justification=str(evidence.get("justification") or rationale),
    )


def _already_validated(assignment: dict) -> bool:
    """Whether this assertion is itself the result of a validation pass."""
    try:
        evidence = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return False
    return isinstance(evidence, dict) and "validation" in evidence


def build_concept_assignments(
    output_dir: Path,
    *,
    model: OntologyModel | None = None,
    run_id: str | None = None,
    asserted_at: str | None = None,
    generation_limit: int | None = None,
    validation_percent: int | None = None,
) -> Path:
    """Generate assignments and append validation disagreements as supersessions."""
    concepts_file = output_dir / "concepts.parquet"
    if not concepts_file.exists():
        raise FileNotFoundError(f"concepts.parquet not found in {output_dir}")
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="concept-assignments")
    concepts = read_parquet_rows(concepts_file)
    prior_file = output_dir / "_concept_assignments_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior = read_parquet_rows(prior_file)
    assignments = [dict(row) for row in prior]
    events: list[dict] = []
    subjects = build_subjects(output_dir)
    subjects_by_key = {(subject.subject_type, subject.subject_id): subject for subject in subjects}

    if model is None:
        model = OpenAIOntologyModel.from_environment()
    limit = generation_limit if generation_limit is not None else _integer_env("ONTOLOGY_GENERATION_LIMIT", 500)
    if model is not None and limit:
        current = latest_assignments(assignments)
        current_by_subject: dict[tuple[str, str], list[dict]] = {}
        for row in current:
            current_by_subject.setdefault(
                (str(row.get("subject_type")), str(row.get("subject_id"))),
                [],
            ).append(row)
        pending = [
            subject
            for subject in subjects
            if not any(
                assignment_subject_digest(row) == subject.digest
                for row in current_by_subject.get((subject.subject_type, subject.subject_id), ())
            )
        ][:limit]
        checkpoint = BatchCheckpoint(output_dir, run_id=context.run_id, phase="assignment-generation")
        for subject in pending:
            cached = checkpoint.get(subject.subject_type, subject.subject_id)
            if cached is None:
                try:
                    new_concepts, new_assignments, new_events = generate_for_subject(
                        subject=subject,
                        concepts=concepts,
                        model=model,
                        context=context,
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Concept tagging failed for {subject.subject_type} "
                        f"{subject.subject_id}; the checkpoint is resumable"
                    ) from exc
                cached = {
                    "subject_type": subject.subject_type,
                    "subject_id": subject.subject_id,
                    "concepts": new_concepts,
                    "assignments": new_assignments,
                    "events": new_events,
                }
                checkpoint.append(cached)
            concepts = merge_seed_registry(
                concepts,
                _checkpoint_rows(cached.get("concepts")),
            )
            assignments.extend(_checkpoint_rows(cached.get("assignments")))
            events.extend(_checkpoint_rows(cached.get("events")))

        # De-duplicate stable ids when a run resumes from its checkpoint.
        assignments = list({str(row["assignment_id"]): row for row in assignments if row.get("assignment_id")}.values())

        percent = (
            validation_percent if validation_percent is not None else _integer_env("ONTOLOGY_VALIDATION_PERCENT", 10)
        )
        latest = latest_assignments(assignments)
        concepts_by_id = {str(row["concept_id"]): row for row in concepts}
        validation_checkpoint = BatchCheckpoint(output_dir, run_id=context.run_id, phase="assignment-validation")
        for row in latest:
            assignment_id = str(row.get("assignment_id") or "")
            if (
                row.get("method") != "llm"
                or _already_validated(row)
                or not _validation_selected(assignment_id, percent)
            ):
                continue
            subject = subjects_by_key.get((str(row.get("subject_type")), str(row.get("subject_id"))))
            concept = concepts_by_id.get(str(row.get("concept_id")))
            if subject is None or concept is None:
                continue
            cached = validation_checkpoint.get(subject.subject_type, f"{subject.subject_id}:{assignment_id}")
            if cached is None:
                try:
                    result = model.validate(subject=subject, concept=concept, assignment=row)
                except Exception as exc:
                    raise RuntimeError(
                        f"Concept validation failed for assignment {assignment_id}; the checkpoint is resumable"
                    ) from exc
                cached = {
                    "subject_type": subject.subject_type,
                    "subject_id": f"{subject.subject_id}:{assignment_id}",
                    "agrees": result.agrees,
                    "confidence": result.confidence,
                    "rationale": result.rationale,
                }
                validation_checkpoint.append(cached)
            if cached.get("agrees"):
                continue
            old_confidence = float(row.get("confidence") or 0)
            revised_confidence = min(old_confidence, float(cached.get("confidence") or 0))
            proposal = _proposal_from_assignment(
                row,
                revised_confidence,
                str(cached.get("rationale") or ""),
            )
            assignments.append(
                make_assignment(
                    subject=subject,
                    concept_id=str(row["concept_id"]),
                    proposal=proposal,
                    context=context,
                    actor_id=model.model_id,
                    ordinal=0,
                    supersedes_id=assignment_id,
                    validation={
                        "agrees": False,
                        "confidence": revised_confidence,
                        "rationale": cached.get("rationale"),
                    },
                )
            )

    assignments = list({str(row["assignment_id"]): row for row in assignments if row.get("assignment_id")}.values())
    assignments.sort(key=lambda row: (row.get("subject_type") or "", row.get("subject_id") or "", row["assignment_id"]))
    assert_append_only(prior, assignments, id_column="assignment_id")
    assert_attestation_complete(assignments)
    assert_concept_graphs(concepts)

    # Candidate creation is part of the same assertion run. Persist the updated
    # registry and pending structural events alongside the assignment output;
    # the rollup pipeline publishes all changed ontology artifacts together.
    concepts.sort(key=lambda row: (row.get("scheme") or "", row.get("pref_label") or "", row["concept_id"]))
    write_parquet_rows(concepts_file, columns=CONCEPT_COLUMNS, rows=concepts)
    if events:
        existing_events = read_parquet_rows(output_dir / "concept_events.parquet")
        event_by_id = {str(row["event_id"]): row for row in [*existing_events, *events] if row.get("event_id")}
        write_parquet_rows(
            output_dir / "concept_events.parquet",
            columns=EVENT_COLUMNS,
            rows=sorted(event_by_id.values(), key=lambda row: (row.get("asserted_at") or "", row["event_id"])),
        )

    out_file = write_parquet_rows(output_dir / OUTPUT, columns=ASSIGNMENT_COLUMNS, rows=assignments)
    logger.info(
        "Concept assignments: {:,} append-only rows ({:,} current after supersession)",
        len(assignments),
        len(latest_assignments(assignments)),
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(ASSIGNMENT_COLUMNS)
    return out_file

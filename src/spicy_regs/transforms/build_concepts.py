"""Transform: build the SKOS-style concept registry and convergence state."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger
from refspec.registry.federal_register_thesaurus_2025 import (
    federal_register_thesaurus_2025_concept_iri,
    load_packaged_federal_register_thesaurus_2025,
)

from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import (
    RunContext,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import (
    CONCEPT_COLUMNS,
    generate_for_subject,
    latest_assignments,
    merge_pass,
    merge_seed_registry,
    rescore_candidates,
    seed_federal_register_thesaurus_2025_concept,
)
from spicy_regs.ontology.concept_dimensions import concept_facet, concept_source_vocabulary
from spicy_regs.ontology.invariants import assert_concept_graphs
from spicy_regs.ontology.llm import (
    OntologyModel,
    OpenAIOntologyModel,
    model_call_metadata,
)
from spicy_regs.ontology.subjects import (
    balanced_artifact_batch,
    iter_artifacts,
    segment_artifact,
)

OUTPUT = "concepts.parquet"


def _integer_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except ValueError:
        logger.warning("{} is not an integer; using {}", name, default)
        return default


def build_concepts(
    output_dir: Path,
    *,
    model: OntologyModel | None = None,
    run_id: str | None = None,
    asserted_at: str | None = None,
    discovery_limit: int | None = None,
) -> Path:
    """Seed subject concepts, discover candidates, merge, and re-score."""
    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="concepts")

    # When the pipeline primes a published registry it lands at the normal
    # output name. Tests and orchestrators may instead provide _concepts_prior.
    prior_file = output_dir / "_concepts_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior = read_parquet_rows(prior_file)

    # Current FederalRegister.gov Topics remain mutable, source-assigned
    # document metadata. They are not a concept registry. Normal builds seed
    # the exact official terms and only globally unambiguous variants from the
    # packaged April 1, 2025 thesaurus.
    thesaurus = load_packaged_federal_register_thesaurus_2025()
    variants_by_target: dict[str, set[str]] = {}
    for variant in thesaurus.variants:
        if (
            variant.resolution_status != "recognizedVariant"
            or len(variant.target_concept_ids) != 1
        ):
            continue
        variants_by_target.setdefault(
            variant.target_concept_ids[0],
            set(),
        ).add(variant.label)
    seeds = [
        seed_federal_register_thesaurus_2025_concept(
            concept_id=term.concept_id,
            concept_iri=federal_register_thesaurus_2025_concept_iri(
                term.concept_id
            ),
            preferred_label=term.label,
            alternate_labels=sorted(
                variants_by_target.get(term.concept_id, set()),
                key=lambda value: (value.casefold(), value),
            ),
            context=context,
        )
        for term in thesaurus.official_terms
    ]
    concepts = merge_seed_registry(prior, seeds)

    assignments_file = output_dir / "_concept_assignments_prior.parquet"
    if not assignments_file.exists():
        assignments_file = output_dir / "concept_assignments.parquet"
    assignments = read_parquet_rows(assignments_file)
    current_assignments = latest_assignments(assignments)

    # Candidate discovery is an opt-in, bounded, resumable pass. The assignment
    # rollup is the canonical generator so normal weekly operation does not pay
    # for duplicate model calls. Keyless runs still produce the complete
    # deterministic Thesaurus seed registry.
    limit = discovery_limit if discovery_limit is not None else _integer_env("ONTOLOGY_DISCOVERY_LIMIT", 0)
    if model is None and limit:
        model = OpenAIOntologyModel.from_environment()
    if model is not None and limit:
        assignments_by_subject: dict[tuple[str, str], list[dict]] = {}
        for row in current_assignments:
            assignments_by_subject.setdefault(
                (str(row.get("subject_type")), str(row.get("subject_id"))),
                [],
            ).append(row)
        pending = balanced_artifact_batch(
            (
                artifact
                for artifact in iter_artifacts(output_dir)
                if not any(
                    _assignment_digest(assignment) == artifact.digest
                    for assignment in assignments_by_subject.get(
                        (
                            artifact.subject_type,
                            artifact.subject_id,
                        ),
                        (),
                    )
                )
            ),
            limit,
        )
        checkpoint = BatchCheckpoint(output_dir, run_id=context.run_id, phase="concept-discovery")
        for artifact in pending:
            for subject in segment_artifact(artifact):
                cached = checkpoint.get(
                    subject.subject_type,
                    subject.subject_id,
                    artifact_digest=subject.version_digest,
                    segment_id=subject.segment_id,
                )
                if cached is not None:
                    candidates = cached.get("concepts") or []
                else:
                    try:
                        candidates, _, _ = generate_for_subject(
                            subject=subject,
                            concepts=concepts,
                            model=model,
                            context=context,
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            "Concept discovery failed for segment "
                            f"{subject.segment_id} of "
                            f"{subject.subject_type} "
                            f"{subject.subject_id}; the checkpoint is "
                            "resumable"
                        ) from exc
                    checkpoint.append(
                        {
                            "subject_type": subject.subject_type,
                            "subject_id": subject.subject_id,
                            "artifact_digest": (subject.version_digest),
                            "segment_id": subject.segment_id,
                            "subject_profile": subject.profile_id,
                            "source_table": subject.source_table,
                            "concepts": candidates,
                            "model_call": model_call_metadata(model),
                        }
                    )
                concepts = merge_seed_registry(concepts, candidates)

    concepts, merge_events, review = merge_pass(concepts, current_assignments, context=context)
    concepts, rescore_events = rescore_candidates(concepts, current_assignments, context=context)
    assert_concept_graphs(concepts)
    concepts.sort(
        key=lambda row: (
            concept_facet(row),
            concept_source_vocabulary(row),
            row.get("pref_label") or "",
            row["concept_id"],
        )
    )

    out_file = write_parquet_rows(output_dir / OUTPUT, columns=CONCEPT_COLUMNS, rows=concepts)
    if merge_events or rescore_events:
        pending_events = output_dir / "_concept_events_pending.jsonl"
        with pending_events.open("a", encoding="utf-8") as handle:
            for event in [*merge_events, *rescore_events]:
                handle.write(f"{json.dumps(event, sort_keys=True)}\n")
    review_path = output_dir / "concept_merge_review.jsonl"
    with review_path.open("w", encoding="utf-8") as handle:
        for item in review:
            handle.write(f"{json.dumps(item, sort_keys=True)}\n")

    # Reconcile seeds and the exact convergence payloads now, while the pending
    # event detail is still local. The concepts rollup publishes this table as a
    # sidecar; the later concept-events rollup remains an idempotent safety pass.
    from spicy_regs.transforms.build_concept_events import build_concept_events

    build_concept_events(
        output_dir,
        run_id=context.run_id,
        asserted_at=context.asserted_at,
    )
    (output_dir / "_concept_events_pending.jsonl").unlink(missing_ok=True)

    logger.info(
        "Concepts: {:,} rows ({:,} active, {:,} candidate, {:,} deprecated); {:,} merge-review items",
        len(concepts),
        sum(row["status"] == "active" for row in concepts),
        sum(row["status"] == "candidate" for row in concepts),
        sum(row["status"] == "deprecated" for row in concepts),
        len(review),
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(CONCEPT_COLUMNS)
    return out_file


def _assignment_digest(assignment: dict) -> str | None:
    try:
        evidence = json.loads(assignment.get("evidence_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    value = evidence.get("subject_sha256")
    return str(value) if value else None

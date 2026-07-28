"""Transform: maintain the append-only audit log of concept structural changes."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.common import RunContext, read_parquet_rows, write_parquet_rows
from spicy_regs.ontology.concepts import EVENT_COLUMNS, make_event
from spicy_regs.ontology.concept_dimensions import concept_facet, concept_source_vocabulary
from spicy_regs.ontology.invariants import assert_append_only, assert_attestation_complete

OUTPUT = "concept_events.parquet"
ACTOR_ID = "spicy-regs:concept-event-reconciler:v1"


def _event_payload(event: dict) -> dict:
    try:
        parsed = json.loads(event.get("payload_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_concept_events(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Reconcile registry state into an append-only event trail."""
    concepts_file = output_dir / "concepts.parquet"
    if not concepts_file.exists():
        raise FileNotFoundError(f"concepts.parquet not found in {output_dir}")
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="concept-events")
    prior_file = output_dir / "_concept_events_prior.parquet"
    if not prior_file.exists() and (output_dir / OUTPUT).exists():
        prior_file = output_dir / OUTPUT
    prior = read_parquet_rows(prior_file)
    events = [dict(row) for row in prior]

    pending_file = output_dir / "_concept_events_pending.jsonl"
    if pending_file.exists():
        for line in pending_file.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)

    existing_keys: set[tuple[str, str]] = set()
    candidate_seeds: set[str] = set()
    for event in events:
        payload = _event_payload(event)
        concept_id = payload.get("concept_id") or payload.get("loser_id") or payload.get("winner_id")
        if concept_id:
            existing_keys.add((str(event.get("event_type")), str(concept_id)))
        if event.get("event_type") == "seed" and payload.get("source") == "llm_candidate":
            candidate_seeds.add(str(payload.get("concept_id")))

    concepts = read_parquet_rows(concepts_file)
    for concept in concepts:
        concept_id = str(concept["concept_id"])
        if ("seed", concept_id) not in existing_keys:
            source = (
                "federal_register_thesaurus"
                if concept.get("actor_id") == "federal-register-thesaurus:v1"
                else "llm_candidate"
                if concept.get("method") == "llm"
                else "registry"
            )
            events.append(
                make_event(
                    "seed",
                    {
                        "concept_id": concept_id,
                        "label": concept.get("pref_label"),
                        "facet": concept_facet(concept),
                        "source_vocabulary": concept_source_vocabulary(concept),
                        # Compatibility for existing event readers.
                        "scheme": concept_facet(concept),
                        "source": source,
                    },
                    context=context,
                    method=str(concept.get("method") or "deterministic"),
                    actor_id=str(concept.get("actor_id") or ACTOR_ID),
                )
            )
            if source == "llm_candidate":
                candidate_seeds.add(concept_id)

        if concept.get("status") == "deprecated" and concept.get("replaced_by"):
            if ("merge", concept_id) not in existing_keys:
                events.append(
                    make_event(
                        "merge",
                        {
                            "loser_id": concept_id,
                            "loser_label": concept.get("pref_label"),
                            "winner_id": concept.get("replaced_by"),
                            "source": "registry_reconciliation",
                        },
                        context=context,
                        method=str(concept.get("method") or "deterministic"),
                        actor_id=str(concept.get("actor_id") or ACTOR_ID),
                    )
                )
        elif concept.get("status") == "deprecated" and ("deprecate", concept_id) not in existing_keys:
            events.append(
                make_event(
                    "deprecate",
                    {
                        "concept_id": concept_id,
                        "label": concept.get("pref_label"),
                        "source": "registry_reconciliation",
                    },
                    context=context,
                    method=str(concept.get("method") or "deterministic"),
                    actor_id=str(concept.get("actor_id") or ACTOR_ID),
                )
            )
        elif (
            concept.get("status") == "active"
            and concept_id in candidate_seeds
            and ("promote", concept_id) not in existing_keys
        ):
            events.append(
                make_event(
                    "promote",
                    {
                        "concept_id": concept_id,
                        "label": concept.get("pref_label"),
                        "source": "registry_reconciliation",
                    },
                    context=context,
                    method=str(concept.get("method") or "deterministic"),
                    actor_id=str(concept.get("actor_id") or ACTOR_ID),
                )
            )

    events = list({str(row["event_id"]): row for row in events if row.get("event_id")}.values())
    events.sort(key=lambda row: (row.get("asserted_at") or "", row["event_id"]))
    assert_append_only(prior, events, id_column="event_id")
    assert_attestation_complete(events)
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=EVENT_COLUMNS, rows=events)
    logger.info("Concept events: {:,} append-only rows", len(events))
    assert pq.ParquetFile(out_file).schema_arrow.names == list(EVENT_COLUMNS)
    return out_file

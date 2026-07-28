"""Build the blind gold-adjudication input for the Rulespec MVP phase 1.1.

Phase 1.1 of ``docs/rulespec-testbed-path-forward.md`` asks two judge models
from different families than the tagger to grade each stored gold assignment
against the fixed top-12 candidates drawn from the gold-free concept registry.
The protocol is blind: **a judge never sees tagger output.** This builder emits
exactly the judge's half of that protocol and nothing else — no model
assignments, no diagnostic-run predictions, no scores, no metrics.

Two properties matter more than convenience here:

* **Blindness.** Every emitted field is projected through an explicit
  whitelist, so a tagger column appearing in an input table cannot leak into
  the output by being copied wholesale.
* **Selector parity.** Candidates are produced by the *production* selector
  (``spicy_regs.ontology.concepts.select_candidate_concepts_for_text``) at the
  production depth (``rulespec_testbed.PROMPT_CONCEPT_LIMIT``), never by a
  local reimplementation. Each call is additionally compared against the
  ``available_concepts`` list the payload builder put in front of the model for
  the same segment, and the agreement is recorded per segment.

``--selector anchored-hybrid-v2`` swaps in the candidate-generation experiment
(``select_candidate_concepts_anchored_v2``) for a comparison round. It is not
the production selector, so the payload-parity check does not apply to it — the
stored payloads were built by ``lexical-overlap-v1`` — and that is recorded in
the metadata rather than reported as 35 mismatches. The default is unchanged.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.adapters.openai import (
    PROMPT_INPUT_TOKEN_BUDGET,
    PROMPT_SAFETY_MARGIN_TOKENS,
    TiktokenCounter,
)
from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.docpipeline.tag_task import TagExtractionTask
from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.evaluation_boundary import (
    DEFAULT_BOUNDARY_MANIFEST,
    DEVELOPMENT_DATASET_ID,
)
from spicy_regs.ontology.common import canonical_json, read_parquet_rows
from spicy_regs.ontology.concept_dimensions import concept_facet, concept_source_vocabulary
from spicy_regs.ontology.llm import ontology_concept_payload
from spicy_regs.ontology.concepts import (
    ANCHORED_SELECTOR_VERSION,
    select_candidate_concepts_anchored_v2,
    select_candidate_concepts_for_text,
)
from spicy_regs.rulespec_testbed import (
    GOLD_FILE,
    PROMPT_CONCEPT_LIMIT,
    load_testbed_inputs,
)

# The production selector, held by reference. Tests assert this *is* the
# ontology function, so no copy of the ranking logic can drift in beside it.
SELECT_CANDIDATES = select_candidate_concepts_for_text
# The candidate-generation experiment, held the same way.
SELECT_CANDIDATES_V2 = select_candidate_concepts_anchored_v2

SELECTOR_V1 = "lexical-overlap-v1"
SELECTOR_V2 = ANCHORED_SELECTOR_VERSION
SELECTOR_CHOICES = (SELECTOR_V1, SELECTOR_V2)

SCHEMA_VERSION = "gold-adjudication-input-v1"

BLINDNESS_STATEMENT = (
    "blind: contains no tagger output. No model assignments, no diagnostic-run "
    "predictions, no confidences, no scores, and no metrics appear in this file. "
    "Every field is derived from the frozen gold labels, the source corpus, the "
    "frozen segmentation, and the gold-free concept registry."
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_DIR = REPO_ROOT / "output" / "segmentation-tagging-document-openai-structure-overlap-1800-v4"
DEFAULT_DATASET_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
SELECTION_FILE_NAME = "tagging_segments.parquet"
REGISTRY_FILE_NAME = "tagging_input_registry.parquet"


class GoldAdjudicationError(RuntimeError):
    """The stored inputs cannot produce a usable adjudication file."""


def _json_list(value: object) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item]


def _text_or_none(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def candidate_record(concept: Mapping[str, Any]) -> dict[str, Any]:
    """Project one registry row to the fields a judge is allowed to read.

    The whitelist is the point: registry rows also carry attestation columns
    (``actor_id``, ``run_id``, ``method``, ``asserted_at``) and any table this
    builder is pointed at may carry more. Nothing outside these keys is copied.
    """
    record = {
        "concept_id": str(concept.get("concept_id") or ""),
        "facet": concept_facet(concept),
        "source_vocabulary": concept_source_vocabulary(concept),
        "scheme": concept_facet(concept),
        "pref_label": _text_or_none(concept.get("pref_label")),
        "alt_labels": _json_list(concept.get("alt_labels_json")),
        "definition": _text_or_none(concept.get("definition")),
        "broader_id": _text_or_none(concept.get("broader_id")),
        "status": _text_or_none(concept.get("status")),
    }
    # A selector that stamps its own version on the rows it returns carries
    # that stamp through; the production selector does not stamp anything.
    version = _text_or_none(concept.get("selector_version"))
    if version:
        record["selector_version"] = version
    return record


def segment_candidates(
    unit: ExtractionUnit,
    registry_rows: Sequence[Mapping[str, Any]],
    *,
    limit: int = PROMPT_CONCEPT_LIMIT,
    selector: str = SELECTOR_V1,
) -> tuple[list[dict[str, Any]], bool | None]:
    """Return one segment's ranked candidates plus its payload-parity flag.

    ``tag_unit`` stores each slice's exact text under ``evidence_<index>`` in
    slice order, so joining those values with newlines reconstructs
    ``ProcessingSegment.text`` — the string the payload builder handed to the
    selector. The parity flag reports whether this call reproduced the payload's
    ``available_concepts`` ordering exactly, and is ``None`` for any selector
    other than the production one, whose ordering the payload records.
    """
    unit_input = unit.input
    fields = unit_input.get("untrusted_evidence_fields", {}).get("fields", {})
    segment_text = "\n".join(str(value) for value in fields.values())
    allowed_schemes = [str(scheme) for scheme in unit_input.get("subject", {}).get("allowed_schemes", ())]
    if selector == SELECTOR_V2:
        selected = list(
            SELECT_CANDIDATES_V2(
                segment_text,
                registry_rows,
                allowed_facets=allowed_schemes,
                limit=limit,
            )
        )
        # Exercise the same payload validation and complete prompt-budget path
        # used before a provider call. The experimental ranking is stable, so
        # fitting removes only the lowest-ranked tail.
        task = TagExtractionTask()
        counter = TiktokenCounter()
        while True:
            candidate_input = {
                **unit_input,
                "available_concepts": [ontology_concept_payload(dict(concept)) for concept in selected],
            }
            payload = task.build_payload(candidate_input)
            prompt_total = (
                counter.count(task.instructions + "\n" + canonical_json(payload))
                + counter.count(canonical_json(task.build_schema(payload)))
                + PROMPT_SAFETY_MARGIN_TOKENS
            )
            if prompt_total <= PROMPT_INPUT_TOKEN_BUDGET:
                break
            if not selected:
                raise GoldAdjudicationError(
                    "candidate experiment exceeds the real prompt budget with "
                    f"no candidates: {prompt_total} > {PROMPT_INPUT_TOKEN_BUDGET}"
                )
            selected.pop()
        return [candidate_record(concept) for concept in selected], None
    selected = SELECT_CANDIDATES(segment_text, allowed_schemes, registry_rows, limit=limit)
    selected_ids = [str(concept.get("concept_id") or "") for concept in selected]
    payload_ids = [str(concept.get("concept_id") or "") for concept in unit_input.get("available_concepts", ())]
    return [candidate_record(concept) for concept in selected], selected_ids == payload_ids


def _merge_segment_candidates(
    per_segment: Sequence[tuple[str, Sequence[Mapping[str, Any]]]],
    *,
    limit: int = PROMPT_CONCEPT_LIMIT,
) -> list[dict[str, Any]]:
    """Union per-segment candidate lists, capped at ``limit``, best rank first.

    A gold span contained by more than one selected segment gets the union of
    those segments' candidates rather than an arbitrary pick. Provenance stays
    attached: every merged candidate records which segments proposed it and at
    what rank.
    """
    merged: dict[str, dict[str, Any]] = {}
    for segment_id, candidates in per_segment:
        for rank, candidate in enumerate(candidates, start=1):
            concept_id = str(candidate.get("concept_id") or "")
            entry = merged.get(concept_id)
            if entry is None:
                entry = {**candidate, "from_segments": []}
                merged[concept_id] = entry
            entry["from_segments"].append({"segment_id": segment_id, "segment_rank": rank})
    ordered = sorted(
        merged.values(),
        key=lambda entry: (
            min(int(source["segment_rank"]) for source in entry["from_segments"]),
            str(entry["concept_id"]),
        ),
    )
    return [{**entry, "rank": rank} for rank, entry in enumerate(ordered[:limit], start=1)]


def _artifact_identity(answer: Mapping[str, Any], unit: ExtractionUnit | None) -> dict[str, Any]:
    context: Mapping[str, Any] = {}
    if unit is not None:
        context = unit.input.get("non_evidentiary_context", {}).get("artifact_context", {})
    return {
        "profile_id": str(answer.get("profile_id") or ""),
        "subject_type": str(answer.get("subject_type") or ""),
        "subject_id": str(answer.get("subject_id") or ""),
        "artifact_digest": str(answer.get("artifact_digest") or ""),
        "title": _text_or_none(context.get("artifact_title")),
    }


def _segment_context(unit: ExtractionUnit, *, selector_matches_payload: bool | None) -> dict[str, Any]:
    unit_input = unit.input
    segment = unit_input.get("processing_segment", {})
    context = unit_input.get("non_evidentiary_context", {})
    return {
        "segment_id": str(segment.get("segment_id") or unit.unit_id),
        "segment_ordinal": segment.get("ordinal"),
        "segment_count": segment.get("segment_count"),
        "segment_policy": segment.get("policy"),
        "headings": [str(heading) for heading in context.get("headings", ())],
        "selector_matches_payload": selector_matches_payload,
    }


def build_document(
    *,
    answers: Mapping[str, Any],
    units: Sequence[ExtractionUnit],
    registry_rows: Sequence[Mapping[str, Any]],
    file_metadata: Mapping[str, Any],
    generated_at: str,
    limit: int = PROMPT_CONCEPT_LIMIT,
    selector: str = SELECTOR_V1,
) -> dict[str, Any]:
    """Assemble the blind adjudication document from already-loaded inputs.

    Kept free of file access so a hermetic test can drive it with a tiny
    synthetic fixture.
    """
    if selector not in SELECTOR_CHOICES:
        raise GoldAdjudicationError(f"unknown candidate selector: {selector!r}")
    units_by_id = {unit.unit_id: unit for unit in units}
    candidate_cache: dict[str, tuple[list[dict[str, Any]], bool | None]] = {}
    items: list[dict[str, Any]] = []
    unbuilt: list[dict[str, Any]] = []

    for answer in answers.get("artifacts", ()):
        for expected in answer.get("expected_tags", ()):
            gold_id = str(expected.get("gold_id") or "")
            segment_ids = [str(value) for value in expected.get("containing_segment_ids", ())]
            per_segment: list[tuple[str, Sequence[Mapping[str, Any]]]] = []
            segment_contexts: list[dict[str, Any]] = []
            missing: list[str] = []
            for segment_id in segment_ids:
                unit = units_by_id.get(segment_id)
                if unit is None:
                    missing.append(segment_id)
                    continue
                if segment_id not in candidate_cache:
                    candidate_cache[segment_id] = segment_candidates(
                        unit, registry_rows, limit=limit, selector=selector
                    )
                candidates, parity = candidate_cache[segment_id]
                per_segment.append((segment_id, candidates))
                segment_contexts.append(_segment_context(unit, selector_matches_payload=parity))

            first_unit = units_by_id.get(segment_ids[0]) if segment_ids else None
            merged = _merge_segment_candidates(per_segment, limit=limit)
            if missing or not merged:
                unbuilt.append(
                    {
                        "item_id": f"gold-adjudication-{gold_id}",
                        "gold_id": gold_id,
                        "reason": (
                            f"no selected segment unit for {sorted(missing)}"
                            if missing
                            else "the production selector returned no candidates"
                        ),
                    }
                )
            items.append(
                {
                    "item_id": f"gold-adjudication-{gold_id}",
                    "gold_id": gold_id,
                    "artifact": _artifact_identity(answer, first_unit),
                    "gold_concept": {
                        "scheme": str(expected.get("scheme") or ""),
                        "label": str(expected.get("label") or ""),
                        # Null means the gold label has no registered target in
                        # the gold-free registry — the abstention branch, which
                        # the judge records rather than infers.
                        "registered_concept_id": _text_or_none(expected.get("concept_id")),
                    },
                    "gold_evidence": [
                        {
                            "source_field": str(expected.get("source_field") or ""),
                            "start_char": expected.get("start_char"),
                            "end_char": expected.get("end_char"),
                            "exact_text": str(expected.get("exact_text") or ""),
                            "coordinate_resolution": str(expected.get("coordinate_resolution") or ""),
                        }
                    ],
                    "segment_context": segment_contexts,
                    "candidates": merged,
                    "candidate_count": len(merged),
                }
            )

    items.sort(key=lambda item: str(item["item_id"]))
    unbuilt.sort(key=lambda item: str(item["item_id"]))
    selected_fn = SELECT_CANDIDATES_V2 if selector == SELECTOR_V2 else SELECT_CANDIDATES
    return {
        "schema_version": SCHEMA_VERSION,
        "blind": BLINDNESS_STATEMENT,
        "generated_at": generated_at,
        "candidate_selector": {
            "selector": selector,
            "function": f"{selected_fn.__module__}.{selected_fn.__qualname__}",
            "limit": limit,
            "method": f"{selector}-limit-{limit}",
            "scope": "processing segment; multi-segment gold spans take the per-segment union, capped at the limit",
            "payload_parity_applicable": selector == SELECTOR_V1,
            "payload_parity_checked": sum(1 for _, parity in candidate_cache.values() if parity is not None),
            "payload_parity_mismatches": sorted(
                segment_id for segment_id, (_, parity) in candidate_cache.items() if parity is False
            ),
        },
        "inputs": dict(file_metadata),
        "item_count": len(items),
        "unbuilt_item_count": len(unbuilt),
        "unbuilt_items": unbuilt,
        "items": items,
    }


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def build_from_stored_inputs(
    *,
    dataset_dir: Path,
    selection_file: Path,
    registry_file: Path,
    generated_at: str | None = None,
    selector: str = SELECTOR_V1,
) -> dict[str, Any]:
    """Load the frozen benchmark through the production reader and build."""
    inputs = load_testbed_inputs(
        dataset_dir,
        selection_file,
        registry_file,
        evaluation_manifest=DEFAULT_BOUNDARY_MANIFEST,
        evaluation_dataset_id=DEVELOPMENT_DATASET_ID,
        candidate_selector=selector,
    )
    registry_rows = read_parquet_rows(registry_file)
    gold_file = dataset_dir / GOLD_FILE
    file_metadata = {
        "dataset_dir": _relative(dataset_dir),
        "gold_file": _relative(gold_file),
        "gold_sha256": sha256_file(gold_file),
        "gold_row_count": inputs.segmentation_facts["gold_span_count"],
        "gold_artifact_count": inputs.gold_artifact_count,
        "selection_file": _relative(selection_file),
        "selection_sha256": sha256_file(selection_file),
        "selected_segment_count": inputs.selected_segment_count,
        "selected_artifact_count": inputs.source_facts["selected_artifact_count"],
        "registry_file": _relative(registry_file),
        "registry_sha256": sha256_file(registry_file),
        "registry_row_count": len(registry_rows),
        "source_dataset_file_sha256": dict(inputs.source_facts["dataset_files"]),
        "segmentation": {
            "policy_version": inputs.segmentation_facts["policy_version"],
            "settings_sha256": inputs.segmentation_facts["settings_sha256"],
            "tokenizer": inputs.segmentation_facts["tokenizer"],
            "tokenizer_version": inputs.segmentation_facts["tokenizer_version"],
            "max_tokens": inputs.segmentation_facts["max_tokens"],
        },
        "gold_artifacts_by_profile": dict(inputs.profile_facts["gold_artifacts_by_profile"]),
        # Recorded, not enforced: this file drives no provider call. A max above
        # the production budget is a tagger-side fact for the tuning loop, not a
        # reason to withhold the judges' input.
        "tag_prompt_tokens": {
            "observed_max": inputs.segmentation_facts["prompt_input_token_max"],
            "production_budget": PROMPT_INPUT_TOKEN_BUDGET,
            "within_production_budget": (
                inputs.segmentation_facts["prompt_input_token_max"] <= PROMPT_INPUT_TOKEN_BUDGET
            ),
        },
    }
    return build_document(
        answers=inputs.answers,
        units=inputs.units,
        registry_rows=registry_rows,
        file_metadata=file_metadata,
        generated_at=generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        selector=selector,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Emit the blind gold-adjudication input: every stored gold assignment "
            "beside the production selector's top candidates from the gold-free registry."
        )
    )
    parser.add_argument("--output", type=Path, required=True, help="Path for the JSON document.")
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=DEFAULT_RUN_DIR,
        help="Stored v4 benchmark run directory holding the frozen segment selection and registry.",
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=DEFAULT_DATASET_DIR,
        help=f"Evaluation dataset directory holding the source tables and {GOLD_FILE}.",
    )
    parser.add_argument(
        "--selection-file",
        type=Path,
        default=None,
        help=f"Frozen segment selection (default: <run-dir>/{SELECTION_FILE_NAME}).",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help=f"Gold-free concept registry (default: <run-dir>/{REGISTRY_FILE_NAME}).",
    )
    parser.add_argument(
        "--selector",
        choices=SELECTOR_CHOICES,
        default=SELECTOR_V1,
        help=(
            "Candidate selector. The default is the production selector, whose ordering the stored "
            "tag payloads record; anchored-hybrid-v2 is the candidate-generation experiment."
        ),
    )
    args = parser.parse_args(argv)

    selection_file = args.selection_file or (args.run_dir / SELECTION_FILE_NAME)
    registry_file = args.registry or (args.run_dir / REGISTRY_FILE_NAME)
    document = build_from_stored_inputs(
        dataset_dir=args.dataset_dir,
        selection_file=selection_file,
        registry_file=registry_file,
        selector=args.selector,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "selector": document["candidate_selector"]["method"],
                "item_count": document["item_count"],
                "unbuilt_item_count": document["unbuilt_item_count"],
                "registry_sha256": document["inputs"]["registry_sha256"],
                "registry_row_count": document["inputs"]["registry_row_count"],
                "gold_sha256": document["inputs"]["gold_sha256"],
                "payload_parity_mismatches": document["candidate_selector"]["payload_parity_mismatches"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if document["unbuilt_items"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

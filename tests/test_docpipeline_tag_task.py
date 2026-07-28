"""Focused contracts for the small v3 concept-tag extraction path."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters import StructuredTextResult
from spicy_regs.docpipeline.extraction import (
    ExtractionTask,
    extraction_plan_facts,
    plan_extraction_items,
    recompute_extraction,
    run_extraction,
)
from spicy_regs.docpipeline.runtime import RunPlan, validate_run
from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifact
from spicy_regs.docpipeline.source import (
    SourceRecord,
    build_source_artifact,
    profile_for_table,
)
from spicy_regs.docpipeline.tag_task import TagExtractionTask, tag_unit
from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.llm import (
    ASSIGNMENT_ROLES,
    EVIDENCE_ALIGNMENT_PROVIDED,
    EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    TAG_INSTRUCTIONS,
    TAG_SCHEMA,
)

TASK = TagExtractionTask()


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()
SETTINGS = SegmentSettings(
    max_tokens=10_000,
    min_tokens=1,
    overlap_tokens=0,
    tokenizer=COUNTER.name,
    tokenizer_version=COUNTER.version,
)


def _concept(
    concept_id: str = "concept:water",
    *,
    scheme: str = "subject",
    facet: str | None = None,
    source_vocabulary: str | None = None,
    label: str = "water policy",
    alt_labels: tuple[str, ...] = (),
) -> dict[str, Any]:
    row = {
        "concept_id": concept_id,
        "scheme": scheme,
        "pref_label": label,
        "alt_labels_json": canonical_json(list(alt_labels)),
        "definition": f"Rules concerning {label}.",
        "status": "active",
    }
    if facet is not None:
        row["facet"] = facet
    if source_vocabulary is not None:
        row["source_vocabulary"] = source_vocabulary
    return row


def _unit(text: str, *, heading: str = "Water policy") -> tuple[Any, Any, Any]:
    outcome = build_source_artifact(
        SourceRecord(
            profile=profile_for_table("cfr_sections"),
            row={
                "granule_id": "CFR-test-tag-task",
                "heading": heading,
                "text": text,
            },
        )
    )
    assert outcome.artifact is not None
    segmented = segment_artifact(outcome.artifact, settings=SETTINGS, counter=COUNTER)
    assert len(segmented.segments) == 1
    segment = segmented.segments[0]
    return outcome.artifact, segment, tag_unit(outcome.artifact, segment, [_concept()])


def _field(payload: Mapping[str, Any], source_field: str = "cfr_sections.text") -> tuple[str, str]:
    fields = payload["untrusted_evidence_fields"]["fields"]
    matches = [
        (key, value)
        for key, value in fields.items()
        if payload["processing_segment"]["source_spans"][key]["source_field"] == source_field
    ]
    assert len(matches) == 1
    return matches[0]


def _tag(
    field_key: str,
    evidence: str,
    start: int,
    *,
    concept_id: str | None = "concept:water",
    scheme: str = "subject",
    role: str = "substantive",
    proposed_label: str | None = None,
    definition: str | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "concept_id": concept_id,
        "proposed_label": proposed_label,
        "scheme": scheme,
        "role": role,
        "definition": definition,
        "confidence": 0.8,
        "evidence_text": evidence,
        "evidence_field": field_key,
        "evidence_start": start,
        "evidence_end": start + len(evidence),
        "justification": "The exact source text states the topic.",
        "external_ids": [],
    }
    item.update(overrides)
    return item


def _nested_keys(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return {
            *(str(key) for key in value),
            *(nested for child in value.values() for nested in _nested_keys(child)),
        }
    if isinstance(value, list):
        return {nested for child in value for nested in _nested_keys(child)}
    return set()


def test_tag_task_uses_the_canonical_prompt_schema_and_a_gold_free_payload() -> None:
    _, _, unit = _unit("The word gold is ordinary source text about water policy.")
    contaminated = {**unit.input, "gold_id": "must-not-enter", "answers": {"expected_tags": ["water"]}}

    payload = TASK.build_payload(contaminated)

    assert isinstance(TASK, ExtractionTask)
    assert TASK.instructions == TAG_INSTRUCTIONS
    assert TASK.build_schema(payload) == TAG_SCHEMA
    assert not (_nested_keys(payload) & TASK.forbidden_payload_keys)
    assert "gold" in canonical_json(payload), "ordinary quoted source text is not mistaken for an answer key"
    assert TASK.review_gate([], {}, protocol_sha256="")["eligible"] is False


def test_tag_payload_keeps_facet_separate_from_source_vocabulary() -> None:
    artifact, segment, _ = _unit("Water policy governs discharge permits.")
    unit = tag_unit(
        artifact,
        segment,
        [
            _concept(
                scheme="subject",
                facet="subject",
                source_vocabulary="fast-topical",
            )
        ],
    )
    offered = unit.input["available_concepts"][0]

    assert offered["facet"] == "subject"
    assert offered["source_vocabulary"] == "fast-topical"
    assert "scheme" not in offered

    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {"tags": [_tag(field_key, "Water policy", text.index("Water policy"))]}
    candidate = TASK.build_candidates(response, payload)["candidates"][0]
    assert candidate["facet"] == "subject"
    assert candidate["source_vocabulary"] == "fast-topical"
    assert candidate["scheme"] == candidate["facet"]


def test_v1_external_scheme_is_a_read_only_migration_shim() -> None:
    artifact, segment, _ = _unit("Water policy governs discharge permits.")
    unit = tag_unit(
        artifact,
        segment,
        [_concept(scheme="fast-topical")],
    )

    offered = unit.input["available_concepts"][0]
    assert offered["facet"] == "subject"
    assert offered["source_vocabulary"] == "fast-topical"


def test_conflicting_facet_and_compatibility_scheme_are_rejected() -> None:
    artifact, segment, _ = _unit("Water policy governs discharge permits.")

    with pytest.raises(ValueError, match="disagrees on facet"):
        tag_unit(
            artifact,
            segment,
            [_concept(scheme="regulated_entity", facet="subject")],
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda response: response["tags"][0].pop("external_ids"),
        lambda response: response["tags"][0].update({"extra": True}),
        lambda response: response["tags"][0].update({"scheme": "unknown"}),
        lambda response: response["tags"][0].update({"confidence": 2.0}),
        lambda response: response["tags"][0].update({"evidence_start": -1}),
        lambda response: response["tags"][0].pop("role"),
        lambda response: response["tags"][0].update({"role": "central"}),
        lambda response: response["tags"][0].update({"role": None}),
        lambda response: response["tags"][0].update({"role": "Primary"}),
    ],
)
def test_tag_response_schema_rejects_structural_violations(mutate: Any) -> None:
    _, _, unit = _unit("Water discharges require a permit.")
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {"tags": [_tag(field_key, "Water", text.index("Water"))]}
    mutate(response)

    with pytest.raises(Exception):
        TASK.check_response(response, TASK.build_schema(payload))


@pytest.mark.parametrize("role", ASSIGNMENT_ROLES)
def test_every_assignment_role_is_accepted_and_carried_onto_the_candidate_row(role: str) -> None:
    _, _, unit = _unit("Water discharges require a permit.")
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {"tags": [_tag(field_key, "Water", text.index("Water"), role=role)]}

    TASK.check_response(response, TASK.build_schema(payload))
    normalized = TASK.build_candidates(response, payload)

    assert normalized["rejections"] == []
    assert [row["role"] for row in normalized["candidates"]] == [role]
    assert ("role", "string") in TASK.candidate_columns()


def test_offsets_are_kept_or_repaired_only_for_one_exact_match() -> None:
    _, _, unit = _unit("PFAS first. Clean water applies. PFAS second.")
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {
        "tags": [
            _tag(field_key, "PFAS", text.rindex("PFAS")),
            _tag(field_key, "Clean water", 0),
            _tag(field_key, "PFAS", 1),
        ]
    }
    TASK.check_response(response, TASK.build_schema(payload))

    normalized = TASK.build_candidates(response, payload)

    assert len(normalized["candidates"]) == 2
    alignments = {row["evidence_text"]: row["evidence_alignment_method"] for row in normalized["candidates"]}
    assert alignments == {
        "PFAS": EVIDENCE_ALIGNMENT_PROVIDED,
        "Clean water": EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    }
    rejected = normalized["rejections"]
    assert [(row["reason"], json.loads(row["item_json"])["evidence_text"]) for row in rejected] == [
        ("ungrounded_evidence", "PFAS")
    ]


def test_schema_valid_semantic_failures_become_rejection_rows() -> None:
    _, _, unit = _unit("Water discharges require a permit.")
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    water = text.index("Water")
    response = {
        "tags": [
            _tag(field_key, "Water", water),
            _tag(field_key, "Water", water, concept_id="concept:unknown"),
            _tag(field_key, "Water", water, concept_id=None, scheme="regulated_entity"),
            _tag(field_key, "Water", water, concept_id=None, proposed_label="water", definition=None),
            _tag("missing-field", "Water", water),
            _tag(field_key, "absent text", 0),
        ]
    }
    TASK.check_response(response, TASK.build_schema(payload))

    normalized = TASK.build_candidates(response, payload)

    assert len(normalized["candidates"]) == 1
    assert [row["reason"] for row in normalized["rejections"]] == [
        "unknown_concept",
        "disallowed_scheme",
        "incomplete_novel_concept",
        "unknown_evidence_field",
        "ungrounded_evidence",
    ]
    assert TASK.is_empty(normalized) is False


def test_null_concept_id_resolves_one_offered_alias_before_scoring() -> None:
    artifact, segment, _ = _unit("Clean water rules apply.")
    unit = tag_unit(
        artifact,
        segment,
        [_concept(alt_labels=("clean water",))],
    )
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {
        "tags": [
            _tag(
                field_key,
                "Clean water",
                text.index("Clean water"),
                concept_id=None,
                proposed_label="clean water",
                definition="A model-supplied duplicate definition.",
            )
        ]
    }

    normalized = TASK.build_candidates(response, payload)

    assert normalized["rejections"] == []
    assert len(normalized["candidates"]) == 1
    candidate = normalized["candidates"][0]
    assert candidate["concept_id"] == "concept:water"
    assert candidate["concept_label"] == "water policy"
    assert candidate["concept_status"] == "existing"


def test_null_concept_id_rejects_an_ambiguous_offered_alias() -> None:
    artifact, segment, _ = _unit("Clean water rules apply.")
    unit = tag_unit(
        artifact,
        segment,
        [
            _concept(alt_labels=("clean water",)),
            _concept(
                "concept:aquatic-policy",
                label="aquatic policy",
                alt_labels=("clean water",),
            ),
        ],
    )
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {
        "tags": [
            _tag(
                field_key,
                "Clean water",
                text.index("Clean water"),
                concept_id=None,
                proposed_label="clean water",
                definition="A proposed definition.",
            )
        ]
    }

    normalized = TASK.build_candidates(response, payload)

    assert normalized["candidates"] == []
    assert [row["reason"] for row in normalized["rejections"]] == ["ambiguous_concept_alias"]


def test_existing_concept_rejects_a_response_scheme_mismatch() -> None:
    _, _, unit = _unit("Water discharges require a permit.")
    payload = TASK.build_payload(unit.input)
    payload["subject"]["allowed_schemes"].append("regulated_entity")
    field_key, text = _field(payload)
    response = {
        "tags": [
            _tag(
                field_key,
                "Water",
                text.index("Water"),
                scheme="regulated_entity",
            )
        ]
    }

    normalized = TASK.build_candidates(response, payload)

    assert normalized["candidates"] == []
    assert [row["reason"] for row in normalized["rejections"]] == ["concept_scheme_mismatch"]


def test_tag_rows_translate_local_offsets_and_retain_source_slice_grades() -> None:
    artifact, segment, unit = _unit("First paragraph.\n\nSecond paragraph names water policy.")
    payload = TASK.build_payload(unit.input)
    fields = payload["untrusted_evidence_fields"]["fields"]
    field_key = next(key for key, value in fields.items() if "Second paragraph" in value)
    field_text = fields[field_key]
    local_start = field_text.index("water policy")
    response = {"tags": [_tag(field_key, "water policy", local_start)]}

    candidate = TASK.build_candidates(response, payload)["candidates"][0]
    binding = payload["processing_segment"]["source_spans"][field_key]

    assert candidate["source_start_char"] == binding["start_char"] + local_start
    assert (
        artifact.raw_fields[candidate["source_field"]][candidate["source_start_char"] : candidate["source_end_char"]]
        == "water policy"
    )
    assert candidate["source_start_char"] != local_start
    assert candidate["evidence_grade"] == binding["evidence_grade"]
    assert candidate["content_layer"] == binding["content_layer"]
    assert candidate["context_only"] == binding["context_only"]
    assert any(source_slice.context_only for source_slice in segment.slices)


def test_durable_heading_slice_is_citable_for_a_topical_tag() -> None:
    artifact, segment, unit = _unit(
        "Employers must publish a written program.",
        heading="Hazard communication",
    )
    payload = TASK.build_payload(unit.input)
    fields = payload["untrusted_evidence_fields"]["fields"]
    field_key = next(
        key for key, binding in payload["processing_segment"]["source_spans"].items() if binding["context_only"] is True
    )
    evidence = "Hazard communication"
    response = {
        "tags": [
            _tag(
                field_key,
                evidence,
                fields[field_key].index(evidence),
            )
        ]
    }

    candidate = TASK.build_candidates(response, payload)["candidates"][0]

    assert candidate["context_only"] is True
    assert (
        artifact.raw_fields[candidate["source_field"]][candidate["source_start_char"] : candidate["source_end_char"]]
        == evidence
    )
    assert any(source_slice.region_kind == "heading" for source_slice in segment.slices)


def _candidate(
    artifact_digest: str,
    segment_id: str,
    concept_id: str | None,
    label: str,
    *,
    profile_id: str,
    subject_id: str,
    status: str = "existing",
    role: str = "substantive",
) -> dict[str, Any]:
    return {
        "candidate_id": f"{artifact_digest}:{segment_id}:{label}",
        "profile_id": profile_id,
        "subject_type": "document",
        "subject_id": subject_id,
        "artifact_digest": artifact_digest,
        "segment_id": segment_id,
        "concept_id": concept_id,
        "concept_label": label,
        "concept_status": status,
        "scheme": "subject",
        "role": role,
        "confidence": 0.8,
        "source_field": "documents.text",
        "source_start_char": 0,
        "source_end_char": len(label),
        "evidence_text": label,
        "definition": "A test concept.",
        "grounded": True,
    }


def test_scoring_reports_exact_overall_profile_and_error_metrics() -> None:
    answers = {
        "artifacts": [
            {
                "profile_id": "p1",
                "subject_type": "document",
                "subject_id": "a",
                "artifact_digest": "a1",
                "expected_tags": [{"gold_id": "g1", "scheme": "subject", "label": "one", "concept_id": "c1"}],
            },
            {
                "profile_id": "p1",
                "subject_type": "document",
                "subject_id": "b",
                "artifact_digest": "b1",
                "expected_tags": [{"gold_id": "g2", "scheme": "subject", "label": "two", "concept_id": "c2"}],
            },
            {
                "profile_id": "p2",
                "subject_type": "document",
                "subject_id": "c",
                "artifact_digest": "c1",
                "expected_tags": [{"gold_id": "g3", "scheme": "subject", "label": "three", "concept_id": None}],
            },
        ],
        "segments": [
            {"segment_id": "s1", "adversarial_case_ids": []},
            {"segment_id": "s2", "adversarial_case_ids": []},
            {"segment_id": "s3", "adversarial_case_ids": []},
        ],
    }
    candidates = {
        "segments": [],
        "candidates": [
            _candidate("a1", "s1", "c1", "one", profile_id="p1", subject_id="a"),
            _candidate("a1", "s1", "cx", "extra", profile_id="p1", subject_id="a"),
            _candidate("c1", "s3", None, "three", profile_id="p2", subject_id="c", status="novel"),
        ],
        "rejections": [],
    }

    metrics = TASK.score(answers, candidates)

    assert (
        metrics["true_positive_count"],
        metrics["false_positive_count"],
        metrics["false_negative_count"],
    ) == (2, 1, 1)
    assert metrics["micro_precision"] == pytest.approx(2 / 3)
    assert metrics["micro_recall"] == pytest.approx(2 / 3)
    assert metrics["micro_f1"] == pytest.approx(2 / 3)
    by_profile = {row["profile_id"]: row for row in metrics["per_profile"]}
    assert by_profile["p1"]["micro_f1"] == pytest.approx(0.5)
    assert by_profile["p2"]["micro_f1"] == 1.0
    assert metrics["empty_tag_rate"] == pytest.approx(1 / 3)
    assert metrics["evidence_grounding_rate"] == 1.0
    assert metrics["novel_tag_rate"] == pytest.approx(1 / 3)
    assert len(metrics["false_positives"]) == 1
    assert len(metrics["false_negatives"]) == 1
    assert len(metrics["novel_tags"]) == 1


def _artifact_answer(
    digest: str,
    subject_id: str,
    concept_id: str,
    label: str,
    *,
    profile_id: str = "p1",
    **extra: Any,
) -> dict[str, Any]:
    expected: dict[str, Any] = {
        "gold_id": f"gold-{concept_id}",
        "scheme": "subject",
        "label": label,
        "concept_id": concept_id,
    }
    role = extra.pop("expected_role", None)
    if role is not None:
        expected["role"] = role
    return {
        "profile_id": profile_id,
        "subject_type": "document",
        "subject_id": subject_id,
        "artifact_digest": digest,
        "expected_tags": [expected],
        **extra,
    }


def test_scoring_partitions_predictions_by_assignment_role() -> None:
    answers = {
        "artifacts": [
            _artifact_answer("a1", "a", "c1", "one"),
            _artifact_answer("b1", "b", "c2", "two"),
            _artifact_answer("c1", "c", "c3", "three"),
            _artifact_answer("d1", "d", "c4", "four", expected_role="primary"),
        ],
        "segments": [],
    }
    candidates = {
        "segments": [],
        "candidates": [
            _candidate("a1", "s1", "c1", "one", profile_id="p1", subject_id="a", role="primary"),
            _candidate("b1", "s2", "cx", "extra", profile_id="p1", subject_id="b", role="mention"),
            _candidate("c1", "s3", "c3", "three", profile_id="p1", subject_id="c", role="contextual"),
            _candidate("d1", "s4", "c4", "four", profile_id="p1", subject_id="d", role="primary"),
        ],
        "rejections": [],
    }

    metrics = TASK.score(answers, candidates)
    by_role = {row["role"]: row for row in metrics["per_role"]}

    assert [row["role"] for row in metrics["per_role"]] == list(ASSIGNMENT_ROLES)
    assert {row["scope"] for row in metrics["per_role"]} == {"role"}
    # Roles partition the scored prediction set exactly once.
    assert sum(row["predicted_positive_count"] for row in metrics["per_role"]) == (metrics["predicted_positive_count"])
    assert (by_role["primary"]["true_positive_count"], by_role["primary"]["false_positive_count"]) == (2, 0)
    assert by_role["primary"]["micro_precision"] == 1.0
    assert by_role["mention"]["true_positive_count"] == 0
    assert by_role["mention"]["micro_precision"] == 0.0
    assert by_role["contextual"]["true_positive_count"] == 1
    assert by_role["substantive"]["predicted_positive_count"] == 0
    # Gold that declares a role scores only in that role; role-free gold is a
    # target for every role.
    assert by_role["primary"]["gold_positive_count"] == 4
    assert by_role["mention"]["gold_positive_count"] == 3
    assert metrics["role"] is None and metrics["scope"] == "all-gold-artifacts"
    assert [row["role"] for row in metrics["false_positives"]] == ["mention"]


def test_scoring_reports_every_gold_split_separately() -> None:
    answers = {
        "artifacts": [
            _artifact_answer("a1", "a", "c1", "one", split="train"),
            _artifact_answer("b1", "b", "c2", "two", split="holdout"),
        ],
        "segments": [],
    }
    candidates = {
        "segments": [],
        "candidates": [
            _candidate("a1", "s1", "c1", "one", profile_id="p1", subject_id="a"),
            _candidate("b1", "s2", "cx", "extra", profile_id="p1", subject_id="b"),
        ],
        "rejections": [],
    }

    metrics = TASK.score(answers, candidates)
    by_split = {row["split"]: row for row in metrics["per_split"]}

    assert sorted(by_split) == ["holdout", "train"]
    assert {row["scope"] for row in metrics["per_split"]} == {"split"}
    assert by_split["train"]["artifact_count"] == 1
    assert by_split["train"]["micro_f1"] == 1.0
    assert by_split["holdout"]["artifact_count"] == 1
    assert by_split["holdout"]["micro_f1"] == 0.0
    assert sum(row["artifact_count"] for row in metrics["per_split"]) == metrics["artifact_count"]
    assert [row["split"] for row in metrics["false_negatives"]] == ["holdout"]


def test_scoring_treats_answers_without_a_split_as_one_train_partition() -> None:
    answers = {
        "artifacts": [_artifact_answer("a1", "a", "c1", "one")],
        "segments": [],
    }
    candidates = {
        "segments": [],
        "candidates": [_candidate("a1", "s1", "c1", "one", profile_id="p1", subject_id="a")],
        "rejections": [],
    }

    metrics = TASK.score(answers, candidates)

    assert len(metrics["per_split"]) == 1
    train = metrics["per_split"][0]
    assert train["split"] == "train"
    assert metrics["split"] is None
    comparable = [key for key in train if key not in {"scope", "split"}]
    assert {key: train[key] for key in comparable} == {key: metrics[key] for key in comparable}


def test_scoring_reports_the_non_gold_control_artifacts_directly() -> None:
    answers = {
        "artifacts": [],
        "segments": [
            {
                "profile_id": "control-profile",
                "subject_type": "document",
                "subject_id": "prompt-injection-control",
                "artifact_digest": "control-digest",
                "segment_id": "control-segment",
                "segment_ordinal": 0,
                "adversarial_case_ids": ["adversarial-prompt-injection"],
            }
        ],
    }
    candidates = {
        "segments": [],
        "candidates": [
            _candidate(
                "control-digest",
                "control-segment",
                "concept:water",
                "water policy",
                profile_id="control-profile",
                subject_id="prompt-injection-control",
            )
        ],
        "rejections": [
            {
                "segment_id": "control-segment",
                "reason": "ungrounded_evidence",
            }
        ],
    }

    metrics = TASK.score(answers, candidates)

    assert metrics["control_artifact_count"] == 1
    assert metrics["controls"] == [
        {
            "profile_id": "control-profile",
            "subject_type": "document",
            "subject_id": "prompt-injection-control",
            "artifact_digest": "control-digest",
            "segment_count": 1,
            "candidate_count": 1,
            "rejection_count": 1,
            "adversarial_case_ids": ["adversarial-prompt-injection"],
            "candidates": [
                {
                    "concept_id": "concept:water",
                    "concept_label": "water policy",
                    "scheme": "subject",
                    "source_field": "documents.text",
                    "start_char": 0,
                    "end_char": len("water policy"),
                    "exact_text": "water policy",
                }
            ],
            "rejection_reasons": {"ungrounded_evidence": 1},
        }
    ]


class _ReplayTagModel:
    model_id = "fake:tag-replay"
    run_configuration = {"provider": "fake", "model_id": model_id, "store": False}

    def __init__(self, response: Mapping[str, Any]) -> None:
        self.response = dict(response)
        self.payloads: list[dict[str, Any]] = []

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        return {
            "model": self.model_id,
            "schema_name": name,
            "schema": dict(schema),
            "instructions": instructions,
            "input": canonical_json(payload),
            "max_output_tokens": max_output_tokens,
        }

    def structured_json(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> StructuredTextResult:
        del schema, instructions
        self.payloads.append(copy.deepcopy(dict(payload)))
        prompt = canonical_json(payload)
        return StructuredTextResult(
            output=copy.deepcopy(self.response),
            call={
                "provider": "fake",
                "transport": "replay",
                "model_id": self.model_id,
                "schema_name": name,
                "response_id": "response-1",
                "response_model": self.model_id,
                "status": "completed",
                "duration_ms": 1.0,
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
                "attempt_count": 1,
                "retry_count": 0,
                "attempts": [{"attempt": 1, "status": "completed"}],
                "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                "request_sha256": hashlib.sha256((prompt + name).encode()).hexdigest(),
                "max_output_tokens": max_output_tokens,
                "schema_validated_locally": True,
            },
        )


def test_diagnostic_run_stores_gold_free_calls_candidates_rejections_and_metrics(tmp_path: Path) -> None:
    artifact, segment, unit = _unit("The gold-colored source says water policy applies.")
    payload = TASK.build_payload(unit.input)
    field_key, text = _field(payload)
    response = {"tags": [_tag(field_key, "water policy", text.index("water policy"))]}
    model = _ReplayTagModel(response)
    answers = {
        "artifacts": [
            {
                "profile_id": artifact.profile_id,
                "subject_type": artifact.subject_type,
                "subject_id": artifact.subject_id,
                "artifact_digest": artifact.content_sha256,
                "expected_tags": [
                    {
                        "gold_id": "hidden-gold-1",
                        "scheme": "subject",
                        "label": "water policy",
                        "concept_id": "concept:water",
                    }
                ],
            }
        ],
        "segments": [{"segment_id": segment.segment_id, "adversarial_case_ids": []}],
    }
    items = plan_extraction_items(TASK, model, (unit,))
    plan = RunPlan(
        run_id="tag-diagnostic-test",
        mode="diagnostic",
        steps=("extract",),
        extraction=extraction_plan_facts(TASK, (unit,), answers=answers),
        provider=model.run_configuration,
        required_work=tuple(item.work_id for item in items),
    )

    result = run_extraction(
        plan,
        tmp_path / "run",
        task=TASK,
        model=model,
        units=(unit,),
        answers=answers,
    )

    assert result.passed
    assert result.outcome.receipt["benchmark_eligible"] is False
    assert result.outcome.receipt["publication_eligible"] is False
    assert result.metrics is not None and result.metrics["micro_f1"] == 1.0
    assert not (_nested_keys(model.payloads[0]) & TASK.forbidden_payload_keys)
    call_directory = next((result.outcome.run_directory / "extraction" / "calls").iterdir())
    stored_payload = json.loads((call_directory / "payload.json").read_text(encoding="utf-8"))
    stored_request = json.loads((call_directory / "request.json").read_text(encoding="utf-8"))
    request_payload = json.loads(stored_request["input"])
    assert not (_nested_keys(stored_payload) & TASK.forbidden_payload_keys)
    assert not (_nested_keys(request_payload) & TASK.forbidden_payload_keys)
    assert pq.read_table(result.outcome.run_directory / TASK.candidate_table).num_rows == 1
    assert pq.read_table(result.outcome.run_directory / TASK.rejection_table).num_rows == 0
    assert pq.read_table(result.outcome.run_directory / "extraction/provider-calls.parquet").num_rows == 1

    validation = validate_run(
        result.outcome.run_directory,
        plan=plan,
        recompute=recompute_extraction(TASK, answers=answers),
    )
    assert validation["status"] == "pass"

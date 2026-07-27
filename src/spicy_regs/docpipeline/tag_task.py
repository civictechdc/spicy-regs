"""Evidence-grounded concept tagging for the v3 extraction step.

The task consumes one new-pipeline :class:`ProcessingSegment` per model call.
It keeps benchmark answers outside the payload, accepts only concepts the call
was offered, and translates each accepted evidence quote back to exact
artifact-field coordinates. Model output that cannot meet those rules becomes
an inspectable rejection row.

This module owns tag behavior only. It does not read historical experiments,
choose a provider, approve tags, or publish ontology changes.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from jsonschema import Draft202012Validator

from spicy_regs.docpipeline.extraction import (
    ExtractionError,
    ExtractionUnit,
    ResponseCheckError,
)
from spicy_regs.docpipeline.segments import ProcessingSegment
from spicy_regs.docpipeline.source import SourceArtifact
from spicy_regs.ontology.common import canonical_json, stable_id
from spicy_regs.ontology.concepts import concept_aliases, normalize_label
from spicy_regs.ontology.llm import (
    TAG_INSTRUCTIONS,
    TAG_MAX_OUTPUT_TOKENS,
    TAG_SCHEMA,
    ontology_concept_payload,
    resolve_exact_evidence_offsets,
    validated_external_ids,
)

TASK_NAME = "concept_tags_v1"
SCHEMA_NAME = "ontology_tags"
CANDIDATE_TABLE = "extraction/tag-candidates.parquet"
REJECTION_TABLE = "extraction/tag-rejections.parquet"

MODEL_INPUT_FORBIDDEN_KEYS = frozenset(
    {
        "answer",
        "answers",
        "expected",
        "expected_tags",
        "gold",
        "gold_id",
        "gold_ids",
        "gold_ids_json",
        "oracle",
        "selection_role",
        "adversarial_case_ids",
        "adversarial_case_ids_json",
    }
)

CANDIDATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("candidate_id", "string"),
    ("profile_id", "string"),
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("source_table", "string"),
    ("segment_id", "string"),
    ("segment_ordinal", "int64"),
    ("segment_count", "int64"),
    ("concept_id", "string"),
    ("concept_label", "string"),
    ("concept_status", "string"),
    ("scheme", "string"),
    ("definition", "string"),
    ("confidence", "double"),
    ("evidence_field_key", "string"),
    ("source_field", "string"),
    ("evidence_grade", "string"),
    ("content_layer", "string"),
    ("coordinate_grade", "string"),
    ("context_only", "bool"),
    ("source_start_char", "int64"),
    ("source_end_char", "int64"),
    ("segment_start_char", "int64"),
    ("segment_end_char", "int64"),
    ("evidence_text", "string"),
    ("evidence_alignment_method", "string"),
    ("justification", "string"),
    ("external_ids_json", "string"),
    ("grounded", "bool"),
)

REJECTION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("rejection_id", "string"),
    ("profile_id", "string"),
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("source_table", "string"),
    ("segment_id", "string"),
    ("segment_ordinal", "int64"),
    ("item_ordinal", "int64"),
    ("reason", "string"),
    ("concept_id", "string"),
    ("scheme", "string"),
    ("evidence_field_key", "string"),
    ("item_json", "string"),
)

_PAYLOAD_KEYS = (
    "subject",
    "processing_segment",
    "non_evidentiary_context",
    "untrusted_evidence_fields",
    "available_concepts",
)


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ExtractionError(f"{label} must be an object")
    return {str(key): item for key, item in value.items()}


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ExtractionError(f"{label} must be an array")
    return list(value)


def _required_text(record: Mapping[str, Any], key: str, label: str) -> str:
    value = str(record.get(key) or "").strip()
    if not value:
        raise ExtractionError(f"{label}.{key} is required")
    return value


def _plain(value: Any) -> Any:
    return json.loads(canonical_json(value))


def _validate_payload(payload: Mapping[str, Any]) -> None:
    if set(payload) != set(_PAYLOAD_KEYS):
        raise ExtractionError(f"tag payload keys must be exactly {list(_PAYLOAD_KEYS)}")
    subject = _object(payload.get("subject"), "subject")
    for key in ("type", "id", "profile", "source_table", "artifact_digest"):
        _required_text(subject, key, "subject")
    schemes = subject.get("allowed_schemes")
    if not isinstance(schemes, list) or not schemes or any(not str(value).strip() for value in schemes):
        raise ExtractionError("subject.allowed_schemes must be a nonempty string array")

    segment = _object(payload.get("processing_segment"), "processing_segment")
    for key in ("segment_id", "policy", "tokenizer"):
        _required_text(segment, key, "processing_segment")
    for key in ("ordinal", "segment_count", "token_count"):
        value = segment.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ExtractionError(f"processing_segment.{key} must be a nonnegative integer")
    if int(segment["segment_count"]) < 1 or int(segment["ordinal"]) >= int(segment["segment_count"]):
        raise ExtractionError("processing_segment ordinal is outside its segment count")

    source_spans = _object(segment.get("source_spans"), "processing_segment.source_spans")
    evidence = _object(payload.get("untrusted_evidence_fields"), "untrusted_evidence_fields")
    fields = _object(evidence.get("fields"), "untrusted_evidence_fields.fields")
    if set(source_spans) != set(fields):
        raise ExtractionError("evidence fields and source-coordinate bindings differ")
    for field_key, field_text in fields.items():
        if not isinstance(field_text, str):
            raise ExtractionError(f"evidence field {field_key!r} is not text")
        span = _object(source_spans[field_key], f"processing_segment.source_spans.{field_key}")
        _required_text(span, "source_field", f"processing_segment.source_spans.{field_key}")
        start = span.get("start_char")
        end = span.get("end_char")
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
            or start < 0
            or end <= start
            or end - start != len(field_text)
        ):
            raise ExtractionError(f"evidence field {field_key!r} has invalid source coordinates")

    concepts = _array(payload.get("available_concepts"), "available_concepts")
    seen: set[str] = set()
    for index, raw in enumerate(concepts):
        concept = _object(raw, f"available_concepts[{index}]")
        concept_id = _required_text(concept, "concept_id", f"available_concepts[{index}]")
        if concept_id in seen:
            raise ExtractionError(f"available concept {concept_id!r} appears twice")
        seen.add(concept_id)
        if str(concept.get("scheme") or "") not in schemes:
            raise ExtractionError(f"available concept {concept_id!r} has a disallowed scheme")


def tag_unit(
    artifact: SourceArtifact,
    segment: ProcessingSegment,
    concepts: Sequence[Mapping[str, Any]],
) -> ExtractionUnit:
    """Build one gold-free extraction unit from new source and segment objects."""
    identity = (
        artifact.subject_type,
        artifact.subject_id,
        artifact.profile_id,
        artifact.content_sha256,
    )
    segment_identity = (
        segment.subject_type,
        segment.subject_id,
        segment.profile_id,
        segment.artifact_sha256,
    )
    if identity != segment_identity:
        raise ExtractionError("the tag segment does not belong to the supplied source artifact")

    fields: dict[str, str] = {}
    source_spans: dict[str, dict[str, Any]] = {}
    # Topical tags may be stated by a durable heading. Some current markup
    # adapters also classify opening regions that contain semantic titles as
    # ``markup-prolog``; five locked gold cues live there. Excluding the whole
    # broad region would discard source meaning, so this diagnostic keeps every
    # exact slice citable and carries its grades into review. The feedback
    # report owns the narrower source-region correction.
    for index, source_slice in enumerate(segment.slices):
        # Keep the prompt key short. The coordinate binding beside it retains
        # the canonical field and offsets; repeating those values in a long key
        # made high-slice-count segments exceed the model input budget.
        field_key = f"evidence_{index}"
        fields[field_key] = source_slice.text
        source_spans[field_key] = {
            "source_field": source_slice.source_field,
            "start_char": source_slice.start_char,
            "end_char": source_slice.end_char,
            "evidence_grade": source_slice.evidence_grade,
            "content_layer": source_slice.content_layer,
            "coordinate_grade": source_slice.coordinate_grade,
            "context_only": source_slice.context_only,
        }

    payload = {
        "subject": {
            "type": artifact.subject_type,
            "id": artifact.subject_id,
            "profile": artifact.profile_id,
            "source_table": artifact.source_table,
            "allowed_schemes": list(artifact.allowed_schemes),
            "artifact_digest": artifact.content_sha256,
        },
        "processing_segment": {
            "segment_id": segment.segment_id,
            "ordinal": segment.ordinal,
            "segment_count": segment.segment_count,
            "policy": segment.settings.policy_version,
            "tokenizer": segment.settings.tokenizer,
            "tokenizer_version": segment.settings.tokenizer_version,
            "token_count": segment.token_count,
            "source_spans": source_spans,
        },
        "non_evidentiary_context": {
            "headings": list(segment.context.headings),
            "artifact_context": dict(segment.context.artifact_context),
        },
        "untrusted_evidence_fields": {
            "begin_delimiter": "BEGIN_UNTRUSTED_SOURCE",
            "fields": fields,
            "end_delimiter": "END_UNTRUSTED_SOURCE",
        },
        "available_concepts": [ontology_concept_payload(dict(concept)) for concept in concepts],
    }
    _validate_payload(payload)
    return ExtractionUnit(unit_id=segment.segment_id, input=_plain(payload))


def _rejection(
    *,
    subject: Mapping[str, Any],
    segment: Mapping[str, Any],
    item: Mapping[str, Any],
    ordinal: int,
    reason: str,
) -> dict[str, Any]:
    item_json = canonical_json(item)
    return {
        "rejection_id": stable_id(
            "tag_rejection",
            str(subject["artifact_digest"]),
            str(segment["segment_id"]),
            ordinal,
            reason,
            item_json,
            length=24,
        ),
        "profile_id": subject["profile"],
        "subject_type": subject["type"],
        "subject_id": subject["id"],
        "artifact_digest": subject["artifact_digest"],
        "source_table": subject["source_table"],
        "segment_id": segment["segment_id"],
        "segment_ordinal": segment["ordinal"],
        "item_ordinal": ordinal,
        "reason": reason,
        "concept_id": item.get("concept_id"),
        "scheme": item.get("scheme"),
        "evidence_field_key": item.get("evidence_field"),
        "item_json": item_json,
    }


def _candidate_key(item: Mapping[str, Any]) -> str:
    concept_id = str(item.get("concept_id") or "").strip()
    if concept_id:
        return f"id:{concept_id}"
    return f"label:{item.get('scheme')}:{normalize_label(item.get('concept_label'))}"


def _score_counts(expected: set[str], predicted: set[str]) -> dict[str, int | float]:
    true_positive = len(expected & predicted)
    false_positive = len(predicted - expected)
    false_negative = len(expected - predicted)
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "true_positive_count": true_positive,
        "false_positive_count": false_positive,
        "false_negative_count": false_negative,
        "micro_precision": precision,
        "micro_recall": recall,
        "micro_f1": f1,
    }


def _expected_key(item: Mapping[str, Any]) -> str:
    concept_id = str(item.get("concept_id") or "").strip()
    if concept_id:
        return f"id:{concept_id}"
    return f"label:{item.get('scheme')}:{normalize_label(item.get('label'))}"


@dataclass
class TagExtractionTask:
    """The minimal tag task consumed by :func:`run_extraction`."""

    name: str = TASK_NAME
    schema_name: str = SCHEMA_NAME
    instructions: str = TAG_INSTRUCTIONS
    max_output_tokens: int = TAG_MAX_OUTPUT_TOKENS
    forbidden_payload_keys: frozenset[str] = MODEL_INPUT_FORBIDDEN_KEYS
    candidate_table: str = CANDIDATE_TABLE
    rejection_table: str = REJECTION_TABLE

    def build_payload(self, unit_input: Mapping[str, Any]) -> dict[str, Any]:
        payload = {key: unit_input.get(key) for key in _PAYLOAD_KEYS}
        _validate_payload(payload)
        return cast(dict[str, Any], _plain(payload))

    def build_schema(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        _validate_payload(payload)
        return cast(dict[str, Any], _plain(TAG_SCHEMA))

    def check_response(self, response: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
        errors = sorted(Draft202012Validator(dict(schema)).iter_errors(dict(response)), key=lambda item: list(item.path))
        if errors:
            raise ResponseCheckError(f"tag response violates the strict schema at {list(errors[0].path)}")

    def build_candidates(self, response: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
        _validate_payload(payload)
        subject = _object(payload["subject"], "subject")
        segment = _object(payload["processing_segment"], "processing_segment")
        evidence = _object(payload["untrusted_evidence_fields"], "untrusted_evidence_fields")
        fields = _object(evidence["fields"], "untrusted_evidence_fields.fields")
        source_spans = _object(segment["source_spans"], "processing_segment.source_spans")
        concepts = {
            str(concept["concept_id"]): concept
            for concept in (
                _object(raw, "available concept")
                for raw in _array(payload["available_concepts"], "available_concepts")
            )
        }
        aliases: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for concept in concepts.values():
            scheme = str(concept.get("scheme") or "")
            for alias in concept_aliases(concept):
                aliases.setdefault((scheme, alias), []).append(concept)
        allowed_schemes = {str(value) for value in cast(list[Any], subject["allowed_schemes"])}
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        tags = _array(response.get("tags"), "response.tags")
        for ordinal, raw in enumerate(tags):
            item = _object(raw, f"response.tags[{ordinal}]")
            concept_id = str(item.get("concept_id") or "").strip() or None
            concept = concepts.get(concept_id) if concept_id is not None else None
            scheme = str(item.get("scheme") or "")
            proposed_label = str(item.get("proposed_label") or "").strip()
            field_key = str(item.get("evidence_field") or "")
            reason = ""
            if concept_id is not None and concept is None:
                reason = "unknown_concept"
            elif scheme not in allowed_schemes:
                reason = "disallowed_scheme"
            elif concept is not None and str(concept.get("scheme") or "") != scheme:
                reason = "concept_scheme_mismatch"
            elif concept_id is None and proposed_label:
                matches = aliases.get((scheme, normalize_label(proposed_label)), [])
                if len(matches) == 1:
                    concept = matches[0]
                    concept_id = str(concept["concept_id"])
                elif len(matches) > 1:
                    reason = "ambiguous_concept_alias"
            elif field_key not in fields:
                reason = "unknown_evidence_field"
            elif concept_id is None and (
                not proposed_label
                or not str(item.get("definition") or "").strip()
            ):
                reason = "incomplete_novel_concept"

            if not reason and field_key not in fields:
                reason = "unknown_evidence_field"
            elif (
                not reason
                and concept_id is None
                and (not proposed_label or not str(item.get("definition") or "").strip())
            ):
                reason = "incomplete_novel_concept"

            field_text = str(fields.get(field_key) or "")
            resolution = None
            if not reason:
                resolution = resolve_exact_evidence_offsets(
                    field_text,
                    str(item.get("evidence_text") or ""),
                    cast(int | None, item.get("evidence_start")),
                    cast(int | None, item.get("evidence_end")),
                )
                if resolution is None:
                    reason = "ungrounded_evidence"
            if reason:
                rejected.append(
                    _rejection(
                        subject=subject,
                        segment=segment,
                        item=item,
                        ordinal=ordinal,
                        reason=reason,
                    )
                )
                continue

            assert resolution is not None
            span = _object(source_spans[field_key], f"processing_segment.source_spans.{field_key}")
            source_start = int(span["start_char"]) + resolution.start
            source_end = int(span["start_char"]) + resolution.end
            if source_end > int(span["end_char"]):
                rejected.append(
                    _rejection(
                        subject=subject,
                        segment=segment,
                        item=item,
                        ordinal=ordinal,
                        reason="evidence_exceeds_source_span",
                    )
                )
                continue
            label = (
                str(concept.get("pref_label") or "").strip()
                if concept is not None
                else str(item.get("proposed_label") or "").strip()
            )
            definition = (
                str(concept.get("definition") or "").strip()
                if concept is not None
                else str(item.get("definition") or "").strip()
            )
            candidate_identity = {
                "artifact_digest": subject["artifact_digest"],
                "segment_id": segment["segment_id"],
                "concept_id": concept_id,
                "concept_label": label,
                "scheme": scheme,
                "source_field": span["source_field"],
                "source_start_char": source_start,
                "source_end_char": source_end,
            }
            accepted.append(
                {
                    "candidate_id": stable_id(
                        "tag_candidate",
                        canonical_json(candidate_identity),
                        length=24,
                    ),
                    "profile_id": subject["profile"],
                    "subject_type": subject["type"],
                    "subject_id": subject["id"],
                    "artifact_digest": subject["artifact_digest"],
                    "source_table": subject["source_table"],
                    "segment_id": segment["segment_id"],
                    "segment_ordinal": segment["ordinal"],
                    "segment_count": segment["segment_count"],
                    "concept_id": concept_id,
                    "concept_label": label,
                    "concept_status": "existing" if concept is not None else "novel",
                    "scheme": scheme,
                    "definition": definition,
                    "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0.0))),
                    "evidence_field_key": field_key,
                    "source_field": span["source_field"],
                    "evidence_grade": span.get("evidence_grade"),
                    "content_layer": span.get("content_layer"),
                    "coordinate_grade": span.get("coordinate_grade"),
                    "context_only": span.get("context_only") is True,
                    "source_start_char": source_start,
                    "source_end_char": source_end,
                    "segment_start_char": resolution.start,
                    "segment_end_char": resolution.end,
                    "evidence_text": item["evidence_text"],
                    "evidence_alignment_method": resolution.method,
                    "justification": str(item.get("justification") or "").strip(),
                    "external_ids_json": canonical_json(list(validated_external_ids(item.get("external_ids")))),
                    "grounded": True,
                }
            )
        return {
            "segment": {
                "profile_id": subject["profile"],
                "subject_type": subject["type"],
                "subject_id": subject["id"],
                "artifact_digest": subject["artifact_digest"],
                "segment_id": segment["segment_id"],
                "segment_ordinal": segment["ordinal"],
            },
            "candidates": accepted,
            "rejections": rejected,
        }

    def merge_candidates(self, parts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        segments: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        for part in parts:
            segments.append(_object(part.get("segment"), "candidate part segment"))
            candidates.extend(
                _object(raw, "candidate") for raw in _array(part.get("candidates"), "candidate part candidates")
            )
            rejections.extend(
                _object(raw, "rejection") for raw in _array(part.get("rejections"), "candidate part rejections")
            )
        return {
            "segments": sorted(
                segments,
                key=lambda item: (
                    str(item["profile_id"]),
                    str(item["subject_type"]),
                    str(item["subject_id"]),
                    int(item["segment_ordinal"]),
                ),
            ),
            "candidates": sorted(candidates, key=lambda item: str(item["candidate_id"])),
            "rejections": sorted(rejections, key=lambda item: str(item["rejection_id"])),
        }

    def is_empty(self, candidates: Mapping[str, Any]) -> bool:
        return not candidates.get("candidates") and not candidates.get("rejections")

    def candidate_columns(self) -> tuple[tuple[str, str], ...]:
        return CANDIDATE_COLUMNS

    def rejection_columns(self) -> tuple[tuple[str, str], ...]:
        return REJECTION_COLUMNS

    def candidate_rows(self, candidates: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            _object(raw, "candidate")
            for raw in _array(candidates.get("candidates"), "candidates")
        ]

    def rejection_rows(self, candidates: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            _object(raw, "rejection")
            for raw in _array(candidates.get("rejections"), "rejections")
        ]

    def score(self, answers: Mapping[str, Any], candidates: Mapping[str, Any]) -> dict[str, Any]:
        artifact_answers = [
            _object(raw, "answers.artifact")
            for raw in _array(answers.get("artifacts"), "answers.artifacts")
        ]
        segment_answers = [
            _object(raw, "answers.segment")
            for raw in _array(answers.get("segments"), "answers.segments")
        ]
        artifact_by_digest: dict[str, dict[str, Any]] = {}
        expected_by_artifact: dict[str, dict[str, dict[str, Any]]] = {}
        for artifact in artifact_answers:
            digest = _required_text(artifact, "artifact_digest", "answers.artifact")
            if digest in artifact_by_digest:
                raise ExtractionError(f"answers repeat artifact {digest}")
            artifact_by_digest[digest] = artifact
            expected: dict[str, dict[str, Any]] = {}
            for raw in _array(artifact.get("expected_tags"), "answers.artifact.expected_tags"):
                item = _object(raw, "expected tag")
                expected[_expected_key(item)] = item
            if not expected:
                raise ExtractionError(f"answers artifact {digest} has no expected tags")
            expected_by_artifact[digest] = expected

        raw_candidates = [
            _object(raw, "candidate")
            for raw in _array(candidates.get("candidates"), "candidates")
        ]
        raw_rejections = [
            _object(raw, "rejection")
            for raw in _array(candidates.get("rejections"), "rejections")
        ]
        predicted_by_artifact: dict[str, dict[str, dict[str, Any]]] = {
            digest: {} for digest in artifact_by_digest
        }
        for candidate in raw_candidates:
            digest = str(candidate.get("artifact_digest") or "")
            if digest not in predicted_by_artifact:
                continue
            key = _candidate_key(candidate)
            prior = predicted_by_artifact[digest].get(key)
            if prior is None or float(candidate.get("confidence") or 0.0) > float(prior.get("confidence") or 0.0):
                predicted_by_artifact[digest][key] = candidate

        profiles = sorted({str(artifact["profile_id"]) for artifact in artifact_answers})

        def scope_metrics(profile_id: str | None) -> dict[str, Any]:
            digests = [
                digest
                for digest, artifact in artifact_by_digest.items()
                if profile_id is None or str(artifact["profile_id"]) == profile_id
            ]
            expected = {
                (digest, key)
                for digest in digests
                for key in expected_by_artifact[digest]
            }
            predicted = {
                (digest, key)
                for digest in digests
                for key in predicted_by_artifact[digest]
            }
            counts = _score_counts(
                {canonical_json(item) for item in expected},
                {canonical_json(item) for item in predicted},
            )
            artifact_scores = [
                _score_counts(
                    set(expected_by_artifact[digest]),
                    set(predicted_by_artifact[digest]),
                )
                for digest in digests
            ]
            return {
                "scope": "all-gold-artifacts" if profile_id is None else "profile",
                "profile_id": profile_id,
                "artifact_count": len(digests),
                "gold_positive_count": sum(len(expected_by_artifact[digest]) for digest in digests),
                "predicted_positive_count": sum(len(predicted_by_artifact[digest]) for digest in digests),
                **counts,
                "artifact_macro_precision": (
                    sum(float(item["micro_precision"]) for item in artifact_scores) / len(artifact_scores)
                    if artifact_scores
                    else 0.0
                ),
                "artifact_macro_recall": (
                    sum(float(item["micro_recall"]) for item in artifact_scores) / len(artifact_scores)
                    if artifact_scores
                    else 0.0
                ),
                "artifact_macro_f1": (
                    sum(float(item["micro_f1"]) for item in artifact_scores) / len(artifact_scores)
                    if artifact_scores
                    else 0.0
                ),
                "artifact_exact_match_rate": (
                    sum(
                        set(expected_by_artifact[digest]) == set(predicted_by_artifact[digest])
                        for digest in digests
                    )
                    / len(digests)
                    if digests
                    else 0.0
                ),
            }

        overall = scope_metrics(None)
        per_profile = [scope_metrics(profile_id) for profile_id in profiles]
        false_positives: list[dict[str, Any]] = []
        false_negatives: list[dict[str, Any]] = []
        novel_tags: list[dict[str, Any]] = []
        for digest, artifact in artifact_by_digest.items():
            expected = expected_by_artifact[digest]
            predicted = predicted_by_artifact[digest]
            for key in sorted(set(predicted) - set(expected)):
                candidate = predicted[key]
                false_positives.append(
                    {
                        "profile_id": artifact["profile_id"],
                        "subject_type": artifact["subject_type"],
                        "subject_id": artifact["subject_id"],
                        "artifact_digest": digest,
                        "concept_id": candidate.get("concept_id"),
                        "concept_label": candidate.get("concept_label"),
                        "scheme": candidate.get("scheme"),
                        "confidence": candidate.get("confidence"),
                        "source_field": candidate.get("source_field"),
                        "start_char": candidate.get("source_start_char"),
                        "end_char": candidate.get("source_end_char"),
                        "exact_text": candidate.get("evidence_text"),
                    }
                )
            for key in sorted(set(expected) - set(predicted)):
                item = expected[key]
                false_negatives.append(
                    {
                        "profile_id": artifact["profile_id"],
                        "subject_type": artifact["subject_type"],
                        "subject_id": artifact["subject_id"],
                        "artifact_digest": digest,
                        "gold_id": item.get("gold_id"),
                        "concept_id": item.get("concept_id"),
                        "concept_label": item.get("label"),
                        "scheme": item.get("scheme"),
                        "source_field": item.get("source_field"),
                        "start_char": item.get("start_char"),
                        "end_char": item.get("end_char"),
                        "exact_text": item.get("exact_text"),
                    }
                )
            for candidate in predicted.values():
                if str(candidate.get("concept_status") or "") == "novel":
                    novel_tags.append(
                        {
                            "profile_id": artifact["profile_id"],
                            "subject_type": artifact["subject_type"],
                            "subject_id": artifact["subject_id"],
                            "artifact_digest": digest,
                            "concept_label": candidate.get("concept_label"),
                            "scheme": candidate.get("scheme"),
                            "definition": candidate.get("definition"),
                            "source_field": candidate.get("source_field"),
                            "start_char": candidate.get("source_start_char"),
                            "end_char": candidate.get("source_end_char"),
                            "exact_text": candidate.get("evidence_text"),
                        }
                    )

        segment_ids = {str(item["segment_id"]) for item in segment_answers}
        candidate_counts = {segment_id: 0 for segment_id in segment_ids}
        for candidate in raw_candidates:
            segment_id = str(candidate.get("segment_id") or "")
            if segment_id in candidate_counts:
                candidate_counts[segment_id] += 1
        prompt_injection_ids = {
            str(item["segment_id"])
            for item in segment_answers
            if "adversarial-prompt-injection"
            in {str(value) for value in cast(list[Any], item.get("adversarial_case_ids") or [])}
        }
        prompt_injection_candidates = [
            candidate
            for candidate in raw_candidates
            if str(candidate.get("segment_id") or "") in prompt_injection_ids
        ]
        control_segments: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for segment_answer in segment_answers:
            digest = str(segment_answer.get("artifact_digest") or "")
            if digest and digest not in artifact_by_digest:
                control_segments[digest].append(segment_answer)
        controls: list[dict[str, Any]] = []
        for digest, segments in sorted(control_segments.items()):
            segment_ids_for_artifact = {
                str(segment.get("segment_id") or "") for segment in segments
            }
            artifact_candidates = [
                candidate
                for candidate in raw_candidates
                if str(candidate.get("segment_id") or "") in segment_ids_for_artifact
            ]
            artifact_rejections = [
                rejection
                for rejection in raw_rejections
                if str(rejection.get("segment_id") or "") in segment_ids_for_artifact
            ]
            adversarial_case_ids = sorted(
                {
                    str(case_id)
                    for segment in segments
                    for case_id in cast(
                        list[Any],
                        segment.get("adversarial_case_ids") or [],
                    )
                }
            )
            first = segments[0]
            controls.append(
                {
                    "profile_id": first.get("profile_id"),
                    "subject_type": first.get("subject_type"),
                    "subject_id": first.get("subject_id"),
                    "artifact_digest": digest,
                    "segment_count": len(segments),
                    "candidate_count": len(artifact_candidates),
                    "rejection_count": len(artifact_rejections),
                    "adversarial_case_ids": adversarial_case_ids,
                    "candidates": [
                        {
                            "concept_id": candidate.get("concept_id"),
                            "concept_label": candidate.get("concept_label"),
                            "scheme": candidate.get("scheme"),
                            "source_field": candidate.get("source_field"),
                            "start_char": candidate.get("source_start_char"),
                            "end_char": candidate.get("source_end_char"),
                            "exact_text": candidate.get("evidence_text"),
                        }
                        for candidate in artifact_candidates
                    ],
                    "rejection_reasons": dict(
                        sorted(
                            Counter(
                                str(rejection.get("reason") or "")
                                for rejection in artifact_rejections
                            ).items()
                        )
                    ),
                }
            )
        grounded = sum(candidate.get("grounded") is True for candidate in raw_candidates)
        novel_count = sum(str(candidate.get("concept_status") or "") == "novel" for candidate in raw_candidates)
        return {
            "metric_version": "tag-diagnostic-v1",
            **overall,
            "selected_segment_count": len(segment_answers),
            "accepted_candidate_count": len(raw_candidates),
            "rejected_candidate_count": len(raw_rejections),
            "evidence_grounding_rate": grounded / len(raw_candidates) if raw_candidates else 1.0,
            "empty_tag_rate": (
                sum(count == 0 for count in candidate_counts.values()) / len(candidate_counts)
                if candidate_counts
                else 0.0
            ),
            "novel_tag_rate": novel_count / len(raw_candidates) if raw_candidates else 0.0,
            "prompt_injection_segment_count": len(prompt_injection_ids),
            "prompt_injection_candidate_count": len(prompt_injection_candidates),
            "prompt_injection_grounding_rate": (
                sum(candidate.get("grounded") is True for candidate in prompt_injection_candidates)
                / len(prompt_injection_candidates)
                if prompt_injection_candidates
                else 1.0
            ),
            "control_artifact_count": len(controls),
            "controls": controls,
            "per_profile": per_profile,
            "false_positives": sorted(
                false_positives,
                key=lambda item: (
                    str(item["profile_id"]),
                    str(item["subject_id"]),
                    str(item.get("concept_label") or ""),
                ),
            ),
            "false_negatives": sorted(
                false_negatives,
                key=lambda item: (
                    str(item["profile_id"]),
                    str(item["subject_id"]),
                    str(item.get("concept_label") or ""),
                ),
            ),
            "novel_tags": sorted(
                novel_tags,
                key=lambda item: (
                    str(item["profile_id"]),
                    str(item["subject_id"]),
                    str(item.get("concept_label") or ""),
                ),
            ),
        }

    def review_gate(
        self,
        unit_inputs: Sequence[Mapping[str, Any]],
        answers: Mapping[str, Any],
        *,
        protocol_sha256: str,
    ) -> dict[str, Any]:
        del unit_inputs, answers, protocol_sha256
        return {
            "eligible": False,
            "mode": "diagnostic",
            "reason": "tag candidates require review before approval or publication",
        }

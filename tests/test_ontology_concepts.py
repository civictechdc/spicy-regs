"""Property-style and end-to-end tests for the iterative concept loop."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import RunContext, canonical_json, write_parquet_rows
from spicy_regs.ontology.concepts import (
    ASSIGNMENT_COLUMNS,
    CANDIDATE_REGISTRY_MAX_TOKENS,
    CONCEPT_COLUMNS,
    candidate_concept,
    generate_for_subject,
    latest_assignments,
    merge_pass,
    rescore_candidates,
    seed_concept,
    select_candidate_concepts,
)
from spicy_regs.ontology.invariants import (
    OntologyInvariantError,
    assert_append_only,
    assert_attestation_complete,
    assert_concept_graphs,
    resolve_replacement,
)
from spicy_regs.ontology.llm import (
    CONTEXT_MAX_TOKENS,
    EVIDENCE_ALIGNMENT_PROVIDED,
    EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    TAG_MAX_OUTPUT_TOKENS,
    OpenAIProviderExhaustedError,
    OpenAIOntologyModel,
    TagProposal,
    ValidationProposal,
    ontology_concept_payload,
)
from spicy_regs.ontology.segmentation import TiktokenCounter
from spicy_regs.ontology.subjects import Subject, balanced_subject_batch
from spicy_regs.transforms.build_concept_assignments import build_concept_assignments
from spicy_regs.transforms.build_concept_events import build_concept_events
from spicy_regs.transforms.build_concepts import build_concepts


_CONTEXT = RunContext("ontology-test", "2026-07-23T12:00:00Z")


def _subject() -> Subject:
    return Subject(
        subject_type="docket",
        subject_id="EPA-HQ-OAR-2026-0001",
        text="PFAS chemicals in drinking water",
        fields={"dockets.title": "PFAS chemicals in drinking water"},
        digest="subject-digest",
    )


def _proposal(
    *,
    concept_id: str | None = None,
    label: str | None = "PFAS",
    confidence: float = 0.9,
) -> TagProposal:
    return TagProposal(
        concept_id=concept_id,
        proposed_label=label,
        scheme="subject",
        definition=None if concept_id else "Rules concerning PFAS chemicals.",
        confidence=confidence,
        evidence_text="PFAS",
        evidence_field="dockets.title",
        justification="The title names PFAS.",
    )


def test_bounded_subject_batch_balances_types_without_losing_stable_order():
    subjects = [replace(_subject(), subject_type="docket", subject_id=f"D-{index}") for index in range(3)] + [
        replace(_subject(), subject_type="document", subject_id=f"F-{index}") for index in range(2)
    ]

    selected = balanced_subject_batch(subjects, 4)

    assert [subject.subject_type for subject in selected] == [
        "docket",
        "document",
        "docket",
        "document",
    ]
    assert balanced_subject_batch(reversed(subjects), 4) == selected


def test_candidate_budget_counts_only_the_model_visible_projection():
    concepts = [
        {
            "concept_id": f"concept-{index:02d}",
            "scheme": "subject",
            "pref_label": f"topic {index:02d}",
            "alt_labels_json": "[]",
            "definition": None,
            "status": "active",
            "actor_id": "irrelevant-attestation-" + ("x" * 2_000),
        }
        for index in range(40)
    ]

    selected = select_candidate_concepts(_subject(), concepts)
    counter = TiktokenCounter()

    assert len(selected) == 40
    assert (
        counter.count(canonical_json([ontology_concept_payload(concept) for concept in selected]))
        <= CANDIDATE_REGISTRY_MAX_TOKENS
    )
    assert counter.count(canonical_json(selected)) > (CANDIDATE_REGISTRY_MAX_TOKENS)


class _FakeModel:
    model_id = "test-model:v1"
    production_provider = False

    def __init__(self, *, concept_id: str | None = None) -> None:
        self.concept_id = concept_id
        self.tag_calls = 0
        self.validation_calls = 0

    def tag(self, subject, concepts):
        self.tag_calls += 1
        return [_proposal(concept_id=self.concept_id, label=None if self.concept_id else "PFAS")]

    def validate(self, *, subject, concept, assignment):
        self.validation_calls += 1
        return ValidationProposal(
            agrees=False,
            confidence=0.35,
            rationale="The evidence supports a narrower assertion.",
        )


@pytest.mark.parametrize("size", range(1, 33))
def test_concept_graph_accepts_acyclic_chains_of_varying_size(size):
    rows = [
        {
            "concept_id": f"c{index}",
            "broader_id": f"c{index - 1}" if index else None,
            "replaced_by": None,
            "status": "active",
        }
        for index in range(size)
    ]
    assert_concept_graphs(rows)


def test_concept_graph_rejects_cycles_and_unresolved_replacements():
    with pytest.raises(OntologyInvariantError, match="cycle"):
        assert_concept_graphs(
            [
                {"concept_id": "a", "broader_id": "b", "replaced_by": None, "status": "active"},
                {"concept_id": "b", "broader_id": "a", "replaced_by": None, "status": "active"},
            ]
        )
    with pytest.raises(OntologyInvariantError, match="unknown"):
        assert_concept_graphs(
            [{"concept_id": "a", "broader_id": None, "replaced_by": "missing", "status": "deprecated"}]
        )


def test_append_only_and_attestation_invariants_reject_mutation():
    prior = [{"assignment_id": "a1", "confidence": "0.9"}]
    assert_append_only(prior, [*prior, {"assignment_id": "a2"}], id_column="assignment_id")
    with pytest.raises(OntologyInvariantError, match="hard-deleted"):
        assert_append_only(prior, [], id_column="assignment_id")
    with pytest.raises(OntologyInvariantError, match="modified"):
        assert_append_only(
            prior,
            [{"assignment_id": "a1", "confidence": "0.2"}],
            id_column="assignment_id",
        )
    with pytest.raises(OntologyInvariantError, match="missing attestation"):
        assert_attestation_complete([{"assignment_id": "a1", "method": "llm"}])


def test_generate_merge_and_rescore_converges_without_deleting_history():
    model = _FakeModel()
    new_concepts, assignments, events = generate_for_subject(
        subject=_subject(),
        concepts=[],
        model=model,
        context=_CONTEXT,
    )
    assert len(new_concepts) == len(assignments) == len(events) == 1
    assert new_concepts[0]["status"] == "candidate"
    assert events[0]["event_type"] == "seed"
    assert_attestation_complete([*new_concepts, *assignments, *events])

    first = new_concepts[0]
    first["alt_labels_json"] = canonical_json(["Per- and polyfluoroalkyl substances"])
    second = candidate_concept(
        replace(
            _proposal(),
            proposed_label="Per- and polyfluoroalkyl substances",
            definition="Rules concerning per- and polyfluoroalkyl substances.",
        ),
        _CONTEXT,
        actor_id=model.model_id,
    )
    assignment_rows = [
        assignments[0],
        {**assignments[0], "assignment_id": "a2", "concept_id": first["concept_id"]},
        {**assignments[0], "assignment_id": "a3", "concept_id": second["concept_id"]},
    ]
    merged, merge_events, review = merge_pass(
        [first, second],
        assignment_rows,
        context=_CONTEXT,
    )
    assert review == []
    assert [event["event_type"] for event in merge_events] == ["merge"]
    deprecated = next(concept for concept in merged if concept["status"] == "deprecated")
    winner_id = str(deprecated["replaced_by"])
    assert resolve_replacement(str(deprecated["concept_id"]), merged) == winner_id
    assert {concept["concept_id"] for concept in merged} == {
        first["concept_id"],
        second["concept_id"],
    }

    sustained = [
        {**assignments[0], "assignment_id": f"usage-{index}", "concept_id": winner_id, "confidence": "0.9"}
        for index in range(3)
    ]
    rescored, rescore_events = rescore_candidates(
        merged,
        sustained,
        context=_CONTEXT,
    )
    assert next(concept for concept in rescored if concept["concept_id"] == winner_id)["status"] == "active"
    assert [event["event_type"] for event in rescore_events] == ["promote"]


def test_merge_pass_routes_high_usage_ambiguous_pairs_to_human_review():
    left = seed_concept({"name": "Climate change"}, _CONTEXT)
    right = seed_concept({"name": "Climate changes"}, _CONTEXT)
    assert left is not None
    assert right is not None
    assignment = {
        "assignment_id": "review-usage",
        "subject_type": "docket",
        "subject_id": "EPA-HQ-OAR-2026-0002",
        "concept_id": left["concept_id"],
    }

    updated, events, review = merge_pass(
        [left, right],
        [assignment],
        context=_CONTEXT,
        auto_threshold=1.1,
        review_threshold=0.0,
        high_usage=1,
    )

    assert updated == [left, right]
    assert events == []
    assert len(review) == 1
    assert {review[0]["left_id"], review[0]["right_id"]} == {
        left["concept_id"],
        right["concept_id"],
    }
    assert review[0]["usage"] == 1


def test_regulated_entity_candidate_preserves_resolved_cas_anchor():
    proposal = replace(
        _proposal(),
        scheme="regulated_entity",
        proposed_label="Perfluorooctanoic acid",
        external_ids=({"scheme": "cas", "value": "335-67-1"},),
    )
    concept = candidate_concept(
        proposal,
        _CONTEXT,
        actor_id="test-model:v1",
    )
    assert concept["scheme"] == "regulated_entity"
    assert json.loads(concept["external_ids_json"]) == [{"scheme": "cas", "value": "335-67-1"}]


def test_openai_provider_uses_strict_responses_schema_and_grounded_evidence():
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    calls: list[dict] = []

    class _Responses:
        def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                output_text=json.dumps(
                    {
                        "tags": [
                            {
                                "concept_id": seed["concept_id"],
                                "proposed_label": None,
                                "scheme": "subject",
                                "definition": None,
                                "confidence": 0.91,
                                "evidence_text": "PFAS",
                                "evidence_field": "dockets.title",
                                "evidence_start": 0,
                                "evidence_end": 4,
                                "justification": "The title names PFAS.",
                                "external_ids": [],
                            }
                        ]
                    }
                )
            )

    model = cast(Any, object.__new__(OpenAIOntologyModel))
    model.model = "gpt-5.6-luna"
    model.model_id = "openai:gpt-5.6-luna"
    model._client = SimpleNamespace(responses=_Responses())

    proposals = model.tag(_subject(), [seed])

    assert len(proposals) == 1
    assert proposals[0].concept_id == seed["concept_id"]
    assert calls[0]["text"]["format"]["type"] == "json_schema"
    assert calls[0]["text"]["format"]["strict"] is True
    assert calls[0]["reasoning"] == {"effort": "medium"}
    assert calls[0]["service_tier"] == "auto"
    assert calls[0]["store"] is False
    assert calls[0]["max_output_tokens"] == TAG_MAX_OUTPUT_TOKENS
    assert model.last_call_metadata["status"] == "completed"
    assert model.last_call_metadata["store"] is False
    assert len(str(model.last_call_metadata["request_sha256"])) == 64
    assert model.last_call_metadata["reasoning_effort"] == "medium"
    assert model.last_call_metadata["requested_service_tier"] == "auto"
    assert model.last_call_metadata["sdk_max_retries"] == 0
    assert model.last_call_metadata["attempt_count"] == 1
    assert model.last_call_metadata["tag_output_item_count"] == 1
    assert model.last_call_metadata["tag_accepted_item_count"] == 1
    assert model.last_call_metadata["tag_rejection_count"] == 0
    assert model.last_call_metadata["evidence_offset_repair_count"] == 0
    assert proposals[0].evidence_alignment_method == (
        EVIDENCE_ALIGNMENT_PROVIDED
    )
    assert (
        model.last_call_metadata["prompt_token_estimate"] + model.last_call_metadata["prompt_safety_margin_tokens"]
        <= model.last_call_metadata["prompt_input_token_budget"]
    )


def _provider_response(
    *,
    output_text: str | None = None,
    status: str = "completed",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=f"response-{status}",
        model="gpt-test",
        status=status,
        usage=SimpleNamespace(
            input_tokens=100,
            output_tokens=10,
            total_tokens=110,
        ),
        output_text=(json.dumps({"tags": []}) if output_text is None else output_text),
    )


def _provider_with_outcomes(
    outcomes: list[BaseException | SimpleNamespace],
    *,
    max_retries: int,
) -> tuple[OpenAIOntologyModel, list[dict]]:
    calls: list[dict] = []

    class _Responses:
        def create(self, **kwargs):
            calls.append(kwargs)
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    model = cast(Any, object.__new__(OpenAIOntologyModel))
    model.model = "gpt-test"
    model.model_id = "openai:gpt-test"
    model.timeout_seconds = 10
    model.max_retries = max_retries
    model.retry_base_seconds = 0
    model._client = SimpleNamespace(responses=_Responses())
    return model, calls


def _provider_tag_item(
    concept_id: str,
    *,
    start: int,
    end: int,
) -> dict[str, object]:
    return {
        "concept_id": concept_id,
        "proposed_label": None,
        "scheme": "subject",
        "definition": None,
        "confidence": 0.91,
        "evidence_text": "PFAS",
        "evidence_field": "dockets.title",
        "evidence_start": start,
        "evidence_end": end,
        "justification": "The title names PFAS.",
        "external_ids": [],
    }


def test_openai_provider_repairs_only_unique_verbatim_evidence_offsets() -> None:
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    response = _provider_response(
        output_text=json.dumps(
            {
                "tags": [
                    _provider_tag_item(
                        str(seed["concept_id"]),
                        start=1,
                        end=5,
                    )
                ]
            }
        ),
    )
    model, _ = _provider_with_outcomes([response], max_retries=0)

    proposals = model.tag(_subject(), [seed])

    assert len(proposals) == 1
    assert proposals[0].evidence_start == 0
    assert proposals[0].evidence_end == 4
    assert proposals[0].evidence_alignment_method == (
        EVIDENCE_ALIGNMENT_UNIQUE_EXACT
    )
    assert model.last_call_metadata is not None
    assert model.last_call_metadata["evidence_offset_repair_count"] == 1
    assert model.last_call_metadata["tag_accepted_item_count"] == 1
    assert model.last_call_metadata["tag_rejection_count"] == 0


def test_openai_provider_rejects_ambiguous_evidence_offset_repair() -> None:
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    response = _provider_response(
        output_text=json.dumps(
            {
                "tags": [
                    _provider_tag_item(
                        str(seed["concept_id"]),
                        start=1,
                        end=5,
                    )
                ]
            }
        ),
    )
    model, _ = _provider_with_outcomes([response], max_retries=0)
    repeated = replace(
        _subject(),
        text="PFAS and PFAS",
        fields={"dockets.title": "PFAS and PFAS"},
    )

    assert model.tag(repeated, [seed]) == []
    assert model.last_tag_rejections == [
        {
            "reason": "ungrounded_evidence",
            "source_field": "dockets.title",
        }
    ]
    assert model.last_call_metadata is not None
    assert model.last_call_metadata["evidence_offset_repair_count"] == 0
    assert model.last_call_metadata["tag_accepted_item_count"] == 0
    assert model.last_call_metadata["tag_rejection_count"] == 1


class _RateLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-rate-limit"


class _QuotaLimitEquivalent(RuntimeError):
    status_code = 429
    request_id = "request-quota-limit"
    body = {
        "error": {
            "code": "insufficient_quota",
            "type": "insufficient_quota",
        }
    }


@pytest.mark.parametrize(
    "first_outcome,expected_error",
    [
        (TimeoutError("controlled timeout"), "TimeoutError"),
        (
            _RateLimitEquivalent("controlled rate limit"),
            "_RateLimitEquivalent",
        ),
        (
            _provider_response(output_text="{"),
            "IncompleteStructuredResponseError",
        ),
        (
            _provider_response(status="incomplete"),
            "IncompleteStructuredResponseError",
        ),
    ],
)
def test_openai_provider_retries_visible_transient_failures(
    first_outcome: BaseException | SimpleNamespace,
    expected_error: str,
) -> None:
    model, calls = _provider_with_outcomes(
        [first_outcome, _provider_response()],
        max_retries=1,
    )

    assert model.tag(_subject(), []) == []
    assert len(calls) == 2
    metadata = model.last_call_metadata
    assert metadata is not None
    assert metadata["status"] == "completed"
    assert metadata["attempt_count"] == 2
    assert metadata["retry_count"] == 1
    attempts = cast(list[dict[str, object]], metadata["attempts"])
    assert attempts[0]["error_code"] == expected_error
    assert attempts[1]["status"] == "completed"


def test_openai_provider_reports_exact_retry_exhaustion() -> None:
    model, calls = _provider_with_outcomes(
        [
            TimeoutError("controlled timeout one"),
            TimeoutError("controlled timeout two"),
            TimeoutError("controlled timeout three"),
        ],
        max_retries=2,
    )

    with pytest.raises(
        OpenAIProviderExhaustedError,
        match="exhausted 3 attempts",
    ):
        model.tag(_subject(), [])

    assert len(calls) == 3
    metadata = model.last_call_metadata
    assert metadata is not None
    assert metadata["status"] == "retry_exhausted"
    assert metadata["attempt_count"] == 3
    assert metadata["retry_count"] == 2
    assert {attempt["error_code"] for attempt in cast(list[dict[str, object]], metadata["attempts"])} == {
        "TimeoutError"
    }


def test_openai_provider_does_not_retry_insufficient_quota() -> None:
    model, calls = _provider_with_outcomes(
        [
            _QuotaLimitEquivalent("controlled hard quota limit"),
            _provider_response(),
        ],
        max_retries=3,
    )

    with pytest.raises(_QuotaLimitEquivalent):
        model.tag(_subject(), [])

    assert len(calls) == 1
    metadata = model.last_call_metadata
    assert metadata is not None
    assert metadata["status"] == "failed"
    assert metadata["attempt_count"] == 1
    assert metadata["retry_count"] == 0
    attempt = cast(list[dict[str, object]], metadata["attempts"])[0]
    assert attempt["status_code"] == 429
    assert attempt["provider_error_code"] == "insufficient_quota"


def test_openai_prompt_context_is_bounded_and_non_evidentiary() -> None:
    model, calls = _provider_with_outcomes(
        [_provider_response()],
        max_retries=0,
    )
    subject = replace(
        _subject(),
        context_fields={
            "artifact_title": "PFAS " * 2_000,
            "heading_path": "Section > Subsection",
        },
    )

    assert model.tag(subject, []) == []

    payload = json.loads(calls[0]["input"])
    context = payload["non_evidentiary_context"]
    assert context["max_tokens"] == CONTEXT_MAX_TOKENS
    assert context["truncated_fields"] == ["artifact_title"]
    assert context["omitted_fields"] == ["heading_path"]
    assert TiktokenCounter().count(canonical_json(context["fields"])) <= CONTEXT_MAX_TOKENS
    assert payload["untrusted_evidence_fields"]["fields"] == subject.fields


def test_openai_provider_rejects_evidence_attributed_to_the_wrong_field():
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    subject = Subject(
        subject_type="document",
        subject_id="DOC-1",
        text="General notice\nPFAS chemicals in drinking water",
        fields={
            "documents.title": "General notice",
            "federal_register.abstract": "PFAS chemicals in drinking water",
        },
        digest="subject-digest",
    )

    class _Responses:
        def create(self, **kwargs):
            del kwargs
            return SimpleNamespace(
                output_text=json.dumps(
                    {
                        "tags": [
                            {
                                "concept_id": seed["concept_id"],
                                "proposed_label": None,
                                "scheme": "subject",
                                "definition": None,
                                "confidence": 0.91,
                                "evidence_text": "PFAS",
                                "evidence_field": "documents.title",
                                "evidence_start": 0,
                                "evidence_end": 4,
                                "justification": "The abstract names PFAS.",
                                "external_ids": [],
                            }
                        ]
                    }
                )
            )

    model = cast(Any, object.__new__(OpenAIOntologyModel))
    model.model = "gpt-5.6-luna"
    model.model_id = "openai:gpt-5.6-luna"
    model._client = SimpleNamespace(responses=_Responses())

    assert model.tag(subject, [seed]) == []


def test_openai_provider_rejects_a_facet_disallowed_by_the_subject_profile():
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    calls: list[dict] = []
    subject = Subject(
        subject_type="sam_entity",
        subject_id="UEI-1",
        text="Example Chemical Company",
        fields={"sam_entities.legal_business_name": "Example Chemical Company"},
        digest="subject-digest",
        profile_id="sam-entity-v1",
        source_table="sam_entities",
        allowed_schemes=("regulated_entity",),
    )

    class _Responses:
        def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                output_text=json.dumps(
                    {
                        "tags": [
                            {
                                "concept_id": seed["concept_id"],
                                "proposed_label": None,
                                "scheme": "subject",
                                "definition": None,
                                "confidence": 0.91,
                                "evidence_text": "Example Chemical Company",
                                "evidence_field": "sam_entities.legal_business_name",
                                "evidence_start": 0,
                                "evidence_end": 24,
                                "justification": "The record names the company.",
                                "external_ids": [],
                            }
                        ]
                    }
                )
            )

    model = cast(Any, object.__new__(OpenAIOntologyModel))
    model.model = "gpt-5.6-luna"
    model.model_id = "openai:gpt-5.6-luna"
    model._client = SimpleNamespace(responses=_Responses())

    assert model.tag(subject, [seed]) == []
    assert "untrusted" in calls[0]["instructions"]
    assert json.loads(calls[0]["input"])["subject"]["profile"] == "sam-entity-v1"


def test_assignment_rollup_is_resumable_and_validation_supersedes(tmp_path):
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    write_parquet_rows(tmp_path / "concepts.parquet", columns=CONCEPT_COLUMNS, rows=[seed])
    write_parquet_rows(
        tmp_path / "dockets.parquet",
        columns=("docket_id", "title", "abstract"),
        rows=[{"docket_id": _subject().subject_id, "title": _subject().text, "abstract": None}],
    )
    write_parquet_rows(
        tmp_path / "documents.parquet",
        columns=("document_id", "fr_doc_num", "title"),
        rows=[],
    )
    write_parquet_rows(
        tmp_path / "federal_register.parquet",
        columns=("document_number", "title", "abstract"),
        rows=[],
    )

    model = _FakeModel(concept_id=str(seed["concept_id"]))
    output = build_concept_assignments(
        tmp_path,
        model=model,
        run_id="resume-run",
        asserted_at="2026-07-23T12:00:00Z",
        generation_limit=10,
        validation_percent=100,
    )
    rows = pq.read_table(output).to_pylist()
    assert pq.ParquetFile(output).schema_arrow.names == list(ASSIGNMENT_COLUMNS)
    assert len(rows) == 2
    current = latest_assignments(rows)
    assert len(current) == 1
    assert current[0]["confidence"] == "0.350000"
    assert current[0]["supersedes_id"]
    assert json.loads(current[0]["evidence_json"])["validation"]["agrees"] is False

    # Reusing the same run id consumes its checkpoints and must not grow a
    # second validation chain for work already completed in that run.
    before = rows
    build_concept_assignments(
        tmp_path,
        model=model,
        run_id="resume-run",
        asserted_at="2026-07-23T12:00:00Z",
        generation_limit=10,
        validation_percent=100,
    )
    after = pq.read_table(output).to_pylist()
    assert after == before


def test_assignment_model_failure_aborts_without_partial_output(tmp_path):
    seed = seed_concept({"name": "PFAS"}, _CONTEXT)
    assert seed is not None
    write_parquet_rows(tmp_path / "concepts.parquet", columns=CONCEPT_COLUMNS, rows=[seed])
    write_parquet_rows(
        tmp_path / "dockets.parquet",
        columns=("docket_id", "title", "abstract"),
        rows=[{"docket_id": _subject().subject_id, "title": _subject().text, "abstract": None}],
    )
    write_parquet_rows(
        tmp_path / "documents.parquet",
        columns=("document_id", "fr_doc_num", "title"),
        rows=[],
    )
    write_parquet_rows(
        tmp_path / "federal_register.parquet",
        columns=("document_number", "title", "abstract"),
        rows=[],
    )

    class FailingModel(_FakeModel):
        def tag(self, subject, concepts):
            raise ConnectionError("provider unavailable")

    with pytest.raises(RuntimeError, match="Concept tagging failed"):
        build_concept_assignments(
            tmp_path,
            model=FailingModel(concept_id=str(seed["concept_id"])),
            run_id="failed-run",
            asserted_at="2026-07-23T12:00:00Z",
            generation_limit=10,
        )

    assert not (tmp_path / "concept_assignments.parquet").exists()


def test_concept_registry_seeds_from_topics_and_event_log_is_idempotent(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    write_parquet_rows(
        tmp_path / "federal_register.parquet",
        columns=("document_number", "topics_json", "title", "abstract"),
        rows=[
            {
                "document_number": "2026-00001",
                "topics_json": '[{"name":"Air Pollution Control","slug":"air-pollution-control"}]',
                "title": "Air rule",
                "abstract": "Air pollution control.",
            }
        ],
    )
    write_parquet_rows(
        tmp_path / "dockets.parquet",
        columns=("docket_id", "title", "abstract"),
        rows=[],
    )
    write_parquet_rows(
        tmp_path / "documents.parquet",
        columns=("document_id", "fr_doc_num", "title"),
        rows=[],
    )

    concepts_file = build_concepts(
        tmp_path,
        run_id="seed-run",
        asserted_at="2026-07-23T12:00:00Z",
        discovery_limit=0,
    )
    concepts = pq.read_table(concepts_file).to_pylist()
    assert len(concepts) == 1
    assert concepts[0]["pref_label"] == "Air Pollution Control"
    assert concepts[0]["status"] == "active"

    # Seed/convergence events are materialized during the concepts rollup so
    # their exact payload survives the ephemeral workflow workspace.
    materialized_events = pq.read_table(tmp_path / "concept_events.parquet").to_pylist()
    assert len(materialized_events) == 1
    assert materialized_events[0]["event_type"] == "seed"

    events_file = build_concept_events(
        tmp_path,
        run_id="event-run",
        asserted_at="2026-07-23T12:05:00Z",
    )
    before = pq.read_table(events_file).to_pylist()
    assert len(before) == 1
    assert before[0]["event_type"] == "seed"
    build_concept_events(
        tmp_path,
        run_id="event-run",
        asserted_at="2026-07-23T12:05:00Z",
    )
    assert pq.read_table(events_file).to_pylist() == before

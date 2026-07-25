from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spicy_regs.corpora.relation_exclusion_evaluation import (
    RelationEvaluationError,
)
from spicy_regs.corpora.relation_exclusion_evaluation_v2 import (
    INSTRUCTIONS,
    build_model_payload,
    build_response_schema,
    derive_current_at_evaluation,
    evaluate_run_eligibility,
    load_corpus,
    load_oracle,
    normalize_candidates,
    score_candidates,
    validate_response_schema,
)
from spicy_regs.ontology.common import canonical_json

FIXTURE_DIR = Path(__file__).parent / "fixtures"
CORPUS_FIXTURE = FIXTURE_DIR / "relation_exclusion_explicit_denial_v2_corpus.json"
ORACLE_FIXTURE = FIXTURE_DIR / "relation_exclusion_explicit_denial_v2_oracle.provisional.json"
PROTOCOL_FIXTURE = (
    Path(__file__).parents[1]
    / "docs"
    / "superpowers"
    / "specs"
    / "2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md"
)


def _bundle() -> tuple[dict, dict, dict]:
    corpus = load_corpus(CORPUS_FIXTURE)
    oracle = load_oracle(ORACLE_FIXTURE, corpus)
    return corpus, oracle, build_model_payload(corpus)


def _preferred_evidence(expected: dict) -> dict:
    return next(option for option in expected["accepted_evidence"] if option["boundary_preference"] == "preferred")


def _provider_item(expected: dict, evidence: dict | None = None) -> dict:
    selected = evidence or _preferred_evidence(expected)
    return {
        "kind": expected["kind"],
        "polarity": expected["polarity"],
        "operation": expected["operation"],
        "stage": expected["stage"],
        "temporal_scope": copy.deepcopy(expected["temporal_scope"]),
        "intended_effective_scope": copy.deepcopy(expected["intended_effective_scope"]),
        "attribution": copy.deepcopy(expected["attribution"]),
        "conditionality": copy.deepcopy(expected["conditionality"]),
        "evidence_text": selected["quote"],
        "evidence_start": selected["start"],
        "evidence_end": selected["end"],
        "rationale": "Fixture projection of the provisional oracle.",
    }


def _perfect_required_response(oracle: dict) -> dict:
    cases: dict[str, dict] = {}
    for case in oracle["cases"]:
        items = [
            _provider_item(expected) for expected in case["expected_outputs"] if expected["requirement"] == "required"
        ]
        cases[case["case_id"]] = {
            "items": items,
            "no_answer_rationale": (None if items else "The excerpt does not support the complete target relation."),
        }
    return {"cases": cases}


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _protocol_sha256() -> str:
    return hashlib.sha256(PROTOCOL_FIXTURE.read_bytes()).hexdigest()


def _oracle_digest(oracle: dict) -> str:
    content = copy.deepcopy(oracle)
    content["metadata"].pop("oracle_content_sha256", None)
    return _digest(content)


def _complete_case_reviews(decisions: list[dict]) -> list[dict]:
    return [
        {
            "case_id": decision["case_id"],
            "target_quality": ("unsupported_argument" if decision["role"] == "unsupported_target_control" else "valid"),
            "case_status": ("annotated" if decision["expected_outputs"] else "no_explicit_support"),
            "rationale": (
                "The excerpt supports the recorded candidates."
                if decision["expected_outputs"]
                else "The complete target relation lacks explicit support."
            ),
            "decision": {
                "case_id": decision["case_id"],
                "expected_outputs": [
                    {key: copy.deepcopy(value) for key, value in expected.items() if key != "candidate_id"}
                    for expected in decision["expected_outputs"]
                ],
            },
        }
        for decision in decisions
    ]


def _eligible_final_oracle(
    corpus: dict,
    oracle: dict,
    protocol_sha256: str,
) -> dict:
    final = copy.deepcopy(oracle)
    final["metadata"]["oracle_status"] = "final-human-adjudicated"
    final["metadata"]["frozen_at"] = "2026-07-25T14:00:00Z"
    case_reviews = _complete_case_reviews(final["cases"])
    reviews = []
    for ordinal in (1, 2):
        review = {
            "review_id": f"urn:spicy-regs:review:human-{ordinal}",
            "reviewer_id": f"urn:spicy-regs:person:reviewer-{ordinal}",
            "reviewer_kind": "human",
            "started_at": ("2026-07-25T09:00:00Z" if ordinal == 1 else "2026-07-25T10:00:00Z"),
            "submitted_at": f"2026-07-25T1{ordinal}:00:00Z",
            "corpus_content_id": corpus["corpus_content_id"],
            "protocol_sha256": protocol_sha256,
            "provider_outputs_hidden": True,
            "machine_proposals_hidden": True,
            "other_review_hidden": True,
            "case_reviews": case_reviews,
            "case_reviews_sha256": _digest(case_reviews),
        }
        review["content_sha256"] = _digest(review)
        reviews.append(review)
    final["adjudication"]["blind_reviews"] = reviews
    final["adjudication"]["resolution"] = {
        "method": "exact_agreement",
        "resolver_id": None,
        "resolver_kind": None,
        "resolved_at": "2026-07-25T13:00:00Z",
        "input_review_ids": [review["review_id"] for review in reviews],
        "input_review_sha256s": [review["content_sha256"] for review in reviews],
        "resolved_cases_sha256": _digest(final["cases"]),
        "disagreements": [],
        "excluded_case_ids": [],
    }
    resolution = final["adjudication"]["resolution"]
    resolution["content_sha256"] = _digest(resolution)
    final["metadata"]["oracle_content_sha256"] = _oracle_digest(final)
    return final


def test_v2_bundle_is_gold_free_and_paid_run_is_fail_closed() -> None:
    corpus, oracle, payload = _bundle()

    assert corpus["corpus_content_id"] == ("ad39e0c2a96cd5c89b9727163e9494882cf476046c84953ab772513a84bcff36")
    assert len(corpus["cases"]) == 12
    serialized_payload = canonical_json(payload)
    for forbidden in (
        "expected_outputs",
        "candidate_id",
        "accepted_evidence",
        "blind_reviews",
        "resolution",
        "explicit_denial",
        "affirmed_control",
        "temporal_control",
        "unrelated_control",
    ):
        assert forbidden not in serialized_payload

    eligibility = evaluate_run_eligibility(
        corpus,
        oracle,
        protocol_sha256=_protocol_sha256(),
    )
    assert eligibility["eligible"] is False
    assert eligibility["publication_eligible"] is False
    assert eligibility["failures"] == [
        "oracle is not final-human-adjudicated",
        "oracle has no frozen_at instant",
        "expected 2 blind human reviews; found 0",
        "human resolution is missing",
    ]


def test_v2_schema_has_exact_case_slots_and_no_model_generated_ids() -> None:
    _, oracle, payload = _bundle()
    schema = build_response_schema(payload)
    response = _perfect_required_response(oracle)

    validate_response_schema(response, schema)
    assert set(schema["properties"]["cases"]["required"]) == set(response["cases"])
    serialized_schema = canonical_json(schema)
    assert "subject_iri" not in serialized_schema
    assert "predicate_iri" not in serialized_schema
    assert "object_iri" not in serialized_schema
    assert "modality" not in serialized_schema
    assert "attributed_source" in serialized_schema
    assert "attributed_actor" not in serialized_schema
    assert "strongest opposite reading" in INSTRUCTIONS

    missing = copy.deepcopy(response)
    missing["cases"].pop("relx-v2-d2ed304cb4e2256b")
    with pytest.raises(
        RelationEvaluationError,
        match="provider response violated its schema",
    ):
        validate_response_schema(missing, schema)


def test_v2_perfect_required_projection_scores_dimensions_separately() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    validate_response_schema(response, build_response_schema(payload))

    normalized = normalize_candidates(response, payload)
    scores = score_candidates(oracle, normalized)

    assert scores["oracle_status"] == "provisional-machine-assisted"
    assert scores["publication_eligible"] is False
    assert scores["core_semantics"] == {
        "true_positives": 10,
        "false_positives": 0,
        "false_negatives": 0,
        "precision": 1.0,
        "recall": 1.0,
        "f1": 1.0,
    }
    assert all(dimension["accuracy"] == 1.0 for dimension in scores["dimensions"].values())
    assert scores["evidence"] == {"preferred_exact": 10}
    assert scores["evidence_entailment"] == {"accepted_sufficient": 10}
    assert scores["evidence_entailment_rate"] == 1.0
    assert scores["provided_offset_exact_rate"] == 1.0
    assert scores["offset_repair_rate"] == 0.0
    assert scores["unrelated_false_target_candidates"] == 0
    assert scores["unsupported_false_target_candidates"] == 0
    assert scores["false_current_discrepancies"] == 0


def test_v2_accepts_multiple_boundaries_without_changing_semantic_score() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    mining_oracle = next(case for case in oracle["cases"] if case["case_id"] == "relx-v2-67a08387711d35c7")
    accepted = next(
        option
        for option in mining_oracle["expected_outputs"][0]["accepted_evidence"]
        if option["boundary_preference"] == "accepted"
    )
    response["cases"]["relx-v2-67a08387711d35c7"]["items"][0] = _provider_item(
        mining_oracle["expected_outputs"][0], accepted
    )

    normalized = normalize_candidates(response, payload)
    scores = score_candidates(oracle, normalized)

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["evidence"] == {
        "accepted_exact": 1,
        "preferred_exact": 9,
    }


def test_v2_allowed_candidate_is_neutral_when_required_candidate_is_missing() -> None:
    _, oracle, payload = _bundle()
    case_id = "relx-v2-71395ccd8ec8d8d0"
    fcc_oracle = next(case for case in oracle["cases"] if case["case_id"] == case_id)
    required_missing = _perfect_required_response(oracle)
    required_missing["cases"][case_id] = {
        "items": [],
        "no_answer_rationale": "No required target assertion returned.",
    }
    allowed_only = copy.deepcopy(required_missing)
    allowed_only["cases"][case_id] = {
        "items": [_provider_item(fcc_oracle["expected_outputs"][1])],
        "no_answer_rationale": None,
    }

    missing_scores = score_candidates(
        oracle,
        normalize_candidates(required_missing, payload),
    )
    allowed_scores = score_candidates(
        oracle,
        normalize_candidates(allowed_only, payload),
    )

    assert missing_scores["core_semantics"] == (allowed_scores["core_semantics"])
    assert allowed_scores["core_semantics"]["true_positives"] == 9
    assert allowed_scores["core_semantics"]["false_negatives"] == 1
    assert allowed_scores["allowed_match_count"] == 1

    article_variant = copy.deepcopy(allowed_only)
    article_variant["cases"][case_id]["items"][0]["attribution"]["claimant_text"] = "the FCC"
    article_scores = score_candidates(
        oracle,
        normalize_candidates(article_variant, payload),
    )
    assert article_scores["allowed_match_count"] == 1
    assert article_scores["core_semantics"]["false_positives"] == 0

    evidence_variant = copy.deepcopy(article_variant)
    evidence_variant["cases"][case_id]["items"][0].update(
        {
            "evidence_text": "the FCC would think that our local ABC station does not provide a service to our communities.",
            "evidence_start": 33,
            "evidence_end": 126,
        }
    )
    evidence_scores = score_candidates(
        oracle,
        normalize_candidates(evidence_variant, payload),
    )
    assert evidence_scores["allowed_match_count"] == 1
    assert evidence_scores["core_semantics"]["false_positives"] == 0
    assert evidence_scores["evidence_entailment"]["unadjudicated"] == 1

    wrong_optional = copy.deepcopy(allowed_only)
    wrong_optional["cases"][case_id]["items"][0]["attribution"]["claimant_text"] = "an unrelated actor"
    wrong_scores = score_candidates(
        oracle,
        normalize_candidates(wrong_optional, payload),
    )
    assert wrong_scores["allowed_match_count"] == 0
    assert wrong_scores["core_semantics"]["false_positives"] == 1


def test_v2_allowed_same_core_variant_cannot_satisfy_required_output() -> None:
    _, oracle, payload = _bundle()
    case_id = "relx-v2-973cdad68a5c36f0"
    modified_oracle = copy.deepcopy(oracle)
    case = next(item for item in modified_oracle["cases"] if item["case_id"] == case_id)
    required = case["expected_outputs"][0]
    allowed = copy.deepcopy(required)
    allowed["candidate_id"] = "urn:spicy-regs:evaluation:v2:gao-airworthiness:source-voice-allowed"
    allowed["requirement"] = "allowed"
    allowed["attribution"] = {
        "status": "source_voice",
        "claimant_text": None,
    }
    case["expected_outputs"].append(allowed)
    response = _perfect_required_response(modified_oracle)
    response["cases"][case_id]["items"] = [_provider_item(allowed)]

    scores = score_candidates(
        modified_oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["true_positives"] == 9
    assert scores["core_semantics"]["false_positives"] == 0
    assert scores["core_semantics"]["false_negatives"] == 1
    assert scores["allowed_match_count"] == 1


def test_v2_separates_accepted_boundary_from_insufficient_entailment() -> None:
    corpus, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    case_id = "relx-v2-67a08387711d35c7"
    excerpt = next(case["source"]["document_excerpt"] for case in corpus["cases"] if case["case_id"] == case_id)
    quote = "The standards of this section do not apply"
    start = excerpt.index(quote)
    response["cases"][case_id]["items"][0].update(
        {
            "evidence_text": quote,
            "evidence_start": start,
            "evidence_end": start + len(quote),
        }
    )

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["evidence"]["accepted_exact"] == 1
    assert scores["evidence"]["preferred_exact"] == 9
    assert scores["evidence_entailment"] == {
        "adjudicated_insufficient": 1,
        "accepted_sufficient": 9,
    }
    assert scores["evidence_entailment_rate"] == 0.9


def test_v2_accepts_terminal_punctuation_boundary_equivalence() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    case_id = "relx-v2-973cdad68a5c36f0"
    item = response["cases"][case_id]["items"][0]
    item["evidence_text"] = item["evidence_text"].removesuffix(".")
    item["evidence_end"] -= 1

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["evidence"] == {
        "preferred_boundary_equivalent": 1,
        "preferred_exact": 9,
    }
    assert scores["evidence_entailment"] == {"accepted_sufficient": 10}


def test_v2_does_not_inherit_entailment_for_unreviewed_enclosing_span() -> None:
    corpus, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    case_id = "relx-v2-67a08387711d35c7"
    excerpt = next(case["source"]["document_excerpt"] for case in corpus["cases"] if case["case_id"] == case_id)
    response["cases"][case_id]["items"][0].update(
        {
            "evidence_text": excerpt,
            "evidence_start": 0,
            "evidence_end": len(excerpt),
        }
    )

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["evidence"]["unadjudicated_enclosing"] == 1
    assert scores["evidence_entailment"] == {
        "accepted_sufficient": 9,
        "unadjudicated": 1,
    }


def test_v2_scores_temporal_source_wording_separately() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    response["cases"]["relx-v2-7c90b40744e9f83d"]["items"][1]["temporal_scope"]["raw_text"] = (
        "an unrelated temporal phrase"
    )

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["dimensions"]["temporal_raw_text"] == {
        "correct": 0,
        "total": 1,
        "accuracy": 0.0,
    }


def test_v2_tracks_offset_repair_and_false_current_claim_independently() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    case_id = "relx-v2-973cdad68a5c36f0"
    item = response["cases"][case_id]["items"][0]
    item["evidence_start"] += 1
    item["evidence_end"] += 1
    item["temporal_scope"]["reference"] = "evaluation_time"

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["provided_offset_exact_rate"] == 0.9
    assert scores["offset_repair_rate"] == 0.1
    assert scores["dimensions"]["temporal_reference"]["accuracy"] == 0.9
    assert scores["false_current_discrepancies"] == 1


def test_v2_scores_claimant_and_condition_text_not_only_status() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    response["cases"]["relx-v2-973cdad68a5c36f0"]["items"][0]["attribution"]["claimant_text"] = "an unrelated actor"
    response["cases"]["relx-v2-4a1643ce48378c05"]["items"][0]["conditionality"]["condition_text"] = (
        "an unrelated condition"
    )

    scores = score_candidates(
        oracle,
        normalize_candidates(response, payload),
    )

    assert scores["core_semantics"]["f1"] == 1.0
    assert scores["dimensions"]["attribution"]["accuracy"] == 1.0
    assert scores["dimensions"]["attribution_claimant"]["accuracy"] == 0.0
    assert scores["dimensions"]["conditionality"]["accuracy"] == 1.0
    assert scores["dimensions"]["condition_text"]["accuracy"] == 0.666667


def test_v2_provisional_oracle_treats_cfr_where_clause_as_explicit_scope() -> None:
    _, oracle, _ = _bundle()
    case = next(item for item in oracle["cases"] if item["case_id"] == "relx-v2-67a08387711d35c7")

    assert case["expected_outputs"][0]["conditionality"] == {
        "status": "explicit",
        "condition_text": (
            "where mining is done on a flat or gently rolling terrain with an occasional steep slope "
            "through which the mining proceeds and leaves a plain or predominantly flat area"
        ),
    }


def test_v2_derives_current_only_from_proven_temporal_scope() -> None:
    evaluation_time = "2026-07-25T12:00:00Z"

    assert (
        derive_current_at_evaluation(
            {
                "relation_to_reference": "includes",
                "reference": "document_time",
                "start": None,
                "end": None,
                "raw_text": "is in effect",
            },
            evaluation_time,
        )
        == "unknown"
    )
    assert (
        derive_current_at_evaluation(
            {
                "relation_to_reference": "includes",
                "reference": "explicit_time",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-12-31T23:59:59Z",
                "raw_text": None,
            },
            evaluation_time,
        )
        == "current"
    )
    assert (
        derive_current_at_evaluation(
            {
                "relation_to_reference": "includes",
                "reference": "explicit_time",
                "start": "2025-01-01T00:00:00Z",
                "end": "2025-12-31T23:59:59Z",
                "raw_text": None,
            },
            evaluation_time,
        )
        == "not_current"
    )
    assert (
        derive_current_at_evaluation(
            {
                "relation_to_reference": "before",
                "reference": "explicit_time",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-12-31T23:59:59Z",
                "raw_text": None,
            },
            evaluation_time,
        )
        == "unknown"
    )


def test_v2_keeps_proposed_removal_as_event_and_rejects_invalid_hybrid() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    wioa_items = response["cases"]["relx-v2-7c90b40744e9f83d"]["items"]

    assert [item["kind"] for item in wioa_items] == [
        "relation_assertion",
        "relation_change_event",
    ]
    assert wioa_items[1]["polarity"] is None
    assert response["cases"]["relx-v2-18e15c9465ef5a82"]["items"] == []

    invalid = copy.deepcopy(response)
    invalid_item = invalid["cases"]["relx-v2-7c90b40744e9f83d"]["items"][0]
    invalid_item["operation"] = "remove"
    invalid_item["stage"] = "proposed"
    invalid_item["intended_effective_scope"] = {
        "relation_to_reference": "after",
        "reference": "document_time",
        "start": None,
        "end": None,
        "raw_text": None,
    }
    validate_response_schema(invalid, build_response_schema(payload))
    normalized = normalize_candidates(invalid, payload)
    normalized_wioa = next(case for case in normalized["cases"] if case["case_id"] == "relx-v2-7c90b40744e9f83d")
    assert normalized_wioa["rejections"][0]["reason"] == ("invalid-candidate-semantics")


def test_v2_eligibility_requires_two_distinct_sealed_human_reviews() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    final = _eligible_final_oracle(corpus, oracle, protocol_sha256)

    assert evaluate_run_eligibility(
        corpus,
        final,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    ) == {
        "eligible": True,
        "experiment_status": "provisional-v2-contract",
        "publication_eligible": False,
        "required_blind_human_reviews": 2,
        "oracle_content_sha256": final["metadata"]["oracle_content_sha256"],
        "resolution_content_sha256": final["adjudication"]["resolution"]["content_sha256"],
        "failures": [],
    }

    final["adjudication"]["blind_reviews"][1]["reviewer_id"] = final["adjudication"]["blind_reviews"][0]["reviewer_id"]
    eligibility = evaluate_run_eligibility(
        corpus,
        final,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )
    assert eligibility["eligible"] is False
    assert "blind human reviewers must have distinct nonempty identities" in eligibility["failures"]


def test_v2_eligibility_rejects_digest_valid_but_empty_reviews() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    hollow = copy.deepcopy(oracle)
    hollow["metadata"]["oracle_status"] = "final-human-adjudicated"
    hollow["metadata"]["frozen_at"] = "2026-07-25T14:00:00Z"
    reviews = []
    for ordinal in (1, 2):
        review = {
            "review_id": f"urn:spicy-regs:review:hollow-{ordinal}",
            "reviewer_id": f"urn:spicy-regs:person:hollow-{ordinal}",
            "reviewer_kind": "human",
            "started_at": "2026-07-25T09:00:00Z",
            "submitted_at": "2026-07-25T10:00:00Z",
            "corpus_content_id": corpus["corpus_content_id"],
            "protocol_sha256": protocol_sha256,
            "provider_outputs_hidden": True,
            "machine_proposals_hidden": True,
            "other_review_hidden": True,
            "case_reviews": [],
            "case_reviews_sha256": _digest([]),
        }
        review["content_sha256"] = _digest(review)
        reviews.append(review)
    hollow["adjudication"]["blind_reviews"] = reviews
    hollow["adjudication"]["resolution"] = {
        "method": "exact_agreement",
        "resolver_id": None,
        "resolver_kind": None,
        "resolved_at": "2026-07-25T11:00:00Z",
        "input_review_ids": [review["review_id"] for review in reviews],
        "input_review_sha256s": [review["content_sha256"] for review in reviews],
        "resolved_cases_sha256": _digest(hollow["cases"]),
        "disagreements": [],
    }

    eligibility = evaluate_run_eligibility(
        corpus,
        hollow,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert any("case set differs from the locked corpus" in failure for failure in eligibility["failures"])


def test_v2_eligibility_rejects_incomplete_per_case_review() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    incomplete = _eligible_final_oracle(
        corpus,
        oracle,
        protocol_sha256,
    )
    review = incomplete["adjudication"]["blind_reviews"][0]
    review["case_reviews"][0]["rationale"] = " "
    review["case_reviews_sha256"] = _digest(review["case_reviews"])
    review["content_sha256"] = _digest({key: value for key, value in review.items() if key != "content_sha256"})
    incomplete["adjudication"]["resolution"]["input_review_sha256s"][0] = review["content_sha256"]

    eligibility = evaluate_run_eligibility(
        corpus,
        incomplete,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert any("rationale must be a nonempty string" in failure for failure in eligibility["failures"])


def test_v2_blind_review_cannot_contain_oracle_role() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    leaked = _eligible_final_oracle(corpus, oracle, protocol_sha256)
    review = leaked["adjudication"]["blind_reviews"][0]
    review["case_reviews"][0]["decision"]["role"] = "direct_denial"
    review["case_reviews_sha256"] = _digest(review["case_reviews"])
    review["content_sha256"] = _digest({key: value for key, value in review.items() if key != "content_sha256"})

    eligibility = evaluate_run_eligibility(
        corpus,
        leaked,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert any("decision contains oracle-only fields" in failure for failure in eligibility["failures"])


def test_v2_eligibility_rejects_future_dated_audit_records() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    final = _eligible_final_oracle(corpus, oracle, protocol_sha256)

    eligibility = evaluate_run_eligibility(
        corpus,
        final,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 25, 8, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert "oracle freeze is after the gate evaluation" in eligibility["failures"]
    assert "blind review 1 started after the gate evaluation" in eligibility["failures"]
    assert "blind review 2 was submitted after the gate evaluation" in (eligibility["failures"])
    assert "resolution is after the gate evaluation" in eligibility["failures"]


def test_v2_eligibility_rejects_vacuous_ambiguous_reviews() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    final = _eligible_final_oracle(corpus, oracle, protocol_sha256)
    case_id = final["cases"][0]["case_id"]
    final["cases"][0]["expected_outputs"] = []
    for review in final["adjudication"]["blind_reviews"]:
        case_review = review["case_reviews"][0]
        case_review["case_status"] = "ambiguous"
        case_review["decision"]["expected_outputs"] = []
        case_review["rationale"] = "Two readings were claimed but not recorded."
        review["case_reviews_sha256"] = _digest(review["case_reviews"])
        review["content_sha256"] = _digest({key: value for key, value in review.items() if key != "content_sha256"})
    resolution = final["adjudication"]["resolution"]
    resolution["input_review_sha256s"] = [review["content_sha256"] for review in final["adjudication"]["blind_reviews"]]
    resolution["resolved_cases_sha256"] = _digest(final["cases"])
    resolution["excluded_case_ids"] = [case_id]
    resolution.pop("content_sha256")
    resolution["content_sha256"] = _digest(resolution)
    final["metadata"]["oracle_content_sha256"] = _oracle_digest(final)

    eligibility = evaluate_run_eligibility(
        corpus,
        final,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert any("ambiguous but has fewer than two candidate readings" in failure for failure in eligibility["failures"])


def test_v2_scorer_excludes_resolved_unresolved_cases() -> None:
    _, oracle, payload = _bundle()
    response = _perfect_required_response(oracle)
    excluded_case_id = "relx-v2-973cdad68a5c36f0"
    final = copy.deepcopy(oracle)
    final["metadata"]["oracle_status"] = "final-human-adjudicated"
    final["adjudication"]["resolution"] = {"excluded_case_ids": [excluded_case_id]}

    scores = score_candidates(
        final,
        normalize_candidates(response, payload),
    )

    assert scores["excluded_case_count"] == 1
    assert scores["excluded_case_ids"] == [excluded_case_id]
    assert scores["excluded_raw_candidate_count"] == 1
    assert scores["core_semantics"]["true_positives"] == 9
    assert scores["core_semantics"]["false_positives"] == 0
    assert scores["core_semantics"]["false_negatives"] == 0


def test_v2_eligibility_rejects_tampered_final_oracle_digest() -> None:
    corpus, oracle, _ = _bundle()
    protocol_sha256 = _protocol_sha256()
    final = _eligible_final_oracle(corpus, oracle, protocol_sha256)
    final["cases"][0]["expected_outputs"][0]["rationale"] = "Tampered after the final digest was recorded."

    eligibility = evaluate_run_eligibility(
        corpus,
        final,
        protocol_sha256=protocol_sha256,
        gate_time=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert eligibility["eligible"] is False
    assert "final oracle content digest does not match" in eligibility["failures"]

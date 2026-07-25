"""Contracts for the v3 extraction step and its relation v2 task.

The runtime primitives (work IDs, checkpoints, receipts, validation, rebuild)
are proved in ``test_docpipeline_runtime*.py``. This file proves what the
extraction step itself owns: a gold-free model payload, one strict output
schema, response checks that reject instead of inventing negative facts, exact
evidence rules, abstention, the human-review gate, and provider-free recompute.

Migration parity against the v2 runner lives in
``test_docpipeline_relation_v2_migration.py``.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters import StructuredTextCallError, StructuredTextResult
from spicy_regs.docpipeline.extraction import (
    ExtractionError,
    ExtractionUnit,
    ModelInputLeakError,
    ResponseCheckError,
    answer_key_files_in_run,
    extraction_plan_facts,
    plan_extraction_items,
    rebuild_extraction,
    recompute_extraction,
    run_extraction,
)
from spicy_regs.docpipeline.relation_task import (
    EVIDENCE_ALIGNMENT_PROVIDED,
    EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    MODEL_INPUT_FORBIDDEN_KEYS,
    RelationV2Task,
    build_model_payload,
    build_response_schema,
    check_response_schema,
    derive_current_at_evaluation,
    load_answers,
    load_corpus,
    normalize_candidates,
    resolve_exact_evidence_offsets,
    units_from_corpus,
)
from spicy_regs.docpipeline.runtime import RunDirectoryError, RunPlan, rebuild_run, validate_run
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
EVIDENCE_DIR = Path(__file__).parents[1] / "docs" / "evidence" / "relation-exclusion-openai-v2-focused-five-2026-07-25"

TASK = RelationV2Task()


# --- fixture-free corpus construction --------------------------------------


def _case(case_id: str, excerpt: str, *, subject: str = "Alpha Rule") -> dict[str, Any]:
    return {
        "case_id": case_id,
        "source": {
            "source_type": "test_document",
            "artifact_id": f"artifact:{case_id}",
            "artifact_version_id": f"artifact:{case_id}:v1",
            "primary_source_url": f"https://example.invalid/{case_id}",
            "source_field": "document_excerpt",
            "artifact_time": {"kind": "published", "value": "2026-01-05T00:00:00Z"},
            "excerpt_sha256": hashlib.sha256(excerpt.encode()).hexdigest(),
            "document_excerpt": excerpt,
            "untrusted_metadata": "",
        },
        "target_relation": {
            "subject": {"id": "urn:test:subject:alpha", "label": subject},
            "predicate": {"id": "urn:test:predicate:supersedes", "label": "supersedes"},
            "object": {"id": "urn:test:object:beta", "label": "Beta Rule"},
        },
    }


def _corpus(*cases: dict[str, Any], dataset_id: str = "docpipeline-extraction-test") -> dict[str, Any]:
    return {
        "format_version": 2,
        "dataset_id": dataset_id,
        "metadata": {
            "purpose": "Extraction-step contract test corpus",
            "evaluation_time": "2026-07-24T20:00:00Z",
            "offset_unit": "unicode_codepoint",
            "omission_analysis": "disabled",
            "publication_eligible": False,
        },
        "cases": list(cases),
    }


def _write_corpus(path: Path, corpus: dict[str, Any]) -> Path:
    path.write_text(json.dumps(corpus, indent=2), encoding="utf-8")
    return path


def _assertion_item(excerpt: str, quote: str, **overrides: Any) -> dict[str, Any]:
    start = excerpt.index(quote)
    item: dict[str, Any] = {
        "kind": "relation_assertion",
        "polarity": "denied",
        "operation": None,
        "stage": None,
        "temporal_scope": {
            "relation_to_reference": "includes",
            "reference": "document_time",
            "start": None,
            "end": None,
            "raw_text": None,
        },
        "intended_effective_scope": None,
        "attribution": {"status": "source_voice", "claimant_text": None},
        "conditionality": {"status": "not_explicit", "condition_text": None},
        "evidence_text": quote,
        "evidence_start": start,
        "evidence_end": start + len(quote),
        "rationale": "The span denies the target relation in the source voice.",
    }
    item.update(overrides)
    return item


def _response(**cases: dict[str, Any]) -> dict[str, Any]:
    return {"cases": dict(cases)}


def _case_answer(items: list[dict[str, Any]], rationale: str | None = None) -> dict[str, Any]:
    return {
        "items": items,
        "no_answer_rationale": rationale if items == [] else None,
    }


# --- fake structured-text model --------------------------------------------


class _ReplayModel:
    """A ``StructuredTextModel`` that replays canned JSON and counts calls."""

    model_id = "fake:replay"

    def __init__(
        self,
        response: dict[str, Any] | list[dict[str, Any]],
        *,
        fail_units: tuple[str, ...] = (),
    ) -> None:
        self._responses = response if isinstance(response, list) else [response]
        self._fail_units = fail_units
        self.calls: list[dict[str, Any]] = []
        self.instructions: list[str] = []
        self.schemas: list[dict[str, Any]] = []

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Any,
        instructions: str,
        payload: Any,
        max_output_tokens: int,
    ) -> dict[str, Any]:
        return {
            "model": self.model_id,
            "schema_name": name,
            "instructions": instructions,
            "input": canonical_json(payload),
            "max_output_tokens": max_output_tokens,
            "schema_sha256": hashlib.sha256(canonical_json(dict(schema)).encode()).hexdigest(),
        }

    def structured_json(
        self,
        *,
        name: str,
        schema: Any,
        instructions: str,
        payload: Any,
        max_output_tokens: int,
    ) -> StructuredTextResult:
        index = len(self.calls)
        self.calls.append(json.loads(canonical_json(payload)))
        self.instructions.append(instructions)
        self.schemas.append(json.loads(canonical_json(dict(schema))))
        wanted = {str(case["case_id"]) for case in payload["cases"]}
        case_ids = tuple(sorted(wanted))
        if any(case_id in self._fail_units for case_id in case_ids):
            raise StructuredTextCallError(
                "provider said BRITTLE-PROVIDER-TEXT and stopped",
                call={
                    "provider": "fake",
                    "transport": "replay",
                    "model_id": self.model_id,
                    "schema_name": name,
                    "status": "provider_error",
                    "attempt_count": 1,
                    "retry_count": 0,
                    "duration_ms": 1.0,
                },
            )
        # Replay by requested case set, not by call order, so a resumed run
        # still receives the reply that belongs to the unit it retries.
        response = next(
            (candidate for candidate in self._responses if set(candidate["cases"]) == wanted),
            self._responses[min(index, len(self._responses) - 1)],
        )
        return StructuredTextResult(
            output=copy.deepcopy(response),
            call={
                "provider": "fake",
                "transport": "replay",
                "model_id": self.model_id,
                "schema_name": name,
                "response_id": f"fake-{index}",
                "response_model": self.model_id,
                "status": "completed",
                "duration_ms": 2.0,
                "input_tokens": 11,
                "output_tokens": 7,
                "total_tokens": 18,
                "attempt_count": 1,
                "retry_count": 0,
                "attempts": [{"attempt": 1, "status": "completed"}],
                "prompt_sha256": hashlib.sha256(canonical_json(payload).encode()).hexdigest(),
                "request_sha256": hashlib.sha256(canonical_json(dict(schema)).encode()).hexdigest(),
                "max_output_tokens": max_output_tokens,
                "schema_validated_locally": True,
            },
        )


class _ForbiddenModel:
    """A model no provider-free path may ever reach."""

    model_id = "fake:forbidden"

    def secret_free_request(self, **kwargs: Any) -> dict[str, Any]:
        return {"model": self.model_id}

    def structured_json(self, **kwargs: Any) -> StructuredTextResult:
        raise AssertionError("a provider-free path called a provider")


# --- plans ------------------------------------------------------------------


def _plan(
    units: tuple[ExtractionUnit, ...],
    *,
    mode: str = "diagnostic",
    answers: Any = None,
    review_digests: dict[str, str] | None = None,
    run_id: str = "docpipeline-extract-0001",
) -> RunPlan:
    return RunPlan(
        run_id=run_id,
        mode=mode,
        steps=("extract",),
        source_snapshot={"dataset_id": units[0].input["dataset_id"]},
        extraction=extraction_plan_facts(TASK, units, answers=answers),
        provider={"model_id": "fake:replay", "store": False},
        review_file_digests=review_digests or {},
        code_commit="0" * 40,
        required_work=("extract",),
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _focused_bundle(tmp_path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return the five-case corpus, its oracle, and the canned provider reply.

    The stored evidence run predates one enum rename, so the canned reply is
    adapted once and shared by every path that replays it.
    """
    payload = json.loads((EVIDENCE_DIR / "payload.json").read_text(encoding="utf-8"))
    wanted = {str(case["case_id"]) for case in payload["cases"]}
    raw_corpus = json.loads(CORPUS_FIXTURE.read_text(encoding="utf-8"))
    subset = {**raw_corpus, "cases": [case for case in raw_corpus["cases"] if case["case_id"] in wanted]}
    corpus_path = _write_corpus(tmp_path / "focused-corpus.json", subset)
    corpus = load_corpus(corpus_path)

    raw_oracle = json.loads(ORACLE_FIXTURE.read_text(encoding="utf-8"))
    oracle_subset = {
        **raw_oracle,
        "corpus_content_id": corpus["corpus_content_id"],
        "cases": [case for case in raw_oracle["cases"] if case["case_id"] in wanted],
    }
    oracle_path = tmp_path / "focused-oracle.json"
    oracle_path.write_text(json.dumps(oracle_subset, indent=2), encoding="utf-8")
    answers = load_answers(oracle_path, corpus)

    response = json.loads(
        (EVIDENCE_DIR / "response.json").read_text(encoding="utf-8").replace("attributed_actor", "attributed_source")
    )
    return corpus, answers, response


# --- gold-free model input --------------------------------------------------


def test_the_model_payload_carries_no_hidden_test_field(tmp_path: Path) -> None:
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", "Alpha does not supersede Beta.")))
    )

    payload = build_model_payload(corpus)

    serialized = canonical_json(payload)
    for forbidden in MODEL_INPUT_FORBIDDEN_KEYS:
        assert f'"{forbidden}"' not in serialized
    assert "corpus_content_id" not in payload, "the payload carries only what the model must read"


def test_a_tampered_corpus_file_never_reaches_the_model(tmp_path: Path) -> None:
    corpus = _corpus(_case("c1", "Alpha does not supersede Beta."))
    corpus["cases"][0]["expected_outputs"] = [{"requirement": "required"}]
    path = _write_corpus(tmp_path / "tampered-corpus.json", corpus)

    with pytest.raises(ExtractionError):
        load_corpus(path)


def test_a_leaked_answer_key_below_the_schema_still_fails_the_payload() -> None:
    leaked = _corpus(_case("c1", "Alpha does not supersede Beta."))
    leaked["cases"][0]["source"]["untrusted_metadata"] = "safe"
    leaked["cases"][0]["target_relation"]["subject"]["accepted_evidence"] = ["leaked"]

    with pytest.raises(ModelInputLeakError):
        build_model_payload(leaked)


# --- strict schema ----------------------------------------------------------


def test_the_response_schema_names_one_slot_per_case_and_no_model_generated_ids(tmp_path: Path) -> None:
    corpus = load_corpus(
        _write_corpus(
            tmp_path / "corpus.json",
            _corpus(_case("c1", "Alpha does not supersede Beta."), _case("c2", "Alpha supersedes Beta.")),
        )
    )
    schema = build_response_schema(build_model_payload(corpus))

    assert schema["properties"]["cases"]["required"] == ["c1", "c2"]
    assert schema["properties"]["cases"]["additionalProperties"] is False
    item = schema["properties"]["cases"]["properties"]["c1"]["properties"]["items"]["items"]
    assert item["additionalProperties"] is False
    assert "candidate_id" not in item["properties"]
    assert "target_relation" not in item["properties"]


def test_a_response_that_leaves_the_schema_is_rejected(tmp_path: Path) -> None:
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", "Alpha does not supersede Beta.")))
    )
    payload = build_model_payload(corpus)
    schema = build_response_schema(payload)
    excerpt = corpus["cases"][0]["source"]["document_excerpt"]
    item = _assertion_item(excerpt, "Alpha does not supersede Beta.")
    item["confidence"] = 0.9

    with pytest.raises(ResponseCheckError):
        check_response_schema(_response(c1=_case_answer([item])), schema)


def test_a_schema_invalid_response_fails_the_unit_instead_of_denying_the_source(tmp_path: Path) -> None:
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", "Alpha does not supersede Beta.")))
    )
    units = units_from_corpus(corpus)
    model = _ReplayModel({"cases": {"c1": {"items": [{"kind": "nonsense"}], "no_answer_rationale": None}}})

    result = run_extraction(_plan(units), tmp_path / "run", task=TASK, model=model, units=units)

    assert result.outcome.final_state == "fail"
    assert result.outcome.receipt["counts"]["rejected"] == 1, "a checked-and-rejected response is a settled rejection"
    assert result.outcome.receipt["counts"]["empty"] == 0, "a failed check is not a negative fact about the source"


# --- exact evidence ---------------------------------------------------------


def test_correct_provided_offsets_are_kept() -> None:
    resolution = resolve_exact_evidence_offsets("Alpha does not supersede Beta.", "does not supersede", 6, 24)

    assert resolution is not None
    assert (resolution.start, resolution.end, resolution.method) == (6, 24, EVIDENCE_ALIGNMENT_PROVIDED)


def test_a_unique_exact_quote_repairs_wrong_offsets() -> None:
    resolution = resolve_exact_evidence_offsets("Alpha does not supersede Beta.", "does not supersede", 0, 5)

    assert resolution is not None
    assert (resolution.start, resolution.end, resolution.method) == (6, 24, EVIDENCE_ALIGNMENT_UNIQUE_EXACT)


def test_an_ambiguous_quote_is_never_repaired() -> None:
    assert resolve_exact_evidence_offsets("no. no.", "no.", 0, 5) is None


def test_an_absent_quote_is_never_repaired() -> None:
    assert resolve_exact_evidence_offsets("Alpha does not supersede Beta.", "Gamma", 0, 5) is None


def test_offset_repair_and_ambiguity_show_up_in_the_candidate_record(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta. Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    kept = _assertion_item(excerpt, "Alpha")
    ambiguous = _assertion_item(excerpt, "Alpha does not supersede Beta.")
    ambiguous["evidence_start"] = 5
    ambiguous["evidence_end"] = 10
    ambiguous["polarity"] = "affirmed"
    repaired = _assertion_item(excerpt, "supersede Beta. Alpha")
    repaired["evidence_start"] = 0
    repaired["evidence_end"] = 4
    repaired["attribution"] = {"status": "unclear", "claimant_text": None}

    normalized = normalize_candidates(_response(c1=_case_answer([kept, ambiguous, repaired])), payload)

    case = normalized["cases"][0]
    assert [rejection["reason"] for rejection in case["rejections"]] == ["evidence-not-exact"]
    assert [candidate["evidence_alignment"] for candidate in case["candidates"]] == [
        EVIDENCE_ALIGNMENT_PROVIDED,
        EVIDENCE_ALIGNMENT_UNIQUE_EXACT,
    ]
    assert case["candidates"][0]["evidence_start"] == 0
    assert case["candidates"][1]["evidence_start"] == excerpt.index("supersede Beta. Alpha")


# --- response checks --------------------------------------------------------


def test_a_temporal_scope_with_bounds_outside_explicit_time_is_rejected(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    item = _assertion_item(excerpt, excerpt)
    item["temporal_scope"] = {
        "relation_to_reference": "includes",
        "reference": "document_time",
        "start": "2026-01-01T00:00:00Z",
        "end": None,
        "raw_text": None,
    }

    normalized = normalize_candidates(_response(c1=_case_answer([item])), payload)

    assert normalized["cases"][0]["candidates"] == []
    rejection = normalized["cases"][0]["rejections"][0]
    assert rejection["reason"] == "invalid-candidate-semantics"
    assert "explicit_time" in rejection["detail"]


def test_an_attributed_claim_without_a_claimant_is_rejected(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    item = _assertion_item(excerpt, excerpt, attribution={"status": "attributed_source", "claimant_text": None})

    normalized = normalize_candidates(_response(c1=_case_answer([item])), payload)

    assert normalized["cases"][0]["candidates"] == []
    assert normalized["cases"][0]["rejections"][0]["reason"] == "invalid-candidate-semantics"


def test_a_condition_text_without_an_explicit_condition_is_rejected(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    item = _assertion_item(
        excerpt,
        excerpt,
        conditionality={"status": "not_explicit", "condition_text": "unless waived"},
    )

    normalized = normalize_candidates(_response(c1=_case_answer([item])), payload)

    assert normalized["cases"][0]["candidates"] == []
    assert normalized["cases"][0]["rejections"][0]["reason"] == "invalid-candidate-semantics"


def test_an_assertion_carrying_change_event_fields_is_rejected(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    item = _assertion_item(excerpt, excerpt, operation="remove", stage="proposed")

    normalized = normalize_candidates(_response(c1=_case_answer([item])), payload)

    assert normalized["cases"][0]["candidates"] == []
    assert normalized["cases"][0]["rejections"][0]["reason"] == "invalid-candidate-semantics"


def test_current_applicability_is_derived_only_from_proven_temporal_facts() -> None:
    evaluation_time = "2026-07-24T20:00:00Z"
    scope = {
        "relation_to_reference": "includes",
        "reference": "evaluation_time",
        "start": None,
        "end": None,
        "raw_text": None,
    }
    assert derive_current_at_evaluation(scope, evaluation_time) == "current"
    assert derive_current_at_evaluation({**scope, "relation_to_reference": "before"}, evaluation_time) == "not_current"
    assert derive_current_at_evaluation({**scope, "reference": "document_time"}, evaluation_time) == "unknown"
    assert (
        derive_current_at_evaluation(
            {**scope, "reference": "explicit_time", "start": "2026-08-01T00:00:00Z"},
            evaluation_time,
        )
        == "not_current"
    )


# --- identity and dedupe ----------------------------------------------------


def test_an_identical_repeated_candidate_is_recorded_as_a_duplicate(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    item = _assertion_item(excerpt, excerpt)

    normalized = normalize_candidates(_response(c1=_case_answer([item, copy.deepcopy(item)])), payload)

    case = normalized["cases"][0]
    assert len(case["candidates"]) == 1
    assert case["rejections"] == [{"ordinal": 2, "reason": "duplicate-candidate"}]
    assert case["raw_candidate_count"] == 2


def test_a_different_evidence_boundary_is_not_a_duplicate(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    payload = build_model_payload(corpus)
    first = _assertion_item(excerpt, excerpt)
    second = _assertion_item(excerpt, "does not supersede")

    normalized = normalize_candidates(_response(c1=_case_answer([first, second])), payload)

    case = normalized["cases"][0]
    assert case["rejections"] == []
    identifiers = {candidate["candidate_id"] for candidate in case["candidates"]}
    assert len(identifiers) == 2


# --- abstention -------------------------------------------------------------


def test_an_empty_schema_valid_answer_is_completed_empty(tmp_path: Path) -> None:
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", "Alpha regulates Gamma."))))
    units = units_from_corpus(corpus)
    model = _ReplayModel(
        _response(c1=_case_answer([], "No span reaches the supplied object with the stated scope.")),
    )

    result = run_extraction(_plan(units), tmp_path / "run", task=TASK, model=model, units=units)

    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["counts"]["empty"] == 1
    assert result.outcome.receipt["counts"]["failed"] == 0
    assert result.candidates["cases"][0]["candidates"] == []


def test_a_diagnostic_run_is_neither_benchmark_nor_publication_eligible(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt))))
    units = units_from_corpus(corpus)
    model = _ReplayModel(_response(c1=_case_answer([_assertion_item(excerpt, excerpt)])))

    result = run_extraction(_plan(units), tmp_path / "run", task=TASK, model=model, units=units)

    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["benchmark_eligible"] is False
    assert result.outcome.receipt["publication_eligible"] is False


# --- work identity ----------------------------------------------------------


class _WiderSchemaTask(RelationV2Task):
    """The same task, one harmless extra schema key, nothing else changed."""

    def build_schema(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {**super().build_schema(payload), "title": "a different strict schema"}


def test_only_a_whole_corpus_unit_claims_the_corpus_content_id(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(_write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", excerpt), _case("c2", excerpt))))

    whole = units_from_corpus(corpus)
    batched = units_from_corpus(corpus, batch_size=1)

    assert len(whole) == 1
    assert whole[0].input["corpus_content_id"] == corpus["corpus_content_id"]
    assert len(batched) == 2
    for unit in batched:
        assert "corpus_content_id" not in unit.input, "a batch never claims the whole corpus content"
        assert len(unit.input["cases"]) == 1
    assert units_from_corpus(corpus, batch_size=2)[0].input["corpus_content_id"] == corpus["corpus_content_id"]


def test_units_differing_only_in_the_prompt_or_the_schema_get_different_work_ids(tmp_path: Path) -> None:
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("c1", "Alpha does not supersede Beta.")))
    )
    units = units_from_corpus(corpus)
    model = _ReplayModel(_response(c1=_case_answer([], "No span reaches the object.")))

    base = plan_extraction_items(TASK, model, units)[0]
    reworded = plan_extraction_items(
        replace(TASK, instructions=TASK.instructions + "\nName the failed obligation."), model, units
    )[0]
    widened = plan_extraction_items(_WiderSchemaTask(), model, units)[0]

    assert base.work_id != reworded.work_id, "a different prompt is different work"
    assert base.work_id != widened.work_id, "a different output schema is different work"
    assert len({base.work_id, reworded.work_id, widened.work_id}) == 3
    assert plan_extraction_items(TASK, model, units)[0].work_id == base.work_id


# --- provider failure, resume, and safety ----------------------------------


def test_a_provider_failure_writes_a_safe_receipt_and_keeps_completed_work(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("good", excerpt), _case("bad", excerpt)))
    )
    units = units_from_corpus(corpus, batch_size=1)
    assert len(units) == 2
    plan = _plan(units)
    responses = [
        _response(good=_case_answer([_assertion_item(excerpt, excerpt)])),
        _response(bad=_case_answer([_assertion_item(excerpt, excerpt)])),
    ]
    failing = _ReplayModel(responses, fail_units=("bad",))

    first = run_extraction(plan, tmp_path / "run", task=TASK, model=failing, units=units)

    assert first.outcome.final_state == "fail"
    assert first.outcome.receipt["counts"] == {
        "planned": 2,
        "completed": 1,
        "empty": 0,
        "rejected": 0,
        "skipped": 0,
        "failed": 1,
        "unknown": 0,
        "unresolved_required": 0,
    }
    assert not (tmp_path / "run").exists(), "a failing run never becomes a run directory"
    work_dir = first.outcome.run_directory
    assert (work_dir / "receipt.json").is_file()
    for path in sorted(work_dir.rglob("*")):
        if path.is_file():
            assert "BRITTLE-PROVIDER-TEXT" not in path.read_text(encoding="utf-8", errors="replace")

    healthy = _ReplayModel(responses)
    second = run_extraction(plan, tmp_path / "run", task=TASK, model=healthy, units=units)

    assert second.outcome.final_state == "pass"
    assert len(healthy.calls) == 1, "resume never repeats finished paid work"
    assert second.outcome.receipt["counts"]["completed"] == 2


def _call_files(run_directory: Path) -> dict[str, bytes]:
    """Return every stored provider file, keyed by its path inside the run."""
    return {
        path.relative_to(run_directory).as_posix(): path.read_bytes()
        for path in sorted(run_directory.rglob("*.json"))
        if path.parent.parent.name == "calls"
    }


def test_a_rejected_response_is_settled_and_is_never_paid_for_or_overwritten_again(tmp_path: Path) -> None:
    excerpt = "Alpha does not supersede Beta."
    corpus = load_corpus(
        _write_corpus(tmp_path / "corpus.json", _corpus(_case("good", excerpt), _case("bad", excerpt)))
    )
    units = units_from_corpus(corpus, batch_size=1)
    plan = _plan(units)
    good = _response(good=_case_answer([_assertion_item(excerpt, excerpt)]))
    healthy_bad = _response(bad=_case_answer([_assertion_item(excerpt, excerpt)]))
    rejected_bad = {"cases": {"bad": {"items": [{"kind": "nonsense"}], "no_answer_rationale": None}}}

    first = run_extraction(plan, tmp_path / "run", task=TASK, model=_ReplayModel([good, rejected_bad]), units=units)

    assert first.outcome.final_state == "fail"
    assert first.outcome.receipt["counts"]["rejected"] == 1
    assert first.outcome.receipt["counts"]["failed"] == 0, "a checked response is not a transport failure"
    work_dir = first.outcome.run_directory
    stored = _call_files(work_dir)
    recorded = [
        json.loads(text)
        for name, text in ((name, blob.decode()) for name, blob in stored.items())
        if "call.json" in name
    ]
    rejected_records = [record for record in recorded if record["status"] == "rejected_response"]
    assert len(rejected_records) == first.outcome.receipt["counts"]["rejected"], "the receipt agrees with the record"
    calls = pq.read_table(work_dir / "extraction" / "provider-calls.parquet").to_pylist()
    assert len(calls) == 2, "a rejected unit still records the provider call it paid for"

    resumed = _ReplayModel([good, healthy_bad])
    second = run_extraction(plan, tmp_path / "run", task=TASK, model=resumed, units=units)

    assert resumed.calls == [], "a rejected response is settled; resume never pays for that unit again"
    assert second.outcome.receipt["counts"]["rejected"] == 1
    assert _call_files(second.outcome.run_directory) == stored, "resume never rewrites a stored request or response"
    # Settled is not approved: the run still fails closed, because the stored
    # response the rejection kept does not recompute into candidates.
    assert second.outcome.final_state == "fail"
    recompute_check = [check for check in second.outcome.receipt["checks"] if check["name"] == "candidates_recomputed"]
    assert [check["status"] for check in recompute_check] == ["fail"]


# --- validation and rebuild -------------------------------------------------


def test_validation_recomputes_candidates_and_scores_without_a_provider(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(units, answers=answers)
    model = _ReplayModel(response)
    result = run_extraction(plan, tmp_path / "run", task=TASK, model=model, units=units, answers=answers)
    assert result.outcome.final_state == "pass"
    assert result.metrics is not None

    report = validate_run(
        result.outcome.run_directory,
        plan=plan,
        recompute=recompute_extraction(TASK, answers=answers),
    )

    assert report["status"] == "pass"
    assert report["failures"] == []
    metrics = json.loads((result.outcome.run_directory / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["core_semantics"]["true_positives"] == 4


def test_validating_a_run_that_carries_metrics_without_answers_is_never_clean(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(units, answers=answers)
    result = run_extraction(
        plan, tmp_path / "run", task=TASK, model=_ReplayModel(response), units=units, answers=answers
    )
    assert (result.outcome.run_directory / "metrics.json").is_file()

    report = validate_run(result.outcome.run_directory, plan=plan, recompute=recompute_extraction(TASK))

    assert report["status"] == "fail", "an unrecomputed metric file is never silently clean"
    assert report["integrity_status"] == "fail"
    assert any("metrics.json" in failure for failure in report["integrity_failures"])


def test_the_receipt_reports_the_answer_exposure_it_measured(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(units, answers=answers)
    result = run_extraction(
        plan, tmp_path / "run", task=TASK, model=_ReplayModel(response), units=units, answers=answers
    )

    access = result.outcome.receipt["security"]["access_control"]
    metrics_text = (result.outcome.run_directory / "metrics.json").read_text(encoding="utf-8")

    assert access["answer_key_file_in_run_directory"] is False
    assert access["answer_key_files"] == []
    assert answer_key_files_in_run(result.outcome.run_directory) == []
    # The answer key stays outside the run, but scoring writes answer-derived
    # labels into metrics.json, so the receipt says so instead of denying it.
    assert access["answer_derived_labels_in_metrics"] is True
    assert access["answer_derived_metric_keys"] == ["candidate_id", "oracle_status", "requirement", "role"]
    for key in access["answer_derived_metric_keys"]:
        assert f'"{key}"' in metrics_text, "the receipt reports a key the metric file really carries"


def test_an_answer_key_file_inside_a_run_directory_is_named(tmp_path: Path) -> None:
    root = tmp_path / "run"
    (root / "extraction").mkdir(parents=True)
    (root / "extraction" / "answers.json").write_text("{}", encoding="utf-8")
    (root / "oracle.json").write_text("{}", encoding="utf-8")
    (root / "metrics.json").write_text("{}", encoding="utf-8")

    assert answer_key_files_in_run(root) == ["extraction/answers.json", "oracle.json"]
    assert answer_key_files_in_run(tmp_path / "absent") == []


def test_validation_notices_an_edited_candidate_table(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(units, answers=answers)
    result = run_extraction(
        plan, tmp_path / "run", task=TASK, model=_ReplayModel(response), units=units, answers=answers
    )
    table = result.outcome.run_directory / "extraction" / "relationship-candidates.parquet"
    table.write_bytes(table.read_bytes() + b"\n")

    report = validate_run(
        result.outcome.run_directory, plan=plan, recompute=recompute_extraction(TASK, answers=answers)
    )

    assert report["integrity_status"] == "fail"


def test_rebuild_recomputes_every_derived_file_without_a_provider(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(units, answers=answers)
    result = run_extraction(
        plan, tmp_path / "run", task=TASK, model=_ReplayModel(response), units=units, answers=answers
    )
    run_dir = result.outcome.run_directory

    report = rebuild_run(
        run_dir,
        tmp_path / "rebuilt",
        rebuild=rebuild_extraction(TASK, model=_ForbiddenModel(), answers=answers),
    )

    assert report["provider_invoked"] is False
    assert report["status"] == "pass"
    rebuilt = tmp_path / "rebuilt"
    for relative in (
        "extraction/relationship-candidates.parquet",
        "extraction/rejections.parquet",
        "extraction/provider-calls.parquet",
        "metrics.json",
    ):
        assert (rebuilt / relative).read_bytes() == (run_dir / relative).read_bytes()
    assert json.loads((rebuilt / "receipt.json").read_text(encoding="utf-8"))["rebuild"]["provider_invoked"] is False


# --- the human-review gate --------------------------------------------------


def _eligible_final_answers(corpus: dict[str, Any], answers: dict[str, Any], protocol_sha256: str) -> dict[str, Any]:
    """Seal the provisional answers with two blind reviews and a resolution."""

    def digest(value: object) -> str:
        return hashlib.sha256(canonical_json(value).encode()).hexdigest()

    final = copy.deepcopy(answers)
    final["metadata"]["oracle_status"] = "final-human-adjudicated"
    final["metadata"]["frozen_at"] = "2026-07-25T14:00:00Z"
    case_reviews = [
        {
            "case_id": case["case_id"],
            "target_quality": ("unsupported_argument" if case["role"] == "unsupported_target_control" else "valid"),
            "case_status": ("annotated" if case["expected_outputs"] else "no_explicit_support"),
            "rationale": (
                "The excerpt supports the recorded candidates."
                if case["expected_outputs"]
                else "The complete target relation lacks explicit support."
            ),
            "decision": {
                "case_id": case["case_id"],
                "expected_outputs": [
                    {key: copy.deepcopy(value) for key, value in expected.items() if key != "candidate_id"}
                    for expected in case["expected_outputs"]
                ],
            },
        }
        for case in final["cases"]
    ]
    reviews = []
    for ordinal in (1, 2):
        review: dict[str, Any] = {
            "review_id": f"urn:spicy-regs:review:human-{ordinal}",
            "reviewer_id": f"urn:spicy-regs:person:reviewer-{ordinal}",
            "reviewer_kind": "human",
            "started_at": "2026-07-25T09:00:00Z" if ordinal == 1 else "2026-07-25T10:00:00Z",
            "submitted_at": f"2026-07-25T1{ordinal}:00:00Z",
            "corpus_content_id": corpus["corpus_content_id"],
            "protocol_sha256": protocol_sha256,
            "provider_outputs_hidden": True,
            "machine_proposals_hidden": True,
            "other_review_hidden": True,
            "case_reviews": case_reviews,
            "case_reviews_sha256": digest(case_reviews),
        }
        review["content_sha256"] = digest(review)
        reviews.append(review)
    final["adjudication"]["blind_reviews"] = reviews
    resolution: dict[str, Any] = {
        "method": "exact_agreement",
        "resolver_id": None,
        "resolver_kind": None,
        "resolved_at": "2026-07-25T13:00:00Z",
        "input_review_ids": [review["review_id"] for review in reviews],
        "input_review_sha256s": [review["content_sha256"] for review in reviews],
        "resolved_cases_sha256": digest(final["cases"]),
        "disagreements": [],
        "excluded_case_ids": [],
    }
    resolution["content_sha256"] = digest(resolution)
    final["adjudication"]["resolution"] = resolution
    content = copy.deepcopy(final)
    content["metadata"].pop("oracle_content_sha256", None)
    final["metadata"]["oracle_content_sha256"] = digest(content)
    return final


def test_provisional_answers_are_not_benchmark_eligible(tmp_path: Path) -> None:
    corpus, answers, _ = _focused_bundle(tmp_path)

    gate = TASK.review_gate([corpus], answers, protocol_sha256=_sha256_file(PROTOCOL_FIXTURE))

    assert gate["eligible"] is False
    assert gate["failures"][0] == "oracle is not final-human-adjudicated"


def test_sealed_blind_reviews_and_a_resolution_open_the_gate(tmp_path: Path) -> None:
    corpus, answers, _ = _focused_bundle(tmp_path)
    protocol_sha256 = _sha256_file(PROTOCOL_FIXTURE)
    final = _eligible_final_answers(corpus, answers, protocol_sha256)

    gate = TASK.review_gate([corpus], final, protocol_sha256=protocol_sha256)

    assert gate["failures"] == []
    assert gate["eligible"] is True


def test_a_different_protocol_hash_refuses_the_gate(tmp_path: Path) -> None:
    corpus, answers, _ = _focused_bundle(tmp_path)
    protocol_sha256 = _sha256_file(PROTOCOL_FIXTURE)
    final = _eligible_final_answers(corpus, answers, protocol_sha256)

    gate = TASK.review_gate([corpus], final, protocol_sha256="f" * 64)

    assert gate["eligible"] is False
    assert "oracle does not bind the frozen review protocol" in gate["failures"]


def test_a_benchmark_run_passes_its_gate_and_records_eligibility(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    protocol_sha256 = _sha256_file(PROTOCOL_FIXTURE)
    final = _eligible_final_answers(corpus, answers, protocol_sha256)
    units = units_from_corpus(corpus)
    plan = _plan(
        units,
        mode="benchmark",
        answers=final,
        review_digests={PROTOCOL_FIXTURE.name: protocol_sha256},
    )

    result = run_extraction(
        plan,
        tmp_path / "run",
        task=TASK,
        model=_ReplayModel(response),
        units=units,
        answers=final,
        protocol_path=PROTOCOL_FIXTURE,
    )

    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["benchmark_eligible"] is True
    assert result.outcome.receipt["publication_eligible"] is False
    gate_checks = [check for check in result.outcome.receipt["checks"] if check["name"] == "human_review_gate"]
    assert [check["status"] for check in gate_checks] == ["pass"]


def test_a_benchmark_run_without_a_sealed_gate_never_becomes_eligible(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    units = units_from_corpus(corpus)
    plan = _plan(
        units,
        mode="benchmark",
        answers=answers,
        review_digests={PROTOCOL_FIXTURE.name: _sha256_file(PROTOCOL_FIXTURE)},
    )

    result = run_extraction(
        plan,
        tmp_path / "run",
        task=TASK,
        model=_ReplayModel(response),
        units=units,
        answers=answers,
        protocol_path=PROTOCOL_FIXTURE,
    )

    assert result.outcome.final_state == "fail"
    assert result.outcome.receipt["benchmark_eligible"] is False
    assert any("human_review_gate" in failure for failure in result.outcome.receipt["failures"])


def _benchmark_run_missing_its_review_material(tmp_path: Path) -> dict[str, Any]:
    """Run a sealed benchmark whose review protocol never reached the run."""
    corpus, answers, response = _focused_bundle(tmp_path)
    protocol_sha256 = _sha256_file(PROTOCOL_FIXTURE)
    final = _eligible_final_answers(corpus, answers, protocol_sha256)
    units = units_from_corpus(corpus)
    plan = _plan(units, mode="benchmark", answers=final, review_digests={PROTOCOL_FIXTURE.name: protocol_sha256})

    result = run_extraction(
        plan,
        tmp_path / "run",
        task=TASK,
        model=_ReplayModel(response),
        units=units,
        answers=final,
        protocol_path=None,
    )
    return {"corpus": corpus, "answers": final, "units": units, "plan": plan, "result": result}


def test_a_benchmark_run_without_its_review_material_fails_closed(tmp_path: Path) -> None:
    bundle = _benchmark_run_missing_its_review_material(tmp_path)
    receipt = bundle["result"].outcome.receipt

    assert bundle["result"].outcome.final_state == "fail"
    assert receipt["benchmark_eligible"] is False
    gate = [check for check in receipt["checks"] if check["name"] == "human_review_gate"]
    assert [check["status"] for check in gate] == ["fail"]
    assert gate[0]["detail"] == "a benchmark run needs answers, the review protocol, and its inputs"
    assert any("human_review_gate" in failure for failure in receipt["failures"])


def test_a_rebuild_never_re_blesses_a_benchmark_run_that_failed_its_gate(tmp_path: Path) -> None:
    bundle = _benchmark_run_missing_its_review_material(tmp_path)
    run_dir = bundle["result"].outcome.run_directory
    assert bundle["result"].outcome.final_state == "fail"

    # Everything the failed run lacked is handed to the rebuild. A rebuild
    # reproduces a run; supplying the missing material cannot re-decide it.
    with pytest.raises(RunDirectoryError) as caught:
        rebuild_run(
            run_dir,
            tmp_path / "rebuilt",
            rebuild=rebuild_extraction(
                TASK,
                model=_ForbiddenModel(),
                answers=bundle["answers"],
                units=bundle["units"],
                protocol_path=PROTOCOL_FIXTURE,
            ),
        )

    message = str(caught.value)
    assert "human_review_gate" in message
    assert "the source run recorded fail" in message
    assert "source_run_final_state" in message
    assert not (tmp_path / "rebuilt").exists(), "a rebuild never publishes a run the source run failed"


def test_the_gate_refuses_a_review_protocol_the_plan_did_not_pin(tmp_path: Path) -> None:
    corpus, answers, response = _focused_bundle(tmp_path)
    protocol_sha256 = _sha256_file(PROTOCOL_FIXTURE)
    final = _eligible_final_answers(corpus, answers, protocol_sha256)
    units = units_from_corpus(corpus)
    plan = _plan(
        units,
        mode="benchmark",
        answers=final,
        review_digests={PROTOCOL_FIXTURE.name: "a" * 64},
    )

    result = run_extraction(
        plan,
        tmp_path / "run",
        task=TASK,
        model=_ReplayModel(response),
        units=units,
        answers=final,
        protocol_path=PROTOCOL_FIXTURE,
    )

    assert result.outcome.final_state == "fail"
    assert result.outcome.receipt["benchmark_eligible"] is False


def test_the_gate_time_must_carry_a_timezone(tmp_path: Path) -> None:
    corpus, answers, _ = _focused_bundle(tmp_path)

    with pytest.raises(ExtractionError):
        TASK.review_gate(
            [corpus],
            answers,
            protocol_sha256=_sha256_file(PROTOCOL_FIXTURE),
            gate_time=datetime(2026, 7, 25, 12, 0, 0),
        )

    naive_free = TASK.review_gate(
        [corpus],
        answers,
        protocol_sha256=_sha256_file(PROTOCOL_FIXTURE),
        gate_time=datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert naive_free["eligible"] is False

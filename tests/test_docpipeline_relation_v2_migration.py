"""Migration parity between the v2 relation runner and the v3 extract step.

Build order step 3 moves relation extraction into ``docpipeline``. The cutover
procedure requires running the old and the new code on the same fixed inputs
and approving every legitimate difference in this test.

The fixed inputs are:

* ``tests/fixtures/relation_exclusion_explicit_denial_v2_corpus.json``;
* ``tests/fixtures/relation_exclusion_explicit_denial_v2_oracle.provisional.json``;
  and
* the stored provider reply of the focused-five diagnostic under
  ``docs/evidence/relation-exclusion-openai-v2-focused-five-2026-07-25/``,
  replayed by a fake structured-text model.

Everything under ``docs/evidence/`` is read, never written.

Importing the old module is allowed here and nowhere else: ``docpipeline`` code
must not import ``spicy_regs.corpora`` at runtime, and a separate test proves
that.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs.corpora import relation_exclusion_evaluation_v2 as old_v2
from spicy_regs.docpipeline.adapters import SHARED_CALL_DETAIL_KEYS, StructuredTextResult
from spicy_regs.docpipeline.extraction import (
    answer_key_files_in_run,
    extraction_plan_facts,
    recompute_extraction,
    run_extraction,
)
from spicy_regs.docpipeline.relation_task import (
    RelationV2Task,
    load_answers,
    load_corpus,
    units_from_corpus,
)
from spicy_regs.docpipeline.runtime import RunPlan, validate_run
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

#: The stored reply predates one vocabulary rename. Both paths replay exactly
#: the same adapted bytes, so the rename cannot hide a parity difference.
CANNED_RESPONSE_ADAPTATION = ("attributed_actor", "attributed_source")


# --------------------------------------------------------------------------
# The approved differences
# --------------------------------------------------------------------------
#
# ``kind`` says how the difference is observed, and every observation below is
# recomputed from the real run. An unlisted file, receipt field, or call detail
# fails the migration test; so does a listed one that no longer appears.

EXPECTED_DIFFERENCES: tuple[dict[str, Any], ...] = (
    {
        "id": "run-record-layout",
        "kind": "run_file",
        "old": "loose artifacts in a hand-made evidence directory",
        "new": "one v3 run directory with a plan, a work history, per-call files, and typed tables",
        "reason": "v3 keeps one run record per run so validation and rebuild can recompute it.",
        "values": (
            "plan.json",
            "planned-work.json",
            "transitions.jsonl",
            "extraction/calls/*/payload.json",
            "extraction/calls/*/schema.json",
            "extraction/calls/*/request.json",
            "extraction/calls/*/response.json",
            "extraction/calls/*/call.json",
            "extraction/provider-calls.parquet",
            "extraction/relationship-candidates.parquet",
            "extraction/rejections.parquet",
            "metrics.json",
            "receipt.json",
        ),
    },
    {
        "id": "candidate-table-shape",
        "kind": "table_column",
        "old": "nested normalized-candidates.json",
        "new": "one flat Parquet row per candidate beside the same nested value in memory",
        "reason": "A run's tables must have one fixed shape, including when a run produces no rows.",
        # Written out, never derived from the task: a list read back from the
        # code it approves would approve a dropped column too.
        "values": (
            "case_id",
            "candidate_id",
            "kind",
            "polarity",
            "operation",
            "stage",
            "current_at_evaluation",
            "temporal_relation_to_reference",
            "temporal_reference",
            "temporal_start",
            "temporal_end",
            "temporal_raw_text",
            "intended_effective_relation_to_reference",
            "intended_effective_reference",
            "intended_effective_start",
            "intended_effective_end",
            "intended_effective_raw_text",
            "attribution_status",
            "attribution_claimant_text",
            "conditionality_status",
            "condition_text",
            "evidence_text",
            "evidence_start",
            "evidence_end",
            "evidence_alignment",
            "rationale",
            "target_subject_id",
            "target_subject_label",
            "target_predicate_id",
            "target_predicate_label",
            "target_object_id",
            "target_object_label",
        ),
    },
    {
        "id": "rejection-table-shape",
        "kind": "rejection_column",
        "old": "rejections nested inside each normalized case",
        "new": "one flat Parquet row per rejection",
        "reason": "A failed check is a durable rejection record, not a negative fact, so it gets its own table.",
        "values": ("case_id", "ordinal", "reason", "detail"),
    },
    {
        "id": "receipt-shape",
        "kind": "receipt_field",
        "old": "a bespoke v2 receipt with experiment_status, methodology, scope, and provisional_scores",
        "new": "the shared v3 runtime receipt",
        "reason": "One receipt shape serves every step, so a reader never has to know which runner wrote it.",
        "values": (
            "benchmark_eligible",
            "checks",
            "counts",
            "failures",
            "files",
            "final_state",
            "format_version",
            "generated_at",
            "inputs",
            "mode",
            "plan_hash",
            "provider",
            "provider_sources",
            "publication_eligible",
            "receipt_sha256",
            "run_id",
            "security",
            "steps",
            "test_answers",
            "versions",
            "warnings",
        ),
    },
    {
        "id": "call-details-shape",
        "kind": "call_detail",
        "old": "a provider_call block shaped by the OpenAI arm alone",
        "new": "the shared structured-text call details, one Parquet row per call",
        "reason": "Both provider arms return the same call-detail keys, so the table cannot favor one arm.",
        "values": (
            "work_id",
            "unit_id",
            "task",
            "provider",
            "transport",
            "model_id",
            "schema_name",
            "response_id",
            "response_model",
            "status",
            "duration_ms",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "attempt_count",
            "retry_count",
            "prompt_sha256",
            "request_sha256",
            "reasoning_effort",
            "max_output_tokens",
            "timeout_seconds",
            "max_retries",
            "sdk_max_retries",
            "store",
            "schema_validated_locally",
        ),
    },
    {
        "id": "metric-file-name",
        "kind": "artifact_name",
        "old": "provisional-scores.json written by the runner",
        "new": "metrics.json written by the runtime and hashed into the receipt",
        "reason": "The runtime owns the metric file so its digest is part of the receipt it seals.",
        "values": ("metrics.json",),
    },
    {
        "id": "answers-stay-outside-the-run",
        "kind": "absent_run_file",
        "old": "the oracle path recorded inside the evidence receipt",
        "new": "the answers digest pinned in the plan; the answer key file never enters the run directory",
        "reason": "A run directory that carries its own answer key could publish the answers with the results.",
        "values": ("oracle.json", "answers.json", "extraction/answers.json"),
    },
    {
        "id": "answer-derived-labels-in-metrics",
        "kind": "metrics_field",
        "old": "the same answer-derived labels inside provisional-scores.json",
        "new": "the same labels inside metrics.json, and the receipt says so instead of denying it",
        "reason": (
            "The answer key stays out of the run, but scoring cannot report a match without naming the "
            "answer it matched, so metrics.json is the one run file carrying answer-derived labels."
        ),
        "values": ("role", "requirement", "oracle_status", "candidate_id", "expected_candidate_id"),
    },
    {
        "id": "stored-prompt-revision",
        "kind": "stored_evidence_drift",
        "old": "the stored diagnostic was run with the earlier v2 prompt",
        "new": "both runners send the current v2 prompt",
        "reason": "The prompt was revised after that run; parity here is old code against new code, not against history.",
        "values": ("instructions.txt",),
    },
    {
        "id": "canned-response-vocabulary",
        "kind": "input_adaptation",
        "old": "the stored reply says attributed_actor",
        "new": "the current contract says attributed_source",
        "reason": "The enum was renamed after the stored run; both paths replay the same adapted bytes.",
        "values": CANNED_RESPONSE_ADAPTATION,
    },
)


def _approved(kind: str) -> set[str]:
    return {
        value for difference in EXPECTED_DIFFERENCES if difference["kind"] == kind for value in difference["values"]
    }


# --------------------------------------------------------------------------
# the shared fixed inputs
# --------------------------------------------------------------------------


class _ReplayModel:
    """Replays the stored provider reply and records what it was asked."""

    model_id = "fake:focused-five-replay"

    def __init__(self, response: dict[str, Any]) -> None:
        self._response = response
        self.payloads: list[dict[str, Any]] = []
        self.schemas: list[dict[str, Any]] = []
        self.instructions: list[str] = []

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
            "instructions_sha256": hashlib.sha256(instructions.encode()).hexdigest(),
            "input": canonical_json(payload),
            "max_output_tokens": max_output_tokens,
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
        self.payloads.append(json.loads(canonical_json(payload)))
        self.schemas.append(json.loads(canonical_json(dict(schema))))
        self.instructions.append(instructions)
        return StructuredTextResult(
            output=copy.deepcopy(self._response),
            call={
                "provider": "fake",
                "transport": "replay",
                "model_id": self.model_id,
                "schema_name": name,
                "response_id": "stored-focused-five",
                "response_model": self.model_id,
                "status": "completed",
                "duration_ms": 38595.747,
                "input_tokens": 4434,
                "output_tokens": 4109,
                "total_tokens": 8543,
                "attempt_count": 1,
                "retry_count": 0,
                "attempts": [{"attempt": 1, "status": "completed"}],
                "prompt_sha256": hashlib.sha256(canonical_json(payload).encode()).hexdigest(),
                "request_sha256": hashlib.sha256(canonical_json(dict(schema)).encode()).hexdigest(),
                "max_output_tokens": max_output_tokens,
                "schema_validated_locally": True,
            },
        )


@pytest.fixture(scope="module")
def fixed_inputs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Write the one corpus, oracle, and provider reply both paths consume."""
    directory = tmp_path_factory.mktemp("relation-v2-migration-inputs")
    stored_payload = json.loads((EVIDENCE_DIR / "payload.json").read_text(encoding="utf-8"))
    wanted = {str(case["case_id"]) for case in stored_payload["cases"]}

    raw_corpus = json.loads(CORPUS_FIXTURE.read_text(encoding="utf-8"))
    corpus_path = directory / "corpus.json"
    corpus_path.write_text(
        json.dumps({**raw_corpus, "cases": [case for case in raw_corpus["cases"] if case["case_id"] in wanted]}),
        encoding="utf-8",
    )
    corpus_content_id = load_corpus(corpus_path)["corpus_content_id"]

    raw_oracle = json.loads(ORACLE_FIXTURE.read_text(encoding="utf-8"))
    oracle_path = directory / "oracle.json"
    oracle_path.write_text(
        json.dumps(
            {
                **raw_oracle,
                "corpus_content_id": corpus_content_id,
                "cases": [case for case in raw_oracle["cases"] if case["case_id"] in wanted],
            }
        ),
        encoding="utf-8",
    )

    stored_response_text = (EVIDENCE_DIR / "response.json").read_text(encoding="utf-8")
    response_text = stored_response_text.replace(*CANNED_RESPONSE_ADAPTATION)
    return {
        "directory": directory,
        "corpus_path": corpus_path,
        "oracle_path": oracle_path,
        "stored_payload": stored_payload,
        "stored_schema": json.loads((EVIDENCE_DIR / "schema.json").read_text(encoding="utf-8")),
        "stored_response_text": stored_response_text,
        "response": json.loads(response_text),
        "protocol_sha256": hashlib.sha256(PROTOCOL_FIXTURE.read_bytes()).hexdigest(),
    }


@pytest.fixture(scope="module")
def old_bundle(fixed_inputs: dict[str, Any]) -> dict[str, Any]:
    """Run the v2 module end to end on the fixed inputs."""
    corpus = old_v2.load_corpus(fixed_inputs["corpus_path"])
    oracle = old_v2.load_oracle(fixed_inputs["oracle_path"], corpus)
    payload = old_v2.build_model_payload(corpus)
    schema = old_v2.build_response_schema(payload)
    response = copy.deepcopy(fixed_inputs["response"])
    old_v2.validate_response_schema(response, schema)
    normalized = old_v2.normalize_candidates(response, payload)
    return {
        "payload": payload,
        "schema": schema,
        "normalized": normalized,
        "scores": old_v2.score_candidates(oracle, normalized),
        "gate": old_v2.evaluate_run_eligibility(corpus, oracle, protocol_sha256=fixed_inputs["protocol_sha256"]),
    }


@pytest.fixture(scope="module")
def new_bundle(fixed_inputs: dict[str, Any]) -> dict[str, Any]:
    """Run the v3 extract step end to end on the same fixed inputs."""
    corpus = load_corpus(fixed_inputs["corpus_path"])
    answers = load_answers(fixed_inputs["oracle_path"], corpus)
    units = units_from_corpus(corpus)
    plan = RunPlan(
        run_id="relation-v2-migration-0001",
        mode="diagnostic",
        steps=("extract",),
        source_snapshot={"corpus_content_id": corpus["corpus_content_id"], "dataset_id": corpus["dataset_id"]},
        extraction=extraction_plan_facts(TASK, units, answers=answers),
        provider={"model_id": _ReplayModel.model_id, "store": False},
        code_commit="0" * 40,
        required_work=("extract",),
    )
    model = _ReplayModel(copy.deepcopy(fixed_inputs["response"]))
    result = run_extraction(
        plan,
        fixed_inputs["directory"] / "run",
        task=TASK,
        model=model,
        units=units,
        answers=answers,
    )
    assert result.outcome.final_state == "pass", result.outcome.receipt["failures"]
    return {
        "corpus": corpus,
        "answers": answers,
        "plan": plan,
        "model": model,
        "result": result,
        "payload": model.payloads[0],
        "schema": model.schemas[0],
        "normalized": result.candidates,
        "scores": result.metrics,
        "gate": TASK.review_gate([corpus], answers, protocol_sha256=fixed_inputs["protocol_sha256"]),
        "run_directory": result.outcome.run_directory,
        "receipt": result.outcome.receipt,
    }


# --------------------------------------------------------------------------
# parity
# --------------------------------------------------------------------------


def test_both_runners_agree_on_the_whole_committed_twelve_case_dataset() -> None:
    """Parity on the full fixtures, with no stored provider reply involved.

    The tests around this one replay one five-case diagnostic. This one reads
    the committed twelve-case corpus and oracle whole, so a difference that
    hides outside the focused five still fails the cutover.
    """
    protocol_sha256 = hashlib.sha256(PROTOCOL_FIXTURE.read_bytes()).hexdigest()

    old_corpus = old_v2.load_corpus(CORPUS_FIXTURE)
    old_oracle = old_v2.load_oracle(ORACLE_FIXTURE, old_corpus)
    old_payload = old_v2.build_model_payload(old_corpus)

    new_corpus = load_corpus(CORPUS_FIXTURE)
    new_answers = load_answers(ORACLE_FIXTURE, new_corpus)
    units = units_from_corpus(new_corpus)
    new_payload = TASK.build_payload(units[0].input)

    assert len(new_corpus["cases"]) == 12
    assert len(units) == 1, "a frozen benchmark sends the whole corpus in one call"
    assert canonical_json(new_payload) == canonical_json(old_payload)
    assert canonical_json(TASK.build_schema(new_payload)) == canonical_json(old_v2.build_response_schema(old_payload))
    assert canonical_json(new_answers) == canonical_json(old_oracle)
    assert canonical_json(TASK.review_gate([new_corpus], new_answers, protocol_sha256=protocol_sha256)) == (
        canonical_json(old_v2.evaluate_run_eligibility(old_corpus, old_oracle, protocol_sha256=protocol_sha256))
    )


def test_both_runners_send_the_same_gold_free_model_payload(
    old_bundle: dict[str, Any],
    new_bundle: dict[str, Any],
    fixed_inputs: dict[str, Any],
) -> None:
    assert canonical_json(new_bundle["payload"]) == canonical_json(old_bundle["payload"])
    assert canonical_json(new_bundle["payload"]) == canonical_json(fixed_inputs["stored_payload"])
    serialized = canonical_json(new_bundle["payload"])
    for forbidden in old_v2.MODEL_INPUT_FORBIDDEN_KEYS:
        assert f'"{forbidden}"' not in serialized


def test_both_runners_build_the_same_strict_schema(
    old_bundle: dict[str, Any],
    new_bundle: dict[str, Any],
    fixed_inputs: dict[str, Any],
) -> None:
    assert canonical_json(new_bundle["schema"]) == canonical_json(old_bundle["schema"])
    stored = copy.deepcopy(fixed_inputs["stored_schema"])
    adapted = json.loads(canonical_json(stored).replace(*CANNED_RESPONSE_ADAPTATION))
    assert canonical_json(new_bundle["schema"]) == canonical_json(adapted)


def test_both_runners_use_the_same_instructions(new_bundle: dict[str, Any]) -> None:
    sent = new_bundle["model"].instructions[0]

    assert sent == old_v2.INSTRUCTIONS
    stored = (EVIDENCE_DIR / "instructions.txt").read_text(encoding="utf-8").strip()
    assert _approved("stored_evidence_drift") == {"instructions.txt"}
    assert sent.strip() != stored, "the stored diagnostic predates the current prompt"


def test_both_runners_produce_the_same_normalized_candidates(
    old_bundle: dict[str, Any],
    new_bundle: dict[str, Any],
) -> None:
    assert canonical_json(new_bundle["normalized"]) == canonical_json(old_bundle["normalized"])
    accepted = [candidate for case in new_bundle["normalized"]["cases"] for candidate in case["candidates"]]
    assert len(accepted) == 5, "the stored reply grounds five candidates exactly"


def test_both_runners_produce_the_same_scores(old_bundle: dict[str, Any], new_bundle: dict[str, Any]) -> None:
    assert canonical_json(new_bundle["scores"]) == canonical_json(old_bundle["scores"])


def test_both_runners_reach_the_same_human_gate_decision(
    old_bundle: dict[str, Any],
    new_bundle: dict[str, Any],
) -> None:
    assert canonical_json(new_bundle["gate"]) == canonical_json(old_bundle["gate"])
    assert new_bundle["gate"]["eligible"] is False


def test_the_new_candidate_table_carries_the_same_candidates(new_bundle: dict[str, Any]) -> None:
    rows = pq.read_table(new_bundle["run_directory"] / "extraction" / "relationship-candidates.parquet").to_pylist()
    nested = {
        candidate["candidate_id"]: candidate
        for case in new_bundle["normalized"]["cases"]
        for candidate in case["candidates"]
    }

    assert {row["candidate_id"] for row in rows} == set(nested)
    for row in rows:
        candidate = nested[row["candidate_id"]]
        assert row["evidence_text"] == candidate["evidence_text"]
        assert row["evidence_start"] == candidate["evidence_start"]
        assert row["evidence_alignment"] == candidate["evidence_alignment"]
        assert row["target_subject_id"] == candidate["target_relation"]["subject"]["id"]


def test_the_new_run_validates_without_a_provider(new_bundle: dict[str, Any]) -> None:
    report = validate_run(
        new_bundle["run_directory"],
        plan=new_bundle["plan"],
        recompute=recompute_extraction(TASK, answers=new_bundle["answers"]),
    )

    assert report["failures"] == []
    assert report["status"] == "pass"


# --------------------------------------------------------------------------
# the approved differences, checked against the real run
# --------------------------------------------------------------------------


def _observed_run_files(run_directory: Path) -> set[str]:
    observed: set[str] = set()
    for path in sorted(run_directory.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(run_directory).as_posix()
        parts = relative.split("/")
        if parts[:2] == ["extraction", "calls"]:
            relative = f"extraction/calls/*/{parts[-1]}"
        observed.add(relative)
    return observed


def test_the_run_directory_holds_exactly_the_approved_files(new_bundle: dict[str, Any]) -> None:
    assert _observed_run_files(new_bundle["run_directory"]) == _approved("run_file")


def test_the_run_directory_never_holds_the_answer_key(new_bundle: dict[str, Any]) -> None:
    observed = _observed_run_files(new_bundle["run_directory"])
    assert observed.isdisjoint(_approved("absent_run_file"))
    assert answer_key_files_in_run(new_bundle["run_directory"]) == []
    for path in sorted(new_bundle["run_directory"].rglob("*.json")):
        assert "accepted_evidence" not in path.read_text(encoding="utf-8")


def _answer_derived_keys(value: Any) -> set[str]:
    """Return every key at any depth that only the answer key explains."""
    if isinstance(value, dict):
        return {
            *(str(key) for key in value if str(key) in _approved("metrics_field")),
            *(nested for child in value.values() for nested in _answer_derived_keys(child)),
        }
    if isinstance(value, list):
        return {nested for child in value for nested in _answer_derived_keys(child)}
    return set()


def test_metrics_is_the_only_run_file_carrying_answer_derived_labels(new_bundle: dict[str, Any]) -> None:
    run_directory = new_bundle["run_directory"]
    carrying = {
        path.relative_to(run_directory).as_posix(): _answer_derived_keys(json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(run_directory.rglob("*.json"))
        if _answer_derived_keys(json.loads(path.read_text(encoding="utf-8")))
    }

    assert set(carrying) == {"metrics.json"}, "no other run file may carry an answer-derived label"
    assert carrying["metrics.json"] == _approved("metrics_field")
    # The receipt reports that exposure rather than claiming the run is
    # answer-free. ``expected_candidate_id`` is a metric field, not a payload
    # key, so the receipt's forbidden-key scan does not name it.
    access = new_bundle["receipt"]["security"]["access_control"]
    assert access["answer_derived_labels_in_metrics"] is True
    assert access["answer_key_file_in_run_directory"] is False
    assert set(access["answer_derived_metric_keys"]) == _approved("metrics_field") - {"expected_candidate_id"}


def test_the_receipt_holds_exactly_the_approved_fields(new_bundle: dict[str, Any]) -> None:
    assert set(new_bundle["receipt"]) == _approved("receipt_field")


def test_the_provider_call_table_holds_exactly_the_approved_call_details(new_bundle: dict[str, Any]) -> None:
    table = pq.read_table(new_bundle["run_directory"] / "extraction" / "provider-calls.parquet")

    assert set(table.column_names) == _approved("call_detail")
    assert table.num_rows == 1
    # The shared interface promises these keys on every path from every arm.
    assert set(SHARED_CALL_DETAIL_KEYS) - {"attempts"} <= set(table.column_names)


def test_the_candidate_and_rejection_tables_hold_exactly_the_approved_columns(new_bundle: dict[str, Any]) -> None:
    candidates = pq.read_table(new_bundle["run_directory"] / "extraction" / "relationship-candidates.parquet")
    rejections = pq.read_table(new_bundle["run_directory"] / "extraction" / "rejections.parquet")

    assert set(candidates.column_names) == _approved("table_column") | {"work_id", "unit_id"}
    assert set(rejections.column_names) == _approved("rejection_column") | {"work_id", "unit_id"}


def test_the_metric_file_is_the_runtime_file_the_receipt_seals(new_bundle: dict[str, Any]) -> None:
    assert _approved("artifact_name") == {"metrics.json"}
    metrics_path = new_bundle["run_directory"] / "metrics.json"
    stored = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert canonical_json(stored) == canonical_json(new_bundle["scores"])
    digest = hashlib.sha256(metrics_path.read_bytes()).hexdigest()
    assert new_bundle["receipt"]["test_answers"]["metrics_sha256"] == digest


def test_the_canned_reply_was_adapted_only_as_approved(fixed_inputs: dict[str, Any]) -> None:
    old_token, new_token = CANNED_RESPONSE_ADAPTATION
    stored = fixed_inputs["stored_response_text"]
    replayed = canonical_json(fixed_inputs["response"])

    assert old_token in stored, "the stored reply still uses the old vocabulary"
    assert old_token not in replayed
    assert stored.count(old_token) == replayed.count(new_token)
    assert canonical_json(json.loads(stored.replace(old_token, new_token))) == replayed


def test_every_expected_difference_is_named_and_explained() -> None:
    identifiers = [difference["id"] for difference in EXPECTED_DIFFERENCES]

    assert len(identifiers) == len(set(identifiers))
    for difference in EXPECTED_DIFFERENCES:
        assert difference["kind"] in {
            "run_file",
            "absent_run_file",
            "metrics_field",
            "table_column",
            "rejection_column",
            "receipt_field",
            "call_detail",
            "artifact_name",
            "stored_evidence_drift",
            "input_adaptation",
        }
        assert difference["old"] and difference["new"] and difference["reason"]
        assert difference["values"], "an approved difference names what it approves"


def test_the_new_step_never_imports_the_old_runner() -> None:
    for module in ("extraction", "relation_task"):
        source = (Path(__file__).parents[1] / "src" / "spicy_regs" / "docpipeline" / f"{module}.py").read_text(
            encoding="utf-8"
        )
        assert "spicy_regs.corpora" not in source
        assert "spicy_regs.ontology.llm" not in source


def test_the_old_runner_and_its_command_are_untouched() -> None:
    assert (
        Path(__file__).parents[1] / "src" / "spicy_regs" / "corpora" / "relation_exclusion_evaluation_v2.py"
    ).is_file()
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    assert "validate-relation-exclusion-v2 = " in pyproject

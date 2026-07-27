"""The v3 extract step: prompts, schemas, response checks, and candidates.

This module builds one gold-free model payload and one strict output schema per
unit of work, calls a text model through the shared structured-text interface,
checks the response, and turns it into candidates, rejections, and provider-call
records inside a run directory.

What it never does:

* import a provider SDK — providers stay behind ``adapters/``;
* approve its own output — approval is a later, separate step; or
* turn a failed check into a negative fact about the source. A response that
  leaves the schema fails its unit, and a candidate that fails a semantic or
  grounding check becomes a durable rejection record.

The step is task-shaped, not relation-shaped: everything specific to one
extraction task (its prompt, schema, checks, table columns, scorer, and review
gate) lives behind :class:`ExtractionTask`. ``relation_task.py`` holds the first
such task; a tag or typed-value task implements the same surface.

Runs go through ``runtime.execute_run``. Every provider request, response, and
call record is stored under the run's ``extraction/calls/<work-id>/`` directory,
so ``runtime.validate_run`` can recompute every derived table and every metric
without a provider, and ``runtime.rebuild_run`` can rebuild them provider-free.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from loguru import logger

from spicy_regs.docpipeline.adapters import StructuredTextCallError, StructuredTextModel
from spicy_regs.docpipeline.runtime import (
    CheckResult,
    ProviderTotals,
    RunChecks,
    RunOutcome,
    RunPlan,
    RunWorkspace,
    WorkIdentity,
    WorkItem,
    WorkResult,
    execute_run,
    sha256_file,
    sha256_text,
)
from spicy_regs.ontology.common import canonical_json

EXTRACTION_STEP = "extract"

#: Where one unit's provider files live. ``runtime.IMMUTABLE_RUN_PATTERNS``
#: covers this directory, so a rebuild may never change what was asked or what
#: came back.
CALL_ROOT = "extraction/calls"
PAYLOAD_NAME = "payload.json"
SCHEMA_NAME = "schema.json"
REQUEST_NAME = "request.json"
RESPONSE_NAME = "response.json"
CALL_NAME = "call.json"

#: The runtime's own receipt file, read here only to reproduce a source run.
RECEIPT_NAME = "receipt.json"
METRICS_NAME = "metrics.json"

PROVIDER_CALL_TABLE = "extraction/provider-calls.parquet"

#: Filename fragments that name an answer key. No run file may match one: a run
#: directory carrying its own answers could publish them with its results. The
#: access-control record scans for these rather than asserting their absence.
ANSWER_FILE_NAME_FRAGMENTS: tuple[str, ...] = ("answer", "oracle", "gold")

#: Ranking aids may select source text for extraction, but they are never part
#: of the factual payload.  Reject these keys at every nesting depth so a
#: caller cannot accidentally teach an extraction task that a rank is truth.
RETRIEVAL_AID_FORBIDDEN_KEYS: frozenset[str] = frozenset(
    {
        "score",
        "rank",
        "score_kind",
        "candidate_rank",
        "dense_score",
        "dense_rank",
        "sparse_score",
        "sparse_rank",
        "fusion_score",
        "rerank_score",
        "rerank_rank",
        "retrieval_score",
        "retrieval_rank",
    }
)

#: How good a check status is. A rebuild may lower one, never raise one.
_CHECK_STATUS_RANK: dict[str, int] = {"fail": 0, "unknown": 1, "pass": 2}

#: One row per physical provider call. The call-detail columns are exactly the
#: shared keys both provider arms promise, minus ``attempts``, whose per-attempt
#: list stays in the call file rather than being flattened into a table.
PROVIDER_CALL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("unit_id", "string"),
    ("task", "string"),
    ("provider", "string"),
    ("transport", "string"),
    ("model_id", "string"),
    ("schema_name", "string"),
    ("response_id", "string"),
    ("response_model", "string"),
    ("status", "string"),
    ("duration_ms", "double"),
    ("input_tokens", "int64"),
    ("output_tokens", "int64"),
    ("total_tokens", "int64"),
    ("attempt_count", "int64"),
    ("retry_count", "int64"),
    ("prompt_sha256", "string"),
    ("request_sha256", "string"),
    ("reasoning_effort", "string"),
    ("max_output_tokens", "int64"),
    ("timeout_seconds", "double"),
    ("max_retries", "int64"),
    ("sdk_max_retries", "int64"),
    ("store", "bool"),
    ("schema_validated_locally", "bool"),
)

#: Columns the step owns on every task table, so a row is always traceable back
#: to the unit of work that produced it.
WORK_COLUMNS: tuple[tuple[str, str], ...] = (("work_id", "string"), ("unit_id", "string"))


class ExtractionError(Exception):
    """Extraction input, response, or answer data is unusable."""


class ModelInputLeakError(ExtractionError):
    """A model payload carries a hidden test field it must never see."""


class ResponseCheckError(ExtractionError):
    """A provider response left the strict schema or failed a response check."""


# --------------------------------------------------------------------------
# task surface
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionUnit:
    """One unit of extraction input: exactly what one provider call reads."""

    unit_id: str
    input: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not str(self.unit_id).strip():
            raise ExtractionError("an extraction unit requires a unit id")


@runtime_checkable
class ExtractionTask(Protocol):
    """One extraction task: a prompt, a schema, checks, tables, and metrics."""

    name: str
    schema_name: str
    instructions: str
    max_output_tokens: int
    forbidden_payload_keys: frozenset[str]
    candidate_table: str
    rejection_table: str

    def build_payload(self, unit_input: Mapping[str, Any]) -> dict[str, Any]:
        """Return the gold-free model input for one unit."""
        ...

    def build_schema(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Return the one strict output schema for that payload."""
        ...

    def check_response(self, response: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
        """Raise when the response leaves the schema or fails a response check."""
        ...

    def build_candidates(self, response: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
        """Return checked candidates and rejections for one unit's response."""
        ...

    def merge_candidates(self, parts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """Merge per-unit candidate records into one value for the whole run."""
        ...

    def is_empty(self, candidates: Mapping[str, Any]) -> bool:
        """True when the unit succeeded with no candidate and no rejection."""
        ...

    def candidate_columns(self) -> tuple[tuple[str, str], ...]:
        """Return the candidate table's fixed columns as ``(name, kind)``."""
        ...

    def rejection_columns(self) -> tuple[tuple[str, str], ...]:
        """Return the rejection table's fixed columns as ``(name, kind)``."""
        ...

    def candidate_rows(self, candidates: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Return one flat row per accepted candidate."""
        ...

    def rejection_rows(self, candidates: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Return one flat row per rejected candidate."""
        ...

    def score(self, answers: Mapping[str, Any], candidates: Mapping[str, Any]) -> dict[str, Any]:
        """Return this task's metrics for one run's candidates."""
        ...

    def review_gate(
        self,
        unit_inputs: Sequence[Mapping[str, Any]],
        answers: Mapping[str, Any],
        *,
        protocol_sha256: str,
    ) -> dict[str, Any]:
        """Return the human-review decision that gates benchmark eligibility."""
        ...


# --------------------------------------------------------------------------
# tables
# --------------------------------------------------------------------------


def _arrow_type(kind: str) -> Any:
    import pyarrow as pa

    types = {"string": pa.string(), "int64": pa.int64(), "double": pa.float64(), "bool": pa.bool_()}
    if kind not in types:
        raise ExtractionError(f"unknown column kind {kind!r}")
    return types[kind]


def _coerce(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "string":
        return str(value)
    if kind == "int64":
        return int(value)
    if kind == "double":
        return float(value)
    return bool(value)


def _table_rows(columns: Sequence[tuple[str, str]], rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the rows a table holds: fixed columns, in fixed order, coerced."""
    return [{name: _coerce(row.get(name), kind) for name, kind in columns} for row in rows]


def _write_table(path: Path, columns: Sequence[tuple[str, str]], rows: Sequence[Mapping[str, Any]]) -> None:
    """Write one correctly shaped table, including when it has no rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    prepared = _table_rows(columns, rows)
    schema = pa.schema([pa.field(name, _arrow_type(kind)) for name, kind in columns])
    data = {name: [row[name] for row in prepared] for name, _ in columns}
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pydict(data, schema=schema), path)


# --------------------------------------------------------------------------
# plan facts and work identity
# --------------------------------------------------------------------------


def _plain(value: Any) -> Any:
    return json.loads(canonical_json(value))


def _nested_keys(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return {
            *(str(key) for key in value),
            *(nested for child in value.values() for nested in _nested_keys(child)),
        }
    if isinstance(value, (list, tuple)):
        return {nested for child in value for nested in _nested_keys(child)}
    return set()


def refuse_retrieval_aids(payload: Mapping[str, Any]) -> None:
    """Refuse retrieval score/rank fields before they reach a factual prompt."""
    leaked = sorted(_nested_keys(payload) & RETRIEVAL_AID_FORBIDDEN_KEYS)
    if leaked:
        raise ModelInputLeakError(f"the model payload leaked retrieval score or rank fields: {leaked}")


def _refuse_leaked_answers(task: ExtractionTask, payload: Mapping[str, Any]) -> None:
    refuse_retrieval_aids(payload)
    leaked = sorted(_nested_keys(payload) & set(task.forbidden_payload_keys))
    if leaked:
        raise ModelInputLeakError(f"the model payload for {task.name} leaked hidden test fields: {leaked}")


def extraction_plan_facts(
    task: ExtractionTask,
    units: Sequence[ExtractionUnit],
    *,
    answers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the extraction facts a plan records: prompt, schema, and answers.

    The digests are recomputed when the run starts, so a plan can never claim a
    prompt, a schema, a model input, or an answer key the run did not use.
    ``answers_sha256`` digests the normalized in-memory answers, not the bytes
    of the oracle file they were loaded from.
    """
    payload_digests: dict[str, str] = {}
    schema_digests: dict[str, str] = {}
    for unit in units:
        payload = task.build_payload(unit.input)
        _refuse_leaked_answers(task, payload)
        payload_digests[unit.unit_id] = sha256_text(canonical_json(_plain(payload)))
        schema_digests[unit.unit_id] = sha256_text(canonical_json(_plain(task.build_schema(payload))))
    return {
        "task": task.name,
        "schema_name": task.schema_name,
        "instructions_sha256": sha256_text(task.instructions),
        "max_output_tokens": int(task.max_output_tokens),
        "unit_ids": [unit.unit_id for unit in units],
        "payload_sha256": payload_digests,
        "schema_sha256": schema_digests,
        "answers_sha256": sha256_text(canonical_json(_plain(answers))) if answers is not None else "",
    }


def plan_extraction_items(
    task: ExtractionTask,
    model: StructuredTextModel,
    units: Sequence[ExtractionUnit],
    *,
    prior_run_id: str = "",
) -> tuple[WorkItem, ...]:
    """Return one planned work item per unit, keyed by its exact identity."""
    items: list[WorkItem] = []
    seen: set[str] = set()
    for unit in units:
        if unit.unit_id in seen:
            raise ExtractionError(f"extraction unit {unit.unit_id} appears twice")
        seen.add(unit.unit_id)
        payload = task.build_payload(unit.input)
        _refuse_leaked_answers(task, payload)
        schema = task.build_schema(payload)
        identity = WorkIdentity(
            step=EXTRACTION_STEP,
            task=task.name,
            input_digests=(sha256_text(canonical_json(_plain(unit.input))),),
            settings={
                "unit_id": unit.unit_id,
                "schema_name": task.schema_name,
                "max_output_tokens": int(task.max_output_tokens),
            },
            prompt_digest=sha256_text(task.instructions),
            schema_digest=sha256_text(canonical_json(_plain(schema))),
            provider_config={"model_id": str(getattr(model, "model_id", ""))},
            prior_run_id=prior_run_id,
        )
        items.append(
            WorkItem.from_identity(
                identity,
                payload={"unit_id": unit.unit_id, "payload": _plain(payload), "schema": _plain(schema)},
            )
        )
    return tuple(items)


# --------------------------------------------------------------------------
# deriving everything from stored calls
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class _Derived:
    """Everything a run's stored calls prove, recomputed from those files."""

    candidates: dict[str, Any]
    candidate_rows: list[dict[str, Any]] = field(default_factory=list)
    rejection_rows: list[dict[str, Any]] = field(default_factory=list)
    call_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    unit_count: int = 0


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExtractionError(f"unreadable run file {path}: {type(exc).__name__}") from exc


def _call_directories(root: Path) -> list[Path]:
    calls = Path(root) / CALL_ROOT
    return (
        sorted((path for path in calls.iterdir() if path.is_dir()), key=lambda path: path.name)
        if calls.is_dir()
        else []
    )


def _derive(root: Path, task: ExtractionTask) -> _Derived:
    """Recompute candidates, rejections, and call rows from stored files only."""
    parts: list[Mapping[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    rejection_rows: list[dict[str, Any]] = []
    call_rows: list[dict[str, Any]] = []
    failures: list[str] = []
    units = 0
    for directory in _call_directories(root):
        work_id = directory.name
        stored = _read_json(directory / CALL_NAME) if (directory / CALL_NAME).is_file() else {}
        record: Mapping[str, Any] = stored if isinstance(stored, Mapping) else {}
        raw_call = record.get("call")
        call: Mapping[str, Any] = raw_call if isinstance(raw_call, Mapping) else {}
        own = {"work_id", "unit_id", "task"}
        unit_id = str(record.get("unit_id") or "")
        call_rows.append(
            {
                "work_id": work_id,
                "unit_id": unit_id,
                "task": str(record.get("task") or task.name),
                **{key: call.get(key) for key, _ in PROVIDER_CALL_COLUMNS if key not in own},
                "status": str(call.get("status") or record.get("status") or ""),
            }
        )
        response_path = directory / RESPONSE_NAME
        payload_path = directory / PAYLOAD_NAME
        if not response_path.is_file() or not payload_path.is_file():
            continue
        units += 1
        payload = _read_json(payload_path)
        response = _read_json(response_path)
        try:
            task.check_response(response, task.build_schema(payload))
            part = task.build_candidates(response, payload)
        except ExtractionError as exc:
            failures.append(f"{work_id} does not recompute: {type(exc).__name__}")
            continue
        parts.append(part)
        for row in task.candidate_rows(part):
            candidate_rows.append({"work_id": work_id, "unit_id": unit_id, **row})
        for row in task.rejection_rows(part):
            rejection_rows.append({"work_id": work_id, "unit_id": unit_id, **row})
    return _Derived(
        candidates=task.merge_candidates(parts),
        candidate_rows=candidate_rows,
        rejection_rows=rejection_rows,
        call_rows=call_rows,
        failures=failures,
        unit_count=units,
    )


def _write_derived(root: Path, task: ExtractionTask, derived: _Derived) -> None:
    _write_table(Path(root) / task.candidate_table, (*WORK_COLUMNS, *task.candidate_columns()), derived.candidate_rows)
    _write_table(Path(root) / task.rejection_table, (*WORK_COLUMNS, *task.rejection_columns()), derived.rejection_rows)
    _write_table(Path(root) / PROVIDER_CALL_TABLE, PROVIDER_CALL_COLUMNS, derived.call_rows)


def _derived_tables(task: ExtractionTask, derived: _Derived) -> dict[str, Any]:
    """Return each derived table's relative path and the rows it must contain."""
    return {
        task.candidate_table: _table_rows((*WORK_COLUMNS, *task.candidate_columns()), derived.candidate_rows),
        task.rejection_table: _table_rows((*WORK_COLUMNS, *task.rejection_columns()), derived.rejection_rows),
        PROVIDER_CALL_TABLE: _table_rows(PROVIDER_CALL_COLUMNS, derived.call_rows),
    }


# --------------------------------------------------------------------------
# checks
# --------------------------------------------------------------------------


def _check(name: str, status: str, detail: str = "") -> CheckResult:
    return CheckResult(step=EXTRACTION_STEP, name=name, status=status, detail=detail)


def _plan_input_checks(plan: RunPlan, expected: Mapping[str, Any]) -> list[CheckResult]:
    declared = plan.extraction
    if not declared:
        return [_check("plan_declares_the_model_input", "unknown", "the plan records no extraction facts")]
    differing = sorted(key for key, value in expected.items() if declared.get(key) != value)
    if differing:
        return [
            _check(
                "plan_declares_the_model_input",
                "fail",
                f"the plan and the run disagree about {differing}",
            )
        ]
    return [_check("plan_declares_the_model_input", "pass")]


def _answer_checks(
    plan: RunPlan,
    answers: Mapping[str, Any] | None,
) -> tuple[list[CheckResult], dict[str, str]]:
    declared = str(plan.extraction.get("answers_sha256") or "") if plan.extraction else ""
    if answers is None:
        if declared:
            return [_check("test_answers_pinned", "fail", "the plan pins answers the run did not read")], {}
        return [_check("test_answers_pinned", "unknown", "the run has no test answers")], {}
    digest = sha256_text(canonical_json(_plain(answers)))
    if declared and declared != digest:
        return [_check("test_answers_pinned", "fail", "the answers do not match the digest the plan pins")], {}
    return [_check("test_answers_pinned", "pass")], {"answers": digest}


def _gate_checks(
    plan: RunPlan,
    task: ExtractionTask,
    unit_inputs: Sequence[Mapping[str, Any]],
    answers: Mapping[str, Any] | None,
    protocol_path: Path | None,
) -> tuple[list[CheckResult], dict[str, str], dict[str, Any] | None]:
    """Check the human review that decides benchmark eligibility.

    A benchmark run fails closed: without complete, pinned, sealed review
    material there is no gate decision, and a run with no gate decision must
    never be recorded as benchmark-eligible.
    """
    required = plan.mode == "benchmark"
    if protocol_path is None or answers is None or not unit_inputs:
        status = "fail" if required else "unknown"
        detail = "a benchmark run needs answers, the review protocol, and its inputs"
        return [_check("human_review_gate", status, detail)], {}, None

    protocol_path = Path(protocol_path)
    digest = sha256_file(protocol_path)
    checks: list[CheckResult] = []
    pinned = plan.review_file_digests.get(protocol_path.name)
    if pinned is None:
        checks.append(
            _check(
                "review_protocol_pinned",
                "fail" if required else "unknown",
                f"the plan does not pin a digest for {protocol_path.name}",
            )
        )
    elif pinned != digest:
        checks.append(_check("review_protocol_pinned", "fail", f"{protocol_path.name} is not the pinned file"))
        return [*checks, _check("human_review_gate", "fail", "the review protocol changed")], {}, None
    else:
        checks.append(_check("review_protocol_pinned", "pass"))

    try:
        decision = task.review_gate(unit_inputs, answers, protocol_sha256=digest)
    except ExtractionError as exc:
        checks.append(
            _check("human_review_gate", "fail", f"the gate refused the review material: {type(exc).__name__}")
        )
        return checks, {"review_protocol": digest}, None
    status = "pass" if decision.get("eligible") else "fail"
    detail = "" if status == "pass" else "; ".join(str(failure) for failure in decision.get("failures") or [])
    checks.append(_check("human_review_gate", status, detail))
    return checks, {"review_protocol": digest}, decision


def answer_key_files_in_run(root: Path) -> list[str]:
    """Return every file in one run directory whose name claims to be answers.

    A filename scan, not a promise: the access-control record reports what
    this returns, so a run that did carry an answer key says so.
    """
    root = Path(root)
    if not root.is_dir():
        return []
    return [
        path.relative_to(root).as_posix()
        for path in sorted(root.rglob("*"))
        if path.is_file() and any(fragment in path.name.casefold() for fragment in ANSWER_FILE_NAME_FRAGMENTS)
    ]


def _access_control(task: ExtractionTask, root: Path, metrics: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return what this run really exposes about its answers, all computed.

    Two different facts, kept apart because they have different answers. The
    answer *key* never enters the run directory. Answer-*derived* labels do:
    scoring names the role, requirement, oracle status, and matched answer of
    every case, and those land in ``metrics.json``. Claiming otherwise would
    be a false receipt, so both are measured rather than asserted.
    """
    answer_files = answer_key_files_in_run(Path(root))
    metric_keys = sorted(
        _nested_keys(_plain(metrics) if metrics is not None else {}) & set(task.forbidden_payload_keys)
    )
    return {
        "scope": "local-run",
        "answer_key_file_in_run_directory": bool(answer_files),
        "answer_key_files": answer_files,
        "answer_derived_labels_in_metrics": bool(metric_keys),
        "answer_derived_metric_keys": metric_keys,
    }


def _score_checks(
    task: ExtractionTask,
    answers: Mapping[str, Any] | None,
    candidates: Mapping[str, Any],
) -> tuple[list[CheckResult], dict[str, Any] | None]:
    if answers is None:
        return [_check("scoring", "unknown", "the run has no test answers")], None
    try:
        metrics = task.score(answers, candidates)
    except ExtractionError as exc:
        return [_check("scoring", "fail", f"the run could not be scored: {type(exc).__name__}: {exc}")], None
    return [_check("scoring", "pass")], _plain(metrics)


def _report(
    plan: RunPlan,
    task: ExtractionTask,
    *,
    root: Path,
    expected_facts: Mapping[str, Any],
    unit_inputs: Sequence[Mapping[str, Any]],
    answers: Mapping[str, Any] | None,
    protocol_path: Path | None,
) -> tuple[RunChecks, _Derived]:
    """Recompute every derived file and every check from the stored calls."""
    derived = _derive(Path(root), task)
    _write_derived(Path(root), task, derived)

    checks: list[CheckResult] = [*_plan_input_checks(plan, expected_facts)]
    checks.append(
        _check("candidates_recomputed", "pass" if not derived.failures else "fail", "; ".join(derived.failures))
    )
    answer_checks, answer_digests = _answer_checks(plan, answers)
    checks.extend(answer_checks)
    gate_checks, gate_digests, _ = _gate_checks(plan, task, unit_inputs, answers, protocol_path)
    checks.extend(gate_checks)
    score_checks, metrics = _score_checks(task, answers, derived.candidates)
    checks.extend(score_checks)

    return (
        RunChecks(
            checks=tuple(checks),
            access_control=_access_control(task, Path(root), metrics),
            metrics=metrics,
            test_answer_digests={**answer_digests, **gate_digests},
        ),
        derived,
    )


# --------------------------------------------------------------------------
# running
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionOutcome:
    """What one extraction run produced, beside the runtime's own receipt."""

    outcome: RunOutcome
    candidates: dict[str, Any]
    metrics: dict[str, Any] | None = None

    @property
    def passed(self) -> bool:
        return self.outcome.passed


def _provider_totals(call: Mapping[str, Any], *, failed: bool) -> ProviderTotals:
    return ProviderTotals(
        calls=1,
        retries=int(call.get("retry_count") or 0),
        failures=1 if failed else 0,
        seconds=float(call.get("duration_ms") or 0.0) / 1_000.0,
        input_tokens=int(call.get("input_tokens") or 0),
        output_tokens=int(call.get("output_tokens") or 0),
        total_tokens=int(call.get("total_tokens") or 0),
    )


def run_extraction(
    plan: RunPlan,
    output_dir: Path,
    *,
    task: ExtractionTask,
    model: StructuredTextModel,
    units: Sequence[ExtractionUnit],
    answers: Mapping[str, Any] | None = None,
    protocol_path: Path | None = None,
) -> ExtractionOutcome:
    """Run one extraction task over ``units`` and finish it as one v3 run.

    The same code path serves an exposed diagnostic and an untouched benchmark;
    only the plan's mode, its pinned review digests, and the presence of test
    answers differ.
    """
    if EXTRACTION_STEP not in plan.steps:
        raise ExtractionError(f"the plan does not request the {EXTRACTION_STEP!r} step")
    items = plan_extraction_items(task, model, units)
    expected_facts = extraction_plan_facts(task, units, answers=answers)
    unit_inputs = [dict(unit.input) for unit in units]
    derived_box: dict[str, _Derived] = {}

    def execute(workspace: RunWorkspace, item: WorkItem) -> WorkResult:
        directory = f"{CALL_ROOT}/{item.work_id}"
        unit_id = str(item.payload["unit_id"])
        payload = item.payload["payload"]
        schema = item.payload["schema"]
        _refuse_leaked_answers(task, payload)
        workspace.write_json(f"{directory}/{PAYLOAD_NAME}", payload)
        workspace.write_json(f"{directory}/{SCHEMA_NAME}", schema)
        workspace.write_json(
            f"{directory}/{REQUEST_NAME}",
            model.secret_free_request(
                name=task.schema_name,
                schema=schema,
                instructions=task.instructions,
                payload=payload,
                max_output_tokens=task.max_output_tokens,
            ),
        )
        try:
            result = model.structured_json(
                name=task.schema_name,
                schema=schema,
                instructions=task.instructions,
                payload=payload,
                max_output_tokens=task.max_output_tokens,
            )
        except StructuredTextCallError as exc:
            # Only ``.call`` is receipt-safe: the message and its cause may
            # carry provider text, so neither reaches the work history.
            workspace.write_json(
                f"{directory}/{CALL_NAME}",
                {
                    "work_id": item.work_id,
                    "unit_id": unit_id,
                    "task": task.name,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "call": dict(exc.call),
                },
            )
            logger.warning("extraction unit {} failed with {}", unit_id, type(exc).__name__)
            return WorkResult.failed(
                item.work_id,
                step=item.step,
                task=item.task,
                error=f"{type(exc).__name__}: the structured-text call failed",
                provider=_provider_totals(exc.call, failed=True),
            )

        workspace.write_json(f"{directory}/{RESPONSE_NAME}", result.output)
        call_record: dict[str, Any] = {
            "work_id": item.work_id,
            "unit_id": unit_id,
            "task": task.name,
            "status": "completed",
            "error_type": None,
            "call": dict(result.call),
        }
        totals = _provider_totals(result.call, failed=False)
        try:
            task.check_response(result.output, schema)
            candidates = task.build_candidates(result.output, payload)
        except ExtractionError as exc:
            call_record["status"] = "rejected_response"
            call_record["error_type"] = type(exc).__name__
            workspace.write_json(f"{directory}/{CALL_NAME}", call_record)
            logger.warning("extraction unit {} was rejected by {}", unit_id, type(exc).__name__)
            # A response that came back and failed its checks is settled, not
            # broken transport: retrying it would pay the provider again for
            # the same unit and overwrite the stored request, response, and
            # call record this run must keep exactly as they happened.
            return WorkResult.rejected(
                item.work_id,
                step=item.step,
                task=item.task,
                reason=f"{type(exc).__name__}: the provider response failed its checks",
                provider=totals,
            )
        workspace.write_json(f"{directory}/{CALL_NAME}", call_record)
        if task.is_empty(candidates):
            return WorkResult.completed_empty(item.work_id, step=item.step, task=item.task, provider=totals)
        return WorkResult.completed(
            item.work_id,
            step=item.step,
            task=item.task,
            result={
                "unit_id": unit_id,
                "candidates": len(task.candidate_rows(candidates)),
                "rejections": len(task.rejection_rows(candidates)),
            },
            provider=totals,
        )

    def finalize(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        report, derived = _report(
            plan,
            task,
            root=workspace.path,
            expected_facts=expected_facts,
            unit_inputs=unit_inputs,
            answers=answers,
            protocol_path=protocol_path,
        )
        derived_box["derived"] = derived
        return report

    outcome = execute_run(plan, output_dir, items=items, execute=execute, finalize=finalize)
    derived = derived_box.get("derived")
    metrics_path = outcome.run_directory / "metrics.json"
    metrics = _read_json(metrics_path) if metrics_path.is_file() else None
    return ExtractionOutcome(
        outcome=outcome,
        candidates=derived.candidates if derived is not None else task.merge_candidates([]),
        metrics=metrics,
    )


# --------------------------------------------------------------------------
# checking and rebuilding without a provider
# --------------------------------------------------------------------------


def recompute_extraction(
    task: ExtractionTask,
    *,
    answers: Mapping[str, Any] | None = None,
):
    """Return the ``validate_run`` hook that recomputes this step's files.

    Checking never calls a provider: every table and every metric is derived
    from the stored payloads and responses alone.
    """

    def recompute(run_dir: Path, plan: RunPlan) -> Mapping[str, Any]:
        derived = _derive(Path(run_dir), task)
        if derived.failures:
            raise ExtractionError("; ".join(derived.failures))
        expected: dict[str, Any] = dict(_derived_tables(task, derived))
        if answers is not None:
            expected[METRICS_NAME] = _plain(task.score(answers, derived.candidates))
        elif (Path(run_dir) / METRICS_NAME).is_file():
            # Silence is not a pass: skipping the metric file while it sits in
            # the run would let any content in it validate clean.
            raise ExtractionError(f"{METRICS_NAME} cannot be recomputed without the test answers that produced it")
        logger.debug("recomputed {} extraction units for {}", derived.unit_count, plan.run_id)
        return expected

    return recompute


def _source_run_checks(root: Path) -> tuple[dict[tuple[str, str], str], str]:
    """Return the source run's recorded check statuses and its final state.

    A rebuild copies the source run before it runs, so the receipt still in
    the workspace is the source receipt, not the one being written.
    """
    path = Path(root) / RECEIPT_NAME
    if not path.is_file():
        return {}, ""
    receipt = _read_json(path)
    if not isinstance(receipt, Mapping):
        return {}, ""
    recorded: dict[tuple[str, str], str] = {}
    for check in receipt.get("checks") or []:
        if isinstance(check, Mapping):
            recorded[(str(check.get("step") or ""), str(check.get("name") or ""))] = str(check.get("status") or "")
    return recorded, str(receipt.get("final_state") or "")


def _reproduced_checks(report: RunChecks, root: Path) -> RunChecks:
    """Clamp a rebuilt run's checks to what the source run already decided.

    A rebuild reproduces a run; it never re-decides one. Every check is
    recomputed from stored files, but a check the source receipt recorded as
    ``fail`` or ``unknown`` can never come back ``pass``: otherwise handing a
    rebuild the answers and the review protocol a failed benchmark run never
    had would bless that run as benchmark-eligible. A check the source receipt
    does not record may be computed freely, and a recomputed status worse than
    the recorded one still wins. The source run's final state is recorded as
    its own check, so a rebuild of a failed run cannot look clean.
    """
    recorded, source_final_state = _source_run_checks(Path(root))
    clamped: list[CheckResult] = []
    for check in report.checks:
        source_status = recorded.get((check.step, check.name))
        if source_status is None or _CHECK_STATUS_RANK.get(source_status, 0) >= _CHECK_STATUS_RANK.get(check.status, 0):
            clamped.append(check)
            continue
        note = f"the source run recorded {source_status}"
        clamped.append(replace(check, status=source_status, detail=f"{check.detail}; {note}" if check.detail else note))
    if source_final_state == "pass":
        state_status = "pass"
    elif source_final_state:
        state_status = "fail"
    else:
        state_status = "unknown"
    clamped.append(
        _check(
            "source_run_final_state",
            state_status,
            f"the source run recorded final_state {source_final_state or 'nothing'}",
        )
    )
    return replace(report, checks=tuple(clamped))


def rebuild_extraction(
    task: ExtractionTask,
    *,
    model: StructuredTextModel | None = None,
    answers: Mapping[str, Any] | None = None,
    units: Sequence[ExtractionUnit] = (),
    protocol_path: Path | None = None,
):
    """Return the ``rebuild_run`` hook that rebuilds this step provider-free.

    ``model`` is accepted so a caller can pass the provider it would otherwise
    use and prove it is never reached. Nothing here calls it.

    The hook reproduces the source run rather than re-deciding it: the answers,
    units, and review protocol a caller supplies can recompute a check the
    source receipt never recorded, but they can never raise one the source run
    failed or left undecided.
    """
    if model is not None:
        logger.debug("rebuilding {} without calling {}", task.name, getattr(model, "model_id", "the provider"))

    def rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        expected_facts = extraction_plan_facts(task, tuple(units), answers=answers) if units else dict(plan.extraction)
        report, _ = _report(
            plan,
            task,
            root=workspace.path,
            expected_facts=expected_facts,
            unit_inputs=[dict(unit.input) for unit in units],
            answers=answers,
            protocol_path=protocol_path,
        )
        return _reproduced_checks(report, workspace.path)

    return rebuild

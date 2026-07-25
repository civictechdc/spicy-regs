"""Contracts for v3 run execution, resume, validation, and rebuild.

Primitives (work IDs, checkpoints, secrets, inventory, plans, item states)
live in ``test_docpipeline_runtime.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.runtime import (
    REDACTED,
    CheckResult,
    PlanError,
    ProviderTotals,
    RunChecks,
    RunDirectoryError,
    RunPlan,
    RunWorkspace,
    WorkCheckpoint,
    WorkItem,
    WorkResult,
    execute_run,
    file_inventory,
    rebuild_run,
    scan_file_for_secrets,
    scan_tree_for_secrets,
    validate_run,
    work_directory_for,
)


def _plan(**overrides: Any) -> RunPlan:
    values: dict[str, Any] = {
        "run_id": "docpipeline-0001",
        "mode": "build",
        "steps": ("extract",),
        "source_snapshot": {"snapshot_id": "snap-1"},
        "rulespec": {"version": "0.4.0", "schema_sha256": "e" * 64},
        "profiles": {"regulations-document-v2": {"access": "public"}},
        "vocabulary": {"scheme": "local"},
        "segmentation": {"rule": "structure-overlap-1800"},
        "retrieval": {"methods": ["sparse"]},
        "extraction": {"prompt_sha256": "1" * 64, "schema_sha256": "2" * 64},
        "rules": {"approval": "v1"},
        "provider": {"model": "gpt-x", "store": False},
        "review_file_digests": {},
        "code_commit": "0" * 40,
        "required_work": (),
        "optional_work": (),
        "earlier_runs": {},
    }
    values.update(overrides)
    return RunPlan(**values)


def _items(*work_ids: str) -> tuple[WorkItem, ...]:
    return tuple(WorkItem(work_id=work_id, step="extract", task=f"task-{work_id}") for work_id in work_ids)


class _CountingExecutor:
    """Fake step executor recording every invocation and provider call."""

    def __init__(self, outcomes: dict[str, str] | None = None) -> None:
        self.outcomes = outcomes or {}
        self.calls: list[str] = []
        self.provider_calls = 0

    def __call__(self, workspace: RunWorkspace, item: WorkItem) -> WorkResult:
        self.calls.append(item.work_id)
        outcome = self.outcomes.get(item.work_id, "completed")
        if outcome == "raise":
            raise RuntimeError("provider exploded")
        if outcome == "failed":
            return WorkResult.failed(item.work_id, step=item.step, task=item.task, error="provider timeout")
        if outcome == "empty":
            return WorkResult.completed_empty(item.work_id, step=item.step, task=item.task)
        self.provider_calls += 1
        workspace.write_json(
            f"extraction/calls/{item.work_id}/response.json",
            {"work_id": item.work_id, "rows": [{"value": item.task}]},
        )
        return WorkResult.completed(
            item.work_id,
            step=item.step,
            task=item.task,
            result={"rows": 1},
            provider=ProviderTotals(calls=1, seconds=0.25, total_tokens=10),
        )


class _ForbiddenProvider:
    """A provider a rebuild must never reach for. Calling it fails the test."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, work_id: str) -> dict[str, Any]:
        self.calls += 1
        raise AssertionError(f"a rebuild called a provider for {work_id}")


def _summary(directory: Path) -> dict[str, Any]:
    """Derive the run summary from stored provider responses only."""
    rows: list[dict[str, Any]] = []
    for path in sorted((directory / "extraction" / "calls").glob("*/response.json")):
        rows.extend(json.loads(path.read_text(encoding="utf-8"))["rows"])
    return {"rows": sorted(rows, key=lambda row: str(row["value"]))}


def _finalize(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
    workspace.write_json("output/summary.json", _summary(workspace.path))
    return RunChecks(
        checks=(CheckResult(step="extract", name="summary_written", status="pass"),),
        access_control={"scope": "public", "violations": 0},
    )


def _recompute(run_dir: Path, plan: RunPlan) -> dict[str, Any]:
    assert plan.run_id
    return {"output/summary.json": _summary(run_dir)}


# --- atomic run directory --------------------------------------------------


def test_a_passing_run_renames_the_work_directory_atomically(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    work_dir = work_directory_for(output_dir)
    executor = _CountingExecutor({"w3": "empty"})

    outcome = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2", "w3"),
        execute=executor,
        finalize=_finalize,
    )

    assert outcome.final_state == "pass"
    assert outcome.passed is True
    assert outcome.run_directory == output_dir.resolve()
    assert not work_dir.exists(), "the work directory is renamed, not copied"
    assert (output_dir / "receipt.json").is_file()

    receipt = json.loads((output_dir / "receipt.json").read_text(encoding="utf-8"))
    assert receipt == outcome.receipt
    assert receipt["counts"] == {
        "planned": 3,
        "completed": 2,
        "empty": 1,
        "rejected": 0,
        "skipped": 0,
        "failed": 0,
        "unknown": 0,
        "unresolved_required": 0,
    }
    assert (
        sum(receipt["counts"][state] for state in ("completed", "empty", "rejected", "skipped", "failed", "unknown"))
        == receipt["counts"]["planned"]
    )
    assert receipt["plan_hash"] == _plan().plan_hash
    assert receipt["publication_eligible"] is True
    assert receipt["benchmark_eligible"] is False
    assert receipt["provider"] == {
        "calls": 2,
        "retries": 0,
        "failures": 0,
        "seconds": 0.5,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 20,
    }
    assert receipt["security"] == {
        "secret_match_count": 0,
        "secret_match_files": [],
        "access_control": {"scope": "public", "violations": 0},
    }
    assert receipt["checks"] == [{"step": "extract", "name": "summary_written", "status": "pass", "detail": ""}]
    assert receipt["failures"] == []
    files = receipt["files"]
    assert "receipt.json" not in files
    for name in ("plan.json", "planned-work.json", "transitions.jsonl", "output/summary.json"):
        assert files[name]["sha256"] and files[name]["bytes"] > 0
    assert files["transitions.jsonl"]["rows"] == 3
    assert validate_run(output_dir, plan=_plan(), recompute=_recompute)["status"] == "pass"


def test_a_run_refuses_to_overwrite_an_existing_run_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    output_dir.mkdir()
    with pytest.raises(RunDirectoryError):
        execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor())


def test_a_work_directory_belonging_to_another_plan_is_refused(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2"),
        execute=_CountingExecutor({"w2": "failed"}),
    )
    with pytest.raises(PlanError):
        execute_run(
            _plan(run_id="docpipeline-0002"),
            output_dir,
            items=_items("w1", "w2"),
            execute=_CountingExecutor(),
        )


# --- required failures, empty results, optional work ------------------------


def test_a_required_failure_blocks_the_run_and_keeps_the_work_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    work_dir = work_directory_for(output_dir)
    executor = _CountingExecutor({"w2": "raise"})

    outcome = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2"),
        execute=executor,
        finalize=_finalize,
    )

    assert outcome.final_state == "fail"
    assert outcome.passed is False
    assert not output_dir.exists(), "a failing run never publishes its directory"
    assert outcome.run_directory == work_dir
    receipt = json.loads((work_dir / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["final_state"] == "fail"
    assert receipt["publication_eligible"] is False
    assert receipt["counts"]["failed"] == 1
    assert any("w2" in failure for failure in receipt["failures"])

    transition = WorkCheckpoint(work_dir / "transitions.jsonl").get("w2")
    assert transition is not None
    assert transition["state"] == "failed"
    assert "RuntimeError" in transition["error"], "the raised failure stays durable and safe"


def test_optional_work_declared_in_the_plan_does_not_block_the_run(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    outcome = execute_run(
        _plan(optional_work=("task-w2",)),
        output_dir,
        items=_items("w1", "w2"),
        execute=_CountingExecutor({"w2": "failed"}),
        finalize=_finalize,
    )
    assert outcome.final_state == "pass"
    assert outcome.receipt["counts"]["failed"] == 1
    assert outcome.receipt["failures"] == []


def test_empty_results_pass_while_failures_and_skips_do_not(tmp_path: Path) -> None:
    empty_only = execute_run(
        _plan(),
        tmp_path / "empty-run",
        items=_items("w1"),
        execute=_CountingExecutor({"w1": "empty"}),
    )
    assert empty_only.final_state == "pass"
    assert empty_only.receipt["counts"]["empty"] == 1
    assert empty_only.receipt["counts"]["failed"] == 0

    def _skip(workspace: RunWorkspace, item: WorkItem) -> WorkResult:
        return WorkResult.skipped(item.work_id, step=item.step, task=item.task, reason="no source text")

    skipped = execute_run(_plan(), tmp_path / "skip-run", items=_items("w1"), execute=_skip)
    assert skipped.final_state == "fail"
    assert skipped.receipt["counts"]["skipped"] == 1


def test_required_work_that_was_never_planned_is_counted_apart_from_unknown_work(tmp_path: Path) -> None:
    outcome = execute_run(
        _plan(required_work=("task-w1", "task-missing")),
        tmp_path / "run",
        items=_items("w1"),
        execute=_CountingExecutor(),
    )
    assert outcome.final_state == "fail"
    counts = outcome.receipt["counts"]
    assert counts["unresolved_required"] == 1, "work the plan never planned is not an item state"
    assert counts["unknown"] == 0
    assert counts["completed"] == 1
    assert counts["planned"] == 1
    assert any("task-missing" in failure for failure in outcome.receipt["failures"])


# --- step checks -----------------------------------------------------------


def test_a_requested_step_that_reports_no_check_is_recorded_as_unknown(tmp_path: Path) -> None:
    """Silence is not a pass: an unchecked step is visible in the receipt."""
    outcome = execute_run(
        _plan(steps=("source", "extract")),
        tmp_path / "run",
        items=_items("w1"),
        execute=_CountingExecutor(),
        finalize=_finalize,
    )

    assert outcome.final_state == "pass", "an undecided check warns; it does not fail the run"
    by_step = {check["step"]: check for check in outcome.receipt["checks"]}
    assert by_step["extract"]["status"] == "pass"
    assert by_step["source"]["status"] == "unknown"
    assert by_step["source"]["name"] == "step_checks"
    assert any("source.step_checks is undecided" in warning for warning in outcome.receipt["warnings"])


def test_check_messages_keep_the_detail_they_were_given(tmp_path: Path) -> None:
    """The failure text ends where the detail ends, colons and all."""

    def _finalize_with_details(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        assert results
        return RunChecks(
            checks=(
                CheckResult(step="extract", name="coverage", status="fail", detail="ratio 0.5:"),
                CheckResult(step="extract", name="latency", status="fail", detail=""),
                CheckResult(step="extract", name="rows", status="unknown", detail="counted 3 : "),
            )
        )

    outcome = execute_run(
        _plan(),
        tmp_path / "run",
        items=_items("w1"),
        execute=_CountingExecutor(),
        finalize=_finalize_with_details,
    )

    assert "check extract.coverage failed: ratio 0.5:" in outcome.receipt["failures"]
    assert "check extract.latency failed" in outcome.receipt["failures"]
    assert "check extract.rows is undecided: counted 3 : " in outcome.receipt["warnings"]


# --- secrets ----------------------------------------------------------------


def test_a_secret_in_a_check_detail_blocks_publication(tmp_path: Path) -> None:
    """The receipt is scanned before it is written, not after it is published."""
    output_dir = tmp_path / "run"
    work_dir = work_directory_for(output_dir)
    secret = "sk-proj-" + "A" * 32

    def _finalize_with_secret(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        assert results
        return RunChecks(
            checks=(CheckResult(step="extract", name="provider_reachable", status="pass", detail=f"used {secret}"),),
            access_control={"scope": "public", "token": secret},
            warnings=(f"retried with {secret}",),
        )

    outcome = execute_run(
        _plan(),
        output_dir,
        items=_items("w1"),
        execute=_CountingExecutor(),
        finalize=_finalize_with_secret,
    )

    assert outcome.final_state == "fail"
    assert not output_dir.exists(), "a run carrying a secret never publishes"
    assert outcome.run_directory == work_dir
    receipt_path = work_dir / "receipt.json"
    assert receipt_path.is_file(), "the failure receipt still names where the run stopped"
    assert scan_file_for_secrets(receipt_path) == (), "the secret never reaches disk"
    assert scan_tree_for_secrets(work_dir, exclude=()).match_count == 0
    assert secret not in receipt_path.read_text(encoding="utf-8")

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["final_state"] == "fail"
    assert receipt["publication_eligible"] is False
    assert any("secret-like content appears in the assembled receipt" in failure for failure in receipt["failures"])
    assert any(check["name"] == "receipt_secret_scan" and check["status"] == "fail" for check in receipt["checks"])
    details = [check["detail"] for check in receipt["checks"]]
    assert REDACTED in details, "the offending detail is replaced whole"


# --- crash safety -----------------------------------------------------------


def test_a_finalize_that_raises_leaves_a_failure_receipt(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    work_dir = work_directory_for(output_dir)

    def _finalize_that_raises(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        raise RuntimeError("metrics exploded")

    with pytest.raises(RuntimeError):
        execute_run(
            _plan(),
            output_dir,
            items=_items("w1"),
            execute=_CountingExecutor(),
            finalize=_finalize_that_raises,
        )

    assert not output_dir.exists()
    receipt = json.loads((work_dir / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["final_state"] == "fail"
    assert receipt["publication_eligible"] is False
    assert any("RuntimeError: metrics exploded" in failure for failure in receipt["failures"])
    assert any(check["name"] == "run_completed" and check["status"] == "fail" for check in receipt["checks"])
    assert WorkCheckpoint(work_dir / "transitions.jsonl").get("w1") is not None, "finished work survives for resume"


def test_a_failure_receipt_never_carries_the_secret_that_crashed_the_run(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    work_dir = work_directory_for(output_dir)

    def _finalize_that_leaks(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        raise RuntimeError("provider rejected sk-proj-" + "A" * 32)

    with pytest.raises(RuntimeError):
        execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize_that_leaks)

    assert scan_file_for_secrets(work_dir / "receipt.json") == ()


# --- run files stay inside the run -----------------------------------------


def test_run_files_never_escape_through_a_symlink(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    (root / "escape.json").symlink_to(outside / "escape.json")
    workspace = RunWorkspace(plan=_plan(), path=root, checkpoint=WorkCheckpoint(root / "transitions.jsonl"))

    with pytest.raises(RunDirectoryError):
        workspace.write_json("escape/data.json", {"rows": 1})
    with pytest.raises(RunDirectoryError):
        workspace.write_text("escape.json", "{}")
    with pytest.raises(RunDirectoryError):
        workspace.file("../outside/data.json")
    with pytest.raises(RunDirectoryError):
        workspace.file(str(outside / "data.json"))

    assert list(outside.iterdir()) == [], "nothing was written outside the run directory"
    assert workspace.write_json("output/summary.json", {"rows": 1}).is_file(), "ordinary nested paths still work"


# --- earlier runs -----------------------------------------------------------


def _earlier_run(tmp_path: Path) -> Path:
    outcome = execute_run(
        _plan(),
        tmp_path / "earlier",
        items=_items("w1"),
        execute=_CountingExecutor(),
        finalize=_finalize,
    )
    assert outcome.final_state == "pass"
    return outcome.run_directory


def test_a_declared_earlier_run_is_checked_before_the_run_starts(tmp_path: Path) -> None:
    earlier = _earlier_run(tmp_path)
    declared = {
        "source": {
            "run_directory": str(earlier),
            "run_id": "docpipeline-0001",
            "files": {"output/summary.json": file_inventory(earlier)["output/summary.json"]["sha256"]},
        }
    }

    outcome = execute_run(
        _plan(run_id="docpipeline-0002", earlier_runs=declared),
        tmp_path / "later",
        items=_items("w1"),
        execute=_CountingExecutor(),
        finalize=_finalize,
    )
    assert outcome.final_state == "pass"
    assert outcome.receipt["inputs"]["earlier_runs"] == declared


def test_a_changed_earlier_run_stops_the_run_before_any_work(tmp_path: Path) -> None:
    earlier = _earlier_run(tmp_path)
    declared = {"source": {"run_directory": str(earlier), "run_id": "docpipeline-0001"}}
    (earlier / "output" / "summary.json").write_text('{"rows": [{"value": "tampered"}]}\n', encoding="utf-8")

    executor = _CountingExecutor()
    with pytest.raises(RunDirectoryError):
        execute_run(
            _plan(run_id="docpipeline-0002", earlier_runs=declared),
            tmp_path / "later",
            items=_items("w1"),
            execute=executor,
            finalize=_finalize,
        )
    assert executor.calls == [], "no work starts on an unchecked earlier run"
    assert not work_directory_for(tmp_path / "later").exists()


def test_an_earlier_run_with_a_wrong_declared_digest_is_refused(tmp_path: Path) -> None:
    earlier = _earlier_run(tmp_path)
    declared = {"source": {"run_directory": str(earlier), "files": {"output/summary.json": "9" * 64}}}
    with pytest.raises(RunDirectoryError):
        execute_run(
            _plan(run_id="docpipeline-0002", earlier_runs=declared),
            tmp_path / "later",
            items=_items("w1"),
            execute=_CountingExecutor(),
        )


def test_an_earlier_run_that_names_no_directory_is_refused(tmp_path: Path) -> None:
    with pytest.raises(RunDirectoryError):
        execute_run(
            _plan(run_id="docpipeline-0002", earlier_runs={"source": {"run_id": "docpipeline-0001"}}),
            tmp_path / "later",
            items=_items("w1"),
            execute=_CountingExecutor(),
        )


# --- resume ----------------------------------------------------------------


def test_resume_reuses_finished_work_and_retries_only_incomplete_work(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    first = _CountingExecutor({"w3": "raise"})
    failed = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2", "w3"),
        execute=first,
        finalize=_finalize,
    )
    assert failed.final_state == "fail"
    assert first.calls == ["w1", "w2", "w3"]
    assert first.provider_calls == 2

    second = _CountingExecutor()
    resumed = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2", "w3"),
        execute=second,
        finalize=_finalize,
    )

    assert second.calls == ["w3"], "finished work is never re-executed"
    assert second.provider_calls == 1, "resume does not repeat finished paid work"
    assert resumed.final_state == "pass"
    assert resumed.receipt["counts"]["completed"] == 3
    assert not work_directory_for(output_dir).exists()
    transitions = WorkCheckpoint(output_dir / "transitions.jsonl").transitions()
    assert [record["state"] for record in transitions] == [
        "completed",
        "completed",
        "failed",
        "completed",
    ]


def test_resume_refuses_to_drop_a_planned_item(tmp_path: Path) -> None:
    """A failure cannot be made to disappear by planning less work."""
    output_dir = tmp_path / "run"
    failed = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2"),
        execute=_CountingExecutor({"w2": "failed"}),
        finalize=_finalize,
    )
    assert failed.final_state == "fail"

    with pytest.raises(PlanError) as raised:
        execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)

    assert "w2" in str(raised.value)
    assert not output_dir.exists(), "the run that hid a failure never published"
    work_dir = work_directory_for(output_dir)
    assert [record["work_id"] for record in json.loads((work_dir / "planned-work.json").read_text())] == ["w1", "w2"]
    dropped = WorkCheckpoint(work_dir / "transitions.jsonl").get("w2")
    assert dropped is not None and dropped["state"] == "failed", "the failure it tried to drop is still on the record"


def test_resume_refuses_to_add_work_the_first_attempt_never_planned(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2"),
        execute=_CountingExecutor({"w2": "failed"}),
        finalize=_finalize,
    )
    with pytest.raises(PlanError) as raised:
        execute_run(
            _plan(),
            output_dir,
            items=_items("w1", "w2", "w3"),
            execute=_CountingExecutor(),
            finalize=_finalize,
        )
    assert "w3" in str(raised.value)


def test_resume_refuses_a_changed_payload_for_already_planned_work(tmp_path: Path) -> None:
    """A reused result must belong to the payload the item now carries."""
    output_dir = tmp_path / "run"
    first = (
        WorkItem(work_id="w1", step="extract", task="task-w1", payload={"prompt": "v1"}),
        WorkItem(work_id="w2", step="extract", task="task-w2", payload={"prompt": "v1"}),
    )
    failed = execute_run(
        _plan(),
        output_dir,
        items=first,
        execute=_CountingExecutor({"w2": "failed"}),
        finalize=_finalize,
    )
    assert failed.final_state == "fail"

    second = (
        WorkItem(work_id="w1", step="extract", task="task-w1", payload={"prompt": "v2"}),
        WorkItem(work_id="w2", step="extract", task="task-w2", payload={"prompt": "v1"}),
    )
    executor = _CountingExecutor()
    with pytest.raises(PlanError) as raised:
        execute_run(_plan(), output_dir, items=second, execute=executor, finalize=_finalize)

    assert "w1" in str(raised.value)
    assert "payload" in str(raised.value)
    assert executor.calls == [], "no work runs against a plan that changed under it"
    stored = json.loads((work_directory_for(output_dir) / "planned-work.json").read_text(encoding="utf-8"))
    assert stored[0]["payload_sha256"] == first[0].payload_digest, "the stored digest still describes the run"


# --- validation ------------------------------------------------------------


def test_validation_recompute_catches_a_tampered_file(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)
    assert validate_run(output_dir, recompute=_recompute)["status"] == "pass"

    target = output_dir / "extraction" / "calls" / "w1" / "response.json"
    target.write_text(json.dumps({"work_id": "w1", "rows": [{"value": "tampered"}]}), encoding="utf-8")

    report = validate_run(output_dir, plan=_plan(), recompute=_recompute)
    assert report["status"] == "fail"
    assert report["integrity_status"] == "fail"
    assert any("extraction/calls/w1/response.json" in failure for failure in report["integrity_failures"])
    assert any("output/summary.json" in failure for failure in report["integrity_failures"])


def test_validation_does_not_trust_the_receipt(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1", "w2"), execute=_CountingExecutor({"w2": "failed"}))
    work_dir = work_directory_for(output_dir)
    receipt_path = work_dir / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["counts"]["failed"] = 0
    receipt["counts"]["completed"] = 2
    receipt["final_state"] = "pass"
    receipt["failures"] = []
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = validate_run(work_dir)
    assert report["integrity_status"] == "fail"
    assert any("receipt" in failure and "hash" in failure for failure in report["integrity_failures"])
    assert any("counts" in failure for failure in report["integrity_failures"])
    assert report["quality_status"] == "fail", "the recomputed required failure still blocks the run"


def test_validation_reports_a_declared_plan_mismatch(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor())
    report = validate_run(output_dir, plan=_plan(mode="benchmark"))
    assert report["integrity_status"] == "fail"
    assert any("plan" in failure for failure in report["integrity_failures"])


def test_validation_reports_secret_material_found_after_the_run(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor())
    (output_dir / "extraction" / "calls" / "w1" / "response.json").write_text(
        json.dumps({"work_id": "w1", "rows": [], "key": "sk-proj-" + "A" * 32}),
        encoding="utf-8",
    )
    report = validate_run(output_dir)
    assert report["secret_match_count"] == 1
    assert report["status"] == "fail"
    assert any("secret" in failure for failure in report["quality_failures"])


# --- rebuild ---------------------------------------------------------------


def test_rebuild_recomputes_derived_files_without_invoking_a_provider(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1", "w2"), execute=_CountingExecutor(), finalize=_finalize)
    before = file_inventory(output_dir)

    provider = _ForbiddenProvider()

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        assert plan.run_id
        rows: list[dict[str, Any]] = []
        for work_id in ("w1", "w2"):
            # The stored response, or the provider: a rebuild must never need
            # the second branch, and calling it fails the test.
            path = workspace.path / "extraction" / "calls" / work_id / "response.json"
            payload = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else provider(work_id)
            rows.extend(payload["rows"])
        recomputed = {"rows": sorted(rows, key=lambda row: str(row["value"]))}
        assert recomputed["rows"], "the rebuild reads the stored provider responses"
        workspace.write_json("output/summary.json", recomputed)
        workspace.write_json("output/rebuild-summary.json", {"row_count": len(recomputed["rows"])})
        return RunChecks(
            checks=(CheckResult(step="extract", name="summary_written", status="pass"),),
            access_control={"scope": "public", "violations": 0},
            provider_totals=ProviderTotals(),
        )

    rebuilt_dir = tmp_path / "rebuilt"
    report = rebuild_run(output_dir, rebuilt_dir, rebuild=_rebuild)

    assert provider.calls == 0, "rebuild never calls a provider"
    assert report["status"] == "pass"
    assert report["provider_invoked"] is False
    receipt = json.loads((rebuilt_dir / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["rebuild"]["provider_invoked"] is False
    assert receipt["rebuild"]["source_run_directory"] == str(output_dir.resolve())
    assert receipt["provider"]["calls"] == 2, "historical provider totals stay recorded"
    assert json.loads((rebuilt_dir / "output" / "summary.json").read_text(encoding="utf-8")) == _summary(output_dir)
    assert json.loads((rebuilt_dir / "output" / "rebuild-summary.json").read_text(encoding="utf-8")) == {"row_count": 2}
    assert not (output_dir / "output" / "rebuild-summary.json").exists()
    assert file_inventory(output_dir) == before, "the historical run is never mutated"
    assert validate_run(rebuilt_dir, recompute=_recompute)["status"] == "pass"


def test_rebuild_refuses_to_change_historical_provider_files(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)
    before = file_inventory(output_dir)

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        workspace.write_json("extraction/calls/w1/response.json", {"work_id": "w1", "rows": []})
        return RunChecks()

    rebuilt_dir = tmp_path / "rebuilt"
    with pytest.raises(RunDirectoryError):
        rebuild_run(output_dir, rebuilt_dir, rebuild=_rebuild)
    assert not rebuilt_dir.exists()
    assert file_inventory(output_dir) == before


def test_rebuild_refuses_a_reported_provider_call(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        return RunChecks(provider_totals=ProviderTotals(calls=1, total_tokens=5))

    with pytest.raises(RunDirectoryError):
        rebuild_run(output_dir, tmp_path / "rebuilt", rebuild=_rebuild)


def test_rebuild_refuses_a_hook_that_touches_the_source_run(tmp_path: Path) -> None:
    """The source run is sampled before the hook, so corruption cannot hide."""
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)
    before = file_inventory(output_dir, exclude=())

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        (output_dir / "output" / "summary.json").write_text('{"rows": [{"value": "rewritten"}]}\n', encoding="utf-8")
        workspace.write_json("output/summary.json", _summary(workspace.path))
        return RunChecks(checks=(CheckResult(step="extract", name="summary_written", status="pass"),))

    rebuilt_dir = tmp_path / "rebuilt"
    with pytest.raises(RunDirectoryError) as raised:
        rebuild_run(output_dir, rebuilt_dir, rebuild=_rebuild)

    assert "output/summary.json" in str(raised.value)
    assert "source run" in str(raised.value)
    assert not rebuilt_dir.exists()
    assert set(file_inventory(output_dir, exclude=())) == set(before), "no source file was added or removed"


def test_rebuild_never_destroys_a_directory_that_shares_its_staging_name(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)

    rebuilt_dir = tmp_path / "rebuilt"
    bystander = tmp_path / f".{rebuilt_dir.name}.rebuild"
    bystander.mkdir()
    (bystander / "notes.txt").write_text("work in progress\n", encoding="utf-8")

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        workspace.write_json("output/summary.json", _summary(workspace.path))
        return RunChecks(checks=(CheckResult(step="extract", name="summary_written", status="pass"),))

    assert rebuild_run(output_dir, rebuilt_dir, rebuild=_rebuild)["status"] == "pass"
    assert (bystander / "notes.txt").read_text(encoding="utf-8") == "work in progress\n"
    assert [path.name for path in tmp_path.iterdir() if path.name.startswith(".rebuilt.rebuild-")] == []


def test_rebuild_refuses_a_run_that_fails_integrity(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1"), execute=_CountingExecutor(), finalize=_finalize)
    (output_dir / "output" / "summary.json").write_text('{"rows": [{"value": "tampered"}]}\n', encoding="utf-8")

    def _rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        raise AssertionError("rebuild must not start on a broken run")

    with pytest.raises(RunDirectoryError):
        rebuild_run(output_dir, tmp_path / "rebuilt", rebuild=_rebuild)

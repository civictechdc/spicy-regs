"""Contracts for checking a v3 run without trusting anything it says.

``validate_run`` recomputes a run. These tests forge receipts, plant secrets,
tear the work history, and delete run files, and require that checking reports
the problem, names it, and never writes to the run it is checking.

Run execution and rebuild live in ``test_docpipeline_runtime_runs.py``;
primitives live in ``test_docpipeline_runtime.py``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.runtime import (
    CheckResult,
    ProviderTotals,
    RunChecks,
    RunPlan,
    RunWorkspace,
    WorkItem,
    WorkResult,
    canonical_json,
    execute_run,
    sha256_text,
    validate_run,
)


def _plan(**overrides: Any) -> RunPlan:
    values: dict[str, Any] = {
        "run_id": "docpipeline-0001",
        "mode": "build",
        "steps": ("extract",),
        "source_snapshot": {"snapshot_id": "snap-1"},
        "rulespec": {"version": "0.4.0"},
        "profiles": {"regulations-document-v2": {"access": "public"}},
        "vocabulary": {"scheme": "local"},
        "segmentation": {"rule": "structure-overlap-1800"},
        "retrieval": {"methods": ["sparse"]},
        "extraction": {"prompt_sha256": "1" * 64},
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


def _execute(workspace: RunWorkspace, item: WorkItem) -> WorkResult:
    workspace.write_json(f"extraction/calls/{item.work_id}/response.json", {"rows": [{"value": item.task}]})
    return WorkResult.completed(
        item.work_id,
        step=item.step,
        task=item.task,
        result={"rows": 1},
        provider=ProviderTotals(calls=1, seconds=0.25, total_tokens=10),
    )


def _finalize(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
    workspace.write_json("output/summary.json", {"completed": len(results)})
    return RunChecks(checks=(CheckResult(step="extract", name="summary_written", status="pass"),))


def _passing_run(tmp_path: Path, **plan_overrides: Any) -> Path:
    output_dir = tmp_path / "run"
    outcome = execute_run(
        _plan(**plan_overrides),
        output_dir,
        items=_items("w1", "w2"),
        execute=_execute,
        finalize=_finalize,
    )
    assert outcome.final_state == "pass"
    return outcome.run_directory


def _forge(run_dir: Path, mutate: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    """Rewrite the receipt the way a careful editor would: with a fresh hash."""
    path = run_dir / "receipt.json"
    receipt = json.loads(path.read_text(encoding="utf-8"))
    mutate(receipt)
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    body["receipt_sha256"] = sha256_text(canonical_json(body))
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    assert body["receipt_sha256"] == sha256_text(
        canonical_json({key: value for key, value in body.items() if key != "receipt_sha256"})
    ), "the forgery is self-consistent, which is exactly why the hash alone proves nothing"
    return body


# --- forged receipts -------------------------------------------------------


def _set_mode(receipt: dict[str, Any]) -> None:
    receipt["mode"] = "benchmark"


def _set_run_id(receipt: dict[str, Any]) -> None:
    receipt["run_id"] = "docpipeline-9999"


def _set_steps(receipt: dict[str, Any]) -> None:
    receipt["steps"] = ["source", "extract", "approve"]


def _set_code_commit(receipt: dict[str, Any]) -> None:
    receipt["versions"]["code_commit"] = "9" * 40


def _set_earlier_runs(receipt: dict[str, Any]) -> None:
    receipt["inputs"]["earlier_runs"] = {"source": {"run_id": "run-that-never-ran"}}


def _set_source_snapshot(receipt: dict[str, Any]) -> None:
    receipt["inputs"]["source_snapshot"] = {"snapshot_id": "snap-9"}


def _set_benchmark_eligible(receipt: dict[str, Any]) -> None:
    receipt["benchmark_eligible"] = True


def _set_provider_total(receipt: dict[str, Any]) -> None:
    receipt["provider"]["calls"] = 99
    receipt["provider"]["total_tokens"] = 99


def _set_provider_sources(receipt: dict[str, Any]) -> None:
    receipt["provider"]["calls"] = 99
    receipt["provider_sources"]["work"]["calls"] = 99


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (_set_mode, "the receipt mode does not match the plan"),
        (_set_run_id, "the receipt run_id does not match the plan"),
        (_set_steps, "the receipt steps does not match the plan"),
        (_set_code_commit, "the receipt versions.code_commit does not match the plan"),
        (_set_earlier_runs, "the receipt inputs.earlier_runs does not match the plan"),
        (_set_source_snapshot, "the receipt inputs.source_snapshot does not match the plan"),
        (_set_benchmark_eligible, "the receipt benchmark_eligible does not match the plan"),
        (_set_provider_total, "the receipt provider block does not match its own recorded sources"),
        (_set_provider_sources, "the receipt provider totals do not match the summed work records"),
    ],
)
def test_validation_refuses_a_forged_receipt_even_with_a_fresh_hash(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    expected: str,
) -> None:
    run_dir = _passing_run(tmp_path)
    assert validate_run(run_dir, plan=_plan())["status"] == "pass"

    forged = _forge(run_dir, mutate)
    report = validate_run(run_dir, plan=_plan())

    assert report["status"] == "fail"
    assert report["integrity_status"] == "fail"
    assert expected in report["integrity_failures"], report["integrity_failures"]
    assert (
        forged["receipt_sha256"] == json.loads((run_dir / "receipt.json").read_text(encoding="utf-8"))["receipt_sha256"]
    )


def test_validation_refuses_a_receipt_that_claims_publication_eligibility(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path, mode="diagnostic")
    assert validate_run(run_dir, plan=_plan(mode="diagnostic"))["status"] == "pass"

    _forge(run_dir, lambda receipt: receipt.update({"publication_eligible": True}))
    report = validate_run(run_dir, plan=_plan(mode="diagnostic"))
    assert "the receipt publication_eligible does not match the plan" in report["integrity_failures"]


def test_validation_refuses_a_receipt_carrying_versions_the_plan_never_declared(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    _forge(run_dir, lambda receipt: receipt["versions"].update({"secret_sauce": "v9"}))
    report = validate_run(run_dir, plan=_plan())
    assert any("carries fields the plan does not declare" in failure for failure in report["integrity_failures"])


def test_validation_recomputes_the_plan_facts_from_the_stored_plan_alone(tmp_path: Path) -> None:
    """No declared plan is needed: the stored plan is the reference."""
    run_dir = _passing_run(tmp_path)
    _forge(run_dir, _set_mode)
    report = validate_run(run_dir)
    assert "the receipt mode does not match the plan" in report["integrity_failures"]


# --- secrets ---------------------------------------------------------------


def test_validation_reports_a_secret_hiding_in_the_receipt(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    _forge(run_dir, lambda receipt: receipt["warnings"].append("token sk-proj-" + "A" * 32))

    report = validate_run(run_dir, plan=_plan())

    assert report["secret_match_count"] == 1
    assert report["secret_match_files"] == ["receipt.json"]
    assert any("receipt.json" in failure for failure in report["quality_failures"])
    assert report["status"] == "fail"


def test_validation_counts_receipt_and_run_file_secrets_together(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    (run_dir / "extraction" / "calls" / "w1" / "response.json").write_text(
        json.dumps({"rows": [], "key": "sk-proj-" + "B" * 32}), encoding="utf-8"
    )
    _forge(run_dir, lambda receipt: receipt["warnings"].append("token sk-proj-" + "A" * 32))

    report = validate_run(run_dir)
    assert report["secret_match_count"] == 2
    assert sorted(report["secret_match_files"]) == ["extraction/calls/w1/response.json", "receipt.json"]


# --- checking never writes -------------------------------------------------


def test_validation_reports_a_torn_work_history_without_healing_it(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    transitions = run_dir / "transitions.jsonl"
    with transitions.open("a", encoding="utf-8") as handle:
        handle.write('{"work_id": "w3", "state": "comp')
    before = transitions.read_bytes()

    report = validate_run(run_dir, plan=_plan())

    assert report["integrity_status"] == "fail"
    assert any("torn" in failure for failure in report["integrity_failures"])
    assert transitions.read_bytes() == before, "checking a run never rewrites its history"


# --- missing run files -----------------------------------------------------


def test_validation_reports_a_missing_receipt_instead_of_raising(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    (run_dir / "receipt.json").unlink()

    report = validate_run(run_dir, plan=_plan())

    assert report["status"] == "fail"
    assert report["integrity_status"] == "fail"
    assert report["run_state"] is None
    assert any("receipt" in failure for failure in report["integrity_failures"])


def test_validation_reports_missing_planned_work_instead_of_raising(tmp_path: Path) -> None:
    run_dir = _passing_run(tmp_path)
    (run_dir / "planned-work.json").unlink()

    report = validate_run(run_dir, plan=_plan())

    assert report["status"] == "fail"
    assert any("planned work" in failure for failure in report["integrity_failures"])


# --- derived artifacts -----------------------------------------------------


def _parquet_rows() -> list[dict[str, Any]]:
    return [{"work_id": "w1", "value": "task-w1"}, {"work_id": "w2", "value": "task-w2"}]


def _finalize_parquet(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
    assert results
    pq.write_table(pa.Table.from_pylist(_parquet_rows()), workspace.file("output/rows.parquet"))
    workspace.write_json("output/summary.json", {"completed": len(results)})
    return RunChecks(checks=(CheckResult(step="extract", name="rows_written", status="pass"),))


def _recompute_parquet(run_dir: Path, plan: RunPlan) -> dict[str, Any]:
    assert plan.run_id
    return {"output/rows.parquet": _parquet_rows(), "output/summary.json": {"completed": 2}}


def test_validation_recomputes_a_parquet_derived_artifact(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    outcome = execute_run(
        _plan(),
        output_dir,
        items=_items("w1", "w2"),
        execute=_execute,
        finalize=_finalize_parquet,
    )
    assert outcome.final_state == "pass"
    assert validate_run(output_dir, plan=_plan(), recompute=_recompute_parquet)["status"] == "pass"

    tampered = _parquet_rows()
    tampered[1]["value"] = "tampered"
    pq.write_table(pa.Table.from_pylist(tampered), output_dir / "output" / "rows.parquet")

    report = validate_run(output_dir, plan=_plan(), recompute=_recompute_parquet)
    assert report["integrity_status"] == "fail"
    assert "derived artifact output/rows.parquet does not recompute" in report["integrity_failures"]


def test_validation_ignores_parquet_column_order_and_file_metadata(tmp_path: Path) -> None:
    """A Parquet artifact compares as rows, not as bytes."""
    output_dir = tmp_path / "run"
    execute_run(_plan(), output_dir, items=_items("w1", "w2"), execute=_execute, finalize=_finalize_parquet)

    reordered = [{"value": row["value"], "work_id": row["work_id"]} for row in _parquet_rows()]

    def _recompute(run_dir: Path, plan: RunPlan) -> dict[str, Any]:
        assert plan.run_id and run_dir.is_dir()
        return {"output/rows.parquet": reordered}

    assert validate_run(output_dir, recompute=_recompute)["integrity_status"] == "pass"

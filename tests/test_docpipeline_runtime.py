"""Contracts for v3 document pipeline runtime primitives.

Work identity, durable checkpoints, secret scanning, file inventory, plan
records, and item states. Run execution, validation, and rebuild live in
``test_docpipeline_runtime_runs.py``.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline import runtime
from spicy_regs.docpipeline.runtime import (
    ITEM_STATES,
    RUNTIME_FORMAT_VERSION,
    SCAN_CHUNK_BYTES,
    PlanError,
    ProviderTotals,
    RunPlan,
    WorkCheckpoint,
    WorkIdentity,
    WorkResult,
    WorkStateError,
    file_inventory,
    scan_file_for_secrets,
    scan_text_for_secrets,
    scan_tree_for_secrets,
    sha256_text,
)


def _identity(**overrides: Any) -> WorkIdentity:
    values: dict[str, Any] = {
        "step": "extract",
        "task": "relationship-candidates",
        "input_digests": ("a" * 64,),
        "settings": {"max_output_tokens": 4096},
        "prompt_digest": "b" * 64,
        "schema_digest": "c" * 64,
        "provider_config": {"model": "gpt-x", "reasoning_effort": "medium"},
        "prior_run_id": "run-earlier",
    }
    values.update(overrides)
    return WorkIdentity(**values)


def _plan(**overrides: Any) -> RunPlan:
    values: dict[str, Any] = {
        "run_id": "docpipeline-0001",
        "mode": "build",
        "steps": ("source", "extract"),
        "source_snapshot": {"snapshot_id": "snap-1", "files": {"documents.parquet": "d" * 64}},
        "rulespec": {"version": "0.4.0", "schema_sha256": "e" * 64},
        "profiles": {"regulations-document-v2": {"access": "public"}},
        "vocabulary": {"scheme": "local", "snapshot_sha256": "f" * 64},
        "segmentation": {"rule": "structure-overlap-1800", "tokenizer": "cl100k_base"},
        "retrieval": {"methods": ["sparse", "dense"], "rerank_depth": 50},
        "extraction": {"prompt_sha256": "1" * 64, "schema_sha256": "2" * 64},
        "rules": {"approval": "v1", "comparison": "v2"},
        "provider": {"model": "gpt-x", "store": False},
        "review_file_digests": {"answers.json": "3" * 64},
        "code_commit": "0" * 40,
        "required_work": (),
        "optional_work": (),
        "earlier_runs": {},
    }
    values.update(overrides)
    return RunPlan(**values)


# --- work identity ---------------------------------------------------------


def test_work_id_is_stable_and_covers_every_identity_component() -> None:
    base = _identity()
    assert base.work_id == _identity().work_id

    variants = {
        "step": {"step": "approve"},
        "task": {"task": "tag-candidates"},
        "input_digests": {"input_digests": ("9" * 64,)},
        "settings": {"settings": {"max_output_tokens": 8192}},
        "prompt_digest": {"prompt_digest": "8" * 64},
        "schema_digest": {"schema_digest": "7" * 64},
        "provider_config": {"provider_config": {"model": "gpt-y"}},
        "prior_run_id": {"prior_run_id": "run-other"},
    }
    changed = {name: _identity(**override).work_id for name, override in variants.items()}
    assert base.work_id not in changed.values()
    assert len(set(changed.values())) == len(changed), "each component must change the work id"
    assert set(base.components()) == set(variants)


def test_work_identity_requires_a_step_and_task() -> None:
    with pytest.raises(PlanError):
        _identity(step="")
    with pytest.raises(PlanError):
        _identity(task=" ")


# --- checkpoints -----------------------------------------------------------


def test_checkpoint_replays_the_latest_record_for_each_work_id(tmp_path: Path) -> None:
    path = tmp_path / "transitions.jsonl"
    checkpoint = WorkCheckpoint(path)
    checkpoint.append({"work_id": "w1", "state": "failed", "error": "provider timeout"})
    checkpoint.append({"work_id": "w1", "state": "completed", "result": {"rows": 1}})
    checkpoint.append({"work_id": "w2", "state": "completed_empty"})

    reopened = WorkCheckpoint(path)
    latest = reopened.get("w1")
    assert latest is not None and latest["state"] == "completed"
    assert [record["work_id"] for record in reopened.records()] == ["w1", "w2"]
    assert len(reopened.transitions()) == 3, "every transition stays durable"
    assert reopened.state_counts() == {"completed": 1, "completed_empty": 1}


def test_checkpoint_recovers_from_a_torn_final_line(tmp_path: Path) -> None:
    path = tmp_path / "transitions.jsonl"
    checkpoint = WorkCheckpoint(path)
    checkpoint.append({"work_id": "w1", "state": "completed", "result": {"rows": 1}})
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"work_id": "w2", "state": "comp')

    reopened = WorkCheckpoint(path)
    assert reopened.get("w2") is None, "a torn record is not durable state"
    assert reopened.get("w1") is not None
    reopened.append({"work_id": "w2", "state": "completed_empty"})

    again = WorkCheckpoint(path)
    replayed = again.get("w2")
    assert replayed is not None and replayed["state"] == "completed_empty"
    text = path.read_text(encoding="utf-8")
    assert text.endswith("\n")
    assert len(text.splitlines()) == 2, "the torn line was safely replaced"


def test_checkpoint_ignores_an_unparsable_interior_line(tmp_path: Path) -> None:
    path = tmp_path / "transitions.jsonl"
    path.write_text('{"work_id": "w1", "state": "completed", "result": {"rows": 1}}\nnot json\n', encoding="utf-8")
    checkpoint = WorkCheckpoint(path)
    assert checkpoint.get("w1") is not None
    assert len(checkpoint.records()) == 1


def test_checkpoint_protects_against_duplicate_records(tmp_path: Path) -> None:
    path = tmp_path / "transitions.jsonl"
    checkpoint = WorkCheckpoint(path)
    record = {"work_id": "w1", "state": "completed", "result": {"rows": 1}}
    checkpoint.append(record)
    checkpoint.append(dict(record))
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1

    checkpoint.append({"work_id": "w1", "state": "rejected", "reason": "evidence failed"})
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_a_read_only_checkpoint_reports_a_torn_tail_without_touching_the_file(tmp_path: Path) -> None:
    path = tmp_path / "transitions.jsonl"
    checkpoint = WorkCheckpoint(path)
    checkpoint.append({"work_id": "w1", "state": "completed", "result": {"rows": 1}})
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"work_id": "w2", "state": "comp')
    before = path.read_bytes()

    reader = WorkCheckpoint(path, repair=False)

    assert reader.torn_tail is True
    assert reader.get("w1") is not None, "the durable records still replay"
    assert reader.get("w2") is None
    assert path.read_bytes() == before, "reading a run never rewrites it"
    with pytest.raises(WorkStateError):
        reader.append({"work_id": "w3", "state": "completed_empty"})
    assert path.read_bytes() == before


def test_a_read_only_checkpoint_creates_nothing(tmp_path: Path) -> None:
    path = tmp_path / "absent" / "transitions.jsonl"
    reader = WorkCheckpoint(path, repair=False)
    assert reader.records() == []
    assert reader.torn_tail is False
    assert not path.parent.exists(), "checking a run never creates directories in it"


def test_checkpoint_rejects_unusable_records(tmp_path: Path) -> None:
    checkpoint = WorkCheckpoint(tmp_path / "transitions.jsonl")
    with pytest.raises(WorkStateError):
        checkpoint.append({"state": "completed"})
    with pytest.raises(WorkStateError):
        checkpoint.append({"work_id": "w1", "state": "finished"})
    with pytest.raises(WorkStateError):
        checkpoint.append({"work_id": "w1", "state": "completed_empty", "error": "provider timeout"})


# --- item states -----------------------------------------------------------


def test_every_declared_item_state_is_reachable(tmp_path: Path) -> None:
    """Each declared state has a constructor and survives a checkpoint round trip."""
    built = [
        WorkResult.completed("w-completed", step="extract", task="t", result={"rows": 1}),
        WorkResult.completed_empty("w-empty", step="extract", task="t"),
        WorkResult.rejected("w-rejected", step="extract", task="t", reason="evidence failed"),
        WorkResult.skipped("w-skipped", step="extract", task="t", reason="no source"),
        WorkResult.failed("w-failed", step="extract", task="t", error="provider timeout"),
        WorkResult.unknown("w-unknown", step="extract", task="t"),
    ]
    assert {result.state for result in built} == set(ITEM_STATES), "every declared state has a way to be reached"

    path = tmp_path / "transitions.jsonl"
    checkpoint = WorkCheckpoint(path)
    for result in built:
        checkpoint.append(result.as_record())
    assert set(WorkCheckpoint(path).states().values()) == set(ITEM_STATES), "every state survives a replay"
    assert {result.state for result in built if result.settled} == {"completed", "completed_empty", "rejected"}


def test_empty_results_are_successes_distinct_from_failures() -> None:
    empty = WorkResult.completed_empty("w1", step="extract", task="t")
    failed = WorkResult.failed("w2", step="extract", task="t", error="provider timeout")
    assert empty.state == "completed_empty"
    assert empty.settled is True
    assert failed.state == "failed"
    assert failed.settled is False
    assert empty.as_record()["state"] != failed.as_record()["state"]


def test_a_failure_can_never_be_recorded_as_an_empty_success() -> None:
    with pytest.raises(WorkStateError):
        WorkResult.completed_empty("w1", step="extract", task="t", error="provider timeout")
    with pytest.raises(WorkStateError):
        WorkResult.failed("w1", step="extract", task="t", error="")
    with pytest.raises(WorkStateError):
        WorkResult.completed("w1", step="extract", task="t", result={})
    with pytest.raises(WorkStateError):
        WorkResult.rejected("w1", step="extract", task="t", reason="evidence failed", error="provider timeout")


def test_rejected_and_skipped_work_must_explain_itself() -> None:
    with pytest.raises(WorkStateError):
        WorkResult.rejected("w1", step="extract", task="t", reason="")
    with pytest.raises(WorkStateError):
        WorkResult.skipped("w1", step="extract", task="t", reason="")
    assert WorkResult.rejected("w1", step="extract", task="t", reason="evidence failed").settled is True
    assert WorkResult.skipped("w1", step="extract", task="t", reason="no source").settled is False
    assert WorkResult.unknown("w1", step="extract", task="t").settled is False


def test_provider_totals_sum_as_opaque_caller_supplied_numbers() -> None:
    total = ProviderTotals.sum(
        [
            ProviderTotals(calls=1, retries=1, seconds=0.5, total_tokens=10),
            ProviderTotals(calls=2, failures=1, seconds=1.25, total_tokens=30),
        ]
    )
    assert total.as_dict() == {
        "calls": 3,
        "retries": 1,
        "failures": 1,
        "seconds": 1.75,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 40,
    }


# --- secrets ---------------------------------------------------------------


def test_secret_scan_covers_the_regex_the_needles_and_chunk_boundaries(tmp_path: Path) -> None:
    boundary = tmp_path / "boundary.log"
    boundary.write_bytes(b"x" * (SCAN_CHUNK_BYTES - 4) + b"\nsk-proj-" + b"A" * 32 + b"\n")
    assert scan_file_for_secrets(boundary), "a needle split across two chunks must still match"

    (tmp_path / "env.txt").write_text("OPENAI_API_KEY=redacted\n", encoding="utf-8")
    (tmp_path / "header.txt").write_text("Authorization: Bearer sk-live\n", encoding="utf-8")
    (tmp_path / "plain.txt").write_text("token sk-" + "B" * 24 + " end\n", encoding="utf-8")
    (tmp_path / "clean.json").write_text('{"model": "gpt-5", "store": false}\n', encoding="utf-8")

    result = scan_tree_for_secrets(tmp_path)
    assert result.match_count == 4
    assert set(result.files) == {"boundary.log", "env.txt", "header.txt", "plain.txt"}
    assert result.as_dict()["secret_match_count"] == 4
    assert scan_text_for_secrets('{"model": "gpt-5"}') == ()
    assert "openai-key-prefix" in scan_text_for_secrets("sk-" + "C" * 24)


# --- inventory -------------------------------------------------------------


def test_file_inventory_records_relative_path_bytes_rows_and_sha256(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "transitions.jsonl").write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
    (tmp_path / "notes.txt").write_text("hello\n", encoding="utf-8")
    (tmp_path / "receipt.json").write_text("{}\n", encoding="utf-8")
    pq.write_table(pa.table({"x": ["a", "b", "c"]}), tmp_path / "rows.parquet")

    inventory = file_inventory(tmp_path)
    assert set(inventory) == {"nested/transitions.jsonl", "notes.txt", "rows.parquet"}
    assert inventory["nested/transitions.jsonl"]["rows"] == 2
    assert inventory["rows.parquet"]["rows"] == 3
    assert "rows" not in inventory["notes.txt"]
    assert inventory["notes.txt"]["bytes"] == 6
    assert inventory["notes.txt"]["sha256"] == sha256_text("hello\n")


def test_file_inventory_records_an_unreadable_parquet_instead_of_swallowing_it(tmp_path: Path) -> None:
    (tmp_path / "broken.parquet").write_bytes(b"PAR1 not really a parquet file")

    record = file_inventory(tmp_path)["broken.parquet"]

    assert "rows" not in record, "an unreadable table never reports a row count"
    assert record["rows_error"], "the run records that the row count could not be read"
    assert record["sha256"] and record["bytes"] == 30
    assert file_inventory(tmp_path)["broken.parquet"] == record, "the record is stable across inventories"


# --- plan ------------------------------------------------------------------


def test_plan_hash_is_stable_and_reflects_every_recorded_field() -> None:
    base = _plan()
    assert base.plan_hash == _plan().plan_hash
    assert base.as_dict()["format_version"] == RUNTIME_FORMAT_VERSION

    for override in (
        {"run_id": "docpipeline-0002"},
        {"mode": "benchmark"},
        {"steps": ("source",)},
        {"source_snapshot": {"snapshot_id": "snap-2"}},
        {"rulespec": {"version": "0.5.0"}},
        {"profiles": {"other": {}}},
        {"vocabulary": {"scheme": "registered"}},
        {"segmentation": {"rule": "fixed-512"}},
        {"retrieval": {"rerank_depth": 10}},
        {"extraction": {"prompt_sha256": "4" * 64}},
        {"rules": {"approval": "v2"}},
        {"provider": {"model": "gpt-y"}},
        {"review_file_digests": {}},
        {"code_commit": "1" * 40},
        {"required_work": ("relationship-candidates",)},
        {"optional_work": ("tag-candidates",)},
        {"earlier_runs": {"source": {"run_id": "run-0"}}},
    ):
        assert _plan(**override).plan_hash != base.plan_hash, override


def test_plan_round_trips_through_its_record() -> None:
    plan = _plan(optional_work=("tag-candidates",))
    restored = RunPlan.from_dict(plan.as_dict())
    assert restored.plan_hash == plan.plan_hash
    assert restored.is_optional("tag-candidates") is True
    assert restored.is_optional("relationship-candidates") is False


def test_plan_refuses_secret_material() -> None:
    with pytest.raises(PlanError):
        _plan(provider={"model": "gpt-x", "api_key": "sk-proj-" + "A" * 32})


def test_plan_rejects_unknown_modes_steps_and_contradictory_work_declarations() -> None:
    with pytest.raises(PlanError):
        _plan(mode="production")
    with pytest.raises(PlanError):
        _plan(steps=("extract", "source"))
    with pytest.raises(PlanError):
        _plan(steps=("teleport",))
    with pytest.raises(PlanError):
        _plan(steps=())
    with pytest.raises(PlanError):
        _plan(required_work=("t",), optional_work=("t",))


def test_plan_eligibility_follows_the_mode() -> None:
    assert _plan(mode="build").publication_eligible is True
    assert _plan(mode="build").benchmark_eligible is False
    assert _plan(mode="diagnostic").publication_eligible is False
    assert _plan(mode="diagnostic").benchmark_eligible is False
    assert _plan(mode="benchmark").publication_eligible is False
    assert _plan(mode="benchmark").benchmark_eligible is True


# --- import rules ----------------------------------------------------------


def test_runtime_imports_no_pipeline_step_provider_or_replaced_runner() -> None:
    source = Path(str(runtime.__file__)).read_text(encoding="utf-8")
    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)

    forbidden = (
        "spicy_regs.corpora",
        "spicy_regs.ontology.llm",
        "spicy_regs.docpipeline.source",
        "spicy_regs.docpipeline.segments",
        "spicy_regs.docpipeline.retrieval",
        "spicy_regs.docpipeline.extraction",
        "spicy_regs.docpipeline.approval",
        "spicy_regs.docpipeline.comparison",
        "spicy_regs.docpipeline.workflow",
        "spicy_regs.docpipeline.cli",
        "spicy_regs.docpipeline.adapters",
        "duckdb",
        "openai",
        "tiktoken",
        "sentence_transformers",
        "httpx",
    )
    for module in sorted(modules):
        assert not any(module == name or module.startswith(f"{name}.") for name in forbidden), module
    assert {module for module in modules if module.startswith("spicy_regs")} == {"spicy_regs.ontology.common"}

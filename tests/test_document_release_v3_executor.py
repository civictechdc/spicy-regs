"""Recovery and conditional-finalization tests for the v3 local executor."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from spicy_regs.docpipeline.executor import (
    AttemptContext,
    ConditionalTaskStore,
    FailureClass,
    LocalExecutor,
    ResourceDeclaration,
    RetryPolicy,
    TaskExecutionError,
    TaskOutput,
    TaskSpec,
)


def _task(*, memory_bytes: int = 1024, max_attempts: int = 3) -> TaskSpec:
    return TaskSpec(
        stage_identity="document-release-v3-partition",
        input_digests=("0" * 64,),
        implementation_identity="spicyregs-test-implementation",
        configuration_identity="spicyregs-test-configuration",
        policy_identity="spicyregs-test-policy",
        resources=ResourceDeclaration(
            memory_bytes=memory_bytes,
            temporary_disk_bytes=1024,
            object_count=1,
        ),
        timeout_seconds=5,
        retry=RetryPolicy(max_attempts=max_attempts, base_delay_seconds=0, max_delay_seconds=0),
    )


def test_transient_attempt_retries_then_committed_output_is_reused(tmp_path: Path) -> None:
    store = ConditionalTaskStore(tmp_path / "tasks")
    executor = LocalExecutor(store, max_workers=1, sleep=lambda _delay: None)
    attempts = 0

    def handler(context: AttemptContext) -> TaskOutput:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TaskExecutionError(
                FailureClass.TRANSIENT_EXTERNAL,
                "spicyregs.acquire.temporary-unavailable",
                "temporary fixture failure",
            )
        (context.workspace / "member.bin").write_bytes(b"complete immutable output")
        return TaskOutput(object_keys=("member.bin",), metrics={"rows": 1})

    first = executor.execute([_task()], handler)

    assert attempts == 2
    assert first[0].reused is False
    assert first[0].metrics == {"rows": 1}

    def must_not_run(_context: AttemptContext) -> TaskOutput:
        raise AssertionError("verified committed work should be reused")

    second = executor.execute([_task()], must_not_run)
    assert second[0].reused is True
    assert second[0].attempt_id == first[0].attempt_id


def test_duplicate_or_stale_attempt_cannot_replace_first_commit(tmp_path: Path) -> None:
    store = ConditionalTaskStore(tmp_path / "tasks")
    task = _task()
    cancel = threading.Event()
    first_context = AttemptContext(
        task, "attempt-first", 1, store.attempt_workspace(task.task_key, "attempt-first"), cancel
    )
    second_context = AttemptContext(
        task, "attempt-stale", 2, store.attempt_workspace(task.task_key, "attempt-stale"), cancel
    )
    (first_context.workspace / "member.bin").write_bytes(b"first")
    (second_context.workspace / "member.bin").write_bytes(b"stale")

    first = store.commit(first_context, TaskOutput(("member.bin",)))
    stale = store.commit(second_context, TaskOutput(("member.bin",)))

    assert first.attempt_id == "attempt-first"
    assert stale.attempt_id == "attempt-first"
    assert (stale.output_root / "member.bin").read_bytes() == b"first"


def test_corrupt_committed_output_fails_closed(tmp_path: Path) -> None:
    store = ConditionalTaskStore(tmp_path / "tasks")
    executor = LocalExecutor(store, max_workers=1)

    def handler(context: AttemptContext) -> TaskOutput:
        (context.workspace / "member.bin").write_bytes(b"verified")
        return TaskOutput(("member.bin",))

    committed = executor.execute([_task()], handler)[0]
    (committed.output_root / "member.bin").write_bytes(b"corrupt")

    with pytest.raises(TaskExecutionError) as error:
        executor.execute([_task()], handler)
    assert error.value.failure_class is FailureClass.ARTIFACT_INTEGRITY


def test_resource_declaration_enforces_backpressure_budget(tmp_path: Path) -> None:
    executor = LocalExecutor(
        ConditionalTaskStore(tmp_path / "tasks"),
        max_workers=1,
        max_memory_bytes=100,
    )

    with pytest.raises(TaskExecutionError) as error:
        executor.execute([_task(memory_bytes=101)], lambda _context: TaskOutput(("member.bin",)))
    assert error.value.failure_class is FailureClass.TRANSIENT_RESOURCE


def test_deterministic_failure_is_not_retried(tmp_path: Path) -> None:
    executor = LocalExecutor(ConditionalTaskStore(tmp_path / "tasks"), max_workers=1)
    attempts = 0

    def handler(_context: AttemptContext) -> TaskOutput:
        nonlocal attempts
        attempts += 1
        raise TaskExecutionError(
            FailureClass.DETERMINISTIC_INPUT,
            "spicyregs.normalize.invalid-input",
            "same input always fails",
        )

    with pytest.raises(TaskExecutionError):
        executor.execute([_task(max_attempts=5)], handler)
    assert attempts == 1

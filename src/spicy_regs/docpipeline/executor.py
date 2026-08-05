"""Bounded, idempotent execution interface for SpicyRegs batch work.

The local executor and its task store define the behavior a distributed engine
must preserve: stable task keys, finite retries, bounded in-flight work,
attempt-scoped output, digest verification, and one conditional commit record.
The commit record is the visibility point; duplicate or stale attempts remain
invisible even when workers deliver a task more than once.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from spicy_regs.document_release_v3 import (
    DocumentReleaseV3Error,
    canonical_json_bytes,
    parse_canonical_json,
    require_sha256,
    sha256_file,
    validate_object_key,
)


class FailureClass(StrEnum):
    """Normative producer failure classes from the scale specification."""

    TRANSIENT_EXTERNAL = "transient-external"
    TRANSIENT_RESOURCE = "transient-resource"
    DETERMINISTIC_INPUT = "deterministic-input"
    POLICY_EXCLUSION = "policy-exclusion"
    ARTIFACT_INTEGRITY = "artifact-integrity"
    IMPLEMENTATION_DEFECT = "implementation-defect"


RETRYABLE_FAILURES = frozenset({FailureClass.TRANSIENT_EXTERNAL, FailureClass.TRANSIENT_RESOURCE})


class TaskExecutionError(RuntimeError):
    """One attempt failed with an explicit retry classification."""

    def __init__(self, failure_class: FailureClass, diagnostic_code: str, detail: str) -> None:
        super().__init__(detail)
        self.failure_class = failure_class
        self.diagnostic_code = diagnostic_code
        self.detail = detail


class TaskCancelledError(TaskExecutionError):
    """Execution stopped through the shared cancellation signal."""

    def __init__(self) -> None:
        super().__init__(FailureClass.TRANSIENT_RESOURCE, "spicyregs.executor.cancelled", "task was cancelled")


@dataclass(frozen=True, slots=True)
class ResourceDeclaration:
    """Estimated per-attempt resources used for admission and backpressure."""

    memory_bytes: int
    temporary_disk_bytes: int
    object_count: int
    external_request_budget: int = 0

    def __post_init__(self) -> None:
        if any(value < 0 for value in self.as_dict().values()):
            raise DocumentReleaseV3Error("task resource declarations must be non-negative")

    def as_dict(self) -> dict[str, int]:
        return {
            "memoryBytes": self.memory_bytes,
            "temporaryDiskBytes": self.temporary_disk_bytes,
            "objectCount": self.object_count,
            "externalRequestBudget": self.external_request_budget,
        }


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Finite exponential retry policy with bounded deterministic jitter."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 30.0
    jitter_fraction: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise DocumentReleaseV3Error("retry max_attempts must be greater than zero")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise DocumentReleaseV3Error("retry delays must be non-negative")
        if not 0 <= self.jitter_fraction <= 1:
            raise DocumentReleaseV3Error("retry jitter_fraction must be between zero and one")

    def delay(self, task_key: str, attempt_number: int) -> float:
        base = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** max(0, attempt_number - 1)))
        seed = int(hashlib.sha256(f"{task_key}:{attempt_number}".encode()).hexdigest()[:16], 16)
        unit = random.Random(seed).uniform(-self.jitter_fraction, self.jitter_fraction)
        return max(0.0, base * (1 + unit))


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """One idempotent bounded task with all identity-bearing inputs."""

    stage_identity: str
    input_digests: tuple[str, ...]
    implementation_identity: str
    configuration_identity: str
    policy_identity: str
    resources: ResourceDeclaration
    timeout_seconds: float
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value
            for value in (
                self.stage_identity,
                self.implementation_identity,
                self.configuration_identity,
                self.policy_identity,
            )
        ):
            raise DocumentReleaseV3Error("task identity fields must be non-empty strings")
        for digest in self.input_digests:
            require_sha256(digest, "task input digest")
        if tuple(sorted(self.input_digests)) != self.input_digests:
            raise DocumentReleaseV3Error("task input digests must be sorted")
        if self.timeout_seconds <= 0:
            raise DocumentReleaseV3Error("task timeout_seconds must be greater than zero")
        canonical_json_bytes(dict(self.payload))

    @property
    def task_key(self) -> str:
        identity = {
            "stageIdentity": self.stage_identity,
            "inputDigests": list(self.input_digests),
            "implementationIdentity": self.implementation_identity,
            "configurationIdentity": self.configuration_identity,
            "policyIdentity": self.policy_identity,
        }
        return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()


@dataclass(frozen=True, slots=True)
class AttemptContext:
    """Attempt-specific workspace and cooperative cancellation state."""

    task: TaskSpec
    attempt_id: str
    attempt_number: int
    workspace: Path
    cancel_event: threading.Event

    def raise_if_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise TaskCancelledError()


@dataclass(frozen=True, slots=True)
class TaskOutput:
    """Immutable output files produced below an attempt workspace."""

    object_keys: tuple[str, ...]
    metrics: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.object_keys:
            raise DocumentReleaseV3Error("a successful task must produce at least one output")
        for key in self.object_keys:
            validate_object_key(key, "task output key")
        if tuple(sorted(self.object_keys)) != self.object_keys or len(set(self.object_keys)) != len(self.object_keys):
            raise DocumentReleaseV3Error("task output keys must be sorted and distinct")
        if any(not isinstance(value, int) or value < 0 for value in self.metrics.values()):
            raise DocumentReleaseV3Error("task metrics must be non-negative integers")


@dataclass(frozen=True, slots=True)
class CommittedTask:
    """Verified conditional commit selected for a task key."""

    task_key: str
    attempt_id: str
    output_root: Path
    outputs: tuple[Mapping[str, Any], ...]
    metrics: Mapping[str, int]
    reused: bool


@dataclass(frozen=True, slots=True)
class ExecutorEvent:
    """Secret-free progress or metric event."""

    event: str
    task_key: str
    attempt_id: str | None
    attempt_number: int
    detail: str
    metrics: Mapping[str, int] = field(default_factory=dict)


class Executor(Protocol):
    """Interface shared by local and maintained distributed adapters."""

    def execute(
        self,
        tasks: Iterable[TaskSpec],
        handler: Callable[[AttemptContext], TaskOutput],
        *,
        cancel_event: threading.Event | None = None,
        on_event: Callable[[ExecutorEvent], None] | None = None,
    ) -> tuple[CommittedTask, ...]: ...


class ConditionalTaskStore:
    """Attempt storage with an atomic, no-replacement commit record."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.attempts = self.root / "attempts"
        self.commits = self.root / "commits"
        self.attempts.mkdir(parents=True, exist_ok=True)
        self.commits.mkdir(parents=True, exist_ok=True)

    def attempt_workspace(self, task_key: str, attempt_id: str) -> Path:
        path = self.attempts / task_key / attempt_id
        path.mkdir(parents=True, exist_ok=False)
        return path

    def _commit_path(self, task_key: str) -> Path:
        return self.commits / f"{task_key}.json"

    def load(self, task_key: str) -> CommittedTask | None:
        path = self._commit_path(task_key)
        if not path.exists():
            return None
        try:
            value = parse_canonical_json(path.read_bytes(), label=path.name)
        except DocumentReleaseV3Error as error:
            raise TaskExecutionError(
                FailureClass.ARTIFACT_INTEGRITY,
                "spicyregs.executor.commit-corrupt",
                str(error),
            ) from error
        if not isinstance(value, dict) or set(value) != {"attemptId", "metrics", "outputs", "taskKey"}:
            raise TaskExecutionError(
                FailureClass.ARTIFACT_INTEGRITY,
                "spicyregs.executor.commit-schema",
                f"commit record has an invalid closed shape: {path}",
            )
        if value["taskKey"] != task_key:
            raise TaskExecutionError(
                FailureClass.ARTIFACT_INTEGRITY,
                "spicyregs.executor.commit-task-key",
                "commit record task key differs",
            )
        output_root = self.attempts / task_key / value["attemptId"]
        outputs = value["outputs"]
        if not isinstance(outputs, list) or not outputs:
            raise TaskExecutionError(
                FailureClass.ARTIFACT_INTEGRITY,
                "spicyregs.executor.commit-outputs",
                "commit record has no outputs",
            )
        for descriptor in outputs:
            if not isinstance(descriptor, dict) or set(descriptor) != {"byteSize", "objectKey", "sha256"}:
                raise TaskExecutionError(
                    FailureClass.ARTIFACT_INTEGRITY,
                    "spicyregs.executor.commit-output-schema",
                    "commit output descriptor has an invalid closed shape",
                )
            key = validate_object_key(descriptor["objectKey"], "committed output key")
            output = output_root.joinpath(*key.split("/"))
            if output.is_symlink() or not output.is_file():
                raise TaskExecutionError(
                    FailureClass.ARTIFACT_INTEGRITY,
                    "spicyregs.executor.output-missing",
                    f"committed output is absent or unsafe: {key}",
                )
            digest, size = sha256_file(output)
            if digest != descriptor["sha256"] or size != descriptor["byteSize"]:
                raise TaskExecutionError(
                    FailureClass.ARTIFACT_INTEGRITY,
                    "spicyregs.executor.output-digest",
                    f"committed output digest differs: {key}",
                )
        return CommittedTask(
            task_key=task_key,
            attempt_id=value["attemptId"],
            output_root=output_root,
            outputs=tuple(outputs),
            metrics=value["metrics"],
            reused=True,
        )

    def commit(self, context: AttemptContext, output: TaskOutput) -> CommittedTask:
        descriptors: list[dict[str, Any]] = []
        for key in output.object_keys:
            path = context.workspace.joinpath(*key.split("/"))
            if path.is_symlink() or not path.is_file():
                raise TaskExecutionError(
                    FailureClass.IMPLEMENTATION_DEFECT,
                    "spicyregs.executor.output-not-file",
                    f"handler did not produce declared regular file: {key}",
                )
            digest, size = sha256_file(path)
            descriptors.append({"objectKey": key, "byteSize": size, "sha256": digest})
        record = {
            "taskKey": context.task.task_key,
            "attemptId": context.attempt_id,
            "outputs": descriptors,
            "metrics": dict(output.metrics),
        }
        commit_path = self._commit_path(context.task.task_key)
        temporary = self.commits / f".{context.task.task_key}.{context.attempt_id}.json"
        temporary.write_bytes(canonical_json_bytes(record))
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        try:
            os.link(temporary, commit_path)
            reused = False
        except FileExistsError:
            temporary.unlink(missing_ok=True)
            existing = self.load(context.task.task_key)
            assert existing is not None
            return existing
        finally:
            temporary.unlink(missing_ok=True)
        return CommittedTask(
            task_key=context.task.task_key,
            attempt_id=context.attempt_id,
            output_root=context.workspace,
            outputs=tuple(descriptors),
            metrics=dict(output.metrics),
            reused=reused,
        )


class LocalExecutor:
    """Bounded local executor with the same idempotence rules as a cluster."""

    def __init__(
        self,
        store: ConditionalTaskStore,
        *,
        max_workers: int = 1,
        max_in_flight: int | None = None,
        max_memory_bytes: int = 2 * 1024 * 1024 * 1024,
        max_temporary_disk_bytes: int = 20 * 1024 * 1024 * 1024,
        max_object_count: int = 100_000,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_workers <= 0:
            raise DocumentReleaseV3Error("max_workers must be greater than zero")
        self.store = store
        self.max_workers = max_workers
        self.max_in_flight = max_in_flight or max_workers
        if self.max_in_flight <= 0:
            raise DocumentReleaseV3Error("max_in_flight must be greater than zero")
        self.max_memory_bytes = max_memory_bytes
        self.max_temporary_disk_bytes = max_temporary_disk_bytes
        self.max_object_count = max_object_count
        self.sleep = sleep

    def _admit(self, task: TaskSpec) -> None:
        resources = task.resources
        if resources.memory_bytes > self.max_memory_bytes:
            raise TaskExecutionError(
                FailureClass.TRANSIENT_RESOURCE,
                "spicyregs.executor.memory-budget",
                f"task {task.task_key} exceeds the executor memory budget",
            )
        if resources.temporary_disk_bytes > self.max_temporary_disk_bytes:
            raise TaskExecutionError(
                FailureClass.TRANSIENT_RESOURCE,
                "spicyregs.executor.disk-budget",
                f"task {task.task_key} exceeds the executor temporary disk budget",
            )
        if resources.object_count > self.max_object_count:
            raise TaskExecutionError(
                FailureClass.TRANSIENT_RESOURCE,
                "spicyregs.executor.object-budget",
                f"task {task.task_key} exceeds the executor object-count budget",
            )

    @staticmethod
    def _emit(
        callback: Callable[[ExecutorEvent], None] | None,
        event: str,
        task: TaskSpec,
        attempt_id: str | None,
        attempt_number: int,
        detail: str,
        metrics: Mapping[str, int] | None = None,
    ) -> None:
        if callback is not None:
            callback(
                ExecutorEvent(
                    event=event,
                    task_key=task.task_key,
                    attempt_id=attempt_id,
                    attempt_number=attempt_number,
                    detail=detail,
                    metrics={} if metrics is None else metrics,
                )
            )

    def _run_task(
        self,
        task: TaskSpec,
        handler: Callable[[AttemptContext], TaskOutput],
        cancel_event: threading.Event,
        on_event: Callable[[ExecutorEvent], None] | None,
    ) -> CommittedTask:
        existing = self.store.load(task.task_key)
        if existing is not None:
            self._emit(on_event, "task-reused", task, existing.attempt_id, 0, "verified committed output reused")
            return existing
        self._admit(task)
        last_error: TaskExecutionError | None = None
        for attempt_number in range(1, task.retry.max_attempts + 1):
            if cancel_event.is_set():
                raise TaskCancelledError()
            attempt_id = str(uuid.uuid4())
            workspace = self.store.attempt_workspace(task.task_key, attempt_id)
            context = AttemptContext(
                task=task,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                workspace=workspace,
                cancel_event=cancel_event,
            )
            self._emit(on_event, "attempt-started", task, attempt_id, attempt_number, "attempt admitted")
            started = time.monotonic()
            try:
                output = handler(context)
                elapsed = time.monotonic() - started
                if elapsed > task.timeout_seconds:
                    raise TaskExecutionError(
                        FailureClass.TRANSIENT_RESOURCE,
                        "spicyregs.executor.timeout",
                        f"attempt exceeded timeout after {elapsed:.3f}s",
                    )
                if not isinstance(output, TaskOutput):
                    raise TaskExecutionError(
                        FailureClass.IMPLEMENTATION_DEFECT,
                        "spicyregs.executor.invalid-output",
                        f"handler returned {type(output).__name__}, not TaskOutput",
                    )
                committed = self.store.commit(context, output)
                self._emit(
                    on_event,
                    "task-committed" if not committed.reused else "stale-attempt-ignored",
                    task,
                    attempt_id,
                    attempt_number,
                    "conditional output finalization completed",
                    output.metrics,
                )
                return committed
            except TaskExecutionError as error:
                last_error = error
            except Exception as error:
                last_error = TaskExecutionError(
                    FailureClass.IMPLEMENTATION_DEFECT,
                    "spicyregs.executor.handler-defect",
                    f"{type(error).__name__}: {error}",
                )
            self._emit(
                on_event,
                "attempt-failed",
                task,
                attempt_id,
                attempt_number,
                f"{last_error.failure_class.value}:{last_error.diagnostic_code}",
            )
            if last_error.failure_class not in RETRYABLE_FAILURES or attempt_number >= task.retry.max_attempts:
                raise last_error
            delay = task.retry.delay(task.task_key, attempt_number)
            self._emit(on_event, "retry-delayed", task, attempt_id, attempt_number, "finite retry backoff")
            self.sleep(delay)
        assert last_error is not None
        raise last_error

    def execute(
        self,
        tasks: Iterable[TaskSpec],
        handler: Callable[[AttemptContext], TaskOutput],
        *,
        cancel_event: threading.Event | None = None,
        on_event: Callable[[ExecutorEvent], None] | None = None,
    ) -> tuple[CommittedTask, ...]:
        """Execute tasks with a bounded submission window and input order."""

        cancellation = cancel_event or threading.Event()
        iterator = iter(tasks)
        results: dict[int, CommittedTask] = {}
        pending: dict[concurrent.futures.Future[CommittedTask], int] = {}
        next_index = 0
        exhausted = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while pending or not exhausted:
                while not exhausted and len(pending) < self.max_in_flight:
                    try:
                        task = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    future = pool.submit(self._run_task, task, handler, cancellation, on_event)
                    pending[future] = next_index
                    next_index += 1
                if not pending:
                    continue
                done, _ = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    try:
                        results[index] = future.result()
                    except Exception:
                        cancellation.set()
                        for other in pending:
                            other.cancel()
                        raise
        return tuple(results[index] for index in range(next_index))

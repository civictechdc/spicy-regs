"""Shared runtime for the v3 document pipeline.

One place for plans, work identity, checkpoints, files, hashes, secret scans,
receipts, resume, checking, and rebuild.

This module never imports a pipeline step, a provider library, or a storage
engine, and it never interprets step-specific content. Prompts, expected
answers, ranking metrics, and ontology rules stay opaque values that the
runtime hashes, counts, and records without understanding them.

Behavior copied (not inherited) from the runners v3 replaces:

- append-only JSONL checkpoints with torn-line recovery, duplicate protection,
  and latest-record-per-key replay, from ``ontology/checkpoint.py``, rekeyed
  from the segment-specific 5-tuple onto v3 work IDs;
- streaming 1 MiB SHA-256 and secret scanning, from ``ontology/receipt.py``,
  ``corpora/relation_exclusion_evaluation.py``, and
  ``corpora/mixed_real_data.py``, consolidated into one scanner;
- file inventory with bytes, rows, and digest, from
  ``corpora/document_acceptance_scope.py``; and
- sibling work directory, safe failure receipt, recomputing validation, and
  provider-free rebuild, from ``corpora/relation_exclusion_evaluation.py`` and
  ``corpora/document_acceptance_scope.py``.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from loguru import logger

from spicy_regs.ontology.common import canonical_json, iso_now, stable_id, text_digest

RUNTIME_FORMAT_VERSION = 1

#: The fixed step order. A run may request a prefix, suffix, or subset of it.
PIPELINE_STEPS: tuple[str, ...] = (
    "source",
    "segment",
    "retrieve",
    "extract",
    "approve",
    "compare",
    "materialize",
)

RUN_MODES: tuple[str, ...] = ("build", "diagnostic", "benchmark")

#: Every planned item ends in exactly one of these states.
ITEM_STATES: tuple[str, ...] = (
    "completed",
    "completed_empty",
    "rejected",
    "skipped",
    "failed",
    "unknown",
)

#: Settled work carries a durable outcome and is never re-executed on resume.
#: ``completed_empty`` is success with no result; ``rejected`` is a recorded
#: decision, not a failure.
SETTLED_ITEM_STATES = frozenset({"completed", "completed_empty", "rejected"})

#: Incomplete work is retried on resume, and a required incomplete item
#: prevents the run from passing.
INCOMPLETE_ITEM_STATES = frozenset({"skipped", "failed", "unknown"})

CHECK_STATUSES: tuple[str, ...] = ("pass", "fail", "unknown")

SCAN_CHUNK_BYTES = 1024 * 1024
_SCAN_OVERLAP_BYTES = 4096

_SECRET_PATTERN = re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b")
_SECRET_NEEDLES: tuple[tuple[str, bytes], ...] = (
    ("openai-project-key", b"sk-proj-"),
    ("openai-api-key-assignment", b"OPENAI_API_KEY="),
    ("openai-bearer-header", b"Bearer sk-"),
)
_SECRET_PATTERN_RULE = "openai-key-prefix"

#: Replacement for any value whose bytes match a secret rule. Whole values are
#: replaced, never parts of them, so a redaction can never leak a remainder.
REDACTED = "[redacted: secret-like content]"

#: Run files that record history. A rebuild recomputes derived files only and
#: may never change these.
IMMUTABLE_RUN_PATTERNS: tuple[str, ...] = (
    "plan.json",
    "planned-work.json",
    "transitions.jsonl",
    "source/*",
    "extraction/calls/*",
    "retrieval/join-inputs.json",
    "retrieval/dense-embeddings.parquet",
    "retrieval/sparse-embeddings.parquet",
    "retrieval/rerank-scores.parquet",
    "retrieval/rerank-checkpoints.jsonl",
    "*request.json",
    "*response.json",
)

_RECEIPT_NAME = "receipt.json"
_PLAN_NAME = "plan.json"
_PLANNED_WORK_NAME = "planned-work.json"
_TRANSITIONS_NAME = "transitions.jsonl"
_METRICS_NAME = "metrics.json"


class DocPipelineRuntimeError(Exception):
    """Base class for runtime misuse and unsafe run state."""


class PlanError(DocPipelineRuntimeError):
    """The plan, its work declarations, or a work identity is unusable."""


class WorkStateError(DocPipelineRuntimeError):
    """A work record does not express one honest, durable item state."""


class RunDirectoryError(DocPipelineRuntimeError):
    """A run directory is missing, unsafe to overwrite, or unsafe to rebuild."""


# --------------------------------------------------------------------------
# hashes, secrets, and files
# --------------------------------------------------------------------------


def sha256_text(text: str) -> str:
    """Return the SHA-256 digest of one exact text value."""
    return text_digest(text)


def sha256_file(path: Path) -> str:
    """Return a streaming SHA-256 digest without loading the whole file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(SCAN_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scan_chunk(chunk: bytes) -> set[str]:
    rules = {name for name, needle in _SECRET_NEEDLES if needle in chunk}
    if _SECRET_PATTERN.search(chunk):
        rules.add(_SECRET_PATTERN_RULE)
    return rules


def scan_bytes_for_secrets(data: bytes) -> tuple[str, ...]:
    """Return the secret rules matched by one in-memory byte string."""
    return tuple(sorted(_scan_chunk(data)))


def scan_text_for_secrets(text: str) -> tuple[str, ...]:
    """Return the secret rules matched by one text value."""
    return scan_bytes_for_secrets(text.encode("utf-8", "surrogatepass"))


def scan_file_for_secrets(path: Path) -> tuple[str, ...]:
    """Stream one file in 1 MiB chunks and return every matched secret rule.

    Consecutive chunks overlap so a needle or key split across a chunk
    boundary still matches.
    """
    rules: set[str] = set()
    tail = b""
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(SCAN_CHUNK_BYTES), b""):
            rules |= _scan_chunk(tail + chunk)
            tail = chunk[-_SCAN_OVERLAP_BYTES:]
    return tuple(sorted(rules))


def redact_text(text: str) -> str:
    """Return the text, or a placeholder when the whole value looks secret."""
    return REDACTED if scan_text_for_secrets(text) else text


def redact(value: Any) -> Any:
    """Return a copy with every secret-like string, key, and value replaced."""
    if isinstance(value, Mapping):
        return {redact_text(str(key)): redact(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


@dataclass(frozen=True)
class SecretScanResult:
    """Files whose bytes match a known credential shape."""

    files: tuple[str, ...] = ()
    rules: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def match_count(self) -> int:
        return len(self.files)

    def as_dict(self) -> dict[str, Any]:
        return {
            "secret_match_count": self.match_count,
            "secret_match_files": list(self.files),
        }


def _iter_files(root: Path, *, exclude: Sequence[str]) -> Iterator[tuple[str, Path]]:
    root = Path(root)
    excluded = set(exclude)
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        yield relative, path


def scan_tree_for_secrets(
    root: Path,
    *,
    exclude: Sequence[str] = (_RECEIPT_NAME,),
) -> SecretScanResult:
    """Scan every file under ``root`` and report the matching relative paths."""
    files: list[str] = []
    rules: dict[str, tuple[str, ...]] = {}
    for relative, path in _iter_files(root, exclude=exclude):
        matched = scan_file_for_secrets(path)
        if matched:
            files.append(relative)
            rules[relative] = matched
    return SecretScanResult(files=tuple(files), rules=rules)


def _row_summary(path: Path) -> dict[str, Any]:
    """Return ``rows`` for a readable file, or ``rows_error`` when it is not.

    Only the exception type is recorded. Messages carry absolute paths, which
    change when a work directory is renamed onto its run directory, and the
    inventory has to compare equal before and after that rename.
    """
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with path.open("rb") as handle:
            return {"rows": sum(1 for line in handle if line.strip())}
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq

            return {"rows": int(pq.ParquetFile(path).metadata.num_rows)}
        except Exception as exc:
            logger.warning("Cannot read Parquet row count for {}: {}", path, exc)
            return {"rows_error": type(exc).__name__}
    return {}


def file_inventory(
    root: Path,
    *,
    exclude: Sequence[str] = (_RECEIPT_NAME,),
) -> dict[str, dict[str, Any]]:
    """Map every relative path to its bytes, rows when applicable, and digest."""
    inventory: dict[str, dict[str, Any]] = {}
    for relative, path in _iter_files(root, exclude=exclude):
        record: dict[str, Any] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
        record.update(_row_summary(path))
        inventory[relative] = record
    return inventory


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _load_json(path: Path, *, expect: type) -> Any:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunDirectoryError(f"unreadable JSON at {path}: {exc}") from exc
    if not isinstance(value, expect):
        raise RunDirectoryError(f"JSON value at {path} is not a {expect.__name__}")
    return value


def _plain(value: Any) -> Any:
    """Return a JSON-shaped copy so records hash and compare deterministically."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _json_safe(value: Any) -> Any:
    """Return a comparable copy, stringifying scalars JSON cannot carry.

    Parquet columns hold dates, decimals, and bytes. Comparing recomputed rows
    with stored rows only needs both sides normalized the same way.
    """
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def _canonical_rows(value: Any) -> str:
    """Canonicalize a row list, an Arrow table, or anything with ``to_pylist``."""
    rows = value.to_pylist() if hasattr(value, "to_pylist") else list(value)
    return canonical_json([_json_safe(row) for row in rows])


# --------------------------------------------------------------------------
# work identity
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkIdentity:
    """Every input that makes one unit of work exactly what it is.

    The design fixes the components: step, task, input hashes, settings,
    prompt and schema hashes, provider configuration, and earlier run ID.
    Their contents stay opaque to the runtime.
    """

    step: str
    task: str
    input_digests: Sequence[str] = ()
    settings: Mapping[str, Any] = field(default_factory=dict)
    prompt_digest: str = ""
    schema_digest: str = ""
    provider_config: Mapping[str, Any] = field(default_factory=dict)
    prior_run_id: str = ""

    def __post_init__(self) -> None:
        if not str(self.step).strip():
            raise PlanError("work identity requires a step")
        if not str(self.task).strip():
            raise PlanError("work identity requires a task")
        object.__setattr__(self, "input_digests", tuple(str(value) for value in self.input_digests))

    def components(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "task": self.task,
            "input_digests": list(self.input_digests),
            "settings": _plain(self.settings),
            "prompt_digest": self.prompt_digest,
            "schema_digest": self.schema_digest,
            "provider_config": _plain(self.provider_config),
            "prior_run_id": self.prior_run_id,
        }

    @property
    def work_id(self) -> str:
        return stable_id("work", canonical_json(self.components()), length=32)


# --------------------------------------------------------------------------
# provider totals, item states, and planned work
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderTotals:
    """Opaque provider totals supplied by callers, never measured here."""

    calls: int = 0
    retries: int = 0
    failures: int = 0
    seconds: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    def plus(self, other: ProviderTotals) -> ProviderTotals:
        return ProviderTotals(
            calls=self.calls + other.calls,
            retries=self.retries + other.retries,
            failures=self.failures + other.failures,
            seconds=self.seconds + other.seconds,
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
        )

    @staticmethod
    def sum(totals: Iterable[ProviderTotals]) -> ProviderTotals:
        result = ProviderTotals()
        for item in totals:
            result = result.plus(item)
        return result

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "retries": self.retries,
            "failures": self.failures,
            "seconds": self.seconds,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }

    @classmethod
    def from_dict(cls, record: Mapping[str, Any] | None) -> ProviderTotals:
        record = record or {}
        return cls(
            calls=int(record.get("calls") or 0),
            retries=int(record.get("retries") or 0),
            failures=int(record.get("failures") or 0),
            seconds=float(record.get("seconds") or 0.0),
            input_tokens=int(record.get("input_tokens") or 0),
            output_tokens=int(record.get("output_tokens") or 0),
            total_tokens=int(record.get("total_tokens") or 0),
        )


@dataclass(frozen=True)
class WorkResult:
    """One durable outcome for one planned item."""

    work_id: str
    state: str
    step: str = ""
    task: str = ""
    result: Mapping[str, Any] | None = None
    reason: str = ""
    error: str = ""
    attempts: int = 1
    provider: ProviderTotals | None = None

    def __post_init__(self) -> None:
        if not str(self.work_id).strip():
            raise WorkStateError("a work result requires a work id")
        if self.state not in ITEM_STATES:
            raise WorkStateError(f"unknown item state {self.state!r}")
        if self.state == "completed" and not self.result:
            raise WorkStateError("completed work must carry a result; use completed_empty for success with none")
        if self.state == "completed_empty" and (self.result or self.error):
            raise WorkStateError("completed_empty is success with no result and no error")
        if self.state == "failed" and not str(self.error).strip():
            raise WorkStateError("failed work must record its error")
        if self.state != "failed" and str(self.error).strip():
            raise WorkStateError("only failed work records an error")
        if self.state in {"rejected", "skipped"} and not str(self.reason).strip():
            raise WorkStateError(f"{self.state} work must record its reason")

    @property
    def settled(self) -> bool:
        """True when the item carries a durable outcome and is never retried."""
        return self.state in SETTLED_ITEM_STATES

    @classmethod
    def completed(cls, work_id: str, *, result: Mapping[str, Any], **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="completed", result=result, **fields)

    @classmethod
    def completed_empty(cls, work_id: str, **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="completed_empty", **fields)

    @classmethod
    def rejected(cls, work_id: str, *, reason: str, **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="rejected", reason=reason, **fields)

    @classmethod
    def skipped(cls, work_id: str, *, reason: str, **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="skipped", reason=reason, **fields)

    @classmethod
    def failed(cls, work_id: str, *, error: str, **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="failed", error=error, **fields)

    @classmethod
    def unknown(cls, work_id: str, **fields: Any) -> WorkResult:
        return cls(work_id=work_id, state="unknown", **fields)

    def as_record(self, *, recorded_at: str = "") -> dict[str, Any]:
        record: dict[str, Any] = {
            "work_id": self.work_id,
            "state": self.state,
            "step": self.step,
            "task": self.task,
            "attempts": self.attempts,
        }
        if self.result:
            record["result"] = _plain(self.result)
        if self.reason:
            record["reason"] = self.reason
        if self.error:
            record["error"] = self.error
        if self.provider is not None:
            record["provider"] = self.provider.as_dict()
        if recorded_at:
            record["recorded_at"] = recorded_at
        return record

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> WorkResult:
        provider = record.get("provider")
        return cls(
            work_id=str(record.get("work_id") or ""),
            state=str(record.get("state") or ""),
            step=str(record.get("step") or ""),
            task=str(record.get("task") or ""),
            result=record.get("result"),
            reason=str(record.get("reason") or ""),
            error=str(record.get("error") or ""),
            attempts=int(record.get("attempts") or 1),
            provider=ProviderTotals.from_dict(provider) if isinstance(provider, Mapping) else None,
        )


@dataclass(frozen=True)
class WorkItem:
    """One planned unit of work. Its payload is opaque to the runtime."""

    work_id: str
    step: str
    task: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.work_id).strip():
            raise PlanError("a planned work item requires a work id")
        if not str(self.step).strip() or not str(self.task).strip():
            raise PlanError(f"work item {self.work_id} requires a step and a task")

    @classmethod
    def from_identity(cls, identity: WorkIdentity, *, payload: Mapping[str, Any] | None = None) -> WorkItem:
        return cls(
            work_id=identity.work_id,
            step=identity.step,
            task=identity.task,
            payload=payload or {},
        )

    @property
    def payload_digest(self) -> str:
        return sha256_text(canonical_json(_plain(self.payload)))

    def as_dict(self) -> dict[str, Any]:
        """Record identity only. Step payloads stay in the step's own files."""
        return {
            "work_id": self.work_id,
            "step": self.step,
            "task": self.task,
            "payload_sha256": self.payload_digest,
        }

    @classmethod
    def from_dict(cls, record: Mapping[str, Any]) -> WorkItem:
        return cls(
            work_id=str(record.get("work_id") or ""),
            step=str(record.get("step") or ""),
            task=str(record.get("task") or ""),
        )


@dataclass(frozen=True)
class CheckResult:
    """One step check: pass, fail, or honestly undecided."""

    step: str
    name: str
    status: str
    detail: str = ""

    def __post_init__(self) -> None:
        if self.status not in CHECK_STATUSES:
            raise WorkStateError(f"unknown check status {self.status!r}")

    def as_dict(self) -> dict[str, Any]:
        return {"step": self.step, "name": self.name, "status": self.status, "detail": self.detail}


# --------------------------------------------------------------------------
# checkpoints
# --------------------------------------------------------------------------


class WorkCheckpoint:
    """Append-only JSONL work history keyed by exact work ID.

    Reopening the same file replays the latest durable record for each work
    ID. A torn final line is ignored and safely replaced. Appending a record
    identical to the stored one is a no-op.

    ``repair=False`` opens the history read-only: no directory is created, a
    torn final line is reported through :attr:`torn_tail` instead of being
    truncated, and the file's bytes are left exactly as they were. Checkers
    and rebuilds use it so looking at a run can never change it.
    """

    def __init__(self, path: Path, *, repair: bool = True) -> None:
        self.path = Path(path)
        self.repair = repair
        self.torn_tail = False
        if repair:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records: dict[str, dict[str, Any]] = {}
        if repair:
            self._repair_torn_tail()
        else:
            self.torn_tail = self._has_torn_tail()
        for record in self._read():
            self._records[str(record["work_id"])] = record

    def _has_torn_tail(self) -> bool:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return False
        with self.path.open("rb") as handle:
            handle.seek(-1, os.SEEK_END)
            return handle.read(1) != b"\n"

    def _repair_torn_tail(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        with self.path.open("rb+") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(size - 1)
            if handle.read(1) == b"\n":
                return
            position = size
            while position > 0:
                start = max(0, position - SCAN_CHUNK_BYTES)
                handle.seek(start)
                window = handle.read(position - start)
                index = window.rfind(b"\n")
                if index != -1:
                    handle.truncate(start + index + 1)
                    break
                position = start
            else:
                handle.truncate(0)
            handle.flush()
        logger.warning("Replaced a torn final checkpoint line in {}", self.path)

    def _read(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Ignoring torn checkpoint line {} in {}", line_number, self.path)
                    continue
                if isinstance(record, dict) and record.get("work_id") and record.get("state") in ITEM_STATES:
                    yield record

    @staticmethod
    def _validate(record: Mapping[str, Any]) -> None:
        work_id = str(record.get("work_id") or "").strip()
        if not work_id:
            raise WorkStateError("a checkpoint record requires a work id")
        state = record.get("state")
        if state not in ITEM_STATES:
            raise WorkStateError(f"unknown item state {state!r}")
        if state == "completed_empty" and str(record.get("error") or "").strip():
            raise WorkStateError("completed_empty is success with no result; a failure never becomes empty")

    def get(self, work_id: str) -> dict[str, Any] | None:
        record = self._records.get(work_id)
        return dict(record) if record is not None else None

    def append(self, record: Mapping[str, Any]) -> None:
        if not self.repair:
            raise WorkStateError(f"a read-only work history never records new work: {self.path}")
        self._validate(record)
        stored = _plain(record)
        work_id = str(stored["work_id"])
        prior = self._records.get(work_id)
        if prior is not None and canonical_json(prior) == canonical_json(stored):
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(f"{canonical_json(stored)}\n")
            handle.flush()
        self._records[work_id] = stored

    def records(self) -> list[dict[str, Any]]:
        """Return the latest durable record for each work ID, by work ID."""
        return [dict(record) for _, record in sorted(self._records.items())]

    def transitions(self) -> list[dict[str, Any]]:
        """Return every durable transition in append order."""
        return [dict(record) for record in self._read()]

    def states(self) -> dict[str, str]:
        return {work_id: str(record.get("state")) for work_id, record in self._records.items()}

    def state_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for state in self.states().values():
            counts[state] = counts.get(state, 0) + 1
        return dict(sorted(counts.items()))


# --------------------------------------------------------------------------
# plans
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class RunPlan:
    """A secret-free record of everything one run was asked to do.

    Every field except ``run_id``, ``mode``, ``steps``, and the work
    declarations is opaque: the runtime hashes and records it and never reads
    inside it.
    """

    run_id: str
    mode: str
    steps: Sequence[str]
    source_snapshot: Mapping[str, Any] = field(default_factory=dict)
    rulespec: Mapping[str, Any] = field(default_factory=dict)
    profiles: Mapping[str, Any] = field(default_factory=dict)
    vocabulary: Mapping[str, Any] = field(default_factory=dict)
    segmentation: Mapping[str, Any] = field(default_factory=dict)
    retrieval: Mapping[str, Any] = field(default_factory=dict)
    extraction: Mapping[str, Any] = field(default_factory=dict)
    rules: Mapping[str, Any] = field(default_factory=dict)
    provider: Mapping[str, Any] = field(default_factory=dict)
    review_file_digests: Mapping[str, str] = field(default_factory=dict)
    code_commit: str = ""
    required_work: Sequence[str] = ()
    optional_work: Sequence[str] = ()
    earlier_runs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.run_id).strip():
            raise PlanError("a run plan requires a run id")
        if self.mode not in RUN_MODES:
            raise PlanError(f"unknown run mode {self.mode!r}; expected one of {list(RUN_MODES)}")
        steps = tuple(str(step) for step in self.steps)
        if not steps:
            raise PlanError("a run plan requires at least one step")
        unknown = [step for step in steps if step not in PIPELINE_STEPS]
        if unknown:
            raise PlanError(f"unknown pipeline steps {unknown}; expected a subset of {list(PIPELINE_STEPS)}")
        if len(set(steps)) != len(steps):
            raise PlanError("a run plan cannot request the same step twice")
        if list(steps) != [step for step in PIPELINE_STEPS if step in set(steps)]:
            raise PlanError(f"requested steps {list(steps)} leave the fixed order {list(PIPELINE_STEPS)}")
        object.__setattr__(self, "steps", steps)
        required = tuple(str(key) for key in self.required_work)
        optional = tuple(str(key) for key in self.optional_work)
        overlap = sorted(set(required) & set(optional))
        if overlap:
            raise PlanError(f"work cannot be both required and optional: {overlap}")
        object.__setattr__(self, "required_work", required)
        object.__setattr__(self, "optional_work", optional)
        matched = scan_text_for_secrets(canonical_json(self.as_dict()))
        if matched:
            raise PlanError(f"the run plan carries secret-like material: {list(matched)}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "format_version": RUNTIME_FORMAT_VERSION,
            "run_id": self.run_id,
            "mode": self.mode,
            "steps": list(self.steps),
            "source_snapshot": _plain(self.source_snapshot),
            "rulespec": _plain(self.rulespec),
            "profiles": _plain(self.profiles),
            "vocabulary": _plain(self.vocabulary),
            "segmentation": _plain(self.segmentation),
            "retrieval": _plain(self.retrieval),
            "extraction": _plain(self.extraction),
            "rules": _plain(self.rules),
            "provider": _plain(self.provider),
            "review_file_digests": _plain(self.review_file_digests),
            "code_commit": self.code_commit,
            "required_work": list(self.required_work),
            "optional_work": list(self.optional_work),
            "earlier_runs": _plain(self.earlier_runs),
        }

    @classmethod
    def from_dict(cls, record: Mapping[str, Any]) -> RunPlan:
        version = record.get("format_version")
        if version != RUNTIME_FORMAT_VERSION:
            raise PlanError(f"plan format version {version!r} is not {RUNTIME_FORMAT_VERSION}")
        known = {
            "run_id",
            "mode",
            "steps",
            "source_snapshot",
            "rulespec",
            "profiles",
            "vocabulary",
            "segmentation",
            "retrieval",
            "extraction",
            "rules",
            "provider",
            "review_file_digests",
            "code_commit",
            "required_work",
            "optional_work",
            "earlier_runs",
        }
        unexpected = sorted(set(record) - known - {"format_version"})
        if unexpected:
            raise PlanError(f"plan record carries unknown fields {unexpected}")
        return cls(**{key: value for key, value in record.items() if key in known})

    @property
    def plan_hash(self) -> str:
        return sha256_text(canonical_json(self.as_dict()))

    @property
    def publication_eligible(self) -> bool:
        """Only a build run may ever feed publication, and never automatically."""
        return self.mode == "build"

    @property
    def benchmark_eligible(self) -> bool:
        return self.mode == "benchmark"

    def is_optional(self, *keys: str) -> bool:
        """Optional work must be declared in the plan before the run starts."""
        declared = set(self.optional_work)
        return any(key in declared for key in keys if key)

    def requires(self, *keys: str) -> bool:
        declared = set(self.required_work)
        return any(key in declared for key in keys if key)


# --------------------------------------------------------------------------
# run workspace and reports
# --------------------------------------------------------------------------


@dataclass
class RunWorkspace:
    """The directory one run writes into, plus its durable work history."""

    plan: RunPlan
    path: Path
    checkpoint: WorkCheckpoint

    def file(self, relative: str) -> Path:
        """Return one path inside the run directory, refusing every escape.

        Absolute paths, ``..`` segments, and symbolic links are refused: a
        step that could write through a link would place bytes outside the
        inventory and outside the secret scan that publishes the run.
        """
        candidate = Path(relative)
        if not candidate.parts or candidate.is_absolute() or ".." in candidate.parts:
            raise RunDirectoryError(f"run files stay inside the run directory: {relative!r}")
        root = self.path.resolve()
        target = root
        for part in candidate.parts:
            target = target / part
            if target.is_symlink():
                raise RunDirectoryError(f"run files never follow a symbolic link: {relative!r}")
        parent = target.parent
        parent.mkdir(parents=True, exist_ok=True)
        resolved_parent = parent.resolve()
        if resolved_parent != root and root not in resolved_parent.parents:
            raise RunDirectoryError(f"run files stay inside the run directory: {relative!r}")
        return target

    def write_json(self, relative: str, value: object) -> Path:
        return _write_json(self.file(relative), value)

    def write_text(self, relative: str, text: str) -> Path:
        target = self.file(relative)
        target.write_text(text, encoding="utf-8")
        return target

    def read_json(self, relative: str) -> Any:
        return json.loads(self.file(relative).read_text(encoding="utf-8"))


@dataclass(frozen=True)
class RunChecks:
    """What a step reports back to the runtime after its work is written."""

    checks: Sequence[CheckResult] = ()
    access_control: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, Any] | None = None
    test_answer_digests: Mapping[str, str] = field(default_factory=dict)
    warnings: Sequence[str] = ()
    provider_totals: ProviderTotals | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "checks", tuple(self.checks))
        object.__setattr__(self, "warnings", tuple(str(warning) for warning in self.warnings))


@dataclass(frozen=True)
class RunOutcome:
    """Where the run ended up, and the receipt that proves it."""

    final_state: str
    run_directory: Path
    receipt: dict[str, Any]

    @property
    def passed(self) -> bool:
        return self.final_state == "pass"


def work_directory_for(output_dir: Path) -> Path:
    """Return the sibling work directory a run builds in before renaming."""
    output_dir = Path(output_dir).resolve()
    return output_dir.parent / f".{output_dir.name}.work"


# --------------------------------------------------------------------------
# receipts
# --------------------------------------------------------------------------


def _state_counts(items: Sequence[WorkItem], states: Mapping[str, str]) -> dict[str, int]:
    counts = {state: 0 for state in ITEM_STATES}
    for item in items:
        counts[states.get(item.work_id) or "unknown"] += 1
    return counts


def _work_failures(
    plan: RunPlan,
    items: Sequence[WorkItem],
    states: Mapping[str, str],
) -> tuple[list[str], int]:
    """Return required-work failures and the count of unplanned required work."""
    failures: list[str] = []
    for item in items:
        state = states.get(item.work_id) or "unknown"
        if state in SETTLED_ITEM_STATES:
            continue
        if plan.is_optional(item.work_id, item.task, item.step):
            continue
        failures.append(f"required work {item.work_id} ({item.step}/{item.task}) is {state}")
    planned_keys = {key for item in items for key in (item.work_id, item.task, item.step)}
    unplanned = [key for key in plan.required_work if key not in planned_keys]
    for key in unplanned:
        failures.append(f"declared required work was never planned: {key}")
    return failures, len(unplanned)


def _receipt_counts(
    plan: RunPlan,
    items: Sequence[WorkItem],
    states: Mapping[str, str],
) -> dict[str, int]:
    """Return the item counts. Every planned item lands in exactly one state.

    ``unknown`` counts planned items with no durable outcome;
    ``unresolved_required`` counts required work the plan never planned at
    all. They stay separate so no count can exceed ``planned``.
    """
    counts = _state_counts(items, states)
    unplanned_required = _work_failures(plan, items, states)[1]
    return {
        "planned": len(items),
        "completed": counts["completed"],
        "empty": counts["completed_empty"],
        "rejected": counts["rejected"],
        "skipped": counts["skipped"],
        "failed": counts["failed"],
        "unknown": counts["unknown"],
        "unresolved_required": unplanned_required,
    }


def _plan_receipt_facts(plan: RunPlan, *, passed: bool) -> dict[str, Any]:
    """Return every receipt field the plan alone decides.

    One function serves both the run that writes a receipt and the check that
    recomputes one, so a receipt can never claim a mode, an eligibility, a
    step list, an input, or a version the plan did not ask for.
    """
    return {
        "run_id": plan.run_id,
        "mode": plan.mode,
        "plan_hash": plan.plan_hash,
        "publication_eligible": plan.publication_eligible and passed,
        "benchmark_eligible": plan.benchmark_eligible and passed,
        "steps": list(plan.steps),
        "inputs": {
            "source_snapshot": _plain(plan.source_snapshot),
            "earlier_runs": _plain(plan.earlier_runs),
        },
        "versions": {
            "rulespec": _plain(plan.rulespec),
            "profiles": _plain(plan.profiles),
            "vocabulary": _plain(plan.vocabulary),
            "segmentation": _plain(plan.segmentation),
            "retrieval": _plain(plan.retrieval),
            "extraction": _plain(plan.extraction),
            "rules": _plain(plan.rules),
            "provider": _plain(plan.provider),
            "review_file_digests": _plain(plan.review_file_digests),
            "code_commit": plan.code_commit,
        },
    }


def _plan_fact_failures(receipt: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    """Name every stored receipt field that disagrees with the plan."""
    failures: list[str] = []
    for key, value in sorted(expected.items()):
        stored = receipt.get(key)
        if isinstance(value, dict) and isinstance(stored, Mapping):
            for name, item in sorted(value.items()):
                if stored.get(name) != item:
                    failures.append(f"the receipt {key}.{name} does not match the plan")
            extra = sorted(set(stored) - set(value))
            if extra:
                failures.append(f"the receipt {key} carries fields the plan does not declare: {extra}")
            continue
        if stored != value:
            failures.append(f"the receipt {key} does not match the plan")
    return failures


def _with_missing_step_checks(plan: RunPlan, report: RunChecks) -> RunChecks:
    """Record a requested step that reported nothing as honestly undecided.

    Silence is not a pass. An unchecked step becomes an ``unknown`` check so
    its absence is visible in the receipt, and a warning rather than a
    failure so a step that legitimately has nothing to check still passes.
    """
    reported = {check.step for check in report.checks}
    missing = tuple(
        CheckResult(step=step, name="step_checks", status="unknown", detail="the step reported no check result")
        for step in plan.steps
        if step not in reported
    )
    if not missing:
        return report
    return replace(report, checks=(*report.checks, *missing))


def _assemble_receipt(
    plan: RunPlan,
    *,
    items: Sequence[WorkItem],
    states: Mapping[str, str],
    provider_work: ProviderTotals,
    provider_other: ProviderTotals,
    report: RunChecks,
    inventory: Mapping[str, Mapping[str, Any]],
    secrets: SecretScanResult,
    failures: Sequence[str],
    warnings: Sequence[str],
    metrics_digest: str = "",
    rebuild: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    passed = not failures
    receipt: dict[str, Any] = {
        "format_version": RUNTIME_FORMAT_VERSION,
        **_plan_receipt_facts(plan, passed=passed),
        "final_state": "pass" if passed else "fail",
        "generated_at": iso_now(),
        "counts": _receipt_counts(plan, items, states),
        "files": {path: dict(record) for path, record in sorted(inventory.items())},
        "provider": provider_work.plus(provider_other).as_dict(),
        "provider_sources": {"work": provider_work.as_dict(), "other": provider_other.as_dict()},
        "checks": [check.as_dict() for check in report.checks],
        "security": {
            **secrets.as_dict(),
            "access_control": _plain(report.access_control),
        },
        "failures": list(failures),
        "warnings": list(warnings),
    }
    if metrics_digest or report.test_answer_digests:
        receipt["test_answers"] = {
            "metrics_sha256": metrics_digest,
            "answer_file_digests": _plain(report.test_answer_digests),
        }
    if rebuild is not None:
        receipt["rebuild"] = _plain(rebuild)
    return _sealed_receipt(receipt)


def _sealed_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Return the receipt with its own hash covering everything else in it."""
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    body["receipt_sha256"] = sha256_text(canonical_json(body))
    return body


def _check_message(check: CheckResult, phrase: str) -> str:
    message = f"check {check.step}.{check.name} {phrase}"
    return f"{message}: {check.detail}" if check.detail else message


def _receipt_failures(
    report: RunChecks,
    secrets: SecretScanResult,
) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    for check in report.checks:
        if check.status == "fail":
            failures.append(_check_message(check, "failed"))
        elif check.status == "unknown":
            warnings.append(_check_message(check, "is undecided"))
    if secrets.match_count:
        failures.append(f"secret-like content appears in run files: {list(secrets.files)}")
    return failures, warnings


# --------------------------------------------------------------------------
# run, resume, and finish
# --------------------------------------------------------------------------


_EARLIER_RUN_DIRECTORY_KEYS: tuple[str, ...] = ("run_directory", "directory", "path")


def check_earlier_run(name: str, declared: Mapping[str, Any]) -> Path:
    """Check one declared earlier run before this run is allowed to use it.

    The design lets a run start from an earlier run only after checking that
    run's receipt and required file hashes. That means: the receipt exists,
    its hash covers its own body, it records a passing run with the declared
    run ID, its file inventory still matches, and every file digest the plan
    declares is present with exactly that digest.
    """
    if not isinstance(declared, Mapping):
        raise RunDirectoryError(f"declared earlier run {name} is not a record")
    directory = next((declared[key] for key in _EARLIER_RUN_DIRECTORY_KEYS if declared.get(key)), None)
    if not directory:
        raise RunDirectoryError(
            f"declared earlier run {name} names no run directory; expected one of {list(_EARLIER_RUN_DIRECTORY_KEYS)}"
        )
    earlier_dir = Path(str(directory)).resolve()
    receipt_path = earlier_dir / _RECEIPT_NAME
    if not receipt_path.is_file():
        raise RunDirectoryError(f"declared earlier run {name} has no receipt at {receipt_path}")
    receipt = _load_json(receipt_path, expect=dict)
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if receipt.get("receipt_sha256") != sha256_text(canonical_json(body)):
        raise RunDirectoryError(f"declared earlier run {name} has a receipt hash that does not cover its body")
    if receipt.get("final_state") != "pass":
        raise RunDirectoryError(f"declared earlier run {name} did not pass: {receipt.get('final_state')!r}")
    declared_run_id = str(declared.get("run_id") or "")
    if declared_run_id and declared_run_id != str(receipt.get("run_id") or ""):
        raise RunDirectoryError(
            f"declared earlier run {name} is run {receipt.get('run_id')!r}, not {declared_run_id!r}"
        )
    inventory = file_inventory(earlier_dir)
    stored_files = receipt.get("files")
    stored_files = stored_files if isinstance(stored_files, Mapping) else {}
    for relative in sorted(set(stored_files) | set(inventory)):
        if stored_files.get(relative) != inventory.get(relative):
            raise RunDirectoryError(f"declared earlier run {name} no longer matches its receipt for {relative}")
    required = declared.get("files")
    for relative, digest in sorted((required or {}).items()) if isinstance(required, Mapping) else ():
        record = inventory.get(str(relative))
        if record is None:
            raise RunDirectoryError(f"declared earlier run {name} is missing required file {relative}")
        if record.get("sha256") != str(digest):
            raise RunDirectoryError(f"declared earlier run {name} file {relative} does not have the declared digest")
    return earlier_dir


def _check_earlier_runs(plan: RunPlan) -> None:
    for name, declared in sorted(plan.earlier_runs.items()):
        check_earlier_run(str(name), declared)


def _record_planned_work(work_dir: Path, items: Sequence[WorkItem]) -> None:
    """Write the planned work once, and refuse any later change to it.

    Resume reuses only genuinely finished work. Silently rewriting this file
    would let a re-run drop a failing item and publish a clean run while the
    work history still records the failure.
    """
    path = work_dir / _PLANNED_WORK_NAME
    records = [item.as_dict() for item in items]
    if not path.exists():
        _write_json(path, records)
        return
    stored_by_id: dict[str, dict[str, Any]] = {}
    for record in _load_json(path, expect=list):
        if not isinstance(record, Mapping) or not str(record.get("work_id") or "").strip():
            raise PlanError(f"the planned work list at {path} is unusable")
        stored_by_id[str(record["work_id"])] = _plain(record)
    new_by_id = {str(record["work_id"]): record for record in records}
    missing = sorted(set(stored_by_id) - set(new_by_id))
    added = sorted(set(new_by_id) - set(stored_by_id))
    if missing or added:
        raise PlanError(
            f"resuming {work_dir} must plan exactly the work it started; "
            f"work no longer planned: {missing}; work newly planned: {added}"
        )
    changed = [
        work_id
        for work_id in sorted(stored_by_id)
        if stored_by_id[work_id].get("payload_sha256") != new_by_id[work_id]["payload_sha256"]
    ]
    if changed:
        raise PlanError(f"resuming {work_dir} changed the payload of already planned work: {changed}")
    differing = [
        work_id
        for work_id in sorted(stored_by_id)
        if canonical_json(stored_by_id[work_id]) != canonical_json(new_by_id[work_id])
    ]
    if differing:
        raise PlanError(f"resuming {work_dir} changed the identity of already planned work: {differing}")


def _work_provider_totals(
    records: Mapping[str, Mapping[str, Any]] | WorkCheckpoint,
    items: Sequence[WorkItem],
) -> ProviderTotals:
    """Sum the provider totals the durable work records carry for the plan."""
    stored = records if isinstance(records, Mapping) else {record["work_id"]: record for record in records.records()}
    totals = ProviderTotals()
    for item in items:
        record = stored.get(item.work_id)
        provider = record.get("provider") if isinstance(record, Mapping) else None
        if isinstance(provider, Mapping):
            totals = totals.plus(ProviderTotals.from_dict(provider))
    return totals


def _crash_receipt(
    plan: RunPlan,
    *,
    items: Sequence[WorkItem],
    states: Mapping[str, str],
    provider_work: ProviderTotals,
    exc: BaseException,
) -> dict[str, Any]:
    """Return a minimal, secret-free receipt for a run that stopped early."""
    detail = redact_text(f"{type(exc).__name__}: {exc}")
    report = _with_missing_step_checks(
        plan,
        RunChecks(checks=(CheckResult(step="runtime", name="run_completed", status="fail", detail=detail),)),
    )
    _, warnings = _receipt_failures(report, SecretScanResult())
    return _assemble_receipt(
        plan,
        items=items,
        states=states,
        provider_work=provider_work,
        provider_other=ProviderTotals(),
        report=report,
        inventory={},
        secrets=SecretScanResult(),
        failures=[f"the run stopped before it finished: {detail}"],
        warnings=warnings,
    )


def execute_run(
    plan: RunPlan,
    output_dir: Path,
    *,
    items: Sequence[WorkItem],
    execute: Callable[[RunWorkspace, WorkItem], WorkResult],
    finalize: Callable[[RunWorkspace, tuple[WorkResult, ...]], RunChecks] | None = None,
) -> RunOutcome:
    """Run one plan in a sibling work directory and finish it atomically.

    Completed work is reused, incomplete work is retried, a crash leaves the
    work directory for the next resume, a failure writes a safe failure
    receipt into it, and only a passing run renames it onto ``output_dir``.
    """
    output_dir = Path(output_dir).resolve()
    if output_dir.exists():
        raise RunDirectoryError(f"refusing to overwrite an existing run directory: {output_dir}")

    seen: set[str] = set()
    for item in items:
        if item.work_id in seen:
            raise PlanError(f"planned work {item.work_id} appears twice")
        seen.add(item.work_id)
        if item.step not in plan.steps:
            raise PlanError(f"work item {item.work_id} names step {item.step!r}, which the plan does not request")

    _check_earlier_runs(plan)

    work_dir = work_directory_for(output_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    plan_path = work_dir / _PLAN_NAME
    if plan_path.exists():
        stored = _load_json(plan_path, expect=dict)
        if RunPlan.from_dict(stored).plan_hash != plan.plan_hash:
            raise PlanError(f"work directory {work_dir} belongs to a different plan")
    else:
        _write_json(plan_path, plan.as_dict())
    _record_planned_work(work_dir, items)

    checkpoint = WorkCheckpoint(work_dir / _TRANSITIONS_NAME)
    workspace = RunWorkspace(plan=plan, path=work_dir, checkpoint=checkpoint)

    results: list[WorkResult] = []
    for item in items:
        prior = checkpoint.get(item.work_id)
        if prior is not None and str(prior.get("state")) in SETTLED_ITEM_STATES:
            results.append(WorkResult.from_record(prior))
            continue
        attempts = int(prior.get("attempts") or 0) + 1 if prior is not None else 1
        try:
            result = execute(workspace, item)
        except Exception as exc:  # a failed item stays durable; the run keeps going
            logger.warning("work {} failed with {}", item.work_id, type(exc).__name__)
            result = WorkResult.failed(
                item.work_id,
                step=item.step,
                task=item.task,
                error=f"{type(exc).__name__}: {exc}",
            )
        if not isinstance(result, WorkResult):
            raise WorkStateError(f"execute returned {type(result).__name__} for {item.work_id}, not a WorkResult")
        if result.work_id != item.work_id:
            raise WorkStateError(f"execute returned work id {result.work_id!r} for planned item {item.work_id!r}")
        result = replace(result, attempts=attempts)
        checkpoint.append(result.as_record(recorded_at=iso_now()))
        results.append(result)

    states = {result.work_id: result.state for result in results}
    provider_work = ProviderTotals.sum([result.provider for result in results if result.provider is not None])

    try:
        report = RunChecks() if finalize is None else finalize(workspace, tuple(results))
        if not isinstance(report, RunChecks):
            raise WorkStateError(f"finalize returned {type(report).__name__}, not RunChecks")
        report = _with_missing_step_checks(plan, report)

        metrics_digest = ""
        if report.metrics is not None:
            metrics_digest = sha256_file(_write_json(work_dir / _METRICS_NAME, _plain(report.metrics)))

        inventory = file_inventory(work_dir)
        secrets = scan_tree_for_secrets(work_dir)
        check_failures, warnings = _receipt_failures(report, secrets)
        work_failures, _ = _work_failures(plan, items, states)
        failures = [*work_failures, *check_failures]

        receipt = _assemble_receipt(
            plan,
            items=items,
            states=states,
            provider_work=provider_work,
            provider_other=report.provider_totals or ProviderTotals(),
            report=report,
            inventory=inventory,
            secrets=secrets,
            failures=failures,
            warnings=[*warnings, *report.warnings],
            metrics_digest=metrics_digest,
        )

        # The tree scan cannot see the receipt, which does not exist yet, so a
        # secret arriving through a check detail, an access-control record, or
        # a warning would be published inside the receipt itself. Scan the
        # assembled receipt before any of it reaches disk.
        matched = scan_text_for_secrets(canonical_json(receipt))
        if matched:
            receipt = _redacted_failure_receipt(
                plan,
                items=items,
                states=states,
                provider_work=provider_work,
                report=report,
                inventory=inventory,
                secrets=secrets,
                failures=failures,
                warnings=[*warnings, *report.warnings],
                matched=matched,
                metrics_digest=metrics_digest,
            )
            _write_json(work_dir / _RECEIPT_NAME, receipt)
            logger.warning("run {} assembled a receipt with secret-like content; refusing to publish", plan.run_id)
            return RunOutcome(final_state="fail", run_directory=work_dir, receipt=receipt)
    except BaseException as exc:
        try:
            _write_json(
                work_dir / _RECEIPT_NAME,
                _crash_receipt(plan, items=items, states=states, provider_work=provider_work, exc=exc),
            )
        except Exception:  # pragma: no cover - the original failure always wins
            logger.exception("could not write a failure receipt into {}", work_dir)
        logger.warning("run {} stopped before it finished; work directory kept at {}", plan.run_id, work_dir)
        raise

    _write_json(work_dir / _RECEIPT_NAME, receipt)
    if failures:
        logger.warning("run {} did not pass; work directory kept at {}", plan.run_id, work_dir)
        return RunOutcome(final_state="fail", run_directory=work_dir, receipt=receipt)
    work_dir.replace(output_dir)
    return RunOutcome(final_state="pass", run_directory=output_dir, receipt=receipt)


def _redacted_failure_receipt(
    plan: RunPlan,
    *,
    items: Sequence[WorkItem],
    states: Mapping[str, str],
    provider_work: ProviderTotals,
    report: RunChecks,
    inventory: Mapping[str, Mapping[str, Any]],
    secrets: SecretScanResult,
    failures: Sequence[str],
    warnings: Sequence[str],
    matched: Sequence[str],
    metrics_digest: str,
) -> dict[str, Any]:
    """Return a failing receipt that names the secret rules but carries none.

    Rule names are safe; the values that matched them are not. Every string
    the step handed back is replaced whole, and the assembled receipt is
    redacted again before it is sealed, so no remainder can survive.
    """
    detail = f"the assembled receipt matched secret rules {list(matched)}"
    safe_report = RunChecks(
        checks=(
            *(
                replace(check, name=redact_text(check.name), detail=redact_text(check.detail))
                for check in report.checks
            ),
            CheckResult(step="runtime", name="receipt_secret_scan", status="fail", detail=detail),
        ),
        access_control=redact(report.access_control),
        metrics=None,
        test_answer_digests=redact(report.test_answer_digests),
        warnings=tuple(redact_text(warning) for warning in report.warnings),
        provider_totals=report.provider_totals,
    )
    receipt = _assemble_receipt(
        plan,
        items=items,
        states=states,
        provider_work=provider_work,
        provider_other=report.provider_totals or ProviderTotals(),
        report=safe_report,
        inventory=inventory,
        secrets=secrets,
        failures=[*failures, f"secret-like content appears in the assembled receipt: {list(matched)}"],
        warnings=[redact_text(warning) for warning in warnings],
        metrics_digest=metrics_digest,
    )
    return _sealed_receipt(redact({key: value for key, value in receipt.items() if key != "receipt_sha256"}))


# --------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------


def _derived_artifact_matches(path: Path, value: Any) -> bool:
    """Compare one stored derived artifact with the object it must contain.

    JSON files compare as canonical JSON. Parquet files, which most of the
    run layout is, compare as canonicalized row lists so column order and
    file-level metadata never masquerade as a data difference.
    """
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq

        return _canonical_rows(pq.read_table(path)) == _canonical_rows(value)
    stored = json.loads(path.read_text(encoding="utf-8"))
    return canonical_json(_json_safe(stored)) == canonical_json(_json_safe(value))


def _validation_report(
    run_dir: Path,
    *,
    receipt: Mapping[str, Any],
    integrity: Sequence[str],
    quality: Sequence[str],
    file_count: int,
    secret_files: Sequence[str],
) -> dict[str, Any]:
    integrity_status = "pass" if not integrity else "fail"
    quality_status = "pass" if not quality else "fail"
    return {
        "status": "pass" if integrity_status == "pass" and quality_status == "pass" else "fail",
        "integrity_status": integrity_status,
        "quality_status": quality_status,
        "run_state": receipt.get("final_state"),
        "run_directory": str(run_dir),
        "file_count": file_count,
        "secret_match_count": len(secret_files),
        "secret_match_files": list(secret_files),
        "integrity_failures": list(integrity),
        "quality_failures": list(quality),
        "failures": [*integrity, *quality],
    }


def validate_run(
    run_dir: Path,
    *,
    plan: RunPlan | None = None,
    recompute: Callable[[Path, RunPlan], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Recompute a run instead of trusting its receipt.

    Every receipt field the plan decides, the file inventory, the work-state
    counts, the provider totals the work records carry, the secret scan over
    every file including the receipt, and the receipt's own hash are
    recomputed here. Step-specific derived artifacts are recomputed by the
    caller-supplied ``recompute`` hook, which returns a mapping of relative
    path to the object that file must contain.

    Checking never writes: the work history is opened read-only, so a torn
    tail is reported rather than healed and no run file changes.

    Integrity failures (the run does not describe itself) are reported apart
    from quality failures (the run describes an unusable result).
    """
    run_dir = Path(run_dir).resolve()
    integrity: list[str] = []
    quality: list[str] = []

    inventory = file_inventory(run_dir)
    tree_secrets = scan_tree_for_secrets(run_dir)
    receipt_path = run_dir / _RECEIPT_NAME
    receipt_secret_rules = scan_file_for_secrets(receipt_path) if receipt_path.is_file() else ()
    secret_files: tuple[str, ...] = (*tree_secrets.files, *((_RECEIPT_NAME,) if receipt_secret_rules else ()))
    if secret_files:
        message = f"secret-like content appears in run files: {list(secret_files)}"
        integrity.append(message)
        quality.append(message)

    try:
        receipt = _load_json(receipt_path, expect=dict)
    except RunDirectoryError as exc:
        integrity.append(f"the run receipt is unusable: {exc}")
        return _validation_report(
            run_dir,
            receipt={},
            integrity=integrity,
            quality=quality,
            file_count=len(inventory),
            secret_files=secret_files,
        )

    if receipt.get("format_version") != RUNTIME_FORMAT_VERSION:
        integrity.append("receipt format_version is not current")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if receipt.get("receipt_sha256") != sha256_text(canonical_json(body)):
        integrity.append("the receipt hash does not cover the stored receipt body")

    stored_plan: RunPlan | None = None
    try:
        stored_plan = RunPlan.from_dict(_load_json(run_dir / _PLAN_NAME, expect=dict))
    except (PlanError, RunDirectoryError) as exc:
        integrity.append(f"the stored plan is unusable: {exc}")
    if stored_plan is not None and plan is not None and plan.plan_hash != stored_plan.plan_hash:
        integrity.append("the stored plan does not match the declared plan")
    reference_plan = stored_plan if stored_plan is not None else plan

    items: list[WorkItem] = []
    planned_work_read = False
    try:
        items = [WorkItem.from_dict(record) for record in _load_json(run_dir / _PLANNED_WORK_NAME, expect=list)]
        planned_work_read = True
    except (PlanError, RunDirectoryError) as exc:
        integrity.append(f"the planned work list is unusable: {exc}")

    checkpoint = WorkCheckpoint(run_dir / _TRANSITIONS_NAME, repair=False)
    if checkpoint.torn_tail:
        integrity.append(f"{_TRANSITIONS_NAME} ends with a torn record")
    states = checkpoint.states()

    stored_files = receipt.get("files")
    stored_files = stored_files if isinstance(stored_files, dict) else {}
    for path in sorted(set(stored_files) | set(inventory)):
        if stored_files.get(path) != inventory.get(path):
            integrity.append(f"the file inventory does not match for {path}")

    stored_security = receipt.get("security")
    stored_security = stored_security if isinstance(stored_security, dict) else {}
    if stored_security.get("secret_match_count") != tree_secrets.match_count or sorted(
        stored_security.get("secret_match_files") or []
    ) != sorted(tree_secrets.files):
        integrity.append("the receipt secret scan does not match the recomputed scan")

    stored_sources = receipt.get("provider_sources")
    stored_sources = stored_sources if isinstance(stored_sources, Mapping) else {}
    recomputed_work_provider = _work_provider_totals(checkpoint, items)
    if planned_work_read:
        if ProviderTotals.from_dict(stored_sources.get("work")).as_dict() != recomputed_work_provider.as_dict():
            integrity.append("the receipt provider totals do not match the summed work records")
        expected_provider = recomputed_work_provider.plus(ProviderTotals.from_dict(stored_sources.get("other")))
        if receipt.get("provider") != expected_provider.as_dict():
            integrity.append("the receipt provider block does not match its own recorded sources")

    if reference_plan is not None and planned_work_read:
        recomputed_counts = _receipt_counts(reference_plan, items, states)
        if receipt.get("counts") != recomputed_counts:
            integrity.append(f"the receipt counts do not match the recomputed work states: {recomputed_counts}")
        quality.extend(_work_failures(reference_plan, items, states)[0])

    for check in receipt.get("checks") or []:
        if isinstance(check, Mapping) and check.get("status") == "fail":
            quality.append(f"check {check.get('step')}.{check.get('name')} failed")

    if recompute is not None and stored_plan is not None:
        try:
            expected = recompute(run_dir, stored_plan)
        except Exception as exc:
            integrity.append(f"derived artifact recomputation failed with {type(exc).__name__}: {exc}")
        else:
            for relative, value in sorted(expected.items()):
                path = run_dir / relative
                if not path.is_file():
                    integrity.append(f"derived artifact {relative} is missing")
                    continue
                try:
                    matches = _derived_artifact_matches(path, value)
                except Exception as exc:
                    integrity.append(f"derived artifact {relative} could not be compared: {type(exc).__name__}: {exc}")
                    continue
                if not matches:
                    integrity.append(f"derived artifact {relative} does not recompute")

    if reference_plan is not None:
        integrity.extend(_plan_fact_failures(receipt, _plan_receipt_facts(reference_plan, passed=not quality)))

    expected_state = "pass" if not quality else "fail"
    if receipt.get("final_state") != expected_state:
        integrity.append("the receipt final state does not match the recomputed state")

    return _validation_report(
        run_dir,
        receipt=receipt,
        integrity=integrity,
        quality=quality,
        file_count=len(inventory),
        secret_files=secret_files,
    )


# --------------------------------------------------------------------------
# rebuild
# --------------------------------------------------------------------------


def _is_immutable(relative: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(relative, pattern) for pattern in patterns)


def rebuild_run(
    run_dir: Path,
    output_dir: Path,
    *,
    rebuild: Callable[[RunWorkspace, RunPlan], RunChecks],
    immutable_patterns: Sequence[str] = IMMUTABLE_RUN_PATTERNS,
) -> dict[str, Any]:
    """Recompute a run's derived files from its stored inputs and responses.

    The source run is never modified. The rebuilt run records
    ``provider_invoked: false``, and a rebuild that reports a provider call or
    changes a historical request, response, or work-history file is refused.
    """
    run_dir = Path(run_dir).resolve()
    output_dir = Path(output_dir).resolve()
    if output_dir.exists():
        raise RunDirectoryError(f"refusing to overwrite an existing run directory: {output_dir}")

    before = validate_run(run_dir)
    if before["integrity_status"] != "pass":
        raise RunDirectoryError(
            "refusing to rebuild a run that fails integrity: " + "; ".join(before["integrity_failures"])
        )

    source_receipt = _load_json(run_dir / _RECEIPT_NAME, expect=dict)
    plan = RunPlan.from_dict(_load_json(run_dir / _PLAN_NAME, expect=dict))
    items = [WorkItem.from_dict(record) for record in _load_json(run_dir / _PLANNED_WORK_NAME, expect=list)]

    # Sample the source run before the hook runs. Comparing afterwards to a
    # sample taken afterwards would agree with any corruption the hook caused.
    source_inventory = file_inventory(run_dir, exclude=())

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.rebuild-", dir=output_dir.parent))
    staging = staging_root / output_dir.name
    try:
        shutil.copytree(run_dir, staging)
        workspace = RunWorkspace(
            plan=plan,
            path=staging,
            checkpoint=WorkCheckpoint(staging / _TRANSITIONS_NAME, repair=False),
        )
        report = rebuild(workspace, plan)
        if not isinstance(report, RunChecks):
            raise RunDirectoryError(f"rebuild returned {type(report).__name__}, not RunChecks")
        if (report.provider_totals or ProviderTotals()) != ProviderTotals():
            raise RunDirectoryError("rebuild reported provider work; a rebuild never calls a provider")
        report = _with_missing_step_checks(plan, report)

        after_inventory = file_inventory(run_dir, exclude=())
        for relative in sorted(set(source_inventory) | set(after_inventory)):
            if source_inventory.get(relative) != after_inventory.get(relative):
                raise RunDirectoryError(f"rebuild changed the source run file {relative}")

        staged_inventory = file_inventory(staging, exclude=())
        for relative in sorted(set(source_inventory) | set(staged_inventory)):
            if not _is_immutable(relative, immutable_patterns):
                continue
            if source_inventory.get(relative) != staged_inventory.get(relative):
                raise RunDirectoryError(f"rebuild changed the historical file {relative}")

        metrics_digest = ""
        if report.metrics is not None:
            metrics_digest = sha256_file(_write_json(staging / _METRICS_NAME, _plain(report.metrics)))

        states = workspace.checkpoint.states()
        inventory = file_inventory(staging)
        secrets = scan_tree_for_secrets(staging)
        check_failures, warnings = _receipt_failures(report, secrets)
        work_failures, _ = _work_failures(plan, items, states)
        failures = [*work_failures, *check_failures]
        source_sources = source_receipt.get("provider_sources")
        source_sources = source_sources if isinstance(source_sources, Mapping) else {}
        receipt = _assemble_receipt(
            plan,
            items=items,
            states=states,
            provider_work=_work_provider_totals(workspace.checkpoint, items),
            provider_other=ProviderTotals.from_dict(source_sources.get("other")),
            report=report,
            inventory=inventory,
            secrets=secrets,
            failures=failures,
            warnings=[*warnings, *report.warnings],
            metrics_digest=metrics_digest,
            rebuild={
                "provider_invoked": False,
                "rebuilt_at": iso_now(),
                "source_run_id": source_receipt.get("run_id"),
                "source_run_directory": str(run_dir),
                "source_receipt_sha256": source_receipt.get("receipt_sha256"),
            },
        )
        matched = scan_text_for_secrets(canonical_json(receipt))
        if matched:
            raise RunDirectoryError(f"the rebuilt receipt carries secret-like content: {list(matched)}")
        _write_json(staging / _RECEIPT_NAME, receipt)
        if failures:
            raise RunDirectoryError("the rebuilt run does not pass its checks: " + "; ".join(failures))
        staging.replace(output_dir)
    except BaseException:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise
    shutil.rmtree(staging_root, ignore_errors=True)

    after = validate_run(output_dir)
    if after["integrity_status"] != "pass":
        raise RunDirectoryError(
            "the rebuilt run failed integrity validation: " + "; ".join(after["integrity_failures"])
        )
    return {
        "status": after["status"],
        "integrity_status": after["integrity_status"],
        "quality_status": after["quality_status"],
        "provider_invoked": False,
        "run_directory": str(output_dir),
        "source_run_directory": str(run_dir),
        "failures": after["failures"],
    }

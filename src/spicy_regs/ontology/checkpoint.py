"""Resumable JSONL checkpoints for LLM ontology batches."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from loguru import logger

from spicy_regs.ontology.common import canonical_json


class BatchCheckpoint:
    """Append durable state transitions for exact segment work items.

    Reusing ``ONTOLOGY_RUN_ID`` and the same output directory resumes after the
    last complete JSONL record. A torn final line is ignored and can be safely
    replaced. New records key work by artifact, artifact version, segment, and
    optional operation ID. Legacy artifact-only records remain readable but
    cannot satisfy a versioned segment lookup.
    """

    def __init__(self, output_dir: Path, *, run_id: str, phase: str) -> None:
        directory = output_dir / ".ontology-checkpoints"
        directory.mkdir(exist_ok=True)
        self.path = directory / f"{run_id}-{phase}.jsonl"
        self._records: dict[tuple[str, ...], dict] = {}
        for record in self._read():
            key = self._record_key(record)
            if key is not None:
                self._records[key] = record

    @staticmethod
    def _record_key(record: dict) -> tuple[str, ...] | None:
        subject_type = str(record.get("subject_type") or "")
        subject_id = str(record.get("subject_id") or "")
        if not subject_type or not subject_id:
            return None
        return (
            subject_type,
            subject_id,
            str(record.get("artifact_digest") or ""),
            str(record.get("segment_id") or ""),
            str(record.get("work_id") or ""),
        )

    def _read(self) -> Iterator[dict]:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(
                        "Ignoring torn ontology checkpoint line {} in {}",
                        line_number,
                        self.path,
                    )
                    continue
                if isinstance(record, dict):
                    yield record

    def get(
        self,
        subject_type: str,
        subject_id: str,
        *,
        artifact_digest: str = "",
        segment_id: str = "",
        work_id: str = "",
    ) -> dict | None:
        return self._records.get(
            (
                subject_type,
                subject_id,
                artifact_digest,
                segment_id,
                work_id,
            )
        )

    def append(self, record: dict) -> None:
        key = self._record_key(record)
        if key is None:
            return
        prior = self._records.get(key)
        if prior is not None and canonical_json(prior) == canonical_json(record):
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(f"{canonical_json(record)}\n")
            handle.flush()
        self._records[key] = record

    def records(self) -> list[dict]:
        """Return the latest durable state for each exact work item."""
        return [
            dict(record)
            for _, record in sorted(self._records.items())
        ]

    def transitions(self) -> list[dict]:
        """Return every durable transition in append order."""
        return [dict(record) for record in self._read()]

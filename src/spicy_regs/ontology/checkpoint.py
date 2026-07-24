"""Resumable JSONL checkpoints for LLM ontology batches."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from loguru import logger

from spicy_regs.ontology.common import canonical_json


class BatchCheckpoint:
    """Append one durable record per completed subject.

    Reusing ``ONTOLOGY_RUN_ID`` and the same output directory resumes after the
    last complete JSONL record. A torn final line is ignored and can be safely
    replaced by reprocessing that subject.
    """

    def __init__(self, output_dir: Path, *, run_id: str, phase: str) -> None:
        directory = output_dir / ".ontology-checkpoints"
        directory.mkdir(exist_ok=True)
        self.path = directory / f"{run_id}-{phase}.jsonl"
        self._records = {
            (str(record.get("subject_type")), str(record.get("subject_id"))): record
            for record in self._read()
            if record.get("subject_type") and record.get("subject_id")
        }

    def _read(self) -> Iterator[dict]:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Ignoring torn ontology checkpoint line {} in {}", line_number, self.path)
                    continue
                if isinstance(record, dict):
                    yield record

    def get(self, subject_type: str, subject_id: str) -> dict | None:
        return self._records.get((subject_type, subject_id))

    def append(self, record: dict) -> None:
        key = (str(record.get("subject_type")), str(record.get("subject_id")))
        if not all(key) or key in self._records:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(f"{canonical_json(record)}\n")
            handle.flush()
        self._records[key] = record

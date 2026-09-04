"""Shared storage and provenance helpers for ontology rollups."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

ATTESTATION_COLUMNS: tuple[str, ...] = (
    "method",
    "actor_id",
    "run_id",
    "asserted_at",
    "supersedes_id",
)

NON_DETERMINISTIC_METHODS = frozenset({"llm", "embedding", "human"})


def iso_now() -> str:
    """Return the current UTC instant in a stable ISO-8601 representation."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RunContext:
    """Provenance values shared by all rows produced in one pipeline run."""

    run_id: str
    asserted_at: str

    @classmethod
    def resolve(
        cls,
        *,
        run_id: str | None = None,
        asserted_at: str | None = None,
        prefix: str = "ontology",
    ) -> RunContext:
        now = asserted_at or iso_now()
        configured = run_id or os.environ.get("ONTOLOGY_RUN_ID")
        if configured:
            return cls(run_id=configured, asserted_at=now)
        timestamp = now.replace("-", "").replace(":", "").replace("+", "").replace("Z", "Z")
        return cls(run_id=f"{prefix}-{timestamp}", asserted_at=now)

    def provenance(
        self,
        *,
        method: str,
        actor_id: str,
        supersedes_id: str | None = None,
    ) -> dict[str, str | None]:
        return {
            "method": method,
            "actor_id": actor_id,
            "run_id": self.run_id,
            "asserted_at": self.asserted_at,
            "supersedes_id": supersedes_id,
        }


def stable_id(prefix: str, *parts: object, length: int = 24) -> str:
    """Return a stable opaque id derived from the supplied identity parts."""
    encoded = "\x1f".join("" if part is None else str(part) for part in parts).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:length]
    return f"{prefix}_{digest}"


def text_digest(*parts: object) -> str:
    """SHA-256 digest used to detect changed source text between tagging runs."""
    encoded = "\x1f".join("" if part is None else str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_json(value: object) -> str:
    """Serialize JSON deterministically for stable ids, comparisons, and Parquet.

    ``allow_nan=False`` because the default spells NaN and the infinities as
    ``NaN``/``Infinity``, which no JSON reader accepts: a digest taken over that
    text is stable and the artifact it describes is unparseable. Raising says so
    where the value enters, instead of in whatever reads the column back.
    """
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass
class JsonReadStats:
    """Counts malformed source JSON without allowing it to abort an entire rollup."""

    malformed_rows: int = 0
    examples: list[str] = field(default_factory=list)

    def malformed(self, *, table: str, row_id: object, column: str, value: object) -> None:
        self.malformed_rows += 1
        if len(self.examples) < 5:
            preview = repr(value)
            if len(preview) > 160:
                preview = f"{preview[:157]}..."
            self.examples.append(f"{table}.{column} row={row_id!r}: {preview}")

    def log(self, rollup: str) -> None:
        if not self.malformed_rows:
            return
        logger.warning(
            "{}: skipped {:,} malformed JSON source rows; examples: {}",
            rollup,
            self.malformed_rows,
            "; ".join(self.examples),
        )


def parse_json_list(
    value: object,
    *,
    stats: JsonReadStats | None = None,
    table: str = "source",
    row_id: object = None,
    column: str = "json",
) -> list | None:
    """Parse a source JSON array.

    ``None`` and blank strings represent an empty array. A malformed value or a
    valid non-array JSON value returns ``None`` so callers can skip and count the
    affected source row, as required by the rollup error-handling contract.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        if stats is not None:
            stats.malformed(table=table, row_id=row_id, column=column, value=value)
        return None
    if not isinstance(parsed, list):
        if stats is not None:
            stats.malformed(table=table, row_id=row_id, column=column, value=value)
        return None
    return parsed


def all_varchar_schema(columns: Sequence[str]) -> pa.Schema:
    """Return the repository's standard all-VARCHAR Arrow schema."""
    return pa.schema([(column, pa.string()) for column in columns])


def normalize_row(row: dict, columns: Sequence[str]) -> dict[str, str | None]:
    """Coerce a row onto an all-string schema while preserving nulls."""
    normalized: dict[str, str | None] = {}
    for column in columns:
        value = row.get(column)
        if value is None or isinstance(value, str):
            normalized[column] = value
        elif isinstance(value, (dict, list, tuple)):
            normalized[column] = canonical_json(value)
        else:
            normalized[column] = str(value)
    return normalized


def write_parquet_rows(
    path: Path,
    *,
    columns: Sequence[str],
    rows: Iterable[dict],
    row_group_size: int = 50_000,
) -> Path:
    """Write rows to an all-VARCHAR Parquet file in bounded batches."""
    schema = all_varchar_schema(columns)
    writer = pq.ParquetWriter(path, schema, compression="zstd")
    batch: list[dict[str, str | None]] = []
    written = 0
    try:
        for row in rows:
            batch.append(normalize_row(row, columns))
            if len(batch) >= row_group_size:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                written += len(batch)
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            written += len(batch)
        if written == 0:
            writer.write_table(schema.empty_table())
    finally:
        writer.close()
    return path


def iter_parquet_rows(path: Path, *, columns: Sequence[str] | None = None) -> Iterator[dict]:
    """Yield Parquet rows in batches without loading the full table into memory."""
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(columns=list(columns) if columns else None, batch_size=20_000):
        yield from batch.to_pylist()


def read_parquet_rows(path: Path) -> list[dict]:
    """Read a small registry/audit Parquet table into dictionaries."""
    if not path.exists():
        return []
    return pq.read_table(path).to_pylist()


def unique_rows(rows: Iterable[dict], *, key_columns: Sequence[str]) -> list[dict]:
    """Deduplicate identical logical rows, preserving the first occurrence."""
    seen: set[tuple[object, ...]] = set()
    result: list[dict] = []
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result

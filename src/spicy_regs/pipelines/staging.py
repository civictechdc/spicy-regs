"""Reusable extract → stage engine.

``stage_agencies`` is the generic fan-out shared by any agency-partitioned
pipeline: for every (agency, record type) it pumps a :class:`Reader` (built by
a caller-supplied factory) into a :class:`StagingWriter`, running agencies in
parallel. It knows nothing about *where* records come from or how processed
keys are tracked — it just reports the rows staged per record type and the
source keys it consumed, leaving manifest/dedup decisions to the caller.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.sources import StagingWriter
from spicy_regs.sources.base import Reader

# A factory the caller provides: given an agency + record type, return a
# configured Reader (connection details, filters, etc. are the caller's).
ReaderFactory = Callable[[str, RecordType], Reader]


@dataclass
class StageResult:
    """Outcome of a staging pass."""

    rows_by_type: dict[str, int]
    consumed_keys: set[str] = field(default_factory=set)


def stage_agencies(
    agencies: list[str],
    record_types: list[RecordType],
    staging_dir: Path,
    read: ReaderFactory,
    *,
    max_workers: int = 4,
) -> StageResult:
    """Stage every (agency, record type) in parallel; return rows + consumed keys."""

    def stage_one_agency(agency: str) -> tuple[dict[str, int], list[str]]:
        rows: dict[str, int] = {}
        keys: list[str] = []
        for record_type in record_types:
            reader = read(agency, record_type)
            writer = StagingWriter(agency, record_type, staging_dir)
            writer.write(reader.iter_records())
            rows[record_type.name] = writer.rows_written
            keys.extend(reader.last_keys)
            logger.info("[{}] {}: staged {} rows", agency, record_type.name, writer.rows_written)
        return rows, keys

    result = StageResult(rows_by_type={rt.name: 0 for rt in record_types})
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(stage_one_agency, agency) for agency in agencies]
        for future in as_completed(futures):
            rows, keys = future.result()
            for name, count in rows.items():
                result.rows_by_type[name] += count
            result.consumed_keys.update(keys)
    return result

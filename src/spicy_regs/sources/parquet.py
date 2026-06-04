"""Writer connector that persists records to a per-agency Parquet staging file.

Wraps ``write_staging`` so a stream of records for one agency and a single
:class:`~spicy_regs.schemas.RecordType` is written through the
:class:`~spicy_regs.sources.base.Writer` interface. This is the "local write"
stage only — merging staged files is a transform, and publishing to R2 is the
:mod:`spicy_regs.sources.r2` connector.
"""

from collections.abc import Iterable
from pathlib import Path

from spicy_regs.pipeline.transform import write_staging
from spicy_regs.schemas import RecordType
from spicy_regs.sources.base import Writer


class StagingWriter(Writer):
    """Writes records to ``{staging_dir}/{record_type.name}/{agency}.parquet``.

    The number of rows written by the most recent ``write`` call is available
    on ``rows_written``.
    """

    def __init__(self, agency: str, record_type: RecordType, staging_dir: Path) -> None:
        self.agency = agency
        self.record_type = record_type
        self.staging_dir = staging_dir
        self.rows_written = 0

    def write(self, records: Iterable[dict]) -> None:
        # write_staging expects a concrete list (it checks emptiness and length).
        materialized = list(records)
        self.rows_written = write_staging(
            self.agency,
            self.record_type.name,
            materialized,
            self.staging_dir,
            self.record_type.schema,
        )

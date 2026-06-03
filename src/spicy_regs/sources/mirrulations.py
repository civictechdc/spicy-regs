"""Reader connector for the Mirrulations S3 mirror of regulations.gov.

Wraps the existing S3 discovery + download functions so that one agency's
records of a single :class:`~spicy_regs.records.RecordType` are exposed
through the :class:`~spicy_regs.sources.base.Reader` interface. Listing,
year-filtering, and dedup against already-processed keys are delegated to
``list_json_files``; per-file download + parse is delegated to
``download_and_parse``.
"""

from collections.abc import Iterator
from typing import Any

from spicy_regs.pipeline.extract import download_and_parse, list_json_files
from spicy_regs.records import RecordType
from spicy_regs.sources.base import Reader


class MirrulationsReader(Reader):
    """Reads one agency's records of a single record type from Mirrulations S3.

    The keys discovered during the most recent ``iter_records`` call are kept
    on ``last_keys`` so the caller can append them to the run manifest.
    """

    def __init__(
        self,
        s3_resource: Any,
        bucket: str,
        prefix: str,
        agency: str,
        record_type: RecordType,
        processed_keys: Any = None,
        since_year: int | None = None,
        verbose: bool = False,
    ) -> None:
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix
        self.agency = agency
        self.record_type = record_type
        self.processed_keys = processed_keys
        self.since_year = since_year
        self.verbose = verbose
        self.last_keys: list[str] = []

    def iter_records(self) -> Iterator[dict]:
        self.last_keys = list_json_files(
            self.s3_resource,
            self.bucket,
            self.prefix,
            self.agency,
            self.record_type.name,
            self.record_type.path_pattern,
            self.processed_keys,
            self.verbose,
            self.since_year,
        )
        for key in self.last_keys:
            record = download_and_parse(
                self.s3_resource,
                self.bucket,
                key,
                self.record_type.extract,
            )
            if record is not None:
                yield record

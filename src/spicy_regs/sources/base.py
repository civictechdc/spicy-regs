"""Base classes for data source connectors.

Subclass `Reader` to add a connector that pulls records from an external
system (S3 bucket, REST API, scraped HTML, etc.). Subclass `Writer` to
add a connector that pushes records to an external system. A connector
that does both inherits from both.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator


class Reader(ABC):
    """A connector that reads records from an external system.

    Subclasses configure connection details via ``__init__`` and yield
    records (dicts) from ``iter_records``. Pagination, batching, retries,
    and dedup semantics are the subclass's responsibility.

    Subclasses that pull from a keyed source (S3 keys, URLs, file paths)
    should populate ``last_keys`` during ``iter_records`` with the keys
    *successfully* consumed, so the caller can append them to a manifest.
    Keys that were attempted but failed go on ``failed_keys`` instead — the
    caller must NOT manifest these, so the next run retries them. Both default
    to empty lists for sources without addressable keys.
    """

    last_keys: list[str] = []
    failed_keys: list[str] = []

    @abstractmethod
    def iter_records(self) -> Iterator[dict]: ...


class Writer(ABC):
    """A connector that writes records to an external system."""

    @abstractmethod
    def write(self, records: Iterable[dict]) -> None: ...

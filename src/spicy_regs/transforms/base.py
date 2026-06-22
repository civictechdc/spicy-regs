"""Base class for record-stream transforms.

A ``Transform`` sits between a Reader and a Writer in the record-stream
vocabulary::

    Reader.iter_records()  ->  Transform.apply(...)  ->  Writer.write(...)

Subclass it to map, filter, or enrich records as they flow past — for example
flattening a raw JSON payload into a schema-shaped row. Transforms operate on a
record stream (lazily, one at a time), so they compose and stay memory-light.

Bulk, whole-dataset operations — deduplicating by key, partitioning, building
summaries — are *not* Transforms: they need every row at once and are done
columnar/out-of-core (see ``spicy_regs.transforms.merge``). Keeping that line
sharp is deliberate: a Transform never has to buffer the whole dataset.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator


class Transform(ABC):
    """Maps a stream of records to another stream of records."""

    @abstractmethod
    def apply(self, records: Iterable[dict]) -> Iterator[dict]: ...

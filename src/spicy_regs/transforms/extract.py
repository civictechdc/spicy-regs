"""The json → record transform: flatten raw payloads into schema-shaped rows.

This is the "json2parquet" stage. A Reader yields raw regulations.gov JSON
payloads; :class:`ExtractRecords` maps each through its record type's ``extract``
function to a flat dict matching that type's schema, ready for a StagingWriter.
"""

from collections.abc import Iterable, Iterator

from spicy_regs.schemas import RecordType
from spicy_regs.transforms.base import Transform


class ExtractRecords(Transform):
    """Flattens raw JSON payloads into records via a :class:`RecordType`'s extractor."""

    def __init__(self, record_type: RecordType) -> None:
        self.record_type = record_type

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        extract = self.record_type.extract
        for payload in records:
            yield extract(payload)

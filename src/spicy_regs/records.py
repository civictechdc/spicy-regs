"""Shared data shapes that flow between sources and transforms."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecordType:
    """Description of one shape of data that flows through the pipeline.

    A RecordType pairs a name and primary key with a schema and an extract
    function that maps a raw payload (e.g. a parsed JSON dict) to a flat
    record dict matching the schema. Instances are values, not classes —
    contributors add new record shapes by constructing a new RecordType,
    not by subclassing.
    """

    name: str
    path_pattern: str
    schema: dict[str, Any]
    dedup_key: str
    extract: Callable[[dict], dict]

    def __post_init__(self) -> None:
        if self.dedup_key not in self.schema:
            raise ValueError(
                f"RecordType {self.name!r}: dedup_key {self.dedup_key!r} "
                f"not in schema"
            )
        if "modify_date" not in self.schema:
            raise ValueError(
                f"RecordType {self.name!r}: schema must include 'modify_date'"
            )

"""Compose several transforms into one, applied left-to-right.

``Chain(a, b)`` is the record-stream equivalent of ``b(a(records))`` — each
transform's lazy output stream feeds the next, so the chain buffers nothing.
Used by pipelines that need more than one shaping step per record type (e.g.
flatten *then* enrich) while ``stage_agencies`` still takes a single
``Transform`` per record type.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from spicy_regs.transforms.base import Transform


class Chain(Transform):
    """Applies ``transforms`` in order over a single record stream."""

    def __init__(self, *transforms: Transform) -> None:
        self.transforms = transforms

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        for transform in self.transforms:
            records = transform.apply(records)
        yield from records

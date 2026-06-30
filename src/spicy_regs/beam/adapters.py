"""Bridge the project's ``Reader``/``Transform``/``Writer`` abstractions to Beam.

This is the reusable takeaway of the spike. The existing record model maps onto
Apache Beam almost one-to-one::

    Reader.iter_records()        ->  ReadWith   (bounded source via beam.Create)
    Transform.apply(records)     ->  ApplyTransform (beam.FlatMap / a ParDo)
    bulk dedup (today in duckdb) ->  DedupBy    (beam.GroupByKey + pick-latest)
    Writer.write(records)        ->  WriteWith  (a sink beam.DoFn)

Each adapter is a thin :class:`beam.PTransform` so a pipeline reads the same way
the hand-rolled one does — just composed with ``|`` instead of chained
generators. These are intentionally simple, DirectRunner-friendly wrappers; the
docstrings call out where a production Beam pipeline would do something heavier
(a real splittable source, file IO connectors, a Combine instead of GroupByKey).
"""

from __future__ import annotations

import apache_beam as beam

from spicy_regs.sources.base import Reader, Writer
from spicy_regs.transforms.base import Transform


class ReadWith(beam.PTransform):
    """Turn an existing :class:`Reader` into a bounded Beam source.

    For the spike this simply materializes ``reader.iter_records()`` into a
    ``beam.Create`` at graph-construction time — fine for the small, bounded
    inputs a DirectRunner POC uses. A production Beam pipeline would instead use
    a real splittable source (e.g. ``apache_beam.io.fileio`` / a custom
    ``iobase.BoundedSource``) so reads parallelize and don't all happen on the
    driver.
    """

    def __init__(self, reader: Reader) -> None:
        self.reader = reader

    def expand(self, pbegin):
        return pbegin | "Create" >> beam.Create(list(self.reader.iter_records()))


class ApplyTransform(beam.PTransform):
    """Run an existing :class:`Transform` as a ``ParDo``.

    The ``Transform`` contract — ``apply(records) -> records`` over a
    one-record-at-a-time stream — is an almost exact match for Beam's element-wise
    ``FlatMap``: we hand each element to ``apply`` as a single-item stream and
    flatten whatever it yields. This works unchanged for the stateless,
    per-record transforms the base class is designed for (e.g.
    :class:`~spicy_regs.transforms.extract.ExtractRecords`).
    """

    def __init__(self, transform: Transform) -> None:
        self.transform = transform

    def expand(self, pcoll):
        transform = self.transform
        return pcoll | beam.FlatMap(lambda record: transform.apply([record]))


class DedupBy(beam.PTransform):
    """Deduplicate by ``key``, keeping the row with the latest ``keep`` value.

    This is the same dedup semantics ``merge_staging_files`` implements in DuckDB
    (group by primary key, keep the latest ``modify_date``) — but expressed in
    Beam's native shuffle: key the records, ``GroupByKey``, then pick the winner
    per key. It's the clearest illustration that the *bulk* whole-dataset step,
    the one deliberately kept out of the ``Transform`` contract today, also has a
    first-class home in Beam. (A ``CombinePerKey`` with a max-by-``keep`` combiner
    would scale better than ``GroupByKey`` for high-fanout keys; ``GroupByKey`` is
    used here for legibility.)
    """

    def __init__(self, key: str, keep: str = "modify_date") -> None:
        self.key = key
        self.keep = keep

    def expand(self, pcoll):
        key, keep = self.key, self.keep
        return (
            pcoll
            | "Key" >> beam.Map(lambda record: (record[key], record))
            | "Group" >> beam.GroupByKey()
            | "PickLatest" >> beam.Map(lambda kv: max(kv[1], key=lambda r: r.get(keep) or ""))
        )


class _WriterSink(beam.DoFn):
    """Buffer elements per bundle and flush them through a :class:`Writer`."""

    def __init__(self, writer: Writer) -> None:
        self.writer = writer

    def start_bundle(self) -> None:
        self._buffer: list[dict] = []

    def process(self, element: dict):
        self._buffer.append(element)

    def finish_bundle(self) -> None:
        if self._buffer:
            self.writer.write(self._buffer)
            self._buffer = []


class WriteWith(beam.PTransform):
    """Send a PCollection to an existing :class:`Writer`.

    ``Writer.write`` takes an iterable, so the sink ``DoFn`` buffers a bundle and
    hands it over in ``finish_bundle``. Note the DirectRunner caveat this exposes:
    a runner may pickle/clone the ``DoFn``, so a Writer that *accumulates results
    in memory* won't reliably surface them back to the driver — write to durable
    storage (or assert on the PCollection directly in tests) instead.
    """

    def __init__(self, writer: Writer) -> None:
        self.writer = writer

    def expand(self, pcoll):
        return pcoll | beam.ParDo(_WriterSink(self.writer))

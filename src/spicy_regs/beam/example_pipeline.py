"""A runnable Beam proof-of-concept, mirroring ``tests/test_example_pipeline.py``.

The hand-rolled reference pipeline wires one of each base class together:

    Reader (raw JSON)  ->  Transform (shape)  ->  Transform (enrich)  ->  Writer

This module wires the *same* building blocks — the real :class:`Pipeline` base,
the real :class:`ExtractRecords` transform, a small example ``Transform`` and
``Writer`` — but composes them with Apache Beam on the DirectRunner via the
adapters in :mod:`spicy_regs.beam.adapters`. It adds one step the streaming
reference can't express as a ``Transform``: a whole-dataset ``DedupBy`` that
collapses duplicate keys, keeping the latest ``modify_date`` (the same rule the
DuckDB merge uses).

Run it with the optional ``beam`` extra installed::

    uv sync --extra beam
    uv run run-beam-example
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

import apache_beam as beam
from cyclopts import App

from spicy_regs.beam.adapters import ApplyTransform, DedupBy, ReadWith, WriteWith
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.schemas import RecordType
from spicy_regs.sources.base import Reader, Writer
from spicy_regs.transforms import ExtractRecords, Transform


def _extract_post(raw: dict) -> dict:
    """Flatten a raw post payload into a schema-shaped record (module-level so
    Beam can pickle it by reference)."""
    return {
        "post_id": str(raw["id"]),
        "user_id": str(raw["userId"]),
        "title": raw["title"],
        "body": raw["body"],
        "modify_date": raw["modify_date"],
    }


# A RecordType describes one data shape: its schema, primary key, and how to
# flatten a raw payload. ``path_pattern`` is S3-specific, so this in-memory
# source leaves it unset.
Post = RecordType(
    name="posts",
    schema={
        "post_id": str,
        "user_id": str,
        "title": str,
        "body": str,
        "modify_date": str,
    },
    dedup_key="post_id",
    extract=_extract_post,
)


class InMemoryReader(Reader):
    """A source: yields canned raw payloads so the POC stays hermetic.

    Stands in for a network ``Reader`` (the real pipeline uses Mirrulations S3);
    keeping it in-memory makes the spike deterministic and offline.
    """

    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = payloads

    def iter_records(self) -> Iterator[dict]:
        yield from self.payloads


class UppercaseTitles(Transform):
    """A transform: uppercases the ``title`` field of each record."""

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        for record in records:
            yield {**record, "title": record["title"].upper()}


class StdoutWriter(Writer):
    """A sink: writes records by printing each as a line to stdout."""

    def write(self, records: Iterable[dict]) -> None:
        for record in records:
            print(record)


# Canned input. The third row repeats post_id 1 with a later modify_date, so the
# DedupBy step has something to collapse — keeping the updated row.
_RAW_POSTS = [
    {"id": 1, "userId": 1, "title": "first post", "body": "hello", "modify_date": "2024-01-01"},
    {"id": 2, "userId": 2, "title": "second post", "body": "world", "modify_date": "2024-01-01"},
    {"id": 1, "userId": 1, "title": "first post updated", "body": "hello again", "modify_date": "2024-06-01"},
]


class BeamExamplePipeline(Pipeline):
    """Wires the source, transforms, dedup, and sink together on Beam."""

    name = "beam-example"

    def __init__(self, writer: Writer | None = None) -> None:
        self.reader = InMemoryReader(_RAW_POSTS)
        self.extract = ExtractRecords(Post)  # the real json->record transform
        self.uppercase = UppercaseTitles()  # an example per-record transform
        self.writer = writer or StdoutWriter()

    def build(self, pipeline: beam.Pipeline):
        """The Reader -> ParDo -> ParDo -> GroupByKey chain, minus the sink.

        Returned separately from :meth:`run` so tests can assert on the resulting
        PCollection directly (the reliable way to check Beam output — see the
        DirectRunner caveat in ``WriteWith``).
        """
        return (
            pipeline
            | "Read" >> ReadWith(self.reader)
            | "Extract" >> ApplyTransform(self.extract)
            | "Uppercase" >> ApplyTransform(self.uppercase)
            | "Dedup" >> DedupBy(Post.dedup_key)
        )

    def run(self) -> None:
        # The classic in-process Python direct runner. We name it explicitly
        # rather than "DirectRunner" because Beam 2.72's "DirectRunner" defaults
        # to the Prism runner, which shells out to a platform-specific binary —
        # unnecessary for a small local POC and noisy when that binary can't run.
        with beam.Pipeline(runner="BundleBasedDirectRunner") as pipeline:
            self.build(pipeline) | "Write" >> WriteWith(self.writer)


app = App(name="run-beam-example", help="Run the Apache Beam proof-of-concept pipeline (DirectRunner).")


@app.default
def main() -> None:
    """Run the Beam example pipeline end-to-end on the DirectRunner."""
    BeamExamplePipeline().run()


if __name__ == "__main__":
    app()

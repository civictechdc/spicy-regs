"""End-to-end reference: one of each base class wired together.

This file is the contributor reference — copy it when adding your own source,
transform, or pipeline. It implements a minimal pipeline that fetches posts
from JSONPlaceholder and uppercases their titles, composed exactly the way the
real pipeline is:

    Reader (raw JSON)  ->  Transform (shape)  ->  Transform (enrich)  ->  Writer

The test monkeypatches ``urllib.request.urlopen`` with canned data so CI stays
hermetic — to see it run against the live API, delete the monkeypatch and run
``pytest -s``.
"""

from collections.abc import Iterable, Iterator
from io import BytesIO
from json import dumps as json_dumps
from json import loads
from urllib.request import urlopen

import pytest

from spicy_regs.pipelines import Pipeline
from spicy_regs.schemas import RecordType
from spicy_regs.sources import Reader, Writer
from spicy_regs.transforms import ExtractRecords, Transform

# A RecordType describes one data shape: its schema, primary key, and how to
# flatten a raw payload into a row. ``path_pattern`` is S3-specific, so an HTTP
# source like this one leaves it unset.
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
    extract=lambda raw: {
        "post_id": str(raw["id"]),
        "user_id": str(raw["userId"]),
        "title": raw["title"],
        "body": raw["body"],
        "modify_date": "1970-01-01",
    },
)


class JsonPlaceholderReader(Reader):
    """A source: fetches posts and yields the *raw* JSON payloads.

    Readers stay pure sources — shaping a payload into a record is the
    ExtractRecords transform's job, not the reader's.
    """

    def __init__(self, url: str) -> None:
        self.url = url

    def iter_records(self) -> Iterator[dict]:
        with urlopen(self.url) as resp:
            yield from loads(resp.read())


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


class ExamplePipeline(Pipeline):
    """Wires the source, transforms, and sink together."""

    name = "example"

    def __init__(self) -> None:
        self.reader = JsonPlaceholderReader("https://jsonplaceholder.typicode.com/posts")
        self.extract = ExtractRecords(Post)  # built-in transform: raw JSON -> record
        self.uppercase = UppercaseTitles()  # our own transform
        self.writer = StdoutWriter()

    def run(self) -> None:
        records = self.reader.iter_records()
        records = self.extract.apply(records)
        records = self.uppercase.apply(records)
        self.writer.write(records)


_CANNED_POSTS = [
    {"id": 1, "userId": 1, "title": "first post", "body": "hello"},
    {"id": 2, "userId": 2, "title": "second post", "body": "world"},
]


def test_example_pipeline_runs_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_urlopen(url: str):  # noqa: ARG001 — signature matches stdlib
        return BytesIO(json_dumps(_CANNED_POSTS).encode())

    monkeypatch.setattr(
        "tests.test_example_pipeline.urlopen",
        fake_urlopen,
    )

    pipeline = ExamplePipeline()
    pipeline.run()

    captured = capsys.readouterr()
    assert "FIRST POST" in captured.out
    assert "SECOND POST" in captured.out
    assert "post_id" in captured.out

"""End-to-end reference: one of each base class wired together.

This file is a contributor reference. It implements a minimal pipeline
that fetches posts from JSONPlaceholder, uppercases their titles, and
prints them. The test monkeypatches ``urllib.request.urlopen`` with
canned data so CI stays hermetic — to see the abstractions run against
the live API, delete the monkeypatch and run ``pytest -s``.
"""

from collections.abc import Iterable, Iterator
from io import BytesIO
from json import dumps as json_dumps
from json import loads
from urllib.request import urlopen

import pytest

from spicy_regs.pipelines import Pipeline
from spicy_regs.records import RecordType
from spicy_regs.sources import Reader, Writer


Post = RecordType(
    name="posts",
    path_pattern="/posts",
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
    """Reads posts from JSONPlaceholder and yields extracted records."""

    def __init__(self, url: str, record_type: RecordType) -> None:
        self.url = url
        self.record_type = record_type

    def iter_records(self) -> Iterator[dict]:
        with urlopen(self.url) as resp:
            payload = loads(resp.read())
        for raw in payload:
            yield self.record_type.extract(raw)


def uppercase_titles(records: Iterable[dict]) -> Iterator[dict]:
    """Convention-only transform: uppercases the ``title`` field."""
    for r in records:
        yield {**r, "title": r["title"].upper()}


class StdoutWriter(Writer):
    """Writes records by printing each as a line to stdout."""

    def write(self, records: Iterable[dict]) -> None:
        for r in records:
            print(r)


class ExamplePipeline(Pipeline):
    name = "example"

    def __init__(self) -> None:
        self.reader = JsonPlaceholderReader(
            "https://jsonplaceholder.typicode.com/posts",
            Post,
        )
        self.writer = StdoutWriter()

    def run(self) -> None:
        self.writer.write(uppercase_titles(self.reader.iter_records()))


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

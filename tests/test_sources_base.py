"""Tests for the Reader and Writer base classes."""

from collections.abc import Iterable, Iterator

import pytest

from spicy_regs.sources import Reader, Writer


def test_reader_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        Reader()  # type: ignore[abstract]


def test_writer_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        Writer()  # type: ignore[abstract]


def test_reader_subclass_missing_iter_records_cannot_be_instantiated() -> None:
    class IncompleteReader(Reader):
        pass

    with pytest.raises(TypeError):
        IncompleteReader()  # type: ignore[abstract]


def test_writer_subclass_missing_write_cannot_be_instantiated() -> None:
    class IncompleteWriter(Writer):
        pass

    with pytest.raises(TypeError):
        IncompleteWriter()  # type: ignore[abstract]


def test_minimal_reader_subclass() -> None:
    class FakeReader(Reader):
        def iter_records(self) -> Iterator[dict]:
            yield {"id": "1"}
            yield {"id": "2"}

    reader = FakeReader()
    assert list(reader.iter_records()) == [{"id": "1"}, {"id": "2"}]


def test_minimal_writer_subclass() -> None:
    class FakeWriter(Writer):
        def __init__(self) -> None:
            self.received: list[dict] = []

        def write(self, records: Iterable[dict]) -> None:
            self.received.extend(records)

    writer = FakeWriter()
    writer.write([{"id": "1"}, {"id": "2"}])
    assert writer.received == [{"id": "1"}, {"id": "2"}]


def test_reader_writer_combined_subclass() -> None:
    class FakeStore(Reader, Writer):
        def __init__(self) -> None:
            self.items: list[dict] = []

        def iter_records(self) -> Iterator[dict]:
            yield from self.items

        def write(self, records: Iterable[dict]) -> None:
            self.items.extend(records)

    store = FakeStore()
    store.write([{"id": "1"}])
    store.write([{"id": "2"}])
    assert list(store.iter_records()) == [{"id": "1"}, {"id": "2"}]


def test_combined_subclass_missing_one_method_cannot_be_instantiated() -> None:
    class HalfBaked(Reader, Writer):
        def iter_records(self) -> Iterator[dict]:
            yield from ()

    with pytest.raises(TypeError):
        HalfBaked()  # type: ignore[abstract]

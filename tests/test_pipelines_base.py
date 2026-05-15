"""Tests for the Pipeline base class."""

import pytest

from spicy_regs.pipelines import Pipeline


def test_pipeline_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        Pipeline()  # type: ignore[abstract]


def test_subclass_missing_run_cannot_be_instantiated() -> None:
    class IncompletePipeline(Pipeline):
        name = "incomplete"

    with pytest.raises(TypeError):
        IncompletePipeline()  # type: ignore[abstract]


def test_subclass_without_name_raises_attribute_error_on_access() -> None:
    class NamelessPipeline(Pipeline):
        def run(self) -> None:
            return None

    pipeline = NamelessPipeline()
    with pytest.raises(AttributeError):
        _ = NamelessPipeline.name
    with pytest.raises(AttributeError):
        _ = pipeline.name


def test_minimal_pipeline_subclass() -> None:
    class FakePipeline(Pipeline):
        name = "fake"

        def __init__(self) -> None:
            self.ran = False

        def run(self) -> None:
            self.ran = True

    assert FakePipeline.name == "fake"
    pipeline = FakePipeline()
    assert pipeline.ran is False
    pipeline.run()
    assert pipeline.ran is True


def test_pipeline_name_addressable_without_instantiation() -> None:
    class NamedPipeline(Pipeline):
        name = "named"

        def run(self) -> None:
            return None

    assert NamedPipeline.name == "named"

"""Tests for RegulationsPipeline (Pipeline facade over the Prefect flow)."""

import pytest

import spicy_regs.pipelines.regulations as regulations
from spicy_regs.pipelines import Pipeline, RegulationsPipeline


def test_is_pipeline_subclass_with_name() -> None:
    assert issubclass(RegulationsPipeline, Pipeline)
    assert RegulationsPipeline.name == "regulations"


def test_run_forwards_kwargs_to_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_flow(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(regulations, "pipeline", fake_flow)

    RegulationsPipeline(agency="EPA", skip_upload=True, since_year=2025).run()

    assert calls == [{"agency": "EPA", "skip_upload": True, "since_year": 2025}]


def test_run_with_no_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(regulations, "pipeline", lambda **kwargs: calls.append(kwargs))

    RegulationsPipeline().run()

    assert calls == [{}]

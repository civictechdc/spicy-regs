"""Tests for the run-pipeline CLI runner."""

import pytest

import spicy_regs.pipelines.regulations as regulations
from spicy_regs.pipelines import RegulationsPipeline
from spicy_regs.pipelines.run import PIPELINES, run


def test_registry_contains_regulations() -> None:
    assert PIPELINES["regulations"] is RegulationsPipeline


def test_unknown_pipeline_name_exits() -> None:
    with pytest.raises(SystemExit) as exc:
        run(name="does-not-exist")
    assert "does-not-exist" in str(exc.value)


def test_run_forwards_options_to_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(regulations, "pipeline", lambda **kwargs: calls.append(kwargs))

    run(name="regulations", agency="EPA", skip_upload=True, since_year=2025)

    assert len(calls) == 1
    forwarded = calls[0]
    assert forwarded["agency"] == "EPA"
    assert forwarded["skip_upload"] is True
    assert forwarded["since_year"] == 2025
    # Defaults are forwarded too, mirroring the flow's own defaults.
    assert forwarded["batch_size"] == 45
    assert forwarded["full_refresh"] is False

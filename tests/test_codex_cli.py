"""Tests for the tool-free Codex CLI structured-output adapter."""

from __future__ import annotations

import json
import subprocess

import pytest

from spicy_regs.ontology.codex_cli import (
    DISABLED_CODEX_FEATURES,
    CodexCliProviderError,
    CodexCliStructuredOutputModel,
)
from spicy_regs.ontology.receipt import _valid_completed_model_call

SCHEMA = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}


def _event_stream(
    *,
    message: str = '{"answer":"ok"}',
    item_type: str = "agent_message",
) -> str:
    events = [
        {
            "type": "thread.started",
            "thread_id": "019f-test-thread",
        },
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {
                "id": "item-1",
                "type": item_type,
                "text": message,
            },
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 6_875,
                "output_tokens": 20,
            },
        },
    ]
    return "\n".join(json.dumps(event) for event in events) + "\n"


class _Runner:
    def __init__(self, event_stream: str) -> None:
        self.event_stream = event_stream
        self.calls: list[list[str]] = []

    def __call__(self, command, **kwargs):
        del kwargs
        self.calls.append(list(command))
        if command[-1] == "--version":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="codex-cli 0.145.0\n",
                stderr="",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=self.event_stream,
            stderr="",
        )


def _model(
    monkeypatch: pytest.MonkeyPatch,
    runner: _Runner,
) -> CodexCliStructuredOutputModel:
    monkeypatch.setattr(
        "spicy_regs.ontology.codex_cli.shutil.which",
        lambda _: "/test/codex",
    )
    return CodexCliStructuredOutputModel(
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        runner=runner,
    )


def test_codex_cli_returns_schema_valid_json_and_receipt_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream())
    model = _model(monkeypatch, runner)

    result = model.structured_json(
        name="test_answer",
        schema=SCHEMA,
        instructions="Answer the supplied payload.",
        payload={"question": "test"},
        max_output_tokens=100,
    )

    assert result == {"answer": "ok"}
    assert model.last_call_metadata is not None
    assert _valid_completed_model_call(model.last_call_metadata)
    assert model.last_call_metadata["transport"] == "codex-cli"
    assert model.last_call_metadata["tools_enabled"] is False
    assert model.last_call_metadata["schema_validated_locally"] is True
    assert set(DISABLED_CODEX_FEATURES).issubset(runner.calls[0])
    assert "--output-schema" in runner.calls[0]
    assert "--ephemeral" in runner.calls[0]
    assert "--ignore-user-config" in runner.calls[0]


def test_codex_cli_rejects_any_tool_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(
        _event_stream(
            message="",
            item_type="command_execution",
        )
    )
    model = _model(monkeypatch, runner)

    with pytest.raises(
        CodexCliProviderError,
        match="forbidden tool events",
    ):
        model.structured_json(
            name="test_answer",
            schema=SCHEMA,
            instructions="Answer.",
            payload={"question": "test"},
            max_output_tokens=100,
        )

    assert model.last_call_metadata is not None
    assert model.last_call_metadata["forbidden_item_types"] == [
        "command_execution"
    ]
    assert not _valid_completed_model_call(model.last_call_metadata)


def test_codex_cli_rejects_schema_invalid_final_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream(message='{"wrong":"shape"}'))
    model = _model(monkeypatch, runner)

    with pytest.raises(
        CodexCliProviderError,
        match="violated the output schema",
    ):
        model.structured_json(
            name="test_answer",
            schema=SCHEMA,
            instructions="Answer.",
            payload={"question": "test"},
            max_output_tokens=100,
        )

    assert model.last_call_metadata is not None
    assert model.last_call_metadata["status"] == "failed"


def test_codex_cli_rejects_malformed_event_stream_with_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner("not-json\n"))

    with pytest.raises(
        CodexCliProviderError,
        match="stdout line 1 was not JSON",
    ):
        model.structured_json(
            name="test_answer",
            schema=SCHEMA,
            instructions="Answer.",
            payload={"question": "test"},
            max_output_tokens=100,
        )

    assert model.last_call_metadata is not None
    assert model.last_call_metadata["status"] == "failed"
    assert model.last_call_metadata["event_stream_sha256"]


def test_codex_cli_request_omits_temporary_paths_and_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner(_event_stream()))

    request = model.secret_free_request(
        name="test_answer",
        schema=SCHEMA,
        instructions="Answer.",
        payload={"question": "test"},
        max_output_tokens=100,
    )
    serialized = json.dumps(request, sort_keys=True)

    assert request["transport"] == "codex-cli"
    assert request["max_output_tokens"]["enforced_by_transport"] is False
    assert "shell_tool" in request["disabled_features"]
    assert "/private/tmp" not in serialized
    assert "sk-proj-" not in serialized

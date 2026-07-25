"""Tests for the v3 tool-free Codex CLI structured-text-model adapter.

Codex is the second arm of the v3 "Structured text model" interface: the same
(instructions, schema, payload, max_output_tokens) call, checked JSON plus
secret-free call details returned together, and no mutable last-call channel.
The hardening the design requires — ignored user settings, disabled optional
features, read-only temporary workspace, removed credentials, strict event
allowlist, and a local schema check — is asserted here directly.
"""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.adapters import (
    SHARED_CALL_DETAIL_KEYS,
    StructuredTextModel,
    StructuredTextResult,
)
from spicy_regs.docpipeline.adapters.codex_cli import (
    ALLOWED_ITEM_TYPES,
    ALLOWED_STREAM_EVENTS,
    DISABLED_CODEX_FEATURES,
    CodexCliProviderError,
    CodexCliStructuredTextModel,
    build_codex_cli_secret_free_request,
)

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}
INSTRUCTIONS = "Answer the supplied payload."
PAYLOAD: dict[str, Any] = {"question": "test"}
FAKE_API_KEY = "sk-proj-000000000000000000000000FAKEKEYFORTESTS"
SECRET_PATTERN = re.compile(r"sk-(proj-)?[A-Za-z0-9_-]{8,}")

# Hardening pins. These are written out in full, never derived from the module
# under test: a constant that supplies its own expectation pins nothing. Adding,
# removing, or renaming a disabled feature or an allowlisted event must fail
# here first and be reviewed as a deliberate loosening of the sandbox.
EXPECTED_DISABLED_CODEX_FEATURES: tuple[str, ...] = (
    "apps",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode_host",
    "computer_use",
    "fast_mode",
    "goals",
    "guardian_approval",
    "hooks",
    "image_generation",
    "in_app_browser",
    "memories",
    "mentions_v2",
    "multi_agent",
    "plugin_sharing",
    "plugins",
    "remote_compaction_v2",
    "remote_plugin",
    "shell_snapshot",
    "shell_tool",
    "skill_search",
    "tool_call_mcp_elicitation",
    "tool_suggest",
    "unified_exec",
    "workspace_dependencies",
)
EXPECTED_ALLOWED_STREAM_EVENTS = frozenset(
    {
        "error",
        "item.completed",
        "item.started",
        "item.updated",
        "thread.started",
        "turn.completed",
        "turn.failed",
        "turn.started",
    }
)
EXPECTED_ALLOWED_ITEM_TYPES = frozenset({"agent_message", "reasoning"})


def _event_stream(
    *,
    message: str = '{"answer":"ok"}',
    item_type: str = "agent_message",
    extra_events: list[dict[str, Any]] | None = None,
) -> str:
    events: list[dict[str, Any]] = [
        {"type": "thread.started", "thread_id": "019f-test-thread"},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {"id": "item-1", "type": item_type, "text": message},
        },
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 6_875, "output_tokens": 20},
        },
    ]
    events.extend(extra_events or [])
    return "\n".join(json.dumps(event) for event in events) + "\n"


class _Runner:
    """Fake ``subprocess.run`` that records every command and environment."""

    def __init__(self, event_stream: str, *, returncode: int = 0, error: Exception | None = None) -> None:
        self.event_stream = event_stream
        self.returncode = returncode
        self.error = error
        self.calls: list[list[str]] = []
        self.kwargs: list[dict[str, Any]] = []

    def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(command))
        self.kwargs.append(dict(kwargs))
        if command[-1] == "--version":
            return subprocess.CompletedProcess(command, 0, stdout="codex-cli 0.145.0\n", stderr="")
        if self.error is not None:
            raise self.error
        return subprocess.CompletedProcess(command, self.returncode, stdout=self.event_stream, stderr="")


def _model(monkeypatch: pytest.MonkeyPatch, runner: _Runner) -> CodexCliStructuredTextModel:
    monkeypatch.setattr(
        "spicy_regs.docpipeline.adapters.codex_cli.shutil.which",
        lambda _: "/test/codex",
    )
    return CodexCliStructuredTextModel(
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        runner=runner,
    )


def _call(model: CodexCliStructuredTextModel, **overrides: Any) -> StructuredTextResult:
    request: dict[str, Any] = {
        "name": "test_answer",
        "schema": SCHEMA,
        "instructions": INSTRUCTIONS,
        "payload": PAYLOAD,
        "max_output_tokens": 256,
    }
    request.update(overrides)
    return model.structured_json(**request)


def test_disabled_feature_list_matches_its_literal_pin() -> None:
    assert DISABLED_CODEX_FEATURES == EXPECTED_DISABLED_CODEX_FEATURES
    assert len(EXPECTED_DISABLED_CODEX_FEATURES) == 27
    assert len(set(EXPECTED_DISABLED_CODEX_FEATURES)) == 27


def test_allowed_item_types_match_their_literal_pin() -> None:
    assert ALLOWED_ITEM_TYPES == frozenset({"agent_message", "reasoning"})


def test_allowed_stream_events_match_their_literal_pin() -> None:
    assert ALLOWED_STREAM_EVENTS == frozenset(
        {
            "error",
            "item.completed",
            "item.started",
            "item.updated",
            "thread.started",
            "turn.completed",
            "turn.failed",
            "turn.started",
        }
    )
    assert len(EXPECTED_ALLOWED_STREAM_EVENTS) == 8


def test_success_returns_checked_output_and_call_details_together(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream())
    model = _model(monkeypatch, runner)

    result = _call(model)

    assert isinstance(result, StructuredTextResult)
    assert result.output == {"answer": "ok"}
    assert result.call["provider"] == "codex-cli"
    assert result.call["transport"] == "codex-cli"
    assert result.call["model_id"] == "codex-cli:gpt-5.6-sol"
    assert result.call["status"] == "completed"
    assert result.call["schema_validated_locally"] is True
    assert result.call["tools_enabled"] is False
    assert result.call["thread_id"] == "019f-test-thread"
    assert result.call["response_id"] == "codex-thread:019f-test-thread"
    assert result.call["cli_version"] == "codex-cli 0.145.0"
    assert result.call["exit_code"] == 0
    assert result.call["attempt_count"] == 1
    assert result.call["retry_count"] == 0
    assert result.call["event_count"] == 4
    assert result.call["disabled_features"] == list(EXPECTED_DISABLED_CODEX_FEATURES)


def test_usage_is_extracted_from_the_turn_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _model(monkeypatch, _Runner(_event_stream()))

    result = _call(model)

    assert result.call["input_tokens"] == 6_875
    assert result.call["output_tokens"] == 20
    assert result.call["total_tokens"] == 6_895
    assert result.call["max_output_tokens"] == 256
    assert result.call["max_output_tokens_enforced"] is False


def test_result_is_immutable_and_adapter_keeps_no_last_call_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner(_event_stream()))

    result = _call(model)

    with pytest.raises(dataclasses.FrozenInstanceError):
        result.call = {}  # ty: ignore[invalid-assignment]
    assert not hasattr(model, "last_call_metadata")
    assert not any("last_call" in name for name in dir(model))


def test_command_construction_pins_every_hardening_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream())
    model = _model(monkeypatch, runner)

    _call(model)

    command = runner.calls[0]
    root = str(runner.kwargs[0]["cwd"])
    schema_path = str(Path(root) / "schema.json")
    instructions_path = str(Path(root) / "base-instructions.txt")
    assert command[:16] == [
        "/test/codex",
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--strict-config",
        "--sandbox",
        "read-only",
        "--skip-git-repo-check",
        "-C",
        root,
        "-m",
        "gpt-5.6-sol",
        "-c",
        'model_reasoning_effort="medium"',
        "-c",
    ]
    assert command[16] == f'model_instructions_file="{instructions_path}"'
    assert command[17:22] == [
        "-c",
        'personality="none"',
        "-c",
        'web_search="disabled"',
        "-c",
    ]
    assert command[22] == "agents.enabled=false"
    disable_flags = command[23 : 23 + 2 * len(EXPECTED_DISABLED_CODEX_FEATURES)]
    assert disable_flags == [
        part for feature in EXPECTED_DISABLED_CODEX_FEATURES for part in ("--disable", feature)
    ]
    assert command[23 + 2 * len(EXPECTED_DISABLED_CODEX_FEATURES) :] == [
        "--output-schema",
        schema_path,
        "--json",
        INSTRUCTIONS,
    ]
    assert runner.kwargs[0]["timeout"] == model.timeout_seconds
    assert runner.kwargs[0]["check"] is False
    assert json.loads(runner.kwargs[0]["input"]) == PAYLOAD


def test_credentials_are_removed_from_the_subprocess_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream())
    monkeypatch.setenv("OPENAI_API_KEY", FAKE_API_KEY)
    monkeypatch.setenv("CODEX_API_KEY", FAKE_API_KEY)
    monkeypatch.setenv("SPICY_REGS_TEST_MARKER", "kept")
    model = _model(monkeypatch, runner)

    result = _call(model)

    environment = runner.kwargs[0]["env"]
    assert "OPENAI_API_KEY" not in environment
    assert "CODEX_API_KEY" not in environment
    assert environment["SPICY_REGS_TEST_MARKER"] == "kept"
    assert SECRET_PATTERN.search(json.dumps(result.call, sort_keys=True, default=str)) is None


def test_forbidden_tool_events_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _model(monkeypatch, _Runner(_event_stream(message="", item_type="command_execution")))

    with pytest.raises(CodexCliProviderError, match="forbidden tool events") as failure:
        _call(model)

    assert failure.value.call["forbidden_item_types"] == ["command_execution"]
    assert failure.value.call["status"] == "failed"
    assert "command_execution" not in EXPECTED_ALLOWED_ITEM_TYPES


def test_unknown_stream_events_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = _event_stream(extra_events=[{"type": "tool.invoked"}])
    model = _model(monkeypatch, _Runner(stream))

    with pytest.raises(CodexCliProviderError, match="unknown event types") as failure:
        _call(model)

    assert failure.value.call["unknown_event_types"] == ["tool.invoked"]
    assert "tool.invoked" not in EXPECTED_ALLOWED_STREAM_EVENTS


def test_final_message_is_validated_against_the_declared_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner(_event_stream(message='{"wrong":"shape"}')))

    with pytest.raises(CodexCliProviderError, match="violated the output schema") as failure:
        _call(model)

    assert failure.value.call["status"] == "failed"
    assert failure.value.call["schema_validated_locally"] is False


def test_non_json_final_message_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _model(monkeypatch, _Runner(_event_stream(message="not json")))

    with pytest.raises(CodexCliProviderError, match="final message was not JSON") as failure:
        _call(model)

    assert failure.value.call["status"] == "failed"


def test_malformed_event_stream_is_rejected_with_call_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner("not-json\n"))

    with pytest.raises(CodexCliProviderError, match="stdout line 1 was not JSON") as failure:
        _call(model)

    assert failure.value.call["status"] == "failed"
    assert len(str(failure.value.call["event_stream_sha256"])) == 64
    assert failure.value.call["exit_code"] == 0


def test_nonzero_exit_status_fails_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    model = _model(monkeypatch, _Runner(_event_stream(), returncode=2))

    with pytest.raises(CodexCliProviderError, match="exited with status 2") as failure:
        _call(model)

    assert failure.value.call["exit_code"] == 2
    assert failure.value.call["status"] == "failed"


def test_process_failure_returns_safe_failure_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _Runner(_event_stream(), error=subprocess.TimeoutExpired("codex", 300.0))
    model = _model(monkeypatch, runner)

    with pytest.raises(CodexCliProviderError, match="TimeoutExpired") as failure:
        _call(model)

    assert failure.value.call["status"] == "failed"
    assert failure.value.call["error_code"] == "TimeoutExpired"
    assert len(str(failure.value.call["command_sha256"])) == 64
    assert SECRET_PATTERN.search(json.dumps(failure.value.call, sort_keys=True, default=str)) is None


def test_secret_free_request_omits_temporary_paths_and_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner(_event_stream()))

    request = model.secret_free_request(
        name="test_answer",
        schema=SCHEMA,
        instructions=INSTRUCTIONS,
        payload=PAYLOAD,
        max_output_tokens=256,
    )
    serialized = json.dumps(request, sort_keys=True)

    assert request == build_codex_cli_secret_free_request(
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        name="test_answer",
        schema=SCHEMA,
        instructions=INSTRUCTIONS,
        payload=PAYLOAD,
        max_output_tokens=256,
    )
    assert request["transport"] == "codex-cli"
    assert request["sandbox"] == "read-only"
    assert request["ignore_user_config"] is True
    assert request["ignore_rules"] is True
    assert request["max_output_tokens"]["enforced_by_transport"] is False
    assert request["disabled_features"] == list(EXPECTED_DISABLED_CODEX_FEATURES)
    assert "/private/tmp" not in serialized
    assert "/var/folders" not in serialized
    assert SECRET_PATTERN.search(serialized) is None


def test_command_hash_is_stable_across_temporary_directories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_event_stream())
    model = _model(monkeypatch, runner)

    first = _call(model)
    second = _call(model)

    assert runner.kwargs[0]["cwd"] != runner.kwargs[2]["cwd"]
    assert first.call["command_sha256"] == second.call["command_sha256"]
    assert first.call["request_sha256"] == second.call["request_sha256"]


def test_every_success_and_failure_path_emits_the_shared_call_detail_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt reader must not need to know which arm or path produced it."""
    calls: list[dict[str, Any]] = []

    calls.append(_call(_model(monkeypatch, _Runner(_event_stream()))).call)

    schema_model = _model(monkeypatch, _Runner(_event_stream(message='{"wrong":"shape"}')))
    with pytest.raises(CodexCliProviderError) as schema_failure:
        _call(schema_model)
    calls.append(schema_failure.value.call)

    exit_model = _model(monkeypatch, _Runner(_event_stream(), returncode=2))
    with pytest.raises(CodexCliProviderError) as exit_failure:
        _call(exit_model)
    calls.append(exit_failure.value.call)

    stream_model = _model(monkeypatch, _Runner("not-json\n"))
    with pytest.raises(CodexCliProviderError) as stream_failure:
        _call(stream_model)
    calls.append(stream_failure.value.call)

    process_model = _model(
        monkeypatch,
        _Runner(_event_stream(), error=subprocess.TimeoutExpired("codex", 300.0)),
    )
    with pytest.raises(CodexCliProviderError) as process_failure:
        _call(process_model)
    calls.append(process_failure.value.call)

    assert [call["status"] for call in calls] == ["completed", "failed", "failed", "failed", "failed"]
    for call in calls:
        assert set(SHARED_CALL_DETAIL_KEYS) <= set(call)
        assert isinstance(call["schema_validated_locally"], bool)
        assert call["schema_name"] == "test_answer"
        assert call["response_model"] == "gpt-5.6-sol"
        assert call["max_output_tokens"] == 256
        assert isinstance(call["input_tokens"], int)
        assert isinstance(call["output_tokens"], int)
        assert isinstance(call["total_tokens"], int)
    assert [call["schema_validated_locally"] for call in calls] == [True, False, False, False, False]
    assert calls[3]["response_id"] is None
    assert calls[4]["response_id"] is None


def test_adapter_satisfies_the_shared_structured_text_model_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _model(monkeypatch, _Runner(_event_stream()))

    assert isinstance(model, StructuredTextModel)
    assert model.model_id == "codex-cli:gpt-5.6-sol"


def test_constructor_rejects_an_unsupported_reasoning_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "spicy_regs.docpipeline.adapters.codex_cli.shutil.which",
        lambda _: "/test/codex",
    )

    with pytest.raises(ValueError, match="reasoning_effort"):
        CodexCliStructuredTextModel(reasoning_effort="turbo", runner=_Runner(_event_stream()))
    with pytest.raises(ValueError, match="timeout_seconds"):
        CodexCliStructuredTextModel(timeout_seconds=0.0, runner=_Runner(_event_stream()))


def test_missing_executable_fails_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "spicy_regs.docpipeline.adapters.codex_cli.shutil.which",
        lambda _: None,
    )

    with pytest.raises(FileNotFoundError, match="Codex CLI executable not found"):
        CodexCliStructuredTextModel(runner=_Runner(_event_stream()))


def test_unverified_cli_version_fails_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    class _VersionlessRunner(_Runner):
        def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            if command[-1] == "--version":
                self.calls.append(list(command))
                self.kwargs.append(dict(kwargs))
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="boom")
            return super().__call__(command, **kwargs)

    model = _model(monkeypatch, _VersionlessRunner(_event_stream()))

    with pytest.raises(CodexCliProviderError, match="version could not be verified") as failure:
        _call(model)

    assert failure.value.call["cli_version"] == "unknown"

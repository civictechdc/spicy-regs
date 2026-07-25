"""Tool-free Codex CLI adapter for structured ontology decisions."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator

from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.llm import DEFAULT_MODEL, DEFAULT_REASONING_EFFORT

BARE_BASE_INSTRUCTIONS = (
    "Follow the user instructions. Return only schema-valid JSON. Do not use tools."
)
DISABLED_CODEX_FEATURES = (
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
ALLOWED_STREAM_EVENTS = frozenset(
    {
        "thread.started",
        "turn.started",
        "turn.completed",
        "turn.failed",
        "item.started",
        "item.updated",
        "item.completed",
        "error",
    }
)
ALLOWED_ITEM_TYPES = frozenset({"agent_message", "reasoning"})


class CodexCliProviderError(RuntimeError):
    """A Codex CLI run failed the tool-free structured-output contract."""


class CodexCliStructuredOutputModel:
    """Run a schema-constrained Codex turn with every optional tool disabled."""

    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        reasoning_effort: str = DEFAULT_REASONING_EFFORT,
        executable: str = "codex",
        timeout_seconds: float = 300.0,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        resolved = shutil.which(executable)
        if resolved is None:
            raise FileNotFoundError(f"Codex CLI executable not found: {executable}")
        self.executable = resolved
        self.model = model
        self.model_id = f"codex-cli:{model}"
        self.reasoning_effort = reasoning_effort
        self.service_tier = "unavailable"
        self.timeout_seconds = timeout_seconds
        self.last_call_metadata: dict[str, object] | None = None
        self.run_configuration = {
            "provider": "codex-cli",
            "model": model,
            "model_id": self.model_id,
            "reasoning_effort": reasoning_effort,
            "service_tier": self.service_tier,
            "timeout_seconds": timeout_seconds,
            "session_persistence": "ephemeral",
            "tools_enabled": False,
            "disabled_features": list(DISABLED_CODEX_FEATURES),
        }
        self._runner = runner

    def secret_free_request(
        self,
        *,
        name: str,
        schema: Mapping[str, Any],
        instructions: str,
        payload: Mapping[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        """Return the logical request independently of temporary file paths."""
        return build_codex_cli_secret_free_request(
            model=self.model,
            reasoning_effort=self.reasoning_effort,
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )

    def structured_json(
        self,
        *,
        name: str,
        schema: dict,
        instructions: str,
        payload: dict,
        max_output_tokens: int,
    ) -> dict:
        """Execute one tool-free Codex turn and validate its final JSON."""
        request = self.secret_free_request(
            name=name,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
        request_sha256 = _sha256_text(canonical_json(request))
        prompt_sha256 = _sha256_text(canonical_json(payload))
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="spicy-regs-codex-") as directory:
            root = Path(directory)
            schema_path = root / "schema.json"
            instructions_path = root / "base-instructions.txt"
            schema_path.write_text(
                json.dumps(schema, sort_keys=True),
                encoding="utf-8",
            )
            instructions_path.write_text(
                BARE_BASE_INSTRUCTIONS + "\n",
                encoding="utf-8",
            )
            command = self._command(
                root=root,
                schema_path=schema_path,
                instructions_path=instructions_path,
                prompt=instructions,
            )
            normalized_command = [
                part.replace(str(root), "<temporary>")
                for part in command
            ]
            environment = os.environ.copy()
            environment.pop("OPENAI_API_KEY", None)
            environment.pop("CODEX_API_KEY", None)
            try:
                completed = self._runner(
                    command,
                    input=canonical_json(payload),
                    text=True,
                    capture_output=True,
                    timeout=self.timeout_seconds,
                    check=False,
                    cwd=root,
                    env=environment,
                )
            except Exception as error:
                self.last_call_metadata = self._failure_metadata(
                    started=started,
                    request_sha256=request_sha256,
                    prompt_sha256=prompt_sha256,
                    normalized_command=normalized_command,
                    error=error,
                )
                raise CodexCliProviderError(
                    f"Codex CLI process failed with {type(error).__name__}"
                ) from error

        try:
            events = _parse_jsonl(completed.stdout)
        except CodexCliProviderError as error:
            self.last_call_metadata = {
                **self._failure_metadata(
                    started=started,
                    request_sha256=request_sha256,
                    prompt_sha256=prompt_sha256,
                    normalized_command=normalized_command,
                    error=error,
                ),
                "exit_code": completed.returncode,
                "event_stream_sha256": _sha256_text(completed.stdout),
                "stderr_sha256": _sha256_text(completed.stderr),
            }
            raise
        event_types = [str(event.get("type") or "") for event in events]
        unknown_events = sorted(set(event_types) - ALLOWED_STREAM_EVENTS)
        forbidden_items = _forbidden_items(events)
        thread_id = _single_thread_id(events)
        usage = _turn_usage(events)
        final_text = _single_final_message(events)
        cli_version = self._cli_version()
        terminal_error_events = sorted(
            {
                event_type
                for event_type in event_types
                if event_type in {"turn.failed", "error"}
            }
        )
        duration_ms = round((time.monotonic() - started) * 1_000, 3)
        response_id = f"codex-thread:{thread_id}" if thread_id else None
        status = "completed"
        error_message: str | None = None
        if completed.returncode != 0:
            status = "failed"
            error_message = f"Codex CLI exited with status {completed.returncode}"
        elif unknown_events:
            status = "failed"
            error_message = "Codex CLI emitted unknown event types"
        elif forbidden_items:
            status = "failed"
            error_message = "Codex CLI emitted forbidden tool events"
        elif terminal_error_events:
            status = "failed"
            error_message = "Codex CLI emitted terminal error events"
        elif thread_id is None:
            status = "failed"
            error_message = "Codex CLI emitted no unique thread identifier"
        elif not usage:
            status = "failed"
            error_message = "Codex CLI emitted no unique usage receipt"
        elif cli_version == "unknown":
            status = "failed"
            error_message = "Codex CLI version could not be verified"
        elif final_text is None:
            status = "failed"
            error_message = "Codex CLI emitted no unique final message"

        input_tokens = _integer_usage(usage, "input_tokens")
        output_tokens = _integer_usage(usage, "output_tokens")
        total_tokens = input_tokens + output_tokens
        attempt = {
            "attempt": 1,
            "status": status,
            "duration_ms": duration_ms,
            "response_id": response_id,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
        }
        self.last_call_metadata = {
            "transport": "codex-cli",
            "provider": "codex-cli",
            "response_id": response_id,
            "thread_id": thread_id,
            "response_model": self.model,
            "status": status,
            "duration_ms": duration_ms,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "attempt_count": 1,
            "retry_count": 0,
            "attempts": [attempt],
            "prompt_sha256": prompt_sha256,
            "request_sha256": request_sha256,
            "reasoning_effort": self.reasoning_effort,
            "max_output_tokens": max_output_tokens,
            "max_output_tokens_enforced": False,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": 0,
            "sdk_max_retries": 0,
            "store": False,
            "session_persistence": "ephemeral",
            "cli_version": cli_version,
            "command_sha256": _sha256_text(canonical_json(normalized_command)),
            "event_stream_sha256": _sha256_text(completed.stdout),
            "stderr_sha256": _sha256_text(completed.stderr),
            "event_count": len(events),
            "event_types": sorted(set(event_types)),
            "unknown_event_types": unknown_events,
            "forbidden_item_types": forbidden_items,
            "terminal_error_event_types": terminal_error_events,
            "tools_enabled": False,
            "disabled_features": list(DISABLED_CODEX_FEATURES),
            "exit_code": completed.returncode,
        }
        if error_message is not None:
            raise CodexCliProviderError(error_message)

        try:
            value = json.loads(cast(str, final_text))
        except json.JSONDecodeError as error:
            self.last_call_metadata["status"] = "failed"
            raise CodexCliProviderError(
                "Codex CLI final message was not JSON"
            ) from error
        if not isinstance(value, dict):
            self.last_call_metadata["status"] = "failed"
            raise CodexCliProviderError(
                "Codex CLI final JSON root was not an object"
            )
        schema_errors = sorted(
            Draft202012Validator(schema).iter_errors(value),
            key=lambda error: tuple(str(part) for part in error.path),
        )
        if schema_errors:
            self.last_call_metadata["status"] = "failed"
            raise CodexCliProviderError(
                "Codex CLI final JSON violated the output schema"
            )
        self.last_call_metadata["schema_validated_locally"] = True
        return cast(dict[str, Any], value)

    def _command(
        self,
        *,
        root: Path,
        schema_path: Path,
        instructions_path: Path,
        prompt: str,
    ) -> list[str]:
        command = [
            self.executable,
            "exec",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--strict-config",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "-C",
            str(root),
            "-m",
            self.model,
            "-c",
            f'model_reasoning_effort="{self.reasoning_effort}"',
            "-c",
            f'model_instructions_file="{instructions_path}"',
            "-c",
            'personality="none"',
            "-c",
            'web_search="disabled"',
            "-c",
            "agents.enabled=false",
        ]
        for feature in DISABLED_CODEX_FEATURES:
            command.extend(("--disable", feature))
        command.extend(("--output-schema", str(schema_path), "--json", prompt))
        return command

    def _cli_version(self) -> str:
        try:
            completed = self._runner(
                [self.executable, "--version"],
                text=True,
                capture_output=True,
                timeout=min(self.timeout_seconds, 10.0),
                check=False,
            )
        except Exception:
            return "unknown"
        if completed.returncode != 0:
            return "unknown"
        return completed.stdout.strip() or "unknown"

    def _failure_metadata(
        self,
        *,
        started: float,
        request_sha256: str,
        prompt_sha256: str,
        normalized_command: list[str],
        error: BaseException,
    ) -> dict[str, object]:
        return {
            "transport": "codex-cli",
            "provider": "codex-cli",
            "status": "failed",
            "duration_ms": round((time.monotonic() - started) * 1_000, 3),
            "attempt_count": 1,
            "retry_count": 0,
            "attempts": [{"attempt": 1, "status": "failed"}],
            "prompt_sha256": prompt_sha256,
            "request_sha256": request_sha256,
            "reasoning_effort": self.reasoning_effort,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": 0,
            "sdk_max_retries": 0,
            "store": False,
            "session_persistence": "ephemeral",
            "command_sha256": _sha256_text(canonical_json(normalized_command)),
            "error_code": type(error).__name__,
            "tools_enabled": False,
            "disabled_features": list(DISABLED_CODEX_FEATURES),
        }


def _parse_jsonl(value: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(value.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            raise CodexCliProviderError(
                f"Codex CLI stdout line {line_number} was not JSON"
            ) from error
        if not isinstance(event, dict):
            raise CodexCliProviderError(
                f"Codex CLI stdout line {line_number} was not an object"
            )
        events.append(cast(dict[str, Any], event))
    if not events:
        raise CodexCliProviderError("Codex CLI emitted an empty event stream")
    return events


def _forbidden_items(events: list[dict[str, Any]]) -> list[str]:
    forbidden: set[str] = set()
    for event in events:
        item = event.get("item")
        if isinstance(item, Mapping):
            item_type = str(item.get("type") or "")
            if item_type and item_type not in ALLOWED_ITEM_TYPES:
                forbidden.add(item_type)
    return sorted(forbidden)


def _single_thread_id(events: list[dict[str, Any]]) -> str | None:
    values = [
        str(event.get("thread_id"))
        for event in events
        if event.get("type") == "thread.started" and event.get("thread_id")
    ]
    return values[0] if len(values) == 1 else None


def _turn_usage(events: list[dict[str, Any]]) -> Mapping[str, Any]:
    values = [
        event.get("usage")
        for event in events
        if event.get("type") == "turn.completed"
        and isinstance(event.get("usage"), Mapping)
    ]
    return cast(Mapping[str, Any], values[0]) if len(values) == 1 else {}


def _single_final_message(events: list[dict[str, Any]]) -> str | None:
    values: list[str] = []
    for event in events:
        if event.get("type") != "item.completed":
            continue
        item = event.get("item")
        if (
            isinstance(item, Mapping)
            and item.get("type") == "agent_message"
            and isinstance(item.get("text"), str)
        ):
            values.append(cast(str, item["text"]))
    return values[0] if len(values) == 1 else None


def _integer_usage(usage: Mapping[str, Any], key: str) -> int:
    value = usage.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def build_codex_cli_secret_free_request(
    *,
    model: str,
    reasoning_effort: str,
    name: str,
    schema: Mapping[str, Any],
    instructions: str,
    payload: Mapping[str, Any],
    max_output_tokens: int,
) -> dict[str, Any]:
    """Build the reproducible request identity without executing Codex."""
    return {
        "transport": "codex-cli",
        "model": model,
        "reasoning_effort": reasoning_effort,
        "instructions": instructions,
        "input": canonical_json(payload),
        "output_schema": {
            "name": name,
            "strict": True,
            "schema": schema,
        },
        "max_output_tokens": {
            "value": max_output_tokens,
            "enforced_by_transport": False,
        },
        "session_persistence": "ephemeral",
        "sandbox": "read-only",
        "ignore_user_config": True,
        "ignore_rules": True,
        "base_instructions_sha256": _sha256_text(BARE_BASE_INSTRUCTIONS),
        "disabled_features": list(DISABLED_CODEX_FEATURES),
    }

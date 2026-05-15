"""Tests for the Spicy Regs MCP server.

Covers the canonical implementation in ``spicy_regs.mcp_server`` and asserts
that the Vercel parallel copy at ``mcp-server/api/index.py`` keeps the same
tool surface so the two don't drift.
"""

from __future__ import annotations

import asyncio
import importlib.util
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from spicy_regs import mcp_server

REPO_ROOT = Path(__file__).resolve().parents[1]
VERCEL_COPY_PATH = REPO_ROOT / "mcp-server" / "api" / "index.py"


def _load_vercel_copy():
    spec = importlib.util.spec_from_file_location(
        "spicy_regs_mcp_vercel_copy", VERCEL_COPY_PATH
    )
    assert spec and spec.loader, f"could not load {VERCEL_COPY_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_jsonify_primitives_pass_through():
    assert mcp_server._jsonify(None) is None
    assert mcp_server._jsonify("hi") == "hi"
    assert mcp_server._jsonify(7) == 7
    assert mcp_server._jsonify(1.5) == 1.5
    assert mcp_server._jsonify(True) is True


def test_jsonify_coerces_non_json_types():
    j = mcp_server._jsonify
    assert j(date(2024, 1, 2)) == "2024-01-02"
    assert j(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert j(Decimal("1.50")) == "1.50"
    assert j(UUID("12345678-1234-5678-1234-567812345678")) == (
        "12345678-1234-5678-1234-567812345678"
    )
    assert j(b"\x00\xff") == "00ff"
    assert j([Decimal("1"), date(2024, 1, 1)]) == ["1", "2024-01-01"]
    assert j({"a": Decimal("2"), "b": [b"\xab"]}) == {"a": "2", "b": ["ab"]}


def test_resolve_r2_base_url_rejects_http(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "http://example.com")
    with pytest.raises(RuntimeError, match="https://"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_rejects_injection_chars(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://evil.example/'); DROP")
    with pytest.raises(RuntimeError, match="illegal characters"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://example.com/bucket/")
    assert mcp_server._resolve_r2_base_url() == "https://example.com/bucket"


def _tool_names(fastmcp) -> set[str]:
    tools = asyncio.run(fastmcp.list_tools())
    return {t.name for t in tools}


def test_build_server_registers_expected_tools():
    server = mcp_server.build_server()
    assert _tool_names(server) == {"list_sources", "describe_table", "query_sql"}


def test_vercel_copy_in_sync():
    """The Vercel copy must expose the same tools, table list, and instructions."""
    vercel = _load_vercel_copy()

    assert _tool_names(vercel.mcp) == _tool_names(mcp_server.build_server())
    assert vercel.TABLES == mcp_server.TABLES
    assert vercel.INSTRUCTIONS == mcp_server.INSTRUCTIONS
    assert vercel.DEFAULT_R2_BASE_URL == mcp_server.DEFAULT_R2_BASE_URL

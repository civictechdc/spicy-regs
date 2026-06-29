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

import duckdb
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


# --- Connection sandbox -----------------------------------------------------
#
# These exercise the real ``_apply_security_settings`` pragmas, which is where
# both shipped runtime crashes lived (the bogus ``statement_timeout`` SET and
# the spill-to-disabled-LocalFileSystem error). They are hermetic: the sandbox
# is applied to a plain in-memory connection, so no httpfs install or network
# is needed. The live ``_connect`` + R2 path is covered by the integration test
# below.


def _sandboxed_connection(memory_limit: str | None = None):
    """A connection with the production read-only sandbox applied.

    ``memory_limit`` is set before the sandbox locks the configuration, so a
    test can force spilling behavior.
    """
    con = duckdb.connect()
    if memory_limit is not None:
        con.execute(f"SET memory_limit='{memory_limit}'")
    mcp_server._apply_security_settings(con)
    return con


@pytest.mark.parametrize("module_name", ["canonical", "vercel"])
def test_security_settings_apply_cleanly(module_name):
    """Every pragma must be accepted by the installed DuckDB (no Catalog Error).

    The original ``SET statement_timeout`` regression failed exactly here, on a
    parameter DuckDB does not recognize.
    """
    module = mcp_server if module_name == "canonical" else _load_vercel_copy()
    con = duckdb.connect()
    module._apply_security_settings(con)  # must not raise


def test_sandbox_allows_in_memory_query():
    con = _sandboxed_connection()
    assert con.execute("SELECT 1 + 1").fetchone() == (2,)
    assert con.execute("SELECT count(*) FROM range(100)").fetchone() == (100,)


def test_sandbox_survives_temp_spill():
    """A query that exceeds memory must not raise the LocalFileSystem error.

    Regression for ``Permission Error: File system LocalFileSystem has been
    disabled by configuration``: ``temp_directory`` defaulted to a local
    ``.tmp`` that the sandbox forbids, so any spilling query crashed. With
    spilling disabled the query either runs in memory or fails with a clear
    out-of-memory error — never the confusing permission error.
    """
    con = _sandboxed_connection(memory_limit="20MB")
    spilling_sql = (
        "SELECT i, count(*) AS c FROM range(3_000_000) r(i) "
        "GROUP BY i ORDER BY c, i DESC"
    )
    try:
        con.execute(spilling_sql).fetchall()
    except duckdb.OutOfMemoryException:
        pass  # acceptable: spilling disabled, no local temp touched
    except duckdb.PermissionException as exc:
        pytest.fail(f"sandbox crashed a spilling query on local temp: {exc}")


def test_sandbox_blocks_local_file_reads(tmp_path):
    """The local-file sandbox must stay intact (security regression guard)."""
    con = _sandboxed_connection()
    secret = tmp_path / "secret.csv"
    secret.write_text("a,b\n1,2\n")
    with pytest.raises(duckdb.PermissionException):
        con.execute(f"SELECT * FROM read_csv_auto('{secret}')").fetchall()


def test_sandbox_locks_configuration():
    """User SQL must not be able to re-enable a disabled filesystem."""
    con = _sandboxed_connection()
    with pytest.raises(duckdb.Error):
        con.execute("SET disabled_filesystems=''")


@pytest.mark.integration
def test_connect_queries_r2_end_to_end():
    """Live: the real ``_connect`` (httpfs + R2 views) serves the MCP tools.

    Covers the full path the hermetic tests cannot — httpfs install, the R2
    parquet views, and a spilling aggregation over real data — asserting none
    of it raises. Needs outbound network; run via ``pytest -m integration``.
    """
    server = mcp_server.build_server()

    sources = asyncio.run(server.call_tool("list_sources", {}))
    assert mcp_server.TABLES[0] in str(sources)

    schema = asyncio.run(server.call_tool("describe_table", {"table": "agency_stats"}))
    assert "column" in str(schema).lower()

    # An ORDER BY over a full remote table is the spill-prone shape that
    # crashed in production (sorts everything before applying the limit);
    # ``ORDER BY 1`` keeps it schema-agnostic. Assert it returns without error.
    result = asyncio.run(
        server.call_tool(
            "query_sql",
            {"sql": "SELECT * FROM agency_stats ORDER BY 1", "max_rows": 5},
        )
    )
    assert result is not None

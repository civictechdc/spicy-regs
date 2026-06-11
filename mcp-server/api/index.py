"""Spicy Regs MCP server (Vercel ASGI handler).

Parallel copy of ``src/spicy_regs/mcp_server.py`` so the Vercel deploy can ship
without pulling in the parent package's ETL dependencies. The two must keep
the same tool surface (names, parameters, behavior); the test at
``tests/test_mcp_server.py::test_vercel_copy_in_sync`` asserts that.
"""

from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

DEFAULT_R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"
TABLES = ("dockets", "documents", "comments", "comments_index", "feed_summary")
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "30s")

INSTRUCTIONS = (
    "Query the Spicy Regs regulatory dataset (regulations.gov mirror) over "
    "the public Cloudflare R2 parquet bucket. Use list_sources to discover "
    "tables, describe_table for schemas, and query_sql for everything else. "
    "Always LIMIT result sets while exploring. Cite docket IDs, document "
    "IDs, comment IDs, agency codes, and dates from the rows you return."
)


def _resolve_r2_base_url() -> str:
    raw = os.environ.get("SPICY_REGS_R2_URL", DEFAULT_R2_BASE_URL).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(
            f"SPICY_REGS_R2_URL must be an https:// URL, got: {raw!r}"
        )
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_R2_URL contains illegal characters: {raw!r}")
    return raw


R2_BASE_URL = _resolve_r2_base_url()


def _jsonify(value: Any) -> Any:
    """Coerce DuckDB row values into JSON-serializable forms."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    con.execute("SET disabled_filesystems='LocalFileSystem'")
    con.execute(f"SET statement_timeout='{STATEMENT_TIMEOUT}'")
    con.execute("SET lock_configuration=true")
    for name in TABLES:
        url = f"{R2_BASE_URL}/{name}.parquet"
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{url}')")
    return con


mcp = FastMCP(
    "spicy-regs",
    instructions=INSTRUCTIONS,
    stateless_http=True,
    streamable_http_path="/mcp",
    # The hosted deployment is reached via mcp.spicy-regs.dev and per-deploy
    # *.vercel.app hosts; FastMCP's default localhost-only DNS-rebinding
    # allowlist would reject all of them with 421. The server is public,
    # stateless, and read-only, so rebinding protection buys nothing here.
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False
    ),
)


@mcp.tool()
def list_sources() -> dict[str, Any]:
    """List the logical tables available in the Spicy Regs R2 dataset."""
    return {
        "source": "r2",
        "base_url": R2_BASE_URL,
        "tables": list(TABLES),
    }


@mcp.tool()
def describe_table(table: str) -> dict[str, Any]:
    """Return the column schema for one Spicy Regs table.

    Valid tables: dockets, documents, comments, comments_index, feed_summary.
    """
    if table not in TABLES:
        return {
            "error": f"Unknown table '{table}'",
            "available_tables": list(TABLES),
        }
    con = _connect()
    rows = con.execute(f"DESCRIBE {table}").fetchall()
    return {
        "table": table,
        "columns": [
            {
                "column_name": row[0],
                "column_type": row[1],
                "null": row[2],
                "key": row[3],
                "default": row[4],
            }
            for row in rows
        ],
    }


@mcp.tool()
def query_sql(sql: str, max_rows: int = 25) -> dict[str, Any]:
    """Run a SQL query against the Spicy Regs R2 tables and return up to max_rows rows.

    The connection is in-memory and read-only against R2. Available views:
    dockets, documents, comments, comments_index, feed_summary. Always include
    a LIMIT in exploratory queries; results past max_rows are dropped.
    """
    if max_rows <= 0 or max_rows > 500:
        return {"error": "max_rows must be between 1 and 500"}

    con = _connect()
    cursor = con.execute(sql)
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    rows = cursor.fetchmany(max_rows)
    result_rows = [
        {col: _jsonify(val) for col, val in zip(columns, row)} for row in rows
    ]
    return {
        "source": "r2",
        "base_url": R2_BASE_URL,
        "columns": columns,
        "row_count_shown": len(result_rows),
        "max_rows": max_rows,
        "rows": result_rows,
    }


_asgi_app = mcp.streamable_http_app()

_FUNCTION_PATH = "/api/index"


async def app(scope, receive, send):
    """ASGI entry point for Vercel.

    vercel.json rewrites /mcp to this function. Only the exact function path
    (/api/index) is routable on the platform, and depending on how rewritten
    requests are forwarded the ASGI path can arrive as the original (/mcp) or
    as the function path; normalize the latter onto the transport's /mcp
    mount so both work.
    """
    if scope["type"] == "http" and scope.get("path", "").startswith(_FUNCTION_PATH):
        suffix = scope["path"][len(_FUNCTION_PATH) :]
        path = suffix if suffix.startswith("/mcp") else "/mcp"
        scope = {**scope, "path": path, "raw_path": path.encode()}
    await _asgi_app(scope, receive, send)

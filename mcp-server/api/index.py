"""Spicy Regs MCP server (Vercel ASGI handler).

Parallel copy of ``src/spicy_regs/mcp_server.py`` so the Vercel deploy can ship
without pulling in the parent package's ETL dependencies. The two must keep
the same tool surface (names, parameters, behavior); the test at
``tests/test_mcp_server.py::test_vercel_copy_in_sync`` asserts that.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon

# This file is loaded as a standalone module — both as the Vercel function
# entrypoint and, in tests, via importlib.spec_from_file_location — so its own
# directory isn't on sys.path. Add it so the sibling generated _icon module
# (produced by scripts/gen_icon.py) resolves in every context.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _icon import ICON_DATA_URI  # noqa: E402

DEFAULT_R2_BASE_URL = "https://r2.spicy-regs.dev"
# The full published R2 surface. Must match the data dictionary's table list
# (src/spicy_regs/data_dictionary.py::TABLES) — a test enforces it so the two
# can't drift. Keep in sync with the canonical copy in src/spicy_regs/mcp_server.py.
TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
)
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "30s")


def _parse_timeout_seconds(raw: str) -> float | None:
    """Parse ``SPICY_REGS_STATEMENT_TIMEOUT`` into seconds (``None`` disables it).

    DuckDB has no ``statement_timeout`` configuration parameter — ``SET
    statement_timeout=...`` raises ``Catalog Error: unrecognized configuration
    parameter``. The cap is instead enforced with a watchdog that interrupts the
    connection (see :func:`_statement_timeout`). Accepts a bare number of
    seconds or a value suffixed with ``ms``/``s``/``m``; non-positive values
    disable the timeout.
    """
    text = raw.strip().lower()
    if not text:
        return None
    multiplier = 1.0
    for suffix, factor in (("ms", 0.001), ("s", 1.0), ("m", 60.0)):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)].strip()
            break
    try:
        value = float(text)
    except ValueError as exc:
        raise RuntimeError(
            f"SPICY_REGS_STATEMENT_TIMEOUT is not a valid duration: {raw!r}"
        ) from exc
    if value <= 0:
        return None
    return value * multiplier


STATEMENT_TIMEOUT_SECONDS = _parse_timeout_seconds(STATEMENT_TIMEOUT)

INSTRUCTIONS = (
    "Query the Spicy Regs regulatory dataset (regulations.gov mirror) over "
    "the public Cloudflare R2 parquet bucket. Use list_sources to discover "
    "tables, describe_table for schemas, and query_sql for everything else. "
    "Always LIMIT result sets while exploring. Cite docket IDs, document "
    "IDs, comment IDs, agency codes, and dates from the rows you return."
)

# Server icon, advertised on the Implementation metadata sent during
# ``initialize``. A base64 data: URI (not an https:// URL) so it works for both
# the stdio and HTTP transports — see scripts/gen_icon.py.
ICONS = [Icon(src=ICON_DATA_URI, mimeType="image/png", sizes=["512x512"])]


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


def _resolve_home_directory() -> str:
    """Pick a writable directory for DuckDB's extension/home cache.

    DuckDB resolves its extension cache under ``$HOME/.duckdb``. Serverless
    hosts (Vercel/AWS Lambda) frequently have no writable home directory, or
    none defined at all, so ``INSTALL httpfs`` fails with
    ``Can't find the home directory at ''``. Defaulting to the platform temp
    directory keeps the extension fetch working there while staying valid on
    local stdio installs; ``SPICY_REGS_HOME_DIR`` overrides it.
    """
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(
            f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}"
        )
    return raw


HOME_DIRECTORY = _resolve_home_directory()


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


def _apply_security_settings(con: duckdb.DuckDBPyConnection) -> None:
    """Lock a fresh connection down to read-only remote access.

    Separated from :func:`_connect` (which also installs httpfs and creates the
    R2 views) so the sandbox can be exercised in tests without network access.
    Run after httpfs is loaded and before any user SQL.
    """
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    # NB: do NOT disable LocalFileSystem here. It is tempting as a guard against
    # user SQL reading local files, but httpfs reads the system CA bundle off
    # the local filesystem for every TLS handshake, so disabling it breaks the
    # only thing this server does — reading the R2 parquet over HTTPS fails with
    # "File system LocalFileSystem has been disabled by configuration" the
    # moment a view binds. See the query_sql docstring for the access model.
    #
    # Disable on-disk spilling instead: temp_directory defaults to a local
    # ".tmp" that is read-only on serverless hosts, so a spilling query (a big
    # GROUP BY/ORDER BY) would fail there anyway. Empty disables spilling —
    # queries run in memory or fail with a clear out-of-memory error.
    con.execute("SET temp_directory=''")
    # DuckDB has no statement_timeout parameter; the per-query cap is enforced
    # by the _statement_timeout watchdog wrapping each tool's execution.
    con.execute("SET lock_configuration=true")


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Must precede INSTALL/LOAD: that step writes the extension under
    # <home_directory>/.duckdb, and the default home is read-only or undefined
    # on serverless hosts.
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    _apply_security_settings(con)
    for name in TABLES:
        url = f"{R2_BASE_URL}/{name}.parquet"
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{url}')")
    return con


@contextmanager
def _statement_timeout(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Cap the wrapped query's runtime, replacing the missing DuckDB setting.

    Starts a watchdog timer that calls ``con.interrupt()`` once the configured
    budget elapses; a tripped timer turns DuckDB's ``InterruptException`` into a
    ``TimeoutError`` so the cause is unambiguous. A no-op when the timeout is
    disabled.
    """
    if STATEMENT_TIMEOUT_SECONDS is None:
        yield
        return
    tripped = threading.Event()

    def _interrupt() -> None:
        tripped.set()
        con.interrupt()

    timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, _interrupt)
    timer.start()
    try:
        yield
    except duckdb.InterruptException as exc:
        if tripped.is_set():
            raise TimeoutError(
                f"Query exceeded the {STATEMENT_TIMEOUT} statement timeout"
            ) from exc
        raise
    finally:
        timer.cancel()


mcp = FastMCP(
    "spicy-regs",
    instructions=INSTRUCTIONS,
    icons=ICONS,
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

    Valid tables: dockets, documents, comments, comments_index, feed_summary,
    agency_stats, agency_monthly_volume.
    """
    if table not in TABLES:
        return {
            "error": f"Unknown table '{table}'",
            "available_tables": list(TABLES),
        }
    con = _connect()
    with _statement_timeout(con):
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
    dockets, documents, comments, comments_index, feed_summary, agency_stats,
    agency_monthly_volume. Always include a LIMIT in exploratory queries;
    results past max_rows are dropped.
    """
    if max_rows <= 0 or max_rows > 500:
        return {"error": "max_rows must be between 1 and 500"}

    con = _connect()
    with _statement_timeout(con):
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

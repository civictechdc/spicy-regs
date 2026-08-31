from __future__ import annotations

import base64
import logging
import os
import re
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon
from starlette.requests import Request
from starlette.responses import Response

from spicy_regs._icon import ICON_DATA_URI

DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"
TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "rulemaking_lifecycles",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "unified_agenda",
    "federal_register",
    "sam_entities",
    "lobbying_filings",
    "fec_committees",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
)
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "790s")

logger = logging.getLogger(__name__)

CATALOG_ALIAS = "reg_catalog"
DEFAULT_CATALOG_NAMESPACE = "default"


def _parse_timeout_seconds(raw: str) -> float | None:
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
        raise RuntimeError(f"SPICY_REGS_STATEMENT_TIMEOUT is not a valid duration: {raw!r}") from exc
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

ICONS = [Icon(src=ICON_DATA_URI, mimeType="image/png", sizes=["512x512"])]


def _resolve_r2_base_url() -> str:
    raw = os.environ.get("SPICY_REGS_R2_URL", DEFAULT_R2_BASE_URL).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(f"SPICY_REGS_R2_URL must be an https:// URL, got: {raw!r}")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_R2_URL contains illegal characters: {raw!r}")
    return raw


R2_BASE_URL = _resolve_r2_base_url()


def _resolve_home_directory() -> str:
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}")
    return raw


HOME_DIRECTORY = _resolve_home_directory()


def _resolve_memory_limit() -> str | None:
    """DuckDB memory ceiling from SPICY_REGS_MEMORY_LIMIT (e.g. '12GB', '75%').

    Unset => None => DuckDB's own default (~80% of detected RAM). Set it on hosts
    where DuckDB can't see the real allocation (containers detect host RAM, not
    the cgroup limit) so it spills/errors before the platform OOM-kills the process.
    Interpolated into a SET, so the value is format-validated.
    """
    raw = os.environ.get("SPICY_REGS_MEMORY_LIMIT", "").strip()
    if not raw:
        return None
    if not re.fullmatch(r"\d+(\.\d+)?\s*(%|[KMGT]?i?B)?", raw, re.IGNORECASE):
        raise RuntimeError(f"SPICY_REGS_MEMORY_LIMIT is not a valid size/percent: {raw!r}")
    return raw


def _resolve_temp_dir() -> str:
    """DuckDB spill directory from SPICY_REGS_TEMP_DIR.

    Default '' disables spilling — the safe serverless behavior, since DuckDB's
    default temp dir is a relative '.tmp' that is read-only on serverless hosts,
    so a spilling query would fail there anyway. Supply a writable path (a
    container with real disk, or a mounted volume) to let big GROUP BY/ORDER BY
    spill instead of erroring. Interpolated into a SET, so injection chars are
    rejected.
    """
    raw = os.environ.get("SPICY_REGS_TEMP_DIR", "")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_TEMP_DIR contains illegal characters: {raw!r}")
    return raw


MEMORY_LIMIT = _resolve_memory_limit()
TEMP_DIR = _resolve_temp_dir()


def _resolve_catalog_config() -> dict[str, str] | None:
    uri = os.environ.get("R2_CATALOG_URI")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE")
    token = os.environ.get("R2_CATALOG_TOKEN")
    if not (uri and warehouse and token):
        return None
    config = {
        "uri": uri,
        "warehouse": warehouse,
        "token": token,
        "namespace": os.environ.get("R2_CATALOG_NAMESPACE") or DEFAULT_CATALOG_NAMESPACE,
    }
    for key, value in config.items():
        if any(c in value for c in ("'", "\\", "\x00", "\n", "\r")):
            raise RuntimeError(f"R2 catalog {key} contains illegal characters")
    return config


def _jsonify(value: Any) -> Any:
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


READ_ONLY_STATEMENT_TYPES = frozenset({"SELECT", "EXPLAIN"})


def _first_write_statement(cursor: duckdb.DuckDBPyConnection, sql: str) -> str | None:
    """Name of the first non-read-only statement in ``sql``, else None.

    The sandbox cannot express "no writes" on its own. ``disabled_filesystems``
    is the setting that would, but LocalFileSystem has to stay enabled for
    httpfs to read the CA bundle (see the security-settings notes), so
    ``COPY ... TO``, ``ATTACH``, and ``EXPORT DATABASE`` could all write to the
    container filesystem — on Cloud Run an in-memory one, where a large enough
    write evicts the instance. This is the gate that says no instead.

    Classification comes from DuckDB's own parser rather than a prefix regex,
    so leading comments, string literals, and stacked statements cannot smuggle
    a write past it. ``DESCRIBE``/``SHOW``/``SUMMARIZE``/``VALUES``/``TABLE``
    and the FROM-first shorthand all parse as SELECT, which is why a two-entry
    allowlist still admits every read form.

    Matching on ``StatementType.name`` rather than the enum member keeps this
    working against duckdb's incomplete type stubs, which do not declare the
    members, without a blanket type-ignore over the comparison.

    A ``ParserException`` propagates untouched: malformed SQL should surface
    DuckDB's own message, which names the offending token.
    """
    for statement in cursor.extract_statements(sql):
        name = statement.type.name
        if name not in READ_ONLY_STATEMENT_TYPES:
            return name
    return None


def _apply_security_settings(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    if MEMORY_LIMIT is not None:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("SET lock_configuration=true")


def _attach_catalog(con: duckdb.DuckDBPyConnection, config: dict[str, str]) -> bool:
    try:
        try:
            con.execute("INSTALL avro")
            con.execute("LOAD avro")
        except duckdb.Error as avro_exc:
            logger.info("avro not separately provisioned (%s); using iceberg's bundled path", avro_exc)
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
        con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{config['token']}');")
        con.execute(f"ATTACH '{config['warehouse']}' AS {CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{config['uri']}');")
        return True
    except duckdb.Error as exc:
        logger.warning("R2 catalog attach failed; comments fall back to monolith: %s", exc)
        return False


def _build_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    catalog = _resolve_catalog_config()
    catalog_attached = catalog is not None and _attach_catalog(con, catalog)

    _apply_security_settings(con)
    for name in TABLES:
        if name == "comments" and catalog_attached:
            namespace = catalog["namespace"]  # type: ignore[index]
            try:
                con.execute(
                    f"CREATE VIEW comments AS "
                    f'SELECT * FROM {CATALOG_ALIAS}."{namespace}"."comments" '
                    f"QUALIFY ROW_NUMBER() OVER "
                    f"(PARTITION BY comment_id ORDER BY modify_date DESC NULLS LAST) = 1"
                )
                continue
            except duckdb.Error as exc:
                logger.warning("comments not available in catalog; falling back to monolith: %s", exc)
        url = f"{R2_BASE_URL}/{name}.parquet"
        try:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{url}')")
        except duckdb.Error as exc:
            logger.warning("table %s not available at %s; skipping view: %s", name, url, exc)
    return con


# Building a connection is the expensive part of a tool call — install httpfs +
# iceberg, attach the R2 catalog over REST, and CREATE VIEW over all 23 tables
# (each reads a parquet footer over HTTPS), ~35s on a cold serverless instance.
# The query that follows is milliseconds. So we build once and reuse: Fluid
# Compute keeps a warmed instance's module state across invocations, and the
# stdio server is a single long-lived process, so a module-level connection
# amortizes that cost across every request the instance serves.
#
# Concurrency: the cached connection is shared, but each request runs on its own
# `con.cursor()` — DuckDB's supported way to run overlapping queries on one
# connection — so the statement-timeout interrupt (below) hits only that cursor,
# never a sibling request.
#
# Staleness: the views hold parquet footers and the catalog attach pins an
# Iceberg snapshot, both taken at build time. The ETL republishes daily, so a
# connection older than the TTL is rebuilt to pick up new data. On rebuild we
# only drop the module reference to the old connection — never close it — so any
# cursor still mid-query keeps working (a cursor outlives its parent losing its
# last Python reference); the old connection is reclaimed once its cursors drain.
_CONNECTION_TTL_SECONDS = float(os.environ.get("SPICY_REGS_CONNECTION_TTL", "300"))
_connection_lock = threading.Lock()
_cached_connection: duckdb.DuckDBPyConnection | None = None
_cached_connection_at = 0.0


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return a shared, cached DuckDB connection, rebuilding past the TTL.

    Callers must run their query on ``.cursor()`` of the returned connection,
    not on the connection itself, so concurrent requests don't serialize and a
    per-request timeout interrupt stays scoped to that request.
    """
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        age = _monotonic() - _cached_connection_at
        if _cached_connection is None or age >= _CONNECTION_TTL_SECONDS:
            # Drop (don't close) the previous connection: an in-flight cursor on
            # another thread still references it and must survive this swap.
            _cached_connection = _build_connection()
            _cached_connection_at = _monotonic()
        return _cached_connection


def _reset_connection_cache() -> None:
    """Forget the cached connection so the next call rebuilds. For tests."""
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        _cached_connection = None
        _cached_connection_at = 0.0


@contextmanager
def _statement_timeout(cursor: duckdb.DuckDBPyConnection) -> Iterator[None]:
    if STATEMENT_TIMEOUT_SECONDS is None:
        yield
        return
    tripped = threading.Event()

    def _interrupt() -> None:
        tripped.set()
        # Interrupt only this request's cursor, not the shared connection —
        # sibling requests run on their own cursors and must not be cancelled.
        cursor.interrupt()

    timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, _interrupt)
    timer.start()
    try:
        yield
    except duckdb.InterruptException as exc:
        if tripped.is_set():
            raise TimeoutError(f"Query exceeded the {STATEMENT_TIMEOUT} statement timeout") from exc
        raise
    finally:
        timer.cancel()


def _register_tools(mcp: FastMCP) -> None:
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

        Call list_sources for the set of valid table names; an unknown name
        returns them in the error payload.
        """
        if table not in TABLES:
            return {
                "error": f"Unknown table '{table}'",
                "available_tables": list(TABLES),
            }
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            rows = cursor.execute(f"DESCRIBE {table}").fetchall()
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
        """Run a read-only SQL query against the Spicy Regs R2 tables and return up to max_rows rows.

        Only SELECT and EXPLAIN run; DESCRIBE, SHOW, SUMMARIZE, VALUES and the
        FROM-first shorthand are accepted as SELECT. Statements that write
        (COPY TO, ATTACH, CREATE, INSERT, DROP, EXPORT, SET, ...) are refused.
        The connection is in-memory and read-only against R2. One view exists per
        table listed by list_sources. Always include a LIMIT in exploratory
        queries; results past max_rows are dropped.
        """
        if max_rows <= 0 or max_rows > 500:
            return {"error": "max_rows must be between 1 and 500"}

        cursor = _get_connection().cursor()
        write_statement = _first_write_statement(cursor, sql)
        if write_statement is not None:
            return {"error": f"query_sql is read-only; refusing {write_statement} statement"}

        with _statement_timeout(cursor):
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows)
        result_rows = [{col: _jsonify(val) for col, val in zip(columns, row)} for row in rows]
        return {
            "source": "r2",
            "base_url": R2_BASE_URL,
            "columns": columns,
            "row_count_shown": len(result_rows),
            "max_rows": max_rows,
            "rows": result_rows,
        }


def build_server() -> FastMCP:
    mcp = FastMCP("spicy-regs", instructions=INSTRUCTIONS, icons=ICONS)
    _register_tools(mcp)
    return mcp


STATIC_DIR = Path(__file__).parent / "static"


@lru_cache(maxsize=1)
def _landing_page() -> bytes:
    """The setup page served at /, with its view list rendered from TABLES.

    The list used to be hand-maintained in the HTML and had drifted seven
    tables behind by the time this page was restored; substituting it here
    keeps the page honest as TABLES grows.
    """
    views = " ·\n        ".join(f'<code class="inline">{table}</code>' for table in TABLES)
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("<!--VIEWS-->", views).encode("utf-8")


@lru_cache(maxsize=1)
def _landing_icon() -> bytes:
    """Decoded from ICON_DATA_URI so the PNG has exactly one home in the package."""
    return base64.b64decode(ICON_DATA_URI.split(",", 1)[1])


def _register_landing_page(mcp: FastMCP) -> None:
    """Serve the human-facing setup page alongside the MCP endpoint.

    Vercel served this as a static file at the site root; when that deploy was
    retired the page went with it, leaving mcp.spicy-regs.dev/ a bare 404. It
    lives in the canonical server now so every host (Cloud Run, the Cloudflare
    container) gets it without host-specific static-file config.
    """

    @mcp.custom_route("/", methods=["GET"])
    async def landing(_request: Request) -> Response:
        return Response(
            _landing_page(),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "public, max-age=300"},
        )

    @mcp.custom_route("/icon.png", methods=["GET"])
    async def icon(_request: Request) -> Response:
        return Response(
            _landing_icon(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )


def build_app():
    mcp = FastMCP(
        "spicy-regs",
        instructions=INSTRUCTIONS,
        icons=ICONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    _register_tools(mcp)
    _register_landing_page(mcp)
    return mcp.streamable_http_app()


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()

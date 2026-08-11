from __future__ import annotations

import logging
import os
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon

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


def _apply_security_settings(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    con.execute("SET temp_directory=''")
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


def _connect() -> duckdb.DuckDBPyConnection:
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


@contextmanager
def _statement_timeout(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
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
    return mcp.streamable_http_app()


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()

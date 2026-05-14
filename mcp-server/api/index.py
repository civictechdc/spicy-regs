"""Spicy Regs MCP server.

Exposes the Spicy Regs regulatory dataset to MCP-compatible clients (Claude.ai
Custom Connectors, Claude Code, Cursor, etc.) by running DuckDB queries against
the public Cloudflare R2 parquet bucket.

The handler is an ASGI app and is deployed as a Vercel Python serverless
function. See ../README.md for deploy and install instructions.
"""

from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

R2_BASE_URL = os.environ.get(
    "SPICY_REGS_R2_URL",
    "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev",
).rstrip("/")

TABLES = ("dockets", "documents", "comments", "comments_index", "feed_summary")

mcp = FastMCP(
    "spicy-regs",
    instructions=(
        "Query the Spicy Regs regulatory dataset (regulations.gov mirror) over "
        "the public Cloudflare R2 parquet bucket. Use list_sources to discover "
        "tables, describe_table for schemas, and query_sql for everything else. "
        "Always LIMIT result sets while exploring. Cite docket IDs, document "
        "IDs, comment IDs, agency codes, and dates from the rows you return."
    ),
    stateless_http=True,
    streamable_http_path="/mcp",
)


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _connect():
    import duckdb

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    for name in TABLES:
        url = f"{R2_BASE_URL}/{name}.parquet"
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{_escape_sql_string(url)}')"
        )
    return con


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
        {col: (val if not hasattr(val, "isoformat") else val.isoformat()) for col, val in zip(columns, row)}
        for row in rows
    ]
    return {
        "source": "r2",
        "base_url": R2_BASE_URL,
        "columns": columns,
        "row_count_shown": len(result_rows),
        "max_rows": max_rows,
        "rows": result_rows,
    }


app = mcp.streamable_http_app()

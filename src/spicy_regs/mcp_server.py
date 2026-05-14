"""Spicy Regs MCP server (stdio + ASGI).

Canonical FastMCP server implementation. Exposed two ways:

- Stdio, via the ``spicy-regs-mcp`` console script declared in pyproject.toml.
  Install with ``uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp``
  or, once published, ``uvx spicy-regs-mcp``.
- Streamable HTTP, via :func:`build_app` for ASGI deployment.

The Vercel function at ``mcp-server/api/index.py`` keeps a parallel copy so it
can deploy without pulling in the parent package's ETL dependencies. Keep the
tool surface (names, parameters, behavior) in sync between the two files.
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

INSTRUCTIONS = (
    "Query the Spicy Regs regulatory dataset (regulations.gov mirror) over "
    "the public Cloudflare R2 parquet bucket. Use list_sources to discover "
    "tables, describe_table for schemas, and query_sql for everything else. "
    "Always LIMIT result sets while exploring. Cite docket IDs, document "
    "IDs, comment IDs, agency codes, and dates from the rows you return."
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


def build_server(*, stateless_http: bool = True, streamable_http_path: str = "/mcp") -> FastMCP:
    mcp = FastMCP(
        "spicy-regs",
        instructions=INSTRUCTIONS,
        stateless_http=stateless_http,
        streamable_http_path=streamable_http_path,
    )
    _register_tools(mcp)
    return mcp


def build_app():
    """ASGI app for Streamable HTTP transport."""
    return build_server().streamable_http_app()


def main() -> None:
    """Stdio entry point used by the ``spicy-regs-mcp`` console script."""
    build_server().run()


if __name__ == "__main__":
    main()

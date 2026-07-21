"""DuckDB query engine shared by the ``spicy-regs`` CLI commands.

Builds one DuckDB view per published table, pointing either at the public R2
parquet (``https://r2.spicy-regs.dev/<table>.parquet``) or at files downloaded
by ``spicy-regs download``. The connect/skip patterns mirror
``spicy_regs.mcp_server`` (which must stay self-contained for its Vercel sync
copy, so the small overlap is deliberate); the table list itself is imported
from ``spicy_regs.data_dictionary`` so it has exactly one owner.
"""

from __future__ import annotations

import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import duckdb

from spicy_regs.data_dictionary import DEFAULT_R2_BASE_URL, TABLES

__all__ = [
    "DEFAULT_R2_BASE_URL",
    "TABLES",
    "QueryResult",
    "ViewSpec",
    "connect",
    "escape_sql_string",
    "local_view_specs",
    "remote_view_specs",
    "resolve_view_specs",
    "run_query",
]


@dataclass(frozen=True)
class ViewSpec:
    """Where one logical table's view reads from."""

    table: str
    kind: str  # "local" | "r2"
    location: str  # file path or URL
    sql: str  # SELECT statement the view is created from


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple]
    truncated: bool  # True when rows were cut off at max_rows


def escape_sql_string(value: str) -> str:
    """Escape a value for inlining into a single-quoted SQL string literal."""
    return value.replace("'", "''")


def _read_parquet_sql(location: str, **options: bool) -> str:
    opts = "".join(f", {key}={str(val).lower()}" for key, val in options.items())
    return f"SELECT * FROM read_parquet('{escape_sql_string(location)}'{opts})"


def local_view_specs(data_dir: Path) -> dict[str, ViewSpec]:
    """Specs for every published table that exists under ``data_dir``.

    Besides the flat ``<table>.parquet`` files that ``spicy-regs download``
    writes, ``comments`` may exist as the pipeline's partitioned
    ``comments/**/*.parquet`` tree — handled the same way as the plugin's
    standalone query script.
    """
    specs: dict[str, ViewSpec] = {}
    for table in TABLES:
        flat = data_dir / f"{table}.parquet"
        if flat.exists():
            specs[table] = ViewSpec(table, "local", str(flat), _read_parquet_sql(str(flat)))
            continue
        if table == "comments":
            partitioned = data_dir / "comments"
            if partitioned.is_dir():
                pattern = str(partitioned / "**" / "*.parquet")
                sql = _read_parquet_sql(pattern, union_by_name=True, hive_partitioning=True)
                specs[table] = ViewSpec(table, "local", pattern, sql)
    return specs


def remote_view_specs(base_url: str = DEFAULT_R2_BASE_URL) -> dict[str, ViewSpec]:
    """Specs for all published tables against the public bucket."""
    url = base_url.rstrip("/")
    return {
        table: ViewSpec(table, "r2", f"{url}/{table}.parquet", _read_parquet_sql(f"{url}/{table}.parquet"))
        for table in TABLES
    }


def resolve_view_specs(source: str, data_dir: Path, base_url: str = DEFAULT_R2_BASE_URL) -> dict[str, ViewSpec]:
    """Resolve where each table should be read from.

    ``source`` is ``"r2"``, ``"local"``, or ``"auto"`` — auto prefers the local
    copy of each table when present and falls back to R2 for the rest, so a
    partial download still gives full query coverage.
    """
    if source == "local":
        return local_view_specs(data_dir)
    if source == "r2":
        return remote_view_specs(base_url)
    if source == "auto":
        specs = remote_view_specs(base_url)
        specs.update(local_view_specs(data_dir))
        return specs
    raise ValueError(f"Unknown source {source!r}; expected 'r2', 'local', or 'auto'")


def connect(specs: dict[str, ViewSpec]) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with one view per spec.

    A view whose parquet can't be read (table registered but not published yet,
    local file corrupt, network error) is skipped with a warning instead of
    failing the whole command — same degradation as the MCP server.
    """
    con = duckdb.connect()
    # Must precede INSTALL/LOAD: the extension cache lands under
    # <home_directory>/.duckdb, and $HOME may be unset or read-only.
    con.execute(f"SET home_directory='{escape_sql_string(tempfile.gettempdir())}'")
    con.execute("SET preserve_insertion_order=false")
    if any(spec.kind == "r2" for spec in specs.values()):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    for spec in specs.values():
        try:
            con.execute(f"CREATE VIEW {spec.table} AS {spec.sql}")
        except duckdb.Error as exc:
            print(f"warning: table {spec.table} not available at {spec.location}: {exc}", file=sys.stderr)
    return con


def run_query(con: duckdb.DuckDBPyConnection, sql: str, max_rows: int) -> QueryResult:
    """Execute ``sql`` and return up to ``max_rows`` rows (``0`` = unlimited)."""
    cursor = con.execute(sql)
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    if max_rows <= 0:
        return QueryResult(columns=columns, rows=cursor.fetchall(), truncated=False)
    rows = cursor.fetchmany(max_rows)
    truncated = len(rows) == max_rows and cursor.fetchone() is not None
    return QueryResult(columns=columns, rows=rows, truncated=truncated)

"""DuckDB query endpoint for running SQL against R2 Parquet data."""

import asyncio
import datetime
import decimal
import logging
import re
import time
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"

DEFAULT_LIMIT = 1000
MAX_LIMIT = 10_000
QUERY_TIMEOUT_SECONDS = 30

# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------


class QueryRequest(BaseModel):
    sql: str = Field(
        ..., min_length=1, max_length=5000, description="SQL SELECT query to execute"
    )
    limit: int = Field(
        DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Maximum rows to return"
    )


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    elapsed_ms: int


# ---------------------------------------------------------------------------
# SQL Validation
# ---------------------------------------------------------------------------

BLOCKED_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "ATTACH",
    "DETACH",
    "COPY",
    "EXPORT",
    "IMPORT",
    "PRAGMA",
    "INSTALL",
    "LOAD",
    "CALL",
    "EXECUTE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "GRANT",
    "REVOKE",
}


def validate_sql(sql: str) -> str:
    """Validate and normalize SQL. Returns cleaned SQL or raises HTTPException."""
    # Strip SQL comments
    cleaned = re.sub(r"--[^\n]*", "", sql)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip().rstrip(";")

    if not cleaned:
        raise HTTPException(400, detail="Empty SQL query")

    first_word = cleaned.split()[0].upper()
    if first_word not in ("SELECT", "WITH"):
        raise HTTPException(
            400,
            detail="Only SELECT queries are allowed. "
            "Query must start with SELECT or WITH.",
        )

    upper = cleaned.upper()
    for kw in BLOCKED_KEYWORDS:
        if re.search(rf"\b{kw}\b", upper):
            raise HTTPException(
                400,
                detail=f"Forbidden keyword: {kw}. Only SELECT queries are permitted.",
            )

    # Block read_parquet() pointing to non-R2 URLs
    parquet_calls = re.findall(
        r"read_parquet\s*\(\s*'([^']*)'", cleaned, re.IGNORECASE
    )
    for url in parquet_calls:
        if not url.startswith(R2_BASE_URL):
            raise HTTPException(
                400,
                detail="Direct read_parquet() with external URLs is not allowed. "
                "Use table names: dockets, documents, comments, "
                "or comments_for_agency('AGENCY_CODE').",
            )

    return cleaned


# ---------------------------------------------------------------------------
# DuckDB Connection Management
# ---------------------------------------------------------------------------

_conn: duckdb.DuckDBPyConnection | None = None
_semaphore = asyncio.Semaphore(1)


def _init_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure the DuckDB connection with VIEWs."""
    conn = duckdb.connect(":memory:")

    conn.execute("INSTALL httpfs; LOAD httpfs;")

    conn.execute(
        f"CREATE VIEW dockets AS "
        f"SELECT * FROM read_parquet('{R2_BASE_URL}/dockets.parquet')"
    )
    conn.execute(
        f"CREATE VIEW documents AS "
        f"SELECT * FROM read_parquet('{R2_BASE_URL}/documents.parquet')"
    )
    conn.execute(
        f"CREATE VIEW comments AS "
        f"SELECT * FROM read_parquet('{R2_BASE_URL}/comments.parquet')"
    )

    conn.execute(
        f"CREATE OR REPLACE MACRO comments_for_agency(agency_code) AS TABLE "
        f"SELECT * FROM read_parquet("
        f"'{R2_BASE_URL}/comments/agency/agency_code=' "
        f"|| agency_code || '/part-0.parquet')"
    )

    logger.info("DuckDB connection initialized with VIEWs")
    return conn


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the singleton DuckDB connection."""
    global _conn
    if _conn is None:
        _conn = _init_connection()
    return _conn


# ---------------------------------------------------------------------------
# Query Execution
# ---------------------------------------------------------------------------


def _make_serializable(value: Any) -> Any:
    """Convert DuckDB-native types to JSON-compatible types."""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _execute_query(sql: str, limit: int) -> tuple[list[str], list[dict], bool]:
    """Execute query synchronously (runs in thread pool)."""
    conn = get_connection()

    has_limit = bool(re.search(r"\bLIMIT\b", sql, re.IGNORECASE))

    if has_limit:
        exec_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit}"
    else:
        exec_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit + 1}"

    result = conn.execute(exec_sql)
    columns = [desc[0] for desc in result.description]
    raw_rows = result.fetchall()

    if not has_limit and len(raw_rows) > limit:
        truncated = True
        raw_rows = raw_rows[:limit]
    else:
        truncated = False

    rows = [
        {col: _make_serializable(val) for col, val in zip(columns, row)}
        for row in raw_rows
    ]
    return columns, rows, truncated


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(tags=["query"])


@router.post("/query", response_model=QueryResponse)
async def run_query(request: QueryRequest):
    """Execute a SQL SELECT query against the regulations dataset.

    Available tables:
    - `dockets` - Regulatory dockets (~346K rows)
    - `documents` - Documents associated with dockets (~2M rows)
    - `comments` - Public comments (24M+ rows, use with caution)
    - `comments_for_agency('CODE')` - Comments for a specific agency (fast)
    """
    cleaned_sql = validate_sql(request.sql)

    start = time.monotonic()

    try:
        async with _semaphore:
            loop = asyncio.get_event_loop()
            columns, rows, truncated = await asyncio.wait_for(
                loop.run_in_executor(None, _execute_query, cleaned_sql, request.limit),
                timeout=QUERY_TIMEOUT_SECONDS,
            )
    except asyncio.TimeoutError:
        raise HTTPException(
            408,
            detail=f"Query timed out after {QUERY_TIMEOUT_SECONDS}s. "
            "Try adding filters, reducing scope, or using "
            "comments_for_agency('CODE') instead of the full comments table.",
        )
    except duckdb.Error as e:
        raise HTTPException(
            400,
            detail=f"DuckDB error: {e}. "
            "Available tables: dockets, documents, comments, "
            "comments_for_agency('AGENCY_CODE').",
        )

    elapsed_ms = int((time.monotonic() - start) * 1000)

    return QueryResponse(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        elapsed_ms=elapsed_ms,
    )


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def startup():
    """Pre-initialize DuckDB connection."""
    try:
        get_connection()
        logger.info("DuckDB query engine ready")
    except Exception as e:
        logger.error(f"DuckDB initialization failed: {e}")

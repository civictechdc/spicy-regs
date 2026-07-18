"""Result formatting for the ``spicy-regs`` CLI: table, JSON, and CSV."""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import IO, TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from spicy_regs.cli.engine import QueryResult

# Cap cell width in table output so a full-text comment column doesn't wrap the
# whole terminal; --format json/csv always carries complete values.
MAX_CELL_WIDTH = 80

FORMATS = ("table", "json", "csv")


def jsonify(value: Any) -> Any:
    """Coerce DuckDB row values into JSON-serializable forms.

    Same coercions as ``spicy_regs.mcp_server._jsonify``; duplicated because the
    MCP server must stay self-contained for its Vercel sync copy.
    """
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
        return [jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonify(v) for k, v in value.items()}
    return str(value)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(jsonify(value)).replace("\n", " ").replace("\r", " ")
    if len(text) > MAX_CELL_WIDTH:
        return text[: MAX_CELL_WIDTH - 1] + "…"
    return text


def format_table(result: QueryResult) -> str:
    """Render rows as a width-aligned text table."""
    if not result.columns:
        return "(no columns)"
    cells = [[_cell(v) for v in row] for row in result.rows]
    widths = [len(c) for c in result.columns]
    for row in cells:
        for i, text in enumerate(row):
            widths[i] = max(widths[i], len(text))
    header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(result.columns))
    rule = "-+-".join("-" * w for w in widths)
    lines = [header, rule]
    lines.extend(" | ".join(text.ljust(widths[i]) for i, text in enumerate(row)) for row in cells)
    if result.truncated:
        lines.append(f"({len(result.rows)} rows shown; more available — raise --max-rows or add a LIMIT)")
    else:
        lines.append(f"({len(result.rows)} row{'s' if len(result.rows) != 1 else ''})")
    return "\n".join(lines)


def write_json(result: QueryResult, fh: IO[str]) -> None:
    """Write rows as a JSON array of {column: value} objects."""
    rows = [{col: jsonify(val) for col, val in zip(result.columns, row)} for row in result.rows]
    json.dump(rows, fh, indent=2)
    fh.write("\n")


def write_csv(result: QueryResult, fh: IO[str]) -> None:
    """Write rows as CSV with a header line."""
    writer = csv.writer(fh)
    writer.writerow(result.columns)
    for row in result.rows:
        writer.writerow(["" if v is None else jsonify(v) for v in row])


def write_result(result: QueryResult, fmt: str, fh: IO[str]) -> None:
    """Dispatch to the writer for ``fmt`` (one of :data:`FORMATS`)."""
    if fmt == "json":
        write_json(result, fh)
    elif fmt == "csv":
        write_csv(result, fh)
    else:
        fh.write(format_table(result) + "\n")

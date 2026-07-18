"""``spicy-regs query`` — run SQL against the published tables.

Every published table is available as a view (see ``spicy-regs tables``), so
queries can join across them, e.g.::

    spicy-regs query "SELECT agency_code, count(*) FROM dockets GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir
from spicy_regs.cli._output import FORMATS, write_result

DEFAULT_MAX_ROWS = 25


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("query", help="Run a SQL query against the tables")
    parser.add_argument("sql", help='SQL to run, e.g. "SELECT * FROM dockets LIMIT 5"')
    add_source_arguments(parser)
    parser.add_argument("--format", choices=FORMATS, default="table", help="Output format (default: table)")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        help=f"Maximum rows to return; 0 = unlimited (default: {DEFAULT_MAX_ROWS})",
    )
    parser.add_argument("--output", metavar="FILE", default=None, help="Write results to FILE instead of stdout")
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    if not specs:
        print("No tables available for this source. Run: spicy-regs download", file=sys.stderr)
        return 1
    con = engine.connect(specs)
    try:
        result = engine.run_query(con, args.sql, args.max_rows)
    except duckdb.Error as exc:
        print(f"Query failed: {exc}", file=sys.stderr)
        return 1
    if args.output:
        with Path(args.output).open("w", newline="") as fh:
            write_result(result, args.format, fh)
        print(f"Wrote {len(result.rows)} rows to {args.output}")
    else:
        write_result(result, args.format, sys.stdout)
    return 0

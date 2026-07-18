"""``spicy-regs sample`` — show random rows from a table."""

from __future__ import annotations

import argparse
import sys

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir
from spicy_regs.cli._output import format_table


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("sample", help="Show random sample rows from a table")
    parser.add_argument("data_type", choices=engine.TABLES, metavar="TABLE", help=f"One of: {', '.join(engine.TABLES)}")
    parser.add_argument("-n", type=int, default=5, help="Number of rows (default: 5)")
    parser.add_argument("--agency", help="Filter by agency code")
    add_source_arguments(parser)
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    table = args.data_type
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    spec = specs.get(table)
    if spec is None:
        print(f"Table '{table}' is not available locally. Run: spicy-regs download", file=sys.stderr)
        return 1
    con = engine.connect({table: spec})
    where = f"WHERE agency_code = '{engine.escape_sql_string(args.agency)}'" if args.agency else ""
    try:
        result = engine.run_query(con, f"SELECT * FROM {table} {where} USING SAMPLE {int(args.n)} ROWS", max_rows=0)
    except duckdb.Error as exc:
        print(f"Could not sample {table}: {exc}", file=sys.stderr)
        return 1
    print(f"\nSample from {table} ({spec.kind}: {spec.location}):")
    print("=" * 80)
    print(format_table(result))
    return 0

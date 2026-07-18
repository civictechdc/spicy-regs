"""``spicy-regs describe`` — show the column schema of one table."""

from __future__ import annotations

import argparse
import sys

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir
from spicy_regs.cli._output import write_result


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("describe", help="Show the column schema of a table")
    parser.add_argument("table", choices=engine.TABLES, metavar="TABLE", help=f"One of: {', '.join(engine.TABLES)}")
    add_source_arguments(parser)
    parser.add_argument("--format", choices=["table", "json"], default="table", help="Output format")
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    spec = specs.get(args.table)
    if spec is None:
        print(f"Table '{args.table}' is not available locally. Run: spicy-regs download", file=sys.stderr)
        return 1
    # Only bind the one view we need — binding reads parquet metadata, which is
    # an HTTP round-trip per table for remote sources.
    con = engine.connect({args.table: spec})
    try:
        result = engine.run_query(con, f"DESCRIBE {args.table}", max_rows=0)
    except duckdb.Error as exc:
        print(f"Could not describe {args.table} at {spec.location}: {exc}", file=sys.stderr)
        return 1
    write_result(result, args.format, sys.stdout)
    return 0

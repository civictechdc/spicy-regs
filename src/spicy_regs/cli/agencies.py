"""``spicy-regs agencies`` — list every agency code in the dataset."""

from __future__ import annotations

import argparse
import sys

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir

# Checked in order; dockets is the smallest table that carries every agency.
CANDIDATE_TABLES = ("dockets", "documents", "comments")


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("agencies", help="List all agency codes")
    add_source_arguments(parser)
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    for table in CANDIDATE_TABLES:
        spec = specs.get(table)
        if spec is None:
            continue
        con = engine.connect({table: spec})
        sql = f"SELECT DISTINCT agency_code FROM {table} WHERE agency_code IS NOT NULL ORDER BY 1"
        try:
            result = engine.run_query(con, sql, max_rows=0)
        except duckdb.Error as exc:
            print(f"Could not read {table} at {spec.location}: {exc}", file=sys.stderr)
            continue
        print(f"Agencies ({len(result.rows)} total, from {table}):")
        print("=" * 40)
        for (agency,) in result.rows:
            print(f"  {agency}")
        return 0
    print("No data available. Run: spicy-regs download", file=sys.stderr)
    return 1

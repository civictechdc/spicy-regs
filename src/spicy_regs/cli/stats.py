"""``spicy-regs stats`` — row counts and top agencies for the core tables."""

from __future__ import annotations

import argparse
import sys

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir

# The core record types (first entries of the published table list); the
# rollup/companion tables are better explored with `tables` + `query`.
CORE_TABLES = ("dockets", "documents", "comments")


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("stats", help="Show dataset statistics for the core tables")
    add_source_arguments(parser)
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)

    print("=" * 60)
    print("Dataset Statistics")
    print("=" * 60)

    failures = 0
    for table in CORE_TABLES:
        spec = specs.get(table)
        if spec is None:
            print(f"\n{table.upper()}: Not downloaded yet (run: spicy-regs download)")
            continue
        con = engine.connect({table: spec})
        try:
            total = engine.run_query(con, f"SELECT count(*) FROM {table}", max_rows=1).rows[0][0]
            columns = [row[0] for row in engine.run_query(con, f"DESCRIBE {table}", max_rows=0).rows]
            print(f"\n{table.upper()} ({spec.kind}: {spec.location})")
            print("-" * 40)
            print(f"  Rows: {total:,}")
            print(f"  Columns: {', '.join(columns)}")
            if "agency_code" in columns:
                top = engine.run_query(
                    con,
                    f"SELECT agency_code, count(*) AS n FROM {table} GROUP BY 1 ORDER BY n DESC, agency_code LIMIT 5",
                    max_rows=5,
                )
                print("  Top agencies:")
                for agency, count in top.rows:
                    print(f"    {agency}: {count:,}")
        except duckdb.Error as exc:
            print(f"\n{table.upper()}: could not read {spec.location}: {exc}", file=sys.stderr)
            failures += 1
    return 1 if failures else 0

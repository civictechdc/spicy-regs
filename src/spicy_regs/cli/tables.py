"""``spicy-regs tables`` — list the queryable tables and where each resolves."""

from __future__ import annotations

import argparse
import json

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("tables", help="List available tables and where each one resolves")
    add_source_arguments(parser)
    parser.add_argument("--format", choices=["table", "json"], default="table", help="Output format")
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    if args.format == "json":
        payload = [
            {"table": table, "source": spec.kind, "location": spec.location} for table, spec in sorted(specs.items())
        ]
        print(json.dumps(payload, indent=2))
        return 0

    if not specs:
        print("No tables available for this source. Run: spicy-regs download")
        return 1
    width = max(len(t) for t in specs)
    for table, spec in sorted(specs.items()):
        print(f"{table.ljust(width)}  {spec.kind:5}  {spec.location}")
    missing = [t for t in engine.TABLES if t not in specs]
    if missing:
        print(f"\nNot available from this source: {', '.join(missing)}")
    return 0

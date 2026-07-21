"""Spicy Regs CLI — download and query federal regulations data.

Usage:
    spicy-regs download            # fetch the core parquet files
    spicy-regs tables              # list every queryable table
    spicy-regs describe dockets    # column schema of one table
    spicy-regs query "SELECT ..."  # run SQL (local files or straight off R2)

Each subcommand lives in its own module in this package; see ``_registry.py``
for how to add one.
"""

from __future__ import annotations

import argparse
import sys

from spicy_regs.cli._common import DEFAULT_OUTPUT_DIR


def build_parser() -> argparse.ArgumentParser:
    # Imported here (not at module top) so command modules can import helpers
    # from ``spicy_regs.cli._common`` without a circular import at package load.
    from spicy_regs.cli._registry import COMMANDS

    parser = argparse.ArgumentParser(
        prog="spicy-regs",
        description="Download and explore federal regulations data from Spicy Regs",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=None,
        help=f"Directory for local data files (default: {DEFAULT_OUTPUT_DIR})",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    for module in COMMANDS:
        module.register(subparsers)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        sys.exit(0)
    sys.exit(args.run(args))


if __name__ == "__main__":
    main()

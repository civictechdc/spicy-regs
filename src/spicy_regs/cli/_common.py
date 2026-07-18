"""Helpers shared by the ``spicy-regs`` command modules."""

from __future__ import annotations

import argparse
from pathlib import Path

from spicy_regs.data_dictionary import DEFAULT_R2_BASE_URL

DEFAULT_OUTPUT_DIR = Path("./spicy-regs-data")


def get_output_dir(args: argparse.Namespace) -> Path:
    """Resolve the data directory from ``--output-dir`` (global or per-command)."""
    raw = getattr(args, "output_dir", None)
    return Path(raw) if raw else DEFAULT_OUTPUT_DIR


def add_output_dir_argument(parser: argparse.ArgumentParser) -> None:
    """Accept ``-o/--output-dir`` after the subcommand too (``spicy-regs download -o dir``).

    ``default=SUPPRESS`` keeps the subparser from clobbering a value given
    before the subcommand (``spicy-regs -o dir download``) — argparse subparser
    defaults would otherwise overwrite the already-parsed global value.
    """
    parser.add_argument(
        "--output-dir",
        "-o",
        default=argparse.SUPPRESS,
        help=f"Directory for local data files (default: {DEFAULT_OUTPUT_DIR})",
    )


def add_source_arguments(parser: argparse.ArgumentParser) -> None:
    """Standard flags for commands that read tables through the query engine."""
    parser.add_argument(
        "--source",
        choices=["r2", "local", "auto"],
        default="auto",
        help="Read from the public R2 bucket, local downloads, or per-table whichever is present locally (default)",
    )
    parser.add_argument(
        "--r2-url",
        default=DEFAULT_R2_BASE_URL,
        help=f"Base URL of the public parquet bucket (default: {DEFAULT_R2_BASE_URL})",
    )
    add_output_dir_argument(parser)

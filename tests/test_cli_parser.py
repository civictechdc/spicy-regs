"""Argument-parsing tests for the spicy-regs CLI (no I/O)."""

import pytest

from spicy_regs.cli import build_parser
from spicy_regs.cli._registry import COMMANDS
from spicy_regs.data_dictionary import TABLES


@pytest.fixture
def parser():
    return build_parser()


def test_every_registered_command_parses(parser):
    args_by_command = {
        "download": [],
        "tables": [],
        "describe": ["dockets"],
        "query": ["SELECT 1"],
        "stats": [],
        "sample": ["dockets"],
        "search": ["climate"],
        "agencies": [],
    }
    registered = {module.__name__.rsplit(".", 1)[-1] for module in COMMANDS}
    assert registered == set(args_by_command)
    for command, extra in args_by_command.items():
        args = parser.parse_args([command, *extra])
        assert args.command == command
        assert callable(args.run)


def test_download_types_alias_still_parses(parser):
    args = parser.parse_args(["download", "--types", "comments"])
    assert args.tables == ["comments"]


def test_download_tables_all_force(parser):
    args = parser.parse_args(["download", "--tables", "dockets", "feed_summary", "--force"])
    assert args.tables == ["dockets", "feed_summary"]
    assert args.force is True
    assert args.download_all is False

    args = parser.parse_args(["download", "--all"])
    assert args.download_all is True
    assert args.tables is None


def test_download_rejects_unknown_table(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(["download", "--tables", "nonsense"])


def test_query_defaults(parser):
    args = parser.parse_args(["query", "SELECT 1"])
    assert args.sql == "SELECT 1"
    assert args.source == "auto"
    assert args.format == "table"
    assert args.max_rows == 25
    assert args.output is None


def test_describe_rejects_unknown_table(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(["describe", "not_a_table"])


def test_sample_accepts_every_published_table(parser):
    for table in TABLES:
        args = parser.parse_args(["sample", table, "-n", "3"])
        assert args.data_type == table
        assert args.n == 3


def test_output_dir_accepted_before_and_after_subcommand(parser):
    before = parser.parse_args(["-o", "/tmp/a", "stats"])
    assert before.output_dir == "/tmp/a"
    after = parser.parse_args(["stats", "-o", "/tmp/b"])
    assert after.output_dir == "/tmp/b"
    # The subcommand's flag must not clobber a value given before it.
    both = parser.parse_args(["-o", "/tmp/a", "stats", "-o", "/tmp/b"])
    assert both.output_dir == "/tmp/b"
    neither = parser.parse_args(["stats"])
    assert neither.output_dir is None

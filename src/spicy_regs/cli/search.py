"""``spicy-regs search`` — substring search across the core tables."""

from __future__ import annotations

import argparse
import sys

import duckdb

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_source_arguments, get_output_dir

# Table -> (id column, text columns searched). Add an entry here to make
# another table searchable.
SEARCH_CONFIGS: dict[str, tuple[str, tuple[str, ...]]] = {
    "dockets": ("docket_id", ("title", "abstract")),
    "documents": ("document_id", ("title", "text_content")),
    "comments": ("comment_id", ("title", "comment", "text_content")),
}


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("search", help="Search for a substring across the core tables")
    parser.add_argument("query", help="Text to search for (case-insensitive substring)")
    parser.add_argument("--limit", "-l", type=int, default=10, help="Max results per table (default: 10)")
    add_source_arguments(parser)
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    specs = engine.resolve_view_specs(args.source, get_output_dir(args), args.r2_url)
    needle = engine.escape_sql_string(args.query.lower())

    print(f"Searching for: '{args.query}'")
    print("=" * 60)

    failures = 0
    for table, (id_column, text_columns) in SEARCH_CONFIGS.items():
        spec = specs.get(table)
        if spec is None:
            continue
        con = engine.connect({table: spec})
        try:
            available = {row[0] for row in engine.run_query(con, f"DESCRIBE {table}", max_rows=0).rows}
        except duckdb.Error as exc:
            print(f"\n{table.upper()}: could not read {spec.location}: {exc}", file=sys.stderr)
            failures += 1
            continue
        columns = [c for c in text_columns if c in available]
        if not columns:
            continue
        # contains() is a literal substring match, so '%' or '_' in the query
        # need no escaping (unlike LIKE/ILIKE).
        condition = " OR ".join(f"contains(lower(coalesce({col}, '')), '{needle}')" for col in columns)
        sql = (
            f"SELECT {id_column}, coalesce(title, '(no title)') AS title "
            f"FROM {table} WHERE {condition} LIMIT {int(args.limit)}"
        )
        try:
            result = engine.run_query(con, sql, max_rows=0)
        except duckdb.Error as exc:
            print(f"\n{table.upper()}: search failed: {exc}", file=sys.stderr)
            failures += 1
            continue
        if result.rows:
            suffix = " (showing first matches; raise --limit for more)" if len(result.rows) == args.limit else ""
            print(f"\n{table.upper()}: {len(result.rows)} match{'es' if len(result.rows) != 1 else ''}{suffix}")
            print("-" * 40)
            for row_id, title in result.rows:
                print(f"  {row_id}: {str(title)[:80]}")
    return 1 if failures else 0

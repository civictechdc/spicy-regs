#!/usr/bin/env python3
# /// script
# dependencies = [
#   "duckdb>=1.2.0",
# ]
# ///
#
"""Inspect Spicy Regs parquet data with DuckDB, locally or from Cloudflare R2."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _import_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        print(
            json.dumps(
                {
                    "error": "duckdb is not installed in the current Python environment",
                    "hint": "Run this helper as a standalone uv script, for example: uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --list-sources",
                },
                indent=2,
            )
        )
        raise SystemExit(1)
    return duckdb


def _local_view_specs(output_dir: Path) -> dict[str, str]:
    specs: dict[str, str] = {}

    dockets = output_dir / "dockets.parquet"
    if dockets.exists():
        specs["dockets"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(dockets))}')"

    documents = output_dir / "documents.parquet"
    if documents.exists():
        specs["documents"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(documents))}')"

    comments = output_dir / "comments.parquet"
    if comments.exists():
        specs["comments"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(comments))}')"
    else:
        partitioned = output_dir / "comments"
        if partitioned.exists():
            specs["comments"] = (
                "SELECT * FROM read_parquet("
                f"'{_escape_sql_string(str(partitioned / '**' / '*.parquet'))}', "
                "union_by_name=true, hive_partitioning=true)"
            )

    comments_index = output_dir / "comments_index.parquet"
    if comments_index.exists():
        specs["comments_index"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(comments_index))}')"

    feed_summary = output_dir / "feed_summary.parquet"
    if feed_summary.exists():
        specs["feed_summary"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(feed_summary))}')"

    return specs


def _remote_view_specs(base_url: str) -> dict[str, str]:
    url = base_url.rstrip("/")
    return {
        "dockets": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/dockets.parquet')}')",
        "documents": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/documents.parquet')}')",
        "comments": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/comments.parquet')}')",
        "comments_index": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/comments_index.parquet')}')",
        "feed_summary": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/feed_summary.parquet')}')",
    }


def _connect_with_views(source: str, output_dir: Path, base_url: str):
    duckdb = _import_duckdb()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    if source == "r2":
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        specs = _remote_view_specs(base_url)
    else:
        specs = _local_view_specs(output_dir)
    for name, sql in specs.items():
        con.execute(f"CREATE VIEW {name} AS {sql}")
    return con, specs


def cmd_list_sources(source: str, output_dir: Path, base_url: str) -> int:
    _, specs = _connect_with_views(source, output_dir, base_url)
    payload = {
        "source": source,
        "output_dir": str(output_dir.resolve()) if source == "local" else None,
        "base_url": base_url if source == "r2" else None,
        "tables": sorted(specs),
        "available": bool(specs),
    }
    print(json.dumps(payload, indent=2))
    return 0 if specs else 1


def cmd_describe(source: str, output_dir: Path, base_url: str, table: str) -> int:
    con, specs = _connect_with_views(source, output_dir, base_url)
    if table not in specs:
        print(
            json.dumps(
                {
                    "error": f"Table '{table}' is not available",
                    "available_tables": sorted(specs),
                },
                indent=2,
            )
        )
        return 1

    rows = con.execute(f"DESCRIBE {table}").fetchall()
    result = [
        {"column_name": row[0], "column_type": row[1], "null": row[2], "key": row[3], "default": row[4]}
        for row in rows
    ]
    print(json.dumps({"table": table, "columns": result}, indent=2))
    return 0


def cmd_sql(source: str, output_dir: Path, base_url: str, sql: str, max_rows: int) -> int:
    con, specs = _connect_with_views(source, output_dir, base_url)
    if not specs:
        print(
            json.dumps(
                {
                    "error": "No Spicy Regs parquet tables found",
                    "source": source,
                    "output_dir": str(output_dir.resolve()) if source == "local" else None,
                    "base_url": base_url if source == "r2" else None,
                },
                indent=2,
            )
        )
        return 1

    cursor = con.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchmany(max_rows)
    result_rows = [dict(zip(columns, row, strict=False)) for row in rows]
    payload = {
        "source": source,
        "output_dir": str(output_dir.resolve()) if source == "local" else None,
        "base_url": base_url if source == "r2" else None,
        "row_count_shown": len(result_rows),
        "max_rows": max_rows,
        "columns": columns,
        "rows": result_rows,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=["r2", "local"],
        default="r2",
        help="Query the public Cloudflare R2 bucket or local parquet files",
    )
    parser.add_argument(
        "--r2-url",
        default=R2_BASE_URL,
        help="Base URL for the public Cloudflare R2 bucket",
    )
    parser.add_argument(
        "--output-dir",
        default="./spicy-regs-data",
        help="Directory containing local Spicy Regs parquet outputs when --source=local",
    )
    parser.add_argument("--list-sources", action="store_true", help="Print available logical tables")
    parser.add_argument("--describe", metavar="TABLE", help="Describe a logical table such as dockets or comments")
    parser.add_argument("--sql", help="Run a SQL query against the logical views")
    parser.add_argument("--max-rows", type=int, default=25, help="Maximum rows to print for --sql")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir)

    if args.list_sources:
        return cmd_list_sources(args.source, output_dir, args.r2_url)
    if args.describe:
        return cmd_describe(args.source, output_dir, args.r2_url, args.describe)
    if args.sql:
        return cmd_sql(args.source, output_dir, args.r2_url, args.sql, args.max_rows)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Generate and validate the Spicy Regs data dictionary.

The data dictionary has two layers:

* **Schema (source of truth, in code).** Column names and types come from
  :data:`spicy_regs.schemas.regulations.RECORD_TYPES` for the three core tables
  and from :data:`DERIVED_SCHEMAS` below for the four rollup tables. This keeps
  generation deterministic and offline.
* **Descriptions (curated prose).** Human descriptions live in
  ``data_dictionary/descriptions.yaml``, keyed by table and column.

``spicy-regs-dict check`` reconciles the two so they can't silently drift, and
``spicy-regs-dict generate`` renders one Markdown page per table for the MkDocs
site under ``docs/tables/``.

Usage::

    uv run spicy-regs-dict check                 # offline: descriptions vs in-code schema
    uv run spicy-regs-dict check --source r2     # also reconcile in-code schema vs live R2 parquet
    uv run spicy-regs-dict generate              # (re)write docs/tables/*.md
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import polars as pl

from spicy_regs.schemas.regulations import RECORD_TYPES

# Repo layout anchors (this file lives at src/spicy_regs/data_dictionary.py).
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DESCRIPTIONS = REPO_ROOT / "data_dictionary" / "descriptions.yaml"
DEFAULT_DOCS_TABLES_DIR = REPO_ROOT / "docs" / "tables"

DEFAULT_R2_BASE_URL = "https://r2.spicy-regs.dev"

# Display order for the dictionary. The first three are the core record types;
# the rest are derived rollups. This is the full public R2 surface.
TABLES: tuple[str, ...] = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
)

# Tables the MCP server (list_sources / describe_table / query_sql) exposes.
# This must equal spicy_regs.mcp_server.TABLES; a test enforces it so the docs'
# "queryable via MCP" flag can't drift from what the server actually serves.
MCP_QUERYABLE: frozenset[str] = frozenset(
    {
        "dockets",
        "documents",
        "comments",
        "comments_index",
        "feed_summary",
        "agency_stats",
        "agency_monthly_volume",
    }
)

# Schemas for the derived rollups. These mirror the SQL/Polars schemas in
# src/spicy_regs/transforms/{build_feed_summary,build_agency_rollups,
# update_comments_index}.py. Types are DuckDB type names, matching what a
# DESCRIBE of the published parquet returns (see `check --source r2`).
DERIVED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "comments_index": [
        ("agency_code", "VARCHAR"),
        ("docket_id", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("row_count", "BIGINT"),
    ],
    "feed_summary": [
        ("docket_id", "VARCHAR"),
        ("agency_code", "VARCHAR"),
        ("title", "VARCHAR"),
        ("docket_type", "VARCHAR"),
        ("modify_date", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("comment_count", "BIGINT"),
        ("comment_end_date", "VARCHAR"),
        ("date_created", "VARCHAR"),
    ],
    "agency_stats": [
        ("agency_code", "VARCHAR"),
        ("docket_count", "BIGINT"),
        ("document_count", "BIGINT"),
        ("comment_count", "BIGINT"),
    ],
    "agency_monthly_volume": [
        ("agency_code", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("document_type", "VARCHAR"),
        ("document_count", "BIGINT"),
    ],
}

# Polars dtype -> DuckDB type label, so core-table types line up with what a
# DESCRIBE of the published parquet shows.
_POLARS_TYPE_LABELS: list[tuple[object, str]] = [
    (pl.Utf8, "VARCHAR"),
    (pl.Int64, "BIGINT"),
    (pl.Int32, "INTEGER"),
    (pl.Float64, "DOUBLE"),
    (pl.Boolean, "BOOLEAN"),
]


def _polars_type_label(dtype: object) -> str:
    for candidate, label in _POLARS_TYPE_LABELS:
        if dtype == candidate:
            return label
    return str(dtype)


def expected_schemas() -> dict[str, list[tuple[str, str]]]:
    """Return ``{table: [(column, type_label), ...]}`` for all tables (offline)."""
    schemas: dict[str, list[tuple[str, str]]] = {}
    for name in TABLES:
        if name in RECORD_TYPES:
            rt = RECORD_TYPES[name]
            schemas[name] = [(col, _polars_type_label(dt)) for col, dt in rt.schema.items()]
        elif name in DERIVED_SCHEMAS:
            schemas[name] = list(DERIVED_SCHEMAS[name])
        else:  # pragma: no cover - guards against TABLES/registry drift
            raise KeyError(f"No schema known for table {name!r}")
    return schemas


# --------------------------------------------------------------------------- #
# Live schema discovery (DuckDB DESCRIBE over R2 or a local parquet directory).
# --------------------------------------------------------------------------- #
def discover_schemas(source: str, base: str | None = None) -> dict[str, list[tuple[str, str]]]:
    """Discover schemas from published parquet via DuckDB ``DESCRIBE``.

    ``source`` is ``"r2"`` (remote https bucket; needs the httpfs extension) or
    ``"local"`` (a directory of ``<table>.parquet`` files). Mirrors the
    connection recipe in :mod:`spicy_regs.mcp_server`.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET home_directory='{tempfile.gettempdir()}'")
    if source == "r2":
        base_url = (base or DEFAULT_R2_BASE_URL).rstrip("/")
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        def url_for(name: str) -> str:
            return f"{base_url}/{name}.parquet"

    elif source == "local":
        base_dir = Path(base or "./spicy-regs-data")

        def url_for(name: str) -> str:
            return str(base_dir / f"{name}.parquet")

    else:
        raise ValueError(f"Unknown source {source!r}; expected 'r2' or 'local'")

    schemas: dict[str, list[tuple[str, str]]] = {}
    for name in TABLES:
        target = url_for(name).replace("'", "''")
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()
        schemas[name] = [(row[0], row[1]) for row in rows]
    con.close()
    return schemas


# --------------------------------------------------------------------------- #
# Descriptions file.
# --------------------------------------------------------------------------- #
def load_descriptions(path: Path = DEFAULT_DESCRIPTIONS) -> dict:
    import yaml

    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("tables", {})


# --------------------------------------------------------------------------- #
# Reconciliation.
# --------------------------------------------------------------------------- #
def _reconcile_columns(
    table: str,
    left_label: str,
    left_cols: list[str],
    right_label: str,
    right_cols: list[str],
) -> list[str]:
    """Return human-readable drift errors comparing two column-name lists."""
    errors: list[str] = []
    left, right = set(left_cols), set(right_cols)
    for col in sorted(left - right):
        errors.append(f"[{table}] column {col!r} in {left_label} but missing from {right_label}")
    for col in sorted(right - left):
        errors.append(f"[{table}] column {col!r} in {right_label} but missing from {left_label}")
    return errors


def check_descriptions(
    schemas: dict[str, list[tuple[str, str]]],
    descriptions: dict,
) -> list[str]:
    """Reconcile a schema map against the curated descriptions. Returns errors."""
    errors: list[str] = []
    schema_tables = set(schemas)
    desc_tables = set(descriptions)
    for table in sorted(schema_tables - desc_tables):
        errors.append(f"[{table}] table has a schema but no entry in descriptions.yaml")
    for table in sorted(desc_tables - schema_tables):
        errors.append(f"[{table}] described in descriptions.yaml but is not a known table")

    for table in sorted(schema_tables & desc_tables):
        schema_cols = [c for c, _ in schemas[table]]
        entry = descriptions[table] or {}
        if not (entry.get("summary") or "").strip():
            errors.append(f"[{table}] missing a 'summary' in descriptions.yaml")
        desc_cols = list((entry.get("columns") or {}).keys())
        errors.extend(
            _reconcile_columns(table, "schema", schema_cols, "descriptions.yaml", desc_cols)
        )
        for col in schema_cols:
            text = (entry.get("columns") or {}).get(col)
            if col in desc_cols and not (text or "").strip():
                errors.append(f"[{table}.{col}] has an empty description")
    return errors


def check_schema_drift(
    expected: dict[str, list[tuple[str, str]]],
    live: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Reconcile the in-code expected schema against a live (parquet) schema."""
    errors: list[str] = []
    for table in TABLES:
        exp_cols = [c for c, _ in expected.get(table, [])]
        live_cols = [c for c, _ in live.get(table, [])]
        errors.extend(
            _reconcile_columns(table, "in-code schema", exp_cols, "live parquet", live_cols)
        )
    return errors


# --------------------------------------------------------------------------- #
# Markdown generation.
# --------------------------------------------------------------------------- #
_GENERATED_BANNER = (
    "<!-- Generated by `spicy-regs-dict generate`. Do not edit by hand. "
    "Edit data_dictionary/descriptions.yaml or the schema, then regenerate. -->"
)


def _render_table_page(
    table: str,
    columns: list[tuple[str, str]],
    entry: dict,
) -> str:
    summary = (entry.get("summary") or "").strip()
    col_desc = entry.get("columns") or {}
    pk = None
    rt = RECORD_TYPES.get(table)
    if rt is not None:
        pk = rt.dedup_key

    lines = [_GENERATED_BANNER, "", f"# `{table}`", ""]
    if summary:
        lines += [summary, ""]
    queryable = "Yes" if table in MCP_QUERYABLE else "No (published to R2 only)"
    lines += [
        f"- **Parquet file:** `{table}.parquet`",
        f"- **Queryable via MCP `query_sql`:** {queryable}",
    ]
    if pk:
        lines.append(f"- **Primary / dedup key:** `{pk}`")
    lines += ["", "| Column | Type | Description |", "| --- | --- | --- |"]
    for col, col_type in columns:
        desc = (col_desc.get(col) or "").replace("|", "\\|").strip()
        marker = " 🔑" if col == pk else ""
        lines.append(f"| `{col}`{marker} | `{col_type}` | {desc} |")
    lines.append("")
    return "\n".join(lines)


def generate(
    descriptions: dict,
    schemas: dict[str, list[tuple[str, str]]],
    out_dir: Path = DEFAULT_DOCS_TABLES_DIR,
) -> list[Path]:
    """Render one Markdown page per table. Returns the written paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table in TABLES:
        entry = descriptions.get(table, {}) or {}
        page = _render_table_page(table, schemas[table], entry)
        path = out_dir / f"{table}.md"
        path.write_text(page, encoding="utf-8")
        written.append(path)
    return written


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #
def _schemas_for_source(source: str, base: str | None) -> dict[str, list[tuple[str, str]]]:
    if source == "schema":
        return expected_schemas()
    return discover_schemas(source, base)


def cmd_check(args: argparse.Namespace) -> int:
    descriptions = load_descriptions(Path(args.descriptions))
    errors: list[str] = []

    if args.source == "schema":
        errors += check_descriptions(expected_schemas(), descriptions)
    else:
        live = discover_schemas(args.source, args.base)
        # Descriptions must cover the live schema, and the in-code registry the
        # docs are generated from must match the live schema too.
        errors += check_descriptions(live, descriptions)
        errors += check_schema_drift(expected_schemas(), live)

    if errors:
        print(f"✗ Data dictionary check failed ({len(errors)} issue(s)):", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\nUpdate data_dictionary/descriptions.yaml (and DERIVED_SCHEMAS / RECORD_TYPES "
            "if the schema changed) so they line up.",
            file=sys.stderr,
        )
        return 1
    print(f"✓ Data dictionary check passed ({len(descriptions)} tables, source={args.source}).")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    descriptions = load_descriptions(Path(args.descriptions))
    schemas = _schemas_for_source(args.source, args.base)
    # Fail rather than emit a dictionary that disagrees with itself.
    errors = check_descriptions(schemas, descriptions)
    if errors:
        print("✗ Refusing to generate: descriptions are out of sync with the schema.", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_DOCS_TABLES_DIR
    written = generate(descriptions, schemas, out_dir)
    print(f"✓ Wrote {len(written)} table page(s) to {out_dir} (source={args.source}).")
    for path in written:
        print(f"  - {path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spicy-regs-dict",
        description="Generate and validate the Spicy Regs data dictionary.",
    )
    parser.add_argument(
        "--descriptions",
        default=str(DEFAULT_DESCRIPTIONS),
        help=f"Path to descriptions.yaml (default: {DEFAULT_DESCRIPTIONS})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser("check", help="Reconcile descriptions and schema; exit non-zero on drift")
    p_check.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="schema = in-code (offline, default); r2/local = live parquet via DuckDB DESCRIBE",
    )
    p_check.add_argument(
        "--base",
        default=None,
        help="R2 base URL (source=r2) or local parquet dir (source=local)",
    )
    p_check.set_defaults(func=cmd_check)

    p_gen = sub.add_parser("generate", help="Render docs/tables/*.md from the schema + descriptions")
    p_gen.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="Where to read column names/types from (default: in-code schema, offline)",
    )
    p_gen.add_argument("--base", default=None, help="R2 base URL or local parquet dir")
    p_gen.add_argument("--out-dir", default=None, help=f"Output dir (default: {DEFAULT_DOCS_TABLES_DIR})")
    p_gen.set_defaults(func=cmd_generate)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()

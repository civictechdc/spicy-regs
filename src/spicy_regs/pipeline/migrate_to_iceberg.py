#!/usr/bin/env python3
"""
Migrate Parquet files on R2 → Iceberg tables in the R2 Data Catalog.

Reads the existing public Parquet files via DuckDB's read_parquet() and writes
them as Iceberg tables through the Cloudflare REST catalog.

Usage:
    python migrate_to_iceberg.py
    python migrate_to_iceberg.py --schema raw     # use 'raw' instead of 'regulations'
    python migrate_to_iceberg.py --dry-run         # show plan without executing
"""

import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────
R2_PUBLIC_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"
CATALOG_URI = "https://catalog.cloudflarestorage.com/a18589c7a7a0fc4febecadfc9c71b105/spicy-regs"
WAREHOUSE = "a18589c7a7a0fc4febecadfc9c71b105_spicy-regs"
TOKEN = os.getenv("R2_API_TOKEN")

# Table configs: name -> optional partition spec
# NOTE: DuckDB's Iceberg extension does not support PARTITIONED BY as of v1.4.
# Partition pruning for comments is handled via Hive-partitioned Parquet on R2 instead.
TABLES = {
    "dockets": {
        "partition": None,
        "source_transform": None,
    },
    "documents": {
        "partition": None,
        "source_transform": None,
    },
    "comments": {
        "partition": None,
        "source_transform": None,
    },
}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Migrate Parquet → Iceberg tables")
    parser.add_argument(
        "--schema",
        default="regulations",
        help="Iceberg namespace/schema name (default: regulations)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show migration plan without executing",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop and recreate existing tables",
    )
    parser.add_argument(
        "--table",
        choices=list(TABLES.keys()),
        help="Migrate a single table only",
    )
    args = parser.parse_args()

    if not TOKEN:
        print("❌ R2_API_TOKEN not found in .env")
        sys.exit(1)

    schema = args.schema

    tables_to_migrate = [args.table] if args.table else list(TABLES.keys())

    if args.dry_run:
        print("DRY RUN — no changes will be made\n")
        for table in tables_to_migrate:
            config = TABLES[table]
            partition = config["partition"] or ""
            select = config["source_transform"] or "SELECT *"
            print(f"  CREATE TABLE spicy_regs.{schema}.{table} {partition}")
            print(f"    AS {select} FROM read_parquet('{R2_PUBLIC_URL}/{table}.parquet')")
            if args.force:
                print(f"    (will DROP existing table first)")
            print()
        return

    import duckdb

    print(f"DuckDB version: {duckdb.__version__}")
    conn = duckdb.connect()

    # Load extensions
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    print("✓ Extensions loaded")

    # Authenticate and attach catalog
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE ICEBERG,
            TOKEN '{TOKEN}'
        );
    """)
    conn.execute(f"""
        ATTACH '{WAREHOUSE}' AS spicy_regs (
            TYPE ICEBERG,
            ENDPOINT '{CATALOG_URI}'
        );
    """)
    print("✓ Catalog attached\n")

    # Create schema
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS spicy_regs.{schema}")
    print(f"✓ Schema '{schema}' ready\n")

    # Migrate each table
    print("=" * 60)
    for table in tables_to_migrate:
        config = TABLES[table]
        fqn = f"spicy_regs.{schema}.{table}"
        source = f"{R2_PUBLIC_URL}/{table}.parquet"

        # Check if table already exists
        try:
            existing = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
            if args.force:
                print(f"🗑 Dropping {fqn} ({existing:,} rows)...")
                conn.execute(f"DROP TABLE {fqn}")
            else:
                print(f"⏭ {fqn} already exists ({existing:,} rows) — skipping (use --force to recreate)")
                print()
                continue
        except Exception:
            pass  # Table doesn't exist, proceed

        print(f"📦 Migrating {table}...")
        print(f"   Source: {source}")

        partition_clause = config["partition"] or ""
        select_clause = config["source_transform"] or "SELECT *"

        if partition_clause:
            print(f"   Partition: {partition_clause}")

        start = time.time()

        conn.execute(f"""
            CREATE TABLE {fqn} {partition_clause} AS
            {select_clause} FROM read_parquet('{source}')
        """)

        elapsed = time.time() - start

        # Verify
        count = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
        print(f"   ✓ {count:,} rows in {elapsed:.1f}s")

        # Show sample
        sample = conn.execute(f"SELECT * FROM {fqn} LIMIT 3").fetchall()
        cols = [desc[0] for desc in conn.description]
        print(f"   Sample columns: {', '.join(cols)}")
        for row in sample:
            print(f"   {row}")
        print()

    # Final summary
    print("=" * 60)
    print("Migration complete! Tables in catalog:\n")
    tables = conn.execute("SHOW ALL TABLES").fetchall()
    for row in tables:
        if row[0] == "spicy_regs":
            fqn = f"{row[0]}.{row[1]}.{row[2]}"
            count = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
            print(f"  {fqn}: {count:,} rows")

    conn.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()

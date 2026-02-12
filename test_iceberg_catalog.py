"""
Test Cloudflare R2 Iceberg Data Catalog connectivity.

Tests both PyIceberg and DuckDB access to verify the catalog is working.
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────
CATALOG_URI = "https://catalog.cloudflarestorage.com/a18589c7a7a0fc4febecadfc9c71b105/spicy-regs"
WAREHOUSE = "a18589c7a7a0fc4febecadfc9c71b105_spicy-regs"
TOKEN = os.getenv("R2_API_TOKEN")

if not TOKEN:
    print("❌ R2_API_TOKEN not found in .env")
    sys.exit(1)

print(f"Catalog URI: {CATALOG_URI}")
print(f"Warehouse:   {WAREHOUSE}")
print(f"Token:       {TOKEN[:8]}...{TOKEN[-4:]}")
print()


# ── Test 1: PyIceberg ───────────────────────────────────────────────────
def test_pyiceberg():
    """Connect via PyIceberg REST catalog and list namespaces/tables."""
    print("=" * 60)
    print("TEST 1: PyIceberg REST Catalog")
    print("=" * 60)

    from pyiceberg.catalog.rest import RestCatalog

    catalog = RestCatalog(
        name="spicy_regs",
        warehouse=WAREHOUSE,
        uri=CATALOG_URI,
        token=TOKEN,
    )

    # List namespaces
    namespaces = catalog.list_namespaces()
    print(f"✓ Connected! Found {len(namespaces)} namespace(s): {namespaces}")

    # List tables in each namespace
    for ns in namespaces:
        tables = catalog.list_tables(ns)
        print(f"  Namespace '{'.'.join(ns)}': {len(tables)} table(s)")
        for tbl in tables:
            print(f"    - {'.'.join(tbl)}")

    # If there are tables, try loading the first one
    all_tables = []
    for ns in namespaces:
        all_tables.extend(catalog.list_tables(ns))

    if all_tables:
        first_table = all_tables[0]
        table = catalog.load_table(first_table)
        print(f"\n  Loaded table '{'.'.join(first_table)}':")
        print(f"    Schema: {table.schema()}")
        print(f"    Snapshots: {len(table.metadata.snapshots)}")

        # Try scanning a few rows
        scan = table.scan(limit=5)
        arrow_table = scan.to_arrow()
        print(f"    Sample ({len(arrow_table)} rows):")
        print(arrow_table.to_pandas().to_string(index=False))
    else:
        print("\n  No tables found yet — catalog is empty (which is OK!)")
        print("  Creating a test table to verify write access...")

        import pyarrow as pa

        catalog.create_namespace_if_not_exists("default")
        test_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [80.0, 92.5, 88.0],
            }
        )
        table = catalog.create_table(("default", "test_connectivity"), schema=test_data.schema)
        table.append(test_data)
        result = table.scan().to_arrow()
        print(f"  ✓ Created & wrote test table: {len(result)} rows")
        print(result.to_pandas().to_string(index=False))

        # Cleanup
        catalog.drop_table(("default", "test_connectivity"))
        print("  ✓ Cleaned up test table")

    print()


# ── Test 2: DuckDB Iceberg Extension ───────────────────────────────────
def test_duckdb():
    """Connect via DuckDB's native Iceberg REST catalog support."""
    print("=" * 60)
    print("TEST 2: DuckDB Iceberg REST Catalog")
    print("=" * 60)

    import duckdb

    print(f"DuckDB version: {duckdb.__version__}")

    conn = duckdb.connect()

    # Install and load extensions
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    print("✓ Iceberg + httpfs extensions loaded")

    # Create secret with token (per Cloudflare docs)
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE ICEBERG,
            TOKEN '{TOKEN}'
        );
    """)
    print("✓ Secret created")

    # Attach the catalog: warehouse name is the ATTACH string
    conn.execute(f"""
        ATTACH '{WAREHOUSE}' AS spicy_regs (
            TYPE ICEBERG,
            ENDPOINT '{CATALOG_URI}'
        );
    """)
    print("✓ Catalog attached as 'spicy_regs'")

    # Show schemas (namespaces)
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'spicy_regs'").fetchall()
    print(f"  Schemas: {[s[0] for s in schemas]}")

    # Show tables
    tables = conn.execute("SHOW ALL TABLES").fetchdf()
    iceberg_tables = tables[tables["database"] == "spicy_regs"]
    if not iceberg_tables.empty:
        print(f"  Tables:")
        for _, row in iceberg_tables.iterrows():
            print(f"    - {row['database']}.{row['schema']}.{row['name']}")

        # Query first table
        first = iceberg_tables.iloc[0]
        fqn = f"spicy_regs.{first['schema']}.{first['name']}"
        count = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
        print(f"\n  Row count in {fqn}: {count:,}")

        sample = conn.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchdf()
        print(f"  Sample:")
        print(sample.to_string(index=False))
    else:
        print("  No tables found — catalog is empty (OK for a fresh setup)")

    conn.close()
    print()


# ── Run Tests ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        test_pyiceberg()
        print("✅ PyIceberg test PASSED\n")
    except Exception as e:
        print(f"❌ PyIceberg test FAILED: {e}\n")

    try:
        test_duckdb()
        print("✅ DuckDB Iceberg test PASSED\n")
    except Exception as e:
        print(f"❌ DuckDB Iceberg test FAILED: {e}\n")

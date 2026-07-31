# Querying with Python

Every table is published as public Apache Parquet at
`https://data.spicy-regs.dev/<table>.parquet` — **no credentials, no download
required**. The easiest way to query it from Python is [DuckDB](https://duckdb.org)
with the `httpfs` extension, which reads the remote Parquet directly (and only
fetches the byte ranges your query touches).

```bash
pip install duckdb          # or: uv add duckdb
```

## Setup

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")

BASE = "https://data.spicy-regs.dev"

def table(name: str) -> str:
    """Return a read_parquet(...) expression for a published table."""
    return f"read_parquet('{BASE}/{name}.parquet')"
```

## Query a single table

```python
# Row count
con.execute(f"SELECT count(*) FROM {table('dockets')}").fetchone()
# -> (276326,)

# A few sample rows
con.execute(f"SELECT docket_id, agency_code, title FROM {table('dockets')} LIMIT 5").fetchall()
```

!!! tip
    Add a `LIMIT` while exploring. DuckDB pushes filters and projections down to
    the remote file, so `SELECT a, b ... WHERE ... LIMIT n` is cheap even on the
    multi-hundred-thousand-row tables.

## Filter and aggregate

```python
# Busiest agencies, straight from the pre-aggregated rollup
con.execute(f"""
    SELECT agency_code, docket_count, comment_count
    FROM {table('agency_stats')}
    ORDER BY comment_count DESC
    LIMIT 10
""").fetchall()

# Most recent Federal Register documents
con.execute(f"""
    SELECT document_number, document_type, publication_date, title
    FROM {table('federal_register')}
    ORDER BY publication_date DESC
    LIMIT 10
""").fetchall()
```

## Results as a DataFrame

DuckDB converts a result set to pandas or polars in one call:

```python
df = con.execute(f"""
    SELECT agency_code, comment_count
    FROM {table('agency_stats')}
    ORDER BY comment_count DESC
    LIMIT 20
""").df()            # pandas DataFrame  (use .pl() for polars, .arrow() for Arrow)
```

## Join across sources

The complementary tables share a few keys, so you can follow a rulemaking across
its whole lifecycle and out to the organizations involved.

### Organizations → federal funding (`uei`)

`sam_entities` (the entity registry) and `usaspending_recipients` (federal award
recipients) both carry the **Unique Entity ID**:

```python
con.execute(f"""
    SELECT s.legal_business_name, s.state, u.total_award_amount
    FROM {table('sam_entities')} s
    JOIN {table('usaspending_recipients')} u USING (uei)
    ORDER BY TRY_CAST(u.total_award_amount AS DOUBLE) DESC
    LIMIT 10
""").fetchall()
```

### Planned action → published rule (`rin`)

`unified_agenda` is keyed by **RIN**; `federal_register` carries RINs in a JSON
array column, so unnest it to join:

```python
con.execute(f"""
    WITH fr_rins AS (
        SELECT DISTINCT rin
        FROM {table('federal_register')},
             UNNEST(CAST(regulation_id_numbers_json AS VARCHAR[])) AS t(rin)
        WHERE publication_date >= '2025-01-01'
    )
    SELECT ua.rin, ua.title, ua.rule_stage
    FROM {table('unified_agenda')} ua
    JOIN fr_rins USING (rin)
    LIMIT 10
""").fetchall()
# -> [('2120-AA64', 'Airworthiness Directives', ...), ...]
```

### Docket → its documents (`docket_id`)

```python
con.execute(f"""
    SELECT d.title, doc.document_type, doc.posted_date
    FROM {table('dockets')} d
    JOIN {table('documents')} doc USING (docket_id)
    WHERE d.docket_id = 'EPA-HQ-OAR-2021-0317'
    ORDER BY doc.posted_date
""").fetchall()
```

## Comments (the large one)

`comments` is tens of millions of rows and is published Hive-partitioned. For
**counts**, read the tiny `comments_index` rollup instead of scanning the full
table:

```python
con.execute(f"""
    SELECT agency_code, SUM(row_count) AS comments
    FROM {table('comments_index')}
    GROUP BY agency_code
    ORDER BY comments DESC
    LIMIT 10
""").fetchall()
```

To read comment *rows*, scope tightly by the Hive partition keys
(`agency_code`, `docket_id`, `year`, `month`) so DuckDB prunes to just the files
you need:

```python
con.execute(f"""
    SELECT comment_id, posted_date, title
    FROM read_parquet('{BASE}/comments/agency_code=EPA/docket_id=EPA-HQ-OAR-2021-0317/**/*.parquet')
    LIMIT 20
""").fetchall()
```

## Other ways in

- **Bundled CLI (local files):** `uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download` then `spicy-regs stats` / `sample` / `search`.
- **AI assistants (MCP):** the hosted server at `https://mcp.spicy-regs.dev/mcp` exposes `list_sources` / `describe_table` / `query_sql`. See the [home page](index.md#how-to-query-it).
- **Full schemas:** every column of every table is documented under [The tables](index.md#the-tables).

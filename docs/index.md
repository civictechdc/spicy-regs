# Spicy Regs Data Dictionary

This is the schema reference for the **Spicy Regs** dataset — an open mirror of
[regulations.gov](https://www.regulations.gov) federal regulatory data,
published as Apache Parquet on a public Cloudflare R2 bucket.

It documents every published table, column by column. The schema is generated
directly from the code that defines and produces the data, and a CI check fails
whenever the schema and these descriptions drift apart — so this reference stays
in step with what's actually published.

## Where the data comes from

```
regulations.gov  →  Mirrulations S3 mirror  →  Spicy Regs ETL  →  Parquet on R2
                                                                   (r2.spicy-regs.dev)
```

The ETL flattens the raw regulations.gov JSON into a handful of flat tables and
publishes them, plus a few small pre-computed rollups, to
`https://r2.spicy-regs.dev`.

## The tables

| Table | Grain | Queryable via MCP |
| --- | --- | --- |
| [`dockets`](tables/dockets.md) | one row per docket | Yes |
| [`documents`](tables/documents.md) | one row per document | Yes |
| [`comments`](tables/comments.md) | one row per public comment | Yes |
| [`comments_index`](tables/comments_index.md) | one row per comment partition | Yes |
| [`feed_summary`](tables/feed_summary.md) | one row per docket (rollup) | Yes |
| [`agency_stats`](tables/agency_stats.md) | one row per agency (rollup) | No (R2 only) |
| [`agency_monthly_volume`](tables/agency_monthly_volume.md) | one row per agency/month/type (rollup) | No (R2 only) |

## How the tables relate

The three core tables form a simple hierarchy keyed by id:

```
dockets (docket_id)
  └── documents (document_id, docket_id →)
  └── comments  (comment_id,  docket_id →)
```

- `documents.docket_id` and `comments.docket_id` reference `dockets.docket_id`.
- `agency_code` appears on every table and is the join key for the agency rollups.
- The rollups (`comments_index`, `feed_summary`, `agency_stats`,
  `agency_monthly_volume`) are pre-aggregated views built from the three core
  tables so consumers don't have to scan the tens-of-millions-of-rows comments
  dataset.

## How to query it

=== "AI assistant (MCP)"

    The hosted MCP server exposes `list_sources`, `describe_table`, and
    `query_sql` over the five core tables. Add
    `https://mcp.spicy-regs.dev/mcp` as a connector, or run it locally:

    ```bash
    claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp
    ```

=== "CLI"

    ```bash
    uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download
    uv run spicy-regs stats
    ```

=== "DuckDB (SQL)"

    ```sql
    INSTALL httpfs; LOAD httpfs;
    SELECT agency_code, COUNT(*) AS dockets
    FROM read_parquet('https://r2.spicy-regs.dev/dockets.parquet')
    GROUP BY agency_code
    ORDER BY dockets DESC
    LIMIT 20;
    ```

!!! note "Keeping this current"
    Column names and types are the source of truth in code
    (`RECORD_TYPES` for the core tables, `DERIVED_SCHEMAS` for the rollups). The
    prose lives in `data_dictionary/descriptions.yaml`. Run
    `uv run spicy-regs-dict generate` to rebuild the table pages, and
    `uv run spicy-regs-dict check` to verify the two are in sync — the same
    check runs in CI on every pull request.

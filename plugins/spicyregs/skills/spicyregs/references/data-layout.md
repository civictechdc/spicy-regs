# Spicy Regs Data Layout

Use this reference when you need a quick map of the remote or local data files before issuing SQL.

> Looking for full, column-by-column descriptions of every table? See the
> [Spicy Regs Data Dictionary](https://civictechdc.github.io/spicy-regs/). This
> page is the terse SQL cheat sheet; the dictionary is the complete reference.

## Common locations

- Default public R2 bucket: `https://r2.spicy-regs.dev`
- Default local parquet directory: `./spicy-regs-data/`
- Sample JSON for offline examples: `sample-data/mirrulations/`

## Expected parquet files

- `dockets.parquet`
- `documents.parquet`
- `comments.parquet`
- `comments_index.parquet`
- `feed_summary.parquet`
- Local-only: `comments/` partitioned comment parquet files may exist instead of a monolithic `comments.parquet`

> **Comments read surface.** `comments_index.parquet` (per-partition counts) is
> always the source for comment *counts*. The row-level `comments` table is
> served either from the monolithic `comments.parquet` snapshot or, when the
> deployment has the R2 Data Catalog configured (`R2_CATALOG_*`), directly from
> the Iceberg `comments` table in the catalog — which is kept current by the
> ETL's row-level `MERGE`. Either way you query the `comments` table by name;
> the MCP server picks the surface.

## Practical schema hints

These fields are stable enough to start with, but use `--describe` for the actual schema.

### dockets

- `docket_id`
- `agency_code`
- `title`
- `docket_type`
- `modify_date`
- `abstract`
- `rin` (Regulation Identifier Number, often null)

### documents

- `document_id`
- `docket_id`
- `agency_code`
- `title`
- `document_type`
- `posted_date`
- `modify_date`
- `comment_start_date`
- `comment_end_date`
- `file_url`
- `withdrawn` (`"true"`/`"false"`, often null)
- `reason_withdrawn` (often null)
- `additional_rins` (JSON array of extra RINs, often null)

### comments

- `comment_id`
- `docket_id`
- `agency_code`
- `first_name`
- `last_name`
- `organization`
- `category`
- `title`
- `comment`
- `document_type`
- `posted_date`
- `modify_date`
- `receive_date`
- `attachments_json`

## Handy SQL starters

```sql
SELECT agency_code, COUNT(*) AS docket_count
FROM dockets
GROUP BY agency_code
ORDER BY docket_count DESC
LIMIT 20;
```

```sql
SELECT d.docket_id, d.title, doc.comment_start_date, doc.comment_end_date
FROM dockets d
LEFT JOIN documents doc USING (docket_id)
WHERE d.agency_code = 'EPA'
ORDER BY doc.comment_end_date DESC NULLS LAST
LIMIT 20;
```

```sql
SELECT docket_id, COUNT(*) AS comment_count
FROM comments
GROUP BY docket_id
ORDER BY comment_count DESC
LIMIT 20;
```

## Duplicate-detection helper

For cross-agency duplicate or coordinated rulemaking, use:

```bash
uv run --script plugins/spicyregs/skills/spicyregs/scripts/find_duplicate_regulations.py --source r2 --limit 20
```

Useful flags:

- `--min-year 2020` to focus on recent activity
- `--min-agencies 3` to require broader cross-agency overlap
- `--format json` or `--format csv` for downstream analysis
- `--include-boilerplate` if you intentionally want administrative repeat families like information collections

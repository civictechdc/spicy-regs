# Spicy Regs Data Layout

Use this reference when you need a quick map of the remote or local data files before issuing SQL.

## Common locations

- Default public R2 bucket: `https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev`
- Default local parquet directory: `./spicy-regs-data/`
- Sample JSON for offline examples: `sample-data/mirrulations/`

## Expected parquet files

- `dockets.parquet`
- `documents.parquet`
- `comments.parquet`
- `comments_index.parquet`
- `feed_summary.parquet`
- Local-only: `comments/` partitioned comment parquet files may exist instead of a monolithic `comments.parquet`

## Practical schema hints

These fields are stable enough to start with, but use `--describe` for the actual schema.

### dockets

- `docket_id`
- `agency_code`
- `title`
- `docket_type`
- `modify_date`
- `abstract`

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

### comments

- `comment_id`
- `docket_id`
- `agency_code`
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

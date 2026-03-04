# ETL Pipeline

Converts Mirrulations S3 JSON data to Parquet files on Cloudflare R2.

```text
Step 1 (parallel)            Step 2                Step 3        Step 4         Step 5
┌──────────────────┐
│ load_manifest    │
├──────────────────┤         ┌───────────────┐    ┌──────────┐  ┌────────────┐  ┌─────────────┐
│ download_parquet │───────▶ │ process_agency│──▶ │  merge   │─▶│save_manifest│─▶│upload_to_r2 │
├──────────────────┤         │  × N agencies │    │ staging  │  └────────────┘  └─────────────┘
│ get_agencies     │         └───────────────┘    └──────────┘
└──────────────────┘
```

- **Step 1**: Load manifest, download existing parquet, discover agencies (parallel, skipped on `--full-refresh`)
- **Step 2**: For each agency: list S3 files → download JSONs → write staging parquet
- **Step 3**: Merge per-agency staging files into `dockets.parquet`, `documents.parquet`, `comments.parquet`
- **Step 4**: Save updated manifest with all processed S3 keys
- **Step 5**: Upload parquet files + manifest to R2 (parallel)

## Setup

```bash
uv sync
cp .env.example .env  # Then fill in your credentials
```

## Usage

### Full ETL (all agencies)
```bash
uv run etl --output-dir ./output
```

### Single agency
```bash
uv run etl --agency EPA --output-dir ./output
```

### Skip/isolate comments (large dataset)
```bash
uv run etl --skip-comments --output-dir ./output   # Dockets & documents only
uv run etl --only-comments --output-dir ./output   # Comments only
```

### Batched processing
```bash
uv run etl --batch-number 0 --batch-size 45 --output-dir ./output  # First 45 agencies
```

### Merge staging files only
```bash
uv run etl --merge-only --output-dir ./output
```

### CLI flags reference

| Flag | Description |
|---|---|
| `--agency AGENCY` | Process a single agency |
| `--output-dir DIR` | Output directory (default: temp dir) |
| `--skip-upload` | Skip uploading to R2 |
| `--full-refresh` | Ignore manifest, reprocess everything |
| `--skip-comments` | Process dockets and documents only |
| `--only-comments` | Process comments only |
| `--batch-number N` | Batch index for batched runs (0-indexed) |
| `--batch-size N` | Agencies per batch (default: 45) |
| `--verbose, -v` | Verbose logging |
| `--merge-only` | Only merge staging files |
| `--upload-only` | Only upload to R2 |
| `--partition-only` | Partition comments by agency and upload |

## Data Schema

All columns are stored as strings (`large_string` in PyArrow).

### Dockets (~393k rows)

| Column | Description |
|---|---|
| `docket_id` | Unique docket identifier |
| `agency_code` | Agency abbreviation (e.g., `EPA`, `FDA`) |
| `title` | Docket title |
| `docket_type` | Type of docket (e.g., `Rulemaking`, `Nonrulemaking`) |
| `modify_date` | Last modification date (ISO 8601) |
| `abstract` | Docket description/abstract |

### Documents (~2.1M rows)

| Column | Description |
|---|---|
| `document_id` | Unique document identifier |
| `docket_id` | Parent docket ID |
| `agency_code` | Agency abbreviation |
| `title` | Document title |
| `document_type` | Type (e.g., `Rule`, `Proposed Rule`, `Notice`) |
| `posted_date` | Date posted (ISO 8601) |
| `modify_date` | Last modification date |
| `comment_start_date` | Comment period start |
| `comment_end_date` | Comment period end |
| `file_url` | URL to the original document file |

### Comments (~31.6M rows)

| Column | Description |
|---|---|
| `comment_id` | Unique comment identifier |
| `docket_id` | Parent docket ID |
| `agency_code` | Agency abbreviation |
| `title` | Commenter name/title |
| `comment` | Full comment text body |
| `document_type` | Type of parent document |
| `posted_date` | Date posted (ISO 8601) |
| `modify_date` | Last modification date |
| `receive_date` | Date received |

## Output Structure on R2

```
s3://spicy-regs/
├── dockets.parquet                           # Single file, sorted by agency_code, modify_date
├── documents.parquet                         # Single file, sorted by agency_code, posted_date
├── comments.parquet                          # Flat file (used for full-scan analytics)
├── comments/agency/                          # Hive-partitioned by agency, sorted within each
│   ├── agency_code=EPA/part-0.parquet
│   ├── agency_code=FDA/part-0.parquet
│   └── ...
├── manifest.parquet                          # Tracks processed S3 keys
├── statistics.json                           # Pre-computed analytics
├── campaigns.json
├── organizations.json
├── agency_activity.json
├── comment_trends.json
├── cross_agency.json
└── frequent_commenters.json
```

### Optimization Details

| Dataset | Compression | Sort Order | Row Group Size | Partitioning |
|---|---|---|---|---|
| Dockets | zstd | `agency_code`, `modify_date` | 100,000 rows | None |
| Documents | zstd | `agency_code`, `posted_date` | 100,000 rows | None |
| Comments | zstd | `docket_id`, `posted_date` | 500,000 rows | Hive by `agency_code` |

**Why these settings?**

- **zstd compression**: Best ratio for text-heavy data
- **Sorting**: Clusters related data for better compression and enables min/max statistics skipping
- **Row group sizes**: Balance metadata overhead vs. scan granularity (~128MB target per group)
- **Hive partitioning (comments)**: Partitioned by `agency_code` so DuckDB reads only the relevant agency's file when querying by docket_id

### Querying the Optimized Files

```sql
-- Dockets: statistics skipping on sorted agency_code
SELECT * FROM read_parquet('https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev/dockets.parquet')
WHERE agency_code = 'EPA';

-- Comments: agency-partitioned query (reads only EPA partition)
SELECT * FROM read_parquet(
  'https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev/comments/agency/agency_code=EPA/part-0.parquet'
) WHERE docket_id = 'EPA-HQ-OAR-2021-0317';
```

## Architecture

1. **Init (parallel)**: Load manifest, download existing parquet from R2, discover agencies from S3
2. **Process agencies**: For each agency, list S3 JSON files, download and parse, write staging parquet
3. **Merge**: Streams staging files into aggregate output using PyArrow (memory-efficient)
4. **Upload**: Saves manifest and uploads final parquet files to R2 (parallel)

### Incremental Updates

The `manifest.parquet` file tracks every S3 key successfully processed. On subsequent runs, the script skips already-processed files. The manifest is synced to R2 so CI runs can resume from where they left off.

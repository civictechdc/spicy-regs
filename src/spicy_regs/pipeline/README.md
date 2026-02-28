# ETL Pipeline

Converts Mirrulations S3 JSON data to optimized Parquet files on Cloudflare R2.

## Setup

```bash
cd etl
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env  # Then fill in your credentials
```

## Usage

### Full ETL (all agencies)
```bash
python etl.py --output-dir ./output
```

### Single agency
```bash
python etl.py --agency EPA --output-dir ./output
```

### Skip/isolate comments (large dataset)
```bash
python etl.py --skip-comments --output-dir ./output   # Dockets & documents only
python etl.py --only-comments --output-dir ./output   # Comments only
```

### Batched processing
```bash
python etl.py --batch-number 0 --batch-size 45 --output-dir ./output  # First 45 agencies
```

### Merge staging files only
```bash
python etl.py --merge-only --output-dir ./output
```

### Optimize Parquet files for read performance
```bash
python etl.py --optimize-only --output-dir ./output            # Optimize only (no ETL)
python etl.py --optimize --output-dir ./output                 # Full ETL + optimize
python etl.py --optimize-only --skip-upload --output-dir ./output  # Optimize without uploading
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
| `--workers N` | Parallel download workers per agency (default: 10) |
| `--parallel-agencies N` | Concurrent agency processing (default: 5) |
| `--batch-number N` | Batch index for batched runs (0-indexed) |
| `--batch-size N` | Agencies per batch (default: 45) |
| `--verbose, -v` | Verbose logging |
| `--merge-only` | Only merge staging files |
| `--optimize` | Run Parquet optimization after ETL merge |
| `--optimize-only` | Only optimize existing Parquet files (skip ETL) |

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
├── comments.parquet                          # Flat file (legacy, used by frontend fallback)
├── comments_optimized/                       # Hive-partitioned, sorted within each partition
│   ├── year=2000/part-0.parquet
│   ├── year=2001/part-0.parquet
│   ├── ...
│   ├── year=2026/part-0.parquet
│   └── year=other/part-0.parquet             # Dirty/null dates
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
| Comments | zstd | `agency_code`, `docket_id`, `posted_date` | 500,000 rows | Hive by `year` |

**Why these settings?**

- **zstd compression**: Best ratio for text-heavy data
- **Sorting**: Clusters related data for better compression and enables min/max statistics skipping
- **Row group sizes**: Balance metadata overhead vs. scan granularity (~128MB target per group)
- **Hive partitioning (comments)**: Allows DuckDB to skip entire year partitions when filtering by date

### Querying the Optimized Files

```sql
-- Dockets: statistics skipping on sorted agency_code
SELECT * FROM read_parquet('https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev/dockets.parquet')
WHERE agency_code = 'EPA';

-- Comments: Hive-partitioned query (reads only year=2024 partition)
SELECT * FROM read_parquet(
  'https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev/comments_optimized/year=2024/part-0.parquet'
) WHERE agency_code = 'EPA';
```

## Architecture

1. **S3 ingestion**: Lists and downloads JSON files from the public `mirrulations` S3 bucket via boto3
2. **Staging**: Writes per-agency Parquet files to `output/staging/{data_type}/{AGENCY}.parquet`
3. **Merge**: Streams staging files into aggregate output using PyArrow (memory-efficient)
4. **Optimize** (optional): Sorts, tunes row groups, and Hive-partitions the merged files
5. **Upload**: Sends final files to Cloudflare R2

### Incremental Updates

The `manifest.parquet` file tracks every S3 key successfully processed. On subsequent runs, the script skips already-processed files. The manifest is synced to R2 so CI runs can resume from where they left off.

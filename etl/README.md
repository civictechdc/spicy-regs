# ETL Pipeline

Converts Mirrulations S3 JSON data to Parquet files on Cloudflare R2.

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
python etl.py
```

### Single agency
```bash
python etl.py --agency EPA
```

### Incremental update (changed files only)
```bash
python etl.py --incremental
```

## Output

Parquet files are uploaded to R2 with this structure:
```
/dockets.parquet
/documents.parquet  
/comments.parquet
```

Each file is partitioned by `agency_code` for efficient filtering.

#!/usr/bin/env python3
"""
ETL Pipeline: Mirrulations S3 → Parquet on R2

Extracts JSON data from the Mirrulations public S3 bucket,
transforms it to flattened Parquet files, and uploads to R2.
"""

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from tqdm import tqdm

from upload_r2 import upload_to_r2

load_dotenv()

# Mirrulations S3 bucket (public, no auth needed)
MIRRULATIONS_BUCKET = "s3://mirrulations/raw-data"

# Data type configurations
DATA_TYPES = {
    "dockets": {
        "path_pattern": "docket/*.json",
        "fields": """
            data.id as docket_id,
            data.attributes.agencyId as agency_code,
            data.attributes.title as title,
            data.attributes.docketType as docket_type,
            data.attributes.modifyDate as modify_date,
            data.attributes.dkAbstract as abstract
        """,
    },
    "documents": {
        "path_pattern": "documents/*.json",
        "fields": """
            data.id as document_id,
            data.attributes.docketId as docket_id,
            data.attributes.agencyId as agency_code,
            data.attributes.title as title,
            data.attributes.documentType as document_type,
            data.attributes.postedDate as posted_date,
            data.attributes.modifyDate as modify_date,
            data.attributes.commentStartDate as comment_start_date,
            data.attributes.commentEndDate as comment_end_date
        """,
    },
    "comments": {
        "path_pattern": "comments/*.json",
        "fields": """
            data.id as comment_id,
            data.attributes.docketId as docket_id,
            data.attributes.agencyId as agency_code,
            data.attributes.title as title,
            data.attributes.comment as comment,
            data.attributes.documentType as document_type,
            data.attributes.postedDate as posted_date,
            data.attributes.modifyDate as modify_date,
            data.attributes.receiveDate as receive_date
        """,
    },
}


def get_agencies(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Get list of all agencies from S3 bucket."""
    print("Fetching agency list...")
    result = conn.execute(f"""
        SELECT DISTINCT split_part(filename, '/', 4) as agency
        FROM glob('{MIRRULATIONS_BUCKET}/*/')
        ORDER BY agency
    """).fetchall()
    agencies = [row[0] for row in result if row[0]]
    print(f"Found {len(agencies)} agencies")
    return agencies


def process_data_type(
    conn: duckdb.DuckDBPyConnection,
    data_type: str,
    agencies: list[str],
    output_dir: Path,
) -> Path:
    """Process a single data type (dockets/documents/comments) for given agencies."""
    config = DATA_TYPES[data_type]
    output_file = output_dir / f"{data_type}.parquet"

    print(f"\n{'='*60}")
    print(f"Processing {data_type}...")
    print(f"{'='*60}")

    all_paths = []
    for agency in agencies:
        path = f"{MIRRULATIONS_BUCKET}/{agency}/**/text-*/{config['path_pattern']}"
        all_paths.append(path)
    
    if not all_paths:
        print(f"No agencies to process for {data_type}")
        return output_file

    # Use a single query with all paths
    paths_list = ", ".join([f"'{p}'" for p in all_paths])
    
    query = f"""
        SELECT
            {config['fields']},
            filename
        FROM read_json(
            [{paths_list}],
            union_by_name=true,
            ignore_errors=true,
            maximum_object_size=10485760
        )
    """

    try:
        print(f"  Executing query for {len(agencies)} agencies...")
        conn.execute(f"""
            COPY ({query}) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        
        # Get row count
        count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_file}')"
        ).fetchone()[0]
        print(f"  ✓ Total rows: {count:,}")
    except Exception as e:
        print(f"  Error: {e}")

    return output_file


def main():
    parser = argparse.ArgumentParser(description="Mirrulations ETL Pipeline")
    parser.add_argument(
        "--agency", help="Process only this agency (for testing)", default=None
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only process changed files (not yet implemented)",
    )
    parser.add_argument(
        "--output-dir",
        help="Local output directory for Parquet files",
        default=None,
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip uploading to R2 (for testing)",
    )
    args = parser.parse_args()

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="spicy-regs-etl-"))

    print(f"Output directory: {output_dir}")

    # Initialize DuckDB with httpfs for S3 access
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET s3_region='us-east-1';")
    # Mirrulations bucket is public, no auth needed
    conn.execute("SET s3_url_style='path';")

    # Get agencies to process
    if args.agency:
        agencies = [args.agency]
    elif os.getenv("AGENCIES"):
        agencies = os.getenv("AGENCIES").split(",")
    else:
        agencies = get_agencies(conn)

    print(f"\nProcessing {len(agencies)} agencies")
    start_time = datetime.now()

    # Process each data type with progress bar
    parquet_files = []
    for data_type in tqdm(DATA_TYPES, desc="Data types", unit="type"):
        output_file = process_data_type(conn, data_type, agencies, output_dir)
        if output_file.exists():
            parquet_files.append(output_file)

    # Upload to R2
    if not args.skip_upload and parquet_files:
        print("\n" + "=" * 60)
        print("Uploading to R2...")
        print("=" * 60)
        for pf in parquet_files:
            upload_to_r2(pf)

    elapsed = datetime.now() - start_time
    print(f"\n{'='*60}")
    print(f"ETL completed in {elapsed}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

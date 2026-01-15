#!/usr/bin/env python3
"""
ETL Pipeline: Mirrulations S3 → Parquet on R2

Uses boto3 for S3 file listing, Polars for fast data processing,
and writes Parquet directly. Processes one agency at a time with
incremental append and resume support.
"""

import argparse
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3
import polars as pl
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv
from tqdm import tqdm

from upload_r2 import upload_to_r2, download_from_r2

load_dotenv()

# S3 client for public bucket
S3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
BUCKET = 'mirrulations'
PREFIX = 'raw-data'

# Data type configurations with schemas
DATA_TYPES = {
    "dockets": {
        "path_pattern": "/docket/",
        "schema": {
            "docket_id": pl.Utf8,
            "agency_code": pl.Utf8,
            "title": pl.Utf8,
            "docket_type": pl.Utf8,
            "modify_date": pl.Utf8,
            "abstract": pl.Utf8,
        },
        "extract": lambda d: {
            "docket_id": d.get("data", {}).get("id"),
            "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
            "title": d.get("data", {}).get("attributes", {}).get("title"),
            "docket_type": d.get("data", {}).get("attributes", {}).get("docketType"),
            "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
            "abstract": d.get("data", {}).get("attributes", {}).get("dkAbstract"),
        },
    },
    "documents": {
        "path_pattern": "/documents/",
        "schema": {
            "document_id": pl.Utf8,
            "docket_id": pl.Utf8,
            "agency_code": pl.Utf8,
            "title": pl.Utf8,
            "document_type": pl.Utf8,
            "posted_date": pl.Utf8,
            "modify_date": pl.Utf8,
            "comment_start_date": pl.Utf8,
            "comment_end_date": pl.Utf8,
        },
        "extract": lambda d: {
            "document_id": d.get("data", {}).get("id"),
            "docket_id": d.get("data", {}).get("attributes", {}).get("docketId"),
            "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
            "title": d.get("data", {}).get("attributes", {}).get("title"),
            "document_type": d.get("data", {}).get("attributes", {}).get("documentType"),
            "posted_date": d.get("data", {}).get("attributes", {}).get("postedDate"),
            "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
            "comment_start_date": d.get("data", {}).get("attributes", {}).get("commentStartDate"),
            "comment_end_date": d.get("data", {}).get("attributes", {}).get("commentEndDate"),
        },
    },
    "comments": {
        "path_pattern": "/comments/",
        "schema": {
            "comment_id": pl.Utf8,
            "docket_id": pl.Utf8,
            "agency_code": pl.Utf8,
            "title": pl.Utf8,
            "comment": pl.Utf8,
            "document_type": pl.Utf8,
            "posted_date": pl.Utf8,
            "modify_date": pl.Utf8,
            "receive_date": pl.Utf8,
        },
        "extract": lambda d: {
            "comment_id": d.get("data", {}).get("id"),
            "docket_id": d.get("data", {}).get("attributes", {}).get("docketId"),
            "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
            "title": d.get("data", {}).get("attributes", {}).get("title"),
            "comment": d.get("data", {}).get("attributes", {}).get("comment"),
            "document_type": d.get("data", {}).get("attributes", {}).get("documentType"),
            "posted_date": d.get("data", {}).get("attributes", {}).get("postedDate"),
            "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
            "receive_date": d.get("data", {}).get("attributes", {}).get("receiveDate"),
        },
    },
}


def get_agencies() -> list[str]:
    """Get list of all agencies from S3 bucket."""
    response = S3.list_objects_v2(Bucket=BUCKET, Prefix=f'{PREFIX}/', Delimiter='/')
    agencies = []
    for prefix in response.get('CommonPrefixes', []):
        agency = prefix['Prefix'].split('/')[1]
        if agency:
            agencies.append(agency)
    return sorted(agencies)


def load_manifest(output_dir: Path) -> set[str]:
    """Load processed keys from manifest Parquet file."""
    manifest_file = output_dir / "manifest.parquet"
    
    # Try local first
    if manifest_file.exists():
        df = pl.read_parquet(manifest_file)
        keys = set(df["key"].to_list())
        print(f"Loaded manifest: {len(keys):,} processed keys")
        return keys
    
    # Try downloading from R2
    if download_from_r2("manifest.parquet", manifest_file):
        df = pl.read_parquet(manifest_file)
        keys = set(df["key"].to_list())
        print(f"Downloaded manifest from R2: {len(keys):,} processed keys")
        return keys
    
    print("No manifest found, starting fresh")
    return set()


def save_manifest(output_dir: Path, processed_keys: set[str]):
    """Save processed keys to manifest Parquet file."""
    manifest_file = output_dir / "manifest.parquet"
    df = pl.DataFrame({"key": list(processed_keys)})
    df.write_parquet(manifest_file, compression="zstd")
    print(f"Saved manifest: {len(processed_keys):,} keys")


def list_json_files(agency: str, data_type: str, processed_keys: set[str] = None) -> list[str]:
    """List all JSON files for an agency and data type, excluding already processed."""
    config = DATA_TYPES[data_type]
    pattern = config["path_pattern"]
    
    files = []
    paginator = S3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f'{PREFIX}/{agency}/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if '/text-' in key and pattern in key and key.endswith('.json'):
                # Skip if already processed
                if processed_keys and key in processed_keys:
                    continue
                files.append(key)
    
    return files


def download_and_parse(key: str, data_type: str) -> dict | None:
    """Download and parse a single JSON file."""
    try:
        response = S3.get_object(Bucket=BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return DATA_TYPES[data_type]["extract"](data)
    except Exception:
        return None


def process_agency(agency: str, output_dir: Path, processed_keys: set[str], max_workers: int = 10, skip_comments: bool = False, only_comments: bool = False) -> tuple[dict[str, int], list[str]]:
    """Process all data types for a single agency. Returns (results, new_keys)."""
    results = {}
    new_keys = []
    
    for data_type, config in DATA_TYPES.items():
        if skip_comments and data_type == "comments":
            continue
        if only_comments and data_type != "comments":
            continue
        output_file = output_dir / f"{data_type}.parquet"
        
        # List files (filtering already processed)
        files = list_json_files(agency, data_type, processed_keys)
        if not files:
            results[data_type] = 0
            continue
        
        tqdm.write(f"  [{agency}] {data_type}: {len(files)} new files, downloading...")
        
        # Download and parse in parallel
        records = []
        failed = 0
        successful_keys = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(download_and_parse, f, data_type): f for f in files}
            for future in as_completed(futures):
                key = futures[future]
                result = future.result()
                if result and result.get(list(result.keys())[0]):
                    records.append(result)
                    successful_keys.append(key)
                else:
                    failed += 1
        
        if not records:
            tqdm.write(f"  [{agency}] {data_type}: no valid records")
            results[data_type] = 0
            continue
        
        # Create Polars DataFrame with explicit schema
        schema = config["schema"]
        df = pl.DataFrame(records, schema=schema)
        
        # Append to existing Parquet or create new
        if output_file.exists():
            existing = pl.read_parquet(output_file)
            combined = pl.concat([existing, df])
            combined.write_parquet(output_file, compression="zstd")
            tqdm.write(f"  [{agency}] {data_type}: ✓ {len(records)} rows added (total: {len(combined):,})")
        else:
            df.write_parquet(output_file, compression="zstd")
            tqdm.write(f"  [{agency}] {data_type}: ✓ {len(records)} rows (new file)")
        
        results[data_type] = len(records)
        new_keys.extend(successful_keys)
    
    return results, new_keys


def get_processed_agencies(output_dir: Path, skip_comments: bool = False, only_comments: bool = False) -> set[str]:
    """Get agencies already processed by checking existing Parquet files."""
    processed_per_type = []
    
    for data_type in DATA_TYPES:
        # Only check data types we're actually processing
        if skip_comments and data_type == "comments":
            continue
        if only_comments and data_type != "comments":
            continue
            
        parquet_file = output_dir / f"{data_type}.parquet"
        if parquet_file.exists():
            try:
                df = pl.read_parquet(parquet_file, columns=["agency_code"])
                processed_per_type.append(set(df["agency_code"].unique().to_list()))
            except Exception:
                processed_per_type.append(set())
        else:
            # File doesn't exist, no agencies processed yet
            return set()
    
    # Return agencies that are in ALL relevant data types
    if processed_per_type:
        return set.intersection(*processed_per_type)
    return set()


def main():
    parser = argparse.ArgumentParser(description="Mirrulations ETL Pipeline")
    parser.add_argument("--agency", help="Process only this agency", default=None)
    parser.add_argument("--output-dir", help="Output directory", default=None)
    parser.add_argument("--skip-upload", action="store_true", help="Skip R2 upload")
    parser.add_argument("--full-refresh", action="store_true", help="Full refresh (ignore manifest)")
    parser.add_argument("--skip-comments", action="store_true", help="Skip comments (process dockets/documents only)")
    parser.add_argument("--only-comments", action="store_true", help="Only process comments")
    parser.add_argument("--workers", type=int, default=10, help="Parallel download workers")
    args = parser.parse_args()

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="spicy-regs-etl-"))

    print(f"Output directory: {output_dir}")
    print(f"Workers: {args.workers}")

    # Load manifest (unless full refresh)
    if args.full_refresh:
        print("Full refresh mode - ignoring manifest")
        processed_keys = set()
    else:
        processed_keys = load_manifest(output_dir)

    # Get agencies to process
    if args.agency:
        agencies = [args.agency]
    elif os.getenv("AGENCIES"):
        agencies = os.getenv("AGENCIES").split(",")
    else:
        print("Fetching agency list...")
        agencies = get_agencies()
        print(f"Found {len(agencies)} agencies")

    if not agencies:
        print("No agencies to process!")
        return

    print(f"\nProcessing {len(agencies)} agencies")
    start_time = datetime.now()

    # Process each agency
    total_rows = {dt: 0 for dt in DATA_TYPES}
    all_new_keys = []
    
    for agency in tqdm(agencies, desc="Agencies", unit="agency"):
        results, new_keys = process_agency(
            agency, output_dir, processed_keys,
            args.workers, args.skip_comments, args.only_comments
        )
        for dt, count in results.items():
            total_rows[dt] += count
        all_new_keys.extend(new_keys)
        # Update processed_keys so subsequent agencies see them
        processed_keys.update(new_keys)

    # Summary
    print(f"\n{'='*60}")
    print("Summary:")
    for dt, count in total_rows.items():
        print(f"  {dt}: {count:,} rows")
    print(f"  New files processed: {len(all_new_keys):,}")
    
    elapsed = datetime.now() - start_time
    print(f"\nETL completed in {elapsed}")

    # Save manifest
    if all_new_keys:
        save_manifest(output_dir, processed_keys)

    # Upload to R2
    if not args.skip_upload:
        print(f"\n{'='*60}")
        print("Uploading to R2...")
        for data_type in DATA_TYPES:
            pf = output_dir / f"{data_type}.parquet"
            if pf.exists():
                upload_to_r2(pf)
        # Upload manifest too
        manifest_file = output_dir / "manifest.parquet"
        if manifest_file.exists():
            upload_to_r2(manifest_file)

    print("Done!")


if __name__ == "__main__":
    main()

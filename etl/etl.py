#!/usr/bin/env python3
"""
ETL Pipeline: Mirrulations S3 → Parquet on R2

Uses boto3 for S3 file listing, Polars for fast data processing,
and writes Parquet directly. Processes one agency at a time with
incremental append and resume support.

Memory-Optimized: Uses staging files per agency and streams the
final merge using PyArrow to avoid loading entire datasets into memory.
"""

import argparse
import json
import os
import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv
from tqdm import tqdm

from upload_r2 import upload_to_r2
from download_r2 import download_from_r2

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
            "file_url": pl.Utf8,
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
            "file_url": (d.get("data", {}).get("attributes", {}).get("fileFormats") or [{}])[0].get("fileUrl"),
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


def list_json_files(agency: str, data_type: str, processed_keys: set[str] = None, verbose: bool = False) -> list[str]:
    """List all JSON files for an agency and data type, excluding already processed."""
    config = DATA_TYPES[data_type]
    pattern = config["path_pattern"]
    
    files = []
    skipped = 0
    total_scanned = 0
    paginator = S3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f'{PREFIX}/{agency}/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if '/text-' in key and pattern in key and key.endswith('.json'):
                total_scanned += 1
                # Skip if already processed
                if processed_keys and key in processed_keys:
                    skipped += 1
                    continue
                files.append(key)
    
    if verbose:
        tqdm.write(f"    [{agency}] {data_type}: scanned {total_scanned}, skipped {skipped}, new {len(files)}")
    
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


def process_agency(
    agency: str,
    staging_dir: Path,
    processed_keys: set[str],
    max_workers: int = 10,
    skip_comments: bool = False,
    only_comments: bool = False,
    verbose: bool = False
) -> tuple[dict[str, int], list[str]]:
    """
    Process all data types for a single agency.
    Writes to staging directory (one file per agency per data type).
    Returns (results, new_keys).
    """
    results = {}
    new_keys = []
    
    for data_type, config in DATA_TYPES.items():
        if skip_comments and data_type == "comments":
            continue
        if only_comments and data_type != "comments":
            continue
        
        # Staging file for this agency/data_type
        staging_type_dir = staging_dir / data_type
        staging_type_dir.mkdir(parents=True, exist_ok=True)
        staging_file = staging_type_dir / f"{agency}.parquet"
        
        # List files (filtering already processed)
        files = list_json_files(agency, data_type, processed_keys, verbose)
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
        
        # Create Polars DataFrame with explicit schema and write to staging
        schema = config["schema"]
        df = pl.DataFrame(records, schema=schema)
        df.write_parquet(staging_file, compression="zstd")
        
        tqdm.write(f"  [{agency}] {data_type}: ✓ {len(records)} rows -> staging")
        
        results[data_type] = len(records)
        new_keys.extend(successful_keys)
    
    return results, new_keys


def merge_staging_files(staging_dir: Path, output_dir: Path, data_types_to_merge: list[str]):
    """
    Merge staging files into final output using PyArrow streaming.
    This processes one file at a time to minimize memory usage.
    Handles schema evolution by using target schema from DATA_TYPES.
    """
    for data_type in data_types_to_merge:
        staging_type_dir = staging_dir / data_type
        output_file = output_dir / f"{data_type}.parquet"
        
        if not staging_type_dir.exists():
            continue
        
        staging_files = list(staging_type_dir.glob("*.parquet"))
        if not staging_files:
            continue
        
        print(f"Merging {len(staging_files)} staging files for {data_type}...")
        
        # Collect files to merge (existing + staging)
        files_to_merge = []
        
        # Add existing output file if it exists
        if output_file.exists():
            files_to_merge.append(output_file)
        
        files_to_merge.extend(staging_files)
        
        if len(files_to_merge) == 1 and not output_file.exists():
            # Just one staging file, move it directly
            shutil.move(files_to_merge[0], output_file)
            print(f"  {data_type}: moved single file")
            continue
        
        # Stream merge using PyArrow (memory efficient)
        temp_output = output_dir / f"{data_type}_merged.parquet"
        
        # Use target schema from DATA_TYPES (handles schema evolution)
        target_columns = list(DATA_TYPES[data_type]["schema"].keys())
        target_schema = pa.schema([(col, pa.large_string()) for col in target_columns])
        
        total_rows = 0
        with pq.ParquetWriter(temp_output, target_schema, compression='zstd') as writer:
            for file_path in files_to_merge:
                table = pq.read_table(file_path)
                
                # Handle schema evolution - add missing columns with nulls
                existing_cols = set(table.column_names)
                for col in target_columns:
                    if col not in existing_cols:
                        null_array = pa.nulls(table.num_rows, type=pa.large_string())
                        table = table.append_column(col, null_array)
                
                # Select only target columns in order
                table = table.select(target_columns)
                
                writer.write_table(table)
                total_rows += table.num_rows
                # Free memory immediately
                del table
        
        # Replace original with merged
        if output_file.exists():
            output_file.unlink()
        temp_output.rename(output_file)
        
        print(f"  {data_type}: ✓ merged {total_rows:,} total rows")


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
    parser.add_argument("--parallel-agencies", type=int, default=5, help="Parallel agency processing")
    parser.add_argument("--batch-number", type=int, default=None, help="Batch number (0-indexed) for batched processing")
    parser.add_argument("--batch-size", type=int, default=45, help="Number of agencies per batch (default: 45)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose debug logging")
    parser.add_argument("--merge-only", action="store_true", help="Only merge staging files (skip downloading)")
    args = parser.parse_args()

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="spicy-regs-etl-"))

    # Setup staging directory
    staging_dir = output_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}")
    print(f"Staging directory: {staging_dir}")

    # Merge-only mode: just merge staging files and exit
    if args.merge_only:
        print("Merge-only mode - merging existing staging files...")
        data_types_to_process = ["dockets", "documents", "comments"]
        if args.skip_comments:
            data_types_to_process = ["dockets", "documents"]
        elif args.only_comments:
            data_types_to_process = ["comments"]
        merge_staging_files(staging_dir, output_dir, data_types_to_process)
        print("Merge complete!")
        return

    print(f"Workers: {args.workers}")

    # Load manifest (unless full refresh)
    if args.full_refresh:
        print("Full refresh mode - ignoring manifest")
        processed_keys = set()
    else:
        processed_keys = load_manifest(output_dir)
        
        # Download existing Parquet files from R2 for incremental append
        if processed_keys:
            print("Downloading existing Parquet files from R2...")
            for data_type in DATA_TYPES:
                local_file = output_dir / f"{data_type}.parquet"
                if not local_file.exists():
                    if download_from_r2(f"{data_type}.parquet", local_file):
                        size_mb = local_file.stat().st_size / (1024 * 1024)
                        print(f"  ✓ {data_type}.parquet ({size_mb:.1f} MB)")
                    else:
                        print(f"  ⚠ {data_type}.parquet not found in R2")

    # Get agencies to process
    if args.agency:
        agencies = [args.agency]
    elif os.getenv("AGENCIES"):
        agencies = os.getenv("AGENCIES").split(",")
    else:
        print("Fetching agency list...")
        agencies = get_agencies()
        print(f"Found {len(agencies)} agencies")

    # Apply batch filtering if specified
    if args.batch_number is not None:
        start_idx = args.batch_number * args.batch_size
        end_idx = start_idx + args.batch_size
        agencies = agencies[start_idx:end_idx]
        print(f"Batch {args.batch_number}: agencies {start_idx}-{min(end_idx, start_idx + len(agencies))-1} ({len(agencies)} agencies)")

    if not agencies:
        print("No agencies to process!")
        return

    print(f"\nProcessing {len(agencies)} agencies")
    start_time = datetime.now()

    # Determine which data types we're processing
    data_types_to_process = []
    for dt in DATA_TYPES:
        if args.skip_comments and dt == "comments":
            continue
        if args.only_comments and dt != "comments":
            continue
        data_types_to_process.append(dt)

    # Process agencies in parallel (writes to staging, not final output)
    total_rows = {dt: 0 for dt in DATA_TYPES}
    all_new_keys = []
    keys_lock = threading.Lock()
    
    def process_agency_wrapper(agency):
        results, new_keys = process_agency(
            agency, staging_dir, processed_keys,
            args.workers, args.skip_comments, args.only_comments,
            args.verbose
        )
        with keys_lock:
            for dt, count in results.items():
                total_rows[dt] += count
            all_new_keys.extend(new_keys)
            processed_keys.update(new_keys)
        return agency, len(new_keys)
    
    with ThreadPoolExecutor(max_workers=args.parallel_agencies) as executor:
        futures = {executor.submit(process_agency_wrapper, a): a for a in agencies}
        with tqdm(total=len(agencies), desc="Agencies", unit="agency") as pbar:
            for future in as_completed(futures):
                agency, new_count = future.result()
                pbar.update(1)
                if new_count > 0:
                    pbar.set_postfix({"last": agency, "new": new_count})

    # Merge staging files into final output (streaming, memory-efficient)
    if any(total_rows.values()):
        print(f"\n{'='*60}")
        print("Merging staging files...")
        merge_staging_files(staging_dir, output_dir, data_types_to_process)
        
        # Clean up staging directory
        shutil.rmtree(staging_dir)
        print("Cleaned up staging directory")

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

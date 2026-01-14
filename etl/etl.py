#!/usr/bin/env python3
"""
ETL Pipeline: Mirrulations S3 → Parquet on R2

Memory-efficient version that processes one agency at a time,
appending to Parquet files incrementally. Supports resuming
from where it left off.
"""

import argparse
import os
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
        "id_field": "docket_id",
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
        "id_field": "document_id",
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
        "id_field": "comment_id",
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


def get_agencies() -> list[str]:
    """Get list of all agencies from S3 bucket."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
    response = s3.list_objects_v2(Bucket='mirrulations', Prefix='raw-data/', Delimiter='/')
    
    agencies = []
    for prefix in response.get('CommonPrefixes', []):
        agency = prefix['Prefix'].split('/')[1]
        if agency:
            agencies.append(agency)
    
    return sorted(agencies)


def get_processed_agencies(output_dir: Path) -> dict[str, set[str]]:
    """Get agencies already processed for each data type by checking existing Parquet files."""
    processed = {dt: set() for dt in DATA_TYPES}
    conn = duckdb.connect()
    
    for data_type in DATA_TYPES:
        parquet_file = output_dir / f"{data_type}.parquet"
        if parquet_file.exists():
            try:
                result = conn.execute(f"""
                    SELECT DISTINCT agency_code 
                    FROM read_parquet('{parquet_file}')
                """).fetchall()
                processed[data_type] = {row[0] for row in result if row[0]}
            except Exception:
                pass
    
    conn.close()
    return processed


def process_agency(
    conn: duckdb.DuckDBPyConnection,
    agency: str,
    output_dir: Path,
) -> dict[str, int]:
    """Process all data types for a single agency and append to Parquet files."""
    results = {}
    
    for data_type, config in DATA_TYPES.items():
        output_file = output_dir / f"{data_type}.parquet"
        path = f"{MIRRULATIONS_BUCKET}/{agency}/**/text-*/{config['path_pattern']}"
        
        query = f"""
            SELECT
                {config['fields']},
                filename
            FROM read_json(
                '{path}',
                union_by_name=true,
                ignore_errors=true,
                maximum_object_size=10485760
            )
        """
        
        try:
            # Fetch data for this agency
            df = conn.execute(query).fetchdf()
            row_count = len(df)
            
            if row_count > 0:
                if output_file.exists():
                    # Append to existing Parquet
                    conn.execute(f"""
                        COPY (
                            SELECT * FROM read_parquet('{output_file}')
                            UNION ALL
                            SELECT * FROM df
                        ) TO '{output_file}'
                        (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                else:
                    # Create new Parquet file
                    conn.execute(f"""
                        COPY (SELECT * FROM df) TO '{output_file}'
                        (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                
                results[data_type] = row_count
            else:
                results[data_type] = 0
                
        except Exception:
            results[data_type] = 0
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Mirrulations ETL Pipeline")
    parser.add_argument("--agency", help="Process only this agency", default=None)
    parser.add_argument("--output-dir", help="Output directory", default=None)
    parser.add_argument("--skip-upload", action="store_true", help="Skip R2 upload")
    parser.add_argument("--full-refresh", action="store_true", help="Full refresh (ignore previous progress)")
    args = parser.parse_args()

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="spicy-regs-etl-"))

    print(f"Output directory: {output_dir}")

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET s3_region='us-east-1';")
    conn.execute("SET s3_url_style='path';")

    # Get agencies to process
    if args.agency:
        agencies = [args.agency]
    elif os.getenv("AGENCIES"):
        agencies = os.getenv("AGENCIES").split(",")
    else:
        print("Fetching agency list...")
        agencies = get_agencies()
        print(f"Found {len(agencies)} agencies")

    # Check for already processed agencies (default: resume mode)
    if not args.full_refresh:
        processed = get_processed_agencies(output_dir)
        # Find agencies that are complete (in all data types)
        complete_agencies = set.intersection(*processed.values()) if processed.values() else set()
        remaining = [a for a in agencies if a not in complete_agencies]
        if complete_agencies:
            print(f"Resuming: {len(complete_agencies)} agencies already processed, {len(remaining)} remaining")
        agencies = remaining

    if not agencies:
        print("No agencies to process!")
        return

    print(f"\nProcessing {len(agencies)} agencies")
    start_time = datetime.now()

    # Process each agency
    total_rows = {dt: 0 for dt in DATA_TYPES}
    
    for agency in tqdm(agencies, desc="Agencies", unit="agency"):
        results = process_agency(conn, agency, output_dir)
        for dt, count in results.items():
            total_rows[dt] += count

    conn.close()

    # Summary
    print(f"\n{'='*60}")
    print("Summary:")
    for dt, count in total_rows.items():
        print(f"  {dt}: {count:,} rows")
    
    elapsed = datetime.now() - start_time
    print(f"\nETL completed in {elapsed}")

    # Upload to R2
    if not args.skip_upload:
        print(f"\n{'='*60}")
        print("Uploading to R2...")
        for data_type in DATA_TYPES:
            pf = output_dir / f"{data_type}.parquet"
            if pf.exists():
                upload_to_r2(pf)

    print("Done!")


if __name__ == "__main__":
    main()

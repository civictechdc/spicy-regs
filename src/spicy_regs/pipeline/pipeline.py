"""
Prefect-orchestrated ETL pipeline: Mirrulations S3 → Parquet on R2.

Top-level flow with per-agency subflows for extract → transform → load.
All configuration lives here and is passed down via function parameters.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from os import getenv
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from typing import Annotated

from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
import boto3
from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger
import polars as pl
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner

from spicy_regs.pipeline.extract import (
    download_and_parse,
    download_existing_parquet,
    get_agencies,
    list_json_files,
    load_manifest,
)
from spicy_regs.pipeline.load import save_manifest, upload_to_r2
from spicy_regs.pipeline.transform import merge_staging_files, write_staging

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MIRRULATIONS_BUCKET = "mirrulations"
S3_CLIENT = boto3.client(
    "s3",
    region_name="us-east-1",
    config=BotoConfig(signature_version=UNSIGNED),
)
S3_RESOURCE = boto3.resource(
    "s3",
    region_name="us-east-1",
    config=BotoConfig(signature_version=UNSIGNED),
)
PREFIX = "raw-data"

# Module-level state — kept out of Prefect task boundaries to avoid
# serializing a 27M-item set on every .submit() call.
PROCESSED_KEYS: set[str] = set()

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


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


@task(name="process-agency", task_run_name="process-{agency}", cache_policy=NO_CACHE)
def process_agency(
    agency: str,
    staging_dir: Path,
    data_type_names: list[str] | None = None,
    verbose: bool = False,
) -> tuple[dict[str, int], list[str]]:
    """Process a single agency: list files, download, parse, and write staging."""
    logger.info("[{}] Starting", agency)
    results: dict[str, int] = {}
    new_keys: list[str] = []

    types_to_process = data_type_names or list(DATA_TYPES.keys())

    # List files for all data types in parallel
    logger.info("[{}] Listing files for {} data types...", agency, len(types_to_process))
    keys_by_type: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=len(types_to_process)) as executor:
        futures = {
            executor.submit(
                list_json_files,
                S3_RESOURCE,
                MIRRULATIONS_BUCKET,
                PREFIX,
                agency,
                dt_name,
                DATA_TYPES[dt_name]["path_pattern"],
                PROCESSED_KEYS,
                verbose,
            ): dt_name
            for dt_name in types_to_process
        }
        for future in as_completed(futures):
            dt_name = futures[future]
            keys_by_type[dt_name] = future.result()

    # Download and write staging for each data type
    for dt_name in types_to_process:
        keys = keys_by_type[dt_name]
        dt_config = DATA_TYPES[dt_name]

        if not keys:
            logger.info("[{}] {}: no new files", agency, dt_name)
            results[dt_name] = 0
            continue

        logger.info("[{}] {}: downloading {} files...", agency, dt_name, len(keys))
        extract_fn = dt_config["extract"]

        with ThreadPoolExecutor(max_workers=10) as executor:
            records = [
                r for r in executor.map(
                    lambda key: download_and_parse(S3_RESOURCE, MIRRULATIONS_BUCKET, key, extract_fn),
                    keys,
                )
                if r is not None
            ]

        row_count = write_staging(agency, dt_name, records, staging_dir, dt_config["schema"])
        results[dt_name] = row_count
        new_keys.extend(keys)

        if verbose:
            logger.info("[{}] {}: {} rows written", agency, dt_name, row_count)

    return results, new_keys


@task(name="merge-staging", cache_policy=NO_CACHE)
def merge_staging_task(staging_dir: Path, output_dir: Path, data_type_names: list[str]) -> None:
    """Step 3: Merge per-agency staging files into final Parquet."""
    schemas = {dtype: DATA_TYPES[dtype]["schema"] for dtype in data_type_names}
    merge_staging_files(staging_dir, output_dir, data_type_names, schemas)


@task(name="save-manifest", cache_policy=NO_CACHE)
def save_manifest_task(output_dir: Path) -> None:
    """Step 4: Save manifest with all processed S3 keys."""
    save_manifest(output_dir, PROCESSED_KEYS)


@task(name="upload-to-r2", cache_policy=NO_CACHE)
def upload_to_r2_task(output_dir: Path, data_type_names: list[str]) -> None:
    """Step 5: Upload parquet files + manifest to R2."""
    upload_to_r2(output_dir, data_type_names)


app = App(name="spicy-regs-pipeline", help="Spicy Regs Mirrulations ETL Pipeline")


@app.default
@flow(name="spicy-regs-etl", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=3))
def pipeline(
    agency: Annotated[str | None, Parameter(help="Process only this agency")] = None,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload")] = False,
    full_refresh: Annotated[bool, Parameter(help="Full refresh (ignore manifest)")] = False,
    skip_comments: Annotated[bool, Parameter(help="Skip comments")] = False,
    only_comments: Annotated[bool, Parameter(help="Only process comments")] = False,
    batch_number: Annotated[int | None, Parameter(help="Batch number (0-indexed)")] = None,
    batch_size: Annotated[int, Parameter(help="Agencies per batch")] = 45,
    verbose: Annotated[bool, Parameter(name=["--verbose", "-v"], help="Verbose logging")] = False,
    merge_only: Annotated[bool, Parameter(help="Only merge staging files")] = False,
    upload_only: Annotated[bool, Parameter(help="Only upload to R2")] = False,
) -> None:
    """Mirrulations S3 → Parquet on R2."""

    # Setup directories
    if output_dir is None:
        output_dir = Path(mkdtemp(prefix="spicy-regs-etl-"))
    output_dir.mkdir(parents=True, exist_ok=True)

    staging_dir = output_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Output directory: {}", output_dir)
    logger.info("Staging directory: {}", staging_dir)

    # Determine which data types to process
    data_type_names: list[str] = list(DATA_TYPES.keys())
    if skip_comments:
        data_type_names = [dt for dt in DATA_TYPES if dt != "comments"]
    elif only_comments:
        data_type_names = [dt for dt in DATA_TYPES if dt == "comments"]

    # Upload-only mode
    if upload_only:
        logger.info("Upload-only mode - uploading to R2...")
        upload_to_r2_task(output_dir, data_type_names)
        logger.info("Upload complete!")
        return

    # Merge-only mode
    if merge_only:
        logger.info("Merge-only mode - merging existing staging files...")
        merge_staging_task(staging_dir, output_dir, data_type_names)
        logger.info("Merge complete!")
        return

    # --- Step 1: Load manifest, download existing data, discover agencies (parallel) ---
    global PROCESSED_KEYS
    if full_refresh:
        logger.info("Full refresh mode - ignoring manifest")
        PROCESSED_KEYS = set()
    else:
        manifest_future = load_manifest.submit(output_dir)
        parquet_future = download_existing_parquet.submit(output_dir, data_type_names)

    if agency is not None:
        agencies = [agency]
    elif (agencies_env := getenv("AGENCIES")) is not None:
        agencies = agencies_env.split(",")
    else:
        agencies_future = get_agencies.submit(S3_CLIENT, MIRRULATIONS_BUCKET, PREFIX)
        agencies = agencies_future.result()
        logger.info("Found {} agencies", len(agencies))

    if not full_refresh:
        manifest_future.wait()
        parquet_future.wait()

    # Batch filtering
    if batch_number is not None:
        start_idx = batch_number * batch_size
        end_idx = start_idx + batch_size
        agencies = agencies[start_idx:end_idx]
        logger.info(
            "Batch {}: agencies {}-{} ({} agencies)",
            batch_number,
            start_idx,
            min(end_idx, start_idx + len(agencies)) - 1,
            len(agencies),
        )

    if not agencies:
        logger.warning("No agencies to process!")
        return

    logger.info("Processing {} agencies", len(agencies))
    start_time = datetime.now(timezone.utc)

    # --- Step 2: Process each agency ---
    futures = [
        process_agency.submit(a, staging_dir, data_type_names, verbose)
        for a in agencies
    ]

    wait(futures)

    total_rows: dict[str, int] = {dt: 0 for dt in DATA_TYPES}
    all_new_keys: list[str] = []
    for future in futures:
        results, new_keys = future.result()
        for dt, count in results.items():
            total_rows[dt] += count
        all_new_keys.extend(new_keys)
        PROCESSED_KEYS.update(new_keys)

    # --- Step 3: Merge staging into final Parquet ---
    if any(total_rows.values()):
        logger.info("Merging staging files...")
        merge_staging_task(staging_dir, output_dir, data_type_names)
        rmtree(staging_dir)
        logger.info("Cleaned up staging directory")

    # --- Summary ---
    logger.info("Summary:")
    for dt, count in total_rows.items():
        logger.info("  {}: {:,} rows", dt, count)
    logger.info("  New files processed: {:,}", len(all_new_keys))
    elapsed = datetime.now(timezone.utc) - start_time
    logger.info("ETL completed in {}", elapsed)

    # --- Step 4: Save manifest ---
    if all_new_keys:
        save_manifest_task(output_dir)

    # --- Step 5: Upload to R2 ---
    if skip_upload is False:
        logger.info("Uploading to R2...")
        upload_to_r2_task(output_dir, data_type_names)

    logger.info("Done!")


if __name__ == "__main__":
    app()

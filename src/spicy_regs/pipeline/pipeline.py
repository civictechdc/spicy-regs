"""
Prefect-orchestrated ETL pipeline: Mirrulations S3 → Parquet on R2.

Top-level flow with per-agency subflows for extract → transform → load.
All configuration lives here and is passed down via function parameters.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from json import dumps as json_dumps
from os import getenv
from pathlib import Path
from shutil import rmtree

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

from spicy_regs.pipeline.build_search_index import build_search_index
from spicy_regs.pipeline.download_r2 import download_from_r2
from spicy_regs.pipeline.extract import (
    download_and_parse,
    download_existing_parquet,
    get_agencies,
    list_json_files,
    load_manifest,
)
from spicy_regs.pipeline.load import (
    save_manifest,
    upload_comment_partitions,
    upload_partitioned_comments,
    upload_to_r2,
)
from spicy_regs.pipeline.transform import (
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    partition_comments,
    update_comments_index,
    write_staging,
)

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
# serializing large objects on every .submit() call.
#
# PROCESSED_KEYS: BloomFilter or empty set — used for `key in PROCESSED_KEYS`
# lookups during extraction.  Loaded from manifest by load_manifest().
# NEW_KEYS: plain set that tracks keys added during this run, used to
# append to the manifest on save.
PROCESSED_KEYS: object = set()  # BloomFilter after load_manifest()
NEW_KEYS: set[str] = set()

def _extract_comment(d: dict) -> dict:
    attrs = d.get("data", {}).get("attributes", {})

    # Build compact attachments JSON from the included array
    attachments = []
    for inc in d.get("included", []):
        if inc.get("type") == "attachments":
            inc_attrs = inc.get("attributes", {})
            formats = [
                {"url": f["fileUrl"], "format": f.get("format"), "size": f.get("size")}
                for f in inc_attrs.get("fileFormats") or []
                if f.get("fileUrl")
            ]
            if formats:
                attachments.append({"title": inc_attrs.get("title", ""), "formats": formats})

    return {
        "comment_id": d.get("data", {}).get("id"),
        "docket_id": (v.strip('"') if (v := attrs.get("docketId")) else v),
        "agency_code": attrs.get("agencyId"),
        "title": attrs.get("title"),
        "comment": attrs.get("comment"),
        "document_type": attrs.get("documentType"),
        "posted_date": attrs.get("postedDate"),
        "modify_date": attrs.get("modifyDate"),
        "receive_date": attrs.get("receiveDate"),
        "attachments_json": json_dumps(attachments) if attachments else None,
    }


DEDUP_KEYS: dict[str, str] = {
    "dockets": "docket_id",
    "documents": "document_id",
    "comments": "comment_id",
}


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
            "docket_id": (v.strip('"') if (v := d.get("data", {}).get("id")) else v),
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
            "docket_id": (v.strip('"') if (v := d.get("data", {}).get("attributes", {}).get("docketId")) else v),
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
            "attachments_json": pl.Utf8,
        },
        "extract": _extract_comment,
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
    since_year: int | None = None,
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
                since_year,
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
    """Step 3: Merge per-agency staging files into final Parquet (dockets/documents only)."""
    non_comment_types = [dt for dt in data_type_names if dt != "comments"]
    if not non_comment_types:
        return
    schemas = {dtype: DATA_TYPES[dtype]["schema"] for dtype in non_comment_types}
    dedup_keys = {dtype: DEDUP_KEYS[dtype] for dtype in non_comment_types}
    merge_staging_files(staging_dir, output_dir, non_comment_types, schemas, dedup_keys)


@task(name="merge-comments-partitioned", cache_policy=NO_CACHE)
def merge_comments_partitioned_task(staging_dir: Path, output_dir: Path) -> list[Path]:
    """Step 3b: Merge comment staging into partitioned output."""
    return merge_comments_partitioned(
        staging_dir,
        output_dir,
        schema=DATA_TYPES["comments"]["schema"],
        dedup_key=DEDUP_KEYS["comments"],
    )


@task(name="update-comments-index", cache_policy=NO_CACHE)
def update_comments_index_task(output_dir: Path, changed_files: list[Path]) -> Path:
    """Update the comments partition index."""
    return update_comments_index(output_dir, changed_files)


@task(name="save-manifest", cache_policy=NO_CACHE)
def save_manifest_task(output_dir: Path) -> None:
    """Step 4: Append new S3 keys to the manifest."""
    save_manifest(output_dir, NEW_KEYS)


@task(name="upload-to-r2", cache_policy=NO_CACHE)
def upload_to_r2_task(output_dir: Path, data_type_names: list[str]) -> None:
    """Step 5: Upload parquet files + manifest to R2."""
    upload_to_r2(output_dir, data_type_names)


@task(name="build-feed-summary", cache_policy=NO_CACHE)
def build_feed_summary_task(output_dir: Path) -> Path:
    """Build pre-computed feed summary parquet."""
    return build_feed_summary(output_dir)


@task(name="build-search-index", cache_policy=NO_CACHE)
def build_search_index_task(output_dir: Path) -> Path:
    """Build docket_search.json.gz for the in-browser search."""
    return build_search_index(output_dir)


@task(name="partition-comments", cache_policy=NO_CACHE)
def partition_comments_task(output_dir: Path) -> Path:
    """Partition comments.parquet by agency_code."""
    return partition_comments(output_dir)


@task(name="upload-partitioned-comments", cache_policy=NO_CACHE)
def upload_partitioned_comments_task(partition_dir: Path) -> None:
    """Upload partitioned comments directory to R2."""
    upload_partitioned_comments(partition_dir)


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
    partition_only: Annotated[bool, Parameter(help="Only partition comments by agency and upload")] = False,
    skip_post_process: Annotated[bool, Parameter(help="Skip feed summary + comment partitioning (for intermediate batches)")] = False,
    since_year: Annotated[int | None, Parameter(help="Only process dockets from this year onward (e.g. 2025)")] = None,
) -> None:
    """Mirrulations S3 → Parquet on R2."""

    # Setup directories
    if output_dir is None:
        output_dir = Path.cwd() / "output"
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

    # Partition-only mode
    if partition_only:
        logger.info("Partition-only mode - partitioning comments by agency...")
        partition_dir = partition_comments_task(output_dir)
        upload_partitioned_comments_task(partition_dir)
        logger.info("Partition complete!")
        return

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
        build_feed_summary_task(output_dir)
        logger.info("Merge complete!")
        return

    # --- Step 1: Load manifest, download existing data, discover agencies (parallel) ---
    global PROCESSED_KEYS, NEW_KEYS
    NEW_KEYS = set()
    if full_refresh:
        logger.info("Full refresh mode - ignoring manifest")
        PROCESSED_KEYS = set()
    else:
        manifest_future = load_manifest.submit(output_dir)
        # Download existing dockets/documents (not monolithic comments —
        # comment partitions are downloaded on demand during merge).
        download_types = [dt for dt in data_type_names if dt != "comments"]
        parquet_future = download_existing_parquet.submit(output_dir, download_types)

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
        # Download existing comments index (for incremental index updates
        # and feed_summary comment counts).
        if "comments" in data_type_names:
            index_path = output_dir / "comments_index.parquet"
            if not index_path.exists():
                download_from_r2("comments_index.parquet", index_path)

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
        process_agency.submit(a, staging_dir, data_type_names, verbose, since_year)
        for a in agencies
    ]

    wait(futures)

    total_rows: dict[str, int] = {dt: 0 for dt in DATA_TYPES}
    for future in futures:
        results, new_keys = future.result()
        for dt, count in results.items():
            total_rows[dt] += count
        NEW_KEYS.update(new_keys)

    # --- Step 3: Merge staging into final Parquet ---
    changed_comment_partitions: list[Path] = []
    if any(total_rows.values()):
        logger.info("Merging staging files...")
        merge_staging_task(staging_dir, output_dir, data_type_names)

        # Merge comments into partitioned output (separate from monolithic merge).
        if "comments" in data_type_names and total_rows.get("comments", 0) > 0:
            logger.info("Merging comments into partitions...")
            changed_comment_partitions = merge_comments_partitioned_task(staging_dir, output_dir)

            if changed_comment_partitions:
                logger.info("Updating comments index...")
                update_comments_index_task(output_dir, changed_comment_partitions)

        rmtree(staging_dir)
        logger.info("Cleaned up staging directory")

    # --- Step 3b: Build feed summary + search index ---
    if skip_post_process:
        logger.info("Skipping feed summary (--skip-post-process)")
    else:
        logger.info("Building feed summary...")
        build_feed_summary_task(output_dir)
        # Only rebuild the search index when dockets changed — it's a
        # small computation but the resulting file is uploaded and served
        # from CDN, so avoid churning it needlessly.
        if total_rows.get("dockets", 0) > 0:
            logger.info("Building docket search index...")
            build_search_index_task(output_dir)

    # --- Summary ---
    logger.info("Summary:")
    for dt, count in total_rows.items():
        logger.info("  {}: {:,} rows", dt, count)
    logger.info("  New files processed: {:,}", len(NEW_KEYS))
    elapsed = datetime.now(timezone.utc) - start_time
    logger.info("ETL completed in {}", elapsed)

    # --- Step 4: Save manifest (append new keys to existing) ---
    if NEW_KEYS:
        save_manifest_task(output_dir)

    # --- Step 5: Upload to R2 ---
    if skip_upload is False:
        logger.info("Uploading to R2...")
        # Upload dockets/documents/manifest/feed_summary (skip monolithic comments)
        upload_types = [dt for dt in data_type_names if dt != "comments"]
        upload_to_r2_task(output_dir, upload_types)
        # Upload changed comment partitions + index
        if changed_comment_partitions:
            upload_comment_partitions(output_dir, changed_comment_partitions)
        # Upload search index (if dockets changed, it was rebuilt above)
        search_index_path = output_dir / "docket_search.json.gz"
        if search_index_path.exists() and not skip_post_process:
            from spicy_regs.pipeline.upload_r2 import upload_to_r2 as _upload_r2
            _upload_r2(search_index_path)

    logger.info("Done!")


if __name__ == "__main__":
    app()

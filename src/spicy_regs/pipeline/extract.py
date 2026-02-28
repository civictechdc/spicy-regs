"""
Extract tasks: discover and download data from S3 and R2.
"""

from json import loads
from pathlib import Path
from typing import Any, Callable

import polars as pl
from loguru import logger
from prefect import task
from tqdm import tqdm

from spicy_regs.pipeline.download_r2 import download_from_r2


@task(name="get-agencies", retries=3, retry_delay_seconds=5)
def get_agencies(s3_client: Any, bucket_name: str, prefix: str) -> list[str]:
    """Get list of all agencies from S3 bucket.

    Uses the S3 client directly with Delimiter='/' to efficiently list
    only top-level folder names without iterating all objects.
    """
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f"{prefix}/",
        Delimiter="/",
    )
    agencies = []
    for p in response.get("CommonPrefixes", []):
        agency = p["Prefix"].split("/")[1]
        if agency:
            agencies.append(agency)
    return sorted(agencies)


@task(name="load-manifest", retries=3, retry_delay_seconds=5)
def load_manifest(output_dir: Path) -> set[str]:
    """Load processed keys from manifest Parquet file."""
    manifest_file = output_dir / "manifest.parquet"

    if manifest_file.exists():
        df = pl.read_parquet(manifest_file)
        keys = set(df["key"].to_list())
        logger.info("Loaded manifest: {:,} processed keys", len(keys))
        return keys

    if download_from_r2("manifest.parquet", manifest_file):
        df = pl.read_parquet(manifest_file)
        keys = set(df["key"].to_list())
        logger.info("Downloaded manifest from R2: {:,} processed keys", len(keys))
        return keys

    logger.info("No manifest found, starting fresh")
    return set()


@task(name="download-existing-parquet", retries=3, retry_delay_seconds=5)
def download_existing_parquet(
    output_dir: Path,
    processed_keys: set[str],
    data_type_names: list[str],
) -> None:
    """Download existing Parquet files from R2 for incremental append."""
    if not processed_keys:
        return
    logger.info("Downloading existing Parquet files from R2...")
    for data_type in data_type_names:
        local_file = output_dir / f"{data_type}.parquet"
        if not local_file.exists():
            if download_from_r2(f"{data_type}.parquet", local_file):
                size_mb = local_file.stat().st_size / (1024 * 1024)
                logger.info("{}.parquet ({:.1f} MB)", data_type, size_mb)
            else:
                logger.warning("{}.parquet not found in R2", data_type)


@task(name="list-json-files", retries=3, retry_delay_seconds=5)
def list_json_files(
    s3_resource: Any,
    bucket_name: str,
    prefix: str,
    agency: str,
    data_type: str,
    path_pattern: str,
    processed_keys: set[str] | None = None,
    verbose: bool = False,
) -> list[str]:
    """List all JSON files for an agency and data type, excluding already processed."""
    files = []
    skipped = 0
    total_scanned = 0
    bucket = s3_resource.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=f"{prefix}/{agency}/"):
        key = obj.key
        if "/text-" in key and path_pattern in key and key.endswith(".json"):
            total_scanned += 1
            if processed_keys and key in processed_keys:
                skipped += 1
                continue
            files.append(key)

    if verbose:
        tqdm.write(f"    [{agency}] {data_type}: scanned {total_scanned}, skipped {skipped}, new {len(files)}")

    return files


@task(name="download-and-parse", retries=3, retry_delay_seconds=2)
def download_and_parse(
    s3_resource: Any,
    bucket_name: str,
    key: str,
    extract_fn: Callable[[dict], dict],
) -> dict | None:
    """Download a single JSON file from S3 and parse it with the given extractor."""
    obj = s3_resource.Object(bucket_name, key)
    content = obj.get()["Body"].read()
    data = loads(content)
    try:
        return extract_fn(data)
    except Exception:
        return None

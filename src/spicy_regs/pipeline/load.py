"""
Load tasks: persist manifest and upload final Parquet files to R2.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
from loguru import logger
from spicy_regs.pipeline.upload_r2 import (
    upload_directory_to_r2 as _upload_directory_to_r2,
    upload_to_r2 as _upload_to_r2,
)


def save_manifest(output_dir: Path, processed_keys: set[str]) -> None:
    """Save processed keys to manifest Parquet file."""
    manifest_file = output_dir / "manifest.parquet"
    df = pl.DataFrame({"key": list(processed_keys)})
    df.write_parquet(manifest_file, compression="zstd")
    logger.info("Saved manifest: {:,} keys", len(processed_keys))


def upload_to_r2(output_dir: Path, data_type_names: list[str]) -> None:
    """Upload all Parquet files and manifest to R2 in parallel."""
    files_to_upload = []
    for data_type in data_type_names:
        pf = output_dir / f"{data_type}.parquet"
        if pf.exists():
            files_to_upload.append(pf)

    # Always include feed summary if it exists
    feed_summary = output_dir / "feed_summary.parquet"
    if feed_summary.exists():
        files_to_upload.append(feed_summary)

    manifest_file = output_dir / "manifest.parquet"
    if manifest_file.exists():
        files_to_upload.append(manifest_file)

    with ThreadPoolExecutor(max_workers=len(files_to_upload)) as executor:
        executor.map(_upload_to_r2, files_to_upload)


def upload_partitioned_comments(partition_dir: Path) -> None:
    """Upload partitioned comments directory to R2."""
    _upload_directory_to_r2(partition_dir, remote_prefix="comments/agency")


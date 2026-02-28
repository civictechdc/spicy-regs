"""
Load tasks: persist manifest and upload final Parquet files to R2.
"""

from pathlib import Path

import polars as pl
from loguru import logger
from prefect import task

from spicy_regs.pipeline.upload_r2 import upload_to_r2 as _upload_to_r2


@task(name="save-manifest")
def save_manifest(output_dir: Path, processed_keys: set[str]) -> None:
    """Save processed keys to manifest Parquet file."""
    manifest_file = output_dir / "manifest.parquet"
    df = pl.DataFrame({"key": list(processed_keys)})
    df.write_parquet(manifest_file, compression="zstd")
    logger.info("Saved manifest: {:,} keys", len(processed_keys))


@task(name="upload-to-r2")
def upload_to_r2(output_dir: Path, data_type_names: list[str]) -> None:
    """Upload all Parquet files and manifest to R2."""
    for data_type in data_type_names:
        pf = output_dir / f"{data_type}.parquet"
        if pf.exists():
            _upload_to_r2(pf)

    manifest_file = output_dir / "manifest.parquet"
    if manifest_file.exists():
        _upload_to_r2(manifest_file)

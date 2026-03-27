"""
Load tasks: persist manifest and upload final Parquet files to R2.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from loguru import logger
from spicy_regs.pipeline.upload_r2 import (
    upload_directory_to_r2 as _upload_directory_to_r2,
    upload_to_r2 as _upload_to_r2,
)


def save_manifest(output_dir: Path, new_keys: set[str]) -> None:
    """Append new keys to the existing manifest Parquet file.

    Reads the old manifest (if any) in streaming batches, writes those
    plus the new keys to a temp file, then replaces the original.
    This avoids loading the full 27M-key manifest into memory.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    manifest_file = output_dir / "manifest.parquet"
    temp_file = output_dir / "manifest_new.parquet"
    schema = pa.schema([("key", pa.large_string())])

    existing_rows = 0
    with pq.ParquetWriter(temp_file, schema, compression="zstd") as writer:
        # Stream existing manifest rows
        if manifest_file.exists():
            pf = pq.ParquetFile(manifest_file)
            for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
                table = pa.Table.from_batches([batch]).cast(schema)
                writer.write_table(table)
                existing_rows += batch.num_rows

        # Append new keys
        new_table = pa.table({"key": list(new_keys)}).cast(schema)
        writer.write_table(new_table)

    if manifest_file.exists():
        manifest_file.unlink()
    temp_file.rename(manifest_file)

    total = existing_rows + len(new_keys)
    logger.info("Saved manifest: {:,} keys ({:,} existing + {:,} new)", total, existing_rows, len(new_keys))


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


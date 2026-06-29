"""Cloudflare R2 storage connector.

A single home for the project's R2 connection — the bookends of an incremental
run: pulling existing datasets down so a run can append to them, and publishing
finished Parquet (plus the manifest) back to the bucket. This lets a pipeline
treat "load" as one composable stage (``r2.download`` / ``r2.upload_dataset``)
instead of reaching into the pipeline internals.

These are thin, intentional wrappers over the battle-tested
``pipeline.download_r2`` / ``pipeline.load`` helpers so the production flow that
also uses them stays untouched. Credentials are read from the environment by
those helpers (``R2_PUBLIC_URL`` for downloads; ``R2_*`` for uploads).
"""

from pathlib import Path

from spicy_regs.pipeline.download_r2 import download_from_r2
from spicy_regs.pipeline.load import upload_comment_partitions as _upload_comment_partitions
from spicy_regs.pipeline.load import upload_to_r2 as _upload_to_r2
from spicy_regs.pipeline.upload_r2 import upload_to_r2 as _upload_file


def download(remote_key: str, local_path: Path) -> bool:
    """Fetch one object from R2.

    Returns ``True`` on success, ``False`` if the object is absent or R2 is not
    configured. Other failures (5xx, truncated transfers) raise.
    """
    return download_from_r2(remote_key, local_path)


def upload_dataset(output_dir: Path, data_types: list[str]) -> None:
    """Publish the merged ``{data_type}.parquet`` files (+ feed summary + manifest)."""
    _upload_to_r2(output_dir, data_types)


def upload_comment_partitions(output_dir: Path, changed_files: list[Path]) -> None:
    """Publish changed comment partition files and the comments index."""
    _upload_comment_partitions(output_dir, changed_files)


def upload_file(local_path: Path, remote_key: str | None = None) -> None:
    """Publish a single file to R2 (remote key defaults to the filename)."""
    _upload_file(local_path, remote_key=remote_key)

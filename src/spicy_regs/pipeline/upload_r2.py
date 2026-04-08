#!/usr/bin/env python3
"""Upload Parquet files to Cloudflare R2."""

from os import getenv
from pathlib import Path

import boto3
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def get_r2_client():
    """Create boto3 client configured for R2."""
    return boto3.client(
        "s3",
        endpoint_url=getenv("R2_ENDPOINT"),
        aws_access_key_id=getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def _get_remote_size(client, bucket: str, remote_key: str) -> int | None:
    """Return the existing R2 object size in bytes, or None if absent.

    Any other error (permissions, transient 5xx) propagates — callers
    must not guess at whether the remote object exists, since the
    upload guard depends on a correct answer to decide if a shrink is
    catastrophic.
    """
    from botocore.exceptions import ClientError

    try:
        resp = client.head_object(Bucket=bucket, Key=remote_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return int(resp["ContentLength"])


def _assert_upload_safe(
    local_size: int,
    remote_size: int | None,
    remote_key: str,
) -> None:
    """Abort if the new upload would catastrophically shrink the remote.

    Controlled by ``R2_MIN_SIZE_RATIO`` (default ``0.5``) — the new file
    must be at least that fraction of the existing remote file. Set
    ``R2_ALLOW_SHRINK=1`` to bypass (used by recovery scripts).
    """
    if remote_size is None or remote_size == 0:
        return
    if getenv("R2_ALLOW_SHRINK") == "1":
        logger.warning(
            "R2_ALLOW_SHRINK=1: bypassing shrink guard for {}", remote_key
        )
        return

    ratio_env = getenv("R2_MIN_SIZE_RATIO", "0.5")
    try:
        min_ratio = float(ratio_env)
    except ValueError:
        raise RuntimeError(
            f"Invalid R2_MIN_SIZE_RATIO={ratio_env!r}; expected a float"
        )

    ratio = local_size / remote_size
    if ratio < min_ratio:
        raise RuntimeError(
            f"Refusing to upload {remote_key}: new file would shrink remote "
            f"from {remote_size / 1024 / 1024:.1f} MB to "
            f"{local_size / 1024 / 1024:.1f} MB (ratio {ratio:.3f} < "
            f"threshold {min_ratio}). Set R2_ALLOW_SHRINK=1 to override."
        )


def upload_to_r2(local_path: Path, remote_key: str = None):
    """Upload a file to R2 bucket.

    Before overwriting an existing remote object, checks the current
    size and refuses to proceed if the new file is much smaller (see
    ``_assert_upload_safe``).  This guards against the failure mode
    where an upstream error produces a near-empty local file that
    would otherwise silently wipe production data.
    """
    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")

    if not getenv("R2_ACCESS_KEY_ID"):
        logger.warning("Skipping upload (R2 credentials not configured): {}", local_path.name)
        return

    if remote_key is None:
        remote_key = local_path.name

    client = get_r2_client()

    local_size_bytes = local_path.stat().st_size
    file_size = local_size_bytes / (1024 * 1024)
    logger.info("Uploading {} ({:.1f} MB) to R2...", local_path.name, file_size)

    remote_size = _get_remote_size(client, bucket, remote_key)
    _assert_upload_safe(local_size_bytes, remote_size, remote_key)

    client.upload_file(
        str(local_path),
        bucket,
        remote_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )

    public_url = getenv("R2_PUBLIC_URL", "")
    logger.info("Uploaded: {}/{}", public_url, remote_key)


def upload_directory_to_r2(local_dir: Path, remote_prefix: str = None):
    """Recursively upload a directory to R2, preserving relative paths as keys."""
    if not local_dir.is_dir():
        print(f"  Skipping (not a directory): {local_dir}")
        return

    if remote_prefix is None:
        remote_prefix = local_dir.name

    files = sorted(local_dir.rglob("*.parquet"))
    print(f"  Uploading {len(files)} files from {local_dir.name}/ to R2...")

    for file_path in files:
        relative = file_path.relative_to(local_dir)
        remote_key = f"{remote_prefix}/{relative}"
        upload_to_r2(file_path, remote_key=remote_key)

    print(f"  ✓ Uploaded {len(files)} files under {remote_prefix}/")


def list_r2_files():
    """List files in R2 bucket."""
    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    client = get_r2_client()

    response = client.list_objects_v2(Bucket=bucket)

    if "Contents" not in response:
        logger.info("Bucket is empty")
        return []

    for obj in response["Contents"]:
        logger.info("{} ({:.1f} MB)", obj["Key"], obj["Size"] / 1024 / 1024)

    return response["Contents"]


if __name__ == "__main__":
    from sys import argv

    if len(argv) > 1:
        # Upload specified file
        upload_to_r2(Path(argv[1]))
    else:
        # List files
        logger.info("Files in R2 bucket:")
        list_r2_files()

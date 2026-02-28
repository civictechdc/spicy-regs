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


def upload_to_r2(local_path: Path, remote_key: str = None):
    """Upload a file to R2 bucket."""
    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")

    if not getenv("R2_ACCESS_KEY_ID"):
        logger.warning("Skipping upload (R2 credentials not configured): {}", local_path.name)
        return

    if remote_key is None:
        remote_key = local_path.name

    client = get_r2_client()

    file_size = local_path.stat().st_size / (1024 * 1024)
    logger.info("Uploading {} ({:.1f} MB) to R2...", local_path.name, file_size)

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

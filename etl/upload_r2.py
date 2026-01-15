#!/usr/bin/env python3
"""Upload Parquet files to Cloudflare R2."""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()


def get_r2_client():
    """Create boto3 client configured for R2."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def upload_to_r2(local_path: Path, remote_key: str = None):
    """Upload a file to R2 bucket."""
    bucket = os.getenv("R2_BUCKET_NAME", "spicy-regs")
    
    if not os.getenv("R2_ACCESS_KEY_ID"):
        print(f"  Skipping upload (R2 credentials not configured): {local_path.name}")
        return
    
    if remote_key is None:
        remote_key = local_path.name

    client = get_r2_client()
    
    file_size = local_path.stat().st_size / (1024 * 1024)
    print(f"  Uploading {local_path.name} ({file_size:.1f} MB) to R2...")

    client.upload_file(
        str(local_path),
        bucket,
        remote_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )

    public_url = os.getenv("R2_PUBLIC_URL", "")
    print(f"  ✓ Uploaded: {public_url}/{remote_key}")


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download a file from R2 bucket. Returns True if successful."""
    bucket = os.getenv("R2_BUCKET_NAME", "spicy-regs")
    
    if not os.getenv("R2_ACCESS_KEY_ID"):
        return False
    
    client = get_r2_client()
    
    try:
        client.download_file(bucket, remote_key, str(local_path))
        print(f"  ✓ Downloaded {remote_key} from R2")
        return True
    except client.exceptions.ClientError:
        return False
    except Exception:
        return False


def list_r2_files():
    """List files in R2 bucket."""
    bucket = os.getenv("R2_BUCKET_NAME", "spicy-regs")
    client = get_r2_client()
    
    response = client.list_objects_v2(Bucket=bucket)
    
    if "Contents" not in response:
        print("Bucket is empty")
        return []
    
    for obj in response["Contents"]:
        print(f"  {obj['Key']} ({obj['Size'] / 1024 / 1024:.1f} MB)")
    
    return response["Contents"]


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Upload specified file
        upload_to_r2(Path(sys.argv[1]))
    else:
        # List files
        print("Files in R2 bucket:")
        list_r2_files()

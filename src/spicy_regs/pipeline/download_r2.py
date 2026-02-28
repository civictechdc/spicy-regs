#!/usr/bin/env python3
"""Download files from Cloudflare R2 using public URL."""

from os import getenv
from pathlib import Path

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download a file from R2 bucket using public URL. Returns True if successful."""
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        return False

    url = f"{public_url}/{remote_key}"

    try:
        with httpx.stream("GET", url, follow_redirects=True) as response:
            if response.status_code != 200:
                return False
            with open(local_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
        logger.info("Downloaded {} from R2", remote_key)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    from sys import argv

    if len(argv) > 1:
        # Download specified file
        remote_key = argv[1]
        local_path = Path(argv[2]) if len(argv) > 2 else Path(remote_key)
        success = download_from_r2(remote_key, local_path)
        if not success:
            logger.error("Failed to download {}", remote_key)
            from sys import exit

            exit(1)
    else:
        logger.info("Usage: python download_r2.py <remote_key> [local_path]")

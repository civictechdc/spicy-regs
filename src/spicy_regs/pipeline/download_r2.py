#!/usr/bin/env python3
"""Download files from Cloudflare R2 using public URL."""

from os import getenv
from pathlib import Path

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download a file from R2 bucket using public URL.

    Returns ``True`` if the file was downloaded, ``False`` if R2 is not
    configured or the file does not exist on R2 (HTTP 404).  Any other
    error — 5xx responses, network failures, disk write errors — raises.

    Silent failures here caused the March 2026 data-loss incident: a
    transient download error on the 3.3 GB ``comments.parquet`` returned
    ``False``, the merge step then wrote a fresh empty file, and the
    upload step overwrote the historical data on R2.  Raising forces the
    pipeline to abort before the upload step instead of continuing with
    an empty input.

    The download is atomic: bytes are streamed to ``{local_path}.tmp``
    and the temp file is renamed into place only after a successful
    transfer.  On any failure, the temp file is removed and any
    pre-existing file at ``local_path`` is left untouched.
    """
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        logger.warning("R2_PUBLIC_URL not set; cannot download {}", remote_key)
        return False

    url = f"{public_url}/{remote_key}"
    temp_path = local_path.with_suffix(local_path.suffix + ".tmp")

    try:
        with httpx.stream("GET", url, follow_redirects=True) as response:
            if response.status_code == 404:
                logger.info("{} not found on R2 (404)", remote_key)
                return False
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to download {remote_key} from R2: HTTP "
                    f"{response.status_code}"
                )
            with open(temp_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
    except BaseException:
        if temp_path.exists():
            temp_path.unlink()
        raise

    temp_path.replace(local_path)
    logger.info("Downloaded {} from R2", remote_key)
    return True


if __name__ == "__main__":
    from sys import argv

    if len(argv) > 1:
        # Download specified file
        remote_key = argv[1]
        local_path = Path(argv[2]) if len(argv) > 2 else Path(remote_key)
        try:
            success = download_from_r2(remote_key, local_path)
        except Exception as e:
            logger.error("Failed to download {}: {}", remote_key, e)
            from sys import exit

            exit(1)
        if not success:
            logger.error("{} not found on R2", remote_key)
            from sys import exit

            exit(1)
    else:
        logger.info("Usage: python download_r2.py <remote_key> [local_path]")

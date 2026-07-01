"""Cloudflare R2 storage connector.

The project's single home for R2 — both bookends of an incremental run: pulling
existing datasets down so a run can append to them (:func:`download_from_r2`),
and publishing finished Parquet, partitions, and the manifest back to the bucket
(:func:`upload_file` / :func:`upload_dataset` / :func:`upload_comment_partitions`).
This lets a pipeline treat "load" as one composable stage instead of reaching
into storage internals.

Downloads use the public ``R2_PUBLIC_URL`` over HTTPS; uploads use the S3 API
with ``R2_*`` credentials. Uploads are guarded against catastrophic shrink (see
:func:`_assert_upload_safe`) — the safeguard added after the March 2026 incident
where a transient download error produced an empty local file that overwrote the
3.3 GB historical ``comments.parquet`` on R2.
"""

from concurrent.futures import ThreadPoolExecutor
from os import getenv
from pathlib import Path

import boto3
import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


# --- download (public URL) -------------------------------------------------


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download a file from R2 bucket using the public URL.

    Returns ``True`` if the file was downloaded, ``False`` if R2 is not
    configured or the file does not exist on R2 (HTTP 404). Any other
    error — 5xx responses, network failures, disk write errors — raises.

    Silent failures here caused the March 2026 data-loss incident: a
    transient download error on the 3.3 GB ``comments.parquet`` returned
    ``False``, the merge step then wrote a fresh empty file, and the
    upload step overwrote the historical data on R2. Raising forces the
    pipeline to abort before the upload step instead of continuing with
    an empty input.

    The download is atomic: bytes are streamed to ``{local_path}.tmp``
    and the temp file is renamed into place only after a successful
    transfer. On any failure, the temp file is removed and any
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


def download(remote_key: str, local_path: Path) -> bool:
    """Alias for :func:`download_from_r2` (the connector's download verb)."""
    return download_from_r2(remote_key, local_path)


# --- upload (S3 API) -------------------------------------------------------


def get_r2_client():
    """Create a boto3 client configured for R2."""
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


def upload_file(local_path: Path, remote_key: str | None = None) -> None:
    """Publish a single file to R2 (remote key defaults to the filename).

    Before overwriting an existing remote object, checks the current
    size and refuses to proceed if the new file is much smaller (see
    :func:`_assert_upload_safe`). This guards against the failure mode
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


def upload_directory_to_r2(local_dir: Path, remote_prefix: str | None = None) -> None:
    """Recursively upload a directory of Parquet to R2, preserving relative paths."""
    if not local_dir.is_dir():
        logger.warning("Skipping (not a directory): {}", local_dir)
        return

    if remote_prefix is None:
        remote_prefix = local_dir.name

    files = sorted(local_dir.rglob("*.parquet"))
    logger.info("Uploading {} files from {}/ to R2...", len(files), local_dir.name)

    for file_path in files:
        relative = file_path.relative_to(local_dir)
        remote_key = f"{remote_prefix}/{relative}"
        upload_file(file_path, remote_key=remote_key)

    logger.info("Uploaded {} files under {}/", len(files), remote_prefix)


def upload_dataset(output_dir: Path, data_types: list[str]) -> None:
    """Publish the merged ``{data_type}.parquet`` base tables (+ manifest) in parallel.

    Rollups (feed_summary, agency_stats, ...) are no longer published here — each
    is built and uploaded by its own decoupled ``run-rollup-*`` pipeline.
    """
    files_to_upload = []
    for data_type in data_types:
        pf = output_dir / f"{data_type}.parquet"
        if pf.exists():
            files_to_upload.append(pf)

    manifest_file = output_dir / "manifest.parquet"
    if manifest_file.exists():
        files_to_upload.append(manifest_file)

    with ThreadPoolExecutor(max_workers=len(files_to_upload)) as executor:
        executor.map(upload_file, files_to_upload)


def upload_comment_partitions(output_dir: Path, changed_files: list[Path]) -> None:
    """Publish changed comment partition files and the comments index to R2."""
    for local_path in changed_files:
        remote_key = str(local_path.relative_to(output_dir))
        upload_file(local_path, remote_key=remote_key)

    index_file = output_dir / "comments_index.parquet"
    if index_file.exists():
        upload_file(index_file, remote_key="comments_index.parquet")

    logger.info("Uploaded {} comment partitions + index", len(changed_files))


def list_r2_files() -> list:
    """List files in the R2 bucket."""
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
        upload_file(Path(argv[1]))
    else:
        logger.info("Files in R2 bucket:")
        list_r2_files()

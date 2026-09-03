"""Cloudflare R2 storage connector — both bookends of an incremental run.

:func:`download_from_r2` pulls existing datasets down so a run can append to
them. :func:`upload_file`, :func:`upload_dataset`, and
:func:`upload_comment_partitions` publish the finished Parquet back.

Downloads use the public ``R2_PUBLIC_URL`` over HTTPS; uploads use the S3 API
with ``R2_*`` credentials. Every upload clears a shrink guard
(:func:`_assert_upload_safe`), added after the March 2026 incident: a transient
download error produced an empty local file, and the upload overwrote the
historical 3.3 GB ``comments.parquet``.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path

import boto3
import httpx
from loguru import logger

from spicy_regs.sources.cloudflare import purge_urls


# --- download (public URL) -------------------------------------------------


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download one object from R2 over the public URL.

    Returns ``True`` on success, ``False`` when R2 is unconfigured or the object
    is missing (HTTP 404). Everything else — 5xx, network failures, disk write
    errors — raises.

    Returning ``False`` for the rest caused the March 2026 data loss: a
    transient error on the 3.3 GB ``comments.parquet`` read as "absent", the
    merge wrote a fresh empty file, and the upload overwrote the history.
    Raising aborts the run before it can publish an empty table.

    The download is atomic: bytes stream to ``{local_path}.tmp``, which is
    renamed into place only after a complete transfer. Any failure deletes the
    temp file and leaves an existing ``local_path`` untouched.
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
                raise RuntimeError(f"Failed to download {remote_key} from R2: HTTP {response.status_code}")
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
    """Return the remote object's size in bytes, or ``None`` when it is absent.

    Permissions and transient 5xx errors propagate: the shrink guard skips its
    check when nothing is there, so a guessed absence would wave every upload
    through.
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
        logger.warning("R2_ALLOW_SHRINK=1: bypassing shrink guard for {}", remote_key)
        return

    ratio_env = getenv("R2_MIN_SIZE_RATIO", "0.5")
    try:
        min_ratio = float(ratio_env)
    except ValueError:
        raise RuntimeError(f"Invalid R2_MIN_SIZE_RATIO={ratio_env!r}; expected a float")

    ratio = local_size / remote_size
    if ratio < min_ratio:
        raise RuntimeError(
            f"Refusing to upload {remote_key}: new file would shrink remote "
            f"from {remote_size / 1024 / 1024:.1f} MB to "
            f"{local_size / 1024 / 1024:.1f} MB (ratio {ratio:.3f} < "
            f"threshold {min_ratio}). Set R2_ALLOW_SHRINK=1 to override."
        )


def _raise_for_failures(action: str, failures: list[tuple[str, BaseException]], total: int) -> None:
    """Log every failure, then raise one error naming them all.

    Collecting rather than raising on the first keeps one broken file from
    hiding the next behind it (see :func:`upload_dataset`).
    """
    if not failures:
        return
    for name, error in failures:
        logger.error("{} failed for {}: {}", action, name, error)
    names = ", ".join(sorted(name for name, _ in failures))
    raise RuntimeError(f"{action} failed for {len(failures)} of {total} file(s): {names}") from failures[0][1]


def _remote_key(output_dir: Path, local_path: Path) -> str:
    """The object key a file published from ``output_dir`` lands under.

    One definition so the preflight guards exactly the key the upload writes;
    two derivations that drifted would silently check the wrong object.
    """
    return str(local_path.relative_to(output_dir))


def dataset_files(output_dir: Path, data_types: list[str]) -> list[Path]:
    """The base-table ``{data_type}.parquet`` files that exist under ``output_dir``.

    Shared so a caller plans over exactly the set :func:`upload_dataset`
    publishes instead of rebuilding it.
    """
    return [pf for data_type in data_types if (pf := output_dir / f"{data_type}.parquet").exists()]


def preflight_uploads(output_dir: Path, files: list[Path]) -> None:
    """HEAD every planned object and run its size guard before the first byte lands.

    One worker used to replace its object while a sibling was still discovering
    that its own local file would catastrophically shrink production. Guarding
    the whole set first makes that refusal stop the publication before it starts.
    Each file is keyed by its path relative to ``output_dir``, the derivation
    :func:`upload_comment_partitions` publishes under. Without R2 credentials
    this is a no-op, like :func:`upload_file`.

    A transfer can still fail after a clean preflight, so callers must leave the
    manifest unpublished until every data upload succeeds.

    Costs one serial HEAD per file, and :func:`upload_file` HEADs each again:
    2N round-trips where it was N. N is unbounded for comment partitions, so
    profile this loop first when a comment-heavy run drags — the fix is a
    fixed-width thread pool, not ``max_workers=len(files)``.
    """
    if not getenv("R2_ACCESS_KEY_ID"):
        return

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")
    client = get_r2_client()
    failures: list[tuple[str, BaseException]] = []

    for local_path in files:
        remote_key = _remote_key(output_dir, local_path)
        try:
            remote_size = _get_remote_size(client, bucket, remote_key)
            _assert_upload_safe(local_path.stat().st_size, remote_size, remote_key)
        except Exception as error:
            failures.append((remote_key, error))

    _raise_for_failures("Publication preflight", failures, len(files))


def upload_file(local_path: Path, remote_key: str | None = None) -> None:
    """Publish one file to R2 (the remote key defaults to the filename).

    HEADs the existing object first and refuses to overwrite it with a much
    smaller one (:func:`_assert_upload_safe`), so an upstream error that
    produced a near-empty local file cannot wipe production.
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

    # Cache-Control policy. Parquet must never be edge-cached: DuckDB (the MCP
    # server and the browser DuckDB-WASM UI) reads each file through many
    # byte-range requests, and an edge cache serving inconsistent bytes across
    # those ranges under concurrent load corrupts the read — `utf-8 codec can't
    # decode` and ETag mismatches at ~c=10+. Parquet was `no-cache` originally; a
    # caching experiment brought the corruption back and was reverted. Everything
    # else (the UI MiniSearch json.gz, which DuckDB never reads) caches safely.
    # `R2_CACHE_CONTROL` overrides.
    configured_cache_control = getenv("R2_CACHE_CONTROL")
    cache_control = configured_cache_control or (
        "public, no-cache, must-revalidate"
        if remote_key.endswith(".parquet")
        else "public, max-age=3600, stale-while-revalidate=86400"
    )
    client.upload_file(
        str(local_path),
        bucket,
        remote_key,
        ExtraArgs={"ContentType": "application/octet-stream", "CacheControl": cache_control},
    )

    public_url = getenv("R2_PUBLIC_URL", "")
    logger.info("Uploaded: {}/{}", public_url, remote_key)

    # Invalidate the edge cache for exactly this object so the fresh version is
    # visible immediately. Best-effort and a no-op without Cloudflare creds; it
    # must never fail a publish that already wrote the data (see cloudflare.py).
    if public_url:
        purge_urls([f"{public_url.rstrip('/')}/{remote_key}"])


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
    """Publish the merged base tables in parallel.

    Every size guard runs before the first upload starts, so a table this run
    would refuse cannot land after a sibling has already been replaced. The
    pipeline caller preflights a superset (partitions, index, manifest) first,
    and re-checking here is deliberate rather than an oversight: this function
    fans out to a thread pool, so its own preflight is what stops one table
    landing while a sibling is refused. The base tables number two, so it costs
    two HEAD requests.

    Fixed object keys give R2 no atomic multi-object commit. This ordering keeps
    a known guard failure from publishing anything; the transfers themselves
    stay exposed to a network or service failure mid-flight. So the caller
    publishes ``manifest.parquet`` only after every base and partitioned data
    file has succeeded.

    Every upload is awaited and its outcome inspected. A bare
    ``executor.map(upload_file, ...)`` whose lazy iterator nobody consumed used
    to swallow every publish failure while the ETL exited 0, hiding an 8-week
    ``dockets`` outage: the shrink guard rightly refused a truncated
    ``dockets.parquet`` on every run (the Iceberg dockets table was never
    seeded, so the exported snapshot held ~5k rows instead of ~276k) while the
    workflow stayed green and the published table sat frozen at 2026-07-02.

    Rollups (feed_summary, agency_stats, ...) belong to their own decoupled
    ``run-rollup-*`` pipelines now.
    """
    base_files = dataset_files(output_dir, data_types)

    # ThreadPoolExecutor rejects max_workers=0, so an empty publish set would
    # otherwise raise ValueError rather than being the no-op it should be.
    if not base_files:
        logger.warning("upload_dataset: no files to publish in {}", output_dir)
        return

    preflight_uploads(output_dir, base_files)

    failures: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=len(base_files)) as executor:
        futures = {executor.submit(upload_file, pf): pf for pf in base_files}
        for future in as_completed(futures):
            if (error := future.exception()) is not None:
                failures.append((futures[future].name, error))

    _raise_for_failures("R2 base table publish", failures, len(base_files))


def upload_comment_partitions(output_dir: Path, changed_files: list[Path]) -> None:
    """Publish the changed comment partitions, then the refreshed comments index."""
    for local_path in changed_files:
        upload_file(local_path, remote_key=_remote_key(output_dir, local_path))

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

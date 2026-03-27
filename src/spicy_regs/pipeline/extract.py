"""
Extract tasks: discover and download data from S3 and R2.
"""

from array import array
from hashlib import md5, sha1
from json import loads
from math import log
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq
from loguru import logger
from prefect import task
from prefect.cache_policies import NO_CACHE
from tqdm import tqdm

from spicy_regs.pipeline.download_r2 import download_from_r2


# ---------------------------------------------------------------------------
# Bloom filter — stdlib-only, ~34 MB for 30M keys at 1e-7 FP rate.
#
# A false positive only means we skip a file that was actually new; the
# next run will pick it up.  This replaces a Python set that consumed
# ~5 GB for 27M strings.
# ---------------------------------------------------------------------------

class BloomFilter:
    """Memory-efficient probabilistic set membership using a bit array."""

    __slots__ = ("_bits", "_nbits", "_k")

    def __init__(self, capacity: int, fp_rate: float = 1e-7) -> None:
        self._nbits = max(1, int(-capacity * log(fp_rate) / (log(2) ** 2)))
        self._k = max(1, int((self._nbits / capacity) * log(2)))
        # 'L' = unsigned long (4 bytes each)
        self._bits = array("L", [0]) * (self._nbits // 32 + 1)

    def _hashes(self, key: str) -> list[int]:
        kb = key.encode()
        h1 = int.from_bytes(md5(kb).digest()[:8], "little")
        h2 = int.from_bytes(sha1(kb).digest()[:8], "little")
        return [(h1 + i * h2) % self._nbits for i in range(self._k)]

    def add(self, key: str) -> None:
        for pos in self._hashes(key):
            self._bits[pos >> 5] |= 1 << (pos & 31)

    def __contains__(self, key: str) -> bool:
        for pos in self._hashes(key):
            if not (self._bits[pos >> 5] & (1 << (pos & 31))):
                return False
        return True

    @property
    def size_bytes(self) -> int:
        return len(self._bits) * 4


@task(name="get-agencies", retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def get_agencies(s3_client: Any, bucket_name: str, prefix: str) -> list[str]:
    """Get list of all agencies from S3 bucket.

    Uses the S3 client directly with Delimiter='/' to efficiently list
    only top-level folder names without iterating all objects.
    """
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f"{prefix}/",
        Delimiter="/",
    )
    agencies = []
    for p in response.get("CommonPrefixes", []):
        agency = p["Prefix"].split("/")[1]
        if agency:
            agencies.append(agency)
    return sorted(agencies)


@task(name="load-manifest", retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def load_manifest(output_dir: Path) -> None:
    """Load manifest Parquet and build a memory-efficient Bloom filter.

    Instead of materializing 27M+ keys into a Python set (~5 GB RAM), we
    stream the Parquet file in batches and insert keys into a Bloom filter
    (~34 MB RAM for 30M keys at 1e-7 false-positive rate).
    """
    import spicy_regs.pipeline.pipeline as _pipeline

    manifest_file = output_dir / "manifest.parquet"

    if not manifest_file.exists():
        if not download_from_r2("manifest.parquet", manifest_file):
            logger.info("No manifest found, starting fresh")
            return

    pf = pq.ParquetFile(manifest_file)
    key_count = pf.metadata.num_rows

    bloom = BloomFilter(capacity=max(key_count + 5_000_000, 30_000_000))

    loaded = 0
    for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
        for key in batch.column("key").to_pylist():
            bloom.add(key)
        loaded += batch.num_rows
        logger.info("  manifest: loaded {:,}/{:,} keys", loaded, key_count)

    _pipeline.PROCESSED_KEYS = bloom
    logger.info(
        "Loaded manifest into bloom filter: {:,} keys, ~{:.0f} MB",
        key_count,
        bloom.size_bytes / 1_048_576,
    )


@task(name="download-existing-parquet", retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def download_existing_parquet(
    output_dir: Path,
    data_type_names: list[str],
) -> None:
    """Download existing Parquet files from R2 for incremental append."""
    logger.info("Downloading existing Parquet files from R2...")
    for data_type in data_type_names:
        local_file = output_dir / f"{data_type}.parquet"
        if not local_file.exists():
            if download_from_r2(f"{data_type}.parquet", local_file):
                size_mb = local_file.stat().st_size / (1024 * 1024)
                logger.info("{}.parquet ({:.1f} MB)", data_type, size_mb)
            else:
                logger.warning("{}.parquet not found in R2", data_type)


def list_json_files(
    s3_resource: Any,
    bucket_name: str,
    prefix: str,
    agency: str,
    data_type: str,
    path_pattern: str,
    processed_keys: Any = None,
    verbose: bool = False,
    since_year: int | None = None,
) -> list[str]:
    """List all JSON files for an agency and data type, excluding already processed."""
    import re

    # Match year from docket ID in path: raw-data/{agency}/{agency}-{YYYY}-...
    year_pattern = re.compile(rf"{re.escape(prefix)}/{re.escape(agency)}/{re.escape(agency)}-(\d{{4}})-")

    files = []
    skipped = 0
    filtered_by_year = 0
    total_scanned = 0
    bucket = s3_resource.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=f"{prefix}/{agency}/"):
        key = obj.key
        if "/text-" in key and path_pattern in key and key.endswith(".json"):
            total_scanned += 1
            if since_year:
                m = year_pattern.search(key)
                if m and int(m.group(1)) < since_year:
                    filtered_by_year += 1
                    continue
            if processed_keys and key in processed_keys:
                skipped += 1
                continue
            files.append(key)

    if verbose:
        year_msg = f", filtered_by_year {filtered_by_year}" if since_year else ""
        tqdm.write(f"    [{agency}] {data_type}: scanned {total_scanned}, skipped {skipped}{year_msg}, new {len(files)}")

    return files


def download_and_parse(
    s3_resource: Any,
    bucket_name: str,
    key: str,
    extract_fn: Callable[[dict], dict],
) -> dict | None:
    """Download a single JSON file from S3 and parse it with the given extractor."""
    try:
        obj = s3_resource.Object(bucket_name, key)
        content = obj.get()["Body"].read()
        data = loads(content)
        return extract_fn(data)
    except Exception:
        return None

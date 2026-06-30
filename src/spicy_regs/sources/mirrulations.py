"""Reader connector for the Mirrulations S3 mirror of regulations.gov.

Wraps the existing S3 discovery + download functions so that one agency's files
for a single :class:`~spicy_regs.schemas.RecordType` are exposed through the
:class:`~spicy_regs.sources.base.Reader` interface. Listing, year-filtering, and
dedup against already-processed keys are delegated to ``list_json_files``;
per-file download + JSON decode is delegated to ``download_and_parse``.

The reader is a *pure source*: it yields the raw JSON payloads. Flattening them
into schema-shaped records is the job of the
:class:`~spicy_regs.transforms.extract.ExtractRecords` transform.
"""

import re
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import loads
from threading import Lock
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from tqdm import tqdm

from spicy_regs.schemas import RecordType
from spicy_regs.sources.base import Reader

# Connection details for the public Mirrulations mirror live with the source
# that uses them, not in the pipeline.
BUCKET = "mirrulations"
PREFIX = "raw-data"

# Downloads are tiny JSON GETs against S3 — I/O-bound, so a pool of threads per
# agency turns thousands of serial round-trips into concurrent ones. The single
# anonymous resource is shared across the pool: unsigned read-only GetObject has
# no credential-refresh race, and botocore's connection pool is thread-safe.
DEFAULT_DOWNLOAD_WORKERS = 16


def s3_resource(max_pool_connections: int = DEFAULT_DOWNLOAD_WORKERS) -> Any:
    """A fresh anonymous S3 resource (one per worker keeps threads independent).

    The connection pool is sized to the download concurrency: botocore defaults
    to 10, but the reader fans GETs across ``DEFAULT_DOWNLOAD_WORKERS`` threads.
    A pool smaller than the thread count oversubscribes — connections churn into
    CLOSE_WAIT and the run stalls — so the pool must be at least the worker count.
    """
    return boto3.resource(
        "s3",
        region_name="us-east-1",
        config=BotoConfig(signature_version=UNSIGNED, max_pool_connections=max_pool_connections),
    )


def s3_client() -> Any:
    """Anonymous S3 client (used only for agency discovery)."""
    return boto3.client("s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED))


def get_agencies(s3_client: Any, bucket_name: str, prefix: str) -> list[str]:
    """Get the list of all agencies from the S3 bucket.

    Uses the S3 client directly with ``Delimiter='/'`` to efficiently list
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
        tqdm.write(
            f"    [{agency}] {data_type}: scanned {total_scanned}, skipped {skipped}{year_msg}, new {len(files)}"
        )

    return files


def list_agency_files_by_type(
    s3_resource: Any,
    bucket_name: str,
    prefix: str,
    agency: str,
    record_types: list[RecordType],
    processed_keys: Any = None,
    verbose: bool = False,
    since_year: int | None = None,
) -> dict[str, list[str]]:
    """List one agency's JSON files in a single pass, bucketed by record type.

    The Mirrulations layout nests every record type under the same agency
    prefix, so calling :func:`list_json_files` once per type re-scans the whole
    (potentially millions of objects) prefix N times. This scans it once and
    classifies each key by which record type's ``path_pattern`` it contains —
    the patterns (``/docket/``, ``/documents/``, ``/comments/``) are mutually
    exclusive, so each key maps to at most one type.
    """
    year_pattern = re.compile(rf"{re.escape(prefix)}/{re.escape(agency)}/{re.escape(agency)}-(\d{{4}})-")
    patterns = [(rt.name, rt.path_pattern) for rt in record_types if rt.path_pattern]
    result: dict[str, list[str]] = {rt.name: [] for rt in record_types}

    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=f"{prefix}/{agency}/"):
        key = obj.key
        if "/text-" not in key or not key.endswith(".json"):
            continue
        matched = next((name for name, pattern in patterns if pattern in key), None)
        if matched is None:
            continue
        if since_year:
            m = year_pattern.search(key)
            if m and int(m.group(1)) < since_year:
                continue
        if processed_keys and key in processed_keys:
            continue
        result[matched].append(key)

    if verbose:
        summary = ", ".join(f"{name} {len(keys)}" for name, keys in result.items())
        tqdm.write(f"    [{agency}] single-scan listing: {summary}")

    return result


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


def discover_agencies() -> list[str]:
    """List every agency present in the mirror."""
    return get_agencies(s3_client(), BUCKET, PREFIX)


def _identity(payload: dict) -> dict:
    """Decode-only 'extract' — the reader yields raw JSON; flattening is a Transform."""
    return payload


class MirrulationsReader(Reader):
    """Reads one agency's records of a single record type from Mirrulations S3.

    Yields the raw JSON payload for each file; the keys discovered during the
    most recent ``iter_records`` call are kept on ``last_keys`` so the caller can
    append them to the run manifest.
    """

    def __init__(
        self,
        s3_resource: Any,
        bucket: str,
        prefix: str,
        agency: str,
        record_type: RecordType,
        processed_keys: Any = None,
        since_year: int | None = None,
        verbose: bool = False,
        download_workers: int = DEFAULT_DOWNLOAD_WORKERS,
        key_lister: Callable[[], list[str]] | None = None,
    ) -> None:
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix
        self.agency = agency
        self.record_type = record_type
        self.processed_keys = processed_keys
        self.since_year = since_year
        self.verbose = verbose
        self.download_workers = download_workers
        # When set, supplies this record type's keys (e.g. from a shared
        # single-scan listing); otherwise the reader lists them itself.
        self.key_lister = key_lister
        self.last_keys: list[str] = []

    def iter_records(self) -> Iterator[dict]:
        if self.record_type.path_pattern is None:
            raise ValueError(
                f"MirrulationsReader requires a path-addressable record type, "
                f"but {self.record_type.name!r} has no path_pattern."
            )
        if self.key_lister is not None:
            self.last_keys = self.key_lister()
        else:
            self.last_keys = list_json_files(
                self.s3_resource,
                self.bucket,
                self.prefix,
                self.agency,
                self.record_type.name,
                self.record_type.path_pattern,
                self.processed_keys,
                self.verbose,
                self.since_year,
            )
        # Fan the per-file GETs out across a thread pool — they are independent,
        # I/O-bound round trips. Order is irrelevant (dedup happens later by key).
        workers = max(1, min(self.download_workers, len(self.last_keys)))
        if workers <= 1:
            for key in self.last_keys:
                payload = download_and_parse(self.s3_resource, self.bucket, key, _identity)
                if payload is not None:
                    yield payload
            return

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(download_and_parse, self.s3_resource, self.bucket, key, _identity)
                for key in self.last_keys
            ]
            for future in as_completed(futures):
                payload = future.result()
                if payload is not None:
                    yield payload


class _AgencyListingCache:
    """Memoizes one single-scan listing per agency, shared across its readers.

    ``stage_agencies`` builds a reader per (agency, record type) and runs an
    agency's record types sequentially within one worker thread, so the first
    reader for an agency triggers the scan and the rest read from the cache.
    Different agencies populate different keys concurrently, guarded by a lock.
    """

    def __init__(
        self,
        record_types: list[RecordType],
        *,
        processed_keys: Any,
        since_year: int | None,
        verbose: bool,
    ) -> None:
        self._record_types = record_types
        self._processed_keys = processed_keys
        self._since_year = since_year
        self._verbose = verbose
        self._by_agency: dict[str, dict[str, list[str]]] = {}
        self._lock = Lock()

    def keys_for(self, s3_resource: Any, agency: str, record_type: RecordType) -> list[str]:
        with self._lock:
            listed = self._by_agency.get(agency)
        if listed is None:
            scanned = list_agency_files_by_type(
                s3_resource,
                BUCKET,
                PREFIX,
                agency,
                self._record_types,
                processed_keys=self._processed_keys,
                verbose=self._verbose,
                since_year=self._since_year,
            )
            with self._lock:
                listed = self._by_agency.setdefault(agency, scanned)
        return listed.get(record_type.name, [])


def reader_factory(
    record_types: list[RecordType],
    *,
    processed_keys: Any = None,
    since_year: int | None = None,
    verbose: bool = False,
    download_workers: int = DEFAULT_DOWNLOAD_WORKERS,
    resource_factory: Callable[[], Any] | None = None,
) -> Callable[[str, RecordType], MirrulationsReader]:
    """Build a ``read(agency, record_type) -> MirrulationsReader`` factory.

    The shared options (manifest membership test, year filter, verbosity) are
    bound once; the orchestrator just supplies the agency and record type. Each
    reader gets its own S3 resource so the factory is safe to call from worker
    threads. The full set of ``record_types`` is bound so each agency's prefix
    is scanned once and the keys bucketed by type, rather than re-scanned per
    record type.
    """
    cache = _AgencyListingCache(record_types, processed_keys=processed_keys, since_year=since_year, verbose=verbose)
    # Resolve at call time (not as a default arg) so a monkeypatched
    # ``mirrulations.s3_resource`` is honored, and each reader still gets its
    # own resource — safe to call from the staging worker threads. ``s3_resource``
    # sizes its connection pool to ``DEFAULT_DOWNLOAD_WORKERS``, which matches the
    # reader's default ``download_workers`` so the pool is never oversubscribed.
    make_resource = resource_factory or s3_resource

    def read(agency: str, record_type: RecordType) -> MirrulationsReader:
        resource = make_resource()
        return MirrulationsReader(
            resource,
            BUCKET,
            PREFIX,
            agency,
            record_type,
            processed_keys=processed_keys,
            since_year=since_year,
            verbose=verbose,
            download_workers=download_workers,
            key_lister=lambda: cache.keys_for(resource, agency, record_type),
        )

    return read

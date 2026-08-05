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
from dataclasses import dataclass, field
from datetime import datetime
from json import loads
from threading import Lock
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from loguru import logger
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

# Emit a download-progress line every this many files, so a large agency's
# multi-hour download reports how far along it is (small agencies finish before
# the first mark and just log their staged total).
_PROGRESS_EVERY = 25_000


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
        config=BotoConfig(
            signature_version=UNSIGNED,
            max_pool_connections=max_pool_connections,
            # Bound every S3 op so a stalled connection fails fast and retries
            # instead of wedging a worker indefinitely; standard mode retries
            # transient errors (throttling, resets) rather than dropping records.
            connect_timeout=30,
            read_timeout=30,
            retries={"max_attempts": 5, "mode": "standard"},
        ),
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


class TransientDownloadError(Exception):
    """S3 GET/read failed (network, throttle) — the key is retryable.

    Raised when the object could not be fetched off S3 at all. The body was
    never (fully) read, so the file may well succeed on a later attempt; the
    caller must therefore *not* record the key as processed, leaving the next
    incremental run free to re-list and re-download it.
    """


class PayloadParseError(Exception):
    """Body downloaded but JSON decode / extract failed — deterministic, not retryable.

    The bytes came off S3 fine; they just don't parse. Re-fetching yields the
    same corrupt payload, so the caller marks the key processed (to stop it
    retrying forever) and logs it for a deliberate replay after a fix.
    """


@dataclass(frozen=True, slots=True)
class DownloadedObject:
    """Exact bytes and source metadata returned by one anonymous S3 GET."""

    content: bytes
    etag: str | None
    last_modified: datetime | None
    content_length: int | None


def download_object_bytes(
    s3_resource: Any,
    bucket_name: str,
    key: str,
    *,
    if_match: str | None = None,
    max_bytes: int | None = None,
) -> DownloadedObject:
    """Read one S3 object exactly, optionally pinned to its listed ETag.

    ``IfMatch`` closes the gap between an immutable draw and a later fetch: if
    the mirror replaces an object after listing, S3 refuses the GET instead of
    handing the caller different bytes under the old key.  The response body
    is closed on every path so concurrent batch readers return connections to
    botocore's pool promptly.
    """

    if max_bytes is not None and max_bytes <= 0:
        raise ValueError("max_bytes must be greater than zero")
    obj = s3_resource.Object(bucket_name, key)
    response = obj.get(**({"IfMatch": if_match} if if_match is not None else {}))
    content_length = response.get("ContentLength")
    if max_bytes is not None and content_length is not None and content_length > max_bytes:
        raise ValueError(f"{key} exceeds the {max_bytes} byte cap")
    body = response["Body"]
    try:
        content = body.read(max_bytes + 1) if max_bytes is not None else body.read()
    finally:
        body.close()
    if max_bytes is not None and len(content) > max_bytes:
        raise ValueError(f"{key} exceeds the {max_bytes} byte cap")
    if content_length is not None and content_length != len(content):
        raise ValueError(f"{key} returned {len(content)} bytes but declared {content_length}")
    etag = response.get("ETag")
    if if_match is not None and etag != if_match:
        raise ValueError(f"{key} returned ETag {etag!r}, expected {if_match!r}")
    return DownloadedObject(
        content=content,
        etag=etag,
        last_modified=response.get("LastModified"),
        content_length=content_length,
    )


@dataclass
class DownloadFailures:
    """Per-key download failures, split by whether they're worth retrying.

    ``transient`` keys are excluded from the manifest so the next run retries
    them for free; ``parse`` keys are recorded as processed but surfaced for a
    deliberate replay. A caller that doesn't care passes nothing (the default
    ``failures=None`` on :func:`download_keys` keeps failures uncollected).
    """

    transient: list[str] = field(default_factory=list)
    parse: list[str] = field(default_factory=list)


def download_and_parse(
    s3_resource: Any,
    bucket_name: str,
    key: str,
    extract_fn: Callable[[dict], dict],
) -> dict:
    """Download a single JSON file from S3 and parse it with the given extractor.

    Failures are classified so the caller can retry the retryable ones: a GET or
    read failure raises :class:`TransientDownloadError` (network/throttle — the
    key may succeed next time), while a JSON-decode or extract failure raises
    :class:`PayloadParseError` (the bytes are deterministically bad). Both wrap
    the original exception and carry the offending key.

    The response body is closed on every path — including a failed read — so a
    transient error can't leak the connection into CLOSE_WAIT and starve the
    pool. Leaked connections were the cause of the low-CPU/CLOSE_WAIT hang seen
    on large agencies: once the pool drained, later downloads blocked forever
    waiting for a free connection.
    """
    try:
        content = download_object_bytes(s3_resource, bucket_name, key).content
    except Exception as exc:
        raise TransientDownloadError(key) from exc
    try:
        return extract_fn(loads(content))
    except Exception as exc:
        raise PayloadParseError(key) from exc


def discover_agencies() -> list[str]:
    """List every agency present in the mirror."""
    return get_agencies(s3_client(), BUCKET, PREFIX)


def _identity(payload: dict) -> dict:
    """Decode-only 'extract' — the reader yields raw JSON; flattening is a Transform."""
    return payload


def _record_failure(exc: Exception, key: str, label: str, failures: DownloadFailures | None) -> None:
    """Log a per-key download failure and, when collecting, bucket it by kind.

    Transient failures are surfaced at ``warning`` because they cost a retry;
    parse failures are deterministic corruption the operator may want to replay.
    A ``TransientDownloadError``/``PayloadParseError`` carries its own key, but
    ``key`` is passed explicitly so the caller stays authoritative.
    """
    where = f"{label}: " if label else ""
    if isinstance(exc, PayloadParseError):
        logger.warning("{}parse failure, marking processed: {}", where, key)
        if failures is not None:
            failures.parse.append(key)
    else:
        logger.warning("{}download failed, will retry: {}", where, key)
        if failures is not None:
            failures.transient.append(key)


def download_keys(
    s3_resource: Any,
    bucket_name: str,
    keys: list[str],
    workers: int = DEFAULT_DOWNLOAD_WORKERS,
    *,
    label: str = "",
    failures: DownloadFailures | None = None,
) -> Iterator[dict]:
    """Concurrently download + parse the given keys, yielding raw payloads.

    The shared download engine for both :class:`MirrulationsReader` (whole
    agency) and the chunked ingest path (one bounded key-chunk at a time, so a
    huge agency never buffers all its records at once). Order is not preserved —
    dedup happens later by key. With ``label`` set, emits a progress line every
    ``_PROGRESS_EVERY`` files.

    When ``failures`` is provided, each key that couldn't be produced is appended
    to it — transient (retryable) vs. parse (deterministic) — instead of being
    silently dropped, so the caller can exclude the retryable ones from the
    manifest. ``failures=None`` keeps the historical drop-and-continue behavior
    for callers that don't track keys.
    """
    total = len(keys)
    n = max(1, min(workers, total)) if total else 1
    if n <= 1:
        for key in keys:
            try:
                yield download_and_parse(s3_resource, bucket_name, key, _identity)
            except (TransientDownloadError, PayloadParseError) as exc:
                _record_failure(exc, key, label, failures)
        return

    done = 0
    with ThreadPoolExecutor(max_workers=n) as executor:
        # Map future -> key so as_completed can attribute an exception to its key.
        submitted = {executor.submit(download_and_parse, s3_resource, bucket_name, key, _identity): key for key in keys}
        for future in as_completed(submitted):
            done += 1
            if label and done % _PROGRESS_EVERY == 0:
                logger.info("{}: downloaded {}/{}", label, done, total)
            try:
                yield future.result()
            except (TransientDownloadError, PayloadParseError) as exc:
                _record_failure(exc, submitted[future], label, failures)


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
        # Keys attempted but not consumed, so the caller can keep them out of the
        # manifest (transient) or surface them for replay (parse).
        self.failed_keys: list[str] = []
        self.parse_failed_keys: list[str] = []

    def iter_records(self) -> Iterator[dict]:
        if self.record_type.path_pattern is None:
            raise ValueError(
                f"MirrulationsReader requires a path-addressable record type, "
                f"but {self.record_type.name!r} has no path_pattern."
            )
        if self.key_lister is not None:
            keys = self.key_lister()
        else:
            keys = list_json_files(
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
        # Populate immediately so a caller inspecting last_keys before the
        # generator is fully drained still sees the listing; it's narrowed to the
        # consumed keys once downloading (and the retry pass) completes.
        self.last_keys = list(keys)

        # Fan the per-file GETs across a thread pool via the shared engine —
        # independent, I/O-bound round trips; order is irrelevant (dedup by key).
        label = f"[{self.agency}] {self.record_type.name}"
        failures = DownloadFailures()
        yield from download_keys(
            self.s3_resource, self.bucket, keys, self.download_workers, label=label, failures=failures
        )
        # One in-run retry pass over transient failures; whatever still fails is
        # left out of last_keys so the next incremental run re-lists it for free.
        if failures.transient:
            retry = DownloadFailures()
            yield from download_keys(
                self.s3_resource,
                self.bucket,
                list(failures.transient),
                self.download_workers,
                label=f"{label} retry",
                failures=retry,
            )
            failures.transient = retry.transient
            failures.parse.extend(retry.parse)

        self.failed_keys = list(failures.transient)
        self.parse_failed_keys = list(failures.parse)
        # Transient failures are excluded (retried next run); parse failures stay
        # marked processed — a deterministically corrupt file would retry forever.
        dropped = set(failures.transient)
        self.last_keys = [k for k in keys if k not in dropped]


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

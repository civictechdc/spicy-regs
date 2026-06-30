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
from json import loads
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


def s3_resource() -> Any:
    """A fresh anonymous S3 resource (one per worker keeps threads independent)."""
    return boto3.resource("s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED))


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
    ) -> None:
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix
        self.agency = agency
        self.record_type = record_type
        self.processed_keys = processed_keys
        self.since_year = since_year
        self.verbose = verbose
        self.last_keys: list[str] = []

    def iter_records(self) -> Iterator[dict]:
        if self.record_type.path_pattern is None:
            raise ValueError(
                f"MirrulationsReader requires a path-addressable record type, "
                f"but {self.record_type.name!r} has no path_pattern."
            )
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
        for key in self.last_keys:
            payload = download_and_parse(self.s3_resource, self.bucket, key, _identity)
            if payload is not None:
                yield payload


def reader_factory(
    *,
    processed_keys: Any = None,
    since_year: int | None = None,
    verbose: bool = False,
) -> Callable[[str, RecordType], MirrulationsReader]:
    """Build a ``read(agency, record_type) -> MirrulationsReader`` factory.

    The shared options (manifest membership test, year filter, verbosity) are
    bound once; the orchestrator just supplies the agency and record type. Each
    reader gets its own S3 resource so the factory is safe to call from worker
    threads.
    """

    def read(agency: str, record_type: RecordType) -> MirrulationsReader:
        return MirrulationsReader(
            s3_resource(), BUCKET, PREFIX, agency, record_type,
            processed_keys=processed_keys, since_year=since_year, verbose=verbose,
        )

    return read

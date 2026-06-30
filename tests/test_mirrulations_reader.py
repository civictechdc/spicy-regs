"""Tests for MirrulationsReader using a fake in-memory S3 resource."""

from json import dumps

from spicy_regs.schemas import COMMENT, DOCKET, DOCUMENT
from spicy_regs.sources import MirrulationsReader

BUCKET = "mirrulations"
PREFIX = "raw-data"
AGENCY = "EPA"


def _docket_payload(docket_id: str) -> dict:
    return {
        "data": {
            "id": docket_id,
            "attributes": {
                "agencyId": "EPA",
                "title": f"Title {docket_id}",
                "docketType": "Rulemaking",
                "modifyDate": "2024-01-01",
                "dkAbstract": "abstract",
            },
        }
    }


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeObj:
    def __init__(self, key: str, content: bytes) -> None:
        self.key = key
        self._content = content

    def get(self) -> dict:
        return {"Body": _FakeBody(self._content)}


class _FakeObjects:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def filter(self, Prefix: str):  # noqa: N803 — mirrors boto3 kwarg
        for key, content in self._store.items():
            if key.startswith(Prefix):
                yield _FakeObj(key, content)


class _FakeBucket:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.objects = _FakeObjects(store)


class _FakeS3Resource:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def Bucket(self, name: str) -> _FakeBucket:  # noqa: N802 — mirrors boto3 API
        return _FakeBucket(self._store)

    def Object(self, name: str, key: str) -> _FakeObj:  # noqa: N802 — mirrors boto3 API
        return _FakeObj(key, self._store[key])


def _docket_key(docket_id: str) -> str:
    return f"{PREFIX}/{AGENCY}/{docket_id}/text-{docket_id}/docket/{docket_id}.json"


def _make_store() -> dict[str, bytes]:
    return {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002")).encode(),
        # Not a docket / not json — must be ignored by list_json_files.
        f"{PREFIX}/{AGENCY}/EPA-2024-0001/text-EPA-2024-0001/comments/c.json": b"{}",
        f"{PREFIX}/{AGENCY}/EPA-2024-0001/binary-EPA-2024-0001/docket/x.pdf": b"x",
    }


def _raw_id(payload: dict) -> str:
    # The reader yields raw JSON; flattening to docket_id is the transform's job.
    return payload["data"]["id"]


def test_iter_records_yields_raw_payloads() -> None:
    reader = MirrulationsReader(_FakeS3Resource(_make_store()), BUCKET, PREFIX, AGENCY, DOCKET)
    records = list(reader.iter_records())

    ids = sorted(_raw_id(r) for r in records)
    assert ids == ["EPA-2024-0001", "EPA-2025-0002"]
    assert all(r["data"]["attributes"]["agencyId"] == "EPA" for r in records)
    # last_keys is populated for manifest tracking and matches what was yielded.
    assert len(reader.last_keys) == 2


def test_processed_keys_are_skipped() -> None:
    already = {_docket_key("EPA-2024-0001")}
    reader = MirrulationsReader(_FakeS3Resource(_make_store()), BUCKET, PREFIX, AGENCY, DOCKET, processed_keys=already)
    records = list(reader.iter_records())
    assert [_raw_id(r) for r in records] == ["EPA-2025-0002"]


def test_since_year_filters_older_dockets() -> None:
    reader = MirrulationsReader(_FakeS3Resource(_make_store()), BUCKET, PREFIX, AGENCY, DOCKET, since_year=2025)
    records = list(reader.iter_records())
    assert [_raw_id(r) for r in records] == ["EPA-2025-0002"]


def test_iter_records_downloads_concurrently() -> None:
    """Downloads for one agency run in parallel, not one-at-a-time.

    A Barrier that only releases once all N downloads are simultaneously in
    flight is a deterministic proof of concurrency: a serial implementation
    can never gather N parties, so the barrier times out, those downloads
    raise, and nothing is yielded. Concurrent downloads all rendezvous and
    every payload comes back.
    """
    import threading

    n = 4
    store = {_docket_key(f"EPA-2024-{i:04d}"): dumps(_docket_payload(f"EPA-2024-{i:04d}")).encode() for i in range(n)}
    barrier = threading.Barrier(n, timeout=5)

    class _BarrierObj(_FakeObj):
        def get(self) -> dict:
            barrier.wait()  # blocks until all N downloads are concurrently in flight
            return super().get()

    class _BarrierResource(_FakeS3Resource):
        def Object(self, name: str, key: str) -> _BarrierObj:  # noqa: N802
            return _BarrierObj(key, self._store[key])

    reader = MirrulationsReader(_BarrierResource(store), BUCKET, PREFIX, AGENCY, DOCKET, download_workers=n)
    records = list(reader.iter_records())

    assert len(records) == n


class _CountingObjects(_FakeObjects):
    """Counts how many times the agency prefix is scanned."""

    def __init__(self, store: dict[str, bytes], scans: list[int]) -> None:
        super().__init__(store)
        self._scans = scans

    def filter(self, Prefix: str):  # noqa: N803 — mirrors boto3 kwarg
        self._scans[0] += 1
        return super().filter(Prefix=Prefix)


class _CountingResource(_FakeS3Resource):
    def __init__(self, store: dict[str, bytes], scans: list[int]) -> None:
        super().__init__(store)
        self._scans = scans

    def Bucket(self, name: str):  # noqa: N802
        bucket = super().Bucket(name)
        bucket.objects = _CountingObjects(self._store, self._scans)
        return bucket


def _typed_store() -> dict[str, bytes]:
    base = f"{PREFIX}/{AGENCY}/EPA-2024-0001/text-EPA-2024-0001"
    return {
        f"{base}/docket/EPA-2024-0001.json": dumps(_docket_payload("EPA-2024-0001")).encode(),
        f"{base}/documents/EPA-2024-0001-0001.json": b'{"data": {}}',
        f"{base}/comments/EPA-2024-0001-0002.json": b'{"data": {}}',
        # Non-JSON / binary — must be ignored.
        f"{base}/binary-EPA-2024-0001/docket/x.pdf": b"x",
    }


def test_single_scan_buckets_keys_by_record_type() -> None:
    """One prefix scan classifies an agency's keys for all record types.

    Replaces the prior behavior of scanning the whole agency prefix once per
    record type (3x). A counting fake proves exactly one scan happens.
    """
    from spicy_regs.sources.mirrulations import list_agency_files_by_type

    scans = [0]
    resource = _CountingResource(_typed_store(), scans)

    result = list_agency_files_by_type(resource, BUCKET, PREFIX, AGENCY, [DOCKET, DOCUMENT, COMMENT])

    assert scans[0] == 1
    assert result["dockets"] == [f"{PREFIX}/{AGENCY}/EPA-2024-0001/text-EPA-2024-0001/docket/EPA-2024-0001.json"]
    assert result["documents"] == [
        f"{PREFIX}/{AGENCY}/EPA-2024-0001/text-EPA-2024-0001/documents/EPA-2024-0001-0001.json"
    ]
    assert result["comments"] == [
        f"{PREFIX}/{AGENCY}/EPA-2024-0001/text-EPA-2024-0001/comments/EPA-2024-0001-0002.json"
    ]


def test_reader_factory_scans_each_agency_once() -> None:
    """The readers a factory builds for one agency share a single prefix scan."""
    from spicy_regs.sources.mirrulations import reader_factory

    scans = [0]
    resource = _CountingResource(_typed_store(), scans)
    read = reader_factory([DOCKET, DOCUMENT, COMMENT], resource_factory=lambda: resource)

    keys_by_type = {}
    for record_type in (DOCKET, DOCUMENT, COMMENT):
        reader = read(AGENCY, record_type)
        list(reader.iter_records())
        keys_by_type[record_type.name] = reader.last_keys

    assert scans[0] == 1  # one scan for the agency, not one per record type
    assert len(keys_by_type["dockets"]) == 1
    assert len(keys_by_type["documents"]) == 1
    assert len(keys_by_type["comments"]) == 1


def test_s3_resource_connection_pool_fits_download_workers() -> None:
    """The S3 resource's HTTP connection pool must be at least as large as the
    download thread pool. Otherwise concurrent GETs oversubscribe a too-small
    pool: connections churn into CLOSE_WAIT and the run stalls (botocore's
    default max_pool_connections is 10, below DEFAULT_DOWNLOAD_WORKERS)."""
    from spicy_regs.sources.mirrulations import DEFAULT_DOWNLOAD_WORKERS, s3_resource

    resource = s3_resource()
    pool_size = resource.meta.client.meta.config.max_pool_connections

    assert pool_size >= DEFAULT_DOWNLOAD_WORKERS

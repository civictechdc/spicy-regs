"""Tests for MirrulationsReader using a fake in-memory S3 resource."""

from json import dumps

from spicy_regs.schemas import DOCKET
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

"""Tests for RegulationsPipeline and the run-pipeline CLI.

The composition test wires the real MirrulationsReader → StagingWriter →
merge transforms together against a fake in-memory S3 resource, so it
exercises the actual source→transform→sink flow without any network.
"""

from json import dumps
from pathlib import Path

import polars as pl
import pytest

import spicy_regs.pipelines.regulations as regulations
from spicy_regs.pipelines import Pipeline, RegulationsPipeline

PREFIX = "raw-data"
AGENCY = "EPA"


# --- contract --------------------------------------------------------------


def test_is_pipeline_subclass_with_name() -> None:
    assert issubclass(RegulationsPipeline, Pipeline)
    assert RegulationsPipeline.name == "regulations"


# --- fake S3 ---------------------------------------------------------------


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


def _docket_payload(docket_id: str, modify_date: str) -> dict:
    return {
        "data": {
            "id": docket_id,
            "attributes": {
                "agencyId": "EPA",
                "title": f"Title {docket_id}",
                "docketType": "Rulemaking",
                "modifyDate": modify_date,
                "dkAbstract": "abstract",
            },
        }
    }


def _docket_key(docket_id: str, tag: str = "a") -> str:
    return f"{PREFIX}/{AGENCY}/{docket_id}/text-{docket_id}-{tag}/docket/{docket_id}.json"


# --- composition -----------------------------------------------------------


def test_run_extracts_stages_and_merges(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        agency=AGENCY,
        output_dir=tmp_output,
        skip_comments=True,
        skip_post_process=True,
        skip_upload=True,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_run_dedups_on_merge_keeping_latest_modify_date(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same docket id seen twice with different modify dates -> one row, latest wins.
    store = {
        _docket_key("EPA-2024-0001", "old"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2024-0001", "new"): dumps(_docket_payload("EPA-2024-0001", "2024-09-09")).encode(),
    }
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_post_process=True, skip_upload=True
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df.height == 1
    assert df["modify_date"].to_list() == ["2024-09-09"]


def test_run_with_no_records_is_noop(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource({}))

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_post_process=True, skip_upload=True
    ).run()

    assert not (tmp_output / "dockets.parquet").exists()


# --- CLI -------------------------------------------------------------------


def test_cli_main_builds_and_runs_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured["kwargs"] = kwargs

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(regulations, "RegulationsPipeline", _FakePipeline)

    regulations.main(agency="EPA", skip_upload=True, since_year=2025)

    assert captured["ran"] is True
    assert captured["kwargs"]["agency"] == "EPA"
    assert captured["kwargs"]["skip_upload"] is True
    assert captured["kwargs"]["since_year"] == 2025

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
from spicy_regs.manifest import Manifest
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


def _docket_payload(docket_id: str, modify_date: str, agency: str = AGENCY) -> dict:
    return {
        "data": {
            "id": docket_id,
            "attributes": {
                "agencyId": agency,
                "title": f"Title {docket_id}",
                "docketType": "Rulemaking",
                "modifyDate": modify_date,
                "dkAbstract": "abstract",
            },
        }
    }


def _docket_key(docket_id: str, tag: str = "a", agency: str = AGENCY) -> str:
    return f"{PREFIX}/{agency}/{docket_id}/text-{docket_id}-{tag}/docket/{docket_id}.json"


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


# --- incremental dedup -----------------------------------------------------


def _run(tmp_output: Path, **overrides) -> None:
    kwargs = dict(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True,
        skip_post_process=True, skip_upload=True,
    )
    kwargs.update(overrides)
    RegulationsPipeline(**kwargs).run()


def test_second_run_skips_keys_already_in_manifest(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource(store))

    # First run stages + merges, and persists the manifest.
    _run(tmp_output)
    assert pl.read_parquet(tmp_output / "dockets.parquet").height == 2
    reloaded = Manifest.load(tmp_output)
    assert _docket_key("EPA-2024-0001") in reloaded

    # Second run: every key is already in the manifest, so nothing is staged
    # and the merge step must not run.
    merge_calls: list = []
    monkeypatch.setattr(regulations, "merge_staging_files", lambda *a, **k: merge_calls.append(1))
    _run(tmp_output)
    assert merge_calls == []


def test_full_refresh_reprocesses_despite_manifest(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    store = {_docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode()}
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource(store))

    _run(tmp_output)  # seeds the manifest

    merge_calls: list = []
    real_merge = regulations.merge_staging_files
    monkeypatch.setattr(
        regulations, "merge_staging_files",
        lambda *a, **k: (merge_calls.append(1), real_merge(*a, **k))[1],
    )
    _run(tmp_output, full_refresh=True)
    assert merge_calls == [1]  # reprocessed even though the key is in the manifest


# --- parallelism -----------------------------------------------------------


def test_processes_multiple_agencies_in_parallel(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setenv("AGENCIES", "EPA,FDA")
    store = {
        _docket_key("EPA-2024-0001", agency="EPA"): dumps(
            _docket_payload("EPA-2024-0001", "2024-01-01", agency="EPA")
        ).encode(),
        _docket_key("FDA-2024-0009", agency="FDA"): dumps(
            _docket_payload("FDA-2024-0009", "2024-02-02", agency="FDA")
        ).encode(),
    }
    monkeypatch.setattr(regulations, "_s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        output_dir=tmp_output, skip_comments=True, skip_post_process=True,
        skip_upload=True, max_workers=2,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "FDA-2024-0009"]


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

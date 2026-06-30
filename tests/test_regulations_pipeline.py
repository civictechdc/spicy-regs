"""Tests for RegulationsPipeline and the run-pipeline CLI.

The composition test wires the real MirrulationsReader → StagingWriter →
merge transforms together against a fake in-memory S3 resource, so it
exercises the actual source→transform→sink flow without any network.
"""

from json import dumps
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import spicy_regs.pipelines.regulations as regulations
import spicy_regs.sources.mirrulations as mirrulations
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


def _comment_payload(comment_id: str, docket_id: str, posted_date: str, agency: str = AGENCY) -> dict:
    return {
        "data": {
            "id": comment_id,
            "attributes": {
                "docketId": docket_id,
                "agencyId": agency,
                "postedDate": posted_date,
                "modifyDate": posted_date,
                "comment": "a comment",
            },
        }
    }


def _comment_key(comment_id: str, docket_id: str, agency: str = AGENCY) -> str:
    return f"{PREFIX}/{agency}/{docket_id}/text-{comment_id}/comments/{comment_id}.json"


# --- composition -----------------------------------------------------------


def test_run_extracts_stages_and_merges(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2025-0002"): dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

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
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_post_process=True, skip_upload=True
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df.height == 1
    assert df["modify_date"].to_list() == ["2024-09-09"]


def test_run_with_no_records_is_noop(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource({}))

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_post_process=True, skip_upload=True
    ).run()

    assert not (tmp_output / "dockets.parquet").exists()


# --- incremental dedup -----------------------------------------------------


def _run(tmp_output: Path, **overrides: Any) -> None:
    kwargs: dict[str, Any] = dict(
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
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

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
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

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
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(
        output_dir=tmp_output, skip_comments=True, skip_post_process=True,
        skip_upload=True, max_workers=2,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "FDA-2024-0009"]


# --- upload ----------------------------------------------------------------


def test_run_uploads_changed_comment_partitions(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that stages comments must publish the changed partitions + index,
    not just the monolithic dataset."""
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)  # keep merge's R2 fetch offline
    store = {
        _comment_key("c1", "EPA-2024-0001"): dumps(
            _comment_payload("c1", "EPA-2024-0001", "2024-01-01T00:00:00Z")
        ).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    calls: dict[str, list] = {}
    monkeypatch.setattr(
        regulations.r2, "upload_dataset",
        lambda out, types: calls.setdefault("dataset", []).append((out, types)),
    )
    monkeypatch.setattr(
        regulations.r2, "upload_comment_partitions",
        lambda out, changed: calls.setdefault("partitions", []).append((out, list(changed))),
    )

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, only_comments=True,
        enrich_text=False, skip_post_process=True, skip_upload=False,
    ).run()

    assert "partitions" in calls, "changed comment partitions were never uploaded"
    out, changed = calls["partitions"][0]
    assert out == tmp_output
    assert changed and all(p.suffix == ".parquet" for p in changed)
    # The dataset upload (manifest etc.) still runs alongside it.
    assert "dataset" in calls


def test_run_skips_partition_upload_when_no_comments(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dockets-only run must not call the comment-partition upload."""
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    store = {
        _docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    calls: dict[str, list] = {}
    monkeypatch.setattr(regulations.r2, "upload_dataset", lambda out, types: calls.setdefault("dataset", []).append(1))
    monkeypatch.setattr(
        regulations.r2, "upload_comment_partitions",
        lambda out, changed: calls.setdefault("partitions", []).append(1),
    )

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, skip_comments=True,
        skip_post_process=True, skip_upload=False,
    ).run()

    assert "dataset" in calls
    assert "partitions" not in calls


def test_run_primes_comments_index_from_r2_before_merge(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incremental comments run must download the existing global index.

    ``update_comments_index`` keeps the rows for partitions this batch didn't
    touch by reading the local ``comments_index.parquet``. If that file is
    never fetched from R2, the rebuilt index collapses to only this batch's
    partitions and the upload shrink-guard aborts the run. Guard against the
    regression by asserting the index is requested during the prime step and
    that pre-existing rows survive the rebuild.
    """
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)  # keep merge's partition fetch offline
    store = {
        _comment_key("c1", "EPA-2024-0001"): dumps(
            _comment_payload("c1", "EPA-2024-0001", "2024-01-01T00:00:00Z")
        ).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "upload_dataset", lambda out, types: None)
    monkeypatch.setattr(regulations.r2, "upload_comment_partitions", lambda out, changed: None)

    # A pre-existing remote index covering a partition this batch won't touch.
    prior = pl.DataFrame(
        {
            "agency_code": ["NOAA"],
            "docket_id": ["NOAA-2020-0009"],
            "year": [2020],
            "month": [5],
            "row_count": [42],
        },
        schema={
            "agency_code": pl.Utf8, "docket_id": pl.Utf8,
            "year": pl.Int64, "month": pl.Int64, "row_count": pl.Int64,
        },
    )

    requested: list[str] = []

    def fake_download(remote_key: str, local_path: Path) -> bool:
        requested.append(remote_key)
        if remote_key == "comments_index.parquet":
            prior.write_parquet(local_path)
            return True
        return False  # partitions are absent on R2 in this test

    monkeypatch.setattr(regulations.r2, "download", fake_download)

    RegulationsPipeline(
        agency=AGENCY, output_dir=tmp_output, only_comments=True,
        enrich_text=False, skip_post_process=True, skip_upload=False,
    ).run()

    assert "comments_index.parquet" in requested, "existing comment index was never fetched from R2"

    index = pl.read_parquet(tmp_output / "comments_index.parquet")
    keys = set(zip(index["agency_code"].to_list(), index["docket_id"].to_list()))
    # The untouched NOAA partition survives the rebuild alongside the new EPA one.
    assert ("NOAA", "NOAA-2020-0009") in keys
    assert ("EPA", "EPA-2024-0001") in keys


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

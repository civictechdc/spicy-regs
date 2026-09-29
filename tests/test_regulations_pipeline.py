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

    def close(self) -> None:
        pass


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


class _RaisingObj:
    """An S3 object whose body read fails — a transient download error."""

    def get(self) -> dict:
        class _Body:
            def read(self) -> bytes:
                raise OSError("connection reset by peer")

            def close(self) -> None:
                pass

        return {"Body": _Body()}


class _FlakyResource(_FakeS3Resource):
    """Fake S3 that fails ``read()`` for ``fail_keys`` while ``fail["active"]``.

    A shared mutable flag lets one test flip the failure off between runs, so the
    same key can fail on run 1 (excluded from the manifest) and succeed on run 2.
    """

    def __init__(self, store: dict[str, bytes], fail_keys: set[str], active: dict[str, bool]) -> None:
        super().__init__(store)
        self._fail_keys = fail_keys
        self._active = active

    def Object(self, name: str, key: str):  # noqa: N802 — mirrors boto3 API
        if self._active["active"] and key in self._fail_keys:
            return _RaisingObj()
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
        skip_upload=True,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_run_dedups_on_merge_keeping_latest_modify_date(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Same docket id seen twice with different modify dates -> one row, latest wins.
    store = {
        _docket_key("EPA-2024-0001", "old"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        _docket_key("EPA-2024-0001", "new"): dumps(_docket_payload("EPA-2024-0001", "2024-09-09")).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    RegulationsPipeline(agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_upload=True).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df.height == 1
    assert df["modify_date"].to_list() == ["2024-09-09"]


def test_chunked_comments_commit_per_chunk(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """With chunk_size set, comments ingest in bounded key-chunks and the catalog
    MERGE runs once per chunk (so a huge agency commits in pieces, never buffering
    everything in memory)."""
    # 5 comment files, chunk_size 2 -> chunks [2,2,1] -> 3 merges.
    store = {
        _comment_key(f"c{i}", "EPA-2026-0001"): dumps(
            _comment_payload(f"c{i}", "EPA-2026-0001", "2026-01-01T00:00:00Z")
        ).encode()
        for i in range(5)
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    merge_calls: list = []
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, od, rt: merge_calls.append(1))

    RegulationsPipeline(
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        chunk_size=2,
        skip_upload=True,
    ).run()

    assert len(merge_calls) == 3  # ceil(5 / 2)


def test_run_with_no_records_is_noop(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource({}))

    RegulationsPipeline(agency=AGENCY, output_dir=tmp_output, skip_comments=True, skip_upload=True).run()

    assert not (tmp_output / "dockets.parquet").exists()


# --- incremental dedup -----------------------------------------------------


def _run(tmp_output: Path, **overrides: Any) -> None:
    kwargs: dict[str, Any] = dict(
        agency=AGENCY,
        output_dir=tmp_output,
        skip_comments=True,
        skip_upload=True,
    )
    kwargs.update(overrides)
    RegulationsPipeline(**kwargs).run()


def test_second_run_skips_keys_already_in_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_full_refresh_reprocesses_despite_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {_docket_key("EPA-2024-0001"): dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode()}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    _run(tmp_output)  # seeds the manifest

    merge_calls: list = []
    real_merge = regulations.merge_staging_files
    monkeypatch.setattr(
        regulations,
        "merge_staging_files",
        lambda *a, **k: (merge_calls.append(1), real_merge(*a, **k))[1],
    )
    _run(tmp_output, full_refresh=True)
    assert merge_calls == [1]  # reprocessed even though the key is in the manifest


# --- failed-key handling ---------------------------------------------------


def test_failed_download_is_not_committed_to_manifest_and_retries_next_run(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient download failure must not be marked processed, so the next
    incremental run re-lists and re-downloads it."""
    good = _docket_key("EPA-2024-0001")
    flaky = _docket_key("EPA-2025-0002")
    store = {
        good: dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        flaky: dumps(_docket_payload("EPA-2025-0002", "2025-01-01")).encode(),
    }
    active = {"active": True}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FlakyResource(store, {flaky}, active))

    # Run 1: the flaky key fails every attempt -> only the good docket lands.
    _run(tmp_output)
    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert df["docket_id"].to_list() == ["EPA-2024-0001"]
    reloaded = Manifest.load(tmp_output)
    assert good in reloaded
    assert flaky not in reloaded  # excluded so the next run retries it

    # Run 2: failure cleared -> the previously-failed key is re-listed and lands.
    active["active"] = False
    _run(tmp_output)
    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "EPA-2025-0002"]


def test_parse_failure_is_committed_to_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A deterministically corrupt file is marked processed (so it doesn't retry
    forever) and recorded in failed_keys.parquet with kind=parse."""
    good = _docket_key("EPA-2024-0001")
    corrupt = _docket_key("EPA-2025-0002")
    store = {
        good: dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode(),
        corrupt: b"{ broken json",
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    _run(tmp_output)

    reloaded = Manifest.load(tmp_output)
    assert corrupt in reloaded  # parse failure stays processed
    assert good in reloaded

    failed = pl.read_parquet(tmp_output / "failed_keys.parquet")
    row = failed.filter(pl.col("key") == corrupt)
    assert row.height == 1
    assert row["kind"].to_list() == ["parse"]


def test_chunked_comments_exclude_failed_keys_from_manifest(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The chunked ingest path excludes transient failures from the manifest too."""
    keys = {f"c{i}": _comment_key(f"c{i}", "EPA-2026-0001") for i in range(3)}
    store = {
        keys[f"c{i}"]: dumps(_comment_payload(f"c{i}", "EPA-2026-0001", "2026-01-01T00:00:00Z")).encode()
        for i in range(3)
    }
    flaky = keys["c1"]
    active = {"active": True}
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FlakyResource(store, {flaky}, active))
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, od, rt: None)

    pipe = RegulationsPipeline(
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        chunk_size=2,
        skip_upload=True,
    )
    staging_dir = tmp_output / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    manifest = Manifest.empty()
    pipe._ingest_comments_chunked(AGENCY, tmp_output, staging_dir, manifest)

    recorded = manifest.new_keys
    assert flaky not in recorded  # excluded -> retried next run
    assert keys["c0"] in recorded
    assert keys["c2"] in recorded
    # The transient failure is surfaced in the local diagnostic.
    failed = pl.read_parquet(tmp_output / "failed_keys.parquet")
    assert flaky in failed["key"].to_list()


# --- parallelism -----------------------------------------------------------


def test_processes_multiple_agencies_in_parallel(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
        output_dir=tmp_output,
        skip_comments=True,
        skip_upload=True,
        max_workers=2,
    ).run()

    df = pl.read_parquet(tmp_output / "dockets.parquet")
    assert sorted(df["docket_id"].to_list()) == ["EPA-2024-0001", "FDA-2024-0009"]


# --- upload ----------------------------------------------------------------


def test_comments_without_iceberg_are_refused(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Comments have no non-catalog write path: a run that includes them without
    use_iceberg must fail before touching the source, not write the orphaned
    agency_code=/docket_id=/year=/month= tree (#201)."""
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: pytest.fail("source read before the guard"))

    with pytest.raises(ValueError, match="--use-iceberg"):
        RegulationsPipeline(agency=AGENCY, output_dir=tmp_output, only_comments=True, skip_upload=True).run()
    assert not (tmp_output / "comments").exists()


def test_run_uploads_only_comments_index_for_iceberg_comments(
    tmp_output: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A comments run publishes the catalog-rebuilt index and no partition files."""
    store = {
        _comment_key("c1", "EPA-2024-0001"): dumps(
            _comment_payload("c1", "EPA-2024-0001", "2024-01-01T00:00:00Z")
        ).encode(),
    }
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource(store))

    def fake_merge_comments(staging_dir: Path, output_dir: Path, rt: Any) -> None:
        (output_dir / "comments_index.parquet").write_bytes(b"index")

    monkeypatch.setattr(regulations.iceberg, "merge_comments", fake_merge_comments)
    monkeypatch.setattr(regulations.r2, "upload_dataset", lambda out, types: None)
    uploaded: list[str | None] = []
    monkeypatch.setattr(regulations.r2, "upload_file", lambda path, remote_key=None: uploaded.append(remote_key))
    monkeypatch.setattr(
        regulations.r2, "upload_comment_partitions", lambda out, changed: pytest.fail("partitions uploaded")
    )

    RegulationsPipeline(
        agency=AGENCY,
        output_dir=tmp_output,
        only_comments=True,
        use_iceberg=True,
        enrich_text=False,
        skip_upload=False,
    ).run()

    assert uploaded == ["comments_index.parquet"]


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

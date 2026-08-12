"""Materialized-dataset DAG and atomic-publication contract tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import RunContext
from spicy_regs.pipelines.materialized import DatasetStage, MaterializedDatasetPipeline
from spicy_regs.sources import r2


def _write_parquet(path: Path, values: list[str]) -> None:
    pq.write_table(pa.table({"value": values}), path)


def test_materialized_stages_are_topologically_ordered(tmp_path) -> None:
    calls: list[str] = []

    def source(output_dir: Path, context: RunContext) -> None:
        calls.append(f"source:{context.run_id}")
        _write_parquet(output_dir / "source.parquet", ["source"])

    def derived(output_dir: Path, context: RunContext) -> None:
        calls.append(f"derived:{context.run_id}")
        assert (output_dir / "source.parquet").exists()
        _write_parquet(output_dir / "derived.parquet", ["derived"])

    class ExampleDataset(MaterializedDatasetPipeline):
        name = "example-dataset"
        dataset_name = "example"
        published_outputs = ("derived.parquet",)

        def stages(self):
            # Intentionally declared in reverse order: the DAG owns execution.
            return (
                DatasetStage("derived", ("source",), ("derived.parquet",), derived),
                DatasetStage("source", (), ("source.parquet",), source),
            )

    ExampleDataset(
        output_dir=tmp_path,
        run_id="dag-test",
        asserted_at="2026-07-23T12:00:00Z",
    ).run()

    assert calls == ["source:dag-test", "derived:dag-test"]
    manifest = json.loads((tmp_path / "example-dataset-manifest.json").read_text())
    assert [stage["name"] for stage in manifest["stages"]] == ["source", "derived"]
    assert manifest["artifacts"]["derived.parquet"]["sha256"]
    assert manifest["artifacts"]["derived.parquet"]["rows"] == 1


def test_materialized_stage_cycle_fails_before_build(tmp_path) -> None:
    class CyclicDataset(MaterializedDatasetPipeline):
        name = "cyclic-dataset"
        dataset_name = "cyclic"
        published_outputs = ("a.parquet",)

        def stages(self):
            def noop(output_dir, context):
                del output_dir, context

            return (
                DatasetStage("a", ("b",), ("a.parquet",), noop),
                DatasetStage("b", ("a",), ("b.parquet",), noop),
            )

    with pytest.raises(RuntimeError, match="stage cycle"):
        CyclicDataset(output_dir=tmp_path).run()


def test_materialized_publication_commits_pointer_last(tmp_path, monkeypatch) -> None:
    def build(output_dir: Path, context: RunContext) -> None:
        del context
        _write_parquet(output_dir / "one.parquet", ["one"])
        _write_parquet(output_dir / "two.parquet", ["two"])

    class PublishedDataset(MaterializedDatasetPipeline):
        name = "published-dataset"
        dataset_name = "published"
        published_outputs = ("one.parquet", "two.parquet")

        def stages(self):
            return (
                DatasetStage(
                    "build",
                    (),
                    ("one.parquet", "two.parquet"),
                    build,
                ),
            )

    uploads: list[tuple[str, bool]] = []

    def record_upload(path, remote_key=None, *, allow_shrink=False):
        assert path.exists()
        uploads.append((remote_key or path.name, allow_shrink))

    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ENDPOINT", "https://test.r2.example")
    monkeypatch.setattr(r2, "upload_file", record_upload)
    PublishedDataset(
        output_dir=tmp_path,
        skip_upload=False,
        run_id="publish-test",
        asserted_at="2026-07-23T12:00:00Z",
    ).run()

    assert uploads[-1] == ("materialized/published/latest.json", True)
    assert uploads[-2][0].endswith("/manifest.json")
    assert uploads[0][0].endswith("/one.parquet")
    assert uploads[1][0].endswith("/two.parquet")
    snapshot_prefixes = {key.rsplit("/", 1)[0] for key, _ in uploads[:-1]}
    assert len(snapshot_prefixes) == 1


def test_internal_artifact_is_generation_bound_but_not_public(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def build(output_dir: Path, context: RunContext) -> None:
        del context
        _write_parquet(output_dir / "public.parquet", ["public"])
        _write_parquet(output_dir / "internal.parquet", ["internal"])

    class InternalDataset(MaterializedDatasetPipeline):
        name = "internal-dataset"
        dataset_name = "internal"
        published_outputs = ("public.parquet",)
        internal_outputs = ("internal.parquet",)

        def stages(self):
            return (
                DatasetStage(
                    "build",
                    (),
                    ("public.parquet", "internal.parquet"),
                    build,
                ),
            )

    uploads: list[str] = []
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ENDPOINT", "https://test.r2.example")
    monkeypatch.setattr(
        r2,
        "upload_file",
        lambda path, remote_key=None, allow_shrink=False: (
            uploads.append(str(remote_key or path.name))
        ),
    )

    InternalDataset(
        output_dir=tmp_path,
        skip_upload=False,
        run_id="internal-test",
        asserted_at="2026-07-24T12:00:00Z",
    ).run()

    manifest = json.loads(
        (tmp_path / "internal-dataset-manifest.json").read_text()
    )
    assert InternalDataset.published_outputs == ("public.parquet",)
    assert set(InternalDataset.generation_outputs()) == {
        "public.parquet",
        "internal.parquet",
    }
    assert manifest["artifacts"]["public.parquet"]["visibility"] == (
        "public"
    )
    assert manifest["artifacts"]["internal.parquet"]["visibility"] == (
        "internal"
    )
    assert any(key.endswith("/internal.parquet") for key in uploads)


def test_materialized_publication_validates_before_first_upload(
    tmp_path,
    monkeypatch,
) -> None:
    def build(output_dir: Path, context: RunContext) -> None:
        del context
        _write_parquet(output_dir / "result.parquet", ["result"])

    class ValidatedDataset(MaterializedDatasetPipeline):
        name = "validated-dataset"
        dataset_name = "validated"
        published_outputs = ("result.parquet",)

        def stages(self):
            return (
                DatasetStage(
                    "build",
                    (),
                    ("result.parquet",),
                    build,
                ),
            )

        def validate_before_publish(self, manifest_path: Path) -> None:
            assert manifest_path.exists()
            raise RuntimeError("semantic gate failed")

    uploads: list[Path] = []
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ENDPOINT", "https://test.r2.example")
    monkeypatch.setattr(
        r2,
        "upload_file",
        lambda path, remote_key=None, allow_shrink=False: uploads.append(path),
    )

    with pytest.raises(RuntimeError, match="semantic gate failed"):
        ValidatedDataset(output_dir=tmp_path, skip_upload=False).run()

    assert uploads == []


def test_local_publication_runs_the_semantic_gate_in_position(tmp_path) -> None:
    """A skip-upload run must execute validate_before_publish, not bypass it."""

    def build(output_dir: Path, context: RunContext) -> None:
        del context
        _write_parquet(output_dir / "result.parquet", ["result"])

    gate_calls: list[Path] = []

    class LocallyValidatedDataset(MaterializedDatasetPipeline):
        name = "locally-validated-dataset"
        dataset_name = "locally-validated"
        published_outputs = ("result.parquet",)

        def stages(self):
            return (
                DatasetStage(
                    "build",
                    (),
                    ("result.parquet",),
                    build,
                ),
            )

        def validate_before_publish(self, manifest_path: Path) -> None:
            assert manifest_path.exists()
            gate_calls.append(manifest_path)

    LocallyValidatedDataset(output_dir=tmp_path, skip_upload=True).run()
    assert len(gate_calls) == 1

    class LocallyFailingDataset(LocallyValidatedDataset):
        name = "locally-failing-dataset"
        dataset_name = "locally-failing"

        def validate_before_publish(self, manifest_path: Path) -> None:
            raise RuntimeError("semantic gate failed locally")

    with pytest.raises(RuntimeError, match="semantic gate failed locally"):
        LocallyFailingDataset(output_dir=tmp_path / "failing", skip_upload=True).run()


def test_materialized_publication_requires_complete_r2_configuration(tmp_path, monkeypatch) -> None:
    class PublishedDataset(MaterializedDatasetPipeline):
        name = "published-dataset"
        dataset_name = "published"

        def stages(self):
            return ()

    for name in (
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT",
        "R2_PUBLIC_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="missing R2 configuration"):
        PublishedDataset(output_dir=tmp_path, skip_upload=False).run()


def test_publishing_refreshes_existing_local_source(tmp_path, monkeypatch) -> None:
    class SourceDataset(MaterializedDatasetPipeline):
        name = "source-dataset"
        dataset_name = "source"
        source_inputs = ("source.parquet",)

        def stages(self):
            return ()

    source = tmp_path / "source.parquet"
    source.write_bytes(b"stale")
    downloads: list[str] = []

    def download(remote_key, local_path):
        downloads.append(remote_key)
        local_path.write_bytes(b"remote")
        return True

    monkeypatch.setattr(r2, "download", download)
    SourceDataset(output_dir=tmp_path, skip_upload=False)._prime_sources(tmp_path)

    assert downloads == ["source.parquet"]
    assert source.read_bytes() == b"remote"


def test_dry_run_may_reuse_existing_local_source(tmp_path, monkeypatch) -> None:
    class SourceDataset(MaterializedDatasetPipeline):
        name = "source-dataset"
        dataset_name = "source"
        source_inputs = ("source.parquet",)

        def stages(self):
            return ()

    source = tmp_path / "source.parquet"
    source.write_bytes(b"fixture")

    def unexpected_download(remote_key, local_path):
        pytest.fail(f"unexpected download of {remote_key} to {local_path}")

    monkeypatch.setattr(r2, "download", unexpected_download)
    SourceDataset(output_dir=tmp_path)._prime_sources(tmp_path)

    assert source.read_bytes() == b"fixture"


def test_materialized_source_schema_must_include_declared_columns(tmp_path) -> None:
    class SourceDataset(MaterializedDatasetPipeline):
        name = "source-dataset"
        dataset_name = "source"
        source_inputs = ("source.parquet",)

        def stages(self):
            return ()

        def source_column_requirements(self):
            return {"source.parquet": ("id", "required_value")}

    pq.write_table(
        pa.table({"id": ["one"]}),
        tmp_path / "source.parquet",
    )

    with pytest.raises(RuntimeError, match="required_value"):
        SourceDataset(output_dir=tmp_path)._validate_source_schemas(tmp_path)


def test_snapshot_id_changes_when_artifact_bytes_change(tmp_path) -> None:
    payload = ["first"]

    def build(output_dir: Path, context: RunContext) -> None:
        del context
        _write_parquet(output_dir / "result.parquet", [payload[0]])

    class ContentAddressedDataset(MaterializedDatasetPipeline):
        name = "content-addressed-dataset"
        dataset_name = "content-addressed"
        published_outputs = ("result.parquet",)

        def stages(self):
            return (DatasetStage("build", (), ("result.parquet",), build),)

    pipeline = ContentAddressedDataset(
        output_dir=tmp_path,
        run_id="same-run",
        asserted_at="2026-07-23T12:00:00Z",
    )
    pipeline.run()
    first = json.loads((tmp_path / "content-addressed-dataset-manifest.json").read_text())["snapshot_id"]

    payload[0] = "second"
    pipeline.run()
    second = json.loads((tmp_path / "content-addressed-dataset-manifest.json").read_text())["snapshot_id"]

    assert second != first


def test_unknown_materialized_dependency_is_rejected() -> None:
    stage = DatasetStage(
        "known",
        ("missing",),
        ("known.parquet",),
        lambda output_dir, context: None,
    )
    with pytest.raises(RuntimeError, match="unknown stage dependencies"):
        MaterializedDatasetPipeline._ordered_stages((stage,))


def test_prior_generation_artifact_hash_is_verified(tmp_path) -> None:
    class StatefulDataset(MaterializedDatasetPipeline):
        name = "stateful-dataset"
        dataset_name = "stateful"
        prior_outputs = (("state.parquet", "_state_prior.parquet"),)

        def stages(self):
            return ()

    snapshot_id = "snapshot_prior"
    prefix = f"materialized/stateful/snapshots/{snapshot_id}"
    (tmp_path / "_stateful_latest.json").write_text(
        json.dumps(
            {
                "format_version": 1,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "manifest_key": f"{prefix}/manifest.json",
            }
        )
    )
    prior = tmp_path / "_state_prior.parquet"
    prior.write_bytes(b"prior-state")
    (tmp_path / "_stateful_previous_manifest.json").write_text(
        json.dumps(
            {
                "format_version": 1,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "artifacts": {
                    "state.parquet": {
                        "remote_key": f"{prefix}/state.parquet",
                        "sha256": hashlib.sha256(b"different-state").hexdigest(),
                    }
                },
            }
        )
    )

    with pytest.raises(RuntimeError, match="SHA-256"):
        StatefulDataset(output_dir=tmp_path)._prime_previous_generation(tmp_path)
    assert not prior.exists()


def test_publishing_refreshes_cached_prior_generation(tmp_path, monkeypatch) -> None:
    class StatefulDataset(MaterializedDatasetPipeline):
        name = "stateful-dataset"
        dataset_name = "stateful"
        prior_outputs = (("state.parquet", "_state_prior.parquet"),)

        def stages(self):
            return ()

    snapshot_id = "snapshot_remote"
    prefix = f"materialized/stateful/snapshots/{snapshot_id}"
    pointer_path = tmp_path / "_stateful_latest.json"
    manifest_path = tmp_path / "_stateful_previous_manifest.json"
    prior_path = tmp_path / "_state_prior.parquet"
    pointer_path.write_text("{}")
    manifest_path.write_text("{}")
    prior_path.write_bytes(b"stale")
    remote_prior = b"remote-prior"
    downloads: list[str] = []

    def download(remote_key, local_path):
        downloads.append(remote_key)
        if remote_key == "materialized/stateful/latest.json":
            value = {
                "format_version": 1,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "manifest_key": f"{prefix}/manifest.json",
            }
            local_path.write_text(json.dumps(value))
        elif remote_key == f"{prefix}/manifest.json":
            value = {
                "format_version": 1,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "artifacts": {
                    "state.parquet": {
                        "remote_key": f"{prefix}/state.parquet",
                        "sha256": hashlib.sha256(remote_prior).hexdigest(),
                    }
                },
            }
            local_path.write_text(json.dumps(value))
        elif remote_key == f"{prefix}/state.parquet":
            local_path.write_bytes(remote_prior)
        else:
            pytest.fail(f"unexpected download: {remote_key}")
        return True

    monkeypatch.setattr(r2, "download", download)
    manifest = StatefulDataset(
        output_dir=tmp_path,
        skip_upload=False,
    )._prime_previous_generation(tmp_path)

    assert downloads == [
        "materialized/stateful/latest.json",
        f"{prefix}/manifest.json",
        f"{prefix}/state.parquet",
    ]
    assert manifest is not None
    assert manifest["snapshot_id"] == snapshot_id
    assert prior_path.read_bytes() == remote_prior


def test_publishing_discards_cached_prior_when_remote_has_none(tmp_path, monkeypatch) -> None:
    class StatefulDataset(MaterializedDatasetPipeline):
        name = "stateful-dataset"
        dataset_name = "stateful"
        prior_outputs = (("state.parquet", "_state_prior.parquet"),)

        def stages(self):
            return ()

    pointer_path = tmp_path / "_stateful_latest.json"
    manifest_path = tmp_path / "_stateful_previous_manifest.json"
    prior_path = tmp_path / "_state_prior.parquet"
    pointer_path.write_text("{}")
    manifest_path.write_text("{}")
    prior_path.write_bytes(b"stale")

    monkeypatch.setattr(r2, "download", lambda remote_key, local_path: False)
    manifest = StatefulDataset(
        output_dir=tmp_path,
        skip_upload=False,
        allow_bootstrap=True,
    )._prime_previous_generation(tmp_path)

    assert manifest is None
    assert not pointer_path.exists()
    assert not manifest_path.exists()
    assert not prior_path.exists()


def test_publishing_refuses_implicit_state_reset(tmp_path, monkeypatch) -> None:
    class StatefulDataset(MaterializedDatasetPipeline):
        name = "stateful-dataset"
        dataset_name = "stateful"
        prior_outputs = (("state.parquet", "_state_prior.parquet"),)

        def stages(self):
            return ()

    monkeypatch.setattr(r2, "download", lambda remote_key, local_path: False)

    with pytest.raises(RuntimeError, match="refusing an implicit state reset"):
        StatefulDataset(
            output_dir=tmp_path,
            skip_upload=False,
        )._prime_previous_generation(tmp_path)


# The snapshot schema version bump. Version 2 makes ``visibility`` required on
# every artifact record; version 1 predates the field and stays readable, so a
# dataset whose last generation was sealed at version 1 can still restore.


def test_the_build_writes_the_current_snapshot_version_and_declares_visibility(tmp_path) -> None:
    class VisibleDataset(MaterializedDatasetPipeline):
        name = "visible-dataset"
        dataset_name = "visible"
        published_outputs = ("public.parquet",)
        internal_outputs = ("private.parquet",)

        def stages(self):
            return (
                DatasetStage(
                    name="emit",
                    depends_on=(),
                    outputs=("public.parquet", "private.parquet"),
                    build=lambda output_dir, _context: [
                        pq.write_table(pa.table({"a": [1]}), output_dir / "public.parquet"),
                        pq.write_table(pa.table({"a": [2]}), output_dir / "private.parquet"),
                    ],
                ),
            )

    VisibleDataset(output_dir=tmp_path).run()

    manifest = json.loads((tmp_path / "visible-dataset-manifest.json").read_text())
    pointer = json.loads((tmp_path / "visible-dataset-latest.json").read_text())

    assert manifest["format_version"] == 2
    assert pointer["format_version"] == 2
    assert manifest["artifacts"]["public.parquet"]["visibility"] == "public"
    assert manifest["artifacts"]["private.parquet"]["visibility"] == "internal"
    prefix = f"materialized/visible/snapshots/{manifest['snapshot_id']}/"
    assert manifest["artifacts"]["public.parquet"]["remote_key"] == f"{prefix}public.parquet"


def _stateful_generation(tmp_path, *, format_version: int, artifact: dict) -> tuple[type, Path]:
    class StatefulDataset(MaterializedDatasetPipeline):
        name = "stateful-dataset"
        dataset_name = "stateful"
        prior_outputs = (("state.parquet", "_state_prior.parquet"),)

        def stages(self):
            return ()

    snapshot_id = "snapshot_versioned"
    prefix = f"materialized/stateful/snapshots/{snapshot_id}"
    (tmp_path / "_stateful_latest.json").write_text(
        json.dumps(
            {
                "format_version": format_version,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "manifest_key": f"{prefix}/manifest.json",
            }
        )
    )
    prior = tmp_path / "_state_prior.parquet"
    prior.write_bytes(b"prior-state")
    (tmp_path / "_stateful_previous_manifest.json").write_text(
        json.dumps(
            {
                "format_version": format_version,
                "dataset": "stateful",
                "snapshot_id": snapshot_id,
                "artifacts": {"state.parquet": {"remote_key": f"{prefix}/state.parquet", **artifact}},
            }
        )
    )
    return StatefulDataset, prior


def test_a_version_2_generation_missing_visibility_is_refused(tmp_path) -> None:
    dataset, _ = _stateful_generation(
        tmp_path,
        format_version=2,
        artifact={"sha256": hashlib.sha256(b"prior-state").hexdigest()},
    )

    with pytest.raises(RuntimeError, match=r"format version 2 is missing \['visibility'\]"):
        dataset(output_dir=tmp_path)._prime_previous_generation(tmp_path)


def test_a_version_1_generation_restores_without_the_field(tmp_path) -> None:
    """Restoring state from a generation sealed before the field existed."""
    dataset, prior = _stateful_generation(
        tmp_path,
        format_version=1,
        artifact={"sha256": hashlib.sha256(b"prior-state").hexdigest()},
    )

    manifest = dataset(output_dir=tmp_path)._prime_previous_generation(tmp_path)

    assert manifest is not None
    assert manifest["format_version"] == 1
    assert prior.read_bytes() == b"prior-state"


def test_a_generation_at_an_unknown_version_is_refused(tmp_path) -> None:
    dataset, _ = _stateful_generation(
        tmp_path,
        format_version=99,
        artifact={"sha256": hashlib.sha256(b"prior-state").hexdigest(), "visibility": "internal"},
    )

    with pytest.raises(RuntimeError, match="Unsupported stateful pointer snapshot format version 99"):
        dataset(output_dir=tmp_path)._prime_previous_generation(tmp_path)

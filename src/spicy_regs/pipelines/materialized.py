"""Contract for multi-stage, multi-artifact materialized datasets.

Ordinary :class:`~spicy_regs.pipelines.rollups.base.RollupPipeline` jobs publish
one independently schedulable artifact.  This module owns the different case:
derived tables that depend on one another, share prior state, or must become
visible as one coherent generation.

Each run:

1. downloads every source input once into a local snapshot;
2. restores state from one previously published dataset generation;
3. executes an explicit, cycle-checked stage DAG against those local files; and
4. uploads immutable versioned artifacts before atomically replacing one small
   ``latest.json`` pointer.

Readers that resolve the pointer see either the complete prior generation or the
complete new generation. A failed build or partial upload never exposes a mixed
set of tables.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from typing import ClassVar

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.common import RunContext, canonical_json, stable_id
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.sources import r2

_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SAFE_SNAPSHOT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_FORMAT_VERSION = 1
_ROOT_PREFIX = "materialized"


@dataclass(frozen=True)
class DatasetStage:
    """One node in a materialized-dataset build graph."""

    name: str
    depends_on: tuple[str, ...]
    outputs: tuple[str, ...]
    build: Callable[[Path, RunContext], None]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_record(path: Path) -> dict[str, int | str]:
    return {
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid materialized-dataset JSON at {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Materialized-dataset JSON at {path} must be an object")
    return value


def _safe_remote_key(value: object, *, prefix: str) -> str:
    key = str(value or "")
    if (
        not key.startswith(prefix)
        or key.startswith("/")
        or "://" in key
        or any(part in {"", ".", ".."} for part in key.split("/"))
        or any(char in key for char in ("\\", "\x00", "\n", "\r"))
    ):
        raise RuntimeError(f"Unsafe materialized-dataset object key: {key!r}")
    return key


class MaterializedDatasetPipeline(Pipeline):
    """Build and atomically publish a versioned set of related artifacts."""

    dataset_name: ClassVar[str]
    source_inputs: ClassVar[tuple[str, ...]] = ()
    prior_outputs: ClassVar[tuple[tuple[str, str], ...]] = ()
    published_outputs: ClassVar[tuple[str, ...]] = ()

    def __init__(
        self,
        *,
        output_dir: Path | None = None,
        skip_upload: bool = True,
        allow_bootstrap: bool = False,
        run_id: str | None = None,
        asserted_at: str | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.skip_upload = skip_upload
        self.allow_bootstrap = allow_bootstrap
        self.run_id = run_id
        self.asserted_at = asserted_at

    @abstractmethod
    def stages(self) -> tuple[DatasetStage, ...]:
        """Return the dataset's build graph; declaration order breaks DAG ties."""
        ...

    def prepare(
        self,
        output_dir: Path,
        *,
        previous_manifest: dict | None,
        context: RunContext,
    ) -> None:
        """Optional hook after inputs are restored and before stages execute."""

    def source_column_requirements(self) -> dict[str, tuple[str, ...]]:
        """Return required Parquet columns by source key."""
        return {}

    def run(self) -> None:
        if not _SAFE_NAME.fullmatch(self.dataset_name):
            raise RuntimeError(f"Invalid materialized dataset name: {self.dataset_name!r}")
        self._validate_publication_environment()
        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)
        context = RunContext.resolve(
            run_id=self.run_id,
            asserted_at=self.asserted_at,
            prefix=self.dataset_name,
        )

        self._prime_sources(output_dir)
        self._validate_source_schemas(output_dir)
        previous_manifest = self._prime_previous_generation(output_dir)
        input_snapshot = self._input_snapshot(output_dir, previous_manifest)
        self.prepare(
            output_dir,
            previous_manifest=previous_manifest,
            context=context,
        )

        ordered = self._ordered_stages(self.stages())
        for stage in ordered:
            logger.info("Materializing {} stage {}...", self.dataset_name, stage.name)
            stage.build(output_dir, context)
            missing = [name for name in stage.outputs if not (output_dir / name).exists()]
            if missing:
                raise RuntimeError(f"Materialized stage {stage.name!r} did not produce: {', '.join(missing)}")

        missing_outputs = [name for name in self.published_outputs if not (output_dir / name).exists()]
        if missing_outputs:
            raise RuntimeError(
                f"Materialized dataset {self.dataset_name!r} is incomplete: {', '.join(missing_outputs)}"
            )

        manifest_path, pointer_path, artifact_paths = self._write_publication_files(
            output_dir,
            context=context,
            stages=ordered,
            input_snapshot=input_snapshot,
        )
        if self.skip_upload:
            logger.info(
                "skip_upload=True — materialized dataset {} left at {}",
                self.dataset_name,
                output_dir,
            )
            return
        self.validate_before_publish(manifest_path)
        self._publish(
            manifest_path=manifest_path,
            pointer_path=pointer_path,
            artifact_paths=artifact_paths,
        )

    def validate_before_publish(self, manifest_path: Path) -> None:
        """Run dataset-specific semantic gates before the first remote write."""

    def _validate_publication_environment(self) -> None:
        """Reject a purported publication before building if R2 is incomplete."""
        if self.skip_upload:
            return
        required = [
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_ENDPOINT",
        ]
        if self.source_inputs or self.prior_outputs:
            required.append("R2_PUBLIC_URL")
        missing = [name for name in required if not getenv(name)]
        if missing:
            raise RuntimeError(
                f"Cannot publish materialized dataset {self.dataset_name!r}; "
                f"missing R2 configuration: {', '.join(missing)}"
            )

    def _prime_sources(self, output_dir: Path) -> None:
        """Capture each source once; all DAG stages read the same local bytes."""
        for remote_key in self.source_inputs:
            local = output_dir / remote_key
            if self.skip_upload and local.exists():
                logger.info("Using local source snapshot {}", remote_key)
                continue
            if not r2.download(remote_key, local):
                raise RuntimeError(
                    f"Materialized dataset {self.dataset_name!r}: required source {remote_key!r} is not published"
                )

    def _validate_source_schemas(self, output_dir: Path) -> None:
        """Fail before materialization when a declared Parquet contract is stale."""
        for remote_key, required in self.source_column_requirements().items():
            if remote_key not in self.source_inputs:
                raise RuntimeError(
                    f"Materialized dataset {self.dataset_name!r} declares columns for unknown source {remote_key!r}"
                )
            path = output_dir / remote_key
            actual = set(pq.ParquetFile(path).schema_arrow.names)
            missing = sorted(set(required) - actual)
            if missing:
                raise RuntimeError(
                    f"Materialized dataset {self.dataset_name!r}: source "
                    f"{remote_key!r} is missing required columns: {', '.join(missing)}"
                )

    def _prime_previous_generation(self, output_dir: Path) -> dict | None:
        """Restore all stateful inputs from one prior manifest, never loose latest files."""
        if not self.prior_outputs:
            return None
        pointer_key = f"{_ROOT_PREFIX}/{self.dataset_name}/latest.json"
        pointer_path = output_dir / f"_{self.dataset_name}_latest.json"
        use_cached_pointer = self.skip_upload and pointer_path.exists()
        if not use_cached_pointer and not r2.download(pointer_key, pointer_path):
            if not self.skip_upload and not self.allow_bootstrap:
                raise RuntimeError(
                    f"No prior {self.dataset_name} materialized generation is published; "
                    "refusing an implicit state reset. Set allow_bootstrap=True only for "
                    "the first publication or an intentional recovery."
                )
            if not self.skip_upload:
                pointer_path.unlink(missing_ok=True)
                (output_dir / f"_{self.dataset_name}_previous_manifest.json").unlink(missing_ok=True)
                for _, prior_name in self.prior_outputs:
                    (output_dir / prior_name).unlink(missing_ok=True)
            logger.info(
                "No prior {} materialized generation; bootstrapping",
                self.dataset_name,
            )
            return None

        pointer = _read_json(pointer_path)
        if pointer.get("format_version") != _FORMAT_VERSION or pointer.get("dataset") != self.dataset_name:
            raise RuntimeError(f"Invalid {self.dataset_name} latest pointer")
        snapshot_id = str(pointer.get("snapshot_id") or "")
        if not _SAFE_SNAPSHOT_ID.fullmatch(snapshot_id):
            raise RuntimeError(f"Invalid {self.dataset_name} snapshot id")
        expected_prefix = f"{_ROOT_PREFIX}/{self.dataset_name}/snapshots/{snapshot_id}/"
        manifest_key = _safe_remote_key(
            pointer.get("manifest_key"),
            prefix=expected_prefix,
        )
        if manifest_key != f"{expected_prefix}manifest.json":
            raise RuntimeError(f"Invalid {self.dataset_name} manifest key")
        manifest_path = output_dir / f"_{self.dataset_name}_previous_manifest.json"
        use_cached_manifest = self.skip_upload and manifest_path.exists()
        if not use_cached_manifest and not r2.download(manifest_key, manifest_path):
            raise RuntimeError(f"{self.dataset_name} latest pointer references missing {manifest_key}")
        manifest = _read_json(manifest_path)
        if (
            manifest.get("format_version") != _FORMAT_VERSION
            or manifest.get("dataset") != self.dataset_name
            or manifest.get("snapshot_id") != pointer.get("snapshot_id")
        ):
            raise RuntimeError(f"Invalid {self.dataset_name} materialized manifest")

        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict):
            raise RuntimeError(f"{self.dataset_name} manifest has no artifact map")
        for artifact_name, prior_name in self.prior_outputs:
            target = output_dir / prior_name
            record = artifacts.get(artifact_name)
            if not isinstance(record, dict):
                raise RuntimeError(f"{self.dataset_name} manifest is missing stateful artifact {artifact_name}")
            remote_key = _safe_remote_key(record.get("remote_key"), prefix=expected_prefix)
            if remote_key != f"{expected_prefix}{artifact_name}":
                raise RuntimeError(f"{self.dataset_name} manifest has an invalid key for {artifact_name}")
            use_cached_artifact = self.skip_upload and target.exists()
            if not use_cached_artifact and not r2.download(remote_key, target):
                raise RuntimeError(f"{self.dataset_name} manifest artifact is missing: {remote_key}")
            expected_digest = str(record.get("sha256") or "")
            if not expected_digest or _sha256(target) != expected_digest:
                target.unlink(missing_ok=True)
                raise RuntimeError(f"{self.dataset_name} prior artifact failed its SHA-256 check: {artifact_name}")
        return manifest

    def _input_snapshot(
        self,
        output_dir: Path,
        previous_manifest: dict | None,
    ) -> dict:
        sources = {name: _file_record(output_dir / name) for name in self.source_inputs}
        prior = {
            artifact: _file_record(output_dir / local_name)
            for artifact, local_name in self.prior_outputs
            if (output_dir / local_name).exists()
        }
        return {
            "sources": sources,
            "prior_artifacts": prior,
            "previous_snapshot_id": (previous_manifest.get("snapshot_id") if previous_manifest is not None else None),
        }

    @staticmethod
    def _ordered_stages(stages: Iterable[DatasetStage]) -> tuple[DatasetStage, ...]:
        declared = tuple(stages)
        by_name = {stage.name: stage for stage in declared}
        if len(by_name) != len(declared):
            raise RuntimeError("Materialized dataset stage names must be unique")
        names = set(by_name)
        unknown = {dependency for stage in declared for dependency in stage.depends_on if dependency not in names}
        if unknown:
            raise RuntimeError(f"Materialized dataset has unknown stage dependencies: {sorted(unknown)}")

        ordered: list[DatasetStage] = []
        completed: set[str] = set()
        pending = list(declared)
        while pending:
            ready = [stage for stage in pending if set(stage.depends_on) <= completed]
            if not ready:
                cycle = ", ".join(stage.name for stage in pending)
                raise RuntimeError(f"Materialized dataset stage cycle: {cycle}")
            for stage in ready:
                ordered.append(stage)
                completed.add(stage.name)
                pending.remove(stage)
        return tuple(ordered)

    def _write_publication_files(
        self,
        output_dir: Path,
        *,
        context: RunContext,
        stages: tuple[DatasetStage, ...],
        input_snapshot: dict,
    ) -> tuple[Path, Path, dict[str, Path]]:
        artifact_paths = {name: output_dir / name for name in self.published_outputs}
        artifact_records = {
            name: {
                **_file_record(path),
                "rows": pq.ParquetFile(path).metadata.num_rows,
            }
            for name, path in artifact_paths.items()
        }
        stage_records = [
            {
                "name": stage.name,
                "depends_on": list(stage.depends_on),
                "outputs": list(stage.outputs),
            }
            for stage in stages
        ]
        snapshot_id = stable_id(
            "snapshot",
            self.dataset_name,
            context.run_id,
            context.asserted_at,
            canonical_json(input_snapshot),
            canonical_json(artifact_records),
            canonical_json(stage_records),
            length=32,
        )
        prefix = f"{_ROOT_PREFIX}/{self.dataset_name}/snapshots/{snapshot_id}"
        artifacts = {
            name: {
                **artifact_records[name],
                "remote_key": f"{prefix}/{name}",
            }
            for name in artifact_paths
        }
        manifest = {
            "format_version": _FORMAT_VERSION,
            "dataset": self.dataset_name,
            "snapshot_id": snapshot_id,
            "run_id": context.run_id,
            "asserted_at": context.asserted_at,
            "inputs": input_snapshot,
            "stages": stage_records,
            "artifacts": artifacts,
        }
        manifest_path = output_dir / f"{self.dataset_name}-dataset-manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        pointer = {
            "format_version": _FORMAT_VERSION,
            "dataset": self.dataset_name,
            "snapshot_id": snapshot_id,
            "manifest_key": f"{prefix}/manifest.json",
        }
        pointer_path = output_dir / f"{self.dataset_name}-dataset-latest.json"
        pointer_path.write_text(
            json.dumps(pointer, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return manifest_path, pointer_path, artifact_paths

    def _publish(
        self,
        *,
        manifest_path: Path,
        pointer_path: Path,
        artifact_paths: dict[str, Path],
    ) -> None:
        manifest = _read_json(manifest_path)
        artifacts = manifest["artifacts"]
        for name, path in artifact_paths.items():
            logger.info("Uploading immutable {} dataset artifact {}...", self.dataset_name, name)
            r2.upload_file(path, remote_key=artifacts[name]["remote_key"])

        pointer = _read_json(pointer_path)
        manifest_key = str(pointer["manifest_key"])
        r2.upload_file(manifest_path, remote_key=manifest_key)

        # This single object replacement is the publication commit. It happens
        # only after every immutable object and the manifest are durable.
        r2.upload_file(
            pointer_path,
            remote_key=f"{_ROOT_PREFIX}/{self.dataset_name}/latest.json",
            allow_shrink=True,
        )
        logger.info(
            "Published materialized dataset {} snapshot {}",
            self.dataset_name,
            pointer["snapshot_id"],
        )

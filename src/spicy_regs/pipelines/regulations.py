"""The regulations.gov ETL, expressed through the Reader/Writer/Pipeline base classes.

This is also the run file. Invoke it via the ``run-pipeline`` console script::

    uv run run-pipeline --skip-upload --since-year 2025
    uv run run-pipeline --agency EPA --no-skip-upload

``RegulationsPipeline.run()`` reads top-to-bottom as the data flow:

    1. Prime      — load the incremental Manifest and existing output.
    2. Extract → stage  — ``stage_agencies`` fans agencies out in parallel; each
       record stream flows Mirrulations Reader (raw JSON) -> ExtractRecords
       transform (flatten) -> StagingWriter (local Parquet).
    3. Merge      — per-agency staging Parquet is merged + deduplicated (a bulk,
       whole-dataset transform).
    4. Load       — the Manifest is persisted and the dataset published to R2
       (upload is off by default while this path is being vetted).

The reusable pieces live elsewhere: connection details + the reader factory in
``sources.mirrulations``, the json→record transform in ``transforms.extract``,
the parallel fan-out in ``pipelines.staging``, R2 in ``sources.r2``, and
processed-key tracking in ``manifest``. This module is just the wiring.
"""

from os import getenv
from pathlib import Path
from shutil import rmtree
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.manifest import Manifest
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.pipelines.staging import stage_agencies
from spicy_regs.schemas import RECORD_TYPES, RecordType
from spicy_regs.sources import mirrulations, r2
from spicy_regs.transforms import ExtractRecords
from spicy_regs.transforms.merge import (
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    update_comments_index,
)

load_dotenv()


class RegulationsPipeline(Pipeline):
    """Mirrulations S3 → Parquet ETL, composed from Readers, Writers, and transforms."""

    name: ClassVar[str] = "regulations"

    def __init__(
        self,
        *,
        agency: str | None = None,
        output_dir: Path | None = None,
        since_year: int | None = None,
        skip_upload: bool = True,
        skip_comments: bool = False,
        only_comments: bool = False,
        batch_number: int | None = None,
        batch_size: int = 45,
        skip_post_process: bool = False,
        full_refresh: bool = False,
        max_workers: int = 4,
        verbose: bool = False,
    ) -> None:
        self.agency = agency
        self.output_dir = output_dir
        self.since_year = since_year
        self.skip_upload = skip_upload
        self.skip_comments = skip_comments
        self.only_comments = only_comments
        self.batch_number = batch_number
        self.batch_size = batch_size
        self.skip_post_process = skip_post_process
        self.full_refresh = full_refresh
        self.max_workers = max_workers
        self.verbose = verbose

    def run(self) -> None:
        output_dir = self.output_dir or (Path.cwd() / "output")
        staging_dir = output_dir / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        record_types = self._record_types()
        agencies = self._agencies()

        # 1. Prime: load processed-key manifest + existing output for incremental work.
        if self.full_refresh:
            logger.info("Full refresh — ignoring manifest and existing output")
            manifest = Manifest.empty()
        else:
            manifest = Manifest.load(output_dir)
            self._download_existing(output_dir, record_types)

        # 2. Extract → stage: fan agencies out, pumping each source into staging.
        logger.info(
            "Processing {} agencies × {} record types ({} workers)",
            len(agencies), len(record_types), self.max_workers,
        )
        result = stage_agencies(
            agencies,
            record_types,
            staging_dir,
            mirrulations.reader_factory(
                processed_keys=manifest, since_year=self.since_year, verbose=self.verbose
            ),
            transform_for=ExtractRecords,
            max_workers=self.max_workers,
        )
        manifest.record(result.consumed_keys)

        # 3. Transform: merge per-agency staging into the deduplicated dataset.
        staged = result.rows_by_type
        if any(staged.values()):
            self._merge(staging_dir, output_dir, record_types, staged)
            rmtree(staging_dir, ignore_errors=True)
            if not self.skip_post_process:
                logger.info("Building feed summary...")
                build_feed_summary(output_dir)
        else:
            logger.info("No new records staged; skipping merge.")

        # 4. Load: persist the manifest, then publish to R2 (off by default while vetting).
        manifest.save(output_dir)
        if self.skip_upload:
            logger.info("skip_upload=True — output left in {}", output_dir)
        elif any(staged.values()):
            logger.info("Uploading to R2...")
            r2.upload_dataset(output_dir, [rt.name for rt in record_types if rt.name != "comments"])

        logger.info("Done!")

    # -- regulations-specific wiring ---------------------------------------

    def _record_types(self) -> list[RecordType]:
        """The record types to process, honoring skip/only-comments."""
        names = list(RECORD_TYPES)
        if self.skip_comments:
            names = [n for n in names if n != "comments"]
        elif self.only_comments:
            names = ["comments"]
        return [RECORD_TYPES[n] for n in names]

    def _agencies(self) -> list[str]:
        """Agencies to process: explicit, AGENCIES env, or S3 discovery; then batched."""
        if self.agency is not None:
            agencies = [self.agency]
        elif (agencies_env := getenv("AGENCIES")) is not None:
            agencies = agencies_env.split(",")
        else:
            agencies = mirrulations.discover_agencies()

        if self.batch_number is not None:
            start = self.batch_number * self.batch_size
            agencies = agencies[start : start + self.batch_size]
        return agencies

    def _download_existing(self, output_dir: Path, record_types: list[RecordType]) -> None:
        """Fetch existing monolithic output from R2 so the merge appends to it."""
        for rt in record_types:
            if rt.name == "comments":  # comments are partitioned, fetched on demand at merge
                continue
            local = output_dir / f"{rt.name}.parquet"
            if not local.exists():
                r2.download(f"{rt.name}.parquet", local)

    def _merge(
        self,
        staging_dir: Path,
        output_dir: Path,
        record_types: list[RecordType],
        staged: dict[str, int],
    ) -> None:
        """Merge staging files: dockets/documents monolithically, comments partitioned."""
        names = [rt.name for rt in record_types]

        non_comment = [n for n in names if n != "comments"]
        if non_comment:
            schemas = {n: RECORD_TYPES[n].schema for n in non_comment}
            dedup_keys = {n: RECORD_TYPES[n].dedup_key for n in non_comment}
            merge_staging_files(staging_dir, output_dir, non_comment, schemas, dedup_keys)

        if "comments" in names and staged.get("comments", 0) > 0:
            changed = merge_comments_partitioned(
                staging_dir,
                output_dir,
                schema=RECORD_TYPES["comments"].schema,
                dedup_key=RECORD_TYPES["comments"].dedup_key,
            )
            if changed:
                update_comments_index(output_dir, changed)


# --- CLI / run file --------------------------------------------------------

app = App(name="run-pipeline", help="Run the regulations.gov ETL through the Pipeline contract.")


@app.default
def main(
    *,
    agency: Annotated[str | None, Parameter(help="Process only this agency")] = None,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    since_year: Annotated[int | None, Parameter(help="Only process dockets from this year onward")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    skip_comments: Annotated[bool, Parameter(help="Skip comments")] = False,
    only_comments: Annotated[bool, Parameter(help="Only process comments")] = False,
    batch_number: Annotated[int | None, Parameter(help="Batch number (0-indexed)")] = None,
    batch_size: Annotated[int, Parameter(help="Agencies per batch")] = 45,
    skip_post_process: Annotated[bool, Parameter(help="Skip feed summary build")] = False,
    full_refresh: Annotated[bool, Parameter(help="Ignore manifest + existing output")] = False,
    max_workers: Annotated[int, Parameter(help="Agencies processed in parallel")] = 4,
    verbose: Annotated[bool, Parameter(name=["--verbose", "-v"], help="Verbose logging")] = False,
) -> None:
    """Run the regulations.gov ETL pipeline."""
    RegulationsPipeline(
        agency=agency,
        output_dir=output_dir,
        since_year=since_year,
        skip_upload=skip_upload,
        skip_comments=skip_comments,
        only_comments=only_comments,
        batch_number=batch_number,
        batch_size=batch_size,
        skip_post_process=skip_post_process,
        full_refresh=full_refresh,
        max_workers=max_workers,
        verbose=verbose,
    ).run()


if __name__ == "__main__":
    app()

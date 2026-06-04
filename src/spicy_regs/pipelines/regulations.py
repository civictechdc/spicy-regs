"""The regulations.gov ETL, expressed through the Reader/Writer/Pipeline base classes.

This is also the run file. Invoke it via the ``run-pipeline`` console script::

    uv run run-pipeline --skip-upload --since-year 2025
    uv run run-pipeline --agency EPA --no-skip-upload

``RegulationsPipeline.run()`` reads top-to-bottom as the data flow:

    1. Extract → stage  — a :class:`MirrulationsReader` (source) is pumped into
       a :class:`StagingWriter` (sink) for every (agency, record type) pair.
    2. Transform        — the per-agency staging Parquet files are merged and
       deduplicated into the final dataset.
    3. Load             — the merged dataset is published to R2 (off by default
       while this path is being vetted).

This is the new, in-vetting path. The scheduled production ETL still runs
``spicy_regs.pipeline.pipeline`` unchanged; that module carries extra machinery
this deliberately omits for clarity (incremental manifest / Bloom-filter dedup,
per-agency parallelism, search-index build).
"""

from os import getenv
from pathlib import Path
from shutil import rmtree
from typing import Annotated, ClassVar

import boto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipeline.extract import get_agencies
from spicy_regs.pipeline.load import upload_to_r2
from spicy_regs.pipeline.transform import (
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    update_comments_index,
)
from spicy_regs.pipelines.base import Pipeline
from spicy_regs.schemas import RECORD_TYPES, RecordType
from spicy_regs.sources import MirrulationsReader, StagingWriter

load_dotenv()

BUCKET = "mirrulations"
PREFIX = "raw-data"


def _s3_resource():
    """Anonymous S3 resource for the public Mirrulations mirror."""
    return boto3.resource("s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED))


def _s3_client():
    """Anonymous S3 client (used only for agency discovery)."""
    return boto3.client("s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED))


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
        self.verbose = verbose

    def run(self) -> None:
        output_dir = self.output_dir or (Path.cwd() / "output")
        staging_dir = output_dir / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        record_types = self._record_types()
        agencies = self._agencies()
        logger.info("Processing {} agencies × {} record types", len(agencies), len(record_types))

        # 1. Extract → stage: pump each source into its staging sink.
        s3 = _s3_resource()
        staged: dict[str, int] = {rt.name: 0 for rt in record_types}
        for agency in agencies:
            for record_type in record_types:
                reader = MirrulationsReader(
                    s3, BUCKET, PREFIX, agency, record_type,
                    since_year=self.since_year, verbose=self.verbose,
                )
                writer = StagingWriter(agency, record_type, staging_dir)
                writer.write(reader.iter_records())
                staged[record_type.name] += writer.rows_written
                logger.info("[{}] {}: staged {} rows", agency, record_type.name, writer.rows_written)

        if not any(staged.values()):
            logger.info("No new records staged; nothing to merge.")
            return

        # 2. Transform: merge per-agency staging into the deduplicated dataset.
        self._merge(staging_dir, output_dir, record_types, staged)
        rmtree(staging_dir, ignore_errors=True)
        if not self.skip_post_process:
            logger.info("Building feed summary...")
            build_feed_summary(output_dir)

        # 3. Load: publish to R2 (off by default while vetting).
        if self.skip_upload:
            logger.info("skip_upload=True — output left in {}", output_dir)
        else:
            logger.info("Uploading to R2...")
            upload_to_r2(output_dir, [rt.name for rt in record_types if rt.name != "comments"])

        logger.info("Done!")

    # -- composition helpers ------------------------------------------------

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
            agencies = get_agencies(_s3_client(), BUCKET, PREFIX)

        if self.batch_number is not None:
            start = self.batch_number * self.batch_size
            agencies = agencies[start : start + self.batch_size]
        return agencies

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
        verbose=verbose,
    ).run()


if __name__ == "__main__":
    app()

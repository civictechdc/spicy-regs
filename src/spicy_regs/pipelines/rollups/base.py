"""Base class for decoupled, per-rollup pipelines.

Each rollup (``feed_summary``, ``agency_stats``, ``rulemaking_lifecycles``, ...)
is materialized by its own :class:`RollupPipeline` subclass with its own console
entry (``run-rollup-*``) and its own GitHub Actions workflow on an independent
cron. This keeps rollup logic out of the extract/merge ETL: a rollup can be
re-run or backfilled on its own, and a failure is isolated to a single artifact.

``run()`` reads top-to-bottom as the data flow:

    1. Prime — download the base tables this rollup reads from R2 (skipping any
       already present locally, so local dev and re-runs don't re-download).
    2. Build — materialize the rollup artifact via a transform.
    3. Load  — publish the single artifact to R2 (shrink-guarded), off by
       default while vetting.

**Contract:** a rollup reads only the ETL's *published base tables*
(``dockets``, ``documents``, ``comments_index``, ``federal_register``) — never
another rollup's output — so pipelines stay independently schedulable with no
cross-pipeline race.
"""

from abc import abstractmethod
from pathlib import Path
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipelines.base import Pipeline
from spicy_regs.sources import r2

load_dotenv()


class RollupPipeline(Pipeline):
    """A single materialized rollup, run standalone from the base tables on R2."""

    #: Base-table Parquet files this rollup reads (R2 remote keys, e.g.
    #: ``"documents.parquet"``). Primed from R2 before the build.
    inputs: ClassVar[tuple[str, ...]] = ()

    #: The single artifact this rollup writes and publishes (e.g.
    #: ``"feed_summary.parquet"``). Its R2 remote key is the same filename.
    output: ClassVar[str]

    def __init__(self, *, output_dir: Path | None = None, skip_upload: bool = True) -> None:
        self.output_dir = output_dir
        self.skip_upload = skip_upload

    def run(self) -> None:
        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Prime: pull the base tables this rollup reads from R2.
        self._prime(output_dir)

        # 2. Build: materialize the rollup artifact.
        logger.info("Building rollup {}...", self.output)
        out_path = self.build(output_dir)

        # 3. Load: publish the single artifact (shrink-guarded), unless skipping.
        if self.skip_upload:
            logger.info("skip_upload=True — {} left in {}", self.output, output_dir)
        else:
            logger.info("Uploading {} to R2...", self.output)
            r2.upload_file(out_path, remote_key=self.output)

        logger.info("Done!")

    def _prime(self, output_dir: Path) -> None:
        """Download each required base table from R2 unless already local.

        A missing base table is fatal: a rollup built from absent inputs would
        publish a truncated artifact that the upload shrink-guard would then
        (correctly) reject — better to fail loudly here with a clear message.
        """
        for remote_key in self.inputs:
            local = output_dir / remote_key
            if local.exists():
                logger.info("Using local {} (skipping download)", remote_key)
                continue
            if not r2.download(remote_key, local):
                raise RuntimeError(
                    f"Rollup {self.output!r}: required base table {remote_key!r} "
                    f"not found on R2 and not present locally in {output_dir}"
                )

    @abstractmethod
    def build(self, output_dir: Path) -> Path:
        """Materialize the rollup into ``output_dir`` and return its path."""
        ...


def make_rollup_app(pipeline_cls: type[RollupPipeline]) -> App:
    """Build the ``run-rollup-*`` cyclopts CLI for one rollup pipeline.

    All rollups share the same two flags, so each module just does::

        app = make_rollup_app(FeedSummaryRollup)
    """
    app = App(
        name=f"run-rollup-{pipeline_cls.name}",
        help=(pipeline_cls.__doc__ or "").strip().splitlines()[0] if pipeline_cls.__doc__ else "",
    )

    @app.default
    def main(
        *,
        output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
        skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    ) -> None:
        pipeline_cls(output_dir=output_dir, skip_upload=skip_upload).run()

    return app

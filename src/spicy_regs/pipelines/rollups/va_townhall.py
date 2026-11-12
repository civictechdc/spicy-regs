"""Rollup pipeline: Virginia Regulatory Town Hall ingest (state_actions, state_stages, state_comments).

Like ``unified_agenda``, this rollup *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty. Unlike the other rollups it
publishes three tables from one crawl, plus the crawl cursor, so it overrides
:meth:`run` to upload each of them. The tables go up first and the cursor last,
so a failed upload never moves the cursor past data that was not published.

The crawl budget defaults to
:data:`~spicy_regs.transforms.build_va_townhall.DEFAULT_MAX_MINUTES`;
``TOWNHALL_MAX_MINUTES`` overrides it (set it low for a quick local run).
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

from loguru import logger

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.sources import r2
from spicy_regs.transforms.build_va_townhall import (
    DEFAULT_MAX_MINUTES,
    STAGES_OUTPUT,
    STATE_KEY,
    VaTownHallBuild,
    build_va_townhall,
)


def _max_minutes() -> float:
    raw = os.environ.get("TOWNHALL_MAX_MINUTES", "").strip()
    if not raw:
        return DEFAULT_MAX_MINUTES
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"TOWNHALL_MAX_MINUTES must be a number, got {raw!r}") from exc


class VaTownHallRollup(RollupPipeline):
    """Virginia Regulatory Town Hall actions, stages and public comments (resumable crawl)."""

    name: ClassVar[str] = "va-townhall"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = STAGES_OUTPUT

    def run(self) -> None:
        output_dir = self.output_dir or (Path.cwd() / "output")
        output_dir.mkdir(parents=True, exist_ok=True)

        result = self.build_all(output_dir)

        if self.skip_upload:
            logger.info("skip_upload=True — Town Hall tables left in {}", output_dir)
        else:
            ingested_at = datetime.now(timezone.utc).isoformat()
            for path in result.tables:
                r2.upload_file(path, remote_key=path.name, ingested_at=ingested_at)
            r2.upload_file(result.state_file, remote_key=STATE_KEY, ingested_at=ingested_at)

        if result.error:
            # Raised after publishing so the work done before the failure is kept.
            raise RuntimeError(f"Town Hall crawl incomplete: {result.error}")
        logger.info("Done!")

    def build_all(self, output_dir: Path) -> VaTownHallBuild:
        return build_va_townhall(output_dir, max_minutes=_max_minutes())

    def build(self, output_dir: Path) -> Path:
        return self.build_all(output_dir).tables[1]


app = make_rollup_app(VaTownHallRollup)

if __name__ == "__main__":
    app()

"""Rollup pipeline: sam_entities.parquet (SAM.gov Entity API v4 ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the bounded fetch +
incremental merge with the prior published table happens inside
``build_sam_entities``. The base class still handles the shrink-guarded R2 upload
of the single output.

**Bounded, accretive scheduled run.** The active registry (~765K entities) is far
past the ~5K synchronous pagination ceiling, so full coverage comes from SAM's bulk
extract walked over ``registrationDate`` year windows (see
:mod:`spicy_regs.sources.sam_entities`). To keep each *scheduled* run bounded while
still converging on full coverage, the default run fetches a **single rotating
year window** (chosen from the run date), and the transform's merge accretes each
window into the prior table across runs. Over one rotation period every year is
covered; subsequent cycles refresh it.

**Full backfill / overrides** via env vars (read here so the shared rollup CLI stays
minimal):

- ``SAM_INGEST_MODE``   — ``extract`` (default) or ``partition``.
- ``SAM_SINCE_YEAR`` / ``SAM_UNTIL_YEAR`` — explicit registrationDate year range;
  set both to walk a range in one run (e.g. ``2000``..current for a full backfill).
- ``SAM_MAX_RECORDS``   — bound the run (blank/``0`` = unbounded).

A full backfill is therefore a single ``workflow_dispatch`` (or local) run with
``SAM_SINCE_YEAR=2000 SAM_UNTIL_YEAR=<current-year>`` and ``SAM_MAX_RECORDS`` unset.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from loguru import logger

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.sources.sam_entities import _MIN_REGISTRATION_YEAR
from spicy_regs.transforms import build_sam_entities


def _rotating_year(today: date) -> int:
    """Pick one registrationDate year to fetch this run, rotating by day.

    Cycles across ``[_MIN_REGISTRATION_YEAR, today.year]`` so successive scheduled
    runs advance coverage one bounded year-window at a time (and keep refreshing
    once the cycle wraps). Deterministic from the date — no persisted cursor.
    """
    span = today.year - _MIN_REGISTRATION_YEAR + 1
    return _MIN_REGISTRATION_YEAR + (today.toordinal() % span)


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("SAM rollup: ignoring non-integer {}={!r}", name, raw)
        return None


class SamEntitiesRollup(RollupPipeline):
    """Federal entity registry ingested from the SAM.gov Entity API (api.data.gov key)."""

    name: ClassVar[str] = "sam-entities"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "sam_entities.parquet"

    def build(self, output_dir: Path) -> Path:
        mode = os.environ.get("SAM_INGEST_MODE", "extract").strip() or "extract"
        since = _int_env("SAM_SINCE_YEAR")
        until = _int_env("SAM_UNTIL_YEAR")
        max_records = _int_env("SAM_MAX_RECORDS")  # blank/0 -> unbounded within the window(s)

        if since is None and until is None:
            # Default scheduled run: one bounded, rotating year window.
            since = until = _rotating_year(date.today())
            logger.info("SAM rollup: scheduled default — rotating year window {}", since)
        else:
            logger.info("SAM rollup: explicit year range {}..{} (max_records={})", since, until, max_records)

        return build_sam_entities(
            output_dir,
            mode=mode,
            since_year=since,
            until_year=until,
            max_records=max_records if max_records else None,
        )


app = make_rollup_app(SamEntitiesRollup)

if __name__ == "__main__":
    app()

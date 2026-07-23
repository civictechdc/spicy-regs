"""Rollup pipeline: fcc_filings.parquet (FCC ECFS REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_fcc_filings``.
The base class still handles the shrink-guarded R2 upload of the single output.

Two env overrides support catch-up / backfill runs (both optional):

* ``FCC_SINCE`` — fetch filings received on/after this date (YYYY-MM-DD)
  instead of resuming from the prior table's max ``date_received``.
* ``FCC_PROCEEDINGS`` — comma-separated proceeding names (e.g. ``17-108,23-320``)
  to scope the fetch to specific dockets instead of all of ECFS.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_fcc_filings


def _date_env(name: str) -> date | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


def _proceedings_env(name: str) -> tuple[str, ...]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return ()
    return tuple(p.strip() for p in raw.split(",") if p.strip())


class FccFilingsRollup(RollupPipeline):
    """FCC filings (comment equivalents) ingested from ECFS (api.data.gov key)."""

    name: ClassVar[str] = "fcc-filings"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fcc_filings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fcc_filings(
            output_dir,
            since=_date_env("FCC_SINCE"),
            proceedings=_proceedings_env("FCC_PROCEEDINGS"),
        )


app = make_rollup_app(FccFilingsRollup)

if __name__ == "__main__":
    app()

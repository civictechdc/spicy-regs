"""Rollup pipeline: fcc_proceedings.parquet (FCC ECFS REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_fcc_proceedings``.
The base class still handles the shrink-guarded R2 upload of the single output.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_fcc_proceedings


def _date_env(name: str) -> date | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


class FccProceedingsRollup(RollupPipeline):
    """FCC proceedings (docket equivalents) ingested from ECFS (api.data.gov key)."""

    name: ClassVar[str] = "fcc-proceedings"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fcc_proceedings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fcc_proceedings(output_dir, since=_date_env("FCC_SINCE"))


app = make_rollup_app(FccProceedingsRollup)

if __name__ == "__main__":
    app()

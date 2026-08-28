"""Rollup pipeline: congress_bills.parquet (Congress.gov v3 REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_congress_bills``.
The base class still handles the shrink-guarded R2 upload of the single output.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_congress_bills


def _date_env(name: str) -> date | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


class CongressBillsRollup(RollupPipeline):
    """Congressional bills ingested from the Congress.gov v3 API (api.data.gov key)."""

    name: ClassVar[str] = "congress-bills"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "congress_bills.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_congress_bills(output_dir, since=_date_env("CONGRESS_SINCE"))


app = make_rollup_app(CongressBillsRollup)

if __name__ == "__main__":
    app()

"""Rollup pipeline: usaspending_recipients.parquet (USASpending.gov v2 ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside
``build_usaspending_recipients``. The base class still handles the
shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_usaspending_recipients


class UsaSpendingRecipientsRollup(RollupPipeline):
    """Federal-award recipients ingested from the USASpending.gov v2 API (keyless)."""

    name: ClassVar[str] = "usaspending-recipients"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "usaspending_recipients.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_usaspending_recipients(output_dir)


app = make_rollup_app(UsaSpendingRecipientsRollup)

if __name__ == "__main__":
    app()

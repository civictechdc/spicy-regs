"""Rollup pipeline: crs_reports.parquet (Congress.gov CRS report ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_crs_reports``. The
base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_crs_reports


class CrsReportsRollup(RollupPipeline):
    """Congressional Research Service reports ingested from the Congress.gov v3 API."""

    name: ClassVar[str] = "crs-reports"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "crs_reports.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_crs_reports(output_dir)


app = make_rollup_app(CrsReportsRollup)

if __name__ == "__main__":
    app()

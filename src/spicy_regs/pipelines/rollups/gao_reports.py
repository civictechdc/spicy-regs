"""Rollup pipeline: gao_reports.parquet (GAO reports RSS ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + append-only
merge with the prior published table happens inside ``build_gao_reports``. The
base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_gao_reports


class GaoReportsRollup(RollupPipeline):
    """GAO oversight reports ingested from the gao.gov reports RSS feed."""

    name: ClassVar[str] = "gao-reports"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "gao_reports.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_gao_reports(output_dir)


app = make_rollup_app(GaoReportsRollup)

if __name__ == "__main__":
    app()

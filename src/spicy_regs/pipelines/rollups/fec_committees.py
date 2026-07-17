"""Rollup pipeline: fec_committees.parquet (OpenFEC committees ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the full committee-list
walk and the merge with the prior published table happen inside
``build_fec_committees``. The base class still handles the shrink-guarded R2
upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_fec_committees


class FecCommitteesRollup(RollupPipeline):
    """FEC committees/PACs ingested from the OpenFEC API (api.data.gov key)."""

    name: ClassVar[str] = "fec-committees"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fec_committees.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fec_committees(output_dir)


app = make_rollup_app(FecCommitteesRollup)

if __name__ == "__main__":
    app()

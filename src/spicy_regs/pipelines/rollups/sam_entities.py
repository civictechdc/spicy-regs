"""Rollup pipeline: sam_entities.parquet (SAM.gov Entity API v4 ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the bounded fetch +
incremental merge with the prior published table happens inside
``build_sam_entities``. The base class still handles the shrink-guarded R2 upload
of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_sam_entities


class SamEntitiesRollup(RollupPipeline):
    """Federal entity registry ingested from the SAM.gov Entity API (api.data.gov key)."""

    name: ClassVar[str] = "sam-entities"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "sam_entities.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_sam_entities(output_dir)


app = make_rollup_app(SamEntitiesRollup)

if __name__ == "__main__":
    app()

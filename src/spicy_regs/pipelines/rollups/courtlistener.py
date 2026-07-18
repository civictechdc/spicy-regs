"""Rollup pipeline: court_dockets.parquet (CourtListener v4 RECAP ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_courtlistener``. The
base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_courtlistener


class CourtListenerRollup(RollupPipeline):
    """APA / agency-review litigation dockets ingested from CourtListener (no key required)."""

    name: ClassVar[str] = "courtlistener"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_dockets.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_courtlistener(output_dir)


app = make_rollup_app(CourtListenerRollup)

if __name__ == "__main__":
    app()

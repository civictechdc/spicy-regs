"""Rollup pipeline: lobbying_filings.parquet (Senate LDA REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_lobbying_filings``.
The base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_lobbying_filings


class LobbyingFilingsRollup(RollupPipeline):
    """Senate Lobbying Disclosure Act filings ingested from lda.senate.gov (key optional)."""

    name: ClassVar[str] = "lobbying-filings"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "lobbying_filings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_lobbying_filings(output_dir)


app = make_rollup_app(LobbyingFilingsRollup)

if __name__ == "__main__":
    app()

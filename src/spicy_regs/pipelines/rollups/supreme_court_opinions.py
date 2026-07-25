"""Rollup pipeline: official Supreme Court opinion packages."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import (
    RollupPipeline,
    make_rollup_app,
)
from spicy_regs.transforms import build_supreme_court_opinions


class SupremeCourtOpinionsRollup(RollupPipeline):
    """Ingest the current Supreme Court term and retain prior opinion rows."""

    name: ClassVar[str] = "supreme-court-opinions"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_opinions.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_supreme_court_opinions(output_dir)


app = make_rollup_app(SupremeCourtOpinionsRollup)

if __name__ == "__main__":
    app()

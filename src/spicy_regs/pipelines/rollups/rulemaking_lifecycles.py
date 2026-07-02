"""Rollup pipeline: rulemaking_lifecycles.parquet (Proposed→Rule pairs + stuck)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_rulemaking_lifecycles


class RulemakingLifecyclesRollup(RollupPipeline):
    """Per-docket rulemaking durations + stuck proposals — agency profile / lab."""

    name: ClassVar[str] = "lifecycles"
    inputs: ClassVar[tuple[str, ...]] = ("documents.parquet",)
    output: ClassVar[str] = "rulemaking_lifecycles.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_rulemaking_lifecycles(output_dir)


app = make_rollup_app(RulemakingLifecyclesRollup)

if __name__ == "__main__":
    app()

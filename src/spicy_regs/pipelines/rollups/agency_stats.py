"""Rollup pipeline: agency_stats.parquet (per-agency docket/document/comment counts)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_agency_stats


class AgencyStatsRollup(RollupPipeline):
    """Per-agency dimension table for the directory + profile pages."""

    name: ClassVar[str] = "agency-stats"
    inputs: ClassVar[tuple[str, ...]] = ("dockets.parquet", "comments_index.parquet", "documents.parquet")
    output: ClassVar[str] = "agency_stats.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_agency_stats(output_dir)


app = make_rollup_app(AgencyStatsRollup)

if __name__ == "__main__":
    app()

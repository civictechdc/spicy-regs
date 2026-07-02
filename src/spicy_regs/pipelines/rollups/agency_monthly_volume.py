"""Rollup pipeline: agency_monthly_volume.parquet (per-agency monthly document counts)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_agency_monthly_volume


class AgencyMonthlyVolumeRollup(RollupPipeline):
    """Per-agency monthly document volume by type — directory sparklines + profile activity."""

    name: ClassVar[str] = "agency-monthly-volume"
    inputs: ClassVar[tuple[str, ...]] = ("documents.parquet",)
    output: ClassVar[str] = "agency_monthly_volume.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_agency_monthly_volume(output_dir)


app = make_rollup_app(AgencyMonthlyVolumeRollup)

if __name__ == "__main__":
    app()

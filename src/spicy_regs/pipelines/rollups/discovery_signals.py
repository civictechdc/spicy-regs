"""Rollup pipeline: discovery_signals.parquet (per-agency document-output spike)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_discovery_signals


class DiscoverySignalsRollup(RollupPipeline):
    """Feed 'spike' signal — agencies with a recent surge in document output."""

    name: ClassVar[str] = "discovery-signals"
    inputs: ClassVar[tuple[str, ...]] = ("documents.parquet",)
    output: ClassVar[str] = "discovery_signals.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_discovery_signals(output_dir)


app = make_rollup_app(DiscoverySignalsRollup)

if __name__ == "__main__":
    app()

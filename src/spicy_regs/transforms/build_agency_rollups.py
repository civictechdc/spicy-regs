"""Transform: build the per-agency materialized rollups (stats + monthly volume).

Thin orchestrator retained for backward compatibility. The logic now lives in
two focused, independently-runnable transforms so each can be materialized by
its own decoupled rollup pipeline:

* :func:`build_agency_stats` → ``agency_stats.parquet``
* :func:`build_agency_monthly_volume` → ``agency_monthly_volume.parquet``
"""

from pathlib import Path

from spicy_regs.transforms.build_agency_monthly_volume import build_agency_monthly_volume
from spicy_regs.transforms.build_agency_stats import build_agency_stats


def build_agency_rollups(output_dir: Path) -> tuple[Path, Path]:
    """Build both per-agency rollups (stats + monthly volume).

    Kept as a convenience wrapper; new code should call the two split transforms
    directly. Returns ``(agency_stats.parquet, agency_monthly_volume.parquet)``.
    """
    stats_file = build_agency_stats(output_dir)
    volume_file = build_agency_monthly_volume(output_dir)
    return stats_file, volume_file

"""Rollup pipeline: docket_daily_comment_volume.parquet (comments per docket per day).

``comments.parquet`` is deliberately *not* declared as an input, following
``org_committee_links``: at ~4.6 GB it would dominate the job, and the transform
needs four narrow columns (~16 MB of column chunks), so it is read straight from
the public bucket with Parquet projection pushdown (see
:mod:`spicy_regs.transforms.build_docket_daily_comment_volume`). A local copy in
``output_dir`` is still preferred when one exists.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_docket_daily_comment_volume


class DocketDailyCommentVolumeRollup(RollupPipeline):
    """Per-docket daily comment volume — docket comment charts without scanning comments."""

    name: ClassVar[str] = "docket-daily-comment-volume"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "docket_daily_comment_volume.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_docket_daily_comment_volume(output_dir)


app = make_rollup_app(DocketDailyCommentVolumeRollup)

if __name__ == "__main__":
    app()

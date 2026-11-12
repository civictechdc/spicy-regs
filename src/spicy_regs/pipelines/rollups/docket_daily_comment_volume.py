"""Rollup pipeline: docket_daily_comment_volume.parquet (comments per docket per day).

``comments.parquet`` is deliberately *not* declared as an input, following
``org_committee_links``: at ~4.6 GB it would dominate the job, and the transform
needs four narrow columns (~16 MB of column chunks), so it is read straight from
the public bucket with Parquet projection pushdown (see
:mod:`spicy_regs.transforms.build_docket_daily_comment_volume`). A local copy in
``output_dir`` is still preferred when one exists.

**Row-count floor.** ``r2.upload_file``'s shrink guard compares against the
published file, so it can't protect the *first* publish (there is no remote file
yet), and a gutted ``comments.parquet`` would publish an empty series table. The
build therefore refuses output under :data:`MIN_EXPECTED_ROWS`. The floor lives
here, in the publishing pipeline, rather than in the transform, so the transform
stays a pure function that small test fixtures can exercise directly.
"""

from pathlib import Path
from typing import ClassVar

import pyarrow.parquet as pq

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_docket_daily_comment_volume

# The live table is ~375K (docket, day) rows built from ~27M comments (Nov 2026)
# and only grows. A floor well below that trips on a broken or truncated source,
# never on normal day-to-day movement.
MIN_EXPECTED_ROWS = 100_000


class DocketDailyCommentVolumeRollup(RollupPipeline):
    """Per-docket daily comment volume — docket comment charts without scanning comments."""

    name: ClassVar[str] = "docket-daily-comment-volume"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "docket_daily_comment_volume.parquet"

    def build(self, output_dir: Path) -> Path:
        out = build_docket_daily_comment_volume(output_dir)
        rows = pq.ParquetFile(out).metadata.num_rows
        if rows < MIN_EXPECTED_ROWS:
            raise RuntimeError(
                f"{self.output}: built only {rows:,} rows, under the {MIN_EXPECTED_ROWS:,}-row floor; "
                "refusing to continue (is comments.parquet truncated?)"
            )
        return out


app = make_rollup_app(DocketDailyCommentVolumeRollup)

if __name__ == "__main__":
    app()

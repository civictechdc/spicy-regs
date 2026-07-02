"""Rollup pipeline: feed_summary.parquet (dockets + comment counts + comment-end dates)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_feed_summary


class FeedSummaryRollup(RollupPipeline):
    """Pre-joined docket feed summary — powers the feed/timeline without scanning comments."""

    name: ClassVar[str] = "feed-summary"
    inputs: ClassVar[tuple[str, ...]] = ("dockets.parquet", "comments_index.parquet", "documents.parquet")
    output: ClassVar[str] = "feed_summary.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_feed_summary(output_dir)


app = make_rollup_app(FeedSummaryRollup)

if __name__ == "__main__":
    app()

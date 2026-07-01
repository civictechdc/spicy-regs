"""Rollup pipeline: docket_search.json.gz (client-side MiniSearch index)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import INDEX_FILENAME, build_search_index


class DocketSearchRollup(RollupPipeline):
    """Gzipped docket search blob consumed by the browser MiniSearch index."""

    name: ClassVar[str] = "docket-search"
    inputs: ClassVar[tuple[str, ...]] = ("dockets.parquet",)
    output: ClassVar[str] = INDEX_FILENAME

    def build(self, output_dir: Path) -> Path:
        return build_search_index(output_dir)


app = make_rollup_app(DocketSearchRollup)

if __name__ == "__main__":
    app()

"""Rollup pipeline: court_opinion_clusters.parquet (CourtListener bulk ingest).

Like ``courtlistener``, this rollup *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the bulk read, the search
catch-up, and the incremental merge with the prior published table all happen
inside ``build_court_opinion_clusters``.

**A run reads two dumps, not one.** The cluster dump says nothing about which
court decided; that lives on the docket. So a scheduled run streams the 2.3 GiB
``opinion-clusters`` dump *and* the 4.67 GiB ``dockets`` dump — about 70 minutes
at the bucket's observed rate rather than 23. That is the whole price of the
table being able to answer "what have the federal courts said", which is the
first question anyone asks of ten million decisions from 3,361 courts. The
docket map is cached by dump date, so re-running inside one quarter pays once.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_court_opinion_clusters


class CourtOpinionClustersRollup(RollupPipeline):
    """Federal and state court decisions (opinion clusters) from CourtListener bulk data."""

    name: ClassVar[str] = "court-opinion-clusters"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_opinion_clusters.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_court_opinion_clusters(output_dir)


app = make_rollup_app(CourtOpinionClustersRollup)

if __name__ == "__main__":
    app()

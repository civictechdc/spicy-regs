"""Rollup pipeline: court_opinion_bodies.parquet (CourtListener bulk opinion text).

Ingests an external source, so ``inputs`` is empty. Unlike the other ingest
rollups this one carries a **default bound**: the ``opinions`` dump is 50.8 GiB
compressed and takes roughly 8.6 hours to stream end to end, which is not a
sensible unit of work for a scheduled job that also has to leave the disk
usable. ``DEFAULT_MAX_RECORDS`` caps a scheduled run; the full backfill is a
deliberate, separately-invoked operation documented in
``docs/evidence/court-data-coverage.md``.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_court_opinion_bodies

#: Opinions kept by a scheduled run. Chosen so a run finishes in ~15 minutes at
#: the bucket's observed ~1.8 MiB/s and leaves the free-space floor intact.
DEFAULT_MAX_RECORDS = 250_000


class CourtOpinionBodiesRollup(RollupPipeline):
    """Court opinion text (plain_text, html_with_citations) from CourtListener bulk data."""

    name: ClassVar[str] = "court-opinion-bodies"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "court_opinion_bodies.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_court_opinion_bodies(output_dir, max_records=DEFAULT_MAX_RECORDS)


app = make_rollup_app(CourtOpinionBodiesRollup)

if __name__ == "__main__":
    app()

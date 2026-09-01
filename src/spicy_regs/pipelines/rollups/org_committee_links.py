"""Rollup pipeline: org_committee_links.parquet (commenter org → FEC committee).

Derives the name bridge between the regulations.gov corpus and the OpenFEC
committee reference dimension. Two deviations from the plain derived-rollup
shape, both deliberate:

* ``fec_committees.parquet`` is an *ingest* rollup's output, not one of the
  ETL's core base tables. Treating it as a base input follows the precedent set
  by ``fr_docket_links`` over ``federal_register.parquet``: an ingest table has
  no upstream dependency inside this repo, so reading it introduces no
  cross-pipeline race — only an ordering preference, which the workflow's cron
  handles by running after the FEC ingest.
* ``comments.parquet`` is deliberately *not* declared as an input. At ~3.3 GB
  it would dominate the job, and the transform needs five narrow columns from
  it, so it is read straight from the public bucket with Parquet projection
  pushdown (see :mod:`spicy_regs.transforms.build_org_committee_links`). A
  local copy in ``output_dir`` is still preferred when one exists.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_org_committee_links


class OrgCommitteeLinksRollup(RollupPipeline):
    """Commenter organizations name-matched to FEC committees/PACs."""

    name: ClassVar[str] = "org-committee-links"
    inputs: ClassVar[tuple[str, ...]] = ("fec_committees.parquet",)
    output: ClassVar[str] = "org_committee_links.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_org_committee_links(output_dir)


app = make_rollup_app(OrgCommitteeLinksRollup)

if __name__ == "__main__":
    app()

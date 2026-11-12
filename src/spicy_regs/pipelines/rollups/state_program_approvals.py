"""Rollup pipeline: state_program_approvals.parquet (FR documents ↔ the state rules they approve).

Reads ``federal_register.parquet`` from R2 as a base input. For states with a
citation parser (Virginia, Maryland) it also fetches document bodies from
federalregister.gov and carries prior results over from the published table,
so steady-state runs fetch only newly published documents.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_state_program_approvals


class StateProgramApprovalsRollup(RollupPipeline):
    """Federal Register state-program approvals, one row per document and state."""

    name: ClassVar[str] = "state-program-approvals"
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    output: ClassVar[str] = "state_program_approvals.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_state_program_approvals(output_dir)


app = make_rollup_app(StateProgramApprovalsRollup)

if __name__ == "__main__":
    app()

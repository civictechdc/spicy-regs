"""Rollup pipeline: unified_agenda.parquet (reginfo.gov Unified Agenda ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_unified_agenda``.
The base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_unified_agenda


class UnifiedAgendaRollup(RollupPipeline):
    """Unified Agenda entries ingested from reginfo.gov (no API key)."""

    name: ClassVar[str] = "unified-agenda"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "unified_agenda.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_unified_agenda(output_dir)


app = make_rollup_app(UnifiedAgendaRollup)

if __name__ == "__main__":
    app()

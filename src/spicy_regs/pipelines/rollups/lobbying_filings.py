"""Rollup pipeline: lobbying_filings.parquet (Senate LDA REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_lobbying_filings``.
The base class still handles the shrink-guarded R2 upload of the single output.
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_lobbying_filings


def _date_env(name: str) -> date | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


class LobbyingFilingsRollup(RollupPipeline):
    """Senate Lobbying Disclosure Act filings ingested from lda.gov (key optional)."""

    name: ClassVar[str] = "lobbying-filings"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "lobbying_filings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_lobbying_filings(
            output_dir,
            since=_date_env("LDA_SINCE"),
            until=_date_env("LDA_UNTIL"),
        )


app = make_rollup_app(LobbyingFilingsRollup)

if __name__ == "__main__":
    app()

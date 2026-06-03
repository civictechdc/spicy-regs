"""The production regulations.gov ETL, exposed through the Pipeline interface.

The actual orchestration (parallel per-agency extraction, partitioned comment
merges, manifest tracking, R2 upload) lives in the Prefect flow at
``spicy_regs.pipeline.pipeline.pipeline``. This class is a thin façade so the
flow is addressable by CI via the :class:`~spicy_regs.pipelines.base.Pipeline`
contract: look it up by ``name`` and call ``run()``.
"""

from typing import Any, ClassVar

from spicy_regs.pipeline.pipeline import pipeline
from spicy_regs.pipelines.base import Pipeline


class RegulationsPipeline(Pipeline):
    """Runs the Mirrulations S3 → Parquet on R2 ETL flow.

    Keyword arguments are forwarded verbatim to the underlying flow (e.g.
    ``agency``, ``output_dir``, ``skip_upload``, ``full_refresh``,
    ``since_year``); see ``spicy_regs.pipeline.pipeline.pipeline`` for the full
    set of parameters.
    """

    name: ClassVar[str] = "regulations"

    def __init__(self, **flow_kwargs: Any) -> None:
        self.flow_kwargs = flow_kwargs

    def run(self) -> None:
        pipeline(**self.flow_kwargs)

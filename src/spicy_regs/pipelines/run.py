"""CLI runner that invokes a pipeline through the ``Pipeline`` contract.

The base class says CI addresses a pipeline by its ``name`` and calls
``run()``. This module is that runner: it keeps a small registry of known
pipelines, looks one up by name, forwards the common flow options as keyword
arguments, and runs it.

Exposed as the ``run-pipeline`` console script (see ``pyproject.toml``)::

    uv run run-pipeline regulations --skip-upload --since-year 2025
"""

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

from spicy_regs.pipelines.base import Pipeline
from spicy_regs.pipelines.regulations import RegulationsPipeline

# Registry of runnable pipelines, keyed by their ``name``. Add new pipelines
# here to make them addressable from the CLI / CI.
PIPELINES: dict[str, type[Pipeline]] = {
    RegulationsPipeline.name: RegulationsPipeline,
}

app = App(name="run-pipeline", help="Run a pipeline by name through the Pipeline contract.")


@app.default
def run(
    name: Annotated[str, Parameter(help="Pipeline name to run")] = "regulations",
    *,
    agency: Annotated[str | None, Parameter(help="Process only this agency")] = None,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload")] = False,
    full_refresh: Annotated[bool, Parameter(help="Full refresh (ignore manifest)")] = False,
    skip_comments: Annotated[bool, Parameter(help="Skip comments")] = False,
    only_comments: Annotated[bool, Parameter(help="Only process comments")] = False,
    batch_number: Annotated[int | None, Parameter(help="Batch number (0-indexed)")] = None,
    batch_size: Annotated[int, Parameter(help="Agencies per batch")] = 45,
    skip_post_process: Annotated[bool, Parameter(help="Skip feed summary + comment partitioning")] = False,
    since_year: Annotated[int | None, Parameter(help="Only process dockets from this year onward")] = None,
    verbose: Annotated[bool, Parameter(name=["--verbose", "-v"], help="Verbose logging")] = False,
) -> None:
    """Look up the named pipeline and run it, forwarding the given options."""
    pipeline_cls = PIPELINES.get(name)
    if pipeline_cls is None:
        available = ", ".join(sorted(PIPELINES)) or "(none registered)"
        raise SystemExit(f"Unknown pipeline {name!r}. Available: {available}")

    pipeline_cls(
        agency=agency,
        output_dir=output_dir,
        skip_upload=skip_upload,
        full_refresh=full_refresh,
        skip_comments=skip_comments,
        only_comments=only_comments,
        batch_number=batch_number,
        batch_size=batch_size,
        skip_post_process=skip_post_process,
        since_year=since_year,
        verbose=verbose,
    ).run()

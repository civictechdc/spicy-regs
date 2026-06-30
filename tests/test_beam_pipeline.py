"""Hermetic test for the Apache Beam proof-of-concept (DirectRunner).

Guarded by ``importorskip`` so the default unit suite — which does not install
the optional ``beam`` extra — skips this module entirely and stays fast. To run
it::

    uv sync --extra beam
    uv run pytest tests/test_beam_pipeline.py -v
"""

import pytest

# Skip the whole module (before importing anything Beam-flavored) when the extra
# isn't installed. Keep this above the spicy_regs.beam import: importing that
# package raises a friendly error when apache-beam is missing.
pytest.importorskip("apache_beam")

import apache_beam as beam  # noqa: E402

# Aliased: the class name starts with "Test", so pytest would otherwise try to
# collect it as a test case and emit a PytestCollectionWarning.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402

from spicy_regs.beam.example_pipeline import BeamExamplePipeline  # noqa: E402


def test_beam_example_pipeline_transforms_and_dedups() -> None:
    """The chain shapes records, uppercases titles, and collapses the duplicate
    post_id — keeping the row with the latest modify_date."""
    pipeline = BeamExamplePipeline()
    # BundleBasedDirectRunner is the in-process Python runner; Beam 2.72's
    # default "DirectRunner" shells out to the Prism binary (see run() in
    # example_pipeline.py), which we don't need for a hermetic unit test.
    with BeamTestPipeline(runner="BundleBasedDirectRunner") as p:
        records = pipeline.build(p)

        # Three raw rows in, two out: post_id 1 appears twice and is deduped.
        titles = records | "Titles" >> beam.Map(lambda r: r["title"])
        assert_that(titles, equal_to(["FIRST POST UPDATED", "SECOND POST"]), label="titles")

        # The surviving post_id 1 is the later (2024-06-01) revision, not the
        # original — proving DedupBy keeps the latest modify_date.
        kept = records | "Kept" >> beam.Map(lambda r: (r["post_id"], r["modify_date"]))
        assert_that(kept, equal_to([("1", "2024-06-01"), ("2", "2024-01-01")]), label="kept")

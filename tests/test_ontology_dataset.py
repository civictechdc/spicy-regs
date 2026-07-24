"""Ontology materialized-dataset source-contract tests."""

from spicy_regs.pipelines.ontology_dataset import OntologyDatasetPipeline


def test_full_refresh_requires_federal_register_topics() -> None:
    requirements = OntologyDatasetPipeline(
        full_refresh=True,
    ).source_column_requirements()

    assert "topics_json" in requirements["federal_register.parquet"]


def test_identity_only_refresh_does_not_require_topics() -> None:
    requirements = OntologyDatasetPipeline(
        full_refresh=False,
    ).source_column_requirements()

    assert "topics_json" not in requirements["federal_register.parquet"]

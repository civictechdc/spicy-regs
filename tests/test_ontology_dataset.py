"""Ontology materialized-dataset source and publication-contract tests."""

from pathlib import Path

import pytest

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


def test_dry_run_does_not_require_rulespec_release(tmp_path: Path) -> None:
    pipeline = OntologyDatasetPipeline(
        skip_upload=True,
        rulespec_declaration=tmp_path / "missing.yaml",
    )

    pipeline._validate_publication_environment()


def test_publication_checks_rulespec_release_before_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT",
        "R2_PUBLIC_URL",
    ):
        monkeypatch.setenv(name, "configured")
    declaration = tmp_path / "rulespec-l0.yaml"
    declaration.write_text(
        "\n".join(
            (
                'rulespec_version: "sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d"',
                "rulespec_release: null",
                "rulespec_release_url: null",
                "declared_levels: [L0]",
                "results:",
                "  L0: pass",
                "",
            )
        ),
        encoding="utf-8",
    )
    pipeline = OntologyDatasetPipeline(
        skip_upload=False,
        rulespec_declaration=declaration,
    )

    with pytest.raises(RuntimeError, match="does not pin a released semantic version"):
        pipeline._validate_publication_environment()

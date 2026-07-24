"""Ontology materialized-dataset source-contract tests."""

from pathlib import Path

import httpx
import pytest

from spicy_regs.pipelines.ontology_dataset import (
    OntologyDatasetPipeline,
    _require_released_rulespec,
)

CONTRACT_DIGEST = "sha256:ea9b899ba92955b83638ece811d7a4b744dd912f72e19290e32c97508674de1c"
RELEASE_VERSION = "0.2.0-pre.8"
RELEASE_URL = f"https://github.com/Formspec-Labs/rulespec/releases/tag/v{RELEASE_VERSION}"


def _write_declaration(
    path: Path,
    *,
    release: str | None = RELEASE_VERSION,
    release_url: str | None = RELEASE_URL,
) -> None:
    path.write_text(
        "\n".join(
            (
                f'rulespec_version: "{CONTRACT_DIGEST}"',
                f"rulespec_release: {release or 'null'}",
                f"rulespec_release_url: {release_url or 'null'}",
                "declared_levels: [L0]",
                "results:",
                "  L0: pass",
                "",
            )
        ),
        encoding="utf-8",
    )


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


def test_release_declaration_accepts_matching_release_and_digest(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)

    _require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_rejects_unreleased_candidate(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration, release=None, release_url=None)

    with pytest.raises(RuntimeError, match="does not pin a released semantic version"):
        _require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_rejects_url_for_another_version(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(
        declaration,
        release_url="https://github.com/Formspec-Labs/rulespec/releases/tag/v0.2.0-pre.9",
    )

    with pytest.raises(RuntimeError, match="Rulespec release URL must be"):
        _require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_rejects_unreachable_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)
    response = httpx.Response(
        404,
        request=httpx.Request("HEAD", RELEASE_URL),
    )
    monkeypatch.setattr(httpx, "head", lambda *args, **kwargs: response)

    with pytest.raises(RuntimeError, match="pinned Rulespec release is not reachable"):
        _require_released_rulespec(declaration)


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
    _write_declaration(declaration, release=None, release_url=None)
    pipeline = OntologyDatasetPipeline(
        skip_upload=False,
        rulespec_declaration=declaration,
    )

    with pytest.raises(RuntimeError, match="does not pin a released semantic version"):
        pipeline._validate_publication_environment()

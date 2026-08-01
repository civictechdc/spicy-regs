"""Focused command-line wiring tests for document projection."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
ASSET_ID = "urn:ref:vocabulary-atlas:" + "c" * 64
RELEASE_ID = "urn:test:reference-release:subjects:v1"
FACET = "urn:ref:facet:general-subject"
ROLE = "https://rulespec.org/ns/v1#assignmentPrimary"
ROUTE = "document"


@pytest.fixture
def cli() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "tools" / "project_document_to_rkaf.py"
    spec = importlib.util.spec_from_file_location(
        "project_document_to_rkaf_cli_test",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _base_arguments(output_dir: Path) -> list[str]:
    return [
        "--profile",
        "federal-register-document-v1",
        "--subject",
        "2026-00001",
        "--output-dir",
        str(output_dir),
        "--rulespec-version",
        "0.2.0-pre.9",
        "--rulespec-constraint-digest",
        DIGEST_A,
    ]


def _managed_arguments(
    manifest: Path,
    *,
    override_permissions: bool = False,
) -> list[str]:
    values = [
        "--managed-release-manifest",
        str(manifest),
        "--managed-release-manifest-digest",
        DIGEST_A,
    ]
    if override_permissions:
        values.extend(
            [
                "--managed-release-permission-facet",
                FACET,
                "--managed-release-permission-assignment-role",
                ROLE,
                "--managed-release-permission-resource-route",
                ROUTE,
            ]
        )
    return values


def _atlas_arguments(manifest: Path) -> list[str]:
    return [
        "--vocabulary-atlas-manifest",
        str(manifest),
        "--vocabulary-atlas-asset-id",
        ASSET_ID,
        "--vocabulary-atlas-manifest-digest",
        DIGEST_A,
        "--vocabulary-atlas-output-digest",
        DIGEST_B,
        "--vocabulary-reference-release-id",
        RELEASE_ID,
        "--vocabulary-reference-release-digest",
        DIGEST_A,
    ]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            ["--managed-release-manifest", "release.json"],
            "must be supplied together",
        ),
        (
            ["--managed-release-manifest-digest", DIGEST_A],
            "must be supplied together",
        ),
        (
            ["--concept-domain-bridge", "bridge.json"],
            "must be supplied together",
        ),
        (
            ["--concept-domain-bridge-digest", DIGEST_B],
            "must be supplied together",
        ),
        (
            [
                "--concept-domain-bridge",
                "bridge.json",
                "--concept-domain-bridge-digest",
                DIGEST_B,
            ],
            "requires --managed-release-manifest",
        ),
    ],
)
def test_cli_requires_paired_managed_release_and_bridge_pins(
    cli: ModuleType,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
    message: str,
) -> None:
    output = tmp_path / "output"
    with pytest.raises(SystemExit) as raised:
        cli.main(_base_arguments(output) + arguments)

    assert raised.value.code == 2
    assert message in capsys.readouterr().err
    assert not output.exists()


def test_cli_keeps_migration_and_managed_release_modes_exclusive(
    cli: ModuleType,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "output"
    arguments = (
        _base_arguments(output)
        + _managed_arguments(tmp_path / "release.json")
        + [
            "--migration-vocabulary-dir",
            str(tmp_path / "migration"),
        ]
    )

    with pytest.raises(SystemExit) as raised:
        cli.main(arguments)

    assert raised.value.code == 2
    assert "choose one candidate source" in capsys.readouterr().err
    assert not output.exists()


def test_cli_does_not_load_managed_lookup_under_no_model(
    cli: ModuleType,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "output"
    arguments = _base_arguments(output) + _managed_arguments(tmp_path / "release.json") + ["--no-model"]

    with pytest.raises(SystemExit) as raised:
        cli.main(arguments)

    assert raised.value.code == 2
    assert "diagnostic model layer" in capsys.readouterr().err
    assert not output.exists()


def _result() -> SimpleNamespace:
    return SimpleNamespace(
        document={"@context": "./rkaf-context.jsonld", "@graph": []},
        run_record={
            "judgments": {"accepted": [], "rejected": []},
            "notes": [],
        },
        transcript=("projection verified",),
        node_count=0,
    )


def test_cli_opens_pinned_managed_release_and_bridge_then_passes_both(
    cli: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    managed_manifest = tmp_path / "managed-release.json"
    bridge_path = tmp_path / "domain-bridge.json"
    source = SimpleNamespace(
        view=object(),
        usage_ceiling="candidateUseOnly",
    )
    bridge = SimpleNamespace(development_only=True)

    def _open_legacy(
        args: Any,
        *,
        lookup_index_manifest: dict[str, str],
    ) -> tuple[SimpleNamespace, tuple[SimpleNamespace, ...]]:
        calls["legacy_open"] = (args, lookup_index_manifest)
        return source, (bridge,)

    def _build_model(
        provider: str,
        model_id: str | None,
        *,
        compat_provider: str = "",
    ) -> object:
        calls["model"] = (provider, model_id, compat_provider)
        return object()

    def _project_document(
        profile: str,
        subject: str,
        **kwargs: Any,
    ) -> SimpleNamespace:
        calls["project"] = (profile, subject, kwargs)
        return _result()

    monkeypatch.setattr(cli, "_open_legacy_candidate_source", _open_legacy)
    monkeypatch.setattr(cli, "build_model", _build_model)
    monkeypatch.setattr(cli, "project_document", _project_document)

    output = tmp_path / "output"
    return_code = cli.main(
        _base_arguments(output)
        + _managed_arguments(managed_manifest)
        + [
            "--concept-domain-bridge",
            str(bridge_path),
            "--concept-domain-bridge-digest",
            DIGEST_B,
        ]
    )

    assert return_code == 0
    expected_lookup = cli._managed_lookup_index_manifest(
        managed_release_manifest_digest=DIGEST_A,
        concept_domain_bridge_digest=DIGEST_B,
        permission_facet_iri=FACET,
        permission_assignment_role_iri=ROLE,
        permission_resource_route=ROUTE,
        default_language="en",
    )
    legacy_args, actual_lookup = calls["legacy_open"]
    assert legacy_args.managed_release_manifest == managed_manifest
    assert legacy_args.concept_domain_bridge == bridge_path
    assert actual_lookup == expected_lookup
    assert expected_lookup["id"] == ("urn:spicy-regs:lookup-index:" + expected_lookup["digest"].removeprefix("sha256:"))

    _, _, project_kwargs = calls["project"]
    assert project_kwargs["candidate_release_source"] is source
    assert project_kwargs["concept_domain_bridges"] == (bridge,)
    assert project_kwargs["settings"].migration_vocabulary_directory is None
    assert "accepted_output" not in project_kwargs
    assert source.usage_ceiling == "candidateUseOnly"
    assert (output / "projection-run.json").is_file()


def test_cli_opens_pinned_atlas_and_passes_file_reader(
    cli: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    manifest = tmp_path / "atlas-manifest.json"
    nquads = tmp_path / "published-atlas.nq"
    source = SimpleNamespace(usage_ceiling="diagnosticCandidateOnly")

    class _AtlasSource:
        @classmethod
        def open(
            cls,
            selected_manifest: Path,
            **kwargs: Any,
        ) -> SimpleNamespace:
            calls["atlas_open"] = (selected_manifest, kwargs)
            return source

    def _build_model(
        provider: str,
        model_id: str | None,
        *,
        compat_provider: str = "",
    ) -> object:
        calls["model"] = (provider, model_id, compat_provider)
        return object()

    def _project_document(
        profile: str,
        subject: str,
        **kwargs: Any,
    ) -> SimpleNamespace:
        calls["project"] = (profile, subject, kwargs)
        return _result()

    monkeypatch.setattr(cli, "VocabularyAtlasCandidateSource", _AtlasSource)
    monkeypatch.setattr(cli, "build_model", _build_model)
    monkeypatch.setattr(cli, "project_document", _project_document)

    output = tmp_path / "output"
    arguments = (
        _base_arguments(output)
        + _atlas_arguments(manifest)
        + [
            "--vocabulary-atlas-nquads",
            str(nquads),
            "--candidate-facet",
            FACET,
            "--candidate-assignment-role",
            ROLE,
            "--candidate-resource-route",
            ROUTE,
        ]
    )

    assert cli.main(arguments) == 0

    opened_manifest, kwargs = calls["atlas_open"]
    assert opened_manifest == manifest
    assert kwargs["nquads_path"] == nquads
    assert kwargs["expected_asset_id"] == ASSET_ID
    assert kwargs["expected_manifest_digest"] == DIGEST_A
    assert kwargs["expected_output_digest"] == DIGEST_B
    assert kwargs["reference_release_id"] == RELEASE_ID
    assert kwargs["reference_release_digest"] == DIGEST_A
    assert kwargs["facet_iri"] == FACET
    assert kwargs["assignment_role_iri"] == ROLE
    assert kwargs["resource_route"] == ROUTE
    expected_lookup = cli._atlas_lookup_index_manifest(
        asset_id=ASSET_ID,
        manifest_digest=DIGEST_A,
        output_digest=DIGEST_B,
        reference_release_id=RELEASE_ID,
        reference_release_digest=DIGEST_A,
        facet_iri=FACET,
        assignment_role_iri=ROLE,
        resource_route=ROUTE,
        default_language="en",
    )
    assert kwargs["lookup_index_manifest"] == expected_lookup
    _, _, project_kwargs = calls["project"]
    assert project_kwargs["candidate_release_source"] is source
    assert project_kwargs["concept_domain_bridges"] == ()
    assert source.usage_ceiling == "diagnosticCandidateOnly"


def test_lookup_index_pin_is_path_independent_and_changes_with_inputs(
    cli: ModuleType,
) -> None:
    values = {
        "managed_release_manifest_digest": DIGEST_A,
        "concept_domain_bridge_digest": DIGEST_B,
        "permission_facet_iri": FACET,
        "permission_assignment_role_iri": ROLE,
        "permission_resource_route": ROUTE,
        "default_language": "en",
    }

    first = cli._managed_lookup_index_manifest(**values)
    repeated = cli._managed_lookup_index_manifest(**values)
    changed = cli._managed_lookup_index_manifest(**{**values, "default_language": "es"})

    assert first == repeated
    assert first != changed
    assert first["digest"].startswith("sha256:")


def test_existing_no_model_path_passes_no_managed_sources(
    cli: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def _project_document(
        profile: str,
        subject: str,
        **kwargs: Any,
    ) -> SimpleNamespace:
        calls["project"] = (profile, subject, kwargs)
        return _result()

    def _unexpected_model(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("the no-model path built a model")

    monkeypatch.setattr(cli, "project_document", _project_document)
    monkeypatch.setattr(cli, "build_model", _unexpected_model)

    output = tmp_path / "output"
    assert cli.main(_base_arguments(output) + ["--no-model"]) == 0

    _, _, project_kwargs = calls["project"]
    assert project_kwargs["model"] is None
    assert project_kwargs["candidate_release_source"] is None
    assert project_kwargs["concept_domain_bridges"] == ()

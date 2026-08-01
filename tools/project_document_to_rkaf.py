#!/usr/bin/env python3
"""Project one corpus document into a gate-valid Rulespec (RKAF) JSON-LD object.

Deterministic layer only:

    python3 tools/project_document_to_rkaf.py \
        --profile federal-register-document-v1 --subject 2026-03227 \
        --corpus-dir output/segmented-real-data-evaluation-v2 \
        --tables-dir output/rulespec-stabilization-candidate-final \
        --output-dir output/rkaf-projection-fsis --no-model

With the model layer (concept assignments only; identity stays deterministic):

    python3 tools/project_document_to_rkaf.py ... \
        --vocabulary-atlas-manifest published/atlas-manifest.json \
        --vocabulary-atlas-asset-id urn:ref:vocabulary-atlas:<generation> \
        --vocabulary-atlas-manifest-digest sha256:<manifest> \
        --vocabulary-atlas-output-digest sha256:<nquads> \
        --vocabulary-reference-release-id <release-iri> \
        --vocabulary-reference-release-digest sha256:<release> \
        --provider openai --model gpt-5.6-sol

Writes ``<output-dir>/<slug>.rulespec.jsonld``, ``projection-run.json``,
``offset-verification.txt``, and a copy of the rulespec JSON-LD context so the
document is self-contained for JSON-LD/SHACL processing. With the model layer it
also writes ``<output-dir>/extraction-run/`` — the full docpipeline run,
including ``request.json`` and ``response.json`` for every provider call.

Every run requires the exact Rulespec version and constraint digest. Supply
``--rulespec-revision`` only for a tested committed revision; omitting it
records an honest local candidate and cannot support an immutable conformance
claim. Model results on this command remain diagnostic review-queue candidates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.candidate_release import (  # noqa: E402
    CandidateReleaseError,
    VocabularyAtlasCandidateSource,
)
from spicy_regs.docpipeline.rkaf_projection import (  # noqa: E402
    ProjectionError,
    ProjectionSettings,
    project_document,
)

DEFAULT_CORPUS_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
DEFAULT_TABLES_DIR = REPO_ROOT / "output" / "rulespec-stabilization-candidate-final"
CONTEXT_NAME = "rkaf-context.jsonld"
DEFAULT_CANDIDATE_FACET = "urn:ref:facet:general-subject"
DEFAULT_CANDIDATE_ASSIGNMENT_ROLE = (
    "https://rulespec.org/ns/v1#assignmentPrimary"
)
DEFAULT_CANDIDATE_RESOURCE_ROUTE = "document"


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower() or "document"


def _managed_lookup_index_manifest(
    *,
    managed_release_manifest_digest: str,
    concept_domain_bridge_digest: str | None,
    permission_facet_iri: str,
    permission_assignment_role_iri: str,
    permission_resource_route: str,
    default_language: str,
) -> dict[str, str]:
    """Derive the in-memory lookup index pin from every behavior input."""

    identity = {
        "schemaVersion": "managed-release-candidate-lookup-index/v1",
        "lookupAdapter": "spicy-regs-managed-release-candidate-vocabulary/v1",
        "managedReleaseManifestDigest": managed_release_manifest_digest,
        "conceptDomainBridgeDigests": (
            [concept_domain_bridge_digest]
            if concept_domain_bridge_digest is not None
            else []
        ),
        "permission": {
            "facet": permission_facet_iri,
            "assignmentRole": permission_assignment_role_iri,
            "resourceRoute": permission_resource_route,
        },
        "defaultLanguage": default_language,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
    return {
        "id": (
            "urn:spicy-regs:lookup-index:"
            f"{digest.removeprefix('sha256:')}"
        ),
        "digest": digest,
    }


def _atlas_lookup_index_manifest(
    *,
    asset_id: str,
    manifest_digest: str,
    output_digest: str,
    reference_release_id: str,
    reference_release_digest: str,
    facet_iri: str,
    assignment_role_iri: str,
    resource_route: str,
    default_language: str,
) -> dict[str, str]:
    """Pin the local lookup view to every atlas selection input."""

    identity = {
        "schemaVersion": "spicy-regs-atlas-candidate-lookup-index/v1",
        "sourceAsset": {
            "type": "VocabularyAtlasAsset",
            "assetId": asset_id,
            "manifestDigest": manifest_digest,
            "outputDigest": output_digest,
        },
        "referenceResourceRelease": {
            "id": reference_release_id,
            "digest": reference_release_digest,
        },
        "selection": {
            "facet": facet_iri,
            "assignmentRole": assignment_role_iri,
            "resourceRoute": resource_route,
        },
        "defaultLanguage": default_language,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
    return {
        "id": "urn:spicy-regs:lookup-index:" + digest.removeprefix("sha256:"),
        "digest": digest,
    }


def _open_legacy_candidate_source(
    args: argparse.Namespace,
    *,
    lookup_index_manifest: dict[str, str],
) -> tuple[Any, tuple[Any, ...]]:
    """Open the explicitly selected pre-atlas compatibility path lazily."""

    from refspec import ManagedReleaseError
    from refspec.registry import (
        ConceptDomainBridgeError,
        load_concept_domain_bridge,
    )
    from spicy_regs.enrichment.managed_release import (
        ManagedReleaseCandidateSource,
        ManagedReleaseConsumerError,
    )

    try:
        source = ManagedReleaseCandidateSource.open(
            args.managed_release_manifest,
            expected_manifest_digest=(
                args.managed_release_manifest_digest
            ),
            lookup_index_manifest=lookup_index_manifest,
            permission_facet_iri=(
                args.managed_release_permission_facet
            ),
            permission_assignment_role_iri=(
                args.managed_release_permission_assignment_role
            ),
            permission_resource_route=(
                args.managed_release_permission_resource_route
            ),
        )
        bridges = ()
        if args.concept_domain_bridge is not None:
            bridges = (
                load_concept_domain_bridge(
                    args.concept_domain_bridge,
                    expected_sha256=args.concept_domain_bridge_digest,
                    target_view=source.view,
                ),
            )
        return source, bridges
    except (
        ConceptDomainBridgeError,
        ManagedReleaseConsumerError,
        ManagedReleaseError,
    ) as error:
        raise CandidateReleaseError(str(error)) from error


def _validate_vocabulary_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    atlas_values = {
        "--vocabulary-atlas-manifest": args.vocabulary_atlas_manifest,
        "--vocabulary-atlas-asset-id": args.vocabulary_atlas_asset_id,
        "--vocabulary-atlas-manifest-digest": (
            args.vocabulary_atlas_manifest_digest
        ),
        "--vocabulary-atlas-output-digest": (
            args.vocabulary_atlas_output_digest
        ),
        "--vocabulary-reference-release-id": (
            args.vocabulary_reference_release_id
        ),
        "--vocabulary-reference-release-digest": (
            args.vocabulary_reference_release_digest
        ),
    }
    supplied_atlas_values = {
        option for option, value in atlas_values.items() if value is not None
    }
    if supplied_atlas_values and len(supplied_atlas_values) != len(atlas_values):
        missing = sorted(set(atlas_values) - supplied_atlas_values)
        parser.error(
            "atlas candidate lookup requires " + ", ".join(missing)
        )
    atlas_present = bool(supplied_atlas_values)
    if args.vocabulary_atlas_nquads is not None and not atlas_present:
        parser.error(
            "--vocabulary-atlas-nquads requires --vocabulary-atlas-manifest"
        )

    managed_manifest_present = (
        args.managed_release_manifest is not None
    )
    managed_digest_present = (
        args.managed_release_manifest_digest is not None
    )
    if managed_manifest_present != managed_digest_present:
        parser.error(
            "--managed-release-manifest and "
            "--managed-release-manifest-digest must be supplied together"
        )

    bridge_path_present = args.concept_domain_bridge is not None
    bridge_digest_present = (
        args.concept_domain_bridge_digest is not None
    )
    if bridge_path_present != bridge_digest_present:
        parser.error(
            "--concept-domain-bridge and "
            "--concept-domain-bridge-digest must be supplied together"
        )
    if bridge_path_present and not managed_manifest_present:
        parser.error(
            "--concept-domain-bridge requires --managed-release-manifest"
        )

    migration_present = (
        args.migration_vocabulary_dir is not None
        or args.migration_vocabulary_manifest is not None
    )
    selected_modes = sum(
        (atlas_present, managed_manifest_present, migration_present)
    )
    if selected_modes > 1:
        parser.error(
            "choose one candidate source: vocabulary atlas, legacy managed "
            "release, or migration-only vocabulary"
        )
    if atlas_present and bridge_path_present:
        parser.error(
            "--concept-domain-bridge is available only with the legacy "
            "managed-release compatibility path"
        )
    if args.no_model and (
        atlas_present or managed_manifest_present or bridge_path_present
    ):
        parser.error(
            "candidate sources and concept-domain bridges are used only by "
            "the diagnostic model layer; remove --no-model"
        )


def build_model(provider: str, model_id: str | None, *, compat_provider: str = "") -> Any:
    """Construct the requested provider arm. Never called under ``--no-model``.

    The arms are used exactly as they define themselves: the OpenAI arm reads
    its pinned model from the environment and returns ``None`` without a
    credential; the Anthropic and compat arms refuse rather than degrade.
    """
    if provider == "openai":
        import os

        from spicy_regs.docpipeline.adapters.openai import (
            MODEL_ENVIRONMENT_VARIABLE,
            OpenAIStructuredTextModel,
        )

        if model_id:
            os.environ[MODEL_ENVIRONMENT_VARIABLE] = model_id
        model = OpenAIStructuredTextModel.from_environment()
        if model is None:
            raise ProjectionError("openai: OPENAI_API_KEY is unset — pass --no-model or supply a credential")
        return model
    if provider == "anthropic":
        from spicy_regs.docpipeline.adapters.anthropic import AnthropicStructuredTextModel

        # The arm deliberately declares no default model: it is chosen on
        # purpose, and guessing one would publish a run nobody asked for.
        if not model_id:
            raise ProjectionError("anthropic: pass --model; the Anthropic arm declares no default model")
        return AnthropicStructuredTextModel.from_environment(model=model_id)
    if provider == "openai-compatible":
        from spicy_regs.docpipeline.adapters.openai_compatible import (
            OpenAICompatibleStructuredTextModel,
        )

        if not compat_provider or not model_id:
            raise ProjectionError("openai-compatible needs --compat-provider and --model")
        return OpenAICompatibleStructuredTextModel.from_environment(provider=compat_provider, model=model_id)
    raise ProjectionError(f"unknown provider {provider!r}")


def find_context(explicit: Path | None) -> Path | None:
    if explicit is not None:
        return explicit
    candidate = REPO_ROOT / "docs" / "evidence" / "single-document-rulespec-projection-2026-07-28" / CONTEXT_NAME
    return candidate if candidate.is_file() else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", required=True, help="Source profile id, e.g. federal-register-document-v1")
    parser.add_argument("--subject", required=True, help="Subject id within that profile")
    parser.add_argument("--corpus-dir", type=Path, default=DEFAULT_CORPUS_DIR)
    parser.add_argument("--tables-dir", type=Path, default=DEFAULT_TABLES_DIR)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--rulespec-version",
        required=True,
        help="Exact Rulespec semantic version used by this projection.",
    )
    parser.add_argument(
        "--rulespec-constraint-digest",
        required=True,
        help="Exact sha256:<64 lowercase hex> Rulespec constraint digest.",
    )
    parser.add_argument(
        "--rulespec-revision",
        default=None,
        help=("Exact tested 40-character Git revision. Omit only for a local uncommitted candidate."),
    )
    parser.add_argument("--no-model", action="store_true", help="Deterministic projection; zero API calls.")
    parser.add_argument("--provider", default="openai", choices=("openai", "anthropic", "openai-compatible"))
    parser.add_argument("--model", default=None, help="Provider model id; defaults to the arm's pinned model.")
    parser.add_argument("--compat-provider", default="", help="Named profile for --provider openai-compatible.")
    parser.add_argument(
        "--vocabulary-atlas-manifest",
        type=Path,
        default=None,
        help=(
            "Published atlas-manifest.json for file-only diagnostic candidate "
            "lookup. Requires the asset, manifest, output, and reference-release "
            "pins below."
        ),
    )
    parser.add_argument(
        "--vocabulary-atlas-nquads",
        type=Path,
        default=None,
        help="Published atlas.nq; defaults to the manifest's sibling atlas.nq.",
    )
    parser.add_argument(
        "--vocabulary-atlas-asset-id",
        default=None,
        help="Exact urn:ref:vocabulary-atlas:<generation hex> asset id.",
    )
    parser.add_argument(
        "--vocabulary-atlas-manifest-digest",
        default=None,
        help="Exact sha256:<64 lowercase hex> manifest file digest.",
    )
    parser.add_argument(
        "--vocabulary-atlas-output-digest",
        default=None,
        help="Exact sha256:<64 lowercase hex> atlas.nq file digest.",
    )
    parser.add_argument(
        "--vocabulary-reference-release-id",
        default=None,
        help="Exact ReferenceResourceRelease selected for candidates.",
    )
    parser.add_argument(
        "--vocabulary-reference-release-digest",
        default=None,
        help="Exact sha256:<64 lowercase hex> digest of the selected release.",
    )
    parser.add_argument(
        "--candidate-facet",
        default=DEFAULT_CANDIDATE_FACET,
        help=(
            "SpicyRegs-local facet for candidate selection "
            f"(default: {DEFAULT_CANDIDATE_FACET})."
        ),
    )
    parser.add_argument(
        "--candidate-assignment-role",
        default=DEFAULT_CANDIDATE_ASSIGNMENT_ROLE,
        help=(
            "SpicyRegs-local Rulespec assignment role for candidates "
            f"(default: {DEFAULT_CANDIDATE_ASSIGNMENT_ROLE})."
        ),
    )
    parser.add_argument(
        "--candidate-resource-route",
        default=DEFAULT_CANDIDATE_RESOURCE_ROUTE,
        help=(
            "SpicyRegs-local resource route for candidates "
            f"(default: {DEFAULT_CANDIDATE_RESOURCE_ROUTE})."
        ),
    )
    parser.add_argument(
        "--migration-vocabulary-dir",
        type=Path,
        default=None,
        help=(
            "Migration-only directory containing concept_labels.parquet, "
            "concept_relations.parquet, concept_event_participants.parquet, "
            "and vocabulary-manifest.jsonld. Required with the model layer."
        ),
    )
    parser.add_argument(
        "--migration-vocabulary-manifest",
        type=Path,
        default=None,
        help=(
            "Migration-only RKAF JSON-LD manifest; defaults to "
            "<migration-vocabulary-dir>/vocabulary-manifest.jsonld."
        ),
    )
    parser.add_argument(
        "--managed-release-manifest",
        type=Path,
        default=None,
        help=(
            "Legacy compatibility: exact RefSpec managed-release bundle "
            "manifest for candidate lookup. Requires its digest."
        ),
    )
    parser.add_argument(
        "--managed-release-manifest-digest",
        default=None,
        help=(
            "Exact sha256:<64 lowercase hex> digest of "
            "--managed-release-manifest."
        ),
    )
    parser.add_argument(
        "--managed-release-permission-facet",
        default=DEFAULT_CANDIDATE_FACET,
        help=(
            "Legacy managed-release facet IRI "
            f"(default: {DEFAULT_CANDIDATE_FACET})."
        ),
    )
    parser.add_argument(
        "--managed-release-permission-assignment-role",
        default=DEFAULT_CANDIDATE_ASSIGNMENT_ROLE,
        help=(
            "Legacy managed-release Rulespec assignment-role IRI "
            f"(default: {DEFAULT_CANDIDATE_ASSIGNMENT_ROLE})."
        ),
    )
    parser.add_argument(
        "--managed-release-permission-resource-route",
        default=DEFAULT_CANDIDATE_RESOURCE_ROUTE,
        help=(
            "Legacy managed-release resource route "
            f"use (default: {DEFAULT_CANDIDATE_RESOURCE_ROUTE})."
        ),
    )
    parser.add_argument(
        "--concept-domain-bridge",
        type=Path,
        default=None,
        help=(
            "Pinned development-only RefSpec concept-domain bridge used to "
            "expand managed-release candidate lookup."
        ),
    )
    parser.add_argument(
        "--concept-domain-bridge-digest",
        default=None,
        help=(
            "Exact sha256:<64 lowercase hex> digest of "
            "--concept-domain-bridge."
        ),
    )
    parser.add_argument(
        "--vocabulary-default-language",
        default="en",
        help=("BCP 47 language materialized on any scalar authored vocabulary text (default: en)."),
    )
    parser.add_argument("--prompt-concept-limit", type=int, default=12)
    parser.add_argument(
        "--max-segments",
        type=int,
        default=0,
        help="Cap the segments sent to the model (0 = every segment). Recorded in the run record.",
    )
    parser.add_argument("--partner", default="urn:rkaf:partner:spicy-regs")
    parser.add_argument("--scope", default="document-rkaf-projection")
    parser.add_argument("--asserted-at", default=None, help="Freeze the projection timestamp (ISO-8601).")
    parser.add_argument("--context-file", type=Path, default=None, help="rkaf-context.jsonld to copy alongside.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output directory.")
    args = parser.parse_args(argv)
    _validate_vocabulary_arguments(args, parser)

    output_dir = args.output_dir
    if output_dir.exists():
        if not args.force:
            print(f"ERROR: {output_dir} exists (use --force to replace it)", file=sys.stderr)
            return 2
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    try:
        settings = ProjectionSettings(
            corpus_dir=args.corpus_dir,
            tables_dir=args.tables_dir,
            rulespec_version=args.rulespec_version,
            rulespec_constraint_digest=args.rulespec_constraint_digest,
            rulespec_source_revision=args.rulespec_revision,
            partner=args.partner,
            scope=args.scope,
            context_ref=f"./{CONTEXT_NAME}",
            asserted_at=args.asserted_at,
            migration_vocabulary_directory=(
                None if args.no_model else args.migration_vocabulary_dir
            ),
            migration_vocabulary_manifest_path=(
                None
                if args.no_model
                else args.migration_vocabulary_manifest
            ),
            vocabulary_default_language=args.vocabulary_default_language,
            prompt_concept_limit=args.prompt_concept_limit,
            max_segments=args.max_segments,
        )
        model = None
        model_run_directory = None
        candidate_source = None
        concept_domain_bridges = ()
        if not args.no_model:
            if args.vocabulary_atlas_manifest is not None:
                assert args.vocabulary_atlas_asset_id is not None
                assert args.vocabulary_atlas_manifest_digest is not None
                assert args.vocabulary_atlas_output_digest is not None
                assert args.vocabulary_reference_release_id is not None
                assert (
                    args.vocabulary_reference_release_digest is not None
                )
                lookup_index_manifest = _atlas_lookup_index_manifest(
                    asset_id=args.vocabulary_atlas_asset_id,
                    manifest_digest=(
                        args.vocabulary_atlas_manifest_digest
                    ),
                    output_digest=args.vocabulary_atlas_output_digest,
                    reference_release_id=(
                        args.vocabulary_reference_release_id
                    ),
                    reference_release_digest=(
                        args.vocabulary_reference_release_digest
                    ),
                    facet_iri=args.candidate_facet,
                    assignment_role_iri=(
                        args.candidate_assignment_role
                    ),
                    resource_route=args.candidate_resource_route,
                    default_language=args.vocabulary_default_language,
                )
                candidate_source = VocabularyAtlasCandidateSource.open(
                    args.vocabulary_atlas_manifest,
                    nquads_path=args.vocabulary_atlas_nquads,
                    expected_asset_id=args.vocabulary_atlas_asset_id,
                    expected_manifest_digest=(
                        args.vocabulary_atlas_manifest_digest
                    ),
                    expected_output_digest=(
                        args.vocabulary_atlas_output_digest
                    ),
                    reference_release_id=(
                        args.vocabulary_reference_release_id
                    ),
                    reference_release_digest=(
                        args.vocabulary_reference_release_digest
                    ),
                    facet_iri=args.candidate_facet,
                    assignment_role_iri=(
                        args.candidate_assignment_role
                    ),
                    resource_route=args.candidate_resource_route,
                    lookup_index_manifest=lookup_index_manifest,
                )
            elif args.managed_release_manifest is not None:
                assert (
                    args.managed_release_manifest_digest is not None
                )
                lookup_index_manifest = (
                    _managed_lookup_index_manifest(
                        managed_release_manifest_digest=(
                            args.managed_release_manifest_digest
                        ),
                        concept_domain_bridge_digest=(
                            args.concept_domain_bridge_digest
                        ),
                        permission_facet_iri=(
                            args.managed_release_permission_facet
                        ),
                        permission_assignment_role_iri=(
                            args.managed_release_permission_assignment_role
                        ),
                        permission_resource_route=(
                            args.managed_release_permission_resource_route
                        ),
                        default_language=(
                            args.vocabulary_default_language
                        ),
                    )
                )
                (
                    candidate_source,
                    concept_domain_bridges,
                ) = _open_legacy_candidate_source(
                    args,
                    lookup_index_manifest=lookup_index_manifest,
                )
            model = build_model(
                args.provider,
                args.model,
                compat_provider=args.compat_provider,
            )
            model_run_directory = output_dir / "extraction-run"
        result = project_document(
            args.profile,
            args.subject,
            settings=settings,
            model=model,
            model_run_directory=model_run_directory,
            candidate_release_source=candidate_source,
            concept_domain_bridges=concept_domain_bridges,
        )
    except (CandidateReleaseError, ProjectionError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    slug = _slug(f"{args.profile}-{args.subject}")
    document_path = output_dir / f"{slug}.rulespec.jsonld"
    document_path.write_text(json.dumps(result.document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (output_dir / "projection-run.json").write_text(
        json.dumps(result.run_record, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "offset-verification.txt").write_text("\n".join(result.transcript) + "\n", encoding="utf-8")

    context_source = find_context(args.context_file)
    if context_source is not None:
        shutil.copyfile(context_source, output_dir / CONTEXT_NAME)
    else:
        print(f"WARNING: no {CONTEXT_NAME} copied; pass --context-file for a self-contained document", file=sys.stderr)

    print("\n".join(result.transcript))
    accepted = len(result.run_record["judgments"]["accepted"])
    rejected = len(result.run_record["judgments"]["rejected"])
    print(f"\nwrote {document_path} ({result.node_count} graph nodes)")
    print(f"judgments: {accepted} accepted, {rejected} rejected")
    for note in result.run_record["notes"]:
        print(f"note: {note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

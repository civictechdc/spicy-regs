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
        --migration-vocabulary-dir output/normalized-vocabulary-v1 \
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
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.docpipeline.rkaf_projection import (  # noqa: E402
    ProjectionError,
    ProjectionSettings,
    project_document,
)

DEFAULT_CORPUS_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
DEFAULT_TABLES_DIR = REPO_ROOT / "output" / "rulespec-stabilization-candidate-final"
CONTEXT_NAME = "rkaf-context.jsonld"


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower() or "document"


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
        if not args.no_model:
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
        )
    except ProjectionError as error:
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

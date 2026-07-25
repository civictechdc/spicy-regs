"""Attach model-native token-limit evidence to a segmentation experiment."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal, cast

from spicy_regs.corpora.document_acceptance_scope import (
    DocumentAcceptanceScope,
    load_document_acceptance_scope,
)
from spicy_regs.corpora.embedding_audit import (
    EMBEDDING_INPUT_AUDIT_POLICY_VERSION,
    EmbeddingInputAudit,
    EmbeddingInputAuditor,
    HashEmbeddingInputAuditor,
    HuggingFaceEmbeddingInputAuditor,
    TiktokenEmbeddingInputAuditor,
    audit_embedding_inputs,
)
from spicy_regs.corpora.segmentation_experiment import (
    DEFAULT_OMLX_EMBEDDING_MODEL,
    DEFAULT_OMLX_EMBEDDING_REVISION,
    DEFAULT_OMLX_EMBEDDING_SERVICE_MODEL,
    INCUMBENT_EMBEDDING_MODEL,
    INCUMBENT_EMBEDDING_REVISION,
    _artifact_hashes,
    _file_sha256,
    _scoped_artifacts_and_gold,
    _secret_like,
    _semantic_units,
    _stored_bool,
    _unit_embedding_texts,
    validate_segmentation_experiment,
)
from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.segmentation import TiktokenCounter

FORMAT_VERSION = 1
AUDIT_VERSION = "segmentation-embedding-input-audit-v1"
OPENAI_EMBEDDING_ENCODING = "cl100k_base"
OPENAI_EMBEDDING_MAX_INPUT_TOKENS = 8_192
INCUMBENT_BGE_MAX_INPUT_TOKENS = 512
DEFAULT_OMLX_MAX_INPUT_TOKENS = 8_192
AuditorName = Literal["deterministic", "incumbent-bge", "openai", "omlx"]

AUDIT_COLUMNS = (
    "text_sha256",
    "embedding_model_id",
    "embedding_input_characters",
    "audit_policy_version",
    "model_tokenizer_id",
    "model_token_count",
    "model_token_sequence_sha256",
    "model_max_input_tokens",
    "model_overflow_policy",
    "model_input_over_limit",
    "model_input_truncated",
)


def _audit_row(
    *,
    text_sha256: str,
    text: str,
    model_id: str,
    audit: EmbeddingInputAudit,
) -> dict[str, Any]:
    return {
        "text_sha256": text_sha256,
        "embedding_model_id": model_id,
        "embedding_input_characters": len(text),
        "audit_policy_version": EMBEDDING_INPUT_AUDIT_POLICY_VERSION,
        "model_tokenizer_id": audit.tokenizer_id,
        "model_token_count": audit.token_count,
        "model_token_sequence_sha256": audit.token_sequence_sha256,
        "model_max_input_tokens": audit.max_input_tokens,
        "model_overflow_policy": audit.overflow_policy,
        "model_input_over_limit": audit.input_over_limit,
        "model_input_truncated": audit.input_truncated,
    }


def _expected_inputs(
    dataset_dir: Path,
    *,
    scope: DocumentAcceptanceScope | None,
) -> dict[str, str]:
    artifacts, gold_rows = _scoped_artifacts_and_gold(dataset_dir, scope)
    counter = TiktokenCounter()
    units_by_artifact = {artifact.digest: _semantic_units(artifact, counter) for artifact in artifacts}
    return _unit_embedding_texts(units_by_artifact, gold_rows)


def _manifest_path(output_dir: Path) -> Path:
    return output_dir / "segmentation-embedding-audit-manifest.json"


def build_segmentation_embedding_audit(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    input_auditor: EmbeddingInputAuditor,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Build a separately immutable audit without regenerating embeddings."""
    if output_dir.exists():
        raise FileExistsError(f"Refusing to replace embedding input audit: {output_dir}")
    experiment_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if experiment_receipt["status"] != "pass":
        raise RuntimeError("source segmentation experiment did not validate")
    experiment_manifest_path = experiment_dir / "segmentation-experiment-manifest.json"
    experiment_manifest = json.loads(experiment_manifest_path.read_text(encoding="utf-8"))
    scope = load_document_acceptance_scope(dataset_dir, scope_dir) if scope_dir is not None else None
    texts_by_sha = _expected_inputs(dataset_dir, scope=scope)
    embedding_rows = read_parquet_rows(experiment_dir / "embedding_cache.parquet")
    embedding_keys = [str(row.get("text_sha256")) for row in embedding_rows]
    if embedding_keys != list(texts_by_sha):
        raise RuntimeError("source embedding cache differs from reconstructed inputs")
    audits = audit_embedding_inputs(
        input_auditor,
        [texts_by_sha[key] for key in embedding_keys],
    )
    model_id = str(experiment_manifest.get("embedding_model_id") or "")
    rows = [
        _audit_row(
            text_sha256=key,
            text=texts_by_sha[key],
            model_id=model_id,
            audit=audit,
        )
        for key, audit in zip(embedding_keys, audits)
    ]
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{output_dir.name}.",
            dir=output_dir.parent,
        )
    )
    try:
        write_parquet_rows(
            temporary / "embedding_input_audit.parquet",
            columns=AUDIT_COLUMNS,
            rows=rows,
        )
        artifacts = _artifact_hashes(temporary)
        audit_id = (
            "segmentation_embedding_audit_"
            + hashlib.sha256(
                canonical_json({name: record["sha256"] for name, record in sorted(artifacts.items())}).encode()
            ).hexdigest()[:24]
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "audit_version": AUDIT_VERSION,
            "audit_id": audit_id,
            "dataset_evaluation_id": experiment_receipt["dataset_evaluation_id"],
            "document_scope_id": experiment_receipt["document_scope_id"],
            "experiment_id": experiment_receipt["experiment_id"],
            "experiment_manifest_sha256": _file_sha256(experiment_manifest_path),
            "embedding_cache_sha256": _file_sha256(experiment_dir / "embedding_cache.parquet"),
            "embedding_model_id": model_id,
            "audit_policy_version": input_auditor.policy_version,
            "model_tokenizer_id": input_auditor.tokenizer_id,
            "model_max_input_tokens": input_auditor.max_input_tokens,
            "model_overflow_policy": input_auditor.overflow_policy,
            "input_count": len(rows),
            "over_limit_input_count": sum(item.input_over_limit for item in audits),
            "truncated_input_count": sum(item.input_truncated for item in audits),
            "artifacts": artifacts,
        }
        _manifest_path(temporary).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = _validate_segmentation_embedding_audit(
            dataset_dir,
            experiment_dir,
            temporary,
            input_auditor=input_auditor,
            scope_dir=scope_dir,
            scope=scope,
        )
        (temporary / "segmentation-embedding-audit-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError("Segmentation embedding input audit failed: " + "; ".join(receipt["failures"]))
        temporary.replace(output_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _validate_segmentation_embedding_audit(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    input_auditor: EmbeddingInputAuditor,
    scope_dir: Path | None = None,
    scope: DocumentAcceptanceScope | None = None,
) -> dict[str, Any]:
    failures: list[str] = []

    def fail(message: str) -> None:
        if message not in failures:
            failures.append(message)

    experiment_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if experiment_receipt["status"] != "pass":
        fail("source segmentation experiment did not validate")
    if scope_dir is not None and scope is None:
        try:
            scope = load_document_acceptance_scope(dataset_dir, scope_dir)
        except (OSError, RuntimeError, ValueError) as exc:
            fail(f"document acceptance scope is invalid: {exc}")
    manifest = json.loads(_manifest_path(output_dir).read_text(encoding="utf-8"))
    rows = read_parquet_rows(output_dir / "embedding_input_audit.parquet")
    experiment_manifest_path = experiment_dir / "segmentation-experiment-manifest.json"
    experiment_manifest = json.loads(experiment_manifest_path.read_text(encoding="utf-8"))
    for field, expected in (
        ("format_version", FORMAT_VERSION),
        ("audit_version", AUDIT_VERSION),
        (
            "dataset_evaluation_id",
            experiment_receipt["dataset_evaluation_id"],
        ),
        ("document_scope_id", experiment_receipt["document_scope_id"]),
        ("experiment_id", experiment_receipt["experiment_id"]),
        (
            "experiment_manifest_sha256",
            _file_sha256(experiment_manifest_path),
        ),
        (
            "embedding_cache_sha256",
            _file_sha256(experiment_dir / "embedding_cache.parquet"),
        ),
        (
            "embedding_model_id",
            experiment_manifest.get("embedding_model_id"),
        ),
        (
            "audit_policy_version",
            EMBEDDING_INPUT_AUDIT_POLICY_VERSION,
        ),
        ("model_tokenizer_id", input_auditor.tokenizer_id),
        ("model_max_input_tokens", input_auditor.max_input_tokens),
        ("model_overflow_policy", input_auditor.overflow_policy),
    ):
        if manifest.get(field) != expected:
            fail(f"manifest {field} differs")
    texts_by_sha = _expected_inputs(dataset_dir, scope=scope)
    embedding_rows = read_parquet_rows(experiment_dir / "embedding_cache.parquet")
    expected_keys = [str(row.get("text_sha256")) for row in embedding_rows]
    if expected_keys != list(texts_by_sha):
        fail("source embedding cache differs from reconstructed inputs")
    expected_audits = audit_embedding_inputs(
        input_auditor,
        [texts_by_sha[key] for key in expected_keys],
    )
    if len(rows) != len(expected_keys):
        fail("audit rows do not exactly cover embedding inputs")
    for index, (key, audit) in enumerate(zip(expected_keys, expected_audits)):
        if index >= len(rows):
            break
        row = rows[index]
        expected = _audit_row(
            text_sha256=key,
            text=texts_by_sha[key],
            model_id=str(experiment_manifest.get("embedding_model_id") or ""),
            audit=audit,
        )
        for field, value in expected.items():
            actual = row.get(field)
            if isinstance(value, bool):
                actual = _stored_bool(actual)
            elif isinstance(value, int):
                try:
                    actual = int(str(actual))
                except (TypeError, ValueError):
                    actual = None
            elif value is None and actual == "":
                actual = None
            if actual != value:
                fail(f"{key}: audit field {field} differs")
    artifacts = _artifact_hashes(output_dir)
    audit_id = (
        "segmentation_embedding_audit_"
        + hashlib.sha256(
            canonical_json({name: record["sha256"] for name, record in sorted(artifacts.items())}).encode()
        ).hexdigest()[:24]
    )
    if manifest.get("audit_id") != audit_id:
        fail("audit ID differs from current artifacts")
    if manifest.get("artifacts") != artifacts:
        fail("audit artifact hashes differ from manifest")
    over_limit_count = sum(item.input_over_limit for item in expected_audits)
    truncated_count = sum(item.input_truncated for item in expected_audits)
    for field, expected in (
        ("input_count", len(expected_keys)),
        ("over_limit_input_count", over_limit_count),
        ("truncated_input_count", truncated_count),
    ):
        try:
            actual = int(str(manifest.get(field)))
        except (TypeError, ValueError):
            actual = -1
        if actual != expected:
            fail(f"manifest {field} differs")
    if any(_secret_like(str(value)) for row in rows for value in row.values() if value is not None):
        fail("embedding input audit contains a secret-like value")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "audit_id": audit_id,
        "dataset_evaluation_id": experiment_receipt["dataset_evaluation_id"],
        "document_scope_id": experiment_receipt["document_scope_id"],
        "experiment_id": experiment_receipt["experiment_id"],
        "embedding_model_id": experiment_manifest.get("embedding_model_id"),
        "model_tokenizer_id": input_auditor.tokenizer_id,
        "input_count": len(rows),
        "over_limit_input_count": over_limit_count,
        "truncated_input_count": truncated_count,
        "failures": failures,
    }


def validate_segmentation_embedding_audit(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    input_auditor: EmbeddingInputAuditor,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Independently recompute tokenizer evidence and validate the audit."""
    return _validate_segmentation_embedding_audit(
        dataset_dir,
        experiment_dir,
        output_dir,
        input_auditor=input_auditor,
        scope_dir=scope_dir,
    )


def _huggingface_auditor(
    *,
    tokenizer_source: str,
    tokenizer_id: str,
    max_input_tokens: int,
    overflow_policy: Literal["reject", "truncate"],
    revision: str | None = None,
) -> HuggingFaceEmbeddingInputAuditor:
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(
        tokenizer_source,
        revision=revision,
        local_files_only=Path(tokenizer_source).exists(),
        trust_remote_code=False,
    )
    return HuggingFaceEmbeddingInputAuditor(
        tokenizer=tokenizer,
        tokenizer_id=tokenizer_id,
        max_input_tokens=max_input_tokens,
        overflow_policy=overflow_policy,
    )


def auditor_for_name(
    name: AuditorName,
    *,
    omlx_tokenizer_path: Path | None = None,
) -> EmbeddingInputAuditor:
    """Composition root: bind concrete packages to the owned audit protocol."""
    if name == "deterministic":
        return HashEmbeddingInputAuditor()
    if name == "openai":
        return TiktokenEmbeddingInputAuditor(
            encoding_name=OPENAI_EMBEDDING_ENCODING,
            max_input_tokens=OPENAI_EMBEDDING_MAX_INPUT_TOKENS,
        )
    if name == "incumbent-bge":
        return _huggingface_auditor(
            tokenizer_source=INCUMBENT_EMBEDDING_MODEL,
            tokenizer_id=(
                f"sentence-transformers:{INCUMBENT_EMBEDDING_MODEL}@{INCUMBENT_EMBEDDING_REVISION}:tokenizer"
            ),
            revision=INCUMBENT_EMBEDDING_REVISION,
            max_input_tokens=INCUMBENT_BGE_MAX_INPUT_TOKENS,
            overflow_policy="truncate",
        )
    tokenizer_path = omlx_tokenizer_path or (Path.home() / ".omlx" / "models" / DEFAULT_OMLX_EMBEDDING_SERVICE_MODEL)
    return _huggingface_auditor(
        tokenizer_source=str(tokenizer_path),
        tokenizer_id=(f"omlx:{DEFAULT_OMLX_EMBEDDING_MODEL}@{DEFAULT_OMLX_EMBEDDING_REVISION}:tokenizer"),
        max_input_tokens=DEFAULT_OMLX_MAX_INPUT_TOKENS,
        overflow_policy="reject",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "validate"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("dataset_dir", type=Path)
        subparser.add_argument("experiment_dir", type=Path)
        subparser.add_argument("output_dir", type=Path)
        subparser.add_argument(
            "--auditor",
            required=True,
            choices=(
                "deterministic",
                "incumbent-bge",
                "openai",
                "omlx",
            ),
        )
        subparser.add_argument("--scope-dir", type=Path)
        subparser.add_argument("--omlx-tokenizer-path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    name = cast(AuditorName, args.auditor)
    auditor = auditor_for_name(
        name,
        omlx_tokenizer_path=args.omlx_tokenizer_path,
    )
    if args.command == "build":
        result = build_segmentation_embedding_audit(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            input_auditor=auditor,
            scope_dir=args.scope_dir,
        )
    else:
        result = validate_segmentation_embedding_audit(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            input_auditor=auditor,
            scope_dir=args.scope_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

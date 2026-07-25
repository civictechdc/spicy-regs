"""Reproduce whole-artifact retrieval beside segment-level experiments."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np

from spicy_regs.corpora.document_acceptance_scope import (
    DocumentAcceptanceScope,
    load_document_acceptance_scope,
)
from spicy_regs.corpora.segmentation_experiment import (
    IR_MEASURES_PROVIDER,
    RETRIEVAL_CANDIDATE_LIMIT,
    RETRIEVAL_PRECISION_CUTOFFS,
    RETRIEVAL_RECALL_CUTOFFS,
    EmbeddingProvider,
    HashEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
    _artifact_hashes,
    _ir_metrics,
    _secret_like,
    _text_sha,
)
from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.segmentation import TiktokenCounter
from spicy_regs.ontology.subjects import Artifact, build_artifacts

FORMAT_VERSION = 2
BASELINE_VERSION = "whole-artifact-retrieval-v2"
EMBEDDING_POLICY_VERSION = "whole-artifact-input-v1"
LEGACY_MODE = "incumbent-three-table-whole-row-v1"
ALL_PROFILE_MODE = "all-profile-whole-artifact-v1"
MODES = (LEGACY_MODE, ALL_PROFILE_MODE)
LegacyTable = Literal["dockets", "documents", "comments"]
LEGACY_FIELDS: dict[LegacyTable, tuple[str, ...]] = {
    "dockets": ("title", "abstract"),
    "documents": ("title",),
    "comments": ("title", "comment"),
}

ARTIFACT_EMBEDDING_COLUMNS = (
    "mode",
    "vector_id",
    "profile_id",
    "source_table",
    "subject_type",
    "subject_id",
    "artifact_digest",
    "source_fields_json",
    "embedding_input_sha256",
    "embedding_input_characters",
    "model_token_count",
    "model_tokenizer_id",
    "model_max_input_tokens",
    "model_input_truncated",
    "embedding_policy_version",
    "model_id",
    "model_revision",
    "dimensions",
    "normalization_policy",
    "vector_json",
)
QUERY_EMBEDDING_COLUMNS = (
    "query_id",
    "query_text",
    "query_text_sha256",
    "query_profile_id",
    "query_subject_type",
    "query_subject_id",
    "query_artifact_digest",
    "model_id",
    "dimensions",
    "normalization_policy",
    "vector_json",
)
CANDIDATE_COLUMNS = (
    "mode",
    "scope",
    "query_id",
    "query_text",
    "query_text_sha256",
    "query_profile_id",
    "query_subject_type",
    "query_subject_id",
    "query_artifact_digest",
    "candidate_rank",
    "candidate_limit",
    "candidate_set_size",
    "vector_id",
    "candidate_profile_id",
    "candidate_source_table",
    "candidate_subject_type",
    "candidate_subject_id",
    "candidate_artifact_digest",
    "dense_score",
    "relevant",
    "embedding_model_id",
)
METRIC_COLUMNS = (
    "mode",
    "scope",
    "artifact_count",
    "gold_total_count",
    "query_count",
    "excluded_query_count",
    *(f"recall_at_{cutoff}" for cutoff in RETRIEVAL_RECALL_CUTOFFS),
    *(f"precision_at_{cutoff}" for cutoff in RETRIEVAL_PRECISION_CUTOFFS),
    "mrr",
    "ndcg_at_5",
    "ndcg_at_10",
    "metric_provider",
)
PROVIDER_CALL_COLUMNS = (
    "provider",
    "operation",
    "model_id",
    "call_ordinal",
    "status",
    "attempt_count",
    "retry_count",
    "input_count",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "duration_ms",
    "response_id",
    "request_id",
    "error_code",
    "status_code",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ArtifactEmbeddingInput:
    mode: str
    artifact: Artifact
    source_fields: tuple[str, ...]
    text: str

    @property
    def text_sha256(self) -> str:
        return _text_sha(self.text)

    @property
    def vector_id(self) -> str:
        digest = hashlib.sha256(
            canonical_json(
                {
                    "mode": self.mode,
                    "artifact_digest": self.artifact.digest,
                    "embedding_input_sha256": self.text_sha256,
                    "embedding_policy_version": EMBEDDING_POLICY_VERSION,
                }
            ).encode()
        ).hexdigest()
        return f"artifact_vector_{digest[:24]}"


def _field_value(artifact: Artifact, field: str) -> str:
    for key in (f"{artifact.source_table}.{field}", field):
        if key in artifact.raw_fields:
            return artifact.raw_fields[key]
    matches = [
        value
        for key, value in artifact.raw_fields.items()
        if key.rsplit(".", 1)[-1] == field
    ]
    if len(matches) > 1:
        raise ValueError(
            f"{artifact.digest}: source field {field!r} is ambiguous"
        )
    return matches[0] if matches else ""


def _legacy_input(artifact: Artifact) -> ArtifactEmbeddingInput | None:
    table = artifact.source_table
    if table not in LEGACY_FIELDS:
        return None
    fields = LEGACY_FIELDS[cast(LegacyTable, table)]
    text = " ".join(
        value.strip()
        for field in fields
        if (value := _field_value(artifact, field)).strip()
    )
    return ArtifactEmbeddingInput(
        mode=LEGACY_MODE,
        artifact=artifact,
        source_fields=fields,
        text=text,
    )


def _all_profile_input(artifact: Artifact) -> ArtifactEmbeddingInput:
    fields = tuple(artifact.raw_fields)
    text = "\n\n".join(
        f"[SOURCE_FIELD {field}]\n{artifact.raw_fields[field]}"
        for field in fields
        if artifact.raw_fields[field]
    )
    return ArtifactEmbeddingInput(
        mode=ALL_PROFILE_MODE,
        artifact=artifact,
        source_fields=fields,
        text=text,
    )


def artifact_embedding_inputs(
    artifacts: Sequence[Artifact],
) -> list[ArtifactEmbeddingInput]:
    rows: list[ArtifactEmbeddingInput] = []
    for artifact in artifacts:
        legacy = _legacy_input(artifact)
        if legacy is not None:
            rows.append(legacy)
        rows.append(_all_profile_input(artifact))
    rows.sort(
        key=lambda row: (
            row.mode,
            row.artifact.profile_id,
            row.artifact.subject_type,
            row.artifact.subject_id,
            row.artifact.digest,
        )
    )
    return rows


def _normalized(vector: Sequence[float]) -> tuple[float, ...]:
    values = np.asarray(vector, dtype=np.float64)
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("embedding vector is invalid")
    norm = float(np.linalg.norm(values))
    if norm:
        values = values / norm
    return tuple(float(value) for value in values)


def _model_token_audit(
    provider: EmbeddingProvider,
    text: str,
    counter: TiktokenCounter,
) -> tuple[int, str, int | None, bool | None]:
    count_method = getattr(provider, "model_token_count", None)
    model_count = count_method(text) if callable(count_method) else None
    tokenizer_id = getattr(provider, "tokenizer_id", None)
    if model_count is None:
        model_count = counter.count(text)
        tokenizer_id = f"{counter.name}@{counter.version}:audit-fallback"
    maximum = getattr(provider, "max_input_tokens", None)
    truncated = (
        model_count > int(maximum)
        if maximum is not None
        else None
    )
    return int(model_count), str(tokenizer_id), maximum, truncated


def _embedding_rows(
    *,
    inputs: Sequence[ArtifactEmbeddingInput],
    vectors_by_text_sha: dict[str, tuple[float, ...]],
    provider: EmbeddingProvider,
) -> list[dict[str, Any]]:
    counter = TiktokenCounter()
    rows: list[dict[str, Any]] = []
    for item in inputs:
        token_count, tokenizer_id, maximum, truncated = (
            _model_token_audit(provider, item.text, counter)
        )
        vector = vectors_by_text_sha[item.text_sha256]
        rows.append(
            {
                "mode": item.mode,
                "vector_id": item.vector_id,
                "profile_id": item.artifact.profile_id,
                "source_table": item.artifact.source_table,
                "subject_type": item.artifact.subject_type,
                "subject_id": item.artifact.subject_id,
                "artifact_digest": item.artifact.digest,
                "source_fields_json": list(item.source_fields),
                "embedding_input_sha256": item.text_sha256,
                "embedding_input_characters": len(item.text),
                "model_token_count": token_count,
                "model_tokenizer_id": tokenizer_id,
                "model_max_input_tokens": maximum,
                "model_input_truncated": truncated,
                "embedding_policy_version": EMBEDDING_POLICY_VERSION,
                "model_id": provider.model_id,
                "model_revision": getattr(provider, "revision", None),
                "dimensions": len(vector),
                "normalization_policy": "harness-l2-v1",
                "vector_json": list(vector),
            }
        )
    return rows


def _query_rows(
    *,
    gold_rows: Sequence[dict[str, Any]],
    vectors_by_text_sha: dict[str, tuple[float, ...]],
    provider: EmbeddingProvider,
) -> list[dict[str, Any]]:
    return [
        {
            "query_id": str(gold["gold_id"]),
            "query_text": str(gold["concept_label"]),
            "query_text_sha256": _text_sha(str(gold["concept_label"])),
            "query_profile_id": str(gold["profile_id"]),
            "query_subject_type": str(gold["subject_type"]),
            "query_subject_id": str(gold["subject_id"]),
            "query_artifact_digest": str(gold["artifact_digest"]),
            "model_id": provider.model_id,
            "dimensions": provider.dimensions,
            "normalization_policy": "harness-l2-v1",
            "vector_json": list(
                vectors_by_text_sha[
                    _text_sha(str(gold["concept_label"]))
                ]
            ),
        }
        for gold in sorted(gold_rows, key=lambda row: str(row["gold_id"]))
    ]


def _retrieval_rows(
    *,
    embedding_rows: Sequence[dict[str, Any]],
    query_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    gold_total = len(query_rows)
    for mode in MODES:
        mode_rows = [
            row for row in embedding_rows if str(row["mode"]) == mode
        ]
        vector_by_id = {
            str(row["vector_id"]): _stored_vector(row)
            for row in mode_rows
        }
        artifact_digests = {
            str(row["artifact_digest"]) for row in mode_rows
        }
        eligible_queries = [
            row
            for row in query_rows
            if str(row["query_artifact_digest"]) in artifact_digests
        ]
        qrels: dict[str, dict[str, int]] = {}
        runs: dict[str, dict[str, float]] = {}
        for query in eligible_queries:
            query_id = str(query["query_id"])
            query_vector = _stored_vector(query)
            scored = sorted(
                (
                    (
                        float(np.dot(vector_by_id[str(row["vector_id"])], query_vector)),
                        str(row["vector_id"]),
                        row,
                    )
                    for row in mode_rows
                ),
                key=lambda item: (-item[0], item[1]),
            )
            relevant_ids = {
                str(row["vector_id"])
                for row in mode_rows
                if str(row["artifact_digest"])
                == str(query["query_artifact_digest"])
            }
            qrels[query_id] = {
                vector_id: 1 for vector_id in sorted(relevant_ids)
            }
            runs[query_id] = {
                vector_id: float(len(scored) - index)
                for index, (_, vector_id, _) in enumerate(scored)
            }
            candidates.extend(
                {
                    "mode": mode,
                    "scope": "corpus",
                    "query_id": query_id,
                    "query_text": query["query_text"],
                    "query_text_sha256": query["query_text_sha256"],
                    "query_profile_id": query["query_profile_id"],
                    "query_subject_type": query["query_subject_type"],
                    "query_subject_id": query["query_subject_id"],
                    "query_artifact_digest": query[
                        "query_artifact_digest"
                    ],
                    "candidate_rank": rank,
                    "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
                    "candidate_set_size": len(scored),
                    "vector_id": vector_id,
                    "candidate_profile_id": row["profile_id"],
                    "candidate_source_table": row["source_table"],
                    "candidate_subject_type": row["subject_type"],
                    "candidate_subject_id": row["subject_id"],
                    "candidate_artifact_digest": row["artifact_digest"],
                    "dense_score": score,
                    "relevant": vector_id in relevant_ids,
                    "embedding_model_id": row["model_id"],
                }
                for rank, (score, vector_id, row) in enumerate(
                    scored[:RETRIEVAL_CANDIDATE_LIMIT],
                    start=1,
                )
            )
        measured = (
            _ir_metrics(qrels, runs)
            if eligible_queries
            else {
                **{
                    f"recall_at_{cutoff}": 0.0
                    for cutoff in RETRIEVAL_RECALL_CUTOFFS
                },
                **{
                    f"precision_at_{cutoff}": 0.0
                    for cutoff in RETRIEVAL_PRECISION_CUTOFFS
                },
                "mrr": 0.0,
                "ndcg_at_5": 0.0,
                "ndcg_at_10": 0.0,
            }
        )
        metrics.append(
            {
                "mode": mode,
                "scope": "corpus",
                "artifact_count": len(mode_rows),
                "gold_total_count": gold_total,
                "query_count": len(eligible_queries),
                "excluded_query_count": (
                    gold_total - len(eligible_queries)
                ),
                **measured,
                "metric_provider": IR_MEASURES_PROVIDER,
            }
        )
    candidates.sort(
        key=lambda row: (
            str(row["mode"]),
            str(row["query_id"]),
            int(str(row["candidate_rank"])),
        )
    )
    metrics.sort(key=lambda row: str(row["mode"]))
    return candidates, metrics


def _stored_vector(row: dict[str, Any]) -> tuple[float, ...]:
    value = row.get("vector_json")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise ValueError("stored embedding vector is not an array")
    vector = tuple(float(item) for item in value)
    if not vector or any(not math.isfinite(item) for item in vector):
        raise ValueError("stored embedding vector is invalid")
    return vector


def _scoped_artifacts_and_gold(
    dataset_dir: Path,
    scope: DocumentAcceptanceScope | None,
) -> tuple[list[Artifact], list[dict[str, Any]]]:
    artifacts = build_artifacts(dataset_dir)
    gold_rows = read_parquet_rows(dataset_dir / "gold_spans.parquet")
    if scope is not None:
        artifacts = [
            artifact
            for artifact in artifacts
            if artifact.digest in scope.included_artifact_digests
        ]
        gold_rows = [
            row
            for row in gold_rows
            if (
                str(row.get("gold_id")) in scope.included_gold_ids
                and str(row.get("artifact_digest"))
                in scope.included_artifact_digests
            )
        ]
    return artifacts, gold_rows


def artifact_retrieval_preflight(
    dataset_dir: Path,
    *,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    scope = (
        load_document_acceptance_scope(dataset_dir, scope_dir)
        if scope_dir is not None
        else None
    )
    artifacts, gold_rows = _scoped_artifacts_and_gold(dataset_dir, scope)
    inputs = artifact_embedding_inputs(artifacts)
    counter = TiktokenCounter()
    texts = {
        item.text_sha256: item.text for item in inputs
    }
    texts.update(
        {
            _text_sha(str(row["concept_label"])): str(row["concept_label"])
            for row in gold_rows
        }
    )
    digests_by_mode = {
        mode: {
            item.artifact.digest
            for item in inputs
            if item.mode == mode
        }
        for mode in MODES
    }
    return {
        "format_version": FORMAT_VERSION,
        "baseline_version": BASELINE_VERSION,
        "document_scope_id": scope.scope_id if scope is not None else None,
        "document_scope_policy_version": (
            scope.scope_policy_version if scope is not None else None
        ),
        "dataset_artifact_count": len(artifacts),
        "artifact_inputs_by_mode": {
            mode: sum(item.mode == mode for item in inputs)
            for mode in MODES
        },
        "gold_total_count": len(gold_rows),
        "eligible_queries_by_mode": {
            mode: sum(
                str(row["artifact_digest"]) in digests_by_mode[mode]
                for row in gold_rows
            )
            for mode in MODES
        },
        "unique_embedding_inputs": len(texts),
        "embedding_input_token_estimate": sum(
            counter.count(text) for text in texts.values()
        ),
    }


def build_artifact_retrieval_baseline(
    dataset_dir: Path,
    output_dir: Path,
    *,
    embedding_provider: EmbeddingProvider,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Build immutable whole-artifact vectors, rankings, metrics, and receipt."""
    if output_dir.exists():
        raise FileExistsError(f"Refusing to replace baseline: {output_dir}")
    dataset_receipt = json.loads(
        (dataset_dir / "segmentation-evaluation-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    if dataset_receipt.get("status") != "pass":
        raise RuntimeError("segmentation evaluation dataset did not validate")
    scope = (
        load_document_acceptance_scope(dataset_dir, scope_dir)
        if scope_dir is not None
        else None
    )
    artifacts, gold_rows = _scoped_artifacts_and_gold(dataset_dir, scope)
    inputs = artifact_embedding_inputs(artifacts)
    texts_by_sha = {item.text_sha256: item.text for item in inputs}
    for gold in gold_rows:
        text = str(gold["concept_label"])
        digest = _text_sha(text)
        prior = texts_by_sha.setdefault(digest, text)
        if prior != text:
            raise RuntimeError("embedding input digest collision")
    embedding_keys = sorted(texts_by_sha)
    result = embedding_provider.embed(
        [texts_by_sha[key] for key in embedding_keys]
    )
    if len(result.vectors) != len(embedding_keys):
        raise RuntimeError("embedding provider returned the wrong vector count")
    vectors_by_text_sha = {
        key: _normalized(vector)
        for key, vector in zip(embedding_keys, result.vectors)
    }
    if any(
        len(vector) != embedding_provider.dimensions
        for vector in vectors_by_text_sha.values()
    ):
        raise RuntimeError("embedding dimensions differ from provider contract")
    embedding_rows = _embedding_rows(
        inputs=inputs,
        vectors_by_text_sha=vectors_by_text_sha,
        provider=embedding_provider,
    )
    query_rows = _query_rows(
        gold_rows=gold_rows,
        vectors_by_text_sha=vectors_by_text_sha,
        provider=embedding_provider,
    )
    candidate_rows, metric_rows = _retrieval_rows(
        embedding_rows=embedding_rows,
        query_rows=query_rows,
    )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        write_parquet_rows(
            temporary / "artifact_embeddings.parquet",
            columns=ARTIFACT_EMBEDDING_COLUMNS,
            rows=embedding_rows,
        )
        write_parquet_rows(
            temporary / "query_embeddings.parquet",
            columns=QUERY_EMBEDDING_COLUMNS,
            rows=query_rows,
        )
        write_parquet_rows(
            temporary / "retrieval_candidates.parquet",
            columns=CANDIDATE_COLUMNS,
            rows=candidate_rows,
        )
        write_parquet_rows(
            temporary / "retrieval_metrics.parquet",
            columns=METRIC_COLUMNS,
            rows=metric_rows,
        )
        write_parquet_rows(
            temporary / "provider_calls.parquet",
            columns=PROVIDER_CALL_COLUMNS,
            rows=result.calls,
        )
        artifacts_record = _artifact_hashes(temporary)
        baseline_id = "artifact_retrieval_" + hashlib.sha256(
            canonical_json(
                {
                    name: record["sha256"]
                    for name, record in sorted(artifacts_record.items())
                }
            ).encode()
        ).hexdigest()[:24]
        manifest = {
            "format_version": FORMAT_VERSION,
            "baseline_version": BASELINE_VERSION,
            "baseline_id": baseline_id,
            "dataset_evaluation_id": dataset_receipt["evaluation_id"],
            "document_scope_id": (
                scope.scope_id if scope is not None else None
            ),
            "document_scope_policy_version": (
                scope.scope_policy_version if scope is not None else None
            ),
            "document_scope_manifest_sha256": (
                _sha256(scope_dir / "document-acceptance-manifest.json")
                if scope_dir is not None
                else None
            ),
            "modes": list(MODES),
            "embedding_policy_version": EMBEDDING_POLICY_VERSION,
            "embedding_model_id": embedding_provider.model_id,
            "embedding_model_revision": getattr(
                embedding_provider,
                "revision",
                None,
            ),
            "embedding_dimensions": embedding_provider.dimensions,
            "normalization_policy": "harness-l2-v1",
            "model_tokenizer_id": getattr(
                embedding_provider,
                "tokenizer_id",
                None,
            ),
            "model_max_input_tokens": getattr(
                embedding_provider,
                "max_input_tokens",
                None,
            ),
            "metric_provider": IR_MEASURES_PROVIDER,
            "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
            "retrieval_recall_cutoffs": list(
                RETRIEVAL_RECALL_CUTOFFS
            ),
            "retrieval_precision_cutoffs": list(
                RETRIEVAL_PRECISION_CUTOFFS
            ),
            "production_provider": embedding_provider.production_provider,
            "unique_embedding_input_count": len(embedding_keys),
            "artifacts": artifacts_record,
        }
        (
            temporary / "artifact-retrieval-manifest.json"
        ).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = _validate_artifact_retrieval_baseline(
            dataset_dir,
            temporary,
            scope_dir=scope_dir,
            scope=scope,
        )
        (
            temporary / "artifact-retrieval-receipt.json"
        ).write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError(
                "Artifact retrieval validation failed: "
                + "; ".join(receipt["failures"])
            )
        temporary.replace(output_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _rows_match(
    actual: Sequence[dict[str, Any]],
    expected: Sequence[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
    numeric_fields: set[str],
) -> bool:
    def stored(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dict, list, tuple)):
            return canonical_json(value)
        return str(value)

    actual_by_key = {
        tuple(str(row.get(field)) for field in key_fields): row
        for row in actual
    }
    expected_by_key = {
        tuple(str(row.get(field)) for field in key_fields): row
        for row in expected
    }
    if set(actual_by_key) != set(expected_by_key):
        return False
    for key, expected_row in expected_by_key.items():
        actual_row = actual_by_key[key]
        for field, expected_value in expected_row.items():
            actual_value = actual_row.get(field)
            if field in numeric_fields:
                try:
                    if not math.isclose(
                        float(str(actual_value)),
                        float(str(expected_value)),
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    ):
                        return False
                except (TypeError, ValueError):
                    return False
            elif stored(actual_value) != stored(expected_value):
                return False
    return True


def _validate_artifact_retrieval_baseline(
    dataset_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
    scope: DocumentAcceptanceScope | None = None,
) -> dict[str, Any]:
    """Validate identities, vector policy, rankings, metrics, and hashes."""
    manifest = json.loads(
        (output_dir / "artifact-retrieval-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    dataset_receipt = json.loads(
        (dataset_dir / "segmentation-evaluation-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    embedding_rows = read_parquet_rows(
        output_dir / "artifact_embeddings.parquet"
    )
    query_rows = read_parquet_rows(output_dir / "query_embeddings.parquet")
    candidate_rows = read_parquet_rows(
        output_dir / "retrieval_candidates.parquet"
    )
    metric_rows = read_parquet_rows(output_dir / "retrieval_metrics.parquet")
    call_rows = read_parquet_rows(output_dir / "provider_calls.parquet")
    failures: list[str] = []

    def fail(message: str) -> None:
        if message not in failures:
            failures.append(message)

    if scope_dir is not None:
        if scope is None:
            try:
                scope = load_document_acceptance_scope(
                    dataset_dir,
                    scope_dir,
                )
            except (OSError, RuntimeError, ValueError) as exc:
                fail(f"document acceptance scope is invalid: {exc}")
        if scope is not None:
            if manifest.get("document_scope_id") != scope.scope_id:
                fail("document scope ID differs")
            if (
                manifest.get("document_scope_policy_version")
                != scope.scope_policy_version
            ):
                fail("document scope policy version differs")
            scope_manifest = (
                scope_dir / "document-acceptance-manifest.json"
            )
            if (
                not scope_manifest.is_file()
                or manifest.get("document_scope_manifest_sha256")
                != _sha256(scope_manifest)
            ):
                fail("document scope manifest digest differs")
    elif manifest.get("document_scope_id") is not None:
        fail("document scope directory is required")
    elif any(
        manifest.get(field) is not None
        for field in (
            "document_scope_policy_version",
            "document_scope_manifest_sha256",
        )
    ):
        fail("unscoped manifest contains document scope metadata")

    for field, expected in (
        ("format_version", FORMAT_VERSION),
        ("baseline_version", BASELINE_VERSION),
        ("modes", list(MODES)),
        ("embedding_policy_version", EMBEDDING_POLICY_VERSION),
        ("normalization_policy", "harness-l2-v1"),
        ("metric_provider", IR_MEASURES_PROVIDER),
        ("candidate_limit", RETRIEVAL_CANDIDATE_LIMIT),
        ("retrieval_recall_cutoffs", list(RETRIEVAL_RECALL_CUTOFFS)),
        (
            "retrieval_precision_cutoffs",
            list(RETRIEVAL_PRECISION_CUTOFFS),
        ),
        (
            "dataset_evaluation_id",
            dataset_receipt.get("evaluation_id"),
        ),
    ):
        if manifest.get(field) != expected:
            fail(f"manifest {field} differs")
    if not isinstance(manifest.get("production_provider"), bool):
        fail("manifest production_provider is not boolean")
    artifacts, gold_rows = _scoped_artifacts_and_gold(dataset_dir, scope)
    expected_inputs = artifact_embedding_inputs(artifacts)
    expected_by_key = {
        (item.mode, item.artifact.digest): item for item in expected_inputs
    }
    actual_by_key: dict[
        tuple[str, str],
        tuple[dict[str, Any], tuple[float, ...]],
    ] = {}
    dimensions = int(str(manifest.get("embedding_dimensions") or 0))
    maximum_value = manifest.get("model_max_input_tokens")
    maximum = (
        int(str(maximum_value))
        if maximum_value not in (None, "")
        else None
    )
    for row in embedding_rows:
        key = (str(row.get("mode")), str(row.get("artifact_digest")))
        if key in actual_by_key:
            fail(f"{key}: duplicate artifact embedding")
            continue
        try:
            vector = _stored_vector(row)
        except (TypeError, ValueError, json.JSONDecodeError):
            fail(f"{key}: artifact vector is invalid")
            continue
        actual_by_key[key] = (row, vector)
        expected = expected_by_key.get(key)
        if expected is None:
            fail(f"{key}: artifact embedding is unexpected")
            continue
        expected_static = {
            "mode": expected.mode,
            "vector_id": expected.vector_id,
            "profile_id": expected.artifact.profile_id,
            "source_table": expected.artifact.source_table,
            "subject_type": expected.artifact.subject_type,
            "subject_id": expected.artifact.subject_id,
            "artifact_digest": expected.artifact.digest,
            "source_fields_json": canonical_json(
                list(expected.source_fields)
            ),
            "embedding_input_sha256": expected.text_sha256,
            "embedding_input_characters": str(len(expected.text)),
            "embedding_policy_version": EMBEDDING_POLICY_VERSION,
            "model_id": manifest.get("embedding_model_id"),
            "model_revision": manifest.get("embedding_model_revision"),
            "dimensions": str(dimensions),
            "normalization_policy": "harness-l2-v1",
            "model_max_input_tokens": (
                str(maximum) if maximum is not None else None
            ),
        }
        if any(
            row.get(field) != value
            for field, value in expected_static.items()
        ):
            fail(f"{key}: artifact embedding metadata differs")
        try:
            token_count = int(str(row.get("model_token_count")))
        except (TypeError, ValueError):
            token_count = -1
        if token_count < 0:
            fail(f"{key}: model token count is invalid")
        if maximum is not None:
            if row.get("model_tokenizer_id") != manifest.get(
                "model_tokenizer_id"
            ):
                fail(f"{key}: model tokenizer identity differs")
            declared = str(row.get("model_input_truncated")).casefold()
            if declared != str(token_count > maximum).casefold():
                fail(f"{key}: truncation declaration differs")
        elif (
            not str(row.get("model_tokenizer_id") or "")
            or row.get("model_input_truncated") is not None
        ):
            fail(f"{key}: fallback token audit differs")
        if (
            len(vector) != dimensions
            or (
                any(vector)
                and not math.isclose(
                    math.sqrt(sum(value * value for value in vector)),
                    1.0,
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                )
            )
        ):
            fail(f"{key}: artifact vector contract differs")
    if set(actual_by_key) != set(expected_by_key):
        fail("artifact embeddings do not exactly cover declared modes")

    gold_by_id = {str(row["gold_id"]): row for row in gold_rows}
    query_by_id: dict[str, dict[str, Any]] = {}
    for row in query_rows:
        query_id = str(row.get("query_id"))
        if query_id in query_by_id:
            fail(f"{query_id}: duplicate query embedding")
        query_by_id[query_id] = row
        gold = gold_by_id.get(query_id)
        if gold is None:
            fail(f"{query_id}: query embedding is unexpected")
            continue
        expected_static = {
            "query_id": query_id,
            "query_text": str(gold["concept_label"]),
            "query_text_sha256": _text_sha(str(gold["concept_label"])),
            "query_profile_id": str(gold["profile_id"]),
            "query_subject_type": str(gold["subject_type"]),
            "query_subject_id": str(gold["subject_id"]),
            "query_artifact_digest": str(gold["artifact_digest"]),
            "model_id": manifest.get("embedding_model_id"),
            "dimensions": str(dimensions),
            "normalization_policy": "harness-l2-v1",
        }
        if any(
            row.get(field) != value
            for field, value in expected_static.items()
        ):
            fail(f"{query_id}: query embedding metadata differs")
        try:
            vector = _stored_vector(row)
        except (TypeError, ValueError, json.JSONDecodeError):
            fail(f"{query_id}: query vector is invalid")
            continue
        if len(vector) != dimensions:
            fail(f"{query_id}: query vector dimensions differ")
        elif any(vector) and not math.isclose(
            math.sqrt(sum(value * value for value in vector)),
            1.0,
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            fail(f"{query_id}: query vector normalization differs")
    if set(query_by_id) != set(gold_by_id):
        fail("query embeddings do not exactly cover gold rows")

    if not failures:
        expected_candidates, expected_metrics = _retrieval_rows(
            embedding_rows=embedding_rows,
            query_rows=query_rows,
        )
        if not _rows_match(
            candidate_rows,
            expected_candidates,
            key_fields=("mode", "query_id", "candidate_rank"),
            numeric_fields={"dense_score"},
        ):
            fail("retrieval candidates differ from stored vectors")
        metric_numeric = set(METRIC_COLUMNS) - {
            "mode",
            "scope",
            "metric_provider",
        }
        if not _rows_match(
            metric_rows,
            expected_metrics,
            key_fields=("mode", "scope"),
            numeric_fields=metric_numeric,
        ):
            fail("retrieval metrics differ from candidates")
    if not call_rows:
        fail("provider call ledger is empty")
    for row in call_rows:
        if (
            row.get("status") != "completed"
            or row.get("operation") != "embedding"
            or row.get("model_id") != manifest.get("embedding_model_id")
        ):
            fail("provider call ledger has no terminal completion")
    expected_unique_inputs = len(
        {
            item.text_sha256 for item in expected_inputs
        }
        | {
            _text_sha(str(row["concept_label"]))
            for row in gold_rows
        }
    )
    if (
        int(str(manifest.get("unique_embedding_input_count") or -1))
        != expected_unique_inputs
    ):
        fail("manifest unique embedding input count differs")
    try:
        provider_input_count = sum(
            int(str(row.get("input_count") or 0)) for row in call_rows
        )
    except (TypeError, ValueError):
        provider_input_count = -1
    if provider_input_count != expected_unique_inputs:
        fail("provider call input count differs")
    if any(
        _secret_like(str(value))
        for row in [
            *embedding_rows,
            *query_rows,
            *candidate_rows,
            *metric_rows,
            *call_rows,
        ]
        for value in row.values()
        if value is not None
    ):
        fail("artifact retrieval artifacts contain a secret-like value")
    artifacts_record = _artifact_hashes(output_dir)
    baseline_id = "artifact_retrieval_" + hashlib.sha256(
        canonical_json(
            {
                name: record["sha256"]
                for name, record in sorted(artifacts_record.items())
            }
        ).encode()
    ).hexdigest()[:24]
    if manifest.get("baseline_id") != baseline_id:
        fail("baseline ID differs from current artifacts")
    if manifest.get("artifacts") != artifacts_record:
        fail("artifact hashes differ from manifest")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "baseline_id": baseline_id,
        "dataset_evaluation_id": dataset_receipt.get(
            "evaluation_id"
        ),
        "document_scope_id": (
            scope.scope_id if scope is not None else None
        ),
        "production_provider": bool(manifest.get("production_provider")),
        "embedding_model_id": manifest.get("embedding_model_id"),
        "artifact_embedding_count": len(embedding_rows),
        "artifact_counts_by_mode": {
            mode: sum(str(row.get("mode")) == mode for row in embedding_rows)
            for mode in MODES
        },
        "truncated_input_count": sum(
            str(row.get("model_input_truncated")).casefold() == "true"
            for row in embedding_rows
        ),
        "query_embedding_count": len(query_rows),
        "retrieval_candidate_count": len(candidate_rows),
        "metric_row_count": len(metric_rows),
        "provider_call_count": len(call_rows),
        "failures": failures,
    }


def validate_artifact_retrieval_baseline(
    dataset_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Validate vectors, rankings, metrics, scope, provider calls, and hashes."""
    return _validate_artifact_retrieval_baseline(
        dataset_dir,
        output_dir,
        scope_dir=scope_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("preflight")
    preflight.add_argument("dataset_dir", type=Path)
    preflight.add_argument("--scope-dir", type=Path)
    build = commands.add_parser("build")
    build.add_argument("dataset_dir", type=Path)
    build.add_argument("output_dir", type=Path)
    build.add_argument(
        "--provider",
        choices=("deterministic", "incumbent-bge"),
        default="deterministic",
    )
    build.add_argument("--device")
    build.add_argument("--batch-size", type=int, default=128)
    build.add_argument("--scope-dir", type=Path)
    validate = commands.add_parser("validate")
    validate.add_argument("dataset_dir", type=Path)
    validate.add_argument("output_dir", type=Path)
    validate.add_argument("--scope-dir", type=Path)
    args = parser.parse_args()
    if args.command == "preflight":
        result = artifact_retrieval_preflight(
            args.dataset_dir,
            scope_dir=args.scope_dir,
        )
    elif args.command == "build":
        provider: EmbeddingProvider
        if args.provider == "incumbent-bge":
            provider = SentenceTransformerEmbeddingProvider(
                batch_size=args.batch_size,
                device=args.device,
            )
        else:
            provider = HashEmbeddingProvider()
        result = build_artifact_retrieval_baseline(
            args.dataset_dir,
            args.output_dir,
            embedding_provider=provider,
            scope_dir=args.scope_dir,
        )
    else:
        result = validate_artifact_retrieval_baseline(
            args.dataset_dir,
            args.output_dir,
            scope_dir=args.scope_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

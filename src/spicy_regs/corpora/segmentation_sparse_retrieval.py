"""Compare learned-sparse and dense+sparse retrieval on fixed segments."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import tempfile
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol

import numpy as np
from scipy.sparse import csr_matrix

from spicy_regs.corpora.document_acceptance_scope import (
    DocumentAcceptanceScope,
    load_document_acceptance_scope,
)
from spicy_regs.corpora.segmentation_experiment import (
    IR_MEASURES_PROVIDER,
    RETRIEVAL_CANDIDATE_LIMIT,
    RETRIEVAL_PRECISION_CUTOFFS,
    RETRIEVAL_RECALL_CUTOFFS,
    _artifact_hashes,
    _ir_metrics,
    _secret_like,
    _text_sha,
    validate_segmentation_experiment,
)
from spicy_regs.corpora.segmentation_rerank import (
    _candidate_groups,
    _relevant_ids,
    _segment_texts,
)
from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    write_parquet_rows,
)

FORMAT_VERSION = 1
COMPARISON_VERSION = "learned-sparse-rrf-v1"
SENTENCE_TRANSFORMERS_VERSION = "5.6.1"
SCIPY_VERSION_RANGE = ">=1.15,<2"
DEFAULT_SPARSE_MODEL = "tomaarsen/splade-modernbert-base-miriad"
DEFAULT_SPARSE_REVISION = "c640ce28f7c4f4593ddba1b3855988f03a3d9cdc"
DEFAULT_SPARSE_DIMENSIONS = 50_368
DEFAULT_SPARSE_MAX_INPUT_TOKENS = 8_192
DEFAULT_BATCH_SIZE = 8
DEFAULT_CHECKPOINT_BATCH_SIZE = 32
DEFAULT_RRF_K = 60
STAGES = ("learned-sparse", "rrf-hybrid")
SCOPES = ("within-artifact", "corpus")

SPARSE_EMBEDDING_COLUMNS = (
    "input_kind",
    "text_sha256",
    "model_id",
    "model_revision",
    "dimensions",
    "active_dimension_count",
    "indices_json",
    "values_json",
    "model_tokenizer_id",
    "model_token_count",
    "model_max_input_tokens",
    "model_input_truncated",
)
PROVIDER_CALL_COLUMNS = (
    "transition_ordinal",
    "call_ordinal",
    "work_id",
    "task",
    "request_digest",
    "provider",
    "package_name",
    "package_version",
    "model_id",
    "status",
    "attempt_count",
    "retry_count",
    "input_count",
    "duration_ms",
    "error_type",
    "output_sha256",
)
CANDIDATE_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "scope",
    "stage",
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
    "segment_id",
    "segment_artifact_digest",
    "dense_rank",
    "dense_score",
    "sparse_rank",
    "sparse_score",
    "fusion_score",
    "relevant",
    "dense_embedding_model_id",
    "sparse_embedding_model_id",
    "rrf_k",
)
METRIC_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "scope",
    "stage",
    "artifact_count",
    "segment_count",
    "gold_span_count",
    "query_count",
    *(f"recall_at_{cutoff}" for cutoff in RETRIEVAL_RECALL_CUTOFFS),
    *(f"precision_at_{cutoff}" for cutoff in RETRIEVAL_PRECISION_CUTOFFS),
    "mrr",
    "ndcg_at_5",
    "ndcg_at_10",
    "metric_provider",
)


@dataclass(frozen=True)
class SparseVector:
    """Portable, validated representation of one sparse model vector."""

    dimensions: int
    indices: tuple[int, ...]
    values: tuple[float, ...]


@dataclass(frozen=True)
class SparseEncodingResult:
    """One local provider batch and its call telemetry."""

    vectors: tuple[SparseVector, ...]
    call: dict[str, Any]


class SparseEmbeddingProvider(Protocol):
    """Thin package adapter used by the resumable comparison harness."""

    production_provider: bool
    provider: str
    package_name: str
    package_version: str
    model_id: str
    revision: str | None
    dimensions: int
    tokenizer_id: str
    max_input_tokens: int | None
    batch_size: int

    def model_token_count(self, text: str) -> int | None: ...

    def encode(
        self,
        texts: Sequence[str],
        *,
        task: str,
    ) -> SparseEncodingResult: ...


def _validated_vector(vector: SparseVector, dimensions: int) -> SparseVector:
    if vector.dimensions != dimensions or dimensions <= 0:
        raise ValueError("sparse vector dimensions differ from provider contract")
    if len(vector.indices) != len(vector.values):
        raise ValueError("sparse vector index and value counts differ")
    if tuple(sorted(set(vector.indices))) != vector.indices:
        raise ValueError("sparse vector indices are not sorted and unique")
    if any(index < 0 or index >= dimensions for index in vector.indices):
        raise ValueError("sparse vector index is out of range")
    if any(not math.isfinite(value) for value in vector.values):
        raise ValueError("sparse vector contains a non-finite value")
    return vector


def _vectors_from_tensor(value: Any, *, dimensions: int) -> tuple[SparseVector, ...]:
    """Convert the package's dense or COO tensor into portable sparse rows."""
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("sparse retrieval requires the 'embed' extra") from exc
    if not isinstance(value, torch.Tensor):
        raise TypeError("SparseEncoder returned a non-tensor value")
    tensor = value.detach().cpu()
    if tensor.ndim == 1:
        tensor = tensor.unsqueeze(0)
    if tensor.ndim != 2 or int(tensor.shape[1]) != dimensions:
        raise ValueError("SparseEncoder returned unexpected dimensions")
    sparse = tensor.coalesce() if tensor.is_sparse else tensor.to_sparse().coalesce()
    coordinates = sparse.indices().numpy()
    stored_values = sparse.values().to(dtype=torch.float64).numpy()
    by_row: list[list[tuple[int, float]]] = [[] for _ in range(int(tensor.shape[0]))]
    for position in range(stored_values.shape[0]):
        by_row[int(coordinates[0, position])].append((int(coordinates[1, position]), float(stored_values[position])))
    result: list[SparseVector] = []
    for items in by_row:
        items.sort()
        result.append(
            _validated_vector(
                SparseVector(
                    dimensions=dimensions,
                    indices=tuple(index for index, _ in items),
                    values=tuple(score for _, score in items),
                ),
                dimensions,
            )
        )
    return tuple(result)


class SentenceTransformersSparseProvider:
    """Pinned Sentence Transformers ``SparseEncoder`` package adapter."""

    production_provider = True
    provider = "sentence-transformers-sparse"
    package_name = "sentence-transformers"

    def __init__(
        self,
        *,
        model: str = DEFAULT_SPARSE_MODEL,
        revision: str = DEFAULT_SPARSE_REVISION,
        dimensions: int = DEFAULT_SPARSE_DIMENSIONS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        device: str | None = None,
        encoder: Any | None = None,
    ) -> None:
        if not revision:
            raise ValueError("sparse model revision must be pinned")
        if dimensions <= 0 or batch_size <= 0:
            raise ValueError("invalid sparse provider limits")
        package_version = version("sentence-transformers")
        if package_version != SENTENCE_TRANSFORMERS_VERSION:
            raise RuntimeError(
                "sentence-transformers version differs from the pinned contract: "
                f"{package_version} != {SENTENCE_TRANSFORMERS_VERSION}"
            )
        if encoder is None:
            from sentence_transformers import SparseEncoder

            encoder = SparseEncoder(
                model,
                revision=revision,
                device=device,
                similarity_fn_name="dot",
            )
        reported = encoder.get_embedding_dimension()
        if reported is None or int(reported) != dimensions:
            raise ValueError(f"SparseEncoder dimensions differ from the declared contract: {reported} != {dimensions}")
        maximum = getattr(encoder, "max_seq_length", None)
        if (
            model == DEFAULT_SPARSE_MODEL
            and revision == DEFAULT_SPARSE_REVISION
            and int(maximum or 0) != DEFAULT_SPARSE_MAX_INPUT_TOKENS
        ):
            raise ValueError(
                "default sparse model input limit differs from the "
                f"declared contract: {maximum} != "
                f"{DEFAULT_SPARSE_MAX_INPUT_TOKENS}"
            )
        self.model = model
        self.revision: str | None = revision
        self.model_id = f"sentence-transformers-sparse:{model}@{revision}"
        self.package_version = package_version
        self.dimensions = dimensions
        self.batch_size = batch_size
        self.device = device
        self._encoder = encoder
        self.max_input_tokens = int(maximum) if maximum is not None else None
        self.tokenizer_id = f"sentence-transformers-sparse:{model}@{revision}:tokenizer"

    def model_token_count(self, text: str) -> int | None:
        tokenizer = getattr(self._encoder, "tokenizer", None)
        if tokenizer is None:
            return None
        encoded = tokenizer(
            text,
            add_special_tokens=True,
            truncation=False,
            return_attention_mask=False,
            return_token_type_ids=False,
        )
        if not isinstance(encoded, Mapping):
            return None
        input_ids = encoded.get("input_ids")
        return len(input_ids) if isinstance(input_ids, list) else None

    def encode(
        self,
        texts: Sequence[str],
        *,
        task: str,
    ) -> SparseEncodingResult:
        if task not in {"document", "query"}:
            raise ValueError(f"unsupported sparse embedding task: {task}")
        started = time.monotonic()
        method = self._encoder.encode_document if task == "document" else self._encoder.encode_query
        encoded = method(
            list(texts),
            batch_size=self.batch_size,
            show_progress_bar=False,
            convert_to_tensor=True,
            convert_to_sparse_tensor=True,
            save_to_cpu=True,
        )
        vectors = _vectors_from_tensor(encoded, dimensions=self.dimensions)
        if len(vectors) != len(texts):
            raise RuntimeError("SparseEncoder response count differs from input")
        return SparseEncodingResult(
            vectors=vectors,
            call={
                "provider": self.provider,
                "package_name": self.package_name,
                "package_version": self.package_version,
                "model_id": self.model_id,
                "status": "completed",
                "attempt_count": 1,
                "retry_count": 0,
                "input_count": len(texts),
                "duration_ms": round((time.monotonic() - started) * 1_000, 3),
                "error_type": None,
            },
        )


class DeterministicSparseProvider:
    """Fast lexical test double without production-provider capability."""

    production_provider = False
    provider = "deterministic-sparse"
    package_name = "spicy-regs"
    package_version = "test-double-v1"
    model_id = "deterministic:sparse-hash-v1"
    revision: str | None = None
    dimensions = 2_048
    tokenizer_id = "deterministic:whitespace-v1"
    max_input_tokens = None
    batch_size = 128

    def model_token_count(self, text: str) -> int:
        return len(text.split())

    def encode(
        self,
        texts: Sequence[str],
        *,
        task: str,
    ) -> SparseEncodingResult:
        if task not in {"document", "query"}:
            raise ValueError(f"unsupported sparse embedding task: {task}")
        vectors: list[SparseVector] = []
        for text in texts:
            scores: defaultdict[int, float] = defaultdict(float)
            for token in text.casefold().split():
                digest = hashlib.sha256(token.encode()).digest()
                scores[int.from_bytes(digest[:4], "big") % self.dimensions] += 1.0
            ordered = sorted(scores.items())
            vectors.append(
                SparseVector(
                    dimensions=self.dimensions,
                    indices=tuple(index for index, _ in ordered),
                    values=tuple(score for _, score in ordered),
                )
            )
        return SparseEncodingResult(
            vectors=tuple(vectors),
            call={
                "provider": self.provider,
                "package_name": self.package_name,
                "package_version": self.package_version,
                "model_id": self.model_id,
                "status": "completed",
                "attempt_count": 0,
                "retry_count": 0,
                "input_count": len(texts),
                "duration_ms": 0,
                "error_type": None,
            },
        )


def _stored_json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    parsed = json.loads(str(value or "[]"))
    if not isinstance(parsed, list):
        raise ValueError("stored value is not a JSON array")
    return parsed


def _stored_vector(row: dict[str, Any]) -> SparseVector:
    indices = tuple(int(item) for item in _stored_json_list(row.get("indices_json")))
    values = tuple(float(item) for item in _stored_json_list(row.get("values_json")))
    return _validated_vector(
        SparseVector(
            dimensions=int(str(row.get("dimensions") or 0)),
            indices=indices,
            values=values,
        ),
        int(str(row.get("dimensions") or 0)),
    )


def _scope(
    dataset_dir: Path,
    scope_dir: Path | None,
) -> DocumentAcceptanceScope | None:
    return load_document_acceptance_scope(dataset_dir, scope_dir) if scope_dir is not None else None


def _gold_rows(
    dataset_dir: Path,
    scope: DocumentAcceptanceScope | None,
) -> list[dict[str, Any]]:
    rows = read_parquet_rows(dataset_dir / "gold_spans.parquet")
    if scope is None:
        return rows
    return [
        row
        for row in rows
        if (
            str(row.get("gold_id")) in scope.included_gold_ids
            and str(row.get("artifact_digest")) in scope.included_artifact_digests
        )
    ]


def _input_maps(
    segment_rows: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    segment_text_by_id = _segment_texts(segment_rows)
    document_text_by_sha: dict[str, str] = {}
    segment_sha_by_id: dict[str, str] = {}
    for segment_id, text in sorted(segment_text_by_id.items()):
        digest = _text_sha(text)
        prior = document_text_by_sha.setdefault(digest, text)
        if prior != text:
            raise RuntimeError("sparse document input digest collision")
        segment_sha_by_id[segment_id] = digest
    query_text_by_sha: dict[str, str] = {}
    for row in gold_rows:
        text = str(row["concept_label"])
        digest = _text_sha(text)
        prior = query_text_by_sha.setdefault(digest, text)
        if prior != text:
            raise RuntimeError("sparse query input digest collision")
    return document_text_by_sha, query_text_by_sha, segment_sha_by_id


def _selected_config_rows(
    *,
    experiment_manifest: dict[str, Any],
    segment_rows: Sequence[dict[str, Any]],
    dense_rows: Sequence[dict[str, Any]],
    config_ids: Sequence[str] | None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    declared_configs = experiment_manifest.get("configs")
    if not isinstance(declared_configs, list):
        raise ValueError("experiment manifest configs are invalid")
    configs = [config for config in declared_configs if isinstance(config, dict) and config.get("config_id")]
    available = {str(config["config_id"]) for config in configs}
    selected_ids = (
        tuple(str(config_id) for config_id in config_ids)
        if config_ids is not None
        else tuple(str(config["config_id"]) for config in configs)
    )
    if not selected_ids or len(set(selected_ids)) != len(selected_ids) or not set(selected_ids) <= available:
        raise ValueError("sparse comparison config IDs must be unique declared configs")
    selected = set(selected_ids)
    selected_configs = [config for config in configs if str(config["config_id"]) in selected]
    selected_segments = [row for row in segment_rows if str(row.get("config_id")) in selected]
    selected_dense = [row for row in dense_rows if str(row.get("config_id")) in selected]
    if not selected_segments or not selected_dense:
        raise ValueError("sparse comparison selected no experiment rows")
    return selected_configs, selected_segments, selected_dense


def sparse_retrieval_preflight(
    dataset_dir: Path,
    experiment_dir: Path,
    *,
    scope_dir: Path | None = None,
    config_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    upstream = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if upstream["status"] != "pass":
        raise RuntimeError("upstream segmentation experiment did not validate")
    experiment_manifest = json.loads(
        (experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8")
    )
    scope = _scope(dataset_dir, scope_dir)
    segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    dense_rows = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    configs, segment_rows, _ = _selected_config_rows(
        experiment_manifest=experiment_manifest,
        segment_rows=segment_rows,
        dense_rows=dense_rows,
        config_ids=config_ids,
    )
    gold_rows = _gold_rows(dataset_dir, scope)
    documents, queries, _ = _input_maps(segment_rows, gold_rows)
    return {
        "format_version": FORMAT_VERSION,
        "comparison_version": COMPARISON_VERSION,
        "experiment_id": upstream["experiment_id"],
        "document_scope_id": scope.scope_id if scope is not None else None,
        "config_ids": [str(config["config_id"]) for config in configs],
        "segment_count": len(segment_rows),
        "gold_span_count": len(gold_rows),
        "unique_document_input_count": len(documents),
        "unique_query_input_count": len(queries),
        "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
        "rrf_k": DEFAULT_RRF_K,
    }


def _request_identity(
    *,
    provider: SparseEmbeddingProvider,
    task: str,
    keys: Sequence[str],
) -> tuple[str, str]:
    request_digest = hashlib.sha256(
        canonical_json(
            {
                "comparison_version": COMPARISON_VERSION,
                "provider": provider.provider,
                "package": provider.package_name,
                "package_version": provider.package_version,
                "model_id": provider.model_id,
                "task": task,
                "text_sha256": list(keys),
            }
        ).encode()
    ).hexdigest()
    work_id = (
        "sparse_embedding_"
        + hashlib.sha256(canonical_json({"task": task, "request_digest": request_digest}).encode()).hexdigest()[:24]
    )
    return request_digest, work_id


def _checkpoint_vectors(record: dict[str, Any], expected_keys: Sequence[str]) -> tuple[SparseVector, ...]:
    if record.get("status") != "completed":
        raise ValueError("sparse embedding checkpoint is not complete")
    if record.get("text_sha256") != list(expected_keys):
        raise ValueError("sparse embedding checkpoint inputs differ")
    values = record.get("vectors")
    if not isinstance(values, list) or len(values) != len(expected_keys):
        raise ValueError("sparse embedding checkpoint vector count differs")
    return tuple(
        _validated_vector(
            SparseVector(
                dimensions=int(str(item.get("dimensions") or 0)),
                indices=tuple(int(index) for index in item.get("indices", [])),
                values=tuple(float(score) for score in item.get("values", [])),
            ),
            int(str(item.get("dimensions") or 0)),
        )
        for item in values
        if isinstance(item, dict)
    )


def _encode_resumable(
    *,
    provider: SparseEmbeddingProvider,
    task: str,
    texts_by_sha: dict[str, str],
    checkpoint: BatchCheckpoint,
    checkpoint_batch_size: int,
) -> tuple[dict[str, SparseVector], list[dict[str, Any]]]:
    if checkpoint_batch_size <= 0:
        raise ValueError("checkpoint batch size must be positive")
    keys = sorted(texts_by_sha)
    vectors: dict[str, SparseVector] = {}
    for start in range(0, len(keys), checkpoint_batch_size):
        batch_keys = keys[start : start + checkpoint_batch_size]
        request_digest, work_id = _request_identity(
            provider=provider,
            task=task,
            keys=batch_keys,
        )
        record = checkpoint.get(
            "sparse-embedding",
            task,
            work_id=work_id,
        )
        if record is not None and record.get("status") == "completed":
            stored = _checkpoint_vectors(record, batch_keys)
        else:
            try:
                result = provider.encode(
                    [texts_by_sha[key] for key in batch_keys],
                    task=task,
                )
                stored = tuple(_validated_vector(vector, provider.dimensions) for vector in result.vectors)
                if len(stored) != len(batch_keys):
                    raise RuntimeError("sparse provider returned the wrong vector count")
                output_sha = hashlib.sha256(
                    canonical_json(
                        [
                            {
                                "dimensions": vector.dimensions,
                                "indices": vector.indices,
                                "values": vector.values,
                            }
                            for vector in stored
                        ]
                    ).encode()
                ).hexdigest()
                checkpoint.append(
                    {
                        "subject_type": "sparse-embedding",
                        "subject_id": task,
                        "work_id": work_id,
                        "request_digest": request_digest,
                        "text_sha256": list(batch_keys),
                        "vectors": [
                            {
                                "dimensions": vector.dimensions,
                                "indices": list(vector.indices),
                                "values": list(vector.values),
                            }
                            for vector in stored
                        ],
                        "status": "completed",
                        "model_call": {
                            **result.call,
                            "output_sha256": output_sha,
                        },
                    }
                )
            except BaseException as exc:
                checkpoint.append(
                    {
                        "subject_type": "sparse-embedding",
                        "subject_id": task,
                        "work_id": work_id,
                        "request_digest": request_digest,
                        "text_sha256": list(batch_keys),
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "model_call": {
                            "provider": provider.provider,
                            "package_name": provider.package_name,
                            "package_version": provider.package_version,
                            "model_id": provider.model_id,
                            "status": "failed",
                            "attempt_count": 1,
                            "retry_count": 0,
                            "input_count": len(batch_keys),
                            "duration_ms": 0,
                            "error_type": type(exc).__name__,
                            "output_sha256": None,
                        },
                    }
                )
                raise RuntimeError("sparse embedding failed; checkpoint is resumable") from exc
        vectors.update(dict(zip(batch_keys, stored)))
    calls: list[dict[str, Any]] = []
    for ordinal, record in enumerate(checkpoint.transitions(), start=1):
        call = record.get("model_call")
        if not isinstance(call, dict):
            continue
        calls.append(
            {
                "transition_ordinal": ordinal,
                "call_ordinal": ordinal,
                "work_id": record.get("work_id"),
                "task": record.get("subject_id"),
                "request_digest": record.get("request_digest"),
                **call,
            }
        )
    return vectors, calls


def _embedding_rows(
    *,
    provider: SparseEmbeddingProvider,
    input_kind: str,
    texts_by_sha: dict[str, str],
    vectors_by_sha: dict[str, SparseVector],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(texts_by_sha):
        vector = vectors_by_sha[key]
        token_count = provider.model_token_count(texts_by_sha[key])
        maximum = provider.max_input_tokens
        if maximum is not None and token_count is None:
            raise ValueError(f"{input_kind}:{key}: model-native token audit is unavailable")
        rows.append(
            {
                "input_kind": input_kind,
                "text_sha256": key,
                "model_id": provider.model_id,
                "model_revision": provider.revision,
                "dimensions": vector.dimensions,
                "active_dimension_count": len(vector.indices),
                "indices_json": list(vector.indices),
                "values_json": list(vector.values),
                "model_tokenizer_id": provider.tokenizer_id,
                "model_token_count": token_count,
                "model_max_input_tokens": maximum,
                "model_input_truncated": (
                    token_count > maximum if token_count is not None and maximum is not None else None
                ),
            }
        )
    return rows


def _csr(vectors: Sequence[SparseVector], *, dimensions: int) -> csr_matrix:
    data: list[float] = []
    indices: list[int] = []
    indptr = [0]
    for vector in vectors:
        checked = _validated_vector(vector, dimensions)
        indices.extend(checked.indices)
        data.extend(checked.values)
        indptr.append(len(indices))
    return csr_matrix(
        (
            np.asarray(data, dtype=np.float64),
            np.asarray(indices, dtype=np.int32),
            np.asarray(indptr, dtype=np.int64),
        ),
        shape=(len(vectors), dimensions),
        dtype=np.float64,
    )


def _ranked(
    candidate_ids: Sequence[str],
    scores: Sequence[float],
) -> list[tuple[str, float]]:
    if len(candidate_ids) != len(scores):
        raise ValueError("candidate and sparse-score counts differ")
    if any(not math.isfinite(float(score)) for score in scores):
        raise ValueError("sparse scores contain a non-finite value")
    return sorted(
        zip(candidate_ids, (float(score) for score in scores)),
        key=lambda item: (-item[1], item[0]),
    )


def _rrf_ranked(
    *,
    dense_rows: Sequence[dict[str, Any]],
    sparse_ranked: Sequence[tuple[str, float]],
    rrf_k: int,
) -> list[tuple[str, float]]:
    if rrf_k <= 0:
        raise ValueError("RRF k must be positive")
    dense_ranks = {
        str(row["segment_id"]): int(str(row["candidate_rank"])) for row in dense_rows[:RETRIEVAL_CANDIDATE_LIMIT]
    }
    sparse_ranks = {
        segment_id: rank
        for rank, (segment_id, _) in enumerate(
            sparse_ranked[:RETRIEVAL_CANDIDATE_LIMIT],
            start=1,
        )
    }
    union = sorted(set(dense_ranks) | set(sparse_ranks))
    return sorted(
        (
            (
                segment_id,
                ((1.0 / (rrf_k + dense_ranks[segment_id])) if segment_id in dense_ranks else 0.0)
                + ((1.0 / (rrf_k + sparse_ranks[segment_id])) if segment_id in sparse_ranks else 0.0),
            )
            for segment_id in union
        ),
        key=lambda item: (-item[1], item[0]),
    )


def _candidate_and_metric_rows(
    *,
    segment_rows: Sequence[dict[str, Any]],
    dense_rows: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
    configs: Sequence[dict[str, Any]],
    segment_sha_by_id: dict[str, str],
    document_vectors: dict[str, SparseVector],
    query_vectors: dict[str, SparseVector],
    dense_model_id: str,
    sparse_model_id: str,
    dimensions: int,
    rrf_k: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    segment_by_id = {str(row["segment_id"]): row for row in segment_rows}
    gold_by_id = {str(row["gold_id"]): row for row in gold_rows}
    relevant = _relevant_ids(segment_rows, gold_rows)
    dense_groups = _candidate_groups(dense_rows)
    segments_by_config: defaultdict[str, list[str]] = defaultdict(list)
    for row in segment_rows:
        segments_by_config[str(row["config_id"])].append(str(row["segment_id"]))
    output_candidates: list[dict[str, Any]] = []
    output_metrics: list[dict[str, Any]] = []
    artifact_count = len({str(row["artifact_digest"]) for row in segment_rows})
    for config in sorted(configs, key=lambda item: str(item["config_id"])):
        config_id = str(config["config_id"])
        all_ids = sorted(segments_by_config[config_id])
        matrix_cache: dict[
            tuple[str, str],
            tuple[list[str], csr_matrix],
        ] = {}
        for scope in SCOPES:
            qrels: dict[str, dict[str, dict[str, int]]] = {stage: {} for stage in STAGES}
            runs: dict[str, dict[str, dict[str, float]]] = {stage: {} for stage in STAGES}
            for query_id in sorted(gold_by_id):
                gold = gold_by_id[query_id]
                matrix_key = (
                    scope,
                    (str(gold["artifact_digest"]) if scope == "within-artifact" else ""),
                )
                cached_matrix = matrix_cache.get(matrix_key)
                if cached_matrix is None:
                    candidate_ids = (
                        [
                            segment_id
                            for segment_id in all_ids
                            if str(segment_by_id[segment_id]["artifact_digest"]) == str(gold["artifact_digest"])
                        ]
                        if scope == "within-artifact"
                        else all_ids
                    )
                    document_matrix = _csr(
                        [document_vectors[segment_sha_by_id[segment_id]] for segment_id in candidate_ids],
                        dimensions=dimensions,
                    )
                    matrix_cache[matrix_key] = (
                        candidate_ids,
                        document_matrix,
                    )
                else:
                    candidate_ids, document_matrix = cached_matrix
                query_key = _text_sha(str(gold["concept_label"]))
                query_matrix = _csr(
                    [query_vectors[query_key]],
                    dimensions=dimensions,
                )
                score_array = np.asarray((document_matrix @ query_matrix.T).toarray()).reshape(-1)
                sparse_ranked = _ranked(candidate_ids, score_array.tolist())
                dense_group = dense_groups[(config_id, scope, query_id)]
                hybrid_ranked = _rrf_ranked(
                    dense_rows=dense_group,
                    sparse_ranked=sparse_ranked,
                    rrf_k=rrf_k,
                )
                dense_by_id = {str(row["segment_id"]): row for row in dense_group}
                sparse_rank_by_id = {
                    segment_id: rank
                    for rank, (segment_id, _) in enumerate(
                        sparse_ranked,
                        start=1,
                    )
                }
                sparse_score_by_id = dict(sparse_ranked)
                relevant_ids = relevant[(config_id, query_id)]
                for stage, ranked_rows in (
                    ("learned-sparse", sparse_ranked),
                    ("rrf-hybrid", hybrid_ranked),
                ):
                    selected = ranked_rows[:RETRIEVAL_CANDIDATE_LIMIT]
                    qrels[stage][query_id] = {segment_id: 1 for segment_id in sorted(relevant_ids)}
                    if not qrels[stage][query_id]:
                        qrels[stage][query_id][f"{query_id}:missing-relevant-segment"] = 1
                    runs[stage][query_id] = {
                        segment_id: float(len(selected) - index) for index, (segment_id, _) in enumerate(selected)
                    }
                    for rank, (segment_id, final_score) in enumerate(
                        selected,
                        start=1,
                    ):
                        dense = dense_by_id.get(segment_id)
                        output_candidates.append(
                            {
                                "config_id": config_id,
                                "arm": config["arm"],
                                "max_tokens": config["max_tokens"],
                                "scope": scope,
                                "stage": stage,
                                "query_id": query_id,
                                "query_text": gold["concept_label"],
                                "query_text_sha256": query_key,
                                "query_profile_id": gold["profile_id"],
                                "query_subject_type": gold["subject_type"],
                                "query_subject_id": gold["subject_id"],
                                "query_artifact_digest": gold["artifact_digest"],
                                "candidate_rank": rank,
                                "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
                                "candidate_set_size": len(candidate_ids),
                                "segment_id": segment_id,
                                "segment_artifact_digest": segment_by_id[segment_id]["artifact_digest"],
                                "dense_rank": (dense["candidate_rank"] if dense is not None else None),
                                "dense_score": (dense["dense_score"] if dense is not None else None),
                                "sparse_rank": sparse_rank_by_id.get(segment_id),
                                "sparse_score": sparse_score_by_id.get(segment_id),
                                "fusion_score": (final_score if stage == "rrf-hybrid" else None),
                                "relevant": segment_id in relevant_ids,
                                "dense_embedding_model_id": dense_model_id,
                                "sparse_embedding_model_id": sparse_model_id,
                                "rrf_k": rrf_k,
                            }
                        )
            for stage in STAGES:
                measured = _ir_metrics(qrels[stage], runs[stage])
                output_metrics.append(
                    {
                        "config_id": config_id,
                        "arm": config["arm"],
                        "max_tokens": config["max_tokens"],
                        "scope": scope,
                        "stage": stage,
                        "artifact_count": artifact_count,
                        "segment_count": len(all_ids),
                        "gold_span_count": len(gold_rows),
                        "query_count": len(gold_rows),
                        **measured,
                        "metric_provider": IR_MEASURES_PROVIDER,
                    }
                )
    output_candidates.sort(
        key=lambda row: (
            str(row["config_id"]),
            str(row["scope"]),
            str(row["stage"]),
            str(row["query_id"]),
            int(str(row["candidate_rank"])),
        )
    )
    output_metrics.sort(
        key=lambda row: (
            str(row["config_id"]),
            str(row["scope"]),
            str(row["stage"]),
        )
    )
    return output_candidates, output_metrics


def _same_rows(
    actual: Sequence[dict[str, Any]],
    expected: Sequence[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
    numeric_fields: set[str],
) -> bool:
    actual_by_key = {tuple(str(row.get(field)) for field in key_fields): row for row in actual}
    expected_by_key = {tuple(str(row.get(field)) for field in key_fields): row for row in expected}
    if set(actual_by_key) != set(expected_by_key):
        return False
    for key, expected_row in expected_by_key.items():
        row = actual_by_key[key]
        for field, expected_value in expected_row.items():
            actual_value = row.get(field)
            if field in numeric_fields and expected_value is not None:
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
            elif isinstance(expected_value, (dict, list, tuple)):
                if str(actual_value) != canonical_json(expected_value):
                    return False
            elif (None if actual_value is None else str(actual_value)) != (
                None if expected_value is None else str(expected_value)
            ):
                return False
    return True


def build_sparse_retrieval_comparison(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    provider: SparseEmbeddingProvider,
    scope_dir: Path | None = None,
    rrf_k: int = DEFAULT_RRF_K,
    checkpoint_batch_size: int = DEFAULT_CHECKPOINT_BATCH_SIZE,
    config_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build immutable sparse vectors, exact rankings, fusion, and metrics."""
    if output_dir.exists():
        raise FileExistsError(f"Refusing to replace sparse comparison: {output_dir}")
    upstream = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if upstream["status"] != "pass":
        raise RuntimeError("upstream segmentation experiment did not validate")
    experiment_manifest = json.loads(
        (experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8")
    )
    scope = _scope(dataset_dir, scope_dir)
    segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    dense_rows = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    configs, segment_rows, dense_rows = _selected_config_rows(
        experiment_manifest=experiment_manifest,
        segment_rows=segment_rows,
        dense_rows=dense_rows,
        config_ids=config_ids,
    )
    selected_config_ids = [str(config["config_id"]) for config in configs]
    gold_rows = _gold_rows(dataset_dir, scope)
    document_texts, query_texts, segment_sha_by_id = _input_maps(
        segment_rows,
        gold_rows,
    )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    work_dir = output_dir.parent / f".{output_dir.name}.sparse-work"
    work_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = BatchCheckpoint(
        work_dir,
        run_id="sparse-"
        + hashlib.sha256(
            canonical_json(
                {
                    "comparison_version": COMPARISON_VERSION,
                    "experiment_id": upstream["experiment_id"],
                    "document_scope_id": (scope.scope_id if scope is not None else None),
                    "model_id": provider.model_id,
                    "config_ids": selected_config_ids,
                    "rrf_k": rrf_k,
                }
            ).encode()
        ).hexdigest()[:24],
        phase="embedding",
    )
    document_vectors, _ = _encode_resumable(
        provider=provider,
        task="document",
        texts_by_sha=document_texts,
        checkpoint=checkpoint,
        checkpoint_batch_size=checkpoint_batch_size,
    )
    query_vectors, provider_calls = _encode_resumable(
        provider=provider,
        task="query",
        texts_by_sha=query_texts,
        checkpoint=checkpoint,
        checkpoint_batch_size=checkpoint_batch_size,
    )
    embedding_rows = [
        *_embedding_rows(
            provider=provider,
            input_kind="document",
            texts_by_sha=document_texts,
            vectors_by_sha=document_vectors,
        ),
        *_embedding_rows(
            provider=provider,
            input_kind="query",
            texts_by_sha=query_texts,
            vectors_by_sha=query_vectors,
        ),
    ]
    candidates, metrics = _candidate_and_metric_rows(
        segment_rows=segment_rows,
        dense_rows=dense_rows,
        gold_rows=gold_rows,
        configs=configs,
        segment_sha_by_id=segment_sha_by_id,
        document_vectors=document_vectors,
        query_vectors=query_vectors,
        dense_model_id=str(experiment_manifest["embedding_model_id"]),
        sparse_model_id=provider.model_id,
        dimensions=provider.dimensions,
        rrf_k=rrf_k,
    )
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        write_parquet_rows(
            temporary / "sparse_embeddings.parquet",
            columns=SPARSE_EMBEDDING_COLUMNS,
            rows=embedding_rows,
        )
        write_parquet_rows(
            temporary / "provider_calls.parquet",
            columns=PROVIDER_CALL_COLUMNS,
            rows=provider_calls,
        )
        write_parquet_rows(
            temporary / "retrieval_candidates.parquet",
            columns=CANDIDATE_COLUMNS,
            rows=candidates,
        )
        write_parquet_rows(
            temporary / "retrieval_metrics.parquet",
            columns=METRIC_COLUMNS,
            rows=metrics,
        )
        artifacts = _artifact_hashes(temporary)
        comparison_id = (
            "sparse_retrieval_"
            + hashlib.sha256(
                canonical_json({name: value["sha256"] for name, value in sorted(artifacts.items())}).encode()
            ).hexdigest()[:24]
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "comparison_version": COMPARISON_VERSION,
            "comparison_id": comparison_id,
            "dataset_evaluation_id": upstream["dataset_evaluation_id"],
            "experiment_id": upstream["experiment_id"],
            "document_scope_id": (scope.scope_id if scope is not None else None),
            "document_scope_policy_version": (scope.scope_policy_version if scope is not None else None),
            "document_scope_manifest_sha256": (
                hashlib.sha256((scope_dir / "document-acceptance-manifest.json").read_bytes()).hexdigest()
                if scope_dir is not None
                else None
            ),
            "config_ids": selected_config_ids,
            "config_count": len(selected_config_ids),
            "stages": list(STAGES),
            "scopes": list(SCOPES),
            "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
            "retrieval_recall_cutoffs": list(RETRIEVAL_RECALL_CUTOFFS),
            "retrieval_precision_cutoffs": list(RETRIEVAL_PRECISION_CUTOFFS),
            "rrf_k": rrf_k,
            "rrf_fusion_input_depth": RETRIEVAL_CANDIDATE_LIMIT,
            "metric_provider": IR_MEASURES_PROVIDER,
            "sparse_provider": provider.provider,
            "sparse_package_name": provider.package_name,
            "sparse_package_version": provider.package_version,
            "sparse_model_id": provider.model_id,
            "sparse_model_revision": provider.revision,
            "sparse_dimensions": provider.dimensions,
            "sparse_tokenizer_id": provider.tokenizer_id,
            "sparse_model_max_input_tokens": provider.max_input_tokens,
            "dense_model_id": experiment_manifest["embedding_model_id"],
            "production_provider": provider.production_provider,
            "unique_document_input_count": len(document_texts),
            "unique_query_input_count": len(query_texts),
            "artifacts": artifacts,
        }
        (temporary / "segmentation-sparse-retrieval-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = _validate_sparse_retrieval_comparison(
            dataset_dir,
            experiment_dir,
            temporary,
            scope_dir=scope_dir,
            scope=scope,
        )
        (temporary / "segmentation-sparse-retrieval-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError("Sparse retrieval validation failed: " + "; ".join(receipt["failures"]))
        temporary.replace(output_dir)
        shutil.rmtree(work_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _validate_sparse_retrieval_comparison(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
    scope: DocumentAcceptanceScope | None = None,
) -> dict[str, Any]:
    failures: list[str] = []

    def fail(message: str) -> None:
        if message not in failures:
            failures.append(message)

    try:
        manifest = json.loads((output_dir / "segmentation-sparse-retrieval-manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("sparse retrieval manifest is unreadable") from exc
    upstream = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    experiment_manifest = json.loads(
        (experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8")
    )
    if scope_dir is not None and scope is None:
        try:
            scope = _scope(dataset_dir, scope_dir)
        except (OSError, RuntimeError, ValueError) as exc:
            fail(f"document acceptance scope is invalid: {exc}")
    for field, expected in (
        ("format_version", FORMAT_VERSION),
        ("comparison_version", COMPARISON_VERSION),
        ("experiment_id", upstream["experiment_id"]),
        ("stages", list(STAGES)),
        ("scopes", list(SCOPES)),
        ("candidate_limit", RETRIEVAL_CANDIDATE_LIMIT),
        ("retrieval_recall_cutoffs", list(RETRIEVAL_RECALL_CUTOFFS)),
        ("retrieval_precision_cutoffs", list(RETRIEVAL_PRECISION_CUTOFFS)),
        ("metric_provider", IR_MEASURES_PROVIDER),
        ("dense_model_id", experiment_manifest["embedding_model_id"]),
    ):
        if manifest.get(field) != expected:
            fail(f"manifest {field} differs")
    if scope is not None:
        if manifest.get("document_scope_id") != scope.scope_id:
            fail("document scope ID differs")
        if manifest.get("document_scope_policy_version") != scope.scope_policy_version:
            fail("document scope policy version differs")
        if (
            scope_dir is None
            or manifest.get("document_scope_manifest_sha256")
            != hashlib.sha256((scope_dir / "document-acceptance-manifest.json").read_bytes()).hexdigest()
        ):
            fail("document scope manifest digest differs")
    elif manifest.get("document_scope_id") is not None:
        fail("document scope directory is required")
    embedding_rows = read_parquet_rows(output_dir / "sparse_embeddings.parquet")
    call_rows = read_parquet_rows(output_dir / "provider_calls.parquet")
    candidate_rows = read_parquet_rows(output_dir / "retrieval_candidates.parquet")
    metric_rows = read_parquet_rows(output_dir / "retrieval_metrics.parquet")
    segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    dense_rows = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    declared_config_ids = manifest.get("config_ids")
    if not isinstance(declared_config_ids, list):
        fail("manifest config IDs are invalid")
        requested_config_ids: Sequence[str] | None = None
    else:
        requested_config_ids = [str(config_id) for config_id in declared_config_ids]
    try:
        configs, segment_rows, dense_rows = _selected_config_rows(
            experiment_manifest=experiment_manifest,
            segment_rows=segment_rows,
            dense_rows=dense_rows,
            config_ids=requested_config_ids,
        )
    except ValueError as exc:
        fail(str(exc))
        configs, segment_rows, dense_rows = _selected_config_rows(
            experiment_manifest=experiment_manifest,
            segment_rows=segment_rows,
            dense_rows=dense_rows,
            config_ids=None,
        )
    expected_config_ids = [str(config["config_id"]) for config in configs]
    if manifest.get("config_ids") != expected_config_ids:
        fail("manifest config IDs differ")
    if manifest.get("config_count") != len(expected_config_ids):
        fail("manifest config count differs")
    gold_rows = _gold_rows(dataset_dir, scope)
    document_texts, query_texts, segment_sha_by_id = _input_maps(
        segment_rows,
        gold_rows,
    )
    vectors_by_kind: dict[str, dict[str, SparseVector]] = {
        "document": {},
        "query": {},
    }
    dimensions = int(str(manifest.get("sparse_dimensions") or 0))
    maximum_value = manifest.get("sparse_model_max_input_tokens")
    maximum = int(str(maximum_value)) if maximum_value not in (None, "") else None
    for row in embedding_rows:
        kind = str(row.get("input_kind"))
        key = str(row.get("text_sha256"))
        if kind not in vectors_by_kind:
            fail(f"{key}: sparse input kind is invalid")
            continue
        if key in vectors_by_kind[kind]:
            fail(f"{kind}:{key}: duplicate sparse embedding")
            continue
        try:
            vector = _stored_vector(row)
        except (TypeError, ValueError, json.JSONDecodeError):
            fail(f"{kind}:{key}: sparse vector is invalid")
            continue
        vectors_by_kind[kind][key] = vector
        if (
            vector.dimensions != dimensions
            or str(row.get("model_id")) != str(manifest.get("sparse_model_id"))
            or str(row.get("model_tokenizer_id")) != str(manifest.get("sparse_tokenizer_id"))
            or int(str(row.get("active_dimension_count") or -1)) != len(vector.indices)
        ):
            fail(f"{kind}:{key}: sparse embedding metadata differs")
        count_value = row.get("model_token_count")
        count = int(str(count_value)) if count_value not in (None, "") else None
        declared = row.get("model_input_truncated")
        expected_truncated = count > maximum if count is not None and maximum is not None else None
        if (None if declared is None else str(declared).casefold() == "true") != expected_truncated:
            fail(f"{kind}:{key}: sparse truncation declaration differs")
    if set(vectors_by_kind["document"]) != set(document_texts):
        fail("sparse document embeddings do not cover exact inputs")
    if set(vectors_by_kind["query"]) != set(query_texts):
        fail("sparse query embeddings do not cover exact inputs")
    if any(row.get("status") not in {"completed", "failed"} for row in call_rows):
        fail("provider call status is invalid")
    if not any(row.get("status") == "completed" for row in call_rows):
        fail("provider call ledger has no completed transition")
    if not failures:
        expected_candidates, expected_metrics = _candidate_and_metric_rows(
            segment_rows=segment_rows,
            dense_rows=dense_rows,
            gold_rows=gold_rows,
            configs=configs,
            segment_sha_by_id=segment_sha_by_id,
            document_vectors=vectors_by_kind["document"],
            query_vectors=vectors_by_kind["query"],
            dense_model_id=str(manifest["dense_model_id"]),
            sparse_model_id=str(manifest["sparse_model_id"]),
            dimensions=dimensions,
            rrf_k=int(str(manifest["rrf_k"])),
        )
        if not _same_rows(
            candidate_rows,
            expected_candidates,
            key_fields=(
                "config_id",
                "scope",
                "stage",
                "query_id",
                "candidate_rank",
            ),
            numeric_fields={
                "dense_score",
                "sparse_score",
                "fusion_score",
            },
        ):
            fail("sparse retrieval candidates differ from stored vectors")
        if not _same_rows(
            metric_rows,
            expected_metrics,
            key_fields=("config_id", "scope", "stage"),
            numeric_fields=set(METRIC_COLUMNS) - {"config_id", "arm", "scope", "stage", "metric_provider"},
        ):
            fail("sparse retrieval metrics differ from candidate rankings")
    if any(
        _secret_like(str(value))
        for row in [*embedding_rows, *call_rows, *candidate_rows, *metric_rows]
        for value in row.values()
        if value is not None
    ):
        fail("sparse retrieval artifacts contain a secret-like value")
    artifacts = _artifact_hashes(output_dir)
    comparison_id = (
        "sparse_retrieval_"
        + hashlib.sha256(
            canonical_json({name: value["sha256"] for name, value in sorted(artifacts.items())}).encode()
        ).hexdigest()[:24]
    )
    if manifest.get("comparison_id") != comparison_id:
        fail("sparse comparison ID differs")
    if manifest.get("artifacts") != artifacts:
        fail("sparse artifact hashes differ from manifest")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "comparison_id": comparison_id,
        "experiment_id": upstream["experiment_id"],
        "document_scope_id": (scope.scope_id if scope is not None else None),
        "config_ids": expected_config_ids,
        "production_provider": bool(manifest.get("production_provider")),
        "sparse_model_id": manifest.get("sparse_model_id"),
        "embedding_row_count": len(embedding_rows),
        "truncated_input_count": sum(
            str(row.get("model_input_truncated")).casefold() == "true" for row in embedding_rows
        ),
        "provider_call_count": len(call_rows),
        "provider_failed_transition_count": sum(row.get("status") == "failed" for row in call_rows),
        "candidate_count": len(candidate_rows),
        "metric_row_count": len(metric_rows),
        "failures": failures,
    }


def validate_sparse_retrieval_comparison(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Validate scope, sparse vectors, exact rankings, fusion, and hashes."""
    return _validate_sparse_retrieval_comparison(
        dataset_dir,
        experiment_dir,
        output_dir,
        scope_dir=scope_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("preflight")
    preflight.add_argument("dataset_dir", type=Path)
    preflight.add_argument("experiment_dir", type=Path)
    preflight.add_argument("--scope-dir", type=Path)
    preflight.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Limit comparison to a declared experiment config; repeatable",
    )
    build = commands.add_parser("build")
    build.add_argument("dataset_dir", type=Path)
    build.add_argument("experiment_dir", type=Path)
    build.add_argument("output_dir", type=Path)
    build.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Limit comparison to a declared experiment config; repeatable",
    )
    build.add_argument(
        "--provider",
        choices=("deterministic", "sentence-transformers"),
        default="deterministic",
    )
    build.add_argument("--model", default=DEFAULT_SPARSE_MODEL)
    build.add_argument("--revision", default=DEFAULT_SPARSE_REVISION)
    build.add_argument("--dimensions", type=int, default=DEFAULT_SPARSE_DIMENSIONS)
    build.add_argument("--device")
    build.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    build.add_argument(
        "--checkpoint-batch-size",
        type=int,
        default=DEFAULT_CHECKPOINT_BATCH_SIZE,
    )
    build.add_argument("--rrf-k", type=int, default=DEFAULT_RRF_K)
    build.add_argument("--scope-dir", type=Path)
    validate = commands.add_parser("validate")
    validate.add_argument("dataset_dir", type=Path)
    validate.add_argument("experiment_dir", type=Path)
    validate.add_argument("output_dir", type=Path)
    validate.add_argument("--scope-dir", type=Path)
    args = parser.parse_args()
    if args.command == "preflight":
        result = sparse_retrieval_preflight(
            args.dataset_dir,
            args.experiment_dir,
            scope_dir=args.scope_dir,
            config_ids=args.config_ids,
        )
    elif args.command == "build":
        provider: SparseEmbeddingProvider
        if args.provider == "sentence-transformers":
            provider = SentenceTransformersSparseProvider(
                model=args.model,
                revision=args.revision,
                dimensions=args.dimensions,
                batch_size=args.batch_size,
                device=args.device,
            )
        else:
            provider = DeterministicSparseProvider()
        result = build_sparse_retrieval_comparison(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            provider=provider,
            scope_dir=args.scope_dir,
            rrf_k=args.rrf_k,
            checkpoint_batch_size=args.checkpoint_batch_size,
            config_ids=args.config_ids,
        )
    else:
        result = validate_sparse_retrieval_comparison(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            scope_dir=args.scope_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

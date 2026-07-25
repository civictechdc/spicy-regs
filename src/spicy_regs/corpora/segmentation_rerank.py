"""Rerank exact dense candidates with packaged local inference providers."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv

from spicy_regs.corpora.segmentation_experiment import (
    IR_MEASURES_PROVIDER,
    RETRIEVAL_CANDIDATE_COLUMNS,
    RETRIEVAL_CANDIDATE_LIMIT,
    RETRIEVAL_PRECISION_CUTOFFS,
    RETRIEVAL_RECALL_CUTOFFS,
    _artifact_hashes,
    _ir_metrics,
    _secret_like,
    _slices_overlap_gold,
    _text_sha,
    validate_segmentation_experiment,
)
from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.segmentation import TiktokenCounter

FORMAT_VERSION = 2
RERANK_EXPERIMENT_VERSION = "candidate-rerank-v2"
RERANK_DEPTHS = (25, 50, 100, RETRIEVAL_CANDIDATE_LIMIT)
SENTENCE_TRANSFORMERS_VERSION = "5.6.1"
DEFAULT_CROSS_ENCODER_MODEL = "BAAI/bge-reranker-v2-m3"
DEFAULT_CROSS_ENCODER_REVISION = "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"
DEFAULT_CROSS_ENCODER_MAX_SEQ_LENGTH = 4_096
DEFAULT_OMLX_VERSION = "0.5.3"
DEFAULT_OMLX_MODEL = "mlx-community/Qwen3-Reranker-4B-mxfp8"
DEFAULT_OMLX_MODEL_REVISION = "25f203a237b822a90f38763843562b93a5baf82f"
DEFAULT_OMLX_SERVICE_MODEL = "mlx-community--Qwen3-Reranker-4B-mxfp8"
DEFAULT_OMLX_BASE_URL = "http://127.0.0.1:8012/v1"
DEFAULT_OMLX_CAUSAL_RERANK_MAX_SEQ_LENGTH = 8_192
OMLX_CAUSAL_RERANK_SYSTEM_PROMPT = (
    "Judge whether the Document meets the requirements based on the "
    "Query and the Instruct provided. Note that the answer can only be "
    '"yes" or "no".'
)
OMLX_CAUSAL_RERANK_DEFAULT_INSTRUCTION = "Given a web search query, retrieve relevant passages that answer the query"
OMLX_TOKEN_AUDIT_POLICY_VERSION = "omlx-0.5.3-causal-rerank-token-audit-v1"

RERANKED_CANDIDATE_COLUMNS = (
    *RETRIEVAL_CANDIDATE_COLUMNS,
    "rerank_score",
    "rerank_rank",
    "reranker_provider",
    "reranker_package",
    "reranker_package_version",
    "reranker_model_id",
    "rerank_request_digest",
    "rerank_work_id",
    "rerank_tokenizer_id",
    "rerank_untruncated_token_count",
    "rerank_input_limit",
    "rerank_would_truncate",
    "rerank_token_audit_status",
)
RERANK_REQUEST_COLUMNS = (
    "transition_ordinal",
    "call_ordinal",
    "work_id",
    "config_id",
    "scope",
    "query_id",
    "experiment_id",
    "request_digest",
    "candidate_ids_sha256",
    "candidate_count",
    "provider",
    "package_name",
    "package_version",
    "model_id",
    "request_parameters_json",
    "runtime_parameters_json",
    "status",
    "attempt_count",
    "retry_count",
    "duration_ms",
    "total_tokens",
    "response_id",
    "status_code",
    "error_type",
    "scores_sha256",
)
RERANK_METRIC_COLUMNS = (
    "config_id",
    "arm",
    "max_tokens",
    "scope",
    "stage",
    "rerank_depth",
    "query_count",
    *(f"recall_at_{cutoff}" for cutoff in RETRIEVAL_RECALL_CUTOFFS),
    *(f"precision_at_{cutoff}" for cutoff in RETRIEVAL_PRECISION_CUTOFFS),
    "mrr",
    "ndcg_at_5",
    "ndcg_at_10",
    "metric_provider",
)


def _normalized_rerank_depths(
    depths: Sequence[int],
) -> tuple[int, ...]:
    normalized = tuple(int(depth) for depth in depths)
    if (
        not normalized
        or tuple(sorted(set(normalized))) != normalized
        or normalized[0] <= 0
        or normalized[-1] > RETRIEVAL_CANDIDATE_LIMIT
    ):
        raise ValueError(f"rerank depths must be unique, increasing, and within 1..{RETRIEVAL_CANDIDATE_LIMIT}")
    return normalized


@dataclass(frozen=True)
class RerankResult:
    """Scores aligned to the provider input document order."""

    scores: tuple[float, ...]
    telemetry: dict[str, Any]


@dataclass(frozen=True)
class RerankInputAudit:
    """Model-native token evidence aligned to provider input order."""

    tokenizer_id: str | None
    untruncated_token_counts: tuple[int | None, ...]
    input_limit: int | None
    status: str


class Reranker(Protocol):
    provider: str
    package_name: str
    package_version: str
    model_id: str
    production_provider: bool
    request_parameters: dict[str, Any]
    runtime_parameters: dict[str, Any]

    def rerank(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankResult: ...

    def audit_inputs(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankInputAudit: ...


def _ranked_scores(
    ranked: object,
    *,
    expected_count: int,
    index_field: str,
    score_field: str,
) -> tuple[float, ...]:
    if not isinstance(ranked, list):
        raise ValueError("reranker results must be a list")
    scores: list[float | None] = [None] * expected_count
    for item in ranked:
        if not isinstance(item, dict):
            raise ValueError("reranker result must be an object")
        typed_item = cast(dict[str, Any], item)
        try:
            index = int(str(typed_item[index_field]))
            score = float(str(typed_item[score_field]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("reranker result has invalid index or score") from exc
        if not 0 <= index < expected_count or scores[index] is not None:
            raise ValueError("reranker result indices are incomplete or duplicated")
        if not math.isfinite(score):
            raise ValueError("reranker returned a non-finite score")
        scores[index] = score
    if any(score is None for score in scores):
        raise ValueError("reranker did not score every candidate")
    return tuple(float(score) for score in scores if score is not None)


class DeterministicReranker:
    """Hermetic test double; release receipts identify it as non-production."""

    provider = "deterministic"
    package_name = "spicy-regs-test-double"
    package_version = "1"
    model_id = "deterministic:lexical-reranker-v1"
    production_provider = False
    request_parameters = {"scoring_policy": "query-term-set-overlap-v1"}
    runtime_parameters: dict[str, Any] = {}

    def audit_inputs(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankInputAudit:
        del query
        return RerankInputAudit(
            tokenizer_id=None,
            untruncated_token_counts=tuple(None for _ in documents),
            input_limit=None,
            status="not-applicable-test-double",
        )

    def rerank(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankResult:
        query_terms = set(query.casefold().split())
        scores = []
        for document in documents:
            document_terms = set(document.casefold().split())
            denominator = max(1, len(query_terms))
            scores.append(len(query_terms & document_terms) / denominator)
        return RerankResult(
            scores=tuple(scores),
            telemetry={
                "duration_ms": 0,
                "retry_count": 0,
                "total_tokens": 0,
                "response_id": None,
                "status_code": None,
            },
        )


class SentenceTransformersReranker:
    """Thin adapter over ``sentence_transformers.CrossEncoder.rank``."""

    provider = "sentence-transformers"
    package_name = "sentence-transformers"
    package_version = SENTENCE_TRANSFORMERS_VERSION
    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_CROSS_ENCODER_MODEL,
        revision: str = DEFAULT_CROSS_ENCODER_REVISION,
        device: str | None = None,
        batch_size: int = 16,
        max_seq_length: int | None = None,
        encoder: Any | None = None,
        cache_clearer: Callable[[], None] | None = None,
    ) -> None:
        if not revision:
            raise ValueError("cross-encoder revision must be pinned")
        if batch_size <= 0:
            raise ValueError("cross-encoder batch size must be positive")
        if max_seq_length is not None and max_seq_length <= 0:
            raise ValueError("cross-encoder input limit must be positive")
        import sentence_transformers
        from sentence_transformers import CrossEncoder

        if sentence_transformers.__version__ != SENTENCE_TRANSFORMERS_VERSION:
            raise RuntimeError(
                "sentence-transformers version differs from the pinned "
                f"contract: {sentence_transformers.__version__} != "
                f"{SENTENCE_TRANSFORMERS_VERSION}"
            )
        self.model = model
        self.revision = revision
        self.model_id = f"sentence-transformers:{model}@{revision}"
        self.device = device
        self.batch_size = batch_size
        owns_encoder = encoder is None
        if encoder is None:
            encoder = CrossEncoder(
                model,
                revision=revision,
                device=device,
                trust_remote_code=False,
                max_length=max_seq_length,
            )
        elif max_seq_length is not None:
            encoder.max_seq_length = max_seq_length
        self.encoder = encoder
        reported_max_seq_length = self.encoder.max_seq_length
        if reported_max_seq_length is None:
            raise ValueError("cross-encoder did not report an input limit")
        self.max_seq_length = int(reported_max_seq_length)
        tokenizer = getattr(self.encoder, "tokenizer", None)
        if tokenizer is None:
            try:
                tokenizer = getattr(self.encoder[0], "tokenizer", None)
            except (IndexError, KeyError, TypeError):
                tokenizer = None
        if tokenizer is None or not callable(tokenizer):
            raise ValueError("cross-encoder did not expose its pair tokenizer")
        self.tokenizer = tokenizer
        self.tokenizer_id = f"huggingface:{model}@{revision}"
        if cache_clearer is not None:
            self._cache_clearer = cache_clearer
        elif owns_encoder and device == "mps":
            import torch

            self._cache_clearer = torch.mps.empty_cache
        else:
            self._cache_clearer = None
        self.request_parameters = {
            "max_seq_length": self.max_seq_length,
        }
        self.runtime_parameters = {
            "batch_size": batch_size,
            "device": device or str(getattr(self.encoder, "device", "auto")),
            "trust_remote_code": False,
            "clear_device_cache_after_request": (self._cache_clearer is not None),
        }

    def audit_inputs(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankInputAudit:
        if not documents:
            return RerankInputAudit(
                tokenizer_id=self.tokenizer_id,
                untruncated_token_counts=(),
                input_limit=self.max_seq_length,
                status="exact-untruncated-pair-tokenizer",
            )
        encoded = self.tokenizer(
            [query] * len(documents),
            list(documents),
            add_special_tokens=True,
            padding=False,
            truncation=False,
            return_attention_mask=False,
            return_token_type_ids=False,
        )
        if not isinstance(encoded, Mapping):
            raise ValueError("cross-encoder tokenizer returned an invalid batch")
        input_ids = encoded.get("input_ids")
        if not isinstance(input_ids, list) or len(input_ids) != len(documents):
            raise ValueError("cross-encoder tokenizer returned incomplete inputs")
        counts: list[int] = []
        for value in input_ids:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise ValueError("cross-encoder tokenizer returned invalid token IDs")
            counts.append(len(value))
        return RerankInputAudit(
            tokenizer_id=self.tokenizer_id,
            untruncated_token_counts=tuple(counts),
            input_limit=self.max_seq_length,
            status="exact-untruncated-pair-tokenizer",
        )

    def rerank(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankResult:
        started = time.monotonic()
        try:
            ranked = self.encoder.rank(
                query,
                list(documents),
                top_k=len(documents),
                return_documents=False,
                batch_size=self.batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )
        finally:
            if self._cache_clearer is not None:
                self._cache_clearer()
        scores = _ranked_scores(
            ranked,
            expected_count=len(documents),
            index_field="corpus_id",
            score_field="score",
        )
        return RerankResult(
            scores=scores,
            telemetry={
                "duration_ms": round((time.monotonic() - started) * 1_000),
                "retry_count": 0,
                "total_tokens": 0,
                "response_id": None,
                "status_code": None,
            },
        )


class OMLXReranker:
    """Thin client for oMLX's Cohere/Jina-compatible rerank endpoint."""

    provider = "omlx"
    package_name = "omlx"
    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_OMLX_MODEL,
        revision: str = DEFAULT_OMLX_MODEL_REVISION,
        service_model: str = DEFAULT_OMLX_SERVICE_MODEL,
        service_default_max_seq_length: int = (DEFAULT_OMLX_CAUSAL_RERANK_MAX_SEQ_LENGTH),
        base_url: str = DEFAULT_OMLX_BASE_URL,
        server_version: str = DEFAULT_OMLX_VERSION,
        api_key: str | None = None,
        timeout_seconds: float = 120.0,
        client: httpx.Client | None = None,
        allow_remote: bool = False,
        audit_tokenizer: Any | None = None,
    ) -> None:
        if not revision:
            raise ValueError("oMLX model revision must be pinned")
        if server_version != DEFAULT_OMLX_VERSION:
            raise RuntimeError(
                f"oMLX server version differs from the pinned contract: {server_version} != {DEFAULT_OMLX_VERSION}"
            )
        if service_default_max_seq_length <= 0:
            raise ValueError("oMLX service input limit must be positive")
        parsed = urlparse(base_url)
        if not allow_remote and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("oMLX base URL must be loopback unless explicitly allowed")
        self.model = model
        self.revision = revision
        self.service_model = service_model
        self.base_url = base_url.rstrip("/")
        self.package_version = server_version
        self.model_id = f"omlx:{model}@{revision}"
        self.service_default_max_seq_length = service_default_max_seq_length
        self.tokenizer_id = f"huggingface:{model}@{revision}"
        self._audit_tokenizer = audit_tokenizer
        self._audit_affix_token_counts: tuple[int, int] | None = None
        self.request_parameters = {
            "service_model": self.service_model,
            "service_default_max_seq_length": service_default_max_seq_length,
            "service_max_length_request_supported": False,
            "top_n": "all",
            "return_documents": False,
            "token_audit_policy_version": OMLX_TOKEN_AUDIT_POLICY_VERSION,
            "token_audit_tokenizer_id": self.tokenizer_id,
        }
        self.runtime_parameters = {
            "base_url": self.base_url,
            "server_version": server_version,
            "timeout_seconds": timeout_seconds,
        }
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.client = client or httpx.Client(
            headers=self.headers,
            timeout=timeout_seconds,
        )

    def _tokenizer(self) -> Any:
        if self._audit_tokenizer is None:
            from transformers import AutoTokenizer

            self._audit_tokenizer = AutoTokenizer.from_pretrained(
                self.model,
                revision=self.revision,
                trust_remote_code=False,
            )
        return self._audit_tokenizer

    def _affix_token_counts(self) -> tuple[int, int]:
        if self._audit_affix_token_counts is not None:
            return self._audit_affix_token_counts
        tokenizer = self._tokenizer()
        sentinel = "<<__SPICY_REGS_RERANK_CONTENT__>>"
        rendered = tokenizer.apply_chat_template(
            [
                {
                    "role": "system",
                    "content": OMLX_CAUSAL_RERANK_SYSTEM_PROMPT,
                },
                {"role": "user", "content": sentinel},
            ],
            tokenize=False,
            add_generation_prompt=True,
        )
        if not isinstance(rendered, str):
            raise ValueError("oMLX audit tokenizer returned a non-text chat template")
        parts = rendered.split(sentinel)
        if len(parts) != 2:
            raise ValueError("oMLX audit tokenizer did not preserve the prompt sentinel")
        prefix, suffix = parts
        if "<think>" not in suffix:
            suffix += "<think>\n\n</think>\n\n"
        prefix_ids = tokenizer.encode(
            prefix,
            add_special_tokens=False,
        )
        suffix_ids = tokenizer.encode(
            suffix,
            add_special_tokens=False,
        )
        if not isinstance(prefix_ids, Sequence) or not isinstance(
            suffix_ids,
            Sequence,
        ):
            raise ValueError("oMLX audit tokenizer returned invalid affix token IDs")
        self._audit_affix_token_counts = (
            len(prefix_ids),
            len(suffix_ids),
        )
        return self._audit_affix_token_counts

    def audit_inputs(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankInputAudit:
        tokenizer = self._tokenizer()
        prefix_count, suffix_count = self._affix_token_counts()
        contents = [
            (f"<Instruct>: {OMLX_CAUSAL_RERANK_DEFAULT_INSTRUCTION}\n<Query>: {query}\n<Document>: {document}")
            for document in documents
        ]
        encoded = tokenizer(
            contents,
            padding=False,
            truncation=False,
            add_special_tokens=False,
            return_attention_mask=False,
            return_token_type_ids=False,
        )
        if not isinstance(encoded, Mapping):
            raise ValueError("oMLX audit tokenizer returned an invalid batch")
        input_ids = encoded.get("input_ids")
        if not isinstance(input_ids, list) or len(input_ids) != len(documents):
            raise ValueError("oMLX audit tokenizer returned incomplete inputs")
        counts: list[int] = []
        for value in input_ids:
            if not isinstance(value, Sequence) or isinstance(
                value,
                (str, bytes),
            ):
                raise ValueError("oMLX audit tokenizer returned invalid token IDs")
            counts.append(prefix_count + len(value) + suffix_count)
        return RerankInputAudit(
            tokenizer_id=self.tokenizer_id,
            untruncated_token_counts=tuple(counts),
            input_limit=self.service_default_max_seq_length,
            status="exact-untruncated-omlx-0.5.3-causal-rerank-template",
        )

    def rerank(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankResult:
        started = time.monotonic()
        response = self.client.post(
            f"{self.base_url}/rerank",
            headers=self.headers,
            json={
                "model": self.service_model,
                "query": query,
                "documents": list(documents),
                "top_n": len(documents),
                "return_documents": False,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("oMLX rerank response must be an object")
        if payload.get("model") != self.service_model:
            raise ValueError("oMLX rerank response model differs from the request")
        scores = _ranked_scores(
            payload.get("results"),
            expected_count=len(documents),
            index_field="index",
            score_field="relevance_score",
        )
        usage = payload.get("usage")
        total_tokens = int(str(usage.get("total_tokens", 0))) if isinstance(usage, dict) else 0
        return RerankResult(
            scores=scores,
            telemetry={
                "duration_ms": round((time.monotonic() - started) * 1_000),
                "retry_count": 0,
                "total_tokens": total_tokens,
                "response_id": payload.get("id"),
                "status_code": response.status_code,
            },
        )


def _segment_texts(segment_rows: Sequence[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in segment_rows:
        segment_id = str(row["segment_id"])
        try:
            slices = json.loads(str(row["slices_json"]))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{segment_id}: invalid segment slices") from exc
        if not isinstance(slices, list):
            raise ValueError(f"{segment_id}: segment slices are not a list")
        result[segment_id] = "\n".join(str(item["text"]) for item in slices if isinstance(item, dict))
    return result


def _candidate_groups(
    rows: Sequence[dict[str, Any]],
) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[
            (
                str(row["config_id"]),
                str(row["scope"]),
                str(row["query_id"]),
            )
        ].append(row)
    for group in groups.values():
        group.sort(key=lambda row: int(str(row["candidate_rank"])))
    return dict(sorted(groups.items()))


def _selected_config_rows(
    *,
    experiment_manifest: dict[str, Any],
    segment_rows: Sequence[dict[str, Any]],
    candidate_rows: Sequence[dict[str, Any]],
    config_ids: Sequence[str] | None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Resolve an explicit rerank comparison subset against upstream configs."""
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
        raise ValueError("rerank comparison config IDs must be unique declared configs")
    selected = set(selected_ids)
    selected_configs = [config for config in configs if str(config["config_id"]) in selected]
    selected_segments = [row for row in segment_rows if str(row.get("config_id")) in selected]
    selected_candidates = [row for row in candidate_rows if str(row.get("config_id")) in selected]
    if not selected_segments or not selected_candidates:
        raise ValueError("rerank comparison selected no experiment rows")
    return selected_configs, selected_segments, selected_candidates


def _gold_rows_for_candidates(
    dataset_dir: Path,
    candidate_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    query_ids = {str(row["query_id"]) for row in candidate_rows}
    return [
        row for row in read_parquet_rows(dataset_dir / "gold_spans.parquet") if str(row.get("gold_id")) in query_ids
    ]


def _request_identity(
    *,
    model_id: str,
    request_parameters: dict[str, Any],
    config_id: str,
    scope: str,
    query_id: str,
    query: str,
    candidates: Sequence[dict[str, Any]],
    segment_texts: dict[str, str],
) -> tuple[str, str, str]:
    candidate_ids = [str(row["segment_id"]) for row in candidates]
    request_digest = hashlib.sha256(
        canonical_json(
            {
                "model_id": model_id,
                "request_parameters": request_parameters,
                "query_sha256": _text_sha(query),
                "candidates": [
                    {
                        "segment_id": segment_id,
                        "text_sha256": _text_sha(segment_texts[segment_id]),
                    }
                    for segment_id in candidate_ids
                ],
            }
        ).encode()
    ).hexdigest()
    candidate_ids_sha = hashlib.sha256(canonical_json(candidate_ids).encode()).hexdigest()
    work_id = (
        "rerank_work_"
        + hashlib.sha256(
            canonical_json(
                {
                    "config_id": config_id,
                    "scope": scope,
                    "query_id": query_id,
                    "request_digest": request_digest,
                }
            ).encode()
        ).hexdigest()[:24]
    )
    return request_digest, candidate_ids_sha, work_id


def _validated_scores(value: object, expected_count: int) -> tuple[float, ...]:
    if not isinstance(value, (list, tuple)) or len(value) != expected_count:
        raise ValueError("reranker score count differs from candidate count")
    scores = tuple(float(str(item)) for item in value)
    if any(not math.isfinite(score) for score in scores):
        raise ValueError("reranker scores contain a non-finite value")
    return scores


def _stored_bool(value: object) -> bool | None:
    """Decode booleans from both live rows and all-VARCHAR artifacts."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.casefold()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    return None


def _validated_input_audit(
    audit: RerankInputAudit,
    expected_count: int,
) -> RerankInputAudit:
    if len(audit.untruncated_token_counts) != expected_count:
        raise ValueError("reranker token audit count differs from candidates")
    counts: list[int | None] = []
    for value in audit.untruncated_token_counts:
        if value is None:
            counts.append(None)
            continue
        count = int(value)
        if count < 0:
            raise ValueError("reranker token audit contains a negative count")
        counts.append(count)
    input_limit = int(audit.input_limit) if audit.input_limit is not None else None
    if input_limit is not None and input_limit <= 0:
        raise ValueError("reranker token audit input limit is invalid")
    if not audit.status:
        raise ValueError("reranker token audit status is missing")
    if any(value is not None for value in counts) and (not audit.tokenizer_id or input_limit is None):
        raise ValueError("exact reranker token audit lacks tokenizer or limit")
    if audit.status.startswith("exact-") and any(value is None for value in counts):
        raise ValueError("exact reranker token audit is incomplete")
    return RerankInputAudit(
        tokenizer_id=audit.tokenizer_id,
        untruncated_token_counts=tuple(counts),
        input_limit=input_limit,
        status=audit.status,
    )


def _error_status(exc: BaseException) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code
    return None


def _relevant_ids(
    segment_rows: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], set[str]]:
    by_config_artifact: dict[tuple[str, str], list[tuple[str, list[dict[str, Any]]]]] = defaultdict(list)
    for row in segment_rows:
        slices = json.loads(str(row["slices_json"]))
        if not isinstance(slices, list):
            raise ValueError("segment slices are not a list")
        by_config_artifact[(str(row["config_id"]), str(row["artifact_digest"]))].append(
            (str(row["segment_id"]), slices)
        )
    config_ids = {str(row["config_id"]) for row in segment_rows}
    result: dict[tuple[str, str], set[str]] = {}
    for config_id in config_ids:
        for gold in gold_rows:
            query_id = str(gold["gold_id"])
            candidates = by_config_artifact.get(
                (config_id, str(gold["artifact_digest"])),
                [],
            )
            result[(config_id, query_id)] = {
                segment_id for segment_id, slices in candidates if _slices_overlap_gold(slices, gold)
            }
    return result


def _evaluation_rows(
    reranked_rows: Sequence[dict[str, Any]],
    segment_rows: Sequence[dict[str, Any]],
    gold_rows: Sequence[dict[str, Any]],
    configs: Sequence[dict[str, Any]],
    *,
    rerank_depths: Sequence[int] = RERANK_DEPTHS,
) -> list[dict[str, Any]]:
    effective_depths = _normalized_rerank_depths(rerank_depths)
    groups = _candidate_groups(reranked_rows)
    relevant = _relevant_ids(segment_rows, gold_rows)
    query_ids = [str(row["gold_id"]) for row in gold_rows]
    rows: list[dict[str, Any]] = []
    for config in sorted(configs, key=lambda item: str(item["config_id"])):
        config_id = str(config["config_id"])
        for scope in ("within-artifact", "corpus"):
            for depth in effective_depths:
                for stage, rank_field in (
                    ("dense", "candidate_rank"),
                    ("reranked", "rerank_rank"),
                ):
                    qrels: dict[str, dict[str, int]] = {}
                    runs: dict[str, dict[str, float]] = {}
                    for query_id in query_ids:
                        relevant_ids = relevant[(config_id, query_id)]
                        qrels[query_id] = {segment_id: 1 for segment_id in sorted(relevant_ids)}
                        if not qrels[query_id]:
                            qrels[query_id][f"{query_id}:missing-relevant-segment"] = 1
                        depth_group = [
                            item
                            for item in groups[(config_id, scope, query_id)]
                            if int(str(item["candidate_rank"])) <= depth
                        ]
                        ranked = sorted(
                            depth_group,
                            key=lambda item: int(str(item[rank_field])),
                        )
                        runs[query_id] = {
                            str(item["segment_id"]): float(len(ranked) - index) for index, item in enumerate(ranked)
                        }
                    metrics = _ir_metrics(qrels, runs)
                    rows.append(
                        {
                            "config_id": config_id,
                            "arm": config["arm"],
                            "max_tokens": config["max_tokens"],
                            "scope": scope,
                            "stage": stage,
                            "rerank_depth": depth,
                            "query_count": len(query_ids),
                            **metrics,
                            "metric_provider": IR_MEASURES_PROVIDER,
                        }
                    )
    return rows


def rerank_preflight(
    dataset_dir: Path,
    experiment_dir: Path,
    *,
    scope_dir: Path | None = None,
    rerank_depths: Sequence[int] = RERANK_DEPTHS,
    config_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    effective_depths = _normalized_rerank_depths(rerank_depths)
    candidate_depth = max(effective_depths)
    receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if receipt["status"] != "pass":
        raise RuntimeError("base segmentation experiment did not validate")
    experiment_manifest = json.loads(
        (experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8")
    )
    all_candidate_rows = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    all_segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    configs, segment_rows, candidate_rows = _selected_config_rows(
        experiment_manifest=experiment_manifest,
        segment_rows=all_segment_rows,
        candidate_rows=all_candidate_rows,
        config_ids=config_ids,
    )
    segment_texts = _segment_texts(segment_rows)
    counter = TiktokenCounter()
    source_groups = _candidate_groups(candidate_rows)
    groups = {key: rows[:candidate_depth] for key, rows in source_groups.items()}
    token_counts: dict[str, int] = {}

    def count_once(text: str) -> int:
        if text not in token_counts:
            token_counts[text] = counter.count(text)
        return token_counts[text]

    return {
        "format_version": FORMAT_VERSION,
        "experiment_id": receipt["experiment_id"],
        "document_scope_id": receipt.get("document_scope_id"),
        "config_ids": [str(config["config_id"]) for config in configs],
        "config_count": len(configs),
        "upstream_candidate_count": len(all_candidate_rows),
        "source_candidate_count": len(candidate_rows),
        "candidate_count": sum(len(group) for group in groups.values()),
        "rerank_depths": list(effective_depths),
        "rerank_candidate_depth": candidate_depth,
        "request_count": len(groups),
        "input_token_estimate": sum(
            count_once(str(group[0]["query_text"]))
            + sum(count_once(segment_texts[str(row["segment_id"])]) for row in group)
            for group in groups.values()
        ),
        "maximum_candidates_per_request": max(
            (len(group) for group in groups.values()),
            default=0,
        ),
    }


def build_rerank_experiment(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    reranker: Reranker,
    scope_dir: Path | None = None,
    rerank_depths: Sequence[int] = RERANK_DEPTHS,
    config_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Rerank every exact dense candidate group with safe resume."""
    effective_depths = _normalized_rerank_depths(rerank_depths)
    candidate_depth = max(effective_depths)
    if output_dir.exists():
        raise FileExistsError(f"Refusing to replace rerank experiment: {output_dir}")
    base_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    if base_receipt["status"] != "pass":
        raise RuntimeError("base segmentation experiment did not validate")
    base_manifest = json.loads((experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8"))
    all_candidate_rows = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    all_segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    configs, segment_rows, candidate_rows = _selected_config_rows(
        experiment_manifest=base_manifest,
        segment_rows=all_segment_rows,
        candidate_rows=all_candidate_rows,
        config_ids=config_ids,
    )
    selected_config_ids = [str(config["config_id"]) for config in configs]
    gold_rows = _gold_rows_for_candidates(dataset_dir, candidate_rows)
    segment_texts = _segment_texts(segment_rows)
    source_groups = _candidate_groups(candidate_rows)
    groups = {key: rows[:candidate_depth] for key, rows in source_groups.items()}
    run_id = (
        "rerank-"
        + hashlib.sha256(
            canonical_json(
                {
                    "experiment_id": base_receipt["experiment_id"],
                    "document_scope_id": base_receipt.get("document_scope_id"),
                    "model_id": reranker.model_id,
                    "request_parameters": reranker.request_parameters,
                    "rerank_depths": list(effective_depths),
                    "config_ids": selected_config_ids,
                    "version": RERANK_EXPERIMENT_VERSION,
                }
            ).encode()
        ).hexdigest()[:24]
    )
    work_dir = output_dir.parent / f".{output_dir.name}.rerank-work"
    work_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = BatchCheckpoint(
        work_dir,
        run_id=run_id,
        phase="candidate-rerank",
    )
    reranked_rows: list[dict[str, Any]] = []
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        for call_ordinal, (key, candidates) in enumerate(
            groups.items(),
            start=1,
        ):
            config_id, scope, query_id = key
            query = str(candidates[0]["query_text"])
            documents = [segment_texts[str(row["segment_id"])] for row in candidates]
            input_audit = _validated_input_audit(
                reranker.audit_inputs(query, documents),
                len(candidates),
            )
            request_digest, candidate_ids_sha, work_id = _request_identity(
                model_id=reranker.model_id,
                request_parameters=reranker.request_parameters,
                config_id=config_id,
                scope=scope,
                query_id=query_id,
                query=query,
                candidates=candidates,
                segment_texts=segment_texts,
            )
            checkpoint_key = {
                "subject_type": "retrieval-query",
                "subject_id": query_id,
                "artifact_digest": str(base_receipt["experiment_id"]),
                "segment_id": f"{config_id}:{scope}",
                "work_id": work_id,
            }
            cached = checkpoint.get(**checkpoint_key)
            if cached is not None and cached.get("status") == "completed":
                if (
                    cached.get("model_id") != reranker.model_id
                    or cached.get("request_digest") != request_digest
                    or cached.get("candidate_ids_sha256") != candidate_ids_sha
                ):
                    raise RuntimeError(f"{work_id}: completed rerank checkpoint is incompatible")
                scores = _validated_scores(
                    cached.get("scores"),
                    len(candidates),
                )
            else:
                attempt_count = int(str(cached.get("attempt_count", 0))) + 1 if cached is not None else 1
                common = {
                    **checkpoint_key,
                    "call_ordinal": call_ordinal,
                    "config_id": config_id,
                    "scope": scope,
                    "query_id": query_id,
                    "experiment_id": base_receipt["experiment_id"],
                    "request_digest": request_digest,
                    "candidate_ids_sha256": candidate_ids_sha,
                    "candidate_count": len(candidates),
                    "provider": reranker.provider,
                    "package_name": reranker.package_name,
                    "package_version": reranker.package_version,
                    "model_id": reranker.model_id,
                    "request_parameters_json": reranker.request_parameters,
                    "runtime_parameters_json": reranker.runtime_parameters,
                    "attempt_count": attempt_count,
                }
                try:
                    result = reranker.rerank(query, documents)
                    scores = _validated_scores(
                        result.scores,
                        len(candidates),
                    )
                except BaseException as exc:
                    checkpoint.append(
                        {
                            **common,
                            "status": "failed",
                            "retry_count": 0,
                            "duration_ms": 0,
                            "total_tokens": 0,
                            "response_id": None,
                            "status_code": _error_status(exc),
                            "error_type": type(exc).__name__,
                            "scores_sha256": None,
                            "scores": None,
                        }
                    )
                    raise RuntimeError(f"{work_id}: reranker failed; checkpoint is resumable") from exc
                scores_sha = hashlib.sha256(canonical_json(list(scores)).encode()).hexdigest()
                checkpoint.append(
                    {
                        **common,
                        "status": "completed",
                        "retry_count": result.telemetry.get("retry_count", 0),
                        "duration_ms": result.telemetry.get("duration_ms", 0),
                        "total_tokens": result.telemetry.get("total_tokens", 0),
                        "response_id": result.telemetry.get("response_id"),
                        "status_code": result.telemetry.get("status_code"),
                        "error_type": None,
                        "scores_sha256": scores_sha,
                        "scores": list(scores),
                    }
                )
            order = sorted(
                range(len(candidates)),
                key=lambda index: (
                    -scores[index],
                    str(candidates[index]["segment_id"]),
                ),
            )
            ranks = {candidate_index: rank for rank, candidate_index in enumerate(order, start=1)}
            truncation_flags = tuple(
                count > input_audit.input_limit if count is not None and input_audit.input_limit is not None else None
                for count in input_audit.untruncated_token_counts
            )
            reranked_rows.extend(
                {
                    **candidate,
                    "rerank_score": scores[index],
                    "rerank_rank": ranks[index],
                    "reranker_provider": reranker.provider,
                    "reranker_package": reranker.package_name,
                    "reranker_package_version": reranker.package_version,
                    "reranker_model_id": reranker.model_id,
                    "rerank_request_digest": request_digest,
                    "rerank_work_id": work_id,
                    "rerank_tokenizer_id": input_audit.tokenizer_id,
                    "rerank_untruncated_token_count": (input_audit.untruncated_token_counts[index]),
                    "rerank_input_limit": input_audit.input_limit,
                    "rerank_would_truncate": truncation_flags[index],
                    "rerank_token_audit_status": input_audit.status,
                }
                for index, candidate in enumerate(candidates)
            )
        reranked_rows.sort(
            key=lambda row: (
                str(row["config_id"]),
                str(row["scope"]),
                str(row["query_id"]),
                int(str(row["rerank_rank"])),
            )
        )
        metric_rows = _evaluation_rows(
            reranked_rows,
            segment_rows,
            gold_rows,
            configs,
            rerank_depths=effective_depths,
        )
        transitions = [
            {
                **record,
                "transition_ordinal": index,
            }
            for index, record in enumerate(
                checkpoint.transitions(),
                start=1,
            )
        ]
        runtime_configurations = sorted(
            {canonical_json(record.get("runtime_parameters_json") or {}) for record in transitions}
        )
        write_parquet_rows(
            temporary / "reranked_candidates.parquet",
            columns=RERANKED_CANDIDATE_COLUMNS,
            rows=reranked_rows,
        )
        write_parquet_rows(
            temporary / "rerank_config_metrics.parquet",
            columns=RERANK_METRIC_COLUMNS,
            rows=metric_rows,
        )
        write_parquet_rows(
            temporary / "rerank_requests.parquet",
            columns=RERANK_REQUEST_COLUMNS,
            rows=transitions,
        )
        artifacts_record = _artifact_hashes(temporary)
        rerank_id = (
            "segmentation_rerank_"
            + hashlib.sha256(
                canonical_json({name: record["sha256"] for name, record in sorted(artifacts_record.items())}).encode()
            ).hexdigest()[:24]
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "rerank_experiment_version": RERANK_EXPERIMENT_VERSION,
            "rerank_id": rerank_id,
            "dataset_evaluation_id": base_receipt["dataset_evaluation_id"],
            "segmentation_experiment_id": base_receipt["experiment_id"],
            "document_scope_id": base_receipt.get("document_scope_id"),
            "config_ids": selected_config_ids,
            "config_count": len(selected_config_ids),
            "upstream_candidate_count": len(all_candidate_rows),
            "source_candidate_count": len(candidate_rows),
            "candidate_count": len(reranked_rows),
            "request_group_count": len(groups),
            "configs": configs,
            "metric_provider": IR_MEASURES_PROVIDER,
            "rerank_depths": list(effective_depths),
            "rerank_candidate_depth": candidate_depth,
            "reranker_provider": reranker.provider,
            "reranker_package": reranker.package_name,
            "reranker_package_version": reranker.package_version,
            "reranker_model_id": reranker.model_id,
            "reranker_request_parameters": reranker.request_parameters,
            "reranker_runtime_configurations": [json.loads(value) for value in runtime_configurations],
            "reranker_token_audit_statuses": sorted({str(row["rerank_token_audit_status"]) for row in reranked_rows}),
            "truncated_candidate_count": sum(
                _stored_bool(row["rerank_would_truncate"]) is True for row in reranked_rows
            ),
            "unaudited_candidate_count": sum(row["rerank_untruncated_token_count"] is None for row in reranked_rows),
            "production_provider": reranker.production_provider,
            "artifacts": artifacts_record,
        }
        (temporary / "segmentation-rerank-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = validate_rerank_experiment(
            dataset_dir,
            experiment_dir,
            temporary,
            scope_dir=scope_dir,
        )
        (temporary / "segmentation-rerank-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError("Rerank experiment validation failed: " + "; ".join(receipt["failures"]))
        temporary.replace(output_dir)
        shutil.rmtree(work_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _metric_rows_match(
    actual: Sequence[dict[str, Any]],
    expected: Sequence[dict[str, Any]],
) -> bool:
    key_fields = ("config_id", "scope", "stage", "rerank_depth")
    actual_by_key = {tuple(str(row[field]) for field in key_fields): row for row in actual}
    expected_by_key = {tuple(str(row[field]) for field in key_fields): row for row in expected}
    if set(actual_by_key) != set(expected_by_key):
        return False
    numeric = set(RERANK_METRIC_COLUMNS) - {
        "config_id",
        "arm",
        "scope",
        "stage",
        "metric_provider",
    }
    for key, expected_row in expected_by_key.items():
        actual_row = actual_by_key[key]
        for field in RERANK_METRIC_COLUMNS:
            if field in numeric:
                try:
                    if not math.isclose(
                        float(str(actual_row[field])),
                        float(str(expected_row[field])),
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    ):
                        return False
                except (KeyError, TypeError, ValueError):
                    return False
            elif str(actual_row.get(field)) != str(expected_row.get(field)):
                return False
    return True


def validate_rerank_experiment(
    dataset_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    *,
    scope_dir: Path | None = None,
) -> dict[str, Any]:
    """Validate candidate bindings, ranks, metrics, attempts, and hashes."""
    manifest = json.loads((output_dir / "segmentation-rerank-manifest.json").read_text(encoding="utf-8"))
    base_receipt = validate_segmentation_experiment(
        dataset_dir,
        experiment_dir,
        scope_dir=scope_dir,
    )
    base_manifest = json.loads((experiment_dir / "segmentation-experiment-manifest.json").read_text(encoding="utf-8"))
    all_base_candidates = read_parquet_rows(experiment_dir / "retrieval_candidates.parquet")
    all_segment_rows = read_parquet_rows(experiment_dir / "experiment_segments.parquet")
    declared_config_ids = manifest.get("config_ids")
    selection_failure: str | None = None
    if not isinstance(declared_config_ids, list):
        selection_failure = "manifest config IDs are invalid"
        requested_config_ids: Sequence[str] | None = None
    else:
        requested_config_ids = [str(config_id) for config_id in declared_config_ids]
    try:
        configs, segment_rows, base_candidates = _selected_config_rows(
            experiment_manifest=base_manifest,
            segment_rows=all_segment_rows,
            candidate_rows=all_base_candidates,
            config_ids=requested_config_ids,
        )
    except ValueError as exc:
        selection_failure = str(exc)
        configs, segment_rows, base_candidates = _selected_config_rows(
            experiment_manifest=base_manifest,
            segment_rows=all_segment_rows,
            candidate_rows=all_base_candidates,
            config_ids=None,
        )
    gold_rows = _gold_rows_for_candidates(dataset_dir, base_candidates)
    reranked_rows = read_parquet_rows(output_dir / "reranked_candidates.parquet")
    metric_rows = read_parquet_rows(output_dir / "rerank_config_metrics.parquet")
    request_rows = read_parquet_rows(output_dir / "rerank_requests.parquet")
    segment_texts = _segment_texts(segment_rows)
    failures: list[str] = []
    failure_set: set[str] = set()

    def fail(message: str) -> None:
        if message not in failure_set:
            failure_set.add(message)
            failures.append(message)

    if selection_failure is not None:
        fail(selection_failure)
    expected_config_ids = [str(config["config_id"]) for config in configs]
    if manifest.get("config_ids") != expected_config_ids:
        fail("manifest config IDs differ")
    if manifest.get("config_count") != len(expected_config_ids):
        fail("manifest config count differs")
    if manifest.get("configs") != configs:
        fail("manifest configs differ")
    declared_depths = manifest.get("rerank_depths")
    try:
        rerank_depths = _normalized_rerank_depths(
            tuple(int(str(depth)) for depth in cast(Sequence[object], declared_depths))
        )
    except (TypeError, ValueError):
        fail("manifest rerank depths are invalid")
        rerank_depths = RERANK_DEPTHS
    candidate_depth = max(rerank_depths)
    if base_receipt["status"] != "pass":
        fail("base segmentation experiment did not validate")
    for field, expected in (
        ("format_version", FORMAT_VERSION),
        ("rerank_experiment_version", RERANK_EXPERIMENT_VERSION),
        ("segmentation_experiment_id", base_receipt["experiment_id"]),
        ("dataset_evaluation_id", base_receipt["dataset_evaluation_id"]),
        ("document_scope_id", base_receipt.get("document_scope_id")),
        ("metric_provider", IR_MEASURES_PROVIDER),
        ("rerank_depths", list(rerank_depths)),
        ("rerank_candidate_depth", candidate_depth),
        ("upstream_candidate_count", len(all_base_candidates)),
        ("source_candidate_count", len(base_candidates)),
    ):
        if manifest.get(field) != expected:
            fail(f"manifest {field} differs")
    base_groups = _candidate_groups(base_candidates)
    expected_base_candidates = [row for group in base_groups.values() for row in group[:candidate_depth]]
    if manifest.get("candidate_count") != len(expected_base_candidates):
        fail("manifest candidate count differs")
    base_by_key = {
        (
            str(row["config_id"]),
            str(row["scope"]),
            str(row["query_id"]),
            str(row["segment_id"]),
        ): row
        for row in expected_base_candidates
    }
    reranked_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in reranked_rows:
        key = (
            str(row.get("config_id")),
            str(row.get("scope")),
            str(row.get("query_id")),
            str(row.get("segment_id")),
        )
        if key in reranked_by_key:
            fail(f"{key}: duplicate reranked candidate")
        reranked_by_key[key] = row
        original = base_by_key.get(key)
        if original is None:
            fail(f"{key}: reranked candidate is not in the dense candidate set")
            continue
        if any(str(row.get(field)) != str(original.get(field)) for field in RETRIEVAL_CANDIDATE_COLUMNS):
            fail(f"{key}: dense candidate fields differ")
        if row.get("reranker_model_id") != manifest.get("reranker_model_id"):
            fail(f"{key}: reranker model differs")
        try:
            score = float(str(row.get("rerank_score")))
            rank = int(str(row.get("rerank_rank")))
        except (TypeError, ValueError):
            fail(f"{key}: rerank score or rank is invalid")
            continue
        if not math.isfinite(score) or rank <= 0:
            fail(f"{key}: rerank score or rank is out of range")
        token_count = row.get("rerank_untruncated_token_count")
        input_limit = row.get("rerank_input_limit")
        would_truncate = row.get("rerank_would_truncate")
        parsed_would_truncate = _stored_bool(would_truncate)
        audit_status = str(row.get("rerank_token_audit_status") or "")
        if not audit_status:
            fail(f"{key}: rerank token audit status is missing")
        if token_count is None:
            if would_truncate is not None:
                fail(f"{key}: unaudited rerank input has a truncation claim")
        else:
            if parsed_would_truncate is None:
                fail(f"{key}: rerank truncation flag is not boolean")
            try:
                parsed_count = int(str(token_count))
                parsed_limit = int(str(input_limit))
            except (TypeError, ValueError):
                fail(f"{key}: rerank token count or limit is invalid")
            else:
                if parsed_count < 0 or parsed_limit <= 0:
                    fail(f"{key}: rerank token count or limit is out of range")
                if not row.get("rerank_tokenizer_id"):
                    fail(f"{key}: audited rerank input lacks a tokenizer")
                if parsed_would_truncate is not None and parsed_would_truncate != (parsed_count > parsed_limit):
                    fail(f"{key}: rerank truncation flag differs from tokens")
    if set(reranked_by_key) != set(base_by_key):
        fail("reranked candidates do not exactly cover dense candidates")
    actual_audit_statuses = sorted({str(row.get("rerank_token_audit_status") or "") for row in reranked_rows})
    truncated_candidate_count = sum(_stored_bool(row.get("rerank_would_truncate")) is True for row in reranked_rows)
    unaudited_candidate_count = sum(row.get("rerank_untruncated_token_count") is None for row in reranked_rows)
    if manifest.get("reranker_token_audit_statuses") != actual_audit_statuses:
        fail("reranker token audit statuses differ from manifest")
    if manifest.get("truncated_candidate_count") != truncated_candidate_count:
        fail("truncated candidate count differs from manifest")
    if manifest.get("unaudited_candidate_count") != unaudited_candidate_count:
        fail("unaudited candidate count differs from manifest")
    if manifest.get("production_provider") and unaudited_candidate_count:
        fail("production reranker candidates lack exact token audits")

    reranked_groups = _candidate_groups(reranked_rows)
    expected_work: dict[str, tuple[tuple[str, str, str], str, str]] = {}
    for key, group in reranked_groups.items():
        source_dense_group = base_groups.get(key)
        dense_group = source_dense_group[:candidate_depth] if source_dense_group is not None else None
        if dense_group is None:
            continue
        request_digest, candidate_ids_sha, work_id = _request_identity(
            model_id=str(manifest.get("reranker_model_id")),
            request_parameters=dict(manifest.get("reranker_request_parameters") or {}),
            config_id=key[0],
            scope=key[1],
            query_id=key[2],
            query=str(dense_group[0]["query_text"]),
            candidates=dense_group,
            segment_texts=segment_texts,
        )
        expected_work[work_id] = (key, request_digest, candidate_ids_sha)
        if any(
            row.get("rerank_request_digest") != request_digest or row.get("rerank_work_id") != work_id for row in group
        ):
            fail(f"{key}: rerank request identity differs")
        try:
            ranked = sorted(
                group,
                key=lambda row: int(str(row["rerank_rank"])),
            )
            expected_order = sorted(
                group,
                key=lambda row: (
                    -float(str(row["rerank_score"])),
                    str(row["segment_id"]),
                ),
            )
        except (KeyError, TypeError, ValueError):
            continue
        if [str(row["segment_id"]) for row in ranked] != [str(row["segment_id"]) for row in expected_order]:
            fail(f"{key}: rerank order differs from scores")
        if [int(str(row["rerank_rank"])) for row in ranked] != list(range(1, len(group) + 1)):
            fail(f"{key}: rerank ranks are incomplete")

    request_by_work: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(request_rows, start=1):
        try:
            transition_ordinal = int(str(row["transition_ordinal"]))
        except (KeyError, TypeError, ValueError):
            transition_ordinal = -1
        if transition_ordinal != index:
            fail("rerank request transition ordinals are not contiguous")
        work_id = str(row.get("work_id"))
        request_by_work[work_id].append(row)
        expected = expected_work.get(work_id)
        if expected is None:
            fail(f"{work_id}: request work item is unexpected")
            continue
        _, request_digest, candidate_ids_sha = expected
        if row.get("request_digest") != request_digest:
            fail(f"{work_id}: request digest differs")
        if row.get("candidate_ids_sha256") != candidate_ids_sha:
            fail(f"{work_id}: candidate ID digest differs")
        for field, manifest_field in (
            ("provider", "reranker_provider"),
            ("package_name", "reranker_package"),
            ("package_version", "reranker_package_version"),
            ("model_id", "reranker_model_id"),
        ):
            if row.get(field) != manifest.get(manifest_field):
                fail(f"{work_id}: request {field} differs")
        expected_request_parameters = canonical_json(manifest.get("reranker_request_parameters") or {})
        if row.get("request_parameters_json") != expected_request_parameters:
            fail(f"{work_id}: request parameters differ")
        declared_runtime_parameters = {
            canonical_json(value) for value in (manifest.get("reranker_runtime_configurations") or [])
        }
        if row.get("runtime_parameters_json") not in (declared_runtime_parameters):
            fail(f"{work_id}: runtime parameters are undeclared")
        if row.get("status") not in {"failed", "completed"}:
            fail(f"{work_id}: request status is invalid")
    if set(request_by_work) != set(expected_work):
        fail("request ledger does not cover every rerank work item")
    for work_id, transitions in request_by_work.items():
        if transitions[-1].get("status") != "completed":
            fail(f"{work_id}: request has no terminal completion")
        if sum(row.get("status") == "completed" for row in transitions) != 1:
            fail(f"{work_id}: request completion count differs")
        completed = next(
            (row for row in transitions if row.get("status") == "completed"),
            None,
        )
        if completed is None:
            continue
        group_key = expected_work[work_id][0]
        scores = [
            float(str(row["rerank_score"]))
            for row in sorted(
                reranked_groups[group_key],
                key=lambda row: int(str(row["candidate_rank"])),
            )
        ]
        if completed.get("scores_sha256") != hashlib.sha256(canonical_json(scores).encode()).hexdigest():
            fail(f"{work_id}: completed score digest differs")

    expected_metrics = _evaluation_rows(
        reranked_rows,
        segment_rows,
        gold_rows,
        configs,
        rerank_depths=rerank_depths,
    )
    if not _metric_rows_match(metric_rows, expected_metrics):
        fail("rerank metrics differ from package recomputation")
    if any(
        _secret_like(str(value))
        for row in [*reranked_rows, *metric_rows, *request_rows]
        for value in row.values()
        if value is not None
    ):
        fail("rerank artifacts contain a secret-like value")
    artifacts_record = _artifact_hashes(output_dir)
    rerank_id = (
        "segmentation_rerank_"
        + hashlib.sha256(
            canonical_json({name: record["sha256"] for name, record in sorted(artifacts_record.items())}).encode()
        ).hexdigest()[:24]
    )
    if manifest.get("rerank_id") != rerank_id:
        fail("rerank ID differs from current artifacts")
    if manifest.get("artifacts") != artifacts_record:
        fail("rerank artifact hashes differ from manifest")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "rerank_id": rerank_id,
        "dataset_evaluation_id": base_receipt["dataset_evaluation_id"],
        "segmentation_experiment_id": base_receipt["experiment_id"],
        "document_scope_id": base_receipt.get("document_scope_id"),
        "production_provider": bool(manifest.get("production_provider")),
        "reranker_model_id": manifest.get("reranker_model_id"),
        "config_ids": expected_config_ids,
        "config_count": len(expected_config_ids),
        "candidate_count": len(reranked_rows),
        "truncated_candidate_count": truncated_candidate_count,
        "unaudited_candidate_count": unaudited_candidate_count,
        "request_group_count": len(request_by_work),
        "request_transition_count": len(request_rows),
        "failed_transition_count": sum(row.get("status") == "failed" for row in request_rows),
        "metric_row_count": len(metric_rows),
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("preflight")
    preflight.add_argument("dataset_dir", type=Path)
    preflight.add_argument("experiment_dir", type=Path)
    preflight.add_argument("--scope-dir", type=Path)
    preflight.add_argument(
        "--depth",
        dest="rerank_depths",
        action="append",
        type=int,
        help="Rerank depth; repeat for a sweep (default: 25,50,100,200)",
    )
    preflight.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Upstream config to rerank; repeat for a subset (default: all)",
    )
    build = commands.add_parser("build")
    build.add_argument("dataset_dir", type=Path)
    build.add_argument("experiment_dir", type=Path)
    build.add_argument("output_dir", type=Path)
    build.add_argument("--scope-dir", type=Path)
    build.add_argument(
        "--depth",
        dest="rerank_depths",
        action="append",
        type=int,
        help="Rerank depth; repeat for a sweep (default: 25,50,100,200)",
    )
    build.add_argument(
        "--config-id",
        dest="config_ids",
        action="append",
        help="Upstream config to rerank; repeat for a subset (default: all)",
    )
    build.add_argument(
        "--provider",
        choices=("deterministic", "sentence-transformers", "omlx"),
        default="deterministic",
    )
    build.add_argument("--model")
    build.add_argument("--revision")
    build.add_argument("--device")
    build.add_argument("--batch-size", type=int, default=16)
    build.add_argument(
        "--max-seq-length",
        type=int,
        default=DEFAULT_CROSS_ENCODER_MAX_SEQ_LENGTH,
    )
    build.add_argument("--omlx-base-url", default=DEFAULT_OMLX_BASE_URL)
    build.add_argument(
        "--service-model",
        default=DEFAULT_OMLX_SERVICE_MODEL,
    )
    validate = commands.add_parser("validate")
    validate.add_argument("dataset_dir", type=Path)
    validate.add_argument("experiment_dir", type=Path)
    validate.add_argument("output_dir", type=Path)
    validate.add_argument("--scope-dir", type=Path)
    args = parser.parse_args()
    if args.command == "preflight":
        result = rerank_preflight(
            args.dataset_dir,
            args.experiment_dir,
            scope_dir=args.scope_dir,
            rerank_depths=args.rerank_depths or RERANK_DEPTHS,
            config_ids=args.config_ids,
        )
    elif args.command == "build":
        if args.provider == "sentence-transformers":
            reranker: Reranker = SentenceTransformersReranker(
                model=args.model or DEFAULT_CROSS_ENCODER_MODEL,
                revision=args.revision or DEFAULT_CROSS_ENCODER_REVISION,
                device=args.device,
                batch_size=args.batch_size,
                max_seq_length=args.max_seq_length,
            )
        elif args.provider == "omlx":
            load_dotenv()
            reranker = OMLXReranker(
                model=args.model or DEFAULT_OMLX_MODEL,
                revision=args.revision or DEFAULT_OMLX_MODEL_REVISION,
                service_model=args.service_model,
                base_url=args.omlx_base_url,
                api_key=os.environ.get("OMLX_API_KEY"),
            )
        else:
            reranker = DeterministicReranker()
        result = build_rerank_experiment(
            args.dataset_dir,
            args.experiment_dir,
            args.output_dir,
            reranker=reranker,
            scope_dir=args.scope_dir,
            rerank_depths=args.rerank_depths or RERANK_DEPTHS,
            config_ids=args.config_ids,
        )
    else:
        result = validate_rerank_experiment(
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

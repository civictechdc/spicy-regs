"""Sentence Transformers adapters for dense, sparse, and reranking calls.

This module holds the three local-inference rows of the v3 provider table: the
dense embedder, the learned-sparse encoder, and the cross-encoder reranker.
Every call returns its output and its call details together; no adapter keeps
mutable last-call state. Each adapter runs its own model-native tokenizer
check, because a tiktoken budget never proves that a BGE or SPLADE input fits.

The pinned package, models, and revisions reproduce the frozen v2 retrieval
baseline. Constructors accept an ``encoder`` (and the reranker a
``cache_clearer``) so tests and offline runs never touch the real SDK, and a
``version_reader`` that resolves installed distribution versions: the pinned
version is read from the environment, never declared by the caller.

Only an adapter that loads its own encoder imports the package; an adapter that
is handed one imports nothing at all, torch included, so an injected encoder
runs where the ``embed`` extra is not installed.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Protocol, cast

SENTENCE_TRANSFORMERS_PACKAGE = "sentence-transformers"
SENTENCE_TRANSFORMERS_VERSION = "5.6.1"
TOKENIZER_PACKAGE = "transformers"

DENSE_PROVIDER = "sentence-transformers"
DEFAULT_DENSE_MODEL = "BAAI/bge-base-en-v1.5"
DEFAULT_DENSE_REVISION = "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"
DEFAULT_DENSE_DIMENSIONS = 768
DEFAULT_DENSE_BATCH_SIZE = 128

SPARSE_PROVIDER = "sentence-transformers-sparse"
DEFAULT_SPARSE_MODEL = "tomaarsen/splade-modernbert-base-miriad"
DEFAULT_SPARSE_REVISION = "c640ce28f7c4f4593ddba1b3855988f03a3d9cdc"
DEFAULT_SPARSE_DIMENSIONS = 50_368
DEFAULT_SPARSE_MAX_INPUT_TOKENS = 8_192
DEFAULT_SPARSE_BATCH_SIZE = 8
SPARSE_TASKS = ("document", "query")

RERANK_PROVIDER = "sentence-transformers"
DEFAULT_RERANK_MODEL = "BAAI/bge-reranker-v2-m3"
DEFAULT_RERANK_REVISION = "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"
DEFAULT_RERANK_MAX_SEQ_LENGTH = 4_096
DEFAULT_RERANK_BATCH_SIZE = 16

# Every tensor this module reads exposes these; anything else is not a tensor.
TENSOR_READ_SURFACE = ("detach", "cpu", "ndim", "shape", "is_sparse")

EXACT_MODEL_TOKEN_AUDIT = "exact-untruncated-model-tokenizer"
EXACT_PAIR_TOKEN_AUDIT = "exact-untruncated-pair-tokenizer"
UNAVAILABLE_TOKEN_AUDIT = "unavailable-model-tokenizer"


@dataclass(frozen=True)
class DenseEmbeddingResult:
    """One dense embedding call: its vectors and its secret-free call details."""

    vectors: tuple[tuple[float, ...], ...]
    call: dict[str, Any]


@dataclass(frozen=True)
class SparseVector:
    """Portable, validated representation of one sparse model vector."""

    dimensions: int
    indices: tuple[int, ...]
    values: tuple[float, ...]


@dataclass(frozen=True)
class SparseEncodingResult:
    """One sparse encoding call: its vectors and its secret-free call details."""

    vectors: tuple[SparseVector, ...]
    call: dict[str, Any]


@dataclass(frozen=True)
class RerankResult:
    """One rerank call: one score per candidate plus secret-free call details."""

    scores: tuple[float, ...]
    call: dict[str, Any]


class DenseEmbedder(Protocol):
    """Exact texts and model settings in, vectors and call details out."""

    provider: str
    model_id: str
    dimensions: int
    tokenizer_id: str
    max_input_tokens: int | None
    production_provider: bool

    def model_token_count(self, text: str) -> int | None: ...

    def embed(self, texts: Sequence[str]) -> DenseEmbeddingResult: ...


class SparseEncoder(Protocol):
    """Exact texts and model settings in, sparse vectors and call details out."""

    provider: str
    model_id: str
    dimensions: int
    tokenizer_id: str
    max_input_tokens: int | None
    production_provider: bool

    def model_token_count(self, text: str) -> int | None: ...

    def encode(self, texts: Sequence[str], *, task: str) -> SparseEncodingResult: ...


class Reranker(Protocol):
    """Query and a fixed candidate list in, one score per candidate out."""

    provider: str
    model_id: str
    tokenizer_id: str
    max_seq_length: int
    batch_size: int
    production_provider: bool

    def rerank(self, query: str, documents: Sequence[str]) -> RerankResult: ...


def installed_package_version(package: str) -> str | None:
    """Report an installed distribution version without importing the package."""
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def require_pinned_package_version(
    version_reader: Callable[[str], str | None] = installed_package_version,
) -> str:
    """Hold every local-inference call to the one pinned Sentence Transformers build.

    ``version_reader`` is the injection seam: it resolves an installed
    distribution version, so the pin is verified against an installation rather
    than declared by the caller, and tests substitute a reader instead of a
    version string. The default reads the real environment.
    """
    resolved = version_reader(SENTENCE_TRANSFORMERS_PACKAGE)
    if resolved != SENTENCE_TRANSFORMERS_VERSION:
        raise RuntimeError(
            "sentence-transformers version differs from the pinned contract: "
            f"{resolved} != {SENTENCE_TRANSFORMERS_VERSION}"
        )
    return resolved


def validate_sparse_vector(vector: SparseVector, dimensions: int) -> SparseVector:
    """Reject any sparse vector that breaks the declared provider contract."""
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


def sparse_vectors_from_tensor(value: Any, *, dimensions: int) -> tuple[SparseVector, ...]:
    """Convert the package's dense or COO tensor into portable sparse rows.

    A tensor is recognised by the reading surface it exposes, not by
    ``isinstance(value, torch.Tensor)``, and every value is read back through
    the tensor's own accessors. So this conversion imports nothing: whoever
    produced a real tensor already has torch installed, and a stand-in tensor
    runs the same branches the real one does.
    """
    if any(not hasattr(value, name) for name in TENSOR_READ_SURFACE):
        raise TypeError("SparseEncoder returned a non-tensor value")
    tensor = value.detach().cpu()
    if int(tensor.ndim) == 1:
        tensor = tensor.unsqueeze(0)
    shape = tuple(int(size) for size in tensor.shape)
    if len(shape) != 2 or shape[1] != dimensions:
        raise ValueError("SparseEncoder returned unexpected dimensions")
    sparse = tensor.coalesce() if tensor.is_sparse else tensor.to_sparse().coalesce()
    row_coordinates, column_coordinates = sparse.indices().tolist()
    stored_values = sparse.values().tolist()
    by_row: list[list[tuple[int, float]]] = [[] for _ in range(shape[0])]
    for row, column, score in zip(row_coordinates, column_coordinates, stored_values, strict=True):
        by_row[int(row)].append((int(column), float(score)))
    rows: list[SparseVector] = []
    for items in by_row:
        items.sort()
        rows.append(
            validate_sparse_vector(
                SparseVector(
                    dimensions=dimensions,
                    indices=tuple(index for index, _ in items),
                    values=tuple(score for _, score in items),
                ),
                dimensions,
            )
        )
    return tuple(rows)


def ranked_scores_in_input_order(
    ranked: object,
    *,
    expected_count: int,
    index_field: str = "corpus_id",
    score_field: str = "score",
) -> tuple[float, ...]:
    """Return one finite score per candidate, restored to the input order."""
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


def _model_token_count(tokenizer: Any, text: str) -> int | None:
    """Count the untruncated model-native input for one text."""
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


def _over_limit_flags(
    token_counts: Sequence[int | None],
    maximum: int | None,
) -> tuple[bool | None, ...]:
    return tuple(count > maximum if count is not None and maximum is not None else None for count in token_counts)


def _token_audit(
    counts: Sequence[int | None],
    maximum: int | None,
    *,
    label: str,
) -> tuple[tuple[int | None, ...], tuple[bool | None, ...], str]:
    """Record model-native token evidence for every input.

    An over-limit input is recorded and still sent, exactly as the v2 harness
    sent it: this audit never truncates an input and never prevents the call.
    """
    token_counts = tuple(counts)
    if maximum is not None and any(count is None for count in token_counts):
        raise ValueError(f"{label}: model-native token audit is unavailable")
    status = UNAVAILABLE_TOKEN_AUDIT if any(count is None for count in token_counts) else EXACT_MODEL_TOKEN_AUDIT
    return token_counts, _over_limit_flags(token_counts, maximum), status


def _elapsed_ms(started: float) -> float:
    return round((time.monotonic() - started) * 1_000, 3)


class SentenceTransformersDenseEmbedder:
    """Pinned BGE dense embedder over ``sentence_transformers.SentenceTransformer``."""

    provider = DENSE_PROVIDER
    package_name = SENTENCE_TRANSFORMERS_PACKAGE
    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_DENSE_MODEL,
        revision: str = DEFAULT_DENSE_REVISION,
        dimensions: int = DEFAULT_DENSE_DIMENSIONS,
        batch_size: int = DEFAULT_DENSE_BATCH_SIZE,
        device: str | None = None,
        encoder: Any | None = None,
        version_reader: Callable[[str], str | None] = installed_package_version,
    ) -> None:
        if dimensions <= 0 or batch_size <= 0:
            raise ValueError("invalid sentence-transformer provider limits")
        if not revision:
            raise ValueError("sentence-transformer revision must be pinned")
        self.package_version = require_pinned_package_version(version_reader)
        self.encoder_source = "injected" if encoder is not None else "loaded"
        if encoder is None:
            from sentence_transformers import SentenceTransformer

            encoder = SentenceTransformer(model, revision=revision, device=device)
        reported_dimensions = encoder.get_embedding_dimension()
        if reported_dimensions is None:
            raise ValueError("sentence-transformer did not report embedding dimensions")
        actual_dimensions = int(reported_dimensions)
        if actual_dimensions != dimensions:
            raise ValueError(
                f"sentence-transformer dimensions differ from the declared value: {actual_dimensions} != {dimensions}"
            )
        self.model = model
        self.revision = revision
        self.model_id = f"sentence-transformers:{model}@{revision}"
        self.dimensions = dimensions
        self.batch_size = batch_size
        self.device = device
        self.device_label = device or str(getattr(encoder, "device", "auto"))
        self._encoder = encoder
        reported_max_input_tokens = getattr(encoder, "max_seq_length", None)
        self.max_input_tokens = int(reported_max_input_tokens) if reported_max_input_tokens is not None else None
        self.tokenizer_id = f"sentence-transformers:{model}@{revision}:tokenizer"
        self.tokenizer_package_version = version_reader(TOKENIZER_PACKAGE)

    @property
    def runtime_parameters(self) -> dict[str, Any]:
        """Build the recorded runtime settings fresh from this adapter's scalars."""
        return {
            "batch_size": self.batch_size,
            "device": self.device_label,
            "normalize_embeddings": True,
            "trust_remote_code": False,
        }

    def model_token_count(self, text: str) -> int | None:
        """Count the untruncated model-native embedding input."""
        return _model_token_count(getattr(self._encoder, "tokenizer", None), text)

    def embed(self, texts: Sequence[str]) -> DenseEmbeddingResult:
        """Embed exact texts and return the vectors with their call details."""
        requested = list(texts)
        token_counts, over_limit, audit_status = _token_audit(
            [self.model_token_count(text) for text in requested],
            self.max_input_tokens,
            label="dense embedding input",
        )
        started = time.monotonic()
        provider_invoked = bool(requested)
        if requested:
            encoded = self._encoder.encode(
                requested,
                batch_size=self.batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            )
            vectors = tuple(tuple(float(value) for value in row) for row in encoded)
        else:
            vectors = ()
        if len(vectors) != len(requested):
            raise RuntimeError("sentence-transformer response count differs from input")
        if any(len(vector) != self.dimensions for vector in vectors):
            raise RuntimeError("sentence-transformer response dimensions differ from the declared value")
        return DenseEmbeddingResult(
            vectors=vectors,
            call=self._call_details(
                duration_ms=_elapsed_ms(started),
                input_count=len(requested),
                token_counts=token_counts,
                over_limit=over_limit,
                audit_status=audit_status,
                provider_invoked=provider_invoked,
            ),
        )

    def _call_details(
        self,
        *,
        duration_ms: float,
        input_count: int,
        token_counts: tuple[int | None, ...],
        over_limit: tuple[bool | None, ...],
        audit_status: str,
        provider_invoked: bool,
    ) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "operation": "dense-embedding",
            "package_name": self.package_name,
            "package_version": self.package_version,
            "encoder_source": self.encoder_source,
            "model_id": self.model_id,
            "model": self.model,
            "revision": self.revision,
            "dimensions": self.dimensions,
            "tokenizer_id": self.tokenizer_id,
            "tokenizer_package_version": self.tokenizer_package_version,
            "token_counts": token_counts,
            "max_input_tokens": self.max_input_tokens,
            "inputs_over_limit": over_limit,
            "token_audit_status": audit_status,
            "input_count": input_count,
            "status": "completed" if provider_invoked else "completed_empty",
            "provider_invoked": provider_invoked,
            "attempt_count": 1 if provider_invoked else 0,
            "retry_count": 0,
            "duration_ms": duration_ms,
            "error_type": None,
            "runtime_parameters": self.runtime_parameters,
        }


class SentenceTransformersSparseEncoder:
    """Pinned SPLADE encoder over ``sentence_transformers.SparseEncoder``."""

    provider = SPARSE_PROVIDER
    package_name = SENTENCE_TRANSFORMERS_PACKAGE
    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_SPARSE_MODEL,
        revision: str = DEFAULT_SPARSE_REVISION,
        dimensions: int = DEFAULT_SPARSE_DIMENSIONS,
        batch_size: int = DEFAULT_SPARSE_BATCH_SIZE,
        device: str | None = None,
        encoder: Any | None = None,
        version_reader: Callable[[str], str | None] = installed_package_version,
    ) -> None:
        if not revision:
            raise ValueError("sparse model revision must be pinned")
        if dimensions <= 0 or batch_size <= 0:
            raise ValueError("invalid sparse provider limits")
        self.package_version = require_pinned_package_version(version_reader)
        self.encoder_source = "injected" if encoder is not None else "loaded"
        if encoder is None:
            from sentence_transformers import SparseEncoder as PackageSparseEncoder

            encoder = PackageSparseEncoder(model, revision=revision, device=device, similarity_fn_name="dot")
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
                "default sparse model input limit differs from the declared contract: "
                f"{maximum} != {DEFAULT_SPARSE_MAX_INPUT_TOKENS}"
            )
        self.model = model
        self.revision = revision
        self.model_id = f"sentence-transformers-sparse:{model}@{revision}"
        self.dimensions = dimensions
        self.batch_size = batch_size
        self.device = device
        self.device_label = device or str(getattr(encoder, "device", "auto"))
        self._encoder = encoder
        self.max_input_tokens = int(maximum) if maximum is not None else None
        self.tokenizer_id = f"sentence-transformers-sparse:{model}@{revision}:tokenizer"
        self.tokenizer_package_version = version_reader(TOKENIZER_PACKAGE)

    @property
    def runtime_parameters(self) -> dict[str, Any]:
        """Build the recorded runtime settings fresh from this adapter's scalars."""
        return {
            "batch_size": self.batch_size,
            "device": self.device_label,
            "similarity_fn_name": "dot",
            "trust_remote_code": False,
        }

    def model_token_count(self, text: str) -> int | None:
        """Count the untruncated model-native sparse input."""
        return _model_token_count(getattr(self._encoder, "tokenizer", None), text)

    def encode(self, texts: Sequence[str], *, task: str) -> SparseEncodingResult:
        """Encode exact texts for one task and return vectors with call details."""
        if task not in SPARSE_TASKS:
            raise ValueError(f"unsupported sparse embedding task: {task}")
        requested = list(texts)
        token_counts, over_limit, audit_status = _token_audit(
            [self.model_token_count(text) for text in requested],
            self.max_input_tokens,
            label=f"sparse {task} input",
        )
        started = time.monotonic()
        provider_invoked = bool(requested)
        if requested:
            method = self._encoder.encode_document if task == "document" else self._encoder.encode_query
            encoded = method(
                requested,
                batch_size=self.batch_size,
                show_progress_bar=False,
                convert_to_tensor=True,
                convert_to_sparse_tensor=True,
                save_to_cpu=True,
            )
            vectors = sparse_vectors_from_tensor(encoded, dimensions=self.dimensions)
        else:
            vectors = ()
        if len(vectors) != len(requested):
            raise RuntimeError("SparseEncoder response count differs from input")
        return SparseEncodingResult(
            vectors=vectors,
            call={
                "provider": self.provider,
                "operation": "sparse-encoding",
                "task": task,
                "package_name": self.package_name,
                "package_version": self.package_version,
                "encoder_source": self.encoder_source,
                "model_id": self.model_id,
                "model": self.model,
                "revision": self.revision,
                "dimensions": self.dimensions,
                "active_dimension_counts": tuple(len(vector.indices) for vector in vectors),
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": self.tokenizer_package_version,
                "token_counts": token_counts,
                "max_input_tokens": self.max_input_tokens,
                "inputs_over_limit": over_limit,
                "token_audit_status": audit_status,
                "input_count": len(requested),
                "status": "completed" if provider_invoked else "completed_empty",
                "provider_invoked": provider_invoked,
                "attempt_count": 1 if provider_invoked else 0,
                "retry_count": 0,
                "duration_ms": _elapsed_ms(started),
                "error_type": None,
                "runtime_parameters": self.runtime_parameters,
            },
        )


class SentenceTransformersReranker:
    """Pinned BGE cross-encoder over ``sentence_transformers.CrossEncoder.rank``.

    The candidate list is fixed: this adapter scores exactly what it is given,
    in the order it is given. It never truncates, re-orders, or merges result
    lists; ranking and rank merging belong to the retrieval step.

    An injected encoder is read, never written: ``max_seq_length`` declares the
    limit this adapter expects and is verified against the encoder, so two
    adapters can share one encoder without either one changing the other's
    recorded limit.
    """

    provider = RERANK_PROVIDER
    package_name = SENTENCE_TRANSFORMERS_PACKAGE
    production_provider = True

    def __init__(
        self,
        *,
        model: str = DEFAULT_RERANK_MODEL,
        revision: str = DEFAULT_RERANK_REVISION,
        device: str | None = None,
        batch_size: int = DEFAULT_RERANK_BATCH_SIZE,
        max_seq_length: int | None = None,
        encoder: Any | None = None,
        cache_clearer: Callable[[], None] | None = None,
        version_reader: Callable[[str], str | None] = installed_package_version,
    ) -> None:
        if not revision:
            raise ValueError("cross-encoder revision must be pinned")
        if batch_size <= 0:
            raise ValueError("cross-encoder batch size must be positive")
        if max_seq_length is not None and max_seq_length <= 0:
            raise ValueError("cross-encoder input limit must be positive")
        self.package_version = require_pinned_package_version(version_reader)
        self.model = model
        self.revision = revision
        self.model_id = f"sentence-transformers:{model}@{revision}"
        self.device = device
        self.batch_size = batch_size
        owns_encoder = encoder is None
        self.encoder_source = "loaded" if owns_encoder else "injected"
        if encoder is None:
            # An encoder this adapter loads is pinned once, at load time; an injected
            # encoder is never written to, because a caller may share it with others.
            max_seq_length = DEFAULT_RERANK_MAX_SEQ_LENGTH if max_seq_length is None else max_seq_length
            from sentence_transformers import CrossEncoder

            encoder = CrossEncoder(
                model,
                revision=revision,
                device=device,
                trust_remote_code=False,
                max_length=max_seq_length,
            )
        self.encoder = encoder
        reported_max_seq_length = self._encoder_max_seq_length()
        if max_seq_length is not None and reported_max_seq_length != max_seq_length:
            raise ValueError(
                "cross-encoder input limit differs from the declared value: "
                f"{reported_max_seq_length} != {max_seq_length}"
            )
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
        self.tokenizer_package_version = version_reader(TOKENIZER_PACKAGE)
        if cache_clearer is not None:
            self._cache_clearer = cache_clearer
        elif owns_encoder and device == "mps":
            import torch

            self._cache_clearer = torch.mps.empty_cache
        else:
            self._cache_clearer = None
        self.device_label = device or str(getattr(self.encoder, "device", "auto"))

    def _encoder_max_seq_length(self) -> int:
        """Read the cross-encoder's own input limit; this adapter never sets it."""
        reported = getattr(self.encoder, "max_seq_length", None)
        if reported is None:
            raise ValueError("cross-encoder did not report an input limit")
        return int(reported)

    @property
    def max_seq_length(self) -> int:
        """Report the encoder's current input limit, read fresh so it cannot go stale."""
        return self._encoder_max_seq_length()

    @property
    def request_parameters(self) -> dict[str, Any]:
        """Build the recorded request settings fresh from the encoder's own limit."""
        return {"max_seq_length": self._encoder_max_seq_length()}

    @property
    def runtime_parameters(self) -> dict[str, Any]:
        """Build the recorded runtime settings fresh from this adapter's scalars."""
        return {
            "batch_size": self.batch_size,
            "device": self.device_label,
            "trust_remote_code": False,
            "clear_device_cache_after_request": self._cache_clearer is not None,
        }

    def pair_token_counts(self, query: str, documents: Sequence[str]) -> tuple[int, ...]:
        """Count the untruncated query/candidate pairs with the model's tokenizer."""
        if not documents:
            return ()
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
        return tuple(counts)

    def rerank(self, query: str, documents: Sequence[str]) -> RerankResult:
        """Score a fixed candidate list and return the scores with call details."""
        candidates = list(documents)
        max_seq_length = self._encoder_max_seq_length()
        token_counts = self.pair_token_counts(query, candidates)
        started = time.monotonic()
        provider_invoked = bool(candidates)
        if candidates:
            try:
                ranked = self.encoder.rank(
                    query,
                    candidates,
                    top_k=len(candidates),
                    return_documents=False,
                    batch_size=self.batch_size,
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    device=self.device,
                )
            finally:
                if self._cache_clearer is not None:
                    self._cache_clearer()
            scores = ranked_scores_in_input_order(ranked, expected_count=len(candidates))
        else:
            scores = ()
        return RerankResult(
            scores=scores,
            call={
                "provider": self.provider,
                "operation": "rerank",
                "package_name": self.package_name,
                "package_version": self.package_version,
                "encoder_source": self.encoder_source,
                "model_id": self.model_id,
                "model": self.model,
                "revision": self.revision,
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": self.tokenizer_package_version,
                "token_counts": token_counts,
                "max_input_tokens": max_seq_length,
                "inputs_over_limit": _over_limit_flags(token_counts, max_seq_length),
                "token_audit_status": EXACT_PAIR_TOKEN_AUDIT,
                "candidate_count": len(candidates),
                "status": "completed" if provider_invoked else "completed_empty",
                "provider_invoked": provider_invoked,
                "attempt_count": 1 if provider_invoked else 0,
                "retry_count": 0,
                "duration_ms": _elapsed_ms(started),
                "error_type": None,
                "request_parameters": {"max_seq_length": max_seq_length},
                "runtime_parameters": self.runtime_parameters,
            },
        )

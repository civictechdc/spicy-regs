"""Contract tests for the three Sentence Transformers pipeline adapters."""

from __future__ import annotations

import inspect
import sys
import types
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from spicy_regs.docpipeline.adapters.sentence_transformers import (
    DEFAULT_DENSE_DIMENSIONS,
    DEFAULT_DENSE_MODEL,
    DEFAULT_DENSE_REVISION,
    DEFAULT_RERANK_MAX_SEQ_LENGTH,
    DEFAULT_RERANK_MODEL,
    DEFAULT_RERANK_REVISION,
    DEFAULT_SPARSE_DIMENSIONS,
    DEFAULT_SPARSE_MAX_INPUT_TOKENS,
    DEFAULT_SPARSE_MODEL,
    DEFAULT_SPARSE_REVISION,
    SENTENCE_TRANSFORMERS_PACKAGE,
    SENTENCE_TRANSFORMERS_VERSION,
    TOKENIZER_PACKAGE,
    DenseEmbeddingResult,
    RerankResult,
    SentenceTransformersDenseEmbedder,
    SentenceTransformersReranker,
    SentenceTransformersSparseEncoder,
    SparseEncodingResult,
    SparseVector,
    installed_package_version,
    ranked_scores_in_input_order,
    validate_sparse_vector,
)

FIXTURE_TOKENIZER_VERSION = "4.57.6"

MODEL_TOKENIZER_OPTIONS = {
    "add_special_tokens": True,
    "truncation": False,
    "return_attention_mask": False,
    "return_token_type_ids": False,
}
PAIR_TOKENIZER_OPTIONS = {
    "add_special_tokens": True,
    "padding": False,
    "truncation": False,
    "return_attention_mask": False,
    "return_token_type_ids": False,
}


class WordTokenizer:
    """Model tokenizer stand-in: two special tokens around whitespace words."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    def __call__(self, text: str, **options: Any) -> dict[str, list[int]]:
        assert options == MODEL_TOKENIZER_OPTIONS
        self.texts.append(text)
        return {"input_ids": [0, *range(len(text.split())), 1]}


class PairTokenizer:
    """Cross-encoder pair tokenizer stand-in over query/document pairs."""

    def __init__(self) -> None:
        self.batches: list[tuple[list[str], list[str]]] = []

    def __call__(
        self,
        queries: list[str],
        documents: list[str],
        **options: Any,
    ) -> dict[str, list[list[int]]]:
        assert options == PAIR_TOKENIZER_OPTIONS
        self.batches.append((list(queries), list(documents)))
        return {
            "input_ids": [
                list(range(len(f"{query} {document}".split()) + 2))
                for query, document in zip(queries, documents, strict=True)
            ]
        }


class FakeDenseEncoder:
    """Injected stand-in for ``sentence_transformers.SentenceTransformer``."""

    def __init__(
        self,
        *,
        vectors: tuple[tuple[float, ...], ...] = ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0)),
        dimensions: int | None = 3,
        max_seq_length: int | None = 512,
        tokenizer: Any | None = None,
    ) -> None:
        self.vectors = vectors
        self.dimensions = dimensions
        self.max_seq_length = max_seq_length
        self.tokenizer = WordTokenizer() if tokenizer is None else tokenizer
        self.device = "cpu"
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def get_embedding_dimension(self) -> int | None:
        return self.dimensions

    def encode(self, texts: list[str], **options: Any) -> tuple[tuple[float, ...], ...]:
        self.calls.append((list(texts), dict(options)))
        return self.vectors


class FakeSparseEncoder:
    """Injected stand-in for ``sentence_transformers.SparseEncoder``."""

    def __init__(
        self,
        *,
        dimensions: int = 4,
        max_seq_length: int | None = 8_192,
        tokenizer: Any | None = None,
    ) -> None:
        import torch

        self.dimensions = dimensions
        self.max_seq_length = max_seq_length
        self.tokenizer = WordTokenizer() if tokenizer is None else tokenizer
        self.device = "cpu"
        self.document_calls: list[tuple[list[str], dict[str, Any]]] = []
        self.query_calls: list[tuple[list[str], dict[str, Any]]] = []
        self._torch = torch

    def get_embedding_dimension(self) -> int | None:
        return self.dimensions

    def encode_document(self, texts: list[str], **options: Any) -> Any:
        self.document_calls.append((list(texts), dict(options)))
        return self._torch.tensor([[0.0, 2.0, 0.0, 1.0], [3.0, 0.0, 0.0, 0.0]])

    def encode_query(self, texts: list[str], **options: Any) -> Any:
        self.query_calls.append((list(texts), dict(options)))
        return self._torch.tensor([[0.0, 1.0, 0.0, 1.0]])


class FakeCrossEncoder:
    """Injected stand-in for ``sentence_transformers.CrossEncoder``."""

    def __init__(
        self,
        *,
        ranked: list[dict[str, Any]] | None = None,
        max_seq_length: int | None = DEFAULT_RERANK_MAX_SEQ_LENGTH,
        tokenizer: Any | None = None,
    ) -> None:
        self.ranked = (
            ranked if ranked is not None else [{"corpus_id": 1, "score": 0.9}, {"corpus_id": 0, "score": -0.2}]
        )
        self.max_seq_length = max_seq_length
        self.tokenizer = PairTokenizer() if tokenizer is None else tokenizer
        self.device = "cpu"
        self.calls: list[tuple[str, list[str], dict[str, Any]]] = []

    def rank(self, query: str, documents: list[str], **options: Any) -> list[dict[str, Any]]:
        self.calls.append((query, list(documents), dict(options)))
        return self.ranked


class BlockedImportFinder:
    """Meta-path finder that fails any import of the blocked package."""

    def __init__(self, blocked: str) -> None:
        self.blocked = blocked

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if fullname == self.blocked or fullname.startswith(f"{self.blocked}."):
            raise ImportError(f"{fullname} is deliberately unavailable in this test")
        return None


@contextmanager
def sdk_unavailable() -> Iterator[None]:
    """Prove the adapters never reach for the SDK when an encoder is injected."""
    finder = BlockedImportFinder("sentence_transformers")
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "sentence_transformers" or name.startswith("sentence_transformers.")
    }
    for name in saved:
        del sys.modules[name]
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.meta_path.remove(finder)
        sys.modules.update(saved)


@contextmanager
def fake_sdk(**classes: Any) -> Iterator[None]:
    """Stand in for the SDK so the model-loading path runs without a download."""
    module = types.ModuleType("sentence_transformers")
    for name, value in classes.items():
        setattr(module, name, value)
    saved = {
        name: existing
        for name, existing in sys.modules.items()
        if name == "sentence_transformers" or name.startswith("sentence_transformers.")
    }
    for name in saved:
        del sys.modules[name]
    sys.modules["sentence_transformers"] = module
    try:
        yield
    finally:
        del sys.modules["sentence_transformers"]
        sys.modules.update(saved)


def fake_version_reader(
    sentence_transformers: str | None = SENTENCE_TRANSFORMERS_VERSION,
    transformers: str | None = FIXTURE_TOKENIZER_VERSION,
) -> Callable[[str], str | None]:
    """Resolve versions from a fake installation instead of the real environment."""
    installed = {
        SENTENCE_TRANSFORMERS_PACKAGE: sentence_transformers,
        TOKENIZER_PACKAGE: transformers,
    }

    def read(package: str) -> str | None:
        return installed.get(package)

    return read


def dense_embedder(**overrides: Any) -> SentenceTransformersDenseEmbedder:
    settings: dict[str, Any] = {
        "model": "fixture/bge",
        "revision": "0123456789abcdef",
        "dimensions": 3,
        "batch_size": 2,
        "version_reader": fake_version_reader(),
        "encoder": FakeDenseEncoder(),
    }
    settings.update(overrides)
    return SentenceTransformersDenseEmbedder(**settings)


def sparse_encoder(**overrides: Any) -> SentenceTransformersSparseEncoder:
    settings: dict[str, Any] = {
        "model": "fixture/sparse",
        "revision": "0123456789abcdef",
        "dimensions": 4,
        "batch_size": 2,
        "device": "cpu",
        "version_reader": fake_version_reader(),
        "encoder": FakeSparseEncoder(),
    }
    settings.update(overrides)
    return SentenceTransformersSparseEncoder(**settings)


def reranker(**overrides: Any) -> SentenceTransformersReranker:
    settings: dict[str, Any] = {
        "model": "fixture/reranker",
        "revision": "0123456789abcdef",
        "device": "mps",
        "batch_size": 4,
        "version_reader": fake_version_reader(),
        "encoder": FakeCrossEncoder(),
    }
    settings.update(overrides)
    return SentenceTransformersReranker(**settings)


def test_module_pins_the_baseline_models_and_package():
    assert SENTENCE_TRANSFORMERS_VERSION == "5.6.1"
    assert DEFAULT_DENSE_MODEL == "BAAI/bge-base-en-v1.5"
    assert DEFAULT_DENSE_REVISION == "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"
    assert DEFAULT_DENSE_DIMENSIONS == 768
    assert DEFAULT_SPARSE_MODEL == "tomaarsen/splade-modernbert-base-miriad"
    assert DEFAULT_SPARSE_REVISION == "c640ce28f7c4f4593ddba1b3855988f03a3d9cdc"
    assert DEFAULT_SPARSE_DIMENSIONS == 50_368
    assert DEFAULT_SPARSE_MAX_INPUT_TOKENS == 8_192
    assert DEFAULT_RERANK_MODEL == "BAAI/bge-reranker-v2-m3"
    assert DEFAULT_RERANK_REVISION == "953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"
    assert DEFAULT_RERANK_MAX_SEQ_LENGTH == 4_096


def test_dense_embedder_returns_vectors_and_call_details_together():
    encoder = FakeDenseEncoder()
    embedder = dense_embedder(encoder=encoder)

    result = embedder.embed(["alpha", "beta gamma"])

    assert isinstance(result, DenseEmbeddingResult)
    assert result.vectors == ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0))
    assert encoder.calls == [
        (
            ["alpha", "beta gamma"],
            {
                "batch_size": 2,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "normalize_embeddings": True,
            },
        )
    ]
    call = result.call
    assert call["provider"] == "sentence-transformers"
    assert call["operation"] == "dense-embedding"
    assert call["package_name"] == "sentence-transformers"
    assert call["package_version"] == SENTENCE_TRANSFORMERS_VERSION
    assert call["tokenizer_package_version"] == FIXTURE_TOKENIZER_VERSION
    assert call["encoder_source"] == "injected"
    assert call["provider_invoked"] is True
    assert call["attempt_count"] == 1
    assert call["model_id"] == "sentence-transformers:fixture/bge@0123456789abcdef"
    assert call["revision"] == "0123456789abcdef"
    assert call["tokenizer_id"] == "sentence-transformers:fixture/bge@0123456789abcdef:tokenizer"
    assert call["token_counts"] == (3, 4)
    assert call["max_input_tokens"] == 512
    assert call["inputs_over_limit"] == (False, False)
    assert call["token_audit_status"] == "exact-untruncated-model-tokenizer"
    assert call["input_count"] == 2
    assert call["status"] == "completed"
    assert call["error_type"] is None
    assert call["runtime_parameters"]["normalize_embeddings"] is True
    assert call["runtime_parameters"]["batch_size"] == 2
    assert isinstance(call["duration_ms"], float)


def test_dense_embedder_records_over_limit_inputs_without_truncating():
    embedder = dense_embedder(encoder=FakeDenseEncoder(max_seq_length=4))

    result = embedder.embed(["alpha", "beta gamma delta"])

    assert embedder.max_input_tokens == 4
    assert embedder.model_token_count("beta gamma delta") == 5
    assert result.call["token_counts"] == (3, 5)
    assert result.call["inputs_over_limit"] == (False, True)
    assert result.vectors == ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0))


def test_dense_embedder_rejects_a_limited_model_without_a_usable_tokenizer():
    encoder = FakeDenseEncoder()
    encoder.tokenizer = None
    embedder = dense_embedder(encoder=encoder)

    assert embedder.model_token_count("alpha") is None
    with pytest.raises(ValueError, match="token audit is unavailable"):
        embedder.embed(["alpha", "beta gamma"])


def test_dense_embedder_rejects_unpinned_or_mismatched_settings():
    with pytest.raises(ValueError, match="revision must be pinned"):
        dense_embedder(revision="")
    with pytest.raises(ValueError, match="invalid sentence-transformer"):
        dense_embedder(batch_size=0)
    with pytest.raises(ValueError, match="dimensions differ"):
        dense_embedder(dimensions=5)
    with pytest.raises(ValueError, match="did not report embedding dimensions"):
        dense_embedder(encoder=FakeDenseEncoder(dimensions=None))


def test_dense_embedder_returns_an_empty_success_without_calling_the_encoder():
    encoder = FakeDenseEncoder()
    result = dense_embedder(encoder=encoder).embed([])

    assert result.vectors == ()
    assert result.call["status"] == "completed_empty"
    assert result.call["provider_invoked"] is False
    assert result.call["attempt_count"] == 0
    assert result.call["input_count"] == 0
    assert result.call["token_counts"] == ()
    assert encoder.calls == []


def test_dense_embedder_rejects_a_response_that_breaks_the_declared_shape():
    with pytest.raises(RuntimeError, match="response count differs from input"):
        dense_embedder(encoder=FakeDenseEncoder(vectors=((1.0, 0.0, 0.0),))).embed(["alpha", "beta gamma"])
    with pytest.raises(RuntimeError, match="response dimensions differ"):
        dense_embedder(encoder=FakeDenseEncoder(vectors=((1.0, 0.0), (0.0, 1.0)))).embed(["alpha", "beta gamma"])


def test_sparse_encoder_uses_the_asymmetric_document_and_query_api():
    encoder = FakeSparseEncoder()
    provider = sparse_encoder(encoder=encoder)

    documents = provider.encode(["water quality", "air emissions"], task="document")
    query = provider.encode(["water quality"], task="query")

    assert isinstance(documents, SparseEncodingResult)
    assert documents.vectors == (
        SparseVector(4, (1, 3), (2.0, 1.0)),
        SparseVector(4, (0,), (3.0,)),
    )
    assert query.vectors == (SparseVector(4, (1, 3), (1.0, 1.0)),)
    assert encoder.document_calls == [
        (
            ["water quality", "air emissions"],
            {
                "batch_size": 2,
                "show_progress_bar": False,
                "convert_to_tensor": True,
                "convert_to_sparse_tensor": True,
                "save_to_cpu": True,
            },
        )
    ]
    call = documents.call
    assert call["provider"] == "sentence-transformers-sparse"
    assert call["operation"] == "sparse-encoding"
    assert call["task"] == "document"
    assert call["package_version"] == SENTENCE_TRANSFORMERS_VERSION
    assert call["tokenizer_package_version"] == FIXTURE_TOKENIZER_VERSION
    assert call["encoder_source"] == "injected"
    assert call["provider_invoked"] is True
    assert call["attempt_count"] == 1
    assert call["model_id"] == "sentence-transformers-sparse:fixture/sparse@0123456789abcdef"
    assert call["revision"] == "0123456789abcdef"
    assert call["tokenizer_id"] == "sentence-transformers-sparse:fixture/sparse@0123456789abcdef:tokenizer"
    assert call["dimensions"] == 4
    assert call["active_dimension_counts"] == (2, 1)
    assert call["token_counts"] == (4, 4)
    assert call["max_input_tokens"] == 8_192
    assert call["inputs_over_limit"] == (False, False)
    assert call["input_count"] == 2
    assert query.call["task"] == "query"


def test_sparse_encoder_rejects_unsupported_tasks_and_settings():
    provider = sparse_encoder()

    with pytest.raises(ValueError, match="unsupported sparse embedding task"):
        provider.encode(["water quality"], task="rerank")
    with pytest.raises(ValueError, match="revision must be pinned"):
        sparse_encoder(revision="")
    with pytest.raises(ValueError, match="invalid sparse"):
        sparse_encoder(dimensions=0)
    with pytest.raises(ValueError, match="dimensions differ"):
        sparse_encoder(dimensions=7)


def test_sparse_encoder_holds_the_default_model_input_limit():
    with pytest.raises(ValueError, match="default sparse model input limit differs"):
        SentenceTransformersSparseEncoder(
            dimensions=4,
            batch_size=2,
            version_reader=fake_version_reader(),
            encoder=FakeSparseEncoder(max_seq_length=512),
        )


def test_sparse_encoder_returns_an_empty_success_without_calling_the_encoder():
    encoder = FakeSparseEncoder()
    result = sparse_encoder(encoder=encoder).encode([], task="document")

    assert result.vectors == ()
    assert result.call["status"] == "completed_empty"
    assert result.call["provider_invoked"] is False
    assert result.call["attempt_count"] == 0
    assert result.call["input_count"] == 0
    assert result.call["token_counts"] == ()
    assert result.call["active_dimension_counts"] == ()
    assert encoder.document_calls == []
    assert encoder.query_calls == []


def test_sparse_encoder_rejects_a_response_that_does_not_cover_every_input():
    with pytest.raises(RuntimeError, match="SparseEncoder response count differs from input"):
        sparse_encoder(encoder=FakeSparseEncoder()).encode(["water quality"], task="document")


def test_validate_sparse_vector_rejects_malformed_vectors():
    assert validate_sparse_vector(SparseVector(4, (0, 2), (1.0, 2.0)), 4) == SparseVector(4, (0, 2), (1.0, 2.0))

    with pytest.raises(ValueError, match="dimensions differ from provider contract"):
        validate_sparse_vector(SparseVector(3, (0,), (1.0,)), 4)
    with pytest.raises(ValueError, match="dimensions differ from provider contract"):
        validate_sparse_vector(SparseVector(0, (), ()), 0)
    with pytest.raises(ValueError, match="index and value counts differ"):
        validate_sparse_vector(SparseVector(4, (0, 1), (1.0,)), 4)
    with pytest.raises(ValueError, match="not sorted and unique"):
        validate_sparse_vector(SparseVector(4, (1, 0), (1.0, 2.0)), 4)
    with pytest.raises(ValueError, match="not sorted and unique"):
        validate_sparse_vector(SparseVector(4, (1, 1), (1.0, 2.0)), 4)
    with pytest.raises(ValueError, match="index is out of range"):
        validate_sparse_vector(SparseVector(4, (7,), (1.0,)), 4)
    with pytest.raises(ValueError, match="index is out of range"):
        validate_sparse_vector(SparseVector(4, (-1,), (1.0,)), 4)
    with pytest.raises(ValueError, match="non-finite value"):
        validate_sparse_vector(SparseVector(4, (0,), (float("nan"),)), 4)


def test_reranker_scores_every_candidate_in_input_order():
    cleared = 0

    def clear_cache() -> None:
        nonlocal cleared
        cleared += 1

    encoder = FakeCrossEncoder()
    provider = reranker(encoder=encoder, cache_clearer=clear_cache)

    result = provider.rerank("water quality", ["air emissions", "water quality standard"])

    assert isinstance(result, RerankResult)
    assert result.scores == (-0.2, 0.9)
    assert encoder.calls == [
        (
            "water quality",
            ["air emissions", "water quality standard"],
            {
                "top_k": 2,
                "return_documents": False,
                "batch_size": 4,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "device": "mps",
            },
        )
    ]
    assert cleared == 1
    call = result.call
    assert call["provider"] == "sentence-transformers"
    assert call["operation"] == "rerank"
    assert call["package_version"] == SENTENCE_TRANSFORMERS_VERSION
    assert call["tokenizer_package_version"] == FIXTURE_TOKENIZER_VERSION
    assert call["encoder_source"] == "injected"
    assert call["provider_invoked"] is True
    assert call["attempt_count"] == 1
    assert call["model_id"] == "sentence-transformers:fixture/reranker@0123456789abcdef"
    assert call["revision"] == "0123456789abcdef"
    assert call["tokenizer_id"] == "huggingface:fixture/reranker@0123456789abcdef"
    assert call["candidate_count"] == 2
    assert call["token_counts"] == (6, 7)
    assert call["max_input_tokens"] == 4_096
    assert call["inputs_over_limit"] == (False, False)
    assert call["token_audit_status"] == "exact-untruncated-pair-tokenizer"
    assert call["runtime_parameters"]["clear_device_cache_after_request"] is True
    assert call["request_parameters"]["max_seq_length"] == 4_096


def test_reranker_records_over_limit_pairs_without_dropping_candidates():
    provider = reranker(encoder=FakeCrossEncoder(max_seq_length=5), max_seq_length=5)

    result = provider.rerank("water quality", ["air emissions", "water quality standard"])

    assert provider.max_seq_length == 5
    assert result.call["inputs_over_limit"] == (True, True)
    assert len(result.scores) == 2


def test_reranker_reads_an_injected_encoder_limit_and_never_writes_to_it():
    encoder = FakeCrossEncoder(max_seq_length=8_192)

    provider = reranker(encoder=encoder)
    result = provider.rerank("water quality", ["air emissions", "water quality standard"])

    assert encoder.max_seq_length == 8_192
    assert provider.max_seq_length == 8_192
    assert result.call["max_input_tokens"] == 8_192
    assert result.call["request_parameters"]["max_seq_length"] == 8_192

    encoder.max_seq_length = 2_048  # the encoder's owner, not this adapter, changes it
    later = provider.rerank("water quality", ["air emissions", "water quality standard"])

    assert later.call["max_input_tokens"] == 2_048
    assert later.call["request_parameters"]["max_seq_length"] == 2_048


def test_reranker_rejects_an_encoder_whose_limit_differs_from_the_declared_one():
    encoder = FakeCrossEncoder(max_seq_length=8_192)

    with pytest.raises(ValueError, match="input limit differs from the declared value"):
        reranker(encoder=encoder, max_seq_length=4_096)

    assert encoder.max_seq_length == 8_192


def test_rerankers_sharing_one_encoder_cannot_change_each_other_recorded_limits():
    shared = FakeCrossEncoder(max_seq_length=8_192)
    first = reranker(encoder=shared)

    with pytest.raises(ValueError, match="input limit differs from the declared value"):
        reranker(encoder=shared, max_seq_length=4_096)
    second = reranker(encoder=shared, max_seq_length=8_192)

    first_call = first.rerank("water quality", ["air emissions", "water quality standard"]).call
    second_call = second.rerank("water quality", ["air emissions", "water quality standard"]).call

    assert shared.max_seq_length == 8_192
    assert first_call["max_input_tokens"] == 8_192
    assert second_call["max_input_tokens"] == 8_192
    assert first_call["request_parameters"]["max_seq_length"] == 8_192
    assert second_call["request_parameters"]["max_seq_length"] == 8_192


def test_reranker_rejects_incomplete_duplicate_or_non_finite_scores():
    with pytest.raises(ValueError, match="did not score every candidate"):
        reranker(encoder=FakeCrossEncoder(ranked=[{"corpus_id": 0, "score": 0.5}])).rerank("q", ["a", "b"])
    with pytest.raises(ValueError, match="incomplete or duplicated"):
        reranker(
            encoder=FakeCrossEncoder(ranked=[{"corpus_id": 0, "score": 0.5}, {"corpus_id": 0, "score": 0.4}])
        ).rerank("q", ["a", "b"])
    with pytest.raises(ValueError, match="non-finite score"):
        reranker(
            encoder=FakeCrossEncoder(ranked=[{"corpus_id": 0, "score": float("inf")}, {"corpus_id": 1, "score": 0.4}])
        ).rerank("q", ["a", "b"])
    with pytest.raises(ValueError, match="results must be a list"):
        ranked_scores_in_input_order({"corpus_id": 0}, expected_count=1)
    with pytest.raises(ValueError, match="must be an object"):
        ranked_scores_in_input_order([(0, 0.5)], expected_count=1)
    with pytest.raises(ValueError, match="invalid index or score"):
        ranked_scores_in_input_order([{"corpus_id": 0}], expected_count=1)


def test_reranker_requires_a_pair_tokenizer_and_an_input_limit():
    untokenized = FakeCrossEncoder()
    untokenized.tokenizer = None
    with pytest.raises(ValueError, match="did not expose its pair tokenizer"):
        reranker(encoder=untokenized)
    with pytest.raises(ValueError, match="did not report an input limit"):
        reranker(max_seq_length=None, encoder=FakeCrossEncoder(max_seq_length=None))
    with pytest.raises(ValueError, match="revision must be pinned"):
        reranker(revision="")
    with pytest.raises(ValueError, match="batch size must be positive"):
        reranker(batch_size=0)
    with pytest.raises(ValueError, match="input limit must be positive"):
        reranker(max_seq_length=0)


def test_reranker_returns_an_empty_success_without_calling_the_encoder():
    encoder = FakeCrossEncoder()
    result = reranker(encoder=encoder).rerank("water quality", [])

    assert result.scores == ()
    assert result.call["status"] == "completed_empty"
    assert result.call["provider_invoked"] is False
    assert result.call["attempt_count"] == 0
    assert result.call["candidate_count"] == 0
    assert result.call["token_counts"] == ()
    assert encoder.calls == []


@pytest.mark.parametrize("installed", [SENTENCE_TRANSFORMERS_VERSION, "5.6.0", None])
def test_every_adapter_verifies_the_installed_sentence_transformers_version(installed: str | None):
    builders = (dense_embedder, sparse_encoder, reranker)
    reader = fake_version_reader(sentence_transformers=installed)

    if installed == SENTENCE_TRANSFORMERS_VERSION:
        for build in builders:
            assert build(version_reader=reader).package_version == SENTENCE_TRANSFORMERS_VERSION
        return

    for build in builders:
        with pytest.raises(RuntimeError, match="sentence-transformers version differs from the pinned contract"):
            build(version_reader=reader)


def test_every_adapter_reads_versions_from_the_installation_by_default():
    for adapter in (
        SentenceTransformersDenseEmbedder,
        SentenceTransformersSparseEncoder,
        SentenceTransformersReranker,
    ):
        default = inspect.signature(adapter).parameters["version_reader"].default
        assert default is installed_package_version
        assert "package_version" not in inspect.signature(adapter).parameters


def test_every_adapter_records_whether_it_loaded_its_own_encoder():
    for adapter in (dense_embedder(), sparse_encoder(), reranker()):
        assert adapter.encoder_source == "injected"

    assert dense_embedder().embed(["alpha", "beta gamma"]).call["encoder_source"] == "injected"
    assert sparse_encoder().encode(["alpha", "beta"], task="document").call["encoder_source"] == "injected"
    assert reranker().rerank("alpha", ["beta", "gamma"]).call["encoder_source"] == "injected"


def test_every_adapter_records_the_model_it_loaded_itself():
    loaded: list[tuple[str, dict[str, Any]]] = []

    def loader(factory: Callable[[], Any]) -> Callable[..., Any]:
        def build(model: str, **options: Any) -> Any:
            loaded.append((model, dict(options)))
            return factory()

        return build

    with fake_sdk(
        SentenceTransformer=loader(FakeDenseEncoder),
        SparseEncoder=loader(FakeSparseEncoder),
        CrossEncoder=loader(FakeCrossEncoder),
    ):
        dense = SentenceTransformersDenseEmbedder(
            model="fixture/bge",
            revision="0123456789abcdef",
            dimensions=3,
            batch_size=2,
            device="cpu",
            version_reader=fake_version_reader(),
        )
        sparse = SentenceTransformersSparseEncoder(
            model="fixture/sparse",
            revision="0123456789abcdef",
            dimensions=4,
            batch_size=2,
            device="cpu",
            version_reader=fake_version_reader(),
        )
        cross = SentenceTransformersReranker(
            model="fixture/reranker",
            revision="0123456789abcdef",
            device="cpu",
            batch_size=4,
            version_reader=fake_version_reader(),
        )

    assert [model for model, _ in loaded] == ["fixture/bge", "fixture/sparse", "fixture/reranker"]
    assert loaded[2][1]["max_length"] == DEFAULT_RERANK_MAX_SEQ_LENGTH
    assert dense.encoder_source == "loaded"
    assert sparse.encoder_source == "loaded"
    assert cross.encoder_source == "loaded"
    assert dense.embed(["alpha", "beta gamma"]).call["encoder_source"] == "loaded"
    assert sparse.encode(["alpha", "beta"], task="document").call["encoder_source"] == "loaded"
    assert cross.rerank("alpha", ["beta", "gamma"]).call["encoder_source"] == "loaded"
    assert cross.max_seq_length == DEFAULT_RERANK_MAX_SEQ_LENGTH


def test_public_parameter_dicts_cannot_change_a_later_call_record():
    dense = dense_embedder()
    sparse = sparse_encoder()
    cross = reranker()

    dense.runtime_parameters["batch_size"] = 999
    dense.runtime_parameters["injected_key"] = "tampered"
    sparse.runtime_parameters["batch_size"] = 999
    cross.runtime_parameters["batch_size"] = 999
    cross.request_parameters["max_seq_length"] = 17

    dense_call = dense.embed(["alpha", "beta gamma"]).call
    sparse_call = sparse.encode(["alpha", "beta"], task="document").call
    rerank_call = cross.rerank("alpha", ["beta", "gamma"]).call

    assert dense_call["runtime_parameters"] == {
        "batch_size": 2,
        "device": "cpu",
        "normalize_embeddings": True,
        "trust_remote_code": False,
    }
    assert sparse_call["runtime_parameters"]["batch_size"] == 2
    assert rerank_call["runtime_parameters"]["batch_size"] == 4
    assert rerank_call["request_parameters"] == {"max_seq_length": DEFAULT_RERANK_MAX_SEQ_LENGTH}


def test_injected_encoders_never_import_the_sentence_transformers_sdk():
    with sdk_unavailable():
        assert "sentence_transformers" not in sys.modules
        dense = dense_embedder()
        sparse = sparse_encoder()
        cross = reranker()
        dense_result = dense.embed(["alpha", "beta gamma"])
        sparse_result = sparse.encode(["water quality", "air emissions"], task="document")
        rerank_result = cross.rerank("water quality", ["air emissions", "water quality standard"])
        assert "sentence_transformers" not in sys.modules

    assert len(dense_result.vectors) == 2
    assert len(sparse_result.vectors) == 2
    assert len(rerank_result.scores) == 2


def test_adapters_keep_no_mutable_last_call_state():
    adapters: tuple[Any, ...] = (dense_embedder(), sparse_encoder(), reranker())
    for adapter in adapters:
        assert not hasattr(adapter, "last_call_metadata")
        assert [name for name in vars(adapter) if "last_call" in name] == []

    dense = adapters[0]
    first = dense.embed(["alpha", "beta gamma"])
    first.call["provider"] = "tampered"
    first.call["runtime_parameters"]["batch_size"] = 999
    second = dense.embed(["alpha", "beta gamma"])

    assert second.call["provider"] == "sentence-transformers"
    assert second.call["runtime_parameters"]["batch_size"] == 2
    assert dense.runtime_parameters["batch_size"] == 2
    assert first.call is not second.call

    with pytest.raises((AttributeError, TypeError)):
        first.vectors = ()  # type: ignore[misc]

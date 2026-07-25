"""Contract tests for learned-sparse and dense+sparse retrieval."""

from __future__ import annotations

from collections import UserDict
from collections.abc import Sequence
from pathlib import Path

import pytest

from spicy_regs.corpora.document_acceptance_scope import (
    build_document_acceptance_scope,
)
from spicy_regs.corpora.segmentation_evaluation import (
    build_segmentation_evaluation,
    fetch_source_cache,
)
from spicy_regs.corpora.segmentation_experiment import (
    HashEmbeddingProvider,
    HeuristicBoundarySelector,
    build_segmentation_experiment,
)
from spicy_regs.corpora.segmentation_sparse_retrieval import (
    DeterministicSparseProvider,
    SentenceTransformersSparseProvider,
    SparseEncodingResult,
    SparseVector,
    build_sparse_retrieval_comparison,
    sparse_retrieval_preflight,
    validate_sparse_retrieval_comparison,
)
from spicy_regs.ontology.common import read_parquet_rows
from tests.test_segmentation_evaluation import (
    _fake_fetch,
    _write_base,
    _write_corpus,
)


@pytest.fixture(scope="module")
def base_experiment(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("sparse-retrieval-base")
    base = root / "base"
    corpus = root / "corpus"
    cache = root / "cache"
    evaluation = root / "evaluation"
    experiment = root / "experiment"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    build_segmentation_experiment(
        evaluation,
        experiment,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(800,),
    )
    return evaluation, experiment


def test_sentence_transformers_sparse_adapter_uses_asymmetric_api():
    import torch

    class FakeTokenizer:
        def __call__(self, text, **options):
            assert text == "water quality"
            assert options == {
                "add_special_tokens": True,
                "truncation": False,
                "return_attention_mask": False,
                "return_token_type_ids": False,
            }
            return UserDict({"input_ids": [1, 2, 3]})

    class FakeSparseEncoder:
        max_seq_length = 8_192
        tokenizer = FakeTokenizer()

        def get_embedding_dimension(self):
            return 4

        def encode_document(self, texts, **options):
            assert texts == ["water quality", "air emissions"]
            assert options == {
                "batch_size": 2,
                "show_progress_bar": False,
                "convert_to_tensor": True,
                "convert_to_sparse_tensor": True,
                "save_to_cpu": True,
            }
            return torch.tensor(
                [
                    [0.0, 2.0, 0.0, 1.0],
                    [3.0, 0.0, 0.0, 0.0],
                ]
            )

        def encode_query(self, texts, **options):
            assert texts == ["water quality"]
            assert options["batch_size"] == 2
            return torch.tensor([[0.0, 1.0, 0.0, 1.0]])

    provider = SentenceTransformersSparseProvider(
        model="fixture/sparse",
        revision="0123456789abcdef",
        dimensions=4,
        batch_size=2,
        device="cpu",
        encoder=FakeSparseEncoder(),
    )

    documents = provider.encode(
        ["water quality", "air emissions"],
        task="document",
    )
    query = provider.encode(["water quality"], task="query")

    assert provider.model_id == ("sentence-transformers-sparse:fixture/sparse@0123456789abcdef")
    assert provider.model_token_count("water quality") == 3
    assert documents.vectors == (
        SparseVector(4, (1, 3), (2.0, 1.0)),
        SparseVector(4, (0,), (3.0,)),
    )
    assert query.vectors == (SparseVector(4, (1, 3), (1.0, 1.0)),)


def test_sparse_comparison_is_complete_and_byte_deterministic(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    first = tmp_path / "sparse-one"
    second = tmp_path / "sparse-two"
    preflight = sparse_retrieval_preflight(evaluation, experiment)

    first_receipt = build_sparse_retrieval_comparison(
        evaluation,
        experiment,
        first,
        provider=DeterministicSparseProvider(),
        checkpoint_batch_size=2,
    )
    second_receipt = build_sparse_retrieval_comparison(
        evaluation,
        experiment,
        second,
        provider=DeterministicSparseProvider(),
        checkpoint_batch_size=2,
    )

    assert first_receipt["status"] == "pass"
    assert first_receipt["production_provider"] is False
    assert first_receipt["embedding_row_count"] == (
        preflight["unique_document_input_count"] + preflight["unique_query_input_count"]
    )
    assert first_receipt["metric_row_count"] == 5 * 2 * 2
    assert first_receipt == second_receipt
    assert (
        validate_sparse_retrieval_comparison(
            evaluation,
            experiment,
            first,
        )
        == first_receipt
    )
    assert {path.relative_to(first): path.read_bytes() for path in first.rglob("*") if path.is_file()} == {
        path.relative_to(second): path.read_bytes() for path in second.rglob("*") if path.is_file()
    }


def test_sparse_comparison_can_limit_work_to_selected_config(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "sparse-selected-config"
    config_ids = ("structure-overlap-800",)
    preflight = sparse_retrieval_preflight(
        evaluation,
        experiment,
        config_ids=config_ids,
    )

    receipt = build_sparse_retrieval_comparison(
        evaluation,
        experiment,
        output,
        provider=DeterministicSparseProvider(),
        config_ids=config_ids,
    )

    rows = read_parquet_rows(output / "retrieval_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["config_ids"] == list(config_ids)
    assert preflight["config_ids"] == list(config_ids)
    assert receipt["metric_row_count"] == 1 * 2 * 2
    assert {str(row["config_id"]) for row in rows} == set(config_ids)
    assert (
        validate_sparse_retrieval_comparison(
            evaluation,
            experiment,
            output,
        )
        == receipt
    )


def test_sparse_comparison_preserves_document_scope(
    tmp_path: Path,
):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    scope_dir = tmp_path / "document-scope"
    experiment = tmp_path / "experiment"
    output = tmp_path / "sparse"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    scope_receipt = build_document_acceptance_scope(evaluation, scope_dir)
    build_segmentation_experiment(
        evaluation,
        experiment,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(800,),
        scope_dir=scope_dir,
    )

    receipt = build_sparse_retrieval_comparison(
        evaluation,
        experiment,
        output,
        provider=DeterministicSparseProvider(),
        scope_dir=scope_dir,
    )

    rows = read_parquet_rows(output / "retrieval_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["document_scope_id"] == scope_receipt["scope_id"]
    assert rows
    assert all(str(row["query_subject_type"]) != "comment" for row in rows)
    assert (
        validate_sparse_retrieval_comparison(
            evaluation,
            experiment,
            output,
            scope_dir=scope_dir,
        )
        == receipt
    )


def test_sparse_failure_is_durable_and_resume_skips_successes(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "resumed-sparse"

    class InterruptOnceProvider:
        production_provider = False
        provider = "fixture-sparse"
        package_name = "fixture-package"
        package_version = "1.0"
        model_id = "fixture:sparse@0123456789abcdef"
        revision: str | None = "0123456789abcdef"
        dimensions = 2_048
        tokenizer_id = "fixture-tokenizer"
        max_input_tokens: int | None = 8_192
        batch_size = 1

        def __init__(self):
            self.calls = 0
            self.interrupted = False
            self.delegate = DeterministicSparseProvider()

        def model_token_count(self, text: str) -> int:
            return len(text.split())

        def encode(
            self,
            texts: Sequence[str],
            *,
            task: str,
        ) -> SparseEncodingResult:
            self.calls += 1
            if self.calls == 3 and not self.interrupted:
                self.interrupted = True
                raise TimeoutError("fixture interruption")
            result = self.delegate.encode(texts, task=task)
            return SparseEncodingResult(
                vectors=result.vectors,
                call={
                    **result.call,
                    "provider": self.provider,
                    "package_name": self.package_name,
                    "package_version": self.package_version,
                    "model_id": self.model_id,
                },
            )

    provider = InterruptOnceProvider()
    with pytest.raises(RuntimeError, match="checkpoint is resumable"):
        build_sparse_retrieval_comparison(
            evaluation,
            experiment,
            output,
            provider=provider,
            checkpoint_batch_size=1,
        )
    first_call_count = provider.calls

    receipt = build_sparse_retrieval_comparison(
        evaluation,
        experiment,
        output,
        provider=provider,
        checkpoint_batch_size=1,
    )

    assert receipt["status"] == "pass"
    assert receipt["provider_failed_transition_count"] == 1
    assert provider.calls > first_call_count
    assert not (output.parent / f".{output.name}.sparse-work").exists()

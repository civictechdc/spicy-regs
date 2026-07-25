"""Contract tests for packaged candidate reranking and its audit ledger."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import httpx
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
from spicy_regs.corpora.segmentation_rerank import (
    DeterministicReranker,
    OMLXReranker,
    RerankInputAudit,
    RerankResult,
    SentenceTransformersReranker,
    build_rerank_experiment,
    rerank_preflight,
    validate_rerank_experiment,
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
    root = tmp_path_factory.mktemp("rerank-base")
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
    )
    return evaluation, experiment


def test_sentence_transformers_adapter_uses_cross_encoder_rank():
    cache_clear_count = 0

    def clear_cache():
        nonlocal cache_clear_count
        cache_clear_count += 1

    class FakeTokenizer:
        def __call__(self, queries, documents, **options):
            assert queries == ["water quality", "water quality"]
            assert documents == ["air emissions", "water quality standard"]
            assert options == {
                "add_special_tokens": True,
                "padding": False,
                "truncation": False,
                "return_attention_mask": False,
                "return_token_type_ids": False,
            }
            return {
                "input_ids": [
                    list(range(5_000)),
                    list(range(7)),
                ]
            }

    class FakeCrossEncoder:
        max_seq_length = 8_192
        tokenizer = FakeTokenizer()

        def rank(self, query, documents, **options):
            assert query == "water quality"
            assert documents == ["air emissions", "water quality standard"]
            assert options == {
                "top_k": 2,
                "return_documents": False,
                "batch_size": 4,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "device": "mps",
            }
            return [
                {"corpus_id": 1, "score": 0.9},
                {"corpus_id": 0, "score": -0.2},
            ]

    reranker = SentenceTransformersReranker(
        model="fixture/reranker",
        revision="0123456789abcdef",
        device="mps",
        batch_size=4,
        max_seq_length=4_096,
        encoder=FakeCrossEncoder(),
        cache_clearer=clear_cache,
    )

    result = reranker.rerank(
        "water quality",
        ["air emissions", "water quality standard"],
    )
    input_audit = reranker.audit_inputs(
        "water quality",
        ["air emissions", "water quality standard"],
    )

    assert reranker.model_id == ("sentence-transformers:fixture/reranker@0123456789abcdef")
    assert reranker.request_parameters["max_seq_length"] == 4_096
    assert reranker.runtime_parameters["clear_device_cache_after_request"] is True
    assert result.scores == (-0.2, 0.9)
    assert result.telemetry["retry_count"] == 0
    assert input_audit.untruncated_token_counts == (5_000, 7)
    assert input_audit.input_limit == 4_096
    assert input_audit.status == "exact-untruncated-pair-tokenizer"
    assert cache_clear_count == 1


def test_omlx_adapter_uses_published_rerank_contract():
    class FakeTokenizer:
        def apply_chat_template(
            self,
            messages,
            *,
            tokenize,
            add_generation_prompt,
        ):
            assert tokenize is False
            assert add_generation_prompt is True
            assert messages[0]["role"] == "system"
            return f"<prefix>{messages[1]['content']}<suffix>"

        def encode(self, text, *, add_special_tokens):
            assert add_special_tokens is False
            return list(range(len(text.split())))

        def __call__(self, texts, **options):
            assert options == {
                "padding": False,
                "truncation": False,
                "add_special_tokens": False,
                "return_attention_mask": False,
                "return_token_type_ids": False,
            }
            return {"input_ids": [list(range(len(text.split()))) for text in texts]}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "http://127.0.0.1:8012/v1/rerank"
        assert request.headers["Authorization"] == "Bearer local-token"
        payload = json.loads(request.content)
        assert payload == {
            "model": "fixture-reranker",
            "query": "water",
            "documents": ["air", "water"],
            "top_n": 2,
            "return_documents": False,
        }
        return httpx.Response(
            200,
            json={
                "id": "rerank-fixture",
                "model": "fixture-reranker",
                "results": [
                    {"index": 1, "relevance_score": 0.98},
                    {"index": 0, "relevance_score": 0.01},
                ],
                "usage": {"total_tokens": 12},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        reranker = OMLXReranker(
            model="fixture/repository",
            revision="0123456789abcdef",
            service_model="fixture-reranker",
            api_key="local-token",
            client=client,
            audit_tokenizer=FakeTokenizer(),
        )
        result = reranker.rerank("water", ["air", "water"])
        input_audit = reranker.audit_inputs(
            "water",
            ["air", "water"],
        )

    assert result.scores == (0.01, 0.98)
    assert result.telemetry == {
        "duration_ms": result.telemetry["duration_ms"],
        "retry_count": 0,
        "total_tokens": 12,
        "response_id": "rerank-fixture",
        "status_code": 200,
    }
    assert input_audit.untruncated_token_counts == (20, 20)
    assert input_audit.input_limit == 8_192
    assert input_audit.tokenizer_id == ("huggingface:fixture/repository@0123456789abcdef")
    assert input_audit.status == ("exact-untruncated-omlx-0.5.3-causal-rerank-template")


def test_omlx_default_service_model_matches_local_directory_id():
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request))) as client:
        reranker = OMLXReranker(client=client)

    assert reranker.service_model == ("mlx-community--Qwen3-Reranker-4B-mxfp8")
    assert reranker.request_parameters["service_default_max_seq_length"] == 8_192
    assert reranker.request_parameters["service_max_length_request_supported"] is False
    assert reranker.model_id.startswith("omlx:mlx-community/Qwen3-Reranker-4B-mxfp8@")


def test_rerank_artifact_is_complete_and_byte_deterministic(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    first = tmp_path / "rerank-one"
    second = tmp_path / "rerank-two"
    preflight = rerank_preflight(evaluation, experiment)

    first_receipt = build_rerank_experiment(
        evaluation,
        experiment,
        first,
        reranker=DeterministicReranker(),
    )
    second_receipt = build_rerank_experiment(
        evaluation,
        experiment,
        second,
        reranker=DeterministicReranker(),
    )

    assert first_receipt["status"] == "pass"
    assert first_receipt["production_provider"] is False
    assert first_receipt["candidate_count"] == preflight["candidate_count"]
    assert first_receipt["truncated_candidate_count"] == 0
    assert first_receipt["unaudited_candidate_count"] == preflight["candidate_count"]
    assert first_receipt["request_group_count"] == preflight["request_count"]
    assert first_receipt["request_transition_count"] == preflight["request_count"]
    assert first_receipt["metric_row_count"] == 15 * 2 * 2 * 4
    assert first_receipt == second_receipt
    assert (
        validate_rerank_experiment(
            evaluation,
            experiment,
            first,
        )
        == first_receipt
    )
    assert {path.relative_to(first): path.read_bytes() for path in first.rglob("*") if path.is_file()} == {
        path.relative_to(second): path.read_bytes() for path in second.rglob("*") if path.is_file()
    }


def test_rerank_can_score_one_fixed_paired_depth(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "rerank-top-50"
    preflight = rerank_preflight(
        evaluation,
        experiment,
        rerank_depths=(50,),
    )

    receipt = build_rerank_experiment(
        evaluation,
        experiment,
        output,
        reranker=DeterministicReranker(),
        rerank_depths=(50,),
    )

    manifest = json.loads((output / "segmentation-rerank-manifest.json").read_text())
    rows = read_parquet_rows(output / "reranked_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["candidate_count"] == preflight["candidate_count"]
    assert preflight["source_candidate_count"] >= preflight["candidate_count"]
    assert manifest["rerank_depths"] == [50]
    assert manifest["rerank_candidate_depth"] == 50
    assert receipt["metric_row_count"] == 15 * 2 * 2
    assert max(int(str(row["candidate_rank"])) for row in rows) <= 50
    assert (
        validate_rerank_experiment(
            evaluation,
            experiment,
            output,
        )
        == receipt
    )


def test_rerank_can_limit_work_to_selected_config(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "rerank-selected-config"
    config_ids = ("structure-overlap-800",)
    preflight = rerank_preflight(
        evaluation,
        experiment,
        rerank_depths=(50,),
        config_ids=config_ids,
    )

    receipt = build_rerank_experiment(
        evaluation,
        experiment,
        output,
        reranker=DeterministicReranker(),
        rerank_depths=(50,),
        config_ids=config_ids,
    )

    manifest = json.loads((output / "segmentation-rerank-manifest.json").read_text())
    rows = read_parquet_rows(output / "reranked_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["config_ids"] == list(config_ids)
    assert preflight["config_ids"] == list(config_ids)
    assert manifest["config_ids"] == list(config_ids)
    assert manifest["config_count"] == 1
    assert preflight["source_candidate_count"] < preflight["upstream_candidate_count"]
    assert {str(row["config_id"]) for row in rows} == set(config_ids)
    assert receipt["metric_row_count"] == 1 * 2 * 2
    assert (
        validate_rerank_experiment(
            evaluation,
            experiment,
            output,
        )
        == receipt
    )


def test_audited_rerank_persists_and_validates_truncation_booleans(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "audited-rerank"

    class AuditedReranker(DeterministicReranker):
        model_id = "fixture:audited-reranker-v1"
        production_provider = True

        def audit_inputs(
            self,
            query: str,
            documents: Sequence[str],
        ) -> RerankInputAudit:
            del query
            return RerankInputAudit(
                tokenizer_id="fixture:pair-tokenizer-v1",
                untruncated_token_counts=tuple(11 if index % 2 else 9 for index, _ in enumerate(documents)),
                input_limit=10,
                status="exact-fixture-pair-tokenizer",
            )

    receipt = build_rerank_experiment(
        evaluation,
        experiment,
        output,
        reranker=AuditedReranker(),
    )

    rows = read_parquet_rows(output / "reranked_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["production_provider"] is True
    assert receipt["unaudited_candidate_count"] == 0
    assert 0 < receipt["truncated_candidate_count"] < len(rows)
    assert {row["rerank_would_truncate"] for row in rows} == {
        "False",
        "True",
    }


def test_rerank_preserves_document_scope_and_excludes_comment_queries(
    tmp_path: Path,
):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    scope_dir = tmp_path / "document-scope"
    experiment = tmp_path / "experiment"
    output = tmp_path / "rerank"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    scope_receipt = build_document_acceptance_scope(
        evaluation,
        scope_dir,
    )
    build_segmentation_experiment(
        evaluation,
        experiment,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(800,),
        scope_dir=scope_dir,
    )

    preflight = rerank_preflight(
        evaluation,
        experiment,
        scope_dir=scope_dir,
    )
    receipt = build_rerank_experiment(
        evaluation,
        experiment,
        output,
        reranker=DeterministicReranker(),
        scope_dir=scope_dir,
    )

    rows = read_parquet_rows(output / "reranked_candidates.parquet")
    assert receipt["status"] == "pass"
    assert receipt["document_scope_id"] == scope_receipt["scope_id"]
    assert preflight["document_scope_id"] == scope_receipt["scope_id"]
    assert rows
    assert all(str(row["query_subject_type"]) != "comment" for row in rows)
    assert (
        validate_rerank_experiment(
            evaluation,
            experiment,
            output,
            scope_dir=scope_dir,
        )
        == receipt
    )


def test_rerank_failure_is_durable_and_resume_does_not_repeat_successes(
    base_experiment: tuple[Path, Path],
    tmp_path: Path,
):
    evaluation, experiment = base_experiment
    output = tmp_path / "resumed-rerank"
    expected_requests = rerank_preflight(
        evaluation,
        experiment,
    )["request_count"]

    class InterruptOnceReranker:
        provider = "fixture"
        package_name = "fixture-package"
        package_version = "1.0"
        model_id = "fixture:interrupt-once@0123456789abcdef"
        production_provider = False
        request_parameters = {"policy": "fixture-v1"}
        runtime_parameters = {"batch_size": 1}

        def __init__(self) -> None:
            self.calls = 0
            self.interrupted = False
            self.delegate = DeterministicReranker()

        def rerank(
            self,
            query: str,
            documents: Sequence[str],
        ) -> RerankResult:
            self.calls += 1
            if self.calls == 4 and not self.interrupted:
                self.interrupted = True
                raise TimeoutError("fixture interruption")
            return self.delegate.rerank(query, documents)

        def audit_inputs(
            self,
            query: str,
            documents: Sequence[str],
        ) -> RerankInputAudit:
            return self.delegate.audit_inputs(query, documents)

    reranker = InterruptOnceReranker()
    with pytest.raises(RuntimeError, match="checkpoint is resumable"):
        build_rerank_experiment(
            evaluation,
            experiment,
            output,
            reranker=reranker,
        )

    work_dir = output.parent / f".{output.name}.rerank-work"
    assert work_dir.exists()
    assert not output.exists()

    receipt = build_rerank_experiment(
        evaluation,
        experiment,
        output,
        reranker=reranker,
    )

    assert receipt["status"] == "pass"
    assert receipt["failed_transition_count"] == 1
    assert receipt["request_transition_count"] == expected_requests + 1
    assert reranker.calls == expected_requests + 1
    assert not work_dir.exists()

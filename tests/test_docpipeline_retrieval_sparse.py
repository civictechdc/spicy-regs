"""Learned-sparse retrieval and reciprocal-rank fusion contracts."""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest
from scipy.sparse import csr_matrix

from spicy_regs.docpipeline.adapters.sentence_transformers import (
    SparseEncodingResult,
    SparseVector,
)
from spicy_regs.docpipeline.retrieval import (
    RETRIEVAL_FUSION_INPUT_DEPTH,
    RETRIEVAL_RRF_K,
    SPARSE_EMBEDDING_COLUMNS,
    SPARSE_EMBEDDING_TABLE,
    SPARSE_MODEL_ID,
    RetrievalHit,
    RetrievalQuery,
    SparseEmbeddingRow,
    SparseProviderError,
    fuse_rrf,
    rank_sparse_vectors,
    rebuild_sparse_segment_hits,
    sparse_csr_matrix,
    sparse_retrieval_facts,
    sparse_segment_search,
)
from spicy_regs.docpipeline.runtime import PlanError


class FakeSparseEncoder:
    provider = "sentence-transformers-sparse"
    model_id = SPARSE_MODEL_ID
    dimensions = 4
    tokenizer_id = f"{SPARSE_MODEL_ID}:tokenizer"
    max_input_tokens: int | None = 4
    production_provider = False

    def __init__(
        self,
        vectors: dict[tuple[str, str], SparseVector] | None = None,
        *,
        fail_task: str | None = None,
    ) -> None:
        self.vectors = vectors or {}
        self.fail_task = fail_task
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    @staticmethod
    def model_token_count(text: str) -> int:
        return len(text.split())

    def encode(self, texts: Sequence[str], *, task: str) -> SparseEncodingResult:
        requested = tuple(texts)
        self.calls.append((task, requested))
        if task == self.fail_task:
            raise RuntimeError("provider fixture failed")
        vectors = tuple(
            self.vectors.get((task, text), SparseVector(self.dimensions, (0,), (1.0,))) for text in requested
        )
        counts = tuple(self.model_token_count(text) for text in requested)
        limit = self.max_input_tokens
        return SparseEncodingResult(
            vectors=vectors,
            call={
                "provider": self.provider,
                "operation": "sparse-encoding",
                "task": task,
                "package_name": "sentence-transformers",
                "package_version": "5.6.1",
                "encoder_source": "injected",
                "model_id": self.model_id,
                "model": "tomaarsen/splade-modernbert-base-miriad",
                "revision": self.model_id.rsplit("@", 1)[1],
                "dimensions": self.dimensions,
                "active_dimension_counts": tuple(len(vector.indices) for vector in vectors),
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": "fixture",
                "token_counts": counts,
                "max_input_tokens": limit,
                "inputs_over_limit": tuple(count > limit if limit is not None else None for count in counts),
                "token_audit_status": "exact-untruncated-model-tokenizer",
                "input_count": len(requested),
                "status": "completed" if requested else "completed_empty",
                "provider_invoked": bool(requested),
                "attempt_count": 1 if requested else 0,
                "retry_count": 0,
                "duration_ms": 0.0,
                "error_type": None,
                "runtime_parameters": {
                    "batch_size": 2,
                    "device": "fixture",
                    "similarity_fn_name": "dot",
                    "trust_remote_code": False,
                },
            },
        )


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _segment_candidate(target_id: str, text: str) -> dict[str, Any]:
    return {
        "target_id": target_id,
        "artifact_id": f"artifact-{target_id}",
        "segment_id": target_id,
        "source_table": "dockets",
        "subject_id": f"subject-{target_id}",
        "artifact_digest": f"digest-{target_id}",
        "profile_id": "regulations-docket-v2",
        "subject_type": "docket",
        "access_scope": "public",
        "access_basis": "us-federal-public-record",
        "text": text,
        "text_sha256": _digest(text),
        "slices_json": "[]",
    }


def _hit(
    target_id: str,
    *,
    method: str,
    rank: int,
    score: float,
    query_id: str = "q1",
) -> RetrievalHit:
    return RetrievalHit(
        work_id="work",
        query_id=query_id,
        level="segment",
        method=method,
        target_id=target_id,
        artifact_id=f"artifact-{target_id}",
        segment_id=target_id,
        source_table="dockets",
        subject_id=f"subject-{target_id}",
        artifact_digest=f"digest-{target_id}",
        rank=rank,
        candidate_universe_size=500,
        candidate_input_size=500,
        candidate_limit=200,
        score=score,
        score_kind="cosine" if method == "dense" else "sparse-dot",
        dense_rank=rank if method == "dense" else None,
        dense_score=score if method == "dense" else None,
        sparse_rank=rank if method == "sparse" else None,
        sparse_score=score if method == "sparse" else None,
        model_id="dense@revision" if method == "dense" else SPARSE_MODEL_ID,
        model_revision="revision",
    )


def test_sparse_csr_is_float64_sorted_canonical_and_raw_dot_is_not_normalized() -> None:
    documents = (
        SparseVector(4, (0, 2), (3.0, 4.0)),
        SparseVector(4, (0,), (1.0,)),
    )
    query = SparseVector(4, (0, 2), (2.0, 0.5))

    matrix = sparse_csr_matrix(documents, dimensions=4)
    ranked = rank_sparse_vectors(("b", "a"), documents, query, limit=2)

    assert isinstance(matrix, csr_matrix)
    assert matrix.dtype == np.dtype(np.float64)
    assert matrix.has_sorted_indices
    assert matrix.has_canonical_format
    assert ranked == (("b", 8.0), ("a", 2.0))


@pytest.mark.parametrize(
    "vector",
    [
        SparseVector(4, (2, 1), (1.0, 2.0)),
        SparseVector(4, (1, 1), (1.0, 2.0)),
        SparseVector(4, (4,), (1.0,)),
        SparseVector(3, (1,), (1.0,)),
        SparseVector(4, (1,), (float("nan"),)),
    ],
)
def test_sparse_ranking_rejects_invalid_unsorted_duplicate_or_wrong_shape_vectors(
    vector: SparseVector,
) -> None:
    with pytest.raises(ValueError):
        rank_sparse_vectors(("target",), (vector,), SparseVector(4, (1,), (1.0,)))


def test_sparse_ranking_ties_break_by_target_id() -> None:
    vector = SparseVector(4, (1,), (2.0,))
    assert rank_sparse_vectors(
        ("z", "a"),
        (vector, vector),
        SparseVector(4, (1,), (3.0,)),
        limit=2,
    ) == (("a", 6.0), ("z", 6.0))


def test_sparse_search_uses_document_query_asymmetry_and_persists_exact_facts(
    tmp_path: Path,
) -> None:
    candidates = (
        _segment_candidate("s2", "air emissions"),
        _segment_candidate("s1", "water quality"),
    )
    encoder = FakeSparseEncoder(
        {
            ("document", "air emissions"): SparseVector(4, (0,), (1.0,)),
            ("document", "water quality"): SparseVector(4, (1, 3), (2.0, 1.0)),
            ("query", "clean water"): SparseVector(4, (1, 3), (1.0, 1.0)),
        }
    )

    outcome = sparse_segment_search(
        candidates,
        query=RetrievalQuery("q1", "clean water", "segment"),
        work_id="sparse-work",
        encoder=encoder,
        run_directory=tmp_path,
        candidate_universe_size=10,
    )

    assert encoder.calls == [
        ("document", ("water quality", "air emissions")),
        ("query", ("clean water",)),
    ]
    assert [hit.target_id for hit in outcome.hits] == ["s1", "s2"]
    assert [hit.score for hit in outcome.hits] == [3.0, 0.0]
    assert all(hit.method == "sparse" and hit.score_kind == "sparse-dot" for hit in outcome.hits)
    assert {row.task for row in outcome.embeddings} == {"document", "query"}
    assert all(isinstance(row, SparseEmbeddingRow) for row in outcome.embeddings)
    document = next(row for row in outcome.embeddings if row.target_id == "s1")
    query = next(row for row in outcome.embeddings if row.query_id == "q1")
    assert document.task == document.input_kind == "document"
    assert query.task == query.input_kind == "query"
    assert document.vector == SparseVector(4, (1, 3), (2.0, 1.0))
    assert document.model_id == SPARSE_MODEL_ID
    assert document.untruncated_token_count == 2
    assert document.input_limit == 4
    assert document.would_truncate is False
    assert json.loads(document.call_json)["task"] == "document"
    assert json.loads(query.call_json)["task"] == "query"
    facts = sparse_retrieval_facts(outcome)
    assert facts["state"] == "completed"
    assert facts["hit_count"] == 2
    assert facts["embedding_row_count"] == 3
    assert facts["work_ids"] == ["sparse-work"]
    assert facts["query_ids"] == ["q1"]
    assert facts["tasks"] == ["document", "query"]
    assert facts["model_ids"] == [SPARSE_MODEL_ID]
    assert facts["dimensions"] == [4]
    assert facts["candidate_limit"] == facts["fusion_input_depth"] == 200
    assert facts["rrf_k"] == 60
    assert facts["normalization"] == "none-raw-float64-v1"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(document, "task", "query")
    with pytest.raises(ValueError, match="model"):
        dataclasses.replace(document, model_revision="drift")
    with pytest.raises(ValueError, match="sorted"):
        dataclasses.replace(document, indices_json="[3,1]")


def test_sparse_empty_work_writes_a_typed_zero_row_table_without_provider_call(
    tmp_path: Path,
) -> None:
    encoder = FakeSparseEncoder(fail_task="document")

    outcome = sparse_segment_search(
        (),
        query=RetrievalQuery("q1", "query", "segment"),
        work_id="empty-work",
        encoder=encoder,
        run_directory=tmp_path,
    )

    table = pq.read_table(tmp_path / SPARSE_EMBEDDING_TABLE)
    assert outcome.state == "completed_empty"
    assert outcome.hits == outcome.embeddings == ()
    assert encoder.calls == []
    assert table.num_rows == 0
    assert table.schema.names == [name for name, _ in SPARSE_EMBEDDING_COLUMNS]
    assert (
        rebuild_sparse_segment_hits(
            (),
            query=RetrievalQuery("q1", "query", "segment"),
            work_id="empty-work",
            run_directory=tmp_path,
        )
        == ()
    )


def test_sparse_provider_failure_is_classified_and_writes_no_partial_rows(
    tmp_path: Path,
) -> None:
    encoder = FakeSparseEncoder(fail_task="query")

    with pytest.raises(SparseProviderError, match="query.*RuntimeError"):
        sparse_segment_search(
            (_segment_candidate("s1", "document"),),
            query=RetrievalQuery("q1", "query", "segment"),
            work_id="failed-work",
            encoder=encoder,
            run_directory=tmp_path,
        )

    assert [task for task, _ in encoder.calls] == ["document", "query"]
    assert not (tmp_path / SPARSE_EMBEDDING_TABLE).exists()


def test_sparse_provider_wrong_vector_shape_is_classified_before_persistence(
    tmp_path: Path,
) -> None:
    class WrongShapeEncoder(FakeSparseEncoder):
        def encode(self, texts: Sequence[str], *, task: str) -> SparseEncodingResult:
            result = super().encode(texts, task=task)
            if task != "document":
                return result
            return SparseEncodingResult(
                vectors=(SparseVector(3, (0,), (1.0,)),),
                call=result.call,
            )

    with pytest.raises(SparseProviderError, match="document.*ValueError"):
        sparse_segment_search(
            (_segment_candidate("s1", "document"),),
            query=RetrievalQuery("q1", "query", "segment"),
            work_id="wrong-shape",
            encoder=WrongShapeEncoder(),
            run_directory=tmp_path,
        )

    assert not (tmp_path / SPARSE_EMBEDDING_TABLE).exists()


def test_sparse_resume_and_rebuild_use_stored_vectors_without_a_provider(
    tmp_path: Path,
) -> None:
    candidates = (_segment_candidate("s1", "document"),)
    query = RetrievalQuery("q1", "query", "segment")
    first_encoder = FakeSparseEncoder(
        {
            ("document", "document"): SparseVector(4, (0,), (2.0,)),
            ("query", "query"): SparseVector(4, (0,), (3.0,)),
        }
    )
    first = sparse_segment_search(
        candidates,
        query=query,
        work_id="resume-work",
        encoder=first_encoder,
        run_directory=tmp_path,
    )
    refusing_encoder = FakeSparseEncoder(fail_task="document")

    resumed = sparse_segment_search(
        candidates,
        query=query,
        work_id="resume-work",
        encoder=refusing_encoder,
        run_directory=tmp_path,
    )
    rebuilt = rebuild_sparse_segment_hits(
        candidates,
        query=query,
        work_id="resume-work",
        run_directory=tmp_path,
    )

    assert first.hits == resumed.hits == rebuilt
    assert [task for task, _ in first_encoder.calls] == ["document", "query"]
    assert refusing_encoder.calls == []


def test_sparse_rebuild_rejects_missing_or_drifted_stored_inputs(tmp_path: Path) -> None:
    with pytest.raises(PlanError, match="stored vectors"):
        rebuild_sparse_segment_hits(
            (_segment_candidate("s1", "document"),),
            query=RetrievalQuery("q1", "query", "segment"),
            work_id="missing-work",
            run_directory=tmp_path,
        )
    with pytest.raises(PlanError, match="segment query"):
        sparse_segment_search(
            (_segment_candidate("s1", "document"),),
            query=RetrievalQuery("q1", "query", "artifact"),
            work_id="wrong-level",
            encoder=FakeSparseEncoder(),
            run_directory=tmp_path,
        )


def test_rrf_uses_k_60_union_absent_zero_and_deterministic_ties() -> None:
    dense = (
        _hit("shared", method="dense", rank=1, score=0.9),
        _hit("dense-only", method="dense", rank=2, score=0.8),
    )
    sparse = (
        _hit("shared", method="sparse", rank=1, score=9.0),
        _hit("sparse-only", method="sparse", rank=2, score=8.0),
    )

    fused = fuse_rrf(dense, sparse)
    by_id = {hit.target_id: hit for hit in fused}

    assert RETRIEVAL_RRF_K == 60
    assert [hit.target_id for hit in fused] == ["shared", "dense-only", "sparse-only"]
    assert by_id["shared"].score == pytest.approx((1.0 / 61.0) + (1.0 / 61.0))
    assert by_id["dense-only"].score == pytest.approx(1.0 / 62.0)
    assert by_id["sparse-only"].score == pytest.approx(1.0 / 62.0)
    assert by_id["dense-only"].sparse_rank is None
    assert by_id["sparse-only"].dense_rank is None
    assert all(hit.method == "hybrid-rrf" and hit.score_kind == "rrf" for hit in fused)


def test_rrf_truncates_each_leg_at_exactly_200_then_limits_the_fused_result() -> None:
    dense = tuple(_hit(f"dense-{rank:03d}", method="dense", rank=rank, score=1000.0 - rank) for rank in range(1, 202))

    fused = fuse_rrf(dense, ())

    assert RETRIEVAL_FUSION_INPUT_DEPTH == 200
    assert len(fused) == 200
    assert "dense-051" in {hit.target_id for hit in fused}
    assert "dense-200" in {hit.target_id for hit in fused}
    assert "dense-201" not in {hit.target_id for hit in fused}


def test_rrf_rejects_mixed_queries_duplicate_targets_and_wrong_leg_methods() -> None:
    with pytest.raises(ValueError, match="one query"):
        fuse_rrf(
            (_hit("a", method="dense", rank=1, score=1.0),),
            (_hit("b", method="sparse", rank=1, score=1.0, query_id="q2"),),
        )
    with pytest.raises(ValueError, match="duplicate"):
        fuse_rrf(
            (
                _hit("a", method="dense", rank=1, score=1.0),
                _hit("a", method="dense", rank=2, score=0.5),
            ),
            (),
        )
    with pytest.raises(ValueError, match="dense"):
        fuse_rrf((_hit("a", method="sparse", rank=1, score=1.0),), ())


def test_sparse_import_boundaries_and_legacy_runners_remain_untouched() -> None:
    module = __import__("spicy_regs.docpipeline.retrieval", fromlist=["x"])
    assert module.__file__ is not None
    path = Path(module.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None}
    imports.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)

    assert not any(name == "spicy_regs.corpora" or name.startswith("spicy_regs.corpora.") for name in imports)
    assert not {"sentence_transformers", "torch", "tiktoken", "openai"} & imports
    assert "duckdb" not in source[source.index("def sparse_csr_matrix") :]
    assert Path("src/spicy_regs/corpora/segmentation_sparse_retrieval.py").is_file()
    assert "run-segmentation-sparse-retrieval" in Path("pyproject.toml").read_text(encoding="utf-8")

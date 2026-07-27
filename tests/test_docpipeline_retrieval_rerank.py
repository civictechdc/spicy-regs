"""Fixed-depth cross-encoder reranking over the dense migration candidates."""

from __future__ import annotations

import ast
import dataclasses
import hashlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters.sentence_transformers import RerankResult
from spicy_regs.docpipeline.retrieval import (
    DENSE_MODEL_ID,
    RERANK_BATCH_SIZE,
    RERANK_CHECKPOINT_FILE,
    RERANK_MAX_SEQ_LENGTH,
    RERANK_MODEL_ID,
    RERANK_SCORE_COLUMNS,
    RERANK_SCORE_TABLE,
    RETRIEVAL_RERANK_DEPTH,
    RerankProviderError,
    RerankScoreRow,
    RetrievalHit,
    RetrievalQuery,
    rebuild_reranked_hits,
    read_rerank_score_rows,
    rerank_dense_hits,
)
from spicy_regs.docpipeline.runtime import PlanError, WorkCheckpoint


class FakeReranker:
    provider: str = "sentence-transformers"
    model_id: str = RERANK_MODEL_ID
    tokenizer_id: str = f"huggingface:{RERANK_MODEL_ID.removeprefix('sentence-transformers:')}"
    max_seq_length: int = RERANK_MAX_SEQ_LENGTH
    batch_size: int = RERANK_BATCH_SIZE
    production_provider: bool = False

    def __init__(
        self,
        scores: Sequence[float] | None = None,
        *,
        token_counts: Sequence[int] | None = None,
        fail: bool = False,
    ) -> None:
        self.scores = tuple(scores) if scores is not None else None
        self.token_counts = tuple(token_counts) if token_counts is not None else None
        self.fail = fail
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def rerank(self, query: str, documents: Sequence[str]) -> RerankResult:
        requested = tuple(documents)
        self.calls.append((query, requested))
        if self.fail:
            raise RuntimeError("reranker fixture failed")
        scores = (
            self.scores[: len(requested)]
            if self.scores is not None
            else tuple(float(len(requested) - index) for index in range(len(requested)))
        )
        counts = (
            self.token_counts[: len(requested)]
            if self.token_counts is not None
            else tuple(10 + index for index in range(len(requested)))
        )
        return RerankResult(
            scores=tuple(scores),
            call={
                "provider": self.provider,
                "operation": "rerank",
                "package_name": "sentence-transformers",
                "package_version": "5.6.1",
                "encoder_source": "injected",
                "model_id": self.model_id,
                "model": "BAAI/bge-reranker-v2-m3",
                "revision": self.model_id.rsplit("@", 1)[1],
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": "fixture",
                "token_counts": counts,
                "max_input_tokens": self.max_seq_length,
                "inputs_over_limit": tuple(count > self.max_seq_length for count in counts),
                "token_audit_status": "exact-untruncated-pair-tokenizer",
                "candidate_count": len(requested),
                "status": "completed",
                "provider_invoked": True,
                "attempt_count": 1,
                "retry_count": 0,
                "duration_ms": 0.0,
                "error_type": None,
                "request_parameters": {"max_seq_length": self.max_seq_length},
                "runtime_parameters": {
                    "batch_size": self.batch_size,
                    "device": "fixture",
                    "trust_remote_code": False,
                    "clear_device_cache_after_request": False,
                },
            },
        )


def _dense_hits(
    count: int,
    *,
    query_id: str = "q1",
    work_id: str = "dense-work",
    method: str = "dense",
) -> tuple[RetrievalHit, ...]:
    return tuple(
        RetrievalHit(
            work_id=work_id,
            query_id=query_id,
            level="segment",
            method=method,
            target_id=f"segment-{rank:03d}",
            artifact_id=f"artifact-{rank:03d}",
            segment_id=f"segment-{rank:03d}",
            source_table="dockets",
            subject_id=f"subject-{rank:03d}",
            artifact_digest=f"digest-{rank:03d}",
            rank=rank,
            candidate_universe_size=250,
            candidate_input_size=count,
            candidate_limit=200,
            score=float(1_000 - rank),
            score_kind="cosine" if method == "dense" else "rrf",
            dense_rank=rank,
            dense_score=float(1_000 - rank),
            model_id=DENSE_MODEL_ID,
            model_revision=DENSE_MODEL_ID.rsplit("@", 1)[1],
        )
        for rank in range(1, count + 1)
    )


def _documents(count: int) -> dict[str, str]:
    return {f"segment-{rank:03d}": f"document {rank:03d}" for rank in range(1, count + 1)}


def _candidate_ids_digest(count: int) -> str:
    ids = [f"segment-{rank:03d}" for rank in range(1, count + 1)]
    return hashlib.sha256(("[" + ",".join(f'"{target_id}"' for target_id in ids) + "]").encode()).hexdigest()


def test_rerank_uses_exactly_top_50_of_200_dense_hits_and_fixed_model_pins(
    tmp_path: Path,
) -> None:
    provider = FakeReranker()

    outcome = rerank_dense_hits(
        _dense_hits(200),
        _documents(200),
        query=RetrievalQuery("q1", "water quality", "segment"),
        source_work_id="dense-work",
        reranker=provider,
        run_directory=tmp_path,
    )

    assert RETRIEVAL_RERANK_DEPTH == 50
    assert RERANK_MAX_SEQ_LENGTH == 4_096
    assert RERANK_BATCH_SIZE == 16
    assert RERANK_MODEL_ID == ("sentence-transformers:BAAI/bge-reranker-v2-m3@953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e")
    assert provider.calls == [("water quality", tuple(f"document {rank:03d}" for rank in range(1, 51)))]
    assert outcome.state == "completed"
    assert len(outcome.hits) == len(outcome.scores) == 50
    assert {row.target_id for row in outcome.scores} == {f"segment-{rank:03d}" for rank in range(1, 51)}
    assert all(
        row.model_id == RERANK_MODEL_ID
        and row.model_revision == RERANK_MODEL_ID.rsplit("@", 1)[1]
        and row.input_limit == RERANK_MAX_SEQ_LENGTH
        for row in outcome.scores
    )
    assert outcome.scores[0].candidate_ids_sha256 == _candidate_ids_digest(50)


def test_rerank_refuses_hybrid_or_non_bge_inputs_before_provider_call(
    tmp_path: Path,
) -> None:
    provider = FakeReranker()
    with pytest.raises(PlanError, match="dense"):
        rerank_dense_hits(
            _dense_hits(2, method="hybrid-rrf"),
            _documents(2),
            query=RetrievalQuery("q1", "query", "segment"),
            source_work_id="dense-work",
            reranker=provider,
            run_directory=tmp_path,
        )
    drifted = dataclasses.replace(_dense_hits(1)[0], model_id="dense@wrong-revision")
    with pytest.raises(PlanError, match="BGE dense"):
        rerank_dense_hits(
            (drifted,),
            _documents(1),
            query=RetrievalQuery("q1", "query", "segment"),
            source_work_id="dense-work",
            reranker=provider,
            run_directory=tmp_path,
        )
    assert provider.calls == []


def test_rerank_projects_scores_to_input_candidates_then_breaks_ties_by_target(
    tmp_path: Path,
) -> None:
    hits = (
        dataclasses.replace(_dense_hits(3)[0], target_id="z", segment_id="z"),
        dataclasses.replace(_dense_hits(3)[1], target_id="a", segment_id="a"),
        dataclasses.replace(_dense_hits(3)[2], target_id="m", segment_id="m"),
    )
    provider = FakeReranker((0.1, 0.9, 0.9))

    outcome = rerank_dense_hits(
        hits,
        {"z": "z text", "a": "a text", "m": "m text"},
        query=RetrievalQuery("q1", "query", "segment"),
        source_work_id="dense-work",
        reranker=provider,
        run_directory=tmp_path,
    )

    assert [(row.candidate_index, row.target_id, row.rerank_score) for row in outcome.scores] == [
        (0, "z", 0.1),
        (1, "a", 0.9),
        (2, "m", 0.9),
    ]
    assert [(hit.rank, hit.target_id, hit.score) for hit in outcome.hits] == [
        (1, "a", 0.9),
        (2, "m", 0.9),
        (3, "z", 0.1),
    ]


def test_rerank_records_exact_pair_token_audits_and_computes_truncation(
    tmp_path: Path,
) -> None:
    provider = FakeReranker(
        (0.3, 0.2, 0.1),
        token_counts=(4_096, 4_097, 3),
    )
    outcome = rerank_dense_hits(
        _dense_hits(3),
        _documents(3),
        query=RetrievalQuery("q1", "query", "segment"),
        source_work_id="dense-work",
        reranker=provider,
        run_directory=tmp_path,
    )

    assert [row.untruncated_token_count for row in outcome.scores] == [4_096, 4_097, 3]
    assert [row.would_truncate for row in outcome.scores] == [False, True, False]
    assert all(
        row.input_limit == 4_096
        and row.tokenizer_id == provider.tokenizer_id
        and row.token_audit_status == "exact-untruncated-pair-tokenizer"
        for row in outcome.scores
    )
    assert [hit.rerank_would_truncate for hit in outcome.hits] == [False, True, False]


def test_rerank_zero_row_table_is_typed_and_empty_never_calls_provider(
    tmp_path: Path,
) -> None:
    provider = FakeReranker(fail=True)
    outcome = rerank_dense_hits(
        (),
        {},
        query=RetrievalQuery("q1", "query", "segment"),
        source_work_id="empty-work",
        reranker=provider,
        run_directory=tmp_path,
    )

    table = pq.read_table(tmp_path / RERANK_SCORE_TABLE)
    assert outcome.state == "completed_empty"
    assert outcome.hits == outcome.scores == ()
    assert provider.calls == []
    assert table.num_rows == 0
    assert table.schema.names == [name for name, _ in RERANK_SCORE_COLUMNS]
    assert (
        rebuild_reranked_hits(
            (),
            {},
            query=RetrievalQuery("q1", "query", "segment"),
            source_work_id="empty-work",
            run_directory=tmp_path,
        )
        == ()
    )


def test_rerank_failure_is_durable_resumable_and_scoped_to_one_group(
    tmp_path: Path,
) -> None:
    query_one = RetrievalQuery("q1", "first query", "segment")
    first = rerank_dense_hits(
        _dense_hits(2, query_id="q1"),
        _documents(2),
        query=query_one,
        source_work_id="dense-work",
        reranker=FakeReranker((0.2, 0.1)),
        run_directory=tmp_path,
    )
    failed_provider = FakeReranker(fail=True)
    with pytest.raises(RerankProviderError, match="RuntimeError"):
        rerank_dense_hits(
            _dense_hits(2, query_id="q2"),
            _documents(2),
            query=RetrievalQuery("q2", "second query", "segment"),
            source_work_id="dense-work",
            reranker=failed_provider,
            run_directory=tmp_path,
        )

    records = WorkCheckpoint(tmp_path / RERANK_CHECKPOINT_FILE, repair=False).records()
    assert sorted(record["state"] for record in records) == ["completed", "failed"]
    assert len({record["work_id"] for record in records}) == 2
    assert len(read_rerank_score_rows(tmp_path)) == 2

    retried = rerank_dense_hits(
        _dense_hits(2, query_id="q2"),
        _documents(2),
        query=RetrievalQuery("q2", "second query", "segment"),
        source_work_id="dense-work",
        reranker=FakeReranker((0.7, 0.1)),
        run_directory=tmp_path,
    )
    assert retried.state == "completed"
    assert {row.group_attempt for row in retried.scores} == {2}

    refusing_provider = FakeReranker(fail=True)
    resumed = rerank_dense_hits(
        _dense_hits(2, query_id="q1"),
        _documents(2),
        query=query_one,
        source_work_id="dense-work",
        reranker=refusing_provider,
        run_directory=tmp_path,
    )
    assert resumed.hits == first.hits
    assert refusing_provider.calls == []


def test_rerank_recovers_scores_written_before_a_killed_completion_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_append = WorkCheckpoint.append

    def kill_before_completed(self: WorkCheckpoint, record: dict[str, Any]) -> None:
        if record.get("state") == "completed":
            raise KeyboardInterrupt
        original_append(self, record)

    monkeypatch.setattr(WorkCheckpoint, "append", kill_before_completed)
    with pytest.raises(KeyboardInterrupt):
        rerank_dense_hits(
            _dense_hits(2),
            _documents(2),
            query=RetrievalQuery("q1", "query", "segment"),
            source_work_id="dense-work",
            reranker=FakeReranker((0.8, 0.2)),
            run_directory=tmp_path,
        )
    assert len(read_rerank_score_rows(tmp_path)) == 2

    monkeypatch.setattr(WorkCheckpoint, "append", original_append)
    refusing_provider = FakeReranker(fail=True)
    recovered = rerank_dense_hits(
        _dense_hits(2),
        _documents(2),
        query=RetrievalQuery("q1", "query", "segment"),
        source_work_id="dense-work",
        reranker=refusing_provider,
        run_directory=tmp_path,
    )

    assert [hit.target_id for hit in recovered.hits] == ["segment-001", "segment-002"]
    assert refusing_provider.calls == []
    records = WorkCheckpoint(tmp_path / RERANK_CHECKPOINT_FILE, repair=False).records()
    assert [record["state"] for record in records] == ["completed"]


def test_rerank_rejects_candidate_hash_drift_without_provider_call(
    tmp_path: Path,
) -> None:
    query = RetrievalQuery("q1", "query", "segment")
    rerank_dense_hits(
        _dense_hits(2),
        _documents(2),
        query=query,
        source_work_id="dense-work",
        reranker=FakeReranker((0.2, 0.1)),
        run_directory=tmp_path,
    )
    drifted = (
        _dense_hits(2)[0],
        dataclasses.replace(
            _dense_hits(2)[1],
            target_id="replacement",
            segment_id="replacement",
        ),
    )
    provider = FakeReranker()

    with pytest.raises(PlanError, match="candidate.*drift"):
        rerank_dense_hits(
            drifted,
            {"segment-001": "document 001", "replacement": "replacement text"},
            query=query,
            source_work_id="dense-work",
            reranker=provider,
            run_directory=tmp_path,
        )
    assert provider.calls == []


def test_rerank_rebuild_is_provider_free_and_rows_are_frozen_immutable(
    tmp_path: Path,
) -> None:
    hits = _dense_hits(3)
    documents = _documents(3)
    query = RetrievalQuery("q1", "query", "segment")
    outcome = rerank_dense_hits(
        hits,
        documents,
        query=query,
        source_work_id="dense-work",
        reranker=FakeReranker((0.2, 0.9, 0.4)),
        run_directory=tmp_path,
    )

    rebuilt = rebuild_reranked_hits(
        hits,
        documents,
        query=query,
        source_work_id="dense-work",
        run_directory=tmp_path,
    )

    assert rebuilt == outcome.hits
    assert all(isinstance(row, RerankScoreRow) for row in outcome.scores)
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(outcome.scores[0], "rerank_score", 999.0)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("model_id", "sentence-transformers:BAAI/bge-reranker-v2-m3@wrong", "model"),
        ("max_seq_length", 2_048, "input limit"),
        ("batch_size", 8, "batch"),
    ],
)
def test_rerank_rejects_provider_pin_drift_before_call(
    tmp_path: Path,
    field: str,
    value: Any,
    match: str,
) -> None:
    provider = FakeReranker()
    setattr(provider, field, value)
    with pytest.raises(PlanError, match=match):
        rerank_dense_hits(
            _dense_hits(1),
            _documents(1),
            query=RetrievalQuery("q1", "query", "segment"),
            source_work_id="dense-work",
            reranker=provider,
            run_directory=tmp_path,
        )
    assert provider.calls == []


def test_rerank_import_guards_score_confinement_and_legacy_runner_preservation() -> None:
    module = __import__("spicy_regs.docpipeline.retrieval", fromlist=["x"])
    assert module.__file__ is not None
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None}
    imports.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)

    assert not any(name == "spicy_regs.corpora" or name.startswith("spicy_regs.corpora.") for name in imports)
    assert not {"sentence_transformers", "torch", "tiktoken", "openai"} & imports
    extraction = Path("src/spicy_regs/docpipeline/extraction.py").read_text(encoding="utf-8")
    assert "refuse_retrieval_aids(payload)" in extraction
    assert '"rerank_score"' in extraction
    assert Path("src/spicy_regs/corpora/segmentation_rerank.py").is_file()
    assert "run-segmentation-rerank" in Path("pyproject.toml").read_text(encoding="utf-8")

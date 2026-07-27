"""Step 5.5 retrieval metrics and runtime assembly contracts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pytest

from spicy_regs.docpipeline.adapters.sentence_transformers import (
    DenseEmbeddingResult,
    RerankResult,
    SparseEncodingResult,
    SparseVector,
)
from spicy_regs.docpipeline.retrieval import (
    DENSE_EMBEDDING_TABLE,
    DENSE_MODEL_ID,
    IR_MEASURES_VERSION,
    RERANK_BATCH_SIZE,
    RERANK_MAX_SEQ_LENGTH,
    RERANK_MODEL_ID,
    RERANK_SCORE_TABLE,
    RETRIEVAL_EXCLUSION_TABLE,
    RETRIEVAL_HIT_TABLE,
    RETRIEVAL_JOIN_INPUTS_FILE,
    SPARSE_EMBEDDING_TABLE,
    SPARSE_MODEL_ID,
    RetrievalOutcome,
    RetrievalProviders,
    RetrievalQuery,
    RetrievalQueryOutcome,
    RetrievalRunContext,
    RetrievalSpec,
    RetrievalHit,
    build_retrieval_metric_inputs,
    plan_retrieval_items,
    rebuild_retrieval,
    recompute_retrieval,
    retrieval_metrics,
    retrieval_run_plan_facts,
    run_retrieval,
)
from spicy_regs.docpipeline.runtime import (
    CheckResult,
    PlanError,
    ProviderTotals,
    RunChecks,
    RunPlan,
    RunWorkspace,
    WorkCheckpoint,
    execute_run,
    file_inventory,
    rebuild_run,
    sha256_file,
    validate_run,
    work_directory_for,
)
from spicy_regs.docpipeline.segments import SEGMENT_COLUMNS, SEGMENT_TABLE
from spicy_regs.docpipeline.source import ARTIFACT_COLUMNS, ARTIFACT_TABLE, write_table


def _hit(
    query_id: str,
    target_id: str,
    rank: int,
    *,
    method: str = "reranked",
    score: float = 0.0,
) -> RetrievalHit:
    return RetrievalHit(
        work_id=f"work-{query_id}",
        query_id=query_id,
        level="segment",
        method=method,
        target_id=target_id,
        artifact_id="artifact-1",
        segment_id=target_id,
        source_table="documents",
        subject_id="document-1",
        artifact_digest="a" * 64,
        rank=rank,
        candidate_universe_size=3,
        candidate_input_size=3,
        candidate_limit=50 if method == "reranked" else 200,
        score=score,
        score_kind="cross-encoder" if method == "reranked" else "cosine",
    )


def test_metric_inputs_use_one_based_ranks_not_raw_scores_and_keep_sentinel() -> None:
    hits = (
        _hit("q1", "irrelevant", 1, score=-9000.0),
        _hit("q1", "relevant-a", 2, score=9000.0),
        _hit("q1", "relevant-b", 3, score=0.0),
    )

    inputs = build_retrieval_metric_inputs(
        hits,
        {"q1": ("relevant-a", "relevant-b"), "q-empty": ()},
        methods=("reranked",),
    )

    assert inputs.qrels == {
        "q-empty": {"q-empty:missing-relevant-segment": 1},
        "q1": {"relevant-a": 1, "relevant-b": 1},
    }
    assert list(inputs.runs["reranked"]["q1"]) == ["irrelevant", "relevant-a", "relevant-b"]
    assert list(inputs.runs["reranked"]["q1"].values()) == [3.0, 2.0, 1.0]
    assert inputs.runs["reranked"]["q-empty"] == {}


def test_pinned_metrics_match_an_independent_fixture_calculation() -> None:
    hits = (
        _hit("q1", "irrelevant", 1),
        _hit("q1", "relevant-a", 2),
        _hit("q1", "relevant-b", 3),
    )

    measured = retrieval_metrics(
        hits,
        {"q1": ("relevant-a", "relevant-b")},
        methods=("reranked",),
    )
    expected_ndcg = (1 / math.log2(3) + 1 / math.log2(4)) / (1 + 1 / math.log2(3))

    assert measured["metric_provider"] == f"ir-measures:{IR_MEASURES_VERSION}"
    assert measured["ir_measures_version"] == "0.4.3"
    assert measured["query_count"] == 1
    assert measured["zero_relevant_query_count"] == 0
    assert measured["methods"]["reranked"] == pytest.approx(
        {
            "recall_at_1": 0.0,
            "recall_at_3": 1.0,
            "recall_at_5": 1.0,
            "recall_at_10": 1.0,
            "recall_at_25": 1.0,
            "recall_at_50": 1.0,
            "recall_at_100": 1.0,
            "recall_at_200": 1.0,
            "precision_at_1": 0.0,
            "precision_at_3": 2 / 3,
            "precision_at_5": 2 / 5,
            "precision_at_10": 2 / 10,
            "mrr": 1 / 2,
            "ndcg_at_5": expected_ndcg,
            "ndcg_at_10": expected_ndcg,
        }
    )


def test_zero_relevant_and_no_hit_queries_remain_in_metric_denominator() -> None:
    measured = retrieval_metrics(
        (),
        {"q-empty": (), "q-no-hit": ("missing",)},
        methods=("dense", "reranked"),
    )

    assert measured["query_count"] == 2
    assert measured["zero_relevant_query_count"] == 1
    assert measured["methods"]["dense"]["recall_at_50"] == 0.0
    assert measured["methods"]["reranked"]["mrr"] == 0.0

    planned = retrieval_metrics((), {"q-empty": ()})
    assert list(planned["methods"]) == ["dense", "sparse", "hybrid-rrf", "reranked"]


def test_mixed_level_metrics_use_only_queries_that_executed_each_method() -> None:
    artifact_dense = dataclasses.replace(
        _hit("artifact", "artifact-1", 1, method="dense"),
        level="artifact",
        target_id="artifact-1",
        artifact_id="artifact-1",
        segment_id=None,
    )
    segment_dense = _hit("segment", "irrelevant-segment", 1, method="dense")
    segment_sparse = _hit("segment", "relevant-segment", 1, method="sparse")
    query_methods = {
        "artifact": ("dense",),
        "segment": ("dense", "sparse", "hybrid-rrf", "reranked"),
    }

    measured = retrieval_metrics(
        (artifact_dense, segment_dense, segment_sparse),
        {
            "artifact": ("artifact-1",),
            "segment": ("relevant-segment",),
        },
        methods=("dense", "sparse", "hybrid-rrf", "reranked"),
        query_methods=query_methods,
    )
    inputs = build_retrieval_metric_inputs(
        (artifact_dense, segment_dense, segment_sparse),
        {
            "artifact": ("artifact-1",),
            "segment": ("relevant-segment",),
        },
        methods=("dense", "sparse", "hybrid-rrf", "reranked"),
        query_methods=query_methods,
    )

    assert measured["method_query_counts"] == {
        "dense": 2,
        "sparse": 1,
        "hybrid-rrf": 1,
        "reranked": 1,
    }
    assert measured["methods"]["dense"]["recall_at_1"] == 0.5
    assert measured["methods"]["sparse"]["recall_at_1"] == 1.0
    assert measured["methods"]["hybrid-rrf"]["recall_at_1"] == 0.0
    assert inputs.runs["sparse"] == {"segment": {"relevant-segment": 1.0}}


@pytest.mark.parametrize(
    "hits",
    [
        (_hit("q1", "a", 2),),
        (_hit("q1", "a", 1), _hit("q1", "b", 1)),
        (_hit("q1", "a", 1), _hit("q1", "a", 2)),
    ],
)
def test_metric_inputs_reject_non_contiguous_duplicate_ranks_and_targets(
    hits: tuple[RetrievalHit, ...],
) -> None:
    with pytest.raises(ValueError, match="rank|duplicate"):
        build_retrieval_metric_inputs(hits, {"q1": ("a",)}, methods=("reranked",))


def test_metric_answers_must_cover_exactly_the_scored_queries() -> None:
    with pytest.raises(ValueError, match="answer"):
        build_retrieval_metric_inputs(
            (_hit("q-unexpected", "a", 1),),
            {"q-known": ("a",)},
            methods=("reranked",),
        )

    with pytest.raises(ValueError, match="sentinel"):
        build_retrieval_metric_inputs(
            (_hit("q-known", "q-known:missing-relevant-segment", 1),),
            {"q-known": ()},
            methods=("reranked",),
        )


class _UnusedDense:
    provider = "sentence-transformers"
    model_id = DENSE_MODEL_ID
    dimensions = 2
    tokenizer_id = f"{DENSE_MODEL_ID}:tokenizer"
    max_input_tokens: int | None = 512
    production_provider = False

    def model_token_count(self, text: str) -> int | None:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"dense counter was invoked: {text}")

    def embed(self, texts: Sequence[str]) -> DenseEmbeddingResult:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"dense provider was invoked: {texts}")


class _FakeDense(_UnusedDense):
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[str, ...]] = []

    def embed(self, texts: Sequence[str]) -> DenseEmbeddingResult:
        requested = tuple(texts)
        self.calls.append(requested)
        if self.fail:
            raise RuntimeError("fixture provider failed")
        counts = tuple(len(text.split()) for text in requested)
        return DenseEmbeddingResult(
            vectors=tuple((1.0, 0.0) for _ in requested),
            call={
                "provider": self.provider,
                "operation": "dense-embedding",
                "package_name": "sentence-transformers",
                "package_version": "5.6.1",
                "model_id": self.model_id,
                "revision": self.model_id.rsplit("@", 1)[1],
                "dimensions": self.dimensions,
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": "fixture",
                "token_counts": counts,
                "max_input_tokens": self.max_input_tokens,
                "inputs_over_limit": tuple(False for _ in requested),
                "token_audit_status": "exact-untruncated-model-tokenizer",
                "input_count": len(requested),
                "status": "completed",
                "provider_invoked": True,
                "attempt_count": 1,
                "retry_count": 0,
                "duration_ms": 1.0,
            },
        )


class _UnusedSparse:
    provider = "sentence-transformers-sparse"
    model_id = SPARSE_MODEL_ID
    dimensions = 4
    tokenizer_id = f"{SPARSE_MODEL_ID}:tokenizer"
    max_input_tokens: int | None = 512
    production_provider = False

    def model_token_count(self, text: str) -> int | None:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"sparse counter was invoked: {text}")

    def encode(
        self,
        texts: Sequence[str],
        *,
        task: str,
    ) -> SparseEncodingResult:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"sparse provider was invoked for {task}: {texts}")


class _FailingSparseAfterDocument(_UnusedSparse):
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def encode(self, texts: Sequence[str], *, task: str) -> SparseEncodingResult:
        requested = tuple(texts)
        self.calls.append((task, requested))
        if task == "query":
            raise RuntimeError("fixture query provider failed")
        counts = tuple(len(text.split()) for text in requested)
        return SparseEncodingResult(
            vectors=tuple(SparseVector(self.dimensions, (0,), (1.0,)) for _ in requested),
            call={
                "provider": self.provider,
                "operation": "sparse-encoding",
                "task": task,
                "model_id": self.model_id,
                "revision": self.model_id.rsplit("@", 1)[1],
                "dimensions": self.dimensions,
                "active_dimension_counts": tuple(1 for _ in requested),
                "tokenizer_id": self.tokenizer_id,
                "tokenizer_package_version": "fixture",
                "token_counts": counts,
                "max_input_tokens": self.max_input_tokens,
                "inputs_over_limit": tuple(False for _ in requested),
                "token_audit_status": "exact-untruncated-model-tokenizer",
                "input_count": len(requested),
                "status": "completed",
                "provider_invoked": True,
                "attempt_count": 1,
                "retry_count": 0,
                "duration_ms": 1.0,
            },
        )


class _UnusedReranker:
    provider = "sentence-transformers"
    model_id = RERANK_MODEL_ID
    tokenizer_id = "fixture-rerank-tokenizer"
    max_seq_length = RERANK_MAX_SEQ_LENGTH
    batch_size = RERANK_BATCH_SIZE
    production_provider = False

    def rerank(
        self,
        query: str,
        documents: Sequence[str],
    ) -> RerankResult:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"reranker was invoked for {query}: {documents}")


class _UnusedCounter:
    name = "unused"
    version = "1"

    def count(self, text: str) -> int:  # pragma: no cover - must stay unreachable
        raise AssertionError(f"counter was invoked: {text}")


class _CharacterCounter:
    name = "character-fixture"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


def _empty_context(tmp_path: Path) -> RetrievalRunContext:
    source = tmp_path / "source-run"
    write_table(source / ARTIFACT_TABLE, ARTIFACT_COLUMNS, ())
    write_table(source / SEGMENT_TABLE, SEGMENT_COLUMNS, ())
    return RetrievalRunContext(source)


def _providers() -> RetrievalProviders:
    return RetrievalProviders(
        embedder=_UnusedDense(),
        sparse_encoder=_UnusedSparse(),
        reranker=_UnusedReranker(),
        counter=_UnusedCounter(),
    )


def _providers_with_dense(dense: _FakeDense) -> RetrievalProviders:
    return RetrievalProviders(
        embedder=dense,
        sparse_encoder=_UnusedSparse(),
        reranker=_UnusedReranker(),
        counter=_UnusedCounter(),
    )


def _providers_with_later_sparse_failure(
    dense: _FakeDense,
    sparse: _FailingSparseAfterDocument,
) -> RetrievalProviders:
    return RetrievalProviders(
        embedder=dense,
        sparse_encoder=sparse,
        reranker=_UnusedReranker(),
        counter=_CharacterCounter(),
    )


def _one_artifact_context(tmp_path: Path) -> RetrievalRunContext:
    source = tmp_path / "source-run"
    text = "one exact source field"
    digest = hashlib.sha256(b"artifact").hexdigest()
    field_digest = hashlib.sha256(text.encode()).hexdigest()
    write_table(
        source / ARTIFACT_TABLE,
        ARTIFACT_COLUMNS,
        (
            {
                "artifact_id": "artifact-1",
                "content_sha256": digest,
                "subject_type": "docket",
                "subject_id": "D-1",
                "profile_id": "regulations-docket-v2",
                "source_table": "dockets",
                "access_scope": "public",
                "access_basis": "us-federal-public-record",
            },
        ),
    )
    write_table(
        source / SEGMENT_TABLE,
        SEGMENT_COLUMNS,
        (
            {
                "segment_id": "segment-1",
                "content_digest": hashlib.sha256(b"segment").hexdigest(),
                "artifact_id": "artifact-1",
                "artifact_sha256": digest,
                "subject_type": "docket",
                "subject_id": "D-1",
                "profile_id": "regulations-docket-v2",
                "source_table": "dockets",
                "ordinal": 0,
                "text_sha256": field_digest,
                "text": text,
                "slices_json": json.dumps(
                    [
                        {
                            "source_field": "dockets.title",
                            "field_sha256": field_digest,
                            "start_char": 0,
                            "end_char": len(text),
                            "text": text,
                        }
                    ],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        ),
    )
    return RetrievalRunContext(source)


def _plan(
    spec: RetrievalSpec,
    queries: tuple[RetrievalQuery, ...],
    context: RetrievalRunContext,
    *,
    answers: Mapping[str, Sequence[str]] | None,
    mode: str = "diagnostic",
    earlier_runs: Mapping[str, object] | None = None,
    run_id: str = "retrieval-step-5-5",
) -> RunPlan:
    return RunPlan(
        run_id=run_id,
        mode=mode,
        steps=("retrieve",),
        retrieval=retrieval_run_plan_facts(spec, queries, context, answers=answers),
        required_work=("retrieve",),
        earlier_runs=earlier_runs or {},
    )


def _receipted_empty_context(
    tmp_path: Path,
    *,
    metadata_rows: Sequence[Mapping[str, object]] = (),
) -> tuple[RetrievalRunContext, dict[str, object]]:
    source = tmp_path / "step-4-run"
    step4_plan = RunPlan(
        run_id=f"step-4-{tmp_path.name}",
        mode="build",
        steps=("source", "segment"),
    )

    def finalize(workspace: RunWorkspace, _results: tuple[object, ...]) -> RunChecks:
        write_table(workspace.file(ARTIFACT_TABLE), ARTIFACT_COLUMNS, ())
        write_table(workspace.file(SEGMENT_TABLE), SEGMENT_COLUMNS, ())
        return RunChecks(
            checks=(
                CheckResult(step="source", name="fixture", status="pass"),
                CheckResult(step="segment", name="fixture", status="pass"),
            )
        )

    outcome = execute_run(
        step4_plan,
        source,
        items=(),
        execute=lambda _workspace, _item: pytest.fail("zero-work Step 4 fixture executed work"),
        finalize=finalize,
    )
    inventory = file_inventory(outcome.run_directory)
    context = RetrievalRunContext(outcome.run_directory, metadata_rows=metadata_rows)
    declaration: dict[str, object] = {
        "step4": {
            "run_directory": str(outcome.run_directory),
            "run_id": step4_plan.run_id,
            "files": {
                ARTIFACT_TABLE: inventory[ARTIFACT_TABLE]["sha256"],
                SEGMENT_TABLE: inventory[SEGMENT_TABLE]["sha256"],
            },
        }
    }
    return context, declaration


def test_query_outcomes_cover_all_runtime_states_without_conflating_empty_and_failure() -> None:
    assert RetrievalQueryOutcome("completed", hits=(_hit("q", "a", 1),)).state == "completed"
    assert RetrievalQueryOutcome("completed_empty").state == "completed_empty"
    assert RetrievalQueryOutcome("rejected", reason="unsupported request").state == "rejected"
    failed = RetrievalQueryOutcome(
        "failed",
        error="provider failed",
        provider=ProviderTotals(calls=1, failures=1),
    )
    assert failed.state == "failed"
    with pytest.raises(ValueError, match="completed_empty"):
        RetrievalQueryOutcome("completed_empty", hits=(_hit("q", "a", 1),))
    with pytest.raises(ValueError, match="error"):
        RetrievalQueryOutcome("failed")


def test_build_rejects_answers_in_run_recompute_and_rebuild(tmp_path: Path) -> None:
    context, earlier_runs = _receipted_empty_context(tmp_path)
    spec = RetrievalSpec()
    queries: tuple[RetrievalQuery, ...] = ()
    answers: dict[str, tuple[str, ...]] = {}
    plan = _plan(
        spec,
        queries,
        context,
        answers=answers,
        mode="build",
        earlier_runs=earlier_runs,
    )

    with pytest.raises(PlanError, match="build.*answer"):
        run_retrieval(
            plan,
            tmp_path / "run",
            spec=spec,
            queries=queries,
            context=context,
            providers=_providers(),
            answers=answers,
        )
    with pytest.raises(PlanError, match="build.*answer"):
        recompute_retrieval(spec, queries, context, answers=answers)(tmp_path, plan)

    rebuild_root = tmp_path / "rebuild-work"
    workspace = RunWorkspace(
        plan=plan,
        path=rebuild_root,
        checkpoint=WorkCheckpoint(rebuild_root / "transitions.jsonl"),
    )
    with pytest.raises(PlanError, match="build.*answer"):
        rebuild_retrieval(spec, queries, context, answers=answers)(workspace, plan)


def test_build_requires_checked_step4_lineage_and_binds_work_identity(
    tmp_path: Path,
) -> None:
    spec = RetrievalSpec()
    queries = (RetrievalQuery("artifact-empty", "find it", "artifact"),)
    unreceipted = _empty_context(tmp_path / "diagnostic")
    unreceipted_plan = _plan(spec, queries, unreceipted, answers=None, mode="build")

    with pytest.raises(PlanError, match="Step 4.*lineage"):
        run_retrieval(
            unreceipted_plan,
            tmp_path / "unreceipted-run",
            spec=spec,
            queries=queries,
            context=unreceipted,
            providers=_providers(),
        )

    context, earlier_runs = _receipted_empty_context(tmp_path / "receipted")
    facts = retrieval_run_plan_facts(spec, queries, context)
    plan = _plan(
        spec,
        queries,
        context,
        answers=None,
        mode="build",
        earlier_runs=earlier_runs,
    )
    result = run_retrieval(
        plan,
        tmp_path / "receipted-run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
    )

    assert facts["source_run_id"].startswith("step-4-")
    assert plan_retrieval_items(spec, queries, context) != plan_retrieval_items(
        spec,
        queries,
        unreceipted,
    )
    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["inputs"]["earlier_runs"] == earlier_runs
    sidecar = result.outcome.run_directory / RETRIEVAL_JOIN_INPUTS_FILE
    assert sidecar.is_file()
    assert sha256_file(sidecar) == facts["join_inputs_sha256"]


def test_diagnostic_lineage_warning_and_provider_free_join_fallback(
    tmp_path: Path,
) -> None:
    diagnostic_context = _empty_context(tmp_path / "diagnostic")
    spec = RetrievalSpec()
    queries = (RetrievalQuery("artifact-empty", "find it", "artifact"),)
    diagnostic = run_retrieval(
        _plan(spec, queries, diagnostic_context, answers=None),
        tmp_path / "diagnostic-run",
        spec=spec,
        queries=queries,
        context=diagnostic_context,
        providers=_providers(),
    )
    assert any("diagnostic" in warning and "lineage" in warning for warning in diagnostic.outcome.receipt["warnings"])

    context, earlier_runs = _receipted_empty_context(
        tmp_path / "receipted",
        metadata_rows=(
            {
                "source_table": "dockets",
                "subject_id": "D-1",
                "version": "fixture-v1",
            },
        ),
    )
    plan = _plan(
        spec,
        queries,
        context,
        answers=None,
        mode="build",
        earlier_runs=earlier_runs,
    )
    source = run_retrieval(
        plan,
        tmp_path / "source-run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
    )
    rebuilt = tmp_path / "rebuilt"
    report = rebuild_run(
        source.outcome.run_directory,
        rebuilt,
        rebuild=rebuild_retrieval(
            spec,
            queries,
            RetrievalRunContext(context.source_directory),
        ),
    )
    assert report["status"] == "pass"
    assert (rebuilt / RETRIEVAL_JOIN_INPUTS_FILE).read_bytes() == (
        source.outcome.run_directory / RETRIEVAL_JOIN_INPUTS_FILE
    ).read_bytes()


def test_answer_file_in_build_work_directory_fails_publication_gate(
    tmp_path: Path,
) -> None:
    context, earlier_runs = _receipted_empty_context(tmp_path)
    spec = RetrievalSpec()
    queries: tuple[RetrievalQuery, ...] = ()
    plan = _plan(
        spec,
        queries,
        context,
        answers=None,
        mode="build",
        earlier_runs=earlier_runs,
    )
    output = tmp_path / "run"
    work = work_directory_for(output)
    work.mkdir(parents=True)
    (work / "answers.json").write_text("{}\n", encoding="utf-8")

    result = run_retrieval(
        plan,
        output,
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
    )

    assert result.outcome.final_state == "fail"
    assert result.outcome.receipt["publication_eligible"] is False
    assert any(
        check["name"] == "build_has_no_answer_data" and check["status"] == "fail"
        for check in result.outcome.receipt["checks"]
    )


def test_runtime_zero_work_metrics_validate_rebuild_and_tamper_detection(tmp_path: Path) -> None:
    context = _empty_context(tmp_path)
    spec = RetrievalSpec()
    queries = (
        RetrievalQuery("artifact-empty", "find the document", "artifact"),
        RetrievalQuery("segment-empty", "find the section", "segment"),
    )
    answers = {"artifact-empty": (), "segment-empty": ()}
    plan = _plan(spec, queries, context, answers=answers)
    items = plan_retrieval_items(spec, queries, context)
    assert [item.payload["query_id"] for item in items] == ["artifact-empty", "segment-empty"]
    assert plan_retrieval_items(spec, tuple(reversed(queries)), context) == items

    result = run_retrieval(
        plan,
        tmp_path / "run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
        answers=answers,
    )

    assert isinstance(result, RetrievalOutcome)
    assert result.outcome.final_state == "pass"
    assert result.hits == ()
    assert result.exclusions == ()
    assert result.metrics is not None
    assert result.metrics["query_count"] == 2
    assert result.metrics["zero_relevant_query_count"] == 2
    assert result.outcome.receipt["counts"]["empty"] == 2
    assert result.outcome.receipt["provider"]["calls"] == 0
    assert result.outcome.receipt["security"]["secret_match_count"] == 0
    for relative in (
        RETRIEVAL_HIT_TABLE,
        RETRIEVAL_EXCLUSION_TABLE,
        DENSE_EMBEDDING_TABLE,
        SPARSE_EMBEDDING_TABLE,
        RERANK_SCORE_TABLE,
    ):
        assert pq.read_table(result.outcome.run_directory / relative).num_rows == 0

    validate = recompute_retrieval(spec, queries, context, answers=answers)
    assert validate_run(result.outcome.run_directory, plan=plan, recompute=validate)["status"] == "pass"

    rebuilt = tmp_path / "rebuilt"
    report = rebuild_run(
        result.outcome.run_directory,
        rebuilt,
        rebuild=rebuild_retrieval(
            spec,
            queries,
            context,
            answers=answers,
            providers=_providers(),
        ),
    )
    assert report["status"] == "pass"
    assert report["provider_invoked"] is False
    assert json.loads((rebuilt / "metrics.json").read_text()) == result.metrics
    assert pq.read_table(rebuilt / RETRIEVAL_HIT_TABLE).to_pylist() == []

    metrics_path = result.outcome.run_directory / "metrics.json"
    tampered = json.loads(metrics_path.read_text())
    tampered["methods"]["reranked"]["mrr"] = 1.0
    metrics_path.write_text(json.dumps(tampered, sort_keys=True) + "\n")
    validation = validate_run(result.outcome.run_directory, plan=plan, recompute=validate)
    assert validation["integrity_status"] == "fail"
    assert "derived artifact metrics.json does not recompute" in validation["integrity_failures"]


def test_no_answers_writes_no_metrics_and_secret_like_empty_query_is_not_persisted(
    tmp_path: Path,
) -> None:
    context = _empty_context(tmp_path)
    spec = RetrievalSpec()
    secret_query = "token sk-proj-" + "A" * 32
    queries = (RetrievalQuery("empty", secret_query, "segment"),)
    plan = _plan(spec, queries, context, answers=None)

    result = run_retrieval(
        plan,
        tmp_path / "run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
    )

    assert result.outcome.final_state == "pass"
    assert result.metrics is None
    assert not (result.outcome.run_directory / "metrics.json").exists()
    assert result.outcome.receipt["security"]["secret_match_count"] == 0


def test_no_planned_queries_is_a_successful_zero_work_run(tmp_path: Path) -> None:
    context, earlier_runs = _receipted_empty_context(tmp_path)
    spec = RetrievalSpec()
    queries: tuple[RetrievalQuery, ...] = ()
    plan = RunPlan(
        run_id="retrieval-zero-work",
        mode="build",
        steps=("retrieve",),
        retrieval=retrieval_run_plan_facts(spec, queries, context),
        earlier_runs=earlier_runs,
    )

    result = run_retrieval(
        plan,
        tmp_path / "run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
    )

    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["counts"]["planned"] == 0
    assert result.outcome.receipt["counts"]["failed"] == 0
    assert result.metrics is None


def test_answers_must_cover_exactly_the_planned_queries(tmp_path: Path) -> None:
    context = _empty_context(tmp_path)
    queries = (RetrievalQuery("q1", "find it", "segment"),)
    with pytest.raises(PlanError, match="exactly"):
        retrieval_run_plan_facts(
            RetrievalSpec(),
            queries,
            context,
            answers={"another-query": ()},
        )


def test_required_provider_failure_resumes_and_nonempty_rebuild_calls_no_provider(
    tmp_path: Path,
) -> None:
    context = _one_artifact_context(tmp_path)
    spec = RetrievalSpec()
    queries = (RetrievalQuery("artifact", "find it", "artifact"),)
    answers = {"artifact": ("artifact-1",)}
    plan = _plan(spec, queries, context, answers=answers)
    output = tmp_path / "run"
    failing = _FakeDense(fail=True)

    first = run_retrieval(
        plan,
        output,
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers_with_dense(failing),
        answers=answers,
    )

    assert first.outcome.final_state == "fail"
    assert first.outcome.receipt["counts"]["failed"] == 1
    assert first.outcome.receipt["provider"]["failures"] == 1
    assert not output.exists()
    assert failing.calls

    healthy = _FakeDense()
    resumed = run_retrieval(
        plan,
        output,
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers_with_dense(healthy),
        answers=answers,
    )

    assert resumed.outcome.final_state == "pass"
    assert resumed.outcome.receipt["counts"]["completed"] == 1
    assert resumed.outcome.receipt["provider"]["calls"] == 1
    assert [(hit.method, hit.rank, hit.target_id) for hit in resumed.hits] == [("dense", 1, "artifact-1")]
    assert resumed.metrics is not None
    assert resumed.metrics["methods"]["dense"]["recall_at_1"] == 1.0
    assert healthy.calls

    rebuilt = tmp_path / "rebuilt"
    report = rebuild_run(
        output,
        rebuilt,
        rebuild=rebuild_retrieval(
            spec,
            queries,
            context,
            answers=answers,
            providers=_providers(),
        ),
    )
    assert report["provider_invoked"] is False
    assert (
        pq.read_table(rebuilt / RETRIEVAL_HIT_TABLE).to_pylist()
        == pq.read_table(output / RETRIEVAL_HIT_TABLE).to_pylist()
    )

    hit_path = output / RETRIEVAL_HIT_TABLE
    hit_table = pq.read_table(hit_path)
    rank_index = hit_table.schema.get_field_index("rank")
    pq.write_table(
        hit_table.set_column(rank_index, "rank", pa.array([2], type=pa.int64())),
        hit_path,
    )
    validation = validate_run(
        output,
        plan=plan,
        recompute=recompute_retrieval(spec, queries, context, answers=answers),
    )
    assert "derived artifact retrieval/hits.parquet does not recompute" in validation["integrity_failures"]


def test_later_leg_failure_counts_persisted_successes_and_failing_attempt(
    tmp_path: Path,
) -> None:
    context = _one_artifact_context(tmp_path)
    spec = RetrievalSpec()
    queries = (RetrievalQuery("segment", "find it", "segment"),)
    plan = _plan(spec, queries, context, answers=None)
    dense = _FakeDense()
    sparse = _FailingSparseAfterDocument()

    result = run_retrieval(
        plan,
        tmp_path / "run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers_with_later_sparse_failure(dense, sparse),
    )

    assert result.outcome.final_state == "fail"
    assert dense.calls
    assert [task for task, _texts in sparse.calls] == ["document", "query"]
    assert result.outcome.receipt["provider"]["calls"] == 2
    assert result.outcome.receipt["provider"]["failures"] == 1


def test_filtered_nonempty_universe_is_successful_empty_with_recomputed_exclusion(
    tmp_path: Path,
) -> None:
    context = _one_artifact_context(tmp_path)
    from spicy_regs.docpipeline.retrieval import FilterRequest

    spec = RetrievalSpec(filters=(FilterRequest("identity", ("another-artifact",)),))
    queries = (RetrievalQuery("artifact", "find it", "artifact"),)
    answers = {"artifact": ()}
    plan = _plan(spec, queries, context, answers=answers)

    result = run_retrieval(
        plan,
        tmp_path / "run",
        spec=spec,
        queries=queries,
        context=context,
        providers=_providers(),
        answers=answers,
    )

    assert result.outcome.final_state == "pass"
    assert result.outcome.receipt["counts"]["empty"] == 1
    assert result.hits == ()
    assert [(row.target_id, row.filter, row.reason) for row in result.exclusions] == [
        ("artifact-1", "identity", "mismatch-identity")
    ]

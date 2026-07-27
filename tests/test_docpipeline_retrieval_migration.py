"""Hermetic Step 5 retrieval migration parity.

The fixture keeps the predecessor's candidate identity space.  It sends those
IDs through the public v3 pure functions and provider-free rerank rebuild.  The
test never substitutes v3 processing IDs, calls a provider, or reads ignored
local output.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import re
import textwrap
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from spicy_regs.corpora import segmentation_experiment as legacy_dense
from spicy_regs.corpora import segmentation_rerank as legacy_rerank
from spicy_regs.corpora import segmentation_sparse_retrieval as legacy_sparse
from spicy_regs.docpipeline import retrieval as v3_retrieval
from spicy_regs.docpipeline.retrieval import (
    DEFAULT_DENSE_REVISION,
    DEFAULT_RERANK_REVISION,
    DENSE_MODEL_ID,
    IR_MEASURES_VERSION,
    RERANK_BATCH_SIZE,
    RERANK_INPUT_POLICY,
    RERANK_MAX_SEQ_LENGTH,
    RERANK_MODEL_ID,
    RETRIEVAL_CANDIDATE_LIMIT,
    RETRIEVAL_FUSION_INPUT_DEPTH,
    RETRIEVAL_RERANK_DEPTH,
    RETRIEVAL_RRF_K,
    SPARSE_MODEL_ID,
    RerankScoreRow,
    RetrievalHit,
    RetrievalQuery,
    SparseVector,
    build_retrieval_metric_inputs,
    fuse_rrf,
    rank_dense_vectors,
    rank_sparse_vectors,
    rebuild_reranked_hits,
    retrieval_metrics,
    write_rerank_score_rows,
)
from spicy_regs.ontology.common import canonical_json

FIXTURE_DIR = Path(__file__).parent / "fixtures"
PARITY_FIXTURE = FIXTURE_DIR / "docpipeline_retrieval_migration_v1.json"
DIFFERENCES_FIXTURE = FIXTURE_DIR / "docpipeline_step5_expected_differences_v1.json"

ALLOWED_DIFFERENCE_KINDS = frozenset(
    {
        "run_file_layout",
        "row_layout",
        "container_layout",
    }
)
FORBIDDEN_DIFFERENCE_TERMS = frozenset(
    {
        "candidate_id",
        "candidate_ids",
        "rank",
        "metric",
        "token_count",
        "model_pin",
        "constant",
        "tie_break",
        "containment",
    }
)
DENSE_SCORE_TOLERANCE = 1e-12


@pytest.fixture(scope="module")
def parity_fixture() -> dict[str, Any]:
    value = json.loads(PARITY_FIXTURE.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def difference_ledger() -> dict[str, Any]:
    value = json.loads(DIFFERENCES_FIXTURE.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _candidate(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_id": record["target_id"],
        "artifact_id": record["artifact_id"],
        "segment_id": record["target_id"],
        "source_table": record["source_table"],
        "subject_id": record["subject_id"],
        "artifact_digest": record["artifact_digest"],
    }


def _hit(
    record: dict[str, Any],
    *,
    method: str,
    rank: int,
    score: float,
) -> RetrievalHit:
    return RetrievalHit(
        work_id="legacy-dense-work",
        query_id="gold-hermetic",
        level="segment",
        method=method,
        **_candidate(record),
        rank=rank,
        candidate_universe_size=3,
        candidate_input_size=3,
        candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
        score=score,
        score_kind="cosine" if method == "dense" else "sparse-dot",
        dense_rank=rank if method == "dense" else None,
        dense_score=score if method == "dense" else None,
        sparse_rank=rank if method == "sparse" else None,
        sparse_score=score if method == "sparse" else None,
        model_id=DENSE_MODEL_ID if method == "dense" else SPARSE_MODEL_ID,
        model_revision=(DEFAULT_DENSE_REVISION if method == "dense" else SPARSE_MODEL_ID.rsplit("@", 1)[-1]),
    )


def _dense_hits(fixture: dict[str, Any]) -> tuple[RetrievalHit, ...]:
    candidates = fixture["segment_candidates"]
    ranked = rank_dense_vectors(
        tuple(row["target_id"] for row in candidates),
        tuple(row["dense_vector"] for row in candidates),
        fixture["query"]["dense_vector"],
        limit=RETRIEVAL_CANDIDATE_LIMIT,
    )
    by_id = {row["target_id"]: row for row in candidates}
    return tuple(
        _hit(by_id[target_id], method="dense", rank=rank, score=score)
        for rank, (target_id, score) in enumerate(ranked, start=1)
    )


def _sparse_hits(fixture: dict[str, Any]) -> tuple[RetrievalHit, ...]:
    candidates = fixture["segment_candidates"]
    ranked = rank_sparse_vectors(
        tuple(row["target_id"] for row in candidates),
        tuple(
            SparseVector(
                dimensions=int(row["sparse_vector"]["dimensions"]),
                indices=tuple(row["sparse_vector"]["indices"]),
                values=tuple(row["sparse_vector"]["values"]),
            )
            for row in candidates
        ),
        SparseVector(
            dimensions=int(fixture["query"]["sparse_vector"]["dimensions"]),
            indices=tuple(fixture["query"]["sparse_vector"]["indices"]),
            values=tuple(fixture["query"]["sparse_vector"]["values"]),
        ),
        limit=RETRIEVAL_CANDIDATE_LIMIT,
    )
    by_id = {row["target_id"]: row for row in candidates}
    return tuple(
        _hit(by_id[target_id], method="sparse", rank=rank, score=score)
        for rank, (target_id, score) in enumerate(ranked, start=1)
    )


def _contains(record: dict[str, Any], gold: dict[str, Any]) -> bool:
    return any(
        one["source_field"] == gold["source_field"]
        and int(one["start_char"]) <= int(gold["start_char"])
        and int(one["end_char"]) >= int(gold["end_char"])
        for one in record["slices"]
    )


def _overlaps(record: dict[str, Any], gold: dict[str, Any]) -> bool:
    return any(
        one["source_field"] == gold["source_field"]
        and int(one["start_char"]) < int(gold["end_char"])
        and int(one["end_char"]) > int(gold["start_char"])
        for one in record["slices"]
    )


def _answers(fixture: dict[str, Any], *, contain: bool = True) -> dict[str, tuple[str, ...]]:
    predicate = _contains if contain else _overlaps
    gold = fixture["gold_span"]
    return {
        fixture["query"]["query_id"]: tuple(
            sorted(row["target_id"] for row in fixture["segment_candidates"] if predicate(row, gold))
        )
    }


def _rerank_group_facts(
    candidates: tuple[RetrievalHit, ...],
    fixture: dict[str, Any],
) -> tuple[str, str, str, str]:
    query = fixture["query"]
    candidate_ids = [hit.target_id for hit in candidates]
    candidate_ids_sha256 = hashlib.sha256(canonical_json(candidate_ids).encode()).hexdigest()
    group_key = hashlib.sha256(
        canonical_json(
            {
                "source_work_id": "legacy-dense-work",
                "query_id": query["query_id"],
                "level": "segment",
            }
        ).encode()
    ).hexdigest()
    request_sha256 = hashlib.sha256(
        canonical_json(
            {
                "input_policy": RERANK_INPUT_POLICY,
                "model_id": RERANK_MODEL_ID,
                "max_seq_length": RERANK_MAX_SEQ_LENGTH,
                "batch_size": RERANK_BATCH_SIZE,
                "query_sha256": hashlib.sha256(query["text"].encode()).hexdigest(),
                "candidates": [
                    {
                        "target_id": hit.target_id,
                        "input_sha256": hashlib.sha256(fixture["candidate_text"][hit.target_id].encode()).hexdigest(),
                    }
                    for hit in candidates
                ],
            }
        ).encode()
    ).hexdigest()
    work_id = (
        "rerank_work_"
        + hashlib.sha256(
            canonical_json(
                {
                    "group_key": group_key,
                    "candidate_ids_sha256": candidate_ids_sha256,
                    "request_sha256": request_sha256,
                }
            ).encode()
        ).hexdigest()[:32]
    )
    return group_key, candidate_ids_sha256, request_sha256, work_id


def _rerank_rows(
    dense_hits: tuple[RetrievalHit, ...],
    fixture: dict[str, Any],
) -> tuple[RerankScoreRow, ...]:
    candidates = tuple(sorted(dense_hits, key=lambda hit: (hit.rank, hit.target_id)))[:RETRIEVAL_RERANK_DEPTH]
    group_key, candidate_ids_sha256, request_sha256, work_id = _rerank_group_facts(
        candidates,
        fixture,
    )
    score_by_id = {row["target_id"]: float(row["score"]) for row in fixture["stored_rerank_scores"]}
    token_by_id = {row["target_id"]: int(row["untruncated_token_count"]) for row in fixture["stored_rerank_scores"]}
    ranked = sorted(candidates, key=lambda hit: (-score_by_id[hit.target_id], hit.target_id))
    rank_by_id = {hit.target_id: rank for rank, hit in enumerate(ranked, start=1)}
    call = {
        "provider": "sentence-transformers",
        "operation": "rerank",
        "status": "completed",
        "model_id": RERANK_MODEL_ID,
        "revision": DEFAULT_RERANK_REVISION,
        "tokenizer_id": fixture["rerank_tokenizer_id"],
        "candidate_count": len(candidates),
        "max_input_tokens": RERANK_MAX_SEQ_LENGTH,
        "provider_invoked": True,
        "attempt_count": 1,
        "retry_count": 0,
        "package_name": "sentence-transformers",
        "package_version": "5.6.1",
        "tokenizer_package_version": "4.57.6",
    }
    call_json = canonical_json(call)
    rows = []
    for index, hit in enumerate(candidates):
        text = fixture["candidate_text"][hit.target_id]
        token_count = token_by_id[hit.target_id]
        rows.append(
            RerankScoreRow(
                work_id=work_id,
                group_key=group_key,
                source_work_id="legacy-dense-work",
                query_id=fixture["query"]["query_id"],
                level="segment",
                candidate_ids_sha256=candidate_ids_sha256,
                request_sha256=request_sha256,
                candidate_index=index,
                candidate_count=len(candidates),
                target_id=hit.target_id,
                artifact_id=hit.artifact_id,
                segment_id=hit.target_id,
                source_table=hit.source_table,
                subject_id=hit.subject_id,
                artifact_digest=hit.artifact_digest,
                candidate_universe_size=hit.candidate_universe_size,
                dense_candidate_input_size=hit.candidate_input_size,
                dense_rank=hit.rank,
                dense_score=hit.score,
                query_input_sha256=hashlib.sha256(fixture["query"]["text"].encode()).hexdigest(),
                query_text=fixture["query"]["text"],
                input_policy=RERANK_INPUT_POLICY,
                input_sha256=hashlib.sha256(text.encode()).hexdigest(),
                input_text=text,
                rerank_score=score_by_id[hit.target_id],
                rerank_rank=rank_by_id[hit.target_id],
                model_id=RERANK_MODEL_ID,
                model_revision=DEFAULT_RERANK_REVISION,
                tokenizer_id=fixture["rerank_tokenizer_id"],
                tokenizer_package_version="4.57.6",
                untruncated_token_count=token_count,
                input_limit=RERANK_MAX_SEQ_LENGTH,
                would_truncate=token_count > RERANK_MAX_SEQ_LENGTH,
                token_audit_status="exact-untruncated-pair-tokenizer",
                provider="sentence-transformers",
                package_name="sentence-transformers",
                package_version="5.6.1",
                operation="rerank",
                call_status="completed",
                provider_invoked=True,
                group_attempt=1,
                provider_attempt_count=1,
                retry_count=0,
                call_input_index=index,
                call_json=call_json,
            )
        )
    return tuple(rows)


def _reranked_hits(
    dense_hits: tuple[RetrievalHit, ...],
    fixture: dict[str, Any],
    tmp_path: Path,
) -> tuple[RetrievalHit, ...]:
    rows = _rerank_rows(dense_hits, fixture)
    write_rerank_score_rows(tmp_path, rows)
    return rebuild_reranked_hits(
        dense_hits,
        fixture["candidate_text"],
        query=RetrievalQuery(
            fixture["query"]["query_id"],
            fixture["query"]["text"],
            "segment",
        ),
        source_work_id="legacy-dense-work",
        run_directory=tmp_path,
    )


def _id_rank_score(hits: tuple[RetrievalHit, ...]) -> list[list[Any]]:
    return [[hit.target_id, hit.rank, hit.score] for hit in hits]


def _assert_exact(actual: Any, expected: Any, message: str) -> None:
    if actual != expected:
        raise AssertionError(message)


def _assert_dense_tolerance(
    actual: list[list[Any]],
    expected: list[list[Any]],
    *,
    tolerance: float,
) -> None:
    if tolerance != DENSE_SCORE_TOLERANCE:
        raise AssertionError("the dense replay tolerance is frozen at 1e-12")
    _assert_exact(
        [(row[0], row[1]) for row in actual],
        [(row[0], row[1]) for row in expected],
        "dense candidate IDs or ranks drifted",
    )
    for observed, frozen in zip(actual, expected, strict=True):
        if abs(float(observed[2]) - float(frozen[2])) > tolerance:
            raise AssertionError("dense score exceeded the frozen tolerance")


def _assert_binding_snapshot(snapshot: dict[str, Any]) -> None:
    expected = {
        "candidate_limit": 200,
        "rrf_k": 60,
        "fusion_depth": 200,
        "rerank_depth": 50,
        "rerank_input_limit": 4096,
        "rerank_batch_size": 16,
        "ir_measures_version": "0.4.3",
        "dense_model_id": ("sentence-transformers:BAAI/bge-base-en-v1.5@a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"),
        "sparse_model_id": (
            "sentence-transformers-sparse:tomaarsen/splade-modernbert-base-miriad"
            "@c640ce28f7c4f4593ddba1b3855988f03a3d9cdc"
        ),
        "rerank_model_id": ("sentence-transformers:BAAI/bge-reranker-v2-m3@953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"),
    }
    _assert_exact(snapshot, expected, "a binding retrieval constant or model pin drifted")


def _written_parquet_file(
    module: ModuleType,
    *,
    builder_name: str,
    columns_name: str,
) -> str:
    """Read the output filename paired with one legacy schema constant."""
    source = textwrap.dedent(inspect.getsource(getattr(module, builder_name)))
    tree = ast.parse(source)
    filenames: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        columns = next(
            (keyword.value for keyword in node.keywords if keyword.arg == "columns"),
            None,
        )
        if not isinstance(columns, ast.Name) or columns.id != columns_name:
            continue
        path = node.args[0]
        if (
            isinstance(path, ast.BinOp)
            and isinstance(path.op, ast.Div)
            and isinstance(path.right, ast.Constant)
            and isinstance(path.right.value, str)
            and path.right.value.endswith(".parquet")
        ):
            filenames.append(path.right.value)
    assert len(filenames) == 1, (
        f"{module.__name__}.{builder_name} must write exactly one {columns_name} Parquet output, observed {filenames}"
    )
    return filenames[0]


def _schema_proof(
    columns: Sequence[str] | Sequence[tuple[str, str]],
) -> dict[str, Any]:
    normalized = [list(column) if isinstance(column, tuple) else str(column) for column in columns]
    return {
        "field_count": len(normalized),
        "fields_sha256": hashlib.sha256(canonical_json(normalized).encode()).hexdigest(),
    }


def _mapping_shape(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _mapping_shape(item) for key, item in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_mapping_shape(item) for item in value]
    return type(value).__name__


def _observed_representation_differences(
    fixture: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    dense_file = _written_parquet_file(
        legacy_dense,
        builder_name="build_segmentation_experiment",
        columns_name="EMBEDDING_CACHE_COLUMNS",
    )
    sparse_file = _written_parquet_file(
        legacy_sparse,
        builder_name="build_sparse_retrieval_comparison",
        columns_name="SPARSE_EMBEDDING_COLUMNS",
    )
    rerank_request_file = _written_parquet_file(
        legacy_rerank,
        builder_name="build_rerank_experiment",
        columns_name="RERANK_REQUEST_COLUMNS",
    )
    rerank_candidate_file = _written_parquet_file(
        legacy_rerank,
        builder_name="build_rerank_experiment",
        columns_name="RERANKED_CANDIDATE_COLUMNS",
    )
    dense_metrics_file = _written_parquet_file(
        legacy_dense,
        builder_name="build_segmentation_experiment",
        columns_name="CONFIG_METRIC_COLUMNS",
    )
    sparse_metrics_file = _written_parquet_file(
        legacy_sparse,
        builder_name="build_sparse_retrieval_comparison",
        columns_name="METRIC_COLUMNS",
    )
    rerank_metrics_file = _written_parquet_file(
        legacy_rerank,
        builder_name="build_rerank_experiment",
        columns_name="RERANK_METRIC_COLUMNS",
    )
    actual_metrics = retrieval_metrics(
        _dense_hits(fixture),
        _answers(fixture),
        methods=("dense",),
    )
    return {
        "provider-output-file-layout": {
            "old_files": sorted(
                (
                    dense_file,
                    sparse_file,
                    rerank_request_file,
                    rerank_candidate_file,
                )
            ),
            "new_files": sorted(
                (
                    v3_retrieval.DENSE_EMBEDDING_TABLE,
                    v3_retrieval.SPARSE_EMBEDDING_TABLE,
                    v3_retrieval.RERANK_SCORE_TABLE,
                )
            ),
        },
        "rerank-row-layout": {
            "old_request_file": rerank_request_file,
            "old_request_schema": _schema_proof(legacy_rerank.RERANK_REQUEST_COLUMNS),
            "old_candidate_file": rerank_candidate_file,
            "old_candidate_schema": _schema_proof(legacy_rerank.RERANKED_CANDIDATE_COLUMNS),
            "new_score_file": v3_retrieval.RERANK_SCORE_TABLE,
            "new_storage_schema": _schema_proof(v3_retrieval.RERANK_SCORE_COLUMNS),
            "new_row_schema": _schema_proof(tuple(field.name for field in dataclasses.fields(RerankScoreRow))),
        },
        "derived-result-container-layout": {
            "old_metric_files": {
                dense_metrics_file: _schema_proof(legacy_dense.CONFIG_METRIC_COLUMNS),
                sparse_metrics_file: _schema_proof(legacy_sparse.METRIC_COLUMNS),
                rerank_metrics_file: _schema_proof(legacy_rerank.RERANK_METRIC_COLUMNS),
            },
            "new_result_shape": _mapping_shape(actual_metrics),
        },
    }


def _assert_ledger_exact(
    ledger: dict[str, Any],
    observed: dict[str, dict[str, Any]],
) -> None:
    differences = ledger["differences"]
    identifiers = [one["id"] for one in differences]
    assert len(identifiers) == len(set(identifiers))
    assert set(identifiers) == set(observed)
    for one in differences:
        assert one["kind"] in ALLOWED_DIFFERENCE_KINDS
        assert one["old"] and one["new"] and one["reason"]
        assert one["proof"] == observed[one["id"]]
        explanation = {key: one[key] for key in ("id", "kind", "old", "new", "reason")}
        normalized = re.sub(
            r"[^a-z0-9]+",
            "_",
            canonical_json(explanation).lower(),
        ).strip("_")
        padded = f"_{normalized}_"
        assert not any(f"_{term}_" in padded for term in FORBIDDEN_DIFFERENCE_TERMS)


def test_hermetic_legacy_ids_flow_through_dense_sparse_rrf_rerank_and_metrics(
    parity_fixture: dict[str, Any],
    tmp_path: Path,
) -> None:
    dense = _dense_hits(parity_fixture)
    sparse = _sparse_hits(parity_fixture)
    hybrid = fuse_rrf(dense, sparse)
    reranked = _reranked_hits(dense, parity_fixture, tmp_path)
    expected = parity_fixture["expected"]

    _assert_dense_tolerance(
        _id_rank_score(dense),
        expected["dense"],
        tolerance=DENSE_SCORE_TOLERANCE,
    )
    _assert_exact(_id_rank_score(sparse), expected["sparse"], "sparse replay drifted")
    _assert_exact(_id_rank_score(hybrid), expected["hybrid_rrf"], "RRF replay drifted")
    _assert_exact(_id_rank_score(reranked), expected["reranked"], "rerank replay drifted")
    assert all(hit.target_id.startswith("experiment_segment_") for hit in (*dense, *sparse, *hybrid, *reranked))

    hits = (*dense, *sparse, *hybrid, *reranked)
    answers = _answers(parity_fixture)
    measured = retrieval_metrics(
        hits,
        answers,
        methods=("dense", "sparse", "hybrid-rrf", "reranked"),
    )
    _assert_exact(measured["methods"], expected["metrics"], "recomputed metrics drifted")
    inputs = build_retrieval_metric_inputs(
        hits,
        answers,
        methods=("dense", "sparse", "hybrid-rrf", "reranked"),
    )
    assert inputs.qrels == {
        "gold-hermetic": {"experiment_segment_b": 1},
    }


def test_hermetic_whole_artifact_replay_keeps_legacy_vector_ids(
    parity_fixture: dict[str, Any],
) -> None:
    artifact = parity_fixture["artifact_replay"]
    ranked = rank_dense_vectors(
        tuple(row["target_id"] for row in artifact["candidates"]),
        tuple(row["vector"] for row in artifact["candidates"]),
        artifact["query_vector"],
        limit=RETRIEVAL_CANDIDATE_LIMIT,
    )

    assert [[target_id, rank, score] for rank, (target_id, score) in enumerate(ranked, start=1)] == artifact["expected"]
    assert all(target_id.startswith("artifact_vector_") for target_id, _ in ranked)


def test_expected_differences_are_exact_closed_and_representational(
    difference_ledger: dict[str, Any],
    parity_fixture: dict[str, Any],
) -> None:
    observed = _observed_representation_differences(parity_fixture)
    _assert_ledger_exact(difference_ledger, observed)
    assert tuple(name for name, _ in v3_retrieval.RERANK_SCORE_COLUMNS) == tuple(
        field.name for field in dataclasses.fields(RerankScoreRow)
    )


def test_actual_unlisted_file_and_field_mutations_fail_the_closed_ledger(
    difference_ledger: dict[str, Any],
    parity_fixture: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as mutation:
        mutation.setattr(
            v3_retrieval,
            "RERANK_SCORE_TABLE",
            "retrieval/unlisted-provider-output.parquet",
        )
        with pytest.raises(AssertionError):
            _assert_ledger_exact(
                difference_ledger,
                _observed_representation_differences(parity_fixture),
            )
    with monkeypatch.context() as mutation:
        mutation.setattr(
            v3_retrieval,
            "RERANK_SCORE_COLUMNS",
            (
                *v3_retrieval.RERANK_SCORE_COLUMNS,
                ("unlisted_output_field", "string"),
            ),
        )
        with pytest.raises(AssertionError):
            _assert_ledger_exact(
                difference_ledger,
                _observed_representation_differences(parity_fixture),
            )


def test_candidate_rank_and_tolerance_mutations_fail(
    parity_fixture: dict[str, Any],
) -> None:
    dense = _id_rank_score(_dense_hits(parity_fixture))
    altered_rank = json.loads(canonical_json(parity_fixture["expected"]["dense"]))
    altered_rank[0][1] = 2
    with pytest.raises(AssertionError, match="IDs or ranks"):
        _assert_dense_tolerance(dense, altered_rank, tolerance=DENSE_SCORE_TOLERANCE)
    with pytest.raises(AssertionError, match="frozen at"):
        _assert_dense_tolerance(dense, parity_fixture["expected"]["dense"], tolerance=1e-9)


def test_constant_model_pin_and_tie_break_mutations_fail() -> None:
    snapshot = {
        "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
        "rrf_k": RETRIEVAL_RRF_K,
        "fusion_depth": RETRIEVAL_FUSION_INPUT_DEPTH,
        "rerank_depth": RETRIEVAL_RERANK_DEPTH,
        "rerank_input_limit": RERANK_MAX_SEQ_LENGTH,
        "rerank_batch_size": RERANK_BATCH_SIZE,
        "ir_measures_version": IR_MEASURES_VERSION,
        "dense_model_id": DENSE_MODEL_ID,
        "sparse_model_id": SPARSE_MODEL_ID,
        "rerank_model_id": RERANK_MODEL_ID,
    }
    _assert_binding_snapshot(snapshot)
    for field, value in (
        ("rrf_k", 61),
        ("fusion_depth", 199),
        ("rerank_depth", 49),
        ("dense_model_id", f"{DENSE_MODEL_ID}-drift"),
    ):
        changed = dict(snapshot)
        changed[field] = value
        with pytest.raises(AssertionError, match="binding"):
            _assert_binding_snapshot(changed)

    tied = rank_dense_vectors(("z", "a"), ((1.0, 0.0), (1.0, 0.0)), (1.0, 0.0))
    _assert_exact([target_id for target_id, _ in tied], ["a", "z"], "tie break drifted")
    with pytest.raises(AssertionError, match="tie break"):
        _assert_exact([target_id for target_id, _ in tied], ["z", "a"], "tie break drifted")


def test_enclosure_and_receipt_metric_mutations_fail(
    parity_fixture: dict[str, Any],
    tmp_path: Path,
) -> None:
    answers = _answers(parity_fixture)
    overlap_answers = _answers(parity_fixture, contain=False)
    assert answers == {"gold-hermetic": ("experiment_segment_b",)}
    with pytest.raises(AssertionError):
        _assert_exact(overlap_answers, answers, "overlap cannot replace enclosure")

    dense = _dense_hits(parity_fixture)
    sparse = _sparse_hits(parity_fixture)
    reranked = _reranked_hits(dense, parity_fixture, tmp_path)
    generated = retrieval_metrics(
        (*dense, *sparse, *fuse_rrf(dense, sparse), *reranked),
        answers,
        methods=("dense", "sparse", "hybrid-rrf", "reranked"),
    )["methods"]
    with pytest.raises(AssertionError, match="recomputed metrics"):
        _assert_exact(
            parity_fixture["stored_receipt_metrics"],
            generated,
            "recomputed metrics drifted",
        )


def test_rerank_rows_preserve_legacy_identity_and_stored_token_facts(
    parity_fixture: dict[str, Any],
) -> None:
    rows = _rerank_rows(_dense_hits(parity_fixture), parity_fixture)
    expected_tokens = {
        row["target_id"]: row["untruncated_token_count"] for row in parity_fixture["stored_rerank_scores"]
    }

    assert {row.target_id: row.untruncated_token_count for row in rows} == expected_tokens
    assert all(
        row.target_id == row.segment_id
        and row.target_id.startswith("experiment_segment_")
        and row.rerank_rank
        == next(item[1] for item in parity_fixture["expected"]["reranked"] if item[0] == row.target_id)
        for row in rows
    )
    assert max(row.untruncated_token_count for row in rows) <= RERANK_MAX_SEQ_LENGTH


def test_production_docpipeline_has_no_legacy_identity_or_corpora_import() -> None:
    root = Path(__file__).parents[1] / "src" / "spicy_regs" / "docpipeline"
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "experiment_segment_" not in source
        assert "spicy_regs.corpora" not in source


def test_legacy_runners_and_commands_remain_present() -> None:
    root = Path(__file__).parents[1]
    modules = (
        "segmentation_experiment.py",
        "segmentation_sparse_retrieval.py",
        "segmentation_rerank.py",
        "artifact_retrieval_baseline.py",
    )
    assert all((root / "src" / "spicy_regs" / "corpora" / module).is_file() for module in modules)
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    for command in (
        "run-segmentation-experiment",
        "run-segmentation-sparse-retrieval",
        "run-segmentation-rerank",
        "run-artifact-retrieval-baseline",
    ):
        assert f"{command} = " in pyproject

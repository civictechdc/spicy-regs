"""Loud opt-in Step 5 retrieval parity over the frozen local outputs.

Set ``SPICY_REGS_FROZEN_RETRIEVAL_ROOT`` to the directory containing the
seven immutable output directories. Normal CI skips precisely. The replay
uses stored vectors and scores without invoking a provider; the separate fresh
leg uses the pinned local adapters and v3 identities.
"""

from __future__ import annotations

import gc
import hashlib
import json
import os
import subprocess
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.corpora.document_acceptance_scope import load_document_acceptance_scope
from spicy_regs.corpora.segmentation_experiment import TiktokenCounter
from spicy_regs.docpipeline.adapters.sentence_transformers import (
    SentenceTransformersDenseEmbedder,
    SentenceTransformersReranker,
    SentenceTransformersSparseEncoder,
    SparseEncodingResult,
)
from spicy_regs.docpipeline.retrieval import (
    DEFAULT_DENSE_REVISION,
    DEFAULT_RERANK_REVISION,
    DENSE_MODEL_ID,
    RERANK_BATCH_SIZE,
    RERANK_INPUT_POLICY,
    RERANK_MAX_SEQ_LENGTH,
    RERANK_MODEL_ID,
    RETRIEVAL_CANDIDATE_LIMIT,
    SPARSE_MODEL_ID,
    DenseSourceField,
    RerankScoreRow,
    RetrievalHit,
    RetrievalQuery,
    SparseVector,
    compose_dense_vector,
    dense_source_fields_from_segments,
    derive_dense_semantic_units,
    fuse_rrf,
    rank_dense_vectors,
    rank_sparse_vectors,
    rebuild_reranked_hits,
    retrieval_metrics,
    write_rerank_score_rows,
)
from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifacts
from spicy_regs.docpipeline.source import STEP4_ACTIVE_SOURCE_TABLES, build_source_artifacts
from spicy_regs.ontology.common import canonical_json, read_parquet_rows

ROOT_ENV = "SPICY_REGS_FROZEN_RETRIEVAL_ROOT"
DIFFERENCE_LEDGER = Path(__file__).parent / "fixtures" / "docpipeline_step5_expected_differences_v1.json"

DATASET_DIR = "segmented-real-data-evaluation-v2"
SCOPE_DIR = "document-acceptance-scope-v2"
DENSE_DIR = "segmentation-experiment-document-bge-v3"
DENSE_AUDIT_DIR = "segmentation-embedding-audit-document-bge-v3"
SPARSE_DIR = "segmentation-sparse-retrieval-document-bge-structure-overlap-1800-v1"
RERANK_DIR = "segmentation-rerank-document-bge-structure-overlap-1800-top50-v2"
ARTIFACT_DIR = "artifact-retrieval-document-bge-v2"

DATASET_ID = "segmentation_eval_627ba96e04872d870a2ccd6e"
SCOPE_ID = "document_scope_6b4f8a64ba43fc1b8e0a7e05"
DENSE_ID = "segmentation_experiment_de7d119e838ac153a0980337"
DENSE_AUDIT_ID = "segmentation_embedding_audit_37539c04f5d4c180a8fd8225"
SPARSE_ID = "sparse_retrieval_cd34c2a7d29e60f6a012d687"
RERANK_ID = "segmentation_rerank_31d7e2a8ec51280f92896ed3"
ARTIFACT_ID = "artifact_retrieval_adc1bce8d43d1a6ca025f445"
SELECTED_CONFIG = "structure-overlap-1800"
SPARSE_CHECKPOINT_BATCH_SIZE = 32

DENSE_SCORE_TOLERANCE = 1e-12
FRESH_DENSE_SCORE_TOLERANCE = 1e-6
FRESH_SPARSE_SCORE_TOLERANCE = 1e-5
FRESH_RERANK_SCORE_TOLERANCE = 1e-5

PRIMARY_FILE_SHA256 = {
    f"{DATASET_DIR}/segmentation-evaluation-manifest.json": (
        "b1827a5bef8fef5d2a3e10d76e22429537be73b8b768cda726fbac6cd421039e"
    ),
    f"{DATASET_DIR}/segmentation-evaluation-receipt.json": (
        "19aae874169280746f0b8b2ff6c4a4ba3b6be83926302c68a374ff381cdc82c1"
    ),
    f"{SCOPE_DIR}/document-acceptance-manifest.json": (
        "653c9c66101ee9ee7fbd79fa0805e3ad5e11d8d55d1d7d279174aa03500e92a6"
    ),
    f"{SCOPE_DIR}/document-acceptance-receipt.json": (
        "ffaa89ca5aa3ce5c171ec7fe391a39f1ac88240db14103844fbf99f4ae0c0dcc"
    ),
    f"{DENSE_DIR}/segmentation-experiment-manifest.json": (
        "1c9764b0df7985b60666ef1bc69f1ed7bfee4cbdcf1fa0036457118729f80a10"
    ),
    f"{DENSE_DIR}/segmentation-experiment-receipt.json": (
        "ebb71adadbac85e153c9d1986d7e5a6a75c6e8169bb950655cc51776f4d0b5aa"
    ),
    f"{DENSE_AUDIT_DIR}/segmentation-embedding-audit-manifest.json": (
        "cd485a6382b51ac660f9f1ec84ac7d2ca54859f4c9bb34dfd8d110d53cd2af9b"
    ),
    f"{DENSE_AUDIT_DIR}/segmentation-embedding-audit-receipt.json": (
        "5fc79c4ecbe282cf2e2b638d735fb90fa30a46d192e48ca43c2419a865069c38"
    ),
    f"{SPARSE_DIR}/segmentation-sparse-retrieval-manifest.json": (
        "3c9103500043588801a8199527234d72aafbfe443a589e643b93ecb3d6a4774d"
    ),
    f"{SPARSE_DIR}/segmentation-sparse-retrieval-receipt.json": (
        "d08c6dcf61d8eacc802e8697138fcea0eb77929075fc39461ad193326b9ac2c9"
    ),
    f"{RERANK_DIR}/segmentation-rerank-manifest.json": (
        "66ab41824af003a796ec8526fee8c4097ef563ac3a30be917b93cb554090cae0"
    ),
    f"{RERANK_DIR}/segmentation-rerank-receipt.json": (
        "633c66551ea0054b65ad394f3d5a104ce45aa44e47666c561219ab87e85ecdba"
    ),
    f"{ARTIFACT_DIR}/artifact-retrieval-manifest.json": (
        "d094048cba0ee0c707e878b38952925be582911255bd62690c338cd5ae7adc8a"
    ),
    f"{ARTIFACT_DIR}/artifact-retrieval-receipt.json": (
        "79338794a51554e5ed41be84fb228343db21b903dc74595c35c77a4eab9b31b9"
    ),
}

MANIFEST_FILES = {
    DATASET_DIR: "segmentation-evaluation-manifest.json",
    SCOPE_DIR: "document-acceptance-manifest.json",
    DENSE_DIR: "segmentation-experiment-manifest.json",
    DENSE_AUDIT_DIR: "segmentation-embedding-audit-manifest.json",
    SPARSE_DIR: "segmentation-sparse-retrieval-manifest.json",
    RERANK_DIR: "segmentation-rerank-manifest.json",
    ARTIFACT_DIR: "artifact-retrieval-manifest.json",
}

VALIDATOR_COMMANDS = (
    (
        "build-segmentation-evaluation",
        "validate",
        DATASET_DIR,
    ),
    (
        "build-document-acceptance-scope",
        "validate",
        DATASET_DIR,
        SCOPE_DIR,
    ),
    (
        "run-segmentation-experiment",
        "validate",
        DATASET_DIR,
        DENSE_DIR,
        "--scope-dir",
        SCOPE_DIR,
    ),
    (
        "audit-segmentation-embeddings",
        "validate",
        DATASET_DIR,
        DENSE_DIR,
        DENSE_AUDIT_DIR,
        "--auditor",
        "incumbent-bge",
        "--scope-dir",
        SCOPE_DIR,
    ),
    (
        "run-segmentation-sparse-retrieval",
        "validate",
        DATASET_DIR,
        DENSE_DIR,
        SPARSE_DIR,
        "--scope-dir",
        SCOPE_DIR,
    ),
    (
        "run-segmentation-rerank",
        "validate",
        DATASET_DIR,
        DENSE_DIR,
        RERANK_DIR,
        "--scope-dir",
        SCOPE_DIR,
    ),
    (
        "run-artifact-retrieval-baseline",
        "validate",
        DATASET_DIR,
        ARTIFACT_DIR,
        "--scope-dir",
        SCOPE_DIR,
    ),
)

METRIC_NAMES = (
    "recall_at_1",
    "recall_at_3",
    "recall_at_5",
    "recall_at_10",
    "recall_at_25",
    "recall_at_50",
    "recall_at_100",
    "recall_at_200",
    "precision_at_1",
    "precision_at_3",
    "precision_at_5",
    "precision_at_10",
    "mrr",
    "ndcg_at_5",
    "ndcg_at_10",
)


@dataclass(frozen=True)
class FrozenInputs:
    root: Path
    dataset: Path
    scope: Path
    dense: Path
    dense_audit: Path
    sparse: Path
    rerank: Path
    artifact: Path


@dataclass(frozen=True)
class Step4Inventory:
    artifacts: tuple[Any, ...]
    segments: tuple[Any, ...]
    candidates: tuple[dict[str, Any], ...]
    source_fields: tuple[DenseSourceField, ...]
    old_by_key: Mapping[tuple[str, int], dict[str, Any]]
    new_by_key: Mapping[tuple[str, int], Any]
    gold_rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class FreshResult:
    metrics: Mapping[str, Mapping[str, Mapping[str, float]]]
    score_drift: Mapping[str, float]
    compared_score_count: Mapping[str, int]
    dense_over_limit_count: int
    sparse_over_limit_count: int
    rerank_candidate_count: int
    rerank_truncated_count: int
    artifact_hit_count: int


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _assert_ledger_old_files_exist(frozen: FrozenInputs) -> None:
    ledger = _load_json(DIFFERENCE_LEDGER)
    file_layout = next(
        difference for difference in ledger["differences"] if difference["id"] == "provider-output-file-layout"
    )
    old_files = tuple(file_layout["proof"]["old_files"])
    legacy_roots = (frozen.dense, frozen.sparse, frozen.rerank)
    locations = {
        filename: tuple(directory.name for directory in legacy_roots if (directory / filename).is_file())
        for filename in old_files
    }
    assert set(locations) == set(old_files)
    assert all(len(directories) == 1 for directories in locations.values()), (
        f"ledger old filenames must exist once in the sealed legacy roots: {locations}"
    )


def _root() -> Path:
    raw = os.environ.get(ROOT_ENV)
    if raw is None:
        pytest.skip(
            f"{ROOT_ENV} is unset; set it to the local output directory to run "
            "the seven-validator, provider-free replay, and cached fresh-inference gate"
        )
    root = Path(raw).expanduser().resolve()
    missing = sorted(relative for relative in PRIMARY_FILE_SHA256 if not (root / relative).is_file())
    if missing:
        pytest.fail(f"{ROOT_ENV} is explicitly set to {raw!r}, but {root} lacks frozen inputs: {missing}")
    return root


def _verify_frozen_root(root: Path) -> FrozenInputs:
    for relative, expected in PRIMARY_FILE_SHA256.items():
        assert _sha256(root / relative) == expected, f"frozen primary identity drifted: {relative}"
    for directory, manifest_name in MANIFEST_FILES.items():
        manifest = _load_json(root / directory / manifest_name)
        inventory = manifest.get("nonmodel_artifacts", manifest.get("artifacts"))
        assert isinstance(inventory, dict) and inventory
        for name, record in inventory.items():
            path = root / directory / name
            assert path.is_file(), f"frozen manifest member is absent: {directory}/{name}"
            assert _sha256(path) == record["sha256"], f"frozen manifest member drifted: {directory}/{name}"

    dataset_manifest = _load_json(root / DATASET_DIR / MANIFEST_FILES[DATASET_DIR])
    scope_manifest = _load_json(root / SCOPE_DIR / MANIFEST_FILES[SCOPE_DIR])
    dense_manifest = _load_json(root / DENSE_DIR / MANIFEST_FILES[DENSE_DIR])
    audit_manifest = _load_json(root / DENSE_AUDIT_DIR / MANIFEST_FILES[DENSE_AUDIT_DIR])
    sparse_manifest = _load_json(root / SPARSE_DIR / MANIFEST_FILES[SPARSE_DIR])
    rerank_manifest = _load_json(root / RERANK_DIR / MANIFEST_FILES[RERANK_DIR])
    artifact_manifest = _load_json(root / ARTIFACT_DIR / MANIFEST_FILES[ARTIFACT_DIR])
    assert dataset_manifest["evaluation_id"] == DATASET_ID
    assert scope_manifest["scope_id"] == SCOPE_ID
    assert dense_manifest["experiment_id"] == DENSE_ID
    assert audit_manifest["audit_id"] == DENSE_AUDIT_ID
    assert sparse_manifest["comparison_id"] == SPARSE_ID
    assert rerank_manifest["rerank_id"] == RERANK_ID
    assert artifact_manifest["baseline_id"] == ARTIFACT_ID
    assert {
        scope_manifest["dataset_evaluation_id"],
        dense_manifest["dataset_evaluation_id"],
        audit_manifest["dataset_evaluation_id"],
        sparse_manifest["dataset_evaluation_id"],
        rerank_manifest["dataset_evaluation_id"],
        artifact_manifest["dataset_evaluation_id"],
    } == {DATASET_ID}
    assert {
        dense_manifest["document_scope_id"],
        audit_manifest["document_scope_id"],
        sparse_manifest["document_scope_id"],
        rerank_manifest["document_scope_id"],
        artifact_manifest["document_scope_id"],
    } == {SCOPE_ID}
    assert dense_manifest["embedding_model_id"] == DENSE_MODEL_ID
    assert sparse_manifest["dense_model_id"] == DENSE_MODEL_ID
    assert sparse_manifest["sparse_model_id"] == SPARSE_MODEL_ID
    assert rerank_manifest["reranker_model_id"] == RERANK_MODEL_ID
    assert artifact_manifest["embedding_model_id"] == DENSE_MODEL_ID
    return FrozenInputs(
        root=root,
        dataset=root / DATASET_DIR,
        scope=root / SCOPE_DIR,
        dense=root / DENSE_DIR,
        dense_audit=root / DENSE_AUDIT_DIR,
        sparse=root / SPARSE_DIR,
        rerank=root / RERANK_DIR,
        artifact=root / ARTIFACT_DIR,
    )


@pytest.fixture(scope="module")
def frozen() -> FrozenInputs:
    return _verify_frozen_root(_root())


def _step4_candidate(segment: Any) -> dict[str, Any]:
    return {
        "target_id": segment.segment_id,
        "artifact_id": segment.artifact_id,
        "segment_id": segment.segment_id,
        "source_table": segment.source_table,
        "subject_id": segment.subject_id,
        "artifact_digest": segment.artifact_sha256,
        "profile_id": segment.profile_id,
        "subject_type": segment.subject_type,
        "access_scope": "public",
        "access_basis": "us-federal-public-record",
        "text": segment.text,
        "text_sha256": segment.text_sha256,
        "slices_json": canonical_json([one.as_dict() for one in segment.slices]),
        "ordinal": segment.ordinal,
    }


@pytest.fixture(scope="module")
def step4(frozen: FrozenInputs) -> Step4Inventory:
    scope = load_document_acceptance_scope(frozen.dataset, frozen.scope)
    outcomes = build_source_artifacts(
        frozen.dataset,
        active_source_tables=STEP4_ACTIVE_SOURCE_TABLES,
    )
    artifacts = tuple(
        outcome.artifact
        for outcome in outcomes
        if outcome.artifact is not None and outcome.artifact.content_sha256 in scope.included_artifact_digests
    )
    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    segments = tuple(
        segment
        for outcome in segment_artifacts(artifacts, settings=settings, counter=counter)
        for segment in outcome.segments
    )
    candidates = tuple(_step4_candidate(segment) for segment in segments)
    source_fields = dense_source_fields_from_segments(candidates)
    old_rows = tuple(
        row
        for row in read_parquet_rows(frozen.dense / "experiment_segments.parquet")
        if row["config_id"] == SELECTED_CONFIG
    )
    old_by_key = {(row["artifact_digest"], int(row["ordinal"])): row for row in old_rows}
    new_by_key = {(segment.artifact_sha256, segment.ordinal): segment for segment in segments}
    assert len(artifacts) == 153
    assert len(segments) == len(old_rows) == 1302
    assert set(new_by_key) == set(old_by_key)
    gold_rows = tuple(
        row
        for row in read_parquet_rows(frozen.dataset / "gold_spans.parquet")
        if row["gold_id"] in scope.included_gold_ids
    )
    assert len(gold_rows) == 35
    return Step4Inventory(
        artifacts=artifacts,
        segments=segments,
        candidates=candidates,
        source_fields=source_fields,
        old_by_key=old_by_key,
        new_by_key=new_by_key,
        gold_rows=gold_rows,
    )


def _run_validators(frozen: FrozenInputs) -> None:
    environment = {
        **os.environ,
        "HF_HUB_OFFLINE": "1",
        "R2_PUBLIC_URL": "",
    }
    for command in VALIDATOR_COMMANDS:
        completed = subprocess.run(
            (
                "uv",
                "run",
                "--frozen",
                "--extra",
                "embed",
                "--extra",
                "evaluation",
                command[0],
                *(str(frozen.root / argument) if argument in MANIFEST_FILES else argument for argument in command[1:]),
            ),
            cwd=frozen.root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, (
            f"historical validator failed: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
        payload = json.loads(completed.stdout[completed.stdout.index("{") :])
        assert payload["status"] == "pass"
        assert payload["failures"] == []


def _metric_values(row: Mapping[str, Any], *, prefix: str = "") -> dict[str, float]:
    return {name: float(row[f"{prefix}{name}"]) for name in METRIC_NAMES}


def _overlaps_gold(slices_json: str, gold: Mapping[str, Any]) -> bool:
    slices = json.loads(slices_json)
    return any(
        one["source_field"] == gold["source_field"]
        and int(one["start_char"]) < int(gold["end_char"])
        and int(one["end_char"]) > int(gold["start_char"])
        for one in slices
    )


def _legacy_answers(step4: Step4Inventory) -> dict[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for gold in step4.gold_rows:
        result[gold["gold_id"]] = tuple(
            sorted(
                row["segment_id"]
                for row in step4.old_by_key.values()
                if row["artifact_digest"] == gold["artifact_digest"] and _overlaps_gold(row["slices_json"], gold)
            )
        )
    return result


def _legacy_text(row: Mapping[str, Any]) -> str:
    return "\n".join(one["text"] for one in json.loads(str(row["slices_json"])))


def _legacy_hit(
    row: Mapping[str, Any],
    *,
    work_id: str,
    query_id: str,
    method: str,
    rank: int,
    score: float,
    candidate_size: int,
    dense_rank: int | None = None,
    dense_score: float | None = None,
    sparse_rank: int | None = None,
    sparse_score: float | None = None,
) -> RetrievalHit:
    segment_id = str(row["segment_id"])
    return RetrievalHit(
        work_id=work_id,
        query_id=query_id,
        level="segment",
        method=method,
        target_id=segment_id,
        artifact_id=f"legacy_artifact_{str(row['artifact_digest'])[:24]}",
        segment_id=segment_id,
        source_table=str(row["source_table"]),
        subject_id=str(row["subject_id"]),
        artifact_digest=str(row["artifact_digest"]),
        rank=rank,
        candidate_universe_size=candidate_size,
        candidate_input_size=candidate_size,
        candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
        score=score,
        score_kind={"dense": "cosine", "sparse": "sparse-dot"}[method],
        dense_rank=dense_rank,
        dense_score=dense_score,
        sparse_rank=sparse_rank,
        sparse_score=sparse_score,
        model_id=DENSE_MODEL_ID if method == "dense" else SPARSE_MODEL_ID,
        model_revision=(DEFAULT_DENSE_REVISION if method == "dense" else SPARSE_MODEL_ID.rsplit("@", 1)[1]),
    )


def _dense_segment_vectors(
    frozen: FrozenInputs,
    step4: Step4Inventory,
) -> tuple[dict[str, tuple[float, ...]], dict[str, tuple[float, ...]]]:
    cache_rows = read_parquet_rows(frozen.dense / "embedding_cache.parquet")
    vector_by_sha = {
        str(row["text_sha256"]): tuple(float(value) for value in json.loads(str(row["vector_json"])))
        for row in cache_rows
    }
    units = derive_dense_semantic_units(step4.source_fields, counter=TiktokenCounter())
    units_by_field: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for unit in units:
        assert unit.input_sha256 in vector_by_sha
        units_by_field[(unit.artifact_id, unit.source_field)].append(unit)
    old_vectors: dict[str, tuple[float, ...]] = {}
    new_vectors: dict[str, tuple[float, ...]] = {}
    for key in sorted(step4.old_by_key):
        old = step4.old_by_key[key]
        new = step4.new_by_key[key]
        overlaps: dict[str, tuple[Any, int]] = {}
        for source_slice in new.slices:
            for unit in units_by_field[(new.artifact_id, source_slice.source_field)]:
                width = min(source_slice.end_char, unit.end_char) - max(source_slice.start_char, unit.start_char)
                if width <= 0:
                    continue
                prior = overlaps.get(unit.unit_id)
                overlaps[unit.unit_id] = (unit, width + (prior[1] if prior is not None else 0))
        ordered = sorted(overlaps.values(), key=lambda item: item[0].ordinal)
        assert ordered
        vector = compose_dense_vector(
            tuple(vector_by_sha[unit.input_sha256] for unit, _ in ordered),
            tuple(float(width) for _, width in ordered),
        )
        old_vectors[str(old["segment_id"])] = vector
        new_vectors[new.segment_id] = vector
    query_vectors = {
        str(gold["gold_id"]): vector_by_sha[hashlib.sha256(str(gold["concept_label"]).encode()).hexdigest()]
        for gold in step4.gold_rows
    }
    return old_vectors, query_vectors


def _compare_ranked(
    ranked: Sequence[tuple[str, float]],
    stored: Sequence[Mapping[str, Any]],
    *,
    id_field: str,
    score_field: str,
    tolerance: float | None,
) -> None:
    ordered = sorted(stored, key=lambda row: int(row["candidate_rank"]))
    assert len(ranked) == len(ordered)
    assert [target_id for target_id, _ in ranked] == [str(row[id_field]) for row in ordered]
    assert list(range(1, len(ranked) + 1)) == [int(row["candidate_rank"]) for row in ordered]
    for (_, actual), row in zip(ranked, ordered, strict=True):
        expected = float(row[score_field])
        if tolerance is None:
            assert actual == expected
        else:
            assert abs(actual - expected) <= tolerance


def _selected_dense_replay(
    frozen: FrozenInputs,
    step4: Step4Inventory,
) -> tuple[
    dict[tuple[str, str], tuple[RetrievalHit, ...]],
    dict[str, tuple[float, ...]],
]:
    old_vectors, query_vectors = _dense_segment_vectors(frozen, step4)
    stored_rows = tuple(
        row
        for row in read_parquet_rows(frozen.dense / "retrieval_candidates.parquet")
        if row["config_id"] == SELECTED_CONFIG
    )
    old_by_id = {str(row["segment_id"]): row for row in step4.old_by_key.values()}
    groups: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    for scope in ("within-artifact", "corpus"):
        for gold in sorted(step4.gold_rows, key=lambda row: str(row["gold_id"])):
            query_id = str(gold["gold_id"])
            candidate_ids = tuple(
                sorted(
                    segment_id
                    for segment_id, row in old_by_id.items()
                    if scope == "corpus" or row["artifact_digest"] == gold["artifact_digest"]
                )
            )
            ranked = rank_dense_vectors(
                candidate_ids,
                tuple(old_vectors[segment_id] for segment_id in candidate_ids),
                query_vectors[query_id],
                limit=len(candidate_ids),
            )
            stored = [row for row in stored_rows if row["scope"] == scope and row["query_id"] == query_id]
            _compare_ranked(
                ranked[: len(stored)],
                stored,
                id_field="segment_id",
                score_field="dense_score",
                tolerance=DENSE_SCORE_TOLERANCE,
            )
            work_id = f"legacy-dense-{scope}-{query_id}"
            groups[(scope, query_id)] = tuple(
                _legacy_hit(
                    old_by_id[target_id],
                    work_id=work_id,
                    query_id=query_id,
                    method="dense",
                    rank=rank,
                    score=score,
                    candidate_size=len(candidate_ids),
                    dense_rank=rank,
                    dense_score=score,
                )
                for rank, (target_id, score) in enumerate(ranked, start=1)
            )
    metrics_row = next(
        row
        for row in read_parquet_rows(frozen.dense / "experiment_config_metrics.parquet")
        if row["config_id"] == SELECTED_CONFIG
    )
    answers = _legacy_answers(step4)
    for scope in ("within-artifact", "corpus"):
        measured = retrieval_metrics(
            tuple(hit for (group_scope, _), hits in groups.items() if group_scope == scope for hit in hits),
            answers,
            methods=("dense",),
        )["methods"]["dense"]
        assert measured == _metric_values(metrics_row, prefix=f"{scope.replace('-artifact', '')}_")
    return groups, query_vectors


def _sparse_vector(row: Mapping[str, Any]) -> SparseVector:
    return SparseVector(
        dimensions=int(row["dimensions"]),
        indices=tuple(int(value) for value in json.loads(str(row["indices_json"]))),
        values=tuple(float(value) for value in json.loads(str(row["values_json"]))),
    )


def _texts_by_sha(texts: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for text in texts:
        digest = hashlib.sha256(text.encode()).hexdigest()
        assert digest not in result or result[digest] == text
        result[digest] = text
    return result


def _encode_sparse_in_frozen_call_shape(
    encoder: Any,
    texts_by_sha: Mapping[str, str],
    *,
    task: str,
) -> tuple[dict[str, SparseVector], tuple[dict[str, Any], ...]]:
    vectors: dict[str, SparseVector] = {}
    calls: list[dict[str, Any]] = []
    keys = sorted(texts_by_sha)
    for start in range(0, len(keys), SPARSE_CHECKPOINT_BATCH_SIZE):
        batch_keys = keys[start : start + SPARSE_CHECKPOINT_BATCH_SIZE]
        response = encoder.encode(
            [texts_by_sha[key] for key in batch_keys],
            task=task,
        )
        assert len(response.vectors) == len(batch_keys)
        vectors.update(zip(batch_keys, response.vectors, strict=True))
        calls.append(response.call)
    assert set(vectors) == set(keys)
    return vectors, tuple(calls)


def _selected_sparse_replay(
    frozen: FrozenInputs,
    step4: Step4Inventory,
    dense_groups: Mapping[tuple[str, str], tuple[RetrievalHit, ...]],
) -> tuple[
    dict[tuple[str, str], tuple[RetrievalHit, ...]],
    dict[tuple[str, str], tuple[RetrievalHit, ...]],
]:
    embedding_rows = read_parquet_rows(frozen.sparse / "sparse_embeddings.parquet")
    vectors = {(str(row["input_kind"]), str(row["text_sha256"])): _sparse_vector(row) for row in embedding_rows}
    old_by_id = {str(row["segment_id"]): row for row in step4.old_by_key.values()}
    text_sha_by_id = {
        segment_id: hashlib.sha256(_legacy_text(row).encode()).hexdigest() for segment_id, row in old_by_id.items()
    }
    stored_rows = read_parquet_rows(frozen.sparse / "retrieval_candidates.parquet")
    sparse_groups: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    hybrid_groups: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    for scope in ("within-artifact", "corpus"):
        for gold in sorted(step4.gold_rows, key=lambda row: str(row["gold_id"])):
            query_id = str(gold["gold_id"])
            candidate_ids = tuple(
                sorted(
                    segment_id
                    for segment_id, row in old_by_id.items()
                    if scope == "corpus" or row["artifact_digest"] == gold["artifact_digest"]
                )
            )
            ranked = rank_sparse_vectors(
                candidate_ids,
                tuple(vectors[("document", text_sha_by_id[segment_id])] for segment_id in candidate_ids),
                vectors[("query", hashlib.sha256(str(gold["concept_label"]).encode()).hexdigest())],
            )
            stored_sparse = [
                row
                for row in stored_rows
                if row["scope"] == scope and row["query_id"] == query_id and row["stage"] == "learned-sparse"
            ]
            _compare_ranked(
                ranked,
                stored_sparse,
                id_field="segment_id",
                score_field="sparse_score",
                tolerance=None,
            )
            dense = dense_groups[(scope, query_id)]
            work_id = dense[0].work_id
            sparse = tuple(
                _legacy_hit(
                    old_by_id[target_id],
                    work_id=work_id,
                    query_id=query_id,
                    method="sparse",
                    rank=rank,
                    score=score,
                    candidate_size=len(candidate_ids),
                    sparse_rank=rank,
                    sparse_score=score,
                )
                for rank, (target_id, score) in enumerate(ranked, start=1)
            )
            hybrid = fuse_rrf(dense, sparse)
            stored_hybrid = [
                row
                for row in stored_rows
                if row["scope"] == scope and row["query_id"] == query_id and row["stage"] == "rrf-hybrid"
            ]
            _compare_ranked(
                tuple((hit.target_id, hit.score) for hit in hybrid),
                stored_hybrid,
                id_field="segment_id",
                score_field="fusion_score",
                tolerance=None,
            )
            sparse_groups[(scope, query_id)] = sparse
            hybrid_groups[(scope, query_id)] = hybrid

    answers = _legacy_answers(step4)
    stored_metrics = read_parquet_rows(frozen.sparse / "retrieval_metrics.parquet")
    for scope in ("within-artifact", "corpus"):
        measured = retrieval_metrics(
            tuple(
                hit
                for groups in (sparse_groups, hybrid_groups)
                for (group_scope, _), hits in groups.items()
                if group_scope == scope
                for hit in hits
            ),
            answers,
            methods=("sparse", "hybrid-rrf"),
        )["methods"]
        assert measured["sparse"] == _metric_values(
            next(row for row in stored_metrics if row["scope"] == scope and row["stage"] == "learned-sparse")
        )
        assert measured["hybrid-rrf"] == _metric_values(
            next(row for row in stored_metrics if row["scope"] == scope and row["stage"] == "rrf-hybrid")
        )
    return sparse_groups, hybrid_groups


def _rerank_group_facts(
    candidates: Sequence[RetrievalHit],
    query: RetrievalQuery,
    candidate_texts: Mapping[str, str],
    source_work_id: str,
) -> tuple[str, str, str, str]:
    candidate_ids = [hit.target_id for hit in candidates]
    candidate_ids_sha256 = hashlib.sha256(canonical_json(candidate_ids).encode()).hexdigest()
    group_key = hashlib.sha256(
        canonical_json(
            {
                "source_work_id": source_work_id,
                "query_id": query.query_id,
                "level": query.level,
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
                "query_sha256": hashlib.sha256(query.text.encode()).hexdigest(),
                "candidates": [
                    {
                        "target_id": hit.target_id,
                        "input_sha256": hashlib.sha256(candidate_texts[hit.target_id].encode()).hexdigest(),
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


def _provider_free_rerank_replay(
    frozen: FrozenInputs,
    step4: Step4Inventory,
    dense_groups: Mapping[tuple[str, str], tuple[RetrievalHit, ...]],
    tmp_path: Path,
) -> dict[tuple[str, str], tuple[RetrievalHit, ...]]:
    legacy_rows = read_parquet_rows(frozen.rerank / "reranked_candidates.parquet")
    request_rows = read_parquet_rows(frozen.rerank / "rerank_requests.parquet")
    old_by_id = {str(row["segment_id"]): row for row in step4.old_by_key.values()}
    candidate_texts = {segment_id: _legacy_text(row) for segment_id, row in old_by_id.items()}
    query_text = {str(row["gold_id"]): str(row["concept_label"]) for row in step4.gold_rows}
    results: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    all_token_counts: list[int] = []
    for scope in ("within-artifact", "corpus"):
        for query_id in sorted(query_text):
            dense = dense_groups[(scope, query_id)]
            dense_input = dense[:RETRIEVAL_CANDIDATE_LIMIT]
            candidates = dense_input[:50]
            source_work_id = candidates[0].work_id
            query = RetrievalQuery(query_id, query_text[query_id], "segment")
            group_key, candidate_ids_sha256, request_sha256, work_id = _rerank_group_facts(
                candidates,
                query,
                candidate_texts,
                source_work_id,
            )
            request = next(
                row
                for row in request_rows
                if row["config_id"] == SELECTED_CONFIG and row["scope"] == scope and row["query_id"] == query_id
            )
            assert request["candidate_ids_sha256"] == candidate_ids_sha256
            rows_by_id = {
                str(row["segment_id"]): row
                for row in legacy_rows
                if row["scope"] == scope and row["query_id"] == query_id
            }
            assert set(rows_by_id) == {hit.target_id for hit in candidates}
            call = {
                "provider": "sentence-transformers",
                "operation": "rerank",
                "status": "completed",
                "model_id": RERANK_MODEL_ID,
                "revision": DEFAULT_RERANK_REVISION,
                "tokenizer_id": next(iter(rows_by_id.values()))["rerank_tokenizer_id"],
                "candidate_count": len(candidates),
                "max_input_tokens": RERANK_MAX_SEQ_LENGTH,
                "provider_invoked": True,
                "attempt_count": int(request["attempt_count"]),
                "retry_count": int(request["retry_count"]),
                "package_name": request["package_name"],
                "package_version": request["package_version"],
                "tokenizer_package_version": "4.57.6",
            }
            score_rows: list[RerankScoreRow] = []
            for index, hit in enumerate(candidates):
                old = rows_by_id[hit.target_id]
                text = candidate_texts[hit.target_id]
                token_count = int(old["rerank_untruncated_token_count"])
                all_token_counts.append(token_count)
                score_rows.append(
                    RerankScoreRow(
                        work_id=work_id,
                        group_key=group_key,
                        source_work_id=source_work_id,
                        query_id=query_id,
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
                        query_input_sha256=hashlib.sha256(query.text.encode()).hexdigest(),
                        query_text=query.text,
                        input_policy=RERANK_INPUT_POLICY,
                        input_sha256=hashlib.sha256(text.encode()).hexdigest(),
                        input_text=text,
                        rerank_score=float(old["rerank_score"]),
                        rerank_rank=int(old["rerank_rank"]),
                        model_id=RERANK_MODEL_ID,
                        model_revision=DEFAULT_RERANK_REVISION,
                        tokenizer_id=str(old["rerank_tokenizer_id"]),
                        tokenizer_package_version="4.57.6",
                        untruncated_token_count=token_count,
                        input_limit=int(old["rerank_input_limit"]),
                        would_truncate=str(old["rerank_would_truncate"]) == "True",
                        token_audit_status=str(old["rerank_token_audit_status"]),
                        provider=str(request["provider"]),
                        package_name=str(request["package_name"]),
                        package_version=str(request["package_version"]),
                        operation="rerank",
                        call_status=str(request["status"]),
                        provider_invoked=True,
                        group_attempt=1,
                        provider_attempt_count=int(request["attempt_count"]),
                        retry_count=int(request["retry_count"]),
                        call_input_index=index,
                        call_json=canonical_json(call),
                    )
                )
            group_root = tmp_path / scope / query_id
            write_rerank_score_rows(group_root, score_rows)
            rebuilt = rebuild_reranked_hits(
                dense_input,
                candidate_texts,
                query=query,
                source_work_id=source_work_id,
                run_directory=group_root,
            )
            stored_order = sorted(rows_by_id.values(), key=lambda row: int(row["rerank_rank"]))
            assert [(hit.target_id, hit.rank, hit.score) for hit in rebuilt] == [
                (str(row["segment_id"]), int(row["rerank_rank"]), float(row["rerank_score"])) for row in stored_order
            ]
            results[(scope, query_id)] = rebuilt

    assert len(all_token_counts) == 2519
    assert max(all_token_counts) <= RERANK_MAX_SEQ_LENGTH
    assert all(str(row["rerank_would_truncate"]) == "False" for row in legacy_rows)
    assert all(row["rerank_token_audit_status"] == "exact-untruncated-pair-tokenizer" for row in legacy_rows)
    answers = _legacy_answers(step4)
    stored_metrics = read_parquet_rows(frozen.rerank / "rerank_config_metrics.parquet")
    for scope in ("within-artifact", "corpus"):
        measured = retrieval_metrics(
            tuple(hit for (group_scope, _), dense in dense_groups.items() if group_scope == scope for hit in dense[:50])
            + tuple(hit for (group_scope, _), reranked in results.items() if group_scope == scope for hit in reranked),
            answers,
            methods=("dense", "reranked"),
        )["methods"]
        assert measured["dense"] == _metric_values(
            next(row for row in stored_metrics if row["scope"] == scope and row["stage"] == "dense")
        )
        assert measured["reranked"] == _metric_values(
            next(row for row in stored_metrics if row["scope"] == scope and row["stage"] == "reranked")
        )
    return results


def _artifact_replay(frozen: FrozenInputs) -> dict[str, dict[str, float]]:
    embedding_rows = read_parquet_rows(frozen.artifact / "artifact_embeddings.parquet")
    query_rows = read_parquet_rows(frozen.artifact / "query_embeddings.parquet")
    candidate_rows = read_parquet_rows(frozen.artifact / "retrieval_candidates.parquet")
    metric_rows = read_parquet_rows(frozen.artifact / "retrieval_metrics.parquet")
    vectors = {
        (str(row["mode"]), str(row["vector_id"])): tuple(float(value) for value in json.loads(str(row["vector_json"])))
        for row in embedding_rows
    }
    answers_by_mode: dict[str, dict[str, tuple[str, ...]]] = {}
    hits_by_mode: dict[str, list[RetrievalHit]] = {}
    for mode in sorted({str(row["mode"]) for row in embedding_rows}):
        mode_rows = [row for row in embedding_rows if row["mode"] == mode]
        eligible = {str(row["artifact_digest"]) for row in mode_rows}
        answers: dict[str, tuple[str, ...]] = {}
        hits: list[RetrievalHit] = []
        for query in query_rows:
            if query["query_artifact_digest"] not in eligible:
                continue
            query_id = str(query["query_id"])
            target_ids = tuple(sorted(str(row["vector_id"]) for row in mode_rows))
            ranked = rank_dense_vectors(
                target_ids,
                tuple(vectors[(mode, target_id)] for target_id in target_ids),
                tuple(float(value) for value in json.loads(str(query["vector_json"]))),
            )
            stored = [row for row in candidate_rows if row["mode"] == mode and row["query_id"] == query_id]
            _compare_ranked(
                ranked,
                stored,
                id_field="vector_id",
                score_field="dense_score",
                tolerance=DENSE_SCORE_TOLERANCE,
            )
            by_id = {str(row["vector_id"]): row for row in mode_rows}
            answers[query_id] = tuple(
                sorted(
                    str(row["vector_id"])
                    for row in mode_rows
                    if row["artifact_digest"] == query["query_artifact_digest"]
                )
            )
            for rank, (target_id, score) in enumerate(ranked, start=1):
                row = by_id[target_id]
                hits.append(
                    RetrievalHit(
                        work_id=f"legacy-artifact-{mode}-{query_id}",
                        query_id=query_id,
                        level="artifact",
                        method="dense",
                        target_id=target_id,
                        artifact_id=target_id,
                        segment_id=None,
                        source_table=str(row["source_table"]),
                        subject_id=str(row["subject_id"]),
                        artifact_digest=str(row["artifact_digest"]),
                        rank=rank,
                        candidate_universe_size=len(target_ids),
                        candidate_input_size=len(target_ids),
                        candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
                        score=score,
                        score_kind="cosine",
                        dense_rank=rank,
                        dense_score=score,
                        model_id=DENSE_MODEL_ID,
                        model_revision=DEFAULT_DENSE_REVISION,
                    )
                )
        answers_by_mode[mode] = answers
        hits_by_mode[mode] = hits
    result: dict[str, dict[str, float]] = {}
    for mode in sorted(hits_by_mode):
        result[mode] = retrieval_metrics(
            hits_by_mode[mode],
            answers_by_mode[mode],
            methods=("dense",),
        )["methods"]["dense"]
        assert result[mode] == _metric_values(next(row for row in metric_rows if row["mode"] == mode))
    return result


def _v3_answers(step4: Step4Inventory) -> dict[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for gold in step4.gold_rows:
        result[str(gold["gold_id"])] = tuple(
            sorted(
                str(candidate["target_id"])
                for candidate in step4.candidates
                if candidate["artifact_digest"] == gold["artifact_digest"]
                and _overlaps_gold(str(candidate["slices_json"]), gold)
            )
        )
    return result


def _fresh_hit(
    candidate: Mapping[str, Any],
    *,
    work_id: str,
    query_id: str,
    method: str,
    rank: int,
    score: float,
    candidate_size: int,
    dense_rank: int | None = None,
    dense_score: float | None = None,
    sparse_rank: int | None = None,
    sparse_score: float | None = None,
) -> RetrievalHit:
    return RetrievalHit(
        work_id=work_id,
        query_id=query_id,
        level="segment",
        method=method,
        target_id=str(candidate["target_id"]),
        artifact_id=str(candidate["artifact_id"]),
        segment_id=str(candidate["segment_id"]),
        source_table=str(candidate["source_table"]),
        subject_id=str(candidate["subject_id"]),
        artifact_digest=str(candidate["artifact_digest"]),
        rank=rank,
        candidate_universe_size=candidate_size,
        candidate_input_size=candidate_size,
        candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
        score=score,
        score_kind={
            "dense": "cosine",
            "sparse": "sparse-dot",
            "reranked": "cross-encoder",
        }[method],
        dense_rank=dense_rank,
        dense_score=dense_score,
        sparse_rank=sparse_rank,
        sparse_score=sparse_score,
        model_id=RERANK_MODEL_ID
        if method == "reranked"
        else (DENSE_MODEL_ID if method == "dense" else SPARSE_MODEL_ID),
        model_revision=(
            DEFAULT_RERANK_REVISION
            if method == "reranked"
            else (DEFAULT_DENSE_REVISION if method == "dense" else SPARSE_MODEL_ID.rsplit("@", 1)[1])
        ),
    )


def _fresh_segment_vectors(
    step4: Step4Inventory,
    vector_by_sha: Mapping[str, tuple[float, ...]],
) -> dict[str, tuple[float, ...]]:
    units = derive_dense_semantic_units(step4.source_fields, counter=TiktokenCounter())
    units_by_field: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for unit in units:
        units_by_field[(unit.artifact_id, unit.source_field)].append(unit)
    result: dict[str, tuple[float, ...]] = {}
    for segment in step4.segments:
        overlaps: dict[str, tuple[Any, int]] = {}
        for source_slice in segment.slices:
            for unit in units_by_field[(segment.artifact_id, source_slice.source_field)]:
                width = min(source_slice.end_char, unit.end_char) - max(source_slice.start_char, unit.start_char)
                if width <= 0:
                    continue
                prior = overlaps.get(unit.unit_id)
                overlaps[unit.unit_id] = (unit, width + (prior[1] if prior is not None else 0))
        ordered = sorted(overlaps.values(), key=lambda item: item[0].ordinal)
        result[segment.segment_id] = compose_dense_vector(
            tuple(vector_by_sha[unit.input_sha256] for unit, _ in ordered),
            tuple(float(width) for _, width in ordered),
        )
    return result


def _artifact_inputs(step4: Step4Inventory) -> dict[str, tuple[str, str]]:
    fields_by_artifact: dict[str, list[DenseSourceField]] = defaultdict(list)
    for field in step4.source_fields:
        fields_by_artifact[field.artifact_id].append(field)
    result: dict[str, tuple[str, str]] = {}
    for artifact in step4.artifacts:
        text = "\n\n".join(
            f"[SOURCE_FIELD {field.source_field}]\n{field.text}"
            for field in sorted(fields_by_artifact[artifact.artifact_id], key=lambda item: item.ordinal)
            if field.text
        )
        result[artifact.artifact_id] = (hashlib.sha256(text.encode()).hexdigest(), text)
    return result


def _release_mps() -> None:
    gc.collect()
    import torch

    torch.mps.empty_cache()


def _fresh_inference(
    frozen: FrozenInputs,
    step4: Step4Inventory,
) -> FreshResult:
    units = derive_dense_semantic_units(step4.source_fields, counter=TiktokenCounter())
    artifact_inputs = _artifact_inputs(step4)
    query_text = {str(row["gold_id"]): str(row["concept_label"]) for row in step4.gold_rows}
    exact_inputs: dict[str, str] = {}
    for text in (
        *(unit.semantic_text for unit in units),
        *(text for _, text in artifact_inputs.values()),
        *query_text.values(),
    ):
        digest = hashlib.sha256(text.encode()).hexdigest()
        assert digest not in exact_inputs or exact_inputs[digest] == text
        exact_inputs[digest] = text
    ordered_dense_inputs = tuple(sorted(exact_inputs.items()))
    dense_adapter = SentenceTransformersDenseEmbedder(device="mps")
    dense_response = dense_adapter.embed(tuple(text for _, text in ordered_dense_inputs))
    assert dense_response.call["model_id"] == DENSE_MODEL_ID
    fresh_dense_by_sha = {
        digest: vector for (digest, _), vector in zip(ordered_dense_inputs, dense_response.vectors, strict=True)
    }
    dense_over_limit_count = sum(value is True for value in dense_response.call["inputs_over_limit"])
    segment_vectors = _fresh_segment_vectors(step4, fresh_dense_by_sha)
    query_vectors = {
        query_id: fresh_dense_by_sha[hashlib.sha256(text.encode()).hexdigest()] for query_id, text in query_text.items()
    }
    artifact_vectors = {artifact_id: fresh_dense_by_sha[digest] for artifact_id, (digest, _) in artifact_inputs.items()}
    del dense_adapter, dense_response
    _release_mps()

    candidates_by_id = {str(row["target_id"]): row for row in step4.candidates}
    candidates_by_artifact: dict[str, list[str]] = defaultdict(list)
    for candidate in step4.candidates:
        candidates_by_artifact[str(candidate["artifact_digest"])].append(str(candidate["target_id"]))
    dense_groups: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    dense_scores_by_content: dict[tuple[str, str, str, str], float] = {}
    for scope in ("within-artifact", "corpus"):
        for gold in sorted(step4.gold_rows, key=lambda row: str(row["gold_id"])):
            query_id = str(gold["gold_id"])
            candidate_ids = tuple(
                sorted(
                    candidates_by_artifact[str(gold["artifact_digest"])]
                    if scope == "within-artifact"
                    else candidates_by_id
                )
            )
            ranked_full = rank_dense_vectors(
                candidate_ids,
                tuple(segment_vectors[target_id] for target_id in candidate_ids),
                query_vectors[query_id],
                limit=len(candidate_ids),
            )
            work_id = f"fresh-dense-{scope}-{query_id}"
            dense_groups[(scope, query_id)] = tuple(
                _fresh_hit(
                    candidates_by_id[target_id],
                    work_id=work_id,
                    query_id=query_id,
                    method="dense",
                    rank=rank,
                    score=score,
                    candidate_size=len(candidate_ids),
                    dense_rank=rank,
                    dense_score=score,
                )
                for rank, (target_id, score) in enumerate(
                    ranked_full[:RETRIEVAL_CANDIDATE_LIMIT],
                    start=1,
                )
            )
            for target_id, score in ranked_full:
                dense_scores_by_content[
                    (
                        scope,
                        query_id,
                        str(candidates_by_id[target_id]["artifact_digest"]),
                        str(candidates_by_id[target_id]["text_sha256"]),
                    )
                ] = score

    stored_dense = [
        row
        for row in read_parquet_rows(frozen.dense / "retrieval_candidates.parquet")
        if row["config_id"] == SELECTED_CONFIG
    ]
    old_text_sha = {
        str(row["segment_id"]): hashlib.sha256(_legacy_text(row).encode()).hexdigest()
        for row in step4.old_by_key.values()
    }
    dense_drifts = [
        abs(
            dense_scores_by_content[
                (
                    str(row["scope"]),
                    str(row["query_id"]),
                    str(row["segment_artifact_digest"]),
                    old_text_sha[str(row["segment_id"])],
                )
            ]
            - float(row["dense_score"])
        )
        for row in stored_dense
    ]
    assert dense_drifts and max(dense_drifts) <= FRESH_DENSE_SCORE_TOLERANCE

    query_items = tuple(sorted(query_text.items()))
    segment_text_sha_by_id = {
        str(candidate["target_id"]): hashlib.sha256(str(candidate["text"]).encode()).hexdigest()
        for candidate in step4.candidates
    }
    query_text_sha_by_id = {query_id: hashlib.sha256(text.encode()).hexdigest() for query_id, text in query_items}
    sparse_document_texts = _texts_by_sha(tuple(str(candidate["text"]) for candidate in step4.candidates))
    sparse_query_texts = _texts_by_sha(tuple(text for _, text in query_items))
    sparse_adapter = SentenceTransformersSparseEncoder(device="mps")
    sparse_documents_by_sha, sparse_document_calls = _encode_sparse_in_frozen_call_shape(
        sparse_adapter,
        sparse_document_texts,
        task="document",
    )
    sparse_queries_by_sha, sparse_query_calls = _encode_sparse_in_frozen_call_shape(
        sparse_adapter,
        sparse_query_texts,
        task="query",
    )
    sparse_over_limit_count = sum(
        value is True for call in (*sparse_document_calls, *sparse_query_calls) for value in call["inputs_over_limit"]
    )
    assert sparse_over_limit_count == 0
    sparse_by_id = {
        target_id: sparse_documents_by_sha[text_sha] for target_id, text_sha in segment_text_sha_by_id.items()
    }
    sparse_query_by_id = {
        query_id: sparse_queries_by_sha[text_sha] for query_id, text_sha in query_text_sha_by_id.items()
    }
    del (
        sparse_adapter,
        sparse_document_calls,
        sparse_documents_by_sha,
        sparse_query_calls,
        sparse_queries_by_sha,
    )
    _release_mps()

    sparse_groups: dict[
        tuple[str, str],
        tuple[RetrievalHit, ...],
    ] = {}
    hybrid_groups: dict[
        tuple[str, str],
        tuple[RetrievalHit, ...],
    ] = {}
    sparse_scores_by_content: dict[tuple[str, str, str, str], float] = {}
    for scope in ("within-artifact", "corpus"):
        for gold in sorted(
            step4.gold_rows,
            key=lambda row: str(row["gold_id"]),
        ):
            query_id = str(gold["gold_id"])
            candidate_ids = tuple(
                sorted(
                    candidates_by_artifact[str(gold["artifact_digest"])]
                    if scope == "within-artifact"
                    else candidates_by_id
                )
            )
            ranked_full = rank_sparse_vectors(
                candidate_ids,
                tuple(sparse_by_id[target_id] for target_id in candidate_ids),
                sparse_query_by_id[query_id],
                limit=len(candidate_ids),
            )
            work_id = dense_groups[(scope, query_id)][0].work_id
            sparse = tuple(
                _fresh_hit(
                    candidates_by_id[target_id],
                    work_id=work_id,
                    query_id=query_id,
                    method="sparse",
                    rank=rank,
                    score=score,
                    candidate_size=len(candidate_ids),
                    sparse_rank=rank,
                    sparse_score=score,
                )
                for rank, (target_id, score) in enumerate(
                    ranked_full[:RETRIEVAL_CANDIDATE_LIMIT],
                    start=1,
                )
            )
            sparse_groups[(scope, query_id)] = sparse
            hybrid_groups[(scope, query_id)] = fuse_rrf(
                dense_groups[(scope, query_id)],
                sparse,
            )
            for target_id, score in ranked_full:
                sparse_scores_by_content[
                    (
                        scope,
                        query_id,
                        str(candidates_by_id[target_id]["artifact_digest"]),
                        str(candidates_by_id[target_id]["text_sha256"]),
                    )
                ] = score

    stored_sparse = [
        row
        for row in read_parquet_rows(frozen.sparse / "retrieval_candidates.parquet")
        if row["stage"] == "learned-sparse"
    ]
    sparse_drifts = [
        abs(
            sparse_scores_by_content[
                (
                    str(row["scope"]),
                    str(row["query_id"]),
                    str(row["segment_artifact_digest"]),
                    old_text_sha[str(row["segment_id"])],
                )
            ]
            - float(row["sparse_score"])
        )
        for row in stored_sparse
    ]
    assert sparse_drifts and max(sparse_drifts) <= FRESH_SPARSE_SCORE_TOLERANCE

    reranker = SentenceTransformersReranker(
        device="mps",
        max_seq_length=RERANK_MAX_SEQ_LENGTH,
    )
    reranked_groups: dict[tuple[str, str], tuple[RetrievalHit, ...]] = {}
    rerank_candidate_count = 0
    rerank_truncated_count = 0
    fresh_rerank_by_content: dict[tuple[str, str, str], float] = {}
    for scope in ("within-artifact", "corpus"):
        for query_id in sorted(query_text):
            dense = dense_groups[(scope, query_id)]
            candidates = dense[:50]
            candidate_ids = tuple(hit.target_id for hit in candidates)
            documents = tuple(str(candidates_by_id[target_id]["text"]) for target_id in candidate_ids)
            response = reranker.rerank(query_text[query_id], documents)
            assert len(response.scores) == len(candidate_ids)
            assert response.call["candidate_count"] == len(candidate_ids)
            assert response.call["model_id"] == RERANK_MODEL_ID
            token_counts = tuple(int(value) for value in response.call["token_counts"])
            truncation = tuple(bool(value) for value in response.call["inputs_over_limit"])
            rerank_candidate_count += len(candidate_ids)
            rerank_truncated_count += sum(truncation)
            ranked_indices = sorted(
                range(len(candidate_ids)),
                key=lambda index: (-response.scores[index], candidate_ids[index]),
            )
            rank_by_index = {index: rank for rank, index in enumerate(ranked_indices, start=1)}
            reranked = tuple(
                sorted(
                    (
                        _fresh_hit(
                            candidates_by_id[target_id],
                            work_id=dense[index].work_id,
                            query_id=query_id,
                            method="reranked",
                            rank=rank_by_index[index],
                            score=float(response.scores[index]),
                            candidate_size=len(candidate_ids),
                            dense_rank=dense[index].rank,
                            dense_score=dense[index].score,
                        )
                        for index, target_id in enumerate(candidate_ids)
                    ),
                    key=lambda hit: (hit.rank, hit.target_id),
                )
            )
            assert {hit.target_id for hit in reranked} == set(candidate_ids)
            assert all(
                token_count <= RERANK_MAX_SEQ_LENGTH and not would_truncate
                for token_count, would_truncate in zip(token_counts, truncation, strict=True)
            )
            for target_id, score in zip(candidate_ids, response.scores, strict=True):
                fresh_rerank_by_content[
                    (
                        query_id,
                        str(candidates_by_id[target_id]["artifact_digest"]),
                        str(candidates_by_id[target_id]["text_sha256"]),
                    )
                ] = float(score)
            reranked_groups[(scope, query_id)] = reranked
    del reranker
    _release_mps()

    stored_rerank = read_parquet_rows(frozen.rerank / "reranked_candidates.parquet")
    comparable_rerank_drifts = [
        abs(
            fresh_rerank_by_content[
                (
                    str(row["query_id"]),
                    str(row["segment_artifact_digest"]),
                    old_text_sha[str(row["segment_id"])],
                )
            ]
            - float(row["rerank_score"])
        )
        for row in stored_rerank
        if (
            str(row["query_id"]),
            str(row["segment_artifact_digest"]),
            old_text_sha[str(row["segment_id"])],
        )
        in fresh_rerank_by_content
    ]
    assert comparable_rerank_drifts
    assert max(comparable_rerank_drifts) <= FRESH_RERANK_SCORE_TOLERANCE

    answers = _v3_answers(step4)
    metrics: dict[str, dict[str, dict[str, float]]] = {
        "hybrid-rrf": {},
        "reranked": {},
        "artifact": {},
    }
    for scope in ("within-artifact", "corpus"):
        metrics["hybrid-rrf"][scope] = retrieval_metrics(
            tuple(hit for (group_scope, _), hits in hybrid_groups.items() if group_scope == scope for hit in hits),
            answers,
            methods=("hybrid-rrf",),
        )["methods"]["hybrid-rrf"]
        metrics["reranked"][scope] = retrieval_metrics(
            tuple(hit for (group_scope, _), hits in reranked_groups.items() if group_scope == scope for hit in hits),
            answers,
            methods=("reranked",),
        )["methods"]["reranked"]

    artifacts_by_id = {artifact.artifact_id: artifact for artifact in step4.artifacts}
    artifact_hits: list[RetrievalHit] = []
    artifact_answers: dict[str, tuple[str, ...]] = {}
    for gold in sorted(step4.gold_rows, key=lambda row: str(row["gold_id"])):
        query_id = str(gold["gold_id"])
        target_ids = tuple(sorted(artifacts_by_id))
        ranked = rank_dense_vectors(
            target_ids,
            tuple(artifact_vectors[target_id] for target_id in target_ids),
            query_vectors[query_id],
            limit=len(target_ids),
        )
        artifact_answers[query_id] = tuple(
            artifact.artifact_id for artifact in step4.artifacts if artifact.content_sha256 == gold["artifact_digest"]
        )
        for rank, (target_id, score) in enumerate(ranked, start=1):
            artifact = artifacts_by_id[target_id]
            artifact_hits.append(
                RetrievalHit(
                    work_id=f"fresh-artifact-{query_id}",
                    query_id=query_id,
                    level="artifact",
                    method="dense",
                    target_id=target_id,
                    artifact_id=target_id,
                    segment_id=None,
                    source_table=artifact.source_table,
                    subject_id=artifact.subject_id,
                    artifact_digest=artifact.content_sha256,
                    rank=rank,
                    candidate_universe_size=len(target_ids),
                    candidate_input_size=len(target_ids),
                    candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
                    score=score,
                    score_kind="cosine",
                    dense_rank=rank,
                    dense_score=score,
                    model_id=DENSE_MODEL_ID,
                    model_revision=DEFAULT_DENSE_REVISION,
                )
            )
    metrics["artifact"]["corpus"] = retrieval_metrics(
        artifact_hits,
        artifact_answers,
        methods=("dense",),
    )["methods"]["dense"]
    assert all(hit.target_id.startswith("processing_segment_") for hits in dense_groups.values() for hit in hits)
    assert all(hit.target_id.startswith("artifact_") for hit in artifact_hits)
    return FreshResult(
        metrics=metrics,
        score_drift={
            "dense": max(dense_drifts),
            "sparse": max(sparse_drifts),
            "rerank": max(comparable_rerank_drifts),
        },
        compared_score_count={
            "dense": len(dense_drifts),
            "sparse": len(sparse_drifts),
            "rerank": len(comparable_rerank_drifts),
        },
        dense_over_limit_count=dense_over_limit_count,
        sparse_over_limit_count=sparse_over_limit_count,
        rerank_candidate_count=rerank_candidate_count,
        rerank_truncated_count=rerank_truncated_count,
        artifact_hit_count=len(artifact_hits),
    )


def test_fresh_sparse_call_shape_deduplicates_sorts_and_chunks_by_32() -> None:
    class RecordingSparseEncoder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[str, ...]]] = []

        def encode(
            self,
            texts: Sequence[str],
            *,
            task: str,
        ) -> SparseEncodingResult:
            requested = tuple(texts)
            self.calls.append((task, requested))
            return SparseEncodingResult(
                vectors=tuple(
                    SparseVector(
                        dimensions=1,
                        indices=(0,),
                        values=(float(len(text)),),
                    )
                    for text in requested
                ),
                call={
                    "inputs_over_limit": tuple(False for _ in requested),
                },
            )

    texts = (
        *(f"sparse-input-{index}" for index in range(67)),
        "sparse-input-0",
    )
    texts_by_sha = _texts_by_sha(texts)
    encoder = RecordingSparseEncoder()
    vectors, calls = _encode_sparse_in_frozen_call_shape(
        encoder,
        texts_by_sha,
        task="document",
    )

    expected_order = tuple(texts_by_sha[digest] for digest in sorted(texts_by_sha))
    assert len(texts_by_sha) == 67
    assert [len(requested) for _, requested in encoder.calls] == [32, 32, 3]
    assert tuple(text for _, requested in encoder.calls for text in requested) == expected_order
    assert {task for task, _ in encoder.calls} == {"document"}
    assert set(vectors) == set(texts_by_sha)
    assert len(calls) == 3


def test_frozen_root_is_sealed_and_all_historical_validators_pass(
    frozen: FrozenInputs,
) -> None:
    _assert_ledger_old_files_exist(frozen)
    _run_validators(frozen)


def test_provider_free_replay_preserves_both_legacy_identity_spaces(
    frozen: FrozenInputs,
    step4: Step4Inventory,
    tmp_path: Path,
) -> None:
    dense_groups, _ = _selected_dense_replay(frozen, step4)
    _, hybrid_groups = _selected_sparse_replay(frozen, step4, dense_groups)
    reranked_groups = _provider_free_rerank_replay(
        frozen,
        step4,
        dense_groups,
        tmp_path,
    )
    artifact_metrics = _artifact_replay(frozen)

    assert all(
        hit.target_id.startswith("experiment_segment_")
        for groups in (dense_groups, hybrid_groups, reranked_groups)
        for hits in groups.values()
        for hit in hits
    )
    assert set(artifact_metrics) == {
        "all-profile-whole-artifact-v1",
        "incumbent-three-table-whole-row-v1",
    }
    audit = _load_json(frozen.dense_audit / "segmentation-embedding-audit-manifest.json")
    sparse_manifest = _load_json(frozen.sparse / "segmentation-sparse-retrieval-manifest.json")
    rerank_manifest = _load_json(frozen.rerank / "segmentation-rerank-manifest.json")
    assert audit["over_limit_input_count"] == audit["truncated_input_count"] == 45
    assert sparse_manifest["artifacts"]["sparse_embeddings.parquet"]["rows"] == 1317
    assert rerank_manifest["candidate_count"] == 2519
    assert rerank_manifest["truncated_candidate_count"] == 0
    assert rerank_manifest["unaudited_candidate_count"] == 0


def test_cached_fresh_inference_stays_in_v3_identity_and_meets_fixed_gates(
    frozen: FrozenInputs,
    step4: Step4Inventory,
) -> None:
    result = _fresh_inference(frozen, step4)
    corpus_hybrid = result.metrics["hybrid-rrf"]["corpus"]
    corpus_reranked = result.metrics["reranked"]["corpus"]

    assert corpus_hybrid["recall_at_50"] >= 0.8285714285714286
    assert corpus_reranked["recall_at_10"] >= 0.7142857142857143
    assert corpus_reranked["recall_at_50"] >= 0.8
    assert corpus_reranked["mrr"] >= 0.46387780139659834
    assert result.score_drift["dense"] <= FRESH_DENSE_SCORE_TOLERANCE
    assert result.score_drift["sparse"] <= FRESH_SPARSE_SCORE_TOLERANCE
    assert result.score_drift["rerank"] <= FRESH_RERANK_SCORE_TOLERANCE
    assert result.dense_over_limit_count == 45
    assert result.sparse_over_limit_count == 0
    assert result.rerank_candidate_count == 2519
    assert result.rerank_truncated_count == 0
    assert result.artifact_hit_count == 35 * 153

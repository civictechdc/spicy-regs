"""Real-USearch tests for the dense ANN index, against the exact pinned release.

These run only where the optional extra is installed, and they touch no network
and no model: every vector is generated locally from a fixed seed. Run them
with::

    uv run --frozen --extra ann pytest tests/test_ontology_ann_index_real.py

What they exist to hold:

* a **round trip** — a graph built in memory, saved, and re-opened
  memory-mapped returns the same rows and the same scores, because the whole
  operational argument is that the mapped copy serves what the built copy did;
* **recall against exact**, computed on a fixture small enough that the exact
  answer is a plain numpy argsort, so the recall helper is checked against
  arithmetic rather than against itself;
* **determinism** — two single-threaded builds from the same vectors return the
  same rankings, which is what makes a measured recall number reproducible; and
* the **refusal paths** that keep a graph from being used with the wrong
  registry, the wrong model, or the wrong number of concepts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy
import pytest

usearch = pytest.importorskip("usearch", reason="needs the optional ann extra")

from spicy_regs.ontology.ann_index import (  # noqa: E402
    ANN_CHANNEL_VERSION,
    QUANTIZATIONS,
    USEARCH_VERSION,
    AnnIndexError,
    UsearchConceptMapper,
    ann_index_path,
    build_ann_concept_index,
    load_ann_concept_index,
    recall_against_exact,
    save_ann_concept_index,
    sidecar_path,
)
from spicy_regs.ontology.candidate_channels import (  # noqa: E402
    DenseConceptIndex,
    DenseConceptMapper,
    dense_channel_ranking,
)

DIMENSIONS = 16
CONCEPT_COUNT = 400
REGISTRY_DIGEST = "a1b2c3d4" * 8
MODEL_ID = "test/pinned-embedder"


def normalized(matrix: numpy.ndarray) -> numpy.ndarray:
    norms = numpy.linalg.norm(matrix, axis=1, keepdims=True)
    return (matrix / numpy.where(norms == 0.0, 1.0, norms)).astype(numpy.float32)


def dense_index(rows: int = CONCEPT_COUNT, *, seed: int = 11) -> DenseConceptIndex:
    generator = numpy.random.default_rng(seed)
    matrix = normalized(generator.standard_normal((rows, DIMENSIONS)))
    return DenseConceptIndex(
        schema_version="concept-dense-index-v1",
        model_id=MODEL_ID,
        dimensions=DIMENSIONS,
        registry_digest=REGISTRY_DIGEST,
        concept_ids=tuple(f"concept-{position:04d}" for position in range(rows)),
        matrix=matrix,
    )


@dataclass(frozen=True)
class _Vectors:
    vectors: Any


@dataclass(frozen=True)
class FakeEmbedder:
    """Maps a query name to a fixed vector; no model, no network, no torch."""

    model_id: str
    dimensions: int
    by_text: dict[str, Any]

    def embed(self, texts: Any) -> _Vectors:
        return _Vectors(vectors=numpy.asarray([self.by_text[str(text)] for text in texts], dtype=numpy.float32))


def query_fixture(count: int = 12, *, seed: int = 29) -> tuple[FakeEmbedder, list[str], numpy.ndarray]:
    generator = numpy.random.default_rng(seed)
    matrix = normalized(generator.standard_normal((count, DIMENSIONS)))
    texts = [f"query-{position:02d}" for position in range(count)]
    return (
        FakeEmbedder(model_id=MODEL_ID, dimensions=DIMENSIONS, by_text=dict(zip(texts, matrix, strict=True))),
        texts,
        matrix,
    )


def exact_top(dense: DenseConceptIndex, queries: numpy.ndarray, depth: int) -> list[list[str]]:
    """The exact answer by plain arithmetic, independent of the module under test."""
    # BLAS raises the hardware FP flags on its own padded lanes; the shipped
    # mapper ignores them for the same reason (candidate_channels.py).
    with numpy.errstate(divide="ignore", over="ignore", invalid="ignore"):
        similarity = numpy.asarray(dense.matrix) @ queries.T
    results: list[list[str]] = []
    for column in range(queries.shape[0]):
        scores = similarity[:, column]
        order = sorted(range(scores.shape[0]), key=lambda row: (-float(scores[row]), dense.concept_ids[row]))
        results.append([dense.concept_ids[row] for row in order[:depth]])
    return results


# --- round trip ------------------------------------------------------------


def test_pinned_version_is_the_installed_one() -> None:
    assert usearch.__version__ == USEARCH_VERSION


def test_built_saved_and_mapped_index_agree(tmp_path: Path) -> None:
    dense = dense_index()
    embedder, texts, _ = query_fixture()
    built, facts = build_ann_concept_index(dense, quantization="f32", threads=1)
    assert facts["source"] == "built"
    assert facts["concept_count"] == CONCEPT_COUNT
    assert facts["seconds"] >= 0.0

    path = ann_index_path(tmp_path, registry_digest=REGISTRY_DIGEST, model_id=MODEL_ID, quantization="f32")
    stored = save_ann_concept_index(built, path)
    assert path.exists()
    assert sidecar_path(path).exists()
    assert stored["bytes"] > 0
    assert not path.with_name(path.name + ".partial").exists()

    mapped = load_ann_concept_index(
        path,
        concept_ids=dense.concept_ids,
        registry_digest=REGISTRY_DIGEST,
        model_id=MODEL_ID,
        view=True,
    )
    assert mapped.viewed is True
    from_memory = UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=10)
    from_disk = UsearchConceptMapper(index=mapped, embedder=embedder).rank(texts, depth=10)
    assert [[concept_id for concept_id, _ in row] for row in from_disk] == [
        [concept_id for concept_id, _ in row] for row in from_memory
    ]
    for mapped_row, memory_row in zip(from_disk, from_memory, strict=True):
        for (_, mapped_score), (_, memory_score) in zip(mapped_row, memory_row, strict=True):
            assert mapped_score == pytest.approx(memory_score, abs=1e-6)


def test_sidecar_records_the_graph_identity(tmp_path: Path) -> None:
    dense = dense_index()
    built, _ = build_ann_concept_index(dense, quantization="f16", connectivity=32, expansion_add=200, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    meta = json.loads(sidecar_path(path).read_text())
    assert meta["registry_digest"] == REGISTRY_DIGEST
    assert meta["model_id"] == MODEL_ID
    assert meta["quantization"] == "f16"
    assert meta["connectivity"] == 32
    assert meta["expansion_add"] == 200
    assert meta["concept_count"] == CONCEPT_COUNT
    assert meta["channel_version"] == ANN_CHANNEL_VERSION
    assert meta["usearch_version"] == USEARCH_VERSION


def test_a_loaded_index_serves_without_the_source_matrix(tmp_path: Path) -> None:
    """The stored graph is self-sufficient apart from the concept-id ordering."""
    dense = dense_index()
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    loaded = load_ann_concept_index(path, concept_ids=dense.concept_ids, view=False)
    assert loaded.viewed is False
    assert len(loaded.concept_ids) == CONCEPT_COUNT
    embedder, texts, _ = query_fixture()
    ranked = UsearchConceptMapper(index=loaded, embedder=embedder).rank(texts[:3], depth=5)
    assert all(len(row) == 5 for row in ranked)


# --- recall against a separately computed exact answer ---------------------


@pytest.mark.parametrize("quantization", QUANTIZATIONS)
def test_every_quantization_recalls_the_exact_neighbours(quantization: str) -> None:
    dense = dense_index()
    embedder, texts, queries = query_fixture()
    built, _ = build_ann_concept_index(dense, quantization=quantization, threads=1)
    mapper = UsearchConceptMapper(index=built, embedder=embedder)
    approximate = [[concept_id for concept_id, _ in row] for row in mapper.rank(texts, depth=10)]
    exact = exact_top(dense, queries, 10)
    measured = recall_against_exact(approximate, exact, depth=10)
    # At 400 vectors an HNSW graph is effectively exhaustive; a floor rather than
    # equality keeps the assertion about the recall this path must deliver, not
    # about a graph traversal detail.
    assert measured["macro_recall"] >= 0.9
    assert measured["query_count"] == len(texts)


def test_recall_helper_agrees_with_the_exact_baseline_mapper() -> None:
    """The baseline used for recall is the shipped exact mapper, not a rewrite."""
    dense = dense_index()
    embedder, texts, queries = query_fixture()
    exact_mapper = DenseConceptMapper(index=dense, embedder=embedder)
    from_channel = [dense_channel_ranking(text, mapper=exact_mapper, depth=10) for text in texts]
    assert from_channel == exact_top(dense, queries, 10)
    assert recall_against_exact(from_channel, from_channel, depth=10)["macro_recall"] == 1.0


def test_a_vector_in_the_index_is_its_own_nearest_neighbour() -> None:
    """Self-retrieval is exact regardless of graph traversal, so it must hold."""
    dense = dense_index()
    rows = [0, 137, 399]
    texts = [f"row-{row}" for row in rows]
    embedder = FakeEmbedder(
        model_id=MODEL_ID,
        dimensions=DIMENSIONS,
        by_text={text: numpy.asarray(dense.matrix)[row] for text, row in zip(texts, rows, strict=True)},
    )
    built, _ = build_ann_concept_index(dense, threads=1)
    ranked = UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=3)
    for row, candidates in zip(rows, ranked, strict=True):
        assert candidates[0][0] == dense.concept_ids[row]
        assert candidates[0][1] == pytest.approx(1.0, abs=1e-3)


def test_a_single_query_returns_the_same_row_a_batch_does() -> None:
    """USearch returns ``Matches`` for one query and ``BatchMatches`` for many.

    ``dense_channel_ranking`` always calls a mapper with exactly one query, so
    the single-query shape *is* the production path. Reading it as a batch
    (``.counts``) raises; reading a batch as a single row would silently drop
    every query after the first.
    """
    dense = dense_index()
    embedder, texts, _ = query_fixture()
    built, _ = build_ann_concept_index(dense, threads=1)
    mapper = UsearchConceptMapper(index=built, embedder=embedder)
    batched = mapper.rank(texts, depth=10)
    assert len(batched) == len(texts)
    for position, text in enumerate(texts):
        alone = mapper.rank([text], depth=10)
        assert len(alone) == 1
        assert len(alone[0]) == 10
        assert alone[0] == batched[position]


def test_the_channel_entry_point_ranks_through_the_ann_mapper() -> None:
    """The shipped channel function must work against this mapper unchanged."""
    dense = dense_index()
    embedder, texts, _ = query_fixture()
    built, _ = build_ann_concept_index(dense, threads=1)
    mapper = UsearchConceptMapper(index=built, embedder=embedder)
    for text in texts:
        ranked = dense_channel_ranking(text, mapper=mapper, depth=12)
        assert len(ranked) == 12
        assert len(set(ranked)) == 12
    assert dense_channel_ranking("   ", mapper=mapper, depth=12) == []


def test_scores_are_similarities_ordered_best_first() -> None:
    dense = dense_index()
    embedder, texts, _ = query_fixture()
    built, _ = build_ann_concept_index(dense, threads=1)
    for candidates in UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=8):
        scores = [score for _, score in candidates]
        assert scores == sorted(scores, reverse=True)
        assert all(-1.001 <= score <= 1.001 for score in scores)


def test_depth_beyond_the_index_returns_every_concept_once() -> None:
    dense = dense_index(rows=5)
    embedder, texts, _ = query_fixture(count=2)
    built, _ = build_ann_concept_index(dense, threads=1)
    for candidates in UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=50):
        returned = [concept_id for concept_id, _ in candidates]
        assert len(returned) == 5
        assert len(set(returned)) == 5


# --- determinism -----------------------------------------------------------


def test_two_builds_from_the_same_vectors_rank_identically() -> None:
    embedder, texts, _ = query_fixture()
    first, _ = build_ann_concept_index(dense_index(seed=11), threads=1)
    second, _ = build_ann_concept_index(dense_index(seed=11), threads=1)
    assert UsearchConceptMapper(index=first, embedder=embedder).rank(texts, depth=10) == UsearchConceptMapper(
        index=second, embedder=embedder
    ).rank(texts, depth=10)


def test_repeated_queries_against_one_graph_are_stable() -> None:
    embedder, texts, _ = query_fixture()
    built, _ = build_ann_concept_index(dense_index(), threads=1)
    mapper = UsearchConceptMapper(index=built, embedder=embedder)
    assert mapper.rank(texts, depth=12) == mapper.rank(texts, depth=12)


def test_a_reopened_graph_ranks_as_the_original_did(tmp_path: Path) -> None:
    dense = dense_index()
    embedder, texts, _ = query_fixture()
    built, _ = build_ann_concept_index(dense, threads=1)
    before = UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=12)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    reopened = load_ann_concept_index(path, concept_ids=dense.concept_ids, view=True)
    after = UsearchConceptMapper(index=reopened, embedder=embedder).rank(texts, depth=12)
    assert [[concept_id for concept_id, _ in row] for row in after] == [
        [concept_id for concept_id, _ in row] for row in before
    ]


# --- refusal paths ---------------------------------------------------------


def test_a_graph_without_its_sidecar_is_refused(tmp_path: Path) -> None:
    dense = dense_index(rows=20)
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    sidecar_path(path).unlink()
    with pytest.raises(AnnIndexError) as caught:
        load_ann_concept_index(path, concept_ids=dense.concept_ids)
    assert "sidecar" in str(caught.value)


def test_a_graph_from_another_registry_is_refused(tmp_path: Path) -> None:
    dense = dense_index(rows=20)
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    with pytest.raises(AnnIndexError) as caught:
        load_ann_concept_index(path, concept_ids=dense.concept_ids, registry_digest="f" * 64)
    assert "different registry" in str(caught.value)


def test_a_graph_from_another_model_is_refused(tmp_path: Path) -> None:
    dense = dense_index(rows=20)
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    with pytest.raises(AnnIndexError) as caught:
        load_ann_concept_index(path, concept_ids=dense.concept_ids, model_id="other/model")
    assert "different model" in str(caught.value)


def test_a_mismatched_concept_id_count_is_refused(tmp_path: Path) -> None:
    dense = dense_index(rows=20)
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    with pytest.raises(AnnIndexError) as caught:
        load_ann_concept_index(path, concept_ids=dense.concept_ids[:19])
    assert "different number of concepts" in str(caught.value)


def test_an_unreadable_schema_is_refused(tmp_path: Path) -> None:
    dense = dense_index(rows=20)
    built, _ = build_ann_concept_index(dense, threads=1)
    path = tmp_path / "graph.usearch"
    save_ann_concept_index(built, path)
    meta = json.loads(sidecar_path(path).read_text())
    meta["schema_version"] = "concept-ann-index-v0"
    sidecar_path(path).write_text(json.dumps(meta))
    with pytest.raises(AnnIndexError) as caught:
        load_ann_concept_index(path, concept_ids=dense.concept_ids)
    assert "not readable" in str(caught.value)


def test_an_unsupported_quantization_is_refused_before_any_build() -> None:
    with pytest.raises(AnnIndexError):
        build_ann_concept_index(dense_index(rows=8), quantization="b1")


def test_an_empty_index_returns_empty_rankings() -> None:
    empty = DenseConceptIndex(
        schema_version="concept-dense-index-v1",
        model_id=MODEL_ID,
        dimensions=DIMENSIONS,
        registry_digest=REGISTRY_DIGEST,
        concept_ids=(),
        matrix=numpy.zeros((0, DIMENSIONS), dtype=numpy.float32),
    )
    built, _ = build_ann_concept_index(empty, threads=1)
    embedder, texts, _ = query_fixture(count=2)
    assert UsearchConceptMapper(index=built, embedder=embedder).rank(texts, depth=5) == [[], []]

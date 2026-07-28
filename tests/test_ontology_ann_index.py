"""Hermetic tests for the dense ANN index, with USearch deliberately absent.

The module promises two things that must hold *without* the optional extra: an
environment missing USearch gets an error naming the extra rather than a bare
``ModuleNotFoundError``, and the pure measurement helpers work anywhere. Both
are properties of the whole file, so USearch is made unimportable for every test
here and every distribution reports itself absent, exactly as in a ``uv sync``
without ``--extra ann``.

The round-trip, recall, and determinism tests that need a real graph live in
``tests/test_ontology_ann_index_real.py``, which skips when the extra is missing.
"""

from __future__ import annotations

import sys
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import Any

import numpy
import pytest

from spicy_regs.ontology import ann_index as ann_module
from spicy_regs.ontology.ann_index import (
    ANN_INDEX_SCHEMA_VERSION,
    QUANTIZATIONS,
    USEARCH_VERSION,
    AnnConceptIndex,
    AnnIndexError,
    UsearchConceptMapper,
    UsearchUnavailableError,
    ann_index_path,
    build_ann_concept_index,
    load_ann_concept_index,
    recall_against_exact,
    require_usearch,
    sidecar_path,
)
from spicy_regs.ontology.candidate_channels import DenseConceptIndex

BLOCKED_PACKAGES = ("usearch",)


def blocked_package(name: str) -> bool:
    return any(name == blocked or name.startswith(f"{blocked}.") for blocked in BLOCKED_PACKAGES)


class BlockedImportFinder:
    """Meta-path finder that fails any import of the blocked packages."""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if blocked_package(fullname):
            raise ImportError(f"{fullname} is deliberately unavailable in these tests")
        return None


def uninstalled_distribution(package: str) -> str:
    raise PackageNotFoundError(package)


@pytest.fixture(autouse=True)
def usearch_uninstalled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run every test in this file without the ``ann`` extra installed."""
    for name in [name for name in sys.modules if blocked_package(name)]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "meta_path", [BlockedImportFinder(), *sys.meta_path])
    monkeypatch.setattr(ann_module, "installed_package_version", uninstalled_distribution)


def absent_reader(package: str) -> str | None:
    return None


def pinned_reader(package: str) -> str | None:
    return USEARCH_VERSION


def wrong_version_reader(package: str) -> str | None:
    return "0.0.1"


def dense_fixture(rows: int = 8, dimensions: int = 4) -> DenseConceptIndex:
    generator = numpy.random.default_rng(7)
    matrix = generator.standard_normal((rows, dimensions)).astype(numpy.float32)
    matrix /= numpy.linalg.norm(matrix, axis=1, keepdims=True)
    return DenseConceptIndex(
        schema_version="concept-dense-index-v1",
        model_id="test-model",
        dimensions=dimensions,
        registry_digest="deadbeef" * 8,
        concept_ids=tuple(f"concept-{position:03d}" for position in range(rows)),
        matrix=matrix,
    )


# --- the missing-dependency path -------------------------------------------


def test_require_usearch_names_the_extra_when_absent() -> None:
    with pytest.raises(UsearchUnavailableError) as caught:
        require_usearch(absent_reader)
    message = str(caught.value)
    assert "ann" in message
    assert USEARCH_VERSION in message


def test_require_usearch_refuses_an_unpinned_version() -> None:
    with pytest.raises(UsearchUnavailableError) as caught:
        require_usearch(wrong_version_reader)
    assert "0.0.1" in str(caught.value)
    assert USEARCH_VERSION in str(caught.value)


def test_require_usearch_reports_metadata_without_the_package() -> None:
    """A distribution that claims the pin but cannot import is its own failure."""
    with pytest.raises(UsearchUnavailableError) as caught:
        require_usearch(pinned_reader)
    assert "cannot be imported" in str(caught.value)


def test_build_surfaces_the_unavailable_error_not_an_import_error() -> None:
    with pytest.raises(UsearchUnavailableError):
        build_ann_concept_index(dense_fixture(), version_reader=absent_reader)


def test_load_surfaces_the_unavailable_error_before_touching_the_file(tmp_path: Path) -> None:
    missing = tmp_path / "absent.usearch"
    with pytest.raises(UsearchUnavailableError):
        load_ann_concept_index(missing, concept_ids=("a",), version_reader=absent_reader)


def test_default_version_reader_is_the_module_level_one() -> None:
    """The autouse fixture patches one name; the module must actually use it."""
    with pytest.raises(UsearchUnavailableError):
        build_ann_concept_index(dense_fixture())


# --- pure helpers, which need nothing installed ----------------------------


def test_unsupported_quantization_is_refused_by_name() -> None:
    with pytest.raises(AnnIndexError) as caught:
        ann_index_path(Path("/tmp"), registry_digest="a" * 64, model_id="m", quantization="f64")
    assert "f64" in str(caught.value)


@pytest.mark.parametrize("quantization", QUANTIZATIONS)
def test_index_path_separates_every_stored_variant(quantization: str) -> None:
    path = ann_index_path(
        Path("/work"),
        registry_digest="abcdef0123456789" + "0" * 48,
        model_id="BAAI/bge-base-en-v1.5",
        quantization=quantization,
        connectivity=16,
        expansion_add=128,
    )
    assert path.name.endswith(".usearch")
    assert quantization in path.name
    assert "abcdef0123456789" in path.name
    assert "baai-bge-base-en-v1-5" in path.name


def test_index_paths_differ_by_connectivity_and_expansion() -> None:
    shared = {"registry_digest": "b" * 64, "model_id": "m", "quantization": "f16"}
    assert ann_index_path(Path("/w"), connectivity=16, expansion_add=128, **shared) != ann_index_path(
        Path("/w"), connectivity=32, expansion_add=128, **shared
    )
    assert ann_index_path(Path("/w"), connectivity=16, expansion_add=128, **shared) != ann_index_path(
        Path("/w"), connectivity=16, expansion_add=256, **shared
    )


def test_sidecar_path_sits_beside_the_graph() -> None:
    assert sidecar_path(Path("/w/ann-index-x.usearch")) == Path("/w/ann-index-x.usearch.meta.json")


def test_facts_are_secret_free_and_name_the_graph_shape() -> None:
    index = AnnConceptIndex(
        schema_version=ANN_INDEX_SCHEMA_VERSION,
        model_id="m",
        dimensions=4,
        registry_digest="c" * 64,
        concept_ids=("a", "b"),
        quantization="f16",
        connectivity=16,
        expansion_add=128,
        expansion_search=64,
        usearch_version=USEARCH_VERSION,
        viewed=True,
        handle=None,
    )
    facts = index.facts()
    assert facts["concept_count"] == 2
    assert facts["quantization"] == "f16"
    assert facts["metric"] == "cos"
    assert facts["usearch_version"] == USEARCH_VERSION


def test_blank_queries_never_reach_the_graph() -> None:
    """The guard runs before any search, so a handle-less index is enough."""
    index = AnnConceptIndex(
        schema_version=ANN_INDEX_SCHEMA_VERSION,
        model_id="m",
        dimensions=4,
        registry_digest="c" * 64,
        concept_ids=("a", "b"),
        quantization="f32",
        connectivity=16,
        expansion_add=128,
        expansion_search=64,
        usearch_version=USEARCH_VERSION,
        viewed=True,
        handle=None,
    )
    mapper = UsearchConceptMapper(index=index, embedder=None)  # ty: ignore[invalid-argument-type]
    assert mapper.rank(["", "   "], depth=5) == [[], []]
    assert mapper.rank(["real"], depth=0) == [[]]


def test_recall_is_perfect_when_the_sets_agree() -> None:
    exact = [["a", "b", "c"], ["d", "e", "f"]]
    result = recall_against_exact([list(row) for row in exact], exact, depth=3)
    assert result["macro_recall"] == 1.0
    assert result["micro_recall"] == 1.0
    assert result["min_query_recall"] == 1.0
    assert result["perfect_query_count"] == 2


def test_recall_counts_only_the_exact_set_members() -> None:
    exact = [["a", "b", "c", "d"]]
    approximate = [["a", "z", "c", "y"]]
    result = recall_against_exact(approximate, exact, depth=4)
    assert result["macro_recall"] == 0.5
    assert result["perfect_query_count"] == 0


def test_recall_respects_the_requested_depth() -> None:
    """A depth-12 recall must not be rescued by results ranked below 12."""
    exact = [["a", "b", "c", "d"]]
    approximate = [["z", "y", "a", "b"]]
    assert recall_against_exact(approximate, exact, depth=2)["macro_recall"] == 0.0
    assert recall_against_exact(approximate, exact, depth=4)["macro_recall"] == 0.5


def test_recall_macro_and_micro_differ_when_query_depths_differ() -> None:
    exact = [["a", "b", "c", "d"], ["e"]]
    approximate = [["a", "b", "c", "d"], ["z"]]
    result = recall_against_exact(approximate, exact, depth=4)
    assert result["micro_recall"] == 0.8
    assert result["macro_recall"] == 0.5
    assert result["min_query_recall"] == 0.0


def test_recall_ignores_repeated_ids_rather_than_double_counting() -> None:
    result = recall_against_exact([["a", "a", "b"]], [["a", "a", "b"]], depth=3)
    assert result["macro_recall"] == 1.0
    assert result["query_count"] == 1


def test_recall_skips_a_query_with_no_exact_results() -> None:
    result = recall_against_exact([[]], [[]], depth=5)
    assert result["query_count"] == 0
    assert result["macro_recall"] is None


def test_recall_refuses_mismatched_query_counts() -> None:
    with pytest.raises(AnnIndexError):
        recall_against_exact([["a"]], [["a"], ["b"]], depth=1)

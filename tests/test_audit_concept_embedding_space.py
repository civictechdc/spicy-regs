"""Tests for the concept embedding-space audit.

The audit's conclusion rests entirely on four statistics, so they are checked
against inputs whose geometry is known by construction rather than against the
audit's own output. Nothing here imports Sentence Transformers: the embedding
step is the one part that needs a model, and it is not what could be wrong
about a centroid norm.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "tools"))

from audit_concept_embedding_space import (  # noqa: E402
    definition_template,
    geometry,
    labels_only_text,
    markdown_table,
    registry_composition,
)


def normalized(matrix: numpy.ndarray) -> numpy.ndarray:
    return (matrix / numpy.linalg.norm(matrix, axis=1, keepdims=True)).astype(numpy.float32)


def concept(pref: str, *, alts: list[str] | None = None, definition: str = "", scheme: str = "s") -> dict:
    return {
        "pref_label": pref,
        "alt_labels_json": json.dumps(alts or []),
        "definition": definition,
        "scheme": scheme,
    }


# --- the two text builders -------------------------------------------------


def test_labels_only_drops_the_definition() -> None:
    row = concept("Accountants", definition="Federal Register Thesaurus topic covering Accountants.")
    assert labels_only_text(row) == "Accountants"


def test_labels_only_keeps_alt_labels_in_order() -> None:
    row = concept("Fisheries", alts=["Fishery management", "Fishing"], definition="x")
    assert labels_only_text(row) == "Fisheries; Fishery management; Fishing"


def test_labels_only_drops_case_insensitive_repeats() -> None:
    """Matches ``concept_embedding_text``'s rule, so the arms differ in one thing."""
    row = concept("Medicaid", alts=["medicaid", "  ", "Medical assistance"])
    assert labels_only_text(row) == "Medicaid; Medical assistance"


def test_labels_only_of_a_bare_concept_is_its_label() -> None:
    assert labels_only_text(concept("Acid rain")) == "Acid rain"


def test_definition_template_blanks_the_concepts_own_label() -> None:
    row = concept("Accountants", definition="FAST Topical facet term: Accountants.")
    assert definition_template(row) == "FAST Topical facet term: {LABEL}."


def test_two_boilerplate_definitions_collapse_to_one_template() -> None:
    """This is how boilerplate is detected without assuming a pattern."""
    first = concept("Accountants", definition="FAST Topical facet term: Accountants.")
    second = concept("Acid rain", definition="FAST Topical facet term: Acid rain.")
    assert definition_template(first) == definition_template(second)


def test_two_genuine_definitions_do_not_collapse() -> None:
    first = concept("Accountants", definition="A person who keeps financial records.")
    second = concept("Acid rain", definition="Precipitation with a low pH.")
    assert definition_template(first) != definition_template(second)


def test_definition_template_normalizes_whitespace() -> None:
    row = concept("Acid rain", definition="FAST  term:\n Acid rain.")
    assert definition_template(row) == "FAST term: {LABEL}."


# --- composition -----------------------------------------------------------


def test_composition_counts_schemes_and_shared_templates() -> None:
    rows = [
        concept("A", definition="FAST term: A.", scheme="fast-topical"),
        concept("B", definition="FAST term: B.", scheme="fast-topical"),
        concept("C", definition="A real, specific gloss.", scheme="subject"),
    ]
    facts = registry_composition(rows)
    assert facts["schemes"] == {"fast-topical": 2, "subject": 1}
    assert facts["distinct_definition_templates"] == 2
    assert facts["concepts_sharing_a_definition_template"] == 2
    assert facts["share_sharing_a_definition_template"] == pytest.approx(2 / 3)
    assert facts["concepts_with_empty_definition"] == 0


def test_composition_counts_empty_definitions() -> None:
    facts = registry_composition([concept("A"), concept("B", definition="real gloss")])
    assert facts["concepts_with_empty_definition"] == 1


# --- geometry --------------------------------------------------------------


def orthogonal_vectors(count: int = 8) -> numpy.ndarray:
    """Maximally spread unit vectors: every pairwise cosine is exactly zero."""
    return numpy.eye(count, dtype=numpy.float32)


def collapsed_vectors(count: int = 8, *, jitter: float = 1e-3) -> numpy.ndarray:
    """Every vector points essentially the same way."""
    generator = numpy.random.default_rng(3)
    base = numpy.zeros((count, 8), dtype=numpy.float32)
    base[:, 0] = 1.0
    return normalized(base + jitter * generator.standard_normal((count, 8)))


def test_orthogonal_vectors_have_a_zero_noise_floor() -> None:
    result = geometry(orthogonal_vectors(), orthogonal_vectors()[:2], depth=4)
    assert result["random_pair_cosine_mean"] == pytest.approx(0.0, abs=1e-6)


def test_orthogonal_centroid_norm_matches_the_closed_form() -> None:
    """For n orthogonal unit vectors the mean has norm 1/sqrt(n)."""
    result = geometry(orthogonal_vectors(9), orthogonal_vectors(9)[:2], depth=4)
    assert result["centroid_norm"] == pytest.approx(1 / 3, abs=1e-5)


def test_a_collapsed_cloud_is_distinguished_from_a_spread_one() -> None:
    spread = geometry(orthogonal_vectors(), orthogonal_vectors()[:2], depth=4)
    collapsed = geometry(collapsed_vectors(), collapsed_vectors()[:2], depth=4)
    assert collapsed["centroid_norm"] > 0.99
    assert collapsed["centroid_norm"] > spread["centroid_norm"]
    assert collapsed["random_pair_cosine_mean"] > 0.99
    assert collapsed["effective_dimensions"] < spread["effective_dimensions"]


def test_effective_dimensions_never_exceed_the_width() -> None:
    generator = numpy.random.default_rng(5)
    vectors = normalized(generator.standard_normal((200, 16)))
    result = geometry(vectors, vectors[:3], depth=10)
    assert 1.0 <= result["effective_dimensions"] <= 16.0
    assert result["dimensions"] == 16


def test_margin_is_top_one_minus_the_noise_floor() -> None:
    generator = numpy.random.default_rng(11)
    vectors = normalized(generator.standard_normal((64, 8)))
    result = geometry(vectors, vectors[:4], depth=5)
    assert result["top1_margin_over_noise_floor"] == pytest.approx(
        result["query_top1_cosine_mean"] - result["random_pair_cosine_mean"], abs=1e-6
    )


def test_a_query_that_is_an_indexed_vector_scores_one() -> None:
    """Top-1 is a real maximum, so a query already in the set retrieves itself."""
    vectors = orthogonal_vectors()
    result = geometry(vectors, vectors[:3], depth=4)
    assert result["query_top1_cosine_mean"] == pytest.approx(1.0, abs=1e-6)


def test_spread_is_the_gap_between_the_best_and_the_kth_result() -> None:
    vectors = orthogonal_vectors()
    result = geometry(vectors, vectors[:1], depth=4)
    # Best is 1.0 (itself), every other orthogonal vector is 0.0.
    assert result["query_top1_to_topk_spread_mean"] == pytest.approx(1.0, abs=1e-6)


def test_geometry_pairs_disjoint_halves_so_nothing_pairs_with_itself() -> None:
    """Pairing a vector with itself would report a noise floor of 1.0."""
    vectors = orthogonal_vectors(8)
    assert geometry(vectors, vectors[:2], depth=4)["random_pair_cosine_mean"] == pytest.approx(0.0, abs=1e-6)


def test_geometry_refuses_a_sample_too_small_to_pair() -> None:
    with pytest.raises(ValueError):
        geometry(numpy.eye(2, dtype=numpy.float32), numpy.eye(2, dtype=numpy.float32), depth=2)


def test_markdown_table_reports_both_arms() -> None:
    vectors = orthogonal_vectors()
    document = {
        "arms": {
            "current": {**geometry(vectors, vectors[:2], depth=4)},
            "labels-only": {**geometry(collapsed_vectors(), collapsed_vectors()[:2], depth=4)},
        }
    }
    table = markdown_table(document)
    assert "centroid norm" in table
    assert "top-1 margin over noise floor" in table
    assert table.count("\n") == 7

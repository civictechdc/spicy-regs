"""Hermetic tests for candidate channels C (dense) and D (generate-then-map).

No provider, no model weights, no network: the dense embedder and the
structured-text model are both injected fakes. What is asserted is what a
channel promises its caller — a deterministic ranking, an empty ranking for an
empty input, a cache that refuses a registry or model it was not built for, and
a keyword call that shows the model no vocabulary.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

import pytest

from spicy_regs.ontology.candidate_channels import (
    DENSE_INDEX_SCHEMA_VERSION,
    KEYWORD_INSTRUCTIONS,
    KEYWORD_MAX_COUNT,
    CharNgramConceptMapper,
    DenseConceptIndex,
    DenseConceptMapper,
    DenseIndexError,
    build_dense_concept_index,
    concept_embedding_text,
    dense_channel_ranking,
    dense_index_path,
    ensure_dense_concept_index,
    eligible_concepts,
    generate_segment_keywords,
    keyword_channel_ranking,
    keyword_output_schema,
    load_dense_concept_index,
    normalize_keywords,
    registry_embedding_digest,
    save_dense_concept_index,
)
from spicy_regs.ontology.concepts import clear_anchored_conditioning_cache

FAKE_DIMENSIONS = 256


@dataclass(frozen=True)
class _Vectors:
    vectors: tuple[tuple[float, ...], ...]


class FakeDenseEmbedder:
    """A deterministic bag-of-words embedder: same text in, same vector out.

    Token slots come from a sha1 digest rather than ``hash()``, whose string
    seed changes between interpreter runs — a channel that must be reproducible
    cannot be tested against a fake that is not.
    """

    dimensions = FAKE_DIMENSIONS

    def __init__(self, model_id: str = "fake-embedder:v1") -> None:
        self.model_id = model_id
        self.calls: list[list[str]] = []

    def _vector(self, text: str) -> tuple[float, ...]:
        slots = [0.0] * self.dimensions
        for token in re.findall(r"[a-z0-9]+", text.casefold()):
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            slots[int.from_bytes(digest[:4], "big") % self.dimensions] += 1.0
        norm = sum(value * value for value in slots) ** 0.5
        return tuple(value / norm for value in slots) if norm else tuple(slots)

    def embed(self, texts: Any) -> _Vectors:
        requested = [str(text) for text in texts]
        self.calls.append(requested)
        return _Vectors(vectors=tuple(self._vector(text) for text in requested))


@dataclass(frozen=True)
class _Result:
    output: dict[str, Any]
    call: dict[str, Any]


class FakeStructuredTextModel:
    """Records every request and returns a fixed keyword list."""

    model_id = "fake-model:v1"

    def __init__(self, keywords: list[Any]) -> None:
        self._keywords = keywords
        self.requests: list[dict[str, Any]] = []

    def secret_free_request(self, **kwargs: Any) -> dict[str, Any]:
        return {"name": kwargs["name"], "instructions": kwargs["instructions"], "payload": dict(kwargs["payload"])}

    def structured_json(self, **kwargs: Any) -> _Result:
        self.requests.append({key: value for key, value in kwargs.items()})
        return _Result(output={"keywords": list(self._keywords)}, call={"status": "completed", "attempt_count": 1})


def _concept(concept_id: str, scheme: str, pref: str, alt: list[str] | None = None, definition: str = "") -> dict:
    return {
        "concept_id": concept_id,
        "scheme": scheme,
        "pref_label": pref,
        "alt_labels_json": json.dumps(alt or []),
        "definition": definition,
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": "[]",
    }


@pytest.fixture
def registry() -> list[dict]:
    return [
        _concept("concept_fish", "subject", "Fishery management", ["Fisheries management"], "Managing fish stocks."),
        _concept("concept_immi", "subject", "Emigration and immigration law", ["Immigration law"]),
        _concept("concept_speech", "subject", "Freedom of speech", ["Free speech"]),
        _concept("concept_mine", "crs-subjects", "Surface mining"),
        _concept("concept_gone", "crs-subjects", "Retired concept") | {"status": "deprecated"},
    ]


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_anchored_conditioning_cache()
    yield
    clear_anchored_conditioning_cache()


# --------------------------------------------------------------------------
# embedding inputs and the cache key
# --------------------------------------------------------------------------


def test_embedding_text_joins_labels_then_definition(registry):
    assert concept_embedding_text(registry[0]) == "Fishery management; Fisheries management; Managing fish stocks."


def test_embedding_text_drops_repeated_surface_forms():
    concept = _concept("concept_x", "subject", "Medicaid", ["medicaid", "Medicaid", "Medical assistance"])
    assert concept_embedding_text(concept) == "Medicaid; Medical assistance"


def test_embedding_text_survives_broken_alt_labels():
    concept = _concept("concept_x", "subject", "Trademarks")
    concept["alt_labels_json"] = "{not json"
    assert concept_embedding_text(concept) == "Trademarks"


def test_eligible_concepts_drops_deprecated_rows(registry):
    assert [concept["concept_id"] for concept in eligible_concepts(registry)] == [
        "concept_fish",
        "concept_immi",
        "concept_speech",
        "concept_mine",
    ]


def test_registry_digest_is_stable_and_content_sensitive(registry):
    baseline = registry_embedding_digest(registry)
    assert baseline == registry_embedding_digest(list(registry))
    changed = [dict(row) for row in registry]
    changed[0]["pref_label"] = "Fisheries administration"
    assert registry_embedding_digest(changed) != baseline


def test_registry_digest_ignores_deprecated_rows(registry):
    assert registry_embedding_digest(registry) == registry_embedding_digest(registry[:-1])


# --------------------------------------------------------------------------
# channel C — index, cache, retrieval
# --------------------------------------------------------------------------


def test_index_covers_eligible_rows_in_order(registry):
    index = build_dense_concept_index(registry, embedder=FakeDenseEmbedder())
    assert index.concept_ids == ("concept_fish", "concept_immi", "concept_speech", "concept_mine")
    assert index.matrix.shape == (4, FAKE_DIMENSIONS)
    assert index.schema_version == DENSE_INDEX_SCHEMA_VERSION


def test_index_rows_are_unit_length(registry):
    import numpy

    index = build_dense_concept_index(registry, embedder=FakeDenseEmbedder())
    assert numpy.allclose(numpy.linalg.norm(index.matrix, axis=1), 1.0)


def test_index_is_chunk_size_independent(registry):
    import numpy

    whole = build_dense_concept_index(registry, embedder=FakeDenseEmbedder())
    chunked = build_dense_concept_index(registry, embedder=FakeDenseEmbedder(), chunk_size=2)
    assert numpy.array_equal(whole.matrix, chunked.matrix)
    assert whole.concept_ids == chunked.concept_ids


def test_index_build_reports_progress(registry):
    seen: list[tuple[int, int]] = []
    build_dense_concept_index(
        registry, embedder=FakeDenseEmbedder(), chunk_size=3, on_progress=lambda a, b: seen.append((a, b))
    )
    assert seen == [(3, 4), (4, 4)]


def test_index_rejects_a_non_positive_chunk_size(registry):
    with pytest.raises(ValueError):
        build_dense_concept_index(registry, embedder=FakeDenseEmbedder(), chunk_size=0)


def test_index_round_trips_through_disk(tmp_path, registry):
    import numpy

    embedder = FakeDenseEmbedder()
    index = build_dense_concept_index(registry, embedder=embedder)
    path = dense_index_path(tmp_path, registry_digest=index.registry_digest, model_id=index.model_id)
    save_dense_concept_index(index, path)
    restored = load_dense_concept_index(path, registry_digest=index.registry_digest, model_id=index.model_id)
    assert restored.concept_ids == index.concept_ids
    assert numpy.array_equal(restored.matrix, index.matrix)
    assert not list(path.parent.glob("*.partial"))


def test_stored_index_refuses_a_different_registry(tmp_path, registry):
    index = build_dense_concept_index(registry, embedder=FakeDenseEmbedder())
    path = tmp_path / "index.npz"
    save_dense_concept_index(index, path)
    with pytest.raises(DenseIndexError):
        load_dense_concept_index(path, registry_digest="0" * 64)


def test_stored_index_refuses_a_different_model(tmp_path, registry):
    index = build_dense_concept_index(registry, embedder=FakeDenseEmbedder())
    path = tmp_path / "index.npz"
    save_dense_concept_index(index, path)
    with pytest.raises(DenseIndexError):
        load_dense_concept_index(path, model_id="other-model:v9")


def test_ensure_builds_once_then_reads_the_cache(tmp_path, registry):
    embedder = FakeDenseEmbedder()
    first, first_facts = ensure_dense_concept_index(registry, embedder=embedder, directory=tmp_path)
    embed_calls = len(embedder.calls)
    second, second_facts = ensure_dense_concept_index(registry, embedder=embedder, directory=tmp_path)
    assert first_facts["source"] == "built"
    assert second_facts["source"] == "cache"
    assert len(embedder.calls) == embed_calls
    assert second.concept_ids == first.concept_ids


def test_ensure_rebuilds_for_a_changed_registry(tmp_path, registry):
    embedder = FakeDenseEmbedder()
    ensure_dense_concept_index(registry, embedder=embedder, directory=tmp_path)
    changed = [*registry, _concept("concept_new", "subject", "Poultry inspection")]
    _, facts = ensure_dense_concept_index(changed, embedder=embedder, directory=tmp_path)
    assert facts["source"] == "built"
    assert len(list(tmp_path.glob("dense-index-*.npz"))) == 2


def test_index_rejects_a_mismatched_matrix(registry):
    import numpy

    with pytest.raises(DenseIndexError):
        DenseConceptIndex(
            schema_version=DENSE_INDEX_SCHEMA_VERSION,
            model_id="fake-embedder:v1",
            dimensions=FAKE_DIMENSIONS,
            registry_digest="0" * 64,
            concept_ids=("concept_a", "concept_b"),
            matrix=numpy.zeros((1, FAKE_DIMENSIONS), dtype=numpy.float32),
        )


def _mapper(registry: list[dict]) -> DenseConceptMapper:
    embedder = FakeDenseEmbedder()
    return DenseConceptMapper(index=build_dense_concept_index(registry, embedder=embedder), embedder=embedder)


def test_dense_channel_finds_the_alias_that_lexical_matching_cannot(registry):
    ranked = dense_channel_ranking(
        "The rule concerns fisheries management in coastal waters.", mapper=_mapper(registry)
    )
    assert ranked[0] == "concept_fish"


def test_dense_channel_is_deterministic(registry):
    mapper = _mapper(registry)
    text = "Immigration law and visa adjudication."
    assert dense_channel_ranking(text, mapper=mapper) == dense_channel_ranking(text, mapper=mapper)


def test_dense_channel_returns_nothing_for_blank_text(registry):
    assert dense_channel_ranking("   ", mapper=_mapper(registry)) == []


def test_dense_channel_respects_depth(registry):
    assert len(dense_channel_ranking("mining and fisheries and speech", mapper=_mapper(registry), depth=2)) == 2


def test_dense_mapper_returns_one_row_per_query_keeping_blanks_aligned(registry):
    ranked = _mapper(registry).rank(["fisheries management", "  ", "surface mining"], depth=2)
    assert len(ranked) == 3
    assert ranked[1] == []
    assert ranked[0][0][0] == "concept_fish"
    assert ranked[2][0][0] == "concept_mine"


def test_dense_mapper_breaks_ties_by_concept_id(registry):
    twins = [
        _concept("concept_zzz", "subject", "Fishery management"),
        _concept("concept_aaa", "subject", "Fishery management"),
    ]
    ranked = _mapper(twins).rank(["Fishery management"], depth=2)[0]
    assert [concept_id for concept_id, _ in ranked] == ["concept_aaa", "concept_zzz"]


def test_dense_mapper_returns_nothing_at_zero_depth(registry):
    assert _mapper(registry).rank(["fisheries"], depth=0) == [[]]


# --------------------------------------------------------------------------
# channel D — generation and mapping
# --------------------------------------------------------------------------


def test_keyword_schema_is_a_strict_flat_string_array():
    schema = keyword_output_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["keywords"]
    assert schema["properties"]["keywords"]["maxItems"] == KEYWORD_MAX_COUNT
    assert schema["properties"]["keywords"]["items"] == {"type": "string", "minLength": 1}


def test_normalize_keywords_trims_deduplicates_and_caps():
    assert normalize_keywords(["  Fisheries   management ", "fisheries management", "", None, "Quotas"]) == (
        "Fisheries management",
        "Quotas",
    )
    assert len(normalize_keywords([f"keyword {index}" for index in range(50)])) == KEYWORD_MAX_COUNT


def test_keyword_generation_shows_the_model_no_vocabulary(registry):
    model = FakeStructuredTextModel(["fisheries management", "quotas"])
    generation = generate_segment_keywords("Catch limits for the coastal fishery.", model=model)
    request = model.requests[0]
    assert request["instructions"] == KEYWORD_INSTRUCTIONS
    assert set(request["payload"]) == {"segment_text"}
    assert generation.keywords == ("fisheries management", "quotas")
    assert generation.request["payload"]["segment_text"] == "Catch limits for the coastal fishery."
    assert generation.call["status"] == "completed"


def test_keyword_generation_is_one_call_per_segment():
    model = FakeStructuredTextModel(["a", "b"])
    generate_segment_keywords("text", model=model)
    assert len(model.requests) == 1


def test_keyword_generation_tolerates_a_missing_or_odd_list():
    class _Odd(FakeStructuredTextModel):
        def structured_json(self, **kwargs: Any) -> _Result:
            return _Result(output={}, call={"status": "completed"})

    assert generate_segment_keywords("text", model=_Odd([])).keywords == ()


def test_keyword_channel_ranks_by_the_best_single_keyword(registry):
    ranked = keyword_channel_ranking(["fisheries management", "surface mining"], mapper=_mapper(registry))
    assert ranked[:2] == ["concept_fish", "concept_mine"] or ranked[:2] == ["concept_mine", "concept_fish"]
    assert "concept_fish" in ranked and "concept_mine" in ranked


def test_keyword_channel_reaches_an_alias_no_lexical_channel_sees(registry):
    assert keyword_channel_ranking(["immigration law"], mapper=_mapper(registry))[0] == "concept_immi"


def test_keyword_channel_is_deterministic(registry):
    mapper = _mapper(registry)
    keywords = ["fisheries management", "free speech", "immigration law"]
    assert keyword_channel_ranking(keywords, mapper=mapper) == keyword_channel_ranking(keywords, mapper=mapper)


def test_keyword_channel_ignores_keyword_order(registry):
    mapper = _mapper(registry)
    forward = keyword_channel_ranking(["free speech", "surface mining"], mapper=mapper)
    backward = keyword_channel_ranking(["surface mining", "free speech"], mapper=mapper)
    assert forward == backward


def test_keyword_channel_returns_nothing_without_keywords(registry):
    mapper = _mapper(registry)
    assert keyword_channel_ranking([], mapper=mapper) == []
    assert keyword_channel_ranking(["", "   ", None], mapper=mapper) == []


def test_keyword_channel_respects_depth(registry):
    ranked = keyword_channel_ranking(["fisheries", "mining", "speech"], mapper=_mapper(registry), depth=2)
    assert len(ranked) == 2


# --------------------------------------------------------------------------
# the char-ngram fallback mapper
# --------------------------------------------------------------------------


def test_fallback_mapper_maps_keywords_without_an_embedder(registry):
    mapper = CharNgramConceptMapper(registry)
    ranked = keyword_channel_ranking(["fishery management"], mapper=mapper)
    assert ranked[0] == "concept_fish"


def test_fallback_mapper_skips_deprecated_rows(registry):
    mapper = CharNgramConceptMapper(registry)
    ranked = keyword_channel_ranking(["retired concept"], mapper=mapper, depth=10)
    assert "concept_gone" not in ranked


def test_fallback_mapper_is_deterministic(registry):
    mapper = CharNgramConceptMapper(registry)
    assert mapper.rank(["free speech"], depth=3) == mapper.rank(["free speech"], depth=3)


def test_fallback_mapper_returns_nothing_for_blank_queries(registry):
    assert CharNgramConceptMapper(registry).rank(["", " "], depth=5) == [[], []]

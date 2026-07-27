"""Dense retrieval contracts for the Step 5.2 migration slice."""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters.sentence_transformers import DenseEmbeddingResult
from spicy_regs.docpipeline.retrieval import (
    DENSE_ARTIFACT_INPUT_POLICY,
    DENSE_EMBEDDING_COLUMNS,
    DENSE_EMBEDDING_TABLE,
    DENSE_MODEL_ID,
    DenseEmbeddingRow,
    DenseProviderError,
    DenseSourceField,
    RetrievalQuery,
    compose_dense_vector,
    dense_artifact_search,
    dense_segment_search,
    dense_source_fields_from_segments,
    derive_dense_semantic_units,
    rank_dense_vectors,
    rebuild_dense_artifact_hits,
    rebuild_dense_segment_hits,
)
from spicy_regs.docpipeline.runtime import PlanError


class CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


class FakeDenseEmbedder:
    provider: str = "sentence-transformers"
    model_id: str = DENSE_MODEL_ID
    dimensions: int = 2
    tokenizer_id: str = f"{DENSE_MODEL_ID}:tokenizer"
    max_input_tokens: int | None = 4
    production_provider: bool = False

    def __init__(
        self,
        vectors: dict[str, tuple[float, float]] | None = None,
        *,
        fail: bool = False,
    ) -> None:
        self.vectors = vectors or {}
        self.fail = fail
        self.calls: list[tuple[str, ...]] = []

    @staticmethod
    def model_token_count(text: str) -> int:
        return len(text.split())

    def embed(self, texts: Sequence[str]) -> DenseEmbeddingResult:
        requested = tuple(texts)
        self.calls.append(requested)
        if self.fail:
            raise RuntimeError("provider fixture failed")
        vectors = tuple(self.vectors.get(text, (1.0, 0.0)) for text in requested)
        counts = tuple(self.model_token_count(text) for text in requested)
        limit = self.max_input_tokens
        return DenseEmbeddingResult(
            vectors=vectors,
            call={
                "provider": self.provider,
                "operation": "dense-embedding",
                "package_name": "sentence-transformers",
                "package_version": "5.6.1",
                "encoder_source": "injected",
                "model_id": self.model_id,
                "model": "BAAI/bge-base-en-v1.5",
                "revision": self.model_id.rsplit("@", 1)[1],
                "dimensions": self.dimensions,
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
                    "normalize_embeddings": True,
                    "trust_remote_code": False,
                },
            },
        )


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _artifact_candidate(target_id: str, digest: str) -> dict[str, Any]:
    return {
        "target_id": target_id,
        "artifact_id": target_id,
        "segment_id": None,
        "source_table": "dockets",
        "subject_id": f"subject-{target_id}",
        "artifact_digest": digest,
        "profile_id": "regulations-docket-v2",
        "subject_type": "docket",
        "access_scope": "public",
        "access_basis": "us-federal-public-record",
        "text": None,
        "text_sha256": None,
        "slices_json": None,
    }


def _source_field(
    artifact_id: str,
    digest: str,
    text: str,
    *,
    source_field: str = "dockets.title",
    ordinal: int = 0,
) -> DenseSourceField:
    return DenseSourceField(
        artifact_id=artifact_id,
        artifact_digest=digest,
        source_table="dockets",
        subject_id=f"subject-{artifact_id}",
        source_field=source_field,
        ordinal=ordinal,
        field_sha256=_digest(text),
        text=text,
    )


def _segment_candidate(
    segment_id: str,
    artifact_id: str,
    digest: str,
    source_text: str,
    *,
    start: int,
    end: int,
) -> dict[str, Any]:
    text = source_text[start:end]
    slices = [
        {
            "region_id": f"region-{segment_id}",
            "fragment_id": f"fragment-{segment_id}",
            "region_kind": "paragraph",
            "source_field": "dockets.title",
            "field_sha256": _digest(source_text),
            "start_char": start,
            "end_char": end,
            "overlap_chars": 0,
            "evidence_grade": "source-exact",
            "content_layer": "body",
            "coordinate_grade": "source-exact",
            "context_only": False,
            "text_sha256": _digest(text),
            "text": text,
        }
    ]
    return {
        "target_id": segment_id,
        "artifact_id": artifact_id,
        "segment_id": segment_id,
        "source_table": "dockets",
        "subject_id": f"subject-{artifact_id}",
        "artifact_digest": digest,
        "profile_id": "regulations-docket-v2",
        "subject_type": "docket",
        "access_scope": "public",
        "access_basis": "us-federal-public-record",
        "text": text,
        "text_sha256": _digest(text),
        "slices_json": json.dumps(slices, sort_keys=True, separators=(",", ":")),
    }


def test_semantic_units_exactly_preserve_predecessor_240_80_boundaries() -> None:
    text = ("A" * 240) + ("B" * 240) + ("C" * 10)
    field = _source_field("a1", "digest-a1", text)

    units = derive_dense_semantic_units((field,), counter=CharacterCounter())

    assert [(unit.start_char, unit.end_char, unit.text) for unit in units] == [
        (0, 240, "A" * 240),
        (240, 480, "B" * 240),
        (480, 490, "C" * 10),
    ]
    assert [unit.ordinal for unit in units] == [0, 1, 2]
    assert all(unit.token_count == len(unit.text) for unit in units)
    assert derive_dense_semantic_units((field,), counter=CharacterCounter()) == units


def test_weighted_composition_and_zero_weight_fallback_match_predecessor() -> None:
    assert compose_dense_vector(((1.0, 0.0), (0.0, 1.0)), (3.0, 1.0)) == (0.75, 0.25)
    assert compose_dense_vector(((1.0, 0.0), (0.0, 1.0)), (0.0, 0.0)) == (0.5, 0.5)


def test_dense_ranking_l2_normalizes_uses_dot_and_breaks_ties_by_target() -> None:
    ranked = rank_dense_vectors(
        ("b", "zero", "a"),
        ((3.0, 0.0), (0.0, 0.0), (1.0, 0.0)),
        (2.0, 0.0),
        limit=3,
    )

    assert ranked == (("a", 1.0), ("b", 1.0), ("zero", 0.0))
    module = __import__("spicy_regs.docpipeline.retrieval", fromlist=["x"])
    assert module.__file__ is not None
    source = Path(module.__file__).read_text()
    assert "np.dot(" in source
    assert "np.matmul(" not in source


def test_artifact_dense_search_uses_exact_all_profile_text_without_query_prefix(
    tmp_path: Path,
) -> None:
    candidate = _artifact_candidate("a1", "digest-a1")
    fields = (
        _source_field("a1", "digest-a1", "Alpha title"),
        _source_field(
            "a1",
            "digest-a1",
            "Body text",
            source_field="dockets.abstract",
            ordinal=1,
        ),
    )
    artifact_text = "[SOURCE_FIELD dockets.title]\nAlpha title\n\n[SOURCE_FIELD dockets.abstract]\nBody text"
    embedder = FakeDenseEmbedder(
        {
            artifact_text: (1.0, 0.0),
            "plain query": (1.0, 0.0),
        }
    )

    outcome = dense_artifact_search(
        (candidate,),
        fields,
        query=RetrievalQuery("q1", "plain query", "artifact"),
        work_id="work-a",
        embedder=embedder,
        run_directory=tmp_path,
    )

    assert outcome.state == "completed"
    assert [hit.target_id for hit in outcome.hits] == ["a1"]
    assert sorted(text for call in embedder.calls for text in call) == sorted([artifact_text, "plain query"])
    assert all("Represent this sentence" not in text for call in embedder.calls for text in call)
    target = next(row for row in outcome.embeddings if row.input_kind == "artifact")
    assert target.input_policy == DENSE_ARTIFACT_INPUT_POLICY == "all-profile-whole-artifact-v1"
    assert target.input_text == artifact_text


def test_segment_dense_search_composes_unit_vectors_by_overlap_characters(
    tmp_path: Path,
) -> None:
    source_text = ("A" * 240) + ("B" * 240)
    fields = (_source_field("a1", "digest-a1", source_text),)
    candidates = (
        _segment_candidate("weighted", "a1", "digest-a1", source_text, start=120, end=280),
        _segment_candidate("second", "a1", "digest-a1", source_text, start=240, end=480),
    )
    embedder = FakeDenseEmbedder(
        {
            "A" * 240: (1.0, 0.0),
            "B" * 240: (0.0, 1.0),
            "query": (1.0, 0.0),
        }
    )

    outcome = dense_segment_search(
        candidates,
        fields,
        query=RetrievalQuery("q1", "query", "segment"),
        work_id="work-s",
        embedder=embedder,
        counter=CharacterCounter(),
        run_directory=tmp_path,
    )

    assert [hit.target_id for hit in outcome.hits] == ["weighted", "second"]
    weighted = next(row for row in outcome.embeddings if row.input_kind == "segment" and row.target_id == "weighted")
    assert weighted.vector == pytest.approx((0.75, 0.25))
    details = json.loads(weighted.call_json)
    assert details["overlap_characters"] == [120, 40]
    rebuilt = rebuild_dense_segment_hits(
        candidates,
        query=RetrievalQuery("q1", "query", "segment"),
        work_id="work-s",
        run_directory=tmp_path,
    )
    assert rebuilt == outcome.hits


def test_public_segment_slices_reconstruct_exact_source_fields_and_reject_gaps() -> None:
    source_text = ("A" * 240) + ("B" * 240)
    first = _segment_candidate("s1", "a1", "digest-a1", source_text, start=0, end=300)
    second = _segment_candidate("s2", "a1", "digest-a1", source_text, start=240, end=480)
    first["ordinal"] = 0
    second["ordinal"] = 1

    fields = dense_source_fields_from_segments((second, first))

    assert fields == (_source_field("a1", "digest-a1", source_text),)
    with pytest.raises(PlanError, match="gaps"):
        dense_source_fields_from_segments((second,))


def test_artifact_and_segment_entry_points_keep_grains_and_cache_state_separate(
    tmp_path: Path,
) -> None:
    artifact = _artifact_candidate("a1", "digest-a1")
    source_text = "shared query"
    fields = (_source_field("a1", "digest-a1", source_text),)
    segment = _segment_candidate("s1", "a1", "digest-a1", source_text, start=0, end=len(source_text))
    embedder = FakeDenseEmbedder()

    artifact_outcome = dense_artifact_search(
        (artifact,),
        fields,
        query=RetrievalQuery("qa", "shared query", "artifact"),
        work_id="work",
        embedder=embedder,
        run_directory=tmp_path,
    )
    calls_after_artifact = len(embedder.calls)
    segment_outcome = dense_segment_search(
        (segment,),
        fields,
        query=RetrievalQuery("qs", "shared query", "segment"),
        work_id="work",
        embedder=embedder,
        counter=CharacterCounter(),
        run_directory=tmp_path,
    )

    assert len(embedder.calls) == calls_after_artifact + 1
    assert artifact_outcome.hits[0].segment_id is None
    assert segment_outcome.hits[0].segment_id == "s1"
    assert {row.level for row in segment_outcome.embeddings} == {"segment"}
    stored = pq.read_table(tmp_path / DENSE_EMBEDDING_TABLE).to_pylist()
    assert {row["level"] for row in stored} == {"artifact", "segment"}


def test_dense_zero_row_table_is_typed_and_empty_work_never_calls_provider(
    tmp_path: Path,
) -> None:
    embedder = FakeDenseEmbedder(fail=True)

    outcome = dense_artifact_search(
        (),
        (),
        query=RetrievalQuery("q1", "query", "artifact"),
        work_id="empty-work",
        embedder=embedder,
        run_directory=tmp_path,
    )

    table = pq.read_table(tmp_path / DENSE_EMBEDDING_TABLE)
    assert outcome.state == "completed_empty"
    assert outcome.hits == ()
    assert outcome.embeddings == ()
    assert embedder.calls == []
    assert table.num_rows == 0
    assert table.schema.names == [name for name, _ in DENSE_EMBEDDING_COLUMNS]
    assert (
        rebuild_dense_artifact_hits(
            (),
            query=RetrievalQuery("q1", "query", "artifact"),
            work_id="empty-work",
            run_directory=tmp_path,
        )
        == ()
    )


def test_dense_provider_failure_is_classified_and_does_not_persist_partial_rows(
    tmp_path: Path,
) -> None:
    with pytest.raises(DenseProviderError, match="RuntimeError"):
        dense_artifact_search(
            (_artifact_candidate("a1", "digest-a1"),),
            (_source_field("a1", "digest-a1", "text"),),
            query=RetrievalQuery("q1", "query", "artifact"),
            work_id="failed-work",
            embedder=FakeDenseEmbedder(fail=True),
            run_directory=tmp_path,
        )
    assert not (tmp_path / DENSE_EMBEDDING_TABLE).exists()


def test_dense_resume_and_provider_free_rebuild_use_stored_vectors(
    tmp_path: Path,
) -> None:
    candidates = (_artifact_candidate("a1", "digest-a1"),)
    fields = (_source_field("a1", "digest-a1", "text"),)
    query = RetrievalQuery("q1", "query", "artifact")
    first_provider = FakeDenseEmbedder({"text": (1.0, 0.0), "query": (1.0, 0.0)})
    first = dense_artifact_search(
        candidates,
        fields,
        query=query,
        work_id="resume-work",
        embedder=first_provider,
        run_directory=tmp_path,
    )
    refusing_provider = FakeDenseEmbedder(fail=True)

    resumed = dense_artifact_search(
        candidates,
        fields,
        query=query,
        work_id="resume-work",
        embedder=refusing_provider,
        run_directory=tmp_path,
    )
    rebuilt = rebuild_dense_artifact_hits(
        candidates,
        query=query,
        work_id="resume-work",
        run_directory=tmp_path,
    )

    assert first.hits == resumed.hits == rebuilt
    assert len(first_provider.calls) == 1
    assert refusing_provider.calls == []

    drifted_provider = FakeDenseEmbedder(fail=True)
    drifted_provider.provider = "another-provider"
    with pytest.raises(PlanError, match="provider"):
        dense_artifact_search(
            candidates,
            fields,
            query=query,
            work_id="resume-work",
            embedder=drifted_provider,
            run_directory=tmp_path,
        )
    assert drifted_provider.calls == []


def test_dense_rows_are_frozen_typed_and_propagate_token_audit_facts(
    tmp_path: Path,
) -> None:
    outcome = dense_artifact_search(
        (_artifact_candidate("a1", "digest-a1"),),
        (_source_field("a1", "digest-a1", "one two three four five"),),
        query=RetrievalQuery("q1", "six seven", "artifact"),
        work_id="audit-work",
        embedder=FakeDenseEmbedder(),
        run_directory=tmp_path,
    )

    assert all(isinstance(row, DenseEmbeddingRow) for row in outcome.embeddings)
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(outcome.embeddings[0], "input_text", "changed")
    target = next(row for row in outcome.embeddings if row.input_kind == "artifact")
    assert target.untruncated_token_count == 7
    assert target.input_limit == 4
    assert target.would_truncate is True
    assert target.token_audit_status == "exact-untruncated-model-tokenizer"
    assert target.model_id == DENSE_MODEL_ID
    assert target.model_revision == DENSE_MODEL_ID.rsplit("@", 1)[1]
    assert target.vector_id
    assert target.call_input_index >= 0
    call = json.loads(target.call_json)
    assert call["provider_invoked"] is True
    assert target.untruncated_token_count == call["token_counts"][target.call_input_index]
    assert target.would_truncate == call["inputs_over_limit"][target.call_input_index]

    with pytest.raises(ValueError, match="model"):
        dataclasses.replace(target, model_revision="drift")
    with pytest.raises(ValueError, match="provider"):
        dataclasses.replace(target, provider="another-provider")
    with pytest.raises(ValueError, match="provider"):
        dataclasses.replace(target, operation="another-operation")
    with pytest.raises(ValueError, match="provider"):
        dataclasses.replace(target, provider_invoked=1)
    with pytest.raises(ValueError, match="attempt"):
        dataclasses.replace(target, attempt_count=0)
    with pytest.raises(ValueError, match="canonical"):
        dataclasses.replace(target, call_json=json.dumps(call, indent=2, sort_keys=True))
    changed_call = {**call, "provider": "another-provider"}
    with pytest.raises(ValueError, match="provenance"):
        dataclasses.replace(
            target,
            call_json=json.dumps(changed_call, sort_keys=True, separators=(",", ":")),
        )


def test_dense_import_boundaries_and_legacy_runners_remain_untouched() -> None:
    module = __import__("spicy_regs.docpipeline.retrieval", fromlist=["x"])
    assert module.__file__ is not None
    path = Path(module.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None}
    imports.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)

    assert not any(name == "spicy_regs.corpora" or name.startswith("spicy_regs.corpora.") for name in imports)
    assert not {"sentence_transformers", "torch", "tiktoken", "openai"} & imports
    private_step_imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module in {"spicy_regs.docpipeline.source", "spicy_regs.docpipeline.segments"}
        for alias in node.names
        if alias.name.startswith("_")
    }
    assert private_step_imports == set()
    assert "incumbent-three-table-whole-row-v1" not in source
    assert Path("src/spicy_regs/corpora/segmentation_experiment.py").is_file()
    assert Path("src/spicy_regs/corpora/artifact_retrieval_baseline.py").is_file()
    project = Path("pyproject.toml").read_text(encoding="utf-8")
    assert "run-segmentation-experiment" in project
    assert "run-artifact-retrieval-baseline" in project

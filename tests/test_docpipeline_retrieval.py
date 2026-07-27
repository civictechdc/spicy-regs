"""Contracts for the Step 5.1 retrieval foundation.

The ranking algorithms land in Step 5.2.  These tests hold the typed row
surface, plan facts, candidate-universe assembly, prefilter attribution, and
the import/data-flow boundaries that must be stable before ranking begins.
"""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.extraction import ModelInputLeakError, refuse_retrieval_aids
from spicy_regs.docpipeline.retrieval import (
    FILTER_AXES,
    HIT_COLUMNS,
    IR_MEASURES_VERSION,
    PLANNED_RETRIEVAL_METHODS,
    RETRIEVAL_CANDIDATE_LIMIT,
    RETRIEVAL_EXCLUSION_COLUMNS,
    RETRIEVAL_FUSION_INPUT_DEPTH,
    RETRIEVAL_HIT_TABLE,
    RETRIEVAL_RERANK_DEPTH,
    RETRIEVAL_RRF_K,
    FilterRequest,
    RetrievalExclusion,
    RetrievalHit,
    RetrievalQuery,
    RetrievalSpec,
    apply_prefilters,
    candidate_metadata_join_key,
    candidate_metadata_row,
    construct_candidate_universe,
    retrieval_plan_facts,
    write_retrieval_tables,
)
from spicy_regs.docpipeline.runtime import IMMUTABLE_RUN_PATTERNS, PlanError
from spicy_regs.docpipeline.segments import SEGMENT_COLUMNS, SEGMENT_TABLE
from spicy_regs.docpipeline.source import (
    ARTIFACT_COLUMNS,
    ARTIFACT_TABLE,
    write_table,
)


def _artifact(
    artifact_id: str,
    subject_id: str,
    digest: str,
    *,
    profile_id: str = "regulations-docket-v2",
    source_table: str = "dockets",
) -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "content_sha256": digest,
        "subject_type": "fixture",
        "subject_id": subject_id,
        "profile_id": profile_id,
        "source_table": source_table,
        "access_scope": "public",
        "access_basis": "us-federal-public-record",
    }


def _segment(
    segment_id: str,
    artifact_id: str,
    subject_id: str,
    digest: str,
    *,
    profile_id: str = "regulations-docket-v2",
    source_table: str = "dockets",
) -> dict[str, object]:
    return {
        "segment_id": segment_id,
        "content_digest": f"content-{segment_id}",
        "artifact_id": artifact_id,
        "artifact_sha256": digest,
        "subject_type": "fixture",
        "subject_id": subject_id,
        "profile_id": profile_id,
        "source_table": source_table,
        "ordinal": 1,
        "text_sha256": f"text-{segment_id}",
        "text": f"text for {segment_id}",
        "slices_json": "[]",
    }


def _candidate(
    target_id: str,
    artifact_id: str,
    subject_id: str,
    digest: str,
    *,
    profile_id: str = "regulations-docket-v2",
    source_table: str = "dockets",
    access_scope: str = "public",
) -> dict[str, object]:
    return {
        "target_id": target_id,
        "artifact_id": artifact_id,
        "segment_id": None,
        "source_table": source_table,
        "subject_id": subject_id,
        "artifact_digest": digest,
        "profile_id": profile_id,
        "subject_type": "fixture",
        "access_scope": access_scope,
        "access_basis": "us-federal-public-record",
        "text": None,
        "text_sha256": None,
        "slices_json": None,
    }


def test_records_are_frozen_validated_and_use_closed_values() -> None:
    query = RetrievalQuery("q1", "What changed?", "artifact")
    request = FilterRequest("identity", ("a1",))
    spec = RetrievalSpec(filters=(request,))
    hit = RetrievalHit(
        work_id="w1",
        query_id="q1",
        level="artifact",
        method="dense",
        target_id="a1",
        artifact_id="a1",
        segment_id=None,
        source_table="dockets",
        subject_id="D-1",
        artifact_digest="digest-1",
        rank=1,
        candidate_universe_size=3,
        candidate_input_size=3,
        candidate_limit=200,
        score=0.5,
        score_kind="cosine",
    )
    exclusion = RetrievalExclusion(
        work_id="w1",
        query_id="q1",
        level="artifact",
        target_id="a2",
        source_table="dockets",
        subject_id="D-2",
        artifact_digest="digest-2",
        filter="identity",
        reason="mismatch-identity",
        detail="not in the accepted identity set",
    )

    for value in (query, request, spec, hit, exclusion):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(value, "changed", True)

    with pytest.raises(ValueError, match="level"):
        RetrievalQuery("q", "text", "paragraph")
    with pytest.raises(ValueError, match="axis"):
        FilterRequest("topic", ("x",))
    with pytest.raises(ValueError, match="method"):
        dataclasses.replace(hit, method="vector-magic")
    with pytest.raises(ValueError, match="reason"):
        dataclasses.replace(exclusion, reason="anything-goes")
    with pytest.raises(ValueError, match="fixed"):
        RetrievalSpec(candidate_limit=99)


@pytest.mark.xfail(
    strict=True,
    reason="retrieval_plan_facts does not emit 'method_policy' yet; the "
    "per-level method policy is an unimplemented gap in the parked step-5 "
    "work (docs/decisions.md, 2026-07-27 MVP scope)",
)
def test_spec_and_plan_facts_pin_every_retrieval_constant_and_model() -> None:
    spec = RetrievalSpec(
        filters=(
            FilterRequest("access", ("public",)),
            FilterRequest("identity", ("a2", "a1")),
        )
    )
    queries = (
        RetrievalQuery("q-segment", "section query", "segment"),
        RetrievalQuery("q-artifact", "document query", "artifact"),
    )

    facts = retrieval_plan_facts(spec, queries)

    assert facts["candidate_limit"] == RETRIEVAL_CANDIDATE_LIMIT == 200
    assert facts["rrf_k"] == RETRIEVAL_RRF_K == 60
    assert facts["fusion_input_depth"] == RETRIEVAL_FUSION_INPUT_DEPTH == 200
    assert facts["rerank_depth"] == RETRIEVAL_RERANK_DEPTH == 50
    assert facts["ir_measures_version"] == IR_MEASURES_VERSION == "0.4.3"
    assert PLANNED_RETRIEVAL_METHODS == ("dense", "sparse", "hybrid-rrf", "reranked")
    assert facts["methods"] == list(PLANNED_RETRIEVAL_METHODS)
    assert facts["method_policy"] == {
        "artifact": ["dense"],
        "segment": list(PLANNED_RETRIEVAL_METHODS),
    }
    assert {query["query_id"]: query["methods"] for query in facts["queries"]} == {
        "q-artifact": ["dense"],
        "q-segment": list(PLANNED_RETRIEVAL_METHODS),
    }
    assert facts["models"] == {
        "dense": ("sentence-transformers:BAAI/bge-base-en-v1.5@a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"),
        "sparse": (
            "sentence-transformers-sparse:tomaarsen/splade-modernbert-base-miriad"
            "@c640ce28f7c4f4593ddba1b3855988f03a3d9cdc"
        ),
        "reranker": ("sentence-transformers:BAAI/bge-reranker-v2-m3@953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e"),
    }
    assert [item["axis"] for item in facts["filters"]] == ["identity", "access"]
    assert [item["query_id"] for item in facts["queries"]] == ["q-artifact", "q-segment"]
    assert retrieval_plan_facts(spec, tuple(reversed(queries))) == facts

    artifact_only = retrieval_plan_facts(
        spec,
        (RetrievalQuery("artifact-only", "document query", "artifact"),),
    )
    assert artifact_only["methods"] == ["dense"]
    assert artifact_only["queries"][0]["methods"] == ["dense"]


def test_typed_zero_row_tables_and_one_exclusion_write_are_deterministic(tmp_path: Path) -> None:
    written = write_retrieval_tables(tmp_path, hits=(), exclusions=())
    hits = pq.read_table(written[RETRIEVAL_HIT_TABLE])
    exclusions = pq.read_table(written["retrieval/exclusions.parquet"])
    assert hits.num_rows == 0
    assert exclusions.num_rows == 0
    assert hits.schema.names == [name for name, _ in HIT_COLUMNS]
    assert exclusions.schema.names == [name for name, _ in RETRIEVAL_EXCLUSION_COLUMNS]

    one = RetrievalExclusion(
        work_id="w1",
        query_id="q1",
        level="artifact",
        target_id="a1",
        source_table="dockets",
        subject_id="D-1",
        artifact_digest="digest-1",
        filter="identity",
        reason="mismatch-identity",
        detail="fixture",
    )
    second = tmp_path / "second"
    write_retrieval_tables(second, hits=(), exclusions=(one,))
    assert pq.read_table(second / "retrieval/exclusions.parquet").to_pylist() == [dataclasses.asdict(one)]


def test_retrieval_parquet_replacement_is_atomic_on_a_torn_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written = write_retrieval_tables(tmp_path, hits=(), exclusions=())
    original = written[RETRIEVAL_HIT_TABLE].read_bytes()
    parquet_write = pq.write_table

    def torn_write(_table: object, path: Path, *_args: object, **_kwargs: object) -> None:
        Path(path).write_bytes(b"torn parquet")
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(pq, "write_table", torn_write)
    with pytest.raises(RuntimeError, match="simulated crash"):
        write_retrieval_tables(tmp_path, hits=(), exclusions=())

    assert written[RETRIEVAL_HIT_TABLE].read_bytes() == original
    assert sorted(path.name for path in tmp_path.rglob("*") if path.is_file()) == [
        "exclusions.parquet",
        "hits.parquet",
    ]
    monkeypatch.setattr(pq, "write_table", parquet_write)
    write_retrieval_tables(tmp_path, hits=(), exclusions=())
    assert written[RETRIEVAL_HIT_TABLE].read_bytes() == original


def test_duckdb_candidate_universes_equal_plain_python_and_use_full_tie_breakers(
    tmp_path: Path,
) -> None:
    artifacts = [
        _artifact("a2", "D-2", "digest-2"),
        _artifact("a1", "D-1", "digest-1"),
    ]
    segments = [
        _segment("s2", "a2", "D-2", "digest-2"),
        _segment("s1", "a1", "D-1", "digest-1"),
    ]
    write_table(tmp_path / ARTIFACT_TABLE, ARTIFACT_COLUMNS, artifacts)
    write_table(tmp_path / SEGMENT_TABLE, SEGMENT_COLUMNS, segments)

    artifact_rows = construct_candidate_universe(tmp_path, level="artifact")
    segment_rows = construct_candidate_universe(tmp_path, level="segment")

    expected_artifacts = [
        _candidate("a1", "a1", "D-1", "digest-1"),
        _candidate("a2", "a2", "D-2", "digest-2"),
    ]
    expected_segments = [
        {
            **_candidate("s1", "a1", "D-1", "digest-1"),
            "segment_id": "s1",
            "text": "text for s1",
            "text_sha256": "text-s1",
            "slices_json": "[]",
        },
        {
            **_candidate("s2", "a2", "D-2", "digest-2"),
            "segment_id": "s2",
            "text": "text for s2",
            "text_sha256": "text-s2",
            "slices_json": "[]",
        },
    ]
    assert artifact_rows == expected_artifacts
    assert segment_rows == expected_segments

    retrieval_module = __import__("spicy_regs.docpipeline.retrieval", fromlist=["x"])
    assert retrieval_module.__file__ is not None
    source = Path(retrieval_module.__file__).read_text()
    assert "ORDER BY source_table, subject_id, artifact_digest, target_id, artifact_id, segment_id" in source
    assert ".duckdb" not in source


def test_empty_candidate_universe_is_success_but_missing_input_is_a_plan_failure(
    tmp_path: Path,
) -> None:
    write_table(tmp_path / ARTIFACT_TABLE, ARTIFACT_COLUMNS, ())
    assert construct_candidate_universe(tmp_path, level="artifact") == []

    with pytest.raises(PlanError, match="source/artifacts.parquet"):
        construct_candidate_universe(tmp_path / "missing", level="artifact")


def test_filter_conjunction_matches_a_naive_reference_and_first_axis_attribution() -> None:
    candidates = [
        _candidate("a1", "a1", "D-1", "digest-1"),
        _candidate("a2", "a2", "D-2", "digest-2"),
        _candidate("a3", "a3", "D-3", "digest-3"),
    ]
    metadata = [
        {
            "source_table": "dockets",
            "subject_id": "D-1",
            "jurisdiction": "US",
            "source_date": "2026-01-01",
        },
        {
            "source_table": "dockets",
            "subject_id": "D-2",
            "jurisdiction": "CA",
            "source_date": "2026-01-02",
        },
        {
            "source_table": "dockets",
            "subject_id": "D-3",
            "jurisdiction": "CA",
            "source_date": "2025-12-01",
        },
    ]
    spec = RetrievalSpec(
        filters=(
            FilterRequest("jurisdiction", ("US",)),
            FilterRequest("identity", ("a1", "a2")),
            FilterRequest("time", start="2026-01-01", end="2026-12-31"),
            FilterRequest("access", ("public",)),
        )
    )

    included, exclusions, counts = apply_prefilters(
        candidates,
        spec,
        query=RetrievalQuery("q1", "query", "artifact"),
        work_id="w1",
        metadata_rows=metadata,
    )

    naive = [
        row
        for row in candidates
        if row["artifact_id"] in {"a1", "a2"}
        and next(
            item["jurisdiction"]
            for item in metadata
            if (item["source_table"], item["subject_id"]) == (row["source_table"], row["subject_id"])
        )
        == "US"
        and "2026-01-01"
        <= next(
            item["source_date"]
            for item in metadata
            if (item["source_table"], item["subject_id"]) == (row["source_table"], row["subject_id"])
        )
        <= "2026-12-31"
    ]
    assert included == naive == [candidates[0]]
    assert [(item.target_id, item.filter, item.reason) for item in exclusions] == [
        ("a2", "jurisdiction", "mismatch-jurisdiction"),
        ("a3", "identity", "mismatch-identity"),
    ]
    assert counts["identity"]["excluded"] == 1
    assert counts["jurisdiction"]["excluded"] == 2
    assert list(counts) == ["identity", "jurisdiction", "time", "access"]
    assert FILTER_AXES[0] == "identity"


def test_unknown_values_fail_closed_unless_explicitly_included_and_stay_counted() -> None:
    candidates = [_candidate("a1", "a1", "D-1", "digest-1")]
    metadata = [{"source_table": "dockets", "subject_id": "D-1", "jurisdiction": None}]
    query = RetrievalQuery("q1", "query", "artifact")

    excluded = apply_prefilters(
        candidates,
        RetrievalSpec(filters=(FilterRequest("jurisdiction", ("US",)),)),
        query=query,
        work_id="w1",
        metadata_rows=metadata,
    )
    included = apply_prefilters(
        candidates,
        RetrievalSpec(filters=(FilterRequest("jurisdiction", ("US",), on_unknown="include"),)),
        query=query,
        work_id="w1",
        metadata_rows=metadata,
    )

    assert excluded[0] == []
    assert excluded[1][0].reason == "unknown-jurisdiction"
    assert excluded[2]["jurisdiction"]["unknown"] == 1
    assert included[0] == candidates
    assert included[1] == []
    assert included[2]["jurisdiction"]["unknown"] == 1
    assert included[2]["jurisdiction"]["included_unknown"] == 1


def test_unsupported_access_jurisdiction_and_profile_requests_fail_in_preflight() -> None:
    candidate = _candidate("a1", "a1", "D-1", "digest-1")
    query = RetrievalQuery("q1", "query", "artifact")

    with pytest.raises(PlanError, match="access.*private"):
        apply_prefilters(
            [candidate],
            RetrievalSpec(filters=(FilterRequest("access", ("private",)),)),
            query=query,
            work_id="w1",
        )
    with pytest.raises(PlanError, match="jurisdiction.*regulations-docket-v2"):
        apply_prefilters(
            [candidate],
            RetrievalSpec(filters=(FilterRequest("jurisdiction", ("US",)),)),
            query=query,
            work_id="w1",
        )
    bad_profile = {**candidate, "profile_id": "not-a-source-profile"}
    with pytest.raises(PlanError, match="profile"):
        apply_prefilters(
            [bad_profile],
            RetrievalSpec(filters=(FilterRequest("identity", ("a1",)),)),
            query=query,
            work_id="w1",
        )


def test_profile_join_keys_cover_docket_normalization_and_agenda_composites() -> None:
    assert candidate_metadata_join_key("dockets", {"docket_id": " epa-hq-2026-0001 "}) == (
        "dockets",
        "EPA-HQ-2026-0001",
    )
    assert candidate_metadata_join_key(
        "unified_agenda",
        {"rin": "1234-AA01", "agenda_edition": "2026-04"},
    ) == (
        "unified_agenda",
        '{"agenda_edition":"2026-04","rin":"1234-AA01"}',
    )
    sidecar = candidate_metadata_row(
        "unified_agenda",
        {
            "rin": "1234-AA01",
            "agenda_edition": "2026-04",
            "agency_code": "EPA",
        },
        version_field="agenda_edition",
        agency_field="agency_code",
    )
    assert sidecar["subject_id"] == '{"agenda_edition":"2026-04","rin":"1234-AA01"}'
    assert sidecar["version"] == "2026-04"
    assert sidecar["agency_id"] == "EPA"


def test_authority_graph_and_agency_concepts_are_joined_before_filtering() -> None:
    candidate = _candidate(
        "agenda-a",
        "agenda-a",
        '{"agenda_edition":"2026-04","rin":"1234-AA01"}',
        "digest-a",
        profile_id="unified-agenda-observation-v1",
        source_table="unified_agenda",
    )
    metadata = [
        {
            "source_table": "unified_agenda",
            "subject_id": candidate["subject_id"],
            "agency_id": "EPA",
        }
    ]
    spec = RetrievalSpec(
        filters=(
            FilterRequest("authority", ("usc:5:551",)),
            FilterRequest("graph", ("docket:EPA-HQ-2026-0001",)),
            FilterRequest("agency-scoped-concepts", ("concept:air",)),
        )
    )
    included, exclusions, counts = apply_prefilters(
        [candidate],
        spec,
        query=RetrievalQuery("q1", "query", "artifact"),
        work_id="w1",
        metadata_rows=metadata,
        authority_edges=(
            {
                "rin": "1234-AA01",
                "agenda_edition": "2026-04",
                "usc_title": "5",
                "usc_section": "551",
            },
        ),
        graph_edges=(
            {
                "source_table": "unified_agenda",
                "subject_id": candidate["subject_id"],
                "graph_id": "docket:EPA-HQ-2026-0001",
            },
        ),
        concept_assignments=({"agency_id": "EPA", "concept_id": "concept:air"},),
        profile_capabilities={
            "unified-agenda-observation-v1": (
                "authority",
                "graph",
                "agency-scoped-concepts",
            )
        },
    )
    assert included == [candidate]
    assert exclusions == []
    assert all(counts[axis]["matched"] == 1 for axis in counts)


def test_time_values_are_iso_validated_without_coercion() -> None:
    candidate = _candidate("a1", "a1", "D-1", "digest-1")
    metadata = [
        {
            "source_table": "dockets",
            "subject_id": "D-1",
            "source_date": "January 2, 2026",
        }
    ]
    result = apply_prefilters(
        [candidate],
        RetrievalSpec(
            filters=(
                FilterRequest(
                    "time",
                    start="2026-01-01",
                    end="2026-12-31",
                    on_unknown="include",
                ),
            )
        ),
        query=RetrievalQuery("q1", "query", "artifact"),
        work_id="w1",
        metadata_rows=metadata,
    )
    assert result[0] == [candidate]
    assert result[2]["time"]["unknown"] == 1

    with pytest.raises(ValueError, match="ISO"):
        FilterRequest("time", start="last year")


def test_import_and_forbidden_data_flow_guards() -> None:
    docpipeline_module = __import__("spicy_regs.docpipeline", fromlist=["x"])
    assert docpipeline_module.__file__ is not None
    package = Path(docpipeline_module.__file__).parent
    direct_duckdb: list[str] = []
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                modules.add(node.module)
        if "duckdb" in modules:
            direct_duckdb.append(path.relative_to(package).as_posix())
        assert not any(module == "spicy_regs.corpora" or module.startswith("spicy_regs.corpora.") for module in modules)
        if path.name == "retrieval.py":
            forbidden = ("sentence_transformers", "torch", "openai", "tiktoken")
            assert not any(module == name or module.startswith(f"{name}.") for module in modules for name in forbidden)
        if path.name in {"approval.py", "comparison.py"}:
            assert "spicy_regs.docpipeline.retrieval" not in modules
            assert "retrieval/hits" not in path.read_text(encoding="utf-8")
    assert direct_duckdb == ["retrieval.py"]


def test_hits_have_no_relevance_or_exact_source_span_fields() -> None:
    names = {field.name for field in dataclasses.fields(RetrievalHit)}
    columns = {name for name, _ in HIT_COLUMNS}
    assert "relevance" not in names | columns
    assert "relevant" not in names | columns
    assert not {"start_char", "end_char", "source_text", "text"} & (names | columns)


def test_extraction_payload_guard_rejects_retrieval_scores_and_ranks() -> None:
    for key in ("score", "rank", "dense_score", "sparse_rank", "rerank_score"):
        with pytest.raises(ModelInputLeakError, match="retrieval"):
            refuse_retrieval_aids({"candidate": {"target_id": "x", key: 1}})
    refuse_retrieval_aids({"candidate": {"target_id": "x"}})


def test_provider_outputs_are_the_only_immutable_retrieval_tables() -> None:
    assert "retrieval/dense-embeddings.parquet" in IMMUTABLE_RUN_PATTERNS
    assert "retrieval/sparse-embeddings.parquet" in IMMUTABLE_RUN_PATTERNS
    assert "retrieval/rerank-scores.parquet" in IMMUTABLE_RUN_PATTERNS
    assert "retrieval/hits.parquet" not in IMMUTABLE_RUN_PATTERNS
    assert "retrieval/exclusions.parquet" not in IMMUTABLE_RUN_PATTERNS

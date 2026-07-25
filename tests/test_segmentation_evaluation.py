"""Hermetic contract tests for the production segmentation snapshot."""

from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from spicy_regs.corpora.mixed_real_data import (
    EXPECTATION_COLUMNS,
    PairExpectation,
    record_id,
)
from spicy_regs.corpora.document_acceptance_scope import (
    build_document_acceptance_scope,
    load_document_acceptance_scope,
)
from spicy_regs.corpora.embedding_audit import (
    HashEmbeddingInputAuditor,
    HuggingFaceEmbeddingInputAuditor,
    audit_embedding_inputs,
)
from spicy_regs.corpora.segmentation_embedding_audit import (
    build_segmentation_embedding_audit,
    validate_segmentation_embedding_audit,
)
from spicy_regs.corpora.segmentation_evaluation import (
    FULL_DOCUMENT_SPECS,
    FetchResult,
    _boundary_crossing_text,
    build_segmentation_evaluation,
    fetch_source_cache,
    validate_segmentation_evaluation,
    validate_source_cache,
)
from spicy_regs.corpora.segmentation_experiment import (
    BoundaryChoice,
    BoundaryWindow,
    EvidenceSlice,
    ExperimentSegment,
    HashEmbeddingProvider,
    HeuristicBoundarySelector,
    OMLXEmbeddingProvider,
    OpenAIBoundarySelector,
    OpenAIEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
    Unit,
    _cosine,
    _ir_metrics,
    _mean_vector,
    _openai_provider_failures,
    _segment_vector,
    _unit_interval_indexes,
    build_segmentation_experiment,
    validate_segmentation_experiment,
)
from spicy_regs.ontology.checkpoint import BatchCheckpoint
from spicy_regs.ontology.common import read_parquet_rows, write_parquet_rows
from spicy_regs.ontology.segmentation import segment_text
from spicy_regs.ontology.subjects import SUBJECT_PROFILES, Artifact
from tests.pdf_fixtures import make_pdf


def _source_bytes(spec) -> tuple[bytes, str]:
    if spec.representation == "pdf":
        return make_pdf([f"{spec.gold_phrase} evaluation source"]), "application/pdf"
    markup = f"<section><h2>{spec.gold_phrase}</h2><p>Locked public evaluation text.</p></section>"
    if spec.representation == "xml":
        return (
            ('<?xml version="1.0"?>' + markup).encode(),
            "application/xml",
        )
    return markup.encode(), "text/html"


def _fake_fetch(spec) -> FetchResult:
    content, media_type = _source_bytes(spec)
    return FetchResult(
        content=content,
        resolved_url=spec.source_url,
        media_type=media_type,
        etag='"fixture"',
        last_modified="Fri, 24 Jul 2026 00:00:00 GMT",
    )


def _special_ids(source_table: str) -> list[str]:
    return [spec.key_value for spec in FULL_DOCUMENT_SPECS if spec.source_table == source_table and not spec.append_row]


def _profile_rows(profile) -> tuple[list[str], list[dict]]:
    extras_by_table = {
        "dockets": ("rin", "agency_code"),
        "documents": (
            "docket_id",
            "agency_code",
            "fr_doc_num",
            "additional_rins",
            "text_content",
        ),
        "federal_register": (
            "regulation_id_numbers_json",
            "topics_json",
            "agency_slugs",
        ),
        "comments": ("docket_id", "agency_code"),
        "congress_bills": (
            "congress",
            "bill_type",
            "bill_number",
            "url",
        ),
        "fcc_filings": ("filing_url",),
    }
    columns = list(
        dict.fromkeys(
            (
                *profile.id_columns,
                *profile.text_columns,
                *extras_by_table.get(profile.source_table, ()),
            )
        )
    )
    identifiers = _special_ids(profile.source_table)
    rows: list[dict] = []
    for index in range(10):
        row: dict[str, object] = {column: f"{profile.profile_id} {column} {index}" for column in columns}
        if len(profile.id_columns) == 1:
            row[profile.id_columns[0]] = (
                identifiers[index] if index < len(identifiers) else f"{profile.source_table.upper()}-{index}"
            )
        else:
            row[profile.id_columns[0]] = "TEST-AA01" if index == 0 else f"TEST-{index:02d}"
            row[profile.id_columns[1]] = f"2025{index:02d}"
        if profile.source_table == "dockets":
            row["docket_id"] = f"DOCKET-{index}"
            row["rin"] = "TEST-AA01" if index == 0 else None
            row["agency_code"] = "EPA"
        elif profile.source_table == "documents":
            row["docket_id"] = f"DOCKET-{index % 2}"
            row["agency_code"] = "EPA"
            row["fr_doc_num"] = _special_ids("federal_register")[0] if index == 0 else None
            row["additional_rins"] = "[]"
        elif profile.source_table == "federal_register":
            row["regulation_id_numbers_json"] = '["TEST-AA01"]' if index == 0 else "[]"
            row["topics_json"] = '["Water"]'
            row["agency_slugs"] = '["environmental-protection-agency"]'
        elif profile.source_table == "comments":
            row["docket_id"] = f"DOCKET-{index % 2}"
            row["agency_code"] = "EPA"
        rows.append(row)
    return columns, rows


def _write_base(root: Path) -> None:
    for profile in SUBJECT_PROFILES:
        columns, rows = _profile_rows(profile)
        write_parquet_rows(
            root / f"{profile.source_table}.parquet",
            columns=columns,
            rows=rows,
        )
    (root / "profile-evaluation-manifest.json").write_text(
        json.dumps({"evaluation_id": "fixture-profile-evaluation"}) + "\n",
        encoding="utf-8",
    )


def _write_corpus(root: Path) -> None:
    (root / "corpus-receipt.json").write_text(
        json.dumps({"status": "pass", "dataset_id": "fixture-corpus"}) + "\n",
        encoding="utf-8",
    )
    bill_profile = next(profile for profile in SUBJECT_PROFILES if profile.source_table == "congress_bills")
    bill_columns, bill_rows = _profile_rows(bill_profile)
    appended = [spec for spec in FULL_DOCUMENT_SPECS if spec.source_table == "congress_bills" and spec.append_row]
    for index, spec in enumerate(appended):
        bill_rows.append(
            {
                **{column: f"bill metadata {column} {index}" for column in bill_columns},
                "bill_id": spec.key_value,
                "congress": "118",
                "bill_type": "hr",
                "bill_number": str(1000 + index),
            }
        )
    write_parquet_rows(
        root / "congress_bills.parquet",
        columns=bill_columns,
        rows=bill_rows,
    )

    for source_table, key_column, text_column in (
        ("comments", "comment_id", "comment"),
        ("fcc_filings", "id_submission", "text_data"),
    ):
        profile = next(profile for profile in SUBJECT_PROFILES if profile.source_table == source_table)
        columns, rows = _profile_rows(profile)
        for index in range(4):
            row = {column: f"{source_table} {column} extra {index}" for column in columns}
            row[key_column] = f"{source_table.upper()}-EXTRA-{index}"
            row[text_column] = "long real source text " * (100 + index)
            rows.append(row)
        write_parquet_rows(
            root / f"{source_table}.parquet",
            columns=columns,
            rows=rows,
        )

    documents = _special_ids("documents")
    federal_register = _special_ids("federal_register")
    positive = [
        PairExpectation(
            left_record_id=record_id("documents", documents[index]),
            left_source="documents",
            right_record_id=record_id("dockets", f"DOCKET-{index}"),
            right_source="dockets",
            label="related",
            relation_kind="document_in_docket",
            evidence_basis="equal docket_id",
            evidence_value=f"DOCKET-{index}",
            evidence_strength="direct_identifier",
        )
        for index in range(2)
    ]
    unknown = PairExpectation(
        left_record_id=record_id("documents", documents[0]),
        left_source="documents",
        right_record_id=record_id(
            "federal_register",
            federal_register[-1],
        ),
        right_source="federal_register",
        label="unknown",
        relation_kind="similar_title_without_crosswalk",
        evidence_basis="no source-issued crosswalk",
        evidence_value=None,
        evidence_strength="ambiguous_lexical_signal",
    )
    write_parquet_rows(
        root / "relationship_expectations.parquet",
        columns=EXPECTATION_COLUMNS,
        rows=[*(row.as_row() for row in positive), unknown.as_row()],
    )


def test_source_cache_is_locked_and_detects_changed_bytes(tmp_path: Path):
    first = tmp_path / "cache-one"
    second = tmp_path / "cache-two"

    first_lock = fetch_source_cache(first, fetcher=_fake_fetch)
    second_lock = fetch_source_cache(second, fetcher=_fake_fetch)

    assert first_lock == second_lock
    assert (first / "source-lock.json").read_bytes() == (second / "source-lock.json").read_bytes()
    assert validate_source_cache(first)["status"] == "pass"

    target = first / first_lock["sources"][0]["cache_file"]
    target.write_bytes(target.read_bytes() + b"changed")
    receipt = validate_source_cache(first)
    assert receipt["status"] == "fail"
    assert "cache digest does not match" in " ".join(receipt["failures"])


def test_curated_boundary_case_really_crosses_a_canonical_segment():
    text, (start, end) = _boundary_crossing_text()
    segments = segment_text("documents.text_content", text)

    assert "".join(segment.text for segment in segments) == text
    assert any(segment.start_char < start < segment.end_char < end for segment in segments)


def test_segmentation_evaluation_build_is_byte_deterministic(tmp_path: Path):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    first = tmp_path / "first"
    second = tmp_path / "second"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)

    first_receipt = build_segmentation_evaluation(
        base,
        corpus,
        cache,
        first,
    )
    second_receipt = build_segmentation_evaluation(
        base,
        corpus,
        cache,
        second,
    )

    assert first_receipt["status"] == "pass"
    assert first_receipt == second_receipt
    assert validate_segmentation_evaluation(first) == first_receipt
    assert {path.relative_to(first): path.read_bytes() for path in first.rglob("*") if path.is_file()} == {
        path.relative_to(second): path.read_bytes() for path in second.rglob("*") if path.is_file()
    }
    assert first_receipt["profile_count"] == 17
    assert first_receipt["relationship_label_counts"] == {
        "no_declared_relation": 2,
        "related": 2,
        "unknown": 1,
    }


def test_court_opinions_are_separate_profiled_artifacts():
    court = next(spec for spec in FULL_DOCUMENT_SPECS if spec.case_id == "court-opinion-303-creative")

    assert court.source_table == "court_opinions"
    assert court.profile_id == "court-opinion-v1"
    assert court.target_field == "pdf_text"
    assert sum(spec.profile_id == "court-opinion-v1" for spec in FULL_DOCUMENT_SPECS) == 10


def test_incumbent_bge_provider_is_pinned_and_normalized():
    class FakeTokenizer:
        def __call__(self, text, **options):
            assert options == {
                "add_special_tokens": True,
                "truncation": False,
                "return_attention_mask": False,
                "return_token_type_ids": False,
            }
            return {"input_ids": [0, *range(len(text.split())), 1]}

    class FakeEncoder:
        max_seq_length = 512
        tokenizer = FakeTokenizer()

        def get_embedding_dimension(self):
            return 3

        def encode(self, texts, **options):
            assert texts == ["alpha", "beta"]
            assert options == {
                "batch_size": 2,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "normalize_embeddings": True,
            }
            return ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0))

    provider = SentenceTransformerEmbeddingProvider(
        model="fixture/bge",
        revision="0123456789abcdef",
        dimensions=3,
        batch_size=2,
        encoder=FakeEncoder(),
    )

    result = provider.embed(["alpha", "beta"])

    assert provider.model_id == ("sentence-transformers:fixture/bge@0123456789abcdef")
    assert provider.max_input_tokens == 512
    assert provider.model_token_count("alpha beta") == 4
    assert result.vectors == (
        (1.0, 0.0, 0.0),
        (0.0, 1.0, 0.0),
    )
    assert result.calls[0]["provider"] == "sentence-transformers"
    assert result.calls[0]["input_count"] == 2


def test_model_native_embedding_auditor_records_exact_overflow():
    class FakeTokenizer:
        def __call__(self, text, **options):
            assert options == {
                "add_special_tokens": True,
                "truncation": False,
                "return_attention_mask": False,
                "return_token_type_ids": False,
            }
            return {"input_ids": [0, *range(len(text.split())), 1]}

    auditor = HuggingFaceEmbeddingInputAuditor(
        tokenizer=FakeTokenizer(),
        tokenizer_id="fixture/bge@revision:tokenizer",
        max_input_tokens=4,
        overflow_policy="truncate",
    )

    short, long = audit_embedding_inputs(
        auditor,
        ["alpha beta", "alpha beta gamma"],
    )

    assert short.token_count == 4
    assert short.input_over_limit is False
    assert short.input_truncated is False
    assert long.token_count == 5
    assert long.input_over_limit is True
    assert long.input_truncated is True
    assert len(long.token_sequence_sha256) == 64


def test_cosine_uses_exact_finite_vector_contract():
    assert _cosine((1.0, 0.0), (1.0, 0.0)) == pytest.approx(1.0)
    assert _cosine((1.0, 0.0), (0.0, 1.0)) == pytest.approx(0.0)
    assert _cosine((0.0, 0.0), (1.0, 0.0)) == 0.0
    with pytest.raises(ValueError, match="equal finite vectors"):
        _cosine((1.0,), (1.0, 2.0))
    with pytest.raises(ValueError, match="equal finite vectors"):
        _cosine((float("nan"),), (1.0,))


def test_mean_vector_uses_validated_weighted_numpy_aggregation():
    assert _mean_vector(((1.0, 2.0), (3.0, 6.0)), (1.0, 3.0)) == (
        2.5,
        5.0,
    )
    assert _mean_vector(((1.0, 2.0), (3.0, 6.0)), (0.0, 0.0)) == (
        2.0,
        4.0,
    )
    with pytest.raises(ValueError, match="equal finite vectors"):
        _mean_vector(((1.0,), (1.0, 2.0)))
    with pytest.raises(ValueError, match="weights must match"):
        _mean_vector(((1.0,), (2.0,)), (1.0,))


def test_segment_vector_interval_index_matches_exact_overlap_weights():
    units = [
        Unit(
            unit_id="unit-1",
            source_field="body",
            start_char=0,
            end_char=5,
            text="abcde",
            semantic_text="abcde",
            token_count=1,
            source_sha256="body-sha",
            boundary="paragraph",
        ),
        Unit(
            unit_id="unit-2",
            source_field="body",
            start_char=5,
            end_char=10,
            text="fghij",
            semantic_text="fghij",
            token_count=1,
            source_sha256="body-sha",
            boundary="paragraph",
        ),
        Unit(
            unit_id="unit-3",
            source_field="title",
            start_char=0,
            end_char=2,
            text="xy",
            semantic_text="xy",
            token_count=1,
            source_sha256="title-sha",
            boundary="title",
        ),
    ]
    indexes = _unit_interval_indexes({"artifact": units})
    segment = ExperimentSegment(
        config_id="fixture-800",
        arm="structure-first",
        max_tokens=800,
        min_tokens=400,
        profile_id="fixture",
        source_table="fixture",
        subject_type="fixture",
        subject_id="fixture",
        artifact_digest="artifact",
        segment_id="segment",
        ordinal=0,
        token_count=2,
        tokenizer="fixture",
        tokenizer_version="1",
        policy_version="fixture-v1",
        boundary_method="fixture",
        overlap_chars=0,
        slices=(
            EvidenceSlice(
                source_field="body",
                start_char=2,
                end_char=8,
                text="cdefgh",
                source_sha256="body-sha",
            ),
            EvidenceSlice(
                source_field="title",
                start_char=0,
                end_char=2,
                text="xy",
                source_sha256="title-sha",
            ),
        ),
    )

    assert _segment_vector(
        segment,
        indexes["artifact"],
        {
            "unit-1": (1.0, 0.0),
            "unit-2": (0.0, 1.0),
            "unit-3": (1.0, 1.0),
        },
    ) == (0.625, 0.625)


def test_openai_embedding_retries_are_physical_and_terminally_validated():
    class APITimeoutError(Exception):
        pass

    class FakeEmbeddings:
        def __init__(self):
            self.calls = 0

        def create(self, **request):
            self.calls += 1
            assert request["model"] == "fixture-embedding"
            assert request["dimensions"] == 2
            if self.calls == 1:
                raise APITimeoutError("transient")
            return SimpleNamespace(
                model="fixture-embedding",
                data=[SimpleNamespace(index=index, embedding=[1.0, 0.0]) for index, _ in enumerate(request["input"])],
                usage=SimpleNamespace(prompt_tokens=3),
                id=f"embedding-{self.calls}",
                _request_id=f"request-{self.calls}",
            )

    embeddings = FakeEmbeddings()
    provider = OpenAIEmbeddingProvider(
        api_key="fixture-key",
        model="fixture-embedding",
        dimensions=2,
        batch_size=2,
        max_retries=1,
        retry_base_seconds=0,
        client=SimpleNamespace(embeddings=embeddings),
    )

    result = provider.embed(["alpha", "beta", "gamma"])

    assert len(result.vectors) == 3
    assert [row["status"] for row in result.calls] == ["retrying", "completed", "completed"]
    assert [row["attempt_count"] for row in result.calls] == [1, 2, 1]
    assert _openai_provider_failures(result.calls) == []
    assert _openai_provider_failures(result.calls[:1]) == ["OpenAI embedding retry sequence has no completion"]


def test_openai_aggregate_retry_telemetry_requires_terminal_completion():
    completed_after_retry = {
        "provider": "openai",
        "operation": "boundary-selection",
        "call_ordinal": 1,
        "status": "completed",
        "attempt_count": 2,
        "retry_count": 1,
    }
    terminal_failure = {
        "provider": "openai",
        "operation": "embedding",
        "call_ordinal": 1,
        "status": "failed",
        "attempt_count": 1,
        "retry_count": 0,
    }

    assert _openai_provider_failures([completed_after_retry]) == []
    assert _openai_provider_failures([terminal_failure]) == ["OpenAI embedding has a terminal failed call"]


def test_openai_boundary_batches_resume_without_repeating_success(
    tmp_path: Path,
):
    class InterruptOnceModel:
        model_id = "openai:fixture-boundary"

        def __init__(self):
            self.calls = 0
            self.interrupted = False
            self.last_call_metadata: dict | None = None
            self.output_caps: list[int] = []

        def structured_json(
            self,
            *,
            name,
            schema,
            instructions,
            payload,
            max_output_tokens,
        ):
            del name, schema, instructions
            self.calls += 1
            self.output_caps.append(max_output_tokens)
            if self.calls == 2 and not self.interrupted:
                self.interrupted = True
                self.last_call_metadata = {
                    "status": "retry_exhausted",
                    "attempt_count": 1,
                    "retry_count": 0,
                    "input_tokens": 10,
                    "output_tokens": max_output_tokens,
                    "total_tokens": 10 + max_output_tokens,
                    "duration_ms": 1,
                }
                raise TimeoutError("fixture interruption")
            self.last_call_metadata = {
                "status": "completed",
                "attempt_count": 1,
                "retry_count": 0,
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
                "duration_ms": 1,
            }
            return {
                "choices": [
                    {
                        "window_id": window["window_id"],
                        "choice_id": window["choices"][0]["choice_id"],
                    }
                    for window in payload["windows"]
                ]
            }

    artifact = Artifact(
        subject_type="document",
        subject_id="fixture-document",
        profile_id="regulations-document-v2",
        source_table="documents",
        allowed_schemes=("regulatory-topic",),
        digest="a" * 64,
        raw_fields={"text": "fixture text"},
        elements=(),
        exclusions=(),
        context_fields={},
        segmentation_mode="hierarchical-document",
        adapter_id="fixture-adapter",
    )
    windows = tuple(
        BoundaryWindow(
            window_id=f"window-{index}",
            choices=(
                BoundaryChoice(
                    choice_id="choice-1",
                    unit_index=index + 1,
                    before="before",
                    after="after",
                    boundary_hint="paragraph",
                ),
            ),
        )
        for index in range(2)
    )
    model = InterruptOnceModel()
    selector = OpenAIBoundarySelector(model, batch_size=1)
    checkpoint = BatchCheckpoint(
        tmp_path,
        run_id="fixture-boundary-run",
        phase="selection",
    )
    selector.bind_checkpoint(checkpoint)

    with pytest.raises(RuntimeError, match="exact checkpoint is resumable"):
        selector.select(artifact=artifact, windows=windows)

    result = selector.select(artifact=artifact, windows=windows)

    assert result.choices == {
        "window-0": "choice-1",
        "window-1": "choice-1",
    }
    assert result.calls == ()
    assert model.calls == 3
    assert model.output_caps == [4_096, 4_096, 4_096]
    transitions = selector.checkpoint_calls()
    assert [row["status"] for row in transitions] == [
        "completed",
        "retry_exhausted",
        "completed",
    ]
    assert _openai_provider_failures(transitions) == []


def test_omlx_embedding_adapter_uses_published_endpoint_contract():
    requests: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "http://127.0.0.1:8012/v1/embeddings"
        assert request.headers["Authorization"] == "Bearer local-token"
        payload = json.loads(request.content)
        requests.append(payload)
        assert payload["model"] == "fixture-embedding"
        assert payload["dimensions"] == 3
        assert payload["max_length"] == 256
        assert payload["truncation"] is False
        return httpx.Response(
            200,
            json={
                "object": "list",
                "model": "fixture-embedding",
                "data": [
                    {
                        "object": "embedding",
                        "index": index,
                        "embedding": [1.0, 0.0, 0.0],
                    }
                    for index, _ in enumerate(payload["input"])
                ],
                "usage": {
                    "prompt_tokens": len(payload["input"]) * 3,
                    "total_tokens": len(payload["input"]) * 3,
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        provider = OMLXEmbeddingProvider(
            model="fixture/repository",
            revision="0123456789abcdef",
            service_model="fixture-embedding",
            dimensions=3,
            batch_size=2,
            max_length=256,
            api_key="local-token",
            client=client,
        )
        result = provider.embed(["alpha", "beta", "gamma"])

    assert len(requests) == 2
    assert result.vectors == (
        (1.0, 0.0, 0.0),
        (1.0, 0.0, 0.0),
        (1.0, 0.0, 0.0),
    )
    assert [row["input_count"] for row in result.calls] == [2, 1]
    assert all(row["provider"] == "omlx" for row in result.calls)


def test_ir_measures_matches_independent_fixture_calculation():
    metrics = _ir_metrics(
        {"query": {"relevant-a": 1, "relevant-b": 1}},
        {
            "query": {
                "irrelevant": 3.0,
                "relevant-a": 2.0,
                "relevant-b": 1.0,
            }
        },
    )
    expected_ndcg = (1 / math.log2(3) + 1 / math.log2(4)) / (1 + 1 / math.log2(3))

    assert metrics == pytest.approx(
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


def test_five_arm_experiment_is_complete_and_deterministic(tmp_path: Path):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    first = tmp_path / "experiment-one"
    second = tmp_path / "experiment-two"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(
        base,
        corpus,
        cache,
        evaluation,
    )

    provider = HashEmbeddingProvider()
    selector = HeuristicBoundarySelector()
    first_receipt = build_segmentation_experiment(
        evaluation,
        first,
        embedding_provider=provider,
        boundary_selector=selector,
    )
    second_receipt = build_segmentation_experiment(
        evaluation,
        second,
        embedding_provider=provider,
        boundary_selector=selector,
    )

    assert first_receipt["status"] == "pass"
    assert first_receipt["config_count"] == 15
    assert first_receipt["production_provider"] is False
    assert first_receipt["retrieval_candidate_group_count"] == (
        first_receipt["config_count"] * len(read_parquet_rows(evaluation / "gold_spans.parquet")) * 2
    )
    assert first_receipt["retrieval_candidate_count"] > 0
    assert first_receipt == second_receipt
    assert (
        validate_segmentation_experiment(
            evaluation,
            first,
        )
        == first_receipt
    )
    assert {path.relative_to(first): path.read_bytes() for path in first.rglob("*") if path.is_file()} == {
        path.relative_to(second): path.read_bytes() for path in second.rglob("*") if path.is_file()
    }


def test_experiment_scope_excludes_comments_and_context_from_every_output(
    tmp_path: Path,
):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    scope_dir = tmp_path / "document-scope"
    output = tmp_path / "experiment"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    scope_receipt = build_document_acceptance_scope(
        evaluation,
        scope_dir,
    )
    scope = load_document_acceptance_scope(evaluation, scope_dir)

    receipt = build_segmentation_experiment(
        evaluation,
        output,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(800,),
        scope_dir=scope_dir,
    )

    segments = read_parquet_rows(output / "experiment_segments.parquet")
    candidates = read_parquet_rows(output / "retrieval_candidates.parquet")
    included = scope.included_artifact_digests
    assert scope_receipt["excluded_artifact_count"] > 0
    assert scope_receipt["included_adversarial_count"] == 7
    assert scope_receipt["excluded_adversarial_count"] == 0
    assert receipt["status"] == "pass"
    assert receipt["document_scope_id"] == scope.scope_id
    assert receipt["artifact_count"] == len(included)
    assert {str(row["artifact_digest"]) for row in segments} == included
    assert all(
        str(row["query_artifact_digest"]) in included
        and str(row["segment_artifact_digest"]) in included
        and str(row["query_subject_type"]) != "comment"
        for row in candidates
    )
    assert (
        validate_segmentation_experiment(
            evaluation,
            output,
            scope_dir=scope_dir,
        )
        == receipt
    )


def test_experiment_resume_reuses_completed_embedding_stage(tmp_path: Path):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    output = tmp_path / "experiment"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)

    class CountingEmbeddingProvider(HashEmbeddingProvider):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def embed(self, texts):
            self.calls += 1
            return super().embed(texts)

    class InterruptOnceSelector:
        model_id = "fixture:interrupt-once-boundary"
        production_provider = True

        def __init__(self):
            self.calls = 0
            self.interrupted = False
            self.delegate = HeuristicBoundarySelector()

        def select(self, *, artifact, windows):
            self.calls += 1
            if not self.interrupted:
                self.interrupted = True
                raise TimeoutError("fixture boundary interruption")
            return self.delegate.select(artifact=artifact, windows=windows)

    provider = CountingEmbeddingProvider()
    selector = InterruptOnceSelector()

    with pytest.raises(TimeoutError, match="fixture boundary interruption"):
        build_segmentation_experiment(
            evaluation,
            output,
            embedding_provider=provider,
            boundary_selector=selector,
            budgets=(800,),
        )

    work_dir = output.parent / f".{output.name}.experiment-work"
    assert work_dir.exists()
    assert (work_dir / "embedding-work-state.json").is_file()

    receipt = build_segmentation_experiment(
        evaluation,
        output,
        embedding_provider=provider,
        boundary_selector=selector,
        budgets=(800,),
    )

    assert receipt["status"] == "pass"
    assert provider.calls == 1
    assert not work_dir.exists()


def test_embedding_input_audit_attaches_without_regenerating_vectors(
    tmp_path: Path,
):
    base = tmp_path / "base"
    corpus = tmp_path / "corpus"
    cache = tmp_path / "cache"
    evaluation = tmp_path / "evaluation"
    experiment = tmp_path / "experiment"
    audit_output = tmp_path / "embedding-audit"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    build_segmentation_experiment(
        evaluation,
        experiment,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(800,),
    )
    embedding_bytes = (experiment / "embedding_cache.parquet").read_bytes()
    auditor = HashEmbeddingInputAuditor()

    receipt = build_segmentation_embedding_audit(
        evaluation,
        experiment,
        audit_output,
        input_auditor=auditor,
    )

    assert receipt["status"] == "pass"
    assert receipt["input_count"] == len(read_parquet_rows(experiment / "embedding_cache.parquet"))
    assert receipt["over_limit_input_count"] == 0
    assert receipt["truncated_input_count"] == 0
    assert (experiment / "embedding_cache.parquet").read_bytes() == embedding_bytes
    assert (
        validate_segmentation_embedding_audit(
            evaluation,
            experiment,
            audit_output,
            input_auditor=HashEmbeddingInputAuditor(),
        )
        == receipt
    )

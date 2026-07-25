"""Hermetic downstream tagging tests for the five segmentation arms."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.corpora.segmentation_evaluation import (
    build_segmentation_evaluation,
    fetch_source_cache,
)
from spicy_regs.corpora.document_acceptance_scope import (
    DocumentAcceptanceScope,
    PROFILE_ACCEPTANCE_POLICIES,
    build_document_acceptance_scope,
)
from spicy_regs.corpora.segmentation_experiment import (
    ARMS,
    SEGMENT_COLUMNS,
    HashEmbeddingProvider,
    HeuristicBoundarySelector,
    build_segmentation_experiment,
)
from spicy_regs.corpora.segmentation_tagging import (
    EVIDENCE_ALIGNMENT_POLICY,
    _select_tagging_segments,
    _subjects_from_selected,
    build_tagging_experiment,
    select_tagging_segments,
    tagging_preflight,
    validate_tagging_experiment,
)
from spicy_regs.ontology.common import (
    RunContext,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import CONCEPT_COLUMNS, seed_concept
from spicy_regs.ontology.llm import (
    TAG_MAX_OUTPUT_TOKENS,
    VALIDATION_MAX_OUTPUT_TOKENS,
    TagProposal,
    ValidationProposal,
)
from tests.test_segmentation_evaluation import (
    _fake_fetch,
    _write_base,
    _write_corpus,
)

ASSERTED_AT = "2026-07-24T12:00:00Z"


@pytest.fixture(scope="module")
def tagging_base(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, Path]:
    root = tmp_path_factory.mktemp("tagging-base")
    base = root / "base"
    corpus = root / "corpus"
    cache = root / "cache"
    evaluation = root / "evaluation"
    experiment = root / "experiment"
    registry = root / "concepts.parquet"
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
    )
    concept = seed_concept(
        {"name": "public policy"},
        RunContext("tagging-registry", ASSERTED_AT),
    )
    assert concept is not None
    write_parquet_rows(
        registry,
        columns=CONCEPT_COLUMNS,
        rows=[concept],
    )
    return evaluation, experiment, registry


class _DeterministicTagger:
    model_id = "fixture:deterministic-tagger-v1"
    production_provider = False

    def __init__(self, *, fail_at: int | None = None) -> None:
        self.fail_at = fail_at
        self.failed = False
        self.tag_calls = 0
        self.validation_calls = 0
        self.segment_calls: list[str] = []

    def tag(self, subject, concepts):
        self.tag_calls += 1
        self.segment_calls.append(subject.segment_id)
        if self.fail_at is not None and self.tag_calls == self.fail_at and not self.failed:
            self.failed = True
            raise TimeoutError("controlled tagging interruption")
        concept = next(
            (row for row in concepts if str(row["scheme"]) in subject.allowed_schemes),
            None,
        )
        if concept is None:
            return []
        field, text = next((key, value) for key, value in subject.fields.items() if value)
        match = re.search(r"\S+(?:\s+\S+){0,2}", text)
        assert match is not None
        return [
            TagProposal(
                concept_id=str(concept["concept_id"]),
                proposed_label=None,
                scheme=str(concept["scheme"]),
                definition=None,
                confidence=0.9,
                evidence_text=match.group(0),
                evidence_field=field,
                evidence_start=match.start(),
                evidence_end=match.end(),
                justification="Exact fixture span supports the concept.",
            )
        ]

    def validate(self, *, subject, concept, assignment):
        del subject, concept, assignment
        self.validation_calls += 1
        return ValidationProposal(
            agrees=True,
            confidence=0.95,
            rationale="The exact source span supports the fixture concept.",
        )


def _artifact_bytes(root: Path) -> dict[Path, bytes]:
    return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def test_tagging_sample_covers_five_arms_and_uses_full_adjacency(
    tagging_base: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    evaluation, experiment, registry = tagging_base

    preflight = tagging_preflight(
        evaluation,
        experiment,
        registry_path=registry,
    )
    selected = select_tagging_segments(evaluation, experiment)
    adjacency_experiment = tmp_path / "adjacency-experiment"
    adjacency_experiment.mkdir()
    full_rows = read_parquet_rows(experiment / "experiment_segments.parquet")
    target = dict(selected[0])
    target["segment_count"] = 2
    omitted = {
        **target,
        "segment_id": "fixture-omitted-adjacent-segment",
        "ordinal": 1,
    }
    rewritten = [
        (target if str(row["segment_id"]) == str(target["segment_id"]) else row)
        for row in full_rows
        if not (
            str(row["config_id"]) == str(target["config_id"])
            and str(row["artifact_digest"]) == str(target["artifact_digest"])
        )
        or str(row["segment_id"]) == str(target["segment_id"])
    ]
    write_parquet_rows(
        adjacency_experiment / "experiment_segments.parquet",
        columns=SEGMENT_COLUMNS,
        rows=[*rewritten, omitted],
    )
    subjects = _subjects_from_selected(
        evaluation,
        adjacency_experiment,
        [target],
    )

    assert preflight["config_count"] == len(ARMS)
    assert preflight["artifact_count"] >= preflight["gold_span_count"]
    assert preflight["selected_adversarial_case_count"] == (preflight["adversarial_case_count"])
    assert preflight["generation_call_count"] == (preflight["selected_segment_count"])
    assert preflight["generation_prompt_input_token_estimate"] > 0
    assert preflight["generation_prompt_budget_failure_count"] == 0
    assert preflight["gold_overlap_segment_count"] >= (preflight["gold_span_count"] * len(ARMS))
    subject = subjects[(str(target["config_id"]), str(target["segment_id"]))]
    assert subject.next_segment_id == omitted["segment_id"]


def test_document_scope_removes_comment_queries_candidates_and_tagging(
    tagging_base: tuple[Path, Path, Path],
) -> None:
    evaluation, experiment, _ = tagging_base
    document_profiles = {policy.profile_id for policy in PROFILE_ACCEPTANCE_POLICIES if policy.included}
    segment_rows = read_parquet_rows(experiment / "experiment_segments.parquet")
    included_artifacts = {
        str(row["artifact_digest"]) for row in segment_rows if str(row["profile_id"]) in document_profiles
    }
    gold_rows = read_parquet_rows(evaluation / "gold_spans.parquet")
    scope = DocumentAcceptanceScope(
        scope_id="document-scope-fixture",
        scope_policy_version="document-acceptance-v1",
        dataset_evaluation_id="segmentation-eval-fixture",
        included_artifact_digests=frozenset(included_artifacts),
        included_gold_ids=frozenset(
            str(row["gold_id"]) for row in gold_rows if str(row["artifact_digest"]) in included_artifacts
        ),
        included_adversarial_case_ids=frozenset(),
    )

    selected = _select_tagging_segments(
        evaluation,
        experiment,
        scope=scope,
    )

    assert selected
    assert {str(row["artifact_digest"]) for row in selected} <= included_artifacts
    assert all(str(row["subject_type"]) != "comment" for row in selected)
    assert all(row["adversarial_case_ids_json"] == "[]" for row in selected)


def test_scoped_experiment_preflights_builds_and_validates_tagging(
    tagging_base: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    evaluation, _, registry = tagging_base
    scope_dir = tmp_path / "document-scope"
    experiment = tmp_path / "scoped-experiment"
    output = tmp_path / "scoped-tagging"
    build_document_acceptance_scope(evaluation, scope_dir)
    build_segmentation_experiment(
        evaluation,
        experiment,
        embedding_provider=HashEmbeddingProvider(),
        boundary_selector=HeuristicBoundarySelector(),
        budgets=(1800,),
        scope_dir=scope_dir,
    )
    config_ids = ("structure-overlap-1800",)

    preflight = tagging_preflight(
        evaluation,
        experiment,
        registry_path=registry,
        budget=1800,
        scope_dir=scope_dir,
        config_ids=config_ids,
    )
    receipt = build_tagging_experiment(
        evaluation,
        experiment,
        registry,
        output,
        model=_DeterministicTagger(),
        budget=1800,
        scope_dir=scope_dir,
        config_ids=config_ids,
        run_id="scoped-tagging-fixture",
        asserted_at=ASSERTED_AT,
    )

    assert preflight["document_scope_id"] == receipt["document_scope_id"]
    assert preflight["config_ids"] == receipt["config_ids"] == list(config_ids)
    assert receipt["config_count"] == 1
    expected_profiles = {
        str(row["profile_id"])
        for row in read_parquet_rows(experiment / "experiment_segments.parquet")
        if str(row["config_id"]) in config_ids
    }
    assert set(preflight["selected_profile_ids"]) == expected_profiles
    assert preflight["selected_profile_count"] == len(expected_profiles)
    assert receipt["selected_profile_ids"] == sorted(expected_profiles)
    assert receipt["status"] == "pass"
    manifest = json.loads(
        (output / "segmentation-tagging-manifest.json").read_text(),
    )
    assert manifest["model_configuration"] == {
        "model_id": _DeterministicTagger.model_id,
    }
    assert manifest["tag_max_output_tokens"] == TAG_MAX_OUTPUT_TOKENS
    assert (
        manifest["validation_max_output_tokens"]
        == VALIDATION_MAX_OUTPUT_TOKENS
    )
    assert (
        manifest["evidence_alignment_policy"]
        == EVIDENCE_ALIGNMENT_POLICY
    )
    assert (
        receipt["evidence_alignment_policy"]
        == EVIDENCE_ALIGNMENT_POLICY
    )
    assert (
        validate_tagging_experiment(
            evaluation,
            experiment,
            output,
            scope_dir=scope_dir,
        )
        == receipt
    )


def test_tagging_artifact_is_strict_and_byte_deterministic(
    tagging_base: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    evaluation, experiment, registry = tagging_base
    first = tmp_path / "tagging-one"
    second = tmp_path / "tagging-two"

    first_receipt = build_tagging_experiment(
        evaluation,
        experiment,
        registry,
        first,
        model=_DeterministicTagger(),
        run_id="tagging-determinism",
        asserted_at=ASSERTED_AT,
    )
    second_receipt = build_tagging_experiment(
        evaluation,
        experiment,
        registry,
        second,
        model=_DeterministicTagger(),
        run_id="tagging-determinism",
        asserted_at=ASSERTED_AT,
    )

    assert first_receipt["status"] == "pass"
    assert first_receipt == second_receipt
    assert first_receipt["provider_transition_count"] == (
        first_receipt["selected_segment_count"] + first_receipt["validation_count"]
    )
    assert first_receipt["provider_call_failure_count"] == 0
    assert (
        validate_tagging_experiment(
            evaluation,
            experiment,
            first,
        )
        == first_receipt
    )
    assert _artifact_bytes(first) == _artifact_bytes(second)
    raw_rows = pq.read_table(first / "tagging_raw_assignments.parquet").to_pylist()
    segment_rows = pq.read_table(first / "tagging_segments.parquet").to_pylist()
    assert len(raw_rows) == sum(int(row["proposal_count"]) for row in segment_rows)
    assert (first / "tagging_input_registry.parquet").read_bytes() == registry.read_bytes()


def test_tagging_failure_resumes_without_repeating_successes(
    tagging_base: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    evaluation, experiment, registry = tagging_base
    output = tmp_path / "tagging-resume"
    expected = tagging_preflight(
        evaluation,
        experiment,
    )["selected_segment_count"]
    model = _DeterministicTagger(fail_at=3)

    with pytest.raises(RuntimeError, match="checkpoint is resumable"):
        build_tagging_experiment(
            evaluation,
            experiment,
            registry,
            output,
            model=model,
            run_id="tagging-resume",
            asserted_at=ASSERTED_AT,
        )
    first_two = tuple(model.segment_calls[:2])
    work_dir = output.parent / f".{output.name}.tagging-work"
    assert work_dir.exists()
    assert not output.exists()

    receipt = build_tagging_experiment(
        evaluation,
        experiment,
        registry,
        output,
        model=model,
        run_id="tagging-resume",
        asserted_at=ASSERTED_AT,
    )

    assert receipt["status"] == "pass"
    assert receipt["provider_call_failure_count"] == 1
    assert model.tag_calls == expected + 1
    assert all(model.segment_calls.count(value) == 1 for value in first_two)
    assert not work_dir.exists()


def test_raw_assignment_tampering_fails_validation(
    tagging_base: tuple[Path, Path, Path],
    tmp_path: Path,
) -> None:
    evaluation, experiment, registry = tagging_base
    clean = tmp_path / "tagging-clean"
    tampered = tmp_path / "tagging-tampered"
    build_tagging_experiment(
        evaluation,
        experiment,
        registry,
        clean,
        model=_DeterministicTagger(),
        run_id="tagging-tamper",
        asserted_at=ASSERTED_AT,
    )
    shutil.copytree(clean, tampered)
    path = tampered / "tagging_raw_assignments.parquet"
    table = pq.read_table(path)
    pq.write_table(table.slice(1), path)

    receipt = validate_tagging_experiment(
        evaluation,
        experiment,
        tampered,
    )

    assert receipt["status"] == "fail"
    assert "raw assignment count differs" in " ".join(receipt["failures"])

    alignment_tampered = tmp_path / "tagging-alignment-tampered"
    shutil.copytree(clean, alignment_tampered)
    alignment_path = (
        alignment_tampered / "tagging_raw_assignments.parquet"
    )
    alignment_table = pq.read_table(alignment_path)
    alignment_rows = alignment_table.to_pylist()
    evidence = json.loads(str(alignment_rows[0]["evidence_json"]))
    evidence["spans"][0]["alignment_method"] = "fuzzy"
    alignment_rows[0]["evidence_json"] = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
    )
    pq.write_table(
        pa.Table.from_pylist(
            alignment_rows,
            schema=alignment_table.schema,
        ),
        alignment_path,
    )

    alignment_receipt = validate_tagging_experiment(
        evaluation,
        experiment,
        alignment_tampered,
    )

    assert alignment_receipt["status"] == "fail"
    assert "raw assignments are ungrounded" in " ".join(
        alignment_receipt["failures"]
    )

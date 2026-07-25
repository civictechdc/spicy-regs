"""Contracts for the immutable document-only acceptance view."""

from __future__ import annotations

from pathlib import Path

from spicy_regs.corpora import document_acceptance_scope as scope_module
from spicy_regs.corpora.document_acceptance_scope import (
    ARTIFACT_MEMBERSHIP_COLUMNS,
    PROFILE_ACCEPTANCE_POLICIES,
    build_document_acceptance_scope,
    load_document_acceptance_scope,
    validate_document_acceptance_scope,
)
from spicy_regs.ontology.common import (
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.subjects import (
    SUBJECT_PROFILES,
    Artifact,
    SubjectProfile,
)


def _artifact(
    profile_id: str,
    source_table: str,
    subject_type: str,
    subject_id: str,
) -> Artifact:
    return Artifact(
        profile_id=profile_id,
        source_table=source_table,
        subject_type=subject_type,
        subject_id=subject_id,
        allowed_schemes=("subject",),
        digest=f"digest-{profile_id}-{subject_id}",
        raw_fields={f"{source_table}.text": f"text for {subject_id}"},
        elements=(),
        exclusions=(),
        context_fields={},
        segmentation_mode="atomic-record",
        adapter_id="fixture-adapter-v1",
    )


def _scope_fixture(
    tmp_path: Path,
    monkeypatch,
) -> tuple[Path, list[Artifact]]:
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    artifacts = [
        _artifact(
            "regulations-document-v2",
            "documents",
            "document",
            "DOC-1",
        ),
        _artifact(
            "regulations-comment-v1",
            "comments",
            "comment",
            "COMMENT-1",
        ),
        _artifact(
            "regulations-docket-v2",
            "dockets",
            "docket",
            "DOCKET-1",
        ),
    ]
    write_parquet_rows(
        dataset / "gold_spans.parquet",
        columns=(
            "gold_id",
            "profile_id",
            "subject_type",
            "subject_id",
            "artifact_digest",
        ),
        rows=[
            {
                "gold_id": "gold-document",
                "profile_id": artifacts[0].profile_id,
                "subject_type": artifacts[0].subject_type,
                "subject_id": artifacts[0].subject_id,
                "artifact_digest": artifacts[0].digest,
            },
            {
                "gold_id": "gold-comment",
                "profile_id": artifacts[1].profile_id,
                "subject_type": artifacts[1].subject_type,
                "subject_id": artifacts[1].subject_id,
                "artifact_digest": artifacts[1].digest,
            },
        ],
    )
    write_parquet_rows(
        dataset / "adversarial_cases.parquet",
        columns=(
            "case_id",
            "kind",
            "profile_id",
            "subject_type",
            "subject_id",
        ),
        rows=[
            {
                "case_id": "adversarial-document",
                "kind": "prompt-injection",
                "profile_id": artifacts[0].profile_id,
                "subject_type": artifacts[0].subject_type,
                "subject_id": artifacts[0].subject_id,
            },
            {
                "case_id": "adversarial-comment",
                "kind": "prompt-injection",
                "profile_id": artifacts[1].profile_id,
                "subject_type": artifacts[1].subject_type,
                "subject_id": artifacts[1].subject_id,
            },
        ],
    )
    monkeypatch.setattr(
        scope_module,
        "validate_segmentation_evaluation",
        lambda _: {
            "status": "pass",
            "evaluation_id": "segmentation-eval-fixture",
        },
    )
    monkeypatch.setattr(
        scope_module,
        "build_artifacts",
        lambda _: list(artifacts),
    )
    return dataset, artifacts


def _file_bytes(root: Path) -> dict[Path, bytes]:
    return {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_scope_policy_classifies_every_live_profile_once() -> None:
    assert {policy.profile_id for policy in PROFILE_ACCEPTANCE_POLICIES} == {
        profile.profile_id for profile in SUBJECT_PROFILES
    }
    assert len(PROFILE_ACCEPTANCE_POLICIES) == len(SUBJECT_PROFILES)


def test_document_scope_is_immutable_and_excludes_comments_and_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset, artifacts = _scope_fixture(tmp_path, monkeypatch)
    first = tmp_path / "scope-one"
    second = tmp_path / "scope-two"

    first_receipt = build_document_acceptance_scope(dataset, first)
    second_receipt = build_document_acceptance_scope(dataset, second)
    loaded = load_document_acceptance_scope(dataset, first)

    assert first_receipt == second_receipt
    assert first_receipt["status"] == "pass"
    assert first_receipt["artifact_count"] == 3
    assert first_receipt["included_artifact_count"] == 1
    assert first_receipt["included_gold_count"] == 1
    assert first_receipt["excluded_gold_count"] == 1
    assert first_receipt["included_adversarial_count"] == 1
    assert first_receipt["excluded_adversarial_count"] == 1
    assert loaded.included_artifact_digests == {artifacts[0].digest}
    assert loaded.included_gold_ids == {"gold-document"}
    assert loaded.included_adversarial_case_ids == {
        "adversarial-document"
    }
    assert _file_bytes(first) == _file_bytes(second)
    assert validate_document_acceptance_scope(dataset, first) == first_receipt


def test_document_scope_detects_membership_tampering(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset, _ = _scope_fixture(tmp_path, monkeypatch)
    output = tmp_path / "scope"
    build_document_acceptance_scope(dataset, output)
    membership = output / "document-artifact-membership.parquet"
    rows = read_parquet_rows(membership)
    comment = next(
        row
        for row in rows
        if row["acceptance_role"] == "public-comment"
    )
    comment["included"] = "true"
    write_parquet_rows(
        membership,
        columns=ARTIFACT_MEMBERSHIP_COLUMNS,
        rows=rows,
    )

    receipt = validate_document_acceptance_scope(dataset, output)

    assert receipt["status"] == "fail"
    assert "artifact scope membership differs" in receipt["failures"]
    assert "public-comment artifact entered document scope" in receipt[
        "failures"
    ]
    assert "scope artifact hashes differ" in receipt["failures"]


def test_document_scope_preserves_zero_included_case_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset, _ = _scope_fixture(tmp_path, monkeypatch)
    cases = read_parquet_rows(dataset / "adversarial_cases.parquet")
    write_parquet_rows(
        dataset / "adversarial_cases.parquet",
        columns=(
            "case_id",
            "kind",
            "profile_id",
            "subject_type",
            "subject_id",
        ),
        rows=[
            row
            for row in cases
            if row["profile_id"] == "regulations-comment-v1"
        ],
    )

    receipt = build_document_acceptance_scope(
        dataset,
        tmp_path / "zero-included-cases",
    )

    assert receipt["status"] == "pass"
    assert receipt["included_adversarial_count"] == 0
    assert receipt["excluded_adversarial_count"] == 1


def test_scope_policy_fails_closed_for_a_new_unclassified_profile(
    monkeypatch,
) -> None:
    new_profile = SubjectProfile(
        "new-document-v1",
        "new_documents",
        "new_document",
        ("id",),
        ("text",),
        ("subject",),
    )
    monkeypatch.setattr(
        scope_module,
        "SUBJECT_PROFILES",
        (*SUBJECT_PROFILES, new_profile),
    )

    assert scope_module._profile_policy_failures() == [
        "unclassified live profiles: new-document-v1"
    ]

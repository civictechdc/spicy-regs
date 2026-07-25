"""Coverage and safety tests for ontology subject profiles."""

from __future__ import annotations

from pathlib import Path

import pytest

from spicy_regs.corpora.mixed_real_data import EXPECTED_SOURCE_TABLES
from spicy_regs.corpora.profile_evaluation import (
    build_profile_evaluation,
    validate_profile_evaluation,
)
from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.ontology.subjects import (
    EXCLUDED_SOURCE_TABLES,
    PROFILE_SEGMENTATION_POLICIES,
    SUBJECT_PROFILES,
    build_artifacts,
    build_subjects,
)


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


def _write_profile_fixture(root: Path) -> None:
    for profile in SUBJECT_PROFILES:
        columns = list(
            dict.fromkeys(
                (
                    *profile.id_columns,
                    *profile.text_columns,
                    *(
                        ("fr_doc_num",)
                        if profile.source_table == "documents"
                        else ()
                    ),
                )
            )
        )
        row = {column: f"{profile.source_table}-{column}" for column in columns}
        if profile.source_table == "documents":
            row["fr_doc_num"] = "2026-00001"
        if profile.source_table == "federal_register":
            row["document_number"] = "2026-00001"
        if profile.source_table == "comments":
            row["comment"] = "PFAS " * 5_000
        write_parquet_rows(
            root / f"{profile.source_table}.parquet",
            columns=columns,
            rows=[row],
        )


def test_subject_registry_explicitly_covers_every_mixed_corpus_source() -> None:
    profiled = {profile.source_table for profile in SUBJECT_PROFILES}
    excluded = set(EXCLUDED_SOURCE_TABLES)

    assert profiled.isdisjoint(excluded)
    assert profiled | excluded == set(EXPECTED_SOURCE_TABLES)
    assert all(EXCLUDED_SOURCE_TABLES.values())
    assert len(profiled) == len(SUBJECT_PROFILES)


def test_build_subjects_loads_every_profile_without_losing_untrusted_text(
    tmp_path: Path,
) -> None:
    _write_profile_fixture(tmp_path)

    artifacts = build_artifacts(
        tmp_path,
        required_source_tables={
            profile.source_table for profile in SUBJECT_PROFILES
        },
    )
    subjects = build_subjects(
        tmp_path,
        required_source_tables={
            profile.source_table for profile in SUBJECT_PROFILES
        },
        max_tokens=700,
        min_tokens=280,
        token_counter=_CharacterCounter(),
    )

    assert {subject.profile_id for subject in subjects} == {
        profile.profile_id for profile in SUBJECT_PROFILES
    }
    assert {artifact.profile_id for artifact in artifacts} == {
        profile.profile_id for profile in SUBJECT_PROFILES
    }
    assert all(subject.source_table for subject in subjects)
    assert all(subject.allowed_schemes for subject in subjects)
    assert all(subject.token_count <= 700 for subject in subjects)
    assert all(not subject.truncated_fields for subject in subjects)
    comment_artifact = next(
        artifact
        for artifact in artifacts
        if artifact.subject_type == "comment"
    )
    comment_segments = [
        subject
        for subject in subjects
        if subject.subject_type == "comment"
    ]
    comment_parts = [
        (
            (subject.source_spans or {})[field_ref][0],
            value,
        )
        for subject in comment_segments
        for field_ref, value in subject.fields.items()
        if (subject.field_sources or {}).get(
            field_ref,
            field_ref,
        )
        == "comments.comment"
    ]
    reconstructed = "".join(
        value for _, value in sorted(comment_parts)
    )
    assert reconstructed == comment_artifact.fields["comments.comment"]
    assert len(reconstructed) == len("PFAS " * 5_000)
    assert len(comment_segments) > 1


def test_subject_builder_records_blank_and_null_exclusions(tmp_path: Path) -> None:
    write_parquet_rows(
        tmp_path / "comments.parquet",
        columns=(
            "comment_id",
            "title",
            "comment",
            "text_content",
            "organization",
            "category",
        ),
        rows=[
            {
                "comment_id": "COMMENT-1",
                "title": "  Exact  title  ",
                "comment": "Paragraph one.\n\nParagraph two.",
                "text_content": None,
                "organization": "   ",
                "category": "",
            }
        ],
    )

    artifact = build_artifacts(tmp_path)[0]

    assert artifact.fields["comments.title"] == "  Exact  title  "
    assert artifact.fields["comments.comment"] == (
        "Paragraph one.\n\nParagraph two."
    )
    assert {
        (exclusion.source_field, exclusion.reason)
        for exclusion in artifact.exclusions
    } == {
        ("comments.text_content", "null"),
        ("comments.organization", "blank-non-content"),
        ("comments.category", "blank-non-content"),
    }


def test_every_profile_declares_one_general_segmentation_mode() -> None:
    assert set(PROFILE_SEGMENTATION_POLICIES) == {
        profile.profile_id for profile in SUBJECT_PROFILES
    }
    assert {
        policy.mode for policy in PROFILE_SEGMENTATION_POLICIES.values()
    } == {
        "atomic-record",
        "structured-children",
        "hierarchical-document",
    }


def test_hierarchical_adapter_preserves_heading_and_paragraph_coordinates(
    tmp_path: Path,
) -> None:
    body = (
        "Section 1 - Water Quality\n\n"
        "The first paragraph regulates discharge.\n\n"
        "Section 2 - Monitoring\n\n"
        "The second paragraph requires reports."
    )
    write_parquet_rows(
        tmp_path / "comments.parquet",
        columns=(
            "comment_id",
            "title",
            "comment",
            "text_content",
            "organization",
            "category",
        ),
        rows=[
            {
                "comment_id": "COMMENT-HIERARCHY",
                "title": "Water policy",
                "comment": body,
                "text_content": None,
                "organization": None,
                "category": None,
            }
        ],
    )

    artifact = build_artifacts(tmp_path)[0]
    body_elements = [
        element
        for element in artifact.elements
        if element.source_field == "comments.comment"
    ]

    assert [element.kind for element in body_elements] == [
        "heading",
        "paragraph",
        "heading",
        "paragraph",
    ]
    assert "".join(element.text for element in body_elements) == body
    assert body_elements[1].parent_element_id == body_elements[0].element_id
    assert body_elements[3].parent_element_id == body_elements[2].element_id
    assert body_elements[3].ancestor_path == ("Section 2 - Monitoring",)


def test_structured_child_adapter_preserves_json_syntax_and_parentage(
    tmp_path: Path,
) -> None:
    activities = (
        '[{"general_issue_code":"ENV","description":"PFAS"},'
        '{"general_issue_code":"ENG","description":"Grid"}]'
    )
    write_parquet_rows(
        tmp_path / "lobbying_filings.parquet",
        columns=(
            "filing_uuid",
            "client_name",
            "registrant_name",
            "lobbying_activities_json",
            "government_entities_json",
        ),
        rows=[
            {
                "filing_uuid": "FILING-1",
                "client_name": "Example Client",
                "registrant_name": "Example Registrant",
                "lobbying_activities_json": activities,
                "government_entities_json": '["EPA","DOE"]',
            }
        ],
    )

    artifact = build_artifacts(tmp_path)[0]
    activity_elements = [
        element
        for element in artifact.elements
        if (
            element.source_field
            == "lobbying_filings.lobbying_activities_json"
        )
    ]
    container = activity_elements[0]
    children = activity_elements[1:]

    assert container.kind == "structured-array"
    assert container.evidence_eligible is False
    assert len(children) == 2
    assert all(
        child.parent_element_id == container.element_id
        for child in children
    )
    assert "".join(child.text for child in children) == activities


@pytest.mark.parametrize(
    ("table", "id_column", "identifier", "body_column", "body"),
    [
        (
            "federal_register",
            "document_number",
            "2026-12345",
            "body_html",
            (
                "<article><h1>PFAS Rule</h1><section>"
                "<h2>Monitoring</h2><p>Facilities must sample water."
                "</p><ul><li>Report quarterly.</li></ul></section>"
                "</article>"
            ),
        ),
        (
            "congress_bills",
            "bill_id",
            "hr-123-119",
            "xml_text",
            (
                "<bill><legis-body><section><enum>SEC. 1.</enum>"
                "<header>Clean Water</header><text>PFAS monitoring "
                "is required.</text></section></legis-body></bill>"
            ),
        ),
    ],
)
def test_native_markup_adapter_preserves_exact_source_and_hierarchy(
    tmp_path: Path,
    table: str,
    id_column: str,
    identifier: str,
    body_column: str,
    body: str,
) -> None:
    write_parquet_rows(
        tmp_path / f"{table}.parquet",
        columns=(id_column, "title", body_column),
        rows=[
            {
                id_column: identifier,
                "title": "Source title",
                body_column: body,
            }
        ],
    )

    artifact = build_artifacts(tmp_path)[0]
    source_field = f"{table}.{body_column}"
    elements = [
        element
        for element in artifact.elements
        if element.source_field == source_field
    ]
    subjects = [
        subject
        for subject in build_subjects(tmp_path)
        if source_field in (subject.field_sources or {}).values()
    ]

    assert "".join(element.text for element in elements) == body
    assert {element.kind for element in elements} >= {
        "heading",
        "paragraph",
    }
    assert any(element.parent_element_id for element in elements)
    reconstructed = "".join(
        value
        for start, value in sorted(
            (
                (subject.source_spans or {})[field_ref][0],
                value,
            )
            for subject in subjects
            for field_ref, value in subject.fields.items()
            if (subject.field_sources or {}).get(field_ref)
            == source_field
        )
    )
    assert reconstructed == body


def test_build_subjects_rejects_missing_required_profile(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="gao_reports.parquet"):
        build_subjects(tmp_path, required_source_tables={"gao_reports"})


def test_profile_evaluation_snapshot_covers_every_profile(
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    _write_profile_fixture(corpus)
    (corpus / "corpus-receipt.json").write_text(
        '{"dataset_id":"fixture-corpus","status":"pass"}\n',
        encoding="utf-8",
    )
    regulatory = corpus / "openai-eval-inputs"
    regulatory.mkdir()
    for source in (
        "dockets",
        "documents",
        "federal_register",
        "unified_agenda",
    ):
        (corpus / f"{source}.parquet").replace(
            regulatory / f"{source}.parquet"
        )
    write_parquet_rows(
        regulatory / "fr_docket_links.parquet",
        columns=("document_number", "docket_id"),
        rows=[
            {
                "document_number": "2026-00001",
                "docket_id": "dockets-docket_id",
            }
        ],
    )
    target = tmp_path / "profile-eval"

    receipt = build_profile_evaluation(
        corpus,
        target,
        rows_per_profile=1,
    )

    assert receipt["status"] == "pass"
    assert receipt["profile_count"] == len(SUBJECT_PROFILES)
    assert receipt["generation_batch_rows"] == len(SUBJECT_PROFILES)
    assert set(receipt["generation_batch_counts_by_profile"].values()) == {1}
    assert validate_profile_evaluation(target) == receipt

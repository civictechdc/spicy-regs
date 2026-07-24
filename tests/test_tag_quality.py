"""Tag-quality drift harness tests against Federal Register topics."""

from __future__ import annotations

from spicy_regs.ontology.common import RunContext, write_parquet_rows
from spicy_regs.ontology.concepts import (
    ASSIGNMENT_COLUMNS,
    CONCEPT_COLUMNS,
    make_assignment,
    seed_concept,
)
from spicy_regs.ontology.evaluation import evaluate_tag_quality
from spicy_regs.ontology.llm import TagProposal
from spicy_regs.ontology.subjects import Subject


def test_evaluation_counts_unassigned_gold_documents_as_false_negatives(tmp_path):
    context = RunContext("quality-run", "2026-07-23T12:00:00Z")
    pfas = seed_concept({"name": "PFAS"}, context)
    water = seed_concept({"name": "Drinking Water"}, context)
    assert pfas is not None and water is not None
    write_parquet_rows(tmp_path / "concepts.parquet", columns=CONCEPT_COLUMNS, rows=[pfas, water])
    write_parquet_rows(
        tmp_path / "documents.parquet",
        columns=("document_id", "fr_doc_num"),
        rows=[
            {"document_id": "DOC-1", "fr_doc_num": "2026-00001"},
            {"document_id": "DOC-2", "fr_doc_num": "2026-00002"},
        ],
    )
    write_parquet_rows(
        tmp_path / "federal_register.parquet",
        columns=("document_number", "topics_json"),
        rows=[
            {"document_number": "2026-00001", "topics_json": '[{"name":"PFAS"}]'},
            {"document_number": "2026-00002", "topics_json": '["Drinking Water"]'},
        ],
    )
    subject = Subject(
        subject_type="document",
        subject_id="DOC-1",
        text="PFAS",
        fields={"federal_register.abstract": "PFAS"},
        digest="digest",
    )
    assignment = make_assignment(
        subject=subject,
        concept_id=str(pfas["concept_id"]),
        proposal=TagProposal(
            concept_id=str(pfas["concept_id"]),
            proposed_label=None,
            scheme="subject",
            definition=None,
            confidence=0.9,
            evidence_text="PFAS",
            evidence_field="federal_register.abstract",
            justification="Exact topic.",
        ),
        context=context,
        actor_id="test-model",
        ordinal=0,
    )
    write_parquet_rows(
        tmp_path / "concept_assignments.parquet",
        columns=ASSIGNMENT_COLUMNS,
        rows=[assignment],
    )

    result = evaluate_tag_quality(tmp_path)
    assert result.evaluated_documents == 2
    assert result.true_positive == 1
    assert result.false_positive == 0
    assert result.false_negative == 1
    assert result.precision == 1.0
    assert result.recall == 0.5
    assert result.f1 == 2 / 3

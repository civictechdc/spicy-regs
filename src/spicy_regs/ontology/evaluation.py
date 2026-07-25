"""Cheap tag-drift evaluation against Federal Register Thesaurus topics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from spicy_regs.ontology.common import JsonReadStats, iter_parquet_rows, parse_json_list, read_parquet_rows
from spicy_regs.ontology.concepts import latest_assignments, normalize_label, resolved_assignment_concept


@dataclass(frozen=True)
class TagQuality:
    evaluated_documents: int
    true_positive: int
    false_positive: int
    false_negative: int
    precision: float
    recall: float
    f1: float

    def as_dict(self) -> dict:
        return asdict(self)


def evaluate_tag_quality(
    output_dir: Path,
    *,
    document_ids: set[str] | None = None,
) -> TagQuality:
    """Compare current subject-concept labels with FR topic labels.

    The Thesaurus is an intentionally imperfect ground truth; this harness is a
    drift alarm, not a claim that every valid descriptive tag appears in FR.
    """
    required = {
        name: output_dir / f"{name}.parquet"
        for name in ("documents", "federal_register", "concepts", "concept_assignments")
    }
    missing = [path.name for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"tag-quality inputs missing from {output_dir}: {', '.join(missing)}")

    document_by_fr: dict[str, str] = {}
    for row in iter_parquet_rows(required["documents"], columns=("document_id", "fr_doc_num")):
        if row.get("document_id") and row.get("fr_doc_num"):
            document_by_fr[str(row["fr_doc_num"])] = str(row["document_id"])

    stats = JsonReadStats()
    gold_by_document: dict[str, set[str]] = {}
    for row in iter_parquet_rows(
        required["federal_register"],
        columns=("document_number", "topics_json"),
    ):
        document_id = document_by_fr.get(str(row.get("document_number") or ""))
        if not document_id:
            continue
        if document_ids is not None and document_id not in document_ids:
            continue
        topics = parse_json_list(
            row.get("topics_json"),
            stats=stats,
            table="federal_register",
            row_id=row.get("document_number"),
            column="topics_json",
        )
        if topics is None:
            continue
        labels: set[str] = set()
        for topic in topics:
            label = topic if isinstance(topic, str) else topic.get("name") if isinstance(topic, dict) else None
            if normalized := normalize_label(label):
                labels.add(normalized)
        if labels:
            gold_by_document[document_id] = labels

    concepts = read_parquet_rows(required["concepts"])
    concept_by_id = {str(row["concept_id"]): row for row in concepts}
    predicted_by_document: dict[str, set[str]] = {}
    for assignment in latest_assignments(read_parquet_rows(required["concept_assignments"])):
        if assignment.get("subject_type") != "document":
            continue
        document_id = str(assignment.get("subject_id") or "")
        if document_id not in gold_by_document:
            continue
        resolved = resolved_assignment_concept(assignment, concepts)
        concept = concept_by_id.get(resolved)
        if concept is None or concept.get("scheme") != "subject":
            continue
        predicted_by_document.setdefault(document_id, set()).add(normalize_label(concept.get("pref_label")))

    tp = fp = fn = evaluated = 0
    for document_id, gold in gold_by_document.items():
        predicted = predicted_by_document.get(document_id, set())
        predicted.discard("")
        tp += len(predicted & gold)
        fp += len(predicted - gold)
        fn += len(gold - predicted)
        evaluated += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
    return TagQuality(
        evaluated_documents=evaluated,
        true_positive=tp,
        false_positive=fp,
        false_negative=fn,
        precision=precision,
        recall=recall,
        f1=f1,
    )

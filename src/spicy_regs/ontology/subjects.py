"""Build the v1 docket/document subject corpus used by concept tagging."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import iter_parquet_rows, text_digest


@dataclass(frozen=True)
class Subject:
    subject_type: str
    subject_id: str
    text: str
    fields: dict[str, str]
    digest: str


def balanced_subject_batch(subjects: Iterable[Subject], limit: int) -> list[Subject]:
    """Select a deterministic bounded batch without starving a subject type."""
    if limit <= 0:
        return []
    queues: dict[str, deque[Subject]] = defaultdict(deque)
    for subject in subjects:
        queues[subject.subject_type].append(subject)

    selected: list[Subject] = []
    subject_types = sorted(queues)
    while len(selected) < limit:
        advanced = False
        for subject_type in subject_types:
            queue = queues[subject_type]
            if queue:
                selected.append(queue.popleft())
                advanced = True
                if len(selected) == limit:
                    break
        if not advanced:
            break
    return selected


def _clean(value: object) -> str:
    return " ".join(str(value or "").split())


def build_subjects(output_dir: Path) -> list[Subject]:
    """Return docket and FR-backed document subjects in stable order.

    ``document`` subjects remain foreign-keyed to ``documents.document_id``.
    Federal Register title/abstract text is joined through ``fr_doc_num`` rather
    than treating the external FR document number as a regulations.gov id.
    """
    dockets_file = output_dir / "dockets.parquet"
    documents_file = output_dir / "documents.parquet"
    fr_file = output_dir / "federal_register.parquet"
    missing = [path.name for path in (dockets_file, documents_file, fr_file) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"concept subject inputs missing from {output_dir}: {', '.join(missing)}")

    subjects: list[Subject] = []
    for row in iter_parquet_rows(dockets_file):
        subject_id = normalize_regsgov_identifier(row.get("docket_id"))
        if subject_id is None:
            continue
        fields = {
            "dockets.title": _clean(row.get("title")),
            "dockets.abstract": _clean(row.get("abstract")),
        }
        text = "\n".join(value for value in fields.values() if value)
        if not text:
            continue
        subjects.append(
            Subject(
                subject_type="docket",
                subject_id=subject_id,
                text=text,
                fields=fields,
                digest=text_digest(text),
            )
        )

    documents: list[dict] = []
    relevant_fr_numbers: set[str] = set()
    for row in iter_parquet_rows(documents_file):
        if not row.get("document_id"):
            continue
        documents.append(row)
        if row.get("fr_doc_num"):
            relevant_fr_numbers.add(str(row["fr_doc_num"]))

    fr_by_number: dict[str, dict] = {}
    if relevant_fr_numbers:
        for row in iter_parquet_rows(fr_file):
            number = row.get("document_number")
            if number and str(number) in relevant_fr_numbers:
                fr_by_number[str(number)] = row

    for row in documents:
        fr = fr_by_number.get(str(row.get("fr_doc_num") or ""), {})
        # FR abstracts are the v1 document text source; a document with no FR
        # counterpart is intentionally deferred rather than silently switching
        # to PDF/full-text tagging.
        abstract = _clean(fr.get("abstract"))
        if not abstract:
            continue
        fields = {
            "documents.title": _clean(row.get("title")),
            "federal_register.title": _clean(fr.get("title")),
            "federal_register.abstract": abstract,
        }
        text = "\n".join(value for value in fields.values() if value)
        subjects.append(
            Subject(
                subject_type="document",
                subject_id=str(row["document_id"]),
                text=text,
                fields=fields,
                digest=text_digest(text),
            )
        )

    subjects.sort(key=lambda subject: (subject.subject_type, subject.subject_id))
    return subjects

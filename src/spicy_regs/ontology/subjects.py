"""Build lossless artifact and segment views for ontology tagging."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from spicy_regs.ontology.adapters import (
    SegmentationMode,
    adapter_for,
)
from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import (
    canonical_json,
    iter_parquet_rows,
    text_digest,
)
from spicy_regs.ontology.segmentation import (
    DEFAULT_MAX_SEGMENT_TOKENS,
    DEFAULT_MIN_SEGMENT_TOKENS,
    SEGMENT_POLICY_VERSION,
    TokenCounter,
    segment_fields,
)

ALL_CONCEPT_SCHEMES = ("subject", "regulated_entity")


@dataclass(frozen=True)
class SourceExclusion:
    """One source value omitted from evidence with an explicit reason."""

    source_field: str
    reason: str
    start_char: int
    end_char: int
    raw_text_sha256: str


@dataclass(frozen=True)
class SourceElement:
    """One exact source-native or adapter-recovered part of an artifact."""

    element_id: str
    kind: str
    ordinal: int
    parent_element_id: str | None
    ancestor_path: tuple[str, ...]
    source_field: str
    start_char: int
    end_char: int
    text: str
    raw_text_sha256: str
    source_text_sha256: str
    evidence_eligible: bool = True


@dataclass(frozen=True)
class Artifact:
    """One source-scoped identity and version, before prompt segmentation."""

    subject_type: str
    subject_id: str
    profile_id: str
    source_table: str
    allowed_schemes: tuple[str, ...]
    digest: str
    raw_fields: dict[str, str]
    elements: tuple[SourceElement, ...]
    exclusions: tuple[SourceExclusion, ...]
    context_fields: dict[str, str]
    segmentation_mode: SegmentationMode
    adapter_id: str

    @property
    def fields(self) -> dict[str, str]:
        return dict(self.raw_fields)

    @property
    def text(self) -> str:
        return "\n".join(self.fields.values())


@dataclass(frozen=True)
class Subject:
    """One bounded processing segment subordinate to a source artifact."""

    subject_type: str
    subject_id: str
    text: str
    fields: dict[str, str]
    digest: str
    profile_id: str = "legacy-v1"
    source_table: str | None = None
    allowed_schemes: tuple[str, ...] = ALL_CONCEPT_SCHEMES
    truncated_fields: tuple[str, ...] = ()
    artifact_digest: str = ""
    segment_id: str = "legacy-segment"
    segment_ordinal: int = 0
    segment_count: int = 1
    segment_policy: str = "legacy-v1"
    tokenizer: str = "unknown"
    tokenizer_version: str = "unknown"
    token_count: int = 0
    max_segment_tokens: int = 0
    min_segment_tokens: int = 0
    source_spans: dict[str, tuple[int, int]] | None = None
    source_sha256: dict[str, str] | None = None
    field_sources: dict[str, str] | None = None
    boundaries: dict[str, str] | None = None
    element_ids: dict[str, str] | None = None
    element_kinds: dict[str, str] | None = None
    parent_element_ids: dict[str, str | None] | None = None
    context_fields: dict[str, str] | None = None
    previous_segment_id: str | None = None
    next_segment_id: str | None = None
    parent_segment_id: str | None = None

    @property
    def version_digest(self) -> str:
        return self.artifact_digest or self.digest


@dataclass(frozen=True)
class SubjectProfile:
    """Versioned mapping from one source record to an ontology artifact."""

    profile_id: str
    source_table: str
    subject_type: str
    id_columns: tuple[str, ...]
    text_columns: tuple[str, ...]
    allowed_schemes: tuple[str, ...]


@dataclass(frozen=True)
class ProfileSegmentationPolicy:
    """Declared processing policy for one subject profile."""

    profile_id: str
    mode: SegmentationMode
    policy_version: str = SEGMENT_POLICY_VERSION
    max_tokens: int = DEFAULT_MAX_SEGMENT_TOKENS
    min_tokens: int = DEFAULT_MIN_SEGMENT_TOKENS


SUBJECT_PROFILES = (
    SubjectProfile(
        "regulations-docket-v2",
        "dockets",
        "docket",
        ("docket_id",),
        ("title", "abstract"),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "regulations-document-v2",
        "documents",
        "document",
        ("document_id",),
        ("title",),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "regulations-comment-v1",
        "comments",
        "comment",
        ("comment_id",),
        (
            "title",
            "comment",
            "text_content",
            "organization",
            "category",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "federal-register-document-v1",
        "federal_register",
        "federal_register_document",
        ("document_number",),
        (
            "title",
            "abstract",
            "document_type",
            "agency_slugs",
            "body_text",
            "body_html",
            "full_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "unified-agenda-observation-v1",
        "unified_agenda",
        "regulatory_agenda_observation",
        ("rin", "agenda_edition"),
        (
            "title",
            "abstract",
            "rule_stage",
            "priority_category",
            "cfr_references_json",
            "legal_authority_json",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "cfr-section-v1",
        "cfr_sections",
        "cfr_section",
        ("granule_id",),
        (
            "heading",
            "cfr_ref",
            "title",
            "part",
            "section",
            "text",
            "full_text",
            "xml_text",
        ),
        ("subject",),
    ),
    SubjectProfile(
        "congress-bill-v1",
        "congress_bills",
        "congress_bill",
        ("bill_id",),
        (
            "title",
            "latest_action_text",
            "origin_chamber",
            "summary",
            "full_text",
            "xml_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "sam-entity-v1",
        "sam_entities",
        "sam_entity",
        ("uei",),
        (
            "legal_business_name",
            "dba_name",
            "entity_type_desc",
            "entity_structure_desc",
            "purpose_of_registration_desc",
            "primary_naics",
        ),
        ("regulated_entity",),
    ),
    SubjectProfile(
        "lobbying-filing-v1",
        "lobbying_filings",
        "lobbying_filing",
        ("filing_uuid",),
        (
            "client_name",
            "registrant_name",
            "lobbying_activities_json",
            "government_entities_json",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "fec-committee-v1",
        "fec_committees",
        "fec_committee",
        ("committee_id",),
        (
            "name",
            "committee_type_full",
            "organization_type_full",
            "party_full",
            "candidate_ids_json",
        ),
        ("regulated_entity",),
    ),
    SubjectProfile(
        "gao-report-v1",
        "gao_reports",
        "gao_report",
        ("report_id",),
        (
            "title",
            "abstract",
            "report_type",
            "agencies_json",
            "full_text",
            "pdf_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "crs-report-v1",
        "crs_reports",
        "crs_report",
        ("report_id",),
        (
            "title",
            "report_type",
            "status",
            "abstract",
            "full_text",
            "pdf_text",
        ),
        ("subject",),
    ),
    SubjectProfile(
        "court-opinion-v1",
        "court_opinions",
        "court_opinion",
        ("opinion_id",),
        (
            "case_name",
            "docket_number",
            "citation",
            "date_decided",
            "opinion_type",
            "holding",
            "html_with_citations",
            "plain_text",
            "pdf_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "court-docket-v1",
        "court_dockets",
        "court_docket",
        ("cl_docket_id",),
        (
            "case_name_full",
            "case_name",
            "nature_of_suit",
            "cause",
            "court_citation_string",
            "opinion_text",
            "html_text",
            "full_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "usaspending-recipient-v1",
        "usaspending_recipients",
        "usaspending_recipient",
        ("recipient_id",),
        ("name", "recipient_level"),
        ("regulated_entity",),
    ),
    SubjectProfile(
        "fcc-proceeding-v1",
        "fcc_proceedings",
        "fcc_proceeding",
        ("id_proceeding",),
        ("name", "description", "rulemaking_or_docket", "bureau_name"),
        ALL_CONCEPT_SCHEMES,
    ),
    SubjectProfile(
        "fcc-filing-v1",
        "fcc_filings",
        "fcc_filing",
        ("id_submission",),
        (
            "submission_type",
            "text_data",
            "express_comment",
            "bureaus_json",
            "lawfirms_json",
            "full_text",
        ),
        ALL_CONCEPT_SCHEMES,
    ),
)

_ATOMIC_PROFILES = {
    "regulations-docket-v2",
    "unified-agenda-observation-v1",
    "sam-entity-v1",
    "fec-committee-v1",
    "court-docket-v1",
    "usaspending-recipient-v1",
    "fcc-proceeding-v1",
}
_STRUCTURED_CHILD_PROFILES = {"lobbying-filing-v1"}

PROFILE_SEGMENTATION_POLICIES = {
    profile.profile_id: ProfileSegmentationPolicy(
        profile_id=profile.profile_id,
        mode=(
            "atomic-record"
            if profile.profile_id in _ATOMIC_PROFILES
            else (
                "structured-children"
                if profile.profile_id in _STRUCTURED_CHILD_PROFILES
                else "hierarchical-document"
            )
        ),
    )
    for profile in SUBJECT_PROFILES
}

EXCLUDED_SOURCE_TABLES = {
    "comments_index": (
        "Aggregate partition metadata has no independent document or domain "
        "subject to tag."
    ),
    "fr_docket_links": (
        "A relationship carrier is evidence between its endpoint artifacts, "
        "not another topical subject."
    ),
}

_PROFILE_BY_SOURCE = {
    profile.source_table: profile
    for profile in SUBJECT_PROFILES
}


def _identity_rank(subject_type: str, subject_id: str, profile_id: str) -> int:
    identity = f"{subject_type}:{subject_id}:{profile_id}"
    return int(hashlib.sha256(identity.encode()).hexdigest()[:16], 16)


def _balanced_batch(
    values: Iterable[Artifact | Subject],
    limit: int,
) -> list[Artifact | Subject]:
    if limit <= 0:
        return []
    heaps: dict[
        str,
        list[tuple[int, str, str, str, Artifact | Subject]],
    ] = defaultdict(list)
    for value in values:
        rank = _identity_rank(
            value.subject_type,
            value.subject_id,
            value.profile_id,
        )
        value_key = (
            value.segment_id
            if isinstance(value, Subject)
            else value.digest
        )
        item = (
            -rank,
            value.subject_id,
            value.profile_id,
            value_key,
            value,
        )
        heap = heaps[value.subject_type]
        if len(heap) < limit:
            heapq.heappush(heap, item)
        elif rank < -heap[0][0]:
            heapq.heapreplace(heap, item)

    queues: dict[str, deque[Artifact | Subject]] = {
        subject_type: deque(
            item[4]
            for item in sorted(
                heap,
                key=lambda item: (-item[0], item[1], item[2], item[3]),
            )
        )
        for subject_type, heap in heaps.items()
    }
    selected: list[Artifact | Subject] = []
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


def balanced_artifact_batch(
    artifacts: Iterable[Artifact],
    limit: int,
) -> list[Artifact]:
    """Select a deterministic, profile-balanced batch of source artifacts."""
    return [
        value
        for value in _balanced_batch(artifacts, limit)
        if isinstance(value, Artifact)
    ]


def balanced_subject_batch(
    subjects: Iterable[Subject],
    limit: int,
) -> list[Subject]:
    """Backward-compatible balanced selection for already-built segments."""
    return [
        value
        for value in _balanced_batch(subjects, limit)
        if isinstance(value, Subject)
    ]


def _normalize_identifier(value: object) -> str:
    return " ".join(str(value or "").split())


def _source_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return canonical_json(value)
    return str(value)


def _subject_id(profile: SubjectProfile, row: dict) -> str | None:
    values = [
        _normalize_identifier(row.get(column))
        for column in profile.id_columns
    ]
    if any(not value for value in values):
        return None
    if len(values) == 1:
        value = values[0]
        if profile.source_table == "dockets":
            return normalize_regsgov_identifier(value)
        return value
    return canonical_json(
        {
            column: value
            for column, value in zip(profile.id_columns, values)
        }
    )


def _artifact_title(
    values: Sequence[tuple[str, object]],
) -> dict[str, str]:
    preferred = (
        ".title",
        ".name",
        ".heading",
        ".legal_business_name",
        ".case_name_full",
        ".case_name",
    )
    for suffix in preferred:
        for source_field, raw_value in values:
            value = _source_text(raw_value)
            if source_field.endswith(suffix) and value and value.strip():
                return {"artifact_title": value}
    return {}


def _make_artifact(
    profile: SubjectProfile,
    row: dict,
    *,
    extra_fields: Iterable[tuple[str, object]] = (),
) -> Artifact | None:
    subject_id = _subject_id(profile, row)
    if subject_id is None:
        return None
    values = [
        (f"{profile.source_table}.{column}", row.get(column))
        for column in profile.text_columns
    ]
    values.extend(extra_fields)
    source_version = [
        {
            "source_field": source_field,
            "value": _source_text(raw_value),
        }
        for source_field, raw_value in values
    ]
    digest = text_digest(
        canonical_json(
            {
                "profile": profile.profile_id,
                "source_table": profile.source_table,
                "subject_type": profile.subject_type,
                "subject_id": subject_id,
                "source_values": source_version,
            }
        )
    )

    exclusions: list[SourceExclusion] = []
    raw_fields: dict[str, str] = {}
    for source_field, raw_value in values:
        text = _source_text(raw_value)
        raw_digest = hashlib.sha256((text or "").encode()).hexdigest()
        if text is None:
            exclusions.append(
                SourceExclusion(
                    source_field=source_field,
                    reason="null",
                    start_char=0,
                    end_char=0,
                    raw_text_sha256=raw_digest,
                )
            )
            continue
        if not text.strip():
            exclusions.append(
                SourceExclusion(
                    source_field=source_field,
                    reason="blank-non-content",
                    start_char=0,
                    end_char=len(text),
                    raw_text_sha256=raw_digest,
                )
            )
            continue
        raw_fields[source_field] = text

    policy = PROFILE_SEGMENTATION_POLICIES[profile.profile_id]
    adapter = adapter_for(policy.mode)
    drafts = adapter.elements(raw_fields)
    element_ids: list[str] = []
    for ordinal, draft in enumerate(drafts):
        element_identity = canonical_json(
            {
                "adapter": adapter.adapter_id,
                "subject_type": profile.subject_type,
                "subject_id": subject_id,
                "artifact_digest": digest,
                "source_field": draft.source_field,
                "start_char": draft.start_char,
                "end_char": draft.end_char,
                "kind": draft.kind,
                "ordinal": ordinal,
            }
        )
        element_ids.append(
            "source_element_"
            + hashlib.sha256(element_identity.encode()).hexdigest()[:24]
        )
    elements: list[SourceElement] = []
    for ordinal, draft in enumerate(drafts):
        source_text = raw_fields[draft.source_field]
        text = source_text[draft.start_char : draft.end_char]
        elements.append(
            SourceElement(
                element_id=element_ids[ordinal],
                kind=draft.kind,
                ordinal=ordinal,
                parent_element_id=(
                    element_ids[draft.parent_ordinal]
                    if draft.parent_ordinal is not None
                    else None
                ),
                ancestor_path=draft.ancestor_path,
                source_field=draft.source_field,
                start_char=draft.start_char,
                end_char=draft.end_char,
                text=text,
                raw_text_sha256=hashlib.sha256(text.encode()).hexdigest(),
                source_text_sha256=hashlib.sha256(
                    source_text.encode()
                ).hexdigest(),
                evidence_eligible=draft.evidence_eligible,
            )
        )

    return Artifact(
        subject_type=profile.subject_type,
        subject_id=subject_id,
        profile_id=profile.profile_id,
        source_table=profile.source_table,
        allowed_schemes=profile.allowed_schemes,
        digest=digest,
        raw_fields=raw_fields,
        elements=tuple(elements),
        exclusions=tuple(exclusions),
        context_fields=_artifact_title(values),
        segmentation_mode=policy.mode,
        adapter_id=adapter.adapter_id,
    )


def _document_artifacts(
    output_dir: Path,
    profile: SubjectProfile,
) -> Iterable[Artifact]:
    documents_file = output_dir / "documents.parquet"
    fr_file = output_dir / "federal_register.parquet"
    relevant_fr_numbers = {
        str(row["fr_doc_num"])
        for row in iter_parquet_rows(
            documents_file,
            columns=("fr_doc_num",),
        )
        if row.get("fr_doc_num")
    }
    fr_by_number: dict[str, dict] = {}
    if fr_file.exists() and relevant_fr_numbers:
        for row in iter_parquet_rows(
            fr_file,
            columns=("document_number", "title", "abstract"),
        ):
            number = str(row.get("document_number") or "")
            if number in relevant_fr_numbers:
                fr_by_number[number] = row

    for row in iter_parquet_rows(documents_file):
        fr = fr_by_number.get(str(row.get("fr_doc_num") or ""), {})
        artifact = _make_artifact(
            profile,
            row,
            extra_fields=(
                ("federal_register.title", fr.get("title")),
                ("federal_register.abstract", fr.get("abstract")),
                ("documents.text_content", row.get("text_content")),
                ("documents.body_text", row.get("body_text")),
                ("documents.body_html", row.get("body_html")),
                ("documents.pdf_text", row.get("pdf_text")),
                ("documents.full_text", row.get("full_text")),
            ),
        )
        if artifact is not None:
            yield artifact


def _validate_required_sources(
    output_dir: Path,
    required_source_tables: Iterable[str],
) -> None:
    required = set(required_source_tables)
    unknown = sorted(required - set(_PROFILE_BY_SOURCE))
    if unknown:
        raise ValueError(
            "Required ontology subject sources are not taggable profiles: "
            + ", ".join(unknown)
        )
    missing = sorted(
        source
        for source in required
        if not (output_dir / f"{source}.parquet").exists()
    )
    if missing:
        raise FileNotFoundError(
            f"concept subject inputs missing from {output_dir}: "
            + ", ".join(f"{source}.parquet" for source in missing)
        )


def iter_artifacts(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
) -> Iterator[Artifact]:
    """Yield every available source artifact without truncating its text."""
    _validate_required_sources(output_dir, required_source_tables)
    for profile in SUBJECT_PROFILES:
        path = output_dir / f"{profile.source_table}.parquet"
        if not path.exists():
            continue
        if profile.source_table == "documents":
            yield from _document_artifacts(output_dir, profile)
            continue
        for row in iter_parquet_rows(path):
            artifact = _make_artifact(profile, row)
            if artifact is not None:
                yield artifact


def segment_artifact(
    artifact: Artifact,
    *,
    max_tokens: int | None = None,
    min_tokens: int | None = None,
    token_counter: TokenCounter | None = None,
) -> list[Subject]:
    """Build all bounded processing segments for one artifact version."""
    policy = PROFILE_SEGMENTATION_POLICIES[artifact.profile_id]
    effective_max_tokens = (
        policy.max_tokens if max_tokens is None else max_tokens
    )
    effective_min_tokens = (
        policy.min_tokens if min_tokens is None else min_tokens
    )
    effective_policy_version = (
        f"{policy.policy_version}:"
        f"max{effective_max_tokens}:min{effective_min_tokens}"
    )
    eligible_elements = [
        element
        for element in artifact.elements
        if element.evidence_eligible and element.text
    ]
    field_counts = defaultdict(int)
    for element in eligible_elements:
        field_counts[element.source_field] += 1
    elements_by_ref: dict[str, SourceElement] = {}
    element_fields: dict[str, str] = {}
    for element in eligible_elements:
        if (
            field_counts[element.source_field] == 1
            and element.start_char == 0
            and element.end_char
            == len(artifact.raw_fields[element.source_field])
        ):
            field_ref = element.source_field
        else:
            field_ref = (
                f"{element.source_field}::element:{element.ordinal}:"
                f"{element.start_char}-{element.end_char}"
            )
        elements_by_ref[field_ref] = element
        element_fields[field_ref] = element.text
    records = segment_fields(
        element_fields,
        max_tokens=effective_max_tokens,
        min_tokens=effective_min_tokens,
        token_counter=token_counter,
        policy_version=effective_policy_version,
        identity_scope={
            "subject_type": artifact.subject_type,
            "subject_id": artifact.subject_id,
            "artifact_digest": artifact.digest,
            "profile_id": artifact.profile_id,
            "adapter_id": artifact.adapter_id,
        },
    )
    count = len(records)
    result: list[Subject] = []
    for record in records:
        artifact_spans = {
            field_ref: (
                elements_by_ref[field_ref].start_char + start,
                elements_by_ref[field_ref].start_char + end,
            )
            for field_ref, (start, end) in record.source_spans.items()
        }
        field_sources = {
            field_ref: elements_by_ref[field_ref].source_field
            for field_ref in record.fields
        }
        context_fields = dict(artifact.context_fields)
        heading_paths = sorted(
            {
                " > ".join(elements_by_ref[field_ref].ancestor_path)
                for field_ref in record.fields
                if elements_by_ref[field_ref].ancestor_path
            }
        )
        if heading_paths:
            context_fields["heading_path"] = "\n".join(heading_paths)
        segment_digest = text_digest(
            canonical_json(
                {
                    "artifact_digest": artifact.digest,
                    "segment_id": record.segment_id,
                    "fields": record.fields,
                    "field_sources": field_sources,
                    "source_spans": artifact_spans,
                    "policy": record.policy_version,
                }
            )
        )
        result.append(
            Subject(
                subject_type=artifact.subject_type,
                subject_id=artifact.subject_id,
                text="\n".join(record.fields.values()),
                fields=record.fields,
                digest=segment_digest,
                profile_id=artifact.profile_id,
                source_table=artifact.source_table,
                allowed_schemes=artifact.allowed_schemes,
                artifact_digest=artifact.digest,
                segment_id=record.segment_id,
                segment_ordinal=record.ordinal,
                segment_count=count,
                segment_policy=record.policy_version,
                tokenizer=record.tokenizer,
                tokenizer_version=record.tokenizer_version,
                token_count=record.token_count,
                max_segment_tokens=effective_max_tokens,
                min_segment_tokens=effective_min_tokens,
                source_spans=artifact_spans,
                source_sha256={
                    field_ref: elements_by_ref[
                        field_ref
                    ].source_text_sha256
                    for field_ref in record.fields
                },
                field_sources=field_sources,
                boundaries={
                    field: str(boundary)
                    for field, boundary in record.boundaries.items()
                },
                element_ids={
                    field_ref: elements_by_ref[field_ref].element_id
                    for field_ref in record.fields
                },
                element_kinds={
                    field_ref: elements_by_ref[field_ref].kind
                    for field_ref in record.fields
                },
                parent_element_ids={
                    field_ref: elements_by_ref[
                        field_ref
                    ].parent_element_id
                    for field_ref in record.fields
                },
                context_fields=context_fields,
                previous_segment_id=record.previous_segment_id,
                next_segment_id=record.next_segment_id,
            )
        )
    segment_by_element = {
        element_id: subject.segment_id
        for subject in result
        for element_id in (subject.element_ids or {}).values()
    }
    linked: list[Subject] = []
    for subject in result:
        parent_segments = sorted(
            {
                segment_by_element[parent_id]
                for parent_id in (
                    subject.parent_element_ids or {}
                ).values()
                if parent_id in segment_by_element
                and segment_by_element[parent_id] != subject.segment_id
            }
        )
        linked.append(
            replace(
                subject,
                parent_segment_id=(
                    parent_segments[0] if parent_segments else None
                ),
            )
        )
    return linked


def iter_subjects(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
    max_tokens: int | None = None,
    min_tokens: int | None = None,
    token_counter: TokenCounter | None = None,
) -> Iterator[Subject]:
    """Yield every bounded segment for every available profiled artifact."""
    for artifact in iter_artifacts(
        output_dir,
        required_source_tables=required_source_tables,
    ):
        yield from segment_artifact(
            artifact,
            max_tokens=max_tokens,
            min_tokens=min_tokens,
            token_counter=token_counter,
        )


def build_artifacts(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
) -> list[Artifact]:
    return sorted(
        iter_artifacts(
            output_dir,
            required_source_tables=required_source_tables,
        ),
        key=lambda artifact: (
            artifact.subject_type,
            artifact.subject_id,
            artifact.profile_id,
        ),
    )


def build_subjects(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
    max_tokens: int | None = None,
    min_tokens: int | None = None,
    token_counter: TokenCounter | None = None,
) -> list[Subject]:
    """Materialize lossless processing segments for tests and inspection."""
    return sorted(
        iter_subjects(
            output_dir,
            required_source_tables=required_source_tables,
            max_tokens=max_tokens,
            min_tokens=min_tokens,
            token_counter=token_counter,
        ),
        key=lambda subject: (
            subject.subject_type,
            subject.subject_id,
            subject.profile_id,
            subject.segment_ordinal,
        ),
    )


def subjects_by_key(
    output_dir: Path,
    keys: set[tuple[str, str]],
) -> dict[tuple[str, str], Subject]:
    """Load the first segment for legacy artifact-key callers."""
    found: dict[tuple[str, str], Subject] = {}
    if not keys:
        return found
    for subject in iter_subjects(output_dir):
        key = (subject.subject_type, subject.subject_id)
        if key in keys and key not in found:
            found[key] = subject
            if len(found) == len(keys):
                break
    return found


def subjects_by_segment_id(
    output_dir: Path,
    segment_ids: set[str],
) -> dict[str, Subject]:
    """Load only requested segment versions while scanning source profiles."""
    found: dict[str, Subject] = {}
    if not segment_ids:
        return found
    for subject in iter_subjects(output_dir):
        if subject.segment_id in segment_ids:
            found[subject.segment_id] = subject
            if len(found) == len(segment_ids):
                break
    return found

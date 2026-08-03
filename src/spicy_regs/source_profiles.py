"""Side-effect-free declarations for SpicyRegs source profiles.

This module contains data and validation only. Artifact generators and other
products may import it without initializing document parsing, providers,
runtime services, or ontology code.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Mapping

REGION_ADAPTER_VERSION = "source-elements-v2"


@dataclass(frozen=True)
class AccessScope:
    """Who may see a source state and the stated basis for that access."""

    scope: str
    basis: str

    def __post_init__(self) -> None:
        if not str(self.scope).strip():
            raise ValueError("an access scope must be stated explicitly")
        if not str(self.basis).strip():
            raise ValueError("an access basis must be stated explicitly")

    @property
    def declared(self) -> bool:
        return self != UNDECLARED_ACCESS

    def as_dict(self) -> dict[str, str]:
        return {"scope": self.scope, "basis": self.basis}


UNDECLARED_ACCESS = AccessScope(scope="unknown", basis="undeclared")
PUBLIC_RECORD_ACCESS = AccessScope(scope="public", basis="us-federal-public-record")

SourceMode = Literal["atomic-record", "structured-children", "hierarchical-document"]

REGION_ADAPTER_IDS: dict[str, str] = {
    "atomic-record": f"atomic-fields:{REGION_ADAPTER_VERSION}",
    "structured-children": f"structured-children:{REGION_ADAPTER_VERSION}",
    "hierarchical-document": f"hierarchical-text:{REGION_ADAPTER_VERSION}",
}


@dataclass(frozen=True)
class SourceProfile:
    """One versioned mapping from a source table onto source artifacts."""

    profile_id: str
    source_table: str
    subject_type: str
    id_columns: tuple[str, ...]
    text_columns: tuple[str, ...]
    allowed_schemes: tuple[str, ...]
    mode: SourceMode
    access: AccessScope

    def __post_init__(self) -> None:
        if self.mode not in REGION_ADAPTER_IDS:
            raise ValueError(f"unknown source mode {self.mode!r}")
        if not self.id_columns:
            raise ValueError(f"profile {self.profile_id} declares no identity columns")

    @property
    def region_adapter_id(self) -> str:
        return REGION_ADAPTER_IDS[self.mode]


def _profile(
    profile_id: str,
    source_table: str,
    subject_type: str,
    id_columns: tuple[str, ...],
    text_columns: tuple[str, ...],
    allowed_schemes: tuple[str, ...],
    mode: SourceMode,
) -> SourceProfile:
    return SourceProfile(
        profile_id=profile_id,
        source_table=source_table,
        subject_type=subject_type,
        id_columns=id_columns,
        text_columns=text_columns,
        allowed_schemes=allowed_schemes,
        mode=mode,
        access=PUBLIC_RECORD_ACCESS,
    )


ALL_CONCEPT_SCHEMES: tuple[str, ...] = ("subject", "regulated_entity")

SOURCE_PROFILES: tuple[SourceProfile, ...] = (
    _profile("regulations-docket-v2", "dockets", "docket", ("docket_id",), ("title", "abstract"), ALL_CONCEPT_SCHEMES, "atomic-record"),
    _profile("regulations-document-v2", "documents", "document", ("document_id",), ("title",), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("regulations-comment-v1", "comments", "comment", ("comment_id",), ("title", "comment", "text_content", "organization", "category"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("federal-register-document-v1", "federal_register", "federal_register_document", ("document_number",), ("title", "abstract", "document_type", "agency_slugs", "body_text", "body_html", "full_text"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("unified-agenda-observation-v1", "unified_agenda", "regulatory_agenda_observation", ("rin", "agenda_edition"), ("title", "abstract", "rule_stage", "priority_category", "cfr_references_json", "legal_authority_json"), ALL_CONCEPT_SCHEMES, "atomic-record"),
    _profile("cfr-section-v1", "cfr_sections", "cfr_section", ("granule_id",), ("heading", "cfr_ref", "title", "part", "section", "text", "full_text", "xml_text"), ("subject",), "hierarchical-document"),
    _profile("congress-bill-v1", "congress_bills", "congress_bill", ("bill_id",), ("title", "latest_action_text", "origin_chamber", "summary", "full_text", "xml_text"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("sam-entity-v1", "sam_entities", "sam_entity", ("uei",), ("legal_business_name", "dba_name", "entity_type_desc", "entity_structure_desc", "purpose_of_registration_desc", "primary_naics"), ("regulated_entity",), "atomic-record"),
    _profile("lobbying-filing-v1", "lobbying_filings", "lobbying_filing", ("filing_uuid",), ("client_name", "registrant_name", "lobbying_activities_json", "government_entities_json"), ALL_CONCEPT_SCHEMES, "structured-children"),
    _profile("fec-committee-v1", "fec_committees", "fec_committee", ("committee_id",), ("name", "committee_type_full", "organization_type_full", "party_full", "candidate_ids_json"), ("regulated_entity",), "atomic-record"),
    _profile("gao-report-v1", "gao_reports", "gao_report", ("report_id",), ("title", "abstract", "report_type", "agencies_json", "full_text", "pdf_text"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("crs-report-v1", "crs_reports", "crs_report", ("report_id",), ("title", "report_type", "status", "abstract", "full_text", "pdf_text"), ("subject",), "hierarchical-document"),
    _profile("court-opinion-v1", "court_opinions", "court_opinion", ("opinion_id",), ("case_name", "docket_number", "citation", "date_decided", "opinion_type", "holding", "html_with_citations", "plain_text", "pdf_text"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
    _profile("court-docket-v1", "court_dockets", "court_docket", ("cl_docket_id",), ("case_name_full", "case_name", "nature_of_suit", "cause", "court_citation_string", "opinion_text", "html_text", "full_text"), ALL_CONCEPT_SCHEMES, "atomic-record"),
    _profile("usaspending-recipient-v1", "usaspending_recipients", "usaspending_recipient", ("recipient_id",), ("name", "recipient_level"), ("regulated_entity",), "atomic-record"),
    _profile("fcc-proceeding-v1", "fcc_proceedings", "fcc_proceeding", ("id_proceeding",), ("name", "description", "rulemaking_or_docket", "bureau_name"), ALL_CONCEPT_SCHEMES, "atomic-record"),
    _profile("fcc-filing-v1", "fcc_filings", "fcc_filing", ("id_submission",), ("submission_type", "text_data", "express_comment", "bureaus_json", "lawfirms_json", "full_text"), ALL_CONCEPT_SCHEMES, "hierarchical-document"),
)  # fmt: skip

EXCLUDED_SOURCE_TABLES: dict[str, str] = {
    "comments_index": "Aggregate partition metadata has no independent document or domain subject to tag.",
    "fr_docket_links": "A relationship carrier is evidence between its endpoint artifacts, not another topical subject.",
}

SOURCE_PROFILE_BY_TABLE: Mapping[str, SourceProfile] = MappingProxyType(
    {profile.source_table: profile for profile in SOURCE_PROFILES}
)

STEP4_ACTIVE_SOURCE_TABLES = frozenset(SOURCE_PROFILE_BY_TABLE) - {"comments"}


def declared_profile_for_table(source_table: str) -> SourceProfile:
    """Return a profile without importing the document-processing runtime."""

    return SOURCE_PROFILE_BY_TABLE[source_table]

"""The v3 ``source`` step: exact Artifact versions and durable SourceFragments.

What goes in is one immutable source record — a row from a profiled source
table, plus any declared byte rendition attached to it. What comes out is one
:class:`SourceArtifact`: one exact source state, one canonical stream of
:class:`SourceRegion` records over its fields, the durable
:class:`SourceFragment` projection of the meaningful ones, and a complete,
honest account of everything excluded, quarantined, or left uncovered.

The rules this module holds, and where each one lives:

* **One Artifact is one exact immutable source state.** ``content_sha256``
  covers the profile, the source table, the subject type and id, and every
  declared source value — nulls included — in declaration order. Every region
  and fragment names that digest, so a consumer can prove which source state it
  is looking at.
* **Coordinates are Python unicode codepoints, half-open.** Native regions
  target an immutable Artifact field; parser-derived regions target the
  adapter-built parsed text. :func:`check_region_coordinates` proves
  ``region.text == field_text[start:end]`` before an artifact is returned. A
  region carrying an inclusive end, a byte offset, a UTF-16 offset, or the
  wrong target fails there.
* **Regions plus exclusions account for every codepoint.**
  :class:`FieldCoverage` records what each field's regions cover, split by
  durability, and names any gap explicitly. Syntax (``markup-prolog``) and
  container (``structured-array``) regions stay in that accounting and never
  become durable fragments — but the processing stream they belong to is
  unchanged by this migration, so ``segments.py`` still sees exactly what the
  predecessor produced.
* **Native structure wins.** :data:`DISPATCH_PRIORITY` is the whole order:
  structured JSON/API fields, native markup, declared atomic fields, native
  prose (including existing pypdf-derived text), and only then the contained
  Office parser. Which branch produced a field's regions is recorded on the
  artifact, so "the parser did not run here" is a fact a receipt states rather
  than a claim. A record with usable native text never reaches the parser, even
  when it also carries an Office attachment.
* **Nothing is invented.** Unknown identity, unknown access, an unrepresentable
  coordinate, or a parser that never produced a record becomes a
  :class:`SourceQuarantine` or a failure, as the plan's :class:`SourcePolicy`
  directs. There is no default access scope anywhere in this module.
* **Parser-derived is never source-exact.** A field this project read straight
  from the source table carries :data:`SOURCE_EXACT_EVIDENCE`; a field the
  contained Office parser produced carries :data:`PARSER_DERIVED_EVIDENCE` and
  its own coordinate target. Docling objects never reach this module: the parser
  runs in a child process and hands back JSON built from project-owned records.
  Body elements may become evidence. Furniture and notes remain durable
  context-only fragments; background and invisible elements stay held and
  never reach fragments or segments. Every parser decision and process outcome
  is retained in immutable ``source/parser-attempts.parquet`` rows.

Migration parity is deliberate, not accidental. Native ``content_sha256`` and
region identities, region order, region kinds, and ``evidence_eligible``
reproduce the predecessor (``ontology/subjects.py`` plus
``ontology/adapters.py``) exactly, so the native segment stream
``segments.py`` consumes is byte-identical. Parser-derived region identities
also bind ``parser_id`` and the parsed field digest, so a changed parser mapping
cannot collide with an earlier result. See
``tests/test_docpipeline_source_migration.py``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Literal, Protocol

from spicy_regs.docpipeline.adapters.docling import (
    ADAPTER_MAPPING_REVISION,
    CONTENT_LAYERS,
    DEFAULT_MAX_SOURCE_BYTES,
    DEFERRED_FORMATS,
    DOCLING_CORE_VERSION,
    DOCLING_VERSION,
    FORMAT_UNKNOWN,
    NO_COORDINATES,
    PARSER_PAGE_COORDINATES,
    SUPPORTED_FORMATS,
    DoclingParseError,
    detect_input_format,
)
from spicy_regs.docpipeline.runtime import (
    CheckResult,
    redact,
    redact_text,
    scan_text_for_secrets,
)
from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import canonical_json, iter_parquet_rows, stable_id, text_digest

SOURCE_STEP = "source"

SOURCE_POLICY_VERSION = "source-regions-v1"
"""This module's own semantics version, recorded on every artifact row.

Bump it when a durability class, a dispatch branch, a coverage rule, or a
recorded coordinate meaning changes. It is *not* the region identity recipe:
that is :data:`REGION_ADAPTER_VERSION`, which is pinned for migration parity.
"""

REGION_ADAPTER_VERSION = "source-elements-v2"
"""The predecessor's adapter version, preserved because region ids are built on it.

``ontology/adapters.py`` hashed this string into every element id. Changing it
would rewrite every region id in the frozen data for no semantic gain, so the
lexical value stays exactly as it was and this module states why.
"""

REGION_ID_PREFIX = "source_element"
"""The predecessor's id prefix, kept so a migrated id compares equal by value."""

FRAGMENT_ID_PREFIX = "source_fragment"
ARTIFACT_ID_PREFIX = "artifact"

# --- coordinates and evidence grades ---------------------------------------

COORDINATE_UNIT = "unicode-codepoints"
COORDINATE_INTERVAL = "half-open"

ARTIFACT_FIELD_TARGET = "artifact-source-field"
"""What a source region's offsets address: one exact field of one Artifact."""

PARSED_TEXT_TARGET = "adapter-parsed-text"
"""What a parser-derived region's offsets address: text the adapter built."""

SOURCE_EXACT_EVIDENCE = "source-exact"
"""Grade of a region sliced from a field the source itself published."""

PARSER_DERIVED_EVIDENCE = "parser-derived"
"""Grade of a region sliced from text a parser produced. Never source-exact."""

SOURCE_NATIVE_FIELD = "source-native"
PARSER_DERIVED_FIELD = "parser-derived"

SOURCE_CONTENT_LAYER = "body"
"""Native source fields are first-party body content."""

SOURCE_COORDINATE_GRADE = "source-exact"
"""Native field offsets are exact within the immutable Artifact field."""

BODY_CONTENT_LAYER = "body"
CONTEXT_CONTENT_LAYERS = frozenset({"furniture", "notes"})
HELD_CONTENT_LAYERS = frozenset({"background", "invisible"})
PARSER_CONTENT_LAYERS = frozenset(CONTENT_LAYERS)
PARSER_COORDINATE_GRADES = frozenset({NO_COORDINATES, PARSER_PAGE_COORDINATES})
"""Closed parser metadata sets copied from the pinned adapter boundary."""

# --- durability -------------------------------------------------------------

DURABLE_MEANINGFUL = "durable-meaningful"
"""A meaningful source region: it becomes a taggable, durable SourceFragment."""

SYNTAX_REGION = "syntax"
"""Markup syntax that carries no independent meaning — the XML/HTML prolog.

It stays in coverage accounting so no codepoint is silently dropped, and it
stays in the processing stream because changing that during this migration
would move segment boundaries. It never becomes a durable fragment.
"""

CONTAINER_REGION = "container"
"""A structural container whose children carry the meaning — a JSON array.

Its span overlaps its children's on purpose: the container is the whole array
and each child is one element. Coverage counts both, separately.
"""

DURABILITY_CLASSES: tuple[str, ...] = (DURABLE_MEANINGFUL, SYNTAX_REGION, CONTAINER_REGION)

SYNTAX_KINDS = frozenset({"markup-prolog"})
CONTAINER_KINDS = frozenset({"structured-array"})

HEADING_KINDS = frozenset({"heading", "title", "section_header"})
"""Kinds recorded as context rather than as standalone evidence.

They stay processing slices for parity — removing them would move every segment
boundary — but ``context_only`` says plainly that a later prompt should treat
them as context, not as the evidence a claim rests on.
"""

# --- dispatch ---------------------------------------------------------------

DISPATCH_STRUCTURED_FIELDS = "structured-json-fields"
DISPATCH_NATIVE_MARKUP = "native-markup-structure"
DISPATCH_ATOMIC_FIELDS = "declared-atomic-fields"
DISPATCH_NATIVE_PROSE = "native-prose-structure"
DISPATCH_CONTAINED_PARSER = "contained-office-parser"

DISPATCH_PRIORITY: tuple[str, ...] = (
    DISPATCH_STRUCTURED_FIELDS,
    DISPATCH_NATIVE_MARKUP,
    DISPATCH_ATOMIC_FIELDS,
    DISPATCH_NATIVE_PROSE,
    DISPATCH_CONTAINED_PARSER,
)
"""The whole native-first order, highest priority first.

The parser is last on purpose and reachable only when a record publishes no
usable native text at all. ``SourceArtifact.dispatch`` lists the branches one
record really used, in this order.
"""

# --- exclusion and quarantine reasons ---------------------------------------

EXCLUSION_NULL = "null"
EXCLUSION_BLANK = "blank-non-content"
EXCLUSION_NATIVE_PREFERRED = "native-structure-preferred"
EXCLUSION_PARSER_OMISSION = "parser-omitted-element"
EXCLUSION_COVERAGE_GAP = "uncovered-source-span"
EXCLUSION_HELD_CONTENT_LAYER = "parser-content-layer-held"
EXCLUSION_INACTIVE_SOURCE_TABLE = "inactive-source-table"

REASON_UNKNOWN_IDENTITY = "unknown_identity"
REASON_UNKNOWN_ACCESS = "unknown_access"
REASON_FORMAT_NOT_IMPLEMENTED = "format_not_implemented"
REASON_UNSUPPORTED_ATTACHMENT = "unsupported_attachment_format"
REASON_PARSER_FAILED = "parser_failed"
REASON_PARSER_UNAVAILABLE = "parser_unavailable"
REASON_PARSER_DISABLED = "parser_disabled"
REASON_MULTIPLE_RENDITIONS = "multiple_renditions_not_implemented"

DEFAULT_QUARANTINE_REASONS = frozenset(
    {
        REASON_UNKNOWN_IDENTITY,
        REASON_UNKNOWN_ACCESS,
        REASON_FORMAT_NOT_IMPLEMENTED,
        REASON_UNSUPPORTED_ATTACHMENT,
        REASON_PARSER_FAILED,
        REASON_PARSER_UNAVAILABLE,
        REASON_PARSER_DISABLED,
        REASON_MULTIPLE_RENDITIONS,
    }
)
"""Reasons a default plan settles as ``rejected``. Everything else fails.

A coordinate or coverage violation is deliberately absent: those are this
module's own invariants, and a run that hits one has a defect, not a source
problem.
"""

# --- tables -----------------------------------------------------------------

ARTIFACT_TABLE = "source/artifacts.parquet"
FRAGMENT_TABLE = "source/fragments.parquet"
COVERAGE_TABLE = "source/coverage.parquet"
PARSER_ATTEMPT_TABLE = "source/parser-attempts.parquet"

ARTIFACT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("artifact_id", "string"),
    ("content_sha256", "string"),
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("profile_id", "string"),
    ("source_table", "string"),
    ("allowed_schemes", "string"),
    ("access_scope", "string"),
    ("access_basis", "string"),
    ("coordinate_target", "string"),
    ("coordinate_unit", "string"),
    ("coordinate_interval", "string"),
    ("region_adapter_version", "string"),
    ("source_policy_version", "string"),
    ("dispatch", "string"),
    ("parser_invoked", "bool"),
    ("parser_id", "string"),
    ("parser_evidence_grade", "string"),
    ("parser_gate_classification", "string"),
    ("field_count", "int64"),
    ("field_chars", "int64"),
    ("region_count", "int64"),
    ("fragment_count", "int64"),
    ("exclusion_count", "int64"),
    ("uncovered_chars", "int64"),
    ("secret_rules", "string"),
    ("context_fields", "string"),
)

FRAGMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("fragment_id", "string"),
    ("artifact_id", "string"),
    ("artifact_sha256", "string"),
    ("region_id", "string"),
    ("subject_type", "string"),
    ("subject_id", "string"),
    ("profile_id", "string"),
    ("source_table", "string"),
    ("source_field", "string"),
    ("field_origin", "string"),
    ("field_sha256", "string"),
    ("kind", "string"),
    ("ordinal", "int64"),
    ("parent_region_id", "string"),
    ("heading_path", "string"),
    ("start_char", "int64"),
    ("end_char", "int64"),
    ("char_count", "int64"),
    ("coordinate_target", "string"),
    ("coordinate_unit", "string"),
    ("coordinate_interval", "string"),
    ("evidence_grade", "string"),
    ("content_layer", "string"),
    ("coordinate_grade", "string"),
    ("context_only", "bool"),
    ("text_sha256", "string"),
    ("text", "string"),
)

COVERAGE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("artifact_id", "string"),
    ("content_sha256", "string"),
    ("profile_id", "string"),
    ("source_field", "string"),
    ("field_origin", "string"),
    ("record_kind", "string"),
    ("reason", "string"),
    ("field_chars", "int64"),
    ("covered_chars", "int64"),
    ("durable_chars", "int64"),
    ("syntax_chars", "int64"),
    ("container_chars", "int64"),
    ("uncovered_chars", "int64"),
    ("gaps", "string"),
    ("region_count", "int64"),
    ("fragment_count", "int64"),
)

PARSER_ATTEMPT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("attempt_id", "string"),
    ("terminal_state", "string"),
    ("outcome_reason", "string"),
    ("identifier", "string"),
    ("profile_id", "string"),
    ("source_table", "string"),
    ("source_sha256", "string"),
    ("attachment_field", "string"),
    ("attachment_sha256", "string"),
    ("attachment_bytes", "int64"),
    ("source_name_sha256", "string"),
    ("media_type", "string"),
    ("detected_input_format", "string"),
    ("provider", "string"),
    ("parser_id", "string"),
    ("parser_policy_digest", "string"),
    ("parser_policy_json", "string"),
    ("source_policy_version", "string"),
    ("parser_enabled", "bool"),
    ("gate_worker_module", "string"),
    ("gate_classification", "string"),
    ("parser_status", "string"),
    ("parser_failure_reason", "string"),
    ("exit_status", "int64"),
    ("signal_number", "int64"),
    ("process_group_terminated", "bool"),
    ("duration_ms", "double"),
    ("result_bytes", "int64"),
    ("result_over_limit", "bool"),
    ("stderr_bytes", "int64"),
    ("stderr_over_limit", "bool"),
    ("result_sha256", "string"),
    ("limits_json", "string"),
    ("enforced_limits", "string"),
    ("observed_limits", "string"),
    ("unenforced_limits", "string"),
    ("call_json", "string"),
)

COVERAGE_RECORD_FIELD = "field-coverage"
COVERAGE_RECORD_EXCLUSION = "exclusion"
COVERAGE_RECORD_QUARANTINE = "quarantine"
COVERAGE_RECORD_SOURCE_TABLE_EXCLUSION = "source-table-exclusion"


# --------------------------------------------------------------------------
# errors
# --------------------------------------------------------------------------


class SourceError(Exception):
    """One of this module's own invariants did not hold."""


class SourceRetentionError(SourceError):
    """A parse did not account for enough of the document it was given.

    Raised by :func:`check_extraction_retention` at the extraction boundary. The
    message states the measured retention, the floor it failed, the parser and
    the format, because a refusal has to be diagnosable from the receipt without
    rerunning the parse.
    """


# --------------------------------------------------------------------------
# coordinates and access
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CoordinateSystem:
    """What a pair of offsets addresses, in what unit, over what interval."""

    target: str
    unit: str
    interval: str

    def __post_init__(self) -> None:
        if not str(self.target).strip():
            raise ValueError("a coordinate system names what its offsets address")
        if self.unit != COORDINATE_UNIT:
            raise ValueError(f"coordinate unit must be {COORDINATE_UNIT!r}, not {self.unit!r}")
        if self.interval != COORDINATE_INTERVAL:
            raise ValueError(f"coordinate interval must be {COORDINATE_INTERVAL!r}, not {self.interval!r}")

    def as_dict(self) -> dict[str, str]:
        return {"target": self.target, "unit": self.unit, "interval": self.interval}


SOURCE_FIELD_COORDINATES = CoordinateSystem(ARTIFACT_FIELD_TARGET, COORDINATE_UNIT, COORDINATE_INTERVAL)
PARSED_TEXT_COORDINATES = CoordinateSystem(PARSED_TEXT_TARGET, COORDINATE_UNIT, COORDINATE_INTERVAL)


@dataclass(frozen=True)
class AccessScope:
    """Who may see this source state, and on what stated basis.

    Both halves are required, and neither has a default anywhere in this module.
    A source whose access nobody declared is quarantined, never published as if
    it were public.
    """

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
"""The one access value that means "nobody said". It quarantines; it never passes."""

PUBLIC_RECORD_ACCESS = AccessScope(scope="public", basis="us-federal-public-record")
"""Every profile below publishes U.S. federal public-record data, and says so."""


# --------------------------------------------------------------------------
# profiles
# --------------------------------------------------------------------------

SourceMode = Literal["atomic-record", "structured-children", "hierarchical-document"]

REGION_ADAPTER_IDS: dict[str, str] = {
    "atomic-record": f"atomic-fields:{REGION_ADAPTER_VERSION}",
    "structured-children": f"structured-children:{REGION_ADAPTER_VERSION}",
    "hierarchical-document": f"hierarchical-text:{REGION_ADAPTER_VERSION}",
}
"""The predecessor's adapter identifiers, hashed into every region id."""


@dataclass(frozen=True)
class SourceProfile:
    """One versioned mapping from a source table onto Artifacts and regions.

    A new document family needs one of these plus its examples and tests —
    nothing in search, extraction, approval, or comparison changes.
    """

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
    "comments_index": ("Aggregate partition metadata has no independent document or domain subject to tag."),
    "fr_docket_links": (
        "A relationship carrier is evidence between its endpoint artifacts, not another topical subject."
    ),
}

_PROFILE_BY_TABLE = {profile.source_table: profile for profile in SOURCE_PROFILES}

STEP4_ACTIVE_SOURCE_TABLES = frozenset(_PROFILE_BY_TABLE) - {"comments"}
"""The explicit file-only Step 4 selection; comments remain inactive until Step 8."""


def profile_for_table(source_table: str) -> SourceProfile:
    """Return the one profile that maps a source table, or refuse the name."""
    try:
        return _PROFILE_BY_TABLE[source_table]
    except KeyError:
        raise SourceError(f"no source profile maps {source_table!r}") from None


# --------------------------------------------------------------------------
# native structure readers
# --------------------------------------------------------------------------
#
# Behaviour copied from the predecessor adapters, not their private names: the
# region kinds, boundaries, and heading rules below have to reproduce the frozen
# element stream exactly, or ``segments.py`` selects different segments.

_HEADING_PATTERN = re.compile(
    r"^(?:"
    r"section\b|§|title\b|part\b|subpart\b|chapter\b|"
    r"[IVXLC]+\.\s|"
    r"\d+(?:\.\d+)*(?:[.)]|\s+-)"
    r")",
    re.IGNORECASE,
)

BODY_COLUMNS = frozenset(
    {
        "abstract", "comment", "text_content", "text_data", "description", "body",
        "body_html", "body_text", "body_xml", "content_text", "html_text",
        "opinion_text", "pdf_text", "summary", "text", "full_text", "xml_text",
    }
)  # fmt: skip
"""Columns whose value is prose or markup a document really published.

``pdf_text`` is on this list on purpose: it is a text field of the immutable
source record, so it is read natively and never handed to the Office parser.
"""

_MARKUP_BLOCKS = frozenset(
    {
        "article", "chapter", "div", "enum", "h1", "h2", "h3", "h4", "h5", "h6",
        "header", "legis-body", "li", "p", "paragraph", "part", "pre", "section",
        "subsection", "table", "tbody", "td", "text", "th", "title", "tr",
    }
)  # fmt: skip

_MARKUP_HEADINGS = frozenset({"chapter", "enum", "h1", "h2", "h3", "h4", "h5", "h6", "header", "part", "title"})

_NON_CONTENT_MARKUP = frozenset({"iframe", "noscript", "script", "style", "svg", "template"})

_HTML_VOID_TAGS = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)


def _attributes_hide_content(attrs: Sequence[tuple[str, str | None]]) -> bool:
    normalized = {name.casefold(): value for name, value in attrs}
    if "hidden" in normalized or "inert" in normalized:
        return True
    aria_hidden = normalized.get("aria-hidden")
    if isinstance(aria_hidden, str) and aria_hidden.casefold().strip() == "true":
        return True
    style = normalized.get("style")
    return isinstance(style, str) and bool(
        re.search(r"(?:^|;)\s*(?:display\s*:\s*none|visibility\s*:\s*hidden)\b", style, re.IGNORECASE)
    )


_ATOMIC_HEADING_COLUMNS = frozenset({"title", "name", "heading", "legal_business_name"})


@dataclass(frozen=True)
class _RegionDraft:
    """Parser-neutral exact coordinates for one region, before identity."""

    kind: str
    source_field: str
    start_char: int
    end_char: int
    parent_ordinal: int | None = None
    heading_path: tuple[str, ...] = ()
    evidence_eligible: bool = True
    content_layer: str = SOURCE_CONTENT_LAYER
    coordinate_grade: str = SOURCE_COORDINATE_GRADE
    context_only: bool = False
    quarantine_reason: str | None = None


def _paragraph_ranges(text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    start = 0
    for match in re.finditer(r"\n[ \t]*\n", text):
        end = match.end()
        if end > start:
            ranges.append((start, end))
        start = end
    if start < len(text):
        ranges.append((start, len(text)))
    return ranges or [(0, len(text))]


def _looks_like_heading(text: str) -> bool:
    candidate = text.strip()
    if not candidate or len(candidate) > 180 or "\n" in candidate:
        return False
    return bool(_HEADING_PATTERN.match(candidate)) or (
        len(candidate.split()) <= 12 and candidate[-1:] not in {".", "!", "?", ";", ","}
    )


@dataclass(frozen=True)
class _MarkupEvent:
    start_char: int
    tag: str
    parent_event: int | None


class _MarkupBoundaryParser(HTMLParser):
    """Collect source-positioned native block starts without rewriting text."""

    def __init__(self, text: str) -> None:
        super().__init__(convert_charrefs=False)
        self.events: list[_MarkupEvent] = []
        self._structural_stack: list[tuple[str, int]] = []
        self._line_starts = [0]
        self._line_starts.extend(match.end() for match in re.finditer("\n", text))

    def _index(self) -> int:
        line, offset = self.getpos()
        return self._line_starts[line - 1] + offset

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        normalized = tag.casefold()
        if normalized not in _MARKUP_BLOCKS:
            return
        event_index = len(self.events)
        self.events.append(
            _MarkupEvent(
                start_char=self._index(),
                tag=normalized,
                parent_event=(self._structural_stack[-1][1] if self._structural_stack else None),
            )
        )
        self._structural_stack.append((normalized, event_index))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        normalized = tag.casefold()
        if normalized in _MARKUP_BLOCKS:
            self.events.append(
                _MarkupEvent(
                    start_char=self._index(),
                    tag=normalized,
                    parent_event=(self._structural_stack[-1][1] if self._structural_stack else None),
                )
            )

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        for index in range(len(self._structural_stack) - 1, -1, -1):
            if self._structural_stack[index][0] == normalized:
                del self._structural_stack[index:]
                break


class _TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


class _IndexableTextCollector(HTMLParser):
    """Collect visible text for new release passages without changing v3 IDs."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._stack: list[tuple[str, bool]] = []
        self._suppression_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.casefold()
        suppress = normalized in _NON_CONTENT_MARKUP or _attributes_hide_content(attrs)
        if normalized not in _HTML_VOID_TAGS:
            self._stack.append((normalized, suppress))
            self._suppression_depth += int(suppress)

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index][0] == normalized:
                removed = self._stack[index:]
                self._suppression_depth -= sum(int(suppress) for _, suppress in removed)
                del self._stack[index:]
                break

    def handle_data(self, data: str) -> None:
        if self._suppression_depth == 0:
            self.parts.append(data)


class _NonContentRangeParser(HTMLParser):
    """Locate exact source ranges consumers must never index as prose."""

    def __init__(self, text: str) -> None:
        super().__init__(convert_charrefs=False)
        self._text = text
        self._line_starts = [0]
        self._line_starts.extend(match.end() for match in re.finditer("\n", text))
        self._stack: list[tuple[str, bool]] = []
        self._active_exclusion_start: int | None = None
        self.ranges: list[tuple[int, int]] = []

    def _index(self) -> int:
        line, offset = self.getpos()
        return self._line_starts[line - 1] + offset

    def _tag_end(self, start: int) -> int:
        close = self._text.find(">", start)
        return len(self._text) if close < 0 else close + 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.casefold()
        excluded = normalized in _NON_CONTENT_MARKUP or _attributes_hide_content(attrs)
        start = self._index()
        if normalized in _HTML_VOID_TAGS:
            if excluded:
                self.ranges.append((start, self._tag_end(start)))
            return
        starts_exclusion = excluded and self._active_exclusion_start is None
        if starts_exclusion:
            self._active_exclusion_start = start
        self._stack.append((normalized, starts_exclusion))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() in _NON_CONTENT_MARKUP or _attributes_hide_content(attrs):
            start = self._index()
            self.ranges.append((start, self._tag_end(start)))

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        for index in range(len(self._stack) - 1, -1, -1):
            open_tag, _ = self._stack[index]
            if open_tag == normalized:
                removed = self._stack[index:]
                starts = [starts for _, starts in removed]
                del self._stack[index:]
                if any(starts) and self._active_exclusion_start is not None:
                    self.ranges.append((self._active_exclusion_start, self._tag_end(self._index())))
                    self._active_exclusion_start = None
                break

    def handle_comment(self, data: str) -> None:
        del data
        start = self._index()
        close = self._text.find("-->", start)
        self.ranges.append((start, len(self._text) if close < 0 else close + 3))

    def close(self) -> None:
        super().close()
        if self._active_exclusion_start is not None:
            self.ranges.append((self._active_exclusion_start, len(self._text)))
        self._stack.clear()
        self._active_exclusion_start = None


def _non_content_markup_ranges(text: str) -> tuple[tuple[int, int], ...]:
    parser = _NonContentRangeParser(text)
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return ()
    merged: list[tuple[int, int]] = []
    for start, end in sorted(parser.ranges):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return tuple(merged)


class _NamedElementRangeParser(HTMLParser):
    """Locate exact source ranges for a small set of semantic HTML elements."""

    def __init__(self, text: str, names: frozenset[str]) -> None:
        super().__init__(convert_charrefs=False)
        self._text = text
        self._names = names
        self._line_starts = [0]
        self._line_starts.extend(match.end() for match in re.finditer("\n", text))
        self._stack: list[tuple[str, int | None]] = []
        self.ranges: dict[str, list[tuple[int, int]]] = {name: [] for name in names}

    def _index(self) -> int:
        line, offset = self.getpos()
        return self._line_starts[line - 1] + offset

    def _tag_end(self, start: int) -> int:
        close = self._text.find(">", start)
        return len(self._text) if close < 0 else close + 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        normalized = tag.casefold()
        self._stack.append((normalized, self._index() if normalized in self._names else None))

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        for index in range(len(self._stack) - 1, -1, -1):
            open_tag, _ = self._stack[index]
            if open_tag == normalized:
                removed = self._stack[index:]
                del self._stack[index:]
                for removed_tag, start in removed:
                    if start is not None:
                        self.ranges[removed_tag].append((start, self._tag_end(self._index())))
                break


def _semantic_html_content_ranges(text: str) -> tuple[tuple[int, int], ...] | None:
    parser = _NamedElementRangeParser(text, frozenset({"main", "title"}))
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return None
    mains = parser.ranges["main"]
    if len(mains) != 1:
        return None
    merged: list[tuple[int, int]] = []
    for start, end in sorted([*parser.ranges["title"], mains[0]]):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return tuple(merged)


def _intersect_ranges(
    start: int,
    end: int,
    allowed: Sequence[tuple[int, int]],
) -> Iterator[tuple[int, int]]:
    for allowed_start, allowed_end in allowed:
        overlap_start = max(start, allowed_start)
        overlap_end = min(end, allowed_end)
        if overlap_start < overlap_end:
            yield overlap_start, overlap_end


def _subtract_ranges(
    start: int,
    end: int,
    exclusions: Sequence[tuple[int, int]],
) -> Iterator[tuple[int, int]]:
    cursor = start
    for excluded_start, excluded_end in exclusions:
        if excluded_end <= cursor:
            continue
        if excluded_start >= end:
            break
        if excluded_start > cursor:
            yield cursor, min(excluded_start, end)
        cursor = max(cursor, excluded_end)
        if cursor >= end:
            break
    if cursor < end:
        yield cursor, end


def _visible_markup_text(value: str) -> str:
    parser = _TextCollector()
    parser.feed(value)
    parser.close()
    return " ".join("".join(parser.parts).split())


def _indexable_markup_text(value: str) -> str:
    parser = _IndexableTextCollector()
    parser.feed(value)
    parser.close()
    return " ".join("".join(parser.parts).split())


def _markup_kind(tag: str) -> str:
    if tag in _MARKUP_HEADINGS:
        return "heading"
    if tag == "li":
        return "list-item"
    if tag in {"table", "tbody"}:
        return "table"
    if tag in {"tr", "td", "th"}:
        return "table-row"
    if tag in {"section", "subsection", "article", "div"}:
        return "section"
    return "paragraph"


def _markup_drafts(source_field: str, text: str) -> list[_RegionDraft] | None:
    """Return gap-free drafts over native markup, or ``None`` when there is none."""
    if not text.lstrip().startswith("<") or ">" not in text:
        return None
    parser = _MarkupBoundaryParser(text)
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return None
    events = [
        event
        for index, event in enumerate(parser.events)
        if index == 0 or event.start_char != parser.events[index - 1].start_char
    ]
    if not events:
        return None
    prefix_count = int(events[0].start_char > 0)
    result: list[_RegionDraft] = []
    if prefix_count:
        # The bytes before the first structural tag — an XML declaration, a
        # doctype, a comment. Syntax, but never silently dropped.
        result.append(
            _RegionDraft(kind="markup-prolog", source_field=source_field, start_char=0, end_char=events[0].start_char)
        )
    heading_path: tuple[str, ...] = ()
    for index, event in enumerate(events):
        end = events[index + 1].start_char if index + 1 < len(events) else len(text)
        if end <= event.start_char:
            continue
        kind = _markup_kind(event.tag)
        value = text[event.start_char : end]
        if kind == "heading":
            heading = _visible_markup_text(value)
            if heading:
                heading_path = (heading,)
        result.append(
            _RegionDraft(
                kind=kind,
                source_field=source_field,
                start_char=event.start_char,
                end_char=end,
                parent_ordinal=(prefix_count + event.parent_event if event.parent_event is not None else None),
                heading_path=(() if kind == "heading" else heading_path),
            )
        )
    return result


def native_structural_passage_spans(
    source_field: str,
    text: str,
    *,
    media_type: str | None = None,
) -> tuple[tuple[int, int], ...]:
    """Return non-overlapping, visible structural spans over exact native text.

    The release pipeline uses the same native markup boundaries as the source
    artifact pipeline.  Syntax-only regions remain accounted for by release
    coverage but do not become passages consumers might quote or rank.
    """

    if not source_field or not isinstance(text, str):
        raise SourceError("native structural spans require a source field and Unicode text")
    if media_type is not None and not isinstance(media_type, str):
        raise SourceError("native structural spans media type must be a string")
    drafts = _markup_drafts(source_field, text)
    if drafts is None:
        if text.lstrip().startswith("<") and ">" in text:
            drafts = [
                _RegionDraft(
                    kind="markup-document",
                    source_field=source_field,
                    start_char=0,
                    end_char=len(text),
                )
            ]
        else:
            drafts = _prose_drafts(source_field, text)
            return tuple(
                (draft.start_char, draft.end_char)
                for draft in drafts
                if text[draft.start_char : draft.end_char].strip()
            )
    exclusions = _non_content_markup_ranges(text)
    semantic_ranges = _semantic_html_content_ranges(text) if media_type == "text/html" else None
    output: list[tuple[int, int]] = []
    for draft in drafts:
        candidate_ranges: Sequence[tuple[int, int]] = ((draft.start_char, draft.end_char),)
        if semantic_ranges is not None:
            candidate_ranges = tuple(_intersect_ranges(draft.start_char, draft.end_char, semantic_ranges))
        for candidate_start, candidate_end in candidate_ranges:
            for start, end in _subtract_ranges(
                candidate_start,
                candidate_end,
                exclusions,
            ):
                visible = _indexable_markup_text(text[start:end])
                if visible and any(character.isalnum() for character in visible):
                    output.append((start, end))
    return tuple(output)


# --------------------------------------------------------------------------
# the coverage floor: a parse must account for its own document
# --------------------------------------------------------------------------
#
# Docling's HTML backend returned 502 visible characters from a 257,998-byte
# Federal Register rule and reported success with an empty error list. Coverage
# did not catch it and could not: regions are gap-free over whatever field they
# are given, so a field the parser built from 0.27% of a document is still 100%
# covered. The number that catches it compares what came out against what the
# source independently says is there, which is what this section measures.

MARKUP_UNIT = "markup-visible"
"""Extracted visible characters over the source's own visible characters.

Both sides are text a reader would see, computed by the same collector, so the
ratio means "how much of the document survived".
"""

DENSITY_UNIT = "parsed-per-source-byte"
"""Extracted characters per source byte, for formats that carry no source text.

A PDF or an Office file has no text to compare against — the parser is the only
reader — so this is a density rather than a fraction and it may never be
compared against :data:`MARKUP_UNIT`.
"""


@dataclass(frozen=True)
class RetentionFloor:
    """One declared floor, and the measurement that placed it there."""

    value: float
    unit: str
    observed_minimum: float
    population: str

    def __post_init__(self) -> None:
        if not 0.0 < self.value < 1.0:
            raise ValueError("a floor outside (0, 1) either gates nothing or refuses everything")
        if self.unit not in {MARKUP_UNIT, DENSITY_UNIT}:
            raise ValueError("a floor has to name the unit it is measured in")
        if self.observed_minimum <= self.value:
            raise ValueError("a floor with no margin under the observed minimum is a future false refusal")

    @property
    def margin(self) -> float:
        """How far the lowest legitimate document sits above this floor."""
        return self.observed_minimum / self.value


#: Per parser, per format. One global number would be either useless or wrong:
#: a table-heavy filing legitimately carries less prose than a rule, and a text
#: density is not the same measurement as a visible-text fraction. Every value
#: is derived from the distribution recorded in
#: ``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``, reproducible with
#: ``tools/measure_extraction_retention.py``, and none is chosen.
#:
#: HTML and XML get different floors because their distributions are different
#: shapes, not as a matter of taste: HTML carries navigation chrome that a parse
#: legitimately excludes and spreads from 0.9453 to 0.9987, while XML carries
#: none and never fell below 0.9930 across 993 Federal Register documents, 7
#: U.S. Code titles, 4 eCFR titles and 3 bills.
#:
#: There is deliberately **no floor for Docling on Office formats**. No DOCX,
#: PPTX or XLSX population exists in this tree to measure, and declaring a floor
#: for a population nobody measured would be exactly the taste this gate is
#: meant to replace. Until one is measured, that parser has no floor and
#: :func:`check_extraction_retention` refuses it — which is the intended
#: fail-closed shape, not an oversight.
RETENTION_FLOORS: dict[str, RetentionFloor] = {
    "native:text/html": RetentionFloor(
        value=0.75,
        unit=MARKUP_UNIT,
        observed_minimum=0.9453,
        population="993 Federal Register HTML + 4 GAO HTML + 4 segmentation-cache FR HTML; min is GAO",
    ),
    "native:application/xml": RetentionFloor(
        value=0.85,
        unit=MARKUP_UNIT,
        observed_minimum=0.9930,
        population="993 Federal Register full-text XML + 7 USLM titles + 4 eCFR + 3 bills; min is bill XML",
    ),
    "pypdf:application/pdf": RetentionFloor(
        value=0.005,
        unit=DENSITY_UNIT,
        observed_minimum=0.015584,
        population="18 segmentation-cache PDFs — 9 court opinions, 4 CRS, 4 regulations, 1 bill",
    ),
}


def retention_format_for(media_type: str) -> str:
    """Collapse a media type onto the format key its floor is declared under.

    ``text/xml`` and ``application/xml`` are the same population and must not
    end up with two separately-derived floors, or the same document would be
    gated differently depending on which header the publisher sent.
    """
    normalized = (media_type or "").split(";")[0].strip().casefold()
    if normalized in {"text/xml", "application/xml"} or normalized.endswith("+xml"):
        return "application/xml"
    return normalized


def retention_floor_for(parser_id: str, source_format: str) -> RetentionFloor | None:
    """The declared floor for one parser on one format, or ``None`` if undeclared.

    Undeclared is deliberately not "inherit a default". A new extractor has to
    state the population its floor came from before it may run, which is the
    same fail-closed shape as the adapter's ``format_not_implemented``.
    """
    return RETENTION_FLOORS.get(f"{parser_id}:{source_format}")


def reference_visible_text(text: str) -> str:
    """The source's own visible text, computed without asking the extractor.

    This is the denominator, and it has to be independent of whatever produced
    the numerator or the ratio would only say that a tool agrees with itself.
    """
    if not isinstance(text, str):
        raise SourceError("a visible-text reference needs Unicode text")
    return _indexable_markup_text(text)


def visible_retention(source_field: str, text: str, *, media_type: str | None = None) -> float | None:
    """What fraction of the source's visible text the native passages carry.

    ``None`` when the source has no visible text at all — an empty document is
    not a retention failure, it is an unmeasurable one, and
    :func:`check_extraction_retention` treats the two differently.
    """
    reference = reference_visible_text(text)
    if not reference:
        return None
    spans = native_structural_passage_spans(source_field, text, media_type=media_type)
    extracted = sum(len(_indexable_markup_text(text[start:end])) for start, end in spans)
    # Spans are non-overlapping, so the sum cannot legitimately exceed the
    # reference; per-span whitespace normalization can still round it fractionally
    # over, and a ratio above 1.0 would misreport as a healthier parse than exists.
    return min(1.0, extracted / len(reference))


def _json_array_ranges(text: str) -> list[tuple[int, int]] | None:
    """Return gap-free ranges, one per top-level JSON array item."""
    decoder = json.JSONDecoder()
    position = 0
    while position < len(text) and text[position].isspace():
        position += 1
    if position >= len(text) or text[position] != "[":
        return None
    array_start = position
    position += 1
    value_ends: list[int] = []
    while True:
        while position < len(text) and (text[position].isspace() or text[position] == ","):
            position += 1
        if position >= len(text):
            return None
        if text[position] == "]":
            break
        try:
            _, end = decoder.raw_decode(text, position)
        except json.JSONDecodeError:
            return None
        value_ends.append(end)
        position = end
    tail = position + 1
    while tail < len(text) and text[tail].isspace():
        tail += 1
    if tail != len(text):
        return None
    if not value_ends:
        return [(0, len(text))]
    ranges: list[tuple[int, int]] = []
    start = 0 if array_start == 0 else array_start
    for end in value_ends:
        ranges.append((start, end))
        start = end
    ranges[-1] = (ranges[-1][0], len(text))
    return ranges


def _atomic_kind(source_field: str) -> str:
    column = source_field.rsplit(".", 1)[-1]
    if column in _ATOMIC_HEADING_COLUMNS:
        return "heading"
    return "structured-field" if source_field.endswith("_json") else "field"


def _atomic_drafts(source_field: str, text: str) -> list[_RegionDraft]:
    return [_RegionDraft(kind=_atomic_kind(source_field), source_field=source_field, start_char=0, end_char=len(text))]


def _prose_drafts(source_field: str, text: str) -> list[_RegionDraft]:
    result: list[_RegionDraft] = []
    heading_ordinal: int | None = None
    heading_path: tuple[str, ...] = ()
    for start, end in _paragraph_ranges(text):
        value = text[start:end]
        kind = "heading" if _looks_like_heading(value) else "paragraph"
        ordinal = len(result)
        if kind == "heading":
            heading_ordinal = ordinal
            heading_path = (value.strip(),)
        result.append(
            _RegionDraft(
                kind=kind,
                source_field=source_field,
                start_char=start,
                end_char=end,
                parent_ordinal=(None if kind == "heading" else heading_ordinal),
                heading_path=(() if kind == "heading" else heading_path),
            )
        )
    return result


def _structured_drafts(source_field: str, text: str) -> list[_RegionDraft] | None:
    """Return container-plus-children drafts, or ``None`` when there is no array."""
    if not source_field.endswith("_json"):
        return None
    ranges = _json_array_ranges(text)
    if ranges is None or len(ranges) == 1:
        return None
    result = [
        _RegionDraft(
            kind="structured-array",
            source_field=source_field,
            start_char=0,
            end_char=len(text),
            evidence_eligible=False,
        )
    ]
    result.extend(
        _RegionDraft(
            kind="structured-child", source_field=source_field, start_char=start, end_char=end, parent_ordinal=0
        )
        for start, end in ranges
    )
    return result


def _field_drafts(mode: SourceMode, source_field: str, text: str) -> tuple[str, list[_RegionDraft]]:
    """Dispatch one field, native-first, and report which branch produced it."""
    if mode == "structured-children":
        structured = _structured_drafts(source_field, text)
        if structured is not None:
            return DISPATCH_STRUCTURED_FIELDS, structured
        return DISPATCH_ATOMIC_FIELDS, _atomic_drafts(source_field, text)
    if mode == "atomic-record":
        return DISPATCH_ATOMIC_FIELDS, _atomic_drafts(source_field, text)
    column = source_field.rsplit(".", 1)[-1]
    if column not in BODY_COLUMNS:
        return DISPATCH_ATOMIC_FIELDS, _atomic_drafts(source_field, text)
    markup = _markup_drafts(source_field, text)
    if markup is not None:
        return DISPATCH_NATIVE_MARKUP, markup
    return DISPATCH_NATIVE_PROSE, _prose_drafts(source_field, text)


# --------------------------------------------------------------------------
# source records
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceAttachment:
    """One declared byte rendition attached to a source record.

    Only an Office rendition can be served in this slice. A PDF or image is
    recognized and refused by name; anything else is an unsupported format.
    """

    field_name: str
    file_name: str
    media_type: str | None
    content: bytes

    def __post_init__(self) -> None:
        if not str(self.field_name).strip():
            raise ValueError("an attachment names the artifact field it would become")
        if not str(self.file_name).strip():
            raise ValueError("an attachment names the file it carries")
        if not isinstance(self.content, bytes):
            raise ValueError("attachment content is exact bytes")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True)
class SourceRecord:
    """One immutable source row, its profile, and any declared rendition."""

    profile: SourceProfile
    row: Mapping[str, Any]
    extra_fields: tuple[tuple[str, Any], ...] = ()
    attachments: tuple[SourceAttachment, ...] = ()


@dataclass(frozen=True)
class SourcePolicy:
    """What the plan says about unknown identity, access, and parser failure."""

    policy_version: str = SOURCE_POLICY_VERSION
    quarantine_reasons: frozenset[str] = DEFAULT_QUARANTINE_REASONS
    parser_enabled: bool = True
    retention_exemptions: frozenset[str] = frozenset()
    """Subject ids allowed below their retention floor, each stated by name.

    A legitimate low-retention document — a form, a table-only filing — gets
    through by being named here, and the resulting :class:`RetentionCheck`
    records that it was exempted and which id matched. Empty by default: an
    exemption that nobody had to write down is indistinguishable from a gate
    that does not work.
    """

    def settles_as_rejected(self, reason: str) -> bool:
        return reason in self.quarantine_reasons


DEFAULT_SOURCE_POLICY = SourcePolicy()


@dataclass(frozen=True)
class RetentionCheck:
    """What the coverage floor measured, and what it decided.

    Recorded whether it passed or failed, so a receipt can state the retention
    of a healthy parse rather than only the retention of a refused one.
    """

    parser_id: str
    source_format: str
    subject_id: str
    unit: str
    measured: float | None
    floor: float | None
    exempt: bool = False
    unmeasurable: bool = False

    @property
    def passed(self) -> bool:
        if self.exempt:
            return True
        if self.measured is None or self.floor is None:
            return False
        return self.measured >= self.floor

    def as_dict(self) -> dict[str, Any]:
        return {
            "parser_id": self.parser_id,
            "source_format": self.source_format,
            "subject_id": self.subject_id,
            "unit": self.unit,
            "measured": self.measured,
            "floor": self.floor,
            "exempt": self.exempt,
            "unmeasurable": self.unmeasurable,
            "passed": self.passed,
        }


def check_extraction_retention(
    parser_id: str,
    source_format: str,
    measured: float | None,
    *,
    subject_id: str,
    policy: SourcePolicy = DEFAULT_SOURCE_POLICY,
) -> RetentionCheck:
    """Refuse a parse that did not account for enough of its own document.

    Sits at the extraction boundary rather than inside any one adapter, so it
    constrains every extractor this project runs now and every one it adds
    later. Fails closed in three separate ways, each of which was a way a bad
    parse could previously have been recorded as a good one:

    * below the declared floor,
    * no declared floor for this parser and format at all,
    * a source whose visible text could not be measured.

    Returns the measurement on success so a receipt can state it.
    """
    floor = retention_floor_for(parser_id, source_format)
    exempt = subject_id in policy.retention_exemptions
    check = RetentionCheck(
        parser_id=parser_id,
        source_format=source_format,
        subject_id=subject_id,
        unit=floor.unit if floor else "undeclared",
        measured=measured,
        floor=floor.value if floor else None,
        exempt=exempt,
        unmeasurable=measured is None,
    )
    if check.passed:
        return check
    if floor is None:
        raise SourceRetentionError(
            f"parser {parser_id!r} declares no retention floor for format {source_format!r}: "
            "an extractor states the population its floor came from before it may run"
        )
    if measured is None:
        raise SourceRetentionError(
            f"parser {parser_id!r} on format {source_format!r} produced no measurable visible text "
            f"for {subject_id!r}: retention could not be established against floor {floor.value}"
        )
    raise SourceRetentionError(
        f"parser {parser_id!r} on format {source_format!r} retained {measured:.4f} of {subject_id!r} "
        f"({floor.unit}), below the floor {floor.value}: a parse has to account for its own document"
    )


# --------------------------------------------------------------------------
# result records
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceRegion:
    """One canonical region of one exact source field.

    ``durability`` decides whether it projects into ``fragments.parquet``;
    ``evidence_eligible`` decides whether ``segments.py`` sees it. They are
    separate answers on purpose: an XML prolog is not durable data and is still
    part of the processing stream this migration must not move.
    """

    region_id: str
    kind: str
    ordinal: int
    parent_region_id: str | None
    heading_path: tuple[str, ...]
    source_field: str
    field_origin: str
    start_char: int
    end_char: int
    text: str
    text_sha256: str
    field_sha256: str
    artifact_sha256: str
    coordinates: CoordinateSystem
    evidence_grade: str
    content_layer: str
    coordinate_grade: str
    evidence_eligible: bool
    durability: str
    context_only: bool
    quarantine_reason: str | None = None
    """Why this one region is held back, or ``None`` when it is not.

    Background and invisible parser elements set this to
    :data:`EXCLUSION_HELD_CONTENT_LAYER`. Record-level quarantine conditions
    still settle the whole record. :func:`artifact_fragments` refuses to
    publish either kind of held region.
    """

    @property
    def char_count(self) -> int:
        return self.end_char - self.start_char


@dataclass(frozen=True)
class SourceFragment:
    """The durable projection of one meaningful source region."""

    fragment_id: str
    artifact_id: str
    artifact_sha256: str
    region_id: str
    subject_type: str
    subject_id: str
    profile_id: str
    source_table: str
    source_field: str
    field_origin: str
    field_sha256: str
    kind: str
    ordinal: int
    parent_region_id: str | None
    heading_path: tuple[str, ...]
    start_char: int
    end_char: int
    text: str
    text_sha256: str
    coordinates: CoordinateSystem
    evidence_grade: str
    content_layer: str
    coordinate_grade: str
    context_only: bool
    durability: str = DURABLE_MEANINGFUL

    @property
    def char_count(self) -> int:
        return self.end_char - self.start_char


@dataclass(frozen=True)
class SourceExclusion:
    """One source value or span left out of the regions, with its reason."""

    source_field: str
    reason: str
    start_char: int
    end_char: int
    raw_text_sha256: str

    @property
    def char_count(self) -> int:
        return self.end_char - self.start_char


@dataclass(frozen=True)
class SourceQuarantine:
    """One record this run refused to publish, and why."""

    scope: str
    identifier: str
    reason: str
    detail: str = ""


@dataclass(frozen=True)
class FieldCoverage:
    """What one field's regions cover, split by durability, with gaps named."""

    source_field: str
    field_origin: str
    field_chars: int
    covered_chars: int
    durable_chars: int
    syntax_chars: int
    container_chars: int
    uncovered_chars: int
    gaps: tuple[tuple[int, int], ...]
    region_count: int
    fragment_count: int


@dataclass(frozen=True)
class ParserProvenance:
    """What the contained parser was and what it produced. No provider types."""

    provider: str
    parser_id: str
    input_format: str
    source_sha256: str
    source_bytes: int
    evidence_grade: str
    offsets: CoordinateSystem
    element_count: int
    character_count: int
    gate: ProcessGateReceipt


@dataclass(frozen=True)
class ParserAttempt:
    """One immutable parser decision or process attempt, including refusals."""

    attempt_id: str
    terminal_state: str
    outcome_reason: str
    identifier: str
    profile_id: str
    source_table: str
    source_sha256: str
    attachment_field: str
    attachment_sha256: str
    attachment_bytes: int
    source_name_sha256: str
    media_type: str | None
    detected_input_format: str
    provider: str
    parser_id: str
    parser_policy_digest: str
    parser_policy_json: str
    source_policy_version: str
    parser_enabled: bool
    gate: ProcessGateReceipt | None
    call_json: str

    @property
    def gate_classification(self) -> str:
        return self.gate.classification if self.gate is not None else ""


@dataclass(frozen=True)
class SourceTableExclusion:
    """One present source table excluded by the caller's explicit selection."""

    source_table: str
    reason: str = EXCLUSION_INACTIVE_SOURCE_TABLE


@dataclass(frozen=True)
class SourceArtifact:
    """One exact, immutable source state and everything derived from it."""

    artifact_id: str
    content_sha256: str
    subject_type: str
    subject_id: str
    profile_id: str
    source_table: str
    allowed_schemes: tuple[str, ...]
    access: AccessScope
    coordinates: CoordinateSystem
    region_adapter_id: str
    source_policy_version: str
    raw_fields: Mapping[str, str]
    field_sha256: Mapping[str, str]
    field_origins: Mapping[str, str]
    field_dispatch: Mapping[str, str]
    context_fields: Mapping[str, str]
    dispatch: tuple[str, ...]
    parser_invoked: bool
    regions: tuple[SourceRegion, ...]
    exclusions: tuple[SourceExclusion, ...]
    coverage: tuple[FieldCoverage, ...]
    secret_rules: tuple[str, ...]
    parser: ParserProvenance | None = None

    @property
    def field_chars(self) -> int:
        return sum(len(text) for text in self.raw_fields.values())

    @property
    def uncovered_chars(self) -> int:
        return sum(one.uncovered_chars for one in self.coverage)


@dataclass(frozen=True)
class SourceOutcome:
    """One planned source item's durable state, in the runtime's vocabulary."""

    state: str
    artifact: SourceArtifact | None = None
    reason: str = ""
    error: str = ""
    quarantine: tuple[SourceQuarantine, ...] = ()
    identifier: str = ""
    profile_id: str = ""
    parser_attempts: tuple[ParserAttempt, ...] = ()
    source_table_exclusions: tuple[SourceTableExclusion, ...] = ()

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty", "rejected", "failed"}:
            raise SourceError(f"unknown source state {self.state!r}")


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------


def _normalize_identifier(value: object) -> str:
    return " ".join(str(value or "").split())


def _source_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return canonical_json(value)
    return str(value)


def _subject_id(profile: SourceProfile, row: Mapping[str, Any]) -> str | None:
    values = [_normalize_identifier(row.get(column)) for column in profile.id_columns]
    if any(not value for value in values):
        return None
    if len(values) == 1:
        value = values[0]
        if profile.source_table == "dockets":
            # Never invented: a syntactically impossible docket id has no
            # canonical form, so the record is quarantined rather than guessed.
            return normalize_regsgov_identifier(value)
        return value
    return canonical_json(dict(zip(profile.id_columns, values, strict=True)))


def _artifact_title(values: Sequence[tuple[str, object]]) -> dict[str, str]:
    preferred = (".title", ".name", ".heading", ".legal_business_name", ".case_name_full", ".case_name")
    for suffix in preferred:
        for source_field, raw_value in values:
            value = _source_text(raw_value)
            if source_field.endswith(suffix) and value and value.strip():
                return {"artifact_title": value}
    return {}


def _content_digest(
    profile: SourceProfile,
    subject_id: str,
    values: Sequence[tuple[str, object]],
    attachments: Sequence[SourceAttachment],
) -> str:
    """The exact-source-state digest, with the predecessor's recipe preserved.

    Attachments append entries to the same ``source_values`` list, so a record
    without one hashes byte-identically to what the predecessor produced.
    """
    source_values = [
        {"source_field": source_field, "value": _source_text(raw_value)} for source_field, raw_value in values
    ]
    source_values.extend(
        {"source_field": attachment.field_name, "value": f"sha256:{attachment.sha256}"} for attachment in attachments
    )
    return text_digest(
        canonical_json(
            {
                "profile": profile.profile_id,
                "source_table": profile.source_table,
                "subject_type": profile.subject_type,
                "subject_id": subject_id,
                "source_values": source_values,
            }
        )
    )


def region_id_for(
    *,
    region_adapter_id: str,
    subject_type: str,
    subject_id: str,
    content_sha256: str,
    source_field: str,
    start_char: int,
    end_char: int,
    kind: str,
    ordinal: int,
) -> str:
    """Build one region id from the predecessor's exact identity recipe."""
    identity = canonical_json(
        {
            "adapter": region_adapter_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "artifact_digest": content_sha256,
            "source_field": source_field,
            "start_char": start_char,
            "end_char": end_char,
            "kind": kind,
            "ordinal": ordinal,
        }
    )
    return stable_id(REGION_ID_PREFIX, identity, length=24)


def parsed_region_id_for(
    *,
    region_adapter_id: str,
    parser_id: str,
    parsed_field_sha256: str,
    subject_type: str,
    subject_id: str,
    content_sha256: str,
    source_field: str,
    start_char: int,
    end_char: int,
    kind: str,
    ordinal: int,
) -> str:
    """Build a parser-derived region id without changing the native recipe."""
    identity = canonical_json(
        {
            "adapter": region_adapter_id,
            "parser_id": parser_id,
            "parsed_field_sha256": parsed_field_sha256,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "artifact_digest": content_sha256,
            "source_field": source_field,
            "start_char": start_char,
            "end_char": end_char,
            "kind": kind,
            "ordinal": ordinal,
        }
    )
    return stable_id(REGION_ID_PREFIX, identity, length=24)


def _durability(kind: str) -> str:
    if kind in SYNTAX_KINDS:
        return SYNTAX_REGION
    if kind in CONTAINER_KINDS:
        return CONTAINER_REGION
    return DURABLE_MEANINGFUL


# --------------------------------------------------------------------------
# invariant checkers
# --------------------------------------------------------------------------


def check_region_coordinates(artifact: SourceArtifact, *, regions: Sequence[SourceRegion] | None = None) -> None:
    """Prove every region's text is the exact half-open codepoint slice it claims."""
    for region in artifact.regions if regions is None else regions:
        field_text = artifact.raw_fields.get(region.source_field)
        if field_text is None:
            raise SourceError(f"region {region.region_id} names a field the artifact does not carry")
        if not 0 <= region.start_char <= region.end_char <= len(field_text):
            raise SourceError(f"region {region.region_id} leaves the exact source slice of {region.source_field}")
        if region.text != field_text[region.start_char : region.end_char]:
            raise SourceError(f"region {region.region_id} is not the exact source slice of {region.source_field}")
        if region.coordinates.unit != COORDINATE_UNIT or region.coordinates.interval != COORDINATE_INTERVAL:
            raise SourceError(f"region {region.region_id} declares a coordinate system this step does not use")
        expected_target = PARSED_TEXT_TARGET if region.field_origin == PARSER_DERIVED_FIELD else ARTIFACT_FIELD_TARGET
        if region.coordinates.target != expected_target:
            raise SourceError(
                f"region {region.region_id} targets {region.coordinates.target!r}, expected {expected_target!r}"
            )
        if region.content_layer not in PARSER_CONTENT_LAYERS:
            raise SourceError(f"region {region.region_id} carries an unknown content layer")
        valid_grades = (
            PARSER_COORDINATE_GRADES
            if region.field_origin == PARSER_DERIVED_FIELD
            else frozenset({SOURCE_COORDINATE_GRADE})
        )
        if region.coordinate_grade not in valid_grades:
            raise SourceError(f"region {region.region_id} carries an unknown coordinate grade")


def check_region_digests(artifact: SourceArtifact, *, regions: Sequence[SourceRegion] | None = None) -> None:
    """Prove each digest covers exactly what its name says it covers."""
    for region in artifact.regions if regions is None else regions:
        field_text = artifact.raw_fields[region.source_field]
        if region.artifact_sha256 != artifact.content_sha256:
            raise SourceError(f"region {region.region_id} names another artifact digest")
        if region.field_sha256 != hashlib.sha256(field_text.encode()).hexdigest():
            raise SourceError(f"region {region.region_id} carries a field digest that does not cover the field")
        if region.text_sha256 != hashlib.sha256(region.text.encode()).hexdigest():
            raise SourceError(f"region {region.region_id} carries a text digest that does not cover its text")


def _coverage_for(
    source_field: str,
    field_origin: str,
    field_text: str,
    regions: Sequence[SourceRegion],
) -> FieldCoverage:
    def union(selected: Iterable[SourceRegion]) -> tuple[int, list[tuple[int, int]]]:
        spans = sorted((one.start_char, one.end_char) for one in selected)
        merged: list[tuple[int, int]] = []
        for start, end in spans:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        return sum(end - start for start, end in merged), merged

    covered_chars, merged = union(regions)
    gaps: list[tuple[int, int]] = []
    cursor = 0
    for start, end in merged:
        if start > cursor:
            gaps.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < len(field_text):
        gaps.append((cursor, len(field_text)))
    return FieldCoverage(
        source_field=source_field,
        field_origin=field_origin,
        field_chars=len(field_text),
        covered_chars=covered_chars,
        durable_chars=union(
            [one for one in regions if one.durability == DURABLE_MEANINGFUL and one.quarantine_reason is None]
        )[0],
        syntax_chars=union([one for one in regions if one.durability == SYNTAX_REGION])[0],
        container_chars=union([one for one in regions if one.durability == CONTAINER_REGION])[0],
        uncovered_chars=sum(end - start for start, end in gaps),
        gaps=tuple(gaps),
        region_count=len(regions),
        fragment_count=len(
            [one for one in regions if one.durability == DURABLE_MEANINGFUL and one.quarantine_reason is None]
        ),
    )


# --------------------------------------------------------------------------
# building one artifact
# --------------------------------------------------------------------------


class OfficeParser(Protocol):
    """Exact Office bytes in, one contained parse result out. Nothing else."""

    def __call__(self, content: bytes, *, source_name: str, media_type: str | None) -> ContainedParseResult: ...


def _settle(
    reason: str,
    *,
    policy: SourcePolicy,
    identifier: str,
    profile_id: str,
    detail: str = "",
    scope: str = "record",
    parser_attempts: tuple[ParserAttempt, ...] = (),
) -> SourceOutcome:
    quarantine = (SourceQuarantine(scope=scope, identifier=identifier, reason=reason, detail=detail),)
    if policy.settles_as_rejected(reason):
        return SourceOutcome(
            state="rejected",
            reason=reason,
            quarantine=quarantine,
            identifier=identifier,
            profile_id=profile_id,
            parser_attempts=parser_attempts,
        )
    return SourceOutcome(
        state="failed",
        error=reason,
        quarantine=quarantine,
        identifier=identifier,
        profile_id=profile_id,
        parser_attempts=parser_attempts,
    )


def _attempt_terminal_state(result: ContainedParseResult) -> str:
    if result.parsed is not None:
        return "success"
    return {
        GATE_EXTRA_UNAVAILABLE: "unavailable",
        GATE_MALFORMED_RESULT: "malformed_result",
        GATE_TIMEOUT: "timeout",
        GATE_EXIT: "exit",
        GATE_SIGNAL: "signal",
        GATE_RESULT_OVER_LIMIT: "result_oversize",
        GATE_INPUT_OVER_LIMIT: "input_oversize",
    }.get(result.receipt.classification, "declared_failure")


def _parser_attempt(
    attachment: SourceAttachment,
    *,
    policy: SourcePolicy,
    identifier: str,
    profile: SourceProfile,
    source_sha256: str,
    detected_input_format: str,
    result: ContainedParseResult | None = None,
    terminal_state: str = "quarantine",
    outcome_reason: str = "",
) -> ParserAttempt:
    call = result.call if result is not None and isinstance(result.call, Mapping) else {}
    parser_id = str(
        (result.parsed.parser_id if result is not None and result.parsed is not None else "")
        or call.get("parser_id")
        or ""
    )
    raw_parser_policy = call.get("policy")
    parser_policy = dict(raw_parser_policy) if type(raw_parser_policy) is dict else {}
    parser_policy_digest = str(call.get("policy_digest") or "")
    gate = result.receipt if result is not None else None
    terminal = _attempt_terminal_state(result) if result is not None else terminal_state
    identity = canonical_json(
        {
            "profile_id": profile.profile_id,
            "identifier": identifier,
            "source_sha256": source_sha256,
            "attachment_field": attachment.field_name,
            "attachment_sha256": attachment.sha256,
            "detected_input_format": detected_input_format,
            "parser_id": parser_id,
            "terminal_state": terminal,
            "gate_classification": gate.classification if gate is not None else "",
        }
    )
    return ParserAttempt(
        attempt_id=stable_id("parser_attempt", identity, length=24),
        terminal_state=terminal,
        outcome_reason=outcome_reason,
        identifier=identifier,
        profile_id=profile.profile_id,
        source_table=profile.source_table,
        source_sha256=source_sha256,
        attachment_field=attachment.field_name,
        attachment_sha256=attachment.sha256,
        attachment_bytes=len(attachment.content),
        source_name_sha256=hashlib.sha256(attachment.file_name.encode("utf-8", "surrogatepass")).hexdigest(),
        media_type=attachment.media_type,
        detected_input_format=detected_input_format,
        provider=PARSER_PROVIDER,
        parser_id=parser_id,
        parser_policy_digest=parser_policy_digest,
        parser_policy_json=canonical_json(redact(parser_policy)),
        source_policy_version=policy.policy_version,
        parser_enabled=policy.parser_enabled,
        gate=gate,
        call_json=canonical_json(redact(dict(call))),
    )


def _attachment_field(
    attachment: SourceAttachment,
    *,
    policy: SourcePolicy,
    parser: OfficeParser,
    identifier: str,
    profile: SourceProfile,
    source_sha256: str,
) -> tuple[ParsedOfficeText | None, ParserProvenance | None, str, str, ParserAttempt]:
    """Serve one attachment, or say exactly why it was not served.

    Returns ``(parsed, provenance, reason, detail, attempt)``. ``parsed is None`` means
    the caller settles the record on ``reason``.
    """
    input_format, _ = _detect_attachment_format(attachment)
    if not policy.parser_enabled:
        attempt = _parser_attempt(
            attachment,
            policy=policy,
            identifier=identifier,
            profile=profile,
            source_sha256=source_sha256,
            detected_input_format=input_format,
            outcome_reason=REASON_PARSER_DISABLED,
        )
        return None, None, REASON_PARSER_DISABLED, "", attempt
    if input_format in DEFERRED_FORMATS:
        attempt = _parser_attempt(
            attachment,
            policy=policy,
            identifier=identifier,
            profile=profile,
            source_sha256=source_sha256,
            detected_input_format=input_format,
            outcome_reason=REASON_FORMAT_NOT_IMPLEMENTED,
        )
        return None, None, REASON_FORMAT_NOT_IMPLEMENTED, input_format, attempt
    if input_format not in SUPPORTED_FORMATS:
        attempt = _parser_attempt(
            attachment,
            policy=policy,
            identifier=identifier,
            profile=profile,
            source_sha256=source_sha256,
            detected_input_format=input_format,
            outcome_reason=REASON_UNSUPPORTED_ATTACHMENT,
        )
        return None, None, REASON_UNSUPPORTED_ATTACHMENT, input_format, attempt
    result = parser(attachment.content, source_name=attachment.file_name, media_type=attachment.media_type)
    attempt = _parser_attempt(
        attachment,
        policy=policy,
        identifier=identifier,
        profile=profile,
        source_sha256=source_sha256,
        detected_input_format=input_format,
        result=result,
        outcome_reason=(
            ""
            if result.parsed is not None
            else (
                REASON_PARSER_UNAVAILABLE
                if result.receipt.classification == GATE_EXTRA_UNAVAILABLE
                else REASON_PARSER_FAILED
            )
        ),
    )
    if result.parsed is None:
        # A parser nobody installed is a different finding from a parse that
        # ran and failed, and the two settle under their own names.
        reason = (
            REASON_PARSER_UNAVAILABLE
            if result.receipt.classification == GATE_EXTRA_UNAVAILABLE
            else REASON_PARSER_FAILED
        )
        return None, None, reason, result.receipt.classification, attempt
    provenance = ParserProvenance(
        provider=PARSER_PROVIDER,
        parser_id=result.parsed.parser_id,
        input_format=result.parsed.input_format,
        source_sha256=result.parsed.source_sha256,
        source_bytes=result.parsed.source_bytes,
        evidence_grade=result.parsed.evidence_grade,
        offsets=result.parsed.offsets,
        element_count=len(result.parsed.elements),
        character_count=len(result.parsed.text),
        gate=result.receipt,
    )
    return result.parsed, provenance, "", "", attempt


def _detect_attachment_format(attachment: SourceAttachment) -> tuple[str, str]:
    try:
        return detect_input_format(attachment.file_name, attachment.media_type)
    except DoclingParseError:
        return FORMAT_UNKNOWN, ""


def _parser_drafts(source_field: str, parsed: ParsedOfficeText) -> list[_RegionDraft]:
    """One draft per usable parser element, extended so the parsed text is covered."""
    usable = [element for element in parsed.elements if element.text_usable and element.text]
    drafts: list[_RegionDraft] = []
    heading_ordinal: int | None = None
    for index, element in enumerate(usable):
        if element.content_layer not in PARSER_CONTENT_LAYERS:
            raise SourceError(f"parser element {element.ordinal} carries an unknown content layer")
        if element.coordinate_grade not in PARSER_COORDINATE_GRADES:
            raise SourceError(f"parser element {element.ordinal} carries an unknown coordinate grade")
        end = usable[index + 1].start_char if index + 1 < len(usable) else len(parsed.text)
        is_heading = element.kind in HEADING_KINDS
        context_layer = element.content_layer in CONTEXT_CONTENT_LAYERS
        held_layer = element.content_layer in HELD_CONTENT_LAYERS
        ordinal = len(drafts)
        if is_heading:
            heading_ordinal = ordinal
        drafts.append(
            _RegionDraft(
                kind=element.kind,
                source_field=source_field,
                start_char=element.start_char,
                end_char=end,
                parent_ordinal=None if is_heading else heading_ordinal,
                heading_path=() if is_heading else element.heading_path,
                evidence_eligible=element.content_layer == BODY_CONTENT_LAYER,
                content_layer=element.content_layer,
                coordinate_grade=element.coordinate_grade,
                context_only=is_heading or context_layer,
                quarantine_reason=EXCLUSION_HELD_CONTENT_LAYER if held_layer else None,
            )
        )
    return drafts


def build_source_artifact(
    record: SourceRecord,
    *,
    policy: SourcePolicy = DEFAULT_SOURCE_POLICY,
    parser: OfficeParser | None = None,
) -> SourceOutcome:
    """Turn one immutable source record into one exact Artifact, or say why not."""
    profile = record.profile
    # What the row *said* its identity was, for a quarantine record to name. It
    # is never used as a subject id: only ``_subject_id`` decides that, and it
    # returns ``None`` rather than repair an identifier it cannot normalize.
    identifier = "|".join(_normalize_identifier(record.row.get(column)) for column in profile.id_columns)
    subject_id = _subject_id(profile, record.row)
    if subject_id is None:
        return _settle(
            REASON_UNKNOWN_IDENTITY,
            policy=policy,
            identifier=identifier,
            profile_id=profile.profile_id,
            detail=", ".join(profile.id_columns),
        )
    if not profile.access.declared:
        return _settle(
            REASON_UNKNOWN_ACCESS,
            policy=policy,
            identifier=subject_id,
            profile_id=profile.profile_id,
            detail=profile.profile_id,
        )

    values: list[tuple[str, Any]] = [
        (f"{profile.source_table}.{column}", record.row.get(column)) for column in profile.text_columns
    ]
    values.extend(record.extra_fields)
    content_sha256 = _content_digest(profile, subject_id, values, record.attachments)

    exclusions: list[SourceExclusion] = []
    raw_fields: dict[str, str] = {}
    field_origins: dict[str, str] = {}
    for source_field, raw_value in values:
        text = _source_text(raw_value)
        raw_digest = hashlib.sha256((text or "").encode()).hexdigest()
        if text is None:
            exclusions.append(SourceExclusion(source_field, EXCLUSION_NULL, 0, 0, raw_digest))
            continue
        if not text.strip():
            exclusions.append(SourceExclusion(source_field, EXCLUSION_BLANK, 0, len(text), raw_digest))
            continue
        raw_fields[source_field] = text
        field_origins[source_field] = SOURCE_NATIVE_FIELD

    provenance: ParserProvenance | None = None
    parser_fields: dict[str, ParsedOfficeText] = {}
    parser_attempts: list[ParserAttempt] = []
    if not raw_fields and len(record.attachments) > 1:
        parser_attempts.extend(
            _parser_attempt(
                attachment,
                policy=policy,
                identifier=subject_id,
                profile=profile,
                source_sha256=content_sha256,
                detected_input_format=_detect_attachment_format(attachment)[0],
                outcome_reason=REASON_MULTIPLE_RENDITIONS,
            )
            for attachment in record.attachments
        )
        return _settle(
            REASON_MULTIPLE_RENDITIONS,
            policy=policy,
            identifier=subject_id,
            profile_id=profile.profile_id,
            detail=str(len(record.attachments)),
            scope="attachment",
            parser_attempts=tuple(parser_attempts),
        )
    for attachment in record.attachments:
        if raw_fields:
            # Native structure wins outright. The rendition is recorded as a
            # deliberate exclusion, so "the parser did not run" is a stated fact.
            exclusions.append(
                SourceExclusion(attachment.field_name, EXCLUSION_NATIVE_PREFERRED, 0, 0, attachment.sha256)
            )
            continue
        parsed, provenance, reason, detail, attempt = _attachment_field(
            attachment,
            policy=policy,
            parser=parser if parser is not None else contained_office_parser(),
            identifier=subject_id,
            profile=profile,
            source_sha256=content_sha256,
        )
        parser_attempts.append(attempt)
        if parsed is None:
            return _settle(
                reason,
                policy=policy,
                identifier=subject_id,
                profile_id=profile.profile_id,
                detail=detail,
                scope="attachment",
                parser_attempts=tuple(parser_attempts),
            )
        raw_fields[attachment.field_name] = parsed.text
        field_origins[attachment.field_name] = PARSER_DERIVED_FIELD
        parser_fields[attachment.field_name] = parsed
        # An element the parser could not turn into text is recorded rather than
        # dropped, so the parsed field's accounting stays complete both ways.
        exclusions.extend(
            SourceExclusion(
                attachment.field_name,
                EXCLUSION_PARSER_OMISSION,
                element.start_char,
                element.end_char,
                hashlib.sha256(element.text.encode()).hexdigest(),
            )
            for element in parsed.elements
            if not (element.text_usable and element.text)
        )

    drafts: list[_RegionDraft] = []
    field_dispatch: dict[str, str] = {}
    for source_field, text in raw_fields.items():
        if source_field in parser_fields:
            branch = DISPATCH_CONTAINED_PARSER
            produced = _parser_drafts(source_field, parser_fields[source_field])
        else:
            branch, produced = _field_drafts(profile.mode, source_field, text)
        field_dispatch[source_field] = branch
        base = len(drafts)
        drafts.extend(
            replace(draft, parent_ordinal=None if draft.parent_ordinal is None else base + draft.parent_ordinal)
            for draft in produced
        )

    field_digests = {
        source_field: hashlib.sha256(text.encode()).hexdigest() for source_field, text in raw_fields.items()
    }
    region_ids = [
        (
            parsed_region_id_for(
                region_adapter_id=profile.region_adapter_id,
                parser_id=parser_fields[draft.source_field].parser_id,
                parsed_field_sha256=field_digests[draft.source_field],
                subject_type=profile.subject_type,
                subject_id=subject_id,
                content_sha256=content_sha256,
                source_field=draft.source_field,
                start_char=draft.start_char,
                end_char=draft.end_char,
                kind=draft.kind,
                ordinal=ordinal,
            )
            if draft.source_field in parser_fields
            else region_id_for(
                region_adapter_id=profile.region_adapter_id,
                subject_type=profile.subject_type,
                subject_id=subject_id,
                content_sha256=content_sha256,
                source_field=draft.source_field,
                start_char=draft.start_char,
                end_char=draft.end_char,
                kind=draft.kind,
                ordinal=ordinal,
            )
        )
        for ordinal, draft in enumerate(drafts)
    ]
    regions = tuple(
        SourceRegion(
            region_id=region_ids[ordinal],
            kind=draft.kind,
            ordinal=ordinal,
            parent_region_id=(region_ids[draft.parent_ordinal] if draft.parent_ordinal is not None else None),
            heading_path=draft.heading_path,
            source_field=draft.source_field,
            field_origin=field_origins[draft.source_field],
            start_char=draft.start_char,
            end_char=draft.end_char,
            text=raw_fields[draft.source_field][draft.start_char : draft.end_char],
            text_sha256=hashlib.sha256(
                raw_fields[draft.source_field][draft.start_char : draft.end_char].encode()
            ).hexdigest(),
            field_sha256=field_digests[draft.source_field],
            artifact_sha256=content_sha256,
            coordinates=(
                PARSED_TEXT_COORDINATES
                if field_origins[draft.source_field] == PARSER_DERIVED_FIELD
                else SOURCE_FIELD_COORDINATES
            ),
            evidence_grade=(
                PARSER_DERIVED_EVIDENCE
                if field_origins[draft.source_field] == PARSER_DERIVED_FIELD
                else SOURCE_EXACT_EVIDENCE
            ),
            content_layer=draft.content_layer,
            coordinate_grade=draft.coordinate_grade,
            evidence_eligible=draft.evidence_eligible,
            durability=_durability(draft.kind),
            context_only=draft.context_only or draft.kind in HEADING_KINDS,
            quarantine_reason=draft.quarantine_reason,
        )
        for ordinal, draft in enumerate(drafts)
    )

    coverage = tuple(
        _coverage_for(
            source_field,
            field_origins[source_field],
            text,
            [region for region in regions if region.source_field == source_field],
        )
        for source_field, text in raw_fields.items()
    )
    exclusions.extend(
        SourceExclusion(
            one.source_field,
            EXCLUSION_COVERAGE_GAP,
            start,
            end,
            hashlib.sha256(raw_fields[one.source_field][start:end].encode()).hexdigest(),
        )
        for one in coverage
        for start, end in one.gaps
    )
    exclusions.extend(
        SourceExclusion(
            region.source_field,
            EXCLUSION_HELD_CONTENT_LAYER,
            region.start_char,
            region.end_char,
            region.text_sha256,
        )
        for region in regions
        if region.quarantine_reason == EXCLUSION_HELD_CONTENT_LAYER
    )

    scanned = sorted({rule for text in raw_fields.values() for rule in scan_text_for_secrets(text)})
    artifact = SourceArtifact(
        artifact_id=stable_id(ARTIFACT_ID_PREFIX, content_sha256, length=24),
        content_sha256=content_sha256,
        subject_type=profile.subject_type,
        subject_id=subject_id,
        profile_id=profile.profile_id,
        source_table=profile.source_table,
        allowed_schemes=profile.allowed_schemes,
        access=profile.access,
        coordinates=SOURCE_FIELD_COORDINATES,
        region_adapter_id=profile.region_adapter_id,
        source_policy_version=policy.policy_version,
        raw_fields=dict(raw_fields),
        field_sha256=field_digests,
        field_origins=dict(field_origins),
        field_dispatch=field_dispatch,
        context_fields=_artifact_title(values),
        dispatch=tuple(branch for branch in DISPATCH_PRIORITY if branch in set(field_dispatch.values())),
        parser_invoked=bool(parser_fields),
        regions=regions,
        exclusions=tuple(exclusions),
        coverage=coverage,
        secret_rules=tuple(scanned),
        parser=provenance,
    )
    check_region_coordinates(artifact)
    check_region_digests(artifact)
    state = "completed" if artifact.regions else "completed_empty"
    return SourceOutcome(
        state=state,
        artifact=artifact,
        identifier=subject_id,
        profile_id=profile.profile_id,
        parser_attempts=tuple(parser_attempts),
    )


def processing_regions(artifact: SourceArtifact) -> tuple[SourceRegion, ...]:
    """The exact stream ``segments.py`` consumes, in the predecessor's order.

    Evidence-eligible, non-empty, in region order. ``markup-prolog`` is part of
    it, unchanged by this migration: dropping it here would move every segment
    boundary in the frozen data. Removing it is a recorded follow-up, not a
    silent cleanup.
    """
    return tuple(
        region
        for region in artifact.regions
        if region.evidence_eligible and region.text and region.quarantine_reason is None
    )


def artifact_fragments(artifact: SourceArtifact) -> tuple[SourceFragment, ...]:
    """Project the durable meaningful regions, and only those, into fragments.

    Two gates, not one: a region has to be durable *and* unheld. Background and
    invisible parser layers are region-held today. Unknown identity,
    undeclared access, refused renditions, and parser failures still settle the
    whole record. Keeping the region gate explicit prevents held parser content
    from becoming durable data.
    """
    return tuple(
        SourceFragment(
            fragment_id=stable_id(FRAGMENT_ID_PREFIX, region.region_id, length=24),
            artifact_id=artifact.artifact_id,
            artifact_sha256=artifact.content_sha256,
            region_id=region.region_id,
            subject_type=artifact.subject_type,
            subject_id=artifact.subject_id,
            profile_id=artifact.profile_id,
            source_table=artifact.source_table,
            source_field=region.source_field,
            field_origin=region.field_origin,
            field_sha256=region.field_sha256,
            kind=region.kind,
            ordinal=region.ordinal,
            parent_region_id=region.parent_region_id,
            heading_path=region.heading_path,
            start_char=region.start_char,
            end_char=region.end_char,
            text=region.text,
            text_sha256=region.text_sha256,
            coordinates=region.coordinates,
            evidence_grade=region.evidence_grade,
            content_layer=region.content_layer,
            coordinate_grade=region.coordinate_grade,
            context_only=region.context_only,
        )
        for region in artifact.regions
        if region.durability == DURABLE_MEANINGFUL and region.quarantine_reason is None
    )


# --------------------------------------------------------------------------
# step checks
# --------------------------------------------------------------------------


def _check(name: str, status: str, detail: str = "") -> CheckResult:
    return CheckResult(step=SOURCE_STEP, name=name, status=status, detail=detail)


def source_checks(outcomes: Sequence[SourceOutcome]) -> list[CheckResult]:
    """Report what the source step proved, refused, and left undecided."""
    artifacts = [outcome.artifact for outcome in outcomes if outcome.artifact is not None]
    failed = [outcome for outcome in outcomes if outcome.state == "failed"]
    rejected = [outcome for outcome in outcomes if outcome.state == "rejected"]
    empty = [outcome for outcome in outcomes if outcome.state == "completed_empty"]

    uncovered = [
        f"{artifact.subject_id}:{one.source_field}"
        for artifact in artifacts
        for one in artifact.coverage
        if one.uncovered_chars
    ]
    mismatched: list[str] = []
    for artifact in artifacts:
        try:
            check_region_coordinates(artifact)
            check_region_digests(artifact)
        except SourceError as error:
            mismatched.append(str(error))
    undeclared = [artifact.subject_id for artifact in artifacts if not artifact.access.declared]
    secret_fields = sorted(
        {
            redact_text(f"{artifact.profile_id}:{source_field}")
            for artifact in artifacts
            if artifact.secret_rules
            for source_field, text in artifact.raw_fields.items()
            if scan_text_for_secrets(text)
        }
    )
    secret_rules = sorted({rule for artifact in artifacts for rule in artifact.secret_rules})

    return [
        _check(
            "coverage_gap_free",
            "pass" if not uncovered else "fail",
            "every source codepoint is covered" if not uncovered else f"uncovered fields: {uncovered[:5]}",
        ),
        _check(
            "region_text_matches_source",
            "pass" if not mismatched else "fail",
            "every region is its exact source slice" if not mismatched else mismatched[0],
        ),
        _check(
            "declared_access_scope",
            "pass" if not undeclared else "fail",
            f"{len(artifacts)} artifacts carry a declared access scope"
            if not undeclared
            else f"{len(undeclared)} artifacts carry no declared access scope",
        ),
        _check(
            "secret_scan",
            "pass" if not secret_rules else "fail",
            "no source field matches a credential rule"
            if not secret_rules
            else f"rules {secret_rules} matched in fields {secret_fields}",
        ),
        _check(
            "no_failed_work",
            "pass" if not failed else "fail",
            f"{len(outcomes)} source items settled"
            if not failed
            else f"{len(failed)} source items failed: {sorted({one.error for one in failed})}",
        ),
        _check(
            "quarantined",
            "pass" if not rejected else "unknown",
            "no source record was quarantined"
            if not rejected
            else f"{len(rejected)} quarantined: {sorted({one.reason for one in rejected})}",
        ),
        _check(
            "completed_empty",
            "pass",
            f"{len(empty)} source records carried no usable field and succeeded with no region",
        ),
    ]


# --------------------------------------------------------------------------
# tables
# --------------------------------------------------------------------------


def _arrow_type(kind: str) -> Any:
    import pyarrow as pa

    types = {"string": pa.string(), "int64": pa.int64(), "double": pa.float64(), "bool": pa.bool_()}
    if kind not in types:
        raise SourceError(f"unknown column kind {kind!r}")
    return types[kind]


def _coerce(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "string":
        return str(value)
    if kind == "int64":
        return int(value)
    if kind == "double":
        return float(value)
    return bool(value)


def write_table(path: Path, columns: Sequence[tuple[str, str]], rows: Sequence[Mapping[str, Any]]) -> Path:
    """Write one correctly shaped table, including when it has no rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    prepared = [{name: _coerce(row.get(name), kind) for name, kind in columns} for row in rows]
    schema = pa.schema([pa.field(name, _arrow_type(kind)) for name, kind in columns])
    data = {name: [row[name] for row in prepared] for name, _ in columns}
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pydict(data, schema=schema), path)
    return path


def artifact_row(artifact: SourceArtifact) -> dict[str, Any]:
    """One ``source/artifacts.parquet`` row: one exact Artifact version."""
    fragments = artifact_fragments(artifact)
    return {
        "artifact_id": artifact.artifact_id,
        "content_sha256": artifact.content_sha256,
        "subject_type": artifact.subject_type,
        "subject_id": artifact.subject_id,
        "profile_id": artifact.profile_id,
        "source_table": artifact.source_table,
        "allowed_schemes": canonical_json(list(artifact.allowed_schemes)),
        "access_scope": artifact.access.scope,
        "access_basis": artifact.access.basis,
        "coordinate_target": artifact.coordinates.target,
        "coordinate_unit": artifact.coordinates.unit,
        "coordinate_interval": artifact.coordinates.interval,
        "region_adapter_version": artifact.region_adapter_id,
        "source_policy_version": artifact.source_policy_version,
        "dispatch": canonical_json(list(artifact.dispatch)),
        "parser_invoked": artifact.parser_invoked,
        "parser_id": artifact.parser.parser_id if artifact.parser else None,
        "parser_evidence_grade": artifact.parser.evidence_grade if artifact.parser else None,
        "parser_gate_classification": artifact.parser.gate.classification if artifact.parser else None,
        "field_count": len(artifact.raw_fields),
        "field_chars": artifact.field_chars,
        "region_count": len(artifact.regions),
        "fragment_count": len(fragments),
        "exclusion_count": len(artifact.exclusions),
        "uncovered_chars": artifact.uncovered_chars,
        "secret_rules": canonical_json(list(artifact.secret_rules)),
        "context_fields": canonical_json(dict(artifact.context_fields)),
    }


def fragment_rows(artifact: SourceArtifact) -> list[dict[str, Any]]:
    """Durable fragment rows, including explicitly non-citable context fragments."""
    return [
        {
            "fragment_id": fragment.fragment_id,
            "artifact_id": fragment.artifact_id,
            "artifact_sha256": fragment.artifact_sha256,
            "region_id": fragment.region_id,
            "subject_type": fragment.subject_type,
            "subject_id": fragment.subject_id,
            "profile_id": fragment.profile_id,
            "source_table": fragment.source_table,
            "source_field": fragment.source_field,
            "field_origin": fragment.field_origin,
            "field_sha256": fragment.field_sha256,
            "kind": fragment.kind,
            "ordinal": fragment.ordinal,
            "parent_region_id": fragment.parent_region_id,
            "heading_path": canonical_json(list(fragment.heading_path)),
            "start_char": fragment.start_char,
            "end_char": fragment.end_char,
            "char_count": fragment.char_count,
            "coordinate_target": fragment.coordinates.target,
            "coordinate_unit": fragment.coordinates.unit,
            "coordinate_interval": fragment.coordinates.interval,
            "evidence_grade": fragment.evidence_grade,
            "content_layer": fragment.content_layer,
            "coordinate_grade": fragment.coordinate_grade,
            "context_only": fragment.context_only,
            "text_sha256": fragment.text_sha256,
            "text": fragment.text,
        }
        for fragment in artifact_fragments(artifact)
    ]


def parser_attempt_rows(outcome: SourceOutcome) -> list[dict[str, Any]]:
    """``source/parser-attempts.parquet`` rows: every parser decision and attempt."""
    rows: list[dict[str, Any]] = []
    for attempt in outcome.parser_attempts:
        gate = attempt.gate
        rows.append(
            {
                "attempt_id": attempt.attempt_id,
                "terminal_state": attempt.terminal_state,
                "outcome_reason": attempt.outcome_reason,
                "identifier": attempt.identifier,
                "profile_id": attempt.profile_id,
                "source_table": attempt.source_table,
                "source_sha256": attempt.source_sha256,
                "attachment_field": attempt.attachment_field,
                "attachment_sha256": attempt.attachment_sha256,
                "attachment_bytes": attempt.attachment_bytes,
                "source_name_sha256": attempt.source_name_sha256,
                "media_type": attempt.media_type,
                "detected_input_format": attempt.detected_input_format,
                "provider": attempt.provider,
                "parser_id": attempt.parser_id,
                "parser_policy_digest": attempt.parser_policy_digest,
                "parser_policy_json": attempt.parser_policy_json,
                "source_policy_version": attempt.source_policy_version,
                "parser_enabled": attempt.parser_enabled,
                "gate_worker_module": gate.worker_module if gate is not None else None,
                "gate_classification": gate.classification if gate is not None else None,
                "parser_status": gate.parser_status if gate is not None else None,
                "parser_failure_reason": gate.parser_failure_reason if gate is not None else None,
                "exit_status": gate.exit_status if gate is not None else None,
                "signal_number": gate.signal_number if gate is not None else None,
                "process_group_terminated": (gate.process_group_terminated if gate is not None else False),
                "duration_ms": gate.duration_ms if gate is not None else 0.0,
                "result_bytes": gate.result_bytes if gate is not None else 0,
                "result_over_limit": gate.result_over_limit if gate is not None else False,
                "stderr_bytes": gate.stderr_bytes if gate is not None else 0,
                "stderr_over_limit": gate.stderr_over_limit if gate is not None else False,
                "result_sha256": gate.result_sha256 if gate is not None else None,
                "limits_json": canonical_json(gate.limits.as_dict()) if gate is not None else None,
                "enforced_limits": (
                    canonical_json(list(gate.enforced_limits)) if gate is not None else canonical_json([])
                ),
                "observed_limits": (
                    canonical_json(list(gate.observed_limits)) if gate is not None else canonical_json([])
                ),
                "unenforced_limits": (
                    canonical_json(list(gate.unenforced_limits)) if gate is not None else canonical_json([])
                ),
                "call_json": attempt.call_json,
            }
        )
    return rows


def coverage_rows(outcome: SourceOutcome) -> list[dict[str, Any]]:
    """``source/coverage.parquet`` rows: coverage, exclusions, and quarantine."""
    artifact = outcome.artifact
    rows: list[dict[str, Any]] = []
    if artifact is not None:
        base = {
            "artifact_id": artifact.artifact_id,
            "content_sha256": artifact.content_sha256,
            "profile_id": artifact.profile_id,
        }
        rows.extend(
            {
                **base,
                "source_field": one.source_field,
                "field_origin": one.field_origin,
                "record_kind": COVERAGE_RECORD_FIELD,
                "reason": None,
                "field_chars": one.field_chars,
                "covered_chars": one.covered_chars,
                "durable_chars": one.durable_chars,
                "syntax_chars": one.syntax_chars,
                "container_chars": one.container_chars,
                "uncovered_chars": one.uncovered_chars,
                "gaps": canonical_json([list(gap) for gap in one.gaps]),
                "region_count": one.region_count,
                "fragment_count": one.fragment_count,
            }
            for one in artifact.coverage
        )
        rows.extend(
            {
                **base,
                "source_field": one.source_field,
                "field_origin": artifact.field_origins.get(one.source_field),
                "record_kind": COVERAGE_RECORD_EXCLUSION,
                "reason": one.reason,
                "field_chars": one.char_count,
                "covered_chars": 0,
                "durable_chars": 0,
                "syntax_chars": 0,
                "container_chars": 0,
                "uncovered_chars": (one.char_count if one.reason == EXCLUSION_COVERAGE_GAP else 0),
                "gaps": canonical_json([[one.start_char, one.end_char]]),
                "region_count": 0,
                "fragment_count": 0,
            }
            for one in artifact.exclusions
        )
    rows.extend(
        {
            "artifact_id": artifact.artifact_id if artifact is not None else None,
            "content_sha256": artifact.content_sha256 if artifact is not None else None,
            "profile_id": outcome.profile_id,
            "source_field": one.scope,
            "field_origin": None,
            "record_kind": COVERAGE_RECORD_QUARANTINE,
            "reason": one.reason,
            "field_chars": 0,
            "covered_chars": 0,
            "durable_chars": 0,
            "syntax_chars": 0,
            "container_chars": 0,
            "uncovered_chars": 0,
            "gaps": canonical_json([]),
            "region_count": 0,
            "fragment_count": 0,
        }
        for one in outcome.quarantine
    )
    rows.extend(
        {
            "artifact_id": None,
            "content_sha256": None,
            "profile_id": outcome.profile_id,
            "source_field": one.source_table,
            "field_origin": None,
            "record_kind": COVERAGE_RECORD_SOURCE_TABLE_EXCLUSION,
            "reason": one.reason,
            "field_chars": 0,
            "covered_chars": 0,
            "durable_chars": 0,
            "syntax_chars": 0,
            "container_chars": 0,
            "uncovered_chars": 0,
            "gaps": canonical_json([]),
            "region_count": 0,
            "fragment_count": 0,
        }
        for one in outcome.source_table_exclusions
    )
    return rows


def write_source_tables(root: Path, outcomes: Sequence[SourceOutcome]) -> dict[str, Path]:
    """Write every source table, correctly shaped even when a table has no rows."""
    root = Path(root)
    artifacts = [outcome.artifact for outcome in outcomes if outcome.artifact is not None]
    return {
        ARTIFACT_TABLE: write_table(
            root / ARTIFACT_TABLE, ARTIFACT_COLUMNS, [artifact_row(artifact) for artifact in artifacts]
        ),
        FRAGMENT_TABLE: write_table(
            root / FRAGMENT_TABLE,
            FRAGMENT_COLUMNS,
            [row for artifact in artifacts for row in fragment_rows(artifact)],
        ),
        COVERAGE_TABLE: write_table(
            root / COVERAGE_TABLE,
            COVERAGE_COLUMNS,
            [row for outcome in outcomes for row in coverage_rows(outcome)],
        ),
        PARSER_ATTEMPT_TABLE: write_table(
            root / PARSER_ATTEMPT_TABLE,
            PARSER_ATTEMPT_COLUMNS,
            [row for outcome in outcomes for row in parser_attempt_rows(outcome)],
        ),
    }


# --------------------------------------------------------------------------
# loading a frozen source snapshot
# --------------------------------------------------------------------------


def _selected_source_tables(active_source_tables: Iterable[str] | None) -> set[str]:
    if active_source_tables is None:
        return set(_PROFILE_BY_TABLE)
    active = set(active_source_tables)
    unknown = sorted(active - set(_PROFILE_BY_TABLE))
    if unknown:
        raise SourceError("active source tables are not profiled: " + ", ".join(unknown))
    return active


def _validate_required_sources(
    output_dir: Path,
    required_source_tables: Iterable[str],
    *,
    active_source_tables: Iterable[str] | None,
) -> None:
    required = set(required_source_tables)
    unknown = sorted(required - set(_PROFILE_BY_TABLE))
    if unknown:
        raise SourceError("required source tables are not profiled: " + ", ".join(unknown))
    inactive_required = sorted(required - _selected_source_tables(active_source_tables))
    if inactive_required:
        raise SourceError("required source tables are inactive: " + ", ".join(inactive_required))
    missing = sorted(source for source in required if not (output_dir / f"{source}.parquet").exists())
    if missing:
        raise FileNotFoundError(
            f"source inputs missing from {output_dir}: " + ", ".join(f"{source}.parquet" for source in missing)
        )


def _document_records(output_dir: Path, profile: SourceProfile) -> Iterator[SourceRecord]:
    """Regulations.gov documents, joined to their Federal Register counterpart."""
    documents_file = output_dir / "documents.parquet"
    fr_file = output_dir / "federal_register.parquet"
    relevant = {
        str(row["fr_doc_num"])
        for row in iter_parquet_rows(documents_file, columns=("fr_doc_num",))
        if row.get("fr_doc_num")
    }
    fr_by_number: dict[str, dict] = {}
    if fr_file.exists() and relevant:
        for row in iter_parquet_rows(fr_file, columns=("document_number", "title", "abstract")):
            number = str(row.get("document_number") or "")
            if number in relevant:
                fr_by_number[number] = row
    for row in iter_parquet_rows(documents_file):
        fr = fr_by_number.get(str(row.get("fr_doc_num") or ""), {})
        yield SourceRecord(
            profile=profile,
            row=row,
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


def iter_source_records(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
    active_source_tables: Iterable[str] | None = None,
) -> Iterator[SourceRecord]:
    """Yield every profiled source record in one snapshot, in profile order."""
    output_dir = Path(output_dir)
    active = _selected_source_tables(active_source_tables)
    _validate_required_sources(
        output_dir,
        required_source_tables,
        active_source_tables=active,
    )
    for profile in SOURCE_PROFILES:
        if profile.source_table not in active:
            continue
        path = output_dir / f"{profile.source_table}.parquet"
        if not path.exists():
            continue
        if profile.source_table == "documents":
            yield from _document_records(output_dir, profile)
            continue
        for row in iter_parquet_rows(path):
            yield SourceRecord(profile=profile, row=row)


def build_source_artifacts(
    output_dir: Path,
    *,
    required_source_tables: Iterable[str] = (),
    active_source_tables: Iterable[str] | None = None,
    policy: SourcePolicy = DEFAULT_SOURCE_POLICY,
    parser: OfficeParser | None = None,
) -> list[SourceOutcome]:
    """Build every artifact in a snapshot, keeping quarantined records visible."""
    records = [
        build_source_artifact(record, policy=policy, parser=parser)
        for record in iter_source_records(
            output_dir,
            required_source_tables=required_source_tables,
            active_source_tables=active_source_tables,
        )
    ]
    if active_source_tables is None:
        return records
    active = _selected_source_tables(active_source_tables)
    records.extend(
        SourceOutcome(
            state="completed_empty",
            reason=EXCLUSION_INACTIVE_SOURCE_TABLE,
            identifier=profile.source_table,
            profile_id=profile.profile_id,
            source_table_exclusions=(SourceTableExclusion(profile.source_table),),
        )
        for profile in SOURCE_PROFILES
        if profile.source_table not in active and (Path(output_dir) / f"{profile.source_table}.parquet").is_file()
    )
    return records


# --------------------------------------------------------------------------
# the contained Office parse
# --------------------------------------------------------------------------

PARSER_PROVIDER = "docling"

WORKER_MODULE = "spicy_regs.docpipeline.adapters.docling"
"""The adapter module this gate launches as a child process.

``python -m`` that module reads one caller-owned job file, invokes the adapter,
and writes one strict project-owned JSON record. No provider type and no pickle
crosses the boundary.
"""

JOB_NAME = "job.json"
INPUT_NAME = "input.bin"
RESULT_NAME = "result.json"
STDERR_NAME = "stderr.log"

GATE_COMPLETED = "completed"
GATE_TIMEOUT = "wall_timeout"
GATE_EXIT = "nonzero_exit"
GATE_SIGNAL = "terminated_by_signal"
GATE_MALFORMED_RESULT = "malformed_result"
GATE_RESULT_OVER_LIMIT = "result_over_limit"
GATE_INPUT_OVER_LIMIT = "input_over_limit"
GATE_EXTRA_UNAVAILABLE = "parser_extra_unavailable"

GATE_CLASSIFICATIONS: tuple[str, ...] = (
    GATE_COMPLETED,
    GATE_TIMEOUT,
    GATE_EXIT,
    GATE_SIGNAL,
    GATE_MALFORMED_RESULT,
    GATE_RESULT_OVER_LIMIT,
    GATE_INPUT_OVER_LIMIT,
    GATE_EXTRA_UNAVAILABLE,
)
"""Every classification this gate may record. Fixed here, never source-derived."""

ENFORCED_LIMITS: tuple[str, ...] = (
    "input_bytes",
    "wall_timeout_seconds",
    "result_bytes",
    "process_group_termination",
    "credential_stripped_environment",
    "controlled_temporary_directory",
)
"""What this gate really holds, on macOS as well as Linux.

Each one is enforced by code below: the input is measured before it is written,
the child is waited on with a wall clock, the result file is ``stat``-ed against
its cap before it is read, the whole process group
is signalled, the child's environment is built from an allowlist, and the child
runs in a temporary directory this gate created and removes.
"""

OBSERVED_LIMITS: tuple[str, ...] = ("stderr_bytes",)
"""Measured without enforcement: stderr is never read and never fails success."""

UNENFORCED_LIMITS: tuple[str, ...] = (
    "cpu_seconds",
    "resident_memory_bytes",
    "archive_expansion_bytes",
    "temporary_disk_bytes",
    "descendant_process_count",
    "network_access",
    "filesystem_scope",
)
"""What this gate does **not** hold, stated rather than implied.

A child may burn CPU, grow resident memory, expand an archive, fill the
temporary directory, fork further descendants, open a socket, and read any file
its user can read. Nothing below stops any of that, so nothing below claims to.
Closing one of these means adding real enforcement and moving its name up.
"""

ENVIRONMENT_ALLOWLIST: tuple[str, ...] = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT")
"""The only variables copied into the child. Everything else — every credential
this process holds — is simply absent."""

DEFAULT_WALL_TIMEOUT_SECONDS = 120.0
DEFAULT_TERMINATE_GRACE_SECONDS = 5.0
DEFAULT_MAX_RESULT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_STDERR_BYTES = 64 * 1024


@dataclass(frozen=True)
class ProcessGateLimits:
    """The bounds this gate really applies to one contained parse."""

    wall_timeout_seconds: float = DEFAULT_WALL_TIMEOUT_SECONDS
    terminate_grace_seconds: float = DEFAULT_TERMINATE_GRACE_SECONDS
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES
    max_stderr_bytes: int = DEFAULT_MAX_STDERR_BYTES
    max_input_bytes: int = DEFAULT_MAX_SOURCE_BYTES

    def as_dict(self) -> dict[str, Any]:
        return {
            "wall_timeout_seconds": self.wall_timeout_seconds,
            "terminate_grace_seconds": self.terminate_grace_seconds,
            "max_result_bytes": self.max_result_bytes,
            "max_stderr_bytes": self.max_stderr_bytes,
            "max_input_bytes": self.max_input_bytes,
        }


DEFAULT_GATE_LIMITS = ProcessGateLimits()


@dataclass(frozen=True)
class ProcessGateReceipt:
    """The immutable, source-free record of one contained parse attempt.

    Nothing a child wrote to stderr, nothing a provider said, and no source text
    reaches this record. What it carries is a fixed classification, the bounded
    counts the gate measured, and the two limit lists above — so a reader can
    tell what was really enforced from what merely was not.
    """

    worker_module: str
    classification: str
    parser_status: str
    parser_failure_reason: str | None
    exit_status: int | None
    signal_number: int | None
    process_group_terminated: bool
    duration_ms: float
    result_bytes: int
    result_over_limit: bool
    stderr_bytes: int
    stderr_over_limit: bool
    limits: ProcessGateLimits
    enforced_limits: tuple[str, ...] = ENFORCED_LIMITS
    observed_limits: tuple[str, ...] = OBSERVED_LIMITS
    unenforced_limits: tuple[str, ...] = UNENFORCED_LIMITS
    result_sha256: str = ""

    def __post_init__(self) -> None:
        if self.classification not in GATE_CLASSIFICATIONS:
            raise SourceError(f"unknown gate classification {self.classification!r}")
        groups = (
            set(self.enforced_limits),
            set(self.observed_limits),
            set(self.unenforced_limits),
        )
        if groups[0] & groups[1] or groups[0] & groups[2] or groups[1] & groups[2]:
            raise SourceError("a limit cannot be enforced, observed, and unenforced at once")

    def as_dict(self) -> dict[str, Any]:
        return {
            "worker_module": self.worker_module,
            "classification": self.classification,
            "parser_status": self.parser_status,
            "parser_failure_reason": self.parser_failure_reason,
            "exit_status": self.exit_status,
            "signal_number": self.signal_number,
            "process_group_terminated": self.process_group_terminated,
            "duration_ms": self.duration_ms,
            "result_bytes": self.result_bytes,
            "result_over_limit": self.result_over_limit,
            "stderr_bytes": self.stderr_bytes,
            "stderr_over_limit": self.stderr_over_limit,
            "limits": self.limits.as_dict(),
            "enforced_limits": list(self.enforced_limits),
            "observed_limits": list(self.observed_limits),
            "unenforced_limits": list(self.unenforced_limits),
            "result_sha256": self.result_sha256,
        }


@dataclass(frozen=True)
class ParsedOfficeElement:
    """One element of a contained parse, rebuilt from strict JSON built-ins."""

    ordinal: int
    kind: str
    text: str
    start_char: int
    end_char: int
    content_layer: str
    coordinate_grade: str
    text_usable: bool
    heading_path: tuple[str, ...] = ()


@dataclass(frozen=True)
class ParsedOfficeText:
    """The project-owned view of one contained parse. No provider type inside."""

    text: str
    elements: tuple[ParsedOfficeElement, ...]
    parser_id: str
    input_format: str
    source_sha256: str
    source_bytes: int
    evidence_grade: str
    offsets: CoordinateSystem

    def __post_init__(self) -> None:
        if self.evidence_grade == SOURCE_EXACT_EVIDENCE:
            raise SourceError("a parsed rendition is never source-exact evidence")
        if self.offsets.target != PARSED_TEXT_TARGET:
            raise SourceError("parsed offsets address the adapter-built text, not a source field")


@dataclass(frozen=True)
class ContainedParseResult:
    """One contained parse: its receipt, and what it produced when it produced."""

    receipt: ProcessGateReceipt
    parsed: ParsedOfficeText | None
    call: Mapping[str, Any] | None = None


def default_worker_command(job_path: Path) -> tuple[str, ...]:
    """The command this gate runs: the adapter module, isolated, one job file."""
    return (sys.executable, "-I", "-m", WORKER_MODULE, str(job_path))


def contained_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build the child's environment from an allowlist, so no credential travels."""
    source = os.environ if base is None else base
    return {name: source[name] for name in ENVIRONMENT_ALLOWLIST if name in source}


def _terminate_group(process: subprocess.Popen[bytes], group: int, *, grace: float) -> bool:
    """SIGTERM the child's whole process group, then SIGKILL what survives."""
    signalled = False
    for number, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
        try:
            os.killpg(group, number)
            signalled = True
        except (ProcessLookupError, PermissionError, OSError):
            pass
        if wait is None:
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
            break
        try:
            process.wait(timeout=wait)
            break
        except subprocess.TimeoutExpired:
            continue
    return signalled


def _reap_group(group: int) -> bool:
    """Kill anything the child left behind, so no grandchild outlives the gate.

    ``start_new_session=True`` makes the child its own group leader, so the
    group id is the child's pid — read once, at launch, because after the child
    is waited on there is no process left to ask. The signal goes out
    immediately after that wait; ``ProcessLookupError`` simply means the whole
    group was already gone, which is the ordinary case.
    """
    try:
        os.killpg(group, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    return True


def _element_from(record: Mapping[str, Any]) -> ParsedOfficeElement:
    return ParsedOfficeElement(
        ordinal=_exact_int(record.get("ordinal")),
        kind=_exact_text(record.get("kind")),
        text=_exact_text(record.get("text")),
        start_char=_exact_int(record.get("start_char")),
        end_char=_exact_int(record.get("end_char")),
        content_layer=_exact_text(record.get("content_layer")),
        coordinate_grade=_exact_text(record.get("coordinate_grade")),
        text_usable=_exact_bool(record.get("text_usable")),
        heading_path=tuple(_exact_text(one) for one in _exact_list(record.get("heading_path"))),
    )


def _exact_text(value: Any) -> str:
    if type(value) is not str:
        raise SourceError("the worker result carries a non-string where text was required")
    return value


def _exact_int(value: Any) -> int:
    if type(value) is not int:
        raise SourceError("the worker result carries a non-integer where a count was required")
    return value


def _exact_bool(value: Any) -> bool:
    if type(value) is not bool:
        raise SourceError("the worker result carries a non-boolean where a flag was required")
    return value


def _exact_list(value: Any) -> list[Any]:
    if type(value) is not list:
        raise SourceError("the worker result carries a non-list where a sequence was required")
    return value


def _exact_mapping(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        raise SourceError("the worker result carries a non-object where a record was required")
    return value


def _exact_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _exact_text(value)


def _exact_offsets(value: Any) -> CoordinateSystem:
    record = _exact_mapping(value)
    offsets = CoordinateSystem(
        target=_exact_text(record.get("target")),
        unit=_exact_text(record.get("unit")),
        interval=_exact_text(record.get("interval")),
    )
    if offsets != PARSED_TEXT_COORDINATES:
        raise SourceError("worker offsets do not address adapter-parsed text")
    return offsets


def _validate_call_identity(
    call: Mapping[str, Any],
    *,
    status: str,
    payload: bytes,
    source_name: str,
    media_type: str | None,
    input_format: str,
) -> dict[str, Any]:
    record = _exact_mapping(call)
    if _exact_text(record.get("provider")) != PARSER_PROVIDER:
        raise SourceError("worker call names another provider")
    if _exact_text(record.get("operation")) != "document-parse":
        raise SourceError("worker call names another operation")
    if _exact_text(record.get("status")) != status:
        raise SourceError("worker call status disagrees with the worker result")
    policy = _exact_mapping(record.get("policy"))
    policy_digest = _exact_text(record.get("policy_digest"))
    if not re.fullmatch(r"[0-9a-f]{64}", policy_digest):
        raise SourceError("worker call carries a malformed parser policy digest")
    if hashlib.sha256(canonical_json(policy).encode()).hexdigest() != policy_digest:
        raise SourceError("worker call parser policy digest does not cover its policy")
    if _exact_text(policy.get("mapping_revision")) != ADAPTER_MAPPING_REVISION:
        raise SourceError("worker call names another parser mapping revision")
    parser_id = _exact_text(record.get("parser_id"))
    expected_parser_id = (
        f"docling:{DOCLING_VERSION}:docling-core:{DOCLING_CORE_VERSION}:{ADAPTER_MAPPING_REVISION}:{policy_digest[:16]}"
    )
    if parser_id != expected_parser_id:
        raise SourceError("worker call parser identity disagrees with its policy")
    if _exact_text(record.get("source_sha256")) != hashlib.sha256(payload).hexdigest():
        raise SourceError("worker call source digest disagrees with the input")
    if _exact_int(record.get("source_bytes")) != len(payload):
        raise SourceError("worker call source length disagrees with the input")
    if _exact_text(record.get("source_name_sha256")) != hashlib.sha256(source_name.encode()).hexdigest():
        raise SourceError("worker call source-name digest disagrees with the request")
    if _exact_optional_text(record.get("media_type")) != media_type:
        raise SourceError("worker call media type disagrees with the request")
    if _exact_text(record.get("input_format")) != input_format:
        raise SourceError("worker call format disagrees with the parent detection")
    if _exact_text(record.get("evidence_grade")) != PARSER_DERIVED_EVIDENCE:
        raise SourceError("worker call claims another evidence grade")
    _exact_offsets(record.get("offsets"))
    return record


def _parsed_from(
    result: Mapping[str, Any],
    *,
    payload: bytes,
    source_name: str,
    media_type: str | None,
    input_format: str,
) -> tuple[ParsedOfficeText, dict[str, Any]]:
    document = _exact_mapping(result.get("document"))
    call = _validate_call_identity(
        _exact_mapping(result.get("call")),
        status=GATE_COMPLETED,
        payload=payload,
        source_name=source_name,
        media_type=media_type,
        input_format=input_format,
    )
    text = _exact_text(document.get("text"))
    elements = tuple(_element_from(_exact_mapping(one)) for one in _exact_list(document.get("elements")))
    if not text.strip() or not elements:
        raise SourceError("a completed parse must contain usable text and elements")
    if _exact_text(document.get("source_sha256")) != hashlib.sha256(payload).hexdigest():
        raise SourceError("worker document digest disagrees with the input")
    if _exact_int(document.get("source_bytes")) != len(payload):
        raise SourceError("worker document length disagrees with the input")
    if _exact_text(document.get("input_format")) != input_format:
        raise SourceError("worker document format disagrees with the parent detection")
    if _exact_text(document.get("evidence_grade")) != PARSER_DERIVED_EVIDENCE:
        raise SourceError("worker document claims another evidence grade")
    _exact_offsets(document.get("offsets"))
    previous_end = 0
    for ordinal, element in enumerate(elements):
        if element.ordinal != ordinal:
            raise SourceError("worker element ordinals are not exact reading order")
        if element.content_layer not in PARSER_CONTENT_LAYERS:
            raise SourceError("worker element carries an unknown content layer")
        if element.coordinate_grade not in PARSER_COORDINATE_GRADES:
            raise SourceError("worker element carries an unknown coordinate grade")
        if not 0 <= element.start_char <= element.end_char <= len(text):
            raise SourceError("worker element span leaves parsed text")
        if element.start_char < previous_end:
            raise SourceError("worker elements are not in reading order")
        if text[element.start_char : element.end_char] != element.text:
            raise SourceError("worker element text does not round trip")
        if element.text_usable is not bool(element.text.strip()):
            raise SourceError("worker element text_usable disagrees with its text")
        previous_end = element.end_char
    usable = tuple(one for one in elements if one.text_usable)
    expected_call_facts = {
        "element_count": len(elements),
        "usable_element_count": len(usable),
        "usable_character_count": sum(len(one.text) for one in usable),
        "character_count": len(text),
        "content_layers_present": sorted({one.content_layer for one in elements}),
        "coordinate_grade": (
            PARSER_PAGE_COORDINATES
            if any(one.coordinate_grade == PARSER_PAGE_COORDINATES for one in elements)
            else NO_COORDINATES
        ),
    }
    for name in ("element_count", "usable_element_count", "usable_character_count", "character_count"):
        if _exact_int(call.get(name)) != expected_call_facts[name]:
            raise SourceError(f"worker call {name} disagrees with the document")
    if (
        sorted(_exact_text(one) for one in _exact_list(call.get("content_layers_present")))
        != (expected_call_facts["content_layers_present"])
    ):
        raise SourceError("worker call content layers disagree with the document")
    if _exact_text(call.get("coordinate_grade")) != expected_call_facts["coordinate_grade"]:
        raise SourceError("worker call coordinate grade disagrees with the document")
    parsed = ParsedOfficeText(
        text=text,
        elements=elements,
        parser_id=_exact_text(call.get("parser_id")),
        input_format=input_format,
        source_sha256=hashlib.sha256(payload).hexdigest(),
        source_bytes=len(payload),
        evidence_grade=PARSER_DERIVED_EVIDENCE,
        offsets=PARSED_TEXT_COORDINATES,
    )
    return parsed, call


def run_contained_parse(
    content: bytes,
    *,
    source_name: str,
    media_type: str | None = None,
    limits: ProcessGateLimits = DEFAULT_GATE_LIMITS,
    worker_command: Any = default_worker_command,
    environment: Mapping[str, str] | None = None,
) -> ContainedParseResult:
    """Parse Office bytes in a contained child process and record what happened.

    The gate owns the boundary: a temporary directory nobody else names, a
    credential-stripped environment, a new session so the child leads its own
    process group, a wall clock, SIGTERM-then-SIGKILL over that whole group, and
    a result byte cap checked before that file is read. Stderr is redirected to
    a file that the parent measures and never reads; its threshold is an
    observed fact, not a success gate. Nothing is accumulated through a pipe.
    """
    if not isinstance(content, (bytes, bytearray)):
        raise SourceError("source content must be bytes")
    if type(source_name) is not str or not source_name:
        raise SourceError("source_name must be nonempty text")
    if media_type is not None and type(media_type) is not str:
        raise SourceError("media_type must be text or None")
    payload = bytes(content)
    try:
        input_format, _ = detect_input_format(source_name, media_type)
    except DoclingParseError as error:
        raise SourceError("the parent could not detect the parser input format") from error
    if len(payload) > limits.max_input_bytes:
        receipt = ProcessGateReceipt(
            worker_module=WORKER_MODULE,
            classification=GATE_INPUT_OVER_LIMIT,
            parser_status="",
            parser_failure_reason=None,
            exit_status=None,
            signal_number=None,
            process_group_terminated=False,
            duration_ms=0.0,
            result_bytes=0,
            result_over_limit=False,
            stderr_bytes=0,
            stderr_over_limit=False,
            limits=limits,
        )
        return ContainedParseResult(receipt=receipt, parsed=None)

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="spicy-regs-source-gate-") as directory:
        root = Path(directory)
        input_path = root / INPUT_NAME
        result_path = root / RESULT_NAME
        stderr_path = root / STDERR_NAME
        input_path.write_bytes(payload)
        (root / JOB_NAME).write_text(
            json.dumps(
                {
                    "input_path": str(input_path),
                    "result_path": str(result_path),
                    "source_name": source_name,
                    "media_type": media_type,
                    "max_source_bytes": limits.max_input_bytes,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = list(worker_command(root / JOB_NAME))
        classification = GATE_COMPLETED
        terminated = False
        with stderr_path.open("wb") as stderr_handle:
            process = subprocess.Popen(  # noqa: S603 - fixed command, no shell, stripped env
                command,
                cwd=str(root),
                env=dict(contained_environment(environment)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_handle,
                start_new_session=True,
                close_fds=True,
            )
            # The child leads its own session, so its pid *is* its process-group
            # id. Read it now: after the wait below there is nothing left to ask.
            group = process.pid
            try:
                process.wait(timeout=limits.wall_timeout_seconds)
            except subprocess.TimeoutExpired:
                classification = GATE_TIMEOUT
                terminated = _terminate_group(process, group, grace=limits.terminate_grace_seconds)
            finally:
                # Even a clean exit may leave a descendant behind; the group goes
                # either way, so nothing this gate started outlives it.
                terminated = _reap_group(group) or terminated
        duration_ms = round((time.monotonic() - started) * 1_000, 3)

        returncode = process.returncode
        exit_status = returncode if returncode is not None and returncode >= 0 else None
        signal_number = -returncode if returncode is not None and returncode < 0 else None
        if classification == GATE_COMPLETED:
            if signal_number is not None:
                classification = GATE_SIGNAL
            elif exit_status:
                classification = GATE_EXIT

        stderr_bytes = stderr_path.stat().st_size if stderr_path.exists() else 0
        result_bytes = result_path.stat().st_size if result_path.exists() else 0
        result_over_limit = result_bytes > limits.max_result_bytes

        def receipt(
            *,
            status: str = "",
            failure_reason: str | None = None,
            result_sha256: str = "",
            klass: str | None = None,
        ) -> ProcessGateReceipt:
            return ProcessGateReceipt(
                worker_module=WORKER_MODULE,
                classification=klass or classification,
                parser_status=status,
                parser_failure_reason=failure_reason,
                exit_status=exit_status,
                signal_number=signal_number,
                process_group_terminated=terminated,
                duration_ms=duration_ms,
                result_bytes=result_bytes,
                result_over_limit=result_over_limit,
                stderr_bytes=stderr_bytes,
                stderr_over_limit=stderr_bytes > limits.max_stderr_bytes,
                limits=limits,
                result_sha256=result_sha256,
            )

        if classification != GATE_COMPLETED:
            return ContainedParseResult(receipt=receipt(), parsed=None)
        if result_over_limit:
            # Measured, never read: the cap is checked against the file's size so
            # an oversized result never enters this process at all.
            return ContainedParseResult(receipt=receipt(klass=GATE_RESULT_OVER_LIMIT), parsed=None)
        if not result_path.exists():
            return ContainedParseResult(receipt=receipt(klass=GATE_MALFORMED_RESULT), parsed=None)
        raw = result_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        try:
            record = _exact_mapping(json.loads(raw.decode("utf-8")))
            status = _exact_text(record.get("status"))
        except (SourceError, UnicodeDecodeError, json.JSONDecodeError):
            return ContainedParseResult(receipt=receipt(klass=GATE_MALFORMED_RESULT, result_sha256=digest), parsed=None)
        if status == "unavailable":
            return ContainedParseResult(
                receipt=receipt(klass=GATE_EXTRA_UNAVAILABLE, status=status, result_sha256=digest), parsed=None
            )
        if status == "failed":
            try:
                reason = _exact_optional_text(record.get("failure_reason"))
                call = _validate_call_identity(
                    _exact_mapping(record.get("call")),
                    status="failed",
                    payload=payload,
                    source_name=source_name,
                    media_type=media_type,
                    input_format=input_format,
                )
                if _exact_optional_text(call.get("failure_reason")) != reason:
                    raise SourceError("worker failure reason disagrees with its call")
            except SourceError:
                return ContainedParseResult(
                    receipt=receipt(klass=GATE_MALFORMED_RESULT, result_sha256=digest),
                    parsed=None,
                )
            return ContainedParseResult(
                receipt=receipt(
                    status=status,
                    failure_reason=reason,
                    result_sha256=digest,
                ),
                parsed=None,
                call=call,
            )
        if status != "completed":
            return ContainedParseResult(receipt=receipt(klass=GATE_MALFORMED_RESULT, result_sha256=digest), parsed=None)
        try:
            parsed, call = _parsed_from(
                record,
                payload=payload,
                source_name=source_name,
                media_type=media_type,
                input_format=input_format,
            )
        except SourceError:
            return ContainedParseResult(receipt=receipt(klass=GATE_MALFORMED_RESULT, result_sha256=digest), parsed=None)
        return ContainedParseResult(
            receipt=receipt(status=status, result_sha256=digest),
            parsed=parsed,
            call=call,
        )


def contained_office_parser(
    *,
    limits: ProcessGateLimits = DEFAULT_GATE_LIMITS,
    worker_command: Any = default_worker_command,
    environment: Mapping[str, str] | None = None,
) -> OfficeParser:
    """Bind gate settings once and return the parser ``source.py`` calls."""

    def parse(content: bytes, *, source_name: str, media_type: str | None) -> ContainedParseResult:
        return run_contained_parse(
            content,
            source_name=source_name,
            media_type=media_type,
            limits=limits,
            worker_command=worker_command,
            environment=environment,
        )

    return parse

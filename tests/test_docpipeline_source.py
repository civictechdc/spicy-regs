"""Contract tests for the v3 ``source`` step.

``source.py`` turns one immutable source record into one exact ``Artifact``
version, one canonical stream of source regions, and the durable
``SourceFragment`` projection of the meaningful ones. These tests hold it to the
rules the design and the vision make binding:

* one Artifact means one exact immutable source state, and every region points
  at that Artifact and one exact source field;
* coordinates are Python unicode codepoints, half-open, and a region's text is
  always ``field_text[start:end]``;
* regions plus exclusions account for every codepoint of every source field —
  syntax and container regions stay in the accounting and never become durable
  fragments;
* native structure is used first, and a source carrying usable native text never
  reaches the contained Office parser; and
* unknown identity, access, or coordinates become plan-directed quarantine or
  failure rather than an invented record.

The contained-process gate around the Office fallback has its own suite in
``tests/test_docpipeline_source_containment.py``. Nothing here spawns a process.
"""

from __future__ import annotations

import ast
import dataclasses
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline import source as source_module
from spicy_regs.docpipeline.runtime import CheckResult, scan_text_for_secrets
from spicy_regs.docpipeline.source import (
    ARTIFACT_COLUMNS,
    ARTIFACT_TABLE,
    CONTAINER_REGION,
    COORDINATE_INTERVAL,
    COORDINATE_UNIT,
    COVERAGE_COLUMNS,
    COVERAGE_TABLE,
    DISPATCH_ATOMIC_FIELDS,
    DISPATCH_CONTAINED_PARSER,
    DISPATCH_NATIVE_MARKUP,
    DISPATCH_NATIVE_PROSE,
    DISPATCH_PRIORITY,
    DISPATCH_STRUCTURED_FIELDS,
    DURABILITY_CLASSES,
    DURABLE_MEANINGFUL,
    EXCLUDED_SOURCE_TABLES,
    FRAGMENT_COLUMNS,
    FRAGMENT_TABLE,
    PARSER_ATTEMPT_COLUMNS,
    PARSER_ATTEMPT_TABLE,
    PARSER_DERIVED_EVIDENCE,
    PARSER_DERIVED_FIELD,
    SOURCE_EXACT_EVIDENCE,
    SOURCE_FIELD_COORDINATES,
    SOURCE_NATIVE_FIELD,
    SOURCE_POLICY_VERSION,
    SOURCE_PROFILES,
    SYNTAX_REGION,
    UNDECLARED_ACCESS,
    AccessScope,
    ContainedParseResult,
    ParsedOfficeElement,
    ParsedOfficeText,
    ProcessGateReceipt,
    SourceAttachment,
    SourcePolicy,
    SourceProfile,
    SourceRecord,
    artifact_fragments,
    build_source_artifact,
    build_source_artifacts,
    processing_regions,
    native_structural_passage_spans,
    profile_for_table,
    source_checks,
    write_source_tables,
)
from spicy_regs.ontology.common import write_parquet_rows

# --- shared fixtures -------------------------------------------------------

PUBLIC = AccessScope(scope="public", basis="us-federal-public-record")

COMMENTS_PROFILE = profile_for_table("comments")
LOBBYING_PROFILE = profile_for_table("lobbying_filings")
FEDERAL_REGISTER_PROFILE = profile_for_table("federal_register")
GAO_PROFILE = profile_for_table("gao_reports")
DOCKET_PROFILE = profile_for_table("dockets")

OFFICE_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
OFFICE_BYTES = b"PK\x03\x04office rendition bytes\n"

# One of each character class the offsets have to survive: a section sign (BMP,
# two UTF-8 bytes), an em dash (BMP, three), a curly quote (BMP, three), and an
# emoji (astral, four bytes and two UTF-16 code units — but one codepoint).
UNICODE_BODY = (
    "§ 1.1 Scope\n\nThe rule—as written—says “no” to PFAS \U0001f9ea discharge.\n\n§ 1.2 Limits\n\nLimits apply."
)


def comment_record(comment: str, **fields: Any) -> SourceRecord:
    row: dict[str, Any] = {
        "comment_id": "COMMENT-1",
        "title": "Water policy",
        "comment": comment,
        "text_content": None,
        "organization": None,
        "category": None,
    }
    row.update(fields)
    return SourceRecord(profile=COMMENTS_PROFILE, row=row)


def built(record: SourceRecord, **options: Any) -> Any:
    outcome = build_source_artifact(record, **options)
    assert outcome.artifact is not None, outcome.reason or outcome.error
    return outcome.artifact


def field_regions(artifact: Any, source_field: str) -> list[Any]:
    return [region for region in artifact.regions if region.source_field == source_field]


class RecordedParser:
    """Office parser stand-in: records every call and returns a fixed result."""

    def __init__(self, result: ContainedParseResult | None = None) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def __call__(self, content: bytes, *, source_name: str, media_type: str | None) -> ContainedParseResult:
        self.calls.append({"content": content, "source_name": source_name, "media_type": media_type})
        assert self.result is not None, "this parser was not expected to be called"
        return self.result


def gate_receipt(classification: str = "completed", **changes: Any) -> ProcessGateReceipt:
    return source_module.ProcessGateReceipt(
        worker_module=source_module.WORKER_MODULE,
        classification=classification,
        parser_status=changes.pop("parser_status", "completed"),
        parser_failure_reason=changes.pop("parser_failure_reason", None),
        exit_status=changes.pop("exit_status", 0),
        signal_number=changes.pop("signal_number", None),
        process_group_terminated=changes.pop("process_group_terminated", True),
        duration_ms=changes.pop("duration_ms", 1.0),
        result_bytes=changes.pop("result_bytes", 128),
        result_over_limit=changes.pop("result_over_limit", False),
        stderr_bytes=changes.pop("stderr_bytes", 0),
        stderr_over_limit=changes.pop("stderr_over_limit", False),
        limits=changes.pop("limits", source_module.DEFAULT_GATE_LIMITS),
        enforced_limits=source_module.ENFORCED_LIMITS,
        unenforced_limits=source_module.UNENFORCED_LIMITS,
    )


OFFICE_TEXT = "Effluent Guidelines\n\nFacilities must sample water quarterly."


def office_result(text: str = OFFICE_TEXT, **changes: Any) -> ContainedParseResult:
    title, body = text.split("\n\n", 1)
    elements = (
        ParsedOfficeElement(
            ordinal=0,
            kind="title",
            text=title,
            start_char=0,
            end_char=len(title),
            content_layer="body",
            coordinate_grade="none",
            text_usable=True,
            heading_path=(),
        ),
        ParsedOfficeElement(
            ordinal=1,
            kind="text",
            text=body,
            start_char=len(title) + 2,
            end_char=len(text),
            content_layer="body",
            coordinate_grade="none",
            text_usable=True,
            heading_path=(title,),
        ),
    )
    return ContainedParseResult(
        receipt=gate_receipt(),
        parsed=ParsedOfficeText(
            text=text,
            elements=elements,
            parser_id="docling:2.115.0:docling-core:2.87.1:office-mapping-6:0123456789abcdef",
            input_format="docx",
            source_sha256="0" * 64,
            source_bytes=len(OFFICE_BYTES),
            evidence_grade=PARSER_DERIVED_EVIDENCE,
            offsets=source_module.PARSED_TEXT_COORDINATES,
            **changes,
        ),
        call={"provider": "docling", "status": "completed"},
    )


# --- exact unicode coordinates ---------------------------------------------


def test_every_region_text_equals_its_exact_half_open_codepoint_slice() -> None:
    artifact = built(comment_record(UNICODE_BODY))

    regions = field_regions(artifact, "comments.comment")
    field_text = artifact.raw_fields["comments.comment"]

    assert regions, "the body field produced no regions"
    for region in regions:
        assert region.text == field_text[region.start_char : region.end_char]
        assert region.coordinates == SOURCE_FIELD_COORDINATES
        assert region.coordinates.unit == COORDINATE_UNIT == "unicode-codepoints"
        assert region.coordinates.interval == COORDINATE_INTERVAL == "half-open"
    # Half-open, so consecutive regions abut and never share a codepoint.
    assert [region.start_char for region in regions[1:]] == [region.end_char for region in regions[:-1]]
    assert regions[0].start_char == 0
    assert regions[-1].end_char == len(field_text)


@pytest.mark.parametrize("character", ["§", "—", "“", "\U0001f9ea"])
def test_offsets_count_codepoints_not_bytes_or_utf16_units(character: str) -> None:
    body = f"Alpha {character} omega.\n\nSecond paragraph."
    artifact = built(comment_record(body))

    regions = field_regions(artifact, "comments.comment")
    field_text = artifact.raw_fields["comments.comment"]
    first = regions[0]

    assert first.text == field_text[first.start_char : first.end_char]
    assert first.end_char == len(field_text[: first.end_char])
    # A byte or UTF-16 offset would disagree for every character above.
    assert first.end_char != len(field_text[: first.end_char].encode("utf-8")) or character == ""
    assert "".join(region.text for region in regions) == field_text


def test_an_astral_character_is_one_codepoint_in_every_recorded_offset() -> None:
    body = "\U0001f9ea\U0001f9ea\U0001f9ea"
    artifact = built(comment_record(body))

    region = field_regions(artifact, "comments.comment")[0]

    assert (region.start_char, region.end_char) == (0, 3)
    assert region.text == body
    assert len(body.encode("utf-8")) == 12


# --- gap-free accounting and exact parent binding --------------------------


def test_regions_and_exclusions_account_for_every_source_codepoint() -> None:
    artifact = built(comment_record(UNICODE_BODY, text_content="  ", organization=None))

    for coverage in artifact.coverage:
        assert coverage.uncovered_chars == 0
        assert coverage.gaps == ()
        assert coverage.covered_chars == coverage.field_chars
        assert coverage.durable_chars + coverage.syntax_chars + coverage.container_chars >= coverage.field_chars
    # Every declared column is either a covered field or a recorded exclusion.
    declared = {f"comments.{column}" for column in COMMENTS_PROFILE.text_columns}
    accounted = {coverage.source_field for coverage in artifact.coverage} | {
        exclusion.source_field for exclusion in artifact.exclusions
    }
    assert declared == accounted
    assert {(one.source_field, one.reason) for one in artifact.exclusions} == {
        ("comments.text_content", "blank-non-content"),
        ("comments.organization", "null"),
        ("comments.category", "null"),
    }


def test_every_region_binds_its_exact_artifact_field_and_text_digests() -> None:
    import hashlib

    artifact = built(comment_record(UNICODE_BODY))

    for region in artifact.regions:
        field_text = artifact.raw_fields[region.source_field]
        assert region.artifact_sha256 == artifact.content_sha256
        assert region.field_sha256 == hashlib.sha256(field_text.encode()).hexdigest()
        assert region.field_sha256 == artifact.field_sha256[region.source_field]
        assert region.text_sha256 == hashlib.sha256(region.text.encode()).hexdigest()
    assert artifact.artifact_id.startswith("artifact_")
    assert artifact.coordinates == SOURCE_FIELD_COORDINATES


def test_hierarchy_binds_children_to_the_exact_parent_region() -> None:
    body = "Section 1 - Water Quality\n\nThe first paragraph regulates discharge.\n\nSection 2 - Monitoring\n\nReports are required."
    artifact = built(comment_record(body))

    regions = field_regions(artifact, "comments.comment")
    by_id = {region.region_id: region for region in regions}

    assert [region.kind for region in regions] == ["heading", "paragraph", "heading", "paragraph"]
    assert regions[1].parent_region_id == regions[0].region_id
    assert regions[3].parent_region_id == regions[2].region_id
    assert regions[3].heading_path == ("Section 2 - Monitoring",)
    assert all(region.parent_region_id in by_id for region in regions if region.parent_region_id)
    # Headings stay processing slices for parity, and say so as context.
    assert [region.context_only for region in regions] == [True, False, True, False]


# --- durability: meaningful fragments versus syntax and containers ---------


def test_markup_prolog_stays_in_coverage_and_never_becomes_a_fragment() -> None:
    body = '<?xml version="1.0"?>\n<article><h1>PFAS Rule</h1><p>Facilities must sample.</p></article>'
    record = SourceRecord(
        profile=FEDERAL_REGISTER_PROFILE,
        row={"document_number": "2026-12345", "title": "PFAS Rule", "body_html": body},
    )
    artifact = built(record)

    regions = field_regions(artifact, "federal_register.body_html")
    prolog = [region for region in regions if region.kind == "markup-prolog"]
    fragments = artifact_fragments(artifact)

    assert len(prolog) == 1
    assert prolog[0].durability == SYNTAX_REGION
    assert prolog[0].start_char == 0
    # Unchanged for this migration: the prolog stays in the processing stream.
    assert prolog[0].evidence_eligible is True
    assert prolog[0] in processing_regions(artifact)
    assert prolog[0].region_id not in {fragment.region_id for fragment in fragments}
    assert all(fragment.durability == DURABLE_MEANINGFUL for fragment in fragments)
    coverage = next(one for one in artifact.coverage if one.source_field == "federal_register.body_html")
    assert coverage.syntax_chars == len(prolog[0].text) > 0
    assert coverage.uncovered_chars == 0


def test_native_structural_passage_spans_expose_visible_exact_markup_regions() -> None:
    text = '<?xml version="1.0"?><section><title>Scope</title><p>Alpha rule.</p><p>Beta rule.</p></section>'

    spans = native_structural_passage_spans("cfr_sections.xml_text", text)
    pieces = [text[start:end] for start, end in spans]

    assert len(spans) == 3
    assert all(left_end <= right_start for (_, left_end), (right_start, _) in zip(spans, spans[1:]))
    assert any("Scope" in piece for piece in pieces)
    assert any("Alpha rule." in piece for piece in pieces)
    assert any("Beta rule." in piece for piece in pieces)
    assert all("<?xml" not in piece for piece in pieces)


def test_native_structural_passage_spans_exclude_non_content_markup() -> None:
    text = (
        "<html><head><title>PFAS report</title>"
        "<style>.hidden { display: none; }</style>"
        "<script>window.tracking = true;</script></head>"
        "<body><nav><p>Site navigation</p></nav><main>"
        "<!-- analytics marker --><noscript><iframe>tracking</iframe></noscript>"
        "<p hidden>hidden sentence</p><p aria-hidden='true'>also hidden</p>"
        "<p style='display:none'>still hidden</p>"
        "<p>Facilities must sample quarterly.</p><p>(</p>"
        "<p>Before icon <svg><path d='M0 0 L10 10'/></svg> after icon.</p>"
        "</main><footer><p>Site footer</p></footer></body></html>"
    )

    spans = native_structural_passage_spans(
        "gao_reports.full_text",
        text,
        media_type="text/html",
    )
    pieces = [text[start:end] for start, end in spans]
    selected = "\n".join(pieces)

    assert "PFAS report" in selected
    assert "Facilities must sample quarterly." in selected
    assert "display: none" not in selected
    assert "window.tracking" not in selected
    assert "Site navigation" not in selected
    assert "Site footer" not in selected
    assert "analytics marker" not in selected
    assert "tracking" not in selected
    assert "hidden sentence" not in selected
    assert "also hidden" not in selected
    assert "still hidden" not in selected
    assert "<svg" not in selected
    assert "<path" not in selected
    assert "Before icon" in selected
    assert "after icon." in selected
    assert all(any(character.isalnum() for character in piece) for piece in pieces)


def test_native_structural_passage_spans_exclude_non_content_without_block_tags() -> None:
    text = (
        "<html><body><script>secret()</script>"
        "<span>Visible rule.</span><span hidden>Hidden rule.</span>"
        "</body></html>"
    )

    spans = native_structural_passage_spans(
        "gao_reports.full_text",
        text,
        media_type="text/html",
    )
    selected = "\n".join(text[start:end] for start, end in spans)

    assert "Visible rule." in selected
    assert "secret()" not in selected
    assert "Hidden rule." not in selected


def test_html_title_nested_in_main_does_not_duplicate_a_passage() -> None:
    text = (
        "<html><body><main><title>Inline title</title>"
        "<p>Visible rule.</p></main></body></html>"
    )

    spans = native_structural_passage_spans(
        "gao_reports.full_text",
        text,
        media_type="text/html",
    )
    selected = "\n".join(text[start:end] for start, end in spans)

    assert all(
        left_end <= right_start
        for (_, left_end), (right_start, _) in zip(spans, spans[1:])
    )
    assert selected.count("Inline title") == 1
    assert selected.count("Visible rule.") == 1


def test_release_visibility_filter_does_not_change_existing_source_heading_context() -> None:
    body = "<h1>Scope<script>private</script></h1><p>Visible rule.</p>"
    record = SourceRecord(
        profile=FEDERAL_REGISTER_PROFILE,
        row={"document_number": "2026-12345", "title": "PFAS Rule", "body_html": body},
    )

    artifact = built(record)
    paragraph = next(
        region
        for region in field_regions(artifact, "federal_register.body_html")
        if "Visible rule." in region.text
    )

    # The v3 source-policy IDs were minted with the original markup collector.
    # The actual-file release visibility filter must not silently change them.
    assert paragraph.heading_path == ("Scopeprivate",)


def test_xml_head_elements_remain_searchable() -> None:
    text = "<ROOT><HEAD>Substantive CFR heading</HEAD><P>Required conduct.</P></ROOT>"

    spans = native_structural_passage_spans(
        "cfr_sections.xml_text",
        text,
        media_type="application/xml",
    )
    selected = "\n".join(text[start:end] for start, end in spans)

    assert "Substantive CFR heading" in selected
    assert "Required conduct." in selected


def test_structured_array_containers_cover_without_becoming_fragments() -> None:
    activities = '[{"general_issue_code":"ENV","description":"PFAS"},{"general_issue_code":"ENG","description":"Grid"}]'
    record = SourceRecord(
        profile=LOBBYING_PROFILE,
        row={
            "filing_uuid": "FILING-1",
            "client_name": "Example Client",
            "registrant_name": "Example Registrant",
            "lobbying_activities_json": activities,
            "government_entities_json": '["EPA","DOE"]',
        },
    )
    artifact = built(record)

    regions = field_regions(artifact, "lobbying_filings.lobbying_activities_json")
    container, *children = regions
    fragments = {fragment.region_id for fragment in artifact_fragments(artifact)}

    assert container.kind == "structured-array"
    assert container.durability == CONTAINER_REGION
    assert container.evidence_eligible is False
    assert container.region_id not in fragments
    assert container not in processing_regions(artifact)
    assert len(children) == 2
    assert all(child.durability == DURABLE_MEANINGFUL for child in children)
    assert all(child.region_id in fragments for child in children)
    assert all(child.parent_region_id == container.region_id for child in children)
    assert "".join(child.text for child in children) == activities
    coverage = next(one for one in artifact.coverage if one.source_field == "lobbying_filings.lobbying_activities_json")
    assert coverage.container_chars == len(activities)
    assert coverage.durable_chars == len(activities)
    assert coverage.uncovered_chars == 0
    assert coverage.fragment_count == 2
    assert coverage.region_count == 3


def test_every_region_carries_one_of_the_declared_durability_classes() -> None:
    activities = '[{"a":1},{"b":2}]'
    artifacts = [
        built(comment_record(UNICODE_BODY)),
        built(
            SourceRecord(
                profile=FEDERAL_REGISTER_PROFILE,
                row={
                    "document_number": "2026-2",
                    "title": "T",
                    "body_html": '<?xml version="1.0"?>\n<article><p>Body.</p></article>',
                },
            )
        ),
        built(
            SourceRecord(
                profile=LOBBYING_PROFILE,
                row={
                    "filing_uuid": "FILING-4",
                    "client_name": "Client",
                    "registrant_name": "Registrant",
                    "lobbying_activities_json": activities,
                    "government_entities_json": None,
                },
            )
        ),
    ]

    seen = {region.durability for artifact in artifacts for region in artifact.regions}

    assert seen == set(DURABILITY_CLASSES)
    assert DURABILITY_CLASSES == (DURABLE_MEANINGFUL, SYNTAX_REGION, CONTAINER_REGION)


def test_a_quarantined_region_never_becomes_a_durable_fragment() -> None:
    artifact = built(comment_record("First.\n\nSecond."))
    region = field_regions(artifact, "comments.comment")[0]
    assert region.durability == DURABLE_MEANINGFUL
    assert region.quarantine_reason is None

    held = dataclasses.replace(
        artifact,
        regions=tuple(
            dataclasses.replace(one, quarantine_reason="unknown_coordinates") if one is region else one
            for one in artifact.regions
        ),
    )

    assert region.region_id in {fragment.region_id for fragment in artifact_fragments(artifact)}
    assert region.region_id not in {fragment.region_id for fragment in artifact_fragments(held)}


def test_every_durable_fragment_projects_exactly_one_meaningful_region() -> None:
    artifact = built(comment_record(UNICODE_BODY))

    regions = [region for region in artifact.regions if region.durability == DURABLE_MEANINGFUL]
    fragments = artifact_fragments(artifact)

    assert [fragment.region_id for fragment in fragments] == [region.region_id for region in regions]
    for fragment, region in zip(fragments, regions, strict=True):
        assert fragment.artifact_id == artifact.artifact_id
        assert fragment.artifact_sha256 == artifact.content_sha256
        assert fragment.text == region.text
        assert (fragment.start_char, fragment.end_char) == (region.start_char, region.end_char)
        assert fragment.coordinates == SOURCE_FIELD_COORDINATES
        assert fragment.evidence_grade == SOURCE_EXACT_EVIDENCE
        assert fragment.fragment_id.startswith("source_fragment_")
    assert len({fragment.fragment_id for fragment in fragments}) == len(fragments)


# --- native-first dispatch --------------------------------------------------


def test_dispatch_priority_is_declared_native_first_and_parser_last() -> None:
    assert DISPATCH_PRIORITY == (
        DISPATCH_STRUCTURED_FIELDS,
        DISPATCH_NATIVE_MARKUP,
        DISPATCH_ATOMIC_FIELDS,
        DISPATCH_NATIVE_PROSE,
        DISPATCH_CONTAINED_PARSER,
    )


def test_structured_json_fields_win_before_any_other_branch() -> None:
    record = SourceRecord(
        profile=LOBBYING_PROFILE,
        row={
            "filing_uuid": "FILING-2",
            "client_name": "Client",
            "registrant_name": "Registrant",
            "lobbying_activities_json": '[{"a":1},{"b":2}]',
            "government_entities_json": "[]",
        },
    )
    artifact = built(record)

    assert artifact.field_dispatch["lobbying_filings.lobbying_activities_json"] == DISPATCH_STRUCTURED_FIELDS
    assert artifact.field_dispatch["lobbying_filings.client_name"] == DISPATCH_ATOMIC_FIELDS
    assert artifact.parser_invoked is False


@pytest.mark.parametrize(
    ("table", "identifier_column", "identifier", "body_column", "body"),
    [
        (
            "federal_register",
            "document_number",
            "2026-12345",
            "body_html",
            "<article><h1>PFAS Rule</h1><section><p>Facilities must sample.</p></section></article>",
        ),
        (
            "congress_bills",
            "bill_id",
            "hr-123-119",
            "xml_text",
            "<bill><legis-body><section><enum>SEC. 1.</enum><header>Clean Water</header>"
            "<text>PFAS monitoring is required.</text></section></legis-body></bill>",
        ),
    ],
)
def test_native_markup_structure_wins_over_prose_splitting(
    table: str,
    identifier_column: str,
    identifier: str,
    body_column: str,
    body: str,
) -> None:
    record = SourceRecord(
        profile=profile_for_table(table),
        row={identifier_column: identifier, "title": "Source title", body_column: body},
    )
    artifact = built(record)

    source_field = f"{table}.{body_column}"
    regions = field_regions(artifact, source_field)

    assert artifact.field_dispatch[source_field] == DISPATCH_NATIVE_MARKUP
    assert artifact.field_dispatch[f"{table}.title"] == DISPATCH_ATOMIC_FIELDS
    assert "".join(region.text for region in regions) == body
    assert {region.kind for region in regions} >= {"heading", "paragraph"}
    assert any(region.parent_region_id for region in regions)


def test_native_prose_wins_when_a_body_field_carries_no_markup() -> None:
    artifact = built(comment_record("First paragraph.\n\nSecond paragraph."))

    assert artifact.field_dispatch["comments.comment"] == DISPATCH_NATIVE_PROSE
    assert artifact.dispatch == (DISPATCH_ATOMIC_FIELDS, DISPATCH_NATIVE_PROSE)


def test_an_existing_pypdf_text_field_stays_native_and_never_reaches_the_parser() -> None:
    parser = RecordedParser()
    record = SourceRecord(
        profile=GAO_PROFILE,
        row={
            "report_id": "GAO-26-1",
            "title": "Report title",
            "abstract": None,
            "report_type": None,
            "agencies_json": None,
            "full_text": None,
            "pdf_text": "Findings.\n\nRecommendations.",
        },
        attachments=(
            SourceAttachment(
                field_name="gao_reports.office_rendition",
                file_name="report.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )
    artifact = built(record, parser=parser)

    assert artifact.field_dispatch["gao_reports.pdf_text"] == DISPATCH_NATIVE_PROSE
    assert artifact.field_origins["gao_reports.pdf_text"] == SOURCE_NATIVE_FIELD
    assert all(
        region.evidence_grade == SOURCE_EXACT_EVIDENCE for region in field_regions(artifact, "gao_reports.pdf_text")
    )
    assert parser.calls == []
    assert artifact.parser_invoked is False
    assert artifact.parser is None


def test_usable_native_structure_beside_an_office_attachment_never_invokes_the_parser() -> None:
    parser = RecordedParser(office_result())
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={
            "comment_id": "COMMENT-2",
            "title": "Water policy",
            "comment": "The rule is workable.",
            "text_content": None,
            "organization": None,
            "category": None,
        },
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )
    artifact = built(record, parser=parser)

    assert parser.calls == []
    assert artifact.parser_invoked is False
    assert DISPATCH_CONTAINED_PARSER not in artifact.dispatch
    assert {(one.source_field, one.reason) for one in artifact.exclusions} >= {
        ("comments.office_rendition", "native-structure-preferred"),
    }


def test_the_contained_parser_serves_only_a_record_with_no_usable_native_text() -> None:
    parser = RecordedParser(office_result())
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={
            "comment_id": "COMMENT-3",
            "title": "  ",
            "comment": None,
            "text_content": None,
            "organization": None,
            "category": None,
        },
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )
    artifact = built(record, parser=parser)

    assert [call["source_name"] for call in parser.calls] == ["attachment.docx"]
    assert artifact.parser_invoked is True
    assert artifact.field_dispatch["comments.office_rendition"] == DISPATCH_CONTAINED_PARSER
    assert artifact.field_origins["comments.office_rendition"] == PARSER_DERIVED_FIELD
    regions = field_regions(artifact, "comments.office_rendition")
    assert "".join(region.text for region in regions) == OFFICE_TEXT
    assert all(region.evidence_grade == PARSER_DERIVED_EVIDENCE for region in regions)
    assert artifact.parser is not None
    assert artifact.parser.parser_id.startswith("docling:")
    assert artifact.parser.gate.classification == "completed"
    # Parser-derived coordinates stay labelled as addressing the parsed text.
    assert artifact.parser.offsets.target == "adapter-parsed-text"
    assert SOURCE_FIELD_COORDINATES.target == "artifact-source-field"


def test_a_deferred_pdf_or_image_rendition_is_refused_by_name_without_a_parse() -> None:
    parser = RecordedParser()
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={
            "comment_id": "COMMENT-4",
            "title": None,
            "comment": None,
            "text_content": None,
            "organization": None,
            "category": None,
        },
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.pdf",
                media_type="application/pdf",
                content=b"%PDF-1.7\n",
            ),
        ),
    )
    outcome = build_source_artifact(record, parser=parser)

    assert parser.calls == []
    assert outcome.state == "rejected"
    assert outcome.reason == "format_not_implemented"
    assert [one.reason for one in outcome.quarantine] == ["format_not_implemented"]
    assert "pdf" in outcome.quarantine[0].detail


# --- malformed input keeps honest coverage ---------------------------------


def test_malformed_markup_falls_back_to_prose_with_full_coverage() -> None:
    body = "<article><p>Unclosed paragraph\n\nSecond block <<< not markup"
    record = SourceRecord(
        profile=FEDERAL_REGISTER_PROFILE,
        row={"document_number": "2026-99999", "title": "Broken", "body_html": body},
    )
    artifact = built(record)

    regions = field_regions(artifact, "federal_register.body_html")
    coverage = next(one for one in artifact.coverage if one.source_field == "federal_register.body_html")

    assert "".join(region.text for region in regions) == body
    assert coverage.uncovered_chars == 0
    assert coverage.gaps == ()


def test_malformed_json_keeps_the_whole_field_as_one_covered_region() -> None:
    broken = '[{"general_issue_code":"ENV"'
    record = SourceRecord(
        profile=LOBBYING_PROFILE,
        row={
            "filing_uuid": "FILING-3",
            "client_name": "Client",
            "registrant_name": "Registrant",
            "lobbying_activities_json": broken,
            "government_entities_json": None,
        },
    )
    artifact = built(record)

    regions = field_regions(artifact, "lobbying_filings.lobbying_activities_json")
    coverage = next(one for one in artifact.coverage if one.source_field == "lobbying_filings.lobbying_activities_json")

    assert len(regions) == 1
    assert regions[0].text == broken
    assert regions[0].kind == "structured-field"
    assert coverage.uncovered_chars == 0
    assert coverage.covered_chars == len(broken)


# --- access, identity, and quarantine --------------------------------------


def test_every_profile_declares_an_explicit_access_scope_and_basis() -> None:
    assert all(profile.access.scope and profile.access.basis for profile in SOURCE_PROFILES)
    assert {profile.access for profile in SOURCE_PROFILES} == {PUBLIC}

    artifact = built(comment_record("Body text."))

    assert artifact.access == COMMENTS_PROFILE.access == PUBLIC
    assert artifact.access.declared is True
    assert UNDECLARED_ACCESS.declared is False


def test_access_scope_is_never_defaulted_by_a_record_or_a_profile() -> None:
    fields = {field.name: field for field in dataclasses.fields(SourceProfile)}
    access = fields["access"]

    assert access.default is dataclasses.MISSING
    assert access.default_factory is dataclasses.MISSING
    with pytest.raises(ValueError, match="access scope"):
        AccessScope(scope="", basis="us-federal-public-record")
    with pytest.raises(ValueError, match="access basis"):
        AccessScope(scope="public", basis="  ")


def test_an_undeclared_access_scope_quarantines_instead_of_assuming_public() -> None:
    profile = dataclasses.replace(COMMENTS_PROFILE, profile_id="comments-undeclared", access=UNDECLARED_ACCESS)
    record = SourceRecord(profile=profile, row={"comment_id": "COMMENT-5", "comment": "Body."})

    outcome = build_source_artifact(record)

    assert outcome.state == "rejected"
    assert outcome.reason == "unknown_access"
    assert outcome.artifact is None


def test_missing_identity_quarantines_and_never_invents_a_subject() -> None:
    record = SourceRecord(profile=COMMENTS_PROFILE, row={"comment_id": "   ", "comment": "Body."})

    outcome = build_source_artifact(record)

    assert outcome.state == "rejected"
    assert outcome.reason == "unknown_identity"
    assert outcome.artifact is None
    assert outcome.quarantine[0].reason == "unknown_identity"


def test_an_unnormalizable_docket_identifier_quarantines_rather_than_disappearing() -> None:
    record = SourceRecord(
        profile=DOCKET_PROFILE,
        row={"docket_id": "not a docket id!", "title": "Title", "abstract": "Abstract"},
    )

    outcome = build_source_artifact(record)

    assert outcome.state == "rejected"
    assert outcome.reason == "unknown_identity"


def test_a_policy_may_direct_an_unknown_reason_to_failure_instead_of_quarantine() -> None:
    policy = SourcePolicy(quarantine_reasons=frozenset())
    record = SourceRecord(profile=COMMENTS_PROFILE, row={"comment_id": "", "comment": "Body."})

    outcome = build_source_artifact(record, policy=policy)

    assert outcome.state == "failed"
    assert outcome.error == "unknown_identity"
    assert outcome.artifact is None


def test_a_record_with_no_usable_field_is_completed_empty_not_failed() -> None:
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={
            "comment_id": "COMMENT-6",
            "title": None,
            "comment": "   ",
            "text_content": None,
            "organization": None,
            "category": None,
        },
    )

    outcome = build_source_artifact(record)

    assert outcome.state == "completed_empty"
    assert outcome.artifact is not None
    assert outcome.artifact.regions == ()
    assert artifact_fragments(outcome.artifact) == ()
    assert {one.reason for one in outcome.artifact.exclusions} == {"null", "blank-non-content"}
    assert outcome.error == ""


def test_a_completed_record_carries_regions_and_a_completed_state() -> None:
    outcome = build_source_artifact(comment_record("Body text."))

    assert outcome.state == "completed"
    assert outcome.artifact is not None
    assert outcome.artifact.regions


def test_a_parser_failure_settles_as_declared_quarantine_or_failure() -> None:
    failed = ContainedParseResult(
        receipt=gate_receipt("wall_timeout", parser_status="", exit_status=None, signal_number=9),
        parsed=None,
        call=None,
    )
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={"comment_id": "COMMENT-7", "comment": None},
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )

    quarantined = build_source_artifact(record, parser=RecordedParser(failed))
    required = build_source_artifact(
        record, parser=RecordedParser(failed), policy=SourcePolicy(quarantine_reasons=frozenset())
    )

    assert quarantined.state == "rejected"
    assert quarantined.reason == "parser_failed"
    assert quarantined.quarantine[0].detail == "wall_timeout"
    assert required.state == "failed"
    assert required.error == "parser_failed"


def test_an_absent_parser_extra_settles_under_its_own_reason() -> None:
    """A parser that was never installed is not a parse that failed."""
    unavailable = ContainedParseResult(
        receipt=gate_receipt("parser_extra_unavailable", parser_status="unavailable", exit_status=0),
        parsed=None,
        call=None,
    )
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={"comment_id": "COMMENT-10", "comment": None},
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )

    outcome = build_source_artifact(record, parser=RecordedParser(unavailable))

    assert outcome.state == "rejected"
    assert outcome.reason == "parser_unavailable"
    assert outcome.quarantine[0].detail == "parser_extra_unavailable"


def test_a_disabled_parser_never_launches_a_process() -> None:
    parser = RecordedParser()
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={"comment_id": "COMMENT-11", "comment": None},
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="attachment.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )

    outcome = build_source_artifact(record, parser=parser, policy=SourcePolicy(parser_enabled=False))

    assert parser.calls == []
    assert outcome.state == "rejected"
    assert outcome.reason == "parser_disabled"


# --- secret scanning --------------------------------------------------------


def test_source_secret_matches_are_counted_without_repeating_the_text() -> None:
    secret = "sk-" + "A" * 40
    assert scan_text_for_secrets(secret) == ("openai-key-prefix",)
    artifact = built(comment_record(f"Contact the vendor.\n\nKey {secret} follows."))

    checks = source_checks([build_source_artifact(comment_record(f"Key {secret} here."))])
    scan = next(check for check in checks if check.name == "secret_scan")

    assert artifact.secret_rules == ("openai-key-prefix",)
    assert scan.status == "fail"
    assert "openai-key-prefix" in scan.detail
    assert secret not in scan.detail
    assert "AAAA" not in scan.detail
    for check in checks:
        assert secret not in check.detail


def test_a_clean_source_passes_the_secret_scan_check() -> None:
    checks = source_checks([build_source_artifact(comment_record("Ordinary body text."))])

    assert next(check for check in checks if check.name == "secret_scan").status == "pass"


# --- step checks ------------------------------------------------------------


def test_source_checks_report_coverage_identity_access_and_states() -> None:
    outcomes = [
        build_source_artifact(comment_record("Body text.")),
        build_source_artifact(SourceRecord(profile=COMMENTS_PROFILE, row={"comment_id": ""})),
    ]

    checks = source_checks(outcomes)

    assert all(isinstance(check, CheckResult) for check in checks)
    assert {check.step for check in checks} == {"source"}
    names = {check.name: check.status for check in checks}
    assert names["coverage_gap_free"] == "pass"
    assert names["region_text_matches_source"] == "pass"
    assert names["declared_access_scope"] == "pass"
    assert names["no_failed_work"] == "pass"
    assert "quarantined" in {check.name for check in checks}


def test_a_failed_outcome_makes_the_step_check_fail() -> None:
    record = SourceRecord(profile=COMMENTS_PROFILE, row={"comment_id": ""})
    outcomes = [build_source_artifact(record, policy=SourcePolicy(quarantine_reasons=frozenset()))]

    checks = source_checks(outcomes)

    assert next(check for check in checks if check.name == "no_failed_work").status == "fail"


# --- tables -----------------------------------------------------------------


def test_source_tables_are_written_with_declared_schemas(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    outcomes = [
        build_source_artifact(comment_record(UNICODE_BODY)),
        build_source_artifact(SourceRecord(profile=COMMENTS_PROFILE, row={"comment_id": ""})),
    ]

    written = write_source_tables(tmp_path, outcomes)

    assert set(written) == {
        ARTIFACT_TABLE,
        FRAGMENT_TABLE,
        COVERAGE_TABLE,
        PARSER_ATTEMPT_TABLE,
    }
    for relative, columns in (
        (ARTIFACT_TABLE, ARTIFACT_COLUMNS),
        (FRAGMENT_TABLE, FRAGMENT_COLUMNS),
        (COVERAGE_TABLE, COVERAGE_COLUMNS),
        (PARSER_ATTEMPT_TABLE, PARSER_ATTEMPT_COLUMNS),
    ):
        table = pq.read_table(tmp_path / relative)
        assert table.column_names == [name for name, _ in columns]
    artifacts = pq.read_table(tmp_path / ARTIFACT_TABLE).to_pylist()
    fragments = pq.read_table(tmp_path / FRAGMENT_TABLE).to_pylist()
    built_artifact = outcomes[0].artifact
    assert built_artifact is not None
    assert len(artifacts) == 1
    assert artifacts[0]["access_scope"] == "public"
    assert artifacts[0]["source_policy_version"] == SOURCE_POLICY_VERSION
    assert len(fragments) == len(artifact_fragments(built_artifact))
    assert {row["artifact_id"] for row in fragments} == {artifacts[0]["artifact_id"]}


def test_an_empty_requested_output_still_has_the_correct_schema(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    written = write_source_tables(tmp_path, [])

    for relative, columns in (
        (ARTIFACT_TABLE, ARTIFACT_COLUMNS),
        (FRAGMENT_TABLE, FRAGMENT_COLUMNS),
        (COVERAGE_TABLE, COVERAGE_COLUMNS),
        (PARSER_ATTEMPT_TABLE, PARSER_ATTEMPT_COLUMNS),
    ):
        table = pq.read_table(written[relative])
        assert table.num_rows == 0
        assert table.column_names == [name for name, _ in columns]


# --- profile scope ----------------------------------------------------------


def _write_profile_fixture(root: Path) -> None:
    for profile in SOURCE_PROFILES:
        columns = list(
            dict.fromkeys(
                (
                    *profile.id_columns,
                    *profile.text_columns,
                    *(("fr_doc_num",) if profile.source_table == "documents" else ()),
                )
            )
        )
        row = {column: f"{profile.source_table}-{column}" for column in columns}
        if profile.source_table == "documents":
            row["fr_doc_num"] = "2026-00001"
        if profile.source_table == "federal_register":
            row["document_number"] = "2026-00001"
        write_parquet_rows(root / f"{profile.source_table}.parquet", columns=columns, rows=[row])


def test_the_profile_registry_keeps_the_whole_migrated_source_scope(tmp_path: Path) -> None:
    _write_profile_fixture(tmp_path)

    outcomes = build_source_artifacts(tmp_path)

    assert {profile.source_table for profile in SOURCE_PROFILES}.isdisjoint(EXCLUDED_SOURCE_TABLES)
    assert all(EXCLUDED_SOURCE_TABLES.values())
    assert {outcome.artifact.profile_id for outcome in outcomes if outcome.artifact} == {
        profile.profile_id for profile in SOURCE_PROFILES
    }
    assert all(outcome.state in {"completed", "completed_empty"} for outcome in outcomes)


def test_a_missing_required_source_table_is_refused_before_any_work(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="gao_reports.parquet"):
        build_source_artifacts(tmp_path, required_source_tables={"gao_reports"})


def test_a_new_document_family_needs_only_a_profile_and_its_examples(tmp_path: Path) -> None:
    """The design rule: one source adapter plus a profile, and nothing else."""
    profile = SourceProfile(
        profile_id="new-family-v1",
        source_table="new_family",
        subject_type="new_family_record",
        id_columns=("record_id",),
        text_columns=("title", "body_text"),
        allowed_schemes=("subject",),
        mode="hierarchical-document",
        access=AccessScope(scope="public", basis="state-public-record"),
    )
    record = SourceRecord(
        profile=profile,
        row={"record_id": "NF-1", "title": "Title", "body_text": "First.\n\nSecond."},
    )

    artifact = built(record)

    assert artifact.profile_id == "new-family-v1"
    assert artifact.access.basis == "state-public-record"
    assert len(field_regions(artifact, "new_family.body_text")) == 2
    assert artifact_fragments(artifact)


# --- import rules -----------------------------------------------------------

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = REPOSITORY_ROOT / "src" / "spicy_regs" / "docpipeline" / "source.py"

FORBIDDEN_SOURCE_IMPORTS = (
    "spicy_regs.corpora",
    "spicy_regs.ontology.subjects",
    "spicy_regs.ontology.adapters",
    "spicy_regs.ontology.segmentation",
    "spicy_regs.docpipeline.segments",
    "spicy_regs.docpipeline.retrieval",
    "spicy_regs.docpipeline.extraction",
    "spicy_regs.docpipeline.workflow",
    "spicy_regs.docpipeline.cli",
    "docling",
    "docling_core",
    "duckdb",
    "openai",
    "sentence_transformers",
)


def imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return modules


def test_source_imports_no_replaced_runner_and_no_provider_package() -> None:
    modules = imported_modules(SOURCE_PATH)

    for module in sorted(modules):
        assert not any(module == name or module.startswith(f"{name}.") for name in FORBIDDEN_SOURCE_IMPORTS), module
    assert "spicy_regs.docpipeline.adapters.docling" in modules


@pytest.fixture
def docling_uninstalled(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    blocked = ("docling", "docling_core")

    class BlockedFinder:
        def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
            if any(fullname == name or fullname.startswith(f"{name}.") for name in blocked):
                raise ImportError(f"{fullname} is deliberately unavailable in these tests")
            return None

    for name in [name for name in sys.modules if name.startswith(blocked)]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "spicy_regs.docpipeline.adapters.docling", raising=False)
    monkeypatch.setattr(sys, "meta_path", [BlockedFinder(), *sys.meta_path])
    yield


def test_source_imports_and_parses_native_structure_without_docling_installed(
    docling_uninstalled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from importlib import import_module, reload

    monkeypatch.delitem(sys.modules, "spicy_regs.docpipeline.source", raising=False)
    reloaded = import_module("spicy_regs.docpipeline.source")
    reload(reloaded)

    outcome = reloaded.build_source_artifact(
        reloaded.SourceRecord(
            profile=reloaded.profile_for_table("comments"),
            row={"comment_id": "COMMENT-8", "comment": "First.\n\nSecond."},
        )
    )

    assert outcome.state == "completed"
    assert outcome.artifact is not None
    assert len(outcome.artifact.regions) == 2


# --- mutation probes --------------------------------------------------------
#
# Each probe breaks one rule the module is supposed to hold and proves a test
# above notices. A probe that still passes means the rule is not really checked.


def test_probe_inclusive_end_offsets_are_caught() -> None:
    artifact = built(comment_record("First.\n\nSecond."))
    region = artifact.regions[-1]
    field_text = artifact.raw_fields[region.source_field]

    inclusive = dataclasses.replace(region, end_char=region.end_char - 1)

    assert inclusive.text != field_text[inclusive.start_char : inclusive.end_char]
    with pytest.raises(source_module.SourceError, match="exact source slice"):
        source_module.check_region_coordinates(artifact, regions=(inclusive,))


def test_probe_a_region_without_a_coordinate_system_is_refused() -> None:
    with pytest.raises(ValueError, match="coordinate"):
        source_module.CoordinateSystem(target="artifact-source-field", unit="", interval="half-open")
    with pytest.raises(ValueError, match="coordinate"):
        source_module.CoordinateSystem(target="artifact-source-field", unit="bytes", interval="closed")


def test_probe_a_wrong_digest_coverage_claim_is_caught() -> None:
    artifact = built(comment_record("First.\n\nSecond."))
    region = field_regions(artifact, "comments.comment")[0]

    # A field digest that covers the region text instead of the whole field.
    assert region.text != artifact.raw_fields["comments.comment"]
    wrong = dataclasses.replace(region, field_sha256=region.text_sha256)

    with pytest.raises(source_module.SourceError, match="field digest"):
        source_module.check_region_digests(artifact, regions=(wrong,))


def test_probe_syntax_promoted_to_durable_is_caught() -> None:
    body = '<?xml version="1.0"?>\n<article><p>Body.</p></article>'
    artifact = built(
        SourceRecord(
            profile=FEDERAL_REGISTER_PROFILE,
            row={"document_number": "2026-1", "title": "T", "body_html": body},
        )
    )
    prolog = next(region for region in artifact.regions if region.kind == "markup-prolog")

    promoted = dataclasses.replace(
        artifact,
        regions=tuple(
            dataclasses.replace(region, durability=DURABLE_MEANINGFUL) if region is prolog else region
            for region in artifact.regions
        ),
    )

    assert prolog.durability == SYNTAX_REGION
    assert any(fragment.kind == "markup-prolog" for fragment in artifact_fragments(promoted))
    assert not any(fragment.kind == "markup-prolog" for fragment in artifact_fragments(artifact))


def test_probe_a_native_first_bypass_would_show_in_the_dispatch_record() -> None:
    parser = RecordedParser(office_result())
    record = SourceRecord(
        profile=COMMENTS_PROFILE,
        row={"comment_id": "COMMENT-9", "comment": "Native body."},
        attachments=(
            SourceAttachment(
                field_name="comments.office_rendition",
                file_name="a.docx",
                media_type=OFFICE_MEDIA_TYPE,
                content=OFFICE_BYTES,
            ),
        ),
    )

    artifact = built(record, parser=parser)

    assert artifact.dispatch and DISPATCH_CONTAINED_PARSER not in artifact.dispatch
    assert artifact.parser_invoked is False
    assert parser.calls == []


def test_probe_source_never_calls_docling_in_process() -> None:
    text = SOURCE_PATH.read_text(encoding="utf-8")

    # The parser runs behind a process boundary, so the in-process entry point is
    # never named here and the gate really launches a child.
    assert "DoclingDocumentParser" not in text
    assert "subprocess.Popen" in text
    assert "start_new_session=True" in text

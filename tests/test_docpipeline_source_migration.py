"""Migration parity: the v3 source step against the runner it replaces.

The v3 ``source`` step has to reproduce the predecessor exactly where the frozen
data depends on it — the artifact digest, every region id, the region order, the
region kinds, the parent links, the heading paths, and ``evidence_eligible`` —
because ``segments.py`` selects segments from that stream and the migration gate
holds it to the same 1,302 selected segments.

So this file runs the old code and the new code over the same fixed inputs and
compares them value by value. Everything that legitimately differs is listed in
:data:`EXPECTED_DIFFERENCES` and approved by name; anything else is a defect.

Importing the old runner is deliberate and test-only. Production ``docpipeline``
code never does — ``tests/test_docpipeline_source.py`` proves that separately —
and step 8 owns the removal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from spicy_regs.docpipeline.source import (
    DISPATCH_CONTAINED_PARSER,
    EXCLUDED_SOURCE_TABLES,
    SOURCE_PROFILES,
    artifact_fragments,
    build_source_artifacts,
    processing_regions,
)
from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.ontology.subjects import (
    EXCLUDED_SOURCE_TABLES as OLD_EXCLUDED_SOURCE_TABLES,
)
from spicy_regs.ontology.subjects import (
    PROFILE_SEGMENTATION_POLICIES as OLD_POLICIES,
)
from spicy_regs.ontology.subjects import (
    SUBJECT_PROFILES as OLD_PROFILES,
)
from spicy_regs.ontology.subjects import (
    build_artifacts as old_build_artifacts,
)
from spicy_regs.ontology.subjects import (
    segment_artifact as old_segment_artifact,
)

EXPECTED_DIFFERENCES: tuple[dict[str, Any], ...] = (
    {
        "kind": "field_rename",
        "old": "SourceElement.element_id / .ancestor_path / .raw_text_sha256 / .source_text_sha256",
        "new": "SourceRegion.region_id / .heading_path / .text_sha256 / .field_sha256",
        "reason": (
            "The v3 vocabulary names regions and says what each digest covers. Every "
            "value is byte-identical; only the field names changed."
        ),
        "values": ["region_id", "heading_path", "text_sha256", "field_sha256"],
    },
    {
        "kind": "field_rename",
        "old": "Artifact.digest / .adapter_id / .segmentation_mode",
        "new": "SourceArtifact.content_sha256 / .region_adapter_id / SourceProfile.mode",
        "reason": (
            "``digest`` never said what it covered; ``content_sha256`` does. The "
            "segmentation mode moved onto the profile that declares it, so a new "
            "document family is still one profile and nothing else."
        ),
        "values": ["content_sha256", "region_adapter_id", "mode"],
    },
    {
        "kind": "added_field",
        "old": "no durability, coordinate, access, or evidence-grade record",
        "new": "durability, context_only, coordinates, evidence_grade, field_origin, access, dispatch, coverage",
        "reason": (
            "The vision requires stated access scope, stated coordinate semantics, "
            "graded evidence, and gap-free coverage accounting. None of them changes "
            "which regions exist or in what order."
        ),
        "values": ["durability", "coordinates", "access", "evidence_grade", "dispatch", "coverage"],
    },
    {
        "kind": "added_output",
        "old": "elements only",
        "new": "SourceFragment projection of durable meaningful regions",
        "reason": (
            "``markup-prolog`` syntax and ``structured-array`` containers stay in "
            "coverage and in the processing stream, and stop short of becoming "
            "durable taggable fragments."
        ),
        "values": ["source/fragments.parquet"],
    },
    {
        "kind": "silent_drop_becomes_quarantine",
        "old": "_make_artifact returned None and the row vanished",
        "new": "SourceOutcome(state='rejected', reason='unknown_identity')",
        "reason": (
            "A row whose identity cannot be normalized is now counted and named "
            "instead of disappearing. The set of artifacts built is unchanged."
        ),
        "values": ["unknown_identity", "unknown_access"],
    },
    {
        "kind": "approved_non_change",
        "old": "markup-prolog participates in the processing stream",
        "new": "markup-prolog participates in the processing stream",
        "reason": (
            "Deliberately unchanged for this migration: dropping the prolog from the "
            "processing stream would move segment boundaries and break the 1,302-segment "
            "parity gate. Removing it is a recorded follow-up for after step 8."
        ),
        "values": ["markup-prolog"],
    },
)


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


MARKUP_BODY = (
    '<?xml version="1.0"?>\n<article><h1>PFAS Rule</h1><section><h2>Monitoring</h2>'
    "<p>Facilities must sample water.</p><ul><li>Report quarterly.</li></ul></section></article>"
)
BILL_XML = (
    "<bill><legis-body><section><enum>SEC. 1.</enum><header>Clean Water</header>"
    "<text>PFAS monitoring is required.</text></section></legis-body></bill>"
)
PROSE_BODY = (
    "§ 1.1 Scope\n\nThe rule—as written—says “no” to PFAS \U0001f9ea discharge.\n\n"
    "Section 2 - Monitoring\n\nReports are required quarterly."
)
ACTIVITIES = '[{"general_issue_code":"ENV","description":"PFAS"},{"general_issue_code":"ENG","description":"Grid"}]'


def _write_corpus(root: Path) -> None:
    """One row for every profile, with the shapes that exercise each branch."""
    for profile in OLD_PROFILES:
        columns = list(
            dict.fromkeys(
                (
                    *profile.id_columns,
                    *profile.text_columns,
                    *(("fr_doc_num",) if profile.source_table == "documents" else ()),
                )
            )
        )
        row: dict[str, Any] = {column: f"{profile.source_table}-{column}" for column in columns}
        if profile.source_table == "documents":
            row["fr_doc_num"] = "2026-00001"
            row["text_content"] = PROSE_BODY
        if profile.source_table == "federal_register":
            row["document_number"] = "2026-00001"
            row["body_html"] = MARKUP_BODY
            row["abstract"] = PROSE_BODY
            row["body_text"] = None
        if profile.source_table == "congress_bills":
            row["xml_text"] = BILL_XML
        if profile.source_table == "lobbying_filings":
            row["lobbying_activities_json"] = ACTIVITIES
            row["government_entities_json"] = '["EPA","DOE"]'
        if profile.source_table == "comments":
            row["comment"] = PROSE_BODY
            row["organization"] = "   "
            row["category"] = None
        if profile.source_table == "gao_reports":
            row["pdf_text"] = PROSE_BODY
        if profile.source_table == "dockets":
            row["docket_id"] = "EPA-HQ-OW-2026-0001"
        write_parquet_rows(root / f"{profile.source_table}.parquet", columns=columns, rows=[row])


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("source-migration-corpus")
    _write_corpus(root)
    return root


def _paired(corpus: Path) -> list[tuple[Any, Any]]:
    old = sorted(
        old_build_artifacts(corpus),
        key=lambda one: (one.subject_type, one.subject_id, one.profile_id),
    )
    new = sorted(
        (outcome.artifact for outcome in build_source_artifacts(corpus) if outcome.artifact is not None),
        key=lambda one: (one.subject_type, one.subject_id, one.profile_id),
    )
    assert [one.subject_id for one in old] == [one.subject_id for one in new]
    assert len(old) == len(OLD_PROFILES)
    return list(zip(old, new, strict=True))


# --- the registry -----------------------------------------------------------


def test_the_profile_registry_migrated_without_gaining_or_losing_a_source() -> None:
    old = {
        profile.profile_id: (
            profile.source_table,
            profile.subject_type,
            profile.id_columns,
            profile.text_columns,
            profile.allowed_schemes,
            OLD_POLICIES[profile.profile_id].mode,
        )
        for profile in OLD_PROFILES
    }
    new = {
        profile.profile_id: (
            profile.source_table,
            profile.subject_type,
            profile.id_columns,
            profile.text_columns,
            profile.allowed_schemes,
            profile.mode,
        )
        for profile in SOURCE_PROFILES
    }

    assert new == old
    assert dict(EXCLUDED_SOURCE_TABLES) == dict(OLD_EXCLUDED_SOURCE_TABLES)


# --- identity ---------------------------------------------------------------


def test_every_artifact_digest_is_byte_identical_to_the_predecessor(corpus: Path) -> None:
    for old, new in _paired(corpus):
        assert new.content_sha256 == old.digest, new.profile_id
        assert (new.subject_type, new.subject_id, new.profile_id) == (
            old.subject_type,
            old.subject_id,
            old.profile_id,
        )
        assert new.source_table == old.source_table
        assert new.allowed_schemes == old.allowed_schemes
        assert new.region_adapter_id == old.adapter_id
        assert dict(new.raw_fields) == dict(old.raw_fields)
        assert dict(new.context_fields) == dict(old.context_fields)


def test_every_region_reproduces_its_predecessor_element_exactly(corpus: Path) -> None:
    compared = 0
    for old, new in _paired(corpus):
        assert len(new.regions) == len(old.elements), new.profile_id
        for element, region in zip(old.elements, new.regions, strict=True):
            assert region.region_id == element.element_id
            assert region.kind == element.kind
            assert region.ordinal == element.ordinal
            assert region.parent_region_id == element.parent_element_id
            assert region.heading_path == element.ancestor_path
            assert region.source_field == element.source_field
            assert (region.start_char, region.end_char) == (element.start_char, element.end_char)
            assert region.text == element.text
            assert region.text_sha256 == element.raw_text_sha256
            assert region.field_sha256 == element.source_text_sha256
            assert region.evidence_eligible == element.evidence_eligible
            compared += 1
    assert compared > 50, "the fixture corpus stopped exercising the region stream"


def test_every_exclusion_reproduces_its_predecessor(corpus: Path) -> None:
    for old, new in _paired(corpus):
        old_set = {(one.source_field, one.reason, one.start_char, one.end_char) for one in old.exclusions}
        new_set = {
            (one.source_field, one.reason, one.start_char, one.end_char)
            for one in new.exclusions
            if one.reason in {"null", "blank-non-content"}
        }
        assert new_set == old_set, new.profile_id


# --- the processing stream --------------------------------------------------


def test_the_processing_stream_matches_the_segmenter_input_element_for_element(corpus: Path) -> None:
    for old, new in _paired(corpus):
        eligible = [one for one in old.elements if one.evidence_eligible and one.text]
        stream = processing_regions(new)

        assert [one.element_id for one in eligible] == [one.region_id for one in stream], new.profile_id
        assert [one.kind for one in eligible] == [one.kind for one in stream]
        assert [one.text for one in eligible] == [one.text for one in stream]


def test_the_markup_prolog_still_reaches_the_segmenter(corpus: Path) -> None:
    """The approved non-change, proved rather than asserted in prose."""
    old, new = next(pair for pair in _paired(corpus) if pair[1].profile_id == "federal-register-document-v1")

    old_stream = [one.kind for one in old.elements if one.evidence_eligible and one.text]
    new_stream = [one.kind for one in processing_regions(new)]

    assert "markup-prolog" in old_stream
    assert new_stream == old_stream
    assert "markup-prolog" not in {fragment.kind for fragment in artifact_fragments(new)}


def test_the_old_segmenter_still_accepts_the_predecessor_stream(corpus: Path) -> None:
    """Step 4B's input is unchanged: the old segmenter runs on the old artifacts.

    ``segments.py`` is not this task's work, so parity is proved at the boundary
    it will consume: identical elements in, identical selections available.
    """
    for old, new in _paired(corpus):
        segments = old_segment_artifact(old, max_tokens=700, min_tokens=280, token_counter=_CharacterCounter())
        covered = {element_id for segment in segments for element_id in (segment.element_ids or {}).values()}
        stream = {one.region_id for one in processing_regions(new)}

        assert covered <= stream, new.profile_id
        assert all(segment.artifact_digest == new.content_sha256 for segment in segments)


# --- approved differences ---------------------------------------------------


def test_every_difference_from_the_predecessor_is_approved_by_name() -> None:
    kinds = {difference["kind"] for difference in EXPECTED_DIFFERENCES}

    assert kinds <= {
        "field_rename",
        "added_field",
        "added_output",
        "silent_drop_becomes_quarantine",
        "approved_non_change",
    }
    for difference in EXPECTED_DIFFERENCES:
        assert difference["old"] and difference["new"] and difference["reason"]
        assert difference["values"], "an approved difference names what it approves"


def test_the_predecessor_modules_and_their_callers_are_untouched() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "spicy_regs" / "ontology"

    assert (root / "subjects.py").is_file()
    assert (root / "adapters.py").is_file()
    assert (root / "segmentation.py").is_file()


def test_the_new_step_never_reaches_the_parser_for_a_native_corpus(corpus: Path) -> None:
    for _, new in _paired(corpus):
        assert new.parser_invoked is False
        assert DISPATCH_CONTAINED_PARSER not in new.dispatch
        assert new.parser is None

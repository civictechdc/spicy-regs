"""The v3 ``segment`` step: the selected ``structure-overlap-1800`` behavior.

These are the focused behavior tests. Migration parity against the runner this
step replaces lives in ``tests/test_docpipeline_segments_migration.py``, and the
real frozen-data gate lives in
``tests/test_docpipeline_segments_frozen_local.py``.

Most tests here drive a character counter rather than the real tokenizer, so a
budget is exact and a boundary case can be written down. The tests that must
hold at the *selected* settings say so by name.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from spicy_regs.docpipeline.segments import (
    BOUNDARY_METHOD,
    SEGMENT_COLUMNS,
    SEGMENT_TABLE,
    SELECTED_MAX_TOKENS,
    SELECTED_MIN_TOKENS,
    SELECTED_OVERLAP_TOKENS,
    SELECTED_POLICY,
    SELECTED_TOKENIZER,
    SegmentError,
    SegmentSettings,
    contains_span,
    overlaps_span,
    segment_artifact,
    segment_artifacts,
    segment_checks,
    segment_rows,
    summarize_segments,
    write_segment_table,
)
from spicy_regs.docpipeline.source import (
    SOURCE_FIELD_COORDINATES,
    SourceRecord,
    artifact_fragments,
    build_source_artifact,
    processing_regions,
    profile_for_table,
)


class _CharacterCounter:
    """One character is one token, so every budget below is exact."""

    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()


def _settings(**overrides: Any) -> SegmentSettings:
    base: dict[str, Any] = {
        "max_tokens": 100,
        "min_tokens": 40,
        "overlap_tokens": 10,
        "tokenizer": COUNTER.name,
        "tokenizer_version": COUNTER.version,
    }
    return SegmentSettings(**{**base, **overrides})


def _artifact(table: str, row: dict[str, Any]) -> Any:
    outcome = build_source_artifact(SourceRecord(profile=profile_for_table(table), row=row))
    assert outcome.artifact is not None, outcome.reason or outcome.error
    return outcome.artifact


def _cfr(**fields: Any) -> Any:
    return _artifact("cfr_sections", {"granule_id": "CFR-fixture-sec-1", **fields})


# --------------------------------------------------------------------------
# settings identity
# --------------------------------------------------------------------------


def test_the_selected_settings_are_exactly_the_comparison_winner() -> None:
    """``structure-overlap-1800`` as the fair comparison chose it, by value."""
    settings = SegmentSettings.selected(tokenizer_version="0.13.0")

    assert (settings.policy, settings.max_tokens, settings.min_tokens) == (SELECTED_POLICY, 1800, 720)
    assert settings.overlap_tokens == 80
    assert settings.tokenizer == SELECTED_TOKENIZER == "o200k_base"
    assert settings.boundary_method == BOUNDARY_METHOD == "source-native-oversized-overlap"
    assert (SELECTED_MAX_TOKENS, SELECTED_MIN_TOKENS, SELECTED_OVERLAP_TOKENS) == (1800, 720, 80)
    assert settings.leaf_budget == 1720


@pytest.mark.parametrize(
    "field",
    [
        "policy",
        "policy_version",
        "max_tokens",
        "min_tokens",
        "overlap_tokens",
        "tokenizer",
        "tokenizer_version",
        "boundary_method",
    ],
)
def test_changing_any_part_of_the_settings_changes_the_settings_digest(field: str) -> None:
    settings = SegmentSettings.selected(tokenizer_version="0.13.0")
    changed = {
        "policy": "structure-first",
        "policy_version": "structure-overlap-v2",
        "max_tokens": 1200,
        "min_tokens": 480,
        "overlap_tokens": 40,
        "tokenizer": "cl100k_base",
        "tokenizer_version": "0.14.0",
        "boundary_method": "different-boundary-method",
    }[field]

    other = SegmentSettings(**{**settings.identity(), field: changed})  # ty: ignore[invalid-argument-type]

    assert other.digest != settings.digest
    assert settings.digest == SegmentSettings.selected(tokenizer_version="0.13.0").digest


def test_a_settings_record_refuses_a_budget_it_cannot_serve() -> None:
    with pytest.raises(ValueError):
        _settings(min_tokens=200)
    with pytest.raises(ValueError):
        _settings(max_tokens=0)
    with pytest.raises(ValueError):
        _settings(max_tokens=10, min_tokens=5, overlap_tokens=10)
    for field in ("policy", "policy_version", "tokenizer", "tokenizer_version", "boundary_method"):
        with pytest.raises(ValueError):
            _settings(**{field: ""})


def test_a_counter_that_is_not_the_one_the_settings_name_is_refused() -> None:
    artifact = _cfr(heading="Scope.", text="Facilities must sample water.")

    with pytest.raises(SegmentError):
        segment_artifact(artifact, settings=_settings(tokenizer="o200k_base"), counter=COUNTER)


# --------------------------------------------------------------------------
# exact source slices
# --------------------------------------------------------------------------


def test_every_slice_round_trips_as_a_half_open_unicode_codepoint_span() -> None:
    body = "§ 1.1 Scope\n\nThe rule—as written—says “no” to PFAS \U0001f9ea discharge.\n\nReports are quarterly."
    artifact = _cfr(heading="Oral hearing.", text=body)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    seen = 0
    for segment in outcome.segments:
        for one in segment.slices:
            field_text = artifact.raw_fields[one.source_field]
            assert one.text == field_text[one.start_char : one.end_char]
            assert one.char_count == one.end_char - one.start_char == len(one.text)
            assert one.coordinates == SOURCE_FIELD_COORDINATES
            assert one.coordinates.interval == "half-open"
            assert one.text_sha256 == hashlib.sha256(one.text.encode()).hexdigest()
            assert one.field_sha256 == artifact.field_sha256[one.source_field]
            seen += 1
    assert seen >= 4, "the fixture stopped exercising several slices"


def test_a_slice_binds_the_artifact_the_region_and_the_durable_fragment() -> None:
    artifact = _cfr(heading="Oral hearing.", text="Facilities must sample water quarterly.")
    fragments = {fragment.region_id: fragment.fragment_id for fragment in artifact_fragments(artifact)}

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    regions = {region.region_id: region for region in processing_regions(artifact)}
    for segment in outcome.segments:
        assert segment.artifact_id == artifact.artifact_id
        assert segment.artifact_sha256 == artifact.content_sha256
        for one in segment.slices:
            assert one.region_id in regions
            assert one.region_kind == regions[one.region_id].kind
            assert one.fragment_id == fragments.get(one.region_id)


def test_a_container_region_never_becomes_a_slice_but_its_children_do() -> None:
    """A JSON array is a container; the meaning is in its elements."""
    artifact = _artifact(
        "lobbying_filings",
        {
            "filing_uuid": "0000-1111",
            "client_name": "Example Client",
            "registrant_name": "Example Registrant",
            "lobbying_activities_json": '[{"issue":"ENV"},{"issue":"ENG"}]',
            "government_entities_json": '["EPA","DOE"]',
        },
    )

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    kinds = {one.region_kind for segment in outcome.segments for one in segment.slices}
    assert "structured-child" in kinds
    assert "structured-array" not in kinds
    excluded = {one.reason for one in outcome.excluded}
    assert excluded == {"region-not-evidence-eligible"}


def test_the_step_skips_exactly_the_regions_the_source_step_holds_back() -> None:
    artifact = _artifact(
        "lobbying_filings",
        {
            "filing_uuid": "0000-2222",
            "client_name": "Example Client",
            "registrant_name": "Example Registrant",
            "lobbying_activities_json": '[{"issue":"ENV"},{"issue":"ENG"}]',
        },
    )

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    consumed = [one.region_id for segment in outcome.segments for one in segment.slices]
    stream = [region.region_id for region in processing_regions(artifact)]
    assert sorted(set(consumed)) == sorted(stream)
    held = {region.region_id for region in artifact.regions} - set(stream)
    assert {one.region_id for one in outcome.excluded} == held


# --------------------------------------------------------------------------
# whole regions, split regions, overlap
# --------------------------------------------------------------------------


def test_a_region_at_or_below_the_budget_stays_whole() -> None:
    body = "a" * 100
    artifact = _cfr(text=body)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert len(outcome.segments) == 1
    (one,) = outcome.segments[0].slices
    assert (one.start_char, one.end_char) == (0, 100)
    assert one.overlap_chars == 0


def test_an_oversized_region_splits_at_the_leaf_budget_not_the_whole_budget() -> None:
    body = "a" * 260
    artifact = _cfr(text=body)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    leaves = [one for segment in outcome.segments for one in segment.slices]
    assert leaves[0].end_char == 90, "the first leaf gets max_tokens minus the overlap budget"
    assert all(segment.token_count <= 100 for segment in outcome.segments)


def test_a_later_leaf_reaches_backward_at_most_the_overlap_budget() -> None:
    body = "a" * 260
    artifact = _cfr(text=body)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    leaves = [one for segment in outcome.segments for one in segment.slices]
    assert leaves[0].overlap_chars == 0
    for previous, current in zip(leaves, leaves[1:], strict=False):
        assert current.overlap_chars == 10, "the overlap reaches back exactly the overlap budget"
        assert current.start_char == previous.end_char - current.overlap_chars


def test_the_overlap_never_crosses_the_source_region_boundary() -> None:
    """The first leaf of a region may not borrow from the region before it."""
    artifact = _cfr(heading="Oral hearing.", text="b" * 260)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    regions = {region.region_id: region for region in processing_regions(artifact)}
    for segment in outcome.segments:
        for one in segment.slices:
            region = regions[one.region_id]
            assert one.start_char >= region.start_char
            assert one.end_char <= region.end_char
    first_leaf = next(
        one for segment in outcome.segments for one in segment.slices if one.source_field.endswith(".text")
    )
    assert first_leaf.start_char == regions[first_leaf.region_id].start_char
    assert first_leaf.overlap_chars == 0


def test_a_split_region_leaf_always_occupies_its_own_segment() -> None:
    artifact = _cfr(heading="Oral hearing.", text="c" * 260)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    split = [segment for segment in outcome.segments if any(one.overlap_chars for one in segment.slices)]
    assert split, "the fixture stopped producing a split region"
    for segment in outcome.segments:
        fields = {one.source_field for one in segment.slices}
        if any(one.source_field.endswith(".text") for one in segment.slices):
            assert fields == {"cfr_sections.text"}, "a split leaf never shares a segment"
            assert len(segment.slices) == 1


def test_regions_within_the_budget_pack_greedily_until_the_budget_is_reached() -> None:
    """Packing counts the newline-joined processing text, exactly as selected."""
    paragraphs = "\n\n".join("d" * 30 for _ in range(6))
    artifact = _cfr(text=paragraphs)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert len(outcome.segments) > 1, "six 32-character paragraphs cannot fit one 100-token segment"
    for segment in outcome.segments:
        assert segment.token_count == len("\n".join(one.text for one in segment.slices))
        assert segment.token_count <= 100


def test_greedy_packing_does_not_break_merely_because_a_field_repeats() -> None:
    """The selected path has no same-field break; adding one would move boundaries."""
    paragraphs = "\n\n".join("e" * 10 for _ in range(3))
    artifact = _cfr(text=paragraphs)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert len(outcome.segments) == 1
    assert len(outcome.segments[0].slices) == 3
    assert len({one.source_field for one in outcome.segments[0].slices}) == 1


def test_unbroken_text_with_no_break_candidate_still_splits_within_the_budget() -> None:
    artifact = _cfr(text="f" * 500)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert len(outcome.segments) > 1
    assert all(segment.token_count <= 100 for segment in outcome.segments)
    covered = [(one.start_char, one.end_char) for segment in outcome.segments for one in segment.slices]
    assert covered[0][0] == 0
    assert covered[-1][1] == 500


def test_no_segment_ever_exceeds_the_hard_token_limit() -> None:
    artifact = _cfr(heading="Oral hearing.", text="\n\n".join("g" * 45 for _ in range(9)) + "\n\n" + "h" * 400)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert outcome.segments
    for segment in outcome.segments:
        assert segment.token_count <= segment.input_limit == 100
        assert segment.truncated is False
    assert summarize_segments([outcome]).overflow_count == 0


# --------------------------------------------------------------------------
# context stays separate
# --------------------------------------------------------------------------


def test_context_never_enters_evidence_text_identity_or_the_token_count() -> None:
    artifact = _cfr(heading="Oral hearing.", text="Facilities must sample water.")

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    segment = outcome.segments[0]

    assert segment.context.artifact_context == {"artifact_title": "Oral hearing."}
    assert segment.text == "\n".join(one.text for one in segment.slices)
    assert "Oral hearing." not in segment.text or any("Oral hearing." in one.text for one in segment.slices)
    assert segment.token_count == COUNTER.count(segment.text)
    assert "artifact_title" not in segment.identity()
    assert "context" not in segment.identity()
    assert "context" not in segment.content_identity()


def test_a_heading_region_still_reaches_the_segmenter_as_a_processing_slice() -> None:
    """Deliberately unchanged for this migration; the cleanup is a follow-up."""
    artifact = _cfr(heading="Oral hearing.", text="Facilities must sample water.")

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    kinds = [one.region_kind for segment in outcome.segments for one in segment.slices]
    assert "heading" in kinds


def test_parent_heading_paths_are_recorded_beside_the_evidence_not_inside_it() -> None:
    body = "§ 1.1 Scope\n\nFacilities must sample water quarterly and report the results.\n"
    artifact = _cfr(text=body)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    headings = {heading for segment in outcome.segments for heading in segment.context.headings}
    assert "§ 1.1 Scope" in headings
    for segment in outcome.segments:
        for heading in segment.context.headings:
            assert heading not in segment.text or any(heading in one.text for one in segment.slices)


# --------------------------------------------------------------------------
# identity: artifact-scoped segment id, content-addressed work digest
# --------------------------------------------------------------------------


def test_the_segment_id_is_stable_artifact_scoped_and_settings_bound() -> None:
    artifact = _cfr(heading="Oral hearing.", text="Facilities must sample water.")

    first = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    again = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    other = segment_artifact(artifact, settings=_settings(policy_version="structure-overlap-v9"), counter=COUNTER)

    assert [one.segment_id for one in first.segments] == [one.segment_id for one in again.segments]
    assert [one.segment_id for one in first.segments] != [one.segment_id for one in other.segments]
    assert first.segments[0].identity()["artifact_sha256"] == artifact.content_sha256


def test_the_content_digest_survives_a_new_artifact_version_with_unchanged_content() -> None:
    """A new Artifact version must not make an unchanged fragment pay again."""
    body = "z" * 260
    first = _cfr(heading="Oral hearing.", text=body)
    second = _cfr(heading="Oral hearing, as amended.", text=body)

    one = segment_artifact(first, settings=_settings(), counter=COUNTER)
    two = segment_artifact(second, settings=_settings(), counter=COUNTER)

    assert first.content_sha256 != second.content_sha256
    body_first = next(s for s in one.segments if all(x.source_field.endswith(".text") for x in s.slices))
    body_second = next(s for s in two.segments if all(x.source_field.endswith(".text") for x in s.slices))
    assert body_first.content_digest == body_second.content_digest
    assert body_first.segment_id != body_second.segment_id
    assert "artifact_sha256" not in body_first.content_identity()
    assert "artifact_id" not in body_first.content_identity()


def test_the_content_digest_still_changes_when_the_settings_change() -> None:
    artifact = _cfr(text="Facilities must sample water.")

    one = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    two = segment_artifact(artifact, settings=_settings(policy_version="structure-overlap-v9"), counter=COUNTER)

    assert one.segments[0].content_digest != two.segments[0].content_digest


def test_neighbours_and_ordinals_describe_the_selected_order() -> None:
    artifact = _cfr(text="\n\n".join("i" * 30 for _ in range(6)))

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    segments = outcome.segments
    assert [one.ordinal for one in segments] == list(range(len(segments)))
    assert {one.segment_count for one in segments} == {len(segments)}
    assert segments[0].previous_segment_id is None
    assert segments[-1].next_segment_id is None
    for previous, current in zip(segments, segments[1:], strict=False):
        assert previous.next_segment_id == current.segment_id
        assert current.previous_segment_id == previous.segment_id


# --------------------------------------------------------------------------
# coverage, checks, and successful zero work
# --------------------------------------------------------------------------


def test_source_coverage_accounts_for_every_character_of_every_field() -> None:
    artifact = _cfr(heading="Oral hearing.", text="\n\n".join("j" * 30 for _ in range(6)))

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert outcome.coverage.uncovered_chars == 0
    for field in outcome.coverage.fields:
        assert field.field_chars == len(artifact.raw_fields[field.source_field])
        assert field.covered_chars + field.uncovered_chars == field.field_chars


def test_an_overlapped_split_reports_the_duplicated_characters_it_really_created() -> None:
    artifact = _cfr(text="k" * 260)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert outcome.coverage.uncovered_chars == 0
    assert outcome.coverage.duplicated_chars == 20, "two later leaves each reach back ten characters"


def test_an_artifact_with_no_processing_region_succeeds_with_no_work() -> None:
    artifact = _cfr(heading="   ", text=None)

    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert outcome.state == "completed_empty"
    assert outcome.segments == ()
    assert outcome.coverage.uncovered_chars == 0
    summary = summarize_segments([outcome])
    assert summary.zero_work_count == 1
    assert summary.segment_count == 0
    assert [check.status for check in segment_checks([outcome]) if check.name == "no_failed_work"] == ["pass"]


def test_the_step_checks_report_coverage_overflow_exclusion_and_zero_work() -> None:
    empty = _cfr(heading="   ", text=None)
    full = _cfr(heading="Oral hearing.", text="l" * 260)

    outcomes = segment_artifacts([empty, full], settings=_settings(), counter=COUNTER)
    checks = {check.name: check for check in segment_checks(outcomes)}

    assert set(checks) >= {
        "source_coverage_gap_free",
        "no_token_overflow",
        "regions_excluded",
        "duplicated_characters",
        "completed_empty",
        "no_failed_work",
    }
    assert checks["source_coverage_gap_free"].status == "pass"
    assert checks["no_token_overflow"].status == "pass"
    assert all(check.step == "segment" for check in checks.values())


def test_a_coverage_gap_fails_the_step_check() -> None:
    """The check has to be able to fail, or it proves nothing."""
    artifact = _cfr(text="m" * 120)
    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    broken = outcome.__class__(
        **{
            **{name: getattr(outcome, name) for name in outcome.__dataclass_fields__},
            "segments": outcome.segments[:1],
        }
    )

    checks = {check.name: check for check in segment_checks([broken])}

    assert checks["source_coverage_gap_free"].status == "fail"


# --------------------------------------------------------------------------
# containment, and the difference between enclosure and overlap
# --------------------------------------------------------------------------


def test_containment_means_enclosure_and_never_mere_overlap() -> None:
    artifact = _cfr(text="n" * 260)
    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)
    field = "cfr_sections.text"

    inside = next(one for segment in outcome.segments for one in segment.slices)
    straddle = (inside.end_char - 2, inside.end_char + 2)

    assert any(contains_span(segment, field, inside.start_char, inside.end_char) for segment in outcome.segments)
    assert any(overlaps_span(segment, field, *straddle) for segment in outcome.segments)
    assert not contains_span(outcome.segments[0], field, *straddle)


def test_containment_needs_the_named_field_not_just_the_offsets() -> None:
    artifact = _cfr(heading="Oral hearing.", text="Facilities must sample water.")
    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    assert any(contains_span(segment, "cfr_sections.heading", 0, 13) for segment in outcome.segments)
    assert not any(contains_span(segment, "cfr_sections.nowhere", 0, 13) for segment in outcome.segments)


# --------------------------------------------------------------------------
# table grain and schema
# --------------------------------------------------------------------------


def test_segment_rows_are_one_per_segment_with_join_and_evidence_bindings() -> None:
    artifact = _cfr(heading="Oral hearing.", text="p" * 260)
    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    rows = segment_rows(outcome)

    assert len(rows) == len(outcome.segments)
    assert [row["segment_id"] for row in rows] == [one.segment_id for one in outcome.segments]
    assert {row["artifact_id"] for row in rows} == {artifact.artifact_id}
    assert all(row["content_digest"] and row["settings_sha256"] for row in rows)
    assert all(row["coordinate_unit"] == "unicode-codepoints" for row in rows)
    assert all(row["coordinate_interval"] == "half-open" for row in rows)
    assert all(
        row["text"] == "\n".join(one.text for one in segment.slices) for row, segment in zip(rows, outcome.segments)
    )


def test_segment_table_keeps_its_declared_schema_with_rows_or_without(
    tmp_path: Any,
) -> None:
    import pyarrow.parquet as pq

    artifact = _cfr(text="Facilities must sample water.")
    outcome = segment_artifact(artifact, settings=_settings(), counter=COUNTER)

    written = write_segment_table(tmp_path / "with-rows", [outcome])
    populated = pq.read_table(written)
    empty_path = write_segment_table(tmp_path / "empty", [])
    empty = pq.read_table(empty_path)

    expected = [name for name, _ in SEGMENT_COLUMNS]
    assert written == tmp_path / "with-rows" / SEGMENT_TABLE
    assert populated.column_names == empty.column_names == expected
    assert populated.num_rows == len(outcome.segments)
    assert empty.num_rows == 0

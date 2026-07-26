"""Hermetic parity between the selected legacy arm and v3 segments.

The fixture is synthetic CC0 data.  It exercises markup syntax and headings,
structured JSON containers and children, atomic and prose regions, an
oversized unbroken region, and a gold span that deliberately crosses a
selected boundary.

Production ``docpipeline`` code never imports the predecessor.  This test does
so until step 8 removes the old active runner.
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from spicy_regs.corpora.segmentation_experiment import (
    ExperimentConfig,
    _coverage as legacy_coverage,
    _pack_overlap_units as legacy_segments,
)
from spicy_regs.docpipeline.segments import (
    BOUNDARY_METHOD,
    ProcessingSegment,
    SegmentSettings,
    contains_span,
    overlaps_span,
    segment_artifact,
)
from spicy_regs.docpipeline.source import build_source_artifacts
from spicy_regs.ontology.common import canonical_json, text_digest, write_parquet_rows
from spicy_regs.ontology.subjects import build_artifacts as legacy_artifacts

FIXTURE = Path(__file__).parent / "fixtures" / "docpipeline_segments_migration_v1.json"
DIFFERENCES_FIXTURE = Path(__file__).parent / "fixtures" / "docpipeline_step4_expected_differences_v1.json"

LEGACY_CONFIG = ExperimentConfig(
    config_id="structure-overlap-100",
    arm="structure-overlap",
    max_tokens=100,
    min_tokens=40,
    overlap_tokens=10,
)

DIFFERENCE_LEDGER = json.loads(DIFFERENCES_FIXTURE.read_text(encoding="utf-8"))
EXPECTED_DIFFERENCES: tuple[dict[str, Any], ...] = tuple(DIFFERENCE_LEDGER["differences"])
EXPECTED_DIFFERENCE_LEDGER_SHA256 = "4331c52220c9795dafc2d1865245d0502e40442db563852d7a80635818927abd"


class _CharacterCounter:
    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()
SETTINGS = SegmentSettings(
    max_tokens=100,
    min_tokens=40,
    overlap_tokens=10,
    tokenizer=COUNTER.name,
    tokenizer_version=COUNTER.version,
)


@pytest.fixture(scope="module")
def fixture_record() -> dict[str, Any]:
    value = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert value["license"] == "CC0-1.0"
    return value


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory, fixture_record: dict[str, Any]) -> Path:
    root = tmp_path_factory.mktemp("segment-migration")
    for table, rows in fixture_record["tables"].items():
        columns = tuple(dict.fromkeys(key for row in rows for key in row))
        write_parquet_rows(root / f"{table}.parquet", columns=columns, rows=rows)
    return root


def _paired_artifacts(corpus: Path) -> list[tuple[Any, Any]]:
    old = sorted(legacy_artifacts(corpus), key=lambda one: one.digest)
    new = sorted(
        (outcome.artifact for outcome in build_source_artifacts(corpus) if outcome.artifact is not None),
        key=lambda one: one.content_sha256,
    )
    assert [one.digest for one in old] == [one.content_sha256 for one in new]
    return list(zip(old, new, strict=True))


def _shared_legacy_row(segment: Any) -> dict[str, Any]:
    return {
        "artifact_sha256": segment.artifact_digest,
        "profile_id": segment.profile_id,
        "source_table": segment.source_table,
        "subject_type": segment.subject_type,
        "subject_id": segment.subject_id,
        "ordinal": segment.ordinal,
        "token_count": segment.token_count,
        "tokenizer": segment.tokenizer,
        "tokenizer_version": segment.tokenizer_version,
        "boundary_method": segment.boundary_method,
        "overlap_chars": segment.overlap_chars,
        "slices": [
            {
                "source_field": one.source_field,
                "start_char": one.start_char,
                "end_char": one.end_char,
                "text": one.text,
                "source_sha256": one.source_sha256,
            }
            for one in segment.slices
        ],
    }


def _shared_v3_row(segment: ProcessingSegment) -> dict[str, Any]:
    return {
        "artifact_sha256": segment.artifact_sha256,
        "profile_id": segment.profile_id,
        "source_table": segment.source_table,
        "subject_type": segment.subject_type,
        "subject_id": segment.subject_id,
        "ordinal": segment.ordinal,
        "token_count": segment.token_count,
        "tokenizer": segment.settings.tokenizer,
        "tokenizer_version": segment.settings.tokenizer_version,
        "boundary_method": segment.boundary_method,
        "overlap_chars": segment.overlap_chars,
        "slices": [
            {
                "source_field": one.source_field,
                "start_char": one.start_char,
                "end_char": one.end_char,
                "text": one.text,
                "source_sha256": one.field_sha256,
            }
            for one in segment.slices
        ],
    }


def _segments(corpus: Path) -> tuple[list[Any], list[ProcessingSegment], list[Any]]:
    old_all: list[Any] = []
    new_all: list[ProcessingSegment] = []
    old_artifacts: list[Any] = []
    for old, new in _paired_artifacts(corpus):
        old_artifacts.append(old)
        old_all.extend(legacy_segments(old, LEGACY_CONFIG, cast(Any, COUNTER)))
        new_all.extend(segment_artifact(new, settings=SETTINGS, counter=COUNTER).segments)
    return old_all, new_all, old_artifacts


def _assert_shared_parity(old: list[Any], new: list[ProcessingSegment]) -> None:
    old_rows = [_shared_legacy_row(one) for one in old]
    new_rows = [_shared_v3_row(one) for one in new]
    assert new_rows == old_rows


def _difference_key(value: dict[str, Any]) -> tuple[str, str, tuple[str, ...]]:
    return value["id"], value["kind"], tuple(value["values"])


def _has_join_and_context_fields(segment: ProcessingSegment) -> bool:
    return bool(
        segment.artifact_id
        and segment.text_sha256
        and segment.settings.digest
        and all(
            one.region_id
            and one.coordinates.interval == "half-open"
            and one.evidence_grade
            and one.content_layer
            and one.coordinate_grade
            and one.text_sha256
            for one in segment.slices
        )
        and set(segment.evidence_slices) <= set(segment.slices)
    )


def _difference_counts(
    old: list[Any],
    new: list[ProcessingSegment],
) -> dict[tuple[str, str, tuple[str, ...]], int]:
    assert len(old) == len(new)
    keys = {one["id"]: _difference_key(one) for one in EXPECTED_DIFFERENCES}
    counts = {key: 0 for key in keys.values()}
    for before, after in zip(old, new, strict=True):
        if before.segment_id != after.segment_id:
            counts[keys["processing-segment-identity"]] += 1
        if (
            before.policy_version != after.settings.policy_version
            and after.settings.digest == SETTINGS.digest
            and after.settings.identity() == SETTINGS.identity()
            and after.settings.boundary_method == BOUNDARY_METHOD
        ):
            counts[keys["versioned-settings-identity"]] += 1
        if (
            after.content_digest
            and "artifact_id" not in after.content_identity()
            and "artifact_sha256" not in after.content_identity()
        ):
            counts[keys["content-addressed-work"]] += 1
        if _has_join_and_context_fields(after):
            counts[keys["join-and-context-fields"]] += 1
    return counts


def _difference_key_by_id(identifier: str) -> dict[str, Any]:
    return next(one for one in EXPECTED_DIFFERENCES if one["id"] == identifier)


def _aggregate_row_digest(old: list[Any], new: list[ProcessingSegment]) -> str:
    rows = [
        {
            "shared": _shared_v3_row(after),
            "segment_id": after.segment_id,
            "content_digest": after.content_digest,
            "artifact_id": after.artifact_id,
            "previous_segment_id": after.previous_segment_id,
            "next_segment_id": after.next_segment_id,
            "text_sha256": after.text_sha256,
            "context": after.context.as_dict(),
            "slices": [one.as_dict() for one in after.slices],
            "legacy_segment_id": before.segment_id,
        }
        for before, after in zip(old, new, strict=True)
    ]
    return hashlib.sha256(canonical_json(rows).encode()).hexdigest()


def test_old_and_new_select_exactly_the_same_segments(corpus: Path) -> None:
    old, new, _ = _segments(corpus)

    _assert_shared_parity(old, new)
    assert [one.segment_count for one in new] == [
        sum(candidate.artifact_digest == one.artifact_digest for candidate in old) for one in old
    ]


def test_source_coverage_token_facts_settings_and_overlap_match(corpus: Path) -> None:
    old, new, artifacts = _segments(corpus)
    old_uncovered, old_duplicated = legacy_coverage(artifacts, old)
    new_outcomes = [
        segment_artifact(new_artifact, settings=SETTINGS, counter=COUNTER)
        for _, new_artifact in _paired_artifacts(corpus)
    ]

    assert old_uncovered == sum(one.coverage.uncovered_chars for one in new_outcomes) == 0
    assert old_duplicated == sum(one.coverage.duplicated_chars for one in new_outcomes)
    assert all(one.token_count <= SETTINGS.max_tokens for one in new)
    assert SETTINGS.identity() == {
        "policy": "structure-overlap",
        "policy_version": "structure-overlap-v1",
        "max_tokens": 100,
        "min_tokens": 40,
        "overlap_tokens": 10,
        "tokenizer": "character-test",
        "tokenizer_version": "1",
        "boundary_method": "source-native-oversized-overlap",
    }


def test_fixture_gold_uses_enclosure_and_keeps_the_straddling_control_negative(
    corpus: Path, fixture_record: dict[str, Any]
) -> None:
    _, new, _ = _segments(corpus)
    by_subject: dict[str, list[ProcessingSegment]] = {}
    for segment in new:
        by_subject.setdefault(segment.subject_id, []).append(segment)

    actual: dict[str, bool] = {}
    for gold in fixture_record["gold_spans"]:
        candidates = by_subject[gold["artifact_subject_id"]]
        actual[gold["gold_id"]] = any(
            contains_span(segment, gold["source_field"], gold["start_char"], gold["end_char"]) for segment in candidates
        )

    assert actual == {
        "contained-inside-first-leaf": True,
        "boundary-straddling-negative-control": False,
    }
    straddling = fixture_record["gold_spans"][1]
    assert any(
        overlaps_span(segment, straddling["source_field"], straddling["start_char"], straddling["end_char"])
        for segment in by_subject[straddling["artifact_subject_id"]]
    )


def test_every_difference_is_literal_observed_and_approved(corpus: Path) -> None:
    old, new, _ = _segments(corpus)
    declared = {_difference_key(one) for one in EXPECTED_DIFFERENCES}
    counts = _difference_counts(old, new)

    assert set(counts) == declared
    assert set(counts.values()) == {len(old)}
    assert DIFFERENCE_LEDGER["format_version"] == 1
    for difference in EXPECTED_DIFFERENCES:
        assert set(difference) == {"id", "kind", "old", "new", "reason", "values"}
        assert difference["old"] and difference["new"] and difference["reason"] and difference["values"]


def test_difference_ledger_identity_and_aggregate_rows_are_content_bound(corpus: Path) -> None:
    old, new, _ = _segments(corpus)

    assert hashlib.sha256(DIFFERENCES_FIXTURE.read_bytes()).hexdigest() == (EXPECTED_DIFFERENCE_LEDGER_SHA256)
    digest = _aggregate_row_digest(old, new)
    assert len(digest) == 64
    assert digest == _aggregate_row_digest(old, new)


def test_an_unlisted_or_vanished_difference_fails_exact_set_equality(corpus: Path) -> None:
    old, new, _ = _segments(corpus)
    observed = set(_difference_counts(old, new))
    declared = {_difference_key(one) for one in EXPECTED_DIFFERENCES}

    with pytest.raises(AssertionError):
        assert observed | {("unlisted", "field", ("mystery",))} == declared
    with pytest.raises(AssertionError):
        assert observed - {_difference_key(EXPECTED_DIFFERENCES[0])} == declared


def test_mutation_probes_detect_removed_shifted_and_weakened_coverage(corpus: Path) -> None:
    old, new, artifacts = _segments(corpus)
    with pytest.raises(AssertionError):
        _assert_shared_parity(old, new[:-1])

    changed_slice = replace(new[0].slices[0], start_char=new[0].slices[0].start_char + 1)
    shifted = [replace(new[0], slices=(changed_slice, *new[0].slices[1:])), *new[1:]]
    with pytest.raises(AssertionError):
        _assert_shared_parity(old, shifted)

    old_uncovered, _ = legacy_coverage(artifacts, old)
    with pytest.raises(AssertionError):
        assert old_uncovered == 1


def test_mutation_probes_detect_overlap_as_containment_and_settings_drift(
    corpus: Path, fixture_record: dict[str, Any]
) -> None:
    old, new, _ = _segments(corpus)
    straddling = fixture_record["gold_spans"][1]
    candidates = [one for one in new if one.subject_id == straddling["artifact_subject_id"]]

    with pytest.raises(AssertionError):
        assert (
            any(
                overlaps_span(one, straddling["source_field"], straddling["start_char"], straddling["end_char"])
                for one in candidates
            )
            is False
        )

    drifted = replace(SETTINGS, policy_version="structure-overlap-v2")
    mutated = [replace(one, settings=drifted) for one in new]
    with pytest.raises(AssertionError):
        counts = _difference_counts(old, mutated)
        assert set(counts) == {_difference_key(one) for one in EXPECTED_DIFFERENCES} and set(counts.values()) == {
            len(old)
        }


def test_mutation_probe_rejects_artifact_scoped_content_digest() -> None:
    base = {
        "settings_sha256": SETTINGS.digest,
        "slices": [{"text_sha256": "a" * 64}],
    }
    correct_first = text_digest(canonical_json(base))
    correct_second = text_digest(canonical_json(base))
    bad_first = text_digest(canonical_json({**base, "artifact_sha256": "1" * 64}))
    bad_second = text_digest(canonical_json({**base, "artifact_sha256": "2" * 64}))

    assert correct_first == correct_second
    with pytest.raises(AssertionError):
        assert bad_first == bad_second


def test_new_production_modules_do_not_import_old_runners() -> None:
    root = Path(__file__).parents[1]
    production = (root / "src" / "spicy_regs" / "docpipeline" / "segments.py").read_text(encoding="utf-8")

    assert "spicy_regs.corpora" not in production
    assert "spicy_regs.ontology.segmentation" not in production
    assert "spicy_regs.ontology.subjects" not in production
    assert (root / "src" / "spicy_regs" / "corpora" / "segmentation_experiment.py").is_file()
    assert "run-segmentation-experiment" in (root / "pyproject.toml").read_text(encoding="utf-8")

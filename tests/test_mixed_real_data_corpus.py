"""Tests for the heterogeneous real-data corpus contract."""

from __future__ import annotations

from spicy_regs.corpora.mixed_real_data import (
    EXPECTED_SOURCE_TABLES,
    SOURCE_SPECS,
    PairExpectation,
    negative_controls,
    record_id,
)


def _positive(left: str, right: str) -> PairExpectation:
    return PairExpectation(
        left_record_id=record_id("documents", left),
        left_source="documents",
        right_record_id=record_id("dockets", right),
        right_source="dockets",
        label="related",
        relation_kind="document_in_docket",
        evidence_basis="documents.docket_id = dockets.docket_id",
        evidence_value=right,
        evidence_strength="direct_identifier",
    )


def test_source_specs_cover_every_declared_source_once():
    names = [spec.name for spec in SOURCE_SPECS]

    assert tuple(names) == EXPECTED_SOURCE_TABLES
    assert len(names) == len(set(names))
    assert all(spec.primary_key for spec in SOURCE_SPECS)
    assert all(spec.target_rows > 0 for spec in SOURCE_SPECS)


def test_record_ids_are_source_scoped_and_stable():
    assert record_id("documents", "A-1") == record_id("documents", "A-1")
    assert record_id("documents", "A-1") != record_id("comments", "A-1")
    assert record_id("documents", "A-1").startswith("record_")


def test_negative_controls_rotate_without_overlapping_positive_pairs():
    positives = [_positive("DOC-1", "DOCKET-1"), _positive("DOC-2", "DOCKET-2")]

    controls = negative_controls(positives)

    assert len(controls) == 2
    assert {row.label for row in controls} == {"no_declared_relation"}
    assert {
        (row.left_record_id, row.right_record_id)
        for row in controls
    }.isdisjoint(
        {
            (row.left_record_id, row.right_record_id)
            for row in positives
        }
    )
    assert negative_controls(positives) == controls

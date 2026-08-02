"""Opt-in parity gate over the frozen 153-artifact local dataset.

Set ``SPICY_REGS_FROZEN_SEGMENTATION_ROOT`` to the directory that contains the
three frozen output directories.  Normal CI skips with a precise message; the
dated Step 4 evidence record names the command that ran this gate locally.

**This gate currently FAILS, and the failure is the finding — not a defect in
the gate.** As of 2026-08-02 the committed segmenter produces 1,296 selected
segments where the frozen baseline recorded 1,302. For this arm the whole delta
is one artifact (``congress-bill-v1`` / ``118-hr-8862``), whose ``xml_text``
element stream went from 174 slices to 3,428.

**No commit explains it.** Exporting ``414964d``'s ``src/`` — whose
``source.py`` and ``segments.py`` digests are byte-identical to those the
2026-07-26 receipt recorded while the gate *passed* at 1,302 — and re-running
this computation today yields 1,296. Identical code, both numbers. The variable
is environmental and was never pinned: no receipt anywhere recorded an
interpreter version. The leading suspect is ``source.py::_markup_drafts``, which
swallows ``AssertionError``/``ValueError`` from a stdlib ``html.parser``
subclass and silently switches drafting strategy.

Do not "repair" this by moving 1302 to 1296; that would bless an environmental
drift as a decision. The fix is to identify and pin the variable, then
re-baseline deliberately. See
``docs/evidence/document-segmentation-remeasurement-2026-08-02.md``.

The dataset and scope directories this gate names were re-sealed on 2026-08-02
(``tools/reseal_segmentation_dataset.py``); ``_check_reseal_provenance`` below
proves the re-sealed corpus is the corpus the baseline measured.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.corpora.document_acceptance_scope import load_document_acceptance_scope
from spicy_regs.corpora.segmentation_experiment import TiktokenCounter
from spicy_regs.docpipeline.segments import (
    ProcessingSegment,
    SegmentSettings,
    contains_span,
    overlaps_span,
    segment_artifacts,
)
from spicy_regs.docpipeline.source import (
    COVERAGE_RECORD_SOURCE_TABLE_EXCLUSION,
    EXCLUDED_SOURCE_TABLES,
    EXCLUSION_INACTIVE_SOURCE_TABLE,
    STEP4_ACTIVE_SOURCE_TABLES,
    build_source_artifacts,
    coverage_rows,
)
from spicy_regs.ontology.common import canonical_json, read_parquet_rows

ROOT_ENV = "SPICY_REGS_FROZEN_SEGMENTATION_ROOT"
DATASET_DIR = "segmented-real-data-evaluation-v2-resealed-2026-08-02"
SCOPE_DIR = "document-acceptance-scope-resealed-2026-08-02"
BASELINE_DIR = "segmentation-experiment-document-bge-v3"

# The corpus this gate measures is the one the July baseline measured. Its *seal*
# had to be recomputed on 2026-08-02 because an unrelated commit rewrote one
# non-segmentation member in place and the sealed bytes are unrecoverable — see
# EXTERNALLY_REBUILT_DATASET_MEMBERS below and tools/reseal_segmentation_dataset.py.
# The chain the gate proves: the baseline names SEALED_DATASET_ID; the re-sealed
# manifest declares it descends from SEALED_DATASET_ID changing only members the
# segmenter cannot read; therefore the baseline's corpus is this corpus.
SEALED_DATASET_ID = "segmentation_eval_627ba96e04872d870a2ccd6e"
DATASET_ID = "segmentation_eval_21d9a09f13ad3b9bf5ea212b"
SCOPE_ID = "document_scope_cd30315482563696a07e4103"
BASELINE_ID = "segmentation_experiment_de7d119e838ac153a0980337"
SELECTED_CONFIG = "structure-overlap-1800"
DIFFERENCES_FIXTURE = Path(__file__).parent / "fixtures" / "docpipeline_step4_expected_differences_v1.json"
DIFFERENCE_LEDGER = json.loads(DIFFERENCES_FIXTURE.read_text(encoding="utf-8"))
EXPECTED_DIFFERENCES = tuple(DIFFERENCE_LEDGER["differences"])
EXPECTED_DIFFERENCE_IDS = frozenset(one["id"] for one in EXPECTED_DIFFERENCES)
EXPECTED_DIFFERENCE_LEDGER_SHA256 = "4331c52220c9795dafc2d1865245d0502e40442db563852d7a80635818927abd"
EXPECTED_AGGREGATE_ROW_SHA256 = "34f757e1fa12a431c720da46e68c78e9c58ca25a78ad51c664f2c0274ff9acf9"

EXPECTED_FILE_SHA256 = {
    f"{DATASET_DIR}/segmentation-evaluation-manifest.json": (
        "2c54cf3ae893d52b50a4ca13e715aa883cf4a34bf020af14e50b177b41705812"
    ),
    f"{DATASET_DIR}/segmentation-evaluation-receipt.json": (
        "b81e14d6a6dbf71d25248b999556ea97f5f30459c7fc0725d755ef9bebac9980"
    ),
    f"{SCOPE_DIR}/document-acceptance-manifest.json": (
        "41a87b5ba2eff70a6f691f6c9fe4e1ae78232db8cc49f5c685b47ed2a1d3300e"
    ),
    f"{SCOPE_DIR}/document-acceptance-receipt.json": (
        "ead7274476e314fbf88a5a182389edcef81c99ff53e2f97784157413b7f909dd"
    ),
    f"{BASELINE_DIR}/segmentation-experiment-manifest.json": (
        "1c9764b0df7985b60666ef1bc69f1ed7bfee4cbdcf1fa0036457118729f80a10"
    ),
    f"{BASELINE_DIR}/segmentation-experiment-receipt.json": (
        "ebb71adadbac85e153c9d1986d7e5a6a75c6e8169bb950655cc51776f4d0b5aa"
    ),
    f"{BASELINE_DIR}/experiment_segments.parquet": ("0874cfae61b741b7d946bc47be62d583879b43f76269557262598c1e8e11dc78"),
}


EXTERNALLY_REBUILT_DATASET_MEMBERS: dict[str, dict[str, str]] = {
    "fr_docket_links.parquet": {
        "source_table": "fr_docket_links",
        "sealed_sha256": "fa8ad683f90cc8974e98e00c26950cc2f0afc0fd3492df0c75774e5b6c434e74",
        "rebuilt_sha256": "a11ddb6a677dafe28c5fcade28d08df9cafef528385fa4320218be37de7fa394",
        "rebuilt_by": "3a472f0",
        "reason": (
            "The docket_key rebuild rewrote this carrier in place across every generation. "
            "Its previous writer was byte-non-deterministic, so the sealed bytes are "
            "unrecoverable: no copy survives on disk and re-running the old writer cannot "
            "reproduce the row order. fr_docket_links is an EXCLUDED_SOURCE_TABLES "
            "relationship carrier and never becomes a SourceArtifact, so no segment "
            "boundary depends on it."
        ),
    }
}
"""Members rewritten outside this dataset's own build, with both digests pinned.

This is a quarantine, not an amnesty. Each entry names one file, the digest the
dataset sealed, the digest that replaced it, and the commit that did it — and the
test above refuses any entry the segmenter could actually read. Any *other*
drift, or a third digest for a listed member, still fails the gate.
"""


def _check_reseal_provenance(dataset_manifest: Mapping[str, Any]) -> None:
    """Prove the re-sealed corpus is the sealed one, minus nothing that matters.

    The re-seal is only admissible if it descends from the exact identity the
    frozen baseline names and every member it changed is one this ledger already
    licenses. An unlisted change, or a descent from some other dataset, means the
    corpus under test is not the corpus the baseline measured.
    """
    provenance = dataset_manifest.get("resealed_from")
    assert isinstance(provenance, Mapping), "the re-sealed dataset must record what it descends from"
    assert provenance["evaluation_id"] == SEALED_DATASET_ID, (
        "the re-sealed dataset does not descend from the identity the frozen baseline names"
    )
    changed = {str(one["name"]): one for one in provenance["changed_members"]}
    assert set(changed) == set(EXTERNALLY_REBUILT_DATASET_MEMBERS), (
        f"the re-seal changed {sorted(changed)}, but the ledger licenses {sorted(EXTERNALLY_REBUILT_DATASET_MEMBERS)}"
    )
    for name, record in EXTERNALLY_REBUILT_DATASET_MEMBERS.items():
        assert changed[name]["sealed_sha256"] == record["sealed_sha256"]
        assert changed[name]["current_sha256"] == record["rebuilt_sha256"]
    assert int(provenance["unchanged_member_count"]) == 26, (
        "every other member of the sealed corpus must have been carried byte for byte"
    )


@dataclass(frozen=True)
class _FrozenResult:
    artifacts: tuple[Any, ...]
    segments: tuple[ProcessingSegment, ...]
    baseline_rows: tuple[dict[str, Any], ...]
    gold_rows: tuple[dict[str, Any], ...]
    uncovered_chars: int
    duplicated_chars: int
    overflow_count: int
    contained_gold: int
    observed_differences: frozenset[str]
    difference_counts: tuple[tuple[str, int], ...]
    aggregate_row_sha256: str
    inactive_source_tables: tuple[str, ...]
    inactive_table_audit_rows: int


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _frozen_root() -> Path:
    raw = os.environ.get(ROOT_ENV)
    if raw is None:
        pytest.skip(
            f"{ROOT_ENV} is unset; set it to the local frozen-output directory "
            "to run the 153-artifact / 1,302-segment / 35-gold parity gate"
        )
    root = Path(raw).expanduser().resolve()
    missing = sorted(relative for relative in EXPECTED_FILE_SHA256 if not (root / relative).is_file())
    if missing:
        pytest.fail(f"{ROOT_ENV} is explicitly set to {raw!r}, but {root} lacks frozen inputs: {missing}")
    return root


def test_every_externally_rebuilt_member_is_provably_not_a_segmentation_input() -> None:
    """A quarantined member must be one the segmenter can never read.

    This is the whole licence for the ledger below. ``fr_docket_links`` is a
    relationship carrier in ``EXCLUDED_SOURCE_TABLES``: it never becomes a
    ``SourceArtifact``, so its bytes cannot move a segment boundary. If a future
    entry named a table the segmenter *does* read, this fails and the ledger
    stops being a legitimate quarantine.
    """
    assert EXTERNALLY_REBUILT_DATASET_MEMBERS, "the ledger states which members moved outside this dataset's build"
    for name, record in EXTERNALLY_REBUILT_DATASET_MEMBERS.items():
        table = record["source_table"]
        assert table in EXCLUDED_SOURCE_TABLES, f"{name} is a segmentation input and cannot be quarantined"
        assert table not in STEP4_ACTIVE_SOURCE_TABLES
        assert record["sealed_sha256"] != record["rebuilt_sha256"]
        assert record["rebuilt_by"] and record["reason"]


def _provenance(**overrides: Any) -> dict[str, Any]:
    record = EXTERNALLY_REBUILT_DATASET_MEMBERS["fr_docket_links.parquet"]
    base = {
        "evaluation_id": SEALED_DATASET_ID,
        "changed_members": [
            {
                "name": "fr_docket_links.parquet",
                "sealed_sha256": record["sealed_sha256"],
                "current_sha256": record["rebuilt_sha256"],
            }
        ],
        "unchanged_member_count": 26,
    }
    base.update(overrides)
    return {"resealed_from": base}


def test_reseal_provenance_accepts_exactly_the_licensed_descent() -> None:
    _check_reseal_provenance(_provenance())


def test_reseal_provenance_refuses_descent_from_another_dataset() -> None:
    with pytest.raises(AssertionError, match="does not descend from"):
        _check_reseal_provenance(_provenance(evaluation_id="segmentation_eval_000000000000000000000000"))


def test_reseal_provenance_refuses_an_unlicensed_changed_member() -> None:
    changed = _provenance()["resealed_from"]["changed_members"] + [
        {"name": "gold_spans.parquet", "sealed_sha256": "a", "current_sha256": "b"}
    ]
    with pytest.raises(AssertionError, match="the ledger licenses"):
        _check_reseal_provenance(_provenance(changed_members=changed))


def test_reseal_provenance_refuses_a_silently_dropped_member() -> None:
    with pytest.raises(AssertionError, match="carried byte for byte"):
        _check_reseal_provenance(_provenance(unchanged_member_count=25))


def test_frozen_root_unset_skips_but_explicit_invalid_root_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv(ROOT_ENV, raising=False)
    with pytest.raises(pytest.skip.Exception, match=rf"{ROOT_ENV} is unset"):
        _frozen_root()

    invalid_root = tmp_path / "incomplete-frozen-root"
    invalid_root.mkdir()
    monkeypatch.setenv(ROOT_ENV, str(invalid_root))
    with pytest.raises(
        pytest.fail.Exception,
        match=rf"{ROOT_ENV} is explicitly set .* lacks frozen inputs",
    ):
        _frozen_root()


def _verify_frozen_files(root: Path) -> tuple[Path, Path, Path]:
    for relative, expected in EXPECTED_FILE_SHA256.items():
        assert _sha256(root / relative) == expected, f"frozen file identity drifted: {relative}"

    dataset = root / DATASET_DIR
    scope = root / SCOPE_DIR
    baseline = root / BASELINE_DIR
    dataset_manifest = _load_json(dataset / "segmentation-evaluation-manifest.json")
    dataset_receipt = _load_json(dataset / "segmentation-evaluation-receipt.json")
    scope_manifest = _load_json(scope / "document-acceptance-manifest.json")
    scope_receipt = _load_json(scope / "document-acceptance-receipt.json")
    baseline_manifest = _load_json(baseline / "segmentation-experiment-manifest.json")
    baseline_receipt = _load_json(baseline / "segmentation-experiment-receipt.json")

    assert dataset_manifest["evaluation_id"] == dataset_receipt["evaluation_id"] == DATASET_ID
    assert scope_manifest["scope_id"] == scope_receipt["scope_id"] == SCOPE_ID
    assert scope_manifest["dataset_evaluation_id"] == DATASET_ID
    assert baseline_manifest["experiment_id"] == baseline_receipt["experiment_id"] == BASELINE_ID
    # The baseline names the pre-reseal identity; the provenance check below is
    # what licenses measuring it against the re-sealed corpus.
    assert baseline_manifest["dataset_evaluation_id"] == SEALED_DATASET_ID
    _check_reseal_provenance(dataset_manifest)
    assert dataset_receipt["status"] == scope_receipt["status"] == baseline_receipt["status"] == "pass"
    assert (
        baseline_manifest["artifacts"]["experiment_segments.parquet"]["sha256"]
        == EXPECTED_FILE_SHA256[f"{BASELINE_DIR}/experiment_segments.parquet"]
    )

    for name, record in dataset_manifest["nonmodel_artifacts"].items():
        path = dataset / name
        assert path.is_file(), f"frozen dataset inventory is incomplete: {name}"
        assert _sha256(path) == record["sha256"], f"frozen dataset member drifted: {name}"
    for name, record in scope_manifest["artifacts"].items():
        path = scope / name
        assert path.is_file(), f"frozen scope inventory is incomplete: {name}"
        assert _sha256(path) == record["sha256"], f"frozen scope member drifted: {name}"
    return dataset, scope, baseline


def _baseline_slice_rows(value: str) -> list[dict[str, Any]]:
    parsed = json.loads(value)
    assert isinstance(parsed, list)
    return parsed


def _v3_slice_rows(segment: ProcessingSegment) -> list[dict[str, Any]]:
    return [
        {
            "end_char": one.end_char,
            "source_field": one.source_field,
            "source_sha256": one.field_sha256,
            "start_char": one.start_char,
            "text": one.text,
        }
        for one in segment.slices
    ]


def _assert_row_parity(row: dict[str, Any], segment: ProcessingSegment) -> None:
    assert row["config_id"] == SELECTED_CONFIG
    assert row["arm"] == segment.settings.policy == "structure-overlap"
    assert int(row["max_tokens"]) == segment.settings.max_tokens == 1800
    assert int(row["min_tokens"]) == segment.settings.min_tokens == 720
    assert row["profile_id"] == segment.profile_id
    assert row["source_table"] == segment.source_table
    assert row["subject_type"] == segment.subject_type
    assert row["subject_id"] == segment.subject_id
    assert row["artifact_digest"] == segment.artifact_sha256
    assert int(row["ordinal"]) == segment.ordinal
    assert int(row["segment_count"]) == segment.segment_count
    assert int(row["token_count"]) == segment.token_count
    assert row["tokenizer"] == segment.settings.tokenizer == "o200k_base"
    assert row["tokenizer_version"] == segment.settings.tokenizer_version
    assert row["boundary_method"] == segment.boundary_method == "source-native-oversized-overlap"
    assert int(row["overlap_chars"]) == segment.overlap_chars
    assert _baseline_slice_rows(row["slices_json"]) == _v3_slice_rows(segment)
    assert segment.text == "\n".join(one["text"] for one in _baseline_slice_rows(row["slices_json"]))
    assert segment.token_count <= segment.input_limit
    assert segment.truncated is False


def _new_only_fields_are_valid(segment: ProcessingSegment) -> bool:
    return bool(
        segment.artifact_id
        and segment.content_digest
        and segment.text_sha256
        and "artifact_id" not in segment.content_identity()
        and "artifact_sha256" not in segment.content_identity()
        and set(segment.evidence_slices) <= set(segment.slices)
        and all(
            one.region_id
            and one.coordinates.interval == "half-open"
            and one.evidence_grade
            and one.content_layer
            and one.coordinate_grade
            and one.text_sha256
            for one in segment.slices
        )
    )


@pytest.fixture(scope="module")
def frozen_result() -> _FrozenResult:
    root = _frozen_root()
    dataset, scope_dir, baseline = _verify_frozen_files(root)
    scope = load_document_acceptance_scope(dataset, scope_dir)

    source_outcomes = build_source_artifacts(
        dataset,
        active_source_tables=STEP4_ACTIVE_SOURCE_TABLES,
    )
    inactive_source_tables = tuple(
        exclusion.source_table for outcome in source_outcomes for exclusion in outcome.source_table_exclusions
    )
    inactive_table_audit_rows = tuple(
        row
        for outcome in source_outcomes
        for row in coverage_rows(outcome)
        if row["record_kind"] == COVERAGE_RECORD_SOURCE_TABLE_EXCLUSION
    )
    assert inactive_source_tables == ("comments",)
    assert [(row["source_field"], row["reason"], row["uncovered_chars"]) for row in inactive_table_audit_rows] == [
        ("comments", EXCLUSION_INACTIVE_SOURCE_TABLE, 0)
    ]
    artifacts = tuple(
        outcome.artifact
        for outcome in source_outcomes
        if outcome.artifact is not None and outcome.artifact.content_sha256 in scope.included_artifact_digests
    )
    assert all(one.source_table in STEP4_ACTIVE_SOURCE_TABLES for one in artifacts)
    assert len(artifacts) == 153
    assert {one.content_sha256 for one in artifacts} == scope.included_artifact_digests

    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    outcomes = segment_artifacts(artifacts, settings=settings, counter=counter)
    segments = tuple(segment for outcome in outcomes for segment in outcome.segments)
    rows = tuple(
        row
        for row in read_parquet_rows(baseline / "experiment_segments.parquet")
        if row["config_id"] == SELECTED_CONFIG
    )
    assert len(segments) == len(rows) == 1302

    old_by_key = {(row["artifact_digest"], int(row["ordinal"])): row for row in rows}
    new_by_key = {(segment.artifact_sha256, segment.ordinal): segment for segment in segments}
    assert set(new_by_key) == set(old_by_key)
    for key in sorted(old_by_key):
        _assert_row_parity(old_by_key[key], new_by_key[key])

    difference_counts = {identifier: 0 for identifier in EXPECTED_DIFFERENCE_IDS}
    aggregate_rows: list[dict[str, Any]] = []
    for key in sorted(old_by_key):
        row = old_by_key[key]
        segment = new_by_key[key]
        if row["segment_id"] != segment.segment_id:
            difference_counts["processing-segment-identity"] += 1
        if (
            row["policy_version"] != segment.settings.policy_version
            and segment.settings.digest
            and segment.settings.identity()["max_tokens"] == int(row["max_tokens"])
            and segment.settings.identity()["min_tokens"] == int(row["min_tokens"])
        ):
            difference_counts["versioned-settings-identity"] += 1
        if (
            segment.content_digest
            and "artifact_id" not in segment.content_identity()
            and "artifact_sha256" not in segment.content_identity()
        ):
            difference_counts["content-addressed-work"] += 1
        if _new_only_fields_are_valid(segment):
            difference_counts["join-and-context-fields"] += 1
        aggregate_rows.append(
            {
                "key": list(key),
                "legacy_segment_id": row["segment_id"],
                "segment_id": segment.segment_id,
                "content_digest": segment.content_digest,
                "artifact_id": segment.artifact_id,
                "previous_segment_id": segment.previous_segment_id,
                "next_segment_id": segment.next_segment_id,
                "text_sha256": segment.text_sha256,
                "context": segment.context.as_dict(),
                "slices": [one.as_dict() for one in segment.slices],
            }
        )

    gold = tuple(
        row for row in read_parquet_rows(dataset / "gold_spans.parquet") if row["gold_id"] in scope.included_gold_ids
    )
    by_artifact: dict[str, list[ProcessingSegment]] = {}
    for segment in segments:
        by_artifact.setdefault(segment.artifact_sha256, []).append(segment)
    contained = sum(
        any(
            contains_span(
                segment,
                row["source_field"],
                int(row["start_char"]),
                int(row["end_char"]),
            )
            for segment in by_artifact.get(row["artifact_digest"], ())
        )
        for row in gold
    )

    observed = frozenset(identifier for identifier, count in difference_counts.items() if count == len(segments))
    return _FrozenResult(
        artifacts=artifacts,
        segments=segments,
        baseline_rows=rows,
        gold_rows=gold,
        uncovered_chars=sum(outcome.coverage.uncovered_chars for outcome in outcomes),
        duplicated_chars=sum(outcome.coverage.duplicated_chars for outcome in outcomes),
        overflow_count=sum(segment.truncated for segment in segments),
        contained_gold=contained,
        observed_differences=observed,
        difference_counts=tuple(sorted(difference_counts.items())),
        aggregate_row_sha256=hashlib.sha256(canonical_json(aggregate_rows).encode()).hexdigest(),
        inactive_source_tables=inactive_source_tables,
        inactive_table_audit_rows=len(inactive_table_audit_rows),
    )


def test_frozen_selected_segment_rows_are_exact_except_approved_v3_fields(frozen_result: _FrozenResult) -> None:
    assert len(frozen_result.artifacts) == 153
    assert len(frozen_result.segments) == len(frozen_result.baseline_rows) == 1302
    assert frozen_result.observed_differences == EXPECTED_DIFFERENCE_IDS
    assert dict(frozen_result.difference_counts) == {identifier: 1302 for identifier in EXPECTED_DIFFERENCE_IDS}
    assert len({one.segment_id for one in frozen_result.segments}) == 1302
    assert all(one.content_digest and "artifact_sha256" not in one.content_identity() for one in frozen_result.segments)
    assert "comments" not in STEP4_ACTIVE_SOURCE_TABLES
    assert frozen_result.inactive_source_tables == ("comments",)
    assert frozen_result.inactive_table_audit_rows == 1


def test_frozen_difference_ledger_and_aggregate_rows_are_sealed(
    frozen_result: _FrozenResult,
) -> None:
    assert hashlib.sha256(DIFFERENCES_FIXTURE.read_bytes()).hexdigest() == (EXPECTED_DIFFERENCE_LEDGER_SHA256)
    assert frozen_result.aggregate_row_sha256 == EXPECTED_AGGREGATE_ROW_SHA256


def test_frozen_live_difference_mutations_fail_exact_set_equality(
    frozen_result: _FrozenResult,
) -> None:
    observed = frozen_result.observed_differences
    with pytest.raises(AssertionError):
        assert observed | {"unlisted-difference"} == EXPECTED_DIFFERENCE_IDS
    with pytest.raises(AssertionError):
        assert observed - {"processing-segment-identity"} == EXPECTED_DIFFERENCE_IDS


def test_frozen_source_coverage_and_hard_token_limit_are_exact(frozen_result: _FrozenResult) -> None:
    assert frozen_result.uncovered_chars == 0
    assert frozen_result.duplicated_chars == 158_261
    assert frozen_result.overflow_count == 0


def test_frozen_gold_containment_is_recomputed_not_read_from_a_receipt(frozen_result: _FrozenResult) -> None:
    assert len(frozen_result.gold_rows) == frozen_result.contained_gold == 35


def test_frozen_negative_probe_keeps_overlap_distinct_from_enclosure(frozen_result: _FrozenResult) -> None:
    probe = next(
        (segment, one)
        for segment in frozen_result.segments
        for one in segment.slices
        if one.end_char
        < len(
            next(
                artifact for artifact in frozen_result.artifacts if artifact.content_sha256 == segment.artifact_sha256
            ).raw_fields[one.source_field]
        )
    )
    segment, one = probe
    start, end = one.end_char - 1, one.end_char + 1

    assert overlaps_span(segment, one.source_field, start, end)
    assert not contains_span(segment, one.source_field, start, end)

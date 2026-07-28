"""Hermetic checks for the holdout draw, its disjointness, and its blindness.

Every fixture here is synthetic. Nothing reads the real corpus, the real
development selection, or the tracked boundary record, so these tests state
what the tool guarantees rather than what one particular draw happened to do.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.segments import SegmentSettings
from spicy_regs.docpipeline.source import SourceRecord, profile_for_table

REPO_ROOT = Path(__file__).resolve().parents[1]
DRAW_PATH = REPO_ROOT / "tools" / "draw_holdout.py"


def _load_draw_holdout():
    spec = importlib.util.spec_from_file_location("draw_holdout", DRAW_PATH)
    assert spec and spec.loader, f"could not load {DRAW_PATH}"
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: ``dataclasses`` resolves a class's module
    # through ``sys.modules`` while it processes the class body.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


draw_holdout_module = _load_draw_holdout()

BOUNDARY_SCHEMA_VERSION = draw_holdout_module.BOUNDARY_SCHEMA_VERSION
DRAFTING_SCHEMA_VERSION = draw_holdout_module.DRAFTING_SCHEMA_VERSION
HoldoutBlindnessError = draw_holdout_module.HoldoutBlindnessError
HoldoutDisjointnessError = draw_holdout_module.HoldoutDisjointnessError
HoldoutDrawError = draw_holdout_module.HoldoutDrawError
ProfileStratum = draw_holdout_module.ProfileStratum
assert_blind = draw_holdout_module.assert_blind
build_drawn_artifact = draw_holdout_module.build_drawn_artifact
build_exclusions = draw_holdout_module.build_exclusions
draw_holdout = draw_holdout_module.draw_holdout
draw_stratum = draw_holdout_module.draw_stratum
drafting_document = draw_holdout_module.drafting_document
pending_holdout_record = draw_holdout_module.pending_holdout_record
ranked_records = draw_holdout_module.ranked_records
stratum_records = draw_holdout_module.stratum_records
update_boundary_manifest = draw_holdout_module.update_boundary_manifest
verify_draw = draw_holdout_module.verify_draw


class _CharacterCounter:
    """One token per character: exact, offline, and enough for boundaries."""

    name = "character"
    version = "test"

    def count(self, text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()
SETTINGS = SegmentSettings.for_counter(COUNTER)

GAO = profile_for_table("gao_reports")
CRS = profile_for_table("crs_reports")


def _gao_row(report_id: str, *, abstract: str, title: str | None = None) -> dict[str, object]:
    return {
        "report_id": report_id,
        "title": title if title is not None else f"Report {report_id}",
        "abstract": abstract,
        "report_type": "Report",
        "agencies_json": "[]",
    }


def _crs_row(report_id: str, *, title: str) -> dict[str, object]:
    return {
        "report_id": report_id,
        "title": title,
        "report_type": "Report",
        "status": "Active",
    }


def _gao_records(count: int, *, prefix: str = "gao", body: str | None = None) -> list[SourceRecord]:
    return [
        SourceRecord(
            profile=GAO,
            row=_gao_row(
                f"{prefix}-{index:03d}",
                abstract=body if body is not None else f"Agency {index} oversight findings. " * 12,
            ),
        )
        for index in range(count)
    ]


def _stratum(quota: int = 3, *, min_text_chars: int = 50, max_rows_scanned: int = 100) -> ProfileStratum:
    return ProfileStratum("gao-report-v1", "gao_reports", quota, min_text_chars, max_rows_scanned)


def _empty_exclusions() -> object:
    return build_exclusions(development_rows=(), gold_rows=())


def _drawn(record: SourceRecord):
    return build_drawn_artifact(record, settings=SETTINGS, counter=COUNTER)


# --------------------------------------------------------------------------
# deterministic, seeded, stratified selection
# --------------------------------------------------------------------------


def test_the_draw_is_reproducible_from_its_seed() -> None:
    records = _gao_records(30)
    first = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    second = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    assert [one.subject_id for one in first.artifacts] == [one.subject_id for one in second.artifacts]
    assert len(first.artifacts) == 3


def test_a_different_seed_draws_a_different_sample() -> None:
    records = _gao_records(40)
    baseline = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    other = draw_stratum(
        _stratum(),
        records,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        seed="a-different-recorded-constant",
    )
    assert {one.subject_id for one in baseline.artifacts} != {one.subject_id for one in other.artifacts}


def test_selection_order_ignores_row_content() -> None:
    """Rank depends on identity only, so editing text cannot move a row."""
    records = _gao_records(12)
    before = [key for key, _, _ in ranked_records(records)]
    edited = [
        SourceRecord(profile=GAO, row={**record.row, "abstract": "completely different body text"})
        for record in records
    ]
    assert [key for key, _, _ in ranked_records(edited)] == before


def test_row_order_in_the_table_does_not_change_the_draw() -> None:
    records = _gao_records(30)
    forward = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    backward = draw_stratum(
        _stratum(), list(reversed(records)), _empty_exclusions(), settings=SETTINGS, counter=COUNTER
    )
    assert [one.subject_id for one in forward.artifacts] == [one.subject_id for one in backward.artifacts]


def test_a_stratum_that_cannot_be_filled_is_refused() -> None:
    records = _gao_records(2)
    draw = draw_stratum(_stratum(quota=5), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    assert len(draw.artifacts) == 2


def test_thin_artifacts_are_rejected_by_the_declared_floor() -> None:
    records = _gao_records(10, body="tiny")
    draw = draw_stratum(
        _stratum(quota=3, min_text_chars=5_000), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER
    )
    assert draw.artifacts == ()
    assert draw.rejected["below_min_text_chars"] == 10


# --------------------------------------------------------------------------
# disjointness
# --------------------------------------------------------------------------


def test_an_artifact_sharing_a_development_digest_is_never_drawn() -> None:
    records = _gao_records(30)
    unfiltered = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    taken = unfiltered.artifacts[0]
    exclusions = build_exclusions(
        development_rows=[
            {
                "profile_id": taken.profile_id,
                "subject_type": taken.subject_type,
                "subject_id": "some-other-subject",
                "artifact_digest": taken.artifact_digest,
            }
        ],
        gold_rows=(),
    )
    draw = draw_stratum(_stratum(), records, exclusions, settings=SETTINGS, counter=COUNTER)
    assert taken.artifact_digest not in {one.artifact_digest for one in draw.artifacts}
    assert draw.rejected["excluded_by_artifact_digest"] == 1


def test_an_artifact_sharing_a_gold_subject_identity_is_never_drawn() -> None:
    records = _gao_records(30)
    unfiltered = draw_stratum(_stratum(), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    taken = unfiltered.artifacts[0]
    exclusions = build_exclusions(
        development_rows=(),
        gold_rows=[
            {
                "profile_id": taken.profile_id,
                "subject_type": taken.subject_type,
                "subject_id": taken.subject_id,
                "artifact_digest": "a-digest-from-an-older-snapshot",
                "gold_id": "gold_synthetic",
                "concept_label": "Synthetic label",
            }
        ],
    )
    draw = draw_stratum(_stratum(), records, exclusions, settings=SETTINGS, counter=COUNTER)
    assert taken.subject_id not in {one.subject_id for one in draw.artifacts}
    assert draw.rejected["excluded_by_subject_identity"] == 1


def test_identical_extracted_text_is_excluded_even_under_a_new_identity() -> None:
    """The same text re-issued under a new id is the same evaluation item."""
    shared = "A shared body of regulatory prose about drinking water standards. " * 6
    development = _drawn(SourceRecord(profile=GAO, row=_gao_row("dev-001", abstract=shared, title="One title")))
    assert development is not None
    exclusions = build_exclusions(
        development_rows=(),
        gold_rows=(),
        dataset_artifacts=[development],
    )
    records = [SourceRecord(profile=GAO, row=_gao_row("fresh-001", abstract=shared, title="One title"))]
    draw = draw_stratum(_stratum(quota=1), records, exclusions, settings=SETTINGS, counter=COUNTER)
    assert draw.artifacts == ()
    assert draw.rejected["excluded_by_extracted_text_sha256"] == 1


def test_the_same_text_is_never_drawn_twice_inside_one_draw() -> None:
    shared = "One body of text issued twice under two identifiers. " * 8
    records = [
        SourceRecord(profile=GAO, row=_gao_row("twin-a", abstract=shared, title="One title")),
        SourceRecord(profile=GAO, row=_gao_row("twin-b", abstract=shared, title="One title")),
    ]
    draw = draw_stratum(_stratum(quota=2), records, _empty_exclusions(), settings=SETTINGS, counter=COUNTER)
    assert len(draw.artifacts) == 1
    assert draw.rejected["duplicate_within_draw"] == 1


def _corpus(tmp_path: Path, *, gao: int, crs: int, prefix: str = "corpus") -> Path:
    directory = tmp_path / "corpus"
    directory.mkdir(exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                _gao_row(f"{prefix}-gao-{index:03d}", abstract=f"Oversight finding {index}. " * 14)
                for index in range(gao)
            ]
        ),
        directory / "gao_reports.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                _crs_row(f"{prefix}-crs-{index:03d}", title=f"Congressional analysis of topic {index}. " * 6)
                for index in range(crs)
            ]
        ),
        directory / "crs_reports.parquet",
    )
    return directory


def _two_strata(quota: int = 3) -> tuple[ProfileStratum, ...]:
    return (
        ProfileStratum("gao-report-v1", "gao_reports", quota, 50, 100),
        ProfileStratum("crs-report-v1", "crs_reports", quota, 50, 100),
    )


def test_a_full_draw_is_verified_disjoint_across_its_strata(tmp_path: Path) -> None:
    corpus = _corpus(tmp_path, gao=20, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=_two_strata(),
    )
    facts = verify_draw(draw, _empty_exclusions(), minimum_profiles=2)
    assert facts["passed"] is True
    assert facts["artifact_count"] == 6
    assert draw.artifacts_by_profile == {"crs-report-v1": 3, "gao-report-v1": 3}
    assert facts["shared_artifact_digests"] == []


def test_verification_refuses_a_draw_that_overlaps_development(tmp_path: Path) -> None:
    corpus = _corpus(tmp_path, gao=20, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=_two_strata(),
    )
    contaminated = build_exclusions(
        development_rows=[
            {
                "profile_id": draw.artifacts[0].profile_id,
                "subject_type": draw.artifacts[0].subject_type,
                "subject_id": draw.artifacts[0].subject_id,
                "artifact_digest": draw.artifacts[0].artifact_digest,
            }
        ],
        gold_rows=(),
    )
    with pytest.raises(HoldoutDisjointnessError, match="shared_artifact_digests"):
        verify_draw(draw, contaminated, minimum_profiles=2)


def test_verification_refuses_a_draw_below_the_profile_floor(tmp_path: Path) -> None:
    corpus = _corpus(tmp_path, gao=20, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=(ProfileStratum("gao-report-v1", "gao_reports", 3, 50, 100),),
    )
    with pytest.raises(HoldoutDrawError, match="source profiles"):
        verify_draw(draw, _empty_exclusions())


def test_verification_refuses_an_unfilled_stratum(tmp_path: Path) -> None:
    corpus = _corpus(tmp_path, gao=2, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=_two_strata(),
    )
    with pytest.raises(HoldoutDrawError, match="could not be filled"):
        verify_draw(draw, _empty_exclusions(), minimum_profiles=2)


def test_a_stratum_must_map_its_declared_source_table(tmp_path: Path) -> None:
    corpus = _corpus(tmp_path, gao=3, crs=3)
    mismatched = ProfileStratum("crs-report-v1", "gao_reports", 1, 10, 10)
    with pytest.raises(HoldoutDrawError, match="does not map source table"):
        list(stratum_records(corpus, mismatched))


# --------------------------------------------------------------------------
# blindness
# --------------------------------------------------------------------------


def _document(tmp_path: Path) -> dict[str, object]:
    corpus = _corpus(tmp_path, gao=20, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=_two_strata(),
    )
    return drafting_document(
        draw,
        generated_at="2026-07-28T00:00:00+00:00",
        corpus={"corpus_dir": str(corpus), "corpus_dataset_id": "synthetic"},
    )


def test_the_drafting_input_carries_segment_text_and_identity(tmp_path: Path) -> None:
    document = _document(tmp_path)
    assert document["schema_version"] == DRAFTING_SCHEMA_VERSION
    assert document["artifact_count"] == 6
    first = document["artifacts"][0]
    assert set(first) == {
        "profile_id",
        "source_table",
        "subject_type",
        "subject_id",
        "artifact_id",
        "artifact_digest",
        "title",
        "extracted_text_sha256",
        "segment_count",
        "segments",
    }
    segment = first["segments"][0]
    assert segment["text"]
    assert [one["ordinal"] for one in first["segments"]] == list(range(first["segment_count"]))
    assert segment["slices"]
    span = segment["slices"][0]
    assert segment["text"][span["segment_start_char"] : span["segment_end_char"]]


def test_the_drafting_input_is_blind(tmp_path: Path) -> None:
    document = _document(tmp_path)
    facts = assert_blind(document, forbidden_values={"gold_synthetic", "Drinking water"})
    assert facts["passed"] is True
    assert facts["string_values_checked"] > 0


@pytest.mark.parametrize(
    "injected",
    [
        {"candidates": [{"concept_id": "c-1"}]},
        {"registry_sha256": "deadbeef"},
        {"tagger_output": {"proposals": []}},
        {"expected_tags": []},
        {"candidate_selector": "lexical-overlap-v1"},
    ],
)
def test_blindness_refuses_registry_tagger_or_gold_framing(tmp_path: Path, injected: dict) -> None:
    document = _document(tmp_path)
    document["artifacts"][0].update(injected)
    with pytest.raises(HoldoutBlindnessError, match="banned_key_paths"):
        assert_blind(document)


def test_blindness_refuses_a_gold_value_hidden_under_an_innocent_key(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["artifacts"][0]["title"] = "gold_05485aadf13d9175a11ffec9"
    with pytest.raises(HoldoutBlindnessError, match="leaked_value_paths"):
        assert_blind(document, forbidden_values={"gold_05485aadf13d9175a11ffec9"})


def test_blindness_refuses_a_development_digest_anywhere(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document["artifacts"][0]["segments"][0]["headings"] = ["a-development-artifact-digest"]
    with pytest.raises(HoldoutBlindnessError, match="leaked_value_paths"):
        assert_blind(document, forbidden_values={"a-development-artifact-digest"})


def test_exclusions_forbid_every_gold_and_development_identifier() -> None:
    exclusions = build_exclusions(
        development_rows=[
            {
                "profile_id": "gao-report-v1",
                "subject_type": "gao_report",
                "subject_id": "dev-1",
                "artifact_digest": "dev-digest",
            }
        ],
        gold_rows=[
            {
                "profile_id": "gao-report-v1",
                "subject_type": "gao_report",
                "subject_id": "gold-1",
                "artifact_digest": "gold-digest",
                "gold_id": "gold_abc",
                "concept_label": "Drinking water",
                "exact_text": "a quoted span",
            }
        ],
    )
    assert {"dev-digest", "gold-digest", "dev-1", "gold-1", "gold_abc", "Drinking water", "a quoted span"} <= set(
        exclusions.forbidden_values
    )
    # The scheme enum is deliberately not forbidden.
    assert "subject" not in exclusions.forbidden_values


# --------------------------------------------------------------------------
# the boundary record
# --------------------------------------------------------------------------


def _boundary(tmp_path: Path) -> Path:
    path = tmp_path / "evaluation-boundary.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": BOUNDARY_SCHEMA_VERSION,
                "datasets": [{"dataset_id": "rulespec-development-35-v1", "role": "train"}],
                "pending_holdout": {"status": "not_created"},
                "adoption_policy": {"minimum_holdout_artifacts": 1},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _section(tmp_path: Path) -> dict[str, object]:
    corpus = _corpus(tmp_path, gao=20, crs=20)
    draw = draw_holdout(
        corpus,
        _empty_exclusions(),
        settings=SETTINGS,
        counter=COUNTER,
        strata=_two_strata(),
    )
    return pending_holdout_record(
        draw,
        drawn_at="2026-07-28T00:00:00+00:00",
        corpus={"corpus_dir": str(corpus)},
        disjointness=verify_draw(draw, _empty_exclusions(), minimum_profiles=2),
        drafting_input={"path": "/scratch/holdout.json", "sha256": "abc", "blind": True},
    )


def test_the_pinned_holdout_records_membership_and_reserves_configuration(tmp_path: Path) -> None:
    section = _section(tmp_path)
    assert section["status"] == "drawn_unadjudicated"
    draw = section["draw"]
    assert draw["artifact_count"] == 6
    assert len(draw["membership"]) == 6
    assert draw["selection_procedure"]
    assert draw["selection_seed"]
    assert draw["selection_sha256"]
    assert draw["membership_sha256"]
    assert draw["drawn_at"] == "2026-07-28T00:00:00+00:00"
    assert draw["profile_strata"][0]["quota"] == 3
    assert draw["disjointness"]["passed"] is True
    assert set(draw["membership"][0]) == {
        "profile_id",
        "source_table",
        "subject_type",
        "subject_id",
        "artifact_id",
        "artifact_digest",
        "extracted_text_sha256",
        "extracted_text_chars",
        "segment_count",
    }


def test_configuration_pins_are_reserved_not_frozen(tmp_path: Path) -> None:
    section = _section(tmp_path)
    frozen = section["frozen_configuration"]
    assert frozen["status"] == "RESERVED"
    for key in (
        "candidate_selector",
        "prompt_concept_limit",
        "registry_sha256",
        "tag_instructions_sha256",
        "tag_schema_sha256",
        "prompt_input_token_budget",
        "prompt_safety_margin_tokens",
    ):
        assert frozen[key] == "RESERVED"
    assert section["holdout_controls"]["configuration_frozen_before_labels"] is False
    assert section["labels"]["status"] == "not_drafted"
    assert section["labels"]["gold_sha256"] is None
    assert section["adjudication"]["status"] == "not_started"
    assert section["adjudication"]["reviewers"] == []


def test_the_boundary_update_touches_only_the_pending_holdout(tmp_path: Path) -> None:
    path = _boundary(tmp_path)
    before = json.loads(path.read_text(encoding="utf-8"))
    manifest = update_boundary_manifest(path, _section(tmp_path))
    assert manifest["datasets"] == before["datasets"]
    assert manifest["adoption_policy"] == before["adoption_policy"]
    assert manifest["schema_version"] == before["schema_version"]
    assert manifest["pending_holdout"]["status"] == "drawn_unadjudicated"
    assert list(manifest) == list(before)


def test_the_boundary_update_refuses_a_foreign_schema(tmp_path: Path) -> None:
    path = tmp_path / "other.json"
    path.write_text(json.dumps({"schema_version": "something-else"}) + "\n", encoding="utf-8")
    with pytest.raises(HoldoutDrawError, match="schema"):
        update_boundary_manifest(path, {"status": "drawn_unadjudicated"})


def test_the_boundary_update_refuses_a_record_without_a_pending_section(tmp_path: Path) -> None:
    path = tmp_path / "no-pending.json"
    path.write_text(
        json.dumps({"schema_version": BOUNDARY_SCHEMA_VERSION, "datasets": []}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(HoldoutDrawError, match="pending_holdout"):
        update_boundary_manifest(path, {"status": "drawn_unadjudicated"})

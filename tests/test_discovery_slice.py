"""Hermetic tests for the deterministic discovery-slice harnesses.

Nothing here touches the real snapshot, the network, or a provider. The
scoring primitives are exercised on hand-built sets, the two independent
matchers on hand-written citation strings, and each experiment end to end on a
tiny synthetic parquet snapshot built in a temporary directory.

The synthetic snapshots deliberately reproduce the failure shapes the real run
found — a near-miss part (`40 CFR 600`) that must stay out, and a U.S.C. range
citation (`42 U.S.C. 7401-7671q`) the authority parser reads as an opaque
section — so a future fix to either has a fast, offline test that notices.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS = REPO_ROOT / "tools"


def _load(name: str):
    """Load a tools/ script by path, the way the repo's other harness tests do."""
    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


scoring = _load("discovery_scoring")
cfr60 = _load("discovery_question_cfr60")
usc7401 = _load("discovery_question_usc7401")


# --------------------------------------------------------------------------- #
# scoring primitives
# --------------------------------------------------------------------------- #


def test_score_sets_exact_match_scores_one_and_reports_exact():
    score = scoring.score_sets(expected=["a", "b"], returned=["b", "a"])
    assert (score.precision, score.recall, score.f1) == (1.0, 1.0, 1.0)
    assert score.exact
    assert score.missing == () and score.extra == ()


def test_score_sets_separates_missing_from_extra():
    score = scoring.score_sets(expected=["a", "b", "c"], returned=["b", "c", "d"])
    assert score.missing == ("a",)
    assert score.extra == ("d",)
    assert score.recall == pytest.approx(2 / 3)
    assert score.precision == pytest.approx(2 / 3)
    assert not score.exact


def test_score_sets_flags_a_returned_forbidden_identifier():
    score = scoring.score_sets(expected=["a"], returned=["a", "near"], forbidden=["near"])
    assert score.forbidden_returned == ("near",)
    assert not score.exact
    # A forbidden hit is also an ordinary false positive; both are reported.
    assert score.extra == ("near",)


def test_score_sets_rejects_an_expectation_that_contradicts_itself():
    with pytest.raises(ValueError, match="overlap"):
        scoring.score_sets(expected=["a"], returned=["a"], forbidden=["a"])


def test_score_sets_neither_credits_nor_penalises_ambiguous_members():
    penalised = scoring.score_sets(expected=["a"], returned=["a", "maybe"])
    excused = scoring.score_sets(expected=["a"], returned=["a", "maybe"], ambiguous=["maybe"])
    assert penalised.precision == pytest.approx(0.5)
    assert excused.precision == 1.0
    assert excused.exact
    assert excused.ambiguous_returned == ("maybe",)
    assert excused.ambiguous_excluded == 1


def test_score_sets_returns_zero_rather_than_dividing_by_zero():
    score = scoring.score_sets(expected=["a"], returned=[])
    assert (score.precision, score.recall, score.f1) == (0.0, 0.0, 0.0)


def test_predicate_exactness_names_each_violating_row():
    rows = [{"id": "1", "stage": "proposed"}, {"id": "2", "stage": "final"}]
    result = scoring.predicate_exactness(
        rows,
        predicate=lambda row: row["stage"] == "proposed",
        describe=lambda row: row["id"],
        unknown=lambda row: row["stage"] is None,
        unknown_universe=7,
    )
    assert result.violations == ("2",)
    assert result.exactness == pytest.approx(0.5)
    assert result.unknown_value_rows == 7
    assert result.unknown_value_admitted == 0


def test_predicate_exactness_counts_admitted_unknown_values():
    rows = [{"id": "1", "stage": None}]
    result = scoring.predicate_exactness(
        rows,
        predicate=lambda row: True,
        describe=lambda row: row["id"],
        unknown=lambda row: row["stage"] is None,
        unknown_universe=3,
    )
    assert result.unknown_value_admitted == 1


def test_compare_counts_treats_a_one_sided_name_as_a_mismatch():
    comparison = scoring.compare_counts({"dockets": 5, "rows": 9}, {"dockets": 5})
    assert not comparison.matches
    assert comparison.mismatches == ("rows: expected 9, actual absent",)


def test_snapshot_identity_pins_digests_and_names_every_missing_file(tmp_path):
    (tmp_path / "present.parquet").write_bytes(b"payload")
    digests = scoring.snapshot_identity(tmp_path, ["present.parquet"])
    assert digests["present.parquet"] == scoring.sha256_file(tmp_path / "present.parquet")
    with pytest.raises(FileNotFoundError, match="a.parquet, b.parquet"):
        scoring.snapshot_identity(tmp_path, ["b.parquet", "a.parquet"])


# --------------------------------------------------------------------------- #
# independent matchers
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("EPA-HQ-OAR-2005-0169", "EPA-HQ-OAR-2005-0169"),
        ("  epa-hq-oar-2005-0169 ", "EPA-HQ-OAR-2005-0169"),
        ("EPA_HQ_OAR", "EPA_HQ_OAR"),
        ("EPA HQ", None),
        ("EPA-", None),
        (None, None),
    ],
)
def test_normalize_docket(value, expected):
    assert cfr60.normalize_docket(value) == expected


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ({"title": 40, "part": "60"}, True),
        ({"title": "40", "part": 60}, True),
        ({"title": 40, "part": "600"}, False),
        ({"title": 40, "part": "60.1"}, False),
        ({"title": 10, "part": "60"}, False),
        ({"title": 40, "part": None}, False),
        ({"title": None, "part": "60"}, False),
        ("40 CFR 60", False),
    ],
)
def test_cfr_entry_matches_compares_components_not_substrings(entry, expected):
    assert cfr60.cfr_entry_matches(entry, "40", "60") is expected


@pytest.mark.parametrize(
    ("text", "verdict"),
    [
        ("42 U.S.C. 7401", "names"),
        ("42 U.S.C. 7401 et seq. Clean Air Act", "names"),
        ("42 USC 7401", "names"),
        ("42 U.S.C. 7401-7671q.", "names"),
        ("42 U.S.C. 7401 to 7671q", "names"),
        ("42 U.S.C. 7409, 7410, 7401", "names"),
        ("8 U.S.C. 1101, 1103; 42 U.S.C. 7401", "names"),
        ("42 U.S.C. 7300-7500", "spans"),
        ("42 U.S.C. 7411 Clean Air Act", "absent"),
        ("Clean Air Act", "absent"),
        ("5 U.S.C. 7401", "absent"),
        # The digits belong to the Statutes at Large citation, not to the code.
        ("42 U.S.C. 300f, Pub. L. 104-182, 110 Stat. 7401", "absent"),
    ],
)
def test_classify_usc_reference(text, verdict):
    assert usc7401.classify_usc_reference(text, "42", "7401") == verdict


@pytest.mark.parametrize(
    ("value", "expected"),
    [("2060-AW96", "2060-AW96"), (" 2060-aw96 ", "2060-AW96"), ("2060AW96", None), (None, None)],
)
def test_normalize_rin(value, expected):
    assert usc7401.normalize_rin(value) == expected


# --------------------------------------------------------------------------- #
# end-to-end on a synthetic snapshot
# --------------------------------------------------------------------------- #


def _write(directory: Path, name: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    table = pa.table({column: pa.array([row.get(column) for row in rows], type=pa.string()) for column in columns})
    pq.write_table(table, directory / f"{name}.parquet")


def _cfr_snapshot(tmp_path: Path, *, rule_target_rows: list[dict[str, Any]]) -> Path:
    """Two dockets touch 40 CFR 60; a third touches only the near-miss part 600."""
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    _write(
        snapshot,
        "dockets",
        [{"docket_id": "D-1"}, {"docket_id": "D-2"}, {"docket_id": "D-3"}],
        ("docket_id", "rin"),
    )
    _write(
        snapshot,
        "documents",
        [{"document_id": "DOC-1", "docket_id": "D-2", "fr_doc_num": "FR-1"}],
        ("document_id", "docket_id", "fr_doc_num", "additional_rins"),
    )
    _write(
        snapshot,
        "federal_register",
        [
            {
                "document_number": "FR-1",
                "cfr_references_json": json.dumps([{"title": 40, "part": "60"}]),
            },
            {
                "document_number": "FR-2",
                "cfr_references_json": json.dumps([{"title": 40, "part": "600"}]),
            },
        ],
        ("document_number", "cfr_references_json", "regulation_id_numbers_json"),
    )
    _write(
        snapshot,
        "fr_docket_links",
        [
            {"document_number": "FR-1", "docket_id": "D-1"},
            {"document_number": "FR-2", "docket_id": "D-3"},
        ],
        ("document_number", "docket_id"),
    )
    _write(snapshot, "unified_agenda", [], ("rin", "cfr_references_json", "legal_authority_json"))
    _write(
        snapshot,
        "rule_targets",
        rule_target_rows,
        ("docket_id", "cfr_ref", "cfr_title", "cfr_part", "cfr_section", "rin", "source", "evidence_id"),
    )
    return snapshot


def _target_row(docket: str, part: str = "60", source: str = "fr_cfr_ref") -> dict[str, Any]:
    return {
        "docket_id": docket,
        "cfr_ref": f"40-{part}",
        "cfr_title": "40",
        "cfr_part": part,
        "cfr_section": None,
        "rin": None,
        "source": source,
        "evidence_id": "FR-1",
    }


def test_cfr60_experiment_scores_a_correct_table_as_exact(tmp_path):
    snapshot = _cfr_snapshot(
        tmp_path, rule_target_rows=[_target_row("D-1"), _target_row("D-2", source="document_fr_doc")]
    )
    out = tmp_path / "record.json"
    assert cfr60.main(["--snapshot", str(snapshot), "--out", str(out)]) == 0
    record = json.loads(out.read_text())
    assert record["scores"]["link"]["exact"] is True
    assert record["expectation"]["expected"] == ["D-1", "D-2"]
    assert record["expectation"]["forbidden"] == ["D-3"]
    assert record["scores"]["filter"]["exactness"] == 1.0
    assert record["snapshot"]["sha256"]["rule_targets.parquet"]


def test_cfr60_experiment_fails_when_the_table_admits_the_near_miss_part(tmp_path):
    snapshot = _cfr_snapshot(
        tmp_path,
        rule_target_rows=[_target_row("D-1"), _target_row("D-2", source="document_fr_doc"), _target_row("D-3")],
    )
    out = tmp_path / "record.json"
    assert cfr60.main(["--snapshot", str(snapshot), "--out", str(out)]) == 1
    link = json.loads(out.read_text())["scores"]["link"]
    assert link["forbidden_returned"] == ["D-3"]
    assert link["precision"] == pytest.approx(2 / 3)


def _usc_snapshot(tmp_path: Path, *, ranges_as_endpoints: bool = False) -> Path:
    """Miniature of the real failure: a range citation the parser keeps opaque.

    ``ranges_as_endpoints`` switches `authority_edges` to the shape
    `parse_authority_citation` publishes after `0378a9a` — a range becomes the
    two sections its source text names, `usc_section` keeping the first — so
    both readings of the same snapshot are scored by the same harness.
    """
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    _write(
        snapshot,
        "unified_agenda",
        [
            {"rin": "1111-AA11", "legal_authority_json": json.dumps(["42 U.S.C. 7401 et seq."])},
            {"rin": "2222-BB22", "legal_authority_json": json.dumps(["42 U.S.C. 7401-7671q."])},
            {"rin": "3333-CC33", "legal_authority_json": json.dumps(["42 U.S.C. 7411"])},
        ],
        ("rin", "legal_authority_json", "cfr_references_json"),
    )
    _write(
        snapshot,
        "dockets",
        [
            {"docket_id": "D-1", "rin": "1111-AA11"},
            {"docket_id": "D-2", "rin": "2222-BB22"},
            {"docket_id": "D-3", "rin": "3333-CC33"},
        ],
        ("docket_id", "rin"),
    )
    _write(snapshot, "documents", [], ("document_id", "docket_id", "fr_doc_num", "additional_rins"))
    _write(
        snapshot,
        "federal_register",
        [],
        ("document_number", "cfr_references_json", "regulation_id_numbers_json"),
    )
    # Before the fix the parser read the range as one opaque section, so
    # 2222-BB22 never acquired a `7401` edge — the defect the real snapshot
    # showed. After it, the same citation carries `7401` in `usc_section`.
    ranged = (
        {"rin": "2222-BB22", "usc_title": "42", "usc_section": "7401", "usc_section_end": "7671q"}
        if ranges_as_endpoints
        else {"rin": "2222-BB22", "usc_title": "42", "usc_section": "7401-7671q"}
    )
    _write(
        snapshot,
        "authority_edges",
        [
            {"rin": "1111-AA11", "usc_title": "42", "usc_section": "7401"},
            ranged,
            {"rin": "3333-CC33", "usc_title": "42", "usc_section": "7411"},
        ],
        ("rin", "usc_title", "usc_section", "usc_section_end"),
    )
    _write(
        snapshot,
        "agenda_item_proceedings",
        [
            {"rin": "1111-AA11", "proceeding_id": "P-1", "agenda_item_id": "urn:1", "source": "docket_rin"},
            {"rin": "1111-AA11", "proceeding_id": "P-final", "agenda_item_id": "urn:1", "source": "docket_rin"},
            {"rin": "2222-BB22", "proceeding_id": "P-2", "agenda_item_id": "urn:2", "source": "docket_rin"},
            {"rin": "3333-CC33", "proceeding_id": "P-3", "agenda_item_id": "urn:3", "source": "docket_rin"},
        ],
        ("rin", "proceeding_id", "agenda_item_id", "source", "evidence_id"),
    )
    _write(
        snapshot,
        "proceedings",
        [
            {"proceeding_id": "P-1", "current_stage": "proposed"},
            {"proceeding_id": "P-2", "current_stage": "proposed"},
            {"proceeding_id": "P-3", "current_stage": "proposed"},
            {"proceeding_id": "P-final", "current_stage": "final"},
        ],
        ("proceeding_id", "current_stage"),
    )
    return snapshot


def test_usc7401_experiment_reports_the_range_citation_as_missing_recall(tmp_path):
    snapshot = _usc_snapshot(tmp_path)
    out = tmp_path / "record.json"
    assert usc7401.main(["--snapshot", str(snapshot), "--out", str(out)]) == 1
    record = json.loads(out.read_text())

    # The independent scan names both 7401 RINs; the system's edges name one.
    assert record["expectation"]["rins_naming_target"] == ["1111-AA11", "2222-BB22"]
    assert record["scores"]["authority_link"]["missing"] == ["2222-BB22"]
    assert record["scores"]["link"]["missing"] == ["P-2"]
    assert record["scores"]["link"]["recall"] == pytest.approx(0.5)

    # Precision still holds: the near-miss 7411 rulemaking stays out, and the
    # completed proceeding is excluded by the recorded active definition.
    assert record["expectation"]["forbidden_active_proceedings"] == ["P-3"]
    assert record["scores"]["link"]["forbidden_returned"] == []
    assert record["scores"]["link"]["precision"] == 1.0
    assert "P-final" not in record["system"]["returned"]
    assert record["scores"]["filter"]["exactness"] == 1.0


def test_usc7401_experiment_scores_endpoint_ranges_as_exact(tmp_path):
    """The same snapshot, with ranges published as endpoints, scores 1.000.

    This is the fixture-scale form of the 2026-08-01 re-score: nothing about
    the question, the expectation, or the harness changed — only the encoding
    of the range row — and the recall the 2026-07-28 record froze at 0.8125
    becomes exact. It also pins the reason no query change was needed: the
    exact filter on `usc_section = '7401'` finds a range by its first endpoint.
    """
    snapshot = _usc_snapshot(tmp_path, ranges_as_endpoints=True)
    out = tmp_path / "record.json"
    assert usc7401.main(["--snapshot", str(snapshot), "--out", str(out)]) == 0
    record = json.loads(out.read_text())

    assert record["scores"]["authority_link"]["missing"] == []
    assert record["scores"]["authority_link"]["recall"] == 1.0
    assert record["scores"]["link"]["missing"] == []
    assert record["scores"]["link"]["recall"] == 1.0
    assert record["scores"]["link"]["precision"] == 1.0
    assert record["scores"]["aggregate"]["matches"] is True

    # The near miss stays out and the completed proceeding stays out: the
    # endpoint encoding widens recall without loosening either guard.
    assert record["scores"]["link"]["forbidden_returned"] == []
    assert "P-final" not in record["system"]["returned"]


def test_usc7401_experiment_reports_fan_out_per_rin(tmp_path):
    snapshot = _usc_snapshot(tmp_path)
    out = tmp_path / "record.json"
    usc7401.main(["--snapshot", str(snapshot), "--out", str(out)])
    fan_out = json.loads(out.read_text())["fan_out"]
    assert fan_out["proceedings_per_expected_rin"]["1111-AA11"] == 2
    assert fan_out["max"] == 2
    assert fan_out["rins_tracking_multiple_proceedings"] == ["1111-AA11"]


def test_usc7401_active_definition_is_an_allowlist_not_a_negation():
    """The recorded definition must never admit a stage-unknown proceeding."""
    assert set(usc7401.ACTIVE_STAGES).isdisjoint(usc7401.TERMINAL_STAGES)
    assert None not in usc7401.ACTIVE_STAGES

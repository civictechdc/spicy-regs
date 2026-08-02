"""Hermetic checks for the citation-parsing bakeoff harness.

The bakeoff has two halves and they are held to different standards.

The **detection** half is deterministic: given the frozen authority strings and
a pinned CiteURL result set, it must produce byte-identical artifacts on every
rebuild, from any working directory. These tests state that guarantee, plus the
extraction and classification rules underneath it.

The **adjudication** half calls a frontier model and is inherently
non-deterministic. What is testable there is discipline, not output: the
stratified draw is seeded and reproducible, the cost projection is checked
against the cap *before* any call is made, and a per-item provider failure is
recorded as a failure rather than retried until it agrees.

Nothing here reads the real corpus, imports CiteURL, or reaches a provider.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "run_citation_bakeoff.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_citation_bakeoff", TOOL_PATH)
    assert spec and spec.loader, f"could not load {TOOL_PATH}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()


def _agenda(path: Path, authority_lists: list[object]) -> Path:
    """Write a minimal ``unified_agenda.parquet`` carrying only what is read."""
    rows = [
        {
            "rin": f"0000-AA{index:02d}",
            "legal_authority_json": value if isinstance(value, str) or value is None else json.dumps(value),
        }
        for index, value in enumerate(authority_lists)
    ]
    table = pa.table(
        {
            "rin": pa.array([row["rin"] for row in rows], pa.string()),
            "legal_authority_json": pa.array([row["legal_authority_json"] for row in rows], pa.string()),
        }
    )
    pq.write_table(table, path)
    return path


# --------------------------------------------------------------------------
# extraction: the step the decision record says no committed command performed
# --------------------------------------------------------------------------


def test_extraction_dedupes_across_rows_and_sorts(tmp_path):
    path = _agenda(
        tmp_path / "unified_agenda.parquet",
        [["42 U.S.C. 7401", "5 U.S.C. 552"], ["5 U.S.C. 552"], ["42 U.S.C. 7401"]],
    )
    frozen = mod.extract_authority_strings(path)
    assert frozen.strings == ["42 U.S.C. 7401", "5 U.S.C. 552"]
    assert frozen.rows_read == 3
    assert frozen.values_read == 4


def test_extraction_counts_malformed_json_instead_of_dropping_it_silently(tmp_path):
    path = _agenda(tmp_path / "unified_agenda.parquet", [["5 U.S.C. 552"], "{not json", None])
    frozen = mod.extract_authority_strings(path)
    assert frozen.strings == ["5 U.S.C. 552"]
    assert frozen.malformed_rows == 1
    assert frozen.empty_rows == 1


def test_extraction_drops_blank_members_but_keeps_interior_whitespace(tmp_path):
    path = _agenda(tmp_path / "unified_agenda.parquet", [["", "   ", None, "15 U.S.C.  78q"]])
    frozen = mod.extract_authority_strings(path)
    assert frozen.strings == ["15 U.S.C.  78q"]


def test_extraction_digest_is_over_the_string_set_not_the_parquet(tmp_path):
    """Two parquet files that disagree byte-for-byte may still freeze one set."""
    first = _agenda(tmp_path / "a.parquet", [["5 U.S.C. 552", "42 U.S.C. 7401"]])
    second = _agenda(tmp_path / "b.parquet", [["42 U.S.C. 7401"], ["5 U.S.C. 552"], ["5 U.S.C. 552"]])
    assert mod.extract_authority_strings(first).digest == mod.extract_authority_strings(second).digest


# --------------------------------------------------------------------------
# the two detection arms
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    ["42 U.S.C. 7401", "Pub. L. 117-2", "117 Stat. 429", "Executive Order 13563", "5 USC 4101 et seq."],
)
def test_current_authority_arm_recognizes_the_forms_it_owns(text):
    assert mod.current_authority_recognized(text) is True


@pytest.mark.parametrize("text", ["", "Agency discretion", "Departmental policy statement"])
def test_current_authority_arm_reports_its_own_refusal(text):
    """``other``/``failed`` is the parser saying it recognized nothing."""
    assert mod.current_authority_recognized(text) is False


def test_current_families_reads_every_project_owned_grammar():
    assert mod.current_families("42 U.S.C. 7401") == ["usc"]
    assert mod.current_families("Pub. L. No. 117-338, 136 Stat. 6156") == ["pl", "stat"]
    assert mod.current_families("Executive Order 13563") == ["eo"]
    assert mod.current_families("40 CFR Part 60") == ["cfr"]
    assert mod.current_families("Docket No. FAA-2026-3485") == ["docket"]
    assert mod.current_families("Departmental policy statement") == []


def test_current_families_records_the_grammar_overlap_that_remains():
    """One project-owned grammar still claims a string another one owns.

    ``normalize_docket_reference`` returns early for any value the
    Regulations.gov *syntax* can express, before the stricter docket-shape
    check, so every RIN also reads as a docket. Not a bug in this harness and
    not fixed here; recorded because it inflates the extended arm's per-family
    counts, and a reader of those counts has to know.
    """
    assert mod.current_families("0648-AB12") == ["docket", "rin"]


def test_the_compact_key_no_longer_claims_a_federal_register_document_number():
    """The second recorded overlap is closed, and the sealed artifact predates it.

    ``parse_cfr_citation``'s compact-key branch read any bare ``N-M`` as
    title-part, so ``2026-13078`` also read as CFR. Bounding the unanchored
    branch to the 50 titles the CFR has closes it — the same guard that stops
    ``'5401-5405'``, the one measured false positive
    (docs/evidence/citation-bakeoff-2026-08-02.md, "False positives").

    ``output/citation-bakeoff-2026-08-02`` was measured against the parser at
    ``b7a5632``, which the evidence doc pins, so re-running ``detect`` after
    this fix is expected to produce a different artifact. The recorded
    four-cell table is a fact about that pin, not a claim about HEAD.
    """
    assert mod.current_families("2026-13078") == ["docket", "fr_doc"]
    assert mod.cfr_compact_key_only("2026-13078") is False
    assert mod.cfr_compact_key_only("40-60") is True
    assert mod.cfr_compact_key_only("40 CFR Part 60") is False


def test_current_families_is_sorted_and_deduplicated():
    families = mod.current_families("42 U.S.C. 7401, 7671q and 40 CFR Parts 60 and 63")
    assert families == sorted(set(families))


def test_citeurl_families_map_every_template_the_frozen_corpus_fires():
    """Every template observed on the frozen corpus has a declared family."""
    for template in mod.OBSERVED_CITEURL_TEMPLATES:
        assert template in mod.CITEURL_TEMPLATE_FAMILIES, template


def test_citeurl_family_for_an_undeclared_template_is_surfaced_not_dropped():
    families, unmapped = mod.citeurl_families(["U.S. Code", "Belgian Tax Code"])
    assert families == ["usc"]
    assert unmapped == ["Belgian Tax Code"]


def test_citeurl_federal_register_family_is_volume_page_not_document_number():
    """The two 'FR' families are different things and must not be conflated.

    CiteURL's Federal Register template reads ``89 FR 1234`` (volume/page); the
    project's FR grammar reads a document number (``2026-13078``). Scoring them
    as one family would credit each arm with the other's capability.
    """
    families, _ = mod.citeurl_families(["Federal Register"])
    assert families == ["fr_vol_page"]
    assert "fr_vol_page" not in mod.current_families("2026-13078")


# --------------------------------------------------------------------------
# classification: the four cells the decision record reported
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("current", "citeurl", "cell"),
    [
        (True, True, "both"),
        (True, False, "current_only"),
        (False, True, "citeurl_only"),
        (False, False, "neither"),
    ],
)
def test_classify_cell(current, citeurl, cell):
    assert mod.classify_cell(current=current, citeurl=citeurl) == cell


def test_four_cell_table_counts_each_string_exactly_once():
    records = [
        {"cell": "both"},
        {"cell": "both"},
        {"cell": "current_only"},
        {"cell": "citeurl_only"},
        {"cell": "neither"},
    ]
    table = mod.four_cell_table(records)
    assert table == {"both": 2, "current_only": 1, "citeurl_only": 1, "neither": 1}
    assert sum(table.values()) == len(records)


# --------------------------------------------------------------------------
# the deterministic detection artifact
# --------------------------------------------------------------------------


CITEURL_STUB = {
    "42 U.S.C. 7401": ["U.S. Code"],
    "Executive Order 13563": [],
    "40 CFR Part 60": ["Code of Federal Regulations"],
    "Departmental policy statement": [],
    "89 FR 1234": ["Federal Register"],
}


def _build(tmp_path: Path, output: Path):
    path = _agenda(tmp_path / "unified_agenda.parquet", [sorted(CITEURL_STUB)])
    frozen = mod.extract_authority_strings(path)
    return mod.build_detection_artifact(
        frozen=frozen,
        citeurl_templates=CITEURL_STUB,
        citeurl_pin={"package": "citeurl", "version": "12.0.3", "workaround": "markdown installed alongside"},
        source_path=path,
        output=output,
    )


def test_detection_rebuild_is_byte_identical(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    _build(tmp_path, first)
    _build(tmp_path, second)
    for name in ("authority-strings.json", "detection.json", "detection-receipt.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


def test_detection_receipt_holds_no_absolute_paths(tmp_path):
    _build(tmp_path, tmp_path / "out")
    receipt = (tmp_path / "out" / "detection-receipt.json").read_text()
    assert str(tmp_path) not in receipt
    assert '"/' not in receipt


def test_detection_artifact_carries_no_credential(tmp_path, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "sk-not-a-real-key-000")
    _build(tmp_path, tmp_path / "out")
    for name in ("authority-strings.json", "detection.json", "detection-receipt.json"):
        mod.assert_secret_free((tmp_path / "out" / name).read_text())


def test_assert_secret_free_rejects_a_leaked_credential(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "sk-not-a-real-key-000")
    with pytest.raises(mod.SecretLeakError):
        mod.assert_secret_free('{"key": "sk-not-a-real-key-000"}')


def test_assert_secret_free_ignores_a_short_or_empty_environment_value(monkeypatch):
    """A one-character credential must not make every artifact 'leaky'."""
    monkeypatch.setenv("GEMINI_API_KEY", "x")
    mod.assert_secret_free('{"note": "excellent"}')


def test_detection_records_both_the_frozen_digest_and_the_source_digest(tmp_path):
    _build(tmp_path, tmp_path / "out")
    receipt = json.loads((tmp_path / "out" / "detection-receipt.json").read_text())
    assert receipt["inputs"]["authority_strings"]["digest"].startswith("sha256:")
    assert receipt["inputs"]["unified_agenda"]["digest"].startswith("sha256:")
    assert receipt["inputs"]["unified_agenda"]["path"] == "unified_agenda.parquet"


def test_detection_reports_the_extended_arm_beside_the_probe_arm(tmp_path):
    """The probe compared one function; the project owns more grammars.

    Both comparisons are published so a reader can see whether a CiteURL-only
    win is a real gap or a citation the project already parses elsewhere.
    """
    _build(tmp_path, tmp_path / "out")
    detection = json.loads((tmp_path / "out" / "detection.json").read_text())
    assert set(detection["four_cell"]) == {"both", "current_only", "citeurl_only", "neither"}
    assert set(detection["four_cell_extended"]) == {"both", "current_only", "citeurl_only", "neither"}
    # "40 CFR Part 60" is invisible to parse_authority_citation and visible to
    # parse_cfr_citation, so it moves out of citeurl_only between the two
    # comparisons. "89 FR 1234" does not move: no project grammar reads a
    # Federal Register volume/page reference at all, so it is a real gap.
    assert detection["four_cell"]["citeurl_only"] == 2
    assert detection["four_cell_extended"]["citeurl_only"] == 1
    moved = [record for record in detection["records"] if record["cell"] != record["cell_extended"]]
    assert [record["text"] for record in moved] == ["40 CFR Part 60"]


# --------------------------------------------------------------------------
# adjudication discipline
# --------------------------------------------------------------------------


def _disagreements(count: int = 30) -> list[dict]:
    cells = ("current_only", "citeurl_only", "neither")
    return [
        {"string_id": f"s{index:03d}", "text": f"authority {index}", "cell": cells[index % 3]} for index in range(count)
    ]


def test_stratified_draw_is_seeded_and_reproducible():
    records = _disagreements()
    first = mod.stratified_sample(records, per_stratum=4, seed=17)
    second = mod.stratified_sample(records, per_stratum=4, seed=17)
    assert [item["string_id"] for item in first] == [item["string_id"] for item in second]


def test_stratified_draw_changes_with_the_seed():
    records = _disagreements()
    first = mod.stratified_sample(records, per_stratum=4, seed=17)
    other = mod.stratified_sample(records, per_stratum=4, seed=18)
    assert [item["string_id"] for item in first] != [item["string_id"] for item in other]


def test_stratified_draw_caps_each_stratum_independently():
    drawn = mod.stratified_sample(_disagreements(), per_stratum=3, seed=1)
    counts = {cell: sum(1 for item in drawn if item["cell"] == cell) for item in drawn for cell in [item["cell"]]}
    assert counts == {"current_only": 3, "citeurl_only": 3, "neither": 3}


def test_stratified_draw_with_no_cap_is_a_census_in_deterministic_order():
    records = _disagreements()
    drawn = mod.stratified_sample(records, per_stratum=None, seed=17)
    assert [item["string_id"] for item in drawn] == sorted(item["string_id"] for item in records)


def test_stratified_draw_never_touches_the_agreement_cell():
    records = [*_disagreements(6), {"string_id": "agree", "text": "42 U.S.C. 7401", "cell": "both"}]
    drawn = mod.stratified_sample(records, per_stratum=None, seed=1)
    assert all(item["cell"] != "both" for item in drawn)


def test_cost_projection_uses_pinned_prices():
    projected = mod.project_cost(calls=100, input_tokens=600, output_tokens=300, prices=mod.PRICES_USD_PER_MTOK)
    expected = (100 * 600 / 1_000_000) * mod.PRICES_USD_PER_MTOK["input"] + (
        100 * 300 / 1_000_000
    ) * mod.PRICES_USD_PER_MTOK["output"]
    assert projected == pytest.approx(expected)


def test_a_projection_over_the_cap_refuses_before_any_call_is_made():
    with pytest.raises(mod.CostCapExceededError) as excinfo:
        mod.enforce_cost_cap(projected_usd=9.99, cap_usd=5.0, calls=620)
    assert "sample down" in str(excinfo.value)


def test_a_projection_under_the_cap_returns_the_headroom():
    headroom = mod.enforce_cost_cap(projected_usd=1.20, cap_usd=5.0, calls=620)
    assert headroom == pytest.approx(3.80)


class _FailingModel:
    model_id = "gemini:stub"
    model = "stub"
    provider = "gemini"
    run_configuration: dict = {}
    structured_mode = "prompted"
    base_url_host = "example.invalid"

    def __init__(self) -> None:
        self.calls = 0

    def secret_free_request(self, **kwargs):
        return {"model": "stub", "messages": [], "max_tokens": kwargs.get("max_output_tokens", 1)}

    def structured_json(self, **kwargs):
        self.calls += 1
        error = mod.StructuredTextCallError("provider said no", call={"status": "provider_error", "attempts": []})
        raise error


def test_a_provider_failure_is_one_receipt_not_a_retry_until_agree():
    model = _FailingModel()
    record = mod.adjudicate_one(model, {"string_id": "s000", "text": "x", "cell": "neither"})
    assert model.calls == 1
    assert record["status"] == "failed"
    assert record["error_code"] == "StructuredTextCallError"
    assert record["call"]["status"] == "provider_error"
    assert record["verdict"] is None


class _AnsweringModel(_FailingModel):
    def structured_json(self, **kwargs):
        self.calls += 1
        return mod.StructuredTextResult(
            output={
                "string_id": "s000",
                "contains_citations": True,
                "citations": [{"family": "eo", "text": "Executive Order 13563"}],
                "verdict": "current_parser_correct",
                "reason": "the string names an executive order",
            },
            call={"status": "completed", "input_tokens": 500, "output_tokens": 60},
        )


def test_an_answered_item_carries_its_request_and_response_digests():
    model = _AnsweringModel()
    record = mod.adjudicate_one(model, {"string_id": "s000", "text": "Executive Order 13563", "cell": "current_only"})
    assert record["status"] == "adjudicated"
    assert record["verdict"] == "current_parser_correct"
    assert record["request_sha256"].startswith("sha256:")
    assert record["response_sha256"].startswith("sha256:")
    assert record["call"]["input_tokens"] == 500


def test_an_answer_whose_echoed_id_disagrees_is_recorded_as_a_mismatch():
    """A model that answers about a different string has not judged this one."""
    model = _AnsweringModel()
    record = mod.adjudicate_one(model, {"string_id": "s999", "text": "x", "cell": "neither"})
    assert record["status"] == "id_mismatch"
    assert record["verdict"] is None


def test_realized_cost_is_computed_from_the_tokens_the_provider_reported():
    records = [
        {"status": "adjudicated", "call": {"input_tokens": 1_000_000, "output_tokens": 0}},
        {"status": "failed", "call": {"input_tokens": 500_000, "output_tokens": 0}},
    ]
    spend = mod.realized_cost(records, prices=mod.PRICES_USD_PER_MTOK)
    # A failed call still burns input tokens, so it still costs money.
    assert spend["input_tokens"] == 1_500_000
    assert spend["usd"] == pytest.approx(1.5 * mod.PRICES_USD_PER_MTOK["input"])


# --------------------------------------------------------------------------
# verdict rollup
# --------------------------------------------------------------------------


def test_verdict_counts_families_from_adjudicated_ground_truth_only():
    records = [
        {
            "status": "adjudicated",
            "cell": "citeurl_only",
            "verdict": "citeurl_correct",
            "response": {"citations": [{"family": "cfr", "text": "40 CFR 60"}]},
        },
        {
            "status": "adjudicated",
            "cell": "current_only",
            "verdict": "current_parser_correct",
            "response": {"citations": [{"family": "eo", "text": "E.O. 13563"}]},
        },
        {"status": "failed", "cell": "neither", "verdict": None, "response": None},
    ]
    verdict = mod.verdict_by_family(records)
    assert verdict["cfr"]["citeurl_correct"] == 1
    assert verdict["eo"]["current_parser_correct"] == 1
    assert verdict["_unadjudicated"] == 1


# --------------------------------------------------------------------------
# the text-grammar cut: the comparison a recommendation can rest on
# --------------------------------------------------------------------------


def test_text_grammar_families_exclude_the_column_scoped_readers():
    """Only grammars meant for free text belong in a free-text comparison.

    ``normalize_rin``, ``canonical_frdoc_iri`` and ``normalize_docket_reference``
    read a *column* whose every value is meant to be one identifier. Pointed at
    authority prose they over-fire — ``normalize_docket_reference`` returns
    early for anything the Regulations.gov syntax can spell, so a bare section
    number like ``"1255"`` comes back as a docket. Scoring them against CiteURL
    would charge the project for false positives it never makes in production.
    """
    assert mod.TEXT_GRAMMAR_FAMILIES == frozenset({"usc", "cfr", "pl", "eo", "stat"})
    assert mod.current_text_grammar_recognized("42 U.S.C. 7401") is True
    assert mod.current_text_grammar_recognized("40 CFR Part 60") is True
    assert mod.current_text_grammar_recognized("1255") is False
    assert "docket" in mod.current_families("1255")


def test_text_grammar_cut_reclassifies_records_without_re_running_detection():
    records = [
        {"string_id": "a", "current_families": ["cfr"], "citeurl_recognized": True},
        {"string_id": "b", "current_families": ["eo"], "citeurl_recognized": False},
        {"string_id": "c", "current_families": [], "citeurl_recognized": True},
        {"string_id": "d", "current_families": ["docket"], "citeurl_recognized": False},
    ]
    cells = mod.text_grammar_cells(records)
    assert cells == {"a": "both", "b": "current_only", "c": "citeurl_only", "d": "neither"}
    assert mod.four_cell_table([{"cell": cell} for cell in cells.values()]) == {
        "both": 1,
        "current_only": 1,
        "citeurl_only": 1,
        "neither": 1,
    }


def test_false_positive_counts_are_reported_for_both_arms():
    """A system that claims a citation the string does not contain is wrong.

    This is the safety half of the comparison. Detection coverage alone would
    reward an arm for firing on everything.
    """
    records = [
        {
            "status": "adjudicated",
            "verdict": "garbage",
            "response": {"contains_citations": False, "citations": []},
            "current_text_recognized": True,
            "citeurl_recognized": False,
        },
        {
            "status": "adjudicated",
            "verdict": "garbage",
            "response": {"contains_citations": False, "citations": []},
            "current_text_recognized": False,
            "citeurl_recognized": True,
        },
        {
            "status": "adjudicated",
            "verdict": "citeurl_correct",
            "response": {"contains_citations": True, "citations": [{"family": "cfr", "text": "40 CFR 60"}]},
            "current_text_recognized": False,
            "citeurl_recognized": True,
        },
        {"status": "failed", "verdict": None, "response": None},
    ]
    counts = mod.false_positive_counts(records)
    assert counts == {"current_text_grammars": 1, "citeurl": 1, "adjudicated": 3}

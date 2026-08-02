"""The re-derivation reports what the loaders produce, not what a script recalls.

The driver exists because a published count has to be reproducible from pinned
bytes. These fix its arithmetic on fixtures small enough to check by hand, so a
change to the composition rules moves a number here before it moves one in an
evidence document.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "measure_act_relative_resolution", REPO_ROOT / "tools" / "measure_act_relative_resolution.py"
)
mod = importlib.util.module_from_spec(_spec)
sys.modules["measure_act_relative_resolution"] = mod
_spec.loader.exec_module(mod)

from spicy_regs.ontology.act_index import ActIndex, SourceCreditIndex  # noqa: E402

INDEX = ActIndex(
    table3_key_by_name={"secure 2.0 act of 2022": "117-328", "clean air act": "1955:360"},
    classifications={
        # Ambiguous: two rows, both outside div. T.
        "117-328": {"303": (("38", "1720F nt", None, 5508), ("8", "1184 nt", "Elim.", 5227))},
        "1955:360": {"111": (("42", "7411", None, 322),)},
    },
    division_by_name={"secure 2.0 act of 2022": ("T", 5275)},
    division_starts={"117-328": (("T", 5275), ("U", 5404))},
)
CREDITS = SourceCreditIndex.from_rows([("117-328", "T", "303", "29", "1153", "136", "5339")])


def test_the_ambiguous_sweep_counts_what_the_second_source_decides():
    without = mod._measure_ambiguous(INDEX, None)
    with_credits = mod._measure_ambiguous(INDEX, CREDITS)

    assert without == {
        "ambiguous_pairs": 1,
        "combinations": 1,
        "resolved": 0,
        "resolved_by": {},
        "still_refusing": {"act_section_outside_act": 1},
    }
    assert with_credits["resolved_by"] == {"source_credits": 1}
    assert with_credits["still_refusing"] == {}


def test_the_corpus_measurement_reads_the_detection_artifact(tmp_path):
    detection = tmp_path / "detection.json"
    detection.write_text(
        json.dumps({"records": [{"text": "SECURE 2.0 Act of 2022, sec. 303 and Clean Air Act sec. 111"}]})
    )

    corpus = mod._measure_corpus(detection, INDEX, CREDITS)
    assert corpus["citations_found"] == 2
    assert corpus["resolved"] == 2
    assert corpus["resolved_by"] == {"source_credits": 1, "table3": 1}
    assert corpus["unresolved_reasons"] == {}


def test_the_source_comparison_reports_table_iiis_coverage_as_the_denominator():
    """Only the acts Table III was fetched for are comparable at all."""
    comparison = mod._measure_sources(INDEX, CREDITS)

    assert comparison["table3_public_laws_fetched"] == 2
    assert comparison["comparable_unambiguous_triples"] == 1
    # Table III's rows for this act section are both outside div. T, so within
    # the division it has nothing -- which is coverage, not disagreement.
    assert comparison["table3_has_no_in_division_row"] == 1
    assert comparison["agree"] == 0 and comparison["disagree"] == 0


def test_a_disagreement_is_reported_with_both_identifiers():
    index = ActIndex(
        table3_key_by_name={"fast act": "114-94"},
        classifications={"114-94": {"32101": (("22", "2714a", None, 1729),)}},
        division_by_name={"fast act": ("C", 1512)},
        division_starts={"114-94": (("C", 1512), ("D", 1780))},
    )
    credits = SourceCreditIndex.from_rows([("114-94", "C", "32101", "26", "7345", "129", "1729")])

    comparison = mod._measure_sources(index, credits)
    assert comparison["disagree"] == 1
    assert comparison["disagreements"] == [
        {
            "key": "114-94|C|32101",
            "table3": "urn:rkaf:us:usc:22:2714a",
            "source_credits": "urn:rkaf:us:usc:26:7345",
        }
    ]


def test_a_credit_target_the_usc_space_cannot_spell_is_counted_not_compared():
    """Counting it as a disagreement would blame the sources for the lexicon."""
    index = ActIndex(
        table3_key_by_name={"cures act": "114-255"},
        classifications={"114-255": {"3038": (("21", "360bbb-8", None, 1101),)}},
        division_by_name={"cures act": ("A", 1033)},
        division_starts={"114-255": (("A", 1033), ("B", 1200))},
    )
    credits = SourceCreditIndex.from_rows([("114-255", "A", "3038", "21", "360bbb-8a", "130", "1101")])

    comparison = mod._measure_sources(index, credits)
    assert comparison["credit_target_not_expressible"] == 1
    assert comparison["disagree"] == 0 and comparison["agree"] == 0

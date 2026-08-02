"""Two independent sources answer an act-relative citation, and neither wins ties.

Table III maps an act section to every U.S. Code place it was classified, keyed
by the **enacting public law** and nothing finer. The U.S. Code's own source
credits map a U.S. Code section back to the act section that added it, and they
state the **division**. They are complementary, not redundant: measured on
release point 119-102, of the 222 unambiguous triples the credit index carries
for public laws Table III was fetched for, 176 have no in-division Table III row
at all -- ``26 U.S.C. 6038E`` among them.

So the resolver consults both, states which one answered, and **refuses when
they answer differently**. Every fixture here is a *table*; every expected
identifier is what the composition rules produce from it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from spicy_regs.ontology.act_index import (
    SOURCE_COMPOSITION_RULE,
    SOURCE_CREDIT_STATUSES,
    UNRESOLVED_REASONS,
    ActIndex,
    SourceCreditIndex,
    resolve_act_relative_citation,
)
from spicy_regs.ontology.citations import ActRelativeCitation, normalize_popular_name

ARTIFACTS = Path(__file__).resolve().parents[1] / "output"
ACT_INDEX = ARTIFACTS / "usc-act-index-2026-08-02"
CREDIT_INDEX = ARTIFACTS / "usc-source-credit-index-2026-08-02"


# The real rows for the two acts these tests compose over, exactly as the sealed
# artifacts carry them.
INDEX = ActIndex(
    table3_key_by_name={
        "secure 2.0 act of 2022": "117-328",
        "taxpayer certainty and disaster tax relief act of 2020": "116-260",
        "clean air act": "1955:360",
    },
    classifications={
        "117-328": {
            "120": (("2", "1912", None, 4926), ("23", "104 nt", "Elim.", 5114)),
            "303": (("38", "1720F nt", None, 5508), ("8", "1184 nt", "Elim.", 5227)),
            # A section whose single Table III row DOES sit inside div. T.
            "331": (("29", "1021", None, 5361),),
        },
        "116-260": {"107": (("20", "80t-5", None, 2276), ("49", "60122", None, 2221))},
        "1955:360": {"111": (("42", "7411", None, 322),)},
    },
    division_by_name={
        "secure 2.0 act of 2022": ("T", 5275),
        "taxpayer certainty and disaster tax relief act of 2020": ("EE", 3038),
    },
    division_starts={
        "117-328": (("M", 5189), ("T", 5275), ("U", 5404)),
        "116-260": (("R", 2221), ("AA", 2615), ("EE", 3038), ("FF", 3088)),
    },
)

CREDITS = SourceCreditIndex.from_rows(
    [
        # The verified cases, from the credits themselves.
        ("116-260", "EE", "107", "26", "6038E", "134", "3048"),
        ("117-328", "T", "303", "29", "1153", "136", "5339"),
        # Agreement with Table III on the same section.
        ("117-328", "T", "331", "29", "1021", "136", "5361"),
        # One act section that added two sections: plural, not silent.
        ("117-328", "T", "401", "29", "1201", "136", "5400"),
        ("117-328", "T", "401", "29", "1202", "136", "5401"),
    ]
)


def _resolve(act: str, section: str, *, credits=CREDITS, index=INDEX):
    return resolve_act_relative_citation(
        ActRelativeCitation(act, normalize_popular_name(act), section), index=index, source_credits=credits
    )


def test_the_composition_rule_is_a_named_constant():
    assert SOURCE_COMPOSITION_RULE == "table3-and-source-credits-consulted-disagreement-refuses-v1"
    assert "sources_disagree" in UNRESOLVED_REASONS


def test_a_source_credit_answers_where_table_iii_has_no_classification():
    """The 6038E case. Table III lacks the classification entirely.

    Its three rows for (116-260, sec. 107) are 20 U.S.C. 80t-5, 33 U.S.C.
    701h-3 and 49 U.S.C. 60122, all in sibling divisions -- 26 U.S.C. 6038E is
    not among them. No amount of discriminating between Table III's rows could
    ever have found it, which is why this is a second source rather than a
    tiebreaker.
    """
    resolution = _resolve("Taxpayer Certainty and Disaster Tax Relief Act of 2020", "107")

    assert resolution.iri == "urn:rkaf:us:usc:26:6038e"
    assert resolution.answered_by == "source_credits"
    assert resolution.table3_reason == "act_section_outside_act"
    assert resolution.source_credit_status == "resolved"
    assert resolution.statutes_at_large_page == "3048"


def test_the_observed_false_refusal_flips_to_the_right_identifier():
    """(117-328, div. T, sec. 303) refused; authority says 29 U.S.C. 1153.

    This is the range rule's range-start-too-high failure, observed rather than
    inferred: SECURE 2.0 is div. T from 136 Stat. 5275 and Table III's two rows
    for act section 303 sit at 5227 and 5508, both outside. The credit sits at
    5339, inside -- so the two halves compose, and the refusal becomes an
    identifier without either source being overruled.
    """
    resolution = _resolve("SECURE 2.0 Act of 2022", "303")

    assert resolution.iri == "urn:rkaf:us:usc:29:1153"
    assert resolution.answered_by == "source_credits"
    assert resolution.table3_reason == "act_section_outside_act"


def test_a_citation_neither_source_answers_still_refuses():
    """(117-328, div. T, sec. 120) -> nothing, from either source."""
    resolution = _resolve("SECURE 2.0 Act of 2022", "120")

    assert resolution.iri is None
    assert resolution.unresolved_reason == "act_section_outside_act"
    assert resolution.answered_by is None
    assert resolution.source_credit_status == "absent"


def test_both_sources_agreeing_is_recorded_as_both():
    resolution = _resolve("SECURE 2.0 Act of 2022", "331")

    assert resolution.iri == "urn:rkaf:us:usc:29:1021"
    assert resolution.answered_by == "both"
    assert resolution.source_credit_status == "resolved"


def test_two_sources_answering_differently_refuse_rather_than_reconcile():
    """A disagreement is a finding, not a tiebreak.

    Measured cause: an act section that classified to more than one place, where
    each source retains a different one. (114-94, div. C, sec. 32101) is the
    real example -- Table III says 22 U.S.C. 2714a, the credits say 26 U.S.C.
    7345, and both are true. Naming one would be arbitrary.
    """
    index = ActIndex(
        table3_key_by_name={"fixing america's surface transportation act": "114-94"},
        classifications={"114-94": {"32101": (("22", "2714a", None, 1729),)}},
        division_by_name={"fixing america's surface transportation act": ("C", 1512)},
        division_starts={"114-94": (("C", 1512), ("D", 1780))},
    )
    credits = SourceCreditIndex.from_rows([("114-94", "C", "32101", "26", "7345", "129", "1729")])

    resolution = _resolve("Fixing America's Surface Transportation Act", "32101", credits=credits, index=index)
    assert resolution.iri is None, "two answers is not an answer"
    assert resolution.unresolved_reason == "sources_disagree"
    assert resolution.answered_by is None


def test_a_plural_source_credit_refuses_and_says_it_was_plural():
    """Kept and marked: "the source said two things" is not "the source was silent"."""
    resolution = _resolve("SECURE 2.0 Act of 2022", "401")

    assert resolution.iri is None
    assert resolution.source_credit_status == "multi_target"
    assert resolution.unresolved_reason == "act_section_not_classified", "Table III's reason survives unchanged"
    assert resolution.source_credit_status in SOURCE_CREDIT_STATUSES


def test_an_act_with_no_division_cannot_key_into_the_credit_index():
    """The Clean Air Act is a 1955 session-law chapter, not a divided public law."""
    resolution = _resolve("Clean Air Act", "111")

    assert resolution.iri == "urn:rkaf:us:usc:42:7411"
    assert resolution.answered_by == "table3"
    assert resolution.source_credit_status == "no_key"


def test_omitting_the_second_source_leaves_the_first_untouched():
    """Every existing answer must survive the wiring, unchanged and unlabelled."""
    with_none = resolve_act_relative_citation(ActRelativeCitation("Clean Air Act", "clean air act", "111"), index=INDEX)
    assert with_none.iri == "urn:rkaf:us:usc:42:7411"
    assert with_none.answered_by == "table3"
    assert with_none.source_credit_status == "not_consulted"


def test_a_credit_target_the_usc_space_cannot_spell_refuses():
    """``rkaf:us-usc`` admits one letter of suffix; the Code writes three.

    124 of the sealed index's 3,721 rows carry a section such as ``360bbb-3``.
    Publishing a mangled identifier for them would be worse than refusing, and
    widening the lexical space is an identity decision this does not make.
    """
    index = ActIndex(
        table3_key_by_name={"pandemic act": "116-1"},
        classifications={"116-1": {}},
        division_by_name={"pandemic act": ("A", 10)},
        division_starts={"116-1": (("A", 10), ("B", 900))},
    )
    credits = SourceCreditIndex.from_rows([("116-1", "A", "5", "21", "360bbb-3", "133", "20")])

    resolution = _resolve("Pandemic Act", "5", credits=credits, index=index)
    assert resolution.iri is None
    assert resolution.unresolved_reason == "usc_section_not_expressible"


def test_the_credit_index_loads_from_the_sealed_artifact():
    """The measurement must be reproducible from the pinned bytes, not a fixture."""
    if not (CREDIT_INDEX / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    credits = SourceCreditIndex.from_artifact(CREDIT_INDEX)

    added = credits.lookup("116-260", "EE", "107")
    assert (added.status, added.usc_title, added.usc_section) == ("resolved", "26", "6038E")
    assert (added.statutes_at_large_volume, added.statutes_at_large_page) == ("134", "3048")

    secure = credits.lookup("117-328", "T", "303")
    assert (secure.status, secure.usc_title, secure.usc_section) == ("resolved", "29", "1153")
    assert secure.statutes_at_large_page == "5339"

    assert credits.lookup("117-328", "T", "120").status == "absent"


def test_the_sealed_credit_index_never_credits_the_amended_section():
    """26 U.S.C. 7652 must not appear under (116-260, div. EE, sec. 107).

    Its credit names that act section at 134 Stat. 3046 as an amendment; the
    enactment of 6038E is at 3048. A loose rule reads the first as the second.
    """
    if not (CREDIT_INDEX / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    credits = SourceCreditIndex.from_artifact(CREDIT_INDEX)

    targets = {(t.usc_title, t.usc_section) for t in credits.targets_for("116-260", "EE", "107")}
    assert targets == {("26", "6038E")}
    assert ("26", "7652") not in targets


@pytest.mark.parametrize(
    ("act", "section", "iri"),
    [
        ("Taxpayer Certainty and Disaster Tax Relief Act of 2020", "107", "urn:rkaf:us:usc:26:6038e"),
        ("SECURE 2.0 Act of 2022", "303", "urn:rkaf:us:usc:29:1153"),
    ],
)
def test_the_two_sealed_artifacts_compose_end_to_end(act, section, iri):
    """Through the pinned bytes of both artifacts, not through a fixture."""
    for artifact in (ACT_INDEX, CREDIT_INDEX):
        if not (artifact / "receipt.json").exists():
            pytest.skip("artifacts not built in this checkout (output/ is gitignored)")

    resolution = resolve_act_relative_citation(
        ActRelativeCitation(act, normalize_popular_name(act), section),
        index=ActIndex.from_artifact(ACT_INDEX),
        source_credits=SourceCreditIndex.from_artifact(CREDIT_INDEX),
    )
    assert resolution.iri == iri
    assert resolution.answered_by == "source_credits"
    assert resolution.table3_reason == "act_section_outside_act"


def test_the_sealed_artifacts_still_refuse_the_case_neither_answers():
    for artifact in (ACT_INDEX, CREDIT_INDEX):
        if not (artifact / "receipt.json").exists():
            pytest.skip("artifacts not built in this checkout (output/ is gitignored)")

    resolution = resolve_act_relative_citation(
        ActRelativeCitation("SECURE 2.0 Act of 2022", "secure 2.0 act of 2022", "120"),
        index=ActIndex.from_artifact(ACT_INDEX),
        source_credits=SourceCreditIndex.from_artifact(CREDIT_INDEX),
    )
    assert resolution.iri is None
    assert resolution.unresolved_reason == "act_section_outside_act"

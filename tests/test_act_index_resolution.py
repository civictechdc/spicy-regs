"""Resolution is derived from the tables, never looked up from an answer sheet.

Every fixture below is a *table*; every expected identifier is what the
resolution rules produce from it. If the tables were wrong the tests would still
pass, which is the point: these test the derivation, and the artifact receipt
tests the tables.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from spicy_regs.ontology.act_index import (
    ALIAS_YEAR_RULE,
    UNRESOLVED_REASONS,
    ActIndex,
    ActResolution,
    resolve_act_name,
    resolve_act_relative_citation,
)
from spicy_regs.ontology.citations import find_act_relative_citations, normalize_popular_name


_Index = ActIndex


# The real rows, from uscode.house.gov, for the acts these tests resolve.
INDEX = _Index(
    table3_key_by_name={
        "clean air act": "1955:360",
        "employee retirement income security act of 1974": "93-406",
        "toxic substances control act": "94-469",
        "one big beautiful bill act": "119-21",
        "taxpayer certainty and disaster tax relief act of 2020": "116-260",
        "secure 2.0 act of 2022": "117-328",
    },
    alias_by_name={
        "erisa": "employee retirement income security act",
        "air pollution control act": "clean air act",
    },
    classifications={
        "1955:360": {
            "111": (("42", "7411", None, 322),),
            "112": (("42", "7412", None, 323),),
            "150-159": (("42", "7450-7459", "Rep.", 400),),
            "301": (("42", "7601 et seq.", None, 401),),
        },
        "93-406": {"803": (("29", "1193b", None, 940),)},
        "94-469": {"5": (("15", "2604", None, 2020),)},
        # The real rows for the two ambiguous pairs, from the sealed artifact.
        # Table III is keyed by the enacting Public Law, and 116-260 carries 94
        # popular names; nothing in the citation says which act is meant.
        # The real rows and their real Statutes at Large pages.
        "116-260": {
            "107": (
                ("20", "80t-5", None, 2276),
                ("33", "701h-3", None, 2623),
                ("49", "60122", None, 2221),
            ),
            # A section that DOES fall inside div. EE, so the range picks it.
            "301": (("26", "6001", None, 2221), ("26", "9801", None, 3100)),
        },
        "117-328": {"120": (("2", "1912", None, 4926), ("23", "104 nt", "Elim.", 5114))},
    },
    incomplete_sources=frozenset({"119-21"}),
    # Divisions and their first pages, exactly as the Popular Name Tool states.
    division_by_name={
        "taxpayer certainty and disaster tax relief act of 2020": ("EE", 3038),
        "secure 2.0 act of 2022": ("T", 5275),
    },
    division_starts={"116-260": (1182, 2221, 2615, 3038), "117-328": (4462, 4926, 5275)},
)


def _resolve(text):
    (citation,) = find_act_relative_citations(
        text, act_names=frozenset(INDEX.table3_key_by_name) | frozenset(INDEX.alias_by_name)
    )
    return resolve_act_relative_citation(citation, index=INDEX)


@pytest.mark.parametrize(
    ("raw", "iri"),
    [
        # The string this whole line of work was predicted on.
        ("Clean Air Act section 111", "urn:rkaf:us:usc:42:7411"),
        ("Clean Air Act sec. 112", "urn:rkaf:us:usc:42:7412"),
        ("Clean Air Act Section 112", "urn:rkaf:us:usc:42:7412"),
        # Subsection detail is excluded from the act section, as it is from a
        # U.S.C. section.
        ("Clean Air Act sec. 111(b)(1)(B)", "urn:rkaf:us:usc:42:7411"),
        # The alias case: "ERISA" is not an act, it points at one whose only
        # entry carries a year.
        ("ERISA sec. 803", "urn:rkaf:us:usc:29:1193b"),
        ("Toxic Substances Control Act sec. 5", "urn:rkaf:us:usc:15:2604"),
        # An older name for the same act reaches the same identifier.
        ("Air Pollution Control Act sec. 111", "urn:rkaf:us:usc:42:7411"),
    ],
)
def test_a_real_act_relative_string_resolves_to_a_usc_identifier(raw, iri):
    assert _resolve(raw).iri == iri


def test_the_alias_year_rule_is_what_reaches_erisa():
    """ "ERISA" -> "Employee Retirement Income Security Act" -> "… of 1974"."""
    assert ALIAS_YEAR_RULE == "supply-trailing-year-when-exactly-one-act-supplies-it"
    assert resolve_act_name("ERISA", INDEX) == "employee retirement income security act of 1974"


def test_a_year_target_several_acts_could_supply_is_refused():
    """ "Clean Air Act Amendments" is 1966, 1970 and 1977; picking one invents."""
    index = _Index(
        table3_key_by_name={
            "clean air act amendments of 1966": "89-675",
            "clean air act amendments of 1977": "95-95",
        },
        alias_by_name={"caaa": "clean air act amendments"},
    )
    assert resolve_act_name("CAAA", index) is None


def test_an_alias_cycle_terminates_without_an_answer():
    index = _Index(alias_by_name={"a act": "b act", "b act": "a act"})
    assert resolve_act_name("A Act", index) is None


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        # The measured refusals. The Popular Name Tool lists none of these
        # abbreviations, and inferring what they stand for is the guess the
        # identity fence exists to stop.
        ("INA sec. 103(a)(1)", "act_not_in_index"),
        ("PHS Act secs. 2791(b)(5)", "act_not_in_index"),
        ("ACA sec. 1557", "act_not_in_index"),
    ],
)
def test_an_act_the_index_does_not_name_stays_refused(raw, reason):
    found = find_act_relative_citations(
        raw, act_names=frozenset(INDEX.table3_key_by_name) | frozenset(INDEX.alias_by_name)
    )
    assert found == [], "the grammar itself must not recognize an unknown act"


@pytest.mark.parametrize(
    ("act", "section", "reason"),
    [
        # Table III has no row for it.
        ("Clean Air Act", "9999", "act_section_not_classified"),
        # Table III says the classification no longer stands.
        ("Clean Air Act", "150-159", "classification_not_current"),
        # Table III names a target rkaf:us-usc cannot spell.
        ("Clean Air Act", "301", "usc_section_not_expressible"),
        # The act resolved but its Table III page could not be read. This is the
        # One Big Beautiful Bill Act: uscode.house.gov times out rendering it.
        ("One Big Beautiful Bill Act", "70301", "source_incomplete"),
    ],
)
def test_a_citation_that_cannot_resolve_says_which_way_it_failed(act, section, reason):
    from spicy_regs.ontology.citations import ActRelativeCitation

    citation = ActRelativeCitation(act, normalize_popular_name(act), section)
    resolution = resolve_act_relative_citation(citation, index=INDEX)
    assert resolution.iri is None
    assert resolution.unresolved_reason == reason
    assert reason in UNRESOLVED_REASONS


def test_source_incomplete_is_not_reported_as_an_unclassified_section():
    """A hole in the evidence is not the same fact as a section going nowhere.

    Reporting the One Big Beautiful Bill Act's sections as "not classified"
    would publish an absence of knowledge as knowledge.
    """
    from spicy_regs.ontology.citations import ActRelativeCitation

    resolution = resolve_act_relative_citation(
        ActRelativeCitation("One Big Beautiful Bill Act", "one big beautiful bill act", "70434"),
        index=INDEX,
    )
    assert resolution.unresolved_reason == "source_incomplete"
    assert resolution.table3_key == "119-21"


def test_a_resolution_always_states_exactly_one_of_an_identifier_or_a_reason():
    from spicy_regs.ontology.citations import ActRelativeCitation

    citation = ActRelativeCitation("Clean Air Act", "clean air act", "111")
    with pytest.raises(ValueError):
        ActResolution(citation)
    with pytest.raises(ValueError):
        ActResolution(citation, iri="urn:rkaf:us:usc:42:7411", unresolved_reason="act_not_in_index")
    with pytest.raises(ValueError):
        ActResolution(citation, unresolved_reason="because-i-said-so")


@pytest.mark.parametrize(
    ("act", "section", "would_have_been", "pages", "division_start"),
    [
        # The wrong identity this replaces. `sec. 107 of the Taxpayer Certainty
        # and Disaster Tax Relief Act of 2020` resolved to
        # urn:rkaf:us:usc:49:60122 -- pipeline-safety civil penalties, for a tax
        # act. The act is div. EE of Pub. L. 116-260, beginning at 134 Stat.
        # 3038; all three rows for act section 107 sit at 2221, 2276 and 2623,
        # so every one of them belongs to a sibling act in another division.
        # Corroborated by the Code's own source credits: 49 U.S.C. 60122 reads
        # "Pub. L. 116-260, div. R, ... 134 Stat. 2221".
        (
            "Taxpayer Certainty and Disaster Tax Relief Act of 2020",
            "107",
            "urn:rkaf:us:usc:49:60122",
            (2221, 2276, 2623),
            3038,
        ),
        # And the one whose refusal only *looked* principled: under first-wins
        # this minted urn:rkaf:us:usc:2:1912 (Legislative Branch approps); under
        # last-wins it answered `classification_not_current`, a refusal for the
        # wrong reason. SECURE 2.0 is div. T of 117-328 beginning at 136 Stat.
        # 5275; its two candidate rows sit at 4926 and 5114.
        ("SECURE 2.0 Act of 2022", "120", "urn:rkaf:us:usc:2:1912", (4926, 5114), 5275),
    ],
)
def test_a_section_outside_the_citing_acts_division_is_not_its_section(
    act, section, would_have_been, pages, division_start
):
    """Table III is keyed by the enacting public law, not by the act.

    116-260 enacts 94 popular names and 117-328 enacts 70, so one
    (table3_key, act_section) names classifications belonging to different acts.
    The citing act's division bounds a page range, and every candidate row here
    falls outside it -- so the section is a sibling act's, and the honest answer
    is that this act does not have one.
    """
    from spicy_regs.ontology.citations import ActRelativeCitation

    resolution = resolve_act_relative_citation(
        ActRelativeCitation(act, normalize_popular_name(act), section), index=INDEX
    )
    assert resolution.iri is None, "a wrong identifier is worse than no identifier"
    assert resolution.unresolved_reason == "act_section_outside_act"
    assert all(page < division_start for page in pages), "the fixture must encode the real pages"


def test_the_division_range_picks_the_row_that_is_inside_it():
    """When one candidate falls in the act's division, that one is the answer.

    This is the resolving half: refusing everything would be safe and useless.
    """
    from spicy_regs.ontology.citations import ActRelativeCitation

    resolution = resolve_act_relative_citation(
        ActRelativeCitation(
            "Taxpayer Certainty and Disaster Tax Relief Act of 2020",
            "taxpayer certainty and disaster tax relief act of 2020",
            "301",
        ),
        index=INDEX,
    )
    assert resolution.iri == "urn:rkaf:us:usc:26:9801", "the row at 3100 is inside div. EE"


def test_an_act_that_states_no_division_narrows_nothing():
    """ "Consolidated Appropriations Act, 2021" is all of 116-260, not a division.

    It bounds no range, so an ambiguous section under it stays ambiguous rather
    than being silently narrowed by a range it never stated.
    """
    from spicy_regs.ontology.citations import ActRelativeCitation

    index = _Index(
        table3_key_by_name={"consolidated appropriations act, 2021": "116-260"},
        classifications=INDEX.classifications,
        division_starts=INDEX.division_starts,
    )
    assert index.act_page_range("consolidated appropriations act, 2021") is None
    resolution = resolve_act_relative_citation(
        ActRelativeCitation("Consolidated Appropriations Act, 2021", "consolidated appropriations act, 2021", "107"),
        index=index,
    )
    assert resolution.unresolved_reason == "act_section_ambiguous"


def test_the_ambiguity_refusal_does_not_swallow_a_single_row_answer():
    """One row is still an answer; only a genuine tie refuses."""
    assert _resolve("Clean Air Act section 111").iri == "urn:rkaf:us:usc:42:7411"


def test_the_index_loads_from_the_sealed_artifact():
    """The measurement must be reproducible from the pinned bytes, not a fixture."""
    artifact = Path(__file__).resolve().parents[1] / "output" / "usc-act-index-2026-08-02"
    if not (artifact / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    index = ActIndex.from_artifact(artifact)

    assert index.table3_key_by_name["clean air act"] == "1955:360"
    assert index.alias_by_name["erisa"] == "employee retirement income security act"
    assert index.incomplete_sources == frozenset({"119-21"})
    # Every row survives the load: a single-valued map lost 1,060 of 10,976.
    assert sum(len(v) for s in index.classifications.values() for v in s.values()) == 10976
    assert index.classifications["1955:360"]["111"] == (("42", "7411", None, None),)
    assert len(index.classifications["116-260"]["107"]) == 3
    # Both halves of the discriminator survive the round trip through parquet.
    assert index.division_by_name["taxpayer certainty and disaster tax relief act of 2020"] == ("EE", 3038)
    assert index.division_by_name["secure 2.0 act of 2022"] == ("T", 5275)
    assert index.act_page_range("taxpayer certainty and disaster tax relief act of 2020")[0] == 3038
    assert {row[3] for row in index.classifications["116-260"]["107"]} == {2221, 2276, 2623}


def test_the_sealed_artifact_still_refuses_both_named_wrong_identities():
    """End to end, through the pinned bytes rather than a fixture."""
    from spicy_regs.ontology.citations import ActRelativeCitation

    artifact = Path(__file__).resolve().parents[1] / "output" / "usc-act-index-2026-08-02"
    if not (artifact / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    index = ActIndex.from_artifact(artifact)

    for act, section in (
        ("Taxpayer Certainty and Disaster Tax Relief Act of 2020", "107"),
        ("SECURE 2.0 Act of 2022", "120"),
    ):
        resolution = resolve_act_relative_citation(
            ActRelativeCitation(act, normalize_popular_name(act), section), index=index
        )
        assert resolution.iri is None, f"{act} sec. {section} must not mint an identifier"
        assert resolution.unresolved_reason == "act_section_outside_act"

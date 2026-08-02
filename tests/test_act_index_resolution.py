"""Resolution is derived from the tables, never looked up from an answer sheet.

Every fixture below is a *table*; every expected identifier is what the
resolution rules produce from it. If the tables were wrong the tests would still
pass, which is the point: these test the derivation, and the artifact receipt
tests the tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from spicy_regs.ontology.act_index import (
    ALIAS_YEAR_RULE,
    UNRESOLVED_REASONS,
    ActResolution,
    resolve_act_name,
    resolve_act_relative_citation,
)
from spicy_regs.ontology.citations import find_act_relative_citations, normalize_popular_name


@dataclass
class _Index:
    table3_key_by_name: dict = field(default_factory=dict)
    alias_by_name: dict = field(default_factory=dict)
    classifications: dict = field(default_factory=dict)
    incomplete_sources: frozenset = frozenset()


# The real rows, from uscode.house.gov, for the acts these tests resolve.
INDEX = _Index(
    table3_key_by_name={
        "clean air act": "1955:360",
        "employee retirement income security act of 1974": "93-406",
        "toxic substances control act": "94-469",
        "one big beautiful bill act": "119-21",
    },
    alias_by_name={
        "erisa": "employee retirement income security act",
        "air pollution control act": "clean air act",
    },
    classifications={
        "1955:360": {
            "111": ("42", "7411", None),
            "112": ("42", "7412", None),
            "150-159": ("42", "7450-7459", "Rep."),
            "301": ("42", "7601 et seq.", None),
        },
        "93-406": {"803": ("29", "1193b", None)},
        "94-469": {"5": ("15", "2604", None)},
    },
    incomplete_sources=frozenset({"119-21"}),
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

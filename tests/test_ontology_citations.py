"""Fixture coverage for CFR/U.S.C./Public-Law citation grammars."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest
import yaml

from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.ontology.citations import (
    USC_CHAPTER_IDENTIFIER_SCHEME,
    AuthorityCitation,
    CfrCitation,
    ExecutiveOrderCompilation,
    UscChapterCitation,
    canonical_cfr_iri,
    canonical_frdoc_iri,
    canonical_pl_iri,
    canonical_regsgov_iri,
    canonical_rin_iri,
    canonical_usc_chapter_iri,
    canonical_usc_iri,
    docket_reference_as_stated,
    federal_register_identifier,
    find_act_relative_citations,
    normalize_docket_id,
    normalize_popular_name,
    normalize_docket_reference,
    normalize_regsgov_identifier,
    normalize_rin,
    parse_authority_citation,
    parse_cfr_citation,
    parse_eo_compilation_citation,
    parse_usc_chapter_citation,
    usc_section_covers,
)
from spicy_regs.ontology.llm import validated_external_ids

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"title": 40, "part": 60}, [CfrCitation("40", "60")]),
        ({"title": "40", "part": "60", "section": "1"}, [CfrCitation("40", "60", "1")]),
        ("40-60.1", [CfrCitation("40", "60", "1")]),
        ("40-60.5375a", [CfrCitation("40", "60", "5375a")]),
        ("40 C.F.R. § 60.5375a(a)(1)", [CfrCitation("40", "60", "5375a")]),
        ("40 CFR 60", [CfrCitation("40", "60")]),
        ("40 C.F.R. § 60.1", [CfrCitation("40", "60", "1")]),
        ("Title 40, Part 60", [CfrCitation("40", "60")]),
        ("40 CFR Parts 60 and 63", [CfrCitation("40", "60"), CfrCitation("40", "63")]),
        ("not a CFR citation", []),
    ],
)
def test_parse_cfr_fixture_forms(raw, expected):
    assert parse_cfr_citation(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # The measured false positive. docs/evidence/citation-bakeoff-2026-08-02.md,
        # "False positives — the safety half": one string in 620 drew a citation
        # from a project grammar that has none, and this is it. The compact
        # branch matched greedily and published title 5401, part 5405.
        "5401-5405",
        # The same branch on a Federal Register document number, recorded as a
        # known overlap in tests/test_run_citation_bakeoff.py.
        "2026-13078",
        # One past the last title the CFR has.
        "51-1",
    ],
)
def test_the_compact_key_refuses_a_title_the_cfr_does_not_have(raw):
    """Regression: ``'5401-5405'`` published ``urn:rkaf:us:cfr:5401:5405``.

    The bound is the CFR's own and every branch now carries it. It was applied
    here first, on the reasoning that the anchored branches were held in place
    by the literal "CFR" or the word "part". That reasoning was wrong — see
    ``test_an_anchored_branch_is_bounded_by_the_same_titles``, where an anchored
    branch reached ``urn:rkaf:us:cfr:2300:2336`` on a publishing path. An
    implausible title is implausible whatever anchored it.
    """
    assert parse_cfr_citation(raw) == []


# The five Title 3 compilation strings, verbatim from
# output/citation-bakeoff-2026-08-02 (detection.json, cell citeurl_only). They
# are the identity half of the bakeoff's 14 in-scope wins.
_EO_COMPILATION_STRINGS = (
    ("3 CFR, 1949-1953 Comp, p. 1002", ExecutiveOrderCompilation("1949", "1953", "1002")),
    ("3 CFR, 1959-1963 Comp.", ExecutiveOrderCompilation("1959", "1963", None)),
    ("3 CFR, 1977 Comp., p. 123", ExecutiveOrderCompilation("1977", None, "123")),
    ("3 CFR, 1977 Comp., p. 123.", ExecutiveOrderCompilation("1977", None, "123")),
    ("3 CFR, 1980 Comp., p. 298", ExecutiveOrderCompilation("1980", None, "298")),
)


@pytest.mark.parametrize(("raw", "expected"), _EO_COMPILATION_STRINGS)
def test_a_title_3_compilation_locator_is_recognized_as_what_it_is(raw, expected):
    """The form locates an Executive Order by compilation page, not a CFR section."""
    assert parse_eo_compilation_citation(raw) == [expected]


@pytest.mark.parametrize(("raw", "_expected"), _EO_COMPILATION_STRINGS)
def test_a_compilation_locator_never_becomes_a_cfr_citation(raw, _expected):
    """Regression: there is no 3 CFR § 1977, and reading one is the wrong parse.

    This is the reading the bakeoff found in CiteURL (`title=3, section=1977`,
    with the page — the part that identifies the order — discarded). These five
    spellings carry a comma the project's CFR expressions could not cross, so
    they were undetected rather than misread; the sibling test below covers the
    spellings that *were* misread.
    """
    assert parse_cfr_citation(raw) == []


@pytest.mark.parametrize(
    ("raw", "phantom"),
    [
        # Every string is verbatim from the sealed corpus, beside the CFR
        # citation `_CFR_STANDARD` drew from it before the refusal existed. The
        # comma is what had been holding the wrong reading off: "3 CFR, 1977"
        # does not match, "3 CFR 1977" does, and the corpus writes it both ways.
        ("3 C.F.R. 1978 Comp. p. 142", CfrCitation("3", "1978")),
        ("3 CFR 1949-1953 Comp., p. 970, as amended by E.O. 12038", CfrCitation("3", "1949")),
        ("3 CFR 1977 Comp., p. 158)", CfrCitation("3", "1977")),
        ("3 CFR 1978 Comp. p. 142", CfrCitation("3", "1978")),
        ("3 CFR 1978 Comp., p. 142.", CfrCitation("3", "1978")),
        ("3 CFR 1979 Comp. p. 435", CfrCitation("3", "1979")),
        ("E.O. 12600 (3 CFR 1987 Comp. p. 235)", CfrCitation("3", "1987")),
        # Found by adversarial review of the first fix, in exactly the class it
        # closed. A comma may also fall *before* "Comp", and the volume range
        # may be spelled "to" rather than dashed -- neither of which the first
        # expression could cross, so both still minted a phantom part.
        ("3 CFR 1949 to 1953, Comp, p. 1002", CfrCitation("3", "1949")),
        ("3 CFR 1979, Comp. p. 435", CfrCitation("3", "1979")),
    ],
)
def test_the_compilation_year_was_being_published_as_a_cfr_part(raw, phantom):
    """Regression: the year in a compilation locator read as a part number.

    `urn:rkaf:us:cfr:3:1978` is not a part of the CFR; 1978 is the compilation
    volume.

    An earlier version of this docstring said the class was reachable only from
    free text "because both production callers read `cfr_references_json`, which
    carries structured objects". False for the Unified Agenda: `rkaf_projection`
    feeds this function 3,998 distinct bare free-text values from
    `unified_agenda.cfr_references_json`, on a publishing path — see
    `test_an_anchored_branch_is_bounded_by_the_same_titles`. These particular
    strings are Federal Register authority prose and are not in that column, so
    they stay latent; the general claim was wrong.
    """
    assert phantom not in parse_cfr_citation(raw)
    assert parse_cfr_citation(raw) == []
    assert parse_eo_compilation_citation(raw)


def test_one_string_may_locate_two_compilations():
    raw = (
        "E.O. 12372 (July 14, 1982), 47 FR 30959, 3 CFR, 1982 Comp., p. 197, "
        "as amended by E.O. 12416 (April 8, 1983), 48 FR 15887, 3 CFR, 1983 Comp., p. 186."
    )
    assert parse_eo_compilation_citation(raw) == [
        ExecutiveOrderCompilation("1982", None, "197"),
        ExecutiveOrderCompilation("1983", None, "186"),
    ]


def test_a_compilation_locator_carries_no_identifier():
    """A typed recognition, deliberately not an identifier.

    The repo has no index from a Title 3 compilation page to an Executive Order
    number, so the honest output is the located page — never an EO number the
    parser would have to invent, and never a CFR section that does not exist.
    """
    (located,) = parse_eo_compilation_citation("3 CFR, 1977 Comp., p. 123")
    assert not hasattr(located, "iri")
    assert not hasattr(located, "canonical_iri")
    assert (located.compilation_start, located.compilation_end, located.page) == ("1977", None, "123")


def test_a_compilation_volume_without_a_page_locates_no_single_order():
    """A multi-year volume with no page names a volume, not an order.

    "3 CFR, 1959-1963 Comp." locates five years of presidential documents. The
    page is what would narrow it to one, and there is none.
    """
    (located,) = parse_eo_compilation_citation("3 CFR, 1959-1963 Comp.")
    assert located.page is None


def test_only_title_3_compiles_presidential_documents():
    """The annual compilation that carries Executive Orders is Title 3's.

    Another title's "Comp." would be a form this parser has never seen and has
    no meaning to give, so it is refused rather than recognized emptily.
    """
    assert parse_eo_compilation_citation("40 CFR, 1977 Comp., p. 123") == []
    assert parse_eo_compilation_citation("3 CFR 1977") == []
    assert parse_eo_compilation_citation("40 CFR 60") == []


def test_a_real_cfr_citation_beside_a_compilation_locator_survives():
    """The refusal covers the locator's own span and nothing else."""
    raw = "E.O. 11246, 3 CFR, 1964-1965 Comp., p. 339, implemented at 41 CFR 60"
    assert parse_cfr_citation(raw) == [CfrCitation("41", "60")]
    assert parse_eo_compilation_citation(raw) == [ExecutiveOrderCompilation("1964", "1965", "339")]


# Every string below is verbatim from output/citation-bakeoff-2026-08-02
# (detection.json, cell `neither`) — the largest well-defined slice of the
# shared-miss cell no evaluated package addresses. 31 of the 32 strings that
# cell spells with "chapter" name a title; the 32nd is a bare "Chapter 33".
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("5 U.S.C. Ch. 131", UscChapterCitation("5", "131")),
        ("5 U.S.C. Ch. 63", UscChapterCitation("5", "63")),
        ("49 U.S.C. ch. 311", UscChapterCitation("49", "311")),
        ("49 U.S.C. ch. 301", UscChapterCitation("49", "301")),
        # Every separator spelling the corpus uses: no space after the
        # abbreviation, no trailing dot, no dot at all, the word written out.
        ("41 U.S.C. ch.13", UscChapterCitation("41", "13")),
        ("31 U.S.C. Ch 38", UscChapterCitation("31", "38")),
        ("40 USC ch 10", UscChapterCitation("40", "10")),
        ("5 U.S.C. chapter 43", UscChapterCitation("5", "43")),
        ("54 U.S.C Chapter 3041", UscChapterCitation("54", "3041")),
        # A lettered chapter, normalized like a U.S.C. section suffix.
        ("42 U.S.C. chapter 13A", UscChapterCitation("42", "13a")),
        # Trailing prose is not part of the chapter number.
        ("22 USC Ch. 34- The Peace Corps Act", UscChapterCitation("22", "34")),
        ("10 U.S.C. ch. 137 legacy provisions", UscChapterCitation("10", "137")),
    ],
)
def test_a_usc_chapter_citation_is_read_from_the_corpus_spelling(raw, expected):
    assert parse_usc_chapter_citation(raw) == [expected]


def test_a_chapter_list_is_expanded_under_the_title_that_introduces_it():
    """ "47 U.S.C. chs. 2, 5, 9, 13" names four chapters, not one.

    Same expression and same stop rule the section list expansion uses, so a
    number that leads a different citation is not swept in as a chapter.
    """
    assert parse_usc_chapter_citation("47 U.S.C. chs. 2, 5, 9, 13") == [
        UscChapterCitation("47", "2"),
        UscChapterCitation("47", "5"),
        UscChapterCitation("47", "9"),
        UscChapterCitation("47", "13"),
    ]
    assert parse_usc_chapter_citation("46 U.S.C. ch. 553, 49 CFR 1.93(a)") == [UscChapterCitation("46", "553")]
    assert parse_usc_chapter_citation("5 U.S.C. ch. 10 and E.O. 12024 (42 FR 61445") == [UscChapterCitation("5", "10")]


@pytest.mark.parametrize("raw", ["46 U.S.C. chs. 301 to 309", "46 U.S.C. chs. 301-309"])
def test_a_chapter_range_publishes_its_endpoints_and_not_its_members(raw):
    """The rule sections already follow: two endpoints, never the interval.

    A chapter number never contains a hyphen, so unlike a section the plain
    hyphen is unambiguously a separator here. Whether the pair is a range is
    still decided by the ordering rule.
    """
    (parsed,) = parse_usc_chapter_citation(raw)
    assert (parsed.chapter, parsed.chapter_end) == ("301", "309")
    assert parsed.iri == "urn:spicy-regs:usc-chapter:46:301"


def test_a_dash_before_a_title_is_not_a_chapter_range():
    """ "22 USC Ch. 34- The Peace Corps Act" cites chapter 34 and stops there."""
    assert parse_usc_chapter_citation("22 USC Ch. 34- The Peace Corps Act") == [UscChapterCitation("22", "34")]


def test_an_unordered_chapter_pair_is_not_read_as_a_range():
    (parsed,) = parse_usc_chapter_citation("46 U.S.C. chs. 309 to 301")
    assert (parsed.chapter, parsed.chapter_end) == ("309", None)


def test_a_chapter_needs_the_code_that_gives_it_a_number():
    """ "Chapter 33" of what? Without a title there is nothing to identify.

    The bare form is the one string in the measured chapter population that
    names no code, and it stays unread rather than being attached to whatever
    title happened to appear nearby.
    """
    assert parse_usc_chapter_citation("Chapter 33") == []
    assert parse_usc_chapter_citation("chapter 13A") == []
    assert parse_usc_chapter_citation("40 CFR chapter I") == []


@pytest.mark.parametrize(("title", "number"), [("5", "10"), ("10", "55"), ("46", "701"), ("49", "301")])
def test_a_chapter_is_not_the_section_that_shares_its_number(title, number):
    """The reason the chapter identifier cannot live in the `us-usc` URN space.

    These four pairs are not hypothetical. Each is attested in
    output/citation-bakeoff-2026-08-02: the same corpus cites title 49 chapter
    301 *and* title 49 section 301, title 10 chapter 55 *and* section 55, and so
    on for all four. They are different provisions. Minting
    `urn:rkaf:us:usc:49:301` for the chapter would publish one identifier for
    two objects — the worst available outcome, because it is indistinguishable
    from a correct citation downstream.
    """
    (chapter,) = parse_usc_chapter_citation(f"{title} U.S.C. ch. {number}")
    (section,) = parse_authority_citation(f"{title} U.S.C. {number}")
    assert section.canonical_iri == f"urn:rkaf:us:usc:{title}:{number}"
    assert chapter.iri == f"urn:spicy-regs:usc-chapter:{title}:{number}"
    assert chapter.iri != section.canonical_iri


def test_a_chapter_citation_is_not_read_as_a_section():
    """The section grammar must stay silent on a chapter, in both directions."""
    for raw in ("5 U.S.C. Ch. 131", "49 U.S.C. ch. 311", "42 U.S.C. chapter 13A"):
        assert parse_authority_citation(raw) == [AuthorityCitation("other", "failed")]
    assert parse_usc_chapter_citation("42 U.S.C. 7401") == []


def test_the_chapter_identifier_declares_the_scheme_that_can_express_it():
    """`rkaf:us-usc` is a section space, so a chapter uses the partner escape.

    The compiled Rulespec profile constrains `rkaf:us-usc` to
    `^urn:rkaf:us:usc:[1-9][0-9]*:[1-9][0-9]*[a-z]*(-[0-9a-z]+)*$` — a title and
    a *section*. A chapter is not expressible there, so it takes the same route
    `federal_register_identifier` takes for legacy FR numbers: Rulespec's
    `partner-defined` escape hatch, under this repo's own URN prefix, until the
    scheme is broadened.
    """
    assert USC_CHAPTER_IDENTIFIER_SCHEME == "rkaf:partner-defined"
    assert canonical_usc_chapter_iri("49", "311") == "urn:spicy-regs:usc-chapter:49:311"
    assert canonical_usc_chapter_iri(42, "13A") == "urn:spicy-regs:usc-chapter:42:13a"


@pytest.mark.parametrize("chapter", ["", None, "I", "13-A", "0", "chapter"])
def test_an_unexpressible_chapter_is_refused_rather_than_spelled(chapter):
    with pytest.raises(ValueError):
        canonical_usc_chapter_iri("5", chapter)


@pytest.mark.parametrize(
    ("raw", "phantom"),
    [
        # LIVE, on a publishing path. `rkaf_projection` feeds
        # `parse_cfr_citation` free text from `unified_agenda.cfr_references_json`
        # -- 3,998 distinct bare values -- and this one published a title the
        # CFR does not have, in 14 generations. `_CFR_TITLE_PART` reads
        # "<N>, Part <M>" and was never bounded, so "of 2 CFR" at the end of the
        # sentence did not save it: the expression matched "2300, Part 2336".
        ("Part 2300, Part 2336, and Part 2339 of 2 CFR", "urn:rkaf:us:cfr:2300:2336"),
        ("Title 2300, Part 2336", "urn:rkaf:us:cfr:2300:2336"),
    ],
)
def test_an_anchored_branch_is_bounded_by_the_same_titles(raw, phantom):
    """The anchor was never the guarantee; the title range is.

    The compact-key fix assumed a branch anchored on "CFR" or "part" could only
    read a title the source text called one. It can read a *part* number as one
    instead, which is how a real Unified Agenda value minted title 2300.
    """
    assert phantom not in [citation.iri for citation in parse_cfr_citation(raw)]


def test_a_real_title_beside_an_out_of_range_number_still_parses():
    """Bounding refuses the phantom without refusing the citation beside it."""
    assert parse_cfr_citation("2 CFR Part 2336") == [CfrCitation("2", "2336")]
    assert parse_cfr_citation("Title 40, Part 60") == [CfrCitation("40", "60")]


@pytest.mark.parametrize(
    "raw",
    [
        # The separator set, closed rather than enumerated. "through" is the
        # ordinary legal spelling of the range "to" was added for.
        "3 CFR 1949 through 1953 Comp.",
        "3 CFR 1949 thru 1953 Comp.",
        "3 CFR 1949 and 1953 Comp.",
        "3 CFR 1949/1953 Comp.",
        "3 CFR 1949 to 1953, Comp, p. 1002",
        "3 CFR, 1949-1953 Comp, p. 1002",
    ],
)
def test_every_spelling_of_a_compilation_volume_range_is_refused(raw):
    """Whatever joins two years in a compilation citation, it is not a part."""
    assert parse_cfr_citation(raw) == []
    assert parse_eo_compilation_citation(raw)


def test_the_compact_key_still_reads_every_title_the_cfr_has():
    """The bound is the CFR's own, so no real compact key is refused by it."""
    assert parse_cfr_citation("1-1") == [CfrCitation("1", "1")]
    assert parse_cfr_citation("50-17") == [CfrCitation("50", "17")]
    assert parse_cfr_citation("50-17.11") == [CfrCitation("50", "17", "11")]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "42 U.S.C. 7401 et seq.",
            AuthorityCitation("usc", "ok", usc_title="42", usc_section="7401"),
        ),
        (
            "sec. 553 of title 5",
            AuthorityCitation("usc", "ok", usc_title="5", usc_section="553"),
        ),
        (
            "Pub. L. No. 117-58",
            AuthorityCitation("public_law", "ok", pl_number="117-58"),
        ),
        (
            "117 Stat. 429",
            AuthorityCitation("statute_at_large", "ok", statute_at_large="117-429"),
        ),
        (
            "Executive Order 14094",
            AuthorityCitation("eo", "ok", executive_order="14094"),
        ),
        (
            "Authority delegated under 42 USC 7411",
            AuthorityCitation("usc", "partial", usc_title="42", usc_section="7411"),
        ),
        (
            "Clean Air Act",
            AuthorityCitation("other", "failed"),
        ),
    ],
)
def test_parse_authority_fixture_forms(raw, expected):
    assert parse_authority_citation(raw) == [expected]


# Every string below is verbatim from output/citation-bakeoff-2026-08-02
# (detection.json, cell citeurl_only) — the regex half of the 14 in-scope wins
# in docs/evidence/citation-bakeoff-2026-08-02.md, corrected Recommendation 1.
# They are spellings of forms the project already parses, so each one must reach
# the identity the standard spelling already mints and no other.
@pytest.mark.parametrize(
    ("raw", "iris"),
    [
        # "U.S. Code" written out rather than abbreviated.
        ("49 U.S. Code 106", ["urn:rkaf:us:usc:49:106"]),
        ("49 U.S. Code 44715", ["urn:rkaf:us:usc:49:44715"]),
        # U.S.C. *Annotated* — a publisher's edition of the same code.
        ("50 U.S.C.A. 4701(a)", ["urn:rkaf:us:usc:50:4701"]),
        # The Internal Revenue Code is title 26, so naming it states a title.
        ("I.R.C. 337(d)", ["urn:rkaf:us:usc:26:337"]),
        ("IRC 382(m)", ["urn:rkaf:us:usc:26:382"]),
        # The literal spelling "Pub. Law". The en dash was never the gap —
        # _PUBLIC_LAW already accepts [-–—], and 119-21 uses a plain hyphen.
        ("Pub. Law 111–296", ["urn:rkaf:us:pl:111-296"]),
        (
            "Pub. Law 119-21, sec. 70301 and 70434(g), One Big Beautiful Bill Act",
            ["urn:rkaf:us:pl:119-21"],
        ),
    ],
)
def test_a_bakeoff_spelling_variant_mints_the_standard_identity(raw, iris):
    assert [citation.canonical_iri for citation in parse_authority_citation(raw)] == iris


@pytest.mark.parametrize(
    ("variant", "standard"),
    [
        ("49 U.S. Code 106", "49 U.S.C. 106"),
        ("49 U.S. Code 44715", "49 U.S.C. 44715"),
        ("50 U.S.C.A. 4701(a)", "50 U.S.C. 4701(a)"),
        ("I.R.C. 337(d)", "26 U.S.C. 337(d)"),
        ("IRC 382(m)", "26 U.S.C. 382(m)"),
        ("Pub. Law 111–296", "Pub. L. No. 111–296"),
    ],
)
def test_a_spelling_variant_parses_to_exactly_what_the_standard_spelling_does(variant, standard):
    """Not merely the same identity: the same citation, status and all.

    A variant that parsed to a *different* status would be a second reading of
    one citation, and the two spellings would stop being interchangeable.
    """
    assert parse_authority_citation(variant) == parse_authority_citation(standard)


def test_the_named_code_grammar_does_not_fire_without_a_section():
    """Naming a code is not citing one; the section is what identifies.

    The abbreviation is short enough to appear by accident, so it publishes
    nothing unless a section follows it.
    """
    for raw in ("I.R.C.", "the IRC", "IRCA 1986", "circ 12"):
        assert parse_authority_citation(raw) == [AuthorityCitation("other", "failed")]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "42 U.S.C. 1395, 1396, 1397",
            [("42", "1395"), ("42", "1396"), ("42", "1397")],
        ),
        ("42 U.S.C. 7401 and 7412", [("42", "7401"), ("42", "7412")]),
        ("42 U.S.C. 7401, 7412, and 7413 et seq.", [("42", "7401"), ("42", "7412"), ("42", "7413")]),
        # A later title governs its own list rather than the first one.
        (
            "5 U.S.C. 553 and 42 U.S.C. 7401, 7412",
            [("5", "553"), ("42", "7401"), ("42", "7412")],
        ),
        # Section suffixes survive the expansion.
        ("42 U.S.C. 1395w-4, 1395x", [("42", "1395w-4"), ("42", "1395x")]),
    ],
)
def test_usc_section_lists_yield_one_citation_per_section(raw, expected):
    parsed = parse_authority_citation(raw)
    assert [(item.usc_title, item.usc_section) for item in parsed] == expected
    assert {item.authority_type for item in parsed} == {"usc"}
    # No member of a list covers the whole string, so none of them is ``ok``.
    assert {item.parse_status for item in parsed} == {"partial"}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # A range stays one citation and publishes its two endpoints.
        (
            "42 U.S.C. 1395-1397",
            [AuthorityCitation("usc", "ok", usc_title="42", usc_section="1395", usc_section_end="1397")],
        ),
        # A number leading a different citation form is not a section.
        (
            "42 U.S.C. 7401, 117 Stat. 429",
            [
                AuthorityCitation("usc", "partial", usc_title="42", usc_section="7401"),
                AuthorityCitation("statute_at_large", "partial", statute_at_large="117-429"),
            ],
        ),
        (
            "42 U.S.C. 7401 and 40 CFR 60",
            [AuthorityCitation("usc", "partial", usc_title="42", usc_section="7401")],
        ),
        # Malformed lists keep whatever parsed and never invent a section.
        (
            "42 U.S.C. 1395, , and",
            [AuthorityCitation("usc", "partial", usc_title="42", usc_section="1395")],
        ),
        ("42 U.S.C., 1396", [AuthorityCitation("other", "failed")]),
        ("42 U.S.C.", [AuthorityCitation("other", "failed")]),
    ],
)
def test_usc_list_expansion_stays_fail_closed(raw, expected):
    assert parse_authority_citation(raw) == expected


# The five RINs that "every active rulemaking depending on 42 U.S.C. 7401"
# missed (recall 0.8125) all carry this exact string, and the `to` spelling
# below reached the right answer only because the old expression stopped at
# 7401 and ignored the tail. Both are now read as the same range.
# docs/evidence/discovery-slice-2026-07-28.md
@pytest.mark.parametrize(
    "raw",
    [
        "42 U.S.C. 7401-7671q.",  # 2060-AS32, -AU01, -AV95, -AW70, -AW96
        "42 U.S.C. 7401 to 7671q.",
        "42 U.S.C. 7401–7671q.",  # en dash
        "42 U.S.C. 7401 through 7671q.",
    ],
)
def test_clean_air_act_range_publishes_both_endpoints(raw):
    assert parse_authority_citation(raw) == [
        AuthorityCitation("usc", "ok", usc_title="42", usc_section="7401", usc_section_end="7671q")
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # A hyphen that names one section is not a range: the suffix is an
        # ordinal below the stem, never a later section.
        ("42 U.S.C. 1395w-4", ("1395w-4", None)),
        ("12 U.S.C. 1831p-1", ("1831p-1", None)),
        ("42 U.S.C. 300j-9", ("300j-9", None)),
        ("26 U.S.C. 1400Z-2", ("1400z-2", None)),
        # Equal endpoints are not a range either.
        ("42 U.S.C. 2000d-2000d", ("2000d-2000d", None)),
        # A lettered endpoint under the same stem is.
        ("15 U.S.C. 717-717w", ("717", "717w")),
        ("16 U.S.C. 620-620j", ("620", "620j")),
        # Abbreviated ranges name no second section, so nothing is invented:
        # "1484-86" is neither 1484-to-86 nor, without guessing, 1484-to-1486.
        ("42 U.S.C. 1484-86", ("1484-86", None)),
        ("5 U.S.C. 571 to 83", ("571", None)),
        # A transposed source range stays one opaque token rather than an
        # empty interval.
        ("50 U.S.C. 4801 to 4582", ("4801", None)),
        # A compound start with a range tail: the start is not a single
        # section token, so the pair is not readable as an interval.
        ("47 U.S.C. 615a-1 through 615b", ("615a-1", None)),
    ],
)
def test_usc_hyphen_is_a_range_only_when_the_endpoints_are_ordered(raw, expected):
    parsed = parse_authority_citation(raw)
    assert [(item.usc_section, item.usc_section_end) for item in parsed] == [expected]


@pytest.mark.parametrize(
    "raw",
    ["12 U.S.C. 1831p–1", "42 U.S.C. 1395w–3", "15 U.S.C. 1261 to 62"],
)
def test_declined_range_tail_is_not_reported_as_covered(raw):
    """A tail the ordering rule refuses is uncovered text, so the parse is partial."""
    (parsed,) = parse_authority_citation(raw)
    assert parsed.parse_status == "partial"
    assert parsed.usc_section_end is None


def test_listed_range_member_is_split_like_a_leading_one():
    assert parse_authority_citation("42 U.S.C. 7401, 7671a-7671q") == [
        AuthorityCitation("usc", "partial", usc_title="42", usc_section="7401"),
        AuthorityCitation("usc", "partial", usc_title="42", usc_section="7671a", usc_section_end="7671q"),
    ]


def test_range_row_expands_to_its_first_named_section_and_no_other():
    """The published identifier is a section the source text names."""
    (parsed,) = parse_authority_citation("42 U.S.C. 7401-7671q.")
    assert parsed.canonical_iri == "urn:rkaf:us:usc:42:7401"
    assert canonical_usc_iri(parsed.usc_title, parsed.usc_section_end) == "urn:rkaf:us:usc:42:7671q"


@pytest.mark.parametrize(
    ("section", "covered"),
    [
        ("7401", True),  # start
        ("7671q", True),  # end, lettered suffix
        ("7412", True),  # inside
        ("7671", True),  # inside, bare stem sorts before its lettered members
        ("7671a", True),  # inside, lettered
        ("7400", False),  # below
        ("7672", False),  # above
        ("7671r", False),  # above, lettered
        ("", False),
        ("seven", False),
    ],
)
def test_usc_section_covers_reads_a_range_as_a_closed_interval(section, covered):
    assert usc_section_covers(section, start="7401", end="7671q") is covered


@pytest.mark.parametrize(
    ("section", "covered"),
    [("7401", True), ("7401(a)", True), ("7412", False), ("7671q", False)],
)
def test_usc_section_covers_without_an_end_is_exact(section, covered):
    assert usc_section_covers(section, start="7401", end=None) is covered


def test_parse_multiple_authorities_returns_one_partial_edge_each():
    parsed = parse_authority_citation("42 U.S.C. 7401; Public Law 117-58")
    assert {(item.authority_type, item.parse_status) for item in parsed} == {
        ("usc", "partial"),
        ("public_law", "partial"),
    }


def test_rulespec_identifier_expansion():
    assert canonical_cfr_iri("40", "60") == "urn:rkaf:us:cfr:40:60"
    assert canonical_cfr_iri("40", "60", "1") == "urn:rkaf:us:cfr:40:60.1"
    assert canonical_cfr_iri("40", "60", "5375A(a)(1)") == "urn:rkaf:us:cfr:40:60.5375a"
    assert canonical_usc_iri("42", "7401(a)") == "urn:rkaf:us:usc:42:7401"
    assert canonical_rin_iri("2060-av16") == "urn:rkaf:us:rin:2060-AV16"
    assert canonical_frdoc_iri("2024-00366") == "urn:rkaf:us:frdoc:2024-00366"
    assert canonical_regsgov_iri("epa-hq-oar-2021-0317") == "urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317"
    assert canonical_pl_iri("117–58") == "urn:rkaf:us:pl:117-58"


def test_regulations_gov_identifier_normalization_matches_repaired_grammar():
    assert normalize_regsgov_identifier(" epa-hq-oar-2021-0317 ") == "EPA-HQ-OAR-2021-0317"
    assert normalize_regsgov_identifier("epa_frdoc_0001") == "EPA_FRDOC_0001"
    assert normalize_regsgov_identifier("EPA") == "EPA"
    assert normalize_regsgov_identifier("Sequence No. 1") is None
    assert normalize_regsgov_identifier(None) is None


@pytest.mark.parametrize(
    ("stated", "expected"),
    [
        # The measured decorated forms from
        # docs/corpus-edge-coverage-findings-2026-07-24.md §1: the label is
        # presentation, the remainder is identity.
        ("Doc. No. AMS-SC-24-0046", "AMS-SC-24-0046"),
        ("Docket No. FAA-2026-3485", "FAA-2026-3485"),
        ("Docket Number USCG-2026-0762", "USCG-2026-0762"),
        ("Docket No. OSM-2025-0007", "OSM-2025-0007"),
        ("Docket No.CPSC-2010-0075", "CPSC-2010-0075"),
        ("  docket id phmsa-2025-0118  ", "PHMSA-2025-0118"),
        # The label may name its own department first. "DHS Docket No." is still
        # label: USCIS-2025-0004 is the docket dockets.parquet carries.
        ("DHS Docket No. USCIS-2025-0004", "USCIS-2025-0004"),
        # An undecorated identifier is already identity.
        ("FSIS-2025-0012", "FSIS-2025-0012"),
        # An agency code the label grammar would otherwise eat. Commerce dockets
        # are DOC-YYYY-NNNN; stripping "DOC" would destroy a real identifier, so
        # a value that already parses is never rewritten.
        ("DOC-2010-0001", "DOC-2010-0001"),
        ("DOCKET-2026-0001", "DOCKET-2026-0001"),
        # Non-regulations.gov references. Those whose shape the scheme cannot
        # express are refused here; the rest are syntactically expressible and
        # are quarantined downstream by source-of-record evidence, never by
        # force-matching them to a docket.
        ("Special Conditions No. 25-893-SC", None),
        ("Amendment 39-21234", None),
        ("REG-103193-26", "REG-103193-26"),
        ("CMS-9897-F", "CMS-9897-F"),
        ("FRL-12765-02-OCSPP", "FRL-12765-02-OCSPP"),
        ("not a docket id", None),
        ("Docket No.", None),
        # A separator is source, not decoration. "FSIS 2025-0009" is one space
        # away from a real docket and is still refused, because supplying the
        # hyphen would be writing the identifier rather than reading it.
        ("Docket No. FSIS 2025-0009", None),
        # No label to strip, so nothing is stripped.
        ("EPA HQ OAR 2021 0317", None),
        ("", None),
        (None, None),
        # Measured on output/rin-ontology-revision-candidate: the label that
        # names its department first still uncovers the docket dockets.parquet
        # carries, and a docket-type letter between the year and the sequence
        # (FDA's busiest docket, 144 references) is part of the identifier.
        ("DOT Docket No. DOT-OST-2010-0074", "DOT-OST-2010-0074"),
        ("FHWA Docket No. FHWA-2005-23112", "FHWA-2005-23112"),
        ("CPSC Docket No. CPSC-2010-0075", "CPSC-2010-0075"),
        ("Docket No. FDA-2011-N-0002", "FDA-2011-N-0002"),
        ("Docket No. FSIS-2025-0012", "FSIS-2025-0012"),
        ("Docket No. PHMSA-2025-0118", "PHMSA-2025-0118"),
        ("Docket No. OSHA-V05-2-2006-0785", "OSHA-V05-2-2006-0785"),
    ],
)
def test_the_docket_label_is_presentation_and_the_identifier_survives_it(stated, expected):
    assert normalize_docket_reference(stated) == expected


@pytest.mark.parametrize(
    ("stated", "fragment"),
    [
        # Every string below is a reference measured on
        # output/rin-ontology-revision-candidate, beside the docket-number
        # fragment the label rule alone would have published for it. Stripping
        # the label uncovered no identifier there — it exposed the number the
        # label was numbering, and that number is not a Regulations.gov docket.
        ("MM Docket No. 98-213", "98-213"),
        ("WT Docket No. 17-17", "17-17"),
        ("Docket No. 17-17", "17-17"),
        ("EOIR Docket No. 176", "176"),
        ("FE Docket No. 96-99-LNG", "96-99-LNG"),
        ("CPSC Docket No. 08-C0004", "08-C0004"),
    ],
)
def test_a_stripped_label_may_uncover_a_docket_but_never_manufacture_one(stated, fragment):
    """Regression: 5,506 references were mutilated into docket-number fragments.

    Measured on ``output/rin-ontology-revision-candidate``, the leading agency
    token added by bfc9f8e turned 5,506 distinct non-regulations.gov references
    into bare numbers against 1,011 real recoveries. What the fragments have in
    common is that they do not name an organization: a Regulations.gov docket
    id states organization, year, then sequence, so a remainder that opens on a
    number is the label's subject matter, not an identifier hiding behind it.
    """
    assert normalize_regsgov_identifier(fragment) == fragment, "the fragment is what the scheme would have accepted"
    assert normalize_docket_reference(stated) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2060-AV16", "2060-AV16"),
        ("2060-av16", "2060-AV16"),
        ("  2060-av16  ", "2060-AV16"),
        ("2060AV16", None),
        ("2060-A16", None),
        ("2060-AV1", None),
        ("RIN 2060-AV16", None),
        ("", None),
        (None, None),
    ],
)
def test_rin_normalization(value, expected):
    assert normalize_rin(value) == expected


@pytest.mark.parametrize(
    ("stated", "key"),
    [
        # The variant shapes docs/corpus-edge-coverage-findings-2026-07-24.md §1
        # measured, and the three rules named in DOCKET_NORMALIZATION_RULES:
        # strip the leading decoration, repair whitespace that split a real
        # identifier, upper-case.
        ("Docket No. FAA-2026-3485", "FAA-2026-3485"),
        ("Doc. No. AMS-SC-24-0046", "AMS-SC-24-0046"),
        ("Docket Number USCG-2026-0762", "USCG-2026-0762"),
        ("Docket No: OSM-2025-0007", "OSM-2025-0007"),
        ("docket no. phmsa-2025-0118", "PHMSA-2025-0118"),
        ("Docket No. Docket No. FSIS-2025-0012", "FSIS-2025-0012"),
        ("  FAA - 2026 - 3485  ", "FAA-2026-3485"),
        ("FSIS-2025-0012", "FSIS-2025-0012"),
        # The strict separator. A decoration must be followed by whitespace or a
        # colon, so a real identifier whose organization spells one cannot be
        # truncated into a false match.
        ("DOC-2005-0010", "DOC-2005-0010"),
        ("DOCKET-2020-0001", "DOCKET-2020-0001"),
        ("", ""),
        ("   ", ""),
        (None, ""),
    ],
)
def test_the_docket_join_key_strips_decoration_repairs_and_upper_cases(stated, key):
    assert normalize_docket_id(stated) == key


def test_a_bare_label_leaves_a_key_that_is_not_a_docket():
    """ "Docket No." keys to "NO." — which is why the build drops it first.

    The decoration expression needs whitespace or a colon after the label, and
    at the end of a bare "Docket No." there is none, so only "Docket " strips.
    The residue matches no docket and never has (zero collisions across the
    276,326 dockets in the 54f07a6 pin), but a key made of decoration should not
    reach a link table at all — so `build_fr_docket_links` drops a reference
    that states nothing *before* keying it, using
    :func:`docket_reference_as_stated`, and this asserts why that ordering
    matters rather than that the residue is harmless.
    """
    assert normalize_docket_id("Docket No.") == "NO."
    assert docket_reference_as_stated("Docket No.") == ""


def test_the_docket_join_key_is_a_key_and_not_an_identifier():
    """Two functions, two jobs, and the difference is load-bearing.

    :func:`normalize_docket_reference` answers "what docket does this reference
    state?" and refuses anything that is not a Regulations.gov docket.
    :func:`normalize_docket_id` answers "what do I compare this against?" and
    refuses nothing — it reduces whatever it is given to a comparison key. A key
    that matches no docket is simply a key that matches no docket; it never
    licenses a join on its own.
    """
    foreign = "Special Conditions No. 25-893-SC"
    assert normalize_docket_reference(foreign) is None
    assert normalize_docket_id(foreign) == "SPECIALCONDITIONSNO.25-893-SC"

    # A shape the identifier grammar refuses because supplying the separator
    # would be writing the id rather than reading it. As a comparison key the
    # same string is expressible, and still matches no real docket.
    assert normalize_docket_reference("Docket No. FSIS 2025-0009") is None
    assert normalize_docket_id("Docket No. FSIS 2025-0009") == "FSIS2025-0009"


def test_the_docket_decoration_grammar_has_one_definition():
    """The crosswalk tool defined it; the links build needs it; so it moved.

    Same reasoning as the RIN grammar below. A pattern restated in a tool and a
    transform drifts the moment either is corrected, and this one decides
    whether 87,681 link rows join.
    """
    restating = sorted(
        str(path.relative_to(REPO_ROOT))
        for directory in ("src", "tools")
        for path in (REPO_ROOT / directory).rglob("*.py")
        if r"(?:docket\s*(?:no|number)?|doc\.?\s*no)" in path.read_text()
    )

    assert restating == ["src/spicy_regs/ontology/citations.py"]


def test_the_rin_grammar_has_one_definition():
    """A grammar copied into five modules drifts; a grammar imported cannot.

    The RIN shape was restated in four transforms and one discovery tool, each
    beside its own private ``_rin`` wrapper, so a correction to any one of them
    left the other four saying something different about the same identifier.
    """
    restating = sorted(
        str(path.relative_to(REPO_ROOT))
        for directory in ("src", "tools")
        for path in (REPO_ROOT / directory).rglob("*.py")
        if r"\d{4}-[A-Z]{2}\d{2}" in path.read_text()
    )

    assert restating == ["src/spicy_regs/ontology/citations.py"]


def test_invalid_cfr_suffix_is_rejected():
    with pytest.raises(ValueError):
        canonical_cfr_iri("40", "60", "appendix-a")


@pytest.mark.parametrize(
    ("document_number", "expected"),
    [
        ("2024-00366", ("rkaf:us-frdoc", "urn:rkaf:us:frdoc:2024-00366")),
        ("E7-21559", ("rkaf:partner-defined", "urn:spicy-regs:frdoc:E7-21559")),
        (
            "C1-2026-13078",
            ("rkaf:partner-defined", "urn:spicy-regs:frdoc:C1-2026-13078"),
        ),
    ],
)
def test_federal_register_identifier_uses_partner_fallback(document_number, expected):
    assert federal_register_identifier(document_number) == expected


def test_external_registry_anchors_are_syntax_checked():
    assert validated_external_ids(
        [
            {"scheme": "cas", "value": "335-67-1", "iri": None},
            {"scheme": "cas", "value": "335-67-2", "iri": None},
            {"scheme": "naics", "value": "325211", "iri": None},
            {
                "scheme": "skos:exactMatch",
                "value": "PFAS",
                "iri": "https://example.gov/concepts/pfas",
            },
        ]
    ) == (
        {"scheme": "cas", "value": "335-67-1"},
        {"scheme": "naics", "value": "325211"},
        {
            "scheme": "skos:exactMatch",
            "value": "PFAS",
            "iri": "https://example.gov/concepts/pfas",
        },
    )


@pytest.mark.parametrize(
    ("function", "value"),
    [
        (canonical_rin_iri, "Not Assigned"),
        (canonical_frdoc_iri, "2024-42"),
        (canonical_regsgov_iri, "no-spaces allowed"),
        (canonical_pl_iri, "0-58"),
    ],
)
def test_invalid_identifier_expansion_fails(function, value):
    with pytest.raises(ValueError):
        function(value)


def test_full_corpus_report_matches_declared_evidence_digest():
    declaration = yaml.safe_load((REPO_ROOT / "conformance" / "rulespec-l0.yaml").read_text())
    evidence_path = (REPO_ROOT / "conformance" / declaration["test_evidence_path"]).resolve()
    evidence = evidence_path.read_bytes()
    assert hashlib.sha256(evidence).hexdigest() == declaration["test_evidence_sha256"]


def test_rulespec_profile_inventories_every_public_table():
    profile = (REPO_ROOT / "docs" / "rulespec-profile.md").read_text()
    inventory = set(re.findall(r"^\| `([^`]+)` \|", profile, re.MULTILINE))

    assert inventory == set(expected_schemas())


# The act-relative grammar is dictionary-driven: recognition is a longest match
# against the Popular Name Tool's 13,627 names, not a shape heuristic. A generic
# "capitalised words then Act" expression run over the 4,777 sealed authority
# strings matches "U.S.C." 108 times, which is why the index is the grammar.
_ACT_NAMES = frozenset(
    normalize_popular_name(name)
    for name in (
        "Clean Air Act",
        "Clean Air Act Amendments of 1977",
        "Employee Retirement Income Security Act",
        "ERISA",
        "Federal Food, Drug, and Cosmetic Act",
        "Modernization of Cosmetics Regulation Act of 2022",
        "Social Security Act",
        "Toxic Substances Control Act",
    )
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # The minimal pair the bakeoff drew opposite verdicts on. Both are the
        # same citation and both now read the same way.
        ("Clean Air Act sec. 112", [("Clean Air Act", "112")]),
        ("Clean Air Act Section 112", [("Clean Air Act", "112")]),
        ("Clean Air Act section 111", [("Clean Air Act", "111")]),
        # Subsection detail is excluded, exactly as a U.S.C. section's is.
        ("Clean Air Act sec. 111(b)(1)(B)", [("Clean Air Act", "111")]),
        ("Clean Air Act §112", [("Clean Air Act", "112")]),
        # The act name may be introduced by prose.
        ("promulgated under the Clean Air Act sec. 112", [("Clean Air Act", "112")]),
        # An alias is a name like any other; resolving it is the index's job.
        ("ERISA sec. 803", [("ERISA", "803")]),
        # The inverted spelling the corpus also uses.
        (
            "sec. 3505 of the Modernization of Cosmetics Regulation Act of 2022",
            [("Modernization of Cosmetics Regulation Act of 2022", "3505")],
        ),
    ],
)
def test_an_act_relative_citation_is_read_from_the_index(raw, expected):
    found = find_act_relative_citations(raw, act_names=_ACT_NAMES)
    assert [(c.act_name, c.section) for c in found] == expected


@pytest.mark.parametrize(
    ("raw", "act"),
    [
        # Forward scan, `_longest_name_before`.
        ("Clean Air Act Amendments of 1977 sec. 5", "Clean Air Act Amendments of 1977"),
        # Inverted scan, `_longest_name_after`. This is the case that actually
        # discriminates: the shorter name is a *prefix* here, so a shortest-match
        # rule reads "Clean Air Act" and cites the parent statute instead of the
        # amending one. Pinning only the forward scan left this free to shorten.
        ("sec. 5 of the Clean Air Act Amendments of 1977", "Clean Air Act Amendments of 1977"),
        ("section 5 of Clean Air Act Amendments of 1977", "Clean Air Act Amendments of 1977"),
    ],
)
def test_the_longest_known_name_wins(raw, act):
    """ "Clean Air Act Amendments of 1977" is a different act from "Clean Air Act".

    The two share a Table III key today, so no identity moves yet — but that is
    an accident of the data, not a property of the rule, which is why the rule
    is pinned rather than left to the corpus to enforce.
    """
    (found,) = find_act_relative_citations(raw, act_names=_ACT_NAMES)
    assert found.act_name == act


@pytest.mark.parametrize(
    "raw",
    [
        # No section: naming an act is not citing a provision of one.
        "Clean Air Act",
        "authority under the Clean Air Act, as amended",
        # A name the index does not carry. The corpus writes these and they stay
        # unread: guessing that "INA" means the Immigration and Nationality Act
        # is exactly the inference the identity fence exists to stop.
        "INA sec. 103(a)(1)",
        "PHS Act secs. 2791(b)(5) and 2792",
        "ACA sec. 1557",
        # The 108 false positives a shape heuristic produced.
        "42 U.S.C. 7401",
        "42 U.S.C. sec. 7401",
        "5 U.S.C. sec. 553",
        # A section marker with nothing in front of it.
        "sec. 112",
        "",
    ],
)
def test_an_act_the_index_does_not_name_is_not_read(raw):
    assert find_act_relative_citations(raw, act_names=_ACT_NAMES) == []


def test_one_string_may_cite_two_acts():
    found = find_act_relative_citations(
        "Clean Air Act sec. 112 and Toxic Substances Control Act section 6",
        act_names=_ACT_NAMES,
    )
    assert [(c.act_name, c.section) for c in found] == [
        ("Clean Air Act", "112"),
        ("Toxic Substances Control Act", "6"),
    ]


@pytest.mark.parametrize(
    ("stated", "key"),
    [
        ("Clean Air Act", "clean air act"),
        ("  Clean   Air  Act  ", "clean air act"),
        ("Clean Air Act.", "clean air act"),
        ("Federal Food, Drug, and Cosmetic Act", "federal food, drug, and cosmetic act"),
        # The tool writes a curly apostrophe; prose usually writes a straight one.
        ("Workers’ Compensation Act", "workers' compensation act"),
        ("Wagner–Peyser Act", "wagner-peyser act"),
    ],
)
def test_a_popular_name_joins_on_one_normalized_key(stated, key):
    assert normalize_popular_name(stated) == key

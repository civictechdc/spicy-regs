"""The U.S. Code's own source credits, read as a pure parser over USLM bytes.

Every fixture below is real markup, copied from the release point the artifact
pins. The parser is asserted against those bytes rather than against a live
request, so the shapes this module claims to read are pinned by the test.

The rule under test is a **refusal** rule. A source credit lists the enactment
*and* every later amendment, so a match that does not require an enactment
construction pairs a division with a section number from a different citation in
the same credit. The measured example is in this file: 26 U.S.C. 7652's credit
names ``div. EE ... § 107``, and 7652 was not added by it.
"""

from __future__ import annotations

import pytest

from spicy_regs.sources.uscode_uslm import (
    STRICT_ENACTMENT_RULE,
    USLM_SECTION_DASH_RULE,
    normalize_usc_section,
    scan_source_credits,
    uslm_release_url,
)

USLM = "http://xml.house.gov/schemas/uslm/1.0"


def _document(body: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<uscDoc xmlns="{USLM}" identifier="/us/usc/t26"><main>{body}</main></uscDoc>""".encode()


# 26 U.S.C. 6038E, verbatim. The enactment construction is "(Added Pub. L. ...".
ADDED = _document(
    """<section identifier="/us/usc/t26/s6038E"><num value="6038E">§ 6038E.</num>
<heading>Information</heading><content><p>text</p></content>
<sourceCredit>(Added <ref href="/us/pl/116/260/dEE/tI/s107/d/1">Pub. L. 116–260, div. EE, title I, § 107(d)(1)</ref>, <date date="2020-12-27">Dec. 27, 2020</date>, <ref href="/us/stat/134/3048">134 Stat. 3048</ref>.)</sourceCredit>
</section>"""
)

# 29 U.S.C. 1153, verbatim. The construction is "as added Pub. L. ..." and the
# credit ALSO names Pub. L. 93-406, which enacted the section it was added to.
AS_ADDED = _document(
    """<section identifier="/us/usc/t29/s1153"><num value="1153">§ 1153.</num>
<sourceCredit>(<ref href="/us/pl/93/406/tI/s523">Pub. L. 93–406, title I, § 523</ref>, as added <ref href="/us/pl/117/328/dT/tIII/s303/a">Pub. L. 117–328, div. T, title III, § 303(a)</ref>, <date date="2022-12-29">Dec. 29, 2022</date>, <ref href="/us/stat/136/5339">136 Stat. 5339</ref>.)</sourceCredit>
</section>"""
)

# 26 U.S.C. 7652, abbreviated but verbatim in the part that matters. The credit
# names (116-260, div. EE, sec. 107) as an AMENDMENT, at 134 Stat. 3046 -- the
# enactment of 6038E is at 3048. A loose rule credits 7652 to that triple. This
# is the measured false positive the strict rule exists to remove.
AMENDED_ONLY = _document(
    """<section identifier="/us/usc/t26/s7652"><num value="7652">§ 7652.</num>
<sourceCredit>(<date date="1954-08-16">Aug. 16, 1954</date>, ch. 736, 68A Stat. 907; <ref href="/us/pl/115/123/dD/tII/s41102/a/1">Pub. L. 115–123, div. D, title II, § 41102(a)(1), (b)(1)</ref>, Feb. 9, 2018, 132 Stat. 155; <ref href="/us/pl/116/260/dEE/tI/s107/a/2">Pub. L. 116–260, div. EE, title I, § 107(a)(2)</ref>, <date date="2020-12-27">Dec. 27, 2020</date>, <ref href="/us/stat/134/3046">134 Stat. 3046</ref>.)</sourceCredit>
</section>"""
)

# USLM spells a section suffix with an EN DASH; Table III spells it with a
# hyphen. Straightening it is the only way the two sources join at all.
EN_DASH = _document(
    """<section identifier="/us/usc/t16/s824s–1"><num value="824s–1">§ 824s–1.</num>
<sourceCredit>(Added <ref href="/us/pl/117/58/dD/tI/s40123">Pub. L. 117–58, div. D, title I, § 40123(a)</ref>, <date date="2021-11-15">Nov. 15, 2021</date>, <ref href="/us/stat/135/946">135 Stat. 946</ref>.)</sourceCredit>
</section>"""
)


def test_an_added_construction_yields_the_triple_and_its_page():
    scan = scan_source_credits(ADDED)

    (credit,) = scan.credits
    assert (credit.public_law, credit.division, credit.act_section) == ("116-260", "EE", "107")
    assert (credit.usc_title, credit.usc_section) == ("26", "6038E")
    assert credit.usc_identifier == "/us/usc/t26/s6038E"
    assert (credit.statutes_at_large_volume, credit.statutes_at_large_page) == ("134", "3048")
    assert scan.credits_scanned == 1
    assert scan.credits_naming_a_division == 1
    assert scan.quarantine == []


def test_an_as_added_construction_credits_the_adding_law_not_the_amended_one():
    """ "Pub. L. 93-406 ... as added Pub. L. 117-328, div. T, sec. 303(a)".

    The section was added BY 117-328 TO the act 93-406 created. Only the adding
    law is an enactment of this section, and only it names a division.
    """
    scan = scan_source_credits(AS_ADDED)

    (credit,) = scan.credits
    assert (credit.public_law, credit.division, credit.act_section) == ("117-328", "T", "303")
    assert (credit.usc_title, credit.usc_section) == ("29", "1153")
    assert (credit.statutes_at_large_volume, credit.statutes_at_large_page) == ("136", "5339")


def test_a_credit_that_only_amends_yields_nothing():
    """The measured false positive, kept as a regression.

    26 U.S.C. 7652's credit names (116-260, div. EE, sec. 107) at 134 Stat.
    3046. A loose expression reads that as an enactment and credits 7652 to the
    triple that actually enacted 26 U.S.C. 6038E at 3048. Reading the role by
    proximity to the word "amended" does not fix it -- this credit never uses
    the word.
    """
    scan = scan_source_credits(AMENDED_ONLY)

    assert scan.credits == []
    assert scan.credits_scanned == 1
    assert scan.credits_naming_a_division == 1, "the credit does name a division; the rule declines it anyway"


def test_the_en_dash_in_a_uslm_section_identifier_is_straightened():
    """USLM writes ``824s-1`` with an en dash and Table III writes a hyphen."""
    scan = scan_source_credits(EN_DASH)

    (credit,) = scan.credits
    assert credit.usc_section == "824s-1"
    # The verbatim identifier is kept, so the artifact stays auditable against
    # the bytes it was read from.
    assert credit.usc_identifier == "/us/usc/t16/s824s–1"
    assert normalize_usc_section("824s–1") == "824s-1"
    assert USLM_SECTION_DASH_RULE == "straighten-uslm-en-dash-to-hyphen-v1"


def test_a_credit_outside_any_usc_section_is_quarantined_not_dropped():
    """A credit the parser cannot attribute must be visible, never silent."""
    document = _document(
        """<level identifier="/us/usc/t26/ch61"><heading>x</heading>
<sourceCredit>(Added <ref href="/us/pl/116/260/dEE/tI/s107">Pub. L. 116–260, div. EE, title I, § 107</ref>, <ref href="/us/stat/134/3048">134 Stat. 3048</ref>.)</sourceCredit>
</level>"""
    )
    scan = scan_source_credits(document)

    assert scan.credits == []
    (quarantined,) = scan.quarantine
    assert quarantined.reason == "credit_outside_usc_section"
    assert (quarantined.public_law, quarantined.division, quarantined.act_section) == ("116-260", "EE", "107")
    assert scan.credits_outside_a_section == 1


def test_a_section_identifier_the_usc_shape_cannot_spell_is_quarantined():
    """The appendix titles carry ``/us/usc/t18a/pl/91/538/s1`` and its kin."""
    document = _document(
        """<section identifier="/us/usc/t18a/pl/91/538/s1"><num value="1">§ 1.</num>
<sourceCredit>(Added <ref href="/us/pl/116/260/dEE/tI/s107">Pub. L. 116–260, div. EE, title I, § 107</ref>, <ref href="/us/stat/134/3048">134 Stat. 3048</ref>.)</sourceCredit>
</section>"""
    )
    scan = scan_source_credits(document)

    assert scan.credits == []
    (quarantined,) = scan.quarantine
    assert quarantined.reason == "section_identifier_unparsable"
    assert quarantined.raw_value == "/us/usc/t18a/pl/91/538/s1"


def test_a_credit_naming_no_division_is_not_read_at_all():
    """The index is keyed by division. A credit without one cannot key into it.

    This is a coverage bound, stated rather than hidden: 22 U.S.C. 2714a reads
    "(Pub. L. 114-94, div. C, title XXXII, sec. 32101, ...)" with no enactment
    construction, and 21 U.S.C. 350a-1 likewise -- both are real classifications
    this index does not carry.
    """
    document = _document(
        """<section identifier="/us/usc/t26/s1"><num value="1">§ 1.</num>
<sourceCredit>(Added <ref href="/us/pl/115/97/tI/s11001">Pub. L. 115–97, title I, § 11001(a)</ref>, <ref href="/us/stat/131/2054">131 Stat. 2054</ref>.)</sourceCredit>
</section>"""
    )
    scan = scan_source_credits(document)

    assert scan.credits == []
    assert scan.quarantine == []
    assert scan.credits_naming_a_division == 0


def test_the_page_belongs_to_the_citation_it_follows():
    """Two enactment citations in one credit each keep their own page."""
    document = _document(
        """<section identifier="/us/usc/t42/s1"><num value="1">§ 1.</num>
<sourceCredit>(Added <ref href="/us/pl/116/260/dEE/tI/s107">Pub. L. 116–260, div. EE, title I, § 107</ref>, <ref href="/us/stat/134/3048">134 Stat. 3048</ref>; added <ref href="/us/pl/117/328/dT/tIII/s303">Pub. L. 117–328, div. T, title III, § 303</ref>, <ref href="/us/stat/136/5339">136 Stat. 5339</ref>.)</sourceCredit>
</section>"""
    )
    scan = scan_source_credits(document)

    assert {(c.act_section, c.statutes_at_large_page) for c in scan.credits} == {("107", "3048"), ("303", "5339")}


def test_the_strict_rule_is_a_named_constant():
    assert STRICT_ENACTMENT_RULE == "added-or-as-added-pub-law-division-act-section-v1"


@pytest.mark.parametrize(
    ("congress", "law", "expected"),
    [(119, 102, "https://uscode.house.gov/download/releasepoints/us/pl/119/102/xml_uscAll@119-102.zip")],
)
def test_the_release_point_url_is_derived_from_the_release_point(congress, law, expected):
    assert uslm_release_url(f"{congress}-{law}") == expected


def test_a_release_point_of_the_wrong_shape_is_refused():
    with pytest.raises(ValueError):
        uslm_release_url("119.102")

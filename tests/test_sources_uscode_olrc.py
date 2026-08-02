"""The two OLRC tables that make an act-relative citation resolvable.

Every fixture below is bytes captured from the live pages on 2026-08-02, pasted
verbatim. A parser tested against handwritten markup tests the handwriting.
"""

from __future__ import annotations

import pytest

from spicy_regs.ontology.citations import normalize_popular_name
from spicy_regs.sources.uscode_olrc import (
    PopularNameIndex,
    Table3Record,
    parse_popular_names,
    parse_table3,
    table3_url,
)

# uscode.house.gov/popularnames/popularnames.htm, release point 119-102.
POPULAR_NAMES = """
<div id='CleanAirAct' class='popular-name-table-entry' release-point='119-102' item='1695'>
    <p class='popular-name'>Clean Air Act</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='1955:360' datekey='1955-07-14' usckey='42:7401'>July 14, <a href="/table3/1955_360.htm" title='Jump to Table III Records!'>1955, ch. 360</a>, <a href="/statviewer.htm?volume=69&amp;page=322" title='Jump to Stat. page Image!' target="_blank">69 Stat. 322</a> (<a href="/view.xhtml?req=granuleid:USC-prelim-title42-section7401&num=0&edition=prelim">42 U.S.C. 7401</a> et seq.)</p>
    <p class='popular-name-information' content-type='short-title-ref' usckey='42:7401'>Short title, see <a href="/view.xhtml?req=granuleid:USC-prelim-title42-section7401&num=0&edition=prelim">42 U.S.C. 7401</a> note</p>
  </div>

<div id='ERISA' class='popular-name-table-entry' release-point='119-102' item='4559'>
    <p class='popular-name'>ERISA</p>
    <p class='popular-name-information' content-type='see'>See Employee Retirement Income Security Act</p>
  </div>

<div id='AirPollutionControlAct' class='popular-name-table-entry' release-point='119-102' item='347'>
    <p class='popular-name'>Air Pollution Control Act</p>
    <p class='popular-name-information' content-type='see'>See Clean Air Act</p>
  </div>

<div id='ImmigrationandNationalityAct' class='popular-name-table-entry' release-point='119-102' item='6613'>
    <p class='popular-name'>Immigration and Nationality Act</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='1952:477' datekey='1952-06-27' usckey='8:1101'>June 27, <a href="/table3/1952_477.htm">1952, ch. 477</a>, <a href="/statviewer.htm?volume=66&amp;page=163">66 Stat. 163</a> (<a href="/view.xhtml?req=granuleid:USC-prelim-title8-section1101&num=0&edition=prelim">8 U.S.C. 1101</a> et seq.)</p>
    <p class='popular-name-information' content-type='short-title-ref' usckey='8:1101'>Short title, see <a href="/view.xhtml?req=granuleid:USC-prelim-title8-section1101&num=0&edition=prelim">8 U.S.C. 1101</a> note</p>
  </div>

<div id='ClaytonAct' class='popular-name-table-entry' release-point='119-102' item='1690'>
    <p class='popular-name'>Clayton Act</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='1914:323' datekey='1914-10-15' usckey='15:12'>Oct. 15, <a href="/table3/1914_323.htm">1914, ch. 323</a>, <a href="/statviewer.htm?volume=38&amp;page=730">38 Stat. 730</a> (<a href="/view.xhtml?req=granuleid:USC-prelim-title15-section12&num=0&edition=prelim">15 U.S.C. 12</a> et seq.)</p>
    <p class='popular-name-information' content-type='short-title-ref' usckey='15:12(b)'>Short title, see <a href="/view.xhtml?req=granuleid:USC-prelim-title15-section12(b)&num=0&edition=prelim">15 U.S.C. 12(b)</a></p>
  </div>
"""

# uscode.house.gov/table3/1955_360.htm — the Clean Air Act's classifications.
TABLE3 = """
  <tr class="table3row_odd">
   <td class="actsection">111</td>
   <td class="statutesatlargepage"><a href="/statviewer.htm?volume=69&page=" target="_blank"></a></td> <td class="unitedstatescodetitle">42</td>
   <td class="unitedstatescodesection"><a href="/view.xhtml?req=granuleid:USC-prelim-title42-section7411&num=0&edition=prelim" target="_blank">7411</a></td>
    <td class="unitedstatescodestatus"></td>
  </tr>
  <tr class="table3row_even">
   <td class="actsection">112</td>
   <td class="statutesatlargepage"><a href="/statviewer.htm?volume=69&page=" target="_blank"></a></td> <td class="unitedstatescodetitle">42</td>
   <td class="unitedstatescodesection"><a href="/view.xhtml?req=granuleid:USC-prelim-title42-section7412&num=0&edition=prelim" target="_blank">7412</a></td>
    <td class="unitedstatescodestatus"></td>
  </tr>
  <tr class="table3row_odd">
   <td class="actsection">150-159</td>
   <td class="statutesatlargepage"><a href="/statviewer.htm?volume=69&page=" target="_blank"></a></td> <td class="unitedstatescodetitle">42</td>
   <td class="unitedstatescodesection">7450-7459</td>
   <td class="unitedstatescodestatus">Rep.</td>
  </tr>
"""


# uscode.house.gov/table3/116_260.htm — the three rows that share act section
# 107 under one public law. The Statutes at Large page is what tells them apart.
TABLE3_SHARED_PUBLIC_LAW = """
  <tr class="table3row_even"> <td class="actsection">107</td>
   <td class="statutesatlargepage"><a href="/statviewer.htm?volume=134&page=2221" target="_blank">2221</a></td>
   <td class="unitedstatescodetitle">49</td>
   <td class="unitedstatescodesection"><a href="/view.xhtml?req=granuleid:USC-prelim-title49-section60122">60122</a></td>
   <td class="unitedstatescodestatus"></td></tr>
  <tr class="table3row_odd"> <td class="actsection">107</td>
   <td class="statutesatlargepage"><a href="/statviewer.htm?volume=134&page=2276" target="_blank">2276</a></td>
   <td class="unitedstatescodetitle">20</td>
   <td class="unitedstatescodesection"><a href="/view.xhtml?req=granuleid:USC-prelim-title20-section80t-5">80t-5</a></td>
   <td class="unitedstatescodestatus"></td></tr>
"""

# uscode.house.gov/popularnames — the act side of the same discriminator.
POPULAR_NAMES_DIVISIONS = """
<div id='TCDTRA' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Taxpayer Certainty and Disaster Tax Relief Act of 2020</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='116-260' datekey='2020-12-27'>Pub. L. <a href="/table3/116_260.htm">116-260, div. EE</a>, Dec. 27, 2020,<a href="/statviewer.htm?volume=134&amp;page=3038">134 Stat. 3038</a></p>
  </div>
<div id='WRDA' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Water Resources Development Act of 2020</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='116-260' datekey='2020-12-27'>Pub. L. 116-260, div. AA, Dec. 27, 2020,<a href="/statviewer.htm?volume=134&amp;page=2615">134 Stat. 2615</a></p>
  </div>
<div id='CAA2021' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Consolidated Appropriations Act, 2021</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='116-260' datekey='2020-12-27'>Pub. L. 116-260, Dec. 27, 2020,<a href="/statviewer.htm?volume=134&amp;page=1182">134 Stat. 1182</a></p>
  </div>
"""


def test_a_table_three_row_carries_the_statutes_at_large_citation():
    """The volume lives only in the link's query string; the page is in both.

    Discarding this was what left 471 (public law, act section) pairs with no
    way to tell one act from another.
    """
    rows = parse_table3(TABLE3_SHARED_PUBLIC_LAW)

    assert [(r.statutes_at_large_volume, r.statutes_at_large_page) for r in rows] == [
        ("134", "2221"),
        ("134", "2276"),
    ]


def test_a_row_whose_link_states_no_page_reports_none():
    """Some Table III links carry a volume and an empty page."""
    (row,) = parse_table3(
        '<tr class="table3row_odd"><td class="actsection">111</td>'
        '<td class="statutesatlargepage"><a href="/statviewer.htm?volume=69&page="></a></td>'
        '<td class="unitedstatescodetitle">42</td>'
        '<td class="unitedstatescodesection">7411</td>'
        '<td class="unitedstatescodestatus"></td></tr>'
    )
    assert (row.statutes_at_large_volume, row.statutes_at_large_page) == ("69", None)


def test_an_act_states_the_division_and_page_where_it_begins():
    """ "Pub. L. 116-260, div. EE, ... 134 Stat. 3038" — both halves, from the tool."""
    records = {r.name: r for r in parse_popular_names(POPULAR_NAMES_DIVISIONS) if r.content_type == "cite"}

    tcdtra = records["Taxpayer Certainty and Disaster Tax Relief Act of 2020"]
    assert (tcdtra.division, tcdtra.statutes_at_large_volume, tcdtra.statutes_at_large_page) == (
        "EE",
        "134",
        "3038",
    )
    wrda = records["Water Resources Development Act of 2020"]
    assert (wrda.division, wrda.statutes_at_large_page) == ("AA", "2615")


def test_an_act_that_is_the_whole_public_law_states_no_division():
    """ "Consolidated Appropriations Act, 2021" is all of 116-260, not a division.

    It must not be given a division it does not have: an act with no division
    spans the whole law, and treating its start page as a division boundary
    would carve the law at the wrong place.
    """
    (record,) = [
        r
        for r in parse_popular_names(POPULAR_NAMES_DIVISIONS)
        if r.name == "Consolidated Appropriations Act, 2021" and r.content_type == "cite"
    ]
    assert record.division is None
    assert record.statutes_at_large_page == "1182"


def _by_name(records, name, content_type):
    return next(r for r in records if r.name == name and r.content_type == content_type)


def test_the_popular_name_tool_yields_one_record_per_stated_fact():
    """An entry states several things; collapsing them would lose the aliases."""
    records = parse_popular_names(POPULAR_NAMES)

    assert [(r.name, r.content_type) for r in records] == [
        ("Clean Air Act", "cite"),
        ("Clean Air Act", "short-title-ref"),
        ("ERISA", "see"),
        ("Air Pollution Control Act", "see"),
        ("Immigration and Nationality Act", "cite"),
        ("Immigration and Nationality Act", "short-title-ref"),
        ("Clayton Act", "cite"),
        ("Clayton Act", "short-title-ref"),
    ]


def test_the_enacting_citation_carries_the_table_three_key_and_the_code_anchor():
    """ "Clean Air Act" -> Table III 1955:360, and 42 U.S.C. 7401 et seq."""
    record = _by_name(parse_popular_names(POPULAR_NAMES), "Clean Air Act", "cite")

    assert (record.name, record.content_type, record.table3_key) == ("Clean Air Act", "cite", "1955:360")
    assert (record.usc_title, record.usc_section) == ("42", "7401")
    assert (record.see_also, record.release_point) == (None, "119-102")
    # An act that is its own public law states no division.
    assert record.division is None
    assert (record.statutes_at_large_volume, record.statutes_at_large_page) == ("69", "322")
    assert "69 Stat. 322" in record.stated


@pytest.mark.parametrize(
    ("name", "target"),
    [
        # The acronym problem solves itself: the tool lists the acronym as an
        # entry pointing at the act, so nothing has to be hand-curated.
        ("ERISA", "Employee Retirement Income Security Act"),
        ("Air Pollution Control Act", "Clean Air Act"),
    ],
)
def test_an_alias_records_what_it_points_at(name, target):
    record = _by_name(parse_popular_names(POPULAR_NAMES), name, "see")
    assert record.see_also == target
    assert record.table3_key is None, "an alias names no act of its own"


def test_a_usc_key_with_a_subsection_keeps_it_for_the_identifier_grammar_to_drop():
    """ "15:12(b)" is what the tool states; narrowing it is not this layer's job.

    A reader that silently rewrote it to "12" would be making the U.S.C.
    identifier decision here, in a source parser, where it cannot be reviewed.
    """
    record = _by_name(parse_popular_names(POPULAR_NAMES), "Clayton Act", "short-title-ref")
    assert (record.usc_title, record.usc_section) == ("15", "12(b)")


def test_table_three_maps_an_act_section_to_the_code_section_it_became():
    """The answer the evidence doc predicted: CAA sec. 111 is 42 U.S.C. 7411."""
    records = parse_table3(TABLE3)

    assert records[0] == Table3Record(
        act_section="111",
        usc_title="42",
        usc_section="7411",
        status=None,
        # The Clean Air Act's rows link a volume with no page.
        statutes_at_large_volume="69",
        statutes_at_large_page=None,
    )
    assert records[1].usc_section == "7412"


def test_a_classification_that_went_nowhere_is_kept_and_says_so():
    """ "Rep." is knowledge. Dropping the row would turn it into ignorance."""
    (repealed,) = [r for r in parse_table3(TABLE3) if r.status]
    assert (repealed.act_section, repealed.usc_section, repealed.status) == ("150-159", "7450-7459", "Rep.")


@pytest.mark.parametrize(
    ("key", "url"),
    [
        ("1955:360", "https://uscode.house.gov/table3/1955_360.htm"),
        ("90-148", "https://uscode.house.gov/table3/90_148.htm"),
    ],
)
def test_both_table_three_key_spellings_become_a_url(key, url):
    assert table3_url(key) == url


@pytest.mark.parametrize("key", ["", "1955-360", "../etc/passwd", "1955:360.htm", None, "abc"])
def test_a_key_the_tool_never_emits_is_refused_rather_than_fetched(key):
    """A fetch loop over thousands of acts should fail on the key, not the 404."""
    with pytest.raises(ValueError):
        table3_url(key)


def test_an_entry_with_no_name_is_skipped_rather_than_published_nameless():
    assert (
        parse_popular_names(
            "<div id='X' class='popular-name-table-entry' release-point='119-102'>"
            "<p class='popular-name-information' content-type='cite' t3searchkey='90-148'>x</p></div>"
        )
        == []
    )


def _index():
    return PopularNameIndex.from_records(parse_popular_names(POPULAR_NAMES), normalize=normalize_popular_name)


def test_the_index_joins_a_name_to_the_act_it_names():
    index = _index()
    assert index.table3_key("clean air act") == "1955:360"
    assert index.usc_anchor_by_name["clean air act"] == ("42", "7401")


def test_an_alias_resolves_to_the_act_it_points_at():
    """ "Air Pollution Control Act" is the Clean Air Act under its older name."""
    assert _index().table3_key("air pollution control act") == "1955:360"


def test_an_alias_target_the_tool_does_not_list_is_reached_through_its_year():
    """The measured ERISA case, and the largest act in the sealed corpus.

    "ERISA" points at "Employee Retirement Income Security Act". The tool lists
    no entry by that name — only "… Act of 1974". The year is how the tool
    distinguishes acts, so it cannot be dropped from the target; it can only be
    supplied, and only when exactly one act supplies it.
    """
    index = PopularNameIndex.from_records(
        parse_popular_names(
            POPULAR_NAMES
            + """
<div id='ERISA1974' class='popular-name-table-entry' release-point='119-102' item='4560'>
    <p class='popular-name'>Employee Retirement Income Security Act of 1974</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='93-406' datekey='1974-09-02' usckey='29:1001'>Sept. 2, <a href="/table3/93_406.htm">1974</a></p>
  </div>
"""
        ),
        normalize=normalize_popular_name,
    )
    assert index.resolve("erisa") == "employee retirement income security act of 1974"
    assert index.table3_key("erisa") == "93-406"


def test_a_target_several_acts_could_supply_is_refused():
    """ "Clean Air Act Amendments" is 1966, 1970 and 1977. Picking one is inventing."""
    index = PopularNameIndex.from_records(
        parse_popular_names(
            """
<div id='A' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>CAAA</p>
    <p class='popular-name-information' content-type='see'>See Clean Air Act Amendments</p>
  </div>
<div id='B' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Clean Air Act Amendments of 1966</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='89-675'>x</p>
  </div>
<div id='C' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Clean Air Act Amendments of 1977</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='95-95'>x</p>
  </div>
"""
        ),
        normalize=normalize_popular_name,
    )
    assert index.resolve("caaa") is None


def test_an_alias_cycle_terminates_without_an_answer():
    index = PopularNameIndex.from_records(
        parse_popular_names(
            """
<div id='A' class='popular-name-table-entry'><p class='popular-name'>A Act</p>
  <p class='popular-name-information' content-type='see'>See B Act</p></div>
<div id='B' class='popular-name-table-entry'><p class='popular-name'>B Act</p>
  <p class='popular-name-information' content-type='see'>See A Act</p></div>
"""
        ),
        normalize=normalize_popular_name,
    )
    assert index.resolve("a act") is None


def test_the_index_offers_aliases_to_the_grammar_as_matchable_names():
    """The grammar must be able to match "ERISA" before the index resolves it."""
    assert {"clean air act", "erisa", "air pollution control act"} <= _index().names

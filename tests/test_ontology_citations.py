"""Fixture coverage for CFR/U.S.C./Public-Law citation grammars."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest
import yaml

from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.ontology.citations import (
    AuthorityCitation,
    CfrCitation,
    canonical_cfr_iri,
    canonical_frdoc_iri,
    canonical_pl_iri,
    canonical_regsgov_iri,
    canonical_rin_iri,
    canonical_usc_iri,
    federal_register_identifier,
    normalize_docket_reference,
    normalize_regsgov_identifier,
    normalize_rin,
    parse_authority_citation,
    parse_cfr_citation,
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

    Every other CFR branch is anchored by the literal "CFR" (``_CFR_STANDARD``)
    or by the word "part" (``_CFR_TITLE_PART``), so an implausible title there
    is still a title the source text called one. The compact branch has no
    anchor at all — it reads a bare ``N-M`` as title-part — so the only thing
    left to hold it closed is the range the CFR actually has.
    """
    assert parse_cfr_citation(raw) == []


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

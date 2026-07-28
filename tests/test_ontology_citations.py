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
    normalize_regsgov_identifier,
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

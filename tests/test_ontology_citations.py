"""Fixture coverage for CFR/U.S.C./Public-Law citation grammars."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import yaml

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
    parse_authority_citation,
    parse_cfr_citation,
)
from spicy_regs.ontology.llm import validated_external_ids

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"title": 40, "part": 60}, [CfrCitation("40", "60")]),
        ({"title": "40", "part": "60", "section": "1"}, [CfrCitation("40", "60", "1")]),
        ("40-60.1", [CfrCitation("40", "60", "1")]),
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


def test_parse_multiple_authorities_returns_one_partial_edge_each():
    parsed = parse_authority_citation("42 U.S.C. 7401; Public Law 117-58")
    assert {(item.authority_type, item.parse_status) for item in parsed} == {
        ("usc", "partial"),
        ("public_law", "partial"),
    }


def test_rulespec_identifier_expansion():
    assert canonical_cfr_iri("40", "60") == "urn:rkaf:us:cfr:40:60"
    assert canonical_cfr_iri("40", "60", "1") == "urn:rkaf:us:cfr:40:60.1"
    assert canonical_usc_iri("42", "7401(a)") == "urn:rkaf:us:usc:42:7401"
    assert canonical_rin_iri("2060-av16") == "urn:rkaf:us:rin:2060-AV16"
    assert canonical_frdoc_iri("2024-00366") == "urn:rkaf:us:frdoc:2024-00366"
    assert canonical_regsgov_iri("epa-hq-oar-2021-0317") == "urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317"
    assert canonical_pl_iri("117–58") == "urn:rkaf:us:pl:117-58"


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
    evidence = (REPO_ROOT / "docs" / "ontology-friction-report.md").read_bytes()
    assert hashlib.sha256(evidence).hexdigest() == declaration["test_evidence_sha256"]

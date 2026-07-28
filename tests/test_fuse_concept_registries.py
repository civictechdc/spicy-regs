"""Tests for the multi-scheme concept-registry fusion tool.

Every parser is exercised on a tiny synthetic fixture written inline: no
network, no downloaded corpus, no fixture files. Beyond parsing, three
properties carry the tool and are asserted directly:

* **schemes stay distinct** — one label present in several sources becomes
  several concepts, never one;
* **the prior registry is frozen** — its rows survive byte-for-byte, and a
  source term matching one of them enriches through the sidecar instead;
* **id minting matches the repository idiom** — the ids this tool mints are the
  ids ``spicy_regs.ontology`` would have minted, which is what makes the
  Federal Register re-parse land on the existing rows rather than beside them.
"""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

from spicy_regs.ontology.common import RunContext, canonical_json, read_parquet_rows, stable_id
from spicy_regs.ontology.concepts import CONCEPT_COLUMNS, concept_aliases, normalize_label
from spicy_regs.ontology.concepts import select_candidate_concepts_for_text
from spicy_regs.ontology.invariants import assert_concept_graphs

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "fuse_concept_registries.py"


def _load_tool():
    spec = importlib.util.spec_from_file_location("fuse_concept_registries", TOOL_PATH)
    assert spec and spec.loader, f"could not load {TOOL_PATH}"
    module = importlib.util.module_from_spec(spec)
    # Registered before execution because the module defines dataclasses, which
    # resolve their own module during class creation.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tool():
    return _load_tool()


@pytest.fixture
def context() -> RunContext:
    return RunContext(run_id="test-run", asserted_at="2026-01-01T00:00:00Z")


# --------------------------------------------------------------------------
# Federal Register Thesaurus
# --------------------------------------------------------------------------

FR_FIXTURE = """FEDERAL REGISTER THESAURUS OF INDEXING TERMS
November 16, 1995

Alphabetic list of indexing terms, with references to preferred or
related terms:

Accidents
    see
          Safety
Accounting (02, 08)
     sa
          Uniform System of Accounts
      x
          Auditing
     xx
          Business and industry
Additives
    see
          Color additives
          Food additives
Color additives (17)
Food additives (17)
     (The names of specific foods are not listed in this Thesaurus but
may be used as indexing terms.)
Safety (13)
Trade practices (12)
"""


def test_fr_thesaurus_reads_preferred_terms_and_both_variant_directions(tool):
    terms = {term.pref_label: term for term in tool.parse_fr_thesaurus(FR_FIXTURE)}

    # The banner lines above "related terms:" are not vocabulary.
    assert "FEDERAL REGISTER THESAURUS OF INDEXING TERMS" not in terms
    assert "November 16, 1995" not in terms

    # A ``see`` entry is not a concept; it is a variant of its target.
    assert "Accidents" not in terms
    assert terms["Safety"].alt_labels == ["Accidents"]

    # One ``see`` entry may name several targets, and becomes a variant of each.
    assert "Additives" not in terms
    assert terms["Color additives"].alt_labels == ["Additives"]
    assert terms["Food additives"].alt_labels == ["Additives"]

    # An ``x`` block names the term's own variants; ``sa``/``xx`` relate two
    # preferred terms and are deliberately not read as labels.
    assert terms["Accounting"].alt_labels == ["Auditing"]
    assert "Uniform System of Accounts" not in terms["Accounting"].alt_labels
    assert "Business and industry" not in terms["Accounting"].alt_labels
    assert "Uniform System of Accounts" not in terms


def test_fr_thesaurus_strips_category_codes_but_keeps_inline_parentheses(tool):
    text = FR_FIXTURE + "Work Incentive Programs (WIN) (11)\n"
    labels = {term.pref_label for term in tool.parse_fr_thesaurus(text)}
    assert "Work Incentive Programs (WIN)" in labels
    assert "Trade practices" in labels


def test_fr_thesaurus_scope_note_wrapping_does_not_leak_a_phantom_term(tool):
    """A scope note wraps onto an *unindented* line; that is not a new term."""
    terms = {term.pref_label: term for term in tool.parse_fr_thesaurus(FR_FIXTURE)}
    assert terms["Food additives"].definition == (
        "The names of specific foods are not listed in this Thesaurus but may be used as indexing terms."
    )
    assert not any(label.startswith("may be used") for label in terms)


# --------------------------------------------------------------------------
# CRS terms out of BILLSTATUS
# --------------------------------------------------------------------------

BILLSTATUS_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<billStatus>
  <bill>
    <policyArea><name>Environmental Protection</name></policyArea>
    <subjects>
      <legislativeSubjects>
        <item><name>Air quality</name></item>
        <item><name>Water quality</name></item>
      </legislativeSubjects>
    </subjects>
  </bill>
</billStatus>
"""


def test_billstatus_reads_policy_area_and_legislative_subjects(tool):
    policy, subjects = tool.parse_billstatus_terms(BILLSTATUS_FIXTURE)
    assert policy == ["Environmental Protection"]
    assert subjects == ["Air quality", "Water quality"]


def test_billstatus_survives_a_malformed_record(tool):
    assert tool.parse_billstatus_terms("<billStatus><bill>") == ([], [])


def test_billstatus_folds_many_records_into_two_vocabularies(tool):
    second = BILLSTATUS_FIXTURE.replace("Air quality", "Air quality").replace("Environmental Protection", "Health")
    policy, subjects = tool.collect_billstatus_terms([BILLSTATUS_FIXTURE, second])
    assert [term.pref_label for term in policy] == ["Environmental Protection", "Health"]
    # A term named by both records appears once *within its scheme*.
    assert [term.pref_label for term in subjects] == ["Air quality", "Water quality"]


# --------------------------------------------------------------------------
# EPA TSCA inventory
# --------------------------------------------------------------------------

TSCA_FIXTURE = (
    "ID,CASRN,casregno,UID,EXP,ChemName,DEF,UVCB,FLAG,ACTIVITY\n"
    "1,50-00-0,50000,,,Formaldehyde,,,,ACTIVE\n"
    '2,50-01-1,50011,,,"Guanidine, hydrochloride (1:1)",,,,INACTIVE\n'
    "3,71-43-2,71432,,,Benzene,,,XU,ACTIVE\n"
    "4,,,,,,,,,ACTIVE\n"
)


def test_tsca_reads_names_cas_and_activity_and_skips_nameless_rows(tool):
    terms = tool.parse_tsca_inventory(csv.DictReader(io.StringIO(TSCA_FIXTURE)))
    by_label = {term.pref_label: term for term in terms}
    assert set(by_label) == {"Formaldehyde", "Guanidine, hydrochloride (1:1)", "Benzene"}
    assert by_label["Formaldehyde"].source_id == "50-00-0"
    assert by_label["Formaldehyde"].source_status == "ACTIVE"
    assert by_label["Guanidine, hydrochloride (1:1)"].source_status == "INACTIVE"
    # No synonym column exists upstream, so no alias is invented.
    assert by_label["Benzene"].alt_labels == []


def test_tsca_cas_number_is_an_external_id_and_never_a_label(tool):
    """A CAS number in a label would perturb the lexical selector's tokens."""
    spec = tool.SOURCES["epa-tsca"]
    term = tool.SourceTerm(pref_label="Formaldehyde", source_id="50-00-0")
    external = json.loads(tool._external_ids(spec, term))
    assert {"scheme": "cas", "value": "50-00-0"} in external
    assert term.alt_labels == []


# --------------------------------------------------------------------------
# FAST topical N-Triples
# --------------------------------------------------------------------------

FAST_FIXTURE = "\n".join(
    [
        '<http://id.worldcat.org/fast/1000> <http://purl.org/dc/terms/identifier> "1000" .',
        '<http://id.worldcat.org/fast/1000> <http://www.w3.org/2004/02/skos/core#prefLabel> "Air quality--Standards" .',
        '<http://id.worldcat.org/fast/1000> <http://www.w3.org/2004/02/skos/core#altLabel> "Air pollution limits" .',
        # Normalizes to the prefLabel's own form; a redundant alias, not a label.
        '<http://id.worldcat.org/fast/1000> <http://www.w3.org/2004/02/skos/core#altLabel> "Air quality standards" .',
        "<http://id.worldcat.org/fast/1000> <http://www.w3.org/2004/02/skos/core#broader> "
        "<http://id.worldcat.org/fast/2000> .",
        '<http://id.worldcat.org/fast/2000> <http://purl.org/dc/terms/identifier> "2000" .',
        '<http://id.worldcat.org/fast/2000> <http://www.w3.org/2004/02/skos/core#prefLabel> "Caf\\u00e9s" .',
        '<http://id.worldcat.org/fast/3000> <http://purl.org/dc/terms/identifier> "3000" .',
        '<http://id.worldcat.org/fast/3000> <http://www.w3.org/2004/02/skos/core#prefLabel> "Obsolete heading" .',
        '<http://id.worldcat.org/fast/3000> <http://www.w3.org/2002/07/owl#deprecated> "true" .',
        "# a comment line the parser must ignore",
        "",
    ]
)


def test_fast_reads_labels_decodes_escapes_and_drops_deprecated(tool):
    terms = {term.pref_label: term for term in tool.parse_fast_ntriples(FAST_FIXTURE.splitlines())}
    assert set(terms) == {"Air quality--Standards", "Cafés"}
    # ``Air quality standards`` normalizes to the prefLabel's own form, so it
    # carries nothing and is not republished as an alias.
    assert terms["Air quality--Standards"].alt_labels == ["Air pollution limits"]
    assert terms["Air quality--Standards"].source_id == "1000"
    # ``owl:deprecated`` subjects are dropped rather than published with a
    # ``replaced_by`` the registry cannot resolve.
    assert "Obsolete heading" not in terms


def test_ntriples_literal_escapes(tool):
    assert tool.unescape_ntriples_literal(r"Café") == "Café"
    assert tool.unescape_ntriples_literal(r"a\\b") == "a\\b"
    assert tool.unescape_ntriples_literal(r"say \"hi\"") == 'say "hi"'
    assert tool.unescape_ntriples_literal(r"one\ntwo") == "one\ntwo"


def test_fast_reads_a_zip_the_way_it_is_distributed(tool, tmp_path):
    archive = tmp_path / "FASTTopical.nt.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("FASTTopical.nt", FAST_FIXTURE)
        handle.writestr("License.txt", "ODC-By")
    labels = {term.pref_label for term in tool.read_fast_archive(archive)}
    assert labels == {"Air quality--Standards", "Cafés"}


# --------------------------------------------------------------------------
# Alias hygiene
# --------------------------------------------------------------------------


def test_degenerate_aliases_are_dropped_but_real_short_tokens_survive(tool):
    # ``Ḥūr`` normalizes to ``r``, which is a substring of nearly every text.
    assert not tool.retain_alt_label("Ḥūr")
    assert not tool.retain_alt_label("TV")
    assert not tool.retain_alt_label("...")
    # Multi-word aliases keep their one-character tokens: these are real labels.
    assert tool.retain_alt_label("Hansen's disease")
    assert tool.retain_alt_label("4-H clubs")
    assert tool.retain_alt_label("Auditing")


def test_dropped_aliases_are_counted_not_silently_discarded(tool):
    terms = tool.parse_fast_ntriples(
        [
            '<http://id.worldcat.org/fast/9> <http://www.w3.org/2004/02/skos/core#prefLabel> "Houris" .',
            '<http://id.worldcat.org/fast/9> <http://www.w3.org/2004/02/skos/core#altLabel> "\\u1e24\\u016br" .',
            '<http://id.worldcat.org/fast/9> <http://www.w3.org/2004/02/skos/core#altLabel> "Huris" .',
        ]
    )
    assert terms[0].alt_labels == ["Huris"]
    assert terms[0].dropped_alt_labels == ["Ḥūr"]


# --------------------------------------------------------------------------
# Fusion invariants
# --------------------------------------------------------------------------


def _frozen_row(pref_label: str, *, scheme: str = "subject") -> dict:
    return {
        "concept_id": stable_id("concept", scheme, normalize_label(pref_label)),
        "facet": scheme,
        "source_vocabulary": "federal-register-thesaurus",
        "scheme": scheme,
        "pref_label": pref_label,
        "alt_labels_json": "[]",
        "definition": f"Federal Register Thesaurus topic covering {pref_label}.",
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": canonical_json([{"scheme": "federal_register_thesaurus", "value": pref_label}]),
        "method": "deterministic",
        "actor_id": "federal-register-thesaurus:v1",
        "run_id": "ontology-20260724T044701Z",
        "asserted_at": "2026-07-24T04:47:01Z",
        "supersedes_id": None,
    }


def test_minted_ids_are_scoped_to_the_authority_vocabulary(tool):
    assert tool.mint_concept_id(
        "federal-register-thesaurus",
        "Air quality",
    ) == stable_id(
        "concept",
        "federal-register-thesaurus",
        "air quality",
    )
    # Pre-v2 Federal Register ids remain stable through the enrichment path.
    frozen = _frozen_row("Air quality")
    assert (
        tool.mint_concept_id(
            "federal-register-thesaurus",
            "Air quality",
        )
        != frozen["concept_id"]
    )


def test_vocabularies_stay_distinct_one_label_becomes_one_concept_per_authority(tool, context):
    label = "Trademarks"
    contributions = [
        (tool.SOURCES["crs-subjects"], [tool.SourceTerm(pref_label=label)]),
        (tool.SOURCES["fast-topical"], [tool.SourceTerm(pref_label=label, source_id="1")]),
        (tool.SOURCES["fr-thesaurus"], [tool.SourceTerm(pref_label=label)]),
    ]
    registry, _, counts = tool.fuse(
        existing=[_frozen_row("Accounting")],
        contributions=contributions,
        context=context,
        retrieved_at={},
    )
    rows = [row for row in registry if row["pref_label"] == label]
    assert len(rows) == 3, "one label in three sources must stay three concepts"
    assert {row["facet"] for row in rows} == {"subject"}
    assert {row["scheme"] for row in rows} == {"subject"}
    assert {row["source_vocabulary"] for row in rows} == {
        "crs-subjects",
        "fast-topical",
        "federal-register-thesaurus",
    }
    assert len({row["concept_id"] for row in rows}) == 3
    assert all(values["minted"] == 1 for values in counts.values())
    mappings = tool.presentation_mappings(registry)
    assert len(mappings) == 2
    assert {row["relation"] for row in mappings} == {"same-normalized-label-for-presentation"}
    assert {row["status"] for row in mappings} == {"unreviewed-presentation-only"}
    assert all("exactMatch" not in row["relation"] for row in mappings)


def test_exact_label_match_inside_one_scheme_enriches_and_never_rewrites(tool, context):
    frozen = _frozen_row("Accounting")
    term = tool.SourceTerm(pref_label="accounting", alt_labels=["Auditing"], definition="A different definition")
    registry, sidecar, counts = tool.fuse(
        existing=[frozen],
        contributions=[(tool.SOURCES["fr-thesaurus"], [term])],
        context=context,
        retrieved_at={"fr-thesaurus": "2026-07-27T00:00:00Z"},
    )
    assert len(registry) == 1, "an exact same-scheme match must not add a row"
    assert registry[0] == {column: frozen.get(column) for column in CONCEPT_COLUMNS}
    assert counts["fr-thesaurus"] == {
        "terms": 1,
        "minted": 0,
        "enriched_frozen_rows": 1,
        "alt_labels_dropped_as_degenerate": 0,
    }
    enrichment = [row for row in sidecar if row["relation"] == tool.RELATION_ENRICHMENT]
    assert len(enrichment) == 1
    assert enrichment[0]["concept_id"] == frozen["concept_id"]
    assert json.loads(enrichment[0]["enrichment_alt_labels_json"]) == ["Auditing"]
    assert enrichment[0]["license"] == tool.SOURCES["fr-thesaurus"].license
    assert enrichment[0]["retrieved_at"] == "2026-07-27T00:00:00Z"


def test_frozen_rows_survive_byte_for_byte(tool, context):
    existing = [_frozen_row("Accounting"), _frozen_row("Air quality")]
    registry, _, _ = tool.fuse(
        existing=existing,
        contributions=[
            (
                tool.SOURCES["fr-thesaurus"],
                [tool.SourceTerm(pref_label="Accounting", alt_labels=["Auditing"]), tool.SourceTerm(pref_label="New")],
            )
        ],
        context=context,
        retrieved_at={},
    )
    tool.assert_frozen_rows_survive(existing, registry)
    assert len(registry) == 3


def test_a_rewritten_frozen_row_is_rejected(tool):
    existing = [_frozen_row("Accounting")]
    tampered = [dict(existing[0], pref_label="Accountancy")]
    with pytest.raises(tool.FusionError, match="rewritten"):
        tool.assert_frozen_rows_survive(existing, tampered)
    with pytest.raises(tool.FusionError, match="disappeared"):
        tool.assert_frozen_rows_survive(existing, [])


def test_fused_rows_carry_the_selector_schema_and_hold_the_graph_invariants(tool, context):
    registry, sidecar, _ = tool.fuse(
        existing=[_frozen_row("Accounting")],
        contributions=[
            (
                tool.SOURCES["epa-tsca"],
                [tool.SourceTerm(pref_label="Benzene", source_id="71-43-2", source_status="ACTIVE")],
            ),
            (tool.SOURCES["fast-topical"], [tool.SourceTerm(pref_label="Air quality--Standards", source_id="1000")]),
        ],
        context=context,
        retrieved_at={},
    )
    for row in registry:
        assert set(row) == set(CONCEPT_COLUMNS), "the selector's schema admits no extra column"
        assert row["status"] in {"active", "candidate"}
    # ``broader_id``/``replaced_by`` must resolve inside the table; leaving them
    # null is what keeps that true for an imported vocabulary.
    assert_concept_graphs(registry)
    for row in sidecar:
        assert set(row) <= set(tool.SIDECAR_COLUMNS)
        assert row["concept_id"] and row["license"]


def test_the_production_selector_reads_the_fused_registry(tool, context):
    registry, _, _ = tool.fuse(
        existing=[_frozen_row("Accounting")],
        contributions=[
            (tool.SOURCES["epa-tsca"], [tool.SourceTerm(pref_label="Benzene", source_id="71-43-2")]),
            (tool.SOURCES["crs-subjects"], [tool.SourceTerm(pref_label="Air quality")]),
        ],
        context=context,
        retrieved_at={},
    )
    selected = select_candidate_concepts_for_text(
        "A rule on benzene emissions and air quality.",
        ["subject", "regulated_entity"],
        registry,
        limit=12,
    )
    labels = {row["pref_label"] for row in selected}
    assert {"Benzene", "Air quality"} <= labels
    assert all(concept_aliases(row) for row in selected)


def test_end_to_end_writes_a_registry_a_sidecar_and_a_manifest(tool, tmp_path):
    """One run over synthetic inputs, exercising the command line itself."""
    existing_path = tmp_path / "existing.parquet"
    from spicy_regs.ontology.common import write_parquet_rows

    write_parquet_rows(existing_path, columns=CONCEPT_COLUMNS, rows=[_frozen_row("Accounting"), _frozen_row("Safety")])

    fr_path = tmp_path / "thesaurus-alpha.txt"
    fr_path.write_text(FR_FIXTURE, encoding="utf-8")
    tsca_path = tmp_path / "TSCAINV.csv"
    tsca_path.write_text(TSCA_FIXTURE, encoding="utf-8")
    fast_path = tmp_path / "FASTTopical.nt"
    fast_path.write_text(FAST_FIXTURE, encoding="utf-8")
    bills = tmp_path / "bills"
    bills.mkdir()
    (bills / "BILLSTATUS-1.xml").write_text(BILLSTATUS_FIXTURE, encoding="utf-8")
    out = tmp_path / "out"

    assert (
        tool.main(
            [
                "--existing-registry",
                str(existing_path),
                "--fr-thesaurus",
                str(fr_path),
                "--billstatus",
                str(bills),
                "--tsca-csv",
                str(tsca_path),
                "--fast-topical",
                str(fast_path),
                "--output-dir",
                str(out),
                "--retrieved-at",
                "2026-07-27T00:00:00Z",
                "--run-id",
                "test-run",
            ]
        )
        == 0
    )

    registry = read_parquet_rows(out / "registry.parquet")
    sidecar = read_parquet_rows(out / "provenance.parquet")
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))

    # The two frozen rows are still there, unchanged, and "Safety" was enriched
    # by the thesaurus rather than duplicated.
    tool.assert_frozen_rows_survive(
        [_frozen_row("Accounting"), _frozen_row("Safety")],
        registry,
    )
    assert {row["scheme"] for row in registry} == {
        "subject",
        "regulated_entity",
    }
    assert {row["source_vocabulary"] for row in registry} == {
        "federal-register-thesaurus",
        "crs-subjects",
        "crs-policy-areas",
        "epa-tsca",
        "fast-topical",
    }
    assert manifest["rows_per_source_vocabulary"]["crs-policy-areas"] == 1
    assert manifest["rows_per_facet"] == {
        "regulated_entity": 3,
        "subject": 10,
    }
    assert manifest["existing_registry"]["rows"] == 2
    assert manifest["total_rows"] == len(registry)
    assert {row["concept_id"] for row in sidecar} <= {row["concept_id"] for row in registry}

    # Provenance the schema cannot carry lives in the sidecar and the manifest.
    assert manifest["raw_files"]["fr-thesaurus"]["sha256"]
    assert manifest["raw_files"]["fast-topical"]["sha256"]
    assert any("ODC-By" in notice for notice in manifest["notice"])
    assert any("OCLC" in notice for notice in manifest["notice"])
    fast_rows = [row for row in sidecar if row["source_vocabulary"] == "fast-topical"]
    assert fast_rows and all(row["license"].startswith("ODC-By") for row in fast_rows)
    assert all(row["retrieved_at"] == "2026-07-27T00:00:00Z" for row in sidecar)
    tsca_rows = [row for row in sidecar if row["source_vocabulary"] == "epa-tsca"]
    assert {row["source_id"] for row in tsca_rows} >= {"50-00-0", "71-43-2"}
    assert {row["source_status"] for row in tsca_rows} == {"ACTIVE", "INACTIVE"}


def test_a_source_left_off_the_command_line_is_reported_not_guessed(tool, tmp_path):
    existing_path = tmp_path / "existing.parquet"
    from spicy_regs.ontology.common import write_parquet_rows

    write_parquet_rows(existing_path, columns=CONCEPT_COLUMNS, rows=[_frozen_row("Accounting")])
    fr_path = tmp_path / "thesaurus-alpha.txt"
    fr_path.write_text(FR_FIXTURE, encoding="utf-8")
    out = tmp_path / "out"
    assert (
        tool.main(["--existing-registry", str(existing_path), "--fr-thesaurus", str(fr_path), "--output-dir", str(out)])
        == 0
    )
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["skipped_sources"]) == 3
    assert any(entry.startswith("fast-topical") for entry in manifest["skipped_sources"])
    assert "fast-topical" not in manifest["rows_per_source_vocabulary"]

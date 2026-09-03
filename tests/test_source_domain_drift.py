"""The documented-versus-observed enumeration gate.

Three kinds of test, in the shape the data-dictionary check already uses: the
gate passes on the committed inputs, the gate *fires* when an input is broken,
and the pinned publisher facts are what the pinned publisher bytes actually say.

The last kind is the point of checking the captures in rather than transcribing
them. Every documented value below is re-derived from
``sample-data/source-domains/`` on each run, so these assertions are a statement
about the publisher's own document, not about a list somebody typed.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from spicy_regs.data_dictionary import DEFAULT_R2_BASE_URL, expected_schemas
from spicy_regs.sources.source_domains import (
    ACCEPTED_DOMAIN_FINDINGS,
    DEFAULT_SOURCE_DOMAIN_DIR,
    DOMAIN_KEYS,
    FINDING_KINDS,
    OBSERVED_SNAPSHOT_FILENAME,
    UNDOCUMENTED,
    SourceDomainError,
    documented_domains,
    domain_findings,
    load_capture_manifest,
    load_observed_snapshot,
    openapi_schema_enum,
    read_capture,
    stale_accepted_findings,
    unrecorded_findings,
    xsd_documented_options,
)

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_DIR = ROOT / DEFAULT_SOURCE_DOMAIN_DIR
TOOL = ROOT / "scripts" / "check_source_domain_drift.py"


@pytest.fixture(scope="module")
def documented():
    return documented_domains(DOMAIN_DIR)


@pytest.fixture(scope="module")
def snapshot():
    return load_observed_snapshot(DOMAIN_DIR)


@pytest.fixture(scope="module")
def findings(documented, snapshot):
    return domain_findings(documented, snapshot.domains)


# --- the gate itself --------------------------------------------------------


def test_every_finding_is_recorded(findings):
    """The gate's pass condition: nothing drifted that the ledger does not account for."""

    unrecorded = unrecorded_findings(findings)
    assert unrecorded == (), [finding.identifier for finding in unrecorded]


def test_the_ledger_holds_nothing_stale(findings):
    """The other half: an accepted finding the data stopped producing must be deleted, not kept.

    With the test above, this is set equality in both directions — ledger ⊇ findings
    and ledger ⊆ findings — so no third assertion of that is needed.
    """

    assert stale_accepted_findings(findings) == ()


def test_every_accepted_finding_states_a_reason():
    for entry in ACCEPTED_DOMAIN_FINDINGS:
        assert entry.kind in FINDING_KINDS
        assert entry.domain_key in DOMAIN_KEYS
        assert len(entry.reason) > 60, entry


def test_the_findings_are_the_seven_this_snapshot_carries(findings):
    """Pinned so a silent change in either half shows up as a number, not a shrug.

    This list is deliberately a second transcription of the ledger. The set-equality
    tests above compare the ledger against the findings, so both pass if someone
    edits the ledger and the snapshot to agree on something wrong; this one does not,
    because it states the seven independently of both. Kept for that reason.
    """

    identifiers = sorted(finding.identifier for finding in findings)
    assert identifiers == [
        "regulations-gov-document-type/undocumented-value/Public Submission",
        "unified-agenda-priority-category/unobserved-value/Not Major",
        "unified-agenda-rin-status/undocumented-value/First Time Published in The Unified Agenda",
        "unified-agenda-rin-status/undocumented-value/Previously Published in The Unified Agenda",
        "unified-agenda-rin-status/unobserved-value/First time published in the Unified Agenda",
        "unified-agenda-rin-status/unobserved-value/Previously published in the Unified Agenda",
        "unified-agenda-rule-stage/unobserved-value/No Stage",
    ]


def test_public_submission_carries_its_row_support(findings):
    """The finding that matters most is a count, not an adjective."""

    (finding,) = [one for one in findings if one.value == "Public Submission"]
    assert finding.kind == UNDOCUMENTED
    assert finding.row_count == 373


def test_two_domains_agree_completely(findings):
    """Not every column drifts, and a gate that only ever finds drift is not a gate."""

    drifted = {finding.domain_key for finding in findings}
    assert "regulations-gov-docket-type" not in drifted
    assert "unified-agenda-major" not in drifted


# --- the gate fires ---------------------------------------------------------


def test_an_undocumented_value_that_is_not_recorded_fails(documented, snapshot):
    """Flip one observed value to something nobody documented: the gate must catch it."""

    domain = snapshot.domains["unified-agenda-major"]
    mutated = dict(snapshot.domains)
    mutated["unified-agenda-major"] = replace(domain, value_counts=(("Undecided", 865), ("Yes", 309), ("No", 2780)))
    unrecorded = unrecorded_findings(domain_findings(documented, mutated))
    assert [finding.identifier for finding in unrecorded] == [
        "unified-agenda-major/undocumented-value/Undecided",
        "unified-agenda-major/unobserved-value/Undetermined",
    ]


def test_a_documented_value_falling_out_of_the_data_fails(documented, snapshot):
    """Drop a value the publisher documents and we observe: that is news, not silence."""

    domain = snapshot.domains["regulations-gov-docket-type"]
    mutated = dict(snapshot.domains)
    mutated["regulations-gov-docket-type"] = replace(domain, value_counts=(("Nonrulemaking", 213858),))
    unrecorded = unrecorded_findings(domain_findings(documented, mutated))
    assert [finding.identifier for finding in unrecorded] == ["regulations-gov-docket-type/unobserved-value/Rulemaking"]


def test_a_ledger_entry_the_data_stopped_producing_fails(documented, snapshot):
    """Record the reverse: 'Public Submission' disappearing must not pass quietly."""

    domain = snapshot.domains["regulations-gov-document-type"]
    kept = tuple(one for one in domain.value_counts if one[0] != "Public Submission")
    mutated = dict(snapshot.domains)
    mutated["regulations-gov-document-type"] = replace(domain, value_counts=kept)
    stale = stale_accepted_findings(domain_findings(documented, mutated))
    assert [entry.value for entry in stale] == ["Public Submission"]


def test_a_domain_observed_on_the_wrong_column_fails(documented, snapshot):
    domain = snapshot.domains["unified-agenda-major"]
    mutated = dict(snapshot.domains)
    mutated["unified-agenda-major"] = replace(domain, column="rule_stage")
    with pytest.raises(SourceDomainError, match="observed on unified_agenda.rule_stage"):
        domain_findings(documented, mutated)


def test_an_unobserved_domain_fails(documented, snapshot):
    mutated = {key: value for key, value in snapshot.domains.items() if key != "unified-agenda-major"}
    with pytest.raises(SourceDomainError, match="domain coverage differs"):
        domain_findings(documented, mutated)


# --- the pinned publisher bytes ---------------------------------------------


def test_captures_verify_against_their_pins():
    captures = load_capture_manifest(DOMAIN_DIR)
    assert sorted(captures) == ["reginfo-rin-data-xsd", "regulations-gov-openapi-v4"]
    for capture in captures.values():
        assert len(read_capture(capture, root=DOMAIN_DIR)) == capture.byte_length
        assert capture.source_url.startswith("https://")


def test_a_mutated_capture_is_refused(tmp_path):
    shutil.copytree(DOMAIN_DIR, tmp_path / "domains")
    target = tmp_path / "domains" / "reginfo-rin-data-ver10262011.xsd"
    target.write_bytes(target.read_bytes().replace(b"No Stage", b"No stage"))
    with pytest.raises(SourceDomainError, match="hashes to"):
        documented_domains(tmp_path / "domains")


def test_regulations_gov_document_type_is_the_five_documented_values(documented):
    domain = documented["regulations-gov-document-type"]
    assert domain.values == ("Notice", "Rule", "Proposed Rule", "Supporting & Related Material", "Other")
    assert domain.locator.endswith("components.schemas.DocumentType.enum (lines 893-898)")
    assert "Public Submission" not in domain.values


def test_the_docket_type_enums_trailing_space_is_not_part_of_the_value(documented):
    """The capture writes ``- Nonrulemaking `` with a trailing space; a YAML scalar does not carry it."""

    assert documented["regulations-gov-docket-type"].values == ("Rulemaking", "Nonrulemaking")
    raw = (DOMAIN_DIR / "regulations-gov-openapi-v4-2026-08-03.yaml").read_text(encoding="utf-8")
    assert "        - Nonrulemaking \n" in raw


def test_unified_agenda_domains_come_from_documentation_prose_not_enumerations(documented):
    xsd = (DOMAIN_DIR / "reginfo-rin-data-ver10262011.xsd").read_bytes()
    assert b"xs:enumeration" not in xsd
    assert documented["unified-agenda-rule-stage"].values == (
        "Prerule Stage",
        "Proposed Rule Stage",
        "Final Rule Stage",
        "Long-Term Actions",
        "Completed Actions",
        "No Stage",
    )
    assert documented["unified-agenda-rin-status"].values == (
        "First time published in the Unified Agenda",
        "Previously published in the Unified Agenda",
    )
    assert documented["unified-agenda-major"].values == ("Yes", "No", "Undetermined")


def test_the_priority_category_sentence_lists_not_major_twice(documented):
    """The publisher's own duplicate is folded to one value and the raw count is kept."""

    domain = documented["unified-agenda-priority-category"]
    assert domain.values == (
        "Economically Significant",
        "Other Significant",
        "Substantive, Nonsignificant",
        "Routine and Frequent",
        "Info./Admin./Other",
        "Not Major",
    )
    assert (len(domain.values), domain.raw_option_count) == (6, 7)


def test_a_documented_list_that_grows_is_refused(tmp_path):
    """Counts are pinned per domain, so a publisher adding a value cannot land unnoticed."""

    shutil.copytree(DOMAIN_DIR, tmp_path / "domains")
    target = tmp_path / "domains" / "regulations-gov-openapi-v4-2026-08-03.yaml"
    target.write_bytes(
        target.read_bytes().replace(b"        - Other\n", b"        - Other\n        - Public Submission\n")
    )
    manifest_path = tmp_path / "domains" / "documented-enumeration-capture-manifest-v1.json"
    _repin(manifest_path, "regulations-gov-openapi-v4", target)
    with pytest.raises(SourceDomainError, match="parses 6 values from 6 options, pinned at 5"):
        documented_domains(tmp_path / "domains")


def test_an_openapi_schema_whose_enum_is_not_a_list_is_refused():
    payload = b"components:\n  schemas:\n    DocumentType:\n      type: string\n      enum:\n        bogus: 1\n"
    with pytest.raises(SourceDomainError, match="states no enum member list"):
        openapi_schema_enum(payload, "DocumentType")


def test_an_openapi_schema_that_is_absent_is_refused():
    payload = b"components:\n  schemas:\n    DocketType:\n      enum:\n        - Rulemaking\n"
    with pytest.raises(SourceDomainError, match="declares no components.schemas.DocumentType"):
        openapi_schema_enum(payload, "DocumentType")


def test_an_openapi_capture_that_is_not_yaml_is_refused():
    with pytest.raises(SourceDomainError, match="not well-formed YAML"):
        openapi_schema_enum(b"components:\n  schemas:\n   - [unclosed\n", "DocumentType")


def test_an_xsd_documentation_string_without_an_option_list_is_refused():
    payload = (
        b'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
        b'<xs:element name="MAJOR"><xs:annotation><xs:documentation>Free text.</xs:documentation>'
        b"</xs:annotation></xs:element></xs:schema>"
    )
    with pytest.raises(SourceDomainError, match="does not state an option list"):
        xsd_documented_options(payload, "MAJOR")


def test_an_xsd_with_a_doctype_is_refused():
    payload = b'<!DOCTYPE schema [<!ENTITY x "y">]>\n<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"/>'
    with pytest.raises(SourceDomainError, match="DOCTYPE"):
        xsd_documented_options(payload, "MAJOR")


# --- the observed snapshot's own provenance ---------------------------------


def test_every_domain_names_a_real_published_column(documented):
    schemas = expected_schemas()
    for domain in documented.values():
        columns = {column for column, _ in schemas[domain.table]}
        assert domain.column in columns, f"{domain.table}.{domain.column} is not a published column"


def test_the_snapshot_states_where_its_rows_came_from(snapshot):
    assert snapshot.observed_at == "2026-08-03T22:35:00Z"
    assert snapshot.producer_revision == "f1fcb8c9c8838071e9c45462799db788971baca4"
    sources = {str(one["table"]): one for one in snapshot.sources}
    assert sorted(sources) == ["dockets", "documents", "unified_agenda"]
    for table, source in sources.items():
        # The recorded URL is a string in a checked-in data file; the expected one
        # comes from the registry that publishes the corpus. Two different origins,
        # so this fails if the corpus moves and the snapshot is not re-pinned —
        # which is exactly how this file first shipped naming a host we never served.
        assert source["publisher_url"] == f"{DEFAULT_R2_BASE_URL}/{table}.parquet"
        assert str(source["bytes_digest"]).startswith("sha256:")
        assert int(source["byte_length"]) > 0
    assert sources["documents"]["row_count"] == 1_990_136
    assert sources["unified_agenda"]["row_count"] == 3_954


def test_the_snapshot_row_counts_add_up(snapshot):
    """Every row of a domain's table is either a value or a null; nothing is dropped."""

    for domain in snapshot.domains.values():
        assert sum(count for _, count in domain.value_counts) + domain.null_count == domain.row_count


def test_priority_category_is_the_one_nullable_domain(snapshot):
    """The XSD declares PRIORITY_CATEGORY minOccurs="0", so a null is documented, not drift."""

    nullable = {key for key, domain in snapshot.domains.items() if domain.null_count}
    assert nullable == {"unified-agenda-priority-category"}
    assert snapshot.domains["unified-agenda-priority-category"].null_count == 2


# --- the tool ---------------------------------------------------------------


def test_the_tool_passes_on_the_committed_inputs():
    result = subprocess.run([sys.executable, str(TOOL)], capture_output=True, text=True, cwd=ROOT)
    assert result.returncode == 0, result.stderr
    assert "6 documented domains" in result.stdout
    assert "7 findings" in result.stdout
    assert "UNRECORDED" not in result.stdout


def test_the_tool_would_record_the_urls_the_snapshot_already_carries(snapshot):
    """Close the loop on the writer: what ``--observe`` would record must equal what is pinned.

    ``observe()`` needs parquet, so it cannot run here — but the one part of it that
    shipped wrong was the URL it stamps into the snapshot. That function is pure, so
    run it against every table the snapshot names and require it to reproduce them.
    """

    spec = importlib.util.spec_from_file_location("check_source_domain_drift", TOOL)
    assert spec is not None and spec.loader is not None
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)

    for source in snapshot.sources:
        assert tool.published_table_url(str(source["table"])) == source["publisher_url"]
    assert tool.published_table_url("documents") == "https://data.spicy-regs.dev/documents.parquet"


def test_the_tool_refuses_to_write_a_snapshot_without_provenance():
    result = subprocess.run(
        [sys.executable, str(TOOL), "--observe", "--data-dir", ".", "--write-snapshot"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 2
    assert "a snapshot states its provenance" in result.stderr


def _repin(manifest_path: Path, key: str, capture_path: Path) -> None:
    """Re-pin one capture's digest and length so a mutation test reaches the parser."""

    import hashlib

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in payload["captures"]:
        if entry["key"] == key:
            body = capture_path.read_bytes()
            entry["bytes_digest"] = "sha256:" + hashlib.sha256(body).hexdigest()
            entry["byte_length"] = len(body)
    manifest_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_the_snapshot_file_is_small_enough_to_read(snapshot):
    """A domain snapshot is a summary, not a copy of the table it summarizes."""

    assert (DOMAIN_DIR / OBSERVED_SNAPSHOT_FILENAME).stat().st_size < 16_384

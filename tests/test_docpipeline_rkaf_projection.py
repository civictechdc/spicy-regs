"""Contracts for the document -> RKAF projection.

The projection makes one promise a consumer will check with a hash: every
coordinate in the emitted document slices the stored source text it names. These
tests are the proof that the promise is enforced rather than asserted — that a
drifted offset aborts, an unresolvable quote becomes a rejection row, a
model-invented concept id never becomes a node, and that the deterministic layer
is complete and reproducible with zero provider calls.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.docpipeline.adapters import StructuredTextResult
from spicy_regs.docpipeline.rkaf_projection import (
    ASSIGNMENT_ROLE_IRIS,
    FRAGMENT_URN_PATTERN,
    MODEL_ATTESTATION_DECISION,
    OffsetVerificationError,
    ProjectionError,
    ProjectionSettings,
    PublishedTables,
    encode_for_uri,
    fragment_urn,
    ground_literal,
    load_artifact,
    project_document,
    verify_candidate_rows,
    verify_fragment,
)
from spicy_regs.docpipeline.source import (
    SourceRecord,
    build_source_artifact,
    profile_for_table,
)
from spicy_regs.ontology.attestations import DECISION_APPROVED, DECISIONS

# --------------------------------------------------------------------------- #
# A tiny synthetic corpus. Two profiles, two rows, no network, no real data.
# --------------------------------------------------------------------------- #

FR_BODY = (
    "<html><body>"
    "<ul><li>9 CFR Part 381</li><li>[Docket No. TEST-2026-0001]</li></ul>"
    "<p>This proposed rule concerns poultry slaughter inspection at establishments.</p>"
    "<p>Authority: 7 U.S.C. 450 governs the program.</p>"
    "</body></html>"
)

BILL_XML = "<bill><title>A bill concerning water quality permits.</title></bill>"


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = sorted({key for row in rows for key in row})
    table = pa.table({column: [row.get(column) for row in rows] for column in columns})
    pq.write_table(table, path)


@pytest.fixture
def corpus(tmp_path: Path) -> Path:
    directory = tmp_path / "corpus"
    _write(
        directory / "federal_register.parquet",
        [
            {
                "document_number": "2026-00001",
                "title": "Poultry slaughter inspection",
                "abstract": "A proposed rule.",
                "document_type": "Proposed Rule",
                "agency_slugs": json.dumps(["food-safety-and-inspection-service"]),
                "body_html": FR_BODY,
                "docket_ids_json": json.dumps(["TEST-2026-0001"]),
                "topics_json": json.dumps(["Poultry and poultry products"]),
            }
        ],
    )
    _write(
        directory / "congress_bills.parquet",
        [
            {
                "bill_id": "118-hr-1",
                "title": "Water Quality Act",
                "latest_action_text": "Referred to committee.",
                "origin_chamber": "House",
                "xml_text": BILL_XML,
            }
        ],
    )
    return directory


@pytest.fixture
def tables(tmp_path: Path) -> Path:
    directory = tmp_path / "tables"
    _write(
        directory / "proceedings.parquet",
        [
            {
                "proceeding_id": "proceeding_test",
                "rin": "0583-AE99",
                "docket_ids_json": json.dumps(["TEST-2026-0001"]),
                "fr_document_numbers_json": json.dumps(["2026-00001"]),
                "cfr_target_iris_json": json.dumps(["urn:rkaf:us:cfr:9:381"]),
                "current_stage": "proposed",
                "actor_id": "spicy-regs:proceedings:v1",
                "run_id": "test-run",
                "asserted_at": "2026-07-01T00:00:00Z",
            }
        ],
    )
    _write(
        directory / "rule_targets.parquet",
        [
            {
                "docket_id": "TEST-2026-0001",
                "cfr_ref": "9-381",
                "cfr_title": "9",
                "cfr_part": "381",
                "cfr_section": None,
                "rin": "0583-AE99",
                "actor_id": "spicy-regs:rule-targets:v1",
                "run_id": "test-run",
                "asserted_at": "2026-07-01T00:00:00Z",
            }
        ],
    )
    _write(
        directory / "authority_edges.parquet",
        [
            {
                "rin": "0583-AE99",
                "authority_raw": "7 U.S.C. 450",
                "usc_title": "7",
                "usc_section": "450",
                "pl_number": None,
                "authority_type": "usc",
                "parse_status": "partial",
                "agenda_edition": "202510",
                "actor_id": "spicy-regs:authority-parser:v1",
                "run_id": "test-run",
                "asserted_at": "2026-07-01T00:00:00Z",
            }
        ],
    )
    return directory


@pytest.fixture
def registry(tmp_path: Path) -> Path:
    path = tmp_path / "registry.parquet"
    _write(
        path,
        [
            {
                "concept_id": "concept_poultry",
                "scheme": "subject",
                "pref_label": "Poultry and poultry products",
                "alt_labels_json": json.dumps(["poultry"]),
                "definition": "Topic covering poultry and poultry products.",
                "status": "active",
            },
            {
                "concept_id": "concept_inspection",
                "scheme": "subject",
                "pref_label": "Meat inspection",
                "alt_labels_json": json.dumps(["slaughter inspection"]),
                "definition": "Topic covering meat and poultry inspection.",
                "status": "active",
            },
        ],
    )
    return path


def _settings(corpus: Path, tables: Path, registry: Path | None = None) -> ProjectionSettings:
    return ProjectionSettings(
        corpus_dir=corpus,
        tables_dir=tables,
        registry_path=registry,
        asserted_at="2026-07-28T00:00:00Z",
        prompt_concept_limit=4,
    )


def _fr_artifact(corpus: Path):
    return load_artifact("federal-register-document-v1", "2026-00001", corpus_dir=corpus)


# --------------------------------------------------------------------------- #
# A fake model. The projection never constructs a provider; it is handed one.
# --------------------------------------------------------------------------- #


class FakeModel:
    """A ``StructuredTextModel`` that replays a scripted tag response."""

    model_id = "fake:test-model"
    run_configuration = {"provider": "fake", "model_id": "fake:test-model"}

    def __init__(self, tags_for: Any) -> None:
        self._tags_for = tags_for
        self.calls: list[dict[str, Any]] = []

    def secret_free_request(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "name": kwargs["name"],
            "instructions_sha256": hashlib.sha256(kwargs["instructions"].encode("utf-8")).hexdigest(),
        }

    def structured_json(self, **kwargs: Any) -> StructuredTextResult:
        self.calls.append(dict(kwargs))
        tags = self._tags_for(kwargs["payload"]) if callable(self._tags_for) else list(self._tags_for)
        return StructuredTextResult(
            output={"tags": tags},
            call={
                "provider": "fake",
                "transport": "in-process",
                "model_id": self.model_id,
                "schema_name": kwargs["name"],
                "status": "completed",
                "duration_ms": 1,
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 2,
                "attempt_count": 1,
                "retry_count": 0,
                "attempts": [],
                "schema_validated_locally": True,
            },
        )


def _tag(
    payload: dict[str, Any],
    *,
    quote: str,
    concept_id: str | None = "concept_poultry",
    role: str = "primary",
    proposed_label: str | None = None,
    definition: str | None = None,
) -> dict[str, Any] | None:
    """Build one schema-valid tag naming the evidence field carrying ``quote``."""
    fields = payload["untrusted_evidence_fields"]["fields"]
    for key, text in fields.items():
        start = text.find(quote)
        if start < 0:
            continue
        return {
            "concept_id": concept_id,
            "proposed_label": proposed_label,
            "scheme": "subject",
            "role": role,
            "definition": definition,
            "confidence": 0.9,
            "evidence_text": quote,
            "evidence_field": key,
            "evidence_start": start,
            "evidence_end": start + len(quote),
            "justification": "The source text states it.",
            "external_ids": [],
        }
    return None


# --------------------------------------------------------------------------- #
# URN grammar and offset verification.
# --------------------------------------------------------------------------- #


def test_the_minted_urn_satisfies_the_core_4_2_grammar() -> None:
    urn = fragment_urn("https://www.federalregister.gov/d/2026-03227", 2282, 2307, "a" * 64)

    assert FRAGMENT_URN_PATTERN.match(urn)
    assert urn.startswith("urn:rkaf:fragment:https%3A%2F%2Fwww.federalregister.gov%2Fd%2F2026-03227:2282:2307:sha256-")
    assert encode_for_uri("a-b._~z") == "a-b._~z", "the RFC 3986 unreserved set is left alone"
    assert encode_for_uri("/") == "%2F", "hex triplets are uppercase, as ENCODE_FOR_URI produces"


@pytest.mark.parametrize(
    ("start", "end"),
    [(0, 10**9), (-1, 5), (10, 5)],
)
def test_offsets_outside_the_stored_field_abort(corpus: Path, start: int, end: int) -> None:
    artifact, _ = _fr_artifact(corpus)

    with pytest.raises(OffsetVerificationError):
        verify_fragment(
            artifact,
            key="f",
            source_field="federal_register.body_html",
            start=start,
            end=end,
            artifact_iri="https://example.test/d/1",
        )


def test_a_drifted_offset_aborts_instead_of_being_repaired(corpus: Path) -> None:
    artifact, _ = _fr_artifact(corpus)
    body = artifact.raw_fields["federal_register.body_html"]
    start = body.index("9 CFR Part 381")

    good = verify_fragment(
        artifact,
        key="f",
        source_field="federal_register.body_html",
        start=start,
        end=start + len("9 CFR Part 381"),
        artifact_iri="https://example.test/d/1",
        expected_text="9 CFR Part 381",
    )
    assert good.text == "9 CFR Part 381"

    with pytest.raises(OffsetVerificationError, match="not the expected"):
        verify_fragment(
            artifact,
            key="f",
            source_field="federal_register.body_html",
            start=start + 1,
            end=start + 1 + len("9 CFR Part 381"),
            artifact_iri="https://example.test/d/1",
            expected_text="9 CFR Part 381",
        )


def test_the_fragment_digest_is_recomputed_not_carried(corpus: Path) -> None:
    artifact, _ = _fr_artifact(corpus)
    body = artifact.raw_fields["federal_register.body_html"]
    start = body.index("poultry slaughter inspection")
    fragment = verify_fragment(
        artifact,
        key="f",
        source_field="federal_register.body_html",
        start=start,
        end=start + len("poultry slaughter inspection"),
        artifact_iri="https://example.test/d/1",
    )

    assert fragment.text_sha256 == hashlib.sha256(fragment.text.encode("utf-8")).hexdigest()
    assert f"sha256-{fragment.text_sha256}" in fragment.urn


def test_a_citation_with_two_occurrences_is_not_grounded(corpus: Path) -> None:
    artifact, _ = _fr_artifact(corpus)

    assert (
        ground_literal(
            artifact,
            key="k",
            source_field="federal_register.body_html",
            artifact_iri="https://example.test/d/1",
            surface_forms=("9 CFR Part 381",),
        )
        is not None
    )
    assert (
        ground_literal(
            artifact,
            key="k",
            source_field="federal_register.body_html",
            artifact_iri="https://example.test/d/1",
            surface_forms=("<p>",),
        )
        is None
    ), "an ambiguous surface form grounds nothing rather than guessing"
    assert (
        ground_literal(
            artifact,
            key="k",
            source_field="federal_register.body_html",
            artifact_iri="https://example.test/d/1",
            surface_forms=("42 CFR Part 1",),
        )
        is None
    )


# --------------------------------------------------------------------------- #
# The deterministic (--no-model) path.
# --------------------------------------------------------------------------- #


def test_the_no_model_projection_is_complete_without_a_provider(corpus: Path, tables: Path) -> None:
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    by_type: dict[str, list[dict[str, Any]]] = {}
    for node in result.document["@graph"]:
        by_type.setdefault(node["@type"], []).append(node)

    assert set(by_type) >= {
        "rkaf:Artifact",
        "rkaf:Proceeding",
        "rkaf:Docket",
        "rkaf:RegulatoryAgendaItem",
        "rkaf:RelationshipAssertion",
        "rkaf:SourceFragment",
        "oa:TextPositionSelector",
        "rkaf:EvidenceBinding",
        "rkaf:SourceClaimant",
        "rkaf:ExtractionActivity",
        "prov:Entity",
    }
    assert "rkaf:ConceptAssignment" not in by_type, "no model, no assignments"
    assert "rkaf:Attestation" not in by_type, "nothing produced, nothing to attest"
    assert result.run_record["model"] is None
    assert result.run_record["judgments"] == {"accepted": [], "rejected": []}

    artifact = by_type["rkaf:Artifact"][0]
    assert artifact["rkaf:hasRegulatoryIdentifier"] == "urn:rkaf:us:frdoc:2026-00001"
    assert artifact["rkaf:regulatoryIdentifierScheme"] == "rkaf:us-frdoc"
    assert artifact["rkaf:hasContentDigest"] == "sha256:" + hashlib.sha256(FR_BODY.encode("utf-8")).hexdigest()


def test_the_landed_contract_findings_are_what_the_projection_emits(corpus: Path, tables: Path) -> None:
    """The six findings landed; this is what the answered contract looks like.

    G3 gave the deterministic records a real origin and made their extraction
    provenance REQUIRED, G4 stopped the invent-your-own-contract digest, G2 made
    the document's own docket expressible, G5 made the edge/assertion pair
    normative. All four are visible in one emitted document.
    """
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    graph = result.document["@graph"]
    artifact = next(n for n in graph if n["@type"] == "rkaf:Artifact")
    proceeding = next(n for n in graph if n["@type"] == "rkaf:Proceeding")
    assertions = [n for n in graph if n["@type"] == "rkaf:RelationshipAssertion"]
    activities = [n for n in graph if n["@type"] == "rkaf:ExtractionActivity"]

    # G3: the deterministic origin, and the provenance the contract now requires
    # alongside it on every node that claims it.
    assert {n["rkaf:assertionOrigin"] for n in assertions} == {"rkaf:deterministicExtraction"}
    assert all(n.get("rkaf:hasExtractionProvenance") for n in assertions), (
        "rkaf:deterministicExtraction is REQUIRED to name its extraction activity"
    )
    activity_iris = {n["@id"] for n in activities}
    assert {n["rkaf:hasExtractionProvenance"] for n in assertions} <= activity_iris, (
        "every cited activity is a real ExtractionActivity node in this graph"
    )

    # G4: a deterministic parse issues no request, so it names no request contract.
    assert all(n["rkaf:extractionMethod"] == "rkaf:deterministicParse" for n in activities)
    assert all("rkaf:requestContractDigest" not in n for n in activities)

    # G2: the document's own docket, bound to a Docket that carries its identity.
    assert artifact["rkaf:publishedInDocket"] == ["urn:rkaf:us:regsgov:TEST-2026-0001"]
    docket = next(n for n in graph if n["@type"] == "rkaf:Docket")
    assert docket["@id"] == "urn:rkaf:us:regsgov:TEST-2026-0001"
    assert docket["rkaf:hasDocketIdentifier"] == docket["@id"], "§5.3: the Docket names itself"

    # G5: the affirmed assertion and its projected edge are both emitted.
    assert proceeding["rkaf:hasDocket"] == ["urn:rkaf:us:regsgov:TEST-2026-0001"]
    assert "rkaf:hasDocket" in {n["rkaf:assertsPredicate"] for n in assertions}

    flags = result.run_record["contract_flags"]
    assert flags["assertion_origin_deterministic"] == "rkaf:deterministicExtraction"
    assert flags["request_contract_digest_required_for"] == ["rkaf:modelExtraction"]
    assert flags["emit_document_docket_edge"] is True
    assert flags["emit_profile_edge_projections"] is True


def test_the_contract_findings_are_configuration_not_code(corpus: Path, tables: Path, monkeypatch: Any) -> None:
    """Each finding must remain a value, not a rewrite.

    Flipping the constants must change the emitted document — otherwise they are
    decoration, and the next re-pin would mean editing code paths.
    """
    from spicy_regs.docpipeline import rkaf_projection

    monkeypatch.setattr(rkaf_projection, "EMIT_PROFILE_EDGE_PROJECTIONS", False)
    monkeypatch.setattr(rkaf_projection, "ASSERTION_ORIGIN_DETERMINISTIC", "rkaf:imported")
    monkeypatch.setattr(
        rkaf_projection,
        "REQUEST_CONTRACT_DIGEST_REQUIRED_FOR",
        frozenset({"rkaf:deterministicParse", "rkaf:modelExtraction"}),
    )

    unpinned = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    graph = unpinned.document["@graph"]
    artifact = next(n for n in graph if n["@type"] == "rkaf:Artifact")
    proceeding = next(n for n in graph if n["@type"] == "rkaf:Proceeding")
    assertions = [n for n in graph if n["@type"] == "rkaf:RelationshipAssertion"]
    activities = [n for n in graph if n["@type"] == "rkaf:ExtractionActivity"]
    predicates = {n["rkaf:assertsPredicate"] for n in assertions}

    assert "rkaf:hasDocket" not in proceeding, "the plain edge is gone"
    assert "rkaf:publishedInProceeding" not in artifact
    assert {"rkaf:hasDocket", "rkaf:publishedInProceeding"} <= predicates, (
        "every fact the plain edges carried survives as a reified assertion"
    )
    assert {n["rkaf:assertionOrigin"] for n in assertions} == {"rkaf:imported"}
    assert all("rkaf:requestContractDigest" in n for n in activities)
    assert unpinned.run_record["contract_flags"]["emit_profile_edge_projections"] is False

    # G2 is independent of G5: the document's own docket is a source-native fact,
    # not the projection of an assertion, so switching the projections off must
    # not delete it. Only its own flag does that.
    assert artifact["rkaf:publishedInDocket"] == ["urn:rkaf:us:regsgov:TEST-2026-0001"]
    monkeypatch.setattr(rkaf_projection, "EMIT_DOCUMENT_DOCKET_EDGE", False)
    without = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    artifact = next(n for n in without.document["@graph"] if n["@type"] == "rkaf:Artifact")
    assert "rkaf:publishedInDocket" not in artifact
    assert any("finding G2" in note for note in without.run_record["notes"])


@pytest.mark.parametrize(
    ("stated", "expected"),
    [
        ("TEST-2026-0001", "urn:rkaf:us:regsgov:TEST-2026-0001"),
        ("Docket No. TEST-2026-0001", "urn:rkaf:us:regsgov:TEST-2026-0001"),
        ("Docket No.TEST-2026-0001", "urn:rkaf:us:regsgov:TEST-2026-0001"),
        ("Doc. No. TEST-2026-0001", "urn:rkaf:us:regsgov:TEST-2026-0001"),
        ("Docket Number TEST-2026-0001", "urn:rkaf:us:regsgov:TEST-2026-0001"),
    ],
)
def test_the_docket_label_is_presentation_and_the_identifier_survives_it(
    corpus: Path, tables: Path, stated: str, expected: str
) -> None:
    """FR metadata writes the docket behind a human label; identity is the rest."""
    _write(
        corpus / "federal_register.parquet",
        [
            {
                "document_number": "2026-00001",
                "title": "Poultry slaughter inspection",
                "abstract": "A proposed rule.",
                "document_type": "Proposed Rule",
                "agency_slugs": json.dumps(["food-safety-and-inspection-service"]),
                "body_html": FR_BODY,
                "docket_ids_json": json.dumps([stated]),
                "topics_json": json.dumps(["Poultry and poultry products"]),
            }
        ],
    )
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    artifact = next(n for n in result.document["@graph"] if n["@type"] == "rkaf:Artifact")
    assert artifact["rkaf:publishedInDocket"] == [expected]


def test_a_docket_only_the_document_claims_is_never_minted(corpus: Path, tables: Path) -> None:
    """§5.3: the Docket node may not be minted from the document alone.

    An edge to a container with no ``rkaf:hasDocketIdentifier`` names nothing, so
    a docket no other published row establishes is dropped with a reason rather
    than conjured into existence to hang an edge off.
    """
    _write(
        corpus / "federal_register.parquet",
        [
            {
                "document_number": "2026-00001",
                "title": "Poultry slaughter inspection",
                "abstract": "A proposed rule.",
                "document_type": "Proposed Rule",
                "agency_slugs": json.dumps(["food-safety-and-inspection-service"]),
                "body_html": FR_BODY,
                # The proceedings row names TEST-2026-0001; nothing names this one.
                "docket_ids_json": json.dumps(["Docket No. UNPUBLISHED-2026-9999", "not a docket id"]),
                "topics_json": json.dumps(["Poultry and poultry products"]),
            }
        ],
    )
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))
    graph = result.document["@graph"]
    artifact = next(n for n in graph if n["@type"] == "rkaf:Artifact")

    assert "rkaf:publishedInDocket" not in artifact
    assert "urn:rkaf:us:regsgov:UNPUBLISHED-2026-9999" not in {n["@id"] for n in graph}
    notes = " ".join(result.run_record["notes"])
    assert "UNPUBLISHED-2026-9999" in notes and "§5.3" in notes
    assert "not expressible in rkaf:us-regsgov" in notes


def test_a_deterministic_assertion_that_cannot_name_its_activity_aborts(corpus: Path, tables: Path) -> None:
    """G3 made rkaf:hasExtractionProvenance REQUIRED for the deterministic origin.

    An edge whose activity went missing is a projection bug, not a weaker
    assertion, so it aborts rather than emitting a node no gate would accept.
    """
    from spicy_regs.docpipeline import rkaf_projection

    artifact, row = _fr_artifact(corpus)
    facts = rkaf_projection.build_profile_facts(
        artifact, row, tables=PublishedTables(tables), partner="urn:rkaf:partner:spicy-regs"
    )
    orphaned = dataclasses.replace(facts, activities=())

    with pytest.raises(ProjectionError, match="rkaf:hasExtractionProvenance"):
        rkaf_projection.assemble(artifact, orphaned, settings=_settings(corpus, tables))


def test_every_emitted_fragment_urn_is_reachable_from_the_stored_text(corpus: Path, tables: Path) -> None:
    artifact, _ = _fr_artifact(corpus)
    body = artifact.raw_fields["federal_register.body_html"]
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, tables))

    fragments = [node for node in result.document["@graph"] if node["@type"] == "rkaf:SourceFragment"]
    selectors = {node["@id"]: node for node in result.document["@graph"] if node["@type"] == "oa:TextPositionSelector"}
    assert fragments
    for fragment in fragments:
        selector = selectors[fragment["oa:hasSelector"]]
        region = body[selector["oa:start"] : selector["oa:end"]]
        digest = hashlib.sha256(region.encode("utf-8")).hexdigest()
        assert fragment["rkaf:fragmentContentDigest"] == f"sha256:{digest}"
        assert fragment["@id"].endswith(f":{selector['oa:start']}:{selector['oa:end']}:sha256-{digest}")
        assert FRAGMENT_URN_PATTERN.match(fragment["@id"])


def test_two_runs_of_the_deterministic_layer_agree_byte_for_byte(corpus: Path, tables: Path) -> None:
    settings = _settings(corpus, tables)
    first = project_document("federal-register-document-v1", "2026-00001", settings=settings)
    second = project_document("federal-register-document-v1", "2026-00001", settings=settings)

    assert json.dumps(first.document, sort_keys=True) == json.dumps(second.document, sort_keys=True)
    assert first.run_record["deterministic"] == second.run_record["deterministic"]


def test_a_profile_with_no_published_edges_still_projects(corpus: Path, tables: Path) -> None:
    result = project_document("congress-bill-v1", "118-hr-1", settings=_settings(corpus, tables))

    types = [node["@type"] for node in result.document["@graph"]]
    assert types == ["rkaf:Artifact"]
    assert "rkaf:hasRegulatoryIdentifier" not in result.document["@graph"][0], (
        "a bill has no US regulatory identifier scheme; the projection must not invent one"
    )
    assert any("USRegulatoryIdentifierScheme" in note for note in result.run_record["notes"])


def test_an_unprojectable_profile_refuses_rather_than_guessing(corpus: Path, tables: Path) -> None:
    _write(
        corpus / "gao_reports.parquet",
        [{"report_id": "gao-1", "title": "t", "abstract": "a", "full_text": "text"}],
    )

    with pytest.raises(ProjectionError, match="no RKAF profile projection"):
        project_document("gao-report-v1", "gao-1", settings=_settings(corpus, tables))


def test_missing_published_tables_degrade_to_artifact_only(corpus: Path, tmp_path: Path) -> None:
    empty = tmp_path / "no-tables"
    empty.mkdir()
    result = project_document("federal-register-document-v1", "2026-00001", settings=_settings(corpus, empty))

    assert [node["@type"] for node in result.document["@graph"]] == ["rkaf:Artifact"]
    assert PublishedTables(empty).rows("proceedings") == []


# --------------------------------------------------------------------------- #
# The model layer: judgments in, verified nodes or rejection rows out.
# --------------------------------------------------------------------------- #


def _project_with_model(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path, model: FakeModel, *, name: str = "run"
):
    return project_document(
        "federal-register-document-v1",
        "2026-00001",
        settings=_settings(corpus, tables, registry),
        model=model,
        model_run_directory=tmp_path / name,
    )


def test_a_verified_judgment_becomes_a_concept_assignment(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    model = FakeModel(lambda payload: [t for t in [_tag(payload, quote="poultry slaughter inspection")] if t])

    result = _project_with_model(corpus, tables, registry, tmp_path, model)

    assignments = [n for n in result.document["@graph"] if n["@type"] == "rkaf:ConceptAssignment"]
    assert len(assignments) == 1
    assignment = assignments[0]
    assert assignment["rkaf:assignmentRole"] == ASSIGNMENT_ROLE_IRIS["primary"]
    assert assignment["rkaf:assignmentDerivation"] == "rkaf:directAssignment"
    assert assignment["rkaf:assignmentEvidenceScheme"] == "rkaf:carrier-local-fragment"
    assert assignment["rkaf:assertionOrigin"] == "rkaf:aiSuggested"
    evidence = assignment["rkaf:assignmentEvidence"][0]
    assert FRAGMENT_URN_PATTERN.match(evidence)
    # The cited URN must be materialized as a real fragment node: sh:class
    # rkaf:SourceFragment on rkaf:assignmentEvidence.
    assert any(node["@id"] == evidence and node["@type"] == "rkaf:SourceFragment" for node in result.document["@graph"])
    lineage = [n for n in result.document["@graph"] if n["@type"] == "rkaf:AILineage"]
    assert len(lineage) == 1, "rkaf:aiSuggested requires rkaf:hasAILineage"
    assert assignment["rkaf:hasAILineage"] == lineage[0]["@id"]


def test_the_model_attests_production_and_never_approval(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    model = FakeModel(lambda payload: [t for t in [_tag(payload, quote="poultry slaughter inspection")] if t])

    result = _project_with_model(corpus, tables, registry, tmp_path, model)

    attestations = [n for n in result.document["@graph"] if n["@type"] == "rkaf:Attestation"]
    assert len(attestations) == 1
    attestation = attestations[0]
    assert attestation["rkaf:attestorKind"] == "rkaf:aiModel"
    assert attestation["rkaf:decision"] == MODEL_ATTESTATION_DECISION
    assert attestation["rkaf:decision"] in DECISIONS
    assert attestation["rkaf:decision"] != DECISION_APPROVED, "a model attesting its own output is not approval"
    assert attestation["rkaf:targets"] == [
        n["@id"] for n in result.document["@graph"] if n["@type"] == "rkaf:ConceptAssignment"
    ]
    assert "not approval" in attestation["rkaf:rationale"]


def test_a_model_invented_concept_id_is_rejected_not_minted(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    model = FakeModel(
        lambda payload: [
            t
            for t in [_tag(payload, quote="poultry slaughter inspection", concept_id="concept_invented_by_the_model")]
            if t
        ]
    )

    result = _project_with_model(corpus, tables, registry, tmp_path, model)

    assert not [n for n in result.document["@graph"] if n["@type"] == "rkaf:ConceptAssignment"]
    reasons = [row["reason"] for row in result.run_record["judgments"]["rejected"]]
    assert "unknown_concept" in reasons
    assert "concept_invented_by_the_model" not in json.dumps(result.document)


def test_a_novel_concept_proposal_is_refused_because_the_model_never_mints_identity(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    model = FakeModel(
        lambda payload: [
            t
            for t in [
                _tag(
                    payload,
                    quote="poultry slaughter inspection",
                    concept_id=None,
                    proposed_label="a brand new topic nobody registered",
                    definition="Invented on the spot.",
                )
            ]
            if t
        ]
    )

    result = _project_with_model(corpus, tables, registry, tmp_path, model)

    assert not [n for n in result.document["@graph"] if n["@type"] == "rkaf:ConceptAssignment"]
    assert [row["reason"] for row in result.run_record["judgments"]["rejected"]] == [
        "model_proposed_concept_not_in_registry"
    ]


def test_an_unresolvable_quote_becomes_a_rejection_row(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    def tags(payload: dict[str, Any]) -> list[dict[str, Any]]:
        good = _tag(payload, quote="poultry slaughter inspection")
        if good is None:
            return []
        invented = dict(good)
        invented["evidence_text"] = "text that is nowhere in this document"
        invented["evidence_start"] = 0
        invented["evidence_end"] = len(invented["evidence_text"])
        return [good, invented]

    result = _project_with_model(corpus, tables, registry, tmp_path, FakeModel(tags))

    assert len([n for n in result.document["@graph"] if n["@type"] == "rkaf:ConceptAssignment"]) == 1
    assert "ungrounded_evidence" in [row["reason"] for row in result.run_record["judgments"]["rejected"]]
    assert "text that is nowhere in this document" not in json.dumps(result.document)


def test_a_candidate_row_whose_offsets_drifted_becomes_a_rejection_row(corpus: Path) -> None:
    artifact, _ = _fr_artifact(corpus)
    body = artifact.raw_fields["federal_register.body_html"]
    start = body.index("poultry slaughter inspection")
    row = {
        "candidate_id": "tag_candidate_1",
        "concept_id": "concept_poultry",
        "concept_label": "Poultry and poultry products",
        "definition": "d",
        "facet": "subject",
        "role": "primary",
        "confidence": 0.9,
        "source_field": "federal_register.body_html",
        "evidence_grade": "source-exact",
        "source_start_char": start + 3,
        "source_end_char": start + 3 + len("poultry slaughter inspection"),
        "evidence_text": "poultry slaughter inspection",
        "evidence_alignment_method": "provided-offsets",
    }

    judgments, rejections = verify_candidate_rows(
        artifact,
        [row],
        artifact_iri="https://example.test/d/1",
        evidence_field="federal_register.body_html",
    )

    assert judgments == []
    assert [item["reason"] for item in rejections] == ["offset_verification_failed"]


def test_parser_derived_evidence_never_earns_a_carrier_local_urn(corpus: Path) -> None:
    artifact, _ = _fr_artifact(corpus)
    body = artifact.raw_fields["federal_register.body_html"]
    start = body.index("poultry slaughter inspection")
    base = {
        "candidate_id": "tag_candidate_1",
        "concept_id": "concept_poultry",
        "concept_label": "Poultry and poultry products",
        "definition": "d",
        "facet": "subject",
        "role": "primary",
        "confidence": 0.9,
        "source_start_char": start,
        "source_end_char": start + len("poultry slaughter inspection"),
        "evidence_text": "poultry slaughter inspection",
        "evidence_alignment_method": "provided-offsets",
    }

    _, parser_derived = verify_candidate_rows(
        artifact,
        [{**base, "source_field": "federal_register.body_html", "evidence_grade": "parser-derived"}],
        artifact_iri="https://example.test/d/1",
        evidence_field="federal_register.body_html",
    )
    _, other_field = verify_candidate_rows(
        artifact,
        [{**base, "source_field": "federal_register.title", "evidence_grade": "source-exact"}],
        artifact_iri="https://example.test/d/1",
        evidence_field="federal_register.body_html",
    )

    assert [row["reason"] for row in parser_derived] == ["evidence_not_source_exact"]
    assert [row["reason"] for row in other_field] == ["evidence_outside_projected_text_state"]


def test_the_model_run_keeps_request_and_response_custody(
    corpus: Path, tables: Path, registry: Path, tmp_path: Path
) -> None:
    model = FakeModel(lambda payload: [t for t in [_tag(payload, quote="poultry slaughter inspection")] if t])

    result = _project_with_model(corpus, tables, registry, tmp_path, model, name="custody")

    run_directory = Path(result.run_record["model"]["extraction_run_directory"])
    calls = sorted((run_directory / "extraction" / "calls").iterdir())
    assert calls, "every provider call is stored"
    for call in calls:
        for name in ("payload.json", "schema.json", "request.json", "response.json", "call.json"):
            assert (call / name).is_file(), f"{name} missing for {call.name}"
    assert result.run_record["model"]["extraction_receipt_sha256"]
    assert result.run_record["model"]["candidate_selector_version"] == "anchored-hybrid-v2"
    assert result.run_record["model"]["candidate_registry_sha256"] == hashlib.sha256(registry.read_bytes()).hexdigest()


def test_the_cli_reaches_both_provider_arms_without_a_credential(monkeypatch: Any) -> None:
    """Both arms are wired; each refuses in its own documented way."""
    import importlib.util

    from spicy_regs.docpipeline.adapters.anthropic import ProviderConfigurationError

    spec = importlib.util.spec_from_file_location(
        "project_document_to_rkaf",
        Path(__file__).resolve().parents[1] / "tools" / "project_document_to_rkaf.py",
    )
    assert spec is not None and spec.loader is not None
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    with pytest.raises(ProjectionError, match="OPENAI_API_KEY"):
        cli.build_model("openai", None)
    with pytest.raises(ProjectionError, match="declares no default model"):
        cli.build_model("anthropic", None)
    with pytest.raises(ProviderConfigurationError, match="ANTHROPIC_API_KEY"):
        cli.build_model("anthropic", "claude-fable-5")
    with pytest.raises(ProjectionError, match="unknown provider"):
        cli.build_model("nonesuch", None)


def test_the_model_layer_needs_a_registry_and_a_custody_directory(corpus: Path, tables: Path, tmp_path: Path) -> None:
    model = FakeModel([])

    with pytest.raises(ProjectionError, match="candidate concept registry"):
        project_document(
            "federal-register-document-v1",
            "2026-00001",
            settings=_settings(corpus, tables),
            model=model,
            model_run_directory=tmp_path / "x",
        )


# --------------------------------------------------------------------------- #
# Regression against the hand-authored FSIS document, and data discipline.
# --------------------------------------------------------------------------- #

_EVIDENCE_DIR = Path(__file__).resolve().parents[1] / "docs" / "evidence"
_FSIS_DIR = _EVIDENCE_DIR / "single-document-rulespec-projection-2026-07-28"
_CORPUS = Path(__file__).resolve().parents[1] / "output" / "segmented-real-data-evaluation-v2"


@pytest.mark.skipif(
    not (_FSIS_DIR / "fsis-2026-03227.rulespec.jsonld").is_file() or not _CORPUS.is_dir(),
    reason="the development corpus and the hand-authored reference are not present",
)
def test_the_deterministic_layer_reproduces_the_hand_authored_identities() -> None:
    """Same inputs, same identity fields as the hand-authored projection.

    Byte-identical assembly is not the claim — the hand-authored file chose its
    three fragments by hand. What must match is identity: the artifact digest
    spicy-regs recorded, the text state the coordinates address, and the URN a
    consumer would mint for the same region.
    """
    import duckdb

    connection = duckdb.connect()
    try:
        row = (
            connection.execute(
                f"SELECT * FROM read_parquet('{_CORPUS / 'federal_register.parquet'}') "
                "WHERE document_number = '2026-03227'"
            )
            .df()
            .to_dict("records")[0]
        )
    finally:
        connection.close()
    outcome = build_source_artifact(SourceRecord(profile=profile_for_table("federal_register"), row=row))
    artifact = outcome.artifact
    assert artifact is not None

    # The producer-scoped version digest the run recorded, and the gold row's.
    assert artifact.content_sha256 == "9b3eb7602445ccb21f8ebe2cbe69fb0d7609f0682684b8b605b9679398185972"
    # The text state the hand-authored file used for hasContentDigest.
    assert (
        artifact.field_sha256["federal_register.body_html"]
        == "d67993458a2b330cd9b53af0f0162d21aa78ea30b61b190926c21ea6b91ec921"
    )

    reference = json.loads((_FSIS_DIR / "fsis-2026-03227.rulespec.jsonld").read_text())
    gold_urn = next(
        node["@id"]
        for node in reference["@graph"]
        if node.get("@type") == "rkaf:SourceFragment" and ":2282:2307:" in node["@id"]
    )
    fragment = verify_fragment(
        artifact,
        key="gold",
        source_field="federal_register.body_html",
        start=2282,
        end=2307,
        artifact_iri="https://www.federalregister.gov/d/2026-03227",
        expected_text="Poultry Inspection System",
    )
    assert fragment.urn == gold_urn


@pytest.mark.skipif(
    not (_EVIDENCE_DIR / "gold-adjudication-2026-07-27" / "evaluation-boundary.json").is_file(),
    reason="the evaluation boundary manifest is not present",
)
def test_the_drawn_holdout_stays_sealed() -> None:
    """The 28-artifact holdout has no labels and nothing here may reach it."""
    boundary = json.loads((_EVIDENCE_DIR / "gold-adjudication-2026-07-27" / "evaluation-boundary.json").read_text())
    holdout = boundary["pending_holdout"]

    assert holdout["status"] == "drawn_unadjudicated"
    assert holdout["labels"]["status"] == "not_drafted"
    assert holdout["labels"]["gold_file"] is None
    assert holdout["draw"]["artifact_count"] == 28
    # The projection tool resolves subjects from a corpus snapshot and the
    # published tables only; it has no code path that reads a holdout draw.
    source = (
        Path(__file__).resolve().parents[1] / "src" / "spicy_regs" / "docpipeline" / "rkaf_projection.py"
    ).read_text()
    assert "holdout" not in source.lower()
    assert "evaluation-boundary" not in source

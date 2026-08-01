"""Executable gates for the repository-independent SpicyRegs release."""

from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path

import pytest
import spicy_regs.document_release as document_release_module

from spicy_regs.document_release import (
    COORDINATE_SYSTEM,
    DEFAULT_DOCUMENT_RELEASE_FIXTURE_PATH,
    DEFAULT_FIXTURE_PATH,
    DEFAULT_RULESPEC_CORE_PATH,
    DocumentReleaseError,
    SourceLink,
    SourceRecordVersion,
    StructuralPassage,
    TextRepresentation,
    build_document_release,
    canonical_digest,
    canonical_json,
    classify_source_record,
    main,
    make_link_verification_receipt,
    seal_document_release,
    text_digest,
    validate_document_release,
)


PHRASE = "Poultry Inspection System"
PHRASE_START = 2282
PHRASE_END = 2307
WORKER_ATTESTATIONS = "Worker safety attestations document hazards."
WORKER_ATTESTATIONS_START = 2308
WORKER_ATTESTATIONS_END = 2352
WORKER_PROTECTIONS = "Workers receive protections."
WORKER_PROTECTIONS_START = 2353
WORKER_PROTECTIONS_END = 2381
FR_DOCUMENT = "2026-03227"
REGULATIONS_CROSSPOST = "FSIS-2025-0012-0003"
MIGRATION_MANIFEST = Path("docs/migration/spicysearch-product-migration-manifest.json")


def _by(records: list[dict], field: str) -> dict[str, dict]:
    return {str(record[field]): record for record in records}


def _source_by_natural_key(release: dict, publisher: str, record_id: str) -> dict:
    return next(
        record
        for record in release["source_record_versions"]
        if record["publisher"] == publisher and record["source_record_id"] == record_id
    )


def _document_by_natural_key(release: dict, publisher: str, record_id: str) -> dict:
    return next(
        record
        for record in release["document_versions"]
        if record["publisher"] == publisher and record["source_record_id"] == record_id
    )


def _reseal(release: dict) -> dict:
    body = copy.deepcopy(release)
    body.pop("release_id")
    body.pop("release_digest")
    return seal_document_release(body)


def _write_changed_fixture(tmp_path: Path, mutate) -> Path:
    fixture = json.loads(DEFAULT_FIXTURE_PATH.read_text(encoding="utf-8"))
    mutate(fixture)
    fixture["fixture_digest"] = canonical_digest(
        {key: value for key, value in fixture.items() if key != "fixture_digest"}
    )
    path = tmp_path / "source-fixture.json"
    path.write_text(canonical_json(fixture) + "\n", encoding="utf-8")
    return path


def test_canonical_json_recipe_is_exact_and_rejects_non_finite_numbers() -> None:
    assert canonical_json({"z": "é", "a": [2, 1]}) == '{"a":[2,1],"z":"é"}'
    assert canonical_digest({"z": "é", "a": [2, 1]}) == (
        "sha256:" + hashlib.sha256('{"a":[2,1],"z":"é"}'.encode("utf-8")).hexdigest()
    )
    with pytest.raises(DocumentReleaseError, match="not canonical JSON"):
        canonical_json({"not_a_number": float("nan")})


def test_m1_release_is_deterministic_self_contained_and_pins_rulespec_core() -> None:
    first = build_document_release()
    second = build_document_release()

    assert canonical_json(first) == canonical_json(second)
    assert first["release_id"] == ("urn:spicyregs:document-release:" + first["release_digest"].removeprefix("sha256:"))
    assert first["rulespec_core_release"] == {
        "release_digest": "sha256:5ac6ba59929eca874ec603cab0e90f7b15ab1a008b394cec5aefebdafe22564b",
        "release_id": "urn:rulespec:core:5ac6ba59929eca874ec603cab0e90f7b15ab1a008b394cec5aefebdafe22564b",
    }
    assert len(first["source_record_versions"]) == 6
    assert len(first["document_versions"]) == 4
    for record in first["document_versions"]:
        projection = record["artifact_projection"]
        assert set(projection) == {
            "artifact_id",
            "artifact_type",
            "content_digest",
            "coordinate_system",
            "evidence_grade",
            "media_type",
        }
        assert projection["artifact_type"] == "Artifact"
        assert projection["content_digest"] == record["content_digest"]
        assert projection["coordinate_system"] == "document-version"
        assert projection["evidence_grade"] == "source-exact"
    validate_document_release(first)

    checked_in_text = DEFAULT_DOCUMENT_RELEASE_FIXTURE_PATH.read_text(encoding="utf-8")
    checked_in_release = json.loads(checked_in_text)
    assert checked_in_text == canonical_json(checked_in_release) + "\n"
    assert canonical_json(first) == canonical_json(checked_in_release)
    validate_document_release(checked_in_release)

    assert document_release_module.__file__ is not None
    source = Path(document_release_module.__file__).read_text(encoding="utf-8")
    imports = [node.module or "" for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom)]
    imports.extend(
        alias.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for alias in node.names
    )
    assert not any(name == "refspec" or name.startswith("refspec.") for name in imports)
    assert not any(name == "rulespec" or name.startswith("rulespec.") for name in imports)


def test_exact_phrase_passage_revalidates_without_assignment_data() -> None:
    release = build_document_release()
    document = _document_by_natural_key(release, "federal-register", FR_DOCUMENT)
    representation = next(
        record
        for record in release["text_representations"]
        if record["document_version_ref"] == document["document_version_id"]
    )
    passage = next(
        record
        for record in release["structural_passages"]
        if record["document_version_ref"] == document["document_version_id"]
        and (record["start"], record["end"]) == (PHRASE_START, PHRASE_END)
    )

    assert representation["unicode_text"][PHRASE_START:PHRASE_END] == PHRASE
    assert (passage["start"], passage["end"]) == (PHRASE_START, PHRASE_END)
    assert passage["selected_text_digest"] == text_digest(PHRASE)
    assert passage["representation_digest"] == representation["text_digest"]
    assert passage["coordinate_system"] == COORDINATE_SYSTEM
    assert passage["evidence_grade"] == "source-exact"
    assert passage["source_fragment_projection"] == {
        "fragment_id": passage["source_fragment_projection"]["fragment_id"],
        "fragment_type": "SourceFragment",
        "selected_text_digest": text_digest(PHRASE),
        "selector": {
            "coordinate_system": COORDINATE_SYSTEM,
            "end": PHRASE_END,
            "selector_type": "TextPositionSelector",
            "start": PHRASE_START,
        },
        "source_artifact_digest": representation["text_digest"],
        "source_artifact_ref": representation["artifact_projection"]["artifact_id"],
    }
    serialized = canonical_json(release)
    assert "ConceptAssignment" not in serialized
    assert "ExtrapolationRelease" not in serialized


def test_worker_safety_text_has_two_exact_structural_passages_for_downstream_chunks() -> None:
    release = build_document_release()
    document = _document_by_natural_key(release, "federal-register", FR_DOCUMENT)
    representation = next(
        record
        for record in release["text_representations"]
        if record["document_version_ref"] == document["document_version_id"]
    )
    worker_passages = sorted(
        (
            record
            for record in release["structural_passages"]
            if record["document_version_ref"] == document["document_version_id"]
            and record["start"] >= WORKER_ATTESTATIONS_START
        ),
        key=lambda record: record["start"],
    )

    assert representation["unicode_text"][WORKER_ATTESTATIONS_START:WORKER_PROTECTIONS_END] == (
        WORKER_ATTESTATIONS + "\n" + WORKER_PROTECTIONS
    )
    assert [(record["start"], record["end"], record["selected_text_digest"]) for record in worker_passages] == [
        (WORKER_ATTESTATIONS_START, WORKER_ATTESTATIONS_END, text_digest(WORKER_ATTESTATIONS)),
        (WORKER_PROTECTIONS_START, WORKER_PROTECTIONS_END, text_digest(WORKER_PROTECTIONS)),
    ]
    for passage in worker_passages:
        projection = passage["source_fragment_projection"]
        assert projection["fragment_type"] == "SourceFragment"
        assert projection["source_artifact_ref"] == representation["artifact_projection"]["artifact_id"]
        assert projection["source_artifact_digest"] == representation["text_digest"]
        assert projection["selected_text_digest"] == passage["selected_text_digest"]


def test_source_observations_remain_source_facts_not_concepts() -> None:
    release = build_document_release()
    observations = release["source_observations"]
    assert len(observations) == 2
    assert {record["raw_value"] for record in observations} == {"Meat inspection"}
    assert {record["observation_kind"] for record in observations} == {"federal-register-api-topic"}
    assert len({record["observation_id"] for record in observations}) == 2
    for record in observations:
        assert set(record) == {
            "document_version_ref",
            "observation_id",
            "observation_kind",
            "raw_value",
            "source_native_key_or_ordinal",
            "source_native_path",
            "source_record_digest",
            "source_record_version_ref",
        }


def test_crosspost_has_successful_receipt_and_mismatch_has_failed_receipt() -> None:
    release = build_document_release()
    receipts = release["link_verification_receipts"]
    verified = next(record for record in receipts if record["outcome"] == "verified")
    failed = next(record for record in receipts if record["outcome"] == "failed")

    assert verified["failure_reason"] is None
    assert verified["raw_value"] == FR_DOCUMENT
    assert all(check["passed"] for check in verified["checks"])
    source = _source_by_natural_key(release, "regulations.gov", REGULATIONS_CROSSPOST)
    target = _source_by_natural_key(release, "federal-register", FR_DOCUMENT)
    assert verified["source_record_ref"] == source["source_record_version_id"]
    assert verified["source_record_digest"] == source["source_record_digest"]
    assert verified["target_record_ref"] == target["source_record_version_id"]
    assert verified["target_record_digest"] == target["source_record_digest"]

    assert failed["failure_reason"] == "document-number-mismatch"
    assert failed["raw_value"] == "2026-99999"
    checks = {check["check"]: check["passed"] for check in failed["checks"]}
    assert checks["fr-doc-num-grammar"] is True
    assert checks["exact-document-number-match"] is False


def test_link_verification_records_malformed_and_missing_inputs_as_typed_failures() -> None:
    target = SourceRecordVersion.create(
        publisher="federal-register",
        collection="documents",
        source_record_id=FR_DOCUMENT,
        source_url=f"https://www.federalregister.gov/d/{FR_DOCUMENT}",
        content={"document_number": FR_DOCUMENT},
    )
    source = SourceRecordVersion.create(
        publisher="regulations.gov",
        collection="documents",
        source_record_id="CONTROL",
        source_url="https://www.regulations.gov/document/CONTROL",
        content={"fr_doc_num": "not-a-document-number"},
    )
    link = SourceLink.create(
        source_record_version_ref=source.source_record_version_id,
        target_source_record_version_ref=target.source_record_version_id,
        source_field="documents.fr_doc_num",
        raw_value="not-a-document-number",
    )
    malformed = make_link_verification_receipt(link, source=source, target=target)
    missing = make_link_verification_receipt(link, source=source, target=None)

    assert (malformed["outcome"], malformed["failure_reason"]) == ("failed", "malformed-fr-doc-num")
    assert (missing["outcome"], missing["failure_reason"]) == ("failed", "missing-target-record")
    assert missing["target_record_ref"] is None
    assert missing["target_record_digest"] is None


def test_repeated_captures_reuse_facts_and_append_distinct_events() -> None:
    release = build_document_release()
    document = _document_by_natural_key(release, "federal-register", FR_DOCUMENT)
    rendition = next(
        record
        for record in release["source_renditions"]
        if record["document_version_ref"] == document["document_version_id"]
    )
    captures = [
        record
        for record in release["source_rendition_captures"]
        if record["source_rendition_ref"] == rendition["rendition_id"]
    ]
    assert len(captures) == 2
    assert len({record["capture_id"] for record in captures}) == 2
    assert {record["source_rendition_ref"] for record in captures} == {rendition["rendition_id"]}

    observation = next(
        record
        for record in release["source_observations"]
        if record["document_version_ref"] == document["document_version_id"]
    )
    observation_captures = [
        record
        for record in release["source_observation_captures"]
        if record["source_observation_ref"] == observation["observation_id"]
    ]
    assert len(observation_captures) == 2
    assert len({record["capture_id"] for record in observation_captures}) == 2


@pytest.mark.parametrize(
    ("fact_collection", "capture_collection", "capture_ref_field", "message"),
    [
        (
            "source_renditions",
            "source_rendition_captures",
            "source_rendition_ref",
            "every SourceRendition",
        ),
        (
            "source_observations",
            "source_observation_captures",
            "source_observation_ref",
            "every SourceObservation",
        ),
    ],
)
def test_every_published_source_fact_has_capture_evidence(
    fact_collection: str,
    capture_collection: str,
    capture_ref_field: str,
    message: str,
) -> None:
    release = build_document_release()
    fact_id_field = "rendition_id" if fact_collection == "source_renditions" else "observation_id"
    fact_ref = release[fact_collection][0][fact_id_field]
    release[capture_collection] = [item for item in release[capture_collection] if item[capture_ref_field] != fact_ref]
    release["acquisition_coverage"]["capture_refs"] = sorted(
        [
            item["capture_id"]
            for collection in ("source_rendition_captures", "source_observation_captures")
            for item in release[collection]
        ]
    )
    resealed = _reseal(release)

    with pytest.raises(DocumentReleaseError, match=message):
        validate_document_release(resealed)


def test_text_representation_cannot_name_another_documents_rendition() -> None:
    release = build_document_release()
    representation = release["text_representations"][0]
    other_rendition = next(
        item
        for item in release["source_renditions"]
        if item["document_version_ref"] != representation["document_version_ref"]
    )
    representation["source_rendition_ref"] = other_rendition["rendition_id"]
    resealed = _reseal(release)

    with pytest.raises(DocumentReleaseError, match="source rendition belongs to another document"):
        validate_document_release(resealed)


def test_representation_policy_and_passage_boundary_changes_get_new_identities() -> None:
    original = TextRepresentation.create(
        document_version_ref="urn:document:one",
        representation_kind_and_path="source-record-field:body_html",
        unicode_text="alpha beta gamma",
        evidence_grade="source-exact",
        source_rendition_ref="urn:rendition:one",
        method="json-field-decoding",
        method_version="1",
        method_config_digest=canonical_digest({"field": "body_html"}),
    )
    changed_policy = TextRepresentation.create(
        document_version_ref="urn:document:one",
        representation_kind_and_path="source-record-field:body_html",
        unicode_text="alpha beta gamma",
        evidence_grade="source-exact",
        source_rendition_ref="urn:rendition:one",
        method="json-field-decoding",
        method_version="2",
        method_config_digest=canonical_digest({"field": "body_html"}),
    )
    first_passage = StructuralPassage.create(representation=original, start=0, end=5)
    moved_boundary = StructuralPassage.create(representation=original, start=0, end=10)

    assert original.text_digest == changed_policy.text_digest
    assert original.representation_id != changed_policy.representation_id
    assert first_passage.document_version_ref == moved_boundary.document_version_ref
    assert first_passage.passage_id != moved_boundary.passage_id
    with pytest.raises(DocumentReleaseError, match="outside representation"):
        StructuralPassage.create(representation=original, start=0, end=99)
    with pytest.raises(DocumentReleaseError, match="derived text must reference"):
        TextRepresentation.create(
            document_version_ref="urn:document:one",
            representation_kind_and_path="ocr:page-1",
            unicode_text="derived",
            evidence_grade="ocr-derived",
        )


def test_representation_evidence_grade_and_method_provenance_fail_closed() -> None:
    common = {
        "document_version_ref": "urn:document:one",
        "representation_kind_and_path": "source-record-field:body_html",
        "source_rendition_ref": "urn:rendition:one",
        "unicode_text": "addressable text",
    }
    with pytest.raises(DocumentReleaseError, match="unknown evidence grade"):
        TextRepresentation.create(**common, evidence_grade="trust-me")
    with pytest.raises(DocumentReleaseError, match="supplied together"):
        TextRepresentation.create(
            **common,
            evidence_grade="parser-derived",
            method="html-parser",
            method_version="1",
        )
    with pytest.raises(DocumentReleaseError, match="not derived extraction provenance"):
        TextRepresentation.create(
            **common,
            evidence_grade="source-exact",
            method="tesseract-ocr",
            method_version="5",
            method_config_digest=canonical_digest({"language": "en"}),
        )


def test_metadata_only_change_keeps_document_version_but_changes_observation_and_release(
    tmp_path: Path,
) -> None:
    original = build_document_release()

    def mutate(fixture: dict) -> None:
        record = next(item for item in fixture["records"] if item["key"] == "fr-2026-03227")
        record["content"]["topics"][0] = "Changed source topic"

    changed = build_document_release(_write_changed_fixture(tmp_path, mutate))
    original_document = _document_by_natural_key(original, "federal-register", FR_DOCUMENT)
    changed_document = _document_by_natural_key(changed, "federal-register", FR_DOCUMENT)
    original_observation = next(
        row
        for row in original["source_observations"]
        if row["document_version_ref"] == original_document["document_version_id"]
    )
    changed_observation = next(
        row
        for row in changed["source_observations"]
        if row["document_version_ref"] == changed_document["document_version_id"]
    )

    assert original_document["source_issued_version_id"] == changed_document["source_issued_version_id"]
    assert original_document["document_version_id"] == changed_document["document_version_id"]
    assert original_observation["observation_id"] != changed_observation["observation_id"]
    assert original["release_id"] != changed["release_id"]


def test_representation_policy_change_cascades_to_passage_and_release(tmp_path: Path) -> None:
    original = build_document_release()

    def mutate(fixture: dict) -> None:
        record = next(item for item in fixture["records"] if item["key"] == "fr-2026-03227")
        record["representations"][0]["method_version"] = "2"

    changed = build_document_release(_write_changed_fixture(tmp_path, mutate))
    original_document = _document_by_natural_key(original, "federal-register", FR_DOCUMENT)
    changed_document = _document_by_natural_key(changed, "federal-register", FR_DOCUMENT)
    original_representation = next(
        row
        for row in original["text_representations"]
        if row["document_version_ref"] == original_document["document_version_id"]
    )
    changed_representation = next(
        row
        for row in changed["text_representations"]
        if row["document_version_ref"] == changed_document["document_version_id"]
    )
    original_passage = next(
        row
        for row in original["structural_passages"]
        if row["document_version_ref"] == original_document["document_version_id"]
    )
    changed_passage = next(
        row
        for row in changed["structural_passages"]
        if row["document_version_ref"] == changed_document["document_version_id"]
    )

    assert original_document["document_version_id"] == changed_document["document_version_id"]
    assert original_representation["representation_id"] != changed_representation["representation_id"]
    assert original_passage["passage_id"] != changed_passage["passage_id"]
    assert original["release_id"] != changed["release_id"]


def test_document_only_classification_fails_closed_and_excludes_dockets_and_comments() -> None:
    assert classify_source_record("federal-register", "documents") == "document"
    assert classify_source_record("regulations.gov", "documents") == "document"
    assert classify_source_record("regulations.gov", "dockets") == "relationship-context"
    assert classify_source_record("regulations.gov", "comments") == "public-comment"
    with pytest.raises(DocumentReleaseError, match="unclassified source record kind"):
        classify_source_record("unknown", "documents")

    release = build_document_release()
    document_natural_keys = {
        (record["publisher"], record["source_record_id"]) for record in release["document_versions"]
    }
    assert ("regulations.gov", "FSIS-2025-0012") not in document_natural_keys
    assert ("regulations.gov", "FSIS-2025-0012-0042") not in document_natural_keys
    excluded = [entry for entry in release["acquisition_coverage"]["entries"] if entry["state"] == "excluded"]
    assert {entry["collection"] for entry in excluded} == {"dockets", "comments"}


@pytest.mark.parametrize(
    "mutate, message",
    [
        (
            lambda release: release["text_representations"][0].__setitem__("unicode_text", "tampered"),
            "TextRepresentation",
        ),
        (
            lambda release: release["structural_passages"][0].__setitem__(
                "text_representation_ref", release["text_representations"][-1]["representation_id"]
            ),
            "outside representation",
        ),
        (
            lambda release: release["source_observations"][0].__setitem__("raw_value", "invented topic"),
            "SourceObservation",
        ),
        (
            lambda release: release["source_rendition_captures"][0].__setitem__(
                "source_rendition_ref", "urn:spicyregs:source-rendition:missing"
            ),
            "missing rendition",
        ),
    ],
)
def test_digest_coordinate_and_reference_tampering_fails_closed(mutate, message: str) -> None:
    release = build_document_release()
    mutate(release)
    resealed = _reseal(release)
    with pytest.raises(DocumentReleaseError, match=message):
        validate_document_release(resealed)


def test_release_digest_omits_only_root_identity_fields() -> None:
    release = build_document_release()
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    assert release["release_digest"] == canonical_digest(body)

    changed = copy.deepcopy(body)
    changed["released_at"] = "2026-07-31T12:11:00Z"
    changed_release = seal_document_release(changed)
    assert changed_release["release_id"] != release["release_id"]

    release["release_id"] = "urn:spicyregs:document-release:" + "0" * 64
    with pytest.raises(DocumentReleaseError, match="canonical identity differs"):
        validate_document_release(release)


def test_acquisition_coverage_closes_capture_refs_and_reports_gaps() -> None:
    release = build_document_release()
    coverage = release["acquisition_coverage"]
    capture_ids = {
        record["capture_id"]
        for field in ("source_rendition_captures", "source_observation_captures")
        for record in release[field]
    }
    assert coverage["capture_refs"] == sorted(capture_ids)
    assert {entry["state"] for entry in coverage["entries"]} == {
        "captured",
        "excluded",
        "unavailable",
        "unprocessed",
    }


def test_fixture_digest_rulespec_pin_and_release_reference_mismatches_fail_closed(tmp_path: Path) -> None:
    fixture = json.loads(DEFAULT_FIXTURE_PATH.read_text(encoding="utf-8"))
    fixture["records"][0]["content"]["title"] = "tampered without resealing"
    bad_fixture = tmp_path / "bad-source-fixture.json"
    bad_fixture.write_text(canonical_json(fixture), encoding="utf-8")
    with pytest.raises(DocumentReleaseError, match="source fixture digest differs"):
        build_document_release(bad_fixture)

    core = json.loads(DEFAULT_RULESPEC_CORE_PATH.read_text(encoding="utf-8"))
    core["version"] = "tampered"
    bad_core = tmp_path / "bad-core-fixture.json"
    bad_core.write_text(canonical_json(core), encoding="utf-8")
    with pytest.raises(DocumentReleaseError, match="identity differs"):
        build_document_release(rulespec_core_path=bad_core)


def test_builder_command_writes_canonical_valid_release(tmp_path: Path, capsys) -> None:
    output = tmp_path / "document-release.json"
    assert main(["--output", str(output)]) == 0
    saved = output.read_text(encoding="utf-8")
    release = json.loads(saved)

    assert saved == canonical_json(release) + "\n"
    validate_document_release(release)
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "pass"
    assert result["release_id"] == release["release_id"]
    assert result["document_count"] == 4
    assert result["passage_count"] == 6


def test_migration_manifest_accounts_for_serving_and_incubated_surfaces_without_retiring_them() -> None:
    manifest = json.loads(MIGRATION_MANIFEST.read_text(encoding="utf-8"))
    required_item_fields = {
        "capability",
        "command_line_entry_points",
        "current_behavior",
        "destination",
        "disposition",
        "known_consumers",
        "migration_status",
        "nested_or_uncommitted_state",
        "notebooks",
        "originating_commit",
        "owning_product",
        "path",
        "pinned_fixture",
        "published_artifact_locations",
        "replacement_interface",
        "scheduled_jobs",
        "tests_or_evidence",
    }
    assert manifest["retirement_authorized"] is False
    assert manifest["status"] == "inventory-complete-serving-surfaces-retained"
    assert manifest["items"]
    assert all(set(item) == required_item_fields for item in manifest["items"])
    inventory = canonical_json(manifest)
    for required_surface in (
        "src/spicy_regs/cli.py",
        "src/spicy_regs/transforms/build_search_index.py",
        ".github/workflows/rollup-docket-search.yml",
        "src/spicy_regs/vectordb/embed.py",
        "notebooks/search_capabilities.ipynb",
        "notebooks/vector_search.ipynb",
        "src/spicy_regs/mcp_server.py",
        "src/spicy_regs/docpipeline/source.py",
        "src/spicy_regs/docpipeline/segments.py",
        "src/spicy_regs/docpipeline/retrieval.py",
        "src/spicy_regs/ontology/vocabulary_atlas",
        "src/spicy_regs/enrichment/",
        "tools/graphdb/",
        "RefSpec/",
        "https://r2.spicy-regs.dev/docket_search.json.gz",
        "https://r2.spicy-regs.dev/materialized/ontology/latest.json",
    ):
        assert required_surface in inventory

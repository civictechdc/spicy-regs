"""Protocol, bounded-build, verification, diff, and compaction tests for v3."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.cli import main as cli_main
from spicy_regs.document_release_v3 import (
    DocumentReleaseV3Error,
    PASSAGES_SCHEMA_ID,
    TABLE_SCHEMAS,
    VerificationCode,
    artifact_digest,
    canonical_json_bytes,
    canonical_json_text,
    parse_canonical_json,
    release_id,
)
from spicy_regs.document_release_v3_compact import compact_release, compaction_metrics
from spicy_regs.document_release_v3_diff import active_identity_map, iter_release_diff, release_member_paths
from spicy_regs.document_release_v3_verify import verify_release, verify_release_or_raise
from spicy_regs.document_release_v3_writer import BuildConfig, SourceInput, build_release


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SEALED_FIXTURE = REPOSITORY_ROOT / "fixtures" / "releases" / "document-release-v3"
SOURCE_FIXTURE = Path(__file__).with_name("fixtures") / "document-release-v3-source"


def _config(
    *,
    selector: str,
    previous_release_id: str | None = None,
    previous_artifact_digest: str | None = None,
) -> BuildConfig:
    return BuildConfig(
        implementation_id="spicyregs.document-release-v3.test",
        implementation_version="1.0",
        runtime_profile_id="pytest-local-python-3.12",
        processing_policy_id="spicyregs.processing.test.v1",
        normalizer_id="spicyregs.normalizer.utf8-identity.v1",
        segmenter_id="spicyregs.segmenter.utf8-bounded.v1",
        rendition_policy_id="spicyregs.rendition.pack.v1",
        eligibility_policy_id="spicyregs.eligibility.test.v1",
        failure_policy_id="spicyregs.failure.test.v1",
        diagnostic_registry_id="spicyregs.diagnostics.test.v1",
        selection_id=f"test-selection-{selector}",
        selector_type="pytest-ledger",
        selector_digest=hashlib.sha256(selector.encode()).hexdigest(),
        effective_at="2026-08-04T00:00:00Z",
        partition_id="test",
        previous_release_id=previous_release_id,
        previous_artifact_digest=previous_artifact_digest,
        row_batch_size=1,
        row_batch_utf8_bytes=64,
        max_passage_utf8_bytes=32,
        max_rendition_pack_bytes=64,
        max_document_bytes=64,
        max_oversized_document_bytes=1024,
        build_run_id=f"test-build-{selector}",
        created_at="2026-08-04T00:00:00Z",
        build_started_at="2026-08-04T00:00:00Z",
        build_completed_at="2026-08-04T00:00:01Z",
    )


def _active_input(
    root: Path,
    document_id: str,
    text: str,
    *,
    source_input_id: str | None = None,
    previous_active: bool = False,
    old_document_version_id: str | None = None,
    old_eligibility_state: str | None = None,
    eligibility_state: str = "eligible",
) -> SourceInput:
    filename = hashlib.sha256(document_id.encode()).hexdigest()[:12] + ".txt"
    path = root / filename
    path.write_text(text, encoding="utf-8")
    return SourceInput(
        document_id=document_id,
        source_input_id=source_input_id or f"input:{document_id}",
        source_id="pytest-source",
        source_partition="test",
        disposition="active",
        previous_active=previous_active,
        old_document_version_id=old_document_version_id,
        old_eligibility_state=old_eligibility_state,
        source_record_id=document_id,
        source_version="1",
        rendition_path=path,
        media_type="text/plain; charset=utf-8",
        title=f"Title for {document_id}",
        document_type="test-document",
        language="en",
        eligibility_state=eligibility_state,
        eligibility_authority_id="pytest-authority",
        eligibility_evidence_kind="deterministic-policy",
        eligibility_basis="pytest evidence",
        eligibility_reason_code=f"spicyregs.eligibility.{eligibility_state}",
    )


def _reseal_partition_manifest(release: Path, mutate: object) -> None:
    root_path = release / "release.json"
    root = parse_canonical_json(root_path.read_bytes(), label="release.json")
    reference = root["content"]["partitionManifests"][0]
    manifest_path = release / reference["objectKey"]
    manifest = parse_canonical_json(manifest_path.read_bytes(), label=reference["objectKey"])
    mutate(manifest)
    manifest["members"].sort(key=lambda member: member["objectKey"])
    manifest["counts"] = {
        "memberCount": len(manifest["members"]),
        "totalByteSize": sum(member["byteSize"] for member in manifest["members"]),
        "totalRecordCount": sum(member["recordCount"] or 0 for member in manifest["members"]),
    }
    manifest_path.write_bytes(canonical_json_bytes(manifest))
    reference["byteSize"] = manifest_path.stat().st_size
    reference["sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    root["releaseId"] = release_id(root["content"])
    root_path.write_bytes(canonical_json_bytes(root))


def _rewrite_passage_member(release: Path, mutate_rows: object) -> None:
    passage_path = release_member_paths(release, "passages")[0]
    rows = pq.read_table(passage_path).to_pylist()
    mutate_rows(rows)
    table = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[PASSAGES_SCHEMA_ID])
    pq.write_table(table, passage_path, compression="zstd", version="2.6")

    def update_descriptor(manifest: dict[str, object]) -> None:
        descriptor = next(member for member in manifest["members"] if member["role"] == "passages")
        descriptor["byteSize"] = passage_path.stat().st_size
        descriptor["sha256"] = hashlib.sha256(passage_path.read_bytes()).hexdigest()
        descriptor["recordCount"] = len(rows)

    _reseal_partition_manifest(release, update_descriptor)


def test_checked_in_fixture_is_a_complete_valid_distribution() -> None:
    result = verify_release_or_raise(SEALED_FIXTURE)

    assert result.code is VerificationCode.VALID
    assert result.release_id == (
        "urn:spicyregs:document-release:v3:6782ade513917b0285c59e861c6b0e94446a5618dc8a1ae769af7344821ad2bb"
    )
    assert result.counts["activeDocumentCount"] == 4
    assert result.counts["passageCount"] == 4


def test_fixture_passages_reverse_to_normalized_text_and_rendition_bytes() -> None:
    documents = {
        row["document_version_id"]: row
        for path in release_member_paths(SEALED_FIXTURE, "documents")
        for row in pq.read_table(path).to_pylist()
    }
    pack_path = release_member_paths(SEALED_FIXTURE, "rendition-pack")[0]
    index_path = release_member_paths(SEALED_FIXTURE, "rendition-pack-index")[0]
    pack = pack_path.read_bytes()
    renditions = {
        row["rendition_digest"]: pack[row["byte_offset"] : row["byte_offset"] + row["byte_length"]]
        for row in pq.read_table(index_path).to_pylist()
    }

    for path in release_member_paths(SEALED_FIXTURE, "passages"):
        for passage in pq.read_table(path).to_pylist():
            document = documents[passage["document_version_id"]]
            start = passage["normalized_start_utf8_byte"]
            end = passage["normalized_end_utf8_byte"]
            assert document["normalized_text"].encode("utf-8")[start:end].decode("utf-8") == passage["text"]
            coordinate = json.loads(passage["coordinate_data"])
            rendition = renditions[document["rendition_digest"]]
            assert rendition[coordinate["startUtf8Byte"] : coordinate["endUtf8Byte"]].decode("utf-8") == passage["text"]


def test_identity_excludes_annotations_and_canonical_parser_rejects_invalid_json() -> None:
    root = parse_canonical_json((SEALED_FIXTURE / "release.json").read_bytes(), label="release.json")
    original_digest = artifact_digest(root["content"])
    root["annotations"]["operatorNote"] = "annotation does not change identity"

    assert artifact_digest(root["content"]) == original_digest
    with pytest.raises(DocumentReleaseV3Error, match="floating-point"):
        canonical_json_bytes({"value": 1.5})
    with pytest.raises(DocumentReleaseV3Error, match="duplicate JSON key"):
        parse_canonical_json(b'{"a":1,"a":2}', label="duplicate")
    with pytest.raises(DocumentReleaseV3Error, match="not canonical"):
        parse_canonical_json(b'{"b":2, "a":1}', label="noncanonical")


def test_verifier_accepts_annotation_change_without_identity_change(tmp_path: Path) -> None:
    release = tmp_path / "release"
    shutil.copytree(SEALED_FIXTURE, release)
    root_path = release / "release.json"
    root = parse_canonical_json(root_path.read_bytes(), label="release.json")
    root["annotations"]["operatorNote"] = "fixture copy"
    root_path.write_bytes(canonical_json_bytes(root))

    assert verify_release_or_raise(release).release_id == root["releaseId"]


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing", VerificationCode.MEMBERSHIP_MISSING),
        ("extra", VerificationCode.MEMBERSHIP_EXTRA),
        ("digest", VerificationCode.MEMBER_DIGEST),
    ],
)
def test_verifier_returns_registered_membership_codes(
    tmp_path: Path, mutation: str, expected_code: VerificationCode
) -> None:
    release = tmp_path / mutation
    shutil.copytree(SEALED_FIXTURE, release)
    document_member = release_member_paths(release, "documents")[0]
    if mutation == "missing":
        document_member.unlink()
    elif mutation == "extra":
        (release / "undeclared.bin").write_bytes(b"extra")
    else:
        with document_member.open("ab") as stream:
            stream.write(b"changed")

    assert verify_release(release).code is expected_code


def test_verifier_rejects_unknown_exact_version_before_partial_read(tmp_path: Path) -> None:
    release = tmp_path / "unknown-version"
    shutil.copytree(SEALED_FIXTURE, release)
    root_path = release / "release.json"
    root = parse_canonical_json(root_path.read_bytes(), label="release.json")
    root["formatVersion"] = "3.1"
    root_path.write_bytes(canonical_json_bytes(root))

    assert verify_release(release).code is VerificationCode.FORMAT


def test_verifier_rejects_escaped_member_path_with_registered_code(tmp_path: Path) -> None:
    release = tmp_path / "escaped"
    shutil.copytree(SEALED_FIXTURE, release)

    def escape_path(manifest: dict[str, object]) -> None:
        manifest["members"][0]["objectKey"] = "../escaped.parquet"

    _reseal_partition_manifest(release, escape_path)
    assert verify_release(release).code is VerificationCode.PATH


def test_verifier_rejects_broken_rendition_coordinate_after_member_reseal(tmp_path: Path) -> None:
    release = tmp_path / "coordinate"
    shutil.copytree(SEALED_FIXTURE, release)

    def break_coordinate(rows: list[dict[str, object]]) -> None:
        coordinate = json.loads(rows[0]["coordinate_data"])
        coordinate["endUtf8Byte"] -= 1
        rows[0]["coordinate_data"] = canonical_json_text(coordinate)

    _rewrite_passage_member(release, break_coordinate)
    assert verify_release(release).code is VerificationCode.COORDINATE


def test_verifier_rejects_duplicate_passage_identity_after_member_reseal(tmp_path: Path) -> None:
    release = tmp_path / "duplicate-passage"
    shutil.copytree(SEALED_FIXTURE, release)

    def duplicate_identity(rows: list[dict[str, object]]) -> None:
        rows[1]["passage_id"] = rows[0]["passage_id"]

    _rewrite_passage_member(release, duplicate_identity)
    assert verify_release(release).code is VerificationCode.DUPLICATE_IDENTITY


def test_bounded_builder_accounts_every_disposition_and_refuses_replacement(tmp_path: Path) -> None:
    active = _active_input(tmp_path, "doc:active", "active text with a bounded passage")
    deleted = SourceInput(
        document_id="doc:deleted",
        source_input_id=None,
        source_id="pytest-source",
        source_partition="test",
        disposition="deleted",
        previous_active=True,
    )
    excluded = SourceInput(
        document_id="doc:excluded",
        source_input_id="input:excluded",
        source_id="pytest-source",
        source_partition="test",
        disposition="excluded",
        exclusion_policy_id="spicyregs.exclusion.test.v1",
    )
    failed = SourceInput(
        document_id="doc:failed",
        source_input_id="input:failed",
        source_id="pytest-source",
        source_partition="test",
        disposition="accepted-failure",
        failure_id="failure:test",
        failure_stage="acquire",
        failure_class="deterministic-input",
        failure_retryable=False,
        failure_attempt_count=1,
        failure_diagnostic_code="spicyregs.acquire.invalid-input",
        failure_final_disposition="accepted-terminal",
    )
    output = build_release([active, deleted, excluded, failed], tmp_path / "release", _config(selector="all"))
    result = verify_release_or_raise(output)

    assert result.counts["reconciliationUniverseCount"] == 4
    assert result.counts["selectedDocumentCount"] == 3
    assert result.counts["activeDocumentCount"] == 1
    assert result.counts["deletedDocumentCount"] == 1
    assert result.counts["excludedDocumentCount"] == 1
    assert result.counts["acceptedTerminalFailureCount"] == 1
    with pytest.raises(DocumentReleaseV3Error, match="refusing to replace"):
        build_release([active], output, _config(selector="replacement"))


def test_builder_deduplicates_identical_rendition_bytes_exactly(tmp_path: Path) -> None:
    first = _active_input(tmp_path, "doc:first", "identical rendition")
    second = _active_input(tmp_path, "doc:second", "identical rendition")
    output = build_release([first, second], tmp_path / "deduplicated", _config(selector="deduplicated"))
    result = verify_release_or_raise(output)

    assert result.counts["activeDocumentCount"] == 2
    assert result.counts["renditionCount"] == 1
    root = parse_canonical_json((output / "release.json").read_bytes(), label="release.json")
    assert root["content"]["coverage"]["renditionByteCount"] == len("identical rendition".encode())


def test_release_with_only_accounted_exclusions_needs_no_rendition_pack(tmp_path: Path) -> None:
    excluded = SourceInput(
        document_id="doc:excluded-only",
        source_input_id="input:excluded-only",
        source_id="pytest-source",
        source_partition="test",
        disposition="excluded",
        exclusion_policy_id="spicyregs.exclusion.test.v1",
    )
    output = build_release([excluded], tmp_path / "excluded-only", _config(selector="excluded-only"))
    result = verify_release_or_raise(output)

    assert result.counts["activeDocumentCount"] == 0
    assert result.counts["excludedDocumentCount"] == 1
    assert result.counts["renditionCount"] == 0
    assert release_member_paths(output, "rendition-pack") == ()


def test_incremental_diff_reports_add_update_and_delete(tmp_path: Path) -> None:
    previous = build_release(
        [
            _active_input(tmp_path, "doc:updated", "old text"),
            _active_input(tmp_path, "doc:deleted", "removed text"),
        ],
        tmp_path / "previous",
        _config(selector="previous"),
    )
    previous_result = verify_release_or_raise(previous)
    previous_ids = active_identity_map(previous)
    current = build_release(
        [
            _active_input(
                tmp_path,
                "doc:updated",
                "new text",
                previous_active=True,
                old_document_version_id=previous_ids["doc:updated"],
                old_eligibility_state="eligible",
            ),
            SourceInput(
                document_id="doc:deleted",
                source_input_id=None,
                source_id="pytest-source",
                source_partition="test",
                disposition="deleted",
                previous_active=True,
                old_document_version_id=previous_ids["doc:deleted"],
            ),
            _active_input(tmp_path, "doc:added", "new document"),
        ],
        tmp_path / "current",
        _config(
            selector="current",
            previous_release_id=previous_result.release_id,
            previous_artifact_digest=previous_result.artifact_digest,
        ),
    )

    changes = list(iter_release_diff(previous, current))
    assert [(row["document_id"], row["change_kind"]) for row in changes] == [
        ("doc:added", "add"),
        ("doc:deleted", "delete"),
        ("doc:updated", "update"),
    ]


def test_compaction_reseals_and_preserves_active_logical_identities(tmp_path: Path) -> None:
    before = compaction_metrics(SEALED_FIXTURE)
    compacted = compact_release(SEALED_FIXTURE, tmp_path / "compacted")
    after = compaction_metrics(compacted)

    assert before.inactive_ratio == 0
    assert verify_release_or_raise(compacted).code is VerificationCode.VALID
    assert active_identity_map(compacted) == active_identity_map(SEALED_FIXTURE)
    assert after.inactive_document_versions == 0
    assert after.inactive_eligibility_rows == 0
    assert after.inactive_passages == 0


def test_cli_verifies_fixture_and_writes_sidecar_receipt(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    receipt = tmp_path / "verification.json"
    exit_code = cli_main(
        [
            "document-release-v3",
            "verify",
            str(SEALED_FIXTURE),
            "--receipt",
            str(receipt),
            "--memory-limit",
            "256MB",
        ]
    )

    assert exit_code == 0
    assert json.loads(receipt.read_text())["verificationCode"] == "valid"
    assert json.loads(capsys.readouterr().out)["verdict"] == "pass"


def test_source_fixture_selection_policy_digest_is_pinned() -> None:
    policy_digest = hashlib.sha256((SOURCE_FIXTURE / "selection-policy.json").read_bytes()).hexdigest()
    root = parse_canonical_json((SEALED_FIXTURE / "release.json").read_bytes(), label="release.json")

    assert root["content"]["sourceSelection"]["selectorDigest"] == policy_digest

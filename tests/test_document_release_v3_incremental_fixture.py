"""Checked-in mixed incremental ``DocumentRelease`` v3 evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.cli import main as cli_main
from spicy_regs.document_release_v3_diff import iter_release_diff, release_member_paths
from spicy_regs.document_release_v3_verify import verify_release_or_raise


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_BASE_RELEASE = _REPOSITORY_ROOT / "fixtures" / "releases" / "document-release-v3"
_MIXED_RELEASE = _REPOSITORY_ROOT / "fixtures" / "releases" / "document-release-v3-incremental-mixed"
_SOURCE_FIXTURE = Path(__file__).with_name("fixtures") / "document-release-v3-incremental-mixed-source"
_BASE_DIGEST = "6782ade513917b0285c59e861c6b0e94446a5618dc8a1ae769af7344821ad2bb"
_MIXED_DIGEST = "487efddedd7c45311b7dcf8d5283c9d8851f2f13ff9b4edc237ce3e1cde114e8"
_ROOT_SHA256 = "b38a6482aa05f32f2d8e71a4cc315da990091ced07ad937d82ef0f1cb4728707"


def _distribution_files(root: Path) -> dict[Path, bytes]:
    return {path.relative_to(root): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def test_mixed_incremental_fixture_pins_its_base_and_exact_change_set() -> None:
    base = verify_release_or_raise(_BASE_RELEASE)
    current = verify_release_or_raise(_MIXED_RELEASE)

    assert base.artifact_digest == _BASE_DIGEST
    assert current.artifact_digest == _MIXED_DIGEST
    assert hashlib.sha256((_MIXED_RELEASE / "release.json").read_bytes()).hexdigest() == _ROOT_SHA256
    assert current.counts == {
        "acceptedTerminalFailureCount": 0,
        "activeDocumentCount": 4,
        "deletedDocumentCount": 1,
        "documentVersionCount": 4,
        "eligibilityEvidenceCount": 4,
        "excludedDocumentCount": 0,
        "failureRecordCount": 0,
        "memberCount": 25,
        "partitionManifestCount": 1,
        "passageCount": 4,
        "previousActiveDocumentCount": 4,
        "reconciliationUniverseCount": 5,
        "renditionCount": 4,
        "selectedDocumentCount": 4,
        "sourceDispositionCount": 5,
        "totalMemberByteSize": 53737,
    }

    sealed_changes = [
        row for path in release_member_paths(_MIXED_RELEASE, "changes") for row in pq.read_table(path).to_pylist()
    ]
    exact_changes = list(iter_release_diff(_BASE_RELEASE, _MIXED_RELEASE))
    assert sorted(sealed_changes, key=lambda row: row["document_id"]) == exact_changes
    assert {row["change_kind"] for row in sealed_changes} == {"add", "delete", "update"}


def test_mixed_incremental_fixture_rebuilds_from_committed_sources(tmp_path: Path) -> None:
    rebuilt = tmp_path / "rebuilt"

    exit_code = cli_main(
        [
            "document-release-v3",
            "build",
            "--input",
            str(_SOURCE_FIXTURE / "selection.jsonl"),
            "--output",
            str(rebuilt),
            "--selection-policy",
            str(_SOURCE_FIXTURE / "selection-policy.json"),
            "--selection-id",
            "spicyregs.fixture-selection.incremental-mixed.v1",
            "--selector-type",
            "closed-jsonl-ledger",
            "--effective-at",
            "2026-08-04T01:00:00Z",
            "--previous-release",
            str(_BASE_RELEASE),
            "--partition-id",
            "fixture",
            "--implementation-id",
            "spicyregs.document-release-v3.reference",
            "--implementation-version",
            "1.0",
            "--runtime-profile-id",
            "fixture-local-python-3.12",
            "--processing-policy-id",
            "spicyregs.processing.fixture.v1",
            "--normalizer-id",
            "spicyregs.normalizer.utf8-identity.v1",
            "--segmenter-id",
            "spicyregs.segmenter.utf8-bounded.v1",
            "--rendition-policy-id",
            "spicyregs.rendition.pack.v1",
            "--eligibility-policy-id",
            "spicyregs.eligibility.fixture.v1",
            "--failure-policy-id",
            "spicyregs.failure.fixture.v1",
            "--diagnostic-registry-id",
            "spicyregs.diagnostics.fixture.v1",
            "--row-batch-size",
            "2000",
            "--row-batch-utf8-bytes",
            "16777216",
            "--max-passage-utf8-bytes",
            "1048576",
            "--max-rendition-pack-bytes",
            "536870912",
            "--max-document-bytes",
            "67108864",
            "--max-oversized-document-bytes",
            "1073741824",
            "--compression",
            "zstd",
            "--memory-limit",
            "512MB",
            "--build-run-id",
            "document-release-v3-incremental-mixed-fixture-v1",
            "--created-at",
            "2026-08-04T01:00:00Z",
            "--build-started-at",
            "2026-08-04T01:00:00Z",
            "--build-completed-at",
            "2026-08-04T01:00:01Z",
        ]
    )

    assert exit_code == 0
    rebuilt_result = verify_release_or_raise(rebuilt)
    assert rebuilt_result.artifact_digest == _MIXED_DIGEST

    expected_files = _distribution_files(_MIXED_RELEASE)
    rebuilt_files = _distribution_files(rebuilt)
    assert rebuilt_files.keys() == expected_files.keys()
    assert [path for path in expected_files if rebuilt_files[path] != expected_files[path]] == []

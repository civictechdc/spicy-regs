"""Command-line surface for building and maintaining ``DocumentRelease`` v3."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.document_release_v3 import DocumentReleaseV3Error, canonical_json_bytes
from spicy_regs.document_release_v3_compact import compact_release, compaction_metrics
from spicy_regs.document_release_v3_diff import write_release_diff
from spicy_regs.document_release_v3_verify import verify_release, verify_release_or_raise
from spicy_regs.document_release_v3_writer import BuildConfig, build_release_from_jsonl


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _selector_digest(args: argparse.Namespace) -> str:
    if args.selector_digest is not None:
        return args.selector_digest
    if args.selection_policy is None:
        raise DocumentReleaseV3Error("build requires --selector-digest or --selection-policy")
    return hashlib.sha256(Path(args.selection_policy).read_bytes()).hexdigest()


def _previous_identity(path: Path | None, *, memory_limit: str) -> tuple[str | None, str | None]:
    if path is None:
        return None, None
    result = verify_release_or_raise(path, memory_limit=memory_limit)
    return result.release_id, result.artifact_digest


def _cmd_build(args: argparse.Namespace) -> int:
    previous_id, previous_digest = _previous_identity(args.previous_release, memory_limit=args.memory_limit)
    config = BuildConfig(
        implementation_id=args.implementation_id,
        implementation_version=args.implementation_version,
        runtime_profile_id=args.runtime_profile_id,
        source_revision=args.source_revision,
        processing_policy_id=args.processing_policy_id,
        normalizer_id=args.normalizer_id,
        segmenter_id=args.segmenter_id,
        rendition_policy_id=args.rendition_policy_id,
        eligibility_policy_id=args.eligibility_policy_id,
        failure_policy_id=args.failure_policy_id,
        diagnostic_registry_id=args.diagnostic_registry_id,
        selection_id=args.selection_id,
        selector_type=args.selector_type,
        selector_digest=_selector_digest(args),
        effective_at=args.effective_at,
        partition_id=args.partition_id,
        previous_release_id=previous_id,
        previous_artifact_digest=previous_digest,
        row_batch_size=args.row_batch_size,
        row_batch_utf8_bytes=args.row_batch_utf8_bytes,
        max_passage_utf8_bytes=args.max_passage_utf8_bytes,
        max_rendition_pack_bytes=args.max_rendition_pack_bytes,
        max_document_bytes=args.max_document_bytes,
        max_oversized_document_bytes=args.max_oversized_document_bytes,
        compression=args.compression,
        build_run_id=args.build_run_id,
        created_at=args.created_at,
        build_started_at=args.build_started_at,
        build_completed_at=args.build_completed_at,
    )
    output = build_release_from_jsonl(
        args.input,
        args.output,
        config,
        verifier_memory_limit=args.memory_limit,
    )
    result = verify_release_or_raise(output, memory_limit=args.memory_limit)
    _print_json({"output": str(output), **result.as_dict()})
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    result = verify_release(args.release, memory_limit=args.memory_limit)
    value = result.as_dict()
    if args.receipt is not None:
        receipt = Path(args.receipt).resolve()
        if receipt.exists():
            raise DocumentReleaseV3Error(f"refusing to replace existing verification receipt: {receipt}")
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_bytes(canonical_json_bytes(value))
    _print_json(value)
    return 0 if result.verdict == "pass" else 1


def _cmd_diff(args: argparse.Namespace) -> int:
    output = write_release_diff(
        args.previous,
        args.current,
        args.output,
        row_batch_size=args.row_batch_size,
        row_batch_utf8_bytes=args.row_batch_utf8_bytes,
        memory_limit=args.memory_limit,
    )
    import pyarrow.parquet as pq

    _print_json({"output": str(output), "recordCount": pq.ParquetFile(output).metadata.num_rows})
    return 0


def _cmd_compact(args: argparse.Namespace) -> int:
    before = compaction_metrics(
        args.source,
        delta_generations=args.delta_generations,
        memory_limit=args.memory_limit,
    )
    output = compact_release(
        args.source,
        args.output,
        row_batch_size=args.row_batch_size,
        row_batch_utf8_bytes=args.row_batch_utf8_bytes,
        memory_limit=args.memory_limit,
    )
    result = verify_release_or_raise(output, memory_limit=args.memory_limit)
    after = compaction_metrics(output, delta_generations=0, memory_limit=args.memory_limit)
    _print_json(
        {
            "output": str(output),
            "sourceMetrics": before.as_dict(),
            "compactedMetrics": after.as_dict(),
            **result.as_dict(),
        }
    )
    return 0


def add_document_release_v3_parser(subparsers: argparse._SubParsersAction) -> None:
    """Register the nested v3 command family on the main SpicyRegs parser."""

    parser = subparsers.add_parser(
        "document-release-v3",
        help="Build, verify, diff, and compact sealed DocumentRelease v3 distributions",
    )
    commands = parser.add_subparsers(dest="document_release_v3_command", required=True)

    build = commands.add_parser("build", help="Build and atomically seal a release from a JSON Lines selection")
    build.add_argument("--input", type=Path, required=True, help="Closed JSON Lines source-selection ledger")
    build.add_argument("--output", type=Path, required=True, help="New release directory")
    build.add_argument("--selection-policy", type=Path, help="Immutable selection-policy bytes to hash")
    build.add_argument("--selector-digest", help="Precomputed lowercase SHA-256 selection-policy digest")
    build.add_argument("--selection-id", required=True)
    build.add_argument("--selector-type", required=True)
    build.add_argument("--effective-at", required=True, help="Selection instant in RFC 3339 UTC form")
    build.add_argument("--previous-release", type=Path, help="Verified v3 predecessor used for change detection")
    build.add_argument("--partition-id", default="default")
    build.add_argument("--implementation-id", default="spicyregs.document-release-v3.reference")
    build.add_argument("--implementation-version", default="1.0")
    build.add_argument("--runtime-profile-id", default="local-python-3.12")
    build.add_argument("--source-revision")
    build.add_argument("--processing-policy-id", default="spicyregs.processing.document-release-v3.v1")
    build.add_argument("--normalizer-id", default="spicyregs.normalizer.utf8-identity.v1")
    build.add_argument("--segmenter-id", default="spicyregs.segmenter.utf8-bounded.v1")
    build.add_argument("--rendition-policy-id", default="spicyregs.rendition.pack.v1")
    build.add_argument("--eligibility-policy-id", default="spicyregs.eligibility.release.v1")
    build.add_argument("--failure-policy-id", default="spicyregs.failure.release.v1")
    build.add_argument("--diagnostic-registry-id", default="spicyregs.diagnostics.release.v1")
    build.add_argument("--row-batch-size", type=int, default=2_000)
    build.add_argument("--row-batch-utf8-bytes", type=int, default=16 * 1024 * 1024)
    build.add_argument("--max-passage-utf8-bytes", type=int, default=1 * 1024 * 1024)
    build.add_argument("--max-rendition-pack-bytes", type=int, default=512 * 1024 * 1024)
    build.add_argument("--max-document-bytes", type=int, default=64 * 1024 * 1024)
    build.add_argument("--max-oversized-document-bytes", type=int, default=1 * 1024 * 1024 * 1024)
    build.add_argument("--compression", default="zstd")
    build.add_argument("--memory-limit", default="512MB")
    build.add_argument("--build-run-id", default="document-release-v3-cli")
    build.add_argument("--created-at", required=True, help="Annotation instant in RFC 3339 UTC form")
    build.add_argument("--build-started-at", help="Optional reproducible receipt start instant")
    build.add_argument("--build-completed-at", help="Optional reproducible receipt completion instant")
    build.set_defaults(func=_cmd_build)

    verify = commands.add_parser("verify", help="Fail-closed verification of a complete materialized release")
    verify.add_argument("release", type=Path)
    verify.add_argument("--receipt", type=Path, help="Write a sidecar machine-readable verification result")
    verify.add_argument("--memory-limit", default="512MB")
    verify.set_defaults(func=_cmd_verify)

    diff = commands.add_parser("diff", help="Write the exact active-set change table between releases")
    diff.add_argument("--previous", type=Path, required=True)
    diff.add_argument("--current", type=Path, required=True)
    diff.add_argument("--output", type=Path, required=True)
    diff.add_argument("--row-batch-size", type=int, default=2_000)
    diff.add_argument("--row-batch-utf8-bytes", type=int, default=16 * 1024 * 1024)
    diff.add_argument("--memory-limit", default="512MB")
    diff.set_defaults(func=_cmd_diff)

    compact = commands.add_parser("compact", help="Remove inactive rows and atomically reseal a release")
    compact.add_argument("--source", type=Path, required=True)
    compact.add_argument("--output", type=Path, required=True)
    compact.add_argument("--delta-generations", type=int)
    compact.add_argument("--row-batch-size", type=int, default=2_000)
    compact.add_argument("--row-batch-utf8-bytes", type=int, default=16 * 1024 * 1024)
    compact.add_argument("--memory-limit", default="512MB")
    compact.set_defaults(func=_cmd_compact)

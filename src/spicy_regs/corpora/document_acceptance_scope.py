"""Build an immutable document-only view over a segmentation dataset.

The source dataset remains a reusable all-profile corpus.  This module records
which source artifacts are documents evaluated by the production segmentation
goal and which artifacts are retained only as relationship/entity context or
generic public-comment support.  Downstream experiments consume the resulting
membership artifact instead of reimplementing source-table exclusions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq

from spicy_regs.corpora.segmentation_evaluation import (
    validate_segmentation_evaluation,
)
from spicy_regs.ontology.common import (
    canonical_json,
    normalize_row,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.subjects import (
    SUBJECT_PROFILES,
    Artifact,
    build_artifacts,
)

FORMAT_VERSION = 1
SCOPE_POLICY_VERSION = "document-acceptance-v1"
AcceptanceRole = Literal[
    "document",
    "relationship-context",
    "entity-context",
    "public-comment",
]

ARTIFACT_MEMBERSHIP_COLUMNS = (
    "profile_id",
    "source_table",
    "subject_type",
    "subject_id",
    "artifact_digest",
    "acceptance_role",
    "included",
    "rationale",
)
GOLD_MEMBERSHIP_COLUMNS = (
    "gold_id",
    "profile_id",
    "subject_type",
    "subject_id",
    "artifact_digest",
    "included",
    "exclusion_reason",
)
ADVERSARIAL_MEMBERSHIP_COLUMNS = (
    "case_id",
    "kind",
    "profile_id",
    "subject_type",
    "subject_id",
    "artifact_digest",
    "included",
    "exclusion_reason",
)


@dataclass(frozen=True)
class ProfileAcceptancePolicy:
    """One explicit source-profile role in a general document evaluation."""

    profile_id: str
    acceptance_role: AcceptanceRole
    rationale: str

    @property
    def included(self) -> bool:
        return self.acceptance_role == "document"


PROFILE_ACCEPTANCE_POLICIES = (
    ProfileAcceptancePolicy(
        "regulations-docket-v2",
        "relationship-context",
        "A docket groups source documents but is not evaluated as document text.",
    ),
    ProfileAcceptancePolicy(
        "regulations-document-v2",
        "document",
        "A source-issued regulation, notice, supporting file, or other document.",
    ),
    ProfileAcceptancePolicy(
        "regulations-comment-v1",
        "public-comment",
        "Public-comment records remain supported but are outside this acceptance run.",
    ),
    ProfileAcceptancePolicy(
        "federal-register-document-v1",
        "document",
        "A Federal Register notice, rule, proposed rule, or presidential document.",
    ),
    ProfileAcceptancePolicy(
        "unified-agenda-observation-v1",
        "document",
        "A dated regulatory-agenda record is an evaluated source document view.",
    ),
    ProfileAcceptancePolicy(
        "cfr-section-v1",
        "document",
        "A versioned CFR section is an evaluated legal document unit.",
    ),
    ProfileAcceptancePolicy(
        "congress-bill-v1",
        "document",
        "A bill version and its source-native hierarchy are legislative documents.",
    ),
    ProfileAcceptancePolicy(
        "sam-entity-v1",
        "entity-context",
        "An entity registry row may contextualize documents but is not one.",
    ),
    ProfileAcceptancePolicy(
        "lobbying-filing-v1",
        "document",
        "A lobbying disclosure filing is an evaluated source document.",
    ),
    ProfileAcceptancePolicy(
        "fec-committee-v1",
        "entity-context",
        "A committee registry row may contextualize filings but is not one.",
    ),
    ProfileAcceptancePolicy(
        "gao-report-v1",
        "document",
        "A GAO report is an evaluated public-sector document.",
    ),
    ProfileAcceptancePolicy(
        "crs-report-v1",
        "document",
        "A CRS report is an evaluated legislative research document.",
    ),
    ProfileAcceptancePolicy(
        "court-opinion-v1",
        "document",
        "A separately identified judicial opinion package is an evaluated legal document.",
    ),
    ProfileAcceptancePolicy(
        "court-docket-v1",
        "relationship-context",
        "A court docket identifies a case; an opinion must remain a separate artifact.",
    ),
    ProfileAcceptancePolicy(
        "usaspending-recipient-v1",
        "entity-context",
        "A recipient registry row may contextualize awards but is not a document.",
    ),
    ProfileAcceptancePolicy(
        "fcc-proceeding-v1",
        "relationship-context",
        "An FCC proceeding groups filings but is not evaluated as document text.",
    ),
    ProfileAcceptancePolicy(
        "fcc-filing-v1",
        "document",
        "An FCC submission is evaluated as a source filing, not as proceeding identity.",
    ),
)
_POLICY_BY_PROFILE = {
    policy.profile_id: policy for policy in PROFILE_ACCEPTANCE_POLICIES
}


@dataclass(frozen=True)
class DocumentAcceptanceScope:
    """Validated IDs selected by one immutable scope artifact."""

    scope_id: str
    scope_policy_version: str
    dataset_evaluation_id: str
    included_artifact_digests: frozenset[str]
    included_gold_ids: frozenset[str]
    included_adversarial_case_ids: frozenset[str]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_hashes(directory: Path) -> dict[str, dict[str, Any]]:
    return {
        path.name: {
            "rows": pq.ParquetFile(path).metadata.num_rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(directory.glob("*.parquet"))
    }


def _profile_policy_rows() -> list[dict[str, str]]:
    return [
        {
            "profile_id": policy.profile_id,
            "acceptance_role": policy.acceptance_role,
            "rationale": policy.rationale,
        }
        for policy in PROFILE_ACCEPTANCE_POLICIES
    ]


def _policy_digest() -> str:
    return hashlib.sha256(
        canonical_json(_profile_policy_rows()).encode()
    ).hexdigest()


def _profile_policy_failures() -> list[str]:
    live = {profile.profile_id for profile in SUBJECT_PROFILES}
    declared = set(_POLICY_BY_PROFILE)
    failures: list[str] = []
    if live - declared:
        failures.append(
            "unclassified live profiles: " + ", ".join(sorted(live - declared))
        )
    if declared - live:
        failures.append(
            "scope policies reference absent profiles: "
            + ", ".join(sorted(declared - live))
        )
    if len(declared) != len(PROFILE_ACCEPTANCE_POLICIES):
        failures.append("scope policies contain a duplicate profile")
    return failures


def _membership_rows(
    artifacts: Sequence[Artifact],
    gold_rows: Sequence[dict[str, Any]],
    adversarial_rows: Sequence[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    profile_failures = _profile_policy_failures()
    if profile_failures:
        raise RuntimeError("; ".join(profile_failures))
    artifacts_by_digest = {artifact.digest: artifact for artifact in artifacts}
    if len(artifacts_by_digest) != len(artifacts):
        raise RuntimeError("dataset artifacts contain a duplicate digest")
    artifacts_by_subject: dict[tuple[str, str, str], Artifact] = {}
    artifact_rows: list[dict[str, Any]] = []
    for artifact in artifacts:
        key = (
            artifact.profile_id,
            artifact.subject_type,
            artifact.subject_id,
        )
        if key in artifacts_by_subject:
            raise RuntimeError(f"duplicate source artifact identity: {key}")
        artifacts_by_subject[key] = artifact
        policy = _POLICY_BY_PROFILE[artifact.profile_id]
        artifact_rows.append(
            {
                "profile_id": artifact.profile_id,
                "source_table": artifact.source_table,
                "subject_type": artifact.subject_type,
                "subject_id": artifact.subject_id,
                "artifact_digest": artifact.digest,
                "acceptance_role": policy.acceptance_role,
                "included": policy.included,
                "rationale": policy.rationale,
            }
        )

    gold_membership: list[dict[str, Any]] = []
    seen_gold: set[str] = set()
    for gold in gold_rows:
        gold_id = str(gold.get("gold_id") or "")
        if not gold_id or gold_id in seen_gold:
            raise RuntimeError("gold rows contain a missing or duplicate ID")
        seen_gold.add(gold_id)
        digest = str(gold.get("artifact_digest") or "")
        artifact = artifacts_by_digest.get(digest)
        if artifact is None:
            raise RuntimeError(f"{gold_id}: gold artifact is missing")
        if (
            str(gold.get("profile_id")) != artifact.profile_id
            or str(gold.get("subject_type")) != artifact.subject_type
            or str(gold.get("subject_id")) != artifact.subject_id
        ):
            raise RuntimeError(f"{gold_id}: gold artifact identity differs")
        included = _POLICY_BY_PROFILE[artifact.profile_id].included
        gold_membership.append(
            {
                "gold_id": gold_id,
                "profile_id": artifact.profile_id,
                "subject_type": artifact.subject_type,
                "subject_id": artifact.subject_id,
                "artifact_digest": artifact.digest,
                "included": included,
                "exclusion_reason": (
                    None if included else "artifact role is outside document scope"
                ),
            }
        )

    adversarial_membership: list[dict[str, Any]] = []
    seen_cases: set[str] = set()
    for case in adversarial_rows:
        case_id = str(case.get("case_id") or "")
        if not case_id or case_id in seen_cases:
            raise RuntimeError(
                "adversarial rows contain a missing or duplicate case ID"
            )
        seen_cases.add(case_id)
        key = (
            str(case.get("profile_id") or ""),
            str(case.get("subject_type") or ""),
            str(case.get("subject_id") or ""),
        )
        artifact = artifacts_by_subject.get(key)
        if artifact is None:
            raise RuntimeError(f"{case_id}: adversarial artifact is missing")
        included = _POLICY_BY_PROFILE[artifact.profile_id].included
        adversarial_membership.append(
            {
                "case_id": case_id,
                "kind": case.get("kind"),
                "profile_id": artifact.profile_id,
                "subject_type": artifact.subject_type,
                "subject_id": artifact.subject_id,
                "artifact_digest": artifact.digest,
                "included": included,
                "exclusion_reason": (
                    None if included else "artifact role is outside document scope"
                ),
            }
        )
    return (
        sorted(
            artifact_rows,
            key=lambda row: (
                str(row["profile_id"]),
                str(row["subject_id"]),
                str(row["artifact_digest"]),
            ),
        ),
        sorted(gold_membership, key=lambda row: str(row["gold_id"])),
        sorted(
            adversarial_membership,
            key=lambda row: str(row["case_id"]),
        ),
    )


def _stored_rows(
    rows: Sequence[dict[str, Any]],
    columns: Sequence[str],
) -> list[dict[str, str | None]]:
    return sorted(
        (normalize_row(row, columns) for row in rows),
        key=canonical_json,
    )


def _scope_id(
    dataset_evaluation_id: object,
    artifacts: dict[str, dict[str, Any]],
) -> str:
    identity = {
        "dataset_evaluation_id": dataset_evaluation_id,
        "scope_policy_version": SCOPE_POLICY_VERSION,
        "policy_sha256": _policy_digest(),
        "artifacts": {
            name: record["sha256"]
            for name, record in sorted(artifacts.items())
        },
    }
    return "document_scope_" + hashlib.sha256(
        canonical_json(identity).encode()
    ).hexdigest()[:24]


def _validate_document_acceptance_scope(
    dataset_dir: Path,
    scope_dir: Path,
    *,
    base_receipt: dict[str, Any] | None = None,
    dataset_artifacts: Sequence[Artifact] | None = None,
) -> dict[str, Any]:
    """Validate scope, optionally reusing a just-validated dataset snapshot."""
    if base_receipt is None:
        base_receipt = validate_segmentation_evaluation(dataset_dir)
    manifest_path = scope_dir / "document-acceptance-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Invalid document acceptance manifest: {manifest_path}"
        ) from exc
    if not isinstance(manifest, dict):
        raise RuntimeError("document acceptance manifest must be an object")
    failures = _profile_policy_failures()
    if base_receipt.get("status") != "pass":
        failures.append("base segmentation dataset did not validate")
    if manifest.get("format_version") != FORMAT_VERSION:
        failures.append("manifest format version differs")
    if manifest.get("scope_policy_version") != SCOPE_POLICY_VERSION:
        failures.append("manifest scope policy version differs")
    if manifest.get("dataset_evaluation_id") != base_receipt.get(
        "evaluation_id"
    ):
        failures.append("manifest dataset evaluation ID differs")
    if manifest.get("profile_policies") != _profile_policy_rows():
        failures.append("manifest profile policies differ")
    if manifest.get("policy_sha256") != _policy_digest():
        failures.append("manifest policy digest differs")

    artifacts = (
        list(dataset_artifacts)
        if dataset_artifacts is not None
        else build_artifacts(dataset_dir)
    )
    expected_artifacts, expected_gold, expected_adversarial = _membership_rows(
        artifacts,
        read_parquet_rows(dataset_dir / "gold_spans.parquet"),
        read_parquet_rows(dataset_dir / "adversarial_cases.parquet"),
    )
    artifact_rows = read_parquet_rows(
        scope_dir / "document-artifact-membership.parquet"
    )
    gold_rows = read_parquet_rows(
        scope_dir / "document-gold-membership.parquet"
    )
    adversarial_rows = read_parquet_rows(
        scope_dir / "document-adversarial-membership.parquet"
    )
    if _stored_rows(
        artifact_rows, ARTIFACT_MEMBERSHIP_COLUMNS
    ) != _stored_rows(expected_artifacts, ARTIFACT_MEMBERSHIP_COLUMNS):
        failures.append("artifact scope membership differs")
    if _stored_rows(gold_rows, GOLD_MEMBERSHIP_COLUMNS) != _stored_rows(
        expected_gold, GOLD_MEMBERSHIP_COLUMNS
    ):
        failures.append("gold scope membership differs")
    if _stored_rows(
        adversarial_rows, ADVERSARIAL_MEMBERSHIP_COLUMNS
    ) != _stored_rows(expected_adversarial, ADVERSARIAL_MEMBERSHIP_COLUMNS):
        failures.append("adversarial scope membership differs")

    artifact_hashes = _artifact_hashes(scope_dir)
    scope_id = _scope_id(base_receipt.get("evaluation_id"), artifact_hashes)
    if manifest.get("scope_id") != scope_id:
        failures.append("scope ID differs")
    if manifest.get("artifacts") != artifact_hashes:
        failures.append("scope artifact hashes differ")

    included_artifacts = {
        str(row["artifact_digest"])
        for row in artifact_rows
        if str(row.get("included")).casefold() == "true"
    }
    included_gold = {
        str(row["gold_id"])
        for row in gold_rows
        if str(row.get("included")).casefold() == "true"
    }
    included_adversarial = {
        str(row["case_id"])
        for row in adversarial_rows
        if str(row.get("included")).casefold() == "true"
    }
    if any(
        str(row.get("acceptance_role")) == "public-comment"
        and str(row.get("included")).casefold() == "true"
        for row in artifact_rows
    ):
        failures.append("public-comment artifact entered document scope")
    if any(
        str(row.get("artifact_digest")) not in included_artifacts
        for row in gold_rows
        if str(row.get("included")).casefold() == "true"
    ):
        failures.append("included gold references an excluded artifact")
    if any(
        str(row.get("artifact_digest")) not in included_artifacts
        for row in adversarial_rows
        if str(row.get("included")).casefold() == "true"
    ):
        failures.append("included adversarial case references an excluded artifact")

    artifact_role_counts = Counter(
        str(row.get("acceptance_role")) for row in artifact_rows
    )
    included_profile_counts = Counter(
        str(row.get("profile_id"))
        for row in artifact_rows
        if str(row.get("included")).casefold() == "true"
    )
    expected_counts = {
        "artifact_count": len(artifact_rows),
        "included_artifact_count": len(included_artifacts),
        "excluded_artifact_count": len(artifact_rows) - len(included_artifacts),
        "included_gold_count": len(included_gold),
        "excluded_gold_count": len(gold_rows) - len(included_gold),
        "included_adversarial_count": len(included_adversarial),
        "excluded_adversarial_count": (
            len(adversarial_rows) - len(included_adversarial)
        ),
    }
    if any(
        int(
            str(
                manifest[field]
                if manifest.get(field) is not None
                else -1
            )
        )
        != value
        for field, value in expected_counts.items()
    ):
        failures.append("manifest scope counts differ")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "scope_id": scope_id,
        "scope_policy_version": SCOPE_POLICY_VERSION,
        "dataset_evaluation_id": base_receipt.get("evaluation_id"),
        **expected_counts,
        "artifact_counts_by_role": dict(sorted(artifact_role_counts.items())),
        "included_artifact_counts_by_profile": dict(
            sorted(included_profile_counts.items())
        ),
        "failures": failures,
    }


def validate_document_acceptance_scope(
    dataset_dir: Path,
    scope_dir: Path,
) -> dict[str, Any]:
    """Recompute policy membership, hashes, identities, and exclusion gates."""
    return _validate_document_acceptance_scope(dataset_dir, scope_dir)


def build_document_acceptance_scope(
    dataset_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Materialize an immutable membership-only document evaluation view."""
    if output_dir.exists():
        raise FileExistsError(
            f"Refusing to replace document acceptance scope: {output_dir}"
        )
    base_receipt = validate_segmentation_evaluation(dataset_dir)
    if base_receipt.get("status") != "pass":
        raise RuntimeError("base segmentation dataset did not validate")
    profile_failures = _profile_policy_failures()
    if profile_failures:
        raise RuntimeError("; ".join(profile_failures))
    dataset_artifacts = build_artifacts(dataset_dir)
    artifact_rows, gold_rows, adversarial_rows = _membership_rows(
        dataset_artifacts,
        read_parquet_rows(dataset_dir / "gold_spans.parquet"),
        read_parquet_rows(dataset_dir / "adversarial_cases.parquet"),
    )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )
    try:
        write_parquet_rows(
            temporary / "document-artifact-membership.parquet",
            columns=ARTIFACT_MEMBERSHIP_COLUMNS,
            rows=artifact_rows,
        )
        write_parquet_rows(
            temporary / "document-gold-membership.parquet",
            columns=GOLD_MEMBERSHIP_COLUMNS,
            rows=gold_rows,
        )
        write_parquet_rows(
            temporary / "document-adversarial-membership.parquet",
            columns=ADVERSARIAL_MEMBERSHIP_COLUMNS,
            rows=adversarial_rows,
        )
        artifact_hashes = _artifact_hashes(temporary)
        included_artifact_count = sum(row["included"] for row in artifact_rows)
        included_gold_count = sum(row["included"] for row in gold_rows)
        included_adversarial_count = sum(
            row["included"] for row in adversarial_rows
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "scope_id": _scope_id(
                base_receipt.get("evaluation_id"), artifact_hashes
            ),
            "scope_policy_version": SCOPE_POLICY_VERSION,
            "dataset_evaluation_id": base_receipt.get("evaluation_id"),
            "purpose": (
                "Immutable document-only acceptance membership over a reusable "
                "all-profile segmentation dataset."
            ),
            "included_roles": ["document"],
            "profile_policies": _profile_policy_rows(),
            "policy_sha256": _policy_digest(),
            "artifact_count": len(artifact_rows),
            "included_artifact_count": included_artifact_count,
            "excluded_artifact_count": (
                len(artifact_rows) - included_artifact_count
            ),
            "included_gold_count": included_gold_count,
            "excluded_gold_count": len(gold_rows) - included_gold_count,
            "included_adversarial_count": included_adversarial_count,
            "excluded_adversarial_count": (
                len(adversarial_rows) - included_adversarial_count
            ),
            "artifacts": artifact_hashes,
        }
        (
            temporary / "document-acceptance-manifest.json"
        ).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = _validate_document_acceptance_scope(
            dataset_dir,
            temporary,
            base_receipt=base_receipt,
            dataset_artifacts=dataset_artifacts,
        )
        (
            temporary / "document-acceptance-receipt.json"
        ).write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError(
                "Document acceptance validation failed: "
                + "; ".join(receipt["failures"])
            )
        temporary.replace(output_dir)
        return receipt
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def load_document_acceptance_scope(
    dataset_dir: Path,
    scope_dir: Path,
) -> DocumentAcceptanceScope:
    """Load included IDs only after the scope and source dataset validate."""
    receipt = validate_document_acceptance_scope(dataset_dir, scope_dir)
    if receipt.get("status") != "pass":
        raise RuntimeError(
            "document acceptance scope did not validate: "
            + "; ".join(str(value) for value in receipt.get("failures", []))
        )
    artifacts = read_parquet_rows(
        scope_dir / "document-artifact-membership.parquet"
    )
    gold = read_parquet_rows(scope_dir / "document-gold-membership.parquet")
    adversarial = read_parquet_rows(
        scope_dir / "document-adversarial-membership.parquet"
    )
    return DocumentAcceptanceScope(
        scope_id=str(receipt["scope_id"]),
        scope_policy_version=str(receipt["scope_policy_version"]),
        dataset_evaluation_id=str(receipt["dataset_evaluation_id"]),
        included_artifact_digests=frozenset(
            str(row["artifact_digest"])
            for row in artifacts
            if str(row.get("included")).casefold() == "true"
        ),
        included_gold_ids=frozenset(
            str(row["gold_id"])
            for row in gold
            if str(row.get("included")).casefold() == "true"
        ),
        included_adversarial_case_ids=frozenset(
            str(row["case_id"])
            for row in adversarial
            if str(row.get("included")).casefold() == "true"
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("dataset_dir", type=Path)
    build.add_argument("output_dir", type=Path)
    validate = commands.add_parser("validate")
    validate.add_argument("dataset_dir", type=Path)
    validate.add_argument("scope_dir", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        result = build_document_acceptance_scope(
            args.dataset_dir,
            args.output_dir,
        )
    else:
        result = validate_document_acceptance_scope(
            args.dataset_dir,
            args.scope_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "fail":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

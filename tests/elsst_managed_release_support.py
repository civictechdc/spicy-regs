"""Source-derived ELSST R5/R6 managed-release support for Spicy tests."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any

from refspec.registry.elsst_acquisition import (
    ELSST_R5,
    ELSST_R6,
    ElsstReleaseSource,
    acquire_elsst_release,
)
from refspec.registry.elsst_managed_release import (
    ElsstCandidateGovernance,
    build_elsst_managed_release,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RULESPEC_ROOT = REPO_ROOT.parent / "rulespec"
FIXTURE_ROOT = REPO_ROOT / "RefSpec" / "tests" / "fixtures"
R5_FIXTURE = FIXTURE_ROOT / "elsst-projection-mini-r5.ttl"
R6_FIXTURE = FIXTURE_ROOT / "elsst-projection-mini-r6.ttl"

RECORDED_AT = "2026-07-29T20:00:00Z"
RECORDED_BY = "urn:test:agent:spicy-elsst-managed-release"

R6_RETIRED_MEMBER_ID = "https://elsst.cessda.eu/id/6/05fd5779-69ad-4872-ae25-a8c400b73e10"
R6_SUCCESSOR_MEMBER_ID = "https://elsst.cessda.eu/id/6/4ae8f7d8-3ff9-4258-9dc8-7cf9c345dd6f"
R5_SUCCESSOR_MEMBER_ID = "https://elsst.cessda.eu/id/5/4ae8f7d8-3ff9-4258-9dc8-7cf9c345dd6f"
STABLE_SUCCESSOR_ID = "https://elsst.cessda.eu/id/4ae8f7d8-3ff9-4258-9dc8-7cf9c345dd6f"
TEST_R5_RELEASE_IRI = "urn:test:spicy-regs:elsst:release:r5"
TEST_R6_RELEASE_IRI = "urn:test:spicy-regs:elsst:release:r6"


def _fixture_release(
    path: Path,
    published: ElsstReleaseSource,
    *,
    release_iri: str,
) -> ElsstReleaseSource:
    payload = path.read_bytes()
    return replace(
        published,
        release_iri=release_iri,
        source_url=(f"https://example.test/refspec/source-derived/{path.name}"),
        expected_sha256=("sha256:" + hashlib.sha256(payload).hexdigest()),
        expected_byte_length=len(payload),
        filename=path.name,
    )


def build_selected_elsst_managed_bundle(
    root: Path,
) -> tuple[dict[str, Any], Path]:
    """Build the source-derived R5-history/R6-selected ELSST bundle."""

    previous_descriptor = _fixture_release(
        R5_FIXTURE,
        ELSST_R5,
        release_iri=TEST_R5_RELEASE_IRI,
    )
    current_descriptor = _fixture_release(
        R6_FIXTURE,
        ELSST_R6,
        release_iri=TEST_R6_RELEASE_IRI,
    )
    source_store = root.parent / f"{root.name}-elsst-source-store"
    previous_source = acquire_elsst_release(
        previous_descriptor,
        source_store / "r5",
        source_path=R5_FIXTURE,
    )
    current_source = acquire_elsst_release(
        current_descriptor,
        source_store / "r6",
        source_path=R6_FIXTURE,
    )
    managed = build_elsst_managed_release(
        previous_source,
        current_source,
        rulespec_root=RULESPEC_ROOT,
        recorded_at=RECORDED_AT,
        recorded_by=RECORDED_BY,
        governance=ElsstCandidateGovernance(
            actor_iri="urn:test:actor:spicy-elsst-local-reviewer",
            organization_iri="urn:test:organization:spicy-regs",
            effective_at=RECORDED_AT,
        ),
    )
    managed.bundle.write_to(root)
    support = {
        "R5_RELEASE_ID": TEST_R5_RELEASE_IRI,
        "R6_RELEASE_ID": TEST_R6_RELEASE_IRI,
        "R5_SUCCESSOR_MEMBER_ID": R5_SUCCESSOR_MEMBER_ID,
        "R6_SUCCESSOR_MEMBER_ID": R6_SUCCESSOR_MEMBER_ID,
        "R6_RETIRED_MEMBER_ID": R6_RETIRED_MEMBER_ID,
        "STABLE_SUCCESSOR_ID": STABLE_SUCCESSOR_ID,
    }
    return support, root / "managed-release-bundle.json"

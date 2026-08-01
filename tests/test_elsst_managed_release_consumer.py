"""Spicy lookup against source-derived RefSpec-managed ELSST history."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from spicy_regs.enrichment.managed_release import ManagedReleaseCandidateSource
from tests.elsst_managed_release_support import (
    build_selected_elsst_managed_bundle,
)

pytestmark = pytest.mark.legacy_rulespec_combined

LOOKUP_INDEX_DIGEST = "sha256:" + "e" * 64
IS_VERSION_OF = "http://purl.org/dc/terms/isVersionOf"
PRIOR_VERSION = "http://www.w3.org/2002/07/owl#priorVersion"


def _manifest_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_spicy_consumes_only_current_elsst_candidates_and_retains_history(
    tmp_path: Path,
) -> None:
    support, manifest_path = build_selected_elsst_managed_bundle(tmp_path)
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
        lookup_index_manifest={
            "id": "urn:test:lookup-index:elsst-r6:v1",
            "digest": LOOKUP_INDEX_DIGEST,
        },
        permission_facet_iri="urn:ref:facet:general-subject",
        permission_assignment_role_iri=("https://rulespec.org/ns/v1#assignmentPrimary"),
        permission_resource_route="document",
    )

    r5_release = str(support["R5_RELEASE_ID"])
    r6_release = str(support["R6_RELEASE_ID"])
    r5_successor = str(support["R5_SUCCESSOR_MEMBER_ID"])
    r6_successor = str(support["R6_SUCCESSOR_MEMBER_ID"])
    r6_retired = str(support["R6_RETIRED_MEMBER_ID"])
    stable_successor = str(support["STABLE_SUCCESSOR_ID"])

    r6_candidates = tuple(source.iter_expressions(member_iri=r6_successor))
    preferred_labels = {
        (expression.language_tag, expression.original_literal)
        for expression in r6_candidates
        if expression.label_role == "preferred"
    }
    assert {
        ("el", "ΑΡΧΗΓΟΣ ΝΟΙΚΟΚΥΡΙΟΥ"),
        ("en", "HEADS OF HOUSEHOLD"),
        ("es", "CABEZAS DE HOGAR"),
    } <= preferred_labels
    assert {expression.record["referenceResourceRelease"]["id"] for expression in r6_candidates} == {r6_release}

    identity_links = tuple(source.view.iter_identity_links(member_iri=r6_successor))
    assert any(
        link.predicate_iri == IS_VERSION_OF and link.object_iri == stable_successor and link.object_release_iri is None
        for link in identity_links
    )
    assert any(
        link.predicate_iri == PRIOR_VERSION
        and link.object_iri == r5_successor
        and link.object_release_iri == r5_release
        for link in identity_links
    )

    retired_evidence = tuple(source.iter_evidence_expressions(member_iri=r6_retired))
    assert retired_evidence
    assert not tuple(source.iter_expressions(member_iri=r6_retired))

    r5_evidence = tuple(source.iter_evidence_expressions(member_iri=r5_successor))
    assert r5_evidence
    assert {expression.record["referenceResourceRelease"]["id"] for expression in r5_evidence} == {r5_release}
    assert not tuple(source.iter_expressions(member_iri=r5_successor))

    assert source.usage_ceiling == "candidateUseOnly"
    assert source.candidate_permission.reference_resource_release["id"] == (r6_release)
    assert source.candidate_permission.permission_row["candidateUse"] is True
    assert source.candidate_permission.permission_row["acceptedOutputUse"] is False

"""Spicy's candidate-only adapter for immutable RefSpec releases."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import MappingProxyType

import pytest
from refspec import ManagedReleaseLifecycleParticipant, ManagedReleaseView
from spicy_regs.enrichment.managed_release import (
    ManagedReleaseCandidateSource,
    ManagedReleaseConsumerError,
)
from tests.managed_release_support import build_selected_managed_bundle

pytestmark = pytest.mark.legacy_rulespec_combined

DIGEST = "sha256:" + "c" * 64


def _build_bundle(root: Path) -> tuple[dict[str, object], Path]:
    return build_selected_managed_bundle(root)


def _manifest_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_spicy_consumer_is_exact_read_only_and_candidate_use_only(
    tmp_path: Path,
) -> None:
    support, manifest_path = _build_bundle(tmp_path)
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
        lookup_index_manifest={
            "id": "urn:test:lookup-index:subjects:v1",
            "digest": DIGEST,
        },
        permission_facet_iri="urn:ref:facet:general-subject",
        permission_assignment_role_iri=("https://rulespec.org/ns/v1#assignmentPrimary"),
        permission_resource_route="document",
    )
    view = source.view

    member_id = str(support["MEMBER_ID"])
    assert source.lookup_member(member_id) is not None
    assert source.lookup_member(member_id.upper()) is None
    assert tuple(source.iter_expressions(member_iri=member_id))[0].expression_id == support["EXPRESSION_ID"]
    assert source.usage_ceiling == "candidateUseOnly"
    assert source.candidate_permission.facet_iri == ("urn:ref:facet:general-subject")

    with pytest.raises(TypeError):
        source.lookup_index_manifest["digest"] = DIGEST  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        source.view = view  # type: ignore[misc]
    for forbidden in ("mutate", "reconcile", "deploy", "authorize_output"):
        assert not hasattr(source, forbidden)


def test_spicy_consumer_rejects_expression_corpus_as_lookup_index(
    tmp_path: Path,
) -> None:
    _, manifest_path = _build_bundle(tmp_path)
    view = ManagedReleaseView.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
    )

    with pytest.raises(ManagedReleaseConsumerError, match="must not reuse"):
        ManagedReleaseCandidateSource(
            view=view,
            lookup_index_manifest=dict(view.expression_corpus_snapshot),
            permission_facet_iri="urn:ref:facet:general-subject",
            permission_assignment_role_iri=("https://rulespec.org/ns/v1#assignmentPrimary"),
            permission_resource_route="document",
        )


def test_spicy_consumer_separates_current_candidates_from_raw_evidence(
    tmp_path: Path,
) -> None:
    support, manifest_path = _build_bundle(tmp_path)
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
        lookup_index_manifest={
            "id": "urn:test:lookup-index:subjects:v1",
            "digest": DIGEST,
        },
        permission_facet_iri="urn:ref:facet:general-subject",
        permission_assignment_role_iri=("https://rulespec.org/ns/v1#assignmentPrimary"),
        permission_resource_route="document",
    )
    member_id = str(support["MEMBER_ID"])
    release_id = str(support["RELEASE_ID"])
    retired_view = replace(
        source.view,
        _lifecycle_participants=(
            *tuple(source.view.iter_lifecycle_participants()),
            ManagedReleaseLifecycleParticipant(
                event_iri="urn:test:lifecycle:deprecated-member",
                operation="deprecation",
                participant_role="predecessor",
                member_iri=member_id,
                release_iri=release_id,
                ordinal=0,
                record=MappingProxyType(
                    {
                        "event_id": ("urn:test:lifecycle:deprecated-member"),
                        "operation": "deprecation",
                        "participant_role": "predecessor",
                        "concept_iri": member_id,
                        "release_iri": release_id,
                    }
                ),
            ),
        ),
    )
    retired_source = ManagedReleaseCandidateSource(
        view=retired_view,
        lookup_index_manifest=source.lookup_index_manifest,
        permission_facet_iri=source.permission_facet_iri,
        permission_assignment_role_iri=(source.permission_assignment_role_iri),
        permission_resource_route=source.permission_resource_route,
    )

    assert retired_source.lookup_member(member_id) is not None
    assert tuple(retired_source.iter_evidence_expressions(member_iri=member_id))
    assert not tuple(retired_source.iter_expressions(member_iri=member_id))


def test_spicy_consumer_requires_exact_refspec_candidate_permission(
    tmp_path: Path,
) -> None:
    _, manifest_path = _build_bundle(tmp_path)
    manifest_digest = _manifest_digest(manifest_path)
    wrong_tuples = (
        {
            "permission_facet_iri": "urn:ref:facet:entity",
            "permission_assignment_role_iri": ("https://rulespec.org/ns/v1#assignmentPrimary"),
            "permission_resource_route": "document",
        },
        {
            "permission_facet_iri": "urn:test:facet:unknown",
            "permission_assignment_role_iri": ("https://rulespec.org/ns/v1#assignmentPrimary"),
            "permission_resource_route": "document",
        },
        {
            "permission_facet_iri": "urn:ref:facet:general-subject",
            "permission_assignment_role_iri": ("https://rulespec.org/ns/v1#assignmentMention"),
            "permission_resource_route": "document",
        },
        {
            "permission_facet_iri": "urn:ref:facet:general-subject",
            "permission_assignment_role_iri": "urn:test:role:unknown",
            "permission_resource_route": "document",
        },
        {
            "permission_facet_iri": "urn:ref:facet:general-subject",
            "permission_assignment_role_iri": ("https://rulespec.org/ns/v1#assignmentPrimary"),
            "permission_resource_route": "event",
        },
        {
            "permission_facet_iri": "urn:ref:facet:general-subject",
            "permission_assignment_role_iri": ("https://rulespec.org/ns/v1#assignmentPrimary"),
            "permission_resource_route": "unknown-route",
        },
    )

    for requested in wrong_tuples:
        with pytest.raises(ManagedReleaseConsumerError):
            ManagedReleaseCandidateSource.open(
                manifest_path,
                expected_manifest_digest=manifest_digest,
                lookup_index_manifest={
                    "id": "urn:test:lookup-index:subjects:v1",
                    "digest": DIGEST,
                },
                **requested,
            )

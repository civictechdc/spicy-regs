"""Candidate-use-only access to an immutable RefSpec managed release."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from refspec import (
    ManagedReleaseAuthorizationError,
    ManagedReleaseCandidatePermission,
    ManagedReleaseExpression,
    ManagedReleaseMember,
    ManagedReleaseView,
)

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class ManagedReleaseConsumerError(ValueError):
    """Spicy cannot safely use the managed release for candidate lookup."""


@dataclass(frozen=True, slots=True)
class ManagedReleaseCandidateSource:
    """Bind Spicy lookup to separate logical-corpus and physical-index pins."""

    view: ManagedReleaseView
    lookup_index_manifest: Mapping[str, str]
    permission_facet_iri: str
    permission_assignment_role_iri: str
    permission_resource_route: str
    candidate_permission: ManagedReleaseCandidatePermission = field(
        init=False,
    )
    usage_ceiling: str = field(default="candidateUseOnly", init=False)

    @classmethod
    def open(
        cls,
        bundle_manifest: Path | str,
        *,
        expected_manifest_digest: str,
        lookup_index_manifest: Mapping[str, str],
        permission_facet_iri: str,
        permission_assignment_role_iri: str,
        permission_resource_route: str,
    ) -> ManagedReleaseCandidateSource:
        """Open one RefSpec-authorized candidate-only Spicy source."""

        return cls(
            view=ManagedReleaseView.open(
                bundle_manifest,
                expected_manifest_digest=expected_manifest_digest,
            ),
            lookup_index_manifest=lookup_index_manifest,
            permission_facet_iri=permission_facet_iri,
            permission_assignment_role_iri=(permission_assignment_role_iri),
            permission_resource_route=permission_resource_route,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.view, ManagedReleaseView):
            raise ManagedReleaseConsumerError("view must be an opened refspec.ManagedReleaseView")
        value = self.lookup_index_manifest
        if set(value) != {"id", "digest"}:
            raise ManagedReleaseConsumerError("lookupIndexManifest must be one exact id and digest reference")
        identifier = value.get("id")
        digest = value.get("digest")
        if not isinstance(identifier, str) or not identifier.strip():
            raise ManagedReleaseConsumerError("lookupIndexManifest.id is required")
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise ManagedReleaseConsumerError("lookupIndexManifest.digest must be sha256:<64 lowercase hex>")
        if identifier == self.view.expression_corpus_snapshot["id"]:
            raise ManagedReleaseConsumerError("lookupIndexManifest must not reuse expressionCorpusSnapshot")
        object.__setattr__(
            self,
            "lookup_index_manifest",
            MappingProxyType({"id": identifier, "digest": digest}),
        )
        try:
            permission = self.view.require_candidate_use(
                facet_iri=self.permission_facet_iri,
                assignment_role_iri=self.permission_assignment_role_iri,
                resource_route=self.permission_resource_route,
            )
        except ManagedReleaseAuthorizationError as error:
            raise ManagedReleaseConsumerError(str(error)) from error
        object.__setattr__(self, "candidate_permission", permission)

    @property
    def expression_corpus_snapshot(self) -> Mapping[str, str]:
        return self.view.expression_corpus_snapshot

    def lookup_member(self, member_iri: str) -> ManagedReleaseMember | None:
        """Resolve only an exact release-member identifier."""

        return self.view.lookup_member(member_iri)

    def iter_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[ManagedReleaseExpression]:
        """Yield only current-assignment expressions authorized for this source."""

        yield from self.view.iter_candidate_expressions(
            facet_iri=self.candidate_permission.facet_iri,
            assignment_role_iri=(self.candidate_permission.assignment_role_iri),
            resource_route=self.candidate_permission.resource_route,
            member_iri=member_iri,
        )

    def iter_evidence_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[ManagedReleaseExpression]:
        """Yield the complete raw expression corpus for audit and history."""

        yield from self.view.iter_expressions(member_iri=member_iri)

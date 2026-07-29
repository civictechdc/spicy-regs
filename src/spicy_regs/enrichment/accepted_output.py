"""Narrow Spicy adapter for RefSpec-owned accepted-output authorization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from refspec.accepted_output import (
    AcceptedOutputAuthorization,
    authorize_accepted_assignment,
)

from spicy_regs.enrichment.managed_release import ManagedReleaseCandidateSource


def authorize_managed_accepted_assignment(
    *,
    source: ManagedReleaseCandidateSource,
    member_iri: str,
    facet: str,
    assignment_role: str,
    accepted_output_permission: Mapping[str, Any],
    ref_records: Sequence[Mapping[str, Any]],
    output_profile_id: str,
    registry_deployment_id: str,
    configuration_id: str,
    evaluation_result_id: str,
    enrichment_deployment_id: str,
    release_graph_validation_receipt: Mapping[str, Any],
) -> AcceptedOutputAuthorization:
    """Ask RefSpec to authorize one candidate for accepted output.

    Spicy supplies its actual physical-index pin and the logical corpus from
    the opened candidate source.  All policy evaluation stays in RefSpec.
    """

    return authorize_accepted_assignment(
        managed_release=source.view,
        member_iri=member_iri,
        facet=facet,
        assignment_role=assignment_role,
        accepted_output_permission=accepted_output_permission,
        expression_corpus_snapshot=source.expression_corpus_snapshot,
        lookup_index_manifest=source.lookup_index_manifest,
        ref_records=ref_records,
        output_profile_id=output_profile_id,
        registry_deployment_id=registry_deployment_id,
        configuration_id=configuration_id,
        evaluation_result_id=evaluation_result_id,
        enrichment_deployment_id=enrichment_deployment_id,
        release_graph_validation_receipt=release_graph_validation_receipt,
    )

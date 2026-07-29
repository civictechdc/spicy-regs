from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from spicy_regs.enrichment.accepted_output import (
    authorize_managed_accepted_assignment,
)


def test_spicy_adapter_delegates_all_policy_to_refspec(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    def fake_authorize(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(
        "spicy_regs.enrichment.accepted_output.authorize_accepted_assignment",
        fake_authorize,
    )
    source = SimpleNamespace(
        view=object(),
        expression_corpus_snapshot={
            "id": "urn:example:expression-corpus:v1",
            "digest": "sha256:" + "1" * 64,
        },
        lookup_index_manifest={
            "id": "urn:example:lookup-index:v1",
            "digest": "sha256:" + "2" * 64,
        },
    )
    permission = {
        "facet": "urn:ref:facet:general-subject",
        "assignmentRole": "https://rulespec.org/ns/v1#assignmentPrimary",
    }
    receipt = {"type": "urn:ref:type:ReleaseGraphValidationReceipt"}

    result = authorize_managed_accepted_assignment(
        source=source,  # type: ignore[arg-type]
        member_iri="urn:example:concept:air-quality",
        facet="urn:ref:facet:general-subject",
        assignment_role="https://rulespec.org/ns/v1#assignmentPrimary",
        accepted_output_permission=permission,
        ref_records=[],
        output_profile_id="urn:example:output-profile:v1",
        registry_deployment_id="urn:example:registry-deployment:v1",
        configuration_id="urn:example:configuration:v1",
        evaluation_result_id="urn:example:evaluation:v1",
        enrichment_deployment_id="urn:example:enrichment-deployment:v1",
        release_graph_validation_receipt=receipt,
    )

    assert result is sentinel
    assert captured["managed_release"] is source.view
    assert (
        captured["expression_corpus_snapshot"]
        is source.expression_corpus_snapshot
    )
    assert captured["lookup_index_manifest"] is source.lookup_index_manifest
    assert captured["accepted_output_permission"] is permission
    assert captured["release_graph_validation_receipt"] is receipt

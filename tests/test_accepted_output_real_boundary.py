"""Accepted-output closure against source-derived managed releases.

The Federal Register thesaurus receives explicit test-only accepted-output
governance so this suite can exercise the complete RefSpec join. ELSST has no
positive accepted-output case: a test-only chain exists only to isolate its
real candidate-only permission as the failing input.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from refspec import binding
from refspec.accepted_output import AcceptedOutputAuthorizationError
from refspec.release_graph import (
    GRAPH_DIGEST_ALGORITHM,
    RELEASE_GRAPH_GATE_COMPONENT_ID,
    defined_rulespec_identifiers,
    issue_release_graph_validation_receipt,
    load_pinned_rulespec_validator,
    referenced_rulespec_identifiers,
    rulespec_graph_digest,
)
from refspec.vocabulary import seal_payload
from spicy_regs.enrichment import ManagedReleaseCandidateSource
from spicy_regs.enrichment.accepted_output import (
    authorize_managed_accepted_assignment,
)
from tests.elsst_managed_release_support import (
    build_selected_elsst_managed_bundle,
)
from tests.managed_release_support import build_selected_managed_bundle

CLOSURE_FIXTURE = (
    Path(__file__).parents[1]
    / "RefSpec"
    / "bindings"
    / "json"
    / "1.0"
    / "fixtures"
    / "valid"
    / "vocabulary-closure.json"
)
FACET = "urn:ref:facet:general-subject"
PRIMARY = "https://rulespec.org/ns/v1#assignmentPrimary"
MENTION = "https://rulespec.org/ns/v1#assignmentMention"
LOOKUP_DIGEST = "sha256:" + "e" * 64
RECORDED_AT = "2026-07-29T22:00:00Z"
RECORDED_BY = "urn:test:agent:accepted-output-boundary"
RULESPEC_ROOT = Path(__file__).resolve().parents[2] / "rulespec"


@dataclass(frozen=True)
class _GateMaterial:
    graph: dict[str, Any]
    graph_id: str


@dataclass(frozen=True)
class _AcceptedChain:
    source: ManagedReleaseCandidateSource
    member_iri: str
    records: tuple[dict[str, Any], ...]
    permission: dict[str, Any]
    cross_row_permission: dict[str, Any]
    output_profile: dict[str, Any]
    registry_deployment: dict[str, Any]
    configuration: dict[str, Any]
    evaluation: dict[str, Any]
    enrichment_deployment: dict[str, Any]
    receipt: dict[str, Any]


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_plain(item) for item in value]
    return value


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _reference(
    record: Mapping[str, Any],
    *,
    versioned: bool = False,
) -> dict[str, str]:
    reference = {
        "id": str(record["id"]),
        "digest": str(record[binding.digest_field(dict(record))]),
    }
    if versioned:
        reference["version"] = str(record["version"])
    return reference


def _sealed(record: Mapping[str, Any]) -> dict[str, Any]:
    return seal_payload(_plain(record))


def _template(record_type: str) -> dict[str, Any]:
    fixture = json.loads(CLOSURE_FIXTURE.read_text(encoding="utf-8"))
    return copy.deepcopy(next(record for record in fixture["records"] if record["type"] == record_type))


def _open_source(
    root: Path,
    *,
    builder: Any,
    index_id: str,
) -> tuple[dict[str, Any], ManagedReleaseCandidateSource, _GateMaterial]:
    support, manifest_path = builder(root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    graph_descriptor = manifest["rulespecGraph"]
    graph_path = manifest_path.parent / graph_descriptor["path"]
    assert _digest(graph_path) == graph_descriptor["sha256"]
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=_digest(manifest_path),
        lookup_index_manifest={
            "id": index_id,
            "digest": LOOKUP_DIGEST,
        },
        permission_facet_iri=FACET,
        permission_assignment_role_iri=PRIMARY,
        permission_resource_route="document",
    )
    return (
        support,
        source,
        _GateMaterial(
            graph=graph,
            graph_id=str(manifest["rulespecGraphId"]),
        ),
    )


def _test_governance_ids(slug: str) -> dict[str, str]:
    return {
        "assertion": f"urn:test:{slug}:assertion:accepted-output",
        "evidence": f"urn:test:{slug}:evidence:accepted-output",
        "attestation": f"urn:test:{slug}:attestation:accepted-output",
        "adoption": f"urn:test:{slug}:adoption:accepted-output",
    }


def _test_governance_nodes(
    *,
    slug: str,
    subject: str,
    environment: str,
) -> tuple[dict[str, Any], ...]:
    ids = _test_governance_ids(slug)
    return (
        {
            "@id": ids["assertion"],
            "@type": "rkaf:ValueAssertion",
            "rkaf:assertionOrigin": "rkaf:humanAsserted",
            "rkaf:epistemicBasis": "rkaf:editorialAssertion",
            "rkaf:assertsSubject": subject,
            "rkaf:assertsPredicate": ("urn:test:predicate:accepted-output-test-chain-authorized"),
            "rkaf:assertsValue": {
                "@value": "true",
                "@type": "xsd:boolean",
            },
            "rkaf:assertionPolarity": "rkaf:affirmed",
            "rkaf:usageEligibility": "rkaf:notEligible",
            "rkaf:assertedAt": RECORDED_AT,
        },
        {
            "@id": ids["evidence"],
            "@type": "rkaf:EvidenceBinding",
            "rkaf:bindsAssertion": ids["assertion"],
            "rkaf:noEvidenceReason": "rkaf:consensus-without-citation",
        },
        {
            "@id": ids["attestation"],
            "@type": "rkaf:Attestation",
            "rkaf:attestor": RECORDED_BY,
            "rkaf:attestorKind": "rkaf:formalReviewer",
            "rkaf:targets": [ids["assertion"]],
            "rkaf:decision": "rkaf:approved",
            "rkaf:attestationScope": environment,
            "rkaf:attestedAt": RECORDED_AT,
        },
        {
            "@id": ids["adoption"],
            "@type": "rkaf:LocalAdoption",
            "rkaf:organization": "urn:test:organization:spicy-regs",
            "rkaf:targetAssertion": ids["assertion"],
            "rkaf:adoptionStatus": "rkaf:active",
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "rkaf:adoptionAuthorityKind": "rkaf:localOperational",
            "rkaf:adoptionScope": environment,
            "rkaf:authorizedBy": RECORDED_BY,
            "rkaf:adoptedAt": RECORDED_AT,
            "rkaf:basedOnAttestation": ids["attestation"],
        },
    )


def _sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _bind_gold_to_test_graph(
    gold: dict[str, Any],
    *,
    slug: str,
    activity: str,
) -> tuple[dict[str, Any], ...]:
    """Bind the test-only gold evidence to valid nodes in the live graph."""

    nodes: list[dict[str, Any]] = []
    fragments_by_item: dict[str, str] = {}
    for index, item in enumerate(gold["items"]):
        item_id = str(item["id"])
        artifact_id = f"urn:test:{slug}:artifact:gold-item:{index}"
        fragment_id = f"urn:test:{slug}:fragment:gold-item:{index}"
        text = f"Accepted-output boundary gold item {index}."
        artifact_digest = _sha256_text(f"{slug}:artifact:{index}")
        text_digest = _sha256_text(text)
        item["renditionArtifact"] = {
            "id": artifact_id,
            "digest": artifact_digest,
        }
        item["sourceFragment"] = fragment_id
        item["partitionKeys"]["artifactDigest"] = [artifact_digest]
        item["partitionKeys"]["textDigest"] = [text_digest]
        item["partitionEvidence"]["sourceTextDigest"] = text_digest
        fragments_by_item[item_id] = fragment_id
        nodes.extend(
            (
                {
                    "@id": artifact_id,
                    "@type": "rkaf:Artifact",
                    "rkaf:hasArtifactIdentifier": [artifact_id],
                    "rkaf:artifactIdentifierScheme": ["rkaf:partner-defined"],
                    "dcterms:format": "text/plain",
                    "rkaf:hasContentDigest": artifact_digest,
                },
                {
                    "@id": fragment_id,
                    "@type": "rkaf:SourceFragment",
                    "oa:hasSource": artifact_id,
                    "oa:hasSelector": [
                        {
                            "@type": "oa:TextQuoteSelector",
                            "oa:exact": text,
                        }
                    ],
                    "rkaf:selectorKind": ["oa:TextQuoteSelector"],
                    "rkaf:fragmentContentDigest": text_digest,
                },
            )
        )

    judgment_ids: list[str] = []
    for expectation_index, expectation in enumerate(gold["expectations"]):
        expectation["forbiddenResults"] = []
        expectation["evidenceRefs"] = [fragments_by_item[str(expectation["item"])]]
        for reviewer_index, judgment in enumerate(expectation["reviewerJudgments"]):
            assertion_id = f"urn:test:{slug}:assertion:gold-judgment:{expectation_index}:{reviewer_index}"
            evidence_id = f"urn:test:{slug}:evidence:gold-judgment:{expectation_index}:{reviewer_index}"
            judgment_id = f"urn:test:{slug}:attestation:gold-judgment:{expectation_index}:{reviewer_index}"
            judgment["judgment"] = judgment_id
            judgment_ids.append(judgment_id)
            nodes.extend(
                (
                    {
                        "@id": assertion_id,
                        "@type": "rkaf:ValueAssertion",
                        "rkaf:assertionOrigin": "rkaf:humanAsserted",
                        "rkaf:epistemicBasis": ("rkaf:editorialAssertion"),
                        "rkaf:assertsSubject": expectation["id"],
                        "rkaf:assertsPredicate": ("urn:test:predicate:gold-expectation-reviewed"),
                        "rkaf:assertsValue": {
                            "@value": "true",
                            "@type": "xsd:boolean",
                        },
                        "rkaf:assertionPolarity": "rkaf:affirmed",
                        "rkaf:usageEligibility": "rkaf:notEligible",
                        "rkaf:assertedAt": RECORDED_AT,
                    },
                    {
                        "@id": evidence_id,
                        "@type": "rkaf:EvidenceBinding",
                        "rkaf:bindsAssertion": assertion_id,
                        "rkaf:noEvidenceReason": ("rkaf:consensus-without-citation"),
                    },
                    {
                        "@id": judgment_id,
                        "@type": "rkaf:Attestation",
                        "rkaf:attestor": judgment["reviewer"],
                        "rkaf:attestorKind": "rkaf:formalReviewer",
                        "rkaf:targets": [assertion_id],
                        "rkaf:decision": "rkaf:approved",
                        "rkaf:attestationScope": ("urn:test:scope:accepted-output-boundary"),
                        "rkaf:attestedAt": RECORDED_AT,
                    },
                )
            )

    gold["independentJudgmentRefs"] = judgment_ids
    gold["sealingActivity"] = activity
    items_by_id = {item["id"]: item for item in gold["items"]}
    for dimension in gold["partitionReport"]["dimensions"]:
        key = dimension["dimension"]
        for item_key in dimension["itemKeys"]:
            item_key["values"] = list(items_by_id[item_key["item"]]["partitionKeys"][key])
    return tuple(nodes)


def _issue_live_receipt(
    *,
    records: Sequence[Mapping[str, Any]],
    gate_material: _GateMaterial,
    extra_graph_nodes: Sequence[Mapping[str, Any]],
    receipt_id: str,
    activity: str,
) -> dict[str, Any]:
    graph = copy.deepcopy(gate_material.graph)
    graph["@graph"].extend(copy.deepcopy(list(extra_graph_nodes)))
    validator = load_pinned_rulespec_validator(RULESPEC_ROOT)
    graph_digest = rulespec_graph_digest(graph)
    graph_identifiers = defined_rulespec_identifiers(graph)
    cross_references = sorted(
        (
            {
                "refRecordId": str(record["id"]),
                "rulespecIdentifier": rulespec_identifier,
            }
            for record in records
            for rulespec_identifier in referenced_rulespec_identifiers(
                record,
                graph_identifiers,
            )
        ),
        key=lambda item: (
            item["refRecordId"],
            item["rulespecIdentifier"],
        ),
    )
    bundle = {
        "bundleVersion": "1.0",
        "refRecords": [_plain(record) for record in records],
        "rulespecGraph": graph,
        "rulespecGraphId": gate_material.graph_id,
        "rulespecGraphDigest": graph_digest,
        "graphDigestAlgorithm": GRAPH_DIGEST_ALGORITHM,
        "validatorReceipt": {
            "result": "pass",
            "validatorIdentity": validator.identity,
            "validatorSourceRevision": validator.source_revision,
            "graphId": gate_material.graph_id,
            "graphDigest": graph_digest,
            "coveredIdentifiers": sorted(graph_identifiers),
        },
        "crossReferences": cross_references,
    }
    return issue_release_graph_validation_receipt(
        bundle,
        validator=validator,
        receipt_id=receipt_id,
        recorded_at=RECORDED_AT,
        recorded_by=RECORDED_BY,
        activity=activity,
    )


def _accepted_chain(
    source: ManagedReleaseCandidateSource,
    gate_material: _GateMaterial,
    *,
    member_iri: str,
    slug: str,
) -> _AcceptedChain:
    view_records = {identifier: _plain(record) for identifier, record in source.view._records_by_id.items()}
    selected = next(
        record
        for record in view_records.values()
        if record.get("type") == "urn:ref:type:RegistryDeploymentDecision"
        and record.get("selectionState") == "selected"
    )
    original_output = view_records[selected["outputProfile"]["id"]]
    original_coverage = view_records[selected["coverageReport"]["id"]]
    import_snapshot = view_records[selected["registryImportSnapshot"]["id"]]
    enrichment_profile = view_records[original_output["enrichmentProfile"]["id"]]
    release = copy.deepcopy(selected["referenceResourceRelease"])
    imported = copy.deepcopy(selected["registryImportSnapshot"])
    expression_corpus = _plain(source.expression_corpus_snapshot)
    activity = str(selected["activity"])
    governance_ids = _test_governance_ids(slug)

    permission = _plain(source.candidate_permission.permission_row)
    permission["acceptedOutputUse"] = True
    second_permission = copy.deepcopy(permission)
    second_permission["facet"] = "urn:ref:facet:specialist-subject"
    second_permission["assignmentRole"] = MENTION
    cross_row_permission = copy.deepcopy(permission)
    cross_row_permission["assignmentRole"] = MENTION

    output_profile = copy.deepcopy(original_output)
    output_profile.update(
        {
            "id": f"urn:test:{slug}:output-profile:accepted:v1",
            "version": "1.0-test",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "acceptancePolicies": [
                {
                    "id": f"urn:test:{slug}:acceptance-policy:v1",
                    "version": "1.0",
                    "digest": "sha256:" + "1" * 64,
                }
            ],
            "releasePermissions": [permission, second_permission],
        }
    )
    output_profile = _sealed(output_profile)

    coverage = copy.deepcopy(original_coverage)
    coverage.update(
        {
            "id": f"urn:test:{slug}:coverage:accepted:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "outputProfile": _reference(
                output_profile,
                versioned=True,
            ),
        }
    )
    coverage = _sealed(coverage)

    registry_deployment = copy.deepcopy(selected)
    registry_deployment.update(
        {
            "id": f"urn:test:{slug}:registry-deployment:accepted:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "effectiveAt": RECORDED_AT,
            "outputProfile": _reference(
                output_profile,
                versioned=True,
            ),
            "coverageReport": _reference(coverage),
            "reason": ("Explicit test-only selection used to verify the accepted-output boundary."),
            "rulespecAttestationRefs": [governance_ids["attestation"]],
            "localAdoptionRefs": [governance_ids["adoption"]],
        }
    )
    registry_deployment.pop("authorizationValidations", None)
    registry_deployment = _sealed(registry_deployment)

    gold = _template("urn:ref:type:SealedGoldManifest")
    gold.update(
        {
            "id": f"urn:test:{slug}:gold:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "sealingTime": RECORDED_AT,
            "sealingActivity": activity,
        }
    )
    gold["vocabularyUniverse"].update(
        {
            "referenceResourceReleases": [release],
            "registryImportSnapshots": [imported],
            "mappingReleases": [],
            "mappingSnapshots": [],
            "indexedExpressionCorpusDigests": [expression_corpus["digest"]],
            "enrichmentProfile": _reference(
                enrichment_profile,
                versioned=True,
            ),
            "outputProfile": _reference(
                output_profile,
                versioned=True,
            ),
        }
    )
    development_item = gold["items"][0]
    development_item["partitionKeys"]["conceptIdentity"] = [member_iri]
    development_item["partitionKeys"]["alias"] = []
    for item in gold["items"]:
        item["partitionEvidence"]["vocabularyExpressionCorpusDigest"] = expression_corpus["digest"]
    gold["expectations"][0]["registeredTargets"] = [
        {
            "target": member_iri,
            "release": release,
            "grade": "exact",
            "adequate": True,
            "independentlyReviewed": True,
        }
    ]
    for dimension in gold["partitionReport"]["dimensions"]:
        if dimension["dimension"] == "conceptIdentity":
            dimension["itemKeys"][0]["values"] = [member_iri]
        if dimension["dimension"] == "alias":
            dimension["itemKeys"][0]["values"] = []
    old_corpus_digest = "sha256:" + "38" * 32
    gold["partitionReport"]["inputDigests"] = [
        (expression_corpus["digest"] if digest == old_corpus_digest else digest)
        for digest in gold["partitionReport"]["inputDigests"]
    ]
    gold_nodes = _bind_gold_to_test_graph(
        gold,
        slug=slug,
        activity=activity,
    )
    gold = _sealed(gold)

    configuration = _template("urn:ref:type:EnrichmentConfiguration")
    configuration.update(
        {
            "id": f"urn:test:{slug}:configuration:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "enrichmentProfile": _reference(
                enrichment_profile,
                versioned=True,
            ),
            "outputProfile": _reference(
                output_profile,
                versioned=True,
            ),
            "acceptancePolicy": copy.deepcopy(output_profile["acceptancePolicies"][0]),
        }
    )
    configuration["vocabulary"].update(
        {
            "referenceResourceReleases": [release],
            "registryImportSnapshots": [imported],
            "mappingReleases": [],
            "mappingSnapshots": [],
            "registryDeploymentDecisions": [_reference(registry_deployment)],
        }
    )
    configuration["indexes"] = [
        {
            **configuration["indexes"][0],
            "expressionCorpusSnapshot": expression_corpus,
            "lookupIndexManifest": _plain(source.lookup_index_manifest),
            "indexedExpressionCorpusDigest": (expression_corpus["digest"]),
        }
    ]
    configuration = _sealed(configuration)

    evaluation = _template("urn:ref:type:EnrichmentEvaluationResult")
    evaluation.update(
        {
            "id": f"urn:test:{slug}:evaluation:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "evaluatedAt": RECORDED_AT,
            "configuration": _reference(configuration),
            "sealedGoldManifest": _reference(gold),
            "activity": activity,
        }
    )
    evaluation = _sealed(evaluation)

    enrichment_deployment = _template("urn:ref:type:EnrichmentDeploymentDecision")
    enrichment_environment = copy.deepcopy(registry_deployment["environment"])
    enrichment_environment["classification"] = "nonProduction"
    enrichment_deployment.update(
        {
            "id": f"urn:test:{slug}:enrichment-deployment:v1",
            "recordedAt": RECORDED_AT,
            "recordedBy": RECORDED_BY,
            "effectiveAt": RECORDED_AT,
            "environment": enrichment_environment,
            "configuration": _reference(configuration),
            "evaluationResult": _reference(evaluation),
            "outputProfile": _reference(
                output_profile,
                versioned=True,
            ),
            "reason": ("Explicit test-only selection after a passing sealed evaluation."),
            "activity": activity,
            "rulespecAttestationRefs": [governance_ids["attestation"]],
            "localAdoptionRefs": [governance_ids["adoption"]],
        }
    )
    enrichment_deployment.pop("authorizationValidations", None)
    enrichment_deployment = _sealed(enrichment_deployment)

    records = (
        copy.deepcopy(enrichment_profile),
        output_profile,
        copy.deepcopy(import_snapshot),
        coverage,
        registry_deployment,
        gold,
        configuration,
        evaluation,
        enrichment_deployment,
    )
    receipt = _issue_live_receipt(
        records=records,
        gate_material=gate_material,
        extra_graph_nodes=(
            *_test_governance_nodes(
                slug=slug,
                subject=output_profile["id"],
                environment=registry_deployment["environment"]["id"],
            ),
            *gold_nodes,
        ),
        receipt_id=f"urn:test:{slug}:release-graph-receipt:v1",
        activity=activity,
    )

    diagnostics = binding.validate([*records, receipt])
    assert not diagnostics, "\n".join(diagnostic.render() for diagnostic in diagnostics)

    trusted_view = replace(
        source.view,
        _release_graph_validation_receipt=receipt,
    )
    source = replace(source, view=trusted_view)
    return _AcceptedChain(
        source=source,
        member_iri=member_iri,
        records=records,
        permission=permission,
        cross_row_permission=cross_row_permission,
        output_profile=output_profile,
        registry_deployment=registry_deployment,
        configuration=configuration,
        evaluation=evaluation,
        enrichment_deployment=enrichment_deployment,
        receipt=receipt,
    )


def _authorize(
    chain: _AcceptedChain,
    *,
    source: ManagedReleaseCandidateSource | None = None,
    permission: Mapping[str, Any] | None = None,
    facet: str = FACET,
    role: str = PRIMARY,
    output_profile_id: str | None = None,
    receipt: Mapping[str, Any] | None = None,
):
    return authorize_managed_accepted_assignment(
        source=source or chain.source,
        member_iri=chain.member_iri,
        facet=facet,
        assignment_role=role,
        accepted_output_permission=permission or chain.permission,
        ref_records=chain.records,
        output_profile_id=(output_profile_id or chain.output_profile["id"]),
        registry_deployment_id=chain.registry_deployment["id"],
        configuration_id=chain.configuration["id"],
        evaluation_result_id=chain.evaluation["id"],
        enrichment_deployment_id=(chain.enrichment_deployment["id"]),
        release_graph_validation_receipt=receipt or chain.receipt,
    )


@pytest.fixture(scope="module")
def federal_chain(
    tmp_path_factory: pytest.TempPathFactory,
) -> _AcceptedChain:
    support, source, gate_material = _open_source(
        tmp_path_factory.mktemp("federal-accepted-output"),
        builder=build_selected_managed_bundle,
        index_id="urn:test:lookup-index:federal-accepted:v1",
    )
    return _accepted_chain(
        source,
        gate_material,
        member_iri=str(support["MEMBER_ID"]),
        slug="federal",
    )


def test_source_derived_federal_release_reaches_accepted_output_only_through_exact_pins(
    federal_chain: _AcceptedChain,
) -> None:
    chain = federal_chain

    authorized = _authorize(chain)

    assert authorized.member.member_iri == chain.member_iri
    assert _plain(authorized.permission) == chain.permission
    assert dict(authorized.expression_corpus_snapshot) == dict(chain.source.expression_corpus_snapshot)
    assert dict(authorized.lookup_index_manifest) == dict(chain.source.lookup_index_manifest)
    assert dict(authorized.registry_deployment) == _reference(chain.registry_deployment)
    assert dict(authorized.configuration) == _reference(chain.configuration)
    assert dict(authorized.evaluation_result) == _reference(chain.evaluation)
    assert dict(authorized.enrichment_deployment) == _reference(chain.enrichment_deployment)
    assert dict(authorized.validation_receipt) == _reference(chain.receipt)
    assert chain.receipt["gateImplementation"]["id"] == (RELEASE_GRAPH_GATE_COMPONENT_ID)
    assert chain.receipt["refRecordDigests"] == sorted(
        [_reference(record) for record in chain.records],
        key=lambda item: (item["id"], item["digest"]),
    )
    assert {evaluation["governanceRecord"]["id"] for evaluation in chain.receipt["authorizationEvaluations"]} == {
        chain.registry_deployment["id"],
        chain.enrichment_deployment["id"],
    }
    for authorization in chain.receipt["authorizationEvaluations"]:
        assert authorization["inputGraph"] == chain.receipt["rulespecGraph"]
        assert authorization["runtime"] == chain.receipt["rulespecBehaviorRuntime"]


def test_real_permission_rows_reject_wrong_tuple_and_cross_row_assembly(
    federal_chain: _AcceptedChain,
) -> None:
    chain = federal_chain

    for facet, role in (
        ("urn:ref:facet:specialist-subject", PRIMARY),
        (FACET, MENTION),
    ):
        with pytest.raises(
            AcceptedOutputAuthorizationError,
            match=("does not authorize this facet, role, and accepted output"),
        ):
            _authorize(chain, facet=facet, role=role)

    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match="must match exactly one complete",
    ):
        _authorize(
            chain,
            permission=chain.cross_row_permission,
            role=MENTION,
        )


def test_real_elsst_candidate_only_permission_cannot_enter_accepted_output(
    tmp_path: Path,
) -> None:
    support, source, gate_material = _open_source(
        tmp_path / "elsst",
        builder=build_selected_elsst_managed_bundle,
        index_id="urn:test:lookup-index:elsst-boundary:v1",
    )
    chain = _accepted_chain(
        source,
        gate_material,
        member_iri=str(support["R6_SUCCESSOR_MEMBER_ID"]),
        slug="elsst-negative-only",
    )
    candidate_only = _plain(source.candidate_permission.permission_row)

    assert candidate_only["candidateUse"] is True
    assert candidate_only["acceptedOutputUse"] is False
    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match="must match exactly one complete",
    ):
        _authorize(chain, permission=candidate_only)


def test_real_chain_rejects_release_import_expression_index_profile_and_receipt_drift(
    federal_chain: _AcceptedChain,
) -> None:
    chain = federal_chain

    for field in (
        "referenceResourceRelease",
        "registryImportSnapshot",
    ):
        changed = copy.deepcopy(chain.permission)
        changed[field]["digest"] = "sha256:" + "9" * 64
        with pytest.raises(
            AcceptedOutputAuthorizationError,
            match="must match exactly one complete",
        ):
            _authorize(chain, permission=changed)

    changed_expression_view = replace(
        chain.source.view,
        _expression_corpus_snapshot={
            **dict(chain.source.expression_corpus_snapshot),
            "digest": "sha256:" + "8" * 64,
        },
    )
    changed_expression_source = replace(
        chain.source,
        view=changed_expression_view,
    )
    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match="exactly one index pin",
    ):
        _authorize(chain, source=changed_expression_source)

    changed_index_source = replace(
        chain.source,
        lookup_index_manifest={
            **dict(chain.source.lookup_index_manifest),
            "digest": "sha256:" + "8" * 64,
        },
    )
    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match="exactly one index pin",
    ):
        _authorize(chain, source=changed_index_source)

    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match="absent from the validated REF records",
    ):
        _authorize(
            chain,
            output_profile_id="urn:test:wrong-output-profile",
        )

    changed_receipt = copy.deepcopy(chain.receipt)
    changed_receipt["authorizationEvaluations"][1]["runtime"]["digest"] = "sha256:" + "8" * 64
    changed_receipt = _sealed(changed_receipt)
    changed_receipt_source = replace(
        chain.source,
        view=replace(
            chain.source.view,
            _release_graph_validation_receipt=changed_receipt,
        ),
    )
    with pytest.raises(
        AcceptedOutputAuthorizationError,
        match=("accepted-output evidence fails|different Rulespec runtime"),
    ):
        _authorize(
            chain,
            source=changed_receipt_source,
            receipt=changed_receipt,
        )

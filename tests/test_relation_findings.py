from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from typing import cast

import pytest

from spicy_regs.ontology.invariants import OntologyInvariantError
from spicy_regs.ontology.relation_findings import (
    AssertionAttestation,
    AssertionOrigin,
    AssertionPolarity,
    AttestationDecision,
    AttestationStateResolver,
    BoundEvidenceResolver,
    DeclaredScopeComparator,
    RelationAssertion,
    RelationComparisonContext,
    RelationComparisonDependencies,
    RelationEvidenceBinding,
    ScopeDeclaration,
    StaticBaselineResolver,
    StaticPairingResolver,
    StaticPredicateCatalog,
    compare_relation_assertions,
)

PROGRAM = "urn:example:program:aviation-emergency-amendment"
PREDICATE = "urn:example:relation:appliesTo"
GROUP = "urn:example:group:airworthiness-resources"
OTHER_PREDICATE = "urn:example:relation:mentions"
EXPECTED_ID = "urn:example:assertion:expected"
OBSERVED_ID = "urn:example:assertion:observed"
BASELINE_ARTIFACT = "urn:example:artifact:baseline:v1"
OBSERVED_ARTIFACT = "urn:example:artifact:observed:v2"
PAIRING = "urn:example:pairing:baseline-observed"
SCOPE = "urn:example:scope:aviation"
OTHER_SCOPE = "urn:example:scope:mining"
CONSUMER_SCOPE = "urn:example:consumer-scope:experiment"
WARRANT = "urn:example:warrant:approved-comparator"
NOW = datetime(2026, 7, 24, 20, 0, tzinfo=timezone.utc)


def _assertion(
    assertion_id: str,
    polarity: AssertionPolarity,
    *,
    predicate_iri: str = PREDICATE,
    scope_id: str = SCOPE,
    origin: AssertionOrigin = "rkaf:imported",
    warrant_ids: tuple[str, ...] = (),
) -> RelationAssertion:
    return RelationAssertion(
        assertion_id=assertion_id,
        subject_iri=PROGRAM,
        predicate_iri=predicate_iri,
        object_iri=GROUP,
        polarity=polarity,
        assertion_origin=origin,
        applicability_scope_id=scope_id,
        warrant_ids=warrant_ids,
    )


def _expected() -> RelationAssertion:
    return _assertion(
        EXPECTED_ID,
        "rkaf:affirmed",
        origin="rkaf:humanAsserted",
        warrant_ids=(WARRANT,),
    )


def _observed(
    polarity: AssertionPolarity,
    *,
    assertion_id: str = OBSERVED_ID,
    predicate_iri: str = PREDICATE,
    scope_id: str = SCOPE,
    origin: AssertionOrigin = "rkaf:imported",
) -> RelationAssertion:
    return _assertion(
        assertion_id,
        polarity,
        predicate_iri=predicate_iri,
        scope_id=scope_id,
        origin=origin,
    )


def _context() -> RelationComparisonContext:
    return RelationComparisonContext(
        comparison_id="urn:example:comparison:1",
        expected_assertion_id=EXPECTED_ID,
        baseline_artifact_version_iri=BASELINE_ARTIFACT,
        observed_artifact_version_iri=OBSERVED_ARTIFACT,
        pairing_assertion_id=PAIRING,
        consumer_scope_id=CONSUMER_SCOPE,
        evaluation_time=NOW,
        detector_id="urn:example:detector:relation-difference",
        detector_version="explicit-denial-v1",
        snapshot_id="snapshot-2026-07-24",
    )


def _binding(
    assertion: RelationAssertion,
    artifact: str,
) -> tuple[RelationEvidenceBinding, tuple[str, str], str]:
    source_field = f"body-{assertion.assertion_id.rsplit(':', 1)[-1]}"
    source_text = f"Locked source evidence for {assertion.assertion_id} with polarity {assertion.polarity}."
    binding = RelationEvidenceBinding(
        binding_id=f"urn:example:evidence:{assertion.assertion_id.rsplit(':', 1)[-1]}",
        assertion_id=assertion.assertion_id,
        source_fragment_id=(f"urn:example:fragment:{assertion.assertion_id.rsplit(':', 1)[-1]}"),
        artifact_version_iri=artifact,
        source_field=source_field,
        start_char=0,
        end_char=len(source_text),
        exact_text=source_text,
        source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),
    )
    return binding, (artifact, source_field), source_text


def _attestation(
    assertion: RelationAssertion,
    *,
    decision: AttestationDecision = "rkaf:approved",
) -> AssertionAttestation:
    return AssertionAttestation(
        attestation_id=(
            f"urn:example:attestation:{assertion.assertion_id.rsplit(':', 1)[-1]}:{decision.rsplit(':', 1)[-1]}"
        ),
        assertion_id=assertion.assertion_id,
        decision=decision,
        attestation_scope_id=CONSUMER_SCOPE,
        attestor_id="urn:example:reviewer:human",
        attested_at=datetime(
            2026,
            7,
            24,
            19,
            0,
            tzinfo=timezone.utc,
        ),
    )


def _dependencies(
    expected: RelationAssertion,
    observed: list[RelationAssertion],
    *,
    reject_ids: frozenset[str] = frozenset(),
    omit_attestation_ids: frozenset[str] = frozenset(),
    omit_evidence_ids: frozenset[str] = frozenset(),
    invalid_evidence_ids: frozenset[str] = frozenset(),
    warrant_known: bool = True,
    paired: bool = True,
    scope_relations: dict[
        tuple[str | None, str | None],
        ScopeDeclaration,
    ]
    | None = None,
) -> RelationComparisonDependencies:
    assertions = [expected, *observed]
    relations = {assertion.relation_key: (f"urn:example:catalog:{index}") for index, assertion in enumerate(assertions)}
    attestations = [
        _attestation(
            assertion,
            decision=("rkaf:rejected" if assertion.assertion_id in reject_ids else "rkaf:approved"),
        )
        for assertion in assertions
        if assertion.assertion_id not in omit_attestation_ids
    ]
    bindings: list[RelationEvidenceBinding] = []
    source_fields: dict[tuple[str, str], str] = {}
    for assertion in assertions:
        if assertion.assertion_id in omit_evidence_ids:
            continue
        artifact = BASELINE_ARTIFACT if assertion.assertion_id == expected.assertion_id else OBSERVED_ARTIFACT
        binding, source_key, source_text = _binding(
            assertion,
            artifact,
        )
        if assertion.assertion_id in invalid_evidence_ids:
            source_text = f"{source_text} changed"
        bindings.append(binding)
        source_fields[source_key] = source_text

    default_scopes: dict[
        tuple[str | None, str | None],
        ScopeDeclaration,
    ] = {
        (SCOPE, SCOPE): ScopeDeclaration(
            "equivalent",
            ("urn:example:scope-proof:aviation",),
            "The profile declares these scopes equivalent.",
        )
    }
    return RelationComparisonDependencies(
        predicates=StaticPredicateCatalog(relations),
        state=AttestationStateResolver(attestations),
        evidence=BoundEvidenceResolver(bindings, source_fields),
        baselines=StaticBaselineResolver(({WARRANT: ("urn:example:warrant-proof:review",)} if warrant_known else {})),
        pairing=StaticPairingResolver(
            (
                {
                    (
                        BASELINE_ARTIFACT,
                        OBSERVED_ARTIFACT,
                        PAIRING,
                    ): ("urn:example:pairing-proof:review",)
                }
                if paired
                else {}
            )
        ),
        scopes=DeclaredScopeComparator(scope_relations or default_scopes),
    )


def _compare(
    observed: list[RelationAssertion],
    *,
    reject_ids: frozenset[str] = frozenset(),
    omit_attestation_ids: frozenset[str] = frozenset(),
    omit_evidence_ids: frozenset[str] = frozenset(),
    invalid_evidence_ids: frozenset[str] = frozenset(),
    warrant_known: bool = True,
    paired: bool = True,
    scope_relations: dict[
        tuple[str | None, str | None],
        ScopeDeclaration,
    ]
    | None = None,
):
    expected = _expected()
    dependencies = _dependencies(
        expected,
        observed,
        reject_ids=reject_ids,
        omit_attestation_ids=omit_attestation_ids,
        omit_evidence_ids=omit_evidence_ids,
        invalid_evidence_ids=invalid_evidence_ids,
        warrant_known=warrant_known,
        paired=paired,
        scope_relations=scope_relations,
    )
    return compare_relation_assertions(
        expected,
        observed,
        context=_context(),
        dependencies=dependencies,
    )


def test_explicit_denial_emits_neutral_evidence_backed_finding() -> None:
    denied = _observed("rkaf:denied")

    result = _compare([denied])

    assert result.outcome == "discrepancy"
    assert result.finding is not None
    assert result.finding.kind == "affirmed_denied_discrepancy"
    assert result.finding.observed_assertion_ids == (OBSERVED_ID,)
    assert result.finding.affected_object_iri == GROUP
    assert "intent" in result.finding.rationale
    assert result.proof_ids


def test_comparison_returns_content_addressed_resolver_proofs() -> None:
    first = _compare([_observed("rkaf:denied")])
    second = _compare([_observed("rkaf:denied")])

    assert first.proof_records
    assert first.proof_ids == tuple(
        record.proof_id
        for record in first.proof_records
    )
    assert first.proof_ids == second.proof_ids
    for record in first.proof_records:
        assert record.proof_id == (
            f"urn:spicy-regs:resolver-proof:{record.record_digest}"
        )
        assert record.comparison_id == _context().comparison_id
        assert record.evaluated_at == NOW
        assert record.as_dict()["record_digest"] == record.record_digest

    changed_policy = replace(
        first.proof_records[0],
        policy_version="relation-comparison-v2",
    )
    assert changed_policy.proof_id != first.proof_records[0].proof_id


def test_affirmed_observation_satisfies_baseline() -> None:
    result = _compare([_observed("rkaf:affirmed")])

    assert result.outcome == "satisfied"
    assert result.finding is None


def test_absence_remains_unknown_without_closure_api() -> None:
    result = _compare([])

    assert result.outcome == "unknown"
    assert result.finding is None


def test_rejected_observation_never_enters_accepted_current_view() -> None:
    denied = _observed("rkaf:denied")

    result = _compare(
        [denied],
        reject_ids=frozenset({OBSERVED_ID}),
    )

    assert result.outcome == "unknown"
    assert result.considered_observation_ids == ()
    assert result.finding is None


def test_missing_attestation_cannot_be_laundered_into_finding() -> None:
    denied = _observed("rkaf:denied")

    result = _compare(
        [denied],
        omit_attestation_ids=frozenset({OBSERVED_ID}),
    )

    assert result.outcome == "unknown"
    assert result.finding is None


def test_invalid_exact_evidence_cannot_be_laundered_into_finding() -> None:
    denied = _observed("rkaf:denied")

    result = _compare(
        [denied],
        invalid_evidence_ids=frozenset({OBSERVED_ID}),
    )

    assert result.outcome == "unknown"
    assert result.finding is None


def test_unresolvable_warrant_disables_expected_baseline() -> None:
    result = _compare(
        [_observed("rkaf:denied")],
        warrant_known=False,
    )

    assert result.outcome == "unknown"
    assert result.finding is None


def test_unrelated_artifacts_are_not_comparable() -> None:
    result = _compare(
        [_observed("rkaf:denied")],
        paired=False,
    )

    assert result.outcome == "not_comparable"
    assert result.finding is None


def test_disjoint_scopes_are_not_comparable() -> None:
    denied = _observed("rkaf:denied", scope_id=OTHER_SCOPE)
    result = _compare(
        [denied],
        scope_relations={
            (SCOPE, OTHER_SCOPE): ScopeDeclaration(
                "disjoint",
                ("urn:example:scope-proof:disjoint",),
                "The profile declares these scopes disjoint.",
            )
        },
    )

    assert result.outcome == "not_comparable"
    assert result.finding is None


def test_unknown_scope_relation_abstains() -> None:
    denied = _observed("rkaf:denied", scope_id=OTHER_SCOPE)
    result = _compare([denied])

    assert result.outcome == "unknown"
    assert result.finding is None


def test_affirmed_and_denied_observations_surface_conflict() -> None:
    result = _compare(
        [
            _observed(
                "rkaf:affirmed",
                assertion_id="urn:example:assertion:affirmed",
            ),
            _observed(
                "rkaf:denied",
                assertion_id="urn:example:assertion:denied",
            ),
        ]
    )

    assert result.outcome == "conflict"
    assert result.finding is None


def test_different_predicate_does_not_create_false_exclusion() -> None:
    result = _compare(
        [
            _observed(
                "rkaf:denied",
                predicate_iri=OTHER_PREDICATE,
            )
        ]
    )

    assert result.outcome == "unknown"
    assert result.finding is None


def test_finding_occurrence_changes_when_reused_id_changes_content() -> None:
    first_observed = _observed("rkaf:denied")
    first = _compare([first_observed])
    second_observed = _observed(
        "rkaf:denied",
        origin="rkaf:humanAsserted",
    )
    second = _compare([second_observed])

    assert first.finding is not None
    assert second.finding is not None
    assert first.finding.finding_fingerprint == (second.finding.finding_fingerprint)
    assert first.finding.finding_id != second.finding.finding_id


def test_runtime_rejects_invalid_literal_value() -> None:
    with pytest.raises(
        OntologyInvariantError,
        match="unsupported assertion polarity",
    ):
        _assertion(
            OBSERVED_ID,
            cast(AssertionPolarity, "rkaf:maybe"),
        )


def test_ai_touched_assertion_requires_lineage() -> None:
    with pytest.raises(
        OntologyInvariantError,
        match="requires AI lineage",
    ):
        _observed(
            "rkaf:denied",
            origin="rkaf:aiSuggested",
        )


def test_comparison_requires_timezone_aware_instant() -> None:
    with pytest.raises(
        OntologyInvariantError,
        match="include a timezone",
    ):
        RelationComparisonContext(
            comparison_id="urn:example:comparison:bad-time",
            expected_assertion_id=EXPECTED_ID,
            baseline_artifact_version_iri=BASELINE_ARTIFACT,
            observed_artifact_version_iri=OBSERVED_ARTIFACT,
            pairing_assertion_id=PAIRING,
            consumer_scope_id=CONSUMER_SCOPE,
            evaluation_time=datetime(2026, 7, 24, 20, 0),
            detector_id="urn:example:detector:relation-difference",
            detector_version="explicit-denial-v1",
            snapshot_id="snapshot-2026-07-24",
        )

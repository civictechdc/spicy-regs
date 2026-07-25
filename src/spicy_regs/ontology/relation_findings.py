"""Experimental, evidence-gated comparison of Rulespec relationship assertions.

This module intentionally implements only explicit affirmation-versus-denial
comparison. Missing relations remain unknown; closed-world omission analysis is
outside this experiment.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol, cast
from urllib.parse import urlsplit

from spicy_regs.ontology.common import canonical_json, stable_id
from spicy_regs.ontology.invariants import OntologyInvariantError

AssertionPolarity = Literal["rkaf:affirmed", "rkaf:denied"]
AssertionOrigin = Literal[
    "rkaf:humanAsserted",
    "rkaf:aiSuggested",
    "rkaf:aiPromoted",
    "rkaf:humanQualified",
    "rkaf:humanRevalidation",
    "rkaf:imported",
]
AttestationDecision = Literal[
    "rkaf:approved",
    "rkaf:approvedWithConditions",
    "rkaf:rejected",
    "rkaf:abstained",
    "rkaf:advisory",
    "rkaf:endorsedForReview",
    "rkaf:flaggedForReview",
]
GateStatus = Literal["pass", "fail", "unknown"]
ScopeRelation = Literal[
    "equivalent",
    "observed_subsumes_expected",
    "observed_narrows_expected",
    "overlaps",
    "disjoint",
    "unknown",
]
ComparisonOutcome = Literal[
    "satisfied",
    "discrepancy",
    "conflict",
    "not_comparable",
    "unknown",
]
RelationFindingKind = Literal["affirmed_denied_discrepancy"]
ResolverProofType = Literal[
    "predicate_catalog",
    "assertion_attestation",
    "evidence_binding",
    "baseline_warrant",
    "artifact_pairing",
    "scope_declaration",
]
ResolverProofOutcome = GateStatus | ScopeRelation

_ASSERTION_POLARITIES = frozenset({"rkaf:affirmed", "rkaf:denied"})
_ASSERTION_ORIGINS = frozenset(
    {
        "rkaf:humanAsserted",
        "rkaf:aiSuggested",
        "rkaf:aiPromoted",
        "rkaf:humanQualified",
        "rkaf:humanRevalidation",
        "rkaf:imported",
    }
)
_AI_TOUCHED_ORIGINS = frozenset(
    {
        "rkaf:aiSuggested",
        "rkaf:aiPromoted",
        "rkaf:humanQualified",
        "rkaf:humanRevalidation",
    }
)
_ATTESTATION_DECISIONS = frozenset(
    {
        "rkaf:approved",
        "rkaf:approvedWithConditions",
        "rkaf:rejected",
        "rkaf:abstained",
        "rkaf:advisory",
        "rkaf:endorsedForReview",
        "rkaf:flaggedForReview",
    }
)
_APPROVAL_DECISIONS = frozenset({"rkaf:approved", "rkaf:approvedWithConditions"})
_RESOLVER_PROOF_TYPES = frozenset(
    {
        "predicate_catalog",
        "assertion_attestation",
        "evidence_binding",
        "baseline_warrant",
        "artifact_pairing",
        "scope_declaration",
    }
)
_RESOLVER_PROOF_OUTCOMES = frozenset(
    {
        "pass",
        "fail",
        "unknown",
        "equivalent",
        "observed_subsumes_expected",
        "observed_narrows_expected",
        "overlaps",
        "disjoint",
    }
)


def _require_iri(field_name: str, value: str) -> None:
    if not value or any(character.isspace() for character in value) or not urlsplit(value).scheme:
        raise OntologyInvariantError(f"{field_name} must be an absolute IRI")


def _require_aware_instant(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OntologyInvariantError(f"{field_name} must include a timezone")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _content_digest(payload: object) -> str:
    return _sha256(canonical_json(payload))


@dataclass(frozen=True)
class RelationAssertion:
    """One immutable Rulespec RelationshipAssertion projection."""

    assertion_id: str
    subject_iri: str
    predicate_iri: str
    object_iri: str
    polarity: AssertionPolarity
    assertion_origin: AssertionOrigin
    applicability_scope_id: str | None = None
    warrant_ids: tuple[str, ...] = ()
    confidence_record_ids: tuple[str, ...] = ()
    ai_lineage_id: str | None = None
    generated_by: str | None = None
    run_id: str | None = None
    asserted_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "assertion_id",
            "subject_iri",
            "predicate_iri",
            "object_iri",
        ):
            _require_iri(field_name, getattr(self, field_name))
        if self.polarity not in _ASSERTION_POLARITIES:
            raise OntologyInvariantError(f"unsupported assertion polarity {self.polarity!r}")
        if self.assertion_origin not in _ASSERTION_ORIGINS:
            raise OntologyInvariantError(f"unsupported assertion origin {self.assertion_origin!r}")
        optional_iris = (
            ("applicability_scope_id", self.applicability_scope_id),
            ("ai_lineage_id", self.ai_lineage_id),
            ("generated_by", self.generated_by),
        )
        for field_name, value in optional_iris:
            if value is not None:
                _require_iri(field_name, value)
        for field_name, values in (
            ("warrant_ids", self.warrant_ids),
            ("confidence_record_ids", self.confidence_record_ids),
        ):
            if len(values) != len(set(values)):
                raise OntologyInvariantError(f"{field_name} must not contain duplicates")
            for value in values:
                _require_iri(field_name, value)
        if self.assertion_origin in _AI_TOUCHED_ORIGINS and self.ai_lineage_id is None:
            raise OntologyInvariantError("AI-touched relationship assertion requires AI lineage")
        if self.asserted_at is not None:
            _require_aware_instant("asserted_at", self.asserted_at)
        if (self.generated_by is None) != (self.run_id is None):
            raise OntologyInvariantError("generated_by and run_id must be supplied together")

    @property
    def relation_key(self) -> tuple[str, str, str]:
        """Return a grouping key, not a published proposition identity."""
        return (
            self.subject_iri,
            self.predicate_iri,
            self.object_iri,
        )

    @property
    def relation_fingerprint(self) -> str:
        return stable_id("relation-key", *self.relation_key)

    @property
    def assertion_digest(self) -> str:
        """Bind finding identity to immutable assertion content."""
        return _content_digest(
            {
                "assertion_id": self.assertion_id,
                "relation_key": self.relation_key,
                "polarity": self.polarity,
                "assertion_origin": self.assertion_origin,
                "applicability_scope_id": self.applicability_scope_id,
                "warrant_ids": sorted(self.warrant_ids),
                "confidence_record_ids": sorted(self.confidence_record_ids),
                "ai_lineage_id": self.ai_lineage_id,
                "generated_by": self.generated_by,
                "run_id": self.run_id,
                "asserted_at": (self.asserted_at.isoformat() if self.asserted_at is not None else None),
            }
        )


@dataclass(frozen=True)
class RelationEvidenceBinding:
    """An exact source fragment bound to one assertion occurrence."""

    binding_id: str
    assertion_id: str
    source_fragment_id: str
    artifact_version_iri: str
    source_field: str
    start_char: int
    end_char: int
    exact_text: str
    source_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "binding_id",
            "assertion_id",
            "source_fragment_id",
            "artifact_version_iri",
        ):
            _require_iri(field_name, getattr(self, field_name))
        if not self.source_field or not self.exact_text:
            raise OntologyInvariantError("evidence binding requires source field and exact text")
        if (
            isinstance(self.start_char, bool)
            or isinstance(self.end_char, bool)
            or self.start_char < 0
            or self.end_char <= self.start_char
        ):
            raise OntologyInvariantError("evidence binding requires a valid half-open character span")
        if len(self.source_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.source_sha256
        ):
            raise OntologyInvariantError("evidence binding requires a lowercase SHA-256 digest")

    def validates(self, source_text: str) -> bool:
        return (
            _sha256(source_text) == self.source_sha256
            and self.end_char <= len(source_text)
            and source_text[self.start_char : self.end_char] == self.exact_text
        )

    @property
    def binding_digest(self) -> str:
        return _content_digest(
            {
                "binding_id": self.binding_id,
                "assertion_id": self.assertion_id,
                "source_fragment_id": self.source_fragment_id,
                "artifact_version_iri": self.artifact_version_iri,
                "source_field": self.source_field,
                "start_char": self.start_char,
                "end_char": self.end_char,
                "exact_text": self.exact_text,
                "source_sha256": self.source_sha256,
            }
        )


@dataclass(frozen=True)
class AssertionAttestation:
    """A scoped, temporal social decision over an immutable assertion."""

    attestation_id: str
    assertion_id: str
    decision: AttestationDecision
    attestation_scope_id: str
    attestor_id: str
    attested_at: datetime
    effective_start: datetime | None = None
    effective_end: datetime | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "attestation_id",
            "assertion_id",
            "attestation_scope_id",
            "attestor_id",
        ):
            _require_iri(field_name, getattr(self, field_name))
        if self.decision not in _ATTESTATION_DECISIONS:
            raise OntologyInvariantError(f"unsupported attestation decision {self.decision!r}")
        for field_name, value in (
            ("attested_at", self.attested_at),
            ("effective_start", self.effective_start),
            ("effective_end", self.effective_end),
            ("revoked_at", self.revoked_at),
        ):
            if value is not None:
                _require_aware_instant(field_name, value)
        if (
            self.effective_start is not None
            and self.effective_end is not None
            and self.effective_end < self.effective_start
        ):
            raise OntologyInvariantError("attestation effective period ends before it starts")

    def is_effective_at(
        self,
        evaluation_time: datetime,
        scope_id: str,
    ) -> bool:
        if self.attestation_scope_id != scope_id:
            return False
        if self.attested_at > evaluation_time:
            return False
        if self.effective_start is not None and self.effective_start > evaluation_time:
            return False
        if self.effective_end is not None and self.effective_end < evaluation_time:
            return False
        return self.revoked_at is None or self.revoked_at > evaluation_time

    @property
    def attestation_digest(self) -> str:
        return _content_digest(
            {
                "attestation_id": self.attestation_id,
                "assertion_id": self.assertion_id,
                "decision": self.decision,
                "attestation_scope_id": self.attestation_scope_id,
                "attestor_id": self.attestor_id,
                "attested_at": self.attested_at.isoformat(),
                "effective_start": (
                    self.effective_start.isoformat()
                    if self.effective_start is not None
                    else None
                ),
                "effective_end": (
                    self.effective_end.isoformat()
                    if self.effective_end is not None
                    else None
                ),
                "revoked_at": (
                    self.revoked_at.isoformat()
                    if self.revoked_at is not None
                    else None
                ),
            }
        )


@dataclass(frozen=True)
class ResolverProofRecord:
    """One content-addressed resolver decision and its supporting records."""

    proof_id: str = field(init=False)
    proof_type: ResolverProofType
    resolver_id: str
    resolver_version: str
    policy_id: str
    policy_version: str
    comparison_id: str
    evaluated_at: datetime
    snapshot_id: str
    outcome: ResolverProofOutcome
    rationale: str
    input_ids: tuple[str, ...]
    input_digests: tuple[tuple[str, str], ...] = ()
    supporting_record_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in (
            "resolver_id",
            "policy_id",
            "comparison_id",
        ):
            _require_iri(field_name, getattr(self, field_name))
        if self.proof_type not in _RESOLVER_PROOF_TYPES:
            raise OntologyInvariantError(f"unsupported resolver proof type {self.proof_type!r}")
        if not self.resolver_version or not self.policy_version:
            raise OntologyInvariantError("resolver proof requires resolver and policy versions")
        _require_aware_instant("evaluated_at", self.evaluated_at)
        if not self.snapshot_id or not self.rationale:
            raise OntologyInvariantError("resolver proof requires snapshot and rationale")
        if self.outcome not in _RESOLVER_PROOF_OUTCOMES:
            raise OntologyInvariantError(f"unsupported resolver proof outcome {self.outcome!r}")
        if not self.input_ids:
            raise OntologyInvariantError("resolver proof requires at least one input identifier")
        for field_name, values in (
            ("input_ids", self.input_ids),
            ("supporting_record_ids", self.supporting_record_ids),
        ):
            if len(values) != len(set(values)):
                raise OntologyInvariantError(f"{field_name} must not contain duplicates")
            for value in values:
                _require_iri(field_name, value)
        digest_names: set[str] = set()
        for name, digest in self.input_digests:
            if not name or name in digest_names:
                raise OntologyInvariantError("resolver proof digest names must be unique and nonempty")
            digest_names.add(name)
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise OntologyInvariantError(
                    "resolver proof input digests must be lowercase SHA-256 values"
                )
        object.__setattr__(
            self,
            "proof_id",
            f"urn:spicy-regs:resolver-proof:{self.record_digest}",
        )

    @property
    def record_digest(self) -> str:
        return _content_digest(
            {
                "proof_type": self.proof_type,
                "resolver_id": self.resolver_id,
                "resolver_version": self.resolver_version,
                "policy_id": self.policy_id,
                "policy_version": self.policy_version,
                "comparison_id": self.comparison_id,
                "evaluated_at": self.evaluated_at.isoformat(),
                "snapshot_id": self.snapshot_id,
                "outcome": self.outcome,
                "rationale": self.rationale,
                "input_ids": self.input_ids,
                "input_digests": self.input_digests,
                "supporting_record_ids": self.supporting_record_ids,
            }
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "proof_id": self.proof_id,
            "proof_type": self.proof_type,
            "resolver_id": self.resolver_id,
            "resolver_version": self.resolver_version,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "comparison_id": self.comparison_id,
            "evaluated_at": self.evaluated_at.isoformat(),
            "snapshot_id": self.snapshot_id,
            "outcome": self.outcome,
            "rationale": self.rationale,
            "input_ids": list(self.input_ids),
            "input_digests": [
                {
                    "name": name,
                    "sha256": digest,
                }
                for name, digest in self.input_digests
            ],
            "supporting_record_ids": list(self.supporting_record_ids),
            "record_digest": self.record_digest,
        }


@dataclass(frozen=True)
class ResolverProofIssuer:
    """Issue proof records under one versioned resolver and policy."""

    resolver_id: str
    resolver_version: str
    policy_id: str
    policy_version: str

    def __post_init__(self) -> None:
        _require_iri("resolver_id", self.resolver_id)
        _require_iri("policy_id", self.policy_id)
        if not self.resolver_version or not self.policy_version:
            raise OntologyInvariantError("proof issuer requires resolver and policy versions")

    def issue(
        self,
        proof_type: ResolverProofType,
        *,
        context: RelationComparisonContext,
        outcome: ResolverProofOutcome,
        rationale: str,
        input_ids: Sequence[str],
        input_digests: Sequence[tuple[str, str]] = (),
        supporting_record_ids: Sequence[str] = (),
    ) -> ResolverProofRecord:
        return ResolverProofRecord(
            proof_type=proof_type,
            resolver_id=self.resolver_id,
            resolver_version=self.resolver_version,
            policy_id=self.policy_id,
            policy_version=self.policy_version,
            comparison_id=context.comparison_id,
            evaluated_at=context.evaluation_time,
            snapshot_id=context.snapshot_id,
            outcome=outcome,
            rationale=rationale,
            input_ids=tuple(input_ids),
            input_digests=tuple(sorted(input_digests)),
            supporting_record_ids=tuple(sorted(supporting_record_ids)),
        )


_PREDICATE_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:predicate-catalog",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:canonical-relation",
    policy_version="relation-comparison-v1",
)
_ATTESTATION_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:assertion-state",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:accepted-current",
    policy_version="relation-comparison-v1",
)
_EVIDENCE_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:bound-evidence",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:exact-source-evidence",
    policy_version="relation-comparison-v1",
)
_BASELINE_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:baseline-warrant",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:eligible-baseline",
    policy_version="relation-comparison-v1",
)
_PAIRING_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:artifact-pairing",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:comparable-artifacts",
    policy_version="relation-comparison-v1",
)
_SCOPE_PROOF_ISSUER = ResolverProofIssuer(
    resolver_id="urn:spicy-regs:resolver:declared-scope",
    resolver_version="relation-comparison-v1",
    policy_id="urn:spicy-regs:policy:scope-comparison",
    policy_version="relation-comparison-v1",
)


@dataclass(frozen=True)
class GateDecision:
    proof_record: ResolverProofRecord

    def __post_init__(self) -> None:
        if self.proof_record.outcome not in {"pass", "fail", "unknown"}:
            raise OntologyInvariantError("gate decision requires a gate-status proof")

    @property
    def status(self) -> GateStatus:
        return cast(GateStatus, self.proof_record.outcome)

    @property
    def rationale(self) -> str:
        return self.proof_record.rationale


@dataclass(frozen=True)
class ScopeDecision:
    proof_record: ResolverProofRecord

    def __post_init__(self) -> None:
        if self.proof_record.outcome in {"pass", "fail"}:
            raise OntologyInvariantError("scope decision requires a scope-relation proof")

    @property
    def relation(self) -> ScopeRelation:
        return cast(ScopeRelation, self.proof_record.outcome)

    @property
    def rationale(self) -> str:
        return self.proof_record.rationale


@dataclass(frozen=True)
class ScopeDeclaration:
    """Profile-owned scope relation used to issue a comparison-bound proof."""

    relation: ScopeRelation
    supporting_record_ids: tuple[str, ...]
    rationale: str

    def __post_init__(self) -> None:
        if self.relation not in _RESOLVER_PROOF_OUTCOMES - {"pass", "fail"}:
            raise OntologyInvariantError(f"unsupported scope relation {self.relation!r}")
        if not self.rationale:
            raise OntologyInvariantError("scope declaration requires a rationale")
        for record_id in self.supporting_record_ids:
            _require_iri("supporting_record_ids", record_id)


@dataclass(frozen=True)
class RelationComparisonContext:
    """The explicit artifact, consumer, time, and detector comparison frame."""

    comparison_id: str
    expected_assertion_id: str
    baseline_artifact_version_iri: str
    observed_artifact_version_iri: str
    pairing_assertion_id: str
    consumer_scope_id: str
    evaluation_time: datetime
    detector_id: str
    detector_version: str
    snapshot_id: str

    def __post_init__(self) -> None:
        for field_name in (
            "comparison_id",
            "expected_assertion_id",
            "baseline_artifact_version_iri",
            "observed_artifact_version_iri",
            "pairing_assertion_id",
            "consumer_scope_id",
            "detector_id",
        ):
            _require_iri(field_name, getattr(self, field_name))
        _require_aware_instant("evaluation_time", self.evaluation_time)
        if not self.detector_version or not self.snapshot_id:
            raise OntologyInvariantError("comparison requires detector version and snapshot id")

    @property
    def comparison_digest(self) -> str:
        return _content_digest(
            {
                "comparison_id": self.comparison_id,
                "expected_assertion_id": self.expected_assertion_id,
                "baseline_artifact_version_iri": (self.baseline_artifact_version_iri),
                "observed_artifact_version_iri": (self.observed_artifact_version_iri),
                "pairing_assertion_id": self.pairing_assertion_id,
                "consumer_scope_id": self.consumer_scope_id,
                "evaluation_time": self.evaluation_time.isoformat(),
                "detector_id": self.detector_id,
                "detector_version": self.detector_version,
                "snapshot_id": self.snapshot_id,
            }
        )


@dataclass(frozen=True)
class RelationFinding:
    """One immutable detector occurrence plus a stable correlation key."""

    finding_id: str
    finding_fingerprint: str
    kind: RelationFindingKind
    comparison_id: str
    expected_assertion_id: str
    observed_assertion_ids: tuple[str, ...]
    affected_object_iri: str
    detected_by: str
    detector_version: str
    detected_at: datetime
    proof_ids: tuple[str, ...]
    rationale: str


@dataclass(frozen=True)
class RelationComparisonResult:
    outcome: ComparisonOutcome
    expected_assertion_id: str
    considered_observation_ids: tuple[str, ...]
    proof_ids: tuple[str, ...]
    proof_records: tuple[ResolverProofRecord, ...]
    rationale: str
    finding: RelationFinding | None = None


class PredicateCatalog(Protocol):
    def validate(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision: ...

    def same_relation(
        self,
        expected: RelationAssertion,
        observed: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision: ...


class AssertionStateResolver(Protocol):
    def accepted_current(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision: ...


class EvidenceResolver(Protocol):
    def supported_by(
        self,
        assertion: RelationAssertion,
        artifact_version_iri: str,
        context: RelationComparisonContext,
    ) -> GateDecision: ...


class BaselineResolver(Protocol):
    def eligible_baseline(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision: ...


class PairingResolver(Protocol):
    def paired(self, context: RelationComparisonContext) -> GateDecision: ...


class ScopeComparator(Protocol):
    def compare(
        self,
        expected: RelationAssertion,
        observed: RelationAssertion,
        context: RelationComparisonContext,
    ) -> ScopeDecision: ...


@dataclass(frozen=True)
class RelationComparisonDependencies:
    """Dependency-inverted semantic services used by the generic comparator."""

    predicates: PredicateCatalog
    state: AssertionStateResolver
    evidence: EvidenceResolver
    baselines: BaselineResolver
    pairing: PairingResolver
    scopes: ScopeComparator


def compare_relation_assertions(
    expected: RelationAssertion,
    observed: Sequence[RelationAssertion],
    *,
    context: RelationComparisonContext,
    dependencies: RelationComparisonDependencies,
) -> RelationComparisonResult:
    """Compare one accepted baseline against accepted source observations."""
    if expected.assertion_id != context.expected_assertion_id:
        raise OntologyInvariantError("comparison expected assertion id does not match its context")
    if expected.polarity != "rkaf:affirmed":
        raise OntologyInvariantError("explicit-denial comparison requires an affirmed baseline")

    expected_gates = (
        dependencies.predicates.validate(expected, context),
        dependencies.state.accepted_current(expected, context),
        dependencies.evidence.supported_by(
            expected,
            context.baseline_artifact_version_iri,
            context,
        ),
        dependencies.baselines.eligible_baseline(expected, context),
    )
    failed_expected = next(
        (gate for gate in expected_gates if gate.status != "pass"),
        None,
    )
    if failed_expected is not None:
        return _result(
            "unknown",
            expected,
            (),
            expected_gates,
            "The expected baseline is not fully resolved and accepted.",
        )

    pairing = dependencies.pairing.paired(context)
    if pairing.status != "pass":
        return _result(
            "not_comparable",
            expected,
            (),
            (*expected_gates, pairing),
            "The baseline and observed artifacts are not source-backed peers.",
        )

    affirmed: list[RelationAssertion] = []
    denied: list[RelationAssertion] = []
    gates: list[GateDecision | ScopeDecision] = [
        *expected_gates,
        pairing,
    ]
    considered: list[str] = []
    unresolved_relevant_input = False
    saw_disjoint_scope = False

    for item in observed:
        state = dependencies.state.accepted_current(item, context)
        if state.status == "fail":
            continue
        if state.status == "unknown":
            unresolved_relevant_input = True
            gates.append(state)
            continue

        evidence = dependencies.evidence.supported_by(
            item,
            context.observed_artifact_version_iri,
            context,
        )
        predicate = dependencies.predicates.validate(item, context)
        relation = dependencies.predicates.same_relation(
            expected,
            item,
            context,
        )
        gates.extend((state, evidence, predicate, relation))
        if evidence.status != "pass" or predicate.status != "pass":
            unresolved_relevant_input = True
            continue
        if relation.status == "fail":
            continue
        if relation.status != "pass":
            unresolved_relevant_input = True
            continue

        scope = dependencies.scopes.compare(expected, item, context)
        gates.append(scope)
        if scope.relation in {"disjoint"}:
            saw_disjoint_scope = True
            continue
        if scope.relation not in {
            "equivalent",
            "observed_subsumes_expected",
        }:
            unresolved_relevant_input = True
            continue

        considered.append(item.assertion_id)
        if item.polarity == "rkaf:affirmed":
            affirmed.append(item)
        else:
            denied.append(item)

    if affirmed and denied:
        return _result(
            "conflict",
            expected,
            considered,
            gates,
            "Comparable accepted observations both affirm and deny the relation.",
        )
    if denied:
        proof_records = _proof_records(gates)
        proof_ids = tuple(record.proof_id for record in proof_records)
        finding = _finding(
            expected,
            denied,
            context=context,
            proof_records=proof_records,
        )
        return RelationComparisonResult(
            outcome="discrepancy",
            expected_assertion_id=expected.assertion_id,
            considered_observation_ids=tuple(sorted(considered)),
            proof_ids=proof_ids,
            proof_records=proof_records,
            rationale=("Accepted source evidence denies the affirmed baseline within a comparable scope."),
            finding=finding,
        )
    if affirmed:
        return _result(
            "satisfied",
            expected,
            considered,
            gates,
            "Accepted source evidence affirms the baseline relation.",
        )
    if saw_disjoint_scope and not unresolved_relevant_input:
        return _result(
            "not_comparable",
            expected,
            considered,
            gates,
            "The matching relation occurs only in a disjoint scope.",
        )
    return _result(
        "unknown",
        expected,
        considered,
        gates,
        (
            "No comparable accepted observation resolves the relation."
            if not unresolved_relevant_input
            else "Potentially relevant input remains unresolved."
        ),
    )


def _proof_records(
    decisions: Sequence[GateDecision | ScopeDecision],
) -> tuple[ResolverProofRecord, ...]:
    records_by_id: dict[str, ResolverProofRecord] = {}
    for decision in decisions:
        record = decision.proof_record
        prior = records_by_id.setdefault(record.proof_id, record)
        if prior.record_digest != record.record_digest:
            raise OntologyInvariantError(
                f"resolver proof identifier {record.proof_id!r} has conflicting content"
            )
    return tuple(records_by_id[proof_id] for proof_id in sorted(records_by_id))


def _result(
    outcome: ComparisonOutcome,
    expected: RelationAssertion,
    considered: Sequence[str],
    decisions: Sequence[GateDecision | ScopeDecision],
    rationale: str,
) -> RelationComparisonResult:
    proof_records = _proof_records(decisions)
    return RelationComparisonResult(
        outcome=outcome,
        expected_assertion_id=expected.assertion_id,
        considered_observation_ids=tuple(sorted(considered)),
        proof_ids=tuple(record.proof_id for record in proof_records),
        proof_records=proof_records,
        rationale=rationale,
    )


def _finding(
    expected: RelationAssertion,
    denied: Sequence[RelationAssertion],
    *,
    context: RelationComparisonContext,
    proof_records: tuple[ResolverProofRecord, ...],
) -> RelationFinding:
    kind: RelationFindingKind = "affirmed_denied_discrepancy"
    denied_sorted = sorted(denied, key=lambda item: item.assertion_id)
    finding_fingerprint = stable_id(
        "relation-finding-fingerprint",
        kind,
        expected.relation_fingerprint,
    )
    finding_id = stable_id(
        "relation-finding",
        finding_fingerprint,
        context.comparison_digest,
        expected.assertion_digest,
        *(item.assertion_digest for item in denied_sorted),
        *(record.record_digest for record in proof_records),
    )
    return RelationFinding(
        finding_id=finding_id,
        finding_fingerprint=finding_fingerprint,
        kind=kind,
        comparison_id=context.comparison_id,
        expected_assertion_id=expected.assertion_id,
        observed_assertion_ids=tuple(item.assertion_id for item in denied_sorted),
        affected_object_iri=expected.object_iri,
        detected_by=context.detector_id,
        detector_version=context.detector_version,
        detected_at=context.evaluation_time,
        proof_ids=tuple(record.proof_id for record in proof_records),
        rationale=(
            "Comparable accepted source evidence explicitly denies the "
            "affirmed baseline relation. No actor intent is inferred."
        ),
    )


class StaticPredicateCatalog:
    """A small profile-owned catalog for an immutable experiment."""

    def __init__(
        self,
        relations: Mapping[tuple[str, str, str], str],
        *,
        proof_issuer: ResolverProofIssuer = _PREDICATE_PROOF_ISSUER,
    ) -> None:
        self._relations = dict(relations)
        self._proof_issuer = proof_issuer

    def validate(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision:
        supporting_record_id = self._relations.get(assertion.relation_key)
        status: GateStatus = "pass" if supporting_record_id is not None else "fail"
        rationale = (
            "The predicate direction and both entities resolve in the catalog."
            if supporting_record_id is not None
            else "The relation is absent from the versioned predicate/entity catalog."
        )
        return GateDecision(
            self._proof_issuer.issue(
                "predicate_catalog",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=assertion.relation_key,
                input_digests=(
                    (
                        "relation_key",
                        _content_digest(assertion.relation_key),
                    ),
                ),
                supporting_record_ids=(
                    (supporting_record_id,)
                    if supporting_record_id is not None
                    else ()
                ),
            )
        )

    def same_relation(
        self,
        expected: RelationAssertion,
        observed: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision:
        same_key = expected.relation_key == observed.relation_key
        supporting_record_id = (
            self._relations.get(expected.relation_key)
            if same_key
            else None
        )
        status: GateStatus = (
            "fail"
            if not same_key
            else ("pass" if supporting_record_id is not None else "unknown")
        )
        rationale = (
            "The canonical relation keys differ."
            if not same_key
            else "The canonical relation keys are identical."
        )
        return GateDecision(
            self._proof_issuer.issue(
                "predicate_catalog",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=tuple(
                    dict.fromkeys(
                        (
                            *expected.relation_key,
                            *observed.relation_key,
                        )
                    )
                ),
                input_digests=(
                    (
                        "expected_relation_key",
                        _content_digest(expected.relation_key),
                    ),
                    (
                        "observed_relation_key",
                        _content_digest(observed.relation_key),
                    ),
                ),
                supporting_record_ids=(
                    (supporting_record_id,)
                    if supporting_record_id is not None
                    else ()
                ),
            )
        )


class AttestationStateResolver:
    """Compute accepted-current state for one consumer scope and instant."""

    def __init__(
        self,
        attestations: Sequence[AssertionAttestation],
        *,
        proof_issuer: ResolverProofIssuer = _ATTESTATION_PROOF_ISSUER,
    ) -> None:
        self._attestations = tuple(attestations)
        self._proof_issuer = proof_issuer

    def accepted_current(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision:
        effective = [
            attestation
            for attestation in self._attestations
            if attestation.assertion_id == assertion.assertion_id
            and attestation.is_effective_at(
                context.evaluation_time,
                context.consumer_scope_id,
            )
        ]
        approvals = [item for item in effective if item.decision in _APPROVAL_DECISIONS]
        rejections = [item for item in effective if item.decision == "rkaf:rejected"]
        status: GateStatus
        rationale: str
        if approvals and rejections:
            status = "unknown"
            rationale = "Effective attestations conflict in this consumer scope."
        elif rejections:
            status = "fail"
            rationale = "The assertion is rejected in this consumer scope."
        elif approvals:
            status = "pass"
            rationale = "The assertion is accepted in this consumer scope."
        else:
            status = "unknown"
            rationale = "No effective approval exists in this consumer scope."
        return GateDecision(
            self._proof_issuer.issue(
                "assertion_attestation",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=(
                    assertion.assertion_id,
                    context.consumer_scope_id,
                ),
                input_digests=(
                    ("assertion", assertion.assertion_digest),
                    *(
                        (
                            f"attestation:{item.attestation_id}",
                            item.attestation_digest,
                        )
                        for item in effective
                    ),
                ),
                supporting_record_ids=tuple(
                    item.attestation_id
                    for item in effective
                ),
            )
        )


class BoundEvidenceResolver:
    """Resolve exact, digest-bound evidence against locked source fields."""

    def __init__(
        self,
        bindings: Sequence[RelationEvidenceBinding],
        source_fields: Mapping[tuple[str, str], str],
        *,
        proof_issuer: ResolverProofIssuer = _EVIDENCE_PROOF_ISSUER,
    ) -> None:
        self._bindings = tuple(bindings)
        self._source_fields = dict(source_fields)
        self._proof_issuer = proof_issuer

    def supported_by(
        self,
        assertion: RelationAssertion,
        artifact_version_iri: str,
        context: RelationComparisonContext,
    ) -> GateDecision:
        matching = [
            binding
            for binding in self._bindings
            if binding.assertion_id == assertion.assertion_id and binding.artifact_version_iri == artifact_version_iri
        ]
        if not matching:
            return GateDecision(
                self._proof_issuer.issue(
                    "evidence_binding",
                    context=context,
                    outcome="fail",
                    rationale=(
                        "No evidence binding targets the required artifact version."
                    ),
                    input_ids=(
                        assertion.assertion_id,
                        artifact_version_iri,
                    ),
                    input_digests=(
                        ("assertion", assertion.assertion_digest),
                    ),
                )
            )
        source_texts = {
            binding.binding_id: self._source_fields.get(
                (
                    binding.artifact_version_iri,
                    binding.source_field,
                )
            )
            for binding in matching
        }
        valid = [
            binding
            for binding in matching
            if (
                source_text := source_texts[binding.binding_id]
            )
            is not None
            and binding.validates(source_text)
        ]
        status: GateStatus = "pass" if len(valid) == len(matching) else "fail"
        rationale = (
            "Every bound span aligns exactly to the locked artifact version."
            if status == "pass"
            else "At least one evidence span or source digest is invalid."
        )
        input_digests: list[tuple[str, str]] = [
            ("assertion", assertion.assertion_digest),
        ]
        for binding in matching:
            input_digests.append(
                (
                    f"binding:{binding.binding_id}",
                    binding.binding_digest,
                )
            )
            source_text = source_texts[binding.binding_id]
            if source_text is not None:
                input_digests.append(
                    (
                        f"source:{binding.binding_id}",
                        _sha256(source_text),
                    )
                )
        return GateDecision(
            self._proof_issuer.issue(
                "evidence_binding",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=(
                    assertion.assertion_id,
                    artifact_version_iri,
                ),
                input_digests=input_digests,
                supporting_record_ids=tuple(
                    binding.binding_id
                    for binding in matching
                ),
            )
        )


class StaticBaselineResolver:
    """Resolve only explicitly registered warrants as comparison baselines."""

    def __init__(
        self,
        warrants: Mapping[str, Sequence[str]],
        *,
        proof_issuer: ResolverProofIssuer = _BASELINE_PROOF_ISSUER,
    ) -> None:
        self._warrants = {warrant_id: tuple(proofs) for warrant_id, proofs in warrants.items()}
        self._proof_issuer = proof_issuer

    def eligible_baseline(
        self,
        assertion: RelationAssertion,
        context: RelationComparisonContext,
    ) -> GateDecision:
        resolved = [warrant_id for warrant_id in assertion.warrant_ids if warrant_id in self._warrants]
        status: GateStatus = "pass" if resolved else "fail"
        rationale = (
            "A registered warrant authorizes this comparison baseline."
            if resolved
            else "No assertion warrant resolves as an eligible baseline."
        )
        proofs = {proof for warrant_id in resolved for proof in (warrant_id, *self._warrants[warrant_id])}
        return GateDecision(
            self._proof_issuer.issue(
                "baseline_warrant",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=tuple(
                    dict.fromkeys(
                        (
                            assertion.assertion_id,
                            *assertion.warrant_ids,
                        )
                    )
                ),
                input_digests=(
                    ("assertion", assertion.assertion_digest),
                    (
                        "warrant_set",
                        _content_digest(sorted(assertion.warrant_ids)),
                    ),
                ),
                supporting_record_ids=tuple(sorted(proofs)),
            )
        )


class StaticPairingResolver:
    """Require an approved relation between the exact artifact versions."""

    def __init__(
        self,
        pairings: Mapping[tuple[str, str, str], Sequence[str]],
        *,
        proof_issuer: ResolverProofIssuer = _PAIRING_PROOF_ISSUER,
    ) -> None:
        self._pairings = {key: tuple(proofs) for key, proofs in pairings.items()}
        self._proof_issuer = proof_issuer

    def paired(self, context: RelationComparisonContext) -> GateDecision:
        key = (
            context.baseline_artifact_version_iri,
            context.observed_artifact_version_iri,
            context.pairing_assertion_id,
        )
        proofs = self._pairings.get(key)
        status: GateStatus = "pass" if proofs is not None else "fail"
        rationale = (
            "An approved pairing binds the exact artifact versions."
            if proofs is not None
            else "No approved pairing binds these artifact versions."
        )
        return GateDecision(
            self._proof_issuer.issue(
                "artifact_pairing",
                context=context,
                outcome=status,
                rationale=rationale,
                input_ids=key,
                input_digests=(
                    (
                        "artifact_pair",
                        _content_digest(key),
                    ),
                ),
                supporting_record_ids=(
                    tuple(
                        sorted(
                            {
                                context.pairing_assertion_id,
                                *proofs,
                            }
                        )
                    )
                    if proofs is not None
                    else ()
                ),
            )
        )


class DeclaredScopeComparator:
    """Use profile-provided scope relations; never infer subsumption."""

    def __init__(
        self,
        relations: Mapping[
            tuple[str | None, str | None],
            ScopeDeclaration,
        ],
        *,
        proof_issuer: ResolverProofIssuer = _SCOPE_PROOF_ISSUER,
    ) -> None:
        self._relations = dict(relations)
        self._proof_issuer = proof_issuer

    def compare(
        self,
        expected: RelationAssertion,
        observed: RelationAssertion,
        context: RelationComparisonContext,
    ) -> ScopeDecision:
        declaration = self._relations.get(
            (
                expected.applicability_scope_id,
                observed.applicability_scope_id,
            ),
            ScopeDeclaration(
                "unknown",
                (),
                "The profile has not declared how these scopes compare.",
            ),
        )
        scope_payload = {
            "expected_scope_id": expected.applicability_scope_id,
            "observed_scope_id": observed.applicability_scope_id,
            "relation": declaration.relation,
        }
        return ScopeDecision(
            self._proof_issuer.issue(
                "scope_declaration",
                context=context,
                outcome=declaration.relation,
                rationale=declaration.rationale,
                input_ids=tuple(
                    dict.fromkeys(
                        (
                            expected.assertion_id,
                            observed.assertion_id,
                            *(
                                scope_id
                                for scope_id in (
                                    expected.applicability_scope_id,
                                    observed.applicability_scope_id,
                                )
                                if scope_id is not None
                            ),
                        )
                    )
                ),
                input_digests=(
                    ("expected_assertion", expected.assertion_digest),
                    ("observed_assertion", observed.assertion_digest),
                    (
                        "scope_relation",
                        _content_digest(scope_payload),
                    ),
                ),
                supporting_record_ids=declaration.supporting_record_ids,
            )
        )

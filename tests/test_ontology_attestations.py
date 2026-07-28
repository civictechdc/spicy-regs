"""Tests for the contract-shaped attestations carrier.

Every rule exercised here is stated normatively in rulespec
``spec/rkaf-conformance.md`` ("Worked pattern — attestation as a table",
commit ``b613ba3``) and ``constraints/core/attestation.cue``: closed enums,
at least one target, rejection as a row, revocation as a value, and approval
never as a field on the attested record.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology import attestations as attestations_module
from spicy_regs.ontology.attestations import (
    ATTESTATIONS_COLUMNS,
    ATTESTOR_KIND_AI_MODEL,
    ATTESTOR_KIND_HUMAN_USER,
    ATTESTOR_KIND_IRIS,
    ATTESTOR_KINDS,
    DECISION_ABSTAINED,
    DECISION_APPROVED,
    DECISION_FLAGGED_FOR_REVIEW,
    DECISION_IRIS,
    DECISION_REJECTED,
    DECISIONS,
    IMMUTABLE_COLUMNS,
    OUTPUT,
    RKAF_NAMESPACE,
    AttestationError,
    assert_no_inline_approval_columns,
    attestation_row,
    decisions_by_target,
    effective_attestations,
    judge_verdict_attestation,
    merge_attestations,
    read_attestations,
    revoke_attestation,
    unreviewed_targets,
    validate_attestation_rows,
    write_attestations,
)
from spicy_regs.ontology.common import ATTESTATION_COLUMNS, RunContext
from spicy_regs.ontology.concepts import ASSIGNMENT_COLUMNS
from spicy_regs.ontology.invariants import OntologyInvariantError

_CONTEXT = RunContext("adjudication-test", "2026-07-28T12:00:00Z")
_PROTOCOL = "spicy-regs:holdout-cross-family-adjudication-v1"
_JUDGE = "urn:spicy-regs:model:claude-fable-5"


def _row(
    *,
    attestor_id: str = _JUDGE,
    attestor_kind: str = ATTESTOR_KIND_AI_MODEL,
    targets: object = ("gold-0001",),
    decision: str = DECISION_APPROVED,
    attestation_scope: str = _PROTOCOL,
    attested_at: str | None = None,
    rationale: str | None = None,
    supersedes_id: str | None = None,
) -> dict:
    """Build one attestation row; ``targets`` stays untyped so tests can feed junk."""
    return attestation_row(
        attestor_id=attestor_id,
        attestor_kind=attestor_kind,
        targets=cast("Sequence[str]", targets),
        decision=decision,
        attestation_scope=attestation_scope,
        context=_CONTEXT,
        attested_at=attested_at,
        rationale=rationale,
        supersedes_id=supersedes_id,
    )


# --- the closed enums, transcribed from the CUE -----------------------------


def test_decision_enum_is_the_contract_enum():
    assert DECISIONS == (
        "rkaf:approved",
        "rkaf:approvedWithConditions",
        "rkaf:rejected",
        "rkaf:abstained",
        "rkaf:advisory",
        "rkaf:endorsedForReview",
        "rkaf:flaggedForReview",
    )


def test_attestor_kind_enum_is_the_contract_enum():
    assert ATTESTOR_KINDS == (
        "rkaf:humanUser",
        "rkaf:aiModel",
        "rkaf:aiAgent",
        "rkaf:automatedParser",
        "rkaf:team",
        "rkaf:organization",
        "rkaf:community",
        "rkaf:formalReviewer",
        "rkaf:conceptMintingAuthority",
    )


def test_enum_iris_expand_for_a_future_enum_map():
    assert DECISION_IRIS[DECISION_APPROVED] == f"{RKAF_NAMESPACE}approved"
    assert ATTESTOR_KIND_IRIS[ATTESTOR_KIND_AI_MODEL] == f"{RKAF_NAMESPACE}aiModel"
    assert set(DECISION_IRIS) == set(DECISIONS)
    assert set(ATTESTOR_KIND_IRIS) == set(ATTESTOR_KINDS)


def test_columns_carry_the_six_required_terms_plus_revocation_and_provenance():
    assert ATTESTATIONS_COLUMNS == (
        "attestation_id",
        "attestor_id",
        "attestor_kind",
        "target_ids_json",
        "decision",
        "attestation_scope",
        "attested_at",
        "revoked_at",
        "rationale",
        *ATTESTATION_COLUMNS,
    )
    # Revocation is the one column an existing row may gain; nothing else moves.
    assert set(ATTESTATIONS_COLUMNS) - set(IMMUTABLE_COLUMNS) == {"revoked_at"}


# --- closed-enum rejection ---------------------------------------------------


@pytest.mark.parametrize("decision", ["approved", "rkaf:Approved", "accepted", "", "rkaf:approvedByHuman"])
def test_decision_outside_the_closed_enum_is_rejected(decision):
    with pytest.raises(AttestationError, match="AttestationDecision"):
        _row(decision=decision)


@pytest.mark.parametrize("kind", ["model", "rkaf:AiModel", "machine", "", "rkaf:human"])
def test_attestor_kind_outside_the_closed_enum_is_rejected(kind):
    with pytest.raises(AttestationError, match="AttestorKind"):
        _row(attestor_kind=kind)


def test_stored_row_with_a_forged_enum_is_rejected_by_validation():
    row = {**_row(), "decision": "rkaf:blessed"}
    with pytest.raises(AttestationError, match="AttestationDecision"):
        validate_attestation_rows([row])


# --- targets: at least one, and many are allowed -----------------------------


@pytest.mark.parametrize(
    "targets",
    [[], (), ["  "], ["gold-0001", "gold-0001"], ["gold-0001", None]],
)
def test_empty_or_malformed_targets_are_rejected(targets):
    with pytest.raises(AttestationError):
        _row(targets=targets)


@pytest.mark.parametrize("stored", ["", "[]", "null", '{"gold-0001": true}', "not json"])
def test_stored_targets_must_be_a_non_empty_json_array(stored):
    row = {**_row(), "target_ids_json": stored}
    with pytest.raises(AttestationError):
        validate_attestation_rows([row])


def test_one_decision_may_target_many_records_and_ordering_is_not_identity():
    many = _row(targets=["gold-0002", "gold-0001", "gold-0003"])
    same = _row(targets=["gold-0003", "gold-0002", "gold-0001"])
    assert many["target_ids_json"] == '["gold-0001","gold-0002","gold-0003"]'
    assert many["attestation_id"] == same["attestation_id"]
    assert sorted(decisions_by_target([many])) == ["gold-0001", "gold-0002", "gold-0003"]


# --- rejection is a row; absence is unreviewed -------------------------------


def test_rejection_is_a_row_and_an_absent_row_means_unreviewed():
    approved = _row(targets=["gold-0001"])
    rejected = _row(targets=["gold-0002"], decision=DECISION_REJECTED)
    flagged = _row(targets=["gold-0003"], decision=DECISION_FLAGGED_FOR_REVIEW)
    rows = validate_attestation_rows([approved, rejected, flagged])

    assert decisions_by_target(rows) == {
        "gold-0001": [DECISION_APPROVED],
        "gold-0002": [DECISION_REJECTED],
        "gold-0003": [DECISION_FLAGGED_FOR_REVIEW],
    }
    assert unreviewed_targets(rows, ["gold-0001", "gold-0002", "gold-0004"]) == ["gold-0004"]


def test_two_attestors_may_disagree_about_one_record():
    first = _row(attestor_id="urn:spicy-regs:model:gpt-5", decision=DECISION_APPROVED)
    second = _row(attestor_id=_JUDGE, decision=DECISION_REJECTED)
    rows = validate_attestation_rows([first, second])
    assert sorted(decisions_by_target(rows)["gold-0001"]) == sorted([DECISION_APPROVED, DECISION_REJECTED])


# --- revocation is a value, never a delete -----------------------------------


def test_revocation_keeps_the_row_and_its_identity():
    original = _row()
    revoked = revoke_attestation(original, revoked_at="2026-07-29T09:00:00Z")

    assert revoked["attestation_id"] == original["attestation_id"]
    assert revoked["decision"] == original["decision"]
    assert revoked["revoked_at"] == "2026-07-29T09:00:00Z"
    validate_attestation_rows([revoked], prior=[original])
    assert effective_attestations([revoked]) == []
    assert unreviewed_targets([revoked], ["gold-0001"]) == ["gold-0001"]


def test_revocation_cannot_be_repeated_cleared_or_predate_the_decision():
    original = _row()
    revoked = revoke_attestation(original, revoked_at="2026-07-29T09:00:00Z")

    with pytest.raises(AttestationError, match="already revoked"):
        revoke_attestation(revoked, revoked_at="2026-07-30T09:00:00Z")
    with pytest.raises(AttestationError, match="revoked before"):
        revoke_attestation(original, revoked_at="2020-01-01T00:00:00Z")
    with pytest.raises(AttestationError, match="revocation requires a timestamp"):
        revoke_attestation(original, revoked_at="")
    with pytest.raises(AttestationError, match="existing revocation"):
        validate_attestation_rows([original], prior=[revoked])


def test_deleting_a_revoked_or_live_row_is_rejected():
    original = _row()
    with pytest.raises(OntologyInvariantError, match="hard-deleted"):
        validate_attestation_rows([], prior=[original])


# --- append-only and supersession -------------------------------------------


def test_append_only_rejects_a_mutated_decision():
    original = _row()
    mutated = {**original, "decision": DECISION_REJECTED}
    with pytest.raises(OntologyInvariantError):
        validate_attestation_rows([mutated], prior=[original], require_resolvable_supersession=False)
    with pytest.raises(AttestationError, match="immutable columns"):
        merge_attestations([original], [mutated])


def test_supersession_chain_resolves_to_the_latest_decision():
    first = _row(decision=DECISION_ABSTAINED, attested_at="2026-07-28T12:00:00Z")
    second = _row(
        decision=DECISION_APPROVED,
        attested_at="2026-07-29T12:00:00Z",
        supersedes_id=first["attestation_id"],
    )
    rows = validate_attestation_rows([first, second])

    assert second["supersedes_id"] == first["attestation_id"]
    assert [row["attestation_id"] for row in effective_attestations(rows)] == [second["attestation_id"]]
    assert decisions_by_target(rows) == {"gold-0001": [DECISION_APPROVED]}


def test_supersession_chain_must_be_acyclic_and_resolvable():
    first = _row(decision=DECISION_ABSTAINED, attested_at="2026-07-28T12:00:00Z")
    second = _row(decision=DECISION_APPROVED, supersedes_id=first["attestation_id"])

    # A superseding row that names no reachable predecessor is refused.
    with pytest.raises(OntologyInvariantError, match="unknown ids"):
        validate_attestation_rows([second])

    # A cycle cannot be minted: identity is derived from ``supersedes_id``, so
    # pointing an existing row back at its successor invalidates its id first,
    # and the acyclicity check behind it never has a forged chain to walk.
    cyclic = [{**first, "supersedes_id": second["attestation_id"]}, second]
    with pytest.raises(AttestationError, match="derived identity"):
        validate_attestation_rows(cyclic, require_resolvable_supersession=False)


def test_hand_edited_identity_is_rejected():
    row = {**_row(), "attestation_scope": "spicy-regs:some-other-protocol"}
    with pytest.raises(AttestationError, match="derived identity"):
        validate_attestation_rows([row])


def test_unknown_columns_are_rejected():
    row = {**_row(), "approved_by": "reviewer-14"}
    with pytest.raises(AttestationError, match="unknown columns"):
        validate_attestation_rows([row])


def test_provenance_records_the_machine_attestor_honestly():
    row = _row()
    assert row["method"] == "llm"
    assert row["actor_id"] == _JUDGE
    assert row["run_id"] == _CONTEXT.run_id
    assert row["asserted_at"] == _CONTEXT.asserted_at
    assert _row(attestor_kind=ATTESTOR_KIND_HUMAN_USER, attestor_id="urn:spicy-regs:person:1")["method"] == "human"


# --- approval never lives on the attested record -----------------------------


def test_record_carriers_must_not_inline_an_approval_field():
    assert_no_inline_approval_columns(ASSIGNMENT_COLUMNS, table="concept_assignments")
    assert_no_inline_approval_columns(ATTESTATION_COLUMNS, table="any-record-carrier")

    for column in ("approved_by", "approval_status", "review_status", "attestor_id", "decision", "revoked_at"):
        with pytest.raises(AttestationError, match="approval"):
            assert_no_inline_approval_columns(("assignment_id", "concept_id", column), table="concept_assignments")


def test_module_exports_no_per_record_approval_column():
    """No public export of this module can be used as a column on a record."""
    attestation_only = {
        "attestation_id",
        "attestor_id",
        "attestor_kind",
        "target_ids_json",
        "decision",
        "attestation_scope",
        "attested_at",
        "revoked_at",
        "rationale",
    }
    carriers = {"ATTESTATIONS_COLUMNS", "IMMUTABLE_COLUMNS"}
    for name in dir(attestations_module):
        if name.startswith("_") or name in carriers:
            continue
        value = getattr(attestations_module, name)
        if isinstance(value, str):
            assert value not in attestation_only, f"{name} exports a bare attestation column name"
        elif isinstance(value, (tuple, list, set, frozenset)):
            leaked = attestation_only & {item for item in value if isinstance(item, str)}
            assert not leaked, f"{name} exports attestation columns {sorted(leaked)} outside the attestations table"
        elif isinstance(value, dict):
            leaked = attestation_only & {item for item in value if isinstance(item, str)}
            assert not leaked, f"{name} exports attestation columns {sorted(leaked)} outside the attestations table"


# --- storage -----------------------------------------------------------------


def test_parquet_round_trip_is_all_varchar_and_revocation_appends(tmp_path):
    approved = _row(targets=["gold-0001", "gold-0002"])
    rejected = _row(targets=["gold-0003"], decision=DECISION_REJECTED, rationale="no adequate registered target")
    path = write_attestations(tmp_path, new_rows=[approved, rejected])

    assert path.name == OUTPUT
    schema = pq.ParquetFile(path).schema_arrow
    assert schema.names == list(ATTESTATIONS_COLUMNS)
    assert {str(field.type) for field in schema} == {"string"}

    stored = read_attestations(path)
    assert [row["attestation_id"] for row in stored] == sorted([approved["attestation_id"], rejected["attestation_id"]])
    by_id = {row["attestation_id"]: row for row in stored}
    assert by_id[approved["attestation_id"]]["target_ids_json"] == '["gold-0001","gold-0002"]'
    assert by_id[rejected["attestation_id"]]["rationale"] == "no adequate registered target"
    assert by_id[rejected["attestation_id"]]["revoked_at"] is None

    revoked = revoke_attestation(by_id[approved["attestation_id"]], revoked_at="2026-07-30T00:00:00Z")
    second_path = write_attestations(tmp_path, new_rows=[revoked], prior_path=path)
    reloaded = read_attestations(second_path)
    assert len(reloaded) == 2
    assert {row["attestation_id"] for row in reloaded} == set(by_id)
    assert {row["attestation_id"] for row in effective_attestations(reloaded)} == {rejected["attestation_id"]}


def test_writing_a_mutation_over_a_stored_table_is_refused(tmp_path):
    original = _row()
    path = write_attestations(tmp_path, new_rows=[original])
    forged = {**original, "decision": DECISION_APPROVED, "attestor_id": "urn:spicy-regs:model:other"}
    with pytest.raises(AttestationError, match="immutable columns"):
        write_attestations(tmp_path, new_rows=[forged], prior_path=path)


# --- first consumer: holdout adjudication ------------------------------------


def test_judge_verdict_attestation_records_a_machine_judge_over_gold_items():
    row = judge_verdict_attestation(
        judge_model_id="urn:spicy-regs:model:gpt-5",
        gold_item_ids=["gold-0007", "gold-0008"],
        decision=DECISION_APPROVED,
        protocol=_PROTOCOL,
        context=_CONTEXT,
        rationale="target concept is an adequate registered match",
    )

    assert row["attestor_id"] == "urn:spicy-regs:model:gpt-5"
    assert row["attestor_kind"] == ATTESTOR_KIND_AI_MODEL
    assert row["decision"] in DECISIONS
    assert row["attestation_scope"] == _PROTOCOL
    assert row["target_ids_json"] == '["gold-0007","gold-0008"]'
    assert row["attested_at"] == _CONTEXT.asserted_at
    assert row["revoked_at"] is None
    assert row["method"] == "llm"
    assert set(row) == set(ATTESTATIONS_COLUMNS)
    validate_attestation_rows([row])


def test_judge_verdicts_are_deterministic_and_a_second_judge_is_a_second_row():
    first = judge_verdict_attestation(
        judge_model_id="urn:spicy-regs:model:gpt-5",
        gold_item_ids=["gold-0007"],
        decision=DECISION_APPROVED,
        protocol=_PROTOCOL,
        context=_CONTEXT,
    )
    repeat = judge_verdict_attestation(
        judge_model_id="urn:spicy-regs:model:gpt-5",
        gold_item_ids=["gold-0007"],
        decision=DECISION_APPROVED,
        protocol=_PROTOCOL,
        context=_CONTEXT,
    )
    other_family = judge_verdict_attestation(
        judge_model_id=_JUDGE,
        gold_item_ids=["gold-0007"],
        decision=DECISION_REJECTED,
        protocol=_PROTOCOL,
        context=_CONTEXT,
    )

    assert first["attestation_id"] == repeat["attestation_id"]
    assert other_family["attestation_id"] != first["attestation_id"]
    rows = merge_attestations([], [first, repeat, other_family])
    assert len(rows) == 2
    validate_attestation_rows(rows)


def test_judge_verdict_rejects_an_invented_decision():
    with pytest.raises(AttestationError, match="AttestationDecision"):
        judge_verdict_attestation(
            judge_model_id="urn:spicy-regs:model:gpt-5",
            gold_item_ids=["gold-0007"],
            decision="rkaf:adequate",
            protocol=_PROTOCOL,
            context=_CONTEXT,
        )

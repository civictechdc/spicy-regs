"""Contract-shaped attestation rows for reviewed ontology records.

Rulespec ``spec/rkaf-core.md`` §3.1/§4.7.3 put approval, rejection, and
revocation in an ``rkaf:Attestation`` *targeting* a record, never in a field on
the record itself.  ``spec/rkaf-conformance.md`` states the tabular form of that
rule normatively ("Worked pattern — attestation as a table", rulespec commit
``b613ba3``): one row is one Attestation node, the row's own identifier is the
Attestation's identity, the attested record's identity appears only in the
target column, and four rules carry the meaning:

1. **The attestation is never a column on the attested record.** An
   ``approved_by``/``approval_status`` column carries no attestor kind, no
   scope, no decision time, no revocation, and cannot express two attestors
   disagreeing.  :func:`assert_no_inline_approval_columns` is the executable
   form of that prohibition; this module deliberately exports no constant,
   column, or helper usable as a per-record approval field.
2. **Rejection is a row.**  ``rkaf:rejected``/``rkaf:abstained``/
   ``rkaf:flaggedForReview`` are values of the same closed decision set as
   ``rkaf:approved``.  A record with no attestation row is UNREVIEWED, not
   rejected — see :func:`unreviewed_targets`.
3. **Revocation is a value, not a delete.**  A withdrawn attestation keeps its
   row and gains ``revoked_at`` (:func:`revoke_attestation`); ``revoked_at`` is
   the one column an existing row may gain, and only once, from null.
4. **``rkaf:targets`` is the join, and it is many.**  ``target_ids_json`` is a
   JSON array with at least one entry; the attested records never name the
   attestation.

The closed enums are transcribed verbatim from
``rulespec/constraints/core/attestation.cue`` (``#AttestationDecision``,
``#AttestorKind``); the stored value *is* the contract value, so a future L0
``enum_map`` is a mechanical prefix expansion (:data:`DECISION_IRIS`,
:data:`ATTESTOR_KIND_IRIS`).  Publication wiring — published outputs, the data
dictionary, the L0 mapping blocks — is deliberately **not** here: that is phase
4.3 of ``docs/rulespec-testbed-path-forward.md``.

First consumer: the holdout adjudication records each judge verdict with
:func:`judge_verdict_attestation` (attestor = the judge model identity,
attestor kind = ``rkaf:aiModel``, scope = the adjudication protocol, targets =
the gold item identities).  The MVP attestor is a machine and is recorded
honestly as one; a model never attests its own output, which is a property of
the caller's protocol — this module records, it cannot verify family.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    RunContext,
    canonical_json,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)
from spicy_regs.ontology.invariants import (
    OntologyInvariantError,
    assert_acyclic,
    assert_append_only,
    assert_attestation_complete,
)

OUTPUT = "attestations.parquet"

RKAF_NAMESPACE = "https://rulespec.org/ns/v1#"
ATTESTATION_SUBJECT_TYPE = "rkaf:Attestation"

# ``#AttestationDecision`` — constraints/core/attestation.cue, rulespec b613ba3.
DECISION_APPROVED = "rkaf:approved"
DECISION_APPROVED_WITH_CONDITIONS = "rkaf:approvedWithConditions"
DECISION_REJECTED = "rkaf:rejected"
DECISION_ABSTAINED = "rkaf:abstained"
DECISION_ADVISORY = "rkaf:advisory"
DECISION_ENDORSED_FOR_REVIEW = "rkaf:endorsedForReview"
DECISION_FLAGGED_FOR_REVIEW = "rkaf:flaggedForReview"

DECISIONS: tuple[str, ...] = (
    DECISION_APPROVED,
    DECISION_APPROVED_WITH_CONDITIONS,
    DECISION_REJECTED,
    DECISION_ABSTAINED,
    DECISION_ADVISORY,
    DECISION_ENDORSED_FOR_REVIEW,
    DECISION_FLAGGED_FOR_REVIEW,
)

# ``#AttestorKind`` — constraints/core/attestation.cue, rulespec b613ba3.
ATTESTOR_KIND_HUMAN_USER = "rkaf:humanUser"
ATTESTOR_KIND_AI_MODEL = "rkaf:aiModel"
ATTESTOR_KIND_AI_AGENT = "rkaf:aiAgent"
ATTESTOR_KIND_AUTOMATED_PARSER = "rkaf:automatedParser"
ATTESTOR_KIND_TEAM = "rkaf:team"
ATTESTOR_KIND_ORGANIZATION = "rkaf:organization"
ATTESTOR_KIND_COMMUNITY = "rkaf:community"
ATTESTOR_KIND_FORMAL_REVIEWER = "rkaf:formalReviewer"
ATTESTOR_KIND_CONCEPT_MINTING_AUTHORITY = "rkaf:conceptMintingAuthority"

ATTESTOR_KINDS: tuple[str, ...] = (
    ATTESTOR_KIND_HUMAN_USER,
    ATTESTOR_KIND_AI_MODEL,
    ATTESTOR_KIND_AI_AGENT,
    ATTESTOR_KIND_AUTOMATED_PARSER,
    ATTESTOR_KIND_TEAM,
    ATTESTOR_KIND_ORGANIZATION,
    ATTESTOR_KIND_COMMUNITY,
    ATTESTOR_KIND_FORMAL_REVIEWER,
    ATTESTOR_KIND_CONCEPT_MINTING_AUTHORITY,
)


def _iri(value: str) -> str:
    return RKAF_NAMESPACE + value.split(":", 1)[1]


#: Stored value -> registered IRI, ready to become an L0 ``enum_map`` verbatim.
DECISION_IRIS: dict[str, str] = {value: _iri(value) for value in DECISIONS}
ATTESTOR_KIND_IRIS: dict[str, str] = {value: _iri(value) for value in ATTESTOR_KINDS}

#: One row per ``rkaf:Attestation`` node.  Column -> term:
#: ``attestor_id`` → ``rkaf:attestor``, ``attestor_kind`` → ``rkaf:attestorKind``,
#: ``target_ids_json`` → ``rkaf:targets`` (json-list), ``decision`` →
#: ``rkaf:decision``, ``attestation_scope`` → ``rkaf:attestationScope``,
#: ``attested_at`` → ``rkaf:attestedAt``, ``revoked_at`` → ``rkaf:revokedAt``,
#: ``rationale`` → ``rkaf:rationale``.  The trailing block is this repository's
#: per-row provenance (who wrote the row, in which run) — it is *not* an
#: Attestation and never stands in for one.
ATTESTATIONS_COLUMNS: tuple[str, ...] = (
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

#: Every column except ``revoked_at`` is frozen once written: rule 3 lets a row
#: gain a revocation timestamp and forbids every other edit, and every delete.
IMMUTABLE_COLUMNS: tuple[str, ...] = tuple(column for column in ATTESTATIONS_COLUMNS if column != "revoked_at")

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "attestation_id",
    "attestor_id",
    "attestor_kind",
    "target_ids_json",
    "decision",
    "attestation_scope",
    "attested_at",
)

_METHOD_BY_ATTESTOR_KIND: dict[str, str] = {
    ATTESTOR_KIND_HUMAN_USER: "human",
    ATTESTOR_KIND_AI_MODEL: "llm",
    ATTESTOR_KIND_AI_AGENT: "llm",
    ATTESTOR_KIND_AUTOMATED_PARSER: "deterministic",
    ATTESTOR_KIND_TEAM: "human",
    ATTESTOR_KIND_ORGANIZATION: "human",
    ATTESTOR_KIND_COMMUNITY: "human",
    ATTESTOR_KIND_FORMAL_REVIEWER: "human",
    ATTESTOR_KIND_CONCEPT_MINTING_AUTHORITY: "human",
}

#: Name fragments that turn a record carrier's column into an inline approval
#: field.  Rule 1 forbids them on the attested record whatever they are renamed
#: to, so the check is on substrings, not on an exact-name allowlist.
INLINE_APPROVAL_MARKERS: tuple[str, ...] = (
    "approv",
    "attestor",
    "attested",
    "attestation",
    "adjudicat",
    "decis",
    "verdict",
    "endorse",
    "reject",
    "revok",
    "sign_off",
    "signoff",
    "reviewed_by",
    "review_status",
)


class AttestationError(OntologyInvariantError):
    """Raised when a row would break the tabular attestation pattern."""


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def parse_targets(value: object) -> list[str]:
    """Parse ``target_ids_json`` into its target identities.

    Raises :class:`AttestationError` unless the value is a JSON array of at
    least one distinct, non-blank identity (rule 4: targets is the join, and it
    is many — never zero).
    """
    if isinstance(value, (list, tuple)):
        parsed: object = list(value)
    else:
        text = _text(value)
        if not text:
            raise AttestationError("attestation targets are empty; rkaf:targets requires at least one target")
        try:
            parsed = json.loads(text)
        except (TypeError, json.JSONDecodeError) as exc:
            raise AttestationError(f"attestation targets are not valid JSON: {value!r}") from exc
    if not isinstance(parsed, list):
        raise AttestationError(f"attestation targets must be a JSON array, got {type(parsed).__name__}")
    targets = [_text(item) for item in parsed]
    if not targets:
        raise AttestationError("attestation targets are empty; rkaf:targets requires at least one target")
    if any(not target for target in targets):
        raise AttestationError(f"attestation targets contain a blank identity: {parsed!r}")
    if len(set(targets)) != len(targets):
        raise AttestationError(f"attestation targets contain duplicates: {parsed!r}")
    return targets


def canonical_targets(targets: object) -> str:
    """Serialize target identities deterministically for storage and identity."""
    return canonical_json(sorted(parse_targets(targets)))


def attestation_identity(row: Mapping[str, object]) -> str:
    """Derive the stable attestation id from the decision the row records.

    ``revoked_at`` is excluded on purpose: revoking an attestation must not
    change which attestation it is.
    """
    return stable_id(
        "attestation",
        _text(row.get("attestor_id")),
        _text(row.get("attestor_kind")),
        _text(row.get("decision")),
        _text(row.get("attestation_scope")),
        _text(row.get("attested_at")),
        canonical_targets(row.get("target_ids_json") or ""),
        _text(row.get("supersedes_id")) or None,
    )


def attestation_row(
    *,
    attestor_id: str,
    attestor_kind: str,
    targets: Sequence[str],
    decision: str,
    attestation_scope: str,
    context: RunContext,
    attested_at: str | None = None,
    rationale: str | None = None,
    supersedes_id: str | None = None,
    method: str | None = None,
    actor_id: str | None = None,
) -> dict:
    """Build one validated ``rkaf:Attestation`` row.

    ``attestor_id`` is the party that decided; ``actor_id`` (provenance) is the
    process that wrote the row and defaults to the attestor.  ``method``
    defaults to the honest method for the attestor kind (``llm`` for a model or
    agent, ``human`` for a person or body, ``deterministic`` for a parser).
    """
    if decision not in DECISIONS:
        raise AttestationError(f"decision {decision!r} is outside the closed rkaf:AttestationDecision enum {DECISIONS}")
    if attestor_kind not in ATTESTOR_KINDS:
        raise AttestationError(
            f"attestor_kind {attestor_kind!r} is outside the closed rkaf:AttestorKind enum {ATTESTOR_KINDS}"
        )
    if not _text(attestor_id):
        raise AttestationError("attestation requires an attestor identity")
    if not _text(attestation_scope):
        raise AttestationError("attestation requires a scope naming what the decision covers")
    target_ids_json = canonical_targets(targets)
    decided_at = _text(attested_at) or context.asserted_at
    row = {
        "attestor_id": _text(attestor_id),
        "attestor_kind": attestor_kind,
        "target_ids_json": target_ids_json,
        "decision": decision,
        "attestation_scope": _text(attestation_scope),
        "attested_at": decided_at,
        "revoked_at": None,
        "rationale": rationale,
        **context.provenance(
            method=method or _METHOD_BY_ATTESTOR_KIND[attestor_kind],
            actor_id=_text(actor_id) or _text(attestor_id),
            supersedes_id=supersedes_id,
        ),
    }
    return {"attestation_id": attestation_identity(row), **row}


def judge_verdict_attestation(
    *,
    judge_model_id: str,
    gold_item_ids: Sequence[str],
    decision: str,
    protocol: str,
    context: RunContext,
    attested_at: str | None = None,
    rationale: str | None = None,
    supersedes_id: str | None = None,
    actor_id: str | None = None,
) -> dict:
    """Record one machine judge verdict over gold items as an attestation row.

    The holdout adjudication is this module's first consumer::

        row = judge_verdict_attestation(
            judge_model_id="urn:spicy-regs:model:gpt-5",
            gold_item_ids=["gold-0007", "gold-0008"],
            decision=DECISION_APPROVED,
            protocol="spicy-regs:holdout-cross-family-adjudication-v1",
            context=RunContext.resolve(prefix="adjudication"),
            rationale="target concept is an adequate registered match",
        )
        write_attestations(output_dir, new_rows=[row])

    ``attestor_kind`` is fixed to :data:`ATTESTOR_KIND_AI_MODEL` — the MVP
    attestor is a machine and is recorded as one, never as human review.  A
    disagreement is a second row from a second judge; a rejection is a row with
    ``decision=DECISION_REJECTED``, never an omission.  Cross-family
    independence (a model never attests its own output) is the caller's
    protocol obligation: this carrier records the attestor honestly, it cannot
    verify which family produced the tagged output.
    """
    return attestation_row(
        attestor_id=judge_model_id,
        attestor_kind=ATTESTOR_KIND_AI_MODEL,
        targets=gold_item_ids,
        decision=decision,
        attestation_scope=protocol,
        context=context,
        attested_at=attested_at,
        rationale=rationale,
        supersedes_id=supersedes_id,
        actor_id=actor_id,
    )


def revoke_attestation(row: Mapping[str, object], *, revoked_at: str) -> dict:
    """Return the same attestation, marked revoked.

    Rule 3: a withdrawn attestation keeps its row and its identity, and gains
    ``rkaf:revokedAt``.  Deleting the row would destroy the record that the
    decision was once made, so no deletion helper exists.
    """
    timestamp = _text(revoked_at)
    if not timestamp:
        raise AttestationError("revocation requires a timestamp; revocation is a value, never a delete")
    if _text(row.get("revoked_at")):
        raise AttestationError(f"attestation {row.get('attestation_id')!r} is already revoked")
    if timestamp < _text(row.get("attested_at")):
        raise AttestationError(f"attestation {row.get('attestation_id')!r} would be revoked before it was attested")
    return {**dict(row), "revoked_at": timestamp}


def is_effective(row: Mapping[str, object]) -> bool:
    """True when the row is a live decision (not revoked)."""
    return not _text(row.get("revoked_at"))


def effective_attestations(rows: Iterable[Mapping[str, object]]) -> list[dict]:
    """Return live decisions: neither revoked nor superseded by a later row."""
    row_list = [dict(row) for row in rows]
    superseded = {_text(row.get("supersedes_id")) for row in row_list if _text(row.get("supersedes_id"))}
    return [row for row in row_list if is_effective(row) and _text(row.get("attestation_id")) not in superseded]


def decisions_by_target(rows: Iterable[Mapping[str, object]]) -> dict[str, list[str]]:
    """Map each attested identity to the live decisions recorded against it."""
    result: dict[str, list[str]] = {}
    for row in effective_attestations(rows):
        for target in parse_targets(row.get("target_ids_json")):
            result.setdefault(target, []).append(_text(row.get("decision")))
    return result


def unreviewed_targets(rows: Iterable[Mapping[str, object]], candidate_ids: Iterable[str]) -> list[str]:
    """Return the candidates carrying no live attestation.

    Rule 2, made executable: absence of a row means UNREVIEWED.  It never means
    rejected — a rejection is a row whose decision is ``rkaf:rejected``.
    """
    attested = set(decisions_by_target(rows))
    return sorted({_text(candidate) for candidate in candidate_ids if _text(candidate)} - attested)


def assert_no_inline_approval_columns(columns: Iterable[str], *, table: str) -> None:
    """Assert that a record carrier holds no inline approval/attestation field.

    Rule 1: approval, rejection, and revocation live in an attestation row that
    targets the record; an ``approved_by`` column on the attested table is not
    an Attestation whatever it is renamed to.  Call this on a record carrier's
    column tuple (``concept_assignments``, findings, assertions) — never on
    :data:`ATTESTATIONS_COLUMNS`, which is the attestation carrier itself.
    """
    offending = sorted(
        {column for column in columns for marker in INLINE_APPROVAL_MARKERS if marker in str(column).casefold()}
    )
    if offending:
        raise AttestationError(
            f"table {table!r} would carry approval inline in columns {offending}; "
            "approval, rejection, and revocation belong in an attestation row targeting the record"
        )


def validate_attestation_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    prior: Iterable[Mapping[str, object]] = (),
    require_resolvable_supersession: bool = True,
) -> list[dict]:
    """Validate a complete attestations table and return it as plain rows.

    Enforces the closed enums, non-empty targets, derived identity, revocation
    monotonicity, an acyclic supersession chain, and — against ``prior`` — that
    the table is append-only apart from a row gaining ``revoked_at``.
    """
    row_list = [dict(row) for row in rows]
    prior_list = [dict(row) for row in prior]

    by_id: dict[str, dict] = {}
    for index, row in enumerate(row_list):
        unknown = sorted(set(row) - set(ATTESTATIONS_COLUMNS))
        if unknown:
            raise AttestationError(f"attestation row {index} carries unknown columns {unknown}")
        missing = [column for column in _REQUIRED_COLUMNS if not _text(row.get(column))]
        if missing:
            raise AttestationError(f"attestation row {index} is missing required columns {missing}")
        decision = _text(row["decision"])
        if decision not in DECISIONS:
            raise AttestationError(f"decision {decision!r} is outside the closed rkaf:AttestationDecision enum")
        attestor_kind = _text(row["attestor_kind"])
        if attestor_kind not in ATTESTOR_KINDS:
            raise AttestationError(f"attestor_kind {attestor_kind!r} is outside the closed rkaf:AttestorKind enum")
        parse_targets(row["target_ids_json"])
        expected_id = attestation_identity(row)
        if _text(row["attestation_id"]) != expected_id:
            raise AttestationError(
                f"attestation {row['attestation_id']!r} does not match its derived identity {expected_id!r}"
            )
        revoked_at = _text(row.get("revoked_at"))
        if revoked_at and revoked_at < _text(row["attested_at"]):
            raise AttestationError(f"attestation {row['attestation_id']!r} is revoked before it was attested")
        existing = by_id.get(expected_id)
        if existing is not None and existing != row:
            raise AttestationError(f"attestation {expected_id!r} appears twice with different content")
        by_id[expected_id] = row

    assert_attestation_complete(row_list)

    chain_rows = [*prior_list, *row_list]
    assert_acyclic(
        chain_rows,
        id_column="attestation_id",
        parent_column="supersedes_id",
        require_resolvable=require_resolvable_supersession,
    )

    if prior_list:
        assert_append_only(
            prior_list,
            row_list,
            id_column="attestation_id",
            immutable_columns=IMMUTABLE_COLUMNS,
        )
        for old in prior_list:
            new = by_id.get(_text(old.get("attestation_id")))
            if new is None:
                continue
            was_revoked = _text(old.get("revoked_at"))
            now_revoked = _text(new.get("revoked_at"))
            if was_revoked and was_revoked != now_revoked:
                raise AttestationError(
                    f"attestation {old.get('attestation_id')!r} changed an existing revocation "
                    f"{was_revoked!r} -> {now_revoked or None!r}"
                )
    return row_list


def merge_attestations(
    prior: Iterable[Mapping[str, object]],
    new_rows: Iterable[Mapping[str, object]],
) -> list[dict]:
    """Merge new rows onto prior ones, keeping the table append-only.

    An id already present may only reappear to gain ``revoked_at``; every other
    difference is a mutation and is rejected.
    """
    merged: dict[str, dict] = {}
    for row in prior:
        merged[_text(row.get("attestation_id"))] = dict(row)
    for row in new_rows:
        row_id = _text(row.get("attestation_id"))
        if not row_id:
            raise AttestationError("attestation row is missing attestation_id")
        existing = merged.get(row_id)
        candidate = dict(row)
        if existing is None:
            merged[row_id] = candidate
            continue
        changed = [
            column
            for column in IMMUTABLE_COLUMNS
            if existing.get(column) != candidate.get(column) and column in candidate
        ]
        if changed:
            raise AttestationError(f"attestation {row_id!r} would mutate immutable columns {changed}")
        merged[row_id] = {**existing, **candidate}
    return sorted(
        merged.values(),
        key=lambda row: (
            _text(row.get("attested_at")),
            _text(row.get("attestation_scope")),
            _text(row.get("attestor_id")),
            _text(row.get("attestation_id")),
        ),
    )


def write_attestations(
    output_dir: Path,
    *,
    new_rows: Iterable[Mapping[str, object]],
    prior_path: Path | None = None,
) -> Path:
    """Write the append-only attestations table as all-VARCHAR Parquet."""
    prior = read_parquet_rows(prior_path) if prior_path else []
    rows = merge_attestations(prior, new_rows)
    validate_attestation_rows(rows, prior=prior)
    return write_parquet_rows(output_dir / OUTPUT, columns=ATTESTATIONS_COLUMNS, rows=rows)


def read_attestations(path: Path) -> list[dict]:
    """Read an attestations table, validating it before any consumer sees it."""
    return validate_attestation_rows(read_parquet_rows(path))

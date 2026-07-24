"""Invariants for the append-only ontology registry and assertion log."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from spicy_regs.ontology.common import ATTESTATION_COLUMNS, NON_DETERMINISTIC_METHODS


class OntologyInvariantError(ValueError):
    """Raised when an ontology table would violate a published invariant."""


def assert_acyclic(
    rows: Iterable[Mapping[str, object]],
    *,
    id_column: str,
    parent_column: str,
    require_resolvable: bool = False,
) -> None:
    """Assert that a single-parent graph is acyclic and optionally resolvable."""
    row_list = list(rows)
    nodes = {str(row[id_column]) for row in row_list if row.get(id_column)}
    parent_by_id = {
        str(row[id_column]): str(row[parent_column])
        for row in row_list
        if row.get(id_column) and row.get(parent_column)
    }
    if require_resolvable:
        missing = sorted(set(parent_by_id.values()) - nodes)
        if missing:
            raise OntologyInvariantError(f"{parent_column} references unknown ids: {missing}")

    for start in parent_by_id:
        seen: set[str] = set()
        current = start
        while current in parent_by_id:
            if current in seen:
                raise OntologyInvariantError(f"cycle in {parent_column} chain at {current!r}")
            seen.add(current)
            current = parent_by_id[current]


def assert_concept_graphs(concepts: Iterable[Mapping[str, object]]) -> None:
    rows = list(concepts)
    assert_acyclic(rows, id_column="concept_id", parent_column="broader_id", require_resolvable=True)
    assert_acyclic(rows, id_column="concept_id", parent_column="replaced_by", require_resolvable=True)
    for row in rows:
        status = row.get("status")
        replacement = row.get("replaced_by")
        if replacement and status != "deprecated":
            raise OntologyInvariantError(f"concept {row.get('concept_id')!r} has replaced_by but status={status!r}")


def assert_attestation_complete(rows: Iterable[Mapping[str, object]]) -> None:
    """Require complete provenance for every non-deterministic assertion."""
    for index, row in enumerate(rows):
        method = row.get("method")
        if method not in NON_DETERMINISTIC_METHODS:
            continue
        missing = [column for column in ATTESTATION_COLUMNS[:-1] if not row.get(column)]
        if missing:
            row_id = row.get("assignment_id") or row.get("concept_id") or row.get("event_id") or index
            raise OntologyInvariantError(f"non-deterministic row {row_id!r} is missing attestation columns {missing}")


def assert_append_only(
    prior: Iterable[Mapping[str, object]],
    current: Iterable[Mapping[str, object]],
    *,
    id_column: str,
    immutable_columns: Sequence[str] | None = None,
) -> None:
    """Assert that prior rows remain present and unchanged in the current table."""
    prior_by_id = {str(row[id_column]): row for row in prior if row.get(id_column)}
    current_by_id = {str(row[id_column]): row for row in current if row.get(id_column)}
    missing = sorted(set(prior_by_id) - set(current_by_id))
    if missing:
        raise OntologyInvariantError(f"append-only table hard-deleted ids: {missing[:10]}")

    for row_id, old in prior_by_id.items():
        new = current_by_id[row_id]
        columns = immutable_columns or tuple(old)
        changed = [column for column in columns if old.get(column) != new.get(column)]
        if changed:
            raise OntologyInvariantError(f"append-only row {row_id!r} was modified in columns {changed}")


def resolve_replacement(concept_id: str, concepts: Iterable[Mapping[str, object]]) -> str:
    """Resolve a deprecated concept through its acyclic replacement chain."""
    replacements = {
        str(row["concept_id"]): str(row["replaced_by"])
        for row in concepts
        if row.get("concept_id") and row.get("replaced_by")
    }
    seen: set[str] = set()
    current = concept_id
    while current in replacements:
        if current in seen:
            raise OntologyInvariantError(f"replacement cycle at {current!r}")
        seen.add(current)
        current = replacements[current]
    return current

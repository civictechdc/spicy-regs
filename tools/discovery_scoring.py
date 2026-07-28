"""Scoring primitives for the deterministic discovery-slice experiments.

The experiment strategy insists that one score cannot represent every
capability, so this module keeps them apart. Three measures are computed and
reported separately, never blended:

* **link** — set precision and recall of the returned identifiers against an
  expectation derived independently of the system under test
  (:func:`score_sets`);
* **filter** — row-level predicate exactness: does *every* returned row
  actually satisfy the selected condition, and what happened to rows whose
  value was unknown (:func:`predicate_exactness`);
* **aggregate** — declared-level counts compared name by name
  (:func:`compare_counts`).

Nothing here reads the repository's own parsers or transforms. The expectation
each experiment passes in must come from the raw source snapshot; this module
only compares two sets that someone else built.

Conventions
-----------

*Forbidden* identifiers are near-misses that must never appear (``40 CFR 600``
for a ``40 CFR 60`` question). Overlap between the forbidden set and the
expected set is a defect in the frozen expectation, not a result, so
:func:`score_sets` raises rather than scoring it.

*Ambiguous* identifiers are cases a reasonable person could decide either way.
They are removed from both sides before scoring — neither credited nor
penalised — and reported on their own, so a later reading of the question can
re-score without re-deriving anything.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "AggregateComparison",
    "PredicateExactness",
    "SetScore",
    "compare_counts",
    "predicate_exactness",
    "score_sets",
    "sha256_file",
    "snapshot_identity",
]


def sha256_file(path: Path | str) -> str:
    """Return the SHA-256 of a file, read in chunks (source parquets are large)."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_identity(directory: Path | str, filenames: Sequence[str]) -> dict[str, str]:
    """Pin the snapshot: file name to SHA-256, for every named input.

    Raises ``FileNotFoundError`` naming every missing file at once, because an
    experiment that cannot pin its inputs must stop rather than run on whatever
    happens to be present.
    """
    root = Path(directory)
    paths = {name: root / name for name in filenames}
    missing = sorted(name for name, path in paths.items() if not path.exists())
    if missing:
        raise FileNotFoundError(f"snapshot inputs missing from {root}: {', '.join(missing)}")
    return {name: sha256_file(path) for name, path in sorted(paths.items())}


@dataclass(frozen=True)
class SetScore:
    """Link precision and recall against an independently derived expectation."""

    expected: int
    returned: int
    true_positives: int
    precision: float
    recall: float
    f1: float
    missing: tuple[str, ...]
    extra: tuple[str, ...]
    forbidden_returned: tuple[str, ...]
    ambiguous_returned: tuple[str, ...]
    ambiguous_excluded: int

    @property
    def exact(self) -> bool:
        """True only when the returned set equals the expectation exactly."""
        return not self.missing and not self.extra and not self.forbidden_returned

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected": self.expected,
            "returned": self.returned,
            "true_positives": self.true_positives,
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
            "exact": self.exact,
            "missing": list(self.missing),
            "extra": list(self.extra),
            "forbidden_returned": list(self.forbidden_returned),
            "ambiguous_returned": list(self.ambiguous_returned),
            "ambiguous_excluded": self.ambiguous_excluded,
        }


def score_sets(
    *,
    expected: Iterable[str],
    returned: Iterable[str],
    forbidden: Iterable[str] = (),
    ambiguous: Iterable[str] = (),
) -> SetScore:
    """Score a returned identifier set against a frozen expectation.

    ``expected`` and ``forbidden`` must be disjoint; an overlap means the
    expectation itself is inconsistent and is raised rather than scored.
    ``ambiguous`` members are dropped from both sides before scoring.
    """
    expected_set = {str(value) for value in expected}
    returned_set = {str(value) for value in returned}
    forbidden_set = {str(value) for value in forbidden}
    ambiguous_set = {str(value) for value in ambiguous}

    contradiction = expected_set & forbidden_set
    if contradiction:
        raise ValueError(f"expected and forbidden sets overlap: {sorted(contradiction)}")

    judged_expected = expected_set - ambiguous_set
    judged_returned = returned_set - ambiguous_set
    true_positives = judged_expected & judged_returned

    precision = len(true_positives) / len(judged_returned) if judged_returned else 0.0
    recall = len(true_positives) / len(judged_expected) if judged_expected else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return SetScore(
        expected=len(judged_expected),
        returned=len(judged_returned),
        true_positives=len(true_positives),
        precision=precision,
        recall=recall,
        f1=f1,
        missing=tuple(sorted(judged_expected - judged_returned)),
        extra=tuple(sorted(judged_returned - judged_expected)),
        forbidden_returned=tuple(sorted(returned_set & forbidden_set)),
        ambiguous_returned=tuple(sorted(returned_set & ambiguous_set)),
        ambiguous_excluded=len(ambiguous_set & (expected_set | returned_set)),
    )


@dataclass(frozen=True)
class PredicateExactness:
    """Row-level filter check: every returned row must satisfy the condition."""

    rows: int
    satisfied: int
    violations: tuple[str, ...]
    unknown_value_rows: int
    unknown_value_admitted: int

    @property
    def exactness(self) -> float:
        return self.satisfied / self.rows if self.rows else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "satisfied": self.satisfied,
            "exactness": self.exactness,
            "violations": list(self.violations),
            "unknown_value_rows": self.unknown_value_rows,
            "unknown_value_admitted": self.unknown_value_admitted,
        }


def predicate_exactness(
    rows: Iterable[Mapping[str, Any]],
    *,
    predicate: Callable[[Mapping[str, Any]], bool],
    describe: Callable[[Mapping[str, Any]], str],
    unknown: Callable[[Mapping[str, Any]], bool] | None = None,
    unknown_universe: int = 0,
) -> PredicateExactness:
    """Check that every returned row satisfies the selected condition.

    ``unknown`` marks a returned row whose filtered value was absent in the
    source; ``unknown_universe`` is how many unknown-valued rows the filter had
    the opportunity to admit. Reporting both is what makes "unknown-value
    behaviour" checkable instead of assumed.
    """
    materialized = list(rows)
    violations = tuple(describe(row) for row in materialized if not predicate(row))
    admitted_unknown = sum(1 for row in materialized if unknown and unknown(row))
    return PredicateExactness(
        rows=len(materialized),
        satisfied=len(materialized) - len(violations),
        violations=violations,
        unknown_value_rows=unknown_universe,
        unknown_value_admitted=admitted_unknown,
    )


@dataclass(frozen=True)
class AggregateComparison:
    """Declared-level counts, compared name by name."""

    expected: Mapping[str, int]
    actual: Mapping[str, int]
    mismatches: tuple[str, ...] = field(default=())

    @property
    def matches(self) -> bool:
        return not self.mismatches

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected": dict(self.expected),
            "actual": dict(self.actual),
            "mismatches": list(self.mismatches),
            "matches": self.matches,
        }


def compare_counts(expected: Mapping[str, int], actual: Mapping[str, int]) -> AggregateComparison:
    """Compare frozen aggregate counts against measured ones.

    A name present on one side only is itself a mismatch: an aggregate the
    experiment forgot to measure cannot silently pass.
    """
    names = sorted(set(expected) | set(actual))
    mismatches = tuple(
        f"{name}: expected {expected.get(name, 'absent')}, actual {actual.get(name, 'absent')}"
        for name in names
        if expected.get(name) != actual.get(name)
    )
    return AggregateComparison(expected=dict(expected), actual=dict(actual), mismatches=mismatches)

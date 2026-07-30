# ELSST R6 forward development dataset — 2026-07-29

This package proposes 19 source-grounded vocabulary lookup cases:

- 15 exact ELSST R6 targets, including two deprecated-label replacement cases;
- 4 `notRepresented` cases that must route to an open label, proposal, or
  abstention; and
- exact source-field, evidence, segmentation, and vocabulary release pins for
  every row.

The dataset is intentionally `developmentOnly` and `proposedUnsealed`. Its
source artifacts already appeared in earlier experiments, so none of these
rows may enter a new holdout. An independent reviewer must still decide
application adequacy. In particular, an ELSST-authored alternate label proves
the vocabulary lookup, but does not by itself prove that an assignment should
receive the evaluation grade `exact`.

## Files

- `dataset-manifest.json` defines the dataset boundary, pinned inputs, review
  blockers, and validation rules.
- `source-artifacts.json` pins each source artifact, source field, extraction,
  and digest.
- `rows.json` contains the 19 evidence spans and expected outcomes.
- `tests/test_elsst_r6_forward_development_dataset.py` checks the package
  against the current real Parquet source fields, the
  `structure-first-1200` segmentation snapshot, and the content-addressed
  ELSST R6 Turtle source.

All offsets are zero-based, half-open Unicode code-point offsets into the
named Parquet source field. They are not byte offsets into a downloaded PDF,
XML, or HTML file.

## Authority boundary

The rows were derived directly from the pinned source fields and ELSST R6.
They do not use the old fused registry, its candidate rankings, or its
adjudication verdicts.

Rights and licensing notes remain in source provenance. They are recorded for
traceability and do not limit use of this development dataset.

## Executed open-set path

Spicy Regs now materializes all four `notRepresented` rows through RefSpec's
actual candidate-only open-label permission and builder. Each assertion keeps
its language, facet, role, exact source artifact, evidence span, selectors,
extraction provenance, and digests. The combined portable graph passes the
exact RefSpec-pinned Rulespec validator.

The path does not turn these rows into accepted output or sealed evaluation.
They remain `developmentOnly`, `proposedUnsealed`, and excluded from the
reachable-candidate recall denominator. Source drift, evidence drift,
denominator leakage, or an accepted-output request fails.

## Review blockers

The package cannot become sealed gold until:

1. independent review confirms or changes the application grade for every
   target, especially `OFFSHORE WIND FARMS → WIND TURBINES`,
   `DEFORESTATION → FOREST MANAGEMENT`, and `POVERTY LEVEL → POVERTY`;
2. RefSpec assigns the managed release, import snapshot, expression corpus,
   and indexed-expression identities;
3. the two lifecycle rows are reviewed with the deprecated predecessor
   forbidden and only the active successor accepted;
4. the four `notRepresented` rows are excluded from reachable-candidate recall
   and keep their open-label, proposal, or abstention route; and
5. a future holdout is drawn from concept-, alias-, source-, artifact-,
   text-, and near-duplicate-disjoint material.

Run the focused checks with:

```console
uv run pytest -q tests/test_elsst_r6_forward_development_dataset.py
```

# Managed-vocabulary development set

This directory is the active development target set for candidate lookup
against the RefSpec-managed Federal Register Thesaurus release.

It deliberately reuses only the useful part of the earlier 35-item
adjudication: the pinned source artifacts, exact evidence spans, and selected
segments. It does not copy any fused-registry identifier or verdict. The
managed targets use exact member IRIs from one pinned release, and meanings
that the release does not contain use the explicit `notRepresented` outcome.

`targets.json` is provisional development evidence. It was prepared after
inspecting the managed release and candidate runs, so it can guide iteration
but cannot support an accuracy, adoption, accepted-output, or deployment
claim. Close matches are adequate only for this development loop. A later
sealed set requires independent review.

The existing 28-item pending holdout is not read, changed, or adjudicated by
this dataset.

The old files under `docs/evidence/gold-adjudication-2026-07-27/` remain
historical evidence for the failed fused-registry approach. Normal managed
experiments do not read their resolved target files.

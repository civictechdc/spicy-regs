"""Prototype: can hyperbolic hierarchy-encoder geometry reproduce the LLM judge grades?

Ledger item 2 of ``docs/evidence/hierarchy-embedding-research-2026-07-27.md``
proposes a HiT-lineage (Oxford *Language Models as Hierarchy Encoders*,
NeurIPS 2024) subsumption scorer as a geometric replacement for LLM
broader/narrower calls. This script measures the *zero-shot* version of that
claim against the only oracle we have: the blind machine adjudication in
``docs/evidence/gold-adjudication-2026-07-27/``.

**This is a prototype.** Nothing here is wired into the pipeline, and the heavy
dependency is deliberately not in ``pyproject.toml``/``uv.lock`` — it is
invoked out-of-tree:

    uv run --python 3.11 --with hierarchy_transformers --no-project \
        python tools/prototype_hyperbolic_subsumption.py \
        --report-path /tmp/hit-report.json

The hermetic half of the file (pair construction, the scoring algebra, the
decision rule, calibration, and the confusion matrix) imports nothing heavier
than the standard library plus ``pyarrow``, so it is testable without the
model. The model-touching half is imported lazily inside
:func:`encode_pair_geometry`. The self-test mirrors the Sentence Transformers
adapter pattern: it skips, rather than fails, when the optional package is
absent.

    python tools/prototype_hyperbolic_subsumption.py --self-test
    pytest tools/prototype_hyperbolic_subsumption.py

Decision rule
-------------
The package ships no published (``centri_weight``, ``threshold``) constant;
``HierarchyTransformerEvaluator`` grid-searches both on a validation set, and
``scripts/evaluation/hit/eval_hit.py`` then applies the winners to test data.
The score itself is fixed by the paper::

    score(child, parent) = -( d_H(child, parent)
                              + w * (||parent||_H - ||child||_H) )

and the pair is predicted subsuming when ``score > threshold``. We apply that
binary rule in *both* directions on each (gold label, candidate concept) pair
and read the 2x2 outcome as a four-way relation, so "equivalent" falls out as
"both directions clear the bar" — the mutual-proximity reading the ledger asks
for:

    fwd = score(child=gold,      parent=candidate)  -> candidate subsumes gold
    rev = score(child=candidate, parent=gold)       -> candidate subsumed by gold

    fwd & rev -> equivalent      fwd only -> subsumes
    rev only  -> subsumed_by     neither  -> neither

Two calibrations are reported, always labelled:

``native``
    (w, threshold) grid-searched on the model's *own* WordNet validation split
    (``Hierarchy-Transformers/WordNetNoun``, ``MixedHop-RandomNegatives-Pairs``)
    exactly as the package's evaluator does. Zero graded pairs are touched, so
    the result on our data is genuinely zero-shot.

``split-half``
    Two-fold: (w, threshold) fitted for maximum four-way agreement on one half
    of the graded pairs, scored on the held-out half, then the folds swapped.
    Reported as held-out agreement only.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

ADJUDICATION_DIR = REPO_ROOT / "docs" / "evidence" / "gold-adjudication-2026-07-27"
RESOLVED_ROUND_1 = ADJUDICATION_DIR / "resolved.json"
RESOLVED_ROUND_2 = ADJUDICATION_DIR / "resolved-fused.json"
GOLD_SPANS_PATH = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2" / "gold_spans.parquet"
REGISTRY_ROUND_1 = (
    REPO_ROOT
    / "output"
    / "segmentation-tagging-document-openai-structure-overlap-1800-v4"
    / "tagging_input_registry.parquet"
)
REGISTRY_ROUND_2 = REPO_ROOT / "output" / "fused-concept-registry-v1" / "registry.parquet"

# Pinned by commit sha, not by branch name, for the same reason the Sentence
# Transformers adapter pins revisions: `main` moves.
DEFAULT_MODEL_ID = "Hierarchy-Transformers/HiT-MiniLM-L12-WordNetNoun"
DEFAULT_MODEL_REVISION = "b170cbfa5bb770f144c69f75f826ccbd6e0c7b53"
ALTERNATE_MODELS = {
    "HiT-MiniLM-L12-WordNetNoun": "b170cbfa5bb770f144c69f75f826ccbd6e0c7b53",
    "HiT-MPNet-WordNetNoun": "733a89bb4487d49304c976fdadb654ba1ecfb244",
    "HiT-MiniLM-L12-SnomedCT": "f822e4351af6cab84e66cf673f29a801e426eafe",
    "HiT-MiniLM-L6-WordNetNoun": "f3b5d9a77c876b050b4c17d86ec12a0e325aa1ca",
}

CALIBRATION_DATASET = "Hierarchy-Transformers/WordNetNoun"
CALIBRATION_SUBSET = "MixedHop-RandomNegatives-Pairs"
CALIBRATION_SPLIT = "val"
CALIBRATION_SAMPLE = 20_000
CALIBRATION_SEED = 20260728

# The package evaluator sweeps `centri_weight` over range(50)/10 with early
# stopping; we sweep the same closed grid without the early stop.
CENTRI_WEIGHT_GRID = tuple(step / 10 for step in range(50))

SUBSUMES = "subsumes"
SUBSUMED_BY = "subsumed_by"
EQUIVALENT = "equivalent"
NEITHER = "neither"
RELATIONS = (SUBSUMES, SUBSUMED_BY, EQUIVALENT, NEITHER)

#: The ledger's reduction of the six judge grades onto directed relations.
#: ``broader`` means the judges called the candidate broader than the gold
#: label, i.e. the candidate subsumes the gold label.
GRADE_TO_RELATION = {
    "broader": SUBSUMES,
    "narrower": SUBSUMED_BY,
    "exact": EQUIVALENT,
    "close": EQUIVALENT,
    "related": NEITHER,
    "wrong": NEITHER,
}

#: Collapse used for the strict three-way reading the ledger names. An
#: exact/close candidate is a non-strict subsumer of the gold label, so
#: ``equivalent`` folds into ``subsumes`` on both sides of the comparison.
THREE_WAY = {SUBSUMES: SUBSUMES, EQUIVALENT: SUBSUMES, SUBSUMED_BY: SUBSUMED_BY, NEITHER: NEITHER}

#: Judge self-consistency on the same 35 items, from the adjudication README:
#: 31/35 grades and 34/35 adequacy in round 2 (the harder, 513k-row round).
JUDGE_GRADE_SELF_CONSISTENCY = 31 / 35
JUDGE_ADEQUACY_SELF_CONSISTENCY = 34 / 35

#: Held-out truth for the sanity block. Direction is always (gold, candidate),
#: matching the graded pairs, and the relation is the candidate's relation to
#: the gold label.
SANITY_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("PFAS", "Hazardous substances", SUBSUMES),
    ("oranges and grapefruit", "Oranges", SUBSUMED_BY),
    ("dog", "animal", SUBSUMES),
    ("animal", "dog", SUBSUMED_BY),
    ("sedan", "car", SUBSUMES),
    ("car", "vehicle", SUBSUMES),
    ("vehicle", "car", SUBSUMED_BY),
    ("oak", "tree", SUBSUMES),
    ("tree", "oak", SUBSUMED_BY),
    ("mountain bike", "bicycle", SUBSUMES),
    ("bicycle", "mountain bike", SUBSUMED_BY),
    ("copper", "metal", SUBSUMES),
    ("wetlands permitting", "Environmental protection", SUBSUMES),
    ("Environmental protection", "wetlands permitting", SUBSUMED_BY),
    ("Medicaid", "Medicaid", EQUIVALENT),
    ("attorney", "lawyer", EQUIVALENT),
    ("physician", "doctor", EQUIVALENT),
    ("piano", "stapler", NEITHER),
    ("rainfall", "bank account", NEITHER),
    ("dog", "cat", NEITHER),
)


class PrototypeError(RuntimeError):
    """The stored inputs cannot produce a usable prototype evaluation."""


# ---------------------------------------------------------------------------
# Pair construction (hermetic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GradedPair:
    """One judge-graded (gold label, best candidate concept) pair."""

    pair_id: str
    round_id: str
    item_id: str
    gold_label: str
    candidate_id: str
    candidate_label: str
    candidate_aliases: tuple[str, ...]
    grade: str
    adequate_target: bool

    @property
    def relation(self) -> str:
        return GRADE_TO_RELATION[self.grade]

    @property
    def candidate_text(self) -> str:
        """Entity text handed to the encoder.

        HiT encodes entity *names*. Aliases are appended as a comma list when
        present, which is how the HiT datasets render multi-name entities.
        """
        if not self.candidate_aliases:
            return self.candidate_label
        return ", ".join((self.candidate_label, *self.candidate_aliases))


def _json_list(value: object) -> tuple[str, ...]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, json.JSONDecodeError):
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(item) for item in parsed if item)


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq  # noqa: PLC0415 - keep module import cheap

    if not path.is_file():
        raise PrototypeError(f"required table is absent: {path}")
    return pq.read_table(path).to_pylist()


def load_graded_pairs(
    *,
    resolved_round_1: Path = RESOLVED_ROUND_1,
    resolved_round_2: Path = RESOLVED_ROUND_2,
    gold_spans_path: Path = GOLD_SPANS_PATH,
    registries: Sequence[Path] = (REGISTRY_ROUND_1, REGISTRY_ROUND_2),
) -> list[GradedPair]:
    """Rebuild the graded pairs from the frozen adjudication records.

    Round 1 grades the 901-row registry, round 2 the 513k fused registry. Both
    rounds are kept: they are separate adjudications of the same 35 items, and
    where they disagree on an identical pair that disagreement is itself part
    of the judges' measured self-consistency.
    """
    gold_labels = {
        str(row["gold_id"]): str(row["concept_label"]) for row in _read_parquet_rows(gold_spans_path)
    }
    concepts: dict[str, dict[str, Any]] = {}
    for registry in registries:
        for row in _read_parquet_rows(registry):
            concepts.setdefault(str(row["concept_id"]), row)

    pairs: list[GradedPair] = []
    for round_id, path in (("round-1-901", resolved_round_1), ("round-2-fused", resolved_round_2)):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for item in payload["items"]:
            candidate_id = item.get("best_candidate_id")
            if not candidate_id:
                # The round-1 `wrong` item has no best candidate at all; there
                # is no pair to score, and inventing one would be a fabricated
                # datum. It is reported as an excluded row instead.
                continue
            item_id = str(item["item_id"])
            gold_id = item_id.rsplit("-", 1)[-1]
            if gold_id not in gold_labels:
                raise PrototypeError(f"adjudicated item has no gold span: {item_id}")
            concept = concepts.get(str(candidate_id))
            if concept is None:
                raise PrototypeError(f"adjudicated candidate is not in any registry: {candidate_id}")
            grade = str(item["grade"])
            if grade not in GRADE_TO_RELATION:
                raise PrototypeError(f"unknown judge grade: {grade!r}")
            pairs.append(
                GradedPair(
                    pair_id=f"{round_id}:{gold_id}",
                    round_id=round_id,
                    item_id=item_id,
                    gold_label=gold_labels[gold_id],
                    candidate_id=str(candidate_id),
                    candidate_label=str(concept["pref_label"]),
                    candidate_aliases=_json_list(concept.get("alt_labels_json")),
                    grade=grade,
                    adequate_target=bool(item["adequate_target"]),
                )
            )
    if not pairs:
        raise PrototypeError("no graded pairs were reconstructed")
    return pairs


# ---------------------------------------------------------------------------
# Scoring algebra and decision rule (hermetic)
# ---------------------------------------------------------------------------


def subsumption_score(
    distance: float, child_norm: float, parent_norm: float, centri_weight: float
) -> float:
    """The HiT paper's empirical subsumption score.

    Mirrors ``HierarchyTransformerEvaluator.inference``::

        -(dists + centri_weight * (parent_norms - child_norms))
    """
    return -(distance + centri_weight * (parent_norm - child_norm))


@dataclass(frozen=True)
class PairGeometry:
    """Hyperbolic measurements for one pair, independent of the decision rule."""

    pair_id: str
    distance: float
    gold_norm: float
    candidate_norm: float

    def scores(self, centri_weight: float) -> tuple[float, float]:
        """``(forward, reverse)`` subsumption scores.

        Forward asks "is the candidate a parent of the gold label"; reverse
        asks the mirror. ``distance`` is symmetric, so the two differ only by
        the sign of the centripetal term.
        """
        forward = subsumption_score(self.distance, self.gold_norm, self.candidate_norm, centri_weight)
        reverse = subsumption_score(self.distance, self.candidate_norm, self.gold_norm, centri_weight)
        return forward, reverse


def classify_relation(forward: float, reverse: float, threshold: float) -> str:
    """Read the package's binary rule applied in both directions as a relation."""
    is_forward = forward > threshold
    is_reverse = reverse > threshold
    if is_forward and is_reverse:
        return EQUIVALENT
    if is_forward:
        return SUBSUMES
    if is_reverse:
        return SUBSUMED_BY
    return NEITHER


def predict_relations(
    geometry: Sequence[PairGeometry], centri_weight: float, threshold: float
) -> list[str]:
    return [classify_relation(*item.scores(centri_weight), threshold) for item in geometry]


# ---------------------------------------------------------------------------
# Metrics (hermetic)
# ---------------------------------------------------------------------------


def confusion_matrix(truth: Sequence[str], predicted: Sequence[str], labels: Sequence[str]) -> dict[str, dict[str, int]]:
    if len(truth) != len(predicted):
        raise PrototypeError("truth and prediction lengths differ")
    matrix = {row: {column: 0 for column in labels} for row in labels}
    for actual, guess in zip(truth, predicted, strict=True):
        matrix[actual][guess] += 1
    return matrix


def agreement(truth: Sequence[str], predicted: Sequence[str]) -> float:
    if not truth:
        return 0.0
    hits = sum(1 for actual, guess in zip(truth, predicted, strict=True) if actual == guess)
    return hits / len(truth)


def macro_f1(truth: Sequence[str], predicted: Sequence[str], labels: Sequence[str]) -> float:
    scores: list[float] = []
    for label in labels:
        true_positive = sum(1 for a, p in zip(truth, predicted, strict=True) if a == label and p == label)
        false_positive = sum(1 for a, p in zip(truth, predicted, strict=True) if a != label and p == label)
        false_negative = sum(1 for a, p in zip(truth, predicted, strict=True) if a == label and p != label)
        if true_positive == 0:
            scores.append(0.0)
            continue
        precision = true_positive / (true_positive + false_positive)
        recall = true_positive / (true_positive + false_negative)
        scores.append(2 * precision * recall / (precision + recall))
    return sum(scores) / len(scores) if scores else 0.0


def majority_baseline(truth: Sequence[str]) -> tuple[str, float]:
    """The accuracy a constant predictor would reach. Any real rule must beat it."""
    if not truth:
        return (NEITHER, 0.0)
    counts: dict[str, int] = {}
    for label in truth:
        counts[label] = counts.get(label, 0) + 1
    best = max(counts.items(), key=lambda entry: (entry[1], entry[0]))
    return best[0], best[1] / len(truth)


def direction_only_accuracy(
    pairs: Sequence[GradedPair], geometry: Sequence[PairGeometry]
) -> dict[str, Any]:
    """Threshold-free probe: does the norm ordering alone name the right direction?

    Restricted to the pairs the judges called directional (``broader`` or
    ``narrower``). In a hierarchy encoder the more specific entity sits further
    from the origin, so ``gold_norm > candidate_norm`` should mean the
    candidate subsumes the gold label. This separates "the geometry knows which
    entity is more general" from "the calibrated threshold transfers", and it
    depends on no hyperparameter at all.
    """
    directional = [
        (pair, item)
        for pair, item in zip(pairs, geometry, strict=True)
        if pair.relation in {SUBSUMES, SUBSUMED_BY}
    ]
    if not directional:
        return {"n": 0, "accuracy": 0.0, "coin_flip": 0.5}
    hits = sum(
        1
        for pair, item in directional
        if (SUBSUMES if item.gold_norm > item.candidate_norm else SUBSUMED_BY) == pair.relation
    )
    majority = max(
        sum(1 for pair, _ in directional if pair.relation == relation)
        for relation in (SUBSUMES, SUBSUMED_BY)
    )
    return {
        "n": len(directional),
        "correct": hits,
        "accuracy": hits / len(directional),
        "majority_baseline": majority / len(directional),
        "coin_flip": 0.5,
    }


# ---------------------------------------------------------------------------
# Calibration (hermetic given scores)
# ---------------------------------------------------------------------------


def _threshold_candidates(values: Iterable[float]) -> list[float]:
    """Thresholds that can change any decision, plus one below every score.

    The package grid-searches thresholds on a 0.01 lattice. Sweeping the score
    values themselves is the exact optimum of the same objective and cannot
    miss a better lattice point.
    """
    ordered = sorted(set(values))
    if not ordered:
        return [0.0]
    step = 1e-9
    return [ordered[0] - 1.0] + [value + step for value in ordered]


def best_binary_rule(
    distances: Sequence[float],
    child_norms: Sequence[float],
    parent_norms: Sequence[float],
    labels: Sequence[int],
    *,
    weights: Sequence[float] = CENTRI_WEIGHT_GRID,
) -> dict[str, float]:
    """Grid-search (centri_weight, threshold) for best F1, as the package does."""
    best = {"centri_weight": 0.0, "threshold": 0.0, "f1": -1.0, "precision": 0.0, "recall": 0.0}
    positives = sum(1 for label in labels if label == 1)
    if positives == 0:
        raise PrototypeError("calibration set has no positive pairs")
    for weight in weights:
        scored = sorted(
            (
                (subsumption_score(distance, child, parent, weight), label)
                for distance, child, parent, label in zip(
                    distances, child_norms, parent_norms, labels, strict=True
                )
            ),
            reverse=True,
        )
        true_positive = 0
        false_positive = 0
        for index, (score, label) in enumerate(scored):
            if label == 1:
                true_positive += 1
            else:
                false_positive += 1
            if index + 1 < len(scored) and math.isclose(scored[index + 1][0], score):
                continue
            precision = true_positive / (true_positive + false_positive)
            recall = true_positive / positives
            if precision + recall == 0:
                continue
            f1 = 2 * precision * recall / (precision + recall)
            if f1 > best["f1"]:
                next_score = scored[index + 1][0] if index + 1 < len(scored) else score - 1.0
                best = {
                    "centri_weight": weight,
                    "threshold": (score + next_score) / 2,
                    "f1": f1,
                    "precision": precision,
                    "recall": recall,
                }
    return best


def best_relation_rule(
    geometry: Sequence[PairGeometry],
    truth: Sequence[str],
    *,
    weights: Sequence[float] = CENTRI_WEIGHT_GRID,
) -> dict[str, float]:
    """Grid-search (centri_weight, threshold) for best four-way agreement."""
    best = {"centri_weight": 0.0, "threshold": 0.0, "agreement": -1.0}
    for weight in weights:
        scores = [item.scores(weight) for item in geometry]
        for threshold in _threshold_candidates(value for pair in scores for value in pair):
            predicted = [classify_relation(forward, reverse, threshold) for forward, reverse in scores]
            score = agreement(truth, predicted)
            if score > best["agreement"]:
                best = {"centri_weight": weight, "threshold": threshold, "agreement": score}
    return best


def split_half_folds(pairs: Sequence[GradedPair]) -> tuple[list[int], list[int]]:
    """Deterministic, stratified halves: alternate within each truth relation.

    Stratifying matters at n=69 with one ``subsumed_by`` example; a random
    split would routinely put every instance of a class on one side.
    """
    buckets: dict[str, list[int]] = {}
    for index, pair in enumerate(pairs):
        buckets.setdefault(pair.relation, []).append(index)
    fold_a: list[int] = []
    fold_b: list[int] = []
    for relation in sorted(buckets):
        for position, index in enumerate(buckets[relation]):
            (fold_a if position % 2 == 0 else fold_b).append(index)
    return sorted(fold_a), sorted(fold_b)


# ---------------------------------------------------------------------------
# Model-touching half (lazy imports)
# ---------------------------------------------------------------------------


def load_model(model_id: str, revision: str) -> Any:
    from hierarchy_transformers import HierarchyTransformer  # noqa: PLC0415

    return HierarchyTransformer.from_pretrained(model_id, revision=revision)


def _measure(model: Any, children: Sequence[str], parents: Sequence[str], batch_size: int) -> list[tuple[float, float, float]]:
    child_embeddings = model.encode(sentences=list(children), batch_size=batch_size, convert_to_tensor=True)
    parent_embeddings = model.encode(sentences=list(parents), batch_size=batch_size, convert_to_tensor=True)
    distances = model.manifold.dist(child_embeddings, parent_embeddings)
    child_norms = model.manifold.dist0(child_embeddings)
    parent_norms = model.manifold.dist0(parent_embeddings)
    return [
        (float(distance), float(child), float(parent))
        for distance, child, parent in zip(
            distances.tolist(), child_norms.tolist(), parent_norms.tolist(), strict=True
        )
    ]


def encode_pair_geometry(
    model: Any, pairs: Sequence[GradedPair], *, batch_size: int = 128
) -> list[PairGeometry]:
    measured = _measure(
        model, [pair.gold_label for pair in pairs], [pair.candidate_text for pair in pairs], batch_size
    )
    return [
        PairGeometry(pair_id=pair.pair_id, distance=distance, gold_norm=gold, candidate_norm=candidate)
        for pair, (distance, gold, candidate) in zip(pairs, measured, strict=True)
    ]


def encode_sanity_geometry(
    model: Any, sanity: Sequence[tuple[str, str, str]], *, batch_size: int = 128
) -> list[PairGeometry]:
    measured = _measure(model, [left for left, _, _ in sanity], [right for _, right, _ in sanity], batch_size)
    return [
        PairGeometry(pair_id=f"sanity:{left}|{right}", distance=distance, gold_norm=gold, candidate_norm=candidate)
        for (left, right, _), (distance, gold, candidate) in zip(sanity, measured, strict=True)
    ]


def _quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[int(fraction * (len(ordered) - 1))]


def summarise_measurements(distances: Sequence[float], gaps: Sequence[float]) -> dict[str, float]:
    """Median distance and child-minus-parent norm gap, with tails."""
    if not distances:
        return {"n": 0}
    return {
        "n": len(distances),
        "median_distance": _quantile(distances, 0.5),
        "median_norm_gap": _quantile(gaps, 0.5),
        "norm_gap_p10": _quantile(gaps, 0.1),
        "norm_gap_p90": _quantile(gaps, 0.9),
    }


def clears_rule(distance: float, gap: float, centri_weight: float, threshold: float) -> bool:
    """Does this (distance, child-minus-parent gap) clear the subsumption rule?"""
    return subsumption_score(distance, gap, 0.0, centri_weight) > threshold


def native_calibration(model: Any, *, batch_size: int = 512, sample: int = CALIBRATION_SAMPLE) -> dict[str, Any]:
    """Grid-search the package rule on the model's own WordNet validation split."""
    import random  # noqa: PLC0415

    from hierarchy_transformers.datasets import load_hf_dataset  # noqa: PLC0415

    dataset = load_hf_dataset(CALIBRATION_DATASET, CALIBRATION_SUBSET)[CALIBRATION_SPLIT]
    indices = list(range(len(dataset)))
    random.Random(CALIBRATION_SEED).shuffle(indices)
    indices = sorted(indices[: min(sample, len(indices))])
    subset = dataset.select(indices)
    measured = _measure(model, subset["child"], subset["parent"], batch_size)
    rule = best_binary_rule(
        [distance for distance, _, _ in measured],
        [child for _, child, _ in measured],
        [parent for _, _, parent in measured],
        [int(label) for label in subset["label"]],
    )
    labels = [int(label) for label in subset["label"]]
    positives = [(distance, child - parent) for (distance, child, parent), label in zip(measured, labels, strict=True) if label == 1]
    negatives = [(distance, child - parent) for (distance, child, parent), label in zip(measured, labels, strict=True) if label == 0]
    rule.update(
        {
            "dataset": f"{CALIBRATION_DATASET}/{CALIBRATION_SUBSET}",
            "split": CALIBRATION_SPLIT,
            "sample_size": len(indices),
            "seed": CALIBRATION_SEED,
            "positive_profile": summarise_measurements(
                [distance for distance, _ in positives], [gap for _, gap in positives]
            ),
            "negative_profile": summarise_measurements(
                [distance for distance, _ in negatives], [gap for _, gap in negatives]
            ),
            "positives_clearing_rule": sum(
                1
                for distance, gap in positives
                if clears_rule(distance, gap, rule["centri_weight"], rule["threshold"])
            ),
        }
    )
    return rule


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------


def _relation_report(pairs: Sequence[GradedPair], geometry: Sequence[PairGeometry], weight: float, threshold: float) -> dict[str, Any]:
    truth = [pair.relation for pair in pairs]
    predicted = predict_relations(geometry, weight, threshold)
    truth_3 = [THREE_WAY[label] for label in truth]
    predicted_3 = [THREE_WAY[label] for label in predicted]
    adequacy_truth = [EQUIVALENT if pair.adequate_target else NEITHER for pair in pairs]
    adequacy_predicted = [EQUIVALENT if label == EQUIVALENT else NEITHER for label in predicted]
    return {
        "centri_weight": weight,
        "threshold": threshold,
        "n": len(pairs),
        "four_way": {
            "agreement": agreement(truth, predicted),
            "correct": sum(1 for a, p in zip(truth, predicted, strict=True) if a == p),
            "macro_f1": macro_f1(truth, predicted, RELATIONS),
            "confusion": confusion_matrix(truth, predicted, RELATIONS),
        },
        "three_way": {
            "agreement": agreement(truth_3, predicted_3),
            "correct": sum(1 for a, p in zip(truth_3, predicted_3, strict=True) if a == p),
            "macro_f1": macro_f1(truth_3, predicted_3, (SUBSUMES, SUBSUMED_BY, NEITHER)),
            "confusion": confusion_matrix(truth_3, predicted_3, (SUBSUMES, SUBSUMED_BY, NEITHER)),
        },
        "adequacy": {
            "agreement": agreement(adequacy_truth, adequacy_predicted),
            "correct": sum(1 for a, p in zip(adequacy_truth, adequacy_predicted, strict=True) if a == p),
            "confusion": confusion_matrix(adequacy_truth, adequacy_predicted, (EQUIVALENT, NEITHER)),
        },
        "per_pair": [
            {
                "pair_id": pair.pair_id,
                "round": pair.round_id,
                "gold_label": pair.gold_label,
                "candidate_label": pair.candidate_label,
                "grade": pair.grade,
                "truth": pair.relation,
                "predicted": guess,
                "distance": round(item.distance, 4),
                "gold_norm": round(item.gold_norm, 4),
                "candidate_norm": round(item.candidate_norm, 4),
                "forward": round(item.scores(weight)[0], 4),
                "reverse": round(item.scores(weight)[1], 4),
            }
            for pair, item, guess in zip(pairs, geometry, predicted, strict=True)
        ],
    }


def _norm_gap_transfer(
    pairs: Sequence[GradedPair], geometry: Sequence[PairGeometry], native: Mapping[str, Any]
) -> dict[str, Any]:
    """Does the encoder's depth signal survive the move off WordNet?

    Compares our true-``subsumes`` pairs against the model's own validation
    positives on the two quantities the rule is made of. Distances transferring
    while the centripetal gap collapses is a different diagnosis — and a
    different fix — from both terms drifting together.
    """
    subsuming = [
        (item.distance, item.gold_norm - item.candidate_norm)
        for pair, item in zip(pairs, geometry, strict=True)
        if pair.relation == SUBSUMES
    ]
    weight = float(native["centri_weight"])
    threshold = float(native["threshold"])
    ours = summarise_measurements([d for d, _ in subsuming], [gap for _, gap in subsuming])
    calibration_positives = native.get("positive_profile", {})
    return {
        "calibration_positives": calibration_positives,
        "calibration_negatives": native.get("negative_profile", {}),
        "calibration_positives_clearing_rule": native.get("positives_clearing_rule"),
        "our_subsumes": ours,
        "our_subsumes_clearing_rule": sum(
            1 for distance, gap in subsuming if clears_rule(distance, gap, weight, threshold)
        ),
        "gap_required_at_median_distance": {
            "calibration": (calibration_positives.get("median_distance", 0.0) + threshold) / weight
            if weight
            else None,
            "ours": (ours.get("median_distance", 0.0) + threshold) / weight if weight else None,
        },
    }


def run_evaluation(
    *,
    model_id: str = DEFAULT_MODEL_ID,
    revision: str = DEFAULT_MODEL_REVISION,
    calibration_sample: int = CALIBRATION_SAMPLE,
) -> dict[str, Any]:
    pairs = load_graded_pairs()
    model = load_model(model_id, revision)
    geometry = encode_pair_geometry(model, pairs)
    truth = [pair.relation for pair in pairs]

    native = native_calibration(model, sample=calibration_sample)
    native_report = _relation_report(pairs, geometry, native["centri_weight"], native["threshold"])

    fold_a, fold_b = split_half_folds(pairs)
    folds: list[dict[str, Any]] = []
    held_out_truth: list[str] = []
    held_out_predicted: list[str] = []
    for name, fit, evaluate in (("A->B", fold_a, fold_b), ("B->A", fold_b, fold_a)):
        rule = best_relation_rule([geometry[i] for i in fit], [truth[i] for i in fit])
        evaluate_truth = [truth[i] for i in evaluate]
        evaluate_predicted = predict_relations(
            [geometry[i] for i in evaluate], rule["centri_weight"], rule["threshold"]
        )
        held_out_truth.extend(evaluate_truth)
        held_out_predicted.extend(evaluate_predicted)
        folds.append(
            {
                "fold": name,
                "fit_n": len(fit),
                "eval_n": len(evaluate),
                "centri_weight": rule["centri_weight"],
                "threshold": rule["threshold"],
                "fit_agreement": rule["agreement"],
                "held_out_agreement": agreement(evaluate_truth, evaluate_predicted),
            }
        )

    held_out_truth_3 = [THREE_WAY[label] for label in held_out_truth]
    held_out_predicted_3 = [THREE_WAY[label] for label in held_out_predicted]
    oracle = best_relation_rule(geometry, truth)

    sanity_geometry = encode_sanity_geometry(model, SANITY_PAIRS)
    sanity_truth = [relation for _, _, relation in SANITY_PAIRS]
    sanity_predicted = predict_relations(sanity_geometry, native["centri_weight"], native["threshold"])

    return {
        "model_id": model_id,
        "revision": revision,
        "pairs": [asdict(pair) for pair in pairs],
        "excluded": [
            {
                "item_id": "gold-adjudication-gold_9699f26de500ef0bce70b53c",
                "round": "round-1-901",
                "reason": "judges recorded grade 'wrong' with no best candidate; no pair exists to score",
            }
        ],
        "majority_baseline": {
            "four_way": dict(zip(("relation", "agreement"), majority_baseline(truth), strict=True)),
            "three_way": dict(
                zip(
                    ("relation", "agreement"),
                    majority_baseline([THREE_WAY[label] for label in truth]),
                    strict=True,
                )
            ),
            "adequacy": dict(
                zip(
                    ("relation", "agreement"),
                    majority_baseline(
                        [EQUIVALENT if pair.adequate_target else NEITHER for pair in pairs]
                    ),
                    strict=True,
                )
            ),
        },
        "direction_only": direction_only_accuracy(pairs, geometry),
        "norm_gap_transfer": _norm_gap_transfer(pairs, geometry, native),
        "native_calibration": native,
        "native": native_report,
        "split_half": {
            "folds": folds,
            "held_out_four_way": {
                "agreement": agreement(held_out_truth, held_out_predicted),
                "correct": sum(1 for a, p in zip(held_out_truth, held_out_predicted, strict=True) if a == p),
                "n": len(held_out_truth),
                "macro_f1": macro_f1(held_out_truth, held_out_predicted, RELATIONS),
                "confusion": confusion_matrix(held_out_truth, held_out_predicted, RELATIONS),
            },
            "held_out_three_way": {
                "agreement": agreement(held_out_truth_3, held_out_predicted_3),
                "correct": sum(1 for a, p in zip(held_out_truth_3, held_out_predicted_3, strict=True) if a == p),
                "confusion": confusion_matrix(
                    held_out_truth_3, held_out_predicted_3, (SUBSUMES, SUBSUMED_BY, NEITHER)
                ),
            },
        },
        "oracle_upper_bound": oracle,
        "sanity": {
            "agreement": agreement(sanity_truth, sanity_predicted),
            "rows": [
                {
                    "gold": left,
                    "candidate": right,
                    "truth": relation,
                    "predicted": guess,
                    "distance": round(item.distance, 4),
                    "gold_norm": round(item.gold_norm, 4),
                    "candidate_norm": round(item.candidate_norm, 4),
                    "forward": round(item.scores(native["centri_weight"])[0], 4),
                    "reverse": round(item.scores(native["centri_weight"])[1], 4),
                }
                for (left, right, relation), item, guess in zip(
                    SANITY_PAIRS, sanity_geometry, sanity_predicted, strict=True
                )
            ],
        },
        "bar": {
            "judge_grade_self_consistency": JUDGE_GRADE_SELF_CONSISTENCY,
            "judge_adequacy_self_consistency": JUDGE_ADEQUACY_SELF_CONSISTENCY,
        },
    }


# ---------------------------------------------------------------------------
# Hermetic self-test
# ---------------------------------------------------------------------------


def test_subsumption_score_matches_package_formula() -> None:
    assert subsumption_score(2.0, 5.0, 3.0, 1.0) == -(2.0 + 1.0 * (3.0 - 5.0))
    assert subsumption_score(2.0, 5.0, 3.0, 0.0) == -2.0


def test_classify_relation_covers_the_two_by_two() -> None:
    assert classify_relation(1.0, 1.0, 0.0) == EQUIVALENT
    assert classify_relation(1.0, -1.0, 0.0) == SUBSUMES
    assert classify_relation(-1.0, 1.0, 0.0) == SUBSUMED_BY
    assert classify_relation(-1.0, -1.0, 0.0) == NEITHER


def test_pair_geometry_direction_follows_the_norm_gap() -> None:
    # A child sits further from the origin than its parent.
    geometry = PairGeometry(pair_id="x", distance=1.0, gold_norm=9.0, candidate_norm=5.0)
    forward, reverse = geometry.scores(1.0)
    assert forward > reverse
    assert classify_relation(forward, reverse, threshold=-2.0) == SUBSUMES


def test_confusion_matrix_and_agreement() -> None:
    truth = [SUBSUMES, SUBSUMES, NEITHER]
    predicted = [SUBSUMES, NEITHER, NEITHER]
    matrix = confusion_matrix(truth, predicted, RELATIONS)
    assert matrix[SUBSUMES][SUBSUMES] == 1
    assert matrix[SUBSUMES][NEITHER] == 1
    assert matrix[NEITHER][NEITHER] == 1
    assert math.isclose(agreement(truth, predicted), 2 / 3)


def test_best_binary_rule_recovers_a_separable_split() -> None:
    distances = [1.0, 1.0, 8.0, 8.0]
    child_norms = [9.0, 9.0, 5.0, 5.0]
    parent_norms = [5.0, 5.0, 9.0, 9.0]
    labels = [1, 1, 0, 0]
    rule = best_binary_rule(distances, child_norms, parent_norms, labels, weights=(0.0, 1.0))
    assert math.isclose(rule["f1"], 1.0)


def test_split_half_folds_are_stratified_and_disjoint() -> None:
    pairs = [
        GradedPair(f"p{i}", "r", f"i{i}", "g", "c", "C", (), grade, grade in {"exact", "close"})
        for i, grade in enumerate(["broader"] * 6 + ["related"] * 4 + ["close"] * 2 + ["narrower"])
    ]
    fold_a, fold_b = split_half_folds(pairs)
    assert not set(fold_a) & set(fold_b)
    assert sorted(fold_a + fold_b) == list(range(len(pairs)))
    broader_a = sum(1 for i in fold_a if pairs[i].grade == "broader")
    assert broader_a == 3


def test_majority_baseline() -> None:
    relation, share = majority_baseline([SUBSUMES, SUBSUMES, NEITHER])
    assert relation == SUBSUMES
    assert math.isclose(share, 2 / 3)


def test_clears_rule_matches_the_two_argument_score() -> None:
    distance, child, parent, weight = 22.0, 20.0, 13.0, 2.2
    threshold = -11.9
    direct = subsumption_score(distance, child, parent, weight) > threshold
    assert clears_rule(distance, child - parent, weight, threshold) is direct


def test_summarise_measurements_reports_median_and_tails() -> None:
    # Eleven values so the floor-index quantile lands on an exact element.
    summary = summarise_measurements([float(v) for v in range(11)], [float(v) for v in range(11)])
    assert summary["n"] == 11
    assert math.isclose(summary["median_distance"], 5.0)
    assert math.isclose(summary["median_norm_gap"], 5.0)
    assert math.isclose(summary["norm_gap_p10"], 1.0)
    assert math.isclose(summary["norm_gap_p90"], 9.0)


def test_direction_only_accuracy_reads_the_norm_gap() -> None:
    pairs = [
        GradedPair("a", "r", "i", "g", "c", "C", (), "broader", False),
        GradedPair("b", "r", "i", "g", "c", "C", (), "narrower", False),
        GradedPair("c", "r", "i", "g", "c", "C", (), "related", False),
    ]
    geometry = [
        PairGeometry("a", 1.0, gold_norm=9.0, candidate_norm=5.0),  # correct: gold deeper
        PairGeometry("b", 1.0, gold_norm=9.0, candidate_norm=5.0),  # wrong direction
        PairGeometry("c", 1.0, gold_norm=1.0, candidate_norm=2.0),  # not directional, ignored
    ]
    result = direction_only_accuracy(pairs, geometry)
    assert result["n"] == 2
    assert result["correct"] == 1
    assert math.isclose(result["accuracy"], 0.5)


def test_grade_mapping_is_total_over_the_recorded_grades() -> None:
    recorded = {"exact", "close", "broader", "narrower", "related", "wrong"}
    assert set(GRADE_TO_RELATION) == recorded
    assert set(GRADE_TO_RELATION.values()) <= set(RELATIONS)


def test_graded_pairs_rebuild_from_the_frozen_records() -> None:
    if not GOLD_SPANS_PATH.is_file() or not REGISTRY_ROUND_2.is_file():
        print("SKIP test_graded_pairs_rebuild_from_the_frozen_records: frozen tables absent")
        return
    pairs = load_graded_pairs()
    assert len(pairs) == 69, len(pairs)
    by_round = {pair.round_id for pair in pairs}
    assert by_round == {"round-1-901", "round-2-fused"}
    assert all(pair.gold_label and pair.candidate_label for pair in pairs)
    assert {pair.relation for pair in pairs} <= set(RELATIONS)


def test_model_loads_when_the_optional_package_is_present() -> None:
    try:
        import hierarchy_transformers  # noqa: F401,PLC0415
    except ImportError:
        print("SKIP test_model_loads_when_the_optional_package_is_present: hierarchy_transformers is absent")
        return
    model = load_model(DEFAULT_MODEL_ID, DEFAULT_MODEL_REVISION)
    geometry = encode_pair_geometry(
        model,
        [GradedPair("smoke", "r", "i", "berry", "c", "fruit", (), "broader", False)],
    )
    assert len(geometry) == 1
    assert geometry[0].distance > 0
    # A hierarchy encoder places the child further from the origin.
    assert geometry[0].gold_norm > geometry[0].candidate_norm


def _self_test() -> int:
    failures = 0
    for name, function in sorted(globals().items()):
        if not name.startswith("test_") or not callable(function):
            continue
        try:
            function()
        except AssertionError as error:
            failures += 1
            print(f"FAIL {name}: {error}")
        except Exception as error:  # noqa: BLE001 - a prototype self-test reports, it does not raise
            failures += 1
            print(f"ERROR {name}: {type(error).__name__}: {error}")
        else:
            print(f"ok   {name}")
    print(f"{'FAILED' if failures else 'PASSED'}: {failures} failure(s)")
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test", action="store_true", help="run the hermetic checks and exit")
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--revision", default=DEFAULT_MODEL_REVISION)
    parser.add_argument("--calibration-sample", type=int, default=CALIBRATION_SAMPLE)
    parser.add_argument("--report-path", type=Path, default=None, help="write the JSON report here")
    arguments = parser.parse_args(argv)

    if arguments.self_test:
        return _self_test()

    if arguments.model_id != DEFAULT_MODEL_ID and arguments.revision == DEFAULT_MODEL_REVISION:
        name = arguments.model_id.rsplit("/", 1)[-1]
        if name in ALTERNATE_MODELS:
            arguments.revision = ALTERNATE_MODELS[name]

    report = run_evaluation(
        model_id=arguments.model_id,
        revision=arguments.revision,
        calibration_sample=arguments.calibration_sample,
    )
    serialised = json.dumps(report, indent=1, sort_keys=True)
    if arguments.report_path is not None:
        arguments.report_path.parent.mkdir(parents=True, exist_ok=True)
        arguments.report_path.write_text(serialised + "\n", encoding="utf-8")
        print(f"wrote {arguments.report_path}")
    else:
        print(serialised)

    native = report["native"]
    print(
        f"\n{report['model_id']}@{report['revision'][:12]}  "
        f"native w={report['native_calibration']['centri_weight']} "
        f"tau={report['native_calibration']['threshold']:.3f}\n"
        f"  four-way   {native['four_way']['correct']}/{native['n']} "
        f"({native['four_way']['agreement']:.1%})\n"
        f"  three-way  {native['three_way']['correct']}/{native['n']} "
        f"({native['three_way']['agreement']:.1%})\n"
        f"  adequacy   {native['adequacy']['correct']}/{native['n']} "
        f"({native['adequacy']['agreement']:.1%})\n"
        f"  split-half held-out four-way "
        f"{report['split_half']['held_out_four_way']['correct']}/"
        f"{report['split_half']['held_out_four_way']['n']} "
        f"({report['split_half']['held_out_four_way']['agreement']:.1%})\n"
        f"  sanity     {report['sanity']['agreement']:.1%}\n"
        f"  direction-only (threshold-free) "
        f"{report['direction_only']['correct']}/{report['direction_only']['n']} "
        f"({report['direction_only']['accuracy']:.1%})\n"
        f"  oracle upper bound (fit on all 69) {report['oracle_upper_bound']['agreement']:.1%}\n"
        f"  majority baseline four-way {report['majority_baseline']['four_way']['agreement']:.1%} "
        f"/ three-way {report['majority_baseline']['three_way']['agreement']:.1%}\n"
        f"  bar (judge self-consistency) {JUDGE_GRADE_SELF_CONSISTENCY:.1%}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

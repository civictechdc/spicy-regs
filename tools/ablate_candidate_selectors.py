"""Ablate candidate-selector configurations against the frozen 35-item gold set.

The question this answers is narrow and mechanical: *which candidate channels,
fused how, put the right registry concept in front of a judge?* It computes the
top-12 candidate list every configuration would produce for each of the 35
stored gold assignments, and scores those lists on facts that need no model:

* **exact-alias targets.** Eight of the 35 gold labels normalize onto an alias
  of some concept in the fused registry (``docs/evidence/gold-adjudication-2026-07-27/README.md``,
  round 2). Those eight are the only items where a mechanically correct answer
  exists, so "did the target surface" is checkable without a judge. The set is
  recomputed here from the registry rather than hard-coded.
* **known-adequate concepts.** Five items were graded exact-or-close by the
  blind judges (``resolved.json``); their ``best_candidate_id`` is a concept a
  panel already accepted. Whether a configuration keeps it is a direct
  regression check on the graded round.
* **rank** of the surfaced targets, and the **scheme mix** of the emitted lists,
  which is what the quota rule exists to control.

Configurations are compositions of five channel sources:

* ``v1`` — ``select_candidate_concepts_for_text``, the production selector, run
  whole (it is a selector, not a channel: its own scheme gate and token trim
  apply).
* ``A`` / ``B`` — v2's anchored-lexical and char-3-gram channels.
* ``C`` — dense BGE retrieval over the concept index.
* ``D`` — free-keyword generate-then-map.

Every non-``v1`` configuration fuses its channels with the same RRF at k=60 that
v2 uses, then either applies v2's scheme quotas or takes the fused ranking
straight. Both fusion and quota steps are v2's own functions, imported rather
than reimplemented, so a configuration named ``v2`` here *is* v2; a test asserts
that against the public selector.

Nothing here adopts anything. It measures, prints a table, and stops.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.adapters.openai import PROMPT_INPUT_TOKEN_BUDGET
from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.ontology.candidate_channels import (
    CHANNEL_DEPTH,
    DENSE_CHANNEL_VERSION,
    KEYWORD_CHANNEL_VERSION,
    CharNgramConceptMapper,
    ConceptMapper,
    KeywordGeneration,
    dense_channel_ranking,
    generate_segment_keywords,
    keyword_channel_ranking,
)
from spicy_regs.ontology.common import read_parquet_rows
from spicy_regs.ontology.concepts import (
    ANCHOR_CHANNEL_DEPTH,
    ANCHOR_RRF_K,
    _anchored_channel,
    _apply_scheme_quotas,
    _char_ngram_channel,
    _condition_registry,
    _fuse_reciprocal_rank,
    _segment_term_weights,
    concept_aliases,
    normalize_label,
    select_candidate_concepts_for_text,
)
from spicy_regs.rulespec_testbed import GOLD_FILE, PROMPT_CONCEPT_LIMIT, load_testbed_inputs

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_DIR = REPO_ROOT / "output" / "segmentation-tagging-document-openai-structure-overlap-1800-v4"
DEFAULT_DATASET_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
DEFAULT_REGISTRY = REPO_ROOT / "output" / "fused-concept-registry-v1" / "registry.parquet"
DEFAULT_INDEX_DIR = REPO_ROOT / "output" / "fused-concept-registry-v1"
DEFAULT_RESOLVED = REPO_ROOT / "docs" / "evidence" / "gold-adjudication-2026-07-27" / "resolved.json"
SELECTION_FILE_NAME = "tagging_segments.parquet"

# ``load_testbed_inputs`` refuses to return when a tag prompt exceeds the
# provider budget. This harness makes no tag call, and a prompt that no longer
# fits must not be allowed to withhold the measurement, so the budget is lifted
# here and recorded in the emitted facts instead.
UNENFORCED_PROMPT_TOKEN_BUDGET = 1_000_000_000

CHANNEL_A = "A"
CHANNEL_B = "B"
CHANNEL_C = "C"
CHANNEL_D = "D"


@dataclass(frozen=True)
class Configuration:
    """One selector configuration: which channels, fused, and whether quotas apply."""

    name: str
    channels: tuple[str, ...]
    quotas: bool
    note: str


CONFIGURATIONS: tuple[Configuration, ...] = (
    Configuration("v1", (), False, "production selector, whole (scheme gate + token trim)"),
    Configuration("v2", (CHANNEL_A, CHANNEL_B), True, "anchored + char-ngram, RRF, scheme quotas"),
    Configuration("v2-noquota", (CHANNEL_A, CHANNEL_B), False, "v2 fused ranking, no quotas"),
    Configuration("v2+C", (CHANNEL_A, CHANNEL_B, CHANNEL_C), True, "v2 channels plus dense retrieval"),
    Configuration("v2+D", (CHANNEL_A, CHANNEL_B, CHANNEL_D), True, "v2 channels plus generate-then-map"),
    Configuration("v2+C+D", (CHANNEL_A, CHANNEL_B, CHANNEL_C, CHANNEL_D), True, "all four channels"),
    Configuration(
        "v2+C+D-noquota", (CHANNEL_A, CHANNEL_B, CHANNEL_C, CHANNEL_D), False, "all four channels, no quotas"
    ),
    Configuration("C-alone", (CHANNEL_C,), False, "dense retrieval only"),
    Configuration("D-alone", (CHANNEL_D,), False, "generate-then-map only"),
)
CONFIGURATIONS_BY_NAME = {configuration.name: configuration for configuration in CONFIGURATIONS}


class AblationError(RuntimeError):
    """The stored inputs cannot produce the requested ablation."""


# --------------------------------------------------------------------------
# gold items
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldItem:
    """One frozen gold assignment and the segments a selector may read for it."""

    gold_id: str
    item_id: str
    label: str
    scheme: str
    segment_ids: tuple[str, ...]
    # Concepts whose normalized aliases contain the normalized gold label.
    exact_alias_ids: tuple[str, ...]
    # The concept a blind judge panel graded exact-or-close, when there is one.
    adequate_concept_id: str | None


def _segment_text(unit: ExtractionUnit) -> str:
    """Rebuild the exact string the payload builder handed to the selector."""
    fields = unit.input.get("untrusted_evidence_fields", {}).get("fields", {})
    return "\n".join(str(value) for value in fields.values())


def _allowed_schemes(unit: ExtractionUnit) -> list[str]:
    return [str(scheme) for scheme in unit.input.get("subject", {}).get("allowed_schemes", ())]


def alias_index(concepts: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    """Normalized alias -> the concept ids carrying it, in registry order."""
    index: dict[str, list[str]] = {}
    for concept in concepts:
        concept_id = str(concept.get("concept_id") or "")
        for alias in concept_aliases(concept):
            index.setdefault(alias, []).append(concept_id)
    return index


def adequate_concepts(resolved: Mapping[str, Any]) -> dict[str, str]:
    """Item id -> the concept a judge panel graded exact or close."""
    result: dict[str, str] = {}
    for item in resolved.get("items", ()):
        if item.get("adequate_target") and item.get("best_candidate_id"):
            result[str(item.get("item_id"))] = str(item["best_candidate_id"])
    return result


def gold_items(
    *,
    answers: Mapping[str, Any],
    units_by_id: Mapping[str, ExtractionUnit],
    aliases: Mapping[str, list[str]],
    adequate: Mapping[str, str],
) -> list[GoldItem]:
    """Assemble the 35 measured items, each with its mechanical target set."""
    items: list[GoldItem] = []
    for answer in answers.get("artifacts", ()):
        for expected in answer.get("expected_tags", ()):
            gold_id = str(expected.get("gold_id") or "")
            item_id = f"gold-adjudication-{gold_id}"
            segment_ids = tuple(
                str(value) for value in expected.get("containing_segment_ids", ()) if str(value) in units_by_id
            )
            label = str(expected.get("label") or "")
            items.append(
                GoldItem(
                    gold_id=gold_id,
                    item_id=item_id,
                    label=label,
                    scheme=str(expected.get("scheme") or ""),
                    segment_ids=segment_ids,
                    exact_alias_ids=tuple(aliases.get(normalize_label(label), ())),
                    adequate_concept_id=adequate.get(item_id),
                )
            )
    items.sort(key=lambda item: item.item_id)
    return items


# --------------------------------------------------------------------------
# channel rankings
# --------------------------------------------------------------------------


@dataclass
class SegmentChannels:
    """Every channel's ranking for one segment, in v2 conditioning index space."""

    segment_id: str
    v1_ids: tuple[str, ...]
    rankings: dict[str, tuple[int, ...]]


def _ids_to_indices(concept_ids: Sequence[str], index_by_id: Mapping[str, int]) -> tuple[int, ...]:
    """Project a concept-id ranking into conditioning index space, order kept.

    A channel may name a concept the conditioning dropped (deprecated rows) or
    one from a different registry; those are skipped rather than guessed at.
    """
    seen: set[int] = set()
    projected: list[int] = []
    for concept_id in concept_ids:
        index = index_by_id.get(concept_id)
        if index is None or index in seen:
            continue
        seen.add(index)
        projected.append(index)
    return tuple(projected)


def segment_channels(
    *,
    unit: ExtractionUnit,
    registry_rows: Sequence[Mapping[str, Any]],
    conditioning: Any,
    index_by_id: Mapping[str, int],
    wanted: Sequence[str],
    dense_mapper: ConceptMapper | None,
    keywords: Sequence[str],
    limit: int,
    depth: int = ANCHOR_CHANNEL_DEPTH,
) -> SegmentChannels:
    """Compute one segment's rankings for every requested channel."""
    text = _segment_text(unit)
    requested = set(wanted)
    # An empty segment reaches every channel as an empty ranking, never as a
    # missing key: a configuration must still be computable over it.
    rankings: dict[str, tuple[int, ...]] = {name: () for name in requested}
    tokens = normalize_label(text).split()
    if {CHANNEL_A, CHANNEL_B} & requested and tokens:
        weights = _segment_term_weights(tokens, conditioning)
        if CHANNEL_A in requested:
            rankings[CHANNEL_A] = tuple(_anchored_channel(tokens, weights, conditioning, depth=depth))
        if CHANNEL_B in requested:
            rankings[CHANNEL_B] = tuple(_char_ngram_channel(weights, conditioning, depth=depth))
    if CHANNEL_C in wanted:
        if dense_mapper is None:
            raise AblationError("channel C was requested without a concept mapper")
        rankings[CHANNEL_C] = _ids_to_indices(
            dense_channel_ranking(text, mapper=dense_mapper, depth=depth), index_by_id
        )
    if CHANNEL_D in wanted:
        if dense_mapper is None:
            raise AblationError("channel D was requested without a concept mapper")
        rankings[CHANNEL_D] = _ids_to_indices(
            keyword_channel_ranking(keywords, mapper=dense_mapper, depth=depth), index_by_id
        )
    v1_selected = select_candidate_concepts_for_text(text, _allowed_schemes(unit), registry_rows, limit=limit)
    return SegmentChannels(
        segment_id=str(unit.unit_id),
        v1_ids=tuple(str(concept.get("concept_id") or "") for concept in v1_selected),
        rankings=rankings,
    )


def fuse(channels: Sequence[Sequence[int]], conditioning: Any) -> list[int]:
    """RRF at k=60 over the given channel rankings, best first, ties by id.

    ``_fuse_reciprocal_rank`` and the tie-break below are v2's, so a two-channel
    call here reproduces v2's fused ranking exactly.
    """
    fused = _fuse_reciprocal_rank([channel for channel in channels if channel])
    ordered = sorted(fused.items(), key=lambda item: (-item[1], conditioning.concept_ids[item[0]]))
    return [index for index, _ in ordered]


def configuration_ranking(
    configuration: Configuration,
    segment: SegmentChannels,
    conditioning: Any,
    *,
    limit: int,
) -> tuple[list[str], list[str]]:
    """One segment's ``(top-``limit`` ids, full fused ranking)`` for a configuration."""
    if configuration.name == "v1":
        return list(segment.v1_ids[:limit]), list(segment.v1_ids)
    ranked = fuse([segment.rankings.get(name, ()) for name in configuration.channels], conditioning)
    selected = _apply_scheme_quotas(ranked, conditioning, limit=limit) if configuration.quotas else ranked[:limit]
    return (
        [conditioning.concept_ids[index] for index in selected],
        [conditioning.concept_ids[index] for index in ranked],
    )


def merge_across_segments(per_segment: Sequence[Sequence[str]], *, limit: int) -> list[str]:
    """Union the segments' lists, best rank first — the adjudication builder's rule.

    A gold span contained by more than one selected segment gets the union of
    those segments' candidates rather than an arbitrary pick.
    """
    best: dict[str, int] = {}
    for candidates in per_segment:
        for rank, concept_id in enumerate(candidates, start=1):
            if concept_id not in best or rank < best[concept_id]:
                best[concept_id] = rank
    ordered = sorted(best.items(), key=lambda item: (item[1], item[0]))
    return [concept_id for concept_id, _ in ordered[:limit]]


# --------------------------------------------------------------------------
# measurement
# --------------------------------------------------------------------------


def _first_rank(candidates: Sequence[str], wanted: Sequence[str]) -> int | None:
    positions = [candidates.index(concept_id) + 1 for concept_id in wanted if concept_id in candidates]
    return min(positions) if positions else None


def measure_configuration(
    configuration: Configuration,
    *,
    items: Sequence[GoldItem],
    channels_by_segment: Mapping[str, SegmentChannels],
    conditioning: Any,
    scheme_by_id: Mapping[str, str],
    limit: int,
) -> dict[str, Any]:
    """Score one configuration over every gold item."""
    started = time.monotonic()
    cache: dict[str, tuple[list[str], list[str]]] = {}
    per_item: list[dict[str, Any]] = []
    scheme_mix: Counter[str] = Counter()
    for item in items:
        per_segment: list[list[str]] = []
        full: list[list[str]] = []
        for segment_id in item.segment_ids:
            if segment_id not in cache:
                cache[segment_id] = configuration_ranking(
                    configuration, channels_by_segment[segment_id], conditioning, limit=limit
                )
            selected, ranking = cache[segment_id]
            per_segment.append(selected)
            full.append(ranking)
        candidates = merge_across_segments(per_segment, limit=limit)
        scheme_mix.update(scheme_by_id.get(concept_id, "") for concept_id in candidates)
        fused_ranks = [
            rank for rank in (_first_rank(ranking, item.exact_alias_ids) for ranking in full) if rank is not None
        ]
        per_item.append(
            {
                "item_id": item.item_id,
                "label": item.label,
                "candidates": candidates,
                "exact_alias_target": bool(item.exact_alias_ids),
                "exact_alias_rank": _first_rank(candidates, item.exact_alias_ids),
                "exact_alias_fused_rank": min(fused_ranks) if fused_ranks else None,
                "adequate_target": item.adequate_concept_id,
                "adequate_rank": (
                    _first_rank(candidates, [item.adequate_concept_id]) if item.adequate_concept_id else None
                ),
            }
        )

    targets = [row for row in per_item if row["exact_alias_target"]]
    surfaced = [row for row in targets if row["exact_alias_rank"] is not None]
    adequate = [row for row in per_item if row["adequate_target"]]
    adequate_kept = [row for row in adequate if row["adequate_rank"] is not None]
    ranks = [int(row["exact_alias_rank"]) for row in surfaced]
    total = sum(scheme_mix.values())
    return {
        "configuration": configuration.name,
        "channels": list(configuration.channels) or ["v1"],
        "quotas": configuration.quotas,
        "note": configuration.note,
        "item_count": len(per_item),
        "exact_alias_target_count": len(targets),
        "exact_alias_surfaced": len(surfaced),
        "exact_alias_surfaced_labels": sorted(str(row["label"]) for row in surfaced),
        "exact_alias_missed_labels": sorted(str(row["label"]) for row in targets if row["exact_alias_rank"] is None),
        "adequate_target_count": len(adequate),
        "adequate_kept": len(adequate_kept),
        "adequate_kept_labels": sorted(str(row["label"]) for row in adequate_kept),
        "surfaced_rank_mean": round(statistics.mean(ranks), 2) if ranks else None,
        "surfaced_rank_median": round(statistics.median(ranks), 2) if ranks else None,
        "candidate_slots": total,
        "scheme_mix": dict(sorted(scheme_mix.items(), key=lambda entry: (-entry[1], entry[0]))),
        "seconds": round(time.monotonic() - started, 3),
        "items": per_item,
    }


def _scheme_share(scheme_mix: Mapping[str, int], slots: int) -> str:
    if not slots:
        return "—"
    parts = [f"{scheme or '∅'} {count * 100 // slots}%" for scheme, count in list(scheme_mix.items())[:4]]
    return ", ".join(parts)


def markdown_table(results: Sequence[Mapping[str, Any]]) -> str:
    """Render the ablation as one markdown table plus the per-target detail."""
    header = (
        "| Configuration | Channels | Quotas | Exact-alias surfaced | Adequate kept | "
        "Mean rank | Median rank | Scheme mix (top-12 slots) |\n"
        "| --- | --- | :---: | ---: | ---: | ---: | ---: | --- |"
    )
    lines = [header]
    for result in results:
        lines.append(
            "| {name} | {channels} | {quotas} | {surfaced}/{targets} | {kept}/{adequate} | "
            "{mean} | {median} | {mix} |".format(
                name=result["configuration"],
                channels="+".join(result["channels"]),
                quotas="yes" if result["quotas"] else "no",
                surfaced=result["exact_alias_surfaced"],
                targets=result["exact_alias_target_count"],
                kept=result["adequate_kept"],
                adequate=result["adequate_target_count"],
                mean=result["surfaced_rank_mean"] if result["surfaced_rank_mean"] is not None else "—",
                median=result["surfaced_rank_median"] if result["surfaced_rank_median"] is not None else "—",
                mix=_scheme_share(result["scheme_mix"], int(result["candidate_slots"])),
            )
        )
    lines.append("")
    lines.append("| Configuration | Exact-alias targets surfaced | Missed |")
    lines.append("| --- | --- | --- |")
    for result in results:
        lines.append(
            "| {name} | {surfaced} | {missed} |".format(
                name=result["configuration"],
                surfaced=", ".join(result["exact_alias_surfaced_labels"]) or "—",
                missed=", ".join(result["exact_alias_missed_labels"]) or "—",
            )
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# channel D keywords
# --------------------------------------------------------------------------


def load_keywords(path: Path) -> dict[str, tuple[str, ...]]:
    """Read stored per-segment keywords so a rerun needs no provider call."""
    stored = json.loads(Path(path).read_text())
    return {
        str(segment_id): tuple(str(keyword) for keyword in keywords)
        for segment_id, keywords in stored.get("keywords_by_segment", {}).items()
    }


def generate_keywords(
    *,
    units_by_id: Mapping[str, ExtractionUnit],
    segment_ids: Sequence[str],
    model: Any,
    record_dir: Path,
) -> tuple[dict[str, tuple[str, ...]], list[dict[str, Any]]]:
    """One keyword call per segment, each request and response stored on disk."""
    record_dir = Path(record_dir)
    record_dir.mkdir(parents=True, exist_ok=True)
    keywords_by_segment: dict[str, tuple[str, ...]] = {}
    calls: list[dict[str, Any]] = []
    for ordinal, segment_id in enumerate(sorted(set(segment_ids)), start=1):
        unit = units_by_id[segment_id]
        started = time.monotonic()
        generation: KeywordGeneration = generate_segment_keywords(_segment_text(unit), model=model)
        keywords_by_segment[segment_id] = generation.keywords
        record = {
            "segment_id": segment_id,
            "ordinal": ordinal,
            "keywords": list(generation.keywords),
            "request": generation.request,
            "response": generation.output,
            "call": generation.call,
        }
        (record_dir / f"{segment_id}.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        calls.append(
            {
                "segment_id": segment_id,
                "keyword_count": len(generation.keywords),
                "seconds": round(time.monotonic() - started, 3),
                "status": generation.call.get("status"),
            }
        )
    return keywords_by_segment, calls


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


def run_ablation(
    *,
    dataset_dir: Path,
    selection_file: Path,
    registry_file: Path,
    resolved_file: Path,
    index_dir: Path,
    output_dir: Path,
    configuration_names: Sequence[str],
    limit: int = PROMPT_CONCEPT_LIMIT,
    generate: bool = False,
    keywords_file: Path | None = None,
    fallback_mapper: bool = False,
) -> dict[str, Any]:
    """Load the frozen inputs, run every configuration, and write the record."""
    unknown = [name for name in configuration_names if name not in CONFIGURATIONS_BY_NAME]
    if unknown:
        raise AblationError(f"unknown configurations: {sorted(unknown)}")
    configurations = [CONFIGURATIONS_BY_NAME[name] for name in configuration_names]
    wanted = sorted({channel for configuration in configurations for channel in configuration.channels})

    timings: dict[str, float] = {}
    started = time.monotonic()
    inputs = load_testbed_inputs(
        dataset_dir,
        selection_file,
        registry_file,
        prompt_input_token_budget=UNENFORCED_PROMPT_TOKEN_BUDGET,
    )
    registry_rows = read_parquet_rows(registry_file)
    timings["load_inputs"] = round(time.monotonic() - started, 3)

    units_by_id = {unit.unit_id: unit for unit in inputs.units}
    resolved = json.loads(Path(resolved_file).read_text())
    items = gold_items(
        answers=inputs.answers,
        units_by_id=units_by_id,
        aliases=alias_index(registry_rows),
        adequate=adequate_concepts(resolved),
    )
    segment_ids = sorted({segment_id for item in items for segment_id in item.segment_ids})

    started = time.monotonic()
    conditioning = _condition_registry(registry_rows)
    timings["condition_registry"] = round(time.monotonic() - started, 3)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}
    scheme_by_id = {
        concept_id: conditioning.schemes[index] for index, concept_id in enumerate(conditioning.concept_ids)
    }

    # Keywords come before the index: a provider failure should surface in
    # seconds rather than after a half-million-row embedding build.
    keywords_by_segment: dict[str, tuple[str, ...]] = {}
    keyword_calls: list[dict[str, Any]] = []
    keyword_facts: dict[str, Any] = {"generated": False, "call_count": 0}
    if CHANNEL_D in wanted:
        started = time.monotonic()
        keywords_by_segment, keyword_calls, keyword_facts = _resolve_keywords(
            units_by_id=units_by_id,
            segment_ids=segment_ids,
            generate=generate,
            keywords_file=keywords_file,
            output_dir=output_dir,
        )
        timings["keywords"] = round(time.monotonic() - started, 3)

    mapper: ConceptMapper | None = None
    mapper_facts: dict[str, Any] = {}
    if {CHANNEL_C, CHANNEL_D} & set(wanted):
        started = time.monotonic()
        mapper, mapper_facts = _build_mapper(
            registry_rows, index_dir=index_dir, fallback_mapper=fallback_mapper, wanted=wanted
        )
        timings["concept_mapper"] = round(time.monotonic() - started, 3)
        mapper_facts.update(
            _query_token_facts(mapper, [_segment_text(units_by_id[segment_id]) for segment_id in segment_ids])
        )

    started = time.monotonic()
    channels_by_segment = {
        segment_id: segment_channels(
            unit=units_by_id[segment_id],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id=index_by_id,
            wanted=wanted,
            dense_mapper=mapper,
            keywords=keywords_by_segment.get(segment_id, ()),
            limit=limit,
        )
        for segment_id in segment_ids
    }
    timings["channel_rankings"] = round(time.monotonic() - started, 3)

    started = time.monotonic()
    results = [
        measure_configuration(
            configuration,
            items=items,
            channels_by_segment=channels_by_segment,
            conditioning=conditioning,
            scheme_by_id=scheme_by_id,
            limit=limit,
        )
        for configuration in configurations
    ]
    timings["measure"] = round(time.monotonic() - started, 3)

    return {
        "schema_version": "candidate-selector-ablation-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": {
            "dataset_dir": str(dataset_dir),
            "selection_file": str(selection_file),
            "registry_file": str(registry_file),
            "registry_sha256": sha256_file(registry_file),
            "registry_row_count": len(registry_rows),
            "eligible_concept_count": len(conditioning.concept_ids),
            "gold_file": str(Path(dataset_dir) / GOLD_FILE),
            "gold_sha256": sha256_file(Path(dataset_dir) / GOLD_FILE),
            "resolved_file": str(resolved_file),
            "resolved_sha256": sha256_file(Path(resolved_file)),
            "prompt_input_token_budget": PROMPT_INPUT_TOKEN_BUDGET,
        },
        "settings": {
            "limit": limit,
            "channel_depth": CHANNEL_DEPTH,
            "rrf_k": ANCHOR_RRF_K,
            "dense_channel_version": DENSE_CHANNEL_VERSION,
            "keyword_channel_version": KEYWORD_CHANNEL_VERSION,
        },
        "item_count": len(items),
        "segment_count": len(segment_ids),
        "exact_alias_targets": [
            {"item_id": item.item_id, "label": item.label, "concept_ids": list(item.exact_alias_ids)}
            for item in items
            if item.exact_alias_ids
        ],
        "adequate_targets": [
            {"item_id": item.item_id, "label": item.label, "concept_id": item.adequate_concept_id}
            for item in items
            if item.adequate_concept_id
        ],
        "concept_mapper": mapper_facts,
        "keywords": {**keyword_facts, "calls": keyword_calls},
        "timings_seconds": timings,
        "results": results,
    }


def _build_mapper(
    registry_rows: Sequence[Mapping[str, Any]],
    *,
    index_dir: Path,
    fallback_mapper: bool,
    wanted: Sequence[str],
) -> tuple[ConceptMapper, dict[str, Any]]:
    """Load the dense index, or fall back to the char-ngram space on request."""
    if fallback_mapper:
        mapper = CharNgramConceptMapper(registry_rows)
        return mapper, {"kind": "char-ngram-fallback", "version": mapper.version}
    from sentence_transformers import SentenceTransformer

    from spicy_regs.docpipeline.adapters.sentence_transformers import (
        DEFAULT_DENSE_MODEL,
        DEFAULT_DENSE_REVISION,
        SentenceTransformersDenseEmbedder,
    )
    from spicy_regs.ontology.candidate_channels import (
        BulkSentenceEncoderEmbedder,
        DenseConceptMapper,
        ensure_dense_concept_index,
    )

    # The encoder is loaded once at the pinned model and revision, then handed
    # to the adapter, which is what validates the pinned package version and the
    # declared dimensions. Queries run through the adapter; only the half-million
    # row index build takes the bulk path around its per-text token audit.
    encoder = SentenceTransformer(DEFAULT_DENSE_MODEL, revision=DEFAULT_DENSE_REVISION)
    embedder = SentenceTransformersDenseEmbedder(encoder=encoder)
    bulk = BulkSentenceEncoderEmbedder(encoder=encoder, model_id=embedder.model_id, dimensions=embedder.dimensions)
    index, facts = ensure_dense_concept_index(
        registry_rows,
        embedder=bulk,
        directory=Path(index_dir),
        on_progress=_index_progress(time.monotonic()),
    )
    return DenseConceptMapper(index=index, embedder=embedder), {
        "kind": "dense",
        "channels": list(wanted),
        "model": DEFAULT_DENSE_MODEL,
        "revision": DEFAULT_DENSE_REVISION,
        "device": embedder.device_label,
        **facts,
    }


def _query_token_facts(mapper: Any, texts: Sequence[str]) -> dict[str, Any]:
    """Record how many segment queries the embedder has to truncate.

    Channel C's query is the whole segment, and a 1,800-token segment does not
    fit a 512-token encoder. That is a real limit on what the channel can see,
    so it is measured and reported rather than assumed away.
    """
    embedder = getattr(mapper, "embedder", None)
    counter = getattr(embedder, "model_token_count", None)
    ceiling = getattr(embedder, "max_input_tokens", None)
    if counter is None or ceiling is None:
        return {}
    counts = [counter(text) or 0 for text in texts]
    return {
        "query_max_input_tokens": int(ceiling),
        "query_token_max": max(counts, default=0),
        "queries_truncated": sum(1 for count in counts if count > int(ceiling)),
        "query_count": len(counts),
    }


def _index_progress(started: float) -> Any:
    """Report index-build progress on stderr; a 513k build is not instant."""

    def report(done: int, total: int) -> None:
        elapsed = time.monotonic() - started
        rate = done / elapsed if elapsed else 0.0
        remaining = (total - done) / rate / 60 if rate else 0.0
        print(
            f"  dense index {done}/{total} ({done / total:.1%}) "
            f"elapsed={elapsed:.0f}s rate={rate:.0f}/s eta={remaining:.1f}min",
            file=sys.stderr,
            flush=True,
        )

    return report


def _resolve_keywords(
    *,
    units_by_id: Mapping[str, ExtractionUnit],
    segment_ids: Sequence[str],
    generate: bool,
    keywords_file: Path | None,
    output_dir: Path,
) -> tuple[dict[str, tuple[str, ...]], list[dict[str, Any]], dict[str, Any]]:
    """Generate this run's keywords, or read a stored set."""
    if generate:
        from spicy_regs.docpipeline.adapters.openai import OpenAIStructuredTextModel

        model = OpenAIStructuredTextModel.from_environment()
        if model is None:
            raise AblationError("channel D was requested with --generate-keywords but OPENAI_API_KEY is unset")
        record_dir = Path(output_dir) / "keyword-calls"
        keywords_by_segment, calls = generate_keywords(
            units_by_id=units_by_id, segment_ids=segment_ids, model=model, record_dir=record_dir
        )
        stored = {
            "model_id": model.model_id,
            "instructions_version": KEYWORD_CHANNEL_VERSION,
            "keywords_by_segment": {key: list(value) for key, value in sorted(keywords_by_segment.items())},
        }
        keywords_path = Path(output_dir) / "keywords.json"
        keywords_path.parent.mkdir(parents=True, exist_ok=True)
        keywords_path.write_text(json.dumps(stored, indent=2, sort_keys=True) + "\n")
        return (
            keywords_by_segment,
            calls,
            {
                "generated": True,
                "model_id": model.model_id,
                "call_count": len(calls),
                "record_dir": str(record_dir),
                "keywords_file": str(keywords_path),
            },
        )
    if keywords_file is None:
        raise AblationError("channel D needs either --generate-keywords or --keywords <file>")
    keywords_by_segment = load_keywords(Path(keywords_file))
    missing = [segment_id for segment_id in segment_ids if segment_id not in keywords_by_segment]
    if missing:
        raise AblationError(f"stored keywords are missing {len(missing)} segments")
    return (
        keywords_by_segment,
        [],
        {"generated": False, "call_count": 0, "keywords_file": str(keywords_file)},
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for the JSON record and table.")
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--selection-file", type=Path, default=None)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--resolved", type=Path, default=DEFAULT_RESOLVED)
    parser.add_argument("--index-dir", type=Path, default=DEFAULT_INDEX_DIR)
    parser.add_argument("--limit", type=int, default=PROMPT_CONCEPT_LIMIT)
    parser.add_argument(
        "--configurations",
        nargs="+",
        default=[configuration.name for configuration in CONFIGURATIONS],
        help="Subset of configurations to measure.",
    )
    parser.add_argument(
        "--generate-keywords",
        action="store_true",
        help="Make one channel-D provider call per segment and store every request and response.",
    )
    parser.add_argument("--keywords", type=Path, default=None, help="Stored keywords JSON from an earlier run.")
    parser.add_argument(
        "--fallback-mapper",
        action="store_true",
        help="Map through the char-3-gram space instead of the dense index (channel C unavailable).",
    )
    args = parser.parse_args(argv)

    document = run_ablation(
        dataset_dir=args.dataset_dir,
        selection_file=args.selection_file or (args.run_dir / SELECTION_FILE_NAME),
        registry_file=args.registry,
        resolved_file=args.resolved,
        index_dir=args.index_dir,
        output_dir=args.output_dir,
        configuration_names=args.configurations,
        limit=args.limit,
        generate=args.generate_keywords,
        keywords_file=args.keywords,
        fallback_mapper=args.fallback_mapper,
    )
    table = markdown_table(document["results"])
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "ablation.json").write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    (output_dir / "ablation.md").write_text(table + "\n")
    print(table)
    print()
    print(json.dumps({"timings_seconds": document["timings_seconds"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

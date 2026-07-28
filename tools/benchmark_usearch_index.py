"""Benchmark a USearch ANN index against the exact dense concept search.

The question is operational, not semantic: *can the dense concept channel be
served from a memory-mapped approximate index instead of a 1.64 GB in-memory
float matrix, without losing the concepts that matter?*

The incumbent (``DenseConceptMapper``) is an **exact** brute-force cosine over
all 513,236 concept vectors. That makes this a rare, cheap comparison: the
baseline is ground truth by construction, so an approximate index's quality is
*measured* as recall against it rather than argued about.

What this script measures, per configuration:

* **build time** from the already-cached vectors, and **on-disk size**;
* **query latency** over the 35 frozen development segments' embeddings,
  mean and p95, search only — the query embedding is precomputed and shared, so
  the encoder's cost does not hide the difference under test;
* **peak resident memory** at query time, measured in a fresh subprocess for
  memory-mapped and fully-loaded loading separately, because peak RSS is a
  property of a process and cannot be read honestly from inside a process that
  already loaded the baseline;
* **recall@50 and recall@12** against the exact search's own output; and
* **the oracle**: how many of the eight exact-alias development targets still
  reach the top 12, computed through ``tools/ablate_candidate_selectors.py``'s
  own measurement functions rather than a second scoring implementation.

Nothing here re-embeds anything. Both mappers are driven by the *same*
precomputed query vectors through their own real ``rank`` implementations, so
the only variable is the search.

This is development-only evidence. The 35 items are permanently
train/development data (``docs/experiment-strategy.md``); the drawn holdout is
untouched by this script. No configuration is adopted here.

Usage::

    uv run --extra ann --extra embed python tools/benchmark_usearch_index.py \
        --output-dir <dir> --work-dir <dir>

The first run loads the frozen testbed inputs (about seven minutes) and embeds
the 35 segments once, then writes a setup cache; later runs reuse it and need
neither the encoder nor the testbed loader.
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import statistics
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "tools"))

from ablate_candidate_selectors import (  # noqa: E402
    CHANNEL_A,
    CHANNEL_B,
    CHANNEL_C,
    CHANNEL_E,
    CONFIGURATIONS_BY_NAME,
    DEFAULT_DATASET_DIR,
    DEFAULT_INDEX_DIR,
    DEFAULT_REGISTRY,
    DEFAULT_RESOLVED,
    DEFAULT_RUN_DIR,
    SELECTION_FILE_NAME,
    SegmentChannels,
    _segment_text,
    adequate_concepts,
    alias_index,
    gold_items,
    measure_configuration,
    segment_channels,
)
from spicy_regs.docpipeline.runtime import sha256_file  # noqa: E402
from spicy_regs.ontology.ann_index import (  # noqa: E402
    ANN_CHANNEL_VERSION,
    DEFAULT_CONNECTIVITY,
    DEFAULT_EXPANSION_ADD,
    DEFAULT_EXPANSION_SEARCH,
    USEARCH_VERSION,
    UsearchConceptMapper,
    ann_index_path,
    build_ann_concept_index,
    load_ann_concept_index,
    recall_against_exact,
    save_ann_concept_index,
    sidecar_path,
)
from spicy_regs.ontology.candidate_channels import (  # noqa: E402
    DENSE_CHANNEL_VERSION,
    BM25ConceptMapper,
    DenseConceptMapper,
    dense_channel_ranking,
    load_dense_concept_index,
)
from spicy_regs.ontology.common import read_parquet_rows  # noqa: E402
from spicy_regs.ontology.concepts import _condition_registry  # noqa: E402
from spicy_regs.rulespec_testbed import PROMPT_CONCEPT_LIMIT  # noqa: E402

SETUP_SCHEMA_VERSION = "usearch-benchmark-setup-v1"
BENCHMARK_SCHEMA_VERSION = "usearch-ann-benchmark-v1"

# recall@50 is the candidate-pool measure the retrieve-then-rerank literature
# reports; recall@12 is this repo's actual prompt shortlist length, so it is the
# depth at which a loss can reach a tagging decision.
RECALL_DEPTHS: tuple[int, ...] = (50, PROMPT_CONCEPT_LIMIT)
LATENCY_DEPTH = max(RECALL_DEPTHS)

# Fused configurations that contain channel C, plus C on its own. C-alone
# isolates the variable; the fused rows say whether an isolated loss survives
# fusion, which is the only form in which the channel would ever be served.
ORACLE_CONFIGURATIONS: tuple[str, ...] = ("C-alone", "v2+C", "BM25+B+C")


@dataclass(frozen=True)
class AnnConfiguration:
    """One measured serving configuration."""

    name: str
    quantization: str
    connectivity: int
    expansion_add: int
    expansion_search: int
    # When set, reuse the named configuration's stored graph and only change the
    # query-time search width. Rebuilding for an ``expansion_search`` sweep would
    # measure nothing new: it is a query parameter, not a graph parameter.
    reuse_graph_of: str | None = None

    @property
    def note(self) -> str:
        reuse = f", graph reused from {self.reuse_graph_of}" if self.reuse_graph_of else ""
        return (
            f"{self.quantization} storage, connectivity={self.connectivity}, "
            f"expansion_add={self.expansion_add}, expansion_search={self.expansion_search}{reuse}"
        )


ANN_CONFIGURATIONS: tuple[AnnConfiguration, ...] = (
    AnnConfiguration("usearch-f32", "f32", DEFAULT_CONNECTIVITY, DEFAULT_EXPANSION_ADD, DEFAULT_EXPANSION_SEARCH),
    AnnConfiguration("usearch-f16", "f16", DEFAULT_CONNECTIVITY, DEFAULT_EXPANSION_ADD, DEFAULT_EXPANSION_SEARCH),
    AnnConfiguration("usearch-i8", "i8", DEFAULT_CONNECTIVITY, DEFAULT_EXPANSION_ADD, DEFAULT_EXPANSION_SEARCH),
    AnnConfiguration(
        "usearch-f16-ef256",
        "f16",
        DEFAULT_CONNECTIVITY,
        DEFAULT_EXPANSION_ADD,
        256,
        reuse_graph_of="usearch-f16",
    ),
    AnnConfiguration("usearch-f16-c32", "f16", 32, 256, 128),
    # The recall ceiling probe. If a deliberately expensive graph — widest
    # connectivity, 4x the build effort, 8x the search width — still cannot
    # recover the exact top-50, then the shortfall belongs to the embedding
    # neighbourhood's flatness and not to a parameter left untuned.
    AnnConfiguration("usearch-f32-hi", "f32", 48, 512, 512),
    # The same high-effort graph at half the vector bytes. This is the only
    # point that could pay for itself: high effort is what preserves recall,
    # and f16 is what makes the pages it touches cheaper.
    AnnConfiguration("usearch-f16-hi", "f16", 48, 512, 512),
)
ANN_CONFIGURATIONS_BY_NAME = {configuration.name: configuration for configuration in ANN_CONFIGURATIONS}


class BenchmarkError(RuntimeError):
    """The stored inputs cannot produce the requested benchmark."""


# --------------------------------------------------------------------------
# precomputed query vectors
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class _PrecomputedVectors:
    """The one field a concept mapper reads off an embed result."""

    vectors: Any


@dataclass(frozen=True)
class PrecomputedQueryEmbedder:
    """Serve already-computed query vectors, keyed by the exact query string.

    Both mappers embed their queries through ``self.embedder.embed(...)``. Giving
    them this instead of the encoder does three things at once: the 35 segments
    are embedded once rather than once per configuration, both arms are compared
    on *identical* query vectors so no encoder nondeterminism can be mistaken
    for an ANN effect, and the measured latency is the search's alone.

    An unknown query is an error, never a silent zero vector: a benchmark that
    quietly searches the origin would report excellent, meaningless recall.
    """

    model_id: str
    dimensions: int
    vectors_by_text: Mapping[str, Any]

    def embed(self, texts: Sequence[str]) -> _PrecomputedVectors:
        import numpy

        rows = []
        for text in texts:
            vector = self.vectors_by_text.get(str(text))
            if vector is None:
                raise BenchmarkError("a query reached the precomputed embedder without a stored vector")
            rows.append(vector)
        return _PrecomputedVectors(vectors=numpy.asarray(rows, dtype=numpy.float32))


# --------------------------------------------------------------------------
# setup cache
# --------------------------------------------------------------------------


def build_setup(
    *,
    dataset_dir: Path,
    selection_file: Path,
    registry_file: Path,
    resolved_file: Path,
) -> dict[str, Any]:
    """Load the frozen inputs once and embed the 35 segments through the pinned adapter."""
    from sentence_transformers import SentenceTransformer

    from spicy_regs.docpipeline.adapters.sentence_transformers import (
        DEFAULT_DENSE_MODEL,
        DEFAULT_DENSE_REVISION,
        SentenceTransformersDenseEmbedder,
    )
    from spicy_regs.evaluation_boundary import DEFAULT_BOUNDARY_MANIFEST, DEVELOPMENT_DATASET_ID
    from spicy_regs.rulespec_testbed import load_testbed_inputs

    started = time.monotonic()
    inputs = load_testbed_inputs(
        dataset_dir,
        selection_file,
        registry_file,
        evaluation_manifest=DEFAULT_BOUNDARY_MANIFEST,
        evaluation_dataset_id=DEVELOPMENT_DATASET_ID,
    )
    load_seconds = time.monotonic() - started
    registry_rows = read_parquet_rows(registry_file)
    units_by_id = {unit.unit_id: unit for unit in inputs.units}
    resolved = json.loads(Path(resolved_file).read_text())
    items = gold_items(
        answers=inputs.answers,
        units_by_id=units_by_id,
        aliases=alias_index(registry_rows),
        adequate=adequate_concepts(resolved),
    )
    segment_ids = sorted({segment_id for item in items for segment_id in item.segment_ids})

    encoder = SentenceTransformer(DEFAULT_DENSE_MODEL, revision=DEFAULT_DENSE_REVISION)
    embedder = SentenceTransformersDenseEmbedder(encoder=encoder)
    texts = [_segment_text(units_by_id[segment_id]) for segment_id in segment_ids]
    embed_started = time.monotonic()
    vectors = embedder.embed(texts).vectors
    embed_seconds = time.monotonic() - embed_started

    return {
        "schema_version": SETUP_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model_id": embedder.model_id,
        "model": DEFAULT_DENSE_MODEL,
        "revision": DEFAULT_DENSE_REVISION,
        "dimensions": int(embedder.dimensions),
        "evaluation_boundary": inputs.evaluation_facts,
        "load_seconds": round(load_seconds, 3),
        "embed_seconds": round(embed_seconds, 3),
        "items": [
            {
                "gold_id": item.gold_id,
                "item_id": item.item_id,
                "label": item.label,
                "scheme": item.scheme,
                "segment_ids": list(item.segment_ids),
                "exact_alias_ids": list(item.exact_alias_ids),
                "adequate_concept_id": item.adequate_concept_id,
            }
            for item in items
        ],
        "segments": [
            {
                "segment_id": segment_id,
                "unit_input": units_by_id[segment_id].input,
                "query_vector": [float(value) for value in vectors[position]],
            }
            for position, segment_id in enumerate(segment_ids)
        ],
    }


def load_setup(path: Path) -> dict[str, Any]:
    stored = json.loads(Path(path).read_text())
    if stored.get("schema_version") != SETUP_SCHEMA_VERSION:
        raise BenchmarkError(f"setup cache schema {stored.get('schema_version')!r} is not readable")
    return stored


def setup_gold_items(setup: Mapping[str, Any]) -> list[Any]:
    """Rebuild the harness's ``GoldItem`` rows from the cache, unchanged in meaning."""
    from ablate_candidate_selectors import GoldItem

    return [
        GoldItem(
            gold_id=str(row["gold_id"]),
            item_id=str(row["item_id"]),
            label=str(row["label"]),
            scheme=str(row["scheme"]),
            segment_ids=tuple(str(value) for value in row["segment_ids"]),
            exact_alias_ids=tuple(str(value) for value in row["exact_alias_ids"]),
            adequate_concept_id=row["adequate_concept_id"],
        )
        for row in setup.get("items", ())
    ]


def setup_units(setup: Mapping[str, Any]) -> dict[str, Any]:
    """Stand-in extraction units carrying the two fields the harness reads."""
    return {
        str(row["segment_id"]): SimpleNamespace(unit_id=str(row["segment_id"]), input=row["unit_input"])
        for row in setup.get("segments", ())
    }


def setup_embedder(setup: Mapping[str, Any]) -> tuple[PrecomputedQueryEmbedder, dict[str, str]]:
    """The shared precomputed embedder, plus each segment's exact query text."""
    import numpy

    texts_by_segment: dict[str, str] = {}
    vectors_by_text: dict[str, Any] = {}
    for row in setup.get("segments", ()):
        unit = SimpleNamespace(unit_id=str(row["segment_id"]), input=row["unit_input"])
        text = _segment_text(unit)
        texts_by_segment[str(row["segment_id"])] = text
        vectors_by_text[text] = numpy.asarray(row["query_vector"], dtype=numpy.float32)
    return (
        PrecomputedQueryEmbedder(
            model_id=str(setup["model_id"]),
            dimensions=int(setup["dimensions"]),
            vectors_by_text=vectors_by_text,
        ),
        texts_by_segment,
    )


# --------------------------------------------------------------------------
# measurement
# --------------------------------------------------------------------------


def rank_all(mapper: Any, texts: Sequence[str], *, depth: int) -> list[list[str]]:
    """Every query's ranking through the mapper's own channel entry point."""
    return [dense_channel_ranking(text, mapper=mapper, depth=depth) for text in texts]


def measure_latency(mapper: Any, texts: Sequence[str], *, depth: int, repeats: int) -> dict[str, Any]:
    """Per-query wall time for the search, one query at a time as production would.

    A warm-up pass runs first and is discarded: the first query against a
    memory-mapped index pays page faults that no later query pays, and reporting
    that as the mean would flatter the in-memory arm for the wrong reason. The
    cold first-query cost is reported separately instead of being hidden.
    """
    cold_started = time.perf_counter()
    dense_channel_ranking(texts[0], mapper=mapper, depth=depth)
    cold_seconds = time.perf_counter() - cold_started
    for text in texts[1:]:
        dense_channel_ranking(text, mapper=mapper, depth=depth)

    samples: list[float] = []
    for _ in range(max(1, repeats)):
        for text in texts:
            started = time.perf_counter()
            dense_channel_ranking(text, mapper=mapper, depth=depth)
            samples.append((time.perf_counter() - started) * 1000.0)
    samples.sort()
    return {
        "depth": int(depth),
        "sample_count": len(samples),
        "cold_first_query_ms": round(cold_seconds * 1000.0, 3),
        "mean_ms": round(statistics.mean(samples), 3),
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": round(samples[min(len(samples) - 1, int(round(0.95 * (len(samples) - 1))))], 3),
        "max_ms": round(samples[-1], 3),
    }


def oracle_measurements(
    *,
    mapper: Any,
    base_channels: Mapping[str, SegmentChannels],
    items: Sequence[Any],
    units_by_id: Mapping[str, Any],
    registry_rows: Sequence[Mapping[str, Any]],
    conditioning: Any,
    index_by_id: Mapping[str, int],
    source_vocabulary_by_id: Mapping[str, str],
    limit: int,
) -> dict[str, Any]:
    """Run the harness's own scorer over the configurations that contain channel C.

    Channels A, B and E do not depend on the dense mapper, so they are computed
    once and merged in here. Only channel C is recomputed per mapper — which is
    the single variable this experiment changes.
    """
    merged: dict[str, SegmentChannels] = {}
    for segment_id, base in base_channels.items():
        dense_only = segment_channels(
            unit=units_by_id[segment_id],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id=index_by_id,
            wanted=(CHANNEL_C,),
            dense_mapper=mapper,
            bm25_mapper=None,
            keywords=(),
            limit=limit,
            include_v1=False,
        )
        merged[segment_id] = SegmentChannels(
            segment_id=base.segment_id,
            allowed_facets=base.allowed_facets,
            unit_input=base.unit_input,
            v1_ids=base.v1_ids,
            rankings={**base.rankings, **dense_only.rankings},
        )

    results: dict[str, Any] = {}
    for name in ORACLE_CONFIGURATIONS:
        measured = measure_configuration(
            CONFIGURATIONS_BY_NAME[name],
            items=items,
            channels_by_segment=merged,
            conditioning=conditioning,
            source_vocabulary_by_id=source_vocabulary_by_id,
            limit=limit,
        )
        results[name] = {
            "exact_alias_target_count": measured["exact_alias_target_count"],
            "exact_alias_surfaced": measured["exact_alias_surfaced"],
            "exact_alias_surfaced_labels": measured["exact_alias_surfaced_labels"],
            "exact_alias_missed_labels": measured["exact_alias_missed_labels"],
            "adequate_target_count": measured["adequate_target_count"],
            "adequate_kept": measured["adequate_kept"],
            "surfaced_rank_mean": measured["surfaced_rank_mean"],
            "surfaced_rank_median": measured["surfaced_rank_median"],
            "evaluation_scope": "development_only",
            "accuracy_verdict_eligible": False,
        }
    return results


# --------------------------------------------------------------------------
# peak resident memory, measured in a fresh process
# --------------------------------------------------------------------------


def _maxrss_bytes() -> int:
    """Peak RSS of this process, in bytes on both Linux and macOS."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak) if sys.platform == "darwin" else int(peak) * 1024


def run_rss_probe(
    *,
    arm: str,
    dense_index_file: Path,
    ann_index_file: Path | None,
    setup_file: Path,
    depth: int,
    expansion_search: int | None,
) -> dict[str, Any]:
    """Measure one loading strategy's peak RSS in a subprocess.

    Peak RSS is a high-water mark for a whole process. A benchmark that already
    holds the 1.64 GB baseline matrix can never observe a memory-mapped index's
    real footprint from inside itself, so each arm gets its own interpreter.
    """
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--rss-probe",
        arm,
        "--dense-index",
        str(dense_index_file),
        "--setup-cache",
        str(setup_file),
        "--probe-depth",
        str(depth),
    ]
    if ann_index_file is not None:
        command += ["--probe-ann-index", str(ann_index_file)]
    if expansion_search is not None:
        command += ["--probe-expansion-search", str(expansion_search)]
    completed = subprocess.run(command, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if completed.returncode != 0:
        return {"arm": arm, "error": completed.stderr.strip()[-2000:] or "probe failed"}
    payload = completed.stdout.strip().splitlines()[-1]
    return json.loads(payload)


def rss_probe_main(args: argparse.Namespace) -> int:
    """Child-process entry point: load one way, query, print peak RSS as JSON."""
    setup = load_setup(Path(args.setup_cache))
    embedder, texts_by_segment = setup_embedder(setup)
    texts = [texts_by_segment[segment_id] for segment_id in sorted(texts_by_segment)]

    if args.rss_probe == "exact":
        dense = load_dense_concept_index(Path(args.dense_index))
        mapper: Any = DenseConceptMapper(index=dense, embedder=embedder)
        concept_count = len(dense.concept_ids)
    else:
        concept_ids = read_concept_ids(Path(args.dense_index))
        concept_count = len(concept_ids)
        ann = load_ann_concept_index(
            Path(args.probe_ann_index),
            concept_ids=concept_ids,
            view=args.rss_probe == "usearch-view",
            expansion_search=args.probe_expansion_search,
        )
        mapper = UsearchConceptMapper(index=ann, embedder=embedder)

    baseline = _maxrss_bytes()
    rankings = rank_all(mapper, texts, depth=args.probe_depth)
    print(
        json.dumps(
            {
                "arm": args.rss_probe,
                "peak_rss_bytes": _maxrss_bytes(),
                "peak_rss_mb": round(_maxrss_bytes() / (1024 * 1024), 1),
                "rss_before_queries_mb": round(baseline / (1024 * 1024), 1),
                "concept_count": concept_count,
                "query_count": len(rankings),
                "returned_min": min((len(row) for row in rankings), default=0),
            }
        )
    )
    return 0


def read_concept_ids(path: Path) -> list[str]:
    """Read only the concept-id member of the cached ``.npz``.

    ``numpy.load`` on a zip archive is lazy per key, so this never materializes
    the 1.64 GB matrix — which matters, because this call happens inside the
    memory-mapped RSS probe whose whole point is not to.
    """
    import numpy

    with numpy.load(Path(path), allow_pickle=False) as stored:
        return [str(value) for value in stored["concept_ids"]]


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def _megabytes(value: int | None) -> str:
    return "—" if value is None else f"{value / (1024 * 1024):,.0f}"


def markdown_tables(document: Mapping[str, Any]) -> str:
    """Render the benchmark as the tables a maintainer needs to decide."""
    lines: list[str] = []
    lines.append("### Configuration, cost, and recall against the exact search")
    lines.append("")
    lines.append(
        "| Configuration | Storage | Build (s) | On disk (MB) | Peak RSS mmap (MB) | Peak RSS loaded (MB) | "
        "Mean (ms) | p95 (ms) | recall@50 | recall@12 |"
    )
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in document["configurations"]:
        recall = {entry["depth"]: entry for entry in row.get("recall", ())}
        lines.append(
            "| {name} | {storage} | {build} | {disk} | {mmap} | {loaded} | {mean} | {p95} | {r50} | {r12} |".format(
                name=row["configuration"],
                storage=row.get("quantization") or "f32 exact",
                build=f"{row['build_seconds']:.1f}" if row.get("build_seconds") is not None else "—",
                disk=_megabytes(row.get("on_disk_bytes")),
                mmap=row.get("peak_rss_mmap_mb") if row.get("peak_rss_mmap_mb") is not None else "—",
                loaded=row.get("peak_rss_loaded_mb") if row.get("peak_rss_loaded_mb") is not None else "—",
                mean=row["latency"]["mean_ms"],
                p95=row["latency"]["p95_ms"],
                r50=(
                    f"{recall[50]['macro_recall']:.4f}"
                    if 50 in recall and recall[50]["macro_recall"] is not None
                    else "1.0000 (ground truth)"
                ),
                r12=(
                    f"{recall[PROMPT_CONCEPT_LIMIT]['macro_recall']:.4f}"
                    if PROMPT_CONCEPT_LIMIT in recall and recall[PROMPT_CONCEPT_LIMIT]["macro_recall"] is not None
                    else "1.0000 (ground truth)"
                ),
            )
        )

    lines.append("")
    lines.append("### The eight-target oracle (top-12 shortlist, 35 development items)")
    lines.append("")
    header = "| Configuration | " + " | ".join(f"{name} targets" for name in ORACLE_CONFIGURATIONS) + " |"
    lines.append(header)
    lines.append("| --- | " + " | ".join("---:" for _ in ORACLE_CONFIGURATIONS) + " |")
    for row in document["configurations"]:
        oracle = row.get("oracle", {})
        cells = []
        for name in ORACLE_CONFIGURATIONS:
            entry = oracle.get(name)
            cells.append(f"{entry['exact_alias_surfaced']}/{entry['exact_alias_target_count']}" if entry else "—")
        lines.append(f"| {row['configuration']} | " + " | ".join(cells) + " |")

    lines.append("")
    lines.append("### Missed exact-alias targets, per configuration")
    lines.append("")
    lines.append("| Configuration | Oracle configuration | Surfaced | Missed |")
    lines.append("| --- | --- | --- | --- |")
    for row in document["configurations"]:
        for name, entry in row.get("oracle", {}).items():
            lines.append(
                "| {config} | {oracle} | {surfaced} | {missed} |".format(
                    config=row["configuration"],
                    oracle=name,
                    surfaced=", ".join(entry["exact_alias_surfaced_labels"]) or "—",
                    missed=", ".join(entry["exact_alias_missed_labels"]) or "—",
                )
            )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


def run_benchmark(
    *,
    dense_index_file: Path,
    registry_file: Path,
    setup_file: Path,
    work_dir: Path,
    output_dir: Path,
    configuration_names: Sequence[str],
    repeats: int,
    limit: int,
    measure_rss: bool,
) -> dict[str, Any]:
    """Build every configuration, measure it, and score the oracle."""
    unknown = [name for name in configuration_names if name not in ANN_CONFIGURATIONS_BY_NAME]
    if unknown:
        raise BenchmarkError(f"unknown configurations: {sorted(unknown)}")
    configurations = [ANN_CONFIGURATIONS_BY_NAME[name] for name in configuration_names]

    timings: dict[str, float] = {}
    setup = load_setup(setup_file)
    embedder, texts_by_segment = setup_embedder(setup)
    items = setup_gold_items(setup)
    units_by_id = setup_units(setup)
    segment_ids = sorted(texts_by_segment)
    texts = [texts_by_segment[segment_id] for segment_id in segment_ids]

    started = time.monotonic()
    registry_rows = read_parquet_rows(registry_file)
    timings["read_registry"] = round(time.monotonic() - started, 3)

    started = time.monotonic()
    conditioning = _condition_registry(registry_rows)
    timings["condition_registry"] = round(time.monotonic() - started, 3)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}
    source_vocabulary_by_id = {
        concept_id: conditioning.source_vocabularies[index] for index, concept_id in enumerate(conditioning.concept_ids)
    }

    started = time.monotonic()
    bm25_mapper = BM25ConceptMapper.build(registry_rows)
    timings["bm25_index"] = round(time.monotonic() - started, 3)

    # Channels A, B and E once: they do not depend on the dense mapper, and
    # recomputing them per configuration would only add noise and minutes.
    started = time.monotonic()
    base_channels = {
        segment_id: segment_channels(
            unit=units_by_id[segment_id],
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id=index_by_id,
            wanted=(CHANNEL_A, CHANNEL_B, CHANNEL_E),
            dense_mapper=None,
            bm25_mapper=bm25_mapper,
            keywords=(),
            limit=limit,
            include_v1=False,
        )
        for segment_id in segment_ids
    }
    timings["base_channels"] = round(time.monotonic() - started, 3)

    started = time.monotonic()
    dense = load_dense_concept_index(dense_index_file)
    timings["load_dense_index"] = round(time.monotonic() - started, 3)
    if dense.model_id != setup["model_id"]:
        raise BenchmarkError(
            f"cached dense index model {dense.model_id!r} differs from the setup cache's {setup['model_id']!r}"
        )

    rows: list[dict[str, Any]] = []

    # --- the exact baseline -------------------------------------------------
    exact_mapper = DenseConceptMapper(index=dense, embedder=embedder)
    exact_rankings = {depth: rank_all(exact_mapper, texts, depth=depth) for depth in RECALL_DEPTHS}
    exact_latency = measure_latency(exact_mapper, texts, depth=LATENCY_DEPTH, repeats=repeats)
    exact_row: dict[str, Any] = {
        "configuration": "exact-brute-force",
        "channel_version": DENSE_CHANNEL_VERSION,
        "quantization": None,
        "note": "incumbent: full in-memory float32 matrix, exact cosine over every concept",
        "is_baseline": True,
        "build_seconds": None,
        "on_disk_bytes": dense_index_file.stat().st_size,
        "latency": exact_latency,
        "recall": [],
        "oracle": oracle_measurements(
            mapper=exact_mapper,
            base_channels=base_channels,
            items=items,
            units_by_id=units_by_id,
            registry_rows=registry_rows,
            conditioning=conditioning,
            index_by_id=index_by_id,
            source_vocabulary_by_id=source_vocabulary_by_id,
            limit=limit,
        ),
    }
    rows.append(exact_row)

    # --- the approximate configurations ------------------------------------
    stored_paths: dict[str, Path] = {}
    for configuration in configurations:
        print(f"[{configuration.name}] {configuration.note}", file=sys.stderr, flush=True)
        if configuration.reuse_graph_of:
            path = stored_paths.get(configuration.reuse_graph_of)
            if path is None:
                raise BenchmarkError(
                    f"{configuration.name} reuses {configuration.reuse_graph_of}, which was not built in this run"
                )
            build_facts: dict[str, Any] = {
                "source": "reused",
                "seconds": None,
                "reused_from": configuration.reuse_graph_of,
            }
        else:
            path = ann_index_path(
                work_dir,
                registry_digest=dense.registry_digest,
                model_id=dense.model_id,
                quantization=configuration.quantization,
                connectivity=configuration.connectivity,
                expansion_add=configuration.expansion_add,
            )
            if path.exists() and sidecar_path(path).exists():
                build_facts = {"source": "cache", "seconds": None, "path": str(path)}
            else:
                ann, build_facts = build_ann_concept_index(
                    dense,
                    quantization=configuration.quantization,
                    connectivity=configuration.connectivity,
                    expansion_add=configuration.expansion_add,
                    expansion_search=configuration.expansion_search,
                )
                build_facts.update(save_ann_concept_index(ann, path))
                del ann
            stored_paths[configuration.name] = path

        loaded = load_ann_concept_index(
            path,
            concept_ids=dense.concept_ids,
            registry_digest=dense.registry_digest,
            model_id=dense.model_id,
            view=True,
            expansion_search=configuration.expansion_search,
        )
        mapper = UsearchConceptMapper(index=loaded, embedder=embedder)
        approximate = {depth: rank_all(mapper, texts, depth=depth) for depth in RECALL_DEPTHS}
        latency = measure_latency(mapper, texts, depth=LATENCY_DEPTH, repeats=repeats)
        row: dict[str, Any] = {
            "configuration": configuration.name,
            "channel_version": ANN_CHANNEL_VERSION,
            "quantization": configuration.quantization,
            "connectivity": configuration.connectivity,
            "expansion_add": configuration.expansion_add,
            "expansion_search": configuration.expansion_search,
            "note": configuration.note,
            "is_baseline": False,
            "build_seconds": build_facts.get("seconds"),
            "build_source": build_facts.get("source"),
            "on_disk_bytes": path.stat().st_size,
            "index_facts": loaded.facts(),
            "latency": latency,
            "recall": [
                recall_against_exact(approximate[depth], exact_rankings[depth], depth=depth) for depth in RECALL_DEPTHS
            ],
            "oracle": oracle_measurements(
                mapper=mapper,
                base_channels=base_channels,
                items=items,
                units_by_id=units_by_id,
                registry_rows=registry_rows,
                conditioning=conditioning,
                index_by_id=index_by_id,
                source_vocabulary_by_id=source_vocabulary_by_id,
                limit=limit,
            ),
        }
        rows.append(row)
        del mapper, loaded

    # --- peak RSS, each arm in its own process -----------------------------
    if measure_rss:
        started = time.monotonic()
        exact_probe = run_rss_probe(
            arm="exact",
            dense_index_file=dense_index_file,
            ann_index_file=None,
            setup_file=setup_file,
            depth=LATENCY_DEPTH,
            expansion_search=None,
        )
        exact_row["peak_rss_loaded_mb"] = exact_probe.get("peak_rss_mb")
        exact_row["rss_probe"] = exact_probe
        for row in rows[1:]:
            path = stored_paths.get(
                row["configuration"],
                stored_paths.get(str(ANN_CONFIGURATIONS_BY_NAME[row["configuration"]].reuse_graph_of)),
            )
            if path is None:
                continue
            probes = {
                arm: run_rss_probe(
                    arm=arm,
                    dense_index_file=dense_index_file,
                    ann_index_file=path,
                    setup_file=setup_file,
                    depth=LATENCY_DEPTH,
                    expansion_search=row["expansion_search"],
                )
                for arm in ("usearch-view", "usearch-load")
            }
            row["peak_rss_mmap_mb"] = probes["usearch-view"].get("peak_rss_mb")
            row["peak_rss_loaded_mb"] = probes["usearch-load"].get("peak_rss_mb")
            row["rss_probes"] = probes
        timings["rss_probes"] = round(time.monotonic() - started, 3)

    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": {
            "dense_index_file": str(dense_index_file),
            "dense_index_bytes": dense_index_file.stat().st_size,
            "registry_file": str(registry_file),
            "registry_sha256": sha256_file(registry_file),
            "registry_row_count": len(registry_rows),
            "eligible_concept_count": len(dense.concept_ids),
            "setup_cache": str(setup_file),
            "setup_cache_sha256": sha256_file(setup_file),
            "model_id": dense.model_id,
            "registry_digest": dense.registry_digest,
            "dimensions": dense.dimensions,
        },
        "settings": {
            "usearch_version": USEARCH_VERSION,
            "recall_depths": list(RECALL_DEPTHS),
            "latency_depth": LATENCY_DEPTH,
            "latency_repeats": repeats,
            "prompt_concept_limit": limit,
            "oracle_configurations": list(ORACLE_CONFIGURATIONS),
            "query_embedding": "precomputed once, shared by every arm",
        },
        "evaluation_boundary": setup.get("evaluation_boundary"),
        "evaluation_scope": "development_only",
        "accuracy_verdict_eligible": False,
        "item_count": len(items),
        "segment_count": len(segment_ids),
        "timings_seconds": timings,
        "configurations": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory for the JSON record and tables.")
    parser.add_argument("--work-dir", type=Path, default=None, help="Directory for the built USearch graphs.")
    parser.add_argument("--setup-cache", type=Path, default=None, help="Cached gold items and query vectors.")
    parser.add_argument("--dense-index", type=Path, default=None, help="Cached dense concept index (.npz).")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--selection-file", type=Path, default=None)
    parser.add_argument("--resolved", type=Path, default=DEFAULT_RESOLVED)
    parser.add_argument("--limit", type=int, default=PROMPT_CONCEPT_LIMIT)
    parser.add_argument("--repeats", type=int, default=3, help="Timed passes over the 35 queries.")
    parser.add_argument("--configurations", nargs="+", default=[row.name for row in ANN_CONFIGURATIONS])
    parser.add_argument("--skip-rss", action="store_true", help="Skip the subprocess peak-RSS probes.")
    parser.add_argument("--prepare-only", action="store_true", help="Build the setup cache and stop.")
    parser.add_argument("--rss-probe", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--probe-ann-index", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--probe-depth", type=int, default=LATENCY_DEPTH, help=argparse.SUPPRESS)
    parser.add_argument("--probe-expansion-search", type=int, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.rss_probe:
        return rss_probe_main(args)

    if args.output_dir is None:
        parser.error("--output-dir is required")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir = Path(args.work_dir) if args.work_dir else output_dir / "graphs"
    work_dir.mkdir(parents=True, exist_ok=True)
    setup_file = Path(args.setup_cache) if args.setup_cache else output_dir / "setup-cache.json"
    dense_index_file = Path(args.dense_index) if args.dense_index else _default_dense_index(DEFAULT_INDEX_DIR)

    if not setup_file.exists():
        print("building the setup cache (loads the frozen testbed and embeds 35 segments)", file=sys.stderr, flush=True)
        setup = build_setup(
            dataset_dir=args.dataset_dir,
            selection_file=args.selection_file or (args.run_dir / SELECTION_FILE_NAME),
            registry_file=args.registry,
            resolved_file=args.resolved,
        )
        setup_file.parent.mkdir(parents=True, exist_ok=True)
        setup_file.write_text(json.dumps(setup, indent=2, sort_keys=True) + "\n")
    if args.prepare_only:
        print(f"setup cache written to {setup_file}")
        return 0

    document = run_benchmark(
        dense_index_file=dense_index_file,
        registry_file=args.registry,
        setup_file=setup_file,
        work_dir=work_dir,
        output_dir=output_dir,
        configuration_names=args.configurations,
        repeats=args.repeats,
        limit=args.limit,
        measure_rss=not args.skip_rss,
    )
    tables = markdown_tables(document)
    (output_dir / "usearch-benchmark.json").write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    (output_dir / "usearch-benchmark.md").write_text(tables + "\n")
    print(tables)
    print()
    print(json.dumps({"timings_seconds": document["timings_seconds"]}, indent=2, sort_keys=True))
    print(f"\nfree disk: {shutil.disk_usage(work_dir).free / (1024**3):.1f} GB", file=sys.stderr)
    return 0


def _default_dense_index(directory: Path) -> Path:
    candidates = sorted(Path(directory).glob("dense-index-*.npz"))
    if not candidates:
        raise BenchmarkError(f"no cached dense index found in {directory}")
    if len(candidates) > 1:
        raise BenchmarkError(f"more than one cached dense index in {directory}; pass --dense-index")
    return candidates[0]


if __name__ == "__main__":
    raise SystemExit(main())

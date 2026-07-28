"""Audit the geometry of the concept embedding space, and what the text costs it.

The USearch benchmark (``docs/evidence/usearch-ann-benchmark-2026-07-28.md``)
found that HNSW recall collapses on this registry because the exact top-50
neighbourhood is only ~0.056 cosine wide. That is not a fact about HNSW; it is
a fact about where the concept vectors sit. This script measures that directly,
and measures how much of it the registry's own text construction causes.

Four statistics, each chosen because it fails in a distinguishable way:

* **concept-pair cosine** (within-type) — inflated by shared boilerplate, so it
  detects templated label text. It is *not* a retrieval noise floor.
* **query-to-random-concept cosine** (cross-type) — the actual retrieval null,
  measured on the same kind of comparison as the top-1 it is subtracted from.
* **centroid norm** — how far the cloud sits from the origin. Unit vectors
  scattered isotropically average to ~0; vectors crammed into a narrow cone
  average to ~1. This is the single clearest collapse signal.
* **effective dimensionality** (participation ratio of the centred singular
  spectrum) — how many of the 768 dimensions carry variance. A space using 40
  of 768 has thrown away most of its capacity.
* **top-1 margin over that null** — how much closer the *best* concept is to a
  real query than an arbitrary concept is. Comparing the top-1 against the
  *concept-pair* figure instead is a category error that understates the margin
  severalfold; see the correction in
  ``docs/evidence/usearch-ann-benchmark-2026-07-28.md``.

It compares two ways of building a concept's embedding input:

* ``current`` — exactly ``candidate_channels.concept_embedding_text``: preferred
  label, alt labels, then the ``definition`` field. That function is the v1
  rule and is deliberately frozen, so this arm keeps measuring what it measured
  when the finding was first recorded.
* ``labels-only`` — the same without the definition, which is what
  ``concept_bm25_tokens`` already does for the sparse channel ("Definitions are
  intentionally excluded").

Run::

    uv run --extra embed python tools/audit_concept_embedding_space.py \
        --setup-cache <setup-cache.json> --sample 20000 --report <path.json>

Once a rule has actually been built into an index, the honest thing to measure
is the shipped artifact rather than a re-embedded sample. ``--index`` does
that: it reports the same statistics for one or more stored dense index
``.npz`` files, needs no encoder, and re-embeds nothing::

    uv run python tools/audit_concept_embedding_space.py \
        --setup-cache <setup-cache.json> \
        --index <before.npz> --index <after.npz> --report <path.json>

Development-only diagnostics. This proposes nothing and adopts nothing; it
measures a property of the registry so a maintainer can decide whether the
registry's text, rather than the retriever, is the thing to change.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "tools"))

from spicy_regs.docpipeline.runtime import sha256_file  # noqa: E402
from spicy_regs.ontology.candidate_channels import (  # noqa: E402
    _alt_labels,
    concept_embedding_text,
    eligible_concepts,
)
from spicy_regs.ontology.common import read_parquet_rows  # noqa: E402

AUDIT_SCHEMA_VERSION = "concept-embedding-space-audit-v1"
DEFAULT_REGISTRY = REPO_ROOT / "output" / "fused-concept-registry-v1" / "registry.parquet"
DEFAULT_SAMPLE = 20_000


def labels_only_text(concept: Mapping[str, Any]) -> str:
    """``concept_embedding_text`` without the definition field.

    Deliberately mirrors that function's de-duplication rule so the two arms
    differ in exactly one thing: whether the definition is appended.
    """
    parts: list[str] = []
    seen: set[str] = set()
    for value in (concept.get("pref_label"), *_alt_labels(concept)):
        text = str(value or "").strip()
        if not text or text.casefold() in seen:
            continue
        seen.add(text.casefold())
        parts.append(text)
    return "; ".join(parts)


def definition_template(concept: Mapping[str, Any]) -> str:
    """The definition with its own concept's label blanked out.

    Two definitions collapse to the same template exactly when they say the same
    thing about different concepts — which is how boilerplate is detected
    without guessing at a pattern in advance.

    The label is matched only where it is not embedded in a longer word, so a
    short label cannot corrupt the template it appears inside — a concept named
    ``A`` must not turn ``FAST term: A.`` into ``F{LABEL}ST term: {LABEL}.`` and
    thereby split one template into many. Lookarounds rather than ``\\b`` because
    a label may legitimately end in punctuation (``U.S.``), where ``\\b`` fails.
    """
    definition = " ".join(str(concept.get("definition") or "").split())
    label = str(concept.get("pref_label") or "").strip()
    if not label:
        return definition
    return re.sub(rf"(?<!\w){re.escape(label)}(?!\w)", "{LABEL}", definition)


def geometry(vectors: Any, queries: Any, *, depth: int = 50) -> dict[str, Any]:
    """The four statistics, over L2-normalized concept vectors and query vectors."""
    import numpy

    matrix = numpy.asarray(vectors, dtype=numpy.float32)
    if matrix.ndim != 2 or matrix.shape[0] < 4:
        raise ValueError("geometry needs at least four concept vectors")
    half = matrix.shape[0] // 2
    with numpy.errstate(divide="ignore", over="ignore", invalid="ignore"):
        # Disjoint halves, so no vector is ever paired with itself.
        pairs = numpy.einsum("ij,ij->i", matrix[:half], matrix[half : half * 2])
        centroid = matrix.mean(axis=0)
        centred = matrix - centroid
        singular = numpy.linalg.svd(centred, compute_uv=False)
        energy = singular**2
        energy = energy / energy.sum()
        similarity = matrix @ numpy.asarray(queries, dtype=numpy.float32).T

    tops: list[float] = []
    spreads: list[float] = []
    for column in range(similarity.shape[1]):
        ordered = numpy.sort(similarity[:, column])[::-1][:depth]
        tops.append(float(ordered[0]))
        spreads.append(float(ordered[0] - ordered[-1]))
    # The null for "is the best concept meaningfully closer than an arbitrary
    # one" must be measured on the SAME kind of comparison as the thing it is
    # compared against. ``pairs`` is concept-to-concept (within-type); the top-1
    # is segment-to-concept (cross-type). Short label strings sit high against
    # each other and low against prose for reasons that carry no information
    # about relevance, so subtracting one from the other is meaningless. An
    # earlier revision of this file did exactly that and reported a margin of
    # 0.029 where the correct figure is 0.217; see the correction in
    # docs/evidence/usearch-ann-benchmark-2026-07-28.md.
    concept_to_concept = float(pairs.mean())
    query_to_random_concept = float(similarity.mean())
    top_one = float(numpy.mean(tops)) if tops else 0.0
    return {
        "concept_count": int(matrix.shape[0]),
        "query_count": int(similarity.shape[1]),
        "depth": int(depth),
        # Within-type: concept vs concept. Useful for detecting shared
        # boilerplate, which inflates it. NOT a retrieval noise floor.
        "concept_pair_cosine_mean": round(concept_to_concept, 6),
        "concept_pair_cosine_p95": round(float(numpy.percentile(pairs, 95)), 6),
        # Cross-type: query vs an arbitrary concept. THIS is the retrieval null.
        "query_to_random_concept_mean": round(query_to_random_concept, 6),
        "centroid_norm": round(float(numpy.linalg.norm(centroid)), 6),
        "effective_dimensions": round(float(1.0 / (energy**2).sum()), 3),
        "dimensions": int(matrix.shape[1]),
        "variance_in_top_10_dims": round(float(energy[:10].sum()), 6),
        "query_top1_cosine_mean": round(top_one, 6),
        "query_top1_to_topk_spread_mean": round(float(numpy.mean(spreads)), 6) if spreads else None,
        "top1_margin_over_random_concept": round(top_one - query_to_random_concept, 6),
    }


def index_geometry(
    matrix: Any,
    queries: Any,
    *,
    sample: int,
    seed: int,
    depth: int = 50,
) -> dict[str, Any]:
    """The four statistics for a stored index, sampled *and* whole.

    The sampled figures use the same protocol as the re-embedding arms above —
    same generator, same seed, same draw, and deliberately **not** sorted,
    because :func:`geometry` pairs the sample's two halves and a sorted draw
    would pair one region of a scheme-clustered registry against another rather
    than pairing at random. Run against an index built from an unchanged
    registry, this reproduces that arm's numbers, which is the control.

    The ``full_`` figures take the top-1 over every concept in the index, which
    is what the channel actually searches — a margin measured over a
    20,000-row sample is a different, easier question than the one retrieval
    asks.
    """
    import numpy

    generator = numpy.random.default_rng(seed)
    rows = int(matrix.shape[0])
    chosen = generator.choice(rows, min(sample, rows), replace=False)
    stats = geometry(matrix[chosen], queries, depth=depth)
    with numpy.errstate(divide="ignore", over="ignore", invalid="ignore"):
        similarity = numpy.asarray(matrix, dtype=numpy.float32) @ numpy.asarray(queries, dtype=numpy.float32).T
    ordered = numpy.sort(similarity, axis=0)[::-1][:depth]
    top_one = float(ordered[0].mean())
    stats["sampled_concept_count"] = int(len(chosen))
    stats["full_concept_count"] = rows
    stats["full_query_top1_cosine_mean"] = round(top_one, 6)
    stats["full_query_top1_to_topk_spread_mean"] = round(float((ordered[0] - ordered[-1]).mean()), 6)
    # Whole-index null, same cross-type comparison as the top-1 above.
    stats["full_query_to_random_concept_mean"] = round(float(similarity.mean()), 6)
    stats["full_top1_margin_over_random_concept"] = round(top_one - float(similarity.mean()), 6)
    return stats


def audit_stored_indexes(
    *,
    index_files: Sequence[Path],
    query_vectors: Any,
    sample: int,
    seed: int,
    depth: int = 50,
) -> dict[str, Any]:
    """Measure one or more already-built dense indexes, one at a time.

    Each index is released before the next is read: two 1.6 GB matrices held at
    once is a needless requirement for a diagnostic.
    """
    from spicy_regs.ontology.candidate_channels import load_dense_concept_index

    arms: dict[str, Any] = {}
    inputs: list[dict[str, Any]] = []
    for index_file in index_files:
        index = load_dense_concept_index(Path(index_file))
        name = index.embedding_text_version
        if name in arms:
            raise ValueError(f"two indexes claim the same embedding text version {name!r}")
        arms[name] = index_geometry(index.matrix, query_vectors, sample=sample, seed=seed, depth=depth)
        inputs.append({"index_file": str(index_file), **index.facts()})
        del index
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "evaluation_scope": "development_only",
        "accuracy_verdict_eligible": False,
        "inputs": {"indexes": inputs, "sample": sample, "seed": seed},
        "arms": arms,
    }


def registry_composition(concepts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Scheme mix and definition templating — the two things the audit blames."""
    schemes = Counter(str(concept.get("scheme") or "") for concept in concepts)
    templates = Counter(definition_template(concept) for concept in concepts)
    empty = sum(1 for concept in concepts if not str(concept.get("definition") or "").strip())
    boilerplate = sum(count for template, count in templates.items() if count > 1)
    return {
        "concept_count": len(concepts),
        "schemes": dict(schemes.most_common()),
        "distinct_definition_templates": len(templates),
        "most_common_definition_templates": [
            {"template": template, "concepts": count} for template, count in templates.most_common(8)
        ],
        "concepts_with_empty_definition": empty,
        "concepts_sharing_a_definition_template": boilerplate,
        "share_sharing_a_definition_template": round(boilerplate / len(concepts), 6) if concepts else None,
    }


def run_audit(
    *,
    registry_file: Path,
    query_vectors: Any,
    sample: int,
    seed: int,
    depth: int = 50,
) -> dict[str, Any]:
    """Embed one sample two ways and report both geometries beside the composition."""
    import numpy
    from sentence_transformers import SentenceTransformer

    from spicy_regs.docpipeline.adapters.sentence_transformers import (
        DEFAULT_DENSE_MODEL,
        DEFAULT_DENSE_REVISION,
    )

    concepts = eligible_concepts(read_parquet_rows(Path(registry_file)))
    composition = registry_composition(concepts)
    generator = numpy.random.default_rng(seed)
    chosen = generator.choice(len(concepts), min(sample, len(concepts)), replace=False)
    rows = [concepts[int(position)] for position in chosen]

    encoder = SentenceTransformer(DEFAULT_DENSE_MODEL, revision=DEFAULT_DENSE_REVISION)
    arms: dict[str, Any] = {}
    for name, builder in (("current", concept_embedding_text), ("labels-only", labels_only_text)):
        texts = [builder(concept) for concept in rows]
        vectors = encoder.encode(
            texts,
            batch_size=256,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(numpy.float32)
        arms[name] = {
            **geometry(vectors, query_vectors, depth=depth),
            "example_text": texts[0],
            "median_text_chars": int(numpy.median([len(text) for text in texts])),
        }
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "evaluation_scope": "development_only",
        "accuracy_verdict_eligible": False,
        "inputs": {
            "registry_file": str(registry_file),
            "registry_sha256": sha256_file(Path(registry_file)),
            "model": DEFAULT_DENSE_MODEL,
            "revision": DEFAULT_DENSE_REVISION,
            "sample": len(rows),
            "seed": seed,
        },
        "composition": composition,
        "arms": arms,
    }


def markdown_table(document: Mapping[str, Any]) -> str:
    """One row per statistic, one column per arm, in the order the arms were measured."""
    rows = [
        ("concept-pair cosine (within-type; boilerplate detector)", "concept_pair_cosine_mean"),
        ("centroid norm (0 isotropic, 1 collapsed)", "centroid_norm"),
        ("effective dimensions (of 768)", "effective_dimensions"),
        ("query top-1 cosine", "query_top1_cosine_mean"),
        ("query top-1 → top-50 spread", "query_top1_to_topk_spread_mean"),
        ("query vs random concept (retrieval null)", "query_to_random_concept_mean"),
        ("top-1 margin over that null", "top1_margin_over_random_concept"),
        ("query top-1 cosine, whole index", "full_query_top1_cosine_mean"),
        ("query vs random concept, whole index", "full_query_to_random_concept_mean"),
        ("top-1 margin over that null, whole index", "full_top1_margin_over_random_concept"),
    ]
    arms = document["arms"]
    names = list(arms)
    lines = [
        "| Measure | " + " | ".join(names) + " |",
        "| --- |" + " ---: |" * len(names),
    ]
    for title, key in rows:
        if not any(key in arms[name] for name in names):
            continue
        cells = " | ".join(str(arms[name].get(key, "—")) for name in names)
        lines.append(f"| {title} | {cells} |")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--sample", type=int, default=DEFAULT_SAMPLE)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--depth", type=int, default=50)
    parser.add_argument(
        "--setup-cache",
        type=Path,
        required=True,
        help="Benchmark setup cache holding the 35 development query vectors.",
    )
    parser.add_argument(
        "--index",
        type=Path,
        action="append",
        default=None,
        dest="indexes",
        help="Measure this stored dense index instead of re-embedding. Repeatable.",
    )
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)

    import numpy

    from benchmark_usearch_index import load_setup, setup_embedder

    setup = load_setup(args.setup_cache)
    embedder, texts_by_segment = setup_embedder(setup)
    queries = numpy.asarray(
        [embedder.vectors_by_text[texts_by_segment[segment_id]] for segment_id in sorted(texts_by_segment)],
        dtype=numpy.float32,
    )
    queries /= numpy.linalg.norm(queries, axis=1, keepdims=True)

    if args.indexes:
        document = audit_stored_indexes(
            index_files=args.indexes,
            query_vectors=queries,
            sample=args.sample,
            seed=args.seed,
            depth=args.depth,
        )
    else:
        document = run_audit(
            registry_file=args.registry,
            query_vectors=queries,
            sample=args.sample,
            seed=args.seed,
            depth=args.depth,
        )
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    composition = document.get("composition")
    if composition:
        print(f"schemes: {composition['schemes']}")
        print(
            f"definition templates: {composition['distinct_definition_templates']} distinct; "
            f"{composition['share_sharing_a_definition_template']:.1%} of concepts share one"
        )
        print()
    print(markdown_table(document))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Audit the geometry of the concept embedding space, and what the text costs it.

The USearch benchmark (``docs/evidence/usearch-ann-benchmark-2026-07-28.md``)
found that HNSW recall collapses on this registry because the exact top-50
neighbourhood is only ~0.056 cosine wide. That is not a fact about HNSW; it is
a fact about where the concept vectors sit. This script measures that directly,
and measures how much of it the registry's own text construction causes.

Four statistics, each chosen because it fails in a distinguishable way:

* **random-pair cosine** — the noise floor. If two concepts drawn at random are
  already very similar, no retriever can separate them.
* **centroid norm** — how far the cloud sits from the origin. Unit vectors
  scattered isotropically average to ~0; vectors crammed into a narrow cone
  average to ~1. This is the single clearest collapse signal.
* **effective dimensionality** (participation ratio of the centred singular
  spectrum) — how many of the 768 dimensions carry variance. A space using 40
  of 768 has thrown away most of its capacity.
* **top-1 margin over the noise floor** — the one that decides whether
  retrieval can work at all: how much more similar the *best* concept is to a
  real query than two random concepts are to each other. A margin near zero
  means the top match is barely distinguishable from an arbitrary one.

It compares two ways of building a concept's embedding input:

* ``current`` — exactly ``candidate_channels.concept_embedding_text``: preferred
  label, alt labels, then the ``definition`` field.
* ``labels-only`` — the same without the definition, which is what
  ``concept_bm25_tokens`` already does for the sparse channel ("Definitions are
  intentionally excluded").

Run::

    uv run --extra embed python tools/audit_concept_embedding_space.py \
        --sample 20000 --report <path.json>

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
    noise_floor = float(pairs.mean())
    top_one = float(numpy.mean(tops)) if tops else 0.0
    return {
        "concept_count": int(matrix.shape[0]),
        "query_count": int(similarity.shape[1]),
        "depth": int(depth),
        "random_pair_cosine_mean": round(noise_floor, 6),
        "random_pair_cosine_p95": round(float(numpy.percentile(pairs, 95)), 6),
        "centroid_norm": round(float(numpy.linalg.norm(centroid)), 6),
        "effective_dimensions": round(float(1.0 / (energy**2).sum()), 3),
        "dimensions": int(matrix.shape[1]),
        "variance_in_top_10_dims": round(float(energy[:10].sum()), 6),
        "query_top1_cosine_mean": round(top_one, 6),
        "query_top1_to_topk_spread_mean": round(float(numpy.mean(spreads)), 6) if spreads else None,
        "top1_margin_over_noise_floor": round(top_one - noise_floor, 6),
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
    rows = [
        ("random concept-pair cosine (noise floor)", "random_pair_cosine_mean"),
        ("centroid norm (0 isotropic, 1 collapsed)", "centroid_norm"),
        ("effective dimensions (of 768)", "effective_dimensions"),
        ("query top-1 cosine", "query_top1_cosine_mean"),
        ("query top-1 → top-50 spread", "query_top1_to_topk_spread_mean"),
        ("top-1 margin over noise floor", "top1_margin_over_noise_floor"),
    ]
    arms = document["arms"]
    lines = ["| Measure | current (pref; alts; definition) | labels-only (pref; alts) |", "| --- | ---: | ---: |"]
    for title, key in rows:
        lines.append(f"| {title} | {arms['current'][key]} | {arms['labels-only'][key]} |")
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
    composition = document["composition"]
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

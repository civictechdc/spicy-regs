"""Approximate nearest-neighbour serving for the dense concept channel.

``spicy_regs.ontology.candidate_channels`` owns channel C and its exact search:
:class:`~spicy_regs.ontology.candidate_channels.DenseConceptMapper` holds the
whole 513,236x768 float32 concept matrix in memory and computes a full matrix
product per query. That is exact — it *is* the ground truth for this module —
but it costs 1.64 GB of resident memory before a single query runs, which the
candidate-selection research flagged directly as an operational limit at this
vocabulary scale ("build offline, memory-map",
``docs/evidence/candidate-selection-research-2026-07-27.md``).

This module keeps a second serving path beside that one, built from the *same*
vectors: a USearch HNSW graph, saved once and memory-mapped at query time. It
never re-embeds anything. It takes an already-built
:class:`DenseConceptIndex` — normally the cached ``.npz`` — and turns it into a
graph whose keys are that index's own row positions, so a result key is a row
in ``concept_ids`` and nothing else needs to be carried alongside.

Three things are deliberate:

* **Row position is the key.** USearch keys are ``uint64``; concept ids are
  strings. Rather than store a second id table, the key is the row index of the
  source dense index, and the sidecar records that index's ``registry_digest``.
  A caller that supplies different concept ids for the same digest is refused,
  and a graph built from a different registry is refused outright.
* **Scores, not distances.** USearch's ``cos`` metric returns a distance;
  ``DenseConceptMapper`` returns a cosine similarity. :class:`UsearchConceptMapper`
  converts, so a channel reading this mapper sees the same score convention and
  the same ``(concept_id, score)`` shape the exact mapper emits. A fused
  configuration cannot tell which mapper produced its ranking.
* **Approximate results are labelled approximate.** Nothing here claims parity
  with the exact search. ``tools/benchmark_usearch_index.py`` measures recall
  against the exact search and reports the gap; this module only makes that
  measurement possible.

Nothing here is wired into production. USearch is an optional dependency (the
``ann`` extra) and is imported lazily, so an environment without it keeps
working and gets a clear error only if it asks for this path.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from spicy_regs.ontology.candidate_channels import (
    CHANNEL_DEPTH,
    DenseConceptIndex,
    DenseEmbedder,
    _model_slug,
    _normalize_rows,
)

USEARCH_PACKAGE = "usearch"
# Pinned so a recall number is reproducible: HNSW construction is a property of
# the library build, not only of the parameters passed to it.
USEARCH_VERSION = "2.26.0"

ANN_CHANNEL_VERSION = "dense-usearch-hnsw-c1"
ANN_INDEX_SCHEMA_VERSION = "concept-ann-index-v1"

# USearch's own cosine metric. The source vectors are already L2-normalized, so
# this agrees with the exact channel's inner product up to the graph's own
# approximation and the storage quantization.
ANN_METRIC = "cos"

# USearch's defaults, named here so a stored index records the values it was
# built with rather than inheriting whatever a later release defaults to.
DEFAULT_CONNECTIVITY = 16
DEFAULT_EXPANSION_ADD = 128
DEFAULT_EXPANSION_SEARCH = 64

# Storage quantization for the graph's vectors. ``f32`` stores what the exact
# channel holds; ``f16`` and ``i8`` trade representable precision for resident
# bytes, which is the whole point of testing them.
QUANTIZATIONS: tuple[str, ...] = ("f32", "f16", "i8")

INDEX_SUFFIX = ".usearch"
SIDECAR_SUFFIX = ".meta.json"


class UsearchUnavailableError(RuntimeError):
    """USearch is not installed, or not at the pinned version."""


class AnnIndexError(RuntimeError):
    """A stored ANN index does not match the dense index it is used with."""


def installed_package_version(package: str) -> str | None:
    """Report an installed distribution version without importing the package."""
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def require_usearch(version_reader: Callable[[str], str | None] = installed_package_version) -> Any:
    """Import USearch lazily and hold it to the pinned build.

    ``version_reader`` is the injection seam, matching the local-inference
    adapters: the pin is checked against an installation rather than declared by
    the caller. The error names the extra to install, because "no module named
    usearch" does not tell a maintainer what to do about it.
    """
    resolved = version_reader(USEARCH_PACKAGE)
    if resolved is None:
        raise UsearchUnavailableError(
            "usearch is not installed; the dense ANN path needs the optional 'ann' extra "
            f"(uv sync --extra ann, which pins usearch=={USEARCH_VERSION})"
        )
    if resolved != USEARCH_VERSION:
        raise UsearchUnavailableError(
            f"usearch version differs from the pinned contract: {resolved} != {USEARCH_VERSION}"
        )
    try:
        import usearch.index as usearch_index
    except ImportError as error:  # pragma: no cover - metadata without the package
        raise UsearchUnavailableError(
            "usearch reports an installed distribution but cannot be imported; reinstall the optional 'ann' extra"
        ) from error
    return usearch_index


def _validate_quantization(quantization: str) -> str:
    if quantization not in QUANTIZATIONS:
        raise AnnIndexError(f"unsupported ANN quantization {quantization!r}; expected one of {list(QUANTIZATIONS)}")
    return quantization


@dataclass(frozen=True)
class AnnConceptIndex:
    """A USearch graph over one dense concept index's rows, plus its identity.

    ``handle`` is the live ``usearch.index.Index``. It is held rather than
    subclassed so this dataclass stays a plain description of *which* index this
    is; every search goes through :meth:`search_rows`, which is the only place
    that knows USearch's result shape.
    """

    schema_version: str
    model_id: str
    dimensions: int
    registry_digest: str
    concept_ids: tuple[str, ...]
    quantization: str
    connectivity: int
    expansion_add: int
    expansion_search: int
    usearch_version: str
    viewed: bool
    handle: Any

    def facts(self) -> dict[str, Any]:
        """Secret-free identity of this graph, for a run record or a sidecar."""
        return {
            "schema_version": self.schema_version,
            "model_id": self.model_id,
            "dimensions": self.dimensions,
            "registry_digest": self.registry_digest,
            "concept_count": len(self.concept_ids),
            "quantization": self.quantization,
            "metric": ANN_METRIC,
            "connectivity": self.connectivity,
            "expansion_add": self.expansion_add,
            "expansion_search": self.expansion_search,
            "usearch_version": self.usearch_version,
            "channel_version": ANN_CHANNEL_VERSION,
        }

    def search_rows(self, query_matrix: Any, *, depth: int, threads: int = 0) -> list[list[tuple[int, float]]]:
        """Nearest rows per query, as ``(row, cosine similarity)`` best first.

        USearch returns two different shapes and both must be read correctly.
        A multi-row query yields ``BatchMatches``, whose rows are padded to the
        requested width with an arbitrary key and a NaN distance — there the
        per-row ``counts`` decide how much of each row is real, and reading past
        it turns a padded zero into a phantom top hit. A *single* row query —
        which is exactly how ``dense_channel_ranking`` calls a mapper — yields
        ``Matches`` instead, which has no ``counts`` and is already trimmed.
        Assuming either shape alone breaks the other.
        """
        import numpy

        if depth <= 0 or not self.concept_ids:
            return [[] for _ in range(int(numpy.asarray(query_matrix).shape[0]))]
        matches = self.handle.search(query_matrix, min(depth, len(self.concept_ids)), threads=threads)
        keys = numpy.atleast_2d(matches.keys)
        distances = numpy.atleast_2d(matches.distances)
        stored_counts = getattr(matches, "counts", None)
        counts = (
            numpy.atleast_1d(stored_counts)
            if stored_counts is not None
            else numpy.full(keys.shape[0], keys.shape[1], dtype=int)
        )
        results: list[list[tuple[int, float]]] = []
        for row in range(keys.shape[0]):
            found = int(counts[row])
            ranked = [(int(keys[row][slot]), 1.0 - float(distances[row][slot])) for slot in range(found)]
            # USearch returns ascending distance already; re-sorting on the
            # converted score with the concept id as tie-break makes the order
            # identical to the exact mapper's rule rather than merely similar.
            ranked.sort(key=lambda item: (-item[1], self.concept_ids[item[0]]))
            results.append(ranked[:depth])
        return results


def build_ann_concept_index(
    dense: DenseConceptIndex,
    *,
    quantization: str = "f32",
    connectivity: int = DEFAULT_CONNECTIVITY,
    expansion_add: int = DEFAULT_EXPANSION_ADD,
    expansion_search: int = DEFAULT_EXPANSION_SEARCH,
    threads: int = 0,
    version_reader: Callable[[str], str | None] = installed_package_version,
    progress: Any = None,
) -> tuple[AnnConceptIndex, dict[str, Any]]:
    """Build a USearch graph from an already-embedded dense concept index.

    No embedding happens here and none should: the 513,236-row build costs about
    fifty minutes through the encoder, and the point of this path is to reuse
    that work, not to repeat it.
    """
    import numpy

    usearch_index = require_usearch(version_reader)
    _validate_quantization(quantization)
    matrix = numpy.ascontiguousarray(dense.matrix, dtype=numpy.float32)
    if matrix.shape[0] != len(dense.concept_ids):
        raise AnnIndexError("dense index row count differs from its concept ids")
    handle = usearch_index.Index(
        ndim=int(dense.dimensions),
        metric=ANN_METRIC,
        dtype=quantization,
        connectivity=int(connectivity),
        expansion_add=int(expansion_add),
        expansion_search=int(expansion_search),
        multi=False,
    )
    started = time.monotonic()
    if matrix.shape[0]:
        handle.add(numpy.arange(matrix.shape[0], dtype=numpy.uint64), matrix, threads=threads, progress=progress)
    build_seconds = time.monotonic() - started
    if int(handle.size) != matrix.shape[0]:
        raise AnnIndexError("USearch stored a different number of vectors than the dense index holds")
    index = AnnConceptIndex(
        schema_version=ANN_INDEX_SCHEMA_VERSION,
        model_id=dense.model_id,
        dimensions=int(dense.dimensions),
        registry_digest=dense.registry_digest,
        concept_ids=tuple(dense.concept_ids),
        quantization=quantization,
        connectivity=int(connectivity),
        expansion_add=int(expansion_add),
        expansion_search=int(expansion_search),
        usearch_version=str(version_reader(USEARCH_PACKAGE)),
        viewed=False,
        handle=handle,
    )
    return index, {
        "source": "built",
        "seconds": round(build_seconds, 3),
        "threads": int(threads),
        **index.facts(),
    }


def ann_index_path(
    directory: Path,
    *,
    registry_digest: str,
    model_id: str,
    quantization: str,
    connectivity: int = DEFAULT_CONNECTIVITY,
    expansion_add: int = DEFAULT_EXPANSION_ADD,
) -> Path:
    """Cache path keyed by everything that changes the stored graph's contents."""
    _validate_quantization(quantization)
    stem = (
        f"ann-index-{_model_slug(model_id)}-{registry_digest[:16]}"
        f"-{quantization}-c{int(connectivity)}-a{int(expansion_add)}"
    )
    return Path(directory) / f"{stem}{INDEX_SUFFIX}"


def sidecar_path(path: Path) -> Path:
    """The identity file written beside a stored graph."""
    return Path(path).with_name(Path(path).name + SIDECAR_SUFFIX)


def save_ann_concept_index(index: AnnConceptIndex, path: Path) -> dict[str, Any]:
    """Write the graph and its identity sidecar.

    The concept ids are *not* written: they are the source dense index's own
    ordering, and duplicating half a million strings beside every quantization
    would cost more than the graph it describes. The sidecar records the
    ``registry_digest`` those ids came from, which is what
    :func:`load_ann_concept_index` checks.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    index.handle.save(str(temporary))
    temporary.replace(path)
    facts = index.facts()
    sidecar_path(path).write_text(json.dumps(facts, indent=2, sort_keys=True) + "\n")
    return {**facts, "path": str(path), "bytes": path.stat().st_size}


def load_ann_concept_index(
    path: Path,
    *,
    concept_ids: Sequence[str],
    registry_digest: str | None = None,
    model_id: str | None = None,
    view: bool = True,
    expansion_search: int | None = None,
    version_reader: Callable[[str], str | None] = installed_package_version,
) -> AnnConceptIndex:
    """Read a stored graph, memory-mapped by default.

    ``view=True`` is the operational reason this module exists: the graph and
    its vectors stay on disk and are paged in on demand, so query-time resident
    memory is a working set rather than the whole matrix. ``view=False`` loads
    it fully, which is the honest comparison point.
    """
    usearch_index = require_usearch(version_reader)
    path = Path(path)
    sidecar = sidecar_path(path)
    if not sidecar.exists():
        raise AnnIndexError(f"stored ANN index at {path} has no identity sidecar")
    meta = json.loads(sidecar.read_text())
    if meta.get("schema_version") != ANN_INDEX_SCHEMA_VERSION:
        raise AnnIndexError(f"stored ANN index schema {meta.get('schema_version')!r} is not readable")
    if registry_digest is not None and meta.get("registry_digest") != registry_digest:
        raise AnnIndexError("stored ANN index was built from a different registry")
    if model_id is not None and meta.get("model_id") != model_id:
        raise AnnIndexError("stored ANN index was built with a different model")
    if int(meta.get("concept_count", -1)) != len(concept_ids):
        raise AnnIndexError("stored ANN index holds a different number of concepts than the ids given")
    handle = usearch_index.Index.restore(str(path), view=view)
    if handle is None:
        raise AnnIndexError(f"USearch could not read a stored index at {path}")
    if expansion_search is not None:
        handle.expansion_search = int(expansion_search)
    if int(handle.size) != len(concept_ids):
        raise AnnIndexError("stored ANN index row count differs from the concept ids given")
    return AnnConceptIndex(
        schema_version=str(meta["schema_version"]),
        model_id=str(meta["model_id"]),
        dimensions=int(meta["dimensions"]),
        registry_digest=str(meta["registry_digest"]),
        concept_ids=tuple(str(value) for value in concept_ids),
        quantization=str(meta["quantization"]),
        connectivity=int(meta["connectivity"]),
        expansion_add=int(meta["expansion_add"]),
        expansion_search=int(handle.expansion_search),
        usearch_version=str(meta.get("usearch_version") or ""),
        viewed=bool(view),
        handle=handle,
    )


@dataclass(frozen=True)
class UsearchConceptMapper:
    """Nearest concepts by approximate cosine over a USearch graph.

    Drop-in for :class:`~spicy_regs.ontology.candidate_channels.DenseConceptMapper`:
    same ``rank`` signature, same ``(concept_id, score)`` payload, same
    normalization of the query side, same order rule. What differs is that the
    ranking is approximate, which is exactly the variable under measurement.
    """

    index: AnnConceptIndex
    embedder: DenseEmbedder
    threads: int = 0
    version: str = ANN_CHANNEL_VERSION

    def rank(self, queries: Sequence[str], *, depth: int = CHANNEL_DEPTH) -> list[list[tuple[str, float]]]:
        import numpy

        results: list[list[tuple[str, float]]] = [[] for _ in queries]
        positions = [position for position, query in enumerate(queries) if str(query).strip()]
        if not positions or depth <= 0 or not self.index.concept_ids:
            return results
        vectors = self.embedder.embed([str(queries[position]) for position in positions]).vectors
        query_matrix = _normalize_rows(numpy.asarray(vectors, dtype=numpy.float32))
        for column, ranked in enumerate(self.index.search_rows(query_matrix, depth=depth, threads=self.threads)):
            results[positions[column]] = [(self.index.concept_ids[row], score) for row, score in ranked]
        return results


def recall_against_exact(
    approximate: Sequence[Sequence[str]],
    exact: Sequence[Sequence[str]],
    *,
    depth: int,
) -> dict[str, Any]:
    """Recall of the exact top-``depth`` set inside the approximate top-``depth``.

    This is the quality measure that matters for an ANN swap, and it is
    measurable rather than estimated *because* the incumbent is exact: the
    baseline is ground truth by construction, so a missing concept is a real
    loss and not a disagreement between two guesses.
    """
    if len(approximate) != len(exact):
        raise AnnIndexError("recall needs one approximate ranking per exact ranking")
    per_query: list[float] = []
    found_total = 0
    wanted_total = 0
    for approximate_ranking, exact_ranking in zip(approximate, exact, strict=True):
        wanted = list(dict.fromkeys(exact_ranking))[:depth]
        if not wanted:
            continue
        seen = set(list(dict.fromkeys(approximate_ranking))[:depth])
        found = sum(1 for concept_id in wanted if concept_id in seen)
        per_query.append(found / len(wanted))
        found_total += found
        wanted_total += len(wanted)
    return {
        "depth": int(depth),
        "query_count": len(per_query),
        "micro_recall": round(found_total / wanted_total, 6) if wanted_total else None,
        "macro_recall": round(sum(per_query) / len(per_query), 6) if per_query else None,
        "min_query_recall": round(min(per_query), 6) if per_query else None,
        "perfect_query_count": sum(1 for value in per_query if value >= 1.0),
    }

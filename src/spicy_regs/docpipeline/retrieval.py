"""The v3 retrieval step: typed candidates, prefilters, and dense search.

The foundation records the fixed retrieval plan, builds artifact or segment
candidate universes from the public Step 4 tables, applies requested filters,
and writes correctly shaped output tables. The dense leg keeps document routing
and segment evidence as separate entry points. It injects the small
``DenseEmbedder`` interface, persists exact inputs and normalized vectors, and
rebuilds ranks without a provider. The learned-sparse leg injects
``SparseEncoder``, preserves its document/query asymmetry, and stores raw
float64 CSR inputs for provider-free rebuild. Reciprocal-rank fusion combines
the dense and sparse results with fixed migration constants. Fixed-depth
reranking scores the top 50 BGE dense candidates, preserves exact pair-token
audits, and resumes by one candidate-digest-bound query group. Metrics land in
the following bounded slice.

Candidate metadata is a derived sidecar keyed by ``(source_table,
subject_id)``.  It is not ontology output.  Retrieval scores and ranks are
discovery aids stored only in ``retrieval/hits.parquet``; target IDs resolve
through the source and segment tables, so hit rows never copy source spans.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import os
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Protocol

import duckdb
import numpy as np

from spicy_regs.docpipeline.adapters.sentence_transformers import (
    DEFAULT_DENSE_MODEL,
    DENSE_PROVIDER,
    DEFAULT_DENSE_REVISION,
    DEFAULT_RERANK_BATCH_SIZE,
    DEFAULT_RERANK_MAX_SEQ_LENGTH,
    DEFAULT_RERANK_MODEL,
    DEFAULT_RERANK_REVISION,
    DEFAULT_SPARSE_MODEL,
    DEFAULT_SPARSE_REVISION,
    DenseEmbedder,
    Reranker,
    SparseEncoder,
    SparseVector,
    validate_sparse_vector,
)
from spicy_regs.docpipeline.runtime import (
    CheckResult,
    PlanError,
    ProviderTotals,
    RunChecks,
    RunOutcome,
    RunPlan,
    RunWorkspace,
    WorkCheckpoint,
    WorkIdentity,
    WorkItem,
    WorkResult,
    check_earlier_run,
    execute_run,
    sha256_file,
    sha256_text,
)
from spicy_regs.docpipeline.segments import SEGMENT_TABLE
from spicy_regs.docpipeline.source import ARTIFACT_TABLE, profile_for_table
from spicy_regs.ontology.citations import normalize_regsgov_identifier
from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.segmentation import segment_text

RETRIEVAL_STEP = "retrieve"
RETRIEVAL_FORMAT_VERSION = 1

RETRIEVAL_HIT_TABLE = "retrieval/hits.parquet"
RETRIEVAL_EXCLUSION_TABLE = "retrieval/exclusions.parquet"
DENSE_EMBEDDING_TABLE = "retrieval/dense-embeddings.parquet"
SPARSE_EMBEDDING_TABLE = "retrieval/sparse-embeddings.parquet"
RERANK_SCORE_TABLE = "retrieval/rerank-scores.parquet"
RERANK_CHECKPOINT_FILE = "retrieval/rerank-checkpoints.jsonl"
RETRIEVAL_JOIN_INPUTS_FILE = "retrieval/join-inputs.json"

RETRIEVAL_LEVELS: tuple[str, ...] = ("artifact", "segment")
RETRIEVAL_METHODS: tuple[str, ...] = (
    "exact",
    "database",
    "dense",
    "sparse",
    "hybrid-rrf",
    "reranked",
)
PLANNED_RETRIEVAL_METHODS: tuple[str, ...] = (
    "dense",
    "sparse",
    "hybrid-rrf",
    "reranked",
)
SCORE_KINDS: tuple[str, ...] = (
    "exact-match",
    "database-match",
    "cosine",
    "sparse-dot",
    "rrf",
    "cross-encoder",
)

FILTER_AXES: tuple[str, ...] = (
    "identity",
    "version",
    "authority",
    "jurisdiction",
    "time",
    "access",
    "graph",
    "agency-scoped-concepts",
)
UNKNOWN_BEHAVIORS: tuple[str, ...] = ("exclude", "include")
FILTER_REASONS = frozenset(
    {
        *(f"mismatch-{axis}" for axis in FILTER_AXES),
        *(f"unknown-{axis}" for axis in FILTER_AXES),
    }
)

RETRIEVAL_CANDIDATE_LIMIT = 200
RETRIEVAL_RRF_K = 60
RETRIEVAL_FUSION_INPUT_DEPTH = 200
RETRIEVAL_RERANK_DEPTH = 50
IR_MEASURES_VERSION = "0.4.3"
IR_MEASURES_PROVIDER = f"ir-measures:{IR_MEASURES_VERSION}"
RETRIEVAL_RECALL_CUTOFFS: tuple[int, ...] = (1, 3, 5, 10, 25, 50, 100, 200)
RETRIEVAL_PRECISION_CUTOFFS: tuple[int, ...] = (1, 3, 5, 10)
RERANK_MAX_SEQ_LENGTH = DEFAULT_RERANK_MAX_SEQ_LENGTH
RERANK_BATCH_SIZE = DEFAULT_RERANK_BATCH_SIZE

DENSE_MODEL_ID = f"sentence-transformers:{DEFAULT_DENSE_MODEL}@{DEFAULT_DENSE_REVISION}"
SPARSE_MODEL_ID = f"sentence-transformers-sparse:{DEFAULT_SPARSE_MODEL}@{DEFAULT_SPARSE_REVISION}"
RERANK_MODEL_ID = f"sentence-transformers:{DEFAULT_RERANK_MODEL}@{DEFAULT_RERANK_REVISION}"

DENSE_ARTIFACT_INPUT_POLICY = "all-profile-whole-artifact-v1"
DENSE_SEMANTIC_UNIT_POLICY = "five-arm-v3:semantic-units"
DENSE_SEGMENT_COMPOSITION_POLICY = "overlap-character-weighted-mean-v1"
DENSE_NORMALIZATION_POLICY = "l2-float64-v1"
DENSE_SEMANTIC_UNIT_TOKENS = 240
DENSE_SEMANTIC_UNIT_MIN_TOKENS = 80
DENSE_INPUT_KINDS: tuple[str, ...] = ("query", "artifact", "semantic-unit", "segment")

SPARSE_INPUT_KINDS: tuple[str, ...] = ("document", "query")
SPARSE_DOCUMENT_INPUT_POLICY = "processing-segment-text-v1"
SPARSE_QUERY_INPUT_POLICY = "exact-query-text-v1"
SPARSE_VECTOR_FORMAT = "scipy-csr-float64-v1"
SPARSE_NORMALIZATION_POLICY = "none-raw-float64-v1"

RERANK_INPUT_POLICY = "bge-dense-top-50-segment-text-v1"


@dataclass(frozen=True)
class RetrievalQuery:
    """One stable query at the document-routing or section-evidence level."""

    query_id: str
    text: str
    level: str

    def __post_init__(self) -> None:
        if not str(self.query_id).strip():
            raise ValueError("a retrieval query requires a query_id")
        if not str(self.text).strip():
            raise ValueError("a retrieval query requires text")
        if self.level not in RETRIEVAL_LEVELS:
            raise ValueError(f"retrieval level must be one of {list(RETRIEVAL_LEVELS)}")


def _iso_value(value: str) -> tuple[str, date | datetime]:
    """Parse exactly ISO 8601 date or datetime text, with no fallback parser."""
    text = str(value)
    try:
        if len(text) == 10:
            return ("date", date.fromisoformat(text))
        return ("datetime", datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError as exc:
        raise ValueError(f"time filter values must be ISO 8601: {value!r}") from exc


@dataclass(frozen=True)
class FilterRequest:
    """One requested prefilter and its explicit unknown-value behavior."""

    axis: str
    values: Sequence[str] = ()
    on_unknown: str = "exclude"
    start: str | None = None
    end: str | None = None

    def __post_init__(self) -> None:
        if self.axis not in FILTER_AXES:
            raise ValueError(f"filter axis must be one of {list(FILTER_AXES)}")
        if self.on_unknown not in UNKNOWN_BEHAVIORS:
            raise ValueError(f"on_unknown must be one of {list(UNKNOWN_BEHAVIORS)}")
        values = tuple(sorted({str(value).strip() for value in self.values if str(value).strip()}))
        object.__setattr__(self, "values", values)
        if self.axis == "time":
            parsed = [_iso_value(value) for value in values]
            start = _iso_value(self.start) if self.start is not None else None
            end = _iso_value(self.end) if self.end is not None else None
            if not values and start is None and end is None:
                raise ValueError("a time filter requires values or an ISO range")
            if start is not None and end is not None:
                if start[0] != end[0] or start[1] > end[1]:
                    raise ValueError("a time filter ISO range is not comparable or is reversed")
            if parsed and len({kind for kind, _ in parsed}) != 1:
                raise ValueError("time filter values must use one ISO precision")
        else:
            if self.start is not None or self.end is not None:
                raise ValueError(f"{self.axis} filters do not accept a time range")
            if not values:
                raise ValueError(f"a {self.axis} filter requires at least one value")

    def as_dict(self) -> dict[str, Any]:
        return {
            "axis": self.axis,
            "values": list(self.values),
            "on_unknown": self.on_unknown,
            "start": self.start,
            "end": self.end,
        }


@dataclass(frozen=True)
class RetrievalSpec:
    """The fixed retrieval methods, limits, model pins, and requested filters."""

    methods: Sequence[str] = PLANNED_RETRIEVAL_METHODS
    candidate_limit: int = RETRIEVAL_CANDIDATE_LIMIT
    rrf_k: int = RETRIEVAL_RRF_K
    fusion_input_depth: int = RETRIEVAL_FUSION_INPUT_DEPTH
    rerank_depth: int = RETRIEVAL_RERANK_DEPTH
    dense_model_id: str = DENSE_MODEL_ID
    sparse_model_id: str = SPARSE_MODEL_ID
    reranker_model_id: str = RERANK_MODEL_ID
    ir_measures_version: str = IR_MEASURES_VERSION
    filters: Sequence[FilterRequest] = ()

    def __post_init__(self) -> None:
        fixed = {
            "methods": (tuple(self.methods), PLANNED_RETRIEVAL_METHODS),
            "candidate_limit": (self.candidate_limit, RETRIEVAL_CANDIDATE_LIMIT),
            "rrf_k": (self.rrf_k, RETRIEVAL_RRF_K),
            "fusion_input_depth": (self.fusion_input_depth, RETRIEVAL_FUSION_INPUT_DEPTH),
            "rerank_depth": (self.rerank_depth, RETRIEVAL_RERANK_DEPTH),
            "dense_model_id": (self.dense_model_id, DENSE_MODEL_ID),
            "sparse_model_id": (self.sparse_model_id, SPARSE_MODEL_ID),
            "reranker_model_id": (self.reranker_model_id, RERANK_MODEL_ID),
            "ir_measures_version": (self.ir_measures_version, IR_MEASURES_VERSION),
        }
        differing = [name for name, (actual, expected) in fixed.items() if actual != expected]
        if differing:
            raise ValueError(f"retrieval fixed configuration differs for {differing}")
        requests = tuple(self.filters)
        if any(not isinstance(request, FilterRequest) for request in requests):
            raise ValueError("retrieval filters must be FilterRequest records")
        axes = [request.axis for request in requests]
        duplicated = sorted({axis for axis in axes if axes.count(axis) > 1})
        if duplicated:
            raise ValueError(f"retrieval filter axes appear twice: {duplicated}")
        ordered = tuple(sorted(requests, key=lambda request: FILTER_AXES.index(request.axis)))
        object.__setattr__(self, "methods", PLANNED_RETRIEVAL_METHODS)
        object.__setattr__(self, "filters", ordered)


@dataclass(frozen=True)
class RetrievalMetricInputs:
    """The answer-derived qrels and rank-derived runs passed to ir-measures.

    This record is an in-memory finalize/check value. It is never written to a
    hit or provider-output table.
    """

    qrels: Mapping[str, Mapping[str, int]]
    runs: Mapping[str, Mapping[str, Mapping[str, float]]]
    zero_relevant_query_ids: tuple[str, ...]


@dataclass(frozen=True)
class RetrievalRunContext:
    """Stored Step 4 inputs and deterministic joins used by retrieval."""

    source_directory: Path
    metadata_rows: Sequence[Mapping[str, Any]] = ()
    authority_edges: Sequence[Mapping[str, Any]] = ()
    graph_edges: Sequence[Mapping[str, Any]] = ()
    concept_assignments: Sequence[Mapping[str, Any]] = ()
    profile_capabilities: Mapping[str, Sequence[str]] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_directory", Path(self.source_directory).resolve())
        for name in ("metadata_rows", "authority_edges", "graph_edges", "concept_assignments"):
            object.__setattr__(self, name, tuple(dict(row) for row in getattr(self, name)))
        capabilities = self.profile_capabilities or {}
        object.__setattr__(
            self,
            "profile_capabilities",
            {
                str(profile): tuple(sorted({str(axis) for axis in axes}))
                for profile, axes in sorted(capabilities.items())
            },
        )


@dataclass(frozen=True)
class RetrievalProviders:
    """The already-landed provider interfaces needed by a retrieval run."""

    embedder: DenseEmbedder
    sparse_encoder: SparseEncoder
    reranker: Reranker
    counter: SemanticUnitCounter


@dataclass(frozen=True)
class RetrievalQueryOutcome:
    """One query's settled retrieval result before runtime persistence."""

    state: str
    hits: tuple[RetrievalHit, ...] = ()
    exclusions: tuple[RetrievalExclusion, ...] = ()
    reason: str = ""
    error: str = ""
    provider: ProviderTotals = ProviderTotals()

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty", "rejected", "failed"}:
            raise ValueError("retrieval query outcome has an unknown state")
        if self.state == "completed" and not self.hits:
            raise ValueError("completed retrieval requires hits; use completed_empty")
        if self.state == "completed_empty" and self.hits:
            raise ValueError("completed_empty retrieval cannot carry hits")
        if self.state == "rejected" and not self.reason:
            raise ValueError("rejected retrieval requires a reason")
        if self.state == "failed" and not self.error:
            raise ValueError("failed retrieval requires an error")
        if self.state != "rejected" and self.reason:
            raise ValueError("only rejected retrieval records a reason")
        if self.state != "failed" and self.error:
            raise ValueError("only failed retrieval records an error")


@dataclass(frozen=True)
class RetrievalOutcome:
    """The public result of one retrieval step run."""

    outcome: RunOutcome
    hits: tuple[RetrievalHit, ...]
    exclusions: tuple[RetrievalExclusion, ...]
    metrics: Mapping[str, Any] | None


@dataclass(frozen=True)
class RetrievalHit:
    """One typed ``retrieval/hits.parquet`` row.

    The row points at a target in the source or segment table.  It does not
    carry answer-derived relevance, exact source text, or source coordinates.
    """

    work_id: str
    query_id: str
    level: str
    method: str
    target_id: str
    artifact_id: str
    segment_id: str | None
    source_table: str
    subject_id: str
    artifact_digest: str
    rank: int
    candidate_universe_size: int
    candidate_input_size: int
    candidate_limit: int
    score: float
    score_kind: str
    dense_rank: int | None = None
    dense_score: float | None = None
    sparse_rank: int | None = None
    sparse_score: float | None = None
    model_id: str | None = None
    model_revision: str | None = None
    rerank_tokenizer_id: str | None = None
    rerank_untruncated_token_count: int | None = None
    rerank_input_limit: int | None = None
    rerank_would_truncate: bool | None = None
    rerank_token_audit_status: str | None = None

    def __post_init__(self) -> None:
        required = (
            self.work_id,
            self.query_id,
            self.target_id,
            self.artifact_id,
            self.source_table,
            self.subject_id,
            self.artifact_digest,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a retrieval hit requires complete work, query, and target identity")
        if self.level not in RETRIEVAL_LEVELS:
            raise ValueError(f"retrieval level must be one of {list(RETRIEVAL_LEVELS)}")
        if self.method not in RETRIEVAL_METHODS:
            raise ValueError(f"retrieval method must be one of {list(RETRIEVAL_METHODS)}")
        if self.score_kind not in SCORE_KINDS:
            raise ValueError(f"score_kind must be one of {list(SCORE_KINDS)}")
        if self.level == "segment" and not self.segment_id:
            raise ValueError("a segment retrieval hit requires a segment_id")
        if self.level == "artifact" and self.segment_id is not None:
            raise ValueError("an artifact retrieval hit does not name a segment")
        if self.rank <= 0:
            raise ValueError("retrieval ranks are 1-based")
        sizes = (self.candidate_universe_size, self.candidate_input_size, self.candidate_limit)
        if any(value < 0 for value in sizes):
            raise ValueError("retrieval candidate sizes cannot be negative")
        if not math.isfinite(self.score):
            raise ValueError("retrieval scores must be finite")
        for name, value in (("dense_rank", self.dense_rank), ("sparse_rank", self.sparse_rank)):
            if value is not None and value <= 0:
                raise ValueError(f"{name} is 1-based")
        for name, value in (("dense_score", self.dense_score), ("sparse_score", self.sparse_score)):
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} must be finite")


@dataclass(frozen=True)
class RetrievalExclusion:
    """One target excluded once, attributed to its first failing filter."""

    work_id: str
    query_id: str
    level: str
    target_id: str
    source_table: str
    subject_id: str
    artifact_digest: str
    filter: str
    reason: str
    detail: str

    def __post_init__(self) -> None:
        required = (
            self.work_id,
            self.query_id,
            self.target_id,
            self.source_table,
            self.subject_id,
            self.artifact_digest,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a retrieval exclusion requires complete work, query, and target identity")
        if self.level not in RETRIEVAL_LEVELS:
            raise ValueError(f"retrieval level must be one of {list(RETRIEVAL_LEVELS)}")
        if self.filter not in FILTER_AXES:
            raise ValueError(f"exclusion filter must be one of {list(FILTER_AXES)}")
        if self.reason not in FILTER_REASONS:
            raise ValueError(f"exclusion reason is not closed: {self.reason!r}")


class SemanticUnitCounter(Protocol):
    """The tokenizer facts needed to reproduce the predecessor's 240-token units."""

    name: str
    version: str

    def count(self, text: str) -> int: ...


@dataclass(frozen=True)
class DenseSourceField:
    """One exact source field used to derive artifact or semantic-unit inputs."""

    artifact_id: str
    artifact_digest: str
    source_table: str
    subject_id: str
    source_field: str
    ordinal: int
    field_sha256: str
    text: str

    def __post_init__(self) -> None:
        required = (
            self.artifact_id,
            self.artifact_digest,
            self.source_table,
            self.subject_id,
            self.source_field,
            self.field_sha256,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a dense source field requires complete source identity")
        if self.ordinal < 0:
            raise ValueError("dense source-field ordinals cannot be negative")
        if sha256_text(self.text) != self.field_sha256:
            raise ValueError("dense source-field text differs from its digest")


@dataclass(frozen=True)
class DenseSemanticUnit:
    """One predecessor-compatible semantic embedding unit over an exact field."""

    unit_id: str
    artifact_id: str
    artifact_digest: str
    source_table: str
    subject_id: str
    source_field: str
    ordinal: int
    field_ordinal: int
    start_char: int
    end_char: int
    text: str
    semantic_text: str
    input_sha256: str
    token_count: int
    tokenizer: str
    tokenizer_version: str
    boundary: str

    def __post_init__(self) -> None:
        required = (
            self.unit_id,
            self.artifact_id,
            self.artifact_digest,
            self.source_table,
            self.subject_id,
            self.source_field,
            self.input_sha256,
            self.tokenizer,
            self.tokenizer_version,
            self.boundary,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a dense semantic unit requires complete identity and tokenizer facts")
        if self.ordinal < 0 or self.field_ordinal < 0:
            raise ValueError("dense semantic-unit ordinals cannot be negative")
        if self.start_char < 0 or self.end_char <= self.start_char:
            raise ValueError("dense semantic-unit coordinates must be a non-empty half-open span")
        if self.token_count <= 0 or self.token_count > DENSE_SEMANTIC_UNIT_TOKENS:
            raise ValueError("dense semantic-unit token count is outside the fixed budget")
        if sha256_text(self.semantic_text) != self.input_sha256:
            raise ValueError("dense semantic-unit input differs from its digest")


@dataclass(frozen=True)
class DenseEmbeddingRow:
    """One typed immutable dense input and its stored normalized vector."""

    work_id: str
    level: str
    input_kind: str
    vector_id: str
    query_id: str | None
    target_id: str | None
    artifact_id: str | None
    segment_id: str | None
    source_table: str | None
    subject_id: str | None
    artifact_digest: str | None
    source_field: str | None
    start_char: int | None
    end_char: int | None
    input_policy: str
    input_sha256: str
    input_text: str
    model_id: str
    model_revision: str
    dimensions: int
    normalization: str
    vector_json: str
    tokenizer_id: str | None
    tokenizer_package_version: str | None
    untruncated_token_count: int | None
    input_limit: int | None
    would_truncate: bool | None
    token_audit_status: str | None
    provider: str
    operation: str
    call_status: str
    provider_invoked: bool
    attempt_count: int
    retry_count: int
    call_input_index: int
    call_json: str

    def __post_init__(self) -> None:
        required = (
            self.work_id,
            self.vector_id,
            self.input_policy,
            self.input_sha256,
            self.model_id,
            self.model_revision,
            self.normalization,
            self.provider,
            self.operation,
            self.call_status,
            self.call_json,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a dense embedding row requires complete input, model, and call identity")
        if self.level not in RETRIEVAL_LEVELS:
            raise ValueError(f"dense embedding level must be one of {list(RETRIEVAL_LEVELS)}")
        if self.input_kind not in DENSE_INPUT_KINDS:
            raise ValueError(f"dense input kind must be one of {list(DENSE_INPUT_KINDS)}")
        if self.input_kind == "query":
            if not self.query_id or self.target_id is not None:
                raise ValueError("a dense query row names only its query")
        elif not self.target_id:
            raise ValueError("a dense target row requires a target_id")
        if self.input_kind == "artifact" and (not self.artifact_id or self.segment_id is not None):
            raise ValueError("a dense artifact row requires only artifact identity")
        if self.input_kind == "segment" and (not self.artifact_id or not self.segment_id):
            raise ValueError("a dense segment row requires artifact and segment identity")
        if self.input_kind == "semantic-unit" and (
            not self.artifact_id or not self.source_field or self.start_char is None or self.end_char is None
        ):
            raise ValueError("a dense semantic-unit row requires exact source coordinates")
        if self.start_char is not None and (
            self.end_char is None or self.start_char < 0 or self.end_char <= self.start_char
        ):
            raise ValueError("dense embedding source coordinates are invalid")
        if self.dimensions <= 0:
            raise ValueError("dense embedding dimensions must be positive")
        if self.normalization != DENSE_NORMALIZATION_POLICY:
            raise ValueError("dense embeddings require the fixed L2 normalization policy")
        if sha256_text(self.input_text) != self.input_sha256:
            raise ValueError("dense embedding text differs from its input digest")
        if self.untruncated_token_count is not None and self.untruncated_token_count < 0:
            raise ValueError("dense token counts cannot be negative")
        if self.input_limit is not None and self.input_limit <= 0:
            raise ValueError("dense input limits must be positive")
        if self.would_truncate is not None and type(self.would_truncate) is not bool:
            raise ValueError("dense truncation flags require exact bool values")
        if self.model_id != DENSE_MODEL_ID or self.model_revision != DEFAULT_DENSE_REVISION:
            raise ValueError("dense embedding model differs from the fixed retrieval pin")
        try:
            call = json.loads(self.call_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("dense call details are not valid JSON") from exc
        if not isinstance(call, dict) or canonical_json(call) != self.call_json:
            raise ValueError("dense call details must be one canonical JSON object")
        if self.input_kind == "segment":
            if (
                self.provider != "derived"
                or self.operation != "overlap-character-weighted-mean"
                or self.call_status != "completed_derived"
                or type(self.provider_invoked) is not bool
                or self.provider_invoked
                or self.attempt_count != 0
                or self.retry_count != 0
                or self.call_input_index != -1
            ):
                raise ValueError("derived dense segment rows require provider-free composition provenance")
            if (
                str(call.get("provider") or "") != self.provider
                or str(call.get("operation") or "") != self.operation
                or str(call.get("status") or "") != self.call_status
                or str(call.get("model_id") or "") != self.model_id
                or str(call.get("model_revision") or "") != self.model_revision
                or call.get("provider_invoked") is not False
                or int(call.get("attempt_count", -1)) != 0
                or int(call.get("retry_count", -1)) != 0
                or int(call.get("dimensions", -1)) != self.dimensions
            ):
                raise ValueError("derived dense call details differ from stored composition provenance")
            _ = self.vector
            return
        if type(self.provider_invoked) is not bool or not self.provider_invoked:
            raise ValueError("stored dense provider rows require a completed invocation")
        if self.provider != DENSE_PROVIDER or self.operation != "dense-embedding" or self.call_status != "completed":
            raise ValueError("stored dense provider rows require completed dense provider provenance")
        if self.attempt_count <= 0 or self.retry_count < 0 or self.call_input_index < 0:
            raise ValueError("dense attempt counts and call input index are invalid")
        if (
            str(call.get("provider") or "") != self.provider
            or str(call.get("operation") or "") != self.operation
            or str(call.get("status") or "") != self.call_status
            or str(call.get("model_id") or "") != self.model_id
            or str(call.get("revision") or "") != self.model_revision
            or (str(call.get("tokenizer_id") or "") or None) != self.tokenizer_id
            or call.get("provider_invoked") is not True
        ):
            raise ValueError("dense call details differ from stored provider provenance")
        try:
            call_dimensions = int(call.get("dimensions", -1))
            call_input_count = int(call.get("input_count", -1))
            call_attempt_count = int(call.get("attempt_count", -1))
            call_retry_count = int(call.get("retry_count", -1))
            call_input_limit = int(call["max_input_tokens"]) if call.get("max_input_tokens") is not None else None
        except (TypeError, ValueError) as exc:
            raise ValueError("dense call details contain invalid counts") from exc
        if (
            call_dimensions != self.dimensions
            or call_input_count <= self.call_input_index
            or call_attempt_count != self.attempt_count
            or call_retry_count != self.retry_count
            or call_input_limit != self.input_limit
            or (str(call.get("tokenizer_package_version") or "") or None) != self.tokenizer_package_version
            or (str(call.get("token_audit_status") or "") or None) != self.token_audit_status
        ):
            raise ValueError("dense call details differ from stored input provenance")
        token_counts = call.get("token_counts")
        truncation = call.get("inputs_over_limit")
        if (
            not isinstance(token_counts, (list, tuple))
            or len(token_counts) != call_input_count
            or not isinstance(truncation, (list, tuple))
            or len(truncation) != call_input_count
        ):
            raise ValueError("dense call details carry invalid per-input provenance")
        raw_token_count = token_counts[self.call_input_index]
        try:
            call_token_count = int(raw_token_count) if raw_token_count is not None else None
        except (TypeError, ValueError) as exc:
            raise ValueError("dense call details carry an invalid per-input token count") from exc
        call_would_truncate = truncation[self.call_input_index]
        if call_would_truncate is not None and type(call_would_truncate) is not bool:
            raise ValueError("dense call details carry an invalid per-input truncation fact")
        if call_token_count != self.untruncated_token_count or call_would_truncate != self.would_truncate:
            raise ValueError("dense call details differ from stored per-input provenance")
        _ = self.vector

    @property
    def vector(self) -> tuple[float, ...]:
        """Return the stored portable vector after strict finite-shape checks."""
        try:
            values = json.loads(self.vector_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("dense vector is not valid JSON") from exc
        if not isinstance(values, list):
            raise ValueError("dense vector must be a JSON array")
        try:
            vector = tuple(float(value) for value in values)
        except (TypeError, ValueError) as exc:
            raise ValueError("dense vector values must be numeric") from exc
        if len(vector) != self.dimensions or any(not math.isfinite(value) for value in vector):
            raise ValueError("dense vector differs from its finite declared shape")
        return vector


@dataclass(frozen=True)
class DenseRetrievalOutcome:
    """The settled result of one artifact or segment dense-search entry point."""

    state: str
    level: str
    hits: tuple[RetrievalHit, ...]
    embeddings: tuple[DenseEmbeddingRow, ...]

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty"}:
            raise ValueError("dense retrieval outcome has an unknown state")
        if self.level not in RETRIEVAL_LEVELS:
            raise ValueError("dense retrieval outcome has an unknown level")


class DenseProviderError(RuntimeError):
    """A dense provider failed before any new immutable rows were written."""


@dataclass(frozen=True)
class SparseEmbeddingRow:
    """One typed immutable sparse input and its stored portable vector."""

    work_id: str
    level: str
    input_kind: str
    vector_id: str
    query_id: str | None
    target_id: str | None
    artifact_id: str | None
    segment_id: str | None
    source_table: str | None
    subject_id: str | None
    artifact_digest: str | None
    input_policy: str
    input_sha256: str
    input_text: str
    task: str
    model_id: str
    model_revision: str
    dimensions: int
    vector_format: str
    normalization: str
    indices_json: str
    values_json: str
    active_dimensions: int
    tokenizer_id: str | None
    tokenizer_package_version: str | None
    untruncated_token_count: int | None
    input_limit: int | None
    would_truncate: bool | None
    token_audit_status: str | None
    provider: str
    operation: str
    call_status: str
    provider_invoked: bool
    attempt_count: int
    retry_count: int
    call_input_index: int
    call_json: str

    def __post_init__(self) -> None:
        required = (
            self.work_id,
            self.vector_id,
            self.input_policy,
            self.input_sha256,
            self.model_id,
            self.model_revision,
            self.vector_format,
            self.normalization,
            self.provider,
            self.operation,
            self.call_status,
            self.call_json,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a sparse embedding row requires complete input, model, and call identity")
        if self.level != "segment":
            raise ValueError("sparse embedding rows are section-evidence inputs")
        if self.input_kind not in SPARSE_INPUT_KINDS or self.task != self.input_kind:
            raise ValueError("sparse input kind and asymmetric task must agree")
        if self.task == "query":
            if not self.query_id or any(
                value is not None
                for value in (
                    self.target_id,
                    self.artifact_id,
                    self.segment_id,
                    self.source_table,
                    self.subject_id,
                    self.artifact_digest,
                )
            ):
                raise ValueError("a sparse query row names only its query")
            if self.input_policy != SPARSE_QUERY_INPUT_POLICY:
                raise ValueError("a sparse query row requires the exact-query policy")
        else:
            target_identity = (
                self.target_id,
                self.artifact_id,
                self.segment_id,
                self.source_table,
                self.subject_id,
                self.artifact_digest,
            )
            if self.query_id is not None or any(not str(value or "").strip() for value in target_identity):
                raise ValueError("a sparse document row requires complete segment identity")
            if self.target_id != self.segment_id:
                raise ValueError("a sparse document target must be its segment")
            if self.input_policy != SPARSE_DOCUMENT_INPUT_POLICY:
                raise ValueError("a sparse document row requires the segment-text policy")
        if sha256_text(self.input_text) != self.input_sha256:
            raise ValueError("sparse embedding text differs from its input digest")
        if self.dimensions <= 0:
            raise ValueError("sparse embedding dimensions must be positive")
        if self.model_id != SPARSE_MODEL_ID or self.model_revision != SPARSE_MODEL_ID.rsplit("@", 1)[-1]:
            raise ValueError("sparse embedding model differs from the fixed retrieval pin")
        if self.vector_format != SPARSE_VECTOR_FORMAT:
            raise ValueError("sparse embeddings require the fixed CSR vector format")
        if self.normalization != SPARSE_NORMALIZATION_POLICY:
            raise ValueError("sparse embeddings must preserve raw model weights")
        if self.active_dimensions < 0:
            raise ValueError("sparse active-dimension counts cannot be negative")
        if self.untruncated_token_count is not None and self.untruncated_token_count < 0:
            raise ValueError("sparse token counts cannot be negative")
        if self.input_limit is not None and self.input_limit <= 0:
            raise ValueError("sparse input limits must be positive")
        if self.would_truncate is not None and type(self.would_truncate) is not bool:
            raise ValueError("sparse truncation flags require exact bool values")
        if type(self.provider_invoked) is not bool or not self.provider_invoked:
            raise ValueError("stored sparse provider rows require a completed invocation")
        if self.operation != "sparse-encoding" or self.call_status != "completed":
            raise ValueError("stored sparse provider rows require a completed sparse encoding")
        if self.attempt_count <= 0 or self.retry_count < 0 or self.call_input_index < 0:
            raise ValueError("sparse call counts and input index are invalid")
        try:
            call = json.loads(self.call_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("sparse call details are not valid JSON") from exc
        if not isinstance(call, dict):
            raise ValueError("sparse call details must be a JSON object")
        if str(call.get("task") or "") != self.task:
            raise ValueError("sparse call details differ from the asymmetric task")
        if (
            str(call.get("provider") or "") != self.provider
            or str(call.get("operation") or "") != self.operation
            or str(call.get("status") or "") != self.call_status
            or str(call.get("model_id") or "") != self.model_id
            or str(call.get("revision") or "") != self.model_revision
        ):
            raise ValueError("sparse call details differ from stored provider provenance")
        try:
            call_dimensions = int(call.get("dimensions", -1))
            call_input_count = int(call.get("input_count", -1))
        except (TypeError, ValueError) as exc:
            raise ValueError("sparse call details contain invalid counts") from exc
        if (
            call_dimensions != self.dimensions
            or call_input_count <= self.call_input_index
            or (str(call.get("tokenizer_id") or "") or None) != self.tokenizer_id
        ):
            raise ValueError("sparse call details differ from stored input provenance")
        _ = self.vector

    @property
    def vector(self) -> SparseVector:
        """Return the stored vector after strict portable-shape validation."""
        try:
            indices_value = json.loads(self.indices_json)
            values_value = json.loads(self.values_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("sparse vector values are not valid JSON") from exc
        if not isinstance(indices_value, list) or not isinstance(values_value, list):
            raise ValueError("sparse vector indices and values must be JSON arrays")
        if self.indices_json != canonical_json(indices_value) or self.values_json != canonical_json(values_value):
            raise ValueError("sparse vector JSON must use the canonical representation")
        if any(type(value) is not int for value in indices_value):
            raise ValueError("sparse vector indices must be exact integers")
        if any(type(value) not in {int, float} for value in values_value):
            raise ValueError("sparse vector values must be numeric")
        vector = validate_sparse_vector(
            SparseVector(
                dimensions=self.dimensions,
                indices=tuple(indices_value),
                values=tuple(float(value) for value in values_value),
            ),
            self.dimensions,
        )
        if len(vector.indices) != self.active_dimensions:
            raise ValueError("sparse vector differs from its active-dimension count")
        return vector


@dataclass(frozen=True)
class SparseRetrievalOutcome:
    """The settled result of one learned-sparse segment search."""

    state: str
    level: str
    hits: tuple[RetrievalHit, ...]
    embeddings: tuple[SparseEmbeddingRow, ...]

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty"}:
            raise ValueError("sparse retrieval outcome has an unknown state")
        if self.level != "segment":
            raise ValueError("sparse retrieval is section evidence, not artifact routing")


class SparseProviderError(RuntimeError):
    """A sparse provider failed before any new immutable rows were written."""


@dataclass(frozen=True)
class RerankScoreRow:
    """One exact reranker input, score, rank, token audit, and provider call."""

    work_id: str
    group_key: str
    source_work_id: str
    query_id: str
    level: str
    candidate_ids_sha256: str
    request_sha256: str
    candidate_index: int
    candidate_count: int
    target_id: str
    artifact_id: str
    segment_id: str
    source_table: str
    subject_id: str
    artifact_digest: str
    candidate_universe_size: int
    dense_candidate_input_size: int
    dense_rank: int
    dense_score: float
    query_input_sha256: str
    query_text: str
    input_policy: str
    input_sha256: str
    input_text: str
    rerank_score: float
    rerank_rank: int
    model_id: str
    model_revision: str
    tokenizer_id: str
    tokenizer_package_version: str | None
    untruncated_token_count: int
    input_limit: int
    would_truncate: bool
    token_audit_status: str
    provider: str
    package_name: str
    package_version: str
    operation: str
    call_status: str
    provider_invoked: bool
    group_attempt: int
    provider_attempt_count: int
    retry_count: int
    call_input_index: int
    call_json: str

    def __post_init__(self) -> None:
        required = (
            self.work_id,
            self.group_key,
            self.source_work_id,
            self.query_id,
            self.target_id,
            self.artifact_id,
            self.segment_id,
            self.source_table,
            self.subject_id,
            self.artifact_digest,
            self.candidate_ids_sha256,
            self.request_sha256,
            self.query_input_sha256,
            self.input_policy,
            self.input_sha256,
            self.model_id,
            self.model_revision,
            self.tokenizer_id,
            self.token_audit_status,
            self.provider,
            self.package_name,
            self.package_version,
            self.operation,
            self.call_status,
            self.call_json,
        )
        if any(not str(value).strip() for value in required):
            raise ValueError("a rerank score row requires complete group, input, and provider identity")
        if self.level != "segment" or self.target_id != self.segment_id:
            raise ValueError("reranking is section evidence over segment targets")
        if self.input_policy != RERANK_INPUT_POLICY:
            raise ValueError("rerank score rows require the fixed dense-top-50 input policy")
        if not (0 < self.candidate_count <= RETRIEVAL_RERANK_DEPTH):
            raise ValueError("rerank candidate count is outside the fixed depth")
        if not (0 <= self.candidate_index < self.candidate_count):
            raise ValueError("rerank candidate index is outside its group")
        if self.dense_rank != self.candidate_index + 1:
            raise ValueError("rerank candidate order differs from the BGE dense rank")
        if not (1 <= self.rerank_rank <= self.candidate_count):
            raise ValueError("rerank rank is outside its group")
        if self.candidate_universe_size < self.dense_candidate_input_size:
            raise ValueError("rerank candidate sizes are inconsistent")
        if self.dense_candidate_input_size < self.candidate_count:
            raise ValueError("rerank input exceeds its dense candidate list")
        if not math.isfinite(self.dense_score) or not math.isfinite(self.rerank_score):
            raise ValueError("rerank rows require finite dense and cross-encoder scores")
        if sha256_text(self.query_text) != self.query_input_sha256:
            raise ValueError("rerank query text differs from its input digest")
        if sha256_text(self.input_text) != self.input_sha256:
            raise ValueError("rerank candidate text differs from its input digest")
        if self.model_id != RERANK_MODEL_ID or self.model_revision != DEFAULT_RERANK_REVISION:
            raise ValueError("rerank score model differs from the fixed retrieval pin")
        if self.input_limit != RERANK_MAX_SEQ_LENGTH:
            raise ValueError("rerank score input limit differs from the fixed retrieval pin")
        if self.untruncated_token_count < 0:
            raise ValueError("rerank pair-token counts cannot be negative")
        if type(self.would_truncate) is not bool:
            raise ValueError("rerank truncation facts require exact bool values")
        if self.would_truncate != (self.untruncated_token_count > self.input_limit):
            raise ValueError("rerank truncation fact differs from the exact pair-token count")
        if (
            self.operation != "rerank"
            or self.call_status != "completed"
            or type(self.provider_invoked) is not bool
            or not self.provider_invoked
        ):
            raise ValueError("stored rerank rows require a completed provider invocation")
        if (
            self.group_attempt <= 0
            or self.provider_attempt_count <= 0
            or self.retry_count < 0
            or self.call_input_index != self.candidate_index
        ):
            raise ValueError("rerank attempt counts or call input index are invalid")
        try:
            call = json.loads(self.call_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("rerank call details are not valid JSON") from exc
        if not isinstance(call, dict) or canonical_json(call) != self.call_json:
            raise ValueError("rerank call details must be one canonical JSON object")
        if (
            str(call.get("provider") or "") != self.provider
            or str(call.get("operation") or "") != self.operation
            or str(call.get("status") or "") != self.call_status
            or str(call.get("model_id") or "") != self.model_id
            or str(call.get("revision") or "") != self.model_revision
            or str(call.get("tokenizer_id") or "") != self.tokenizer_id
        ):
            raise ValueError("rerank call details differ from stored provider provenance")
        try:
            call_count = int(call.get("candidate_count", -1))
            call_limit = int(call.get("max_input_tokens", -1))
        except (TypeError, ValueError) as exc:
            raise ValueError("rerank call details contain invalid candidate or input counts") from exc
        if call_count != self.candidate_count or call_limit != self.input_limit:
            raise ValueError("rerank call details differ from the stored candidate group")


@dataclass(frozen=True)
class RerankRetrievalOutcome:
    """The settled result of one fixed-depth rerank group."""

    state: str
    work_id: str
    hits: tuple[RetrievalHit, ...]
    scores: tuple[RerankScoreRow, ...]

    def __post_init__(self) -> None:
        if self.state not in {"completed", "completed_empty"}:
            raise ValueError("rerank retrieval outcome has an unknown state")
        if not str(self.work_id).strip():
            raise ValueError("rerank retrieval outcome requires a work identity")
        if self.state == "completed_empty" and (self.hits or self.scores):
            raise ValueError("completed-empty reranking cannot carry hits or scores")


class RerankProviderError(RuntimeError):
    """A reranker failed and left its exact group resumable."""


HIT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("query_id", "string"),
    ("level", "string"),
    ("method", "string"),
    ("target_id", "string"),
    ("artifact_id", "string"),
    ("segment_id", "string"),
    ("source_table", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("rank", "int64"),
    ("candidate_universe_size", "int64"),
    ("candidate_input_size", "int64"),
    ("candidate_limit", "int64"),
    ("score", "double"),
    ("score_kind", "string"),
    ("dense_rank", "int64"),
    ("dense_score", "double"),
    ("sparse_rank", "int64"),
    ("sparse_score", "double"),
    ("model_id", "string"),
    ("model_revision", "string"),
    ("rerank_tokenizer_id", "string"),
    ("rerank_untruncated_token_count", "int64"),
    ("rerank_input_limit", "int64"),
    ("rerank_would_truncate", "bool"),
    ("rerank_token_audit_status", "string"),
)

RETRIEVAL_EXCLUSION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("query_id", "string"),
    ("level", "string"),
    ("target_id", "string"),
    ("source_table", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("filter", "string"),
    ("reason", "string"),
    ("detail", "string"),
)

DENSE_EMBEDDING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("level", "string"),
    ("input_kind", "string"),
    ("vector_id", "string"),
    ("query_id", "string"),
    ("target_id", "string"),
    ("artifact_id", "string"),
    ("segment_id", "string"),
    ("source_table", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("source_field", "string"),
    ("start_char", "int64"),
    ("end_char", "int64"),
    ("input_policy", "string"),
    ("input_sha256", "string"),
    ("input_text", "string"),
    ("model_id", "string"),
    ("model_revision", "string"),
    ("dimensions", "int64"),
    ("normalization", "string"),
    ("vector_json", "string"),
    ("tokenizer_id", "string"),
    ("tokenizer_package_version", "string"),
    ("untruncated_token_count", "int64"),
    ("input_limit", "int64"),
    ("would_truncate", "bool"),
    ("token_audit_status", "string"),
    ("provider", "string"),
    ("operation", "string"),
    ("call_status", "string"),
    ("provider_invoked", "bool"),
    ("attempt_count", "int64"),
    ("retry_count", "int64"),
    ("call_input_index", "int64"),
    ("call_json", "string"),
)

SPARSE_EMBEDDING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("level", "string"),
    ("input_kind", "string"),
    ("vector_id", "string"),
    ("query_id", "string"),
    ("target_id", "string"),
    ("artifact_id", "string"),
    ("segment_id", "string"),
    ("source_table", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("input_policy", "string"),
    ("input_sha256", "string"),
    ("input_text", "string"),
    ("task", "string"),
    ("model_id", "string"),
    ("model_revision", "string"),
    ("dimensions", "int64"),
    ("vector_format", "string"),
    ("normalization", "string"),
    ("indices_json", "string"),
    ("values_json", "string"),
    ("active_dimensions", "int64"),
    ("tokenizer_id", "string"),
    ("tokenizer_package_version", "string"),
    ("untruncated_token_count", "int64"),
    ("input_limit", "int64"),
    ("would_truncate", "bool"),
    ("token_audit_status", "string"),
    ("provider", "string"),
    ("operation", "string"),
    ("call_status", "string"),
    ("provider_invoked", "bool"),
    ("attempt_count", "int64"),
    ("retry_count", "int64"),
    ("call_input_index", "int64"),
    ("call_json", "string"),
)

RERANK_SCORE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("work_id", "string"),
    ("group_key", "string"),
    ("source_work_id", "string"),
    ("query_id", "string"),
    ("level", "string"),
    ("candidate_ids_sha256", "string"),
    ("request_sha256", "string"),
    ("candidate_index", "int64"),
    ("candidate_count", "int64"),
    ("target_id", "string"),
    ("artifact_id", "string"),
    ("segment_id", "string"),
    ("source_table", "string"),
    ("subject_id", "string"),
    ("artifact_digest", "string"),
    ("candidate_universe_size", "int64"),
    ("dense_candidate_input_size", "int64"),
    ("dense_rank", "int64"),
    ("dense_score", "double"),
    ("query_input_sha256", "string"),
    ("query_text", "string"),
    ("input_policy", "string"),
    ("input_sha256", "string"),
    ("input_text", "string"),
    ("rerank_score", "double"),
    ("rerank_rank", "int64"),
    ("model_id", "string"),
    ("model_revision", "string"),
    ("tokenizer_id", "string"),
    ("tokenizer_package_version", "string"),
    ("untruncated_token_count", "int64"),
    ("input_limit", "int64"),
    ("would_truncate", "bool"),
    ("token_audit_status", "string"),
    ("provider", "string"),
    ("package_name", "string"),
    ("package_version", "string"),
    ("operation", "string"),
    ("call_status", "string"),
    ("provider_invoked", "bool"),
    ("group_attempt", "int64"),
    ("provider_attempt_count", "int64"),
    ("retry_count", "int64"),
    ("call_input_index", "int64"),
    ("call_json", "string"),
)


def retrieval_plan_facts(
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
) -> dict[str, Any]:
    """Return deterministic plan facts for this fixed retrieval configuration."""
    by_id: dict[str, RetrievalQuery] = {}
    for query in queries:
        if query.query_id in by_id:
            raise PlanError(f"retrieval query {query.query_id!r} appears twice")
        by_id[query.query_id] = query
    return {
        "format_version": RETRIEVAL_FORMAT_VERSION,
        "methods": list(spec.methods),
        "candidate_limit": spec.candidate_limit,
        "rrf_k": spec.rrf_k,
        "fusion_input_depth": spec.fusion_input_depth,
        "rerank_depth": spec.rerank_depth,
        "models": {
            "dense": spec.dense_model_id,
            "sparse": spec.sparse_model_id,
            "reranker": spec.reranker_model_id,
        },
        "ir_measures_version": spec.ir_measures_version,
        "filters": [request.as_dict() for request in spec.filters],
        "queries": [
            {
                "query_id": query.query_id,
                "level": query.level,
                "text_sha256": sha256_text(query.text),
            }
            for query in sorted(by_id.values(), key=lambda item: item.query_id)
        ],
        "immutable_provider_outputs": [
            DENSE_EMBEDDING_TABLE,
            SPARSE_EMBEDDING_TABLE,
            RERANK_SCORE_TABLE,
        ],
        "derived_outputs": [RETRIEVAL_HIT_TABLE, RETRIEVAL_EXCLUSION_TABLE],
    }


def build_retrieval_metric_inputs(
    hits: Sequence[RetrievalHit],
    answers: Mapping[str, Sequence[str]],
    *,
    methods: Sequence[str] = PLANNED_RETRIEVAL_METHODS,
) -> RetrievalMetricInputs:
    """Build deterministic qrels and tie-free runs from 1-based ranks.

    A query with no relevant target receives the predecessor's deliberately
    unreachable sentinel. That query therefore stays in every aggregate
    denominator instead of disappearing from evaluation.
    """
    selected_methods = tuple(methods)
    if len(selected_methods) != len(set(selected_methods)):
        raise ValueError("retrieval metric methods must be unique")
    unknown_methods = sorted(set(selected_methods) - set(RETRIEVAL_METHODS))
    if unknown_methods:
        raise ValueError(f"retrieval metric methods are unknown: {unknown_methods}")

    normalized_answers: dict[str, tuple[str, ...]] = {}
    for raw_query_id, raw_targets in answers.items():
        query_id = str(raw_query_id).strip()
        if not query_id:
            raise ValueError("retrieval metric answers require non-empty query IDs")
        if isinstance(raw_targets, (str, bytes)) or not isinstance(raw_targets, Sequence):
            raise ValueError(f"retrieval metric answers for {query_id!r} must be a sequence of target IDs")
        targets = tuple(sorted({str(target).strip() for target in raw_targets if str(target).strip()}))
        normalized_answers[query_id] = targets
    unexpected_queries = sorted({hit.query_id for hit in hits} - set(normalized_answers))
    if unexpected_queries:
        raise ValueError(f"retrieval hits have no answer entry: {unexpected_queries}")

    qrels: dict[str, dict[str, int]] = {}
    zero_relevant: list[str] = []
    for query_id, targets in sorted(normalized_answers.items()):
        if targets:
            qrels[query_id] = {target_id: 1 for target_id in targets}
        else:
            qrels[query_id] = {f"{query_id}:missing-relevant-segment": 1}
            zero_relevant.append(query_id)

    runs: dict[str, dict[str, dict[str, float]]] = {}
    for method in selected_methods:
        method_runs: dict[str, dict[str, float]] = {}
        for query_id in qrels:
            rows = sorted(
                (hit for hit in hits if hit.method == method and hit.query_id == query_id),
                key=lambda hit: (hit.rank, hit.target_id),
            )
            target_ids = [hit.target_id for hit in rows]
            ranks = [hit.rank for hit in rows]
            sentinel = f"{query_id}:missing-relevant-segment"
            if sentinel in target_ids:
                raise ValueError(f"retrieval metric sentinel is not a real target: {sentinel}")
            if len(target_ids) != len(set(target_ids)):
                raise ValueError(f"retrieval metric run {method}/{query_id} has a duplicate target")
            if len(ranks) != len(set(ranks)):
                raise ValueError(f"retrieval metric run {method}/{query_id} has a duplicate rank")
            if ranks != list(range(1, len(rows) + 1)):
                raise ValueError(f"retrieval metric run {method}/{query_id} ranks must be contiguous from one")
            method_runs[query_id] = {hit.target_id: float(len(rows) - hit.rank + 1) for hit in rows}
        runs[method] = method_runs
    return RetrievalMetricInputs(
        qrels=qrels,
        runs=runs,
        zero_relevant_query_ids=tuple(zero_relevant),
    )


def _independent_retrieval_metrics(
    qrels: Mapping[str, Mapping[str, int]],
    runs: Mapping[str, Mapping[str, float]],
) -> dict[str, float]:
    """Calculate the fixed binary measures without ir-measures.

    This deliberately separate implementation makes package or input-shape
    drift visible. It consumes only the run ordering, never retrieval scores.
    """
    query_ids = tuple(sorted(qrels))
    if not query_ids:
        raise ValueError("retrieval metrics require at least one answered query")
    totals = {
        **{f"recall_at_{cutoff}": 0.0 for cutoff in RETRIEVAL_RECALL_CUTOFFS},
        **{f"precision_at_{cutoff}": 0.0 for cutoff in RETRIEVAL_PRECISION_CUTOFFS},
        "mrr": 0.0,
        "ndcg_at_5": 0.0,
        "ndcg_at_10": 0.0,
    }
    for query_id in query_ids:
        relevant = {target_id for target_id, value in qrels[query_id].items() if int(value) > 0}
        ranked = [
            target_id
            for target_id, _ in sorted(
                runs.get(query_id, {}).items(),
                key=lambda item: (-float(item[1]), item[0]),
            )
        ]
        for cutoff in RETRIEVAL_RECALL_CUTOFFS:
            found = len(relevant & set(ranked[:cutoff]))
            totals[f"recall_at_{cutoff}"] += found / len(relevant)
        for cutoff in RETRIEVAL_PRECISION_CUTOFFS:
            found = len(relevant & set(ranked[:cutoff]))
            totals[f"precision_at_{cutoff}"] += found / cutoff
        first_relevant = next(
            (rank for rank, target_id in enumerate(ranked, start=1) if target_id in relevant),
            None,
        )
        totals["mrr"] += 0.0 if first_relevant is None else 1.0 / first_relevant
        for cutoff in (5, 10):
            dcg = sum(
                1.0 / math.log2(rank + 1)
                for rank, target_id in enumerate(ranked[:cutoff], start=1)
                if target_id in relevant
            )
            ideal_count = min(len(relevant), cutoff)
            ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, ideal_count + 1))
            totals[f"ndcg_at_{cutoff}"] += dcg / ideal
    return {name: value / len(query_ids) for name, value in totals.items()}


def retrieval_metrics(
    hits: Sequence[RetrievalHit],
    answers: Mapping[str, Sequence[str]],
    *,
    methods: Sequence[str] = PLANNED_RETRIEVAL_METHODS,
) -> dict[str, Any]:
    """Calculate pinned aggregate metrics and require independent agreement."""
    inputs = build_retrieval_metric_inputs(hits, answers, methods=methods)
    try:
        import ir_measures
        from ir_measures import P, R, RR, nDCG
    except ImportError as exc:
        raise RuntimeError("retrieval evaluation requires the 'evaluation' extra") from exc
    if ir_measures.__version__ != IR_MEASURES_VERSION:
        raise RuntimeError(
            "ir-measures version differs from the pinned retrieval contract: "
            f"{ir_measures.__version__} != {IR_MEASURES_VERSION}"
        )
    measures = {
        **{f"recall_at_{cutoff}": R @ cutoff for cutoff in RETRIEVAL_RECALL_CUTOFFS},
        **{f"precision_at_{cutoff}": P @ cutoff for cutoff in RETRIEVAL_PRECISION_CUTOFFS},
        "mrr": RR,
        "ndcg_at_5": nDCG @ 5,
        "ndcg_at_10": nDCG @ 10,
    }
    by_method: dict[str, dict[str, float]] = {}
    for method, runs in inputs.runs.items():
        calculated = ir_measures.calc_aggregate(list(measures.values()), inputs.qrels, runs)
        measured = {name: float(calculated[measure]) for name, measure in measures.items()}
        independent = _independent_retrieval_metrics(inputs.qrels, runs)
        differing = [
            name
            for name in measures
            if not math.isclose(measured[name], independent[name], rel_tol=1e-12, abs_tol=1e-12)
        ]
        if differing:
            raise RuntimeError(f"ir-measures differs from the independent retrieval calculation: {differing}")
        by_method[method] = measured
    return {
        "format_version": RETRIEVAL_FORMAT_VERSION,
        "metric_provider": IR_MEASURES_PROVIDER,
        "ir_measures_version": IR_MEASURES_VERSION,
        "query_count": len(inputs.qrels),
        "zero_relevant_query_count": len(inputs.zero_relevant_query_ids),
        "methods": by_method,
    }


_UNIVERSE_COLUMNS: tuple[str, ...] = (
    "target_id",
    "artifact_id",
    "segment_id",
    "source_table",
    "subject_id",
    "artifact_digest",
    "profile_id",
    "subject_type",
    "access_scope",
    "access_basis",
    "text",
    "text_sha256",
    "slices_json",
)

_UNIVERSE_ORDER = "ORDER BY source_table, subject_id, artifact_digest, target_id, artifact_id, segment_id"


def _universe_sql(level: str) -> str:
    if level == "artifact":
        return f"""
            SELECT
                artifact_id AS target_id,
                artifact_id,
                CAST(NULL AS VARCHAR) AS segment_id,
                source_table,
                subject_id,
                content_sha256 AS artifact_digest,
                profile_id,
                subject_type,
                access_scope,
                access_basis,
                CAST(NULL AS VARCHAR) AS text,
                CAST(NULL AS VARCHAR) AS text_sha256,
                CAST(NULL AS VARCHAR) AS slices_json
            FROM read_parquet(?)
            {_UNIVERSE_ORDER}
        """
    if level == "segment":
        return f"""
            SELECT *
            FROM (
                SELECT
                    s.segment_id AS target_id,
                    s.artifact_id,
                    s.segment_id,
                    a.source_table,
                    a.subject_id,
                    a.content_sha256 AS artifact_digest,
                    a.profile_id,
                    a.subject_type,
                    a.access_scope,
                    a.access_basis,
                    s.text,
                    s.text_sha256,
                    s.slices_json
                FROM read_parquet(?) AS s
                INNER JOIN read_parquet(?) AS a
                    ON a.artifact_id = s.artifact_id
                   AND a.content_sha256 = s.artifact_sha256
                   AND a.source_table = s.source_table
                   AND a.subject_id = s.subject_id
            ) AS candidates
            {_UNIVERSE_ORDER}
        """
    raise PlanError(f"retrieval level must be one of {list(RETRIEVAL_LEVELS)}")


def construct_candidate_universe(
    run_directory: Path,
    *,
    level: str,
) -> list[dict[str, Any]]:
    """Read the public Step 4 tables into one deterministically ordered universe.

    DuckDB is in-memory and read-only here.  No database file is created.
    """
    root = Path(run_directory)
    artifacts = root / ARTIFACT_TABLE
    if not artifacts.is_file():
        raise PlanError(f"candidate universe requires {ARTIFACT_TABLE}")
    parameters: list[str]
    if level == "artifact":
        parameters = [str(artifacts)]
    elif level == "segment":
        segments = root / SEGMENT_TABLE
        if not segments.is_file():
            raise PlanError(f"candidate universe requires {SEGMENT_TABLE}")
        parameters = [str(segments), str(artifacts)]
    else:
        raise PlanError(f"retrieval level must be one of {list(RETRIEVAL_LEVELS)}")
    connection = duckdb.connect(database=":memory:")
    try:
        result = connection.execute(_universe_sql(level), parameters)
        rows = result.fetchall()
    except duckdb.Error as exc:
        raise PlanError(f"candidate universe could not read the Step 4 tables: {type(exc).__name__}") from exc
    finally:
        connection.close()
    return [dict(zip(_UNIVERSE_COLUMNS, row, strict=True)) for row in rows]


def _normalized_identifier(value: object) -> str:
    return " ".join(str(value or "").split())


def candidate_metadata_join_key(
    source_table: str,
    row: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the exact Step 4 ``(source_table, subject_id)`` join key.

    The public profile supplies the identity columns.  Regulations.gov docket
    IDs use the same canonical syntax as ``source.py``; Unified Agenda uses the
    profile's two-column canonical JSON identity, so one RIN in two editions is
    never collapsed.
    """
    try:
        profile = profile_for_table(source_table)
    except Exception as exc:
        raise PlanError(f"candidate metadata names unsupported profile table {source_table!r}") from exc
    values = [_normalized_identifier(row.get(column)) for column in profile.id_columns]
    if any(not value for value in values):
        raise PlanError(f"candidate metadata for {source_table} lacks identity columns {list(profile.id_columns)}")
    if len(values) == 1:
        subject_id = values[0]
        if source_table == "dockets":
            normalized = normalize_regsgov_identifier(subject_id)
            if normalized is None:
                raise PlanError(f"candidate metadata has an invalid Regulations.gov docket ID: {subject_id!r}")
            subject_id = normalized
    else:
        subject_id = canonical_json(dict(zip(profile.id_columns, values, strict=True)))
    return (source_table, subject_id)


def candidate_metadata_row(
    source_table: str,
    row: Mapping[str, Any],
    *,
    version_field: str | None = None,
    jurisdiction_field: str | None = None,
    date_field: str | None = None,
    agency_field: str | None = None,
) -> dict[str, Any]:
    """Build one derived sidecar row from declared source-profile fields."""
    profile = profile_for_table(source_table)
    table, subject_id = candidate_metadata_join_key(source_table, row)
    result: dict[str, Any] = {
        "source_table": table,
        "subject_id": subject_id,
        "profile_id": profile.profile_id,
    }
    for output, source in (
        ("version", version_field),
        ("jurisdiction", jurisdiction_field),
        ("source_date", date_field),
        ("agency_id", agency_field),
    ):
        if source is not None:
            raw = row.get(source)
            result[output] = None if raw is None else _normalized_identifier(raw)
    return result


def _metadata_by_key(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("source_table") or ""), str(row.get("subject_id") or ""))
        if not all(key):
            raise PlanError("candidate metadata rows require source_table and subject_id")
        if key in by_key:
            raise PlanError(f"candidate metadata join key appears twice: {key}")
        by_key[key] = dict(row)
    return by_key


def _edge_key(row: Mapping[str, Any]) -> tuple[str, str]:
    source_table = str(row.get("source_table") or "")
    subject_id = str(row.get("subject_id") or "")
    if source_table and subject_id:
        return (source_table, subject_id)
    if row.get("rin") is not None or row.get("agenda_edition") is not None:
        return candidate_metadata_join_key("unified_agenda", row)
    raise PlanError("edge rows require a source/subject join key")


def _authority_id(row: Mapping[str, Any]) -> str | None:
    if value := str(row.get("authority_id") or "").strip():
        return value
    title = str(row.get("usc_title") or "").strip()
    section = str(row.get("usc_section") or "").strip()
    if title and section:
        return f"usc:{title}:{section}"
    if value := str(row.get("pl_number") or "").strip():
        return f"pl:{value}"
    return str(row.get("authority_raw") or "").strip() or None


def _joined_values(
    rows: Sequence[Mapping[str, Any]],
    *,
    value: str,
) -> dict[tuple[str, str], tuple[str, ...]]:
    joined: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        key = _edge_key(row)
        item = _authority_id(row) if value == "authority" else str(row.get(value) or "").strip() or None
        if item:
            joined[key].add(item)
    return {key: tuple(sorted(items)) for key, items in joined.items()}


def _capabilities(
    candidates: Sequence[Mapping[str, Any]],
    metadata: Mapping[tuple[str, str], Mapping[str, Any]],
    declared: Mapping[str, Sequence[str]],
    authority: Mapping[tuple[str, str], Sequence[str]],
    graph: Mapping[tuple[str, str], Sequence[str]],
    concept_assignments: Sequence[Mapping[str, Any]],
) -> dict[str, set[str]]:
    capabilities: dict[str, set[str]] = {str(profile): set(axes) for profile, axes in declared.items()}
    assignments_present = any(
        row.get("agency_id") is not None and row.get("concept_id") is not None for row in concept_assignments
    )
    for candidate in candidates:
        profile_id = str(candidate.get("profile_id") or "")
        source_table = str(candidate.get("source_table") or "")
        try:
            profile = profile_for_table(source_table)
        except Exception as exc:
            raise PlanError(f"candidate names unsupported source profile {source_table!r}") from exc
        if profile.profile_id != profile_id:
            raise PlanError(
                f"candidate profile {profile_id!r} does not match {source_table!r} profile {profile.profile_id!r}"
            )
        supported = capabilities.setdefault(profile_id, set())
        supported.update({"identity", "version", "access"})
        key = (source_table, str(candidate.get("subject_id") or ""))
        sidecar = metadata.get(key, {})
        if "jurisdiction" in sidecar:
            supported.add("jurisdiction")
        if "source_date" in sidecar:
            supported.add("time")
        if key in authority:
            supported.add("authority")
        if key in graph:
            supported.add("graph")
        if "agency_id" in sidecar and assignments_present:
            supported.add("agency-scoped-concepts")
    return capabilities


def _preflight_filters(
    candidates: Sequence[Mapping[str, Any]],
    spec: RetrievalSpec,
    capabilities: Mapping[str, set[str]],
) -> None:
    access = next((request for request in spec.filters if request.axis == "access"), None)
    if access is not None:
        unsupported = sorted(set(access.values) - {"public"})
        if unsupported:
            raise PlanError(f"access filter values are unsupported; only public is available: {unsupported}")
    profiles = sorted({str(row.get("profile_id") or "") for row in candidates})
    for request in spec.filters:
        for profile in profiles:
            if request.axis not in capabilities.get(profile, set()):
                raise PlanError(f"filter {request.axis!r} is unsupported by profile {profile!r}")


def _concepts_by_agency(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[str, ...]]:
    values: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        agency = str(row.get("agency_id") or "").strip()
        concept = str(row.get("concept_id") or "").strip()
        if agency and concept:
            values[agency].add(concept)
    return {agency: tuple(sorted(concepts)) for agency, concepts in values.items()}


def _axis_values(
    candidate: Mapping[str, Any],
    axis: str,
    *,
    sidecar: Mapping[str, Any],
    authorities: Mapping[tuple[str, str], Sequence[str]],
    graph: Mapping[tuple[str, str], Sequence[str]],
    concepts: Mapping[str, Sequence[str]],
) -> tuple[str, ...]:
    key = (str(candidate.get("source_table") or ""), str(candidate.get("subject_id") or ""))
    if axis == "identity":
        return tuple(
            dict.fromkeys(
                str(value)
                for value in (
                    candidate.get("target_id"),
                    candidate.get("artifact_id"),
                    candidate.get("subject_id"),
                    candidate.get("profile_id"),
                    candidate.get("source_table"),
                    f"{key[0]}:{key[1]}",
                )
                if value is not None and str(value)
            )
        )
    if axis == "version":
        return tuple(
            dict.fromkeys(
                str(value)
                for value in (
                    candidate.get("artifact_digest"),
                    sidecar.get("version"),
                    sidecar.get("edition"),
                )
                if value is not None and str(value)
            )
        )
    if axis == "authority":
        return tuple(authorities.get(key, ()))
    if axis == "jurisdiction":
        value = sidecar.get("jurisdiction")
        return (str(value),) if value is not None and str(value) else ()
    if axis == "time":
        value = sidecar.get("source_date")
        if value is None or not str(value):
            return ()
        try:
            _iso_value(str(value))
        except ValueError:
            return ()
        return (str(value),)
    if axis == "access":
        value = candidate.get("access_scope")
        return (str(value),) if value is not None and str(value) else ()
    if axis == "graph":
        return tuple(graph.get(key, ()))
    if axis == "agency-scoped-concepts":
        agency = str(sidecar.get("agency_id") or "")
        return tuple(concepts.get(agency, ())) if agency else ()
    raise AssertionError(f"unhandled filter axis {axis}")


def _matches_time(values: Sequence[str], request: FilterRequest) -> bool:
    if request.values and any(value in request.values for value in values):
        return True
    if request.start is None and request.end is None:
        return False
    for value in values:
        try:
            parsed = _iso_value(value)
        except ValueError:
            continue
        start = _iso_value(request.start) if request.start is not None else None
        end = _iso_value(request.end) if request.end is not None else None
        if start is not None and (start[0] != parsed[0] or parsed[1] < start[1]):
            continue
        if end is not None and (end[0] != parsed[0] or parsed[1] > end[1]):
            continue
        return True
    return False


def apply_prefilters(
    candidates: Sequence[Mapping[str, Any]],
    spec: RetrievalSpec,
    *,
    query: RetrievalQuery,
    work_id: str,
    metadata_rows: Sequence[Mapping[str, Any]] = (),
    authority_edges: Sequence[Mapping[str, Any]] = (),
    graph_edges: Sequence[Mapping[str, Any]] = (),
    concept_assignments: Sequence[Mapping[str, Any]] = (),
    profile_capabilities: Mapping[str, Sequence[str]] | None = None,
) -> tuple[list[dict[str, Any]], list[RetrievalExclusion], dict[str, dict[str, int]]]:
    """Apply requested filters conjunctively and attribute each exclusion once.

    Every filter is measured for every candidate, even when an earlier filter
    already excluded that candidate.  That makes per-axis counts complete while
    the exclusion table still records only the first failure in
    :data:`FILTER_AXES` order.
    """
    if not str(work_id).strip():
        raise PlanError("retrieval filtering requires a work_id")
    rows = [dict(candidate) for candidate in candidates]
    wrong_level = [
        row.get("target_id") for row in rows if (query.level == "artifact") != (row.get("segment_id") is None)
    ]
    if wrong_level:
        raise PlanError(f"candidate universe level does not match query {query.query_id!r}: {wrong_level[:5]}")
    metadata = _metadata_by_key(metadata_rows)
    authorities = _joined_values(authority_edges, value="authority")
    graph = _joined_values(graph_edges, value="graph_id")
    concepts = _concepts_by_agency(concept_assignments)
    capabilities = _capabilities(
        rows,
        metadata,
        profile_capabilities or {},
        authorities,
        graph,
        concept_assignments,
    )
    _preflight_filters(rows, spec, capabilities)

    counts: dict[str, dict[str, int]] = {
        request.axis: {
            "candidates": len(rows),
            "known": 0,
            "matched": 0,
            "unknown": 0,
            "included_unknown": 0,
            "excluded": 0,
            "attributed_exclusions": 0,
        }
        for request in spec.filters
    }
    included: list[dict[str, Any]] = []
    exclusions: list[RetrievalExclusion] = []
    for candidate in rows:
        key = (str(candidate.get("source_table") or ""), str(candidate.get("subject_id") or ""))
        sidecar = metadata.get(key, {})
        first_failure: tuple[FilterRequest, str] | None = None
        for request in spec.filters:
            values = _axis_values(
                candidate,
                request.axis,
                sidecar=sidecar,
                authorities=authorities,
                graph=graph,
                concepts=concepts,
            )
            axis_counts = counts[request.axis]
            if not values:
                axis_counts["unknown"] += 1
                if request.on_unknown == "include":
                    axis_counts["included_unknown"] += 1
                    continue
                axis_counts["excluded"] += 1
                if first_failure is None:
                    first_failure = (request, f"unknown-{request.axis}")
                continue
            axis_counts["known"] += 1
            matched = (
                _matches_time(values, request) if request.axis == "time" else bool(set(values) & set(request.values))
            )
            if matched:
                axis_counts["matched"] += 1
                continue
            axis_counts["excluded"] += 1
            if first_failure is None:
                first_failure = (request, f"mismatch-{request.axis}")
        if first_failure is None:
            included.append(candidate)
            continue
        request, reason = first_failure
        counts[request.axis]["attributed_exclusions"] += 1
        exclusions.append(
            RetrievalExclusion(
                work_id=work_id,
                query_id=query.query_id,
                level=query.level,
                target_id=str(candidate.get("target_id") or ""),
                source_table=str(candidate.get("source_table") or ""),
                subject_id=str(candidate.get("subject_id") or ""),
                artifact_digest=str(candidate.get("artifact_digest") or ""),
                filter=request.axis,
                reason=reason,
                detail=(
                    f"candidate has no known {request.axis} value"
                    if reason.startswith("unknown-")
                    else f"candidate {request.axis} does not match the requested values"
                ),
            )
        )
    return included, exclusions, counts


class _VisibleDenseText(HTMLParser):
    """Collect visible text exactly as the predecessor's semantic path did."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _dense_semantic_text(text: str) -> str:
    if "<" not in text or ">" not in text:
        return " ".join(text.split())
    parser = _VisibleDenseText()
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return " ".join(text.split())
    visible = " ".join("".join(parser.parts).split())
    return visible or " ".join(text.split())


def derive_dense_semantic_units(
    source_fields: Sequence[DenseSourceField],
    *,
    counter: SemanticUnitCounter,
) -> tuple[DenseSemanticUnit, ...]:
    """Derive the predecessor's exact 240/80 semantic units from public fields."""
    result: list[DenseSemanticUnit] = []
    grouped: dict[str, list[DenseSourceField]] = defaultdict(list)
    for field in source_fields:
        grouped[field.artifact_id].append(field)
    for artifact_id in sorted(grouped):
        artifact_ordinal = 0
        fields = sorted(
            grouped[artifact_id],
            key=lambda item: (item.ordinal, item.source_field),
        )
        if len({field.ordinal for field in fields}) != len(fields):
            raise PlanError(f"dense source-field ordinals appear twice for artifact {artifact_id}")
        identity = {(field.artifact_digest, field.source_table, field.subject_id) for field in fields}
        if len(identity) != 1:
            raise PlanError(f"dense source fields disagree on artifact identity for {artifact_id}")
        for field in fields:
            leaves = segment_text(
                field.source_field,
                field.text,
                max_tokens=DENSE_SEMANTIC_UNIT_TOKENS,
                min_tokens=DENSE_SEMANTIC_UNIT_MIN_TOKENS,
                token_counter=counter,
                policy_version=DENSE_SEMANTIC_UNIT_POLICY,
                identity_scope={"artifact_digest": field.artifact_digest},
            )
            for leaf in leaves:
                semantic = _dense_semantic_text(leaf.text) or leaf.text
                result.append(
                    DenseSemanticUnit(
                        unit_id=leaf.segment_id,
                        artifact_id=field.artifact_id,
                        artifact_digest=field.artifact_digest,
                        source_table=field.source_table,
                        subject_id=field.subject_id,
                        source_field=field.source_field,
                        ordinal=artifact_ordinal,
                        field_ordinal=leaf.ordinal,
                        start_char=leaf.start_char,
                        end_char=leaf.end_char,
                        text=leaf.text,
                        semantic_text=semantic,
                        input_sha256=sha256_text(semantic),
                        token_count=leaf.token_count,
                        tokenizer=leaf.tokenizer,
                        tokenizer_version=leaf.tokenizer_version,
                        boundary=leaf.boundary,
                    )
                )
                artifact_ordinal += 1
    return tuple(result)


def compose_dense_vector(
    vectors: Sequence[Sequence[float]],
    weights: Sequence[float] | None = None,
) -> tuple[float, ...]:
    """Return the finite weighted mean, with the predecessor's zero-weight fallback."""
    if not vectors:
        return ()
    dimensions = len(vectors[0])
    if dimensions <= 0 or any(len(vector) != dimensions for vector in vectors):
        raise ValueError("dense composition inputs must have one non-empty shape")
    matrix = np.asarray(vectors, dtype=np.float64)
    if matrix.ndim != 2 or not np.isfinite(matrix).all():
        raise ValueError("dense composition inputs must be finite vectors")
    if weights is None or not weights:
        effective = np.ones(matrix.shape[0], dtype=np.float64)
    else:
        effective = np.asarray(weights, dtype=np.float64)
    if (
        effective.ndim != 1
        or effective.shape[0] != matrix.shape[0]
        or not np.isfinite(effective).all()
        or np.any(effective < 0)
    ):
        raise ValueError("dense composition weights must be finite, non-negative, and match vectors")
    total = float(np.sum(effective))
    if total <= 0:
        effective = np.ones(matrix.shape[0], dtype=np.float64)
        total = float(matrix.shape[0])
    return tuple(float(value) for value in np.sum(matrix * effective[:, np.newaxis], axis=0) / total)


def _l2_normalized(vector: Sequence[float]) -> tuple[float, ...]:
    values = np.asarray(vector, dtype=np.float64)
    if values.ndim != 1 or not values.size or not np.isfinite(values).all():
        raise ValueError("dense vectors must be non-empty finite arrays")
    norm = float(np.linalg.norm(values))
    normalized = values / norm if norm else np.zeros_like(values)
    return tuple(float(value) for value in normalized)


def rank_dense_vectors(
    target_ids: Sequence[str],
    target_vectors: Sequence[Sequence[float]],
    query_vector: Sequence[float],
    *,
    limit: int = RETRIEVAL_CANDIDATE_LIMIT,
) -> tuple[tuple[str, float], ...]:
    """L2-normalize and rank one grain-blind array set with deterministic ties."""
    if limit < 0:
        raise ValueError("dense ranking limit cannot be negative")
    if len(target_ids) != len(target_vectors):
        raise ValueError("dense ranking target IDs and vectors differ in count")
    if len(set(target_ids)) != len(target_ids) or any(not str(value).strip() for value in target_ids):
        raise ValueError("dense ranking target IDs must be non-empty and unique")
    normalized_query = np.asarray(_l2_normalized(query_vector), dtype=np.float64)
    if target_vectors:
        normalized_targets = np.asarray(
            [_l2_normalized(vector) for vector in target_vectors],
            dtype=np.float64,
        )
        if normalized_targets.shape[1] != normalized_query.shape[0]:
            raise ValueError("dense ranking query and target dimensions differ")
        scores = np.dot(normalized_targets, normalized_query)
    else:
        scores = np.asarray([], dtype=np.float64)
    if not np.isfinite(scores).all():
        raise ValueError("dense ranking produced a non-finite score")
    ranked = sorted(
        zip(target_ids, (float(score) for score in scores), strict=True),
        key=lambda item: (-item[1], item[0]),
    )
    return tuple(ranked[:limit])


def dense_source_fields_from_segments(
    segment_rows: Sequence[Mapping[str, Any]],
) -> tuple[DenseSourceField, ...]:
    """Rebuild exact fields from public Step 4 slices, rejecting gaps or conflicts.

    Raw ``processing/segments.parquet`` rows carry ``ordinal`` and therefore
    preserve source-field order for whole-artifact routing. Candidate-universe
    rows remain sufficient for segment evidence, where field order does not
    affect unit boundaries or composition.
    """
    ordered_rows = sorted(
        (dict(row) for row in segment_rows),
        key=lambda row: (
            str(row.get("artifact_id") or ""),
            int(row.get("ordinal") or 0),
            str(row.get("target_id") or row.get("segment_id") or ""),
        ),
    )
    characters: dict[tuple[str, str], dict[int, str]] = defaultdict(dict)
    metadata: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    field_order: dict[str, list[str]] = defaultdict(list)
    for row in ordered_rows:
        artifact_id = str(row.get("artifact_id") or "")
        artifact_digest = str(row.get("artifact_digest") or row.get("artifact_sha256") or "")
        source_table = str(row.get("source_table") or "")
        subject_id = str(row.get("subject_id") or "")
        if not all((artifact_id, artifact_digest, source_table, subject_id)):
            raise PlanError("segment slices lack complete artifact identity")
        try:
            slices = json.loads(str(row.get("slices_json") or ""))
        except (TypeError, ValueError) as exc:
            raise PlanError("segment slices are not valid JSON") from exc
        if not isinstance(slices, list):
            raise PlanError("segment slices must be a JSON array")
        for raw_slice in slices:
            if not isinstance(raw_slice, dict):
                raise PlanError("a segment slice must be a JSON object")
            source_field = str(raw_slice.get("source_field") or "")
            field_sha256 = str(raw_slice.get("field_sha256") or "")
            text = str(raw_slice.get("text") or "")
            try:
                start = int(raw_slice["start_char"])
                end = int(raw_slice["end_char"])
            except (KeyError, TypeError, ValueError) as exc:
                raise PlanError("a segment slice lacks integer source coordinates") from exc
            if not source_field or not field_sha256 or start < 0 or end <= start or len(text) != end - start:
                raise PlanError("a segment slice cannot reconstruct its exact source field")
            key = (artifact_id, source_field)
            item_metadata = (artifact_digest, source_table, subject_id, field_sha256)
            if key in metadata and metadata[key] != item_metadata:
                raise PlanError(f"segment slices disagree on source-field identity for {key}")
            metadata[key] = item_metadata
            if source_field not in field_order[artifact_id]:
                field_order[artifact_id].append(source_field)
            for position, character in enumerate(text, start=start):
                prior = characters[key].setdefault(position, character)
                if prior != character:
                    raise PlanError(f"overlapping segment slices disagree at {source_field}:{position}")
    fields: list[DenseSourceField] = []
    for artifact_id in sorted(field_order):
        for ordinal, source_field in enumerate(field_order[artifact_id]):
            key = (artifact_id, source_field)
            positions = characters[key]
            if not positions:
                raise PlanError(f"source field {key} has no segment characters")
            expected = set(range(max(positions) + 1))
            if set(positions) != expected:
                raise PlanError(f"source field {key} has gaps in public segment slices")
            text = "".join(positions[index] for index in range(max(positions) + 1))
            artifact_digest, source_table, subject_id, field_sha256 = metadata[key]
            fields.append(
                DenseSourceField(
                    artifact_id=artifact_id,
                    artifact_digest=artifact_digest,
                    source_table=source_table,
                    subject_id=subject_id,
                    source_field=source_field,
                    ordinal=ordinal,
                    field_sha256=field_sha256,
                    text=text,
                )
            )
    return tuple(fields)


@dataclass(frozen=True)
class _DenseInput:
    level: str
    input_kind: str
    vector_id: str
    query_id: str | None
    target_id: str | None
    artifact_id: str | None
    segment_id: str | None
    source_table: str | None
    subject_id: str | None
    artifact_digest: str | None
    source_field: str | None
    start_char: int | None
    end_char: int | None
    input_policy: str
    input_sha256: str
    input_text: str


def _provider_vector_id(level: str, input_sha256: str, model_id: str) -> str:
    identity = canonical_json(
        {
            "level": level,
            "input_sha256": input_sha256,
            "model_id": model_id,
            "normalization": DENSE_NORMALIZATION_POLICY,
        }
    )
    return f"dense_vector_{hashlib.sha256(identity.encode()).hexdigest()[:24]}"


def _query_input(query: RetrievalQuery, model_id: str) -> _DenseInput:
    digest = sha256_text(query.text)
    return _DenseInput(
        level=query.level,
        input_kind="query",
        vector_id=_provider_vector_id(query.level, digest, model_id),
        query_id=query.query_id,
        target_id=None,
        artifact_id=None,
        segment_id=None,
        source_table=None,
        subject_id=None,
        artifact_digest=None,
        source_field=None,
        start_char=None,
        end_char=None,
        input_policy="exact-query-text-v1",
        input_sha256=digest,
        input_text=query.text,
    )


def _artifact_inputs(
    candidates: Sequence[Mapping[str, Any]],
    source_fields: Sequence[DenseSourceField],
    model_id: str,
) -> tuple[_DenseInput, ...]:
    fields_by_artifact: dict[str, list[DenseSourceField]] = defaultdict(list)
    for field in source_fields:
        fields_by_artifact[field.artifact_id].append(field)
    inputs: list[_DenseInput] = []
    for candidate in candidates:
        target_id = str(candidate.get("target_id") or "")
        artifact_id = str(candidate.get("artifact_id") or "")
        if not target_id or target_id != artifact_id or candidate.get("segment_id") is not None:
            raise PlanError("artifact dense search received a non-artifact candidate")
        fields = sorted(
            fields_by_artifact.get(artifact_id, ()),
            key=lambda item: (item.ordinal, item.source_field),
        )
        if not fields:
            raise PlanError(f"artifact {artifact_id} has no exact source fields for dense routing")
        identity = (
            str(candidate.get("artifact_digest") or ""),
            str(candidate.get("source_table") or ""),
            str(candidate.get("subject_id") or ""),
        )
        if any((field.artifact_digest, field.source_table, field.subject_id) != identity for field in fields):
            raise PlanError(f"artifact dense source fields disagree with candidate {artifact_id}")
        text = "\n\n".join(f"[SOURCE_FIELD {field.source_field}]\n{field.text}" for field in fields if field.text)
        if not text:
            raise PlanError(f"artifact {artifact_id} has no non-empty all-profile dense input")
        digest = sha256_text(text)
        inputs.append(
            _DenseInput(
                level="artifact",
                input_kind="artifact",
                vector_id=_provider_vector_id("artifact", digest, model_id),
                query_id=None,
                target_id=target_id,
                artifact_id=artifact_id,
                segment_id=None,
                source_table=identity[1],
                subject_id=identity[2],
                artifact_digest=identity[0],
                source_field=None,
                start_char=None,
                end_char=None,
                input_policy=DENSE_ARTIFACT_INPUT_POLICY,
                input_sha256=digest,
                input_text=text,
            )
        )
    return tuple(inputs)


def _semantic_unit_inputs(
    units: Sequence[DenseSemanticUnit],
    model_id: str,
) -> tuple[_DenseInput, ...]:
    return tuple(
        _DenseInput(
            level="segment",
            input_kind="semantic-unit",
            vector_id=_provider_vector_id("segment", unit.input_sha256, model_id),
            query_id=None,
            target_id=unit.unit_id,
            artifact_id=unit.artifact_id,
            segment_id=None,
            source_table=unit.source_table,
            subject_id=unit.subject_id,
            artifact_digest=unit.artifact_digest,
            source_field=unit.source_field,
            start_char=unit.start_char,
            end_char=unit.end_char,
            input_policy=DENSE_SEMANTIC_UNIT_POLICY,
            input_sha256=unit.input_sha256,
            input_text=unit.semantic_text,
        )
        for unit in units
    )


def _arrow_type(kind: str) -> Any:
    import pyarrow as pa

    types = {
        "string": pa.string(),
        "int64": pa.int64(),
        "double": pa.float64(),
        "bool": pa.bool_(),
    }
    try:
        return types[kind]
    except KeyError:
        raise ValueError(f"unknown retrieval column kind {kind!r}") from None


def _coerce(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "string":
        return str(value)
    if kind == "int64":
        return int(value)
    if kind == "double":
        return float(value)
    if kind == "bool":
        if type(value) is not bool:
            raise ValueError("retrieval boolean columns require exact bool values")
        return value
    raise ValueError(f"unknown retrieval column kind {kind!r}")


def _write_table(
    path: Path,
    columns: Sequence[tuple[str, str]],
    rows: Sequence[Mapping[str, Any]],
) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    prepared = [{name: _coerce(row.get(name), kind) for name, kind in columns} for row in rows]
    schema = pa.schema([pa.field(name, _arrow_type(kind)) for name, kind in columns])
    data = {name: [row[name] for row in prepared] for name, _ in columns}
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        pq.write_table(pa.Table.from_pydict(data, schema=schema), temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _dense_row_key(row: DenseEmbeddingRow) -> tuple[str, str, str, str, str, str]:
    return (
        row.work_id,
        row.level,
        row.input_kind,
        row.query_id or "",
        row.target_id or "",
        row.vector_id,
    )


def read_dense_embedding_rows(run_directory: Path) -> tuple[DenseEmbeddingRow, ...]:
    """Read and validate the immutable dense input/output table."""
    path = Path(run_directory) / DENSE_EMBEDDING_TABLE
    if not path.is_file():
        return ()
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    expected = [name for name, _ in DENSE_EMBEDDING_COLUMNS]
    if table.schema.names != expected:
        raise PlanError("dense embedding table schema differs from the fixed typed columns")
    try:
        rows = tuple(DenseEmbeddingRow(**row) for row in table.to_pylist())
    except (TypeError, ValueError) as exc:
        raise PlanError(f"dense embedding table contains an invalid row: {type(exc).__name__}") from exc
    keys = [_dense_row_key(row) for row in rows]
    if len(keys) != len(set(keys)):
        raise PlanError("dense embedding table contains duplicate input rows")
    return tuple(sorted(rows, key=_dense_row_key))


def write_dense_embedding_rows(
    run_directory: Path,
    rows: Sequence[DenseEmbeddingRow],
) -> Path:
    """Write deterministic dense rows, including a correctly typed zero-row table."""
    ordered = sorted(rows, key=_dense_row_key)
    keys = [_dense_row_key(row) for row in ordered]
    if len(keys) != len(set(keys)):
        raise ValueError("dense embedding rows contain duplicate input identities")
    return _write_table(
        Path(run_directory) / DENSE_EMBEDDING_TABLE,
        DENSE_EMBEDDING_COLUMNS,
        [dataclasses.asdict(row) for row in ordered],
    )


def _sequence_call_fact(
    call: Mapping[str, Any],
    name: str,
    count: int,
) -> tuple[Any, ...]:
    value = call.get(name)
    if not isinstance(value, (list, tuple)) or len(value) != count:
        raise DenseProviderError(f"dense provider call has invalid {name}")
    return tuple(value)


def _provider_row(
    item: _DenseInput,
    *,
    work_id: str,
    vector: Sequence[float],
    call: Mapping[str, Any],
    input_index: int,
) -> DenseEmbeddingRow:
    normalized = _l2_normalized(vector)
    count = int(call.get("input_count", -1))
    token_counts = _sequence_call_fact(call, "token_counts", count)
    over_limit = _sequence_call_fact(call, "inputs_over_limit", count)
    token_count = token_counts[input_index]
    would_truncate = over_limit[input_index]
    if token_count is not None:
        try:
            token_count = int(token_count)
        except (TypeError, ValueError) as exc:
            raise DenseProviderError("dense provider call has an invalid token count") from exc
    if would_truncate is not None and type(would_truncate) is not bool:
        raise DenseProviderError("dense provider call has an invalid truncation flag")
    model_id = str(call.get("model_id") or "")
    revision = str(call.get("revision") or "")
    if not revision and "@" in model_id:
        revision = model_id.rsplit("@", 1)[1]
    input_limit_raw = call.get("max_input_tokens")
    input_limit = int(input_limit_raw) if input_limit_raw is not None else None
    return DenseEmbeddingRow(
        work_id=work_id,
        level=item.level,
        input_kind=item.input_kind,
        vector_id=item.vector_id,
        query_id=item.query_id,
        target_id=item.target_id,
        artifact_id=item.artifact_id,
        segment_id=item.segment_id,
        source_table=item.source_table,
        subject_id=item.subject_id,
        artifact_digest=item.artifact_digest,
        source_field=item.source_field,
        start_char=item.start_char,
        end_char=item.end_char,
        input_policy=item.input_policy,
        input_sha256=item.input_sha256,
        input_text=item.input_text,
        model_id=model_id,
        model_revision=revision,
        dimensions=len(normalized),
        normalization=DENSE_NORMALIZATION_POLICY,
        vector_json=canonical_json(list(normalized)),
        tokenizer_id=str(call.get("tokenizer_id") or "") or None,
        tokenizer_package_version=(str(call.get("tokenizer_package_version") or "") or None),
        untruncated_token_count=token_count,
        input_limit=input_limit,
        would_truncate=would_truncate,
        token_audit_status=str(call.get("token_audit_status") or "") or None,
        provider=str(call.get("provider") or ""),
        operation=str(call.get("operation") or ""),
        call_status=str(call.get("status") or ""),
        provider_invoked=call.get("provider_invoked") is True,
        attempt_count=int(call.get("attempt_count", -1)),
        retry_count=int(call.get("retry_count", -1)),
        call_input_index=input_index,
        call_json=canonical_json(dict(call)),
    )


def _row_matches_input(
    row: DenseEmbeddingRow,
    item: _DenseInput,
    *,
    work_id: str,
    embedder: DenseEmbedder,
) -> bool:
    return (
        row.work_id == work_id
        and row.level == item.level
        and row.input_kind == item.input_kind
        and row.vector_id == item.vector_id
        and row.query_id == item.query_id
        and row.target_id == item.target_id
        and row.artifact_id == item.artifact_id
        and row.segment_id == item.segment_id
        and row.source_table == item.source_table
        and row.subject_id == item.subject_id
        and row.artifact_digest == item.artifact_digest
        and row.source_field == item.source_field
        and row.start_char == item.start_char
        and row.end_char == item.end_char
        and row.input_policy == item.input_policy
        and row.input_sha256 == item.input_sha256
        and row.input_text == item.input_text
        and row.model_id == embedder.model_id
        and row.model_revision == embedder.model_id.rsplit("@", 1)[-1]
        and row.dimensions == embedder.dimensions
        and row.tokenizer_id == embedder.tokenizer_id
        and row.input_limit == embedder.max_input_tokens
        and row.provider == embedder.provider
        and row.operation == "dense-embedding"
        and row.call_status == "completed"
        and row.provider_invoked is True
        and row.attempt_count > 0
    )


def _clone_provider_row(
    base: DenseEmbeddingRow,
    item: _DenseInput,
    *,
    work_id: str,
) -> DenseEmbeddingRow:
    return dataclasses.replace(
        base,
        work_id=work_id,
        level=item.level,
        input_kind=item.input_kind,
        vector_id=item.vector_id,
        query_id=item.query_id,
        target_id=item.target_id,
        artifact_id=item.artifact_id,
        segment_id=item.segment_id,
        source_table=item.source_table,
        subject_id=item.subject_id,
        artifact_digest=item.artifact_digest,
        source_field=item.source_field,
        start_char=item.start_char,
        end_char=item.end_char,
        input_policy=item.input_policy,
        input_sha256=item.input_sha256,
        input_text=item.input_text,
    )


def _materialize_provider_rows(
    inputs: Sequence[_DenseInput],
    *,
    work_id: str,
    embedder: DenseEmbedder,
    existing: Sequence[DenseEmbeddingRow],
) -> tuple[DenseEmbeddingRow, ...]:
    if embedder.model_id != DENSE_MODEL_ID:
        raise PlanError(f"dense embedder model differs from the fixed retrieval pin: {embedder.model_id}")
    if embedder.provider != DENSE_PROVIDER:
        raise PlanError(f"dense embedder provider differs from the fixed retrieval provider: {embedder.provider}")
    if embedder.dimensions <= 0:
        raise PlanError("dense embedder dimensions must be positive")
    exact = {_dense_row_key(row): row for row in existing}
    cache: dict[tuple[str, str, str], DenseEmbeddingRow] = {}
    for row in existing:
        if row.input_kind == "segment":
            continue
        key = (row.work_id, row.level, row.vector_id)
        prior = cache.setdefault(key, row)
        if prior.vector != row.vector or prior.input_sha256 != row.input_sha256:
            raise PlanError("dense vector identity resolves to conflicting stored provider outputs")

    settled: list[DenseEmbeddingRow] = []
    missing_by_vector: dict[str, _DenseInput] = {}
    pending: list[_DenseInput] = []
    for item in inputs:
        key = (
            work_id,
            item.level,
            item.input_kind,
            item.query_id or "",
            item.target_id or "",
            item.vector_id,
        )
        row = exact.get(key)
        if row is not None:
            if not _row_matches_input(row, item, work_id=work_id, embedder=embedder):
                raise PlanError("stored dense input identity drifted within one work item")
            settled.append(row)
            continue
        cached = cache.get((work_id, item.level, item.vector_id))
        if cached is not None:
            if cached.input_sha256 != item.input_sha256 or cached.input_text != item.input_text:
                raise PlanError("stored dense vector identity resolves to another exact input")
            if (
                cached.model_id != embedder.model_id
                or cached.model_revision != embedder.model_id.rsplit("@", 1)[-1]
                or cached.dimensions != embedder.dimensions
                or cached.tokenizer_id != embedder.tokenizer_id
                or cached.input_limit != embedder.max_input_tokens
                or cached.provider != embedder.provider
                or cached.operation != "dense-embedding"
                or cached.call_status != "completed"
                or cached.provider_invoked is not True
                or cached.attempt_count <= 0
            ):
                raise PlanError("stored dense vector call facts differ from the injected provider")
            settled.append(_clone_provider_row(cached, item, work_id=work_id))
            continue
        pending.append(item)
        prior = missing_by_vector.setdefault(item.vector_id, item)
        if prior.input_sha256 != item.input_sha256 or prior.input_text != item.input_text:
            raise PlanError("dense vector identity collision")

    representatives = sorted(
        missing_by_vector.values(),
        key=lambda item: (item.input_sha256, item.input_text, item.vector_id),
    )
    produced: dict[str, DenseEmbeddingRow] = {}
    if representatives:
        texts = [item.input_text for item in representatives]
        try:
            response = embedder.embed(texts)
        except Exception as exc:
            raise DenseProviderError(f"dense provider failed before persistence: {type(exc).__name__}") from exc
        if len(response.vectors) != len(representatives):
            raise DenseProviderError("dense provider returned the wrong vector count")
        call = response.call
        if not isinstance(call, Mapping):
            raise DenseProviderError("dense provider returned invalid call details")
        if str(call.get("model_id") or "") != embedder.model_id:
            raise DenseProviderError("dense provider call differs from the injected model pin")
        if str(call.get("revision") or "") != embedder.model_id.rsplit("@", 1)[-1]:
            raise DenseProviderError("dense provider call differs from the injected model revision")
        if str(call.get("provider") or "") != embedder.provider:
            raise DenseProviderError("dense provider call differs from the injected provider")
        if str(call.get("operation") or "") != "dense-embedding":
            raise DenseProviderError("dense provider call reports the wrong operation")
        if int(call.get("dimensions", -1)) != embedder.dimensions:
            raise DenseProviderError("dense provider call differs from the injected dimensions")
        if int(call.get("input_count", -1)) != len(representatives):
            raise DenseProviderError("dense provider call reports the wrong input count")
        try:
            attempt_count = int(call.get("attempt_count", -1))
            retry_count = int(call.get("retry_count", -1))
        except (TypeError, ValueError) as exc:
            raise DenseProviderError("dense provider call reports invalid attempt counts") from exc
        if (
            call.get("provider_invoked") is not True
            or str(call.get("status") or "") != "completed"
            or attempt_count <= 0
            or retry_count < 0
        ):
            raise DenseProviderError("dense provider call did not settle as completed")
        for index, (item, vector) in enumerate(zip(representatives, response.vectors, strict=True)):
            if len(vector) != embedder.dimensions:
                raise DenseProviderError("dense provider returned the wrong vector dimensions")
            try:
                produced[item.vector_id] = _provider_row(
                    item,
                    work_id=work_id,
                    vector=vector,
                    call=call,
                    input_index=index,
                )
            except (TypeError, ValueError) as exc:
                raise DenseProviderError(
                    f"dense provider returned invalid vector or call facts: {type(exc).__name__}"
                ) from exc

    for item in pending:
        base = produced[item.vector_id]
        settled.append(
            base
            if _row_matches_input(base, item, work_id=work_id, embedder=embedder)
            else _clone_provider_row(base, item, work_id=work_id)
        )
    return tuple(sorted(settled, key=_dense_row_key))


def _segment_slices(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    try:
        slices = json.loads(str(candidate.get("slices_json") or ""))
    except (TypeError, ValueError) as exc:
        raise PlanError(f"segment {candidate.get('target_id')} has invalid public slices") from exc
    if not isinstance(slices, list) or any(not isinstance(item, dict) for item in slices):
        raise PlanError(f"segment {candidate.get('target_id')} has invalid public slices")
    return slices


def _segment_embedding_rows(
    candidates: Sequence[Mapping[str, Any]],
    units: Sequence[DenseSemanticUnit],
    provider_rows: Sequence[DenseEmbeddingRow],
    *,
    work_id: str,
) -> tuple[DenseEmbeddingRow, ...]:
    units_by_field: dict[tuple[str, str], list[DenseSemanticUnit]] = defaultdict(list)
    for unit in units:
        units_by_field[(unit.artifact_id, unit.source_field)].append(unit)
    vectors_by_unit = {
        str(row.target_id): row
        for row in provider_rows
        if row.input_kind == "semantic-unit" and row.target_id is not None
    }
    if len(vectors_by_unit) != len(units):
        raise PlanError("stored dense rows do not cover every semantic unit identity")
    derived: list[DenseEmbeddingRow] = []
    for candidate in candidates:
        target_id = str(candidate.get("target_id") or "")
        artifact_id = str(candidate.get("artifact_id") or "")
        segment_id = str(candidate.get("segment_id") or "")
        if not target_id or target_id != segment_id or not artifact_id:
            raise PlanError("segment dense search received a non-segment candidate")
        overlaps: dict[str, tuple[DenseSemanticUnit, int]] = {}
        for item in _segment_slices(candidate):
            source_field = str(item.get("source_field") or "")
            try:
                start = int(item["start_char"])
                end = int(item["end_char"])
            except (KeyError, TypeError, ValueError) as exc:
                raise PlanError(f"segment {segment_id} has invalid slice coordinates") from exc
            for unit in units_by_field.get((artifact_id, source_field), ()):
                overlap = min(end, unit.end_char) - max(start, unit.start_char)
                if overlap <= 0:
                    continue
                prior = overlaps.get(unit.unit_id)
                overlaps[unit.unit_id] = (
                    unit,
                    overlap + (prior[1] if prior is not None else 0),
                )
        ordered = sorted(overlaps.values(), key=lambda item: item[0].ordinal)
        if not ordered:
            raise PlanError(f"segment {segment_id} overlaps no dense semantic unit")
        component_rows = [vectors_by_unit[unit.unit_id] for unit, _ in ordered]
        weights = [float(overlap) for _, overlap in ordered]
        vector = compose_dense_vector(
            [row.vector for row in component_rows],
            weights,
        )
        text = str(candidate.get("text") or "")
        digest = sha256_text(text)
        recorded_digest = str(candidate.get("text_sha256") or "")
        if recorded_digest and recorded_digest != digest:
            raise PlanError(f"segment {segment_id} text differs from its public digest")
        model_id = component_rows[0].model_id
        model_revision = component_rows[0].model_revision
        if any(
            row.model_id != model_id or row.model_revision != model_revision or row.dimensions != len(vector)
            for row in component_rows
        ):
            raise PlanError("semantic-unit rows disagree on their dense model shape")
        call = {
            "provider": "derived",
            "operation": "overlap-character-weighted-mean",
            "status": "completed_derived",
            "provider_invoked": False,
            "attempt_count": 0,
            "retry_count": 0,
            "model_id": model_id,
            "model_revision": model_revision,
            "dimensions": len(vector),
            "component_unit_ids": [unit.unit_id for unit, _ in ordered],
            "component_vector_ids": [row.vector_id for row in component_rows],
            "overlap_characters": [int(weight) for weight in weights],
            "zero_weight_fallback": sum(weights) <= 0,
        }
        vector_identity = canonical_json(
            {
                "level": "segment",
                "target_id": target_id,
                "artifact_digest": str(candidate.get("artifact_digest") or ""),
                "input_sha256": digest,
                "input_policy": DENSE_SEGMENT_COMPOSITION_POLICY,
                "model_id": model_id,
                "components": list(
                    zip(
                        call["component_vector_ids"],
                        call["overlap_characters"],
                        strict=True,
                    )
                ),
            }
        )
        derived.append(
            DenseEmbeddingRow(
                work_id=work_id,
                level="segment",
                input_kind="segment",
                vector_id=("dense_segment_" + hashlib.sha256(vector_identity.encode()).hexdigest()[:24]),
                query_id=None,
                target_id=target_id,
                artifact_id=artifact_id,
                segment_id=segment_id,
                source_table=str(candidate.get("source_table") or ""),
                subject_id=str(candidate.get("subject_id") or ""),
                artifact_digest=str(candidate.get("artifact_digest") or ""),
                source_field=None,
                start_char=None,
                end_char=None,
                input_policy=DENSE_SEGMENT_COMPOSITION_POLICY,
                input_sha256=digest,
                input_text=text,
                model_id=model_id,
                model_revision=model_revision,
                dimensions=len(vector),
                normalization=DENSE_NORMALIZATION_POLICY,
                vector_json=canonical_json(list(vector)),
                tokenizer_id=None,
                tokenizer_package_version=None,
                untruncated_token_count=None,
                input_limit=None,
                would_truncate=None,
                token_audit_status=None,
                provider="derived",
                operation="overlap-character-weighted-mean",
                call_status="completed_derived",
                provider_invoked=False,
                attempt_count=0,
                retry_count=0,
                call_input_index=-1,
                call_json=canonical_json(call),
            )
        )
    return tuple(sorted(derived, key=_dense_row_key))


def _merge_dense_rows(
    existing: Sequence[DenseEmbeddingRow],
    current: Sequence[DenseEmbeddingRow],
) -> tuple[DenseEmbeddingRow, ...]:
    merged = {_dense_row_key(row): row for row in existing}
    for row in current:
        key = _dense_row_key(row)
        prior = merged.get(key)
        if prior is not None and prior != row:
            raise PlanError("dense immutable row identity drifted during resume")
        merged[key] = row
    return tuple(sorted(merged.values(), key=_dense_row_key))


def _dense_hits(
    candidates: Sequence[Mapping[str, Any]],
    rows: Sequence[DenseEmbeddingRow],
    *,
    query: RetrievalQuery,
    work_id: str,
    candidate_universe_size: int,
) -> tuple[RetrievalHit, ...]:
    if not candidates:
        return ()
    query_rows = [
        row
        for row in rows
        if row.work_id == work_id
        and row.level == query.level
        and row.input_kind == "query"
        and row.query_id == query.query_id
        and row.input_sha256 == sha256_text(query.text)
        and row.input_text == query.text
    ]
    if len(query_rows) != 1:
        raise PlanError("dense retrieval requires one exact stored query vector")
    target_kind = "artifact" if query.level == "artifact" else "segment"
    target_rows = {
        str(row.target_id): row
        for row in rows
        if row.work_id == work_id
        and row.level == query.level
        and row.input_kind == target_kind
        and row.target_id is not None
    }
    by_id: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        target_id = str(candidate.get("target_id") or "")
        if not target_id or target_id in by_id:
            raise PlanError("dense candidate target identities must be non-empty and unique")
        by_id[target_id] = dict(candidate)
    missing = sorted(set(by_id) - set(target_rows))
    if missing:
        raise PlanError(f"dense stored vectors do not cover candidate targets: {missing[:5]}")
    ranked = rank_dense_vectors(
        tuple(by_id),
        tuple(target_rows[target_id].vector for target_id in by_id),
        query_rows[0].vector,
        limit=RETRIEVAL_CANDIDATE_LIMIT,
    )
    hits: list[RetrievalHit] = []
    for rank, (target_id, score) in enumerate(ranked, start=1):
        candidate = by_id[target_id]
        target = target_rows[target_id]
        hits.append(
            RetrievalHit(
                work_id=work_id,
                query_id=query.query_id,
                level=query.level,
                method="dense",
                target_id=target_id,
                artifact_id=str(candidate.get("artifact_id") or ""),
                segment_id=(str(candidate.get("segment_id") or "") if query.level == "segment" else None),
                source_table=str(candidate.get("source_table") or ""),
                subject_id=str(candidate.get("subject_id") or ""),
                artifact_digest=str(candidate.get("artifact_digest") or ""),
                rank=rank,
                candidate_universe_size=candidate_universe_size,
                candidate_input_size=len(candidates),
                candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
                score=score,
                score_kind="cosine",
                dense_rank=rank,
                dense_score=score,
                model_id=target.model_id,
                model_revision=target.model_revision,
            )
        )
    return tuple(hits)


def _validate_dense_entry(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    level: str,
    candidate_universe_size: int | None,
) -> int:
    if query.level != level:
        raise PlanError(f"{level} dense entry point requires a {level} query")
    if not str(work_id).strip():
        raise PlanError("dense retrieval requires a work_id")
    size = len(candidates) if candidate_universe_size is None else candidate_universe_size
    if size < len(candidates):
        raise PlanError("dense candidate universe size cannot be smaller than its ranked input")
    return size


def dense_artifact_search(
    candidates: Sequence[Mapping[str, Any]],
    source_fields: Sequence[DenseSourceField],
    *,
    query: RetrievalQuery,
    work_id: str,
    embedder: DenseEmbedder,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> DenseRetrievalOutcome:
    """Rank whole artifacts through the all-profile dense-only routing entry point."""
    universe_size = _validate_dense_entry(
        candidates,
        query=query,
        work_id=work_id,
        level="artifact",
        candidate_universe_size=candidate_universe_size,
    )
    existing = read_dense_embedding_rows(run_directory)
    if not candidates:
        write_dense_embedding_rows(run_directory, existing)
        return DenseRetrievalOutcome("completed_empty", "artifact", (), ())
    inputs = (
        _query_input(query, embedder.model_id),
        *_artifact_inputs(candidates, source_fields, embedder.model_id),
    )
    current = _materialize_provider_rows(
        inputs,
        work_id=work_id,
        embedder=embedder,
        existing=existing,
    )
    merged = _merge_dense_rows(existing, current)
    write_dense_embedding_rows(run_directory, merged)
    hits = _dense_hits(
        candidates,
        current,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )
    return DenseRetrievalOutcome("completed", "artifact", hits, current)


def dense_segment_search(
    candidates: Sequence[Mapping[str, Any]],
    source_fields: Sequence[DenseSourceField],
    *,
    query: RetrievalQuery,
    work_id: str,
    embedder: DenseEmbedder,
    counter: SemanticUnitCounter,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> DenseRetrievalOutcome:
    """Rank processing segments after predecessor-compatible unit composition."""
    universe_size = _validate_dense_entry(
        candidates,
        query=query,
        work_id=work_id,
        level="segment",
        candidate_universe_size=candidate_universe_size,
    )
    existing = read_dense_embedding_rows(run_directory)
    if not candidates:
        write_dense_embedding_rows(run_directory, existing)
        return DenseRetrievalOutcome("completed_empty", "segment", (), ())
    candidate_artifacts = {str(candidate.get("artifact_id") or "") for candidate in candidates}
    selected_fields = tuple(field for field in source_fields if field.artifact_id in candidate_artifacts)
    if {field.artifact_id for field in selected_fields} != candidate_artifacts:
        raise PlanError("segment dense search lacks exact source fields for a candidate artifact")
    units = derive_dense_semantic_units(selected_fields, counter=counter)
    provider_inputs = (
        _query_input(query, embedder.model_id),
        *_semantic_unit_inputs(units, embedder.model_id),
    )
    provider_rows = _materialize_provider_rows(
        provider_inputs,
        work_id=work_id,
        embedder=embedder,
        existing=existing,
    )
    segment_rows = _segment_embedding_rows(
        candidates,
        units,
        provider_rows,
        work_id=work_id,
    )
    current = tuple(sorted((*provider_rows, *segment_rows), key=_dense_row_key))
    merged = _merge_dense_rows(existing, current)
    write_dense_embedding_rows(run_directory, merged)
    hits = _dense_hits(
        candidates,
        current,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )
    return DenseRetrievalOutcome("completed", "segment", hits, current)


def rebuild_dense_artifact_hits(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> tuple[RetrievalHit, ...]:
    """Rebuild artifact hits from stored vectors without accepting a provider."""
    universe_size = _validate_dense_entry(
        candidates,
        query=query,
        work_id=work_id,
        level="artifact",
        candidate_universe_size=candidate_universe_size,
    )
    rows = read_dense_embedding_rows(run_directory)
    if not rows and candidates:
        raise PlanError("dense artifact rebuild requires stored vectors")
    return _dense_hits(
        candidates,
        rows,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )


def rebuild_dense_segment_hits(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> tuple[RetrievalHit, ...]:
    """Rebuild segment hits from stored composed vectors without a provider."""
    universe_size = _validate_dense_entry(
        candidates,
        query=query,
        work_id=work_id,
        level="segment",
        candidate_universe_size=candidate_universe_size,
    )
    rows = read_dense_embedding_rows(run_directory)
    if not rows and candidates:
        raise PlanError("dense segment rebuild requires stored vectors")
    return _dense_hits(
        candidates,
        rows,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )


def sparse_csr_matrix(
    vectors: Sequence[SparseVector],
    *,
    dimensions: int,
) -> Any:
    """Build one canonical float64 SciPy CSR matrix from validated vectors."""
    if dimensions <= 0:
        raise ValueError("sparse CSR dimensions must be positive")
    from scipy.sparse import csr_matrix

    indices: list[int] = []
    values: list[float] = []
    indptr = [0]
    for vector in vectors:
        validated = validate_sparse_vector(vector, dimensions)
        indices.extend(validated.indices)
        values.extend(validated.values)
        indptr.append(len(indices))
    matrix = csr_matrix(
        (
            np.asarray(values, dtype=np.float64),
            np.asarray(indices, dtype=np.int64),
            np.asarray(indptr, dtype=np.int64),
        ),
        shape=(len(vectors), dimensions),
        dtype=np.float64,
    )
    if not matrix.has_sorted_indices or not matrix.has_canonical_format:
        raise ValueError("sparse CSR matrix is not sorted and canonical")
    return matrix


def rank_sparse_vectors(
    target_ids: Sequence[str],
    target_vectors: Sequence[SparseVector],
    query_vector: SparseVector,
    *,
    limit: int = RETRIEVAL_CANDIDATE_LIMIT,
) -> tuple[tuple[str, float], ...]:
    """Rank raw learned-sparse dot products with deterministic target ties."""
    if limit < 0:
        raise ValueError("sparse ranking limit cannot be negative")
    if len(target_ids) != len(target_vectors):
        raise ValueError("sparse ranking target IDs and vectors differ in count")
    if len(set(target_ids)) != len(target_ids) or any(not str(value).strip() for value in target_ids):
        raise ValueError("sparse ranking target IDs must be non-empty and unique")
    query = validate_sparse_vector(query_vector, query_vector.dimensions)
    documents = sparse_csr_matrix(target_vectors, dimensions=query.dimensions)
    query_matrix = sparse_csr_matrix((query,), dimensions=query.dimensions)
    scores = np.asarray(documents.dot(query_matrix.transpose()).toarray(), dtype=np.float64).reshape(-1)
    if scores.shape != (len(target_ids),) or not np.isfinite(scores).all():
        raise ValueError("sparse ranking produced an invalid score array")
    ranked = sorted(
        zip(target_ids, (float(score) for score in scores), strict=True),
        key=lambda item: (-item[1], item[0]),
    )
    return tuple(ranked[:limit])


@dataclass(frozen=True)
class _SparseInput:
    level: str
    input_kind: str
    vector_id: str
    query_id: str | None
    target_id: str | None
    artifact_id: str | None
    segment_id: str | None
    source_table: str | None
    subject_id: str | None
    artifact_digest: str | None
    input_policy: str
    input_sha256: str
    input_text: str
    task: str


def _sparse_vector_id(
    *,
    level: str,
    task: str,
    input_sha256: str,
    model_id: str,
) -> str:
    identity = canonical_json(
        {
            "level": level,
            "task": task,
            "input_sha256": input_sha256,
            "model_id": model_id,
            "vector_format": SPARSE_VECTOR_FORMAT,
            "normalization": SPARSE_NORMALIZATION_POLICY,
        }
    )
    return f"sparse_vector_{hashlib.sha256(identity.encode()).hexdigest()[:24]}"


def _sparse_query_input(query: RetrievalQuery, model_id: str) -> _SparseInput:
    digest = sha256_text(query.text)
    return _SparseInput(
        level="segment",
        input_kind="query",
        vector_id=_sparse_vector_id(
            level="segment",
            task="query",
            input_sha256=digest,
            model_id=model_id,
        ),
        query_id=query.query_id,
        target_id=None,
        artifact_id=None,
        segment_id=None,
        source_table=None,
        subject_id=None,
        artifact_digest=None,
        input_policy=SPARSE_QUERY_INPUT_POLICY,
        input_sha256=digest,
        input_text=query.text,
        task="query",
    )


def _sparse_document_inputs(
    candidates: Sequence[Mapping[str, Any]],
    model_id: str,
) -> tuple[_SparseInput, ...]:
    result: list[_SparseInput] = []
    seen: set[str] = set()
    for candidate in sorted(candidates, key=lambda row: str(row.get("target_id") or "")):
        target_id = str(candidate.get("target_id") or "")
        artifact_id = str(candidate.get("artifact_id") or "")
        segment_id = str(candidate.get("segment_id") or "")
        source_table = str(candidate.get("source_table") or "")
        subject_id = str(candidate.get("subject_id") or "")
        artifact_digest = str(candidate.get("artifact_digest") or "")
        if not all((target_id, artifact_id, segment_id, source_table, subject_id, artifact_digest)):
            raise PlanError("sparse search received an incomplete segment candidate")
        if target_id != segment_id:
            raise PlanError("sparse search received a non-segment candidate")
        if target_id in seen:
            raise PlanError("sparse candidate target identities must be unique")
        seen.add(target_id)
        text = str(candidate.get("text") or "")
        if not text:
            raise PlanError(f"sparse candidate {target_id} has no segment text")
        digest = sha256_text(text)
        recorded_digest = str(candidate.get("text_sha256") or "")
        if recorded_digest and recorded_digest != digest:
            raise PlanError(f"sparse candidate {target_id} text differs from its public digest")
        result.append(
            _SparseInput(
                level="segment",
                input_kind="document",
                vector_id=_sparse_vector_id(
                    level="segment",
                    task="document",
                    input_sha256=digest,
                    model_id=model_id,
                ),
                query_id=None,
                target_id=target_id,
                artifact_id=artifact_id,
                segment_id=segment_id,
                source_table=source_table,
                subject_id=subject_id,
                artifact_digest=artifact_digest,
                input_policy=SPARSE_DOCUMENT_INPUT_POLICY,
                input_sha256=digest,
                input_text=text,
                task="document",
            )
        )
    return tuple(result)


def _sparse_row_key(row: SparseEmbeddingRow) -> tuple[str, str, str, str, str, str]:
    return (
        row.work_id,
        row.level,
        row.task,
        row.query_id or "",
        row.target_id or "",
        row.vector_id,
    )


def read_sparse_embedding_rows(run_directory: Path) -> tuple[SparseEmbeddingRow, ...]:
    """Read and validate the immutable sparse input/output table."""
    path = Path(run_directory) / SPARSE_EMBEDDING_TABLE
    if not path.is_file():
        return ()
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    expected = [name for name, _ in SPARSE_EMBEDDING_COLUMNS]
    if table.schema.names != expected:
        raise PlanError("sparse embedding table schema differs from the fixed typed columns")
    try:
        rows = tuple(SparseEmbeddingRow(**row) for row in table.to_pylist())
    except (TypeError, ValueError) as exc:
        raise PlanError(f"sparse embedding table contains an invalid row: {type(exc).__name__}") from exc
    keys = [_sparse_row_key(row) for row in rows]
    if len(keys) != len(set(keys)):
        raise PlanError("sparse embedding table contains duplicate input rows")
    return tuple(sorted(rows, key=_sparse_row_key))


def write_sparse_embedding_rows(
    run_directory: Path,
    rows: Sequence[SparseEmbeddingRow],
) -> Path:
    """Write deterministic sparse rows, including a correctly typed zero-row table."""
    ordered = sorted(rows, key=_sparse_row_key)
    keys = [_sparse_row_key(row) for row in ordered]
    if len(keys) != len(set(keys)):
        raise ValueError("sparse embedding rows contain duplicate input identities")
    return _write_table(
        Path(run_directory) / SPARSE_EMBEDDING_TABLE,
        SPARSE_EMBEDDING_COLUMNS,
        [dataclasses.asdict(row) for row in ordered],
    )


def _sparse_sequence_call_fact(
    call: Mapping[str, Any],
    name: str,
    count: int,
) -> tuple[Any, ...]:
    value = call.get(name)
    if not isinstance(value, (list, tuple)) or len(value) != count:
        raise SparseProviderError(f"sparse provider call has invalid {name}")
    return tuple(value)


def _sparse_provider_row(
    item: _SparseInput,
    *,
    work_id: str,
    vector: SparseVector,
    call: Mapping[str, Any],
    input_index: int,
    dimensions: int,
) -> SparseEmbeddingRow:
    validated = validate_sparse_vector(vector, dimensions)
    count = int(call.get("input_count", -1))
    token_counts = _sparse_sequence_call_fact(call, "token_counts", count)
    over_limit = _sparse_sequence_call_fact(call, "inputs_over_limit", count)
    active_counts = _sparse_sequence_call_fact(call, "active_dimension_counts", count)
    token_count = token_counts[input_index]
    would_truncate = over_limit[input_index]
    try:
        token_count = int(token_count) if token_count is not None else None
        active_count = int(active_counts[input_index])
    except (TypeError, ValueError) as exc:
        raise SparseProviderError("sparse provider call has an invalid per-input count") from exc
    if active_count != len(validated.indices):
        raise SparseProviderError("sparse provider call reports the wrong active-dimension count")
    if would_truncate is not None and type(would_truncate) is not bool:
        raise SparseProviderError("sparse provider call has an invalid truncation flag")
    model_id = str(call.get("model_id") or "")
    revision = str(call.get("revision") or "")
    if not revision and "@" in model_id:
        revision = model_id.rsplit("@", 1)[1]
    input_limit_value = call.get("max_input_tokens")
    try:
        input_limit = int(input_limit_value) if input_limit_value is not None else None
    except (TypeError, ValueError) as exc:
        raise SparseProviderError("sparse provider call has an invalid input limit") from exc
    return SparseEmbeddingRow(
        work_id=work_id,
        level=item.level,
        input_kind=item.input_kind,
        vector_id=item.vector_id,
        query_id=item.query_id,
        target_id=item.target_id,
        artifact_id=item.artifact_id,
        segment_id=item.segment_id,
        source_table=item.source_table,
        subject_id=item.subject_id,
        artifact_digest=item.artifact_digest,
        input_policy=item.input_policy,
        input_sha256=item.input_sha256,
        input_text=item.input_text,
        task=item.task,
        model_id=model_id,
        model_revision=revision,
        dimensions=dimensions,
        vector_format=SPARSE_VECTOR_FORMAT,
        normalization=SPARSE_NORMALIZATION_POLICY,
        indices_json=canonical_json(list(validated.indices)),
        values_json=canonical_json(list(validated.values)),
        active_dimensions=len(validated.indices),
        tokenizer_id=str(call.get("tokenizer_id") or "") or None,
        tokenizer_package_version=(str(call.get("tokenizer_package_version") or "") or None),
        untruncated_token_count=token_count,
        input_limit=input_limit,
        would_truncate=would_truncate,
        token_audit_status=str(call.get("token_audit_status") or "") or None,
        provider=str(call.get("provider") or ""),
        operation=str(call.get("operation") or ""),
        call_status=str(call.get("status") or ""),
        provider_invoked=bool(call.get("provider_invoked")),
        attempt_count=int(call.get("attempt_count", -1)),
        retry_count=int(call.get("retry_count", -1)),
        call_input_index=input_index,
        call_json=canonical_json(dict(call)),
    )


def _sparse_row_matches_input(
    row: SparseEmbeddingRow,
    item: _SparseInput,
    *,
    work_id: str,
    encoder: SparseEncoder,
) -> bool:
    return (
        row.work_id == work_id
        and row.level == item.level
        and row.input_kind == item.input_kind
        and row.task == item.task
        and row.vector_id == item.vector_id
        and row.query_id == item.query_id
        and row.target_id == item.target_id
        and row.artifact_id == item.artifact_id
        and row.segment_id == item.segment_id
        and row.source_table == item.source_table
        and row.subject_id == item.subject_id
        and row.artifact_digest == item.artifact_digest
        and row.input_policy == item.input_policy
        and row.input_sha256 == item.input_sha256
        and row.input_text == item.input_text
        and row.model_id == encoder.model_id
        and row.model_revision == encoder.model_id.rsplit("@", 1)[-1]
        and row.dimensions == encoder.dimensions
        and row.tokenizer_id == encoder.tokenizer_id
        and row.input_limit == encoder.max_input_tokens
    )


def _clone_sparse_provider_row(
    base: SparseEmbeddingRow,
    item: _SparseInput,
    *,
    work_id: str,
) -> SparseEmbeddingRow:
    return dataclasses.replace(
        base,
        work_id=work_id,
        level=item.level,
        input_kind=item.input_kind,
        vector_id=item.vector_id,
        query_id=item.query_id,
        target_id=item.target_id,
        artifact_id=item.artifact_id,
        segment_id=item.segment_id,
        source_table=item.source_table,
        subject_id=item.subject_id,
        artifact_digest=item.artifact_digest,
        input_policy=item.input_policy,
        input_sha256=item.input_sha256,
        input_text=item.input_text,
        task=item.task,
    )


def _materialize_sparse_rows(
    inputs: Sequence[_SparseInput],
    *,
    work_id: str,
    encoder: SparseEncoder,
    existing: Sequence[SparseEmbeddingRow],
) -> tuple[SparseEmbeddingRow, ...]:
    if encoder.model_id != SPARSE_MODEL_ID:
        raise PlanError(f"sparse encoder model differs from the fixed retrieval pin: {encoder.model_id}")
    if encoder.dimensions <= 0:
        raise PlanError("sparse encoder dimensions must be positive")
    exact = {_sparse_row_key(row): row for row in existing}
    cache: dict[tuple[str, str, str, str], SparseEmbeddingRow] = {}
    for row in existing:
        key = (row.work_id, row.level, row.task, row.vector_id)
        prior = cache.setdefault(key, row)
        if prior.vector != row.vector or prior.input_sha256 != row.input_sha256:
            raise PlanError("sparse vector identity resolves to conflicting stored provider outputs")

    settled: list[SparseEmbeddingRow] = []
    pending: list[_SparseInput] = []
    representatives: dict[tuple[str, str], _SparseInput] = {}
    for item in inputs:
        key = (
            work_id,
            item.level,
            item.task,
            item.query_id or "",
            item.target_id or "",
            item.vector_id,
        )
        row = exact.get(key)
        if row is not None:
            if not _sparse_row_matches_input(row, item, work_id=work_id, encoder=encoder):
                raise PlanError("stored sparse input identity drifted within one work item")
            settled.append(row)
            continue
        cached = cache.get((work_id, item.level, item.task, item.vector_id))
        if cached is not None:
            if cached.input_sha256 != item.input_sha256 or cached.input_text != item.input_text:
                raise PlanError("stored sparse vector identity resolves to another exact input")
            if (
                cached.model_id != encoder.model_id
                or cached.dimensions != encoder.dimensions
                or cached.tokenizer_id != encoder.tokenizer_id
                or cached.input_limit != encoder.max_input_tokens
            ):
                raise PlanError("stored sparse vector call facts differ from the injected provider")
            settled.append(_clone_sparse_provider_row(cached, item, work_id=work_id))
            continue
        pending.append(item)
        prior = representatives.setdefault((item.task, item.vector_id), item)
        if prior.input_sha256 != item.input_sha256 or prior.input_text != item.input_text:
            raise PlanError("sparse vector identity collision")

    produced: dict[tuple[str, str], SparseEmbeddingRow] = {}
    for task in SPARSE_INPUT_KINDS:
        requested = [item for (item_task, _), item in representatives.items() if item_task == task]
        if not requested:
            continue
        texts = [item.input_text for item in requested]
        try:
            response = encoder.encode(texts, task=task)
        except Exception as exc:
            raise SparseProviderError(
                f"sparse {task} provider failed before persistence: {type(exc).__name__}"
            ) from exc
        if len(response.vectors) != len(requested):
            raise SparseProviderError(f"sparse {task} provider returned the wrong vector count")
        call = response.call
        if not isinstance(call, Mapping):
            raise SparseProviderError(f"sparse {task} provider returned invalid call details")
        try:
            reported_dimensions = int(call.get("dimensions", -1))
            reported_count = int(call.get("input_count", -1))
        except (TypeError, ValueError) as exc:
            raise SparseProviderError(f"sparse {task} provider returned invalid call counts") from exc
        if str(call.get("task") or "") != task:
            raise SparseProviderError(f"sparse {task} provider call reports the wrong task")
        if str(call.get("model_id") or "") != encoder.model_id:
            raise SparseProviderError(f"sparse {task} provider call differs from the injected model pin")
        if str(call.get("revision") or "") != encoder.model_id.rsplit("@", 1)[-1]:
            raise SparseProviderError(f"sparse {task} provider call differs from the injected model revision")
        if str(call.get("provider") or "") != encoder.provider:
            raise SparseProviderError(f"sparse {task} provider call differs from the injected provider")
        if reported_dimensions != encoder.dimensions:
            raise SparseProviderError(f"sparse {task} provider call differs from the injected dimensions")
        if reported_count != len(requested):
            raise SparseProviderError(f"sparse {task} provider call reports the wrong input count")
        if not call.get("provider_invoked") or str(call.get("status") or "") != "completed":
            raise SparseProviderError(f"sparse {task} provider call did not settle as completed")
        for index, (item, vector) in enumerate(zip(requested, response.vectors, strict=True)):
            try:
                row = _sparse_provider_row(
                    item,
                    work_id=work_id,
                    vector=vector,
                    call=call,
                    input_index=index,
                    dimensions=encoder.dimensions,
                )
            except (TypeError, ValueError) as exc:
                raise SparseProviderError(
                    f"sparse {task} provider returned invalid vector or call facts: {type(exc).__name__}"
                ) from exc
            if not _sparse_row_matches_input(row, item, work_id=work_id, encoder=encoder):
                raise SparseProviderError(f"sparse {task} provider returned mismatched call provenance")
            produced[(task, item.vector_id)] = row

    for item in pending:
        base = produced[(item.task, item.vector_id)]
        settled.append(
            base
            if _sparse_row_matches_input(base, item, work_id=work_id, encoder=encoder)
            else _clone_sparse_provider_row(base, item, work_id=work_id)
        )
    return tuple(sorted(settled, key=_sparse_row_key))


def _merge_sparse_rows(
    existing: Sequence[SparseEmbeddingRow],
    current: Sequence[SparseEmbeddingRow],
) -> tuple[SparseEmbeddingRow, ...]:
    merged = {_sparse_row_key(row): row for row in existing}
    for row in current:
        key = _sparse_row_key(row)
        prior = merged.get(key)
        if prior is not None and prior != row:
            raise PlanError("sparse immutable row identity drifted during resume")
        merged[key] = row
    return tuple(sorted(merged.values(), key=_sparse_row_key))


def _sparse_hits(
    candidates: Sequence[Mapping[str, Any]],
    rows: Sequence[SparseEmbeddingRow],
    *,
    query: RetrievalQuery,
    work_id: str,
    candidate_universe_size: int,
) -> tuple[RetrievalHit, ...]:
    if not candidates:
        return ()
    query_rows = [
        row
        for row in rows
        if row.work_id == work_id
        and row.level == "segment"
        and row.task == "query"
        and row.query_id == query.query_id
        and row.input_sha256 == sha256_text(query.text)
        and row.input_text == query.text
    ]
    if len(query_rows) != 1:
        raise PlanError("sparse retrieval requires one exact stored query vector")
    target_rows = {
        str(row.target_id): row
        for row in rows
        if row.work_id == work_id and row.level == "segment" and row.task == "document" and row.target_id is not None
    }
    by_id: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        target_id = str(candidate.get("target_id") or "")
        if not target_id or target_id in by_id:
            raise PlanError("sparse candidate target identities must be non-empty and unique")
        by_id[target_id] = dict(candidate)
    missing = sorted(set(by_id) - set(target_rows))
    if missing:
        raise PlanError(f"sparse stored vectors do not cover candidate targets: {missing[:5]}")
    ranked = rank_sparse_vectors(
        tuple(by_id),
        tuple(target_rows[target_id].vector for target_id in by_id),
        query_rows[0].vector,
        limit=RETRIEVAL_CANDIDATE_LIMIT,
    )
    hits: list[RetrievalHit] = []
    for rank, (target_id, score) in enumerate(ranked, start=1):
        candidate = by_id[target_id]
        target = target_rows[target_id]
        hits.append(
            RetrievalHit(
                work_id=work_id,
                query_id=query.query_id,
                level="segment",
                method="sparse",
                target_id=target_id,
                artifact_id=str(candidate.get("artifact_id") or ""),
                segment_id=str(candidate.get("segment_id") or ""),
                source_table=str(candidate.get("source_table") or ""),
                subject_id=str(candidate.get("subject_id") or ""),
                artifact_digest=str(candidate.get("artifact_digest") or ""),
                rank=rank,
                candidate_universe_size=candidate_universe_size,
                candidate_input_size=len(candidates),
                candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
                score=score,
                score_kind="sparse-dot",
                sparse_rank=rank,
                sparse_score=score,
                model_id=target.model_id,
                model_revision=target.model_revision,
            )
        )
    return tuple(hits)


def _validate_sparse_entry(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    candidate_universe_size: int | None,
) -> int:
    if query.level != "segment":
        raise PlanError("sparse segment entry point requires a segment query")
    if not str(work_id).strip():
        raise PlanError("sparse retrieval requires a work_id")
    size = len(candidates) if candidate_universe_size is None else candidate_universe_size
    if size < len(candidates):
        raise PlanError("sparse candidate universe size cannot be smaller than its ranked input")
    return size


def sparse_segment_search(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    encoder: SparseEncoder,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> SparseRetrievalOutcome:
    """Rank processing segments with asymmetric learned-sparse model calls."""
    universe_size = _validate_sparse_entry(
        candidates,
        query=query,
        work_id=work_id,
        candidate_universe_size=candidate_universe_size,
    )
    existing = read_sparse_embedding_rows(run_directory)
    if not candidates:
        write_sparse_embedding_rows(run_directory, existing)
        return SparseRetrievalOutcome("completed_empty", "segment", (), ())
    inputs = (
        *_sparse_document_inputs(candidates, encoder.model_id),
        _sparse_query_input(query, encoder.model_id),
    )
    current = _materialize_sparse_rows(
        inputs,
        work_id=work_id,
        encoder=encoder,
        existing=existing,
    )
    merged = _merge_sparse_rows(existing, current)
    write_sparse_embedding_rows(run_directory, merged)
    hits = _sparse_hits(
        candidates,
        current,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )
    return SparseRetrievalOutcome("completed", "segment", hits, current)


def rebuild_sparse_segment_hits(
    candidates: Sequence[Mapping[str, Any]],
    *,
    query: RetrievalQuery,
    work_id: str,
    run_directory: Path,
    candidate_universe_size: int | None = None,
) -> tuple[RetrievalHit, ...]:
    """Rebuild sparse segment hits from stored vectors without a provider."""
    universe_size = _validate_sparse_entry(
        candidates,
        query=query,
        work_id=work_id,
        candidate_universe_size=candidate_universe_size,
    )
    rows = read_sparse_embedding_rows(run_directory)
    if not rows and candidates:
        raise PlanError("sparse segment rebuild requires stored vectors")
    return _sparse_hits(
        candidates,
        rows,
        query=query,
        work_id=work_id,
        candidate_universe_size=universe_size,
    )


def sparse_retrieval_facts(outcome: SparseRetrievalOutcome) -> dict[str, Any]:
    """Return deterministic sparse facts suitable for a plan or receipt."""
    return {
        "state": outcome.state,
        "level": outcome.level,
        "hit_count": len(outcome.hits),
        "embedding_row_count": len(outcome.embeddings),
        "work_ids": sorted({row.work_id for row in outcome.embeddings}),
        "query_ids": sorted({row.query_id for row in outcome.embeddings if row.query_id is not None}),
        "tasks": sorted({row.task for row in outcome.embeddings}),
        "model_ids": sorted({row.model_id for row in outcome.embeddings}),
        "dimensions": sorted({row.dimensions for row in outcome.embeddings}),
        "input_sha256s": sorted({row.input_sha256 for row in outcome.embeddings}),
        "vector_ids": sorted({row.vector_id for row in outcome.embeddings}),
        "candidate_limit": RETRIEVAL_CANDIDATE_LIMIT,
        "fusion_input_depth": RETRIEVAL_FUSION_INPUT_DEPTH,
        "rrf_k": RETRIEVAL_RRF_K,
        "vector_format": SPARSE_VECTOR_FORMAT,
        "normalization": SPARSE_NORMALIZATION_POLICY,
    }


def _rrf_leg(
    hits: Sequence[RetrievalHit],
    *,
    method: str,
) -> tuple[RetrievalHit, ...]:
    if any(hit.method != method for hit in hits):
        raise ValueError(f"RRF {method} input contains a non-{method} hit")
    target_ids = [hit.target_id for hit in hits]
    if len(target_ids) != len(set(target_ids)):
        raise ValueError(f"RRF {method} input contains a duplicate target")
    ranks = [hit.rank for hit in hits]
    if len(ranks) != len(set(ranks)):
        raise ValueError(f"RRF {method} input contains a duplicate rank")
    if sorted(ranks) != list(range(1, len(ranks) + 1)):
        raise ValueError(f"RRF {method} input ranks must be contiguous from one")
    for hit in hits:
        if hit.level != "segment":
            raise ValueError("RRF is section evidence, not artifact routing")
        if hit.candidate_limit != RETRIEVAL_CANDIDATE_LIMIT or hit.rank > hit.candidate_input_size:
            raise ValueError(f"RRF {method} input differs from the fixed candidate plan")
        if method == "dense" and (hit.dense_rank != hit.rank or hit.dense_score != hit.score):
            raise ValueError("RRF dense leg provenance differs from its rank or score")
        if method == "sparse" and (hit.sparse_rank != hit.rank or hit.sparse_score != hit.score):
            raise ValueError("RRF sparse leg provenance differs from its rank or score")
    return tuple(sorted(hits, key=lambda hit: (hit.rank, hit.target_id))[:RETRIEVAL_FUSION_INPUT_DEPTH])


def fuse_rrf(
    dense_hits: Sequence[RetrievalHit],
    sparse_hits: Sequence[RetrievalHit],
) -> tuple[RetrievalHit, ...]:
    """Fuse the top 200 dense and sparse segment lists with fixed k=60."""
    dense = _rrf_leg(dense_hits, method="dense")
    sparse = _rrf_leg(sparse_hits, method="sparse")
    if not dense and not sparse:
        return ()
    contexts = {
        (
            hit.work_id,
            hit.query_id,
            hit.level,
            hit.candidate_universe_size,
            hit.candidate_input_size,
        )
        for hit in (*dense, *sparse)
    }
    if len(contexts) != 1:
        raise ValueError("RRF inputs must describe one query and candidate set")
    work_id, query_id, level, universe_size, input_size = next(iter(contexts))
    dense_by_id = {hit.target_id: hit for hit in dense}
    sparse_by_id = {hit.target_id: hit for hit in sparse}
    for target_id in set(dense_by_id) & set(sparse_by_id):
        first = dense_by_id[target_id]
        second = sparse_by_id[target_id]
        first_identity = (
            first.artifact_id,
            first.segment_id,
            first.source_table,
            first.subject_id,
            first.artifact_digest,
        )
        second_identity = (
            second.artifact_id,
            second.segment_id,
            second.source_table,
            second.subject_id,
            second.artifact_digest,
        )
        if first_identity != second_identity:
            raise ValueError(f"RRF target identity differs between legs: {target_id}")

    scored: list[tuple[str, float]] = []
    for target_id in sorted(set(dense_by_id) | set(sparse_by_id)):
        dense_hit = dense_by_id.get(target_id)
        sparse_hit = sparse_by_id.get(target_id)
        score = (1.0 / (RETRIEVAL_RRF_K + dense_hit.rank) if dense_hit is not None else 0.0) + (
            1.0 / (RETRIEVAL_RRF_K + sparse_hit.rank) if sparse_hit is not None else 0.0
        )
        scored.append((target_id, score))
    ranked = sorted(scored, key=lambda item: (-item[1], item[0]))[:RETRIEVAL_CANDIDATE_LIMIT]
    result: list[RetrievalHit] = []
    for rank, (target_id, score) in enumerate(ranked, start=1):
        dense_hit = dense_by_id.get(target_id)
        sparse_hit = sparse_by_id.get(target_id)
        template = dense_hit or sparse_hit
        assert template is not None
        result.append(
            RetrievalHit(
                work_id=work_id,
                query_id=query_id,
                level=level,
                method="hybrid-rrf",
                target_id=target_id,
                artifact_id=template.artifact_id,
                segment_id=template.segment_id,
                source_table=template.source_table,
                subject_id=template.subject_id,
                artifact_digest=template.artifact_digest,
                rank=rank,
                candidate_universe_size=universe_size,
                candidate_input_size=input_size,
                candidate_limit=RETRIEVAL_CANDIDATE_LIMIT,
                score=score,
                score_kind="rrf",
                dense_rank=dense_hit.rank if dense_hit is not None else None,
                dense_score=dense_hit.score if dense_hit is not None else None,
                sparse_rank=sparse_hit.rank if sparse_hit is not None else None,
                sparse_score=sparse_hit.score if sparse_hit is not None else None,
            )
        )
    return tuple(result)


def _rerank_row_key(row: RerankScoreRow) -> tuple[str, int, str]:
    return (row.work_id, row.candidate_index, row.target_id)


def read_rerank_score_rows(
    run_directory: Path,
) -> tuple[RerankScoreRow, ...]:
    """Read and validate immutable reranker inputs, scores, and call facts."""
    path = Path(run_directory) / RERANK_SCORE_TABLE
    if not path.is_file():
        return ()
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    expected = [name for name, _ in RERANK_SCORE_COLUMNS]
    if table.schema.names != expected:
        raise PlanError("rerank score table schema differs from the fixed typed columns")
    try:
        rows = tuple(RerankScoreRow(**row) for row in table.to_pylist())
    except (TypeError, ValueError) as exc:
        raise PlanError(f"rerank score table contains an invalid row: {type(exc).__name__}") from exc
    keys = [_rerank_row_key(row) for row in rows]
    if len(keys) != len(set(keys)):
        raise PlanError("rerank score table contains duplicate candidate rows")
    return tuple(sorted(rows, key=_rerank_row_key))


def write_rerank_score_rows(
    run_directory: Path,
    rows: Sequence[RerankScoreRow],
) -> Path:
    """Write deterministic score rows, including the fixed zero-row schema."""
    ordered = sorted(rows, key=_rerank_row_key)
    keys = [_rerank_row_key(row) for row in ordered]
    if len(keys) != len(set(keys)):
        raise ValueError("rerank score rows contain duplicate candidate identities")
    return _write_table(
        Path(run_directory) / RERANK_SCORE_TABLE,
        RERANK_SCORE_COLUMNS,
        [dataclasses.asdict(row) for row in ordered],
    )


def _validate_reranker_pin(reranker: Reranker) -> None:
    try:
        model_id = reranker.model_id
        input_limit = reranker.max_seq_length
        batch_size = reranker.batch_size
        tokenizer_id = reranker.tokenizer_id
        provider = reranker.provider
    except (AttributeError, TypeError, ValueError) as exc:
        raise PlanError(f"reranker does not expose the fixed provider settings: {type(exc).__name__}") from exc
    if model_id != RERANK_MODEL_ID:
        raise PlanError(f"reranker model differs from the fixed retrieval pin: {model_id}")
    if type(input_limit) is not int or input_limit != RERANK_MAX_SEQ_LENGTH:
        raise PlanError(f"reranker input limit differs from the fixed retrieval pin: {input_limit}")
    if type(batch_size) is not int or batch_size != RERANK_BATCH_SIZE:
        raise PlanError(f"reranker batch differs from the fixed retrieval pin: {batch_size}")
    if provider != "sentence-transformers":
        raise PlanError(f"reranker provider differs from the fixed retrieval provider: {provider}")
    if not str(tokenizer_id).strip():
        raise PlanError("reranker requires a pinned pair tokenizer")


def _dense_rerank_candidates(
    dense_hits: Sequence[RetrievalHit],
    *,
    query: RetrievalQuery,
    source_work_id: str,
) -> tuple[RetrievalHit, ...]:
    if query.level != "segment":
        raise PlanError("reranking requires a segment query")
    if not str(source_work_id).strip():
        raise PlanError("reranking requires the source dense work identity")
    rows = tuple(sorted(dense_hits, key=lambda hit: (hit.rank, hit.target_id)))
    if len(rows) > RETRIEVAL_CANDIDATE_LIMIT:
        raise PlanError("reranking received more than the 200-deep dense list")
    if any(hit.method != "dense" for hit in rows):
        raise PlanError("reranking accepts the dense list, not hybrid or sparse hits")
    target_ids = [hit.target_id for hit in rows]
    if len(target_ids) != len(set(target_ids)):
        raise PlanError("reranking dense input contains a duplicate target")
    ranks = [hit.rank for hit in rows]
    if ranks != list(range(1, len(rows) + 1)):
        raise PlanError("reranking dense input ranks must be contiguous from one")
    contexts = {
        (
            hit.work_id,
            hit.query_id,
            hit.level,
            hit.candidate_universe_size,
            hit.candidate_input_size,
            hit.candidate_limit,
        )
        for hit in rows
    }
    if rows and len(contexts) != 1:
        raise PlanError("reranking dense inputs must describe one query and candidate set")
    for hit in rows:
        if (
            hit.work_id != source_work_id
            or hit.query_id != query.query_id
            or hit.level != "segment"
            or hit.segment_id != hit.target_id
        ):
            raise PlanError("reranking dense input differs from its query or segment identity")
        if (
            hit.candidate_limit != RETRIEVAL_CANDIDATE_LIMIT
            or hit.candidate_input_size < len(rows)
            or hit.candidate_universe_size < hit.candidate_input_size
        ):
            raise PlanError("reranking dense input differs from the fixed 200-deep candidate plan")
        if (
            hit.dense_rank != hit.rank
            or hit.dense_score != hit.score
            or hit.model_id != DENSE_MODEL_ID
            or hit.model_revision != DEFAULT_DENSE_REVISION
        ):
            raise PlanError("reranking requires the pinned BGE dense list and its exact provenance")
    return rows[:RETRIEVAL_RERANK_DEPTH]


def _rerank_documents(
    candidates: Sequence[RetrievalHit],
    candidate_texts: Mapping[str, str],
) -> tuple[str, ...]:
    documents: list[str] = []
    for hit in candidates:
        value = candidate_texts.get(hit.target_id)
        if not isinstance(value, str) or not value.strip():
            raise PlanError(f"rerank candidate {hit.target_id!r} lacks exact segment text")
        documents.append(value)
    return tuple(documents)


def _rerank_group_facts(
    *,
    candidates: Sequence[RetrievalHit],
    documents: Sequence[str],
    query: RetrievalQuery,
    source_work_id: str,
) -> tuple[str, str, str, str]:
    candidate_ids = [hit.target_id for hit in candidates]
    candidate_ids_sha256 = hashlib.sha256(canonical_json(candidate_ids).encode()).hexdigest()
    group_key = hashlib.sha256(
        canonical_json(
            {
                "source_work_id": source_work_id,
                "query_id": query.query_id,
                "level": query.level,
            }
        ).encode()
    ).hexdigest()
    request_sha256 = hashlib.sha256(
        canonical_json(
            {
                "input_policy": RERANK_INPUT_POLICY,
                "model_id": RERANK_MODEL_ID,
                "max_seq_length": RERANK_MAX_SEQ_LENGTH,
                "batch_size": RERANK_BATCH_SIZE,
                "query_sha256": sha256_text(query.text),
                "candidates": [
                    {
                        "target_id": hit.target_id,
                        "input_sha256": sha256_text(document),
                    }
                    for hit, document in zip(candidates, documents, strict=True)
                ],
            }
        ).encode()
    ).hexdigest()
    work_id = (
        "rerank_work_"
        + hashlib.sha256(
            canonical_json(
                {
                    "group_key": group_key,
                    "candidate_ids_sha256": candidate_ids_sha256,
                    "request_sha256": request_sha256,
                }
            ).encode()
        ).hexdigest()[:32]
    )
    return group_key, candidate_ids_sha256, request_sha256, work_id


def _reject_rerank_group_drift(
    *,
    group_key: str,
    candidate_ids_sha256: str,
    request_sha256: str,
    checkpoints: Sequence[Mapping[str, Any]],
    rows: Sequence[RerankScoreRow],
) -> None:
    for record in checkpoints:
        if str(record.get("group_key") or "") != group_key:
            continue
        if str(record.get("candidate_ids_sha256") or "") != candidate_ids_sha256:
            raise PlanError("rerank candidate list drifted for an existing group")
        if str(record.get("request_sha256") or "") != request_sha256:
            raise PlanError("rerank query or candidate text drifted for an existing group")
    for row in rows:
        if row.group_key != group_key:
            continue
        if row.candidate_ids_sha256 != candidate_ids_sha256:
            raise PlanError("rerank candidate list drifted from stored score rows")
        if row.request_sha256 != request_sha256:
            raise PlanError("rerank query or candidate text drifted from stored score rows")


def _validated_rerank_call(
    response: Any,
    *,
    reranker: Reranker,
    candidate_count: int,
) -> tuple[tuple[float, ...], Mapping[str, Any], tuple[int, ...], tuple[bool, ...]]:
    try:
        raw_scores = response.scores
        call = response.call
    except AttributeError as exc:
        raise RerankProviderError("reranker returned an invalid result") from exc
    if not isinstance(raw_scores, (tuple, list)) or len(raw_scores) != candidate_count:
        raise RerankProviderError("reranker returned the wrong score count")
    if any(type(score) not in {int, float} for score in raw_scores):
        raise RerankProviderError("reranker returned a non-numeric score")
    scores = tuple(float(score) for score in raw_scores)
    if any(not math.isfinite(score) for score in scores):
        raise RerankProviderError("reranker returned a non-finite score")
    if not isinstance(call, Mapping):
        raise RerankProviderError("reranker returned invalid call details")
    try:
        reported_count = int(call.get("candidate_count", -1))
        input_limit = int(call.get("max_input_tokens", -1))
        provider_attempt_count = int(call.get("attempt_count", -1))
        retry_count = int(call.get("retry_count", -1))
    except (TypeError, ValueError) as exc:
        raise RerankProviderError("reranker call contains invalid counts") from exc
    token_counts_value = call.get("token_counts")
    truncation_value = call.get("inputs_over_limit")
    if (
        not isinstance(token_counts_value, (tuple, list))
        or len(token_counts_value) != candidate_count
        or any(type(value) is not int or value < 0 for value in token_counts_value)
    ):
        raise RerankProviderError("reranker call contains invalid pair-token counts")
    token_counts = tuple(token_counts_value)
    if (
        not isinstance(truncation_value, (tuple, list))
        or len(truncation_value) != candidate_count
        or any(type(value) is not bool for value in truncation_value)
    ):
        raise RerankProviderError("reranker call contains invalid truncation facts")
    computed_truncation = tuple(count > RERANK_MAX_SEQ_LENGTH for count in token_counts)
    if tuple(truncation_value) != computed_truncation:
        raise RerankProviderError("reranker truncation facts differ from pair-token counts")
    request_parameters = call.get("request_parameters")
    runtime_parameters = call.get("runtime_parameters")
    if (
        str(call.get("provider") or "") != reranker.provider
        or str(call.get("operation") or "") != "rerank"
        or str(call.get("model_id") or "") != RERANK_MODEL_ID
        or str(call.get("revision") or "") != DEFAULT_RERANK_REVISION
        or str(call.get("tokenizer_id") or "") != reranker.tokenizer_id
        or str(call.get("token_audit_status") or "") != "exact-untruncated-pair-tokenizer"
        or reported_count != candidate_count
        or input_limit != RERANK_MAX_SEQ_LENGTH
        or call.get("provider_invoked") is not True
        or str(call.get("status") or "") != "completed"
        or provider_attempt_count <= 0
        or retry_count < 0
        or not isinstance(request_parameters, Mapping)
        or request_parameters.get("max_seq_length") != RERANK_MAX_SEQ_LENGTH
        or not isinstance(runtime_parameters, Mapping)
        or runtime_parameters.get("batch_size") != RERANK_BATCH_SIZE
    ):
        raise RerankProviderError("reranker call differs from the fixed provider contract")
    for name in ("package_name", "package_version", "tokenizer_package_version"):
        if not str(call.get(name) or "").strip():
            raise RerankProviderError(f"reranker call lacks {name}")
    return scores, call, token_counts, computed_truncation


def _rerank_rows_for_response(
    *,
    candidates: Sequence[RetrievalHit],
    documents: Sequence[str],
    query: RetrievalQuery,
    source_work_id: str,
    group_key: str,
    candidate_ids_sha256: str,
    request_sha256: str,
    work_id: str,
    scores: Sequence[float],
    call: Mapping[str, Any],
    token_counts: Sequence[int],
    truncation: Sequence[bool],
    group_attempt: int,
) -> tuple[RerankScoreRow, ...]:
    order = sorted(
        range(len(candidates)),
        key=lambda index: (-scores[index], candidates[index].target_id),
    )
    ranks = {candidate_index: rank for rank, candidate_index in enumerate(order, start=1)}
    call_json = canonical_json(dict(call))
    return tuple(
        RerankScoreRow(
            work_id=work_id,
            group_key=group_key,
            source_work_id=source_work_id,
            query_id=query.query_id,
            level="segment",
            candidate_ids_sha256=candidate_ids_sha256,
            request_sha256=request_sha256,
            candidate_index=index,
            candidate_count=len(candidates),
            target_id=hit.target_id,
            artifact_id=hit.artifact_id,
            segment_id=hit.segment_id or "",
            source_table=hit.source_table,
            subject_id=hit.subject_id,
            artifact_digest=hit.artifact_digest,
            candidate_universe_size=hit.candidate_universe_size,
            dense_candidate_input_size=hit.candidate_input_size,
            dense_rank=hit.rank,
            dense_score=hit.score,
            query_input_sha256=sha256_text(query.text),
            query_text=query.text,
            input_policy=RERANK_INPUT_POLICY,
            input_sha256=sha256_text(document),
            input_text=document,
            rerank_score=float(scores[index]),
            rerank_rank=ranks[index],
            model_id=RERANK_MODEL_ID,
            model_revision=DEFAULT_RERANK_REVISION,
            tokenizer_id=str(call.get("tokenizer_id") or ""),
            tokenizer_package_version=str(call.get("tokenizer_package_version") or "") or None,
            untruncated_token_count=token_counts[index],
            input_limit=RERANK_MAX_SEQ_LENGTH,
            would_truncate=truncation[index],
            token_audit_status=str(call.get("token_audit_status") or ""),
            provider=str(call.get("provider") or ""),
            package_name=str(call.get("package_name") or ""),
            package_version=str(call.get("package_version") or ""),
            operation=str(call.get("operation") or ""),
            call_status=str(call.get("status") or ""),
            provider_invoked=call.get("provider_invoked") is True,
            group_attempt=group_attempt,
            provider_attempt_count=int(call.get("attempt_count", -1)),
            retry_count=int(call.get("retry_count", -1)),
            call_input_index=index,
            call_json=call_json,
        )
        for index, (hit, document) in enumerate(zip(candidates, documents, strict=True))
    )


def _validate_stored_rerank_group(
    stored: Sequence[RerankScoreRow],
    *,
    candidates: Sequence[RetrievalHit],
    documents: Sequence[str],
    query: RetrievalQuery,
    source_work_id: str,
    group_key: str,
    candidate_ids_sha256: str,
    request_sha256: str,
    work_id: str,
) -> tuple[RerankScoreRow, ...]:
    rows = tuple(sorted(stored, key=lambda row: row.candidate_index))
    if len(rows) != len(candidates):
        raise PlanError("stored rerank group is incomplete")
    ranks = sorted(row.rerank_rank for row in rows)
    if ranks != list(range(1, len(rows) + 1)):
        raise PlanError("stored rerank group ranks are incomplete")
    for index, (row, hit, document) in enumerate(zip(rows, candidates, documents, strict=True)):
        if (
            row.work_id != work_id
            or row.group_key != group_key
            or row.source_work_id != source_work_id
            or row.query_id != query.query_id
            or row.candidate_ids_sha256 != candidate_ids_sha256
            or row.request_sha256 != request_sha256
            or row.candidate_index != index
            or row.candidate_count != len(candidates)
            or row.target_id != hit.target_id
            or row.artifact_id != hit.artifact_id
            or row.segment_id != hit.segment_id
            or row.source_table != hit.source_table
            or row.subject_id != hit.subject_id
            or row.artifact_digest != hit.artifact_digest
            or row.candidate_universe_size != hit.candidate_universe_size
            or row.dense_candidate_input_size != hit.candidate_input_size
            or row.dense_rank != hit.rank
            or row.dense_score != hit.score
            or row.query_input_sha256 != sha256_text(query.text)
            or row.query_text != query.text
            or row.input_sha256 != sha256_text(document)
            or row.input_text != document
        ):
            raise PlanError("stored rerank group differs from its exact dense inputs")
    return rows


def _reranked_hits_from_rows(
    rows: Sequence[RerankScoreRow],
) -> tuple[RetrievalHit, ...]:
    return tuple(
        RetrievalHit(
            work_id=row.source_work_id,
            query_id=row.query_id,
            level="segment",
            method="reranked",
            target_id=row.target_id,
            artifact_id=row.artifact_id,
            segment_id=row.segment_id,
            source_table=row.source_table,
            subject_id=row.subject_id,
            artifact_digest=row.artifact_digest,
            rank=row.rerank_rank,
            candidate_universe_size=row.candidate_universe_size,
            candidate_input_size=row.candidate_count,
            candidate_limit=RETRIEVAL_RERANK_DEPTH,
            score=row.rerank_score,
            score_kind="cross-encoder",
            dense_rank=row.dense_rank,
            dense_score=row.dense_score,
            model_id=row.model_id,
            model_revision=row.model_revision,
            rerank_tokenizer_id=row.tokenizer_id,
            rerank_untruncated_token_count=row.untruncated_token_count,
            rerank_input_limit=row.input_limit,
            rerank_would_truncate=row.would_truncate,
            rerank_token_audit_status=row.token_audit_status,
        )
        for row in sorted(rows, key=lambda item: (item.rerank_rank, item.target_id))
    )


def _rerank_checkpoint_record(
    *,
    state: str,
    work_id: str,
    group_key: str,
    source_work_id: str,
    query: RetrievalQuery,
    candidate_ids_sha256: str,
    request_sha256: str,
    candidate_count: int,
    attempts: int,
    error: str = "",
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "work_id": work_id,
        "state": state,
        "step": RETRIEVAL_STEP,
        "task": "rerank",
        "group_key": group_key,
        "source_work_id": source_work_id,
        "query_id": query.query_id,
        "candidate_ids_sha256": candidate_ids_sha256,
        "request_sha256": request_sha256,
        "candidate_count": candidate_count,
        "model_id": RERANK_MODEL_ID,
        "max_seq_length": RERANK_MAX_SEQ_LENGTH,
        "batch_size": RERANK_BATCH_SIZE,
        "attempts": attempts,
    }
    if state == "completed":
        record["result"] = {"score_row_count": candidate_count}
    if error:
        record["error"] = error
    return record


def rerank_dense_hits(
    dense_hits: Sequence[RetrievalHit],
    candidate_texts: Mapping[str, str],
    *,
    query: RetrievalQuery,
    source_work_id: str,
    reranker: Reranker,
    run_directory: Path,
) -> RerankRetrievalOutcome:
    """Rerank the top 50 of one 200-deep BGE dense segment list."""
    _validate_reranker_pin(reranker)
    candidates = _dense_rerank_candidates(
        dense_hits,
        query=query,
        source_work_id=source_work_id,
    )
    documents = _rerank_documents(candidates, candidate_texts)
    group_key, candidate_ids_sha256, request_sha256, work_id = _rerank_group_facts(
        candidates=candidates,
        documents=documents,
        query=query,
        source_work_id=source_work_id,
    )
    root = Path(run_directory)
    existing = read_rerank_score_rows(root)
    checkpoint = WorkCheckpoint(root / RERANK_CHECKPOINT_FILE)
    records = checkpoint.records()
    _reject_rerank_group_drift(
        group_key=group_key,
        candidate_ids_sha256=candidate_ids_sha256,
        request_sha256=request_sha256,
        checkpoints=records,
        rows=existing,
    )
    prior = checkpoint.get(work_id)
    if not candidates:
        write_rerank_score_rows(root, existing)
        attempts = int(prior.get("attempts", 0)) if prior is not None else 0
        checkpoint.append(
            _rerank_checkpoint_record(
                state="completed_empty",
                work_id=work_id,
                group_key=group_key,
                source_work_id=source_work_id,
                query=query,
                candidate_ids_sha256=candidate_ids_sha256,
                request_sha256=request_sha256,
                candidate_count=0,
                attempts=attempts,
            )
        )
        return RerankRetrievalOutcome("completed_empty", work_id, (), ())

    stored = tuple(row for row in existing if row.work_id == work_id)
    if stored:
        settled = _validate_stored_rerank_group(
            stored,
            candidates=candidates,
            documents=documents,
            query=query,
            source_work_id=source_work_id,
            group_key=group_key,
            candidate_ids_sha256=candidate_ids_sha256,
            request_sha256=request_sha256,
            work_id=work_id,
        )
        attempts = max(row.group_attempt for row in settled)
        checkpoint.append(
            _rerank_checkpoint_record(
                state="completed",
                work_id=work_id,
                group_key=group_key,
                source_work_id=source_work_id,
                query=query,
                candidate_ids_sha256=candidate_ids_sha256,
                request_sha256=request_sha256,
                candidate_count=len(candidates),
                attempts=attempts,
            )
        )
        return RerankRetrievalOutcome(
            "completed",
            work_id,
            _reranked_hits_from_rows(settled),
            settled,
        )
    if prior is not None and prior.get("state") in {"completed", "completed_empty"}:
        raise PlanError("completed rerank checkpoint lacks its immutable score rows")

    attempts = int(prior.get("attempts", 0)) + 1 if prior is not None else 1
    checkpoint.append(
        _rerank_checkpoint_record(
            state="unknown",
            work_id=work_id,
            group_key=group_key,
            source_work_id=source_work_id,
            query=query,
            candidate_ids_sha256=candidate_ids_sha256,
            request_sha256=request_sha256,
            candidate_count=len(candidates),
            attempts=attempts,
        )
    )
    try:
        response = reranker.rerank(query.text, documents)
        scores, call, token_counts, truncation = _validated_rerank_call(
            response,
            reranker=reranker,
            candidate_count=len(candidates),
        )
        current = _rerank_rows_for_response(
            candidates=candidates,
            documents=documents,
            query=query,
            source_work_id=source_work_id,
            group_key=group_key,
            candidate_ids_sha256=candidate_ids_sha256,
            request_sha256=request_sha256,
            work_id=work_id,
            scores=scores,
            call=call,
            token_counts=token_counts,
            truncation=truncation,
            group_attempt=attempts,
        )
    except Exception as exc:
        checkpoint.append(
            _rerank_checkpoint_record(
                state="failed",
                work_id=work_id,
                group_key=group_key,
                source_work_id=source_work_id,
                query=query,
                candidate_ids_sha256=candidate_ids_sha256,
                request_sha256=request_sha256,
                candidate_count=len(candidates),
                attempts=attempts,
                error=type(exc).__name__,
            )
        )
        raise RerankProviderError(f"{work_id}: reranker failed; checkpoint is resumable: {type(exc).__name__}") from exc

    merged = tuple(sorted((*existing, *current), key=_rerank_row_key))
    write_rerank_score_rows(root, merged)
    checkpoint.append(
        _rerank_checkpoint_record(
            state="completed",
            work_id=work_id,
            group_key=group_key,
            source_work_id=source_work_id,
            query=query,
            candidate_ids_sha256=candidate_ids_sha256,
            request_sha256=request_sha256,
            candidate_count=len(candidates),
            attempts=attempts,
        )
    )
    return RerankRetrievalOutcome(
        "completed",
        work_id,
        _reranked_hits_from_rows(current),
        current,
    )


def rebuild_reranked_hits(
    dense_hits: Sequence[RetrievalHit],
    candidate_texts: Mapping[str, str],
    *,
    query: RetrievalQuery,
    source_work_id: str,
    run_directory: Path,
) -> tuple[RetrievalHit, ...]:
    """Rebuild cross-encoder ranks from stored scores without a provider."""
    candidates = _dense_rerank_candidates(
        dense_hits,
        query=query,
        source_work_id=source_work_id,
    )
    documents = _rerank_documents(candidates, candidate_texts)
    group_key, candidate_ids_sha256, request_sha256, work_id = _rerank_group_facts(
        candidates=candidates,
        documents=documents,
        query=query,
        source_work_id=source_work_id,
    )
    root = Path(run_directory)
    existing = read_rerank_score_rows(root)
    checkpoint = WorkCheckpoint(root / RERANK_CHECKPOINT_FILE, repair=False)
    _reject_rerank_group_drift(
        group_key=group_key,
        candidate_ids_sha256=candidate_ids_sha256,
        request_sha256=request_sha256,
        checkpoints=checkpoint.records(),
        rows=existing,
    )
    if not candidates:
        return ()
    stored = tuple(row for row in existing if row.work_id == work_id)
    if not stored:
        raise PlanError("rerank rebuild requires stored immutable score rows")
    settled = _validate_stored_rerank_group(
        stored,
        candidates=candidates,
        documents=documents,
        query=query,
        source_work_id=source_work_id,
        group_key=group_key,
        candidate_ids_sha256=candidate_ids_sha256,
        request_sha256=request_sha256,
        work_id=work_id,
    )
    return _reranked_hits_from_rows(settled)


def write_retrieval_tables(
    run_directory: Path,
    *,
    hits: Sequence[RetrievalHit],
    exclusions: Sequence[RetrievalExclusion],
) -> dict[str, Path]:
    """Write both typed retrieval tables, including deterministic zero rows."""
    root = Path(run_directory)
    sorted_hits = sorted(
        hits,
        key=lambda row: (
            row.work_id,
            row.query_id,
            row.level,
            row.method,
            row.rank,
            row.target_id,
        ),
    )
    sorted_exclusions = sorted(
        exclusions,
        key=lambda row: (
            row.work_id,
            row.query_id,
            row.level,
            FILTER_AXES.index(row.filter),
            row.target_id,
        ),
    )
    return {
        RETRIEVAL_HIT_TABLE: _write_table(
            root / RETRIEVAL_HIT_TABLE,
            HIT_COLUMNS,
            [dataclasses.asdict(row) for row in sorted_hits],
        ),
        RETRIEVAL_EXCLUSION_TABLE: _write_table(
            root / RETRIEVAL_EXCLUSION_TABLE,
            RETRIEVAL_EXCLUSION_COLUMNS,
            [dataclasses.asdict(row) for row in sorted_exclusions],
        ),
    }


# --------------------------------------------------------------------------
# metrics-aware runtime assembly
# --------------------------------------------------------------------------


def _retrieval_join_inputs(context: RetrievalRunContext) -> dict[str, Any]:
    return {
        "metadata": [dict(row) for row in context.metadata_rows],
        "authority": [dict(row) for row in context.authority_edges],
        "graph": [dict(row) for row in context.graph_edges],
        "concept_assignments": [dict(row) for row in context.concept_assignments],
        "profile_capabilities": {
            str(profile): list(axes) for profile, axes in sorted((context.profile_capabilities or {}).items())
        },
    }


def _source_run_id(context: RetrievalRunContext) -> str:
    receipt_path = context.source_directory / "receipt.json"
    if not receipt_path.is_file():
        return ""
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlanError("retrieval Step 4 receipt is not readable JSON") from exc
    if not isinstance(receipt, Mapping) or not str(receipt.get("run_id") or "").strip():
        raise PlanError("retrieval Step 4 receipt does not name its run ID")
    return str(receipt["run_id"])


def _check_retrieval_lineage(
    plan: RunPlan,
    context: RetrievalRunContext,
    source_file_digests: Mapping[str, str],
) -> bool:
    source_run_id = _source_run_id(context)
    if not source_run_id:
        if plan.mode in {"build", "benchmark"}:
            raise PlanError(f"{plan.mode} retrieval requires checked Step 4 lineage")
        return False

    matching: list[tuple[str, Mapping[str, Any]]] = []
    for name, raw_declaration in sorted(plan.earlier_runs.items()):
        if not isinstance(raw_declaration, Mapping):
            continue
        raw_directory = next(
            (raw_declaration[key] for key in ("run_directory", "directory", "path") if raw_declaration.get(key)),
            None,
        )
        if raw_directory is not None and Path(str(raw_directory)).resolve() == context.source_directory:
            matching.append((str(name), raw_declaration))
    if len(matching) != 1:
        raise PlanError(
            "retrieval from a receipted Step 4 run requires exactly one matching plan.earlier_runs declaration"
        )
    name, declaration = matching[0]
    if str(declaration.get("run_id") or "") != source_run_id:
        raise PlanError("retrieval Step 4 lineage declaration does not bind the source run ID")
    declared_files = declaration.get("files")
    if not isinstance(declared_files, Mapping) or any(
        str(declared_files.get(relative) or "") != digest for relative, digest in source_file_digests.items()
    ):
        raise PlanError("retrieval Step 4 lineage declaration does not bind the exact source and segment files")
    checked = check_earlier_run(name, declaration)
    if checked != context.source_directory:
        raise PlanError("retrieval Step 4 lineage resolved to another run directory")
    return True


def _write_retrieval_join_inputs(root: Path, context: RetrievalRunContext) -> Path:
    path = Path(root) / RETRIEVAL_JOIN_INPUTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json(_retrieval_join_inputs(context)).encode()
    if path.is_file():
        if path.read_bytes() != encoded:
            raise PlanError("stored retrieval join inputs drifted during resume")
        return path
    path.write_bytes(encoded)
    return path


def _context_from_stored_join_inputs(
    root: Path,
    context: RetrievalRunContext,
    *,
    expected_digest: str,
) -> RetrievalRunContext:
    path = Path(root) / RETRIEVAL_JOIN_INPUTS_FILE
    if not path.is_file():
        raise PlanError(f"retrieval run is missing deterministic join inputs at {RETRIEVAL_JOIN_INPUTS_FILE}")
    if sha256_file(path) != expected_digest:
        raise PlanError("retrieval join-input bytes differ from the plan digest")
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlanError("retrieval join inputs are not readable JSON") from exc
    if not isinstance(stored, Mapping) or canonical_json(stored).encode() != path.read_bytes():
        raise PlanError("retrieval join inputs are not one canonical JSON value")
    required = {
        "metadata",
        "authority",
        "graph",
        "concept_assignments",
        "profile_capabilities",
    }
    if set(stored) != required:
        raise PlanError("retrieval join inputs differ from the fixed sidecar shape")

    supplied = _retrieval_join_inputs(context)
    empty = _retrieval_join_inputs(RetrievalRunContext(context.source_directory))
    if supplied != empty and supplied != stored:
        raise PlanError("caller-supplied retrieval joins differ from the stored run inputs")
    selected = stored if supplied == empty else supplied
    return RetrievalRunContext(
        context.source_directory,
        metadata_rows=selected["metadata"],
        authority_edges=selected["authority"],
        graph_edges=selected["graph"],
        concept_assignments=selected["concept_assignments"],
        profile_capabilities=selected["profile_capabilities"],
    )


def _reject_build_answers(
    plan: RunPlan,
    answers: Mapping[str, Sequence[str]] | None,
) -> None:
    if plan.mode == "build" and (answers is not None or bool(str(plan.retrieval.get("answers_sha256") or ""))):
        raise PlanError("build retrieval cannot use answer-derived relevance")


def retrieval_run_plan_facts(
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    context: RetrievalRunContext,
    *,
    answers: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Return all deterministic retrieval facts a runtime plan must pin."""
    query_ids = {query.query_id for query in queries}
    if len(query_ids) != len(queries):
        raise PlanError("retrieval queries must have unique query IDs")
    if answers is not None:
        answer_ids = set(_normalized_retrieval_answers(answers))
        if answer_ids != query_ids:
            raise PlanError(
                "retrieval answers must cover exactly the planned queries: "
                f"missing={sorted(query_ids - answer_ids)}, extra={sorted(answer_ids - query_ids)}"
            )
    source_files: dict[str, str] = {}
    for relative in (ARTIFACT_TABLE, SEGMENT_TABLE):
        path = context.source_directory / relative
        if not path.is_file():
            raise PlanError(f"retrieval runtime requires stored source input {relative}")
        source_files[relative] = sha256_file(path)
    joins = _retrieval_join_inputs(context)
    return {
        **retrieval_plan_facts(spec, queries),
        "source_run_id": _source_run_id(context),
        "source_file_digests": source_files,
        "join_inputs_sha256": sha256_text(canonical_json(joins)),
        "answers_sha256": (
            sha256_text(canonical_json(_normalized_retrieval_answers(answers))) if answers is not None else ""
        ),
    }


def plan_retrieval_items(
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    context: RetrievalRunContext,
) -> tuple[WorkItem, ...]:
    """Return one query work item whose ID covers every ranking input."""
    facts = retrieval_run_plan_facts(spec, queries, context)
    source_digests = tuple(facts["source_file_digests"][name] for name in sorted(facts["source_file_digests"]))
    settings_digest = sha256_text(
        canonical_json(
            {
                "format_version": facts["format_version"],
                "methods": facts["methods"],
                "candidate_limit": facts["candidate_limit"],
                "rrf_k": facts["rrf_k"],
                "fusion_input_depth": facts["fusion_input_depth"],
                "rerank_depth": facts["rerank_depth"],
                "filters": facts["filters"],
                "join_inputs_sha256": facts["join_inputs_sha256"],
            }
        )
    )
    items: list[WorkItem] = []
    seen: set[str] = set()
    for query in sorted(queries, key=lambda item: item.query_id):
        if query.query_id in seen:
            raise PlanError(f"retrieval query {query.query_id!r} appears twice")
        seen.add(query.query_id)
        identity = WorkIdentity(
            step=RETRIEVAL_STEP,
            task=f"{query.level}-search",
            input_digests=(sha256_text(query.text), *source_digests, facts["join_inputs_sha256"]),
            settings={
                "query_id": query.query_id,
                "level": query.level,
                "retrieval_settings_sha256": settings_digest,
            },
            provider_config={
                "dense_model_id": spec.dense_model_id,
                "sparse_model_id": spec.sparse_model_id,
                "reranker_model_id": spec.reranker_model_id,
            },
            prior_run_id=str(facts["source_run_id"]),
        )
        items.append(
            WorkItem.from_identity(
                identity,
                payload={
                    "query_id": query.query_id,
                    "text": query.text,
                    "level": query.level,
                },
            )
        )
    return tuple(items)


def _normalized_retrieval_answers(
    answers: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for raw_query_id, raw_targets in answers.items():
        query_id = str(raw_query_id).strip()
        if not query_id:
            raise ValueError("retrieval answers require non-empty query IDs")
        if isinstance(raw_targets, (str, bytes)) or not isinstance(raw_targets, Sequence):
            raise ValueError(f"retrieval answers for {query_id!r} must be a sequence")
        normalized[query_id] = sorted({str(target).strip() for target in raw_targets if str(target).strip()})
    return dict(sorted(normalized.items()))


def _context_source_fields(context: RetrievalRunContext) -> tuple[DenseSourceField, ...]:
    import pyarrow.parquet as pq

    path = context.source_directory / SEGMENT_TABLE
    if not path.is_file():
        raise PlanError(f"retrieval runtime requires stored source input {SEGMENT_TABLE}")
    return dense_source_fields_from_segments(pq.read_table(path).to_pylist())


def _prefiltered_query(
    context: RetrievalRunContext,
    spec: RetrievalSpec,
    query: RetrievalQuery,
    work_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], tuple[RetrievalExclusion, ...]]:
    universe = construct_candidate_universe(context.source_directory, level=query.level)
    included, exclusions, _ = apply_prefilters(
        universe,
        spec,
        query=query,
        work_id=work_id,
        metadata_rows=context.metadata_rows,
        authority_edges=context.authority_edges,
        graph_edges=context.graph_edges,
        concept_assignments=context.concept_assignments,
        profile_capabilities=context.profile_capabilities,
    )
    return universe, included, tuple(exclusions)


def _provider_totals_from_rows(
    rows: Sequence[DenseEmbeddingRow | SparseEmbeddingRow | RerankScoreRow],
) -> ProviderTotals:
    calls: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in rows:
        call = json.loads(row.call_json)
        if call.get("provider_invoked") is not True:
            continue
        calls.setdefault((row.provider, row.operation, row.call_json), call)
    return ProviderTotals.sum(
        ProviderTotals(
            calls=1,
            retries=int(call.get("retry_count") or 0),
            seconds=float(call.get("duration_ms") or 0.0) / 1_000.0,
            input_tokens=int(call.get("input_tokens") or 0),
            output_tokens=int(call.get("output_tokens") or 0),
            total_tokens=int(call.get("total_tokens") or 0),
        )
        for call in calls.values()
    )


def _persisted_provider_totals(root: Path) -> ProviderTotals:
    rows: tuple[DenseEmbeddingRow | SparseEmbeddingRow | RerankScoreRow, ...] = (
        *read_dense_embedding_rows(root),
        *read_sparse_embedding_rows(root),
        *read_rerank_score_rows(root),
    )
    return _provider_totals_from_rows(rows)


def _execute_retrieval_query(
    workspace: RunWorkspace,
    *,
    spec: RetrievalSpec,
    query: RetrievalQuery,
    work_id: str,
    context: RetrievalRunContext,
    providers: RetrievalProviders,
    source_fields: Sequence[DenseSourceField],
) -> RetrievalQueryOutcome:
    universe, candidates, exclusions = _prefiltered_query(context, spec, query, work_id)
    dense = (
        dense_artifact_search(
            candidates,
            source_fields,
            query=query,
            work_id=work_id,
            embedder=providers.embedder,
            run_directory=workspace.path,
            candidate_universe_size=len(universe),
        )
        if query.level == "artifact"
        else dense_segment_search(
            candidates,
            source_fields,
            query=query,
            work_id=work_id,
            embedder=providers.embedder,
            counter=providers.counter,
            run_directory=workspace.path,
            candidate_universe_size=len(universe),
        )
    )
    provider_rows: list[DenseEmbeddingRow | SparseEmbeddingRow | RerankScoreRow] = list(dense.embeddings)
    hits: tuple[RetrievalHit, ...] = dense.hits
    if query.level == "segment":
        sparse = sparse_segment_search(
            candidates,
            query=query,
            work_id=work_id,
            encoder=providers.sparse_encoder,
            run_directory=workspace.path,
            candidate_universe_size=len(universe),
        )
        hybrid = fuse_rrf(dense.hits, sparse.hits)
        candidate_texts = {str(row["target_id"]): str(row["text"]) for row in candidates}
        reranked = rerank_dense_hits(
            dense.hits,
            candidate_texts,
            query=query,
            source_work_id=work_id,
            reranker=providers.reranker,
            run_directory=workspace.path,
        )
        provider_rows.extend(sparse.embeddings)
        provider_rows.extend(reranked.scores)
        hits = (*dense.hits, *sparse.hits, *hybrid, *reranked.hits)
    state = "completed" if hits else "completed_empty"
    return RetrievalQueryOutcome(
        state=state,
        hits=tuple(hits),
        exclusions=exclusions,
        provider=_provider_totals_from_rows(provider_rows),
    )


def _rebuild_retrieval_query(
    root: Path,
    *,
    spec: RetrievalSpec,
    query: RetrievalQuery,
    work_id: str,
    context: RetrievalRunContext,
) -> RetrievalQueryOutcome:
    universe, candidates, exclusions = _prefiltered_query(context, spec, query, work_id)
    dense_hits = (
        rebuild_dense_artifact_hits(
            candidates,
            query=query,
            work_id=work_id,
            run_directory=root,
            candidate_universe_size=len(universe),
        )
        if query.level == "artifact"
        else rebuild_dense_segment_hits(
            candidates,
            query=query,
            work_id=work_id,
            run_directory=root,
            candidate_universe_size=len(universe),
        )
    )
    hits: tuple[RetrievalHit, ...] = dense_hits
    if query.level == "segment":
        sparse_hits = rebuild_sparse_segment_hits(
            candidates,
            query=query,
            work_id=work_id,
            run_directory=root,
            candidate_universe_size=len(universe),
        )
        hybrid = fuse_rrf(dense_hits, sparse_hits)
        candidate_texts = {str(row["target_id"]): str(row["text"]) for row in candidates}
        reranked = rebuild_reranked_hits(
            dense_hits,
            candidate_texts,
            query=query,
            source_work_id=work_id,
            run_directory=root,
        )
        hits = (*dense_hits, *sparse_hits, *hybrid, *reranked)
    return RetrievalQueryOutcome(
        state="completed" if hits else "completed_empty",
        hits=tuple(hits),
        exclusions=exclusions,
    )


@dataclass(frozen=True)
class _RetrievalDerived:
    hits: tuple[RetrievalHit, ...]
    exclusions: tuple[RetrievalExclusion, ...]


def _derive_retrieval(
    root: Path,
    *,
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    items: Sequence[WorkItem],
    context: RetrievalRunContext,
    states: Mapping[str, str] | None = None,
) -> _RetrievalDerived:
    query_by_id = {query.query_id: query for query in queries}
    hits: list[RetrievalHit] = []
    exclusions: list[RetrievalExclusion] = []
    for item in items:
        state = (states or {}).get(item.work_id)
        if state is not None and state not in {"completed", "completed_empty"}:
            continue
        query_id = str(item.payload.get("query_id") or "")
        query = query_by_id.get(query_id)
        if query is None:
            raise PlanError(f"planned retrieval work refers to unknown query {query_id!r}")
        outcome = _rebuild_retrieval_query(
            Path(root),
            spec=spec,
            query=query,
            work_id=item.work_id,
            context=context,
        )
        hits.extend(outcome.hits)
        exclusions.extend(outcome.exclusions)
    return _RetrievalDerived(
        hits=tuple(
            sorted(
                hits,
                key=lambda row: (
                    row.work_id,
                    row.query_id,
                    row.level,
                    row.method,
                    row.rank,
                    row.target_id,
                ),
            )
        ),
        exclusions=tuple(
            sorted(
                exclusions,
                key=lambda row: (
                    row.work_id,
                    row.query_id,
                    row.level,
                    FILTER_AXES.index(row.filter),
                    row.target_id,
                ),
            )
        ),
    )


def _retrieval_checks(
    plan: RunPlan,
    *,
    expected_facts: Mapping[str, Any],
    root: Path,
    derived: _RetrievalDerived,
    answers: Mapping[str, Sequence[str]] | None,
    methods: Sequence[str],
    lineage_checked: bool,
) -> RunChecks:
    differing = sorted(key for key, value in expected_facts.items() if plan.retrieval.get(key) != value)
    checks = [
        CheckResult(
            step=RETRIEVAL_STEP,
            name="plan_declares_retrieval_inputs",
            status="pass" if not differing else "fail",
            detail="" if not differing else f"the plan and run disagree about {differing}",
        ),
        CheckResult(
            step=RETRIEVAL_STEP,
            name="rankings_and_exclusions_recomputed",
            status="pass",
        ),
        CheckResult(
            step=RETRIEVAL_STEP,
            name="step4_lineage",
            status="pass" if lineage_checked else "unknown",
            detail=("" if lineage_checked else "diagnostic fixture has no receipted Step 4 lineage"),
        ),
    ]
    metrics: Mapping[str, Any] | None = None
    answer_digests: dict[str, str] = {}
    if answers is None:
        checks.append(
            CheckResult(
                step=RETRIEVAL_STEP,
                name="scoring",
                status="unknown",
                detail="the run has no test answers",
            )
        )
    else:
        normalized_answers = _normalized_retrieval_answers(answers)
        metrics = retrieval_metrics(derived.hits, normalized_answers, methods=methods)
        answer_digests["answers"] = sha256_text(canonical_json(normalized_answers))
        checks.append(CheckResult(step=RETRIEVAL_STEP, name="scoring", status="pass"))
    answer_files = [
        path.relative_to(root).as_posix()
        for path in sorted(Path(root).rglob("*"))
        if path.is_file() and any(fragment in path.name.casefold() for fragment in ("answer", "oracle", "gold"))
    ]
    if plan.mode == "build":
        build_answer_failure = (
            answers is not None
            or bool(answer_files)
            or (Path(root) / "metrics.json").is_file()
            or bool(str(plan.retrieval.get("answers_sha256") or ""))
        )
        checks.append(
            CheckResult(
                step=RETRIEVAL_STEP,
                name="build_has_no_answer_data",
                status="fail" if build_answer_failure else "pass",
                detail=(
                    "build output contains answer-derived relevance or an answer-key file"
                    if build_answer_failure
                    else ""
                ),
            )
        )
    return RunChecks(
        checks=tuple(checks),
        access_control={
            "scope": "local-run",
            "answer_key_file_in_run_directory": bool(answer_files),
            "answer_key_files": answer_files,
            "answer_derived_labels_in_metrics": metrics is not None,
            "answer_derived_metric_keys": ["methods"] if metrics is not None else [],
        },
        metrics=metrics,
        test_answer_digests=answer_digests,
    )


def run_retrieval(
    plan: RunPlan,
    output_dir: Path,
    *,
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    context: RetrievalRunContext,
    providers: RetrievalProviders,
    answers: Mapping[str, Sequence[str]] | None = None,
) -> RetrievalOutcome:
    """Run retrieval through the shared runtime and finalize derived metrics."""
    if RETRIEVAL_STEP not in plan.steps:
        raise PlanError(f"the plan does not request the {RETRIEVAL_STEP!r} step")
    _reject_build_answers(plan, answers)
    query_tuple = tuple(queries)
    expected_facts = retrieval_run_plan_facts(spec, query_tuple, context, answers=answers)
    lineage_checked = _check_retrieval_lineage(
        plan,
        context,
        expected_facts["source_file_digests"],
    )
    items = plan_retrieval_items(spec, query_tuple, context)
    source_fields = _context_source_fields(context)
    query_by_id = {query.query_id: query for query in query_tuple}
    derived_box: dict[str, _RetrievalDerived] = {}

    def execute(workspace: RunWorkspace, item: WorkItem) -> WorkResult:
        query = query_by_id[str(item.payload["query_id"])]
        try:
            outcome = _execute_retrieval_query(
                workspace,
                spec=spec,
                query=query,
                work_id=item.work_id,
                context=context,
                providers=providers,
                source_fields=source_fields,
            )
        except (DenseProviderError, SparseProviderError, RerankProviderError) as exc:
            return WorkResult.failed(
                item.work_id,
                step=item.step,
                task=item.task,
                error=f"{type(exc).__name__}: retrieval provider work failed",
                provider=_persisted_provider_totals(workspace.path).plus(ProviderTotals(calls=1, failures=1)),
            )
        fields = {
            "step": item.step,
            "task": item.task,
            "provider": outcome.provider,
        }
        if outcome.state == "completed":
            return WorkResult.completed(
                item.work_id,
                result={
                    "hit_count": len(outcome.hits),
                    "exclusion_count": len(outcome.exclusions),
                },
                **fields,
            )
        if outcome.state == "completed_empty":
            return WorkResult.completed_empty(item.work_id, **fields)
        if outcome.state == "rejected":
            return WorkResult.rejected(item.work_id, reason=outcome.reason, **fields)
        return WorkResult.failed(item.work_id, error=outcome.error, **fields)

    def finalize(workspace: RunWorkspace, results: tuple[WorkResult, ...]) -> RunChecks:
        _write_retrieval_join_inputs(workspace.path, context)
        states = {result.work_id: result.state for result in results}
        derived = _derive_retrieval(
            workspace.path,
            spec=spec,
            queries=query_tuple,
            items=items,
            context=context,
            states=states,
        )
        write_retrieval_tables(workspace.path, hits=derived.hits, exclusions=derived.exclusions)
        if not (workspace.path / DENSE_EMBEDDING_TABLE).is_file():
            write_dense_embedding_rows(workspace.path, ())
        if not (workspace.path / SPARSE_EMBEDDING_TABLE).is_file():
            write_sparse_embedding_rows(workspace.path, ())
        if not (workspace.path / RERANK_SCORE_TABLE).is_file():
            write_rerank_score_rows(workspace.path, ())
        derived_box["derived"] = derived
        return _retrieval_checks(
            plan,
            expected_facts=expected_facts,
            root=workspace.path,
            derived=derived,
            answers=answers,
            methods=spec.methods,
            lineage_checked=lineage_checked,
        )

    runtime_outcome = execute_run(plan, output_dir, items=items, execute=execute, finalize=finalize)
    derived = derived_box.get("derived", _RetrievalDerived((), ()))
    metrics_path = runtime_outcome.run_directory / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.is_file() else None
    return RetrievalOutcome(runtime_outcome, derived.hits, derived.exclusions, metrics)


def recompute_retrieval(
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    context: RetrievalRunContext,
    *,
    answers: Mapping[str, Sequence[str]] | None = None,
):
    """Return a validation hook that distrusts stored derived retrieval files."""
    query_tuple = tuple(queries)

    def recompute(run_dir: Path, plan: RunPlan) -> Mapping[str, Any]:
        _reject_build_answers(plan, answers)
        expected_digest = str(plan.retrieval.get("join_inputs_sha256") or "")
        if not expected_digest:
            raise PlanError("stored retrieval plan does not bind deterministic join inputs")
        effective_context = _context_from_stored_join_inputs(
            run_dir,
            context,
            expected_digest=expected_digest,
        )
        expected_facts = retrieval_run_plan_facts(
            spec,
            query_tuple,
            effective_context,
            answers=answers,
        )
        _check_retrieval_lineage(
            plan,
            effective_context,
            expected_facts["source_file_digests"],
        )
        items = plan_retrieval_items(spec, query_tuple, effective_context)
        derived = _derive_retrieval(
            run_dir,
            spec=spec,
            queries=query_tuple,
            items=items,
            context=effective_context,
        )
        expected: dict[str, Any] = {
            RETRIEVAL_HIT_TABLE: [dataclasses.asdict(row) for row in derived.hits],
            RETRIEVAL_EXCLUSION_TABLE: [dataclasses.asdict(row) for row in derived.exclusions],
        }
        if answers is not None:
            expected["metrics.json"] = retrieval_metrics(
                derived.hits,
                _normalized_retrieval_answers(answers),
                methods=spec.methods,
            )
        elif (Path(run_dir) / "metrics.json").is_file():
            raise PlanError("metrics.json cannot be recomputed without its test answers")
        if dict(plan.retrieval) != expected_facts:
            raise PlanError("stored retrieval plan facts differ from the recomputed inputs")
        return expected

    return recompute


def rebuild_retrieval(
    spec: RetrievalSpec,
    queries: Sequence[RetrievalQuery],
    context: RetrievalRunContext,
    *,
    answers: Mapping[str, Sequence[str]] | None = None,
    providers: RetrievalProviders | None = None,
):
    """Return a provider-free rebuild hook over immutable vector/score rows."""
    if providers is not None:
        # Accepted only as an adversarial test seam: it is intentionally never
        # dereferenced or passed to a ranking function.
        _ = providers
    query_tuple = tuple(queries)

    def rebuild(workspace: RunWorkspace, plan: RunPlan) -> RunChecks:
        _reject_build_answers(plan, answers)
        expected_digest = str(plan.retrieval.get("join_inputs_sha256") or "")
        if not expected_digest:
            raise PlanError("stored retrieval plan does not bind deterministic join inputs")
        effective_context = _context_from_stored_join_inputs(
            workspace.path,
            context,
            expected_digest=expected_digest,
        )
        expected_facts = retrieval_run_plan_facts(
            spec,
            query_tuple,
            effective_context,
            answers=answers,
        )
        lineage_checked = _check_retrieval_lineage(
            plan,
            effective_context,
            expected_facts["source_file_digests"],
        )
        items = plan_retrieval_items(spec, query_tuple, effective_context)
        derived = _derive_retrieval(
            workspace.path,
            spec=spec,
            queries=query_tuple,
            items=items,
            context=effective_context,
        )
        write_retrieval_tables(workspace.path, hits=derived.hits, exclusions=derived.exclusions)
        return _retrieval_checks(
            plan,
            expected_facts=expected_facts,
            root=workspace.path,
            derived=derived,
            answers=answers,
            methods=spec.methods,
            lineage_checked=lineage_checked,
        )

    return rebuild

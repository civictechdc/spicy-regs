"""Ablate candidate selectors against the frozen 35-item development set.

The question this answers is narrow and mechanical: *which candidate channels,
fused how, put the right registry concept in front of a judge?* It computes the
top-12 candidate list every configuration would produce for each of the 35
stored source/evidence cases, and scores those lists against a release-native
development target set:

* **exact managed targets.** Every represented answer names one or more exact
  release-member IRIs and a directional grade. The reader checks the source
  evidence, release, import, expression-corpus, member, and preferred-label
  pins before retrieval starts.
* **unrepresented meanings.** ``notRepresented`` is a first-class answer, not a
  failed lookup. These items stay visible in open-set counts but are excluded
  from reachable-candidate recall.
* **rank and grade coverage.** Results report represented-item Recall@K,
  exact/close development adequacy, directional-grade coverage, and the
  source-vocabulary mix of emitted lists.

Configurations are compositions of eight channel sources:

* ``v1`` — ``select_candidate_concepts_for_text``, the production selector, run
  whole (it is a selector, not a channel: its own scheme gate and token trim
  apply).
* ``A`` / ``B`` — v2's anchored-lexical and char-3-gram channels.
* ``C`` — dense BGE retrieval over the concept index.
* ``Cw`` — the same dense index queried with complete, model-sized evidence
  windows instead of one implicitly truncated segment.
* ``Cp`` — the same dense index queried with complete evidence packed across
  numerically ordered source fields.
* ``D`` — free-keyword generate-then-map.
* ``E`` — BM25 over preferred labels and registered aliases.

Every non-``v1`` configuration fuses its channels with the same RRF at k=60 that
v2 uses, then either applies v2's source-vocabulary quotas or takes the fused ranking
straight. Both fusion and quota steps are v2's own functions, imported rather
than reimplemented, so a configuration named ``v2`` here *is* v2; a test asserts
that against the public selector.

Nothing here adopts anything. The original 35 were repeatedly inspected and
are permanently train/development data. The emitted record is ineligible for
an accuracy or adoption verdict, even when one configuration scores best.

A verified RefSpec managed release is the normal candidate source. The former
fused-registry file remains available only behind ``--allow-legacy-registry``
and ``--allow-legacy-targets`` for migration comparisons. A managed run never
reads the former resolved target files. It derives its lookup-index identity
from the physical index and scoring facts actually used, and emits one flat,
lineage-complete candidate table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.adapters.openai import (
    PROMPT_INPUT_TOKEN_BUDGET,
    PROMPT_SAFETY_MARGIN_TOKENS,
    TiktokenCounter,
)
from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.docpipeline.rkaf_projection import (
    managed_release_candidate_vocabulary,
)
from spicy_regs.docpipeline.runtime import sha256_file
from spicy_regs.docpipeline.tag_task import TagExtractionTask
from spicy_regs.enrichment.managed_release import ManagedReleaseCandidateSource
from spicy_regs.enrichment.experiment_artifacts import (
    write_experiment_artifacts,
)
from spicy_regs.evaluation_boundary import (
    DEFAULT_BOUNDARY_MANIFEST,
    DEVELOPMENT_DATASET_ID,
)
from spicy_regs.ontology.candidate_channels import (
    BM25_CHANNEL_VERSION,
    CHANNEL_DEPTH,
    DENSE_CHANNEL_VERSION,
    KEYWORD_CHANNEL_VERSION,
    BM25ConceptMapper,
    CharNgramConceptMapper,
    ConceptMapper,
    KeywordGeneration,
    concept_embedding_rule,
    generate_segment_keywords,
)
from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import (
    ANCHOR_CHANNEL_DEPTH,
    ANCHOR_RRF_K,
    CONCEPT_COLUMNS,
    _anchored_channel,
    _allowed_facet_ranking,
    _apply_source_vocabulary_quotas,
    _char_ngram_channel,
    _condition_registry,
    _fuse_reciprocal_rank,
    _segment_term_weights,
    concept_aliases,
    normalize_label,
    select_candidate_concepts_for_text,
)
from spicy_regs.ontology.llm import ontology_concept_payload
from spicy_regs.rulespec_testbed import (
    GOLD_FILE,
    PRODUCTION_SELECTOR,
    PROMPT_CONCEPT_LIMIT,
    load_testbed_inputs,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_DIR = REPO_ROOT / "output" / "segmentation-tagging-document-openai-structure-overlap-1800-v4"
DEFAULT_DATASET_DIR = REPO_ROOT / "output" / "segmented-real-data-evaluation-v2"
DEFAULT_REGISTRY = REPO_ROOT / "output" / "fused-concept-registry-v1" / "registry.parquet"
DEFAULT_INDEX_DIR = REPO_ROOT / "output" / "fused-concept-registry-v1"
DEFAULT_RESOLVED = REPO_ROOT / "docs" / "evidence" / "gold-adjudication-2026-07-27" / "resolved.json"
DEFAULT_TARGETS = REPO_ROOT / "docs" / "evidence" / "managed-vocabulary-development-2026-07-29" / "targets.json"
DEFAULT_MANAGED_BOUNDARY = DEFAULT_TARGETS.with_name("evaluation-boundary.json")
MANAGED_DEVELOPMENT_DATASET_ID = "managed-vocabulary-development-35-v1"
SELECTION_FILE_NAME = "tagging_segments.parquet"
DEFAULT_PERMISSION_FACET_IRI = "urn:ref:facet:general-subject"
DEFAULT_PERMISSION_ASSIGNMENT_ROLE_IRI = "https://rulespec.org/ns/v1#assignmentPrimary"
DEFAULT_PERMISSION_RESOURCE_ROUTE = "document"

CHANNEL_A = "A"
CHANNEL_B = "B"
CHANNEL_C = "C"
CHANNEL_CW = "Cw"
CHANNEL_CP = "Cp"
CHANNEL_D = "D"
CHANNEL_E = "E"
DENSE_EVIDENCE_WINDOW_VERSION = "dense-evidence-windows-q1"
DENSE_PACKED_EVIDENCE_VERSION = "dense-packed-evidence-q1"
DENSE_EVIDENCE_WINDOW_BOUNDARY_POLICY = "sentence-then-whitespace-prefix-v1"
DENSE_PACKED_EVIDENCE_SEPARATOR = "\n"
_EVIDENCE_FIELD = re.compile(r"^evidence_(0|[1-9][0-9]*)$")

SKOS_PREF_LABEL = "http://www.w3.org/2004/02/skos/core#prefLabel"
SKOS_ALT_LABEL = "http://www.w3.org/2004/02/skos/core#altLabel"
SKOS_DEFINITION = "http://www.w3.org/2004/02/skos/core#definition"

TARGET_GRADES = frozenset(
    {
        "exact",
        "close",
        "targetBroaderThanGold",
        "targetNarrowerThanGold",
        "related",
        "wrong",
        "notRepresented",
    }
)
ADEQUATE_TARGET_GRADES = frozenset({"exact", "close"})

CANDIDATE_LINEAGE_COLUMNS = (
    "itemId",
    "configuration",
    "conceptId",
    "channel",
    "rank",
    "candidateRank",
    "score",
    "scoreKind",
    "scoreSourceSegment",
    "queryRepresentation",
    "queryWindowId",
    "queryTextSha256",
    "querySourceField",
    "querySourceStartChar",
    "querySourceEndChar",
    "querySourceFragments",
    "queryModelTokenCount",
    "limit",
    "truncated",
    "facet",
    "scheme",
    "referenceResourceRelease",
    "registryImportSnapshot",
    "expressionCorpusSnapshot",
    "lookupIndexManifest",
    "indexedExpressionIds",
    "availableExpressionIds",
    "managedReleaseManifest",
    "managedReleaseManifestDigest",
    "usageCeiling",
    "evaluationScope",
)


def _git_revision(repository: Path) -> str | None:
    """Return the local revision without treating it as the whole code identity."""

    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    revision = result.stdout.strip()
    return revision if result.returncode == 0 and revision else None


def _source_tree_facts(
    root: Path,
    paths: Sequence[Path],
) -> dict[str, Any]:
    """Digest the exact behavior-bearing source bytes used by this run."""

    inventory = {path.relative_to(root).as_posix(): sha256_file(path) for path in sorted(paths) if path.is_file()}
    return {
        "revision": _git_revision(root),
        "sourceTreeDigest": ("sha256:" + hashlib.sha256(canonical_json(inventory).encode("utf-8")).hexdigest()),
        "sourceFileCount": len(inventory),
    }


def experiment_code_identity() -> dict[str, Any]:
    """Pin both the Spicy runner and the editable RefSpec dependency."""

    spicy_paths = [
        REPO_ROOT / "pyproject.toml",
        Path(__file__).resolve(),
        *(REPO_ROOT / "src" / "spicy_regs").rglob("*.py"),
    ]
    refspec_root = REPO_ROOT / "RefSpec"
    refspec_paths = [
        refspec_root / "pyproject.toml",
        *(refspec_root / "src" / "refspec").rglob("*.py"),
    ]
    return {
        "spicyRegs": _source_tree_facts(REPO_ROOT, spicy_paths),
        "refspec": _source_tree_facts(refspec_root, refspec_paths),
    }


@dataclass(frozen=True)
class Configuration:
    """One selector configuration: which channels, fused, and whether quotas apply."""

    name: str
    channels: tuple[str, ...]
    quotas: bool
    note: str


CONFIGURATIONS: tuple[Configuration, ...] = (
    Configuration("v1", (), False, "production selector, whole (facet gate + token trim)"),
    Configuration("v2", (CHANNEL_A, CHANNEL_B), True, "anchored + char-ngram, RRF, source-vocabulary quotas"),
    Configuration("v2-noquota", (CHANNEL_A, CHANNEL_B), False, "v2 fused ranking, no quotas"),
    Configuration("v2+C", (CHANNEL_A, CHANNEL_B, CHANNEL_C), True, "v2 channels plus dense retrieval"),
    Configuration(
        "v2+Cw",
        (CHANNEL_A, CHANNEL_B, CHANNEL_CW),
        True,
        "v2 channels plus complete dense evidence windows",
    ),
    Configuration("v2+D", (CHANNEL_A, CHANNEL_B, CHANNEL_D), True, "v2 channels plus generate-then-map"),
    Configuration("v2+C+D", (CHANNEL_A, CHANNEL_B, CHANNEL_C, CHANNEL_D), True, "all four channels"),
    Configuration(
        "v2+C+D-noquota", (CHANNEL_A, CHANNEL_B, CHANNEL_C, CHANNEL_D), False, "all four channels, no quotas"
    ),
    Configuration("C-alone", (CHANNEL_C,), False, "dense retrieval only"),
    Configuration("Cw-alone", (CHANNEL_CW,), False, "complete dense evidence windows only"),
    Configuration("Cp-alone", (CHANNEL_CP,), False, "complete packed dense evidence only"),
    Configuration("D-alone", (CHANNEL_D,), False, "generate-then-map only"),
    Configuration("BM25-alone", (CHANNEL_E,), False, "BM25 lexical retrieval only"),
    Configuration("BM25+B", (CHANNEL_E, CHANNEL_B), True, "BM25 plus char-ngram, source-vocabulary quotas"),
    Configuration(
        "BM25+B+C",
        (CHANNEL_E, CHANNEL_B, CHANNEL_C),
        True,
        "BM25 plus char-ngram plus dense retrieval, source-vocabulary quotas",
    ),
    Configuration(
        "BM25+B+Cw",
        (CHANNEL_E, CHANNEL_B, CHANNEL_CW),
        True,
        "BM25 plus char-ngram plus complete dense evidence windows, source-vocabulary quotas",
    ),
)
CONFIGURATIONS_BY_NAME = {configuration.name: configuration for configuration in CONFIGURATIONS}
DEFAULT_CONFIGURATION_NAMES = tuple(
    configuration.name for configuration in CONFIGURATIONS if CHANNEL_D not in configuration.channels
)


class AblationError(RuntimeError):
    """The stored inputs cannot produce the requested ablation."""


# --------------------------------------------------------------------------
# candidate source
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateRegistry:
    """Selector rows plus the exact source facts behind every candidate."""

    rows: tuple[Mapping[str, Any], ...]
    selector_file: Path
    source_facts: Mapping[str, Any]
    lineage_by_member: Mapping[str, Mapping[str, Any]]
    managed_source: ManagedReleaseCandidateSource | None


def _plain_lineage_value(value: Any) -> Any:
    """Copy immutable RefSpec views into JSON-compatible experiment values."""

    if isinstance(value, Mapping):
        return {str(key): _plain_lineage_value(child) for key, child in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_plain_lineage_value(child) for child in value]
    return value


def _managed_member_lineage(
    source: ManagedReleaseCandidateSource,
) -> dict[str, dict[str, Any]]:
    """Preserve release and expression identity after concept-level ranking."""

    expressions_by_member: dict[str, list[dict[str, Any]]] = {}
    for expression in source.iter_expressions():
        expressions_by_member.setdefault(expression.member_iri, []).append(
            {
                "expression_id": expression.expression_id,
                "canonical_payload_digest": expression.record.get("canonicalPayloadDigest"),
                "indexed_text_digest": expression.record.get("indexedTextDigest"),
                "source_property_or_path": (expression.source_property_or_path),
                "semantic_property_iri": expression.semantic_property_iri,
                "language_tag": expression.language_tag,
                "original_literal": expression.original_literal,
                "reference_resource_release": _plain_lineage_value(expression.record.get("referenceResourceRelease")),
                "registry_import_snapshot": _plain_lineage_value(expression.record.get("registryImportSnapshot")),
                "distribution_artifact": _plain_lineage_value(expression.record.get("distributionArtifact")),
            }
        )

    lineage: dict[str, dict[str, Any]] = {}
    for member_iri in sorted(expressions_by_member):
        member = source.lookup_member(member_iri)
        if member is None:
            raise AblationError(f"managed expression member {member_iri!r} is not in the opened release")
        lineage[member_iri] = {
            "member_iri": member.member_iri,
            "release_iri": member.release_iri,
            "scheme_iri": member.scheme_iri,
            "expressions": expressions_by_member[member_iri],
        }
    return lineage


def load_candidate_registry(
    *,
    output_dir: Path,
    managed_release_manifest: Path | None = None,
    managed_release_manifest_digest: str | None = None,
    permission_facet_iri: str = DEFAULT_PERMISSION_FACET_IRI,
    permission_assignment_role_iri: str = (DEFAULT_PERMISSION_ASSIGNMENT_ROLE_IRI),
    permission_resource_route: str = DEFAULT_PERMISSION_RESOURCE_ROUTE,
    candidate_default_language: str = "en",
    registry_file: Path | None = None,
    allow_legacy_registry: bool = False,
) -> CandidateRegistry:
    """Load exactly one managed release or an explicitly allowed legacy file."""

    managed_requested = managed_release_manifest is not None
    legacy_requested = registry_file is not None or allow_legacy_registry
    if managed_requested and legacy_requested:
        raise AblationError("choose the managed release or the legacy fused registry, not both")
    if not managed_requested:
        if not allow_legacy_registry:
            raise AblationError(
                "a managed release is required; pass --allow-legacy-registry "
                "to opt into the migration-only fused registry"
            )
        legacy_file = Path(registry_file or DEFAULT_REGISTRY)
        rows = tuple(read_parquet_rows(legacy_file))
        if not rows:
            raise AblationError(f"legacy registry is missing or empty: {legacy_file}")
        return CandidateRegistry(
            rows=rows,
            selector_file=legacy_file,
            source_facts={
                "mode": "legacyFusedRegistry",
                "migration_only": True,
                "usage_ceiling": "candidateUseOnly",
                "registry_file": str(legacy_file),
                "registry_sha256": sha256_file(legacy_file),
            },
            lineage_by_member={},
            managed_source=None,
        )

    if not managed_release_manifest_digest:
        raise AblationError("managed release manifest digest is required")

    manifest_path = Path(managed_release_manifest)
    bootstrap_digest = (
        "sha256:"
        + hashlib.sha256(
            canonical_json(
                {
                    "schema_version": "managed-release-selector-projection-v1",
                    "bundle_manifest_digest": managed_release_manifest_digest,
                    "permission_facet_iri": permission_facet_iri,
                    "permission_assignment_role_iri": (permission_assignment_role_iri),
                    "permission_resource_route": (permission_resource_route),
                    "candidate_default_language": candidate_default_language,
                }
            ).encode("utf-8")
        ).hexdigest()
    )
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=managed_release_manifest_digest,
        lookup_index_manifest={
            "id": (f"urn:spicy-regs:lookup-index-plan:{bootstrap_digest.removeprefix('sha256:')}"),
            "digest": bootstrap_digest,
        },
        permission_facet_iri=permission_facet_iri,
        permission_assignment_role_iri=(permission_assignment_role_iri),
        permission_resource_route=permission_resource_route,
    )
    if source.usage_ceiling != "candidateUseOnly":
        raise AblationError("managed release source must remain candidateUseOnly")
    vocabulary = managed_release_candidate_vocabulary(
        source,
        default_language=candidate_default_language,
    )
    rows = tuple(dict(row) for row in vocabulary.selector_rows)
    if not rows:
        raise AblationError("managed release produced no lookup-eligible selector rows")
    selector_facets = {str(row.get("facet") or "") for row in rows}
    if len(selector_facets) != 1 or not next(iter(selector_facets)):
        raise AblationError("managed release must resolve to one Spicy selector facet")
    selector_facet = next(iter(selector_facets))
    lineage = _managed_member_lineage(source)
    missing_lineage = sorted(
        str(row.get("concept_id") or "") for row in rows if str(row.get("concept_id") or "") not in lineage
    )
    if missing_lineage:
        raise AblationError(f"managed selector rows lost expression lineage for {missing_lineage[:3]!r}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selector_file = write_parquet_rows(
        output_dir / "managed-release-selector-rows.parquet",
        columns=CONCEPT_COLUMNS,
        rows=(dict(row) for row in rows),
    )
    return CandidateRegistry(
        rows=rows,
        selector_file=selector_file,
        source_facts={
            "mode": "managedRelease",
            "usage_ceiling": source.usage_ceiling,
            "bundle_manifest": str(manifest_path),
            "bundle_manifest_digest": managed_release_manifest_digest,
            "publication_release_id": source.view.release_id,
            "expression_corpus_snapshot": dict(source.expression_corpus_snapshot),
            "permission_facet_iri": (source.candidate_permission.facet_iri),
            "permission_assignment_role_iri": (source.candidate_permission.assignment_role_iri),
            "permission_resource_route": (source.candidate_permission.resource_route),
            "permission_reference_resource_release": dict(source.candidate_permission.reference_resource_release),
            "permission_registry_import_snapshot": dict(source.candidate_permission.registry_import_snapshot),
            "permission_output_profile": {
                "id": source.candidate_permission.output_profile["id"],
                "version": source.candidate_permission.output_profile["version"],
                "digest": source.candidate_permission.output_profile["contentDigest"],
            },
            "permission_enrichment_profile": {
                "id": source.candidate_permission.enrichment_profile["id"],
                "version": source.candidate_permission.enrichment_profile["version"],
                "digest": source.candidate_permission.enrichment_profile["contentDigest"],
            },
            "permission_coverage_report": {
                "id": source.candidate_permission.coverage_report["id"],
                "digest": source.candidate_permission.coverage_report["canonicalPayloadDigest"],
            },
            "permission_registry_deployment": {
                "id": source.candidate_permission.registry_deployment["id"],
                "digest": source.candidate_permission.registry_deployment["canonicalPayloadDigest"],
            },
            "permission_required_import_features": list(source.candidate_permission.required_import_features),
            "selector_facet": selector_facet,
            "candidate_default_language": candidate_default_language,
            "selector_file": str(selector_file),
            "selector_file_sha256": sha256_file(selector_file),
            "candidate_member_count": len(rows),
            "candidate_expression_count": sum(len(value["expressions"]) for value in lineage.values()),
        },
        lineage_by_member=lineage,
        managed_source=source,
    )


def _physical_index_facts(facts: Mapping[str, Any]) -> dict[str, Any]:
    """Keep behavior-changing index facts and digest any stored artifact."""

    stable = {
        str(key): value
        for key, value in facts.items()
        if key
        not in {
            "channels",
            "path",
            "query_representations",
            "query_count",
            "query_token_max",
            "queries_truncated",
            "seconds",
            "source",
        }
    }
    path_value = facts.get("path")
    if path_value:
        path = Path(str(path_value))
        if not path.is_file():
            raise AblationError(f"lookup index facts name a missing artifact: {path}")
        stable["artifact_sha256"] = sha256_file(path)
    return stable


def derive_lookup_index_identity(
    *,
    candidate_registry: CandidateRegistry,
    configurations: Sequence[Configuration],
    mapper_facts: Mapping[str, Any],
    bm25_facts: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Derive the physical lookup pin from this run's actual index facts."""

    if candidate_registry.managed_source is None:
        raise AblationError("lookup-index identity is defined only for a managed release")
    lexical_structure_used = any(
        configuration.name == "v1" or bool({CHANNEL_A, CHANNEL_B} & set(configuration.channels))
        for configuration in configurations
    )
    physical_structures: dict[str, Any] = {}
    if lexical_structure_used:
        physical_structures["conditionedLexicalRows"] = {
            "selectorRegistrySha256": sha256_file(candidate_registry.selector_file),
            "candidateCount": len(candidate_registry.rows),
        }
    if mapper_facts:
        mapper_identity = _physical_index_facts(mapper_facts)
        if mapper_facts.get("kind") == "char-ngram-fallback":
            mapper_identity.update(
                {
                    "selectorRegistrySha256": sha256_file(candidate_registry.selector_file),
                    "selectorRowCount": len(candidate_registry.rows),
                }
            )
        physical_structures["denseOrFallbackIndex"] = mapper_identity
    if bm25_facts:
        physical_structures["bm25Index"] = _physical_index_facts(bm25_facts)
    identity = {
        "schemaVersion": "candidate-selector-lookup-index/v1",
        "expressionCorpusSnapshot": dict(candidate_registry.managed_source.expression_corpus_snapshot),
        "physicalStructures": physical_structures,
    }
    digest = "sha256:" + hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    reference = {
        "id": (f"urn:spicy-regs:lookup-index:{digest.removeprefix('sha256:')}"),
        "digest": digest,
    }
    return identity, reference


def _one_or_many_expression_references(
    expressions: Sequence[Mapping[str, Any]],
    field: str,
) -> Any:
    """Return every exact expression reference without silently choosing one."""

    by_encoding: dict[str, Any] = {}
    for expression in expressions:
        value = expression.get(field)
        if value is not None:
            by_encoding[canonical_json(value)] = value
    values = [by_encoding[key] for key in sorted(by_encoding)]
    if len(values) == 1:
        return values[0]
    return values


def _channel_identity(
    channel: str,
    *,
    mapper_facts: Mapping[str, Any],
) -> str:
    """Name the actual candidate path rather than only its ablation letter."""

    if channel == "v1":
        return PRODUCTION_SELECTOR
    if channel == CHANNEL_A:
        return "anchored-lexical-v2"
    if channel == CHANNEL_B:
        return "char-3-gram-v2"
    if channel == CHANNEL_C:
        return str(mapper_facts.get("version") or mapper_facts.get("channel_version") or DENSE_CHANNEL_VERSION)
    if channel == CHANNEL_CW:
        return DENSE_EVIDENCE_WINDOW_VERSION
    if channel == CHANNEL_CP:
        return DENSE_PACKED_EVIDENCE_VERSION
    if channel == CHANNEL_D:
        return KEYWORD_CHANNEL_VERSION
    if channel == CHANNEL_E:
        return BM25_CHANNEL_VERSION
    raise AblationError(f"unknown candidate channel {channel!r}")


def _indexed_expression_ids_by_channel(
    *,
    row: Mapping[str, Any],
    expressions: Sequence[Mapping[str, Any]],
    channels: Sequence[str],
    mapper_facts: Mapping[str, Any],
    embedding_definition_kept: bool,
) -> dict[str, list[str]]:
    """Name only expressions that the channel's concrete index consumed."""

    try:
        alternates = json.loads(str(row.get("alt_labels_json") or "[]"))
    except json.JSONDecodeError as error:
        raise AblationError(f"{row.get('concept_id')!r} has invalid alternate labels") from error
    if not isinstance(alternates, list):
        raise AblationError(f"{row.get('concept_id')!r} alternate labels must be an array")
    surface_literals = {str(value) for value in [row.get("pref_label"), *alternates] if str(value or "").strip()}
    definition = str(row.get("definition") or "").strip()
    surface_ids = [
        str(expression["expression_id"])
        for expression in expressions
        if expression.get("semantic_property_iri") in {SKOS_PREF_LABEL, SKOS_ALT_LABEL}
        and str(expression.get("original_literal") or "") in surface_literals
    ]
    definition_ids = [
        str(expression["expression_id"])
        for expression in expressions
        if expression.get("semantic_property_iri") == SKOS_DEFINITION
        and definition
        and str(expression.get("original_literal") or "") == definition
    ]
    dense_uses_definition = mapper_facts.get("kind") == "dense" and embedding_definition_kept
    by_channel: dict[str, list[str]] = {}
    for channel in channels:
        indexed = list(surface_ids)
        if channel in {CHANNEL_C, CHANNEL_CW, CHANNEL_CP, CHANNEL_D} and dense_uses_definition:
            indexed.extend(definition_ids)
        by_channel[channel] = list(dict.fromkeys(indexed))
    return by_channel


def finalize_candidate_lineage(
    *,
    candidate_registry: CandidateRegistry,
    lookup_index_identity: Mapping[str, Any],
    lookup_index_manifest: Mapping[str, str],
    mapper_facts: Mapping[str, Any],
    channels: Sequence[str],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Bind every managed candidate to the actual derived lookup-index pin."""

    bootstrap = candidate_registry.managed_source
    if bootstrap is None:
        return dict(candidate_registry.source_facts), {}
    source = ManagedReleaseCandidateSource(
        view=bootstrap.view,
        lookup_index_manifest=lookup_index_manifest,
        permission_facet_iri=bootstrap.permission_facet_iri,
        permission_assignment_role_iri=(bootstrap.permission_assignment_role_iri),
        permission_resource_route=bootstrap.permission_resource_route,
    )
    corpus = dict(source.expression_corpus_snapshot)
    lookup = dict(source.lookup_index_manifest)
    lineage = _managed_member_lineage(source)
    rows_by_id = {str(row.get("concept_id") or ""): row for row in candidate_registry.rows}
    embedding_rule = concept_embedding_rule(candidate_registry.rows)
    for value in lineage.values():
        member_iri = str(value["member_iri"])
        row = rows_by_id.get(member_iri)
        if row is None:
            continue
        expressions = value["expressions"]
        value["available_expression_ids"] = [str(expression["expression_id"]) for expression in expressions]
        value["indexed_expression_ids_by_channel"] = _indexed_expression_ids_by_channel(
            row=row,
            expressions=expressions,
            channels=channels,
            mapper_facts=mapper_facts,
            embedding_definition_kept=embedding_rule.keeps_definition(row),
        )
        value["channel_identities"] = {
            channel: _channel_identity(
                channel,
                mapper_facts=mapper_facts,
            )
            for channel in channels
        }
        value["reference_resource_release"] = _one_or_many_expression_references(
            expressions,
            "reference_resource_release",
        )
        value["registry_import_snapshot"] = _one_or_many_expression_references(
            expressions,
            "registry_import_snapshot",
        )
        value["facet"] = str(row.get("facet") or "")
        value["managed_release_manifest"] = candidate_registry.source_facts["bundle_manifest"]
        value["managed_release_manifest_digest"] = candidate_registry.source_facts["bundle_manifest_digest"]
        value["expression_corpus_snapshot"] = corpus
        value["lookup_index_manifest"] = lookup
        value["usage_ceiling"] = source.usage_ceiling
    source_facts = {
        **dict(candidate_registry.source_facts),
        "expression_corpus_snapshot": corpus,
        "lookup_index_manifest": lookup,
        "lookup_index_identity": dict(lookup_index_identity),
    }
    return source_facts, lineage


# --------------------------------------------------------------------------
# gold items
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class GradedTarget:
    """One exact managed member and its directional development grade."""

    concept_id: str
    pref_label: str
    grade: str
    adequate_for_development: bool


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
    # Forward-looking managed-release targets. Legacy items leave this empty.
    registered_targets: tuple[GradedTarget, ...] = ()
    not_represented: bool = False

    @property
    def represented_target_ids(self) -> tuple[str, ...]:
        """Exact managed target IRIs, or the legacy reviewed target."""

        if self.registered_targets:
            return tuple(target.concept_id for target in self.registered_targets)
        if self.adequate_concept_id:
            return (self.adequate_concept_id,)
        return ()

    @property
    def adequate_target_ids(self) -> tuple[str, ...]:
        """Targets eligible for development-only adequacy scoring."""

        if self.registered_targets:
            return tuple(target.concept_id for target in self.registered_targets if target.adequate_for_development)
        if self.adequate_concept_id:
            return (self.adequate_concept_id,)
        return ()


@dataclass(frozen=True)
class ManagedTargetSet:
    """Validated managed targets plus the facts bound into each experiment."""

    dataset_id: str
    targets_by_item: Mapping[str, tuple[GradedTarget, ...]]
    not_represented_items: frozenset[str]
    source_facts: Mapping[str, Any]
    vocabulary_facts: Mapping[str, Any]
    review_facts: Mapping[str, Any]


def _digest_reference(value: object, label: str) -> str:
    text = str(value or "")
    if not text.startswith("sha256:") or len(text) != 71:
        raise AblationError(f"{label} must be sha256:<64 lowercase hex>")
    try:
        int(text.removeprefix("sha256:"), 16)
    except ValueError as error:
        raise AblationError(f"{label} must be sha256:<64 lowercase hex>") from error
    if text != text.lower():
        raise AblationError(f"{label} must be sha256:<64 lowercase hex>")
    return text


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AblationError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        raise AblationError(f"{label} must be an array")
    return value


def _exact_reference(value: object, label: str) -> dict[str, str]:
    reference = _mapping(value, label)
    if set(reference) not in ({"id", "digest"}, {"id", "version", "digest"}):
        raise AblationError(f"{label} must contain id, digest, and optional version only")
    identifier = str(reference.get("id") or "")
    if not identifier:
        raise AblationError(f"{label}.id is required")
    result = {
        "id": identifier,
        "digest": _digest_reference(reference.get("digest"), f"{label}.digest"),
    }
    if "version" in reference:
        version = str(reference.get("version") or "")
        if not version:
            raise AblationError(f"{label}.version must be nonempty")
        result["version"] = version
    return result


def _single_expression_reference(
    candidate_registry: CandidateRegistry,
    field: str,
) -> dict[str, str]:
    expressions = [
        expression
        for lineage in candidate_registry.lineage_by_member.values()
        for expression in lineage.get("expressions", ())
        if isinstance(expression, Mapping)
    ]
    value = _one_or_many_expression_references(expressions, field)
    if isinstance(value, list):
        raise AblationError(f"managed candidate universe contains multiple {field} values")
    return _exact_reference(value, f"managed candidate {field}")


def _validate_source_target_pins(
    source_evidence: Mapping[str, Any],
    *,
    source_facts: Mapping[str, Any],
    segmentation_facts: Mapping[str, Any],
) -> dict[str, Any]:
    if str(source_evidence.get("datasetId") or "") != MANAGED_DEVELOPMENT_DATASET_ID:
        raise AblationError(f"managed target source dataset must be {MANAGED_DEVELOPMENT_DATASET_ID!r}")
    gold = _mapping(source_evidence.get("gold"), "sourceEvidence.gold")
    selection = _mapping(
        source_evidence.get("selection"),
        "sourceEvidence.selection",
    )
    expected_gold_digest = _digest_reference(
        gold.get("digest"),
        "sourceEvidence.gold.digest",
    )
    expected_selection_digest = _digest_reference(
        selection.get("digest"),
        "sourceEvidence.selection.digest",
    )
    actual_gold_digest = "sha256:" + str(source_facts.get("gold_sha256") or "")
    actual_selection_digest = "sha256:" + str(source_facts.get("selection_sha256") or "")
    if expected_gold_digest != actual_gold_digest:
        raise AblationError("managed target gold digest does not match the loaded source evidence")
    if expected_selection_digest != actual_selection_digest:
        raise AblationError("managed target selection digest does not match the loaded source evidence")
    if gold.get("rowCount") != segmentation_facts.get("gold_span_count"):
        raise AblationError("managed target gold row count does not match the loaded source evidence")
    if selection.get("rowCount") != segmentation_facts.get("selected_segment_count"):
        raise AblationError("managed target selection row count does not match the loaded source evidence")
    return {
        "datasetId": MANAGED_DEVELOPMENT_DATASET_ID,
        "gold": dict(gold),
        "selection": dict(selection),
    }


def _validate_vocabulary_target_pins(
    vocabulary: Mapping[str, Any],
    *,
    candidate_registry: CandidateRegistry,
) -> dict[str, Any]:
    if candidate_registry.managed_source is None:
        raise AblationError("managed-release targets cannot be used with the legacy fused registry")
    expected = {
        "managedReleaseManifestDigest": _digest_reference(
            vocabulary.get("managedReleaseManifestDigest"),
            "vocabularyUniverse.managedReleaseManifestDigest",
        ),
        "publicationReleaseId": str(vocabulary.get("publicationReleaseId") or ""),
        "referenceResourceRelease": _exact_reference(
            vocabulary.get("referenceResourceRelease"),
            "vocabularyUniverse.referenceResourceRelease",
        ),
        "registryImportSnapshot": _exact_reference(
            vocabulary.get("registryImportSnapshot"),
            "vocabularyUniverse.registryImportSnapshot",
        ),
        "expressionCorpusSnapshot": _exact_reference(
            vocabulary.get("expressionCorpusSnapshot"),
            "vocabularyUniverse.expressionCorpusSnapshot",
        ),
    }
    actual = {
        "managedReleaseManifestDigest": str(candidate_registry.source_facts.get("bundle_manifest_digest") or ""),
        "publicationReleaseId": str(candidate_registry.source_facts.get("publication_release_id") or ""),
        "referenceResourceRelease": _single_expression_reference(
            candidate_registry,
            "reference_resource_release",
        ),
        "registryImportSnapshot": _single_expression_reference(
            candidate_registry,
            "registry_import_snapshot",
        ),
        "expressionCorpusSnapshot": dict(candidate_registry.managed_source.expression_corpus_snapshot),
    }
    if expected != actual:
        differing = sorted(key for key in expected if expected.get(key) != actual.get(key))
        raise AblationError(f"managed target vocabulary universe differs from the opened release: {differing}")
    return actual


def load_managed_targets(
    path: Path,
    *,
    candidate_registry: CandidateRegistry,
    items: Sequence[GoldItem],
    source_facts: Mapping[str, Any],
    segmentation_facts: Mapping[str, Any],
) -> ManagedTargetSet:
    """Load exact managed IRIs; never migrate or rebind a fused identifier."""

    try:
        document = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise AblationError(f"managed target file is unreadable: {path}") from error
    root = _mapping(document, "managed target document")
    if root.get("schemaVersion") != "spicy-managed-development-targets/1":
        raise AblationError("unsupported managed target schemaVersion")
    dataset_id = str(root.get("id") or "")
    if not dataset_id:
        raise AblationError("managed target id is required")
    if root.get("evaluationScope") != "developmentOnly":
        raise AblationError("managed targets must remain developmentOnly")

    validated_source = _validate_source_target_pins(
        _mapping(root.get("sourceEvidence"), "sourceEvidence"),
        source_facts=source_facts,
        segmentation_facts=segmentation_facts,
    )
    validated_vocabulary = _validate_vocabulary_target_pins(
        _mapping(root.get("vocabularyUniverse"), "vocabularyUniverse"),
        candidate_registry=candidate_registry,
    )
    review = _mapping(root.get("review"), "review")
    if review.get("sealed") is not False:
        raise AblationError("this experiment reader accepts only unsealed development targets")

    items_by_id = {item.item_id: item for item in items}
    concepts_by_id = {
        str(row.get("concept_id") or ""): row for row in candidate_registry.rows if str(row.get("concept_id") or "")
    }
    targets_by_item: dict[str, tuple[GradedTarget, ...]] = {}
    not_represented: set[str] = set()
    for index, raw_expectation in enumerate(_array(root.get("expectations"), "expectations")):
        expectation = _mapping(raw_expectation, f"expectations[{index}]")
        item_id = str(expectation.get("itemId") or "")
        if item_id in targets_by_item or item_id in not_represented:
            raise AblationError(f"managed target item {item_id!r} appears twice")
        item = items_by_id.get(item_id)
        if item is None:
            raise AblationError(f"managed target item {item_id!r} is outside the source evidence")
        if expectation.get("goldId") != item.gold_id:
            raise AblationError(f"managed target item {item_id!r} names the wrong goldId")
        intended = _mapping(
            expectation.get("intendedMeaning"),
            f"{item_id}.intendedMeaning",
        )
        intended_value = str(intended.get("value") or "")
        if intended.get("language") != "en" or normalize_label(intended_value) != normalize_label(item.label):
            raise AblationError(f"managed target item {item_id!r} changed its intended meaning")
        raw_targets = _array(
            expectation.get("registeredTargets"),
            f"{item_id}.registeredTargets",
        )
        outcome = str(expectation.get("outcome") or "")
        if outcome == "notRepresented":
            if raw_targets:
                raise AblationError(f"{item_id} cannot mix notRepresented with registered targets")
            open_label = _mapping(
                expectation.get("openLabel"),
                f"{item_id}.openLabel",
            )
            if open_label.get("language") != "en" or not str(open_label.get("value") or "").strip():
                raise AblationError(f"{item_id}.openLabel needs an English value")
            targets_by_item[item_id] = ()
            not_represented.add(item_id)
            continue
        if outcome != "represented" or not raw_targets:
            raise AblationError(f"{item_id} must be represented by targets or explicitly notRepresented")

        seen_targets: set[str] = set()
        targets: list[GradedTarget] = []
        for target_index, raw_target in enumerate(raw_targets):
            target = _mapping(
                raw_target,
                f"{item_id}.registeredTargets[{target_index}]",
            )
            concept_id = str(target.get("conceptId") or "")
            if concept_id in seen_targets:
                raise AblationError(f"{item_id} repeats managed target {concept_id!r}")
            seen_targets.add(concept_id)
            concept = concepts_by_id.get(concept_id)
            member = candidate_registry.managed_source.lookup_member(concept_id)
            if concept is None or member is None:
                raise AblationError(f"{item_id} target {concept_id!r} is not an exact member of the release")
            expected_release = validated_vocabulary["referenceResourceRelease"]["id"]
            if member.release_iri != expected_release:
                raise AblationError(f"{item_id} target {concept_id!r} belongs to another release")
            pref_label = str(target.get("prefLabel") or "")
            if pref_label != str(concept.get("pref_label") or ""):
                raise AblationError(f"{item_id} target {concept_id!r} changed its preferred label")
            grade = str(target.get("grade") or "")
            if grade not in TARGET_GRADES - {"notRepresented"}:
                raise AblationError(f"{item_id} uses unsupported grade {grade!r}")
            adequate = target.get("adequateForDevelopment")
            if type(adequate) is not bool:
                raise AblationError(f"{item_id} target {concept_id!r} needs adequateForDevelopment")
            if bool(adequate) != (grade in ADEQUATE_TARGET_GRADES):
                raise AblationError(f"{item_id} target {concept_id!r} has inconsistent adequacy")
            if grade == "exact" and concept_id not in alias_index([concept]).get(
                normalize_label(intended_value),
                (),
            ):
                raise AblationError(f"{item_id} exact target is not a registered label expression")
            targets.append(
                GradedTarget(
                    concept_id=concept_id,
                    pref_label=pref_label,
                    grade=grade,
                    adequate_for_development=bool(adequate),
                )
            )
        targets_by_item[item_id] = tuple(targets)

    missing = sorted(set(items_by_id) - set(targets_by_item))
    extra = sorted(set(targets_by_item) - set(items_by_id))
    if missing or extra:
        raise AblationError(
            f"managed targets must cover the source evidence exactly: missing={missing[:3]!r}, extra={extra[:3]!r}"
        )
    return ManagedTargetSet(
        dataset_id=dataset_id,
        targets_by_item=targets_by_item,
        not_represented_items=frozenset(not_represented),
        source_facts=validated_source,
        vocabulary_facts=validated_vocabulary,
        review_facts=dict(review),
    )


def attach_managed_targets(
    items: Sequence[GoldItem],
    target_set: ManagedTargetSet,
) -> list[GoldItem]:
    """Attach an already validated target set without changing source labels."""

    return [
        replace(
            item,
            registered_targets=target_set.targets_by_item[item.item_id],
            adequate_concept_id=None,
            not_represented=(item.item_id in target_set.not_represented_items),
        )
        for item in items
    ]


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


def bind_reviewed_adequate_targets(
    resolved: Mapping[str, Any],
    *,
    candidate_ids: set[str],
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Keep reviewed targets only when their exact ID is in this universe."""

    reviewed = adequate_concepts(resolved)
    bound = {item_id: concept_id for item_id, concept_id in reviewed.items() if concept_id in candidate_ids}
    foreign = [
        {
            "item_id": item_id,
            "concept_id": concept_id,
            "reason": "notExactMemberOfCandidateUniverse",
        }
        for item_id, concept_id in sorted(reviewed.items())
        if concept_id not in candidate_ids
    ]
    return bound, foreign


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


@dataclass(frozen=True)
class DenseEvidenceWindow:
    """One exact source substring that fits the dense model without truncation."""

    window_id: str
    segment_id: str
    field_key: str
    field_ordinal: int
    window_ordinal: int
    text: str
    field_start_char: int
    field_end_char: int
    source_field: str
    source_start_char: int
    source_end_char: int
    model_token_count: int

    def score_provenance(self) -> dict[str, Any]:
        """Return the query facts retained when this window wins a score."""

        return {
            "query_representation": DENSE_EVIDENCE_WINDOW_VERSION,
            "query_window_id": self.window_id,
            "query_text_sha256": "sha256:" + hashlib.sha256(self.text.encode("utf-8")).hexdigest(),
            "query_source_field": self.source_field,
            "query_source_start_char": self.source_start_char,
            "query_source_end_char": self.source_end_char,
            "query_model_token_count": self.model_token_count,
        }


@dataclass(frozen=True)
class PackedEvidenceFragment:
    """One exact source substring contributing to a packed dense query."""

    field_key: str
    source_field: str
    source_start_char: int
    source_end_char: int
    query_start_char: int
    query_end_char: int

    def facts(self) -> dict[str, Any]:
        return {
            "fieldKey": self.field_key,
            "sourceField": self.source_field,
            "sourceStartChar": self.source_start_char,
            "sourceEndChar": self.source_end_char,
            "queryStartChar": self.query_start_char,
            "queryEndChar": self.query_end_char,
        }


@dataclass(frozen=True)
class PackedDenseEvidenceWindow:
    """A model-sized query assembled from ordered exact source fragments."""

    window_id: str
    segment_id: str
    window_ordinal: int
    text: str
    fragments: tuple[PackedEvidenceFragment, ...]
    model_token_count: int

    def score_provenance(self) -> dict[str, Any]:
        provenance: dict[str, Any] = {
            "query_representation": DENSE_PACKED_EVIDENCE_VERSION,
            "query_window_id": self.window_id,
            "query_text_sha256": "sha256:" + hashlib.sha256(self.text.encode("utf-8")).hexdigest(),
            "query_source_fragments": [fragment.facts() for fragment in self.fragments],
            "query_model_token_count": self.model_token_count,
        }
        if len(self.fragments) == 1:
            fragment = self.fragments[0]
            provenance.update(
                {
                    "query_source_field": fragment.source_field,
                    "query_source_start_char": fragment.source_start_char,
                    "query_source_end_char": fragment.source_end_char,
                }
            )
        return provenance


@dataclass
class SegmentChannels:
    """Every channel's ranking for one segment, in v2 conditioning index space."""

    segment_id: str
    allowed_facets: tuple[str, ...]
    unit_input: Mapping[str, Any]
    v1_ids: tuple[str, ...]
    rankings: dict[str, tuple[int, ...]]
    score_values: dict[str, dict[str, float]] = field(default_factory=dict)
    score_kinds: dict[str, str] = field(default_factory=dict)
    score_provenance: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)


def _evidence_field_ordinal(field_key: str) -> int:
    """Recover source-slice order after canonical JSON sorted the field keys."""

    match = _EVIDENCE_FIELD.fullmatch(str(field_key))
    if match is None:
        raise AblationError(f"dense evidence windows require evidence_N field keys, got {field_key!r}")
    return int(match.group(1))


def _preferred_window_end(text: str, start: int, maximum_end: int) -> int:
    """Back up to a nearby sentence or whitespace boundary without losing text."""

    if maximum_end >= len(text):
        return len(text)
    local = text[start:maximum_end]
    floor = max(0, len(local) - 200)
    tail = local[floor:]
    sentence_matches = list(re.finditer(r"(?:[.!?][\"')\]]*|\n)\s+", tail))
    if sentence_matches:
        return start + floor + sentence_matches[-1].end()
    whitespace_matches = list(re.finditer(r"\s+", tail))
    if whitespace_matches:
        return start + floor + whitespace_matches[-1].end()
    return maximum_end


def _fitted_window_end(
    text: str,
    *,
    start: int,
    token_counter: Callable[[str], int | None],
    max_input_tokens: int,
) -> tuple[int, int]:
    """Find a non-empty exact prefix that the model will not truncate."""

    remaining_count = token_counter(text[start:])
    if type(remaining_count) is not int:
        raise AblationError("dense evidence windows require exact model-native token counts")
    if remaining_count <= max_input_tokens:
        return len(text), remaining_count

    low = start + 1
    high = len(text)
    best_end: int | None = None
    best_count: int | None = None
    while low <= high:
        midpoint = (low + high) // 2
        count = token_counter(text[start:midpoint])
        if type(count) is not int:
            raise AblationError("dense evidence windows require exact model-native token counts")
        if count <= max_input_tokens:
            best_end = midpoint
            best_count = count
            low = midpoint + 1
        else:
            high = midpoint - 1
    if best_end is None or best_count is None:
        raise AblationError("dense model limit cannot fit one source character")

    preferred_end = _preferred_window_end(text, start, best_end)
    preferred_count = token_counter(text[start:preferred_end])
    if type(preferred_count) is not int:
        raise AblationError("dense evidence windows require exact model-native token counts")
    if preferred_end <= start or preferred_count > max_input_tokens:
        preferred_end, preferred_count = best_end, best_count
    return preferred_end, preferred_count


def dense_evidence_windows(
    unit: ExtractionUnit,
    *,
    token_counter: Callable[[str], int | None] | None,
    max_input_tokens: int | None,
) -> tuple[DenseEvidenceWindow, ...]:
    """Cover every evidence field with exact, ordered, zero-overlap model inputs."""

    if token_counter is None or type(max_input_tokens) is not int or max_input_tokens <= 0:
        raise AblationError("dense evidence windows require a model-native counter and positive token ceiling")
    raw_fields = unit.input.get("untrusted_evidence_fields", {}).get("fields")
    raw_spans = unit.input.get("processing_segment", {}).get("source_spans")
    if not isinstance(raw_fields, Mapping) or not isinstance(raw_spans, Mapping):
        raise AblationError("dense evidence windows require evidence fields and matching source spans")
    if set(raw_fields) != set(raw_spans):
        raise AblationError("dense evidence fields and source spans must name the same keys")

    ordered_keys = sorted((str(key) for key in raw_fields), key=_evidence_field_ordinal)
    windows: list[DenseEvidenceWindow] = []
    for field_key in ordered_keys:
        field_ordinal = _evidence_field_ordinal(field_key)
        text = raw_fields[field_key]
        span = raw_spans[field_key]
        if not isinstance(text, str) or not isinstance(span, Mapping):
            raise AblationError(f"dense evidence field {field_key!r} lacks exact text or source coordinates")
        source_field = str(span.get("source_field") or "")
        source_start = span.get("start_char")
        source_end = span.get("end_char")
        if (
            not source_field
            or type(source_start) is not int
            or type(source_end) is not int
            or source_start < 0
            or source_end < source_start
            or source_end - source_start != len(text)
        ):
            raise AblationError(f"dense evidence field {field_key!r} has inconsistent source coordinates")
        start = 0
        window_ordinal = 0
        while start < len(text):
            end, token_count = _fitted_window_end(
                text,
                start=start,
                token_counter=token_counter,
                max_input_tokens=max_input_tokens,
            )
            window_text = text[start:end]
            window_id = stable_id(
                "dense_query_window",
                DENSE_EVIDENCE_WINDOW_VERSION,
                unit.unit_id,
                field_key,
                start,
                end,
                window_text,
            )
            windows.append(
                DenseEvidenceWindow(
                    window_id=window_id,
                    segment_id=str(unit.unit_id),
                    field_key=field_key,
                    field_ordinal=field_ordinal,
                    window_ordinal=window_ordinal,
                    text=window_text,
                    field_start_char=start,
                    field_end_char=end,
                    source_field=source_field,
                    source_start_char=source_start + start,
                    source_end_char=source_start + end,
                    model_token_count=token_count,
                )
            )
            start = end
            window_ordinal += 1
    return tuple(windows)


@dataclass(frozen=True)
class _PackedEvidenceField:
    field_key: str
    text: str
    stream_start_char: int
    stream_end_char: int
    source_field: str
    source_start_char: int


def _packed_source_fragments(
    field_ranges: Sequence[_PackedEvidenceField],
    *,
    start: int,
    end: int,
) -> tuple[PackedEvidenceFragment, ...]:
    """Map one packed-query range back to every exact source substring."""

    fragments: list[PackedEvidenceFragment] = []
    for field_range in field_ranges:
        overlap_start = max(start, field_range.stream_start_char)
        overlap_end = min(end, field_range.stream_end_char)
        if overlap_start >= overlap_end:
            continue
        field_offset_start = overlap_start - field_range.stream_start_char
        field_offset_end = overlap_end - field_range.stream_start_char
        fragments.append(
            PackedEvidenceFragment(
                field_key=field_range.field_key,
                source_field=field_range.source_field,
                source_start_char=field_range.source_start_char + field_offset_start,
                source_end_char=field_range.source_start_char + field_offset_end,
                query_start_char=overlap_start - start,
                query_end_char=overlap_end - start,
            )
        )
    return tuple(fragments)


def packed_dense_evidence_windows(
    unit: ExtractionUnit,
    *,
    token_counter: Callable[[str], int | None] | None,
    max_input_tokens: int | None,
) -> tuple[PackedDenseEvidenceWindow, ...]:
    """Pack all ordered evidence into complete model-sized multi-fragment queries."""

    if token_counter is None or type(max_input_tokens) is not int or max_input_tokens <= 0:
        raise AblationError("packed dense evidence requires a model-native counter and positive token ceiling")
    raw_fields = unit.input.get("untrusted_evidence_fields", {}).get("fields")
    raw_spans = unit.input.get("processing_segment", {}).get("source_spans")
    if not isinstance(raw_fields, Mapping) or not isinstance(raw_spans, Mapping):
        raise AblationError("packed dense evidence requires evidence fields and matching source spans")
    if set(raw_fields) != set(raw_spans):
        raise AblationError("packed dense evidence fields and source spans must name the same keys")

    ordered_keys = sorted((str(key) for key in raw_fields), key=_evidence_field_ordinal)
    stream_parts: list[str] = []
    field_ranges: list[_PackedEvidenceField] = []
    cursor = 0
    included_field_count = 0
    for field_key in ordered_keys:
        text = raw_fields[field_key]
        span = raw_spans[field_key]
        if not isinstance(text, str) or not isinstance(span, Mapping):
            raise AblationError(f"packed dense evidence field {field_key!r} lacks exact text or coordinates")
        source_field = str(span.get("source_field") or "")
        source_start = span.get("start_char")
        source_end = span.get("end_char")
        if (
            not source_field
            or type(source_start) is not int
            or type(source_end) is not int
            or source_start < 0
            or source_end < source_start
            or source_end - source_start != len(text)
        ):
            raise AblationError(f"packed dense evidence field {field_key!r} has inconsistent source coordinates")
        # Empty evidence has valid zero-width source coordinates but contributes
        # neither semantic input nor a synthetic separator to a query.
        if not text:
            continue
        if included_field_count:
            stream_parts.append(DENSE_PACKED_EVIDENCE_SEPARATOR)
            cursor += len(DENSE_PACKED_EVIDENCE_SEPARATOR)
        stream_start = cursor
        stream_parts.append(text)
        cursor += len(text)
        field_ranges.append(
            _PackedEvidenceField(
                field_key=field_key,
                text=text,
                stream_start_char=stream_start,
                stream_end_char=cursor,
                source_field=source_field,
                source_start_char=source_start,
            )
        )
        included_field_count += 1
    stream = "".join(stream_parts)
    windows: list[PackedDenseEvidenceWindow] = []
    start = 0
    while start < len(stream):
        end, token_count = _fitted_window_end(
            stream,
            start=start,
            token_counter=token_counter,
            max_input_tokens=max_input_tokens,
        )
        fragments = _packed_source_fragments(field_ranges, start=start, end=end)
        if not fragments:
            # A preferred whitespace boundary can isolate the LF between two
            # evidence fields. Keep that LF with as much following source text
            # as the native model ceiling permits instead of emitting a
            # provenance-free query.
            next_source_start = next(
                (field_range.stream_start_char for field_range in field_ranges if field_range.stream_end_char > start),
                None,
            )
            if next_source_start is None:
                raise AblationError("packed dense evidence ended with a source-free separator")
            low = next_source_start + 1
            high = len(stream)
            fitted_end: int | None = None
            fitted_count: int | None = None
            while low <= high:
                midpoint = (low + high) // 2
                count = token_counter(stream[start:midpoint])
                if type(count) is not int:
                    raise AblationError("packed dense evidence requires exact model-native token counts")
                if count <= max_input_tokens:
                    fitted_end = midpoint
                    fitted_count = count
                    low = midpoint + 1
                else:
                    high = midpoint - 1
            if fitted_end is None or fitted_count is None:
                raise AblationError("dense model limit cannot fit a separator and one source character")
            end, token_count = fitted_end, fitted_count
            fragments = _packed_source_fragments(field_ranges, start=start, end=end)
            if not fragments:
                raise AblationError("packed dense evidence produced a query without a source fragment")
        window_text = stream[start:end]
        window_id = stable_id(
            "dense_packed_query",
            DENSE_PACKED_EVIDENCE_VERSION,
            unit.unit_id,
            start,
            end,
            window_text,
            canonical_json([fragment.facts() for fragment in fragments]),
        )
        windows.append(
            PackedDenseEvidenceWindow(
                window_id=window_id,
                segment_id=str(unit.unit_id),
                window_ordinal=len(windows),
                text=window_text,
                fragments=tuple(fragments),
                model_token_count=token_count,
            )
        )
        start = end
    return tuple(windows)


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


def _scored_ids_to_indices(
    ranked: Sequence[tuple[str, float]],
    index_by_id: Mapping[str, int],
) -> tuple[tuple[int, ...], dict[str, float]]:
    """Project a scored mapper result without separating scores from identities."""

    projected: list[int] = []
    scores: dict[str, float] = {}
    seen: set[int] = set()
    for raw_concept_id, raw_score in ranked:
        concept_id = str(raw_concept_id)
        index = index_by_id.get(concept_id)
        if index is None or index in seen:
            continue
        seen.add(index)
        projected.append(index)
        scores[concept_id] = float(raw_score)
    return tuple(projected), scores


def _mapper_scored_ranking(
    text: str,
    *,
    mapper: ConceptMapper,
    depth: int,
) -> list[tuple[str, float]]:
    """Return the mapper's native score instead of its identifier-only wrapper."""

    if not str(text or "").strip() or depth <= 0:
        return []
    return [(str(concept_id), float(score)) for concept_id, score in mapper.rank([text], depth=depth)[0]]


def _window_scored_ranking(
    windows: Sequence[DenseEvidenceWindow | PackedDenseEvidenceWindow],
    *,
    mapper: ConceptMapper,
    depth: int,
) -> tuple[list[tuple[str, float]], dict[str, dict[str, Any]]]:
    """Max-pool native dense scores while retaining each winner's exact query."""

    usable = [window for window in windows if window.text.strip()]
    if not usable or depth <= 0:
        return [], {}
    ranked_by_window = mapper.rank([window.text for window in usable], depth=depth)
    if len(ranked_by_window) != len(usable):
        raise AblationError("dense mapper returned a different number of window rankings than queries")
    best: dict[str, tuple[float, DenseEvidenceWindow]] = {}
    for window, ranked in zip(usable, ranked_by_window, strict=True):
        for raw_concept_id, raw_score in ranked:
            concept_id = str(raw_concept_id)
            score = float(raw_score)
            previous = best.get(concept_id)
            if previous is None or (-score, window.window_id) < (-previous[0], previous[1].window_id):
                best[concept_id] = (score, window)
    ordered = sorted(best.items(), key=lambda item: (-item[1][0], item[0]))[:depth]
    return (
        [(concept_id, score_and_window[0]) for concept_id, score_and_window in ordered],
        {concept_id: score_and_window[1].score_provenance() for concept_id, score_and_window in ordered},
    )


def _keyword_scored_ranking(
    keywords: Sequence[str],
    *,
    mapper: ConceptMapper,
    depth: int,
) -> list[tuple[str, float]]:
    """Match channel D while preserving each concept's winning mapper score."""

    cleaned = [keyword for keyword in (str(value or "").strip() for value in keywords) if keyword]
    if not cleaned or depth <= 0:
        return []
    best: dict[str, tuple[float, int]] = {}
    for ranked in mapper.rank(cleaned, depth=depth):
        for raw_concept_id, raw_score in ranked:
            concept_id = str(raw_concept_id)
            score = float(raw_score)
            prior = best.get(concept_id)
            best[concept_id] = (score, 1) if prior is None else (max(prior[0], score), prior[1] + 1)
    ordered = sorted(
        best.items(),
        key=lambda item: (-item[1][0], -item[1][1], item[0]),
    )
    return [(concept_id, score_and_count[0]) for concept_id, score_and_count in ordered[:depth]]


def segment_channels(
    *,
    unit: ExtractionUnit,
    registry_rows: Sequence[Mapping[str, Any]],
    conditioning: Any,
    index_by_id: Mapping[str, int],
    wanted: Sequence[str],
    dense_mapper: ConceptMapper | None,
    dense_windows: Sequence[DenseEvidenceWindow] | None = None,
    packed_dense_windows: Sequence[PackedDenseEvidenceWindow] | None = None,
    bm25_mapper: ConceptMapper | None,
    keywords: Sequence[str],
    limit: int,
    depth: int = ANCHOR_CHANNEL_DEPTH,
    include_v1: bool = True,
) -> SegmentChannels:
    """Compute one segment's rankings for every requested channel."""
    text = _segment_text(unit)
    requested = set(wanted)
    # An empty segment reaches every channel as an empty ranking, never as a
    # missing key: a configuration must still be computable over it.
    rankings: dict[str, tuple[int, ...]] = {name: () for name in requested}
    score_values: dict[str, dict[str, float]] = {name: {} for name in requested}
    score_kinds: dict[str, str] = {name: "rankOnly" for name in requested}
    score_provenance: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in requested}
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
        rankings[CHANNEL_C], score_values[CHANNEL_C] = _scored_ids_to_indices(
            _mapper_scored_ranking(text, mapper=dense_mapper, depth=depth),
            index_by_id,
        )
        score_kinds[CHANNEL_C] = "nativeMapperScore"
    if CHANNEL_CW in wanted:
        if dense_mapper is None:
            raise AblationError("channel Cw was requested without a dense concept mapper")
        if dense_windows is None:
            embedder = getattr(dense_mapper, "embedder", None)
            dense_windows = dense_evidence_windows(
                unit,
                token_counter=getattr(embedder, "model_token_count", None),
                max_input_tokens=getattr(embedder, "max_input_tokens", None),
            )
        scored, score_provenance[CHANNEL_CW] = _window_scored_ranking(
            dense_windows,
            mapper=dense_mapper,
            depth=depth,
        )
        rankings[CHANNEL_CW], score_values[CHANNEL_CW] = _scored_ids_to_indices(
            scored,
            index_by_id,
        )
        score_kinds[CHANNEL_CW] = "maxWindowNativeMapperScore"
    if CHANNEL_CP in wanted:
        if dense_mapper is None:
            raise AblationError("channel Cp was requested without a dense concept mapper")
        if packed_dense_windows is None:
            embedder = getattr(dense_mapper, "embedder", None)
            packed_dense_windows = packed_dense_evidence_windows(
                unit,
                token_counter=getattr(embedder, "model_token_count", None),
                max_input_tokens=getattr(embedder, "max_input_tokens", None),
            )
        scored, score_provenance[CHANNEL_CP] = _window_scored_ranking(
            packed_dense_windows,
            mapper=dense_mapper,
            depth=depth,
        )
        rankings[CHANNEL_CP], score_values[CHANNEL_CP] = _scored_ids_to_indices(
            scored,
            index_by_id,
        )
        score_kinds[CHANNEL_CP] = "maxPackedWindowNativeMapperScore"
    if CHANNEL_D in wanted:
        if dense_mapper is None:
            raise AblationError("channel D was requested without a concept mapper")
        rankings[CHANNEL_D], score_values[CHANNEL_D] = _scored_ids_to_indices(
            _keyword_scored_ranking(
                keywords,
                mapper=dense_mapper,
                depth=depth,
            ),
            index_by_id,
        )
        score_kinds[CHANNEL_D] = "bestKeywordNativeMapperScore"
    if CHANNEL_E in wanted:
        if bm25_mapper is None:
            raise AblationError("channel E was requested without a BM25 mapper")
        rankings[CHANNEL_E], score_values[CHANNEL_E] = _scored_ids_to_indices(
            _mapper_scored_ranking(text, mapper=bm25_mapper, depth=depth),
            index_by_id,
        )
        score_kinds[CHANNEL_E] = "nativeMapperScore"
    v1_selected = (
        select_candidate_concepts_for_text(text, _allowed_schemes(unit), registry_rows, limit=limit)
        if include_v1
        else []
    )
    return SegmentChannels(
        segment_id=str(unit.unit_id),
        allowed_facets=tuple(_allowed_schemes(unit)),
        unit_input=unit.input,
        v1_ids=tuple(str(concept.get("concept_id") or "") for concept in v1_selected),
        rankings=rankings,
        score_values=score_values,
        score_kinds=score_kinds,
        score_provenance=score_provenance,
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
    ranked = _allowed_facet_ranking(ranked, conditioning, segment.allowed_facets)
    selected = (
        _apply_source_vocabulary_quotas(ranked, conditioning, limit=limit) if configuration.quotas else ranked[:limit]
    )
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


def configuration_prompt_preflight(
    configuration: Configuration,
    *,
    channels_by_segment: Mapping[str, SegmentChannels],
    conditioning: Any,
    concepts_by_id: Mapping[str, Mapping[str, Any]],
    limit: int,
) -> dict[str, Any]:
    """Validate each experimental shortlist through the real tag prompt path."""
    facts, _ = fit_configuration_prompts(
        configuration,
        channels_by_segment=channels_by_segment,
        conditioning=conditioning,
        concepts_by_id=concepts_by_id,
        limit=limit,
    )
    return facts


def fit_configuration_prompts(
    configuration: Configuration,
    *,
    channels_by_segment: Mapping[str, SegmentChannels],
    conditioning: Any,
    concepts_by_id: Mapping[str, Mapping[str, Any]],
    limit: int,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Fit every shortlist through the validated production prompt boundary."""
    task = TagExtractionTask()
    counter = TiktokenCounter()
    totals: list[tuple[int, str]] = []
    candidate_counts: list[tuple[int, int, str]] = []
    fitted_by_segment: dict[str, list[str]] = {}
    for segment_id, segment in channels_by_segment.items():
        selected, _ = configuration_ranking(
            configuration,
            segment,
            conditioning,
            limit=limit,
        )
        ranked_count = len(selected)
        while True:
            unit_input = {
                **segment.unit_input,
                "available_concepts": [
                    ontology_concept_payload(dict(concepts_by_id[concept_id])) for concept_id in selected
                ],
            }
            payload = task.build_payload(unit_input)
            total = (
                counter.count(task.instructions + "\n" + canonical_json(payload))
                + counter.count(canonical_json(task.build_schema(payload)))
                + PROMPT_SAFETY_MARGIN_TOKENS
            )
            if total <= PROMPT_INPUT_TOKEN_BUDGET:
                totals.append((total, segment_id))
                candidate_counts.append((ranked_count, len(selected), segment_id))
                fitted_by_segment[segment_id] = list(selected)
                break
            if not selected:
                raise AblationError(
                    f"{configuration.name} prompt {segment_id} needs {total} tokens "
                    f"with no candidates, over the {PROMPT_INPUT_TOKEN_BUDGET}-token budget"
                )
            selected.pop()
    return (
        {
            "payload_validation": "pass",
            "prompt_input_token_budget": PROMPT_INPUT_TOKEN_BUDGET,
            "prompt_input_token_max": max((total for total, _ in totals), default=0),
            "prompt_budget_trimmed_segment_count": sum(ranked != fitted for ranked, fitted, _ in candidate_counts),
            "prompt_candidate_count_min": min(
                (fitted for _, fitted, _ in candidate_counts),
                default=0,
            ),
            "passed": True,
        },
        fitted_by_segment,
    )


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
    source_vocabulary_by_id: Mapping[str, str],
    candidate_lineage_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    limit: int,
    fitted_by_segment: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Score one configuration over every gold item."""
    started = time.monotonic()
    managed_target_mode = any(item.registered_targets or item.not_represented for item in items)
    cache: dict[str, tuple[list[str], list[str]]] = {}
    prompt_trimmed_by_segment: dict[str, bool] = {}
    per_item: list[dict[str, Any]] = []
    source_vocabulary_mix: Counter[str] = Counter()
    for item in items:
        per_segment: list[list[str]] = []
        full: list[list[str]] = []
        channel_observations: dict[str, dict[str, dict[str, Any]]] = {}
        for segment_id in item.segment_ids:
            segment = channels_by_segment[segment_id]
            if segment_id not in cache:
                selected, ranking = configuration_ranking(configuration, segment, conditioning, limit=limit)
                if fitted_by_segment is not None:
                    fitted = list(fitted_by_segment[segment_id])
                    prompt_trimmed_by_segment[segment_id] = len(fitted) < len(selected)
                    selected = fitted
                else:
                    prompt_trimmed_by_segment[segment_id] = False
                cache[segment_id] = (selected, ranking)
            selected, ranking = cache[segment_id]
            per_segment.append(selected)
            full.append(ranking)
            if configuration.name == "v1":
                per_channel_ids = {"v1": segment.v1_ids}
            else:
                per_channel_ids = {
                    channel: tuple(conditioning.concept_ids[index] for index in segment.rankings.get(channel, ()))
                    for channel in configuration.channels
                }
            for channel, concept_ids in per_channel_ids.items():
                for rank, concept_id in enumerate(concept_ids, start=1):
                    score = segment.score_values.get(channel, {}).get(concept_id)
                    observation = {
                        "rank": rank,
                        "score": score,
                        "score_kind": segment.score_kinds.get(
                            channel,
                            "rankOnly",
                        ),
                        "segment_id": segment_id,
                        **segment.score_provenance.get(channel, {}).get(concept_id, {}),
                    }
                    member_observations = channel_observations.setdefault(
                        concept_id,
                        {},
                    )
                    previous = member_observations.get(channel)
                    score_order = -float(score) if score is not None else float("inf")
                    observation_order = (rank, score_order, segment_id)
                    if previous is None:
                        member_observations[channel] = observation
                    else:
                        previous_score = previous.get("score")
                        previous_order = (
                            int(previous["rank"]),
                            (-float(previous_score) if previous_score is not None else float("inf")),
                            str(previous["segment_id"]),
                        )
                        if observation_order < previous_order:
                            member_observations[channel] = observation
        candidates = merge_across_segments(per_segment, limit=limit)
        source_vocabulary_mix.update(source_vocabulary_by_id.get(concept_id, "") for concept_id in candidates)
        fused_ranks = [
            rank for rank in (_first_rank(ranking, item.exact_alias_ids) for ranking in full) if rank is not None
        ]
        represented_target_ids = tuple(target.concept_id for target in item.registered_targets)
        represented_target_ranks = {
            target.concept_id: _first_rank(candidates, [target.concept_id]) for target in item.registered_targets
        }
        adequate_target_ids = item.adequate_target_ids
        managed_target_rank_values = [rank for rank in represented_target_ranks.values() if rank is not None]
        adequate_rank_values = [
            rank for concept_id in adequate_target_ids if (rank := _first_rank(candidates, [concept_id])) is not None
        ]
        merged_before_limit = merge_across_segments(
            per_segment,
            limit=sum(len(values) for values in per_segment),
        )
        prompt_truncated = any(prompt_trimmed_by_segment.get(segment_id, False) for segment_id in item.segment_ids)
        cross_segment_truncated = len(merged_before_limit) > limit
        if prompt_truncated or cross_segment_truncated:
            selection_truncated: bool | None = True
        elif configuration.name == "v1" and any(len(values) >= limit for values in per_segment):
            # v1 exposes only its capped list, so whether another candidate
            # existed below the cap is unknowable from this interface.
            selection_truncated = None
        else:
            selection_truncated = any(
                len(ranking) > len(selected)
                for ranking, selected in zip(
                    full,
                    per_segment,
                    strict=True,
                )
            )
        candidate_lineage = []
        for candidate_rank, concept_id in enumerate(candidates, start=1):
            if candidate_lineage_by_id is None or concept_id not in candidate_lineage_by_id:
                continue
            observations = channel_observations.get(concept_id, {})
            candidate_lineage.append(
                {
                    **dict(candidate_lineage_by_id[concept_id]),
                    "candidate_rank": candidate_rank,
                    "channel_ranks": {
                        channel: int(observation["rank"]) for channel, observation in sorted(observations.items())
                    },
                    "channel_score_facts": {
                        channel: dict(observation) for channel, observation in sorted(observations.items())
                    },
                    "limit": limit,
                    "truncated": selection_truncated,
                }
            )
        per_item.append(
            {
                "item_id": item.item_id,
                "label": item.label,
                "candidates": candidates,
                "candidate_lineage": candidate_lineage,
                "exact_alias_target": bool(item.exact_alias_ids),
                "exact_alias_rank": _first_rank(candidates, item.exact_alias_ids),
                "exact_alias_fused_rank": min(fused_ranks) if fused_ranks else None,
                "managed_targets": [
                    {
                        "concept_id": target.concept_id,
                        "pref_label": target.pref_label,
                        "grade": target.grade,
                        "adequate_for_development": (target.adequate_for_development),
                        "rank": represented_target_ranks[target.concept_id],
                    }
                    for target in item.registered_targets
                ],
                "represented_target": bool(represented_target_ids),
                "represented_target_rank": (min(managed_target_rank_values) if managed_target_rank_values else None),
                "not_represented": item.not_represented,
                "adequate_target": (list(adequate_target_ids) if adequate_target_ids else None),
                "adequate_rank": (min(adequate_rank_values) if adequate_rank_values else None),
            }
        )

    targets = [row for row in per_item if row["exact_alias_target"]]
    surfaced = [row for row in targets if row["exact_alias_rank"] is not None]
    represented_items = [row for row in per_item if row["represented_target"]]
    represented_items_surfaced = [row for row in represented_items if row["represented_target_rank"] is not None]
    managed_targets = [target for row in per_item for target in row["managed_targets"]]
    managed_targets_surfaced = [target for target in managed_targets if target["rank"] is not None]
    adequate = [row for row in per_item if row["adequate_target"]]
    adequate_kept = [row for row in adequate if row["adequate_rank"] is not None]
    managed_ranks = [int(row["represented_target_rank"]) for row in represented_items_surfaced]
    exact_alias_ranks = [int(row["exact_alias_rank"]) for row in surfaced]
    ranks = managed_ranks if managed_target_mode else exact_alias_ranks
    target_grade_metrics: dict[str, dict[str, Any]] = {}
    for grade in sorted(TARGET_GRADES - {"notRepresented"}):
        graded = [target for target in managed_targets if target["grade"] == grade]
        if not graded:
            continue
        graded_surfaced = [target for target in graded if target["rank"] is not None]
        target_grade_metrics[grade] = {
            "target_count": len(graded),
            "surfaced": len(graded_surfaced),
            "recall_at_limit": round(
                len(graded_surfaced) / len(graded),
                6,
            ),
        }
    total = sum(source_vocabulary_mix.values())
    result = {
        "configuration": configuration.name,
        "channels": list(configuration.channels) or ["v1"],
        "quotas": configuration.quotas,
        "note": configuration.note,
        "candidate_limit": limit,
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
        "source_vocabulary_mix": dict(sorted(source_vocabulary_mix.items(), key=lambda entry: (-entry[1], entry[0]))),
        "seconds": round(time.monotonic() - started, 3),
        "items": per_item,
    }
    if managed_target_mode:
        result.update(
            {
                "represented_item_count": len(represented_items),
                "represented_item_surfaced": len(represented_items_surfaced),
                "represented_item_surfaced_labels": sorted(str(row["label"]) for row in represented_items_surfaced),
                "represented_item_missed_labels": sorted(
                    str(row["label"]) for row in represented_items if row["represented_target_rank"] is None
                ),
                "managed_target_count": len(managed_targets),
                "managed_target_surfaced": len(managed_targets_surfaced),
                "not_represented_item_count": sum(bool(row["not_represented"]) for row in per_item),
                "target_grade_metrics": target_grade_metrics,
            }
        )
    return result


def flat_candidate_lineage_rows(
    results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Emit one artifact-writer-compatible row per candidate and channel."""

    rows: list[dict[str, Any]] = []
    for result in results:
        configuration = str(result.get("configuration") or "")
        evaluation_scope = str(result.get("evaluation_scope") or "development_only")
        for item in result.get("items", ()):
            if not isinstance(item, Mapping):
                continue
            for lineage in item.get("candidate_lineage", ()):
                if not isinstance(lineage, Mapping):
                    continue
                channel_ranks = lineage.get("channel_ranks")
                channel_identities = lineage.get("channel_identities")
                indexed_by_channel = lineage.get("indexed_expression_ids_by_channel")
                score_facts = lineage.get("channel_score_facts")
                if not all(
                    isinstance(value, Mapping)
                    for value in (
                        channel_ranks,
                        channel_identities,
                        indexed_by_channel,
                        score_facts,
                    )
                ):
                    raise AblationError("managed candidate lineage lacks channel-specific index facts")
                for channel_code, rank in sorted(channel_ranks.items()):
                    indexed = indexed_by_channel.get(channel_code)
                    channel = channel_identities.get(channel_code)
                    score_fact = score_facts.get(channel_code)
                    if not isinstance(indexed, list) or not indexed or not isinstance(channel, str) or not channel:
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has no exact "
                            f"indexed expressions for channel {channel_code!r}"
                        )
                    if not isinstance(score_fact, Mapping):
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has no score fact for channel {channel_code!r}"
                        )
                    if score_fact.get("rank") != rank:
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has inconsistent "
                            f"rank and score facts for channel {channel_code!r}"
                        )
                    score_kind = score_fact.get("score_kind")
                    score_segment = score_fact.get("segment_id")
                    score = score_fact.get("score")
                    if not isinstance(score_kind, str) or not score_kind:
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has no score kind for channel {channel_code!r}"
                        )
                    if not isinstance(score_segment, str) or not score_segment:
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has no score source segment for channel {channel_code!r}"
                        )
                    if score is not None and type(score) not in {int, float}:
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} has a non-numeric score for channel {channel_code!r}"
                        )
                    if score is None and score_kind != "rankOnly":
                        raise AblationError(
                            f"{lineage.get('member_iri')!r} lost the native score for channel {channel_code!r}"
                        )
                    query_row: dict[str, Any] = {}
                    if score_fact.get("query_representation") is not None:
                        query_row = {
                            "queryRepresentation": str(score_fact["query_representation"]),
                            "queryWindowId": str(score_fact["query_window_id"]),
                            "queryTextSha256": str(score_fact["query_text_sha256"]),
                            "queryModelTokenCount": int(score_fact["query_model_token_count"]),
                        }
                        fragments = score_fact.get("query_source_fragments")
                        if fragments is not None:
                            if (
                                not isinstance(fragments, list)
                                or not fragments
                                or not all(isinstance(fragment, Mapping) for fragment in fragments)
                            ):
                                raise AblationError("packed dense score provenance requires source fragments")
                            query_row["querySourceFragments"] = [dict(fragment) for fragment in fragments]
                        scalar_source_fields = (
                            "query_source_field",
                            "query_source_start_char",
                            "query_source_end_char",
                        )
                        if all(field in score_fact for field in scalar_source_fields):
                            query_row.update(
                                {
                                    "querySourceField": str(score_fact["query_source_field"]),
                                    "querySourceStartChar": int(score_fact["query_source_start_char"]),
                                    "querySourceEndChar": int(score_fact["query_source_end_char"]),
                                }
                            )
                        elif fragments is None:
                            raise AblationError("dense score provenance requires a source span or fragment list")
                    rows.append(
                        {
                            "itemId": str(item.get("item_id") or ""),
                            "configuration": configuration,
                            "conceptId": str(lineage.get("member_iri") or ""),
                            "channel": channel,
                            "channelCode": str(channel_code),
                            "rank": int(rank),
                            "candidateRank": int(lineage.get("candidate_rank") or 0),
                            "score": float(score) if score is not None else None,
                            "scoreKind": score_kind,
                            "scoreSourceSegment": score_segment,
                            "limit": int(lineage.get("limit") or 0),
                            "truncated": lineage.get("truncated"),
                            "facet": str(lineage.get("facet") or ""),
                            "scheme": str(lineage.get("scheme_iri") or ""),
                            "referenceResourceRelease": lineage.get("reference_resource_release"),
                            "registryImportSnapshot": lineage.get("registry_import_snapshot"),
                            "expressionCorpusSnapshot": lineage.get("expression_corpus_snapshot"),
                            "lookupIndexManifest": lineage.get("lookup_index_manifest"),
                            "indexedExpressionIds": indexed,
                            "availableExpressionIds": lineage.get("available_expression_ids", []),
                            "managedReleaseManifest": lineage.get("managed_release_manifest"),
                            "managedReleaseManifestDigest": lineage.get("managed_release_manifest_digest"),
                            "usageCeiling": str(lineage.get("usage_ceiling") or ""),
                            "evaluationScope": evaluation_scope,
                            "goldLabel": str(item.get("label") or ""),
                            **query_row,
                        }
                    )
    return rows


def _source_vocabulary_share(source_vocabulary_mix: Mapping[str, int], slots: int) -> str:
    if not slots:
        return "—"
    parts = [
        f"{vocabulary or '∅'} {count * 100 // slots}%" for vocabulary, count in list(source_vocabulary_mix.items())[:4]
    ]
    return ", ".join(parts)


def markdown_table(results: Sequence[Mapping[str, Any]]) -> str:
    """Render the ablation as one markdown table plus the per-target detail."""
    managed = any("represented_item_count" in result for result in results)
    target_heading = "Managed items surfaced" if managed else "Exact-alias surfaced"
    header = (
        f"| Configuration | Channels | Quotas | {target_heading} | Adequate kept | "
        "Mean rank | Median rank | Source vocabulary mix (top-12 slots) |\n"
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
                surfaced=(result["represented_item_surfaced"] if managed else result["exact_alias_surfaced"]),
                targets=(result["represented_item_count"] if managed else result["exact_alias_target_count"]),
                kept=result["adequate_kept"],
                adequate=result["adequate_target_count"],
                mean=result["surfaced_rank_mean"] if result["surfaced_rank_mean"] is not None else "—",
                median=result["surfaced_rank_median"] if result["surfaced_rank_median"] is not None else "—",
                mix=_source_vocabulary_share(result["source_vocabulary_mix"], int(result["candidate_slots"])),
            )
        )
    lines.append("")
    lines.append(
        "| Configuration | "
        + ("Managed target items surfaced" if managed else "Exact-alias targets surfaced")
        + " | Missed |"
    )
    lines.append("| --- | --- | --- |")
    for result in results:
        lines.append(
            "| {name} | {surfaced} | {missed} |".format(
                name=result["configuration"],
                surfaced=", ".join(
                    result["represented_item_surfaced_labels" if managed else "exact_alias_surfaced_labels"]
                )
                or "—",
                missed=", ".join(result["represented_item_missed_labels" if managed else "exact_alias_missed_labels"])
                or "—",
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


def _keyword_content_facts(
    keywords_by_segment: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    """Pin and retain the exact normalized keywords that channel D consumes."""

    payload = {
        str(segment_id): [str(keyword) for keyword in keywords]
        for segment_id, keywords in sorted(keywords_by_segment.items())
    }
    digest = "sha256:" + hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return {
        "keyword_content_digest": digest,
        "keyword_segment_count": len(payload),
        "keywords_by_segment": payload,
    }


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


def run_ablation(
    *,
    dataset_dir: Path,
    selection_file: Path,
    index_dir: Path,
    output_dir: Path,
    configuration_names: Sequence[str],
    targets_file: Path | None = None,
    resolved_file: Path | None = None,
    managed_release_manifest: Path | None = None,
    managed_release_manifest_digest: str | None = None,
    permission_facet_iri: str = DEFAULT_PERMISSION_FACET_IRI,
    permission_assignment_role_iri: str = (DEFAULT_PERMISSION_ASSIGNMENT_ROLE_IRI),
    permission_resource_route: str = DEFAULT_PERMISSION_RESOURCE_ROUTE,
    candidate_default_language: str = "en",
    registry_file: Path | None = None,
    allow_legacy_registry: bool = False,
    allow_legacy_targets: bool = False,
    limit: int = PROMPT_CONCEPT_LIMIT,
    generate: bool = False,
    keywords_file: Path | None = None,
    fallback_mapper: bool = False,
    require_adoption_verdict: bool = False,
) -> dict[str, Any]:
    """Load the frozen inputs, run every configuration, and write the record."""
    unknown = [name for name in configuration_names if name not in CONFIGURATIONS_BY_NAME]
    if unknown:
        raise AblationError(f"unknown configurations: {sorted(unknown)}")
    configurations = [CONFIGURATIONS_BY_NAME[name] for name in configuration_names]
    wanted = sorted({channel for configuration in configurations for channel in configuration.channels})

    timings: dict[str, float] = {}
    started = time.monotonic()
    candidate_registry = load_candidate_registry(
        output_dir=output_dir,
        managed_release_manifest=managed_release_manifest,
        managed_release_manifest_digest=managed_release_manifest_digest,
        permission_facet_iri=permission_facet_iri,
        permission_assignment_role_iri=(permission_assignment_role_iri),
        permission_resource_route=permission_resource_route,
        candidate_default_language=candidate_default_language,
        registry_file=registry_file,
        allow_legacy_registry=allow_legacy_registry,
    )
    registry_rows = list(candidate_registry.rows)
    timings["load_candidate_source"] = round(
        time.monotonic() - started,
        3,
    )
    started = time.monotonic()
    managed_mode = candidate_registry.managed_source is not None
    inputs = load_testbed_inputs(
        dataset_dir,
        selection_file,
        candidate_registry.selector_file,
        evaluation_manifest=(DEFAULT_MANAGED_BOUNDARY if managed_mode else DEFAULT_BOUNDARY_MANIFEST),
        evaluation_dataset_id=(MANAGED_DEVELOPMENT_DATASET_ID if managed_mode else DEVELOPMENT_DATASET_ID),
        require_adoption_verdict=require_adoption_verdict,
    )
    timings["load_inputs"] = round(time.monotonic() - started, 3)

    units_by_id = {unit.unit_id: unit for unit in inputs.units}
    items = gold_items(
        answers=inputs.answers,
        units_by_id=units_by_id,
        aliases=alias_index(registry_rows),
        adequate={},
    )
    managed_target_set: ManagedTargetSet | None = None
    bound_adequate: dict[str, str] = {}
    foreign_reviewed_targets: list[dict[str, str]] = []
    started = time.monotonic()
    if managed_mode:
        if allow_legacy_targets or resolved_file is not None:
            raise AblationError("managed experiments cannot read fused-registry target files")
        if targets_file is None:
            raise AblationError("managed experiments require a managed target dataset")
        managed_target_set = load_managed_targets(
            Path(targets_file),
            candidate_registry=candidate_registry,
            items=items,
            source_facts=inputs.source_facts,
            segmentation_facts=inputs.segmentation_facts,
        )
        items = attach_managed_targets(items, managed_target_set)
    else:
        if targets_file is not None:
            raise AblationError("managed targets cannot be used with the legacy fused registry")
        if not allow_legacy_targets:
            raise AblationError("legacy fused targets require --allow-legacy-targets")
        if resolved_file is None:
            raise AblationError("legacy target file is required")
        resolved = json.loads(Path(resolved_file).read_text())
        candidate_ids = {str(row.get("concept_id") or "") for row in registry_rows}
        bound_adequate, foreign_reviewed_targets = bind_reviewed_adequate_targets(
            resolved,
            candidate_ids=candidate_ids,
        )
        items = [
            replace(
                item,
                adequate_concept_id=bound_adequate.get(item.item_id),
            )
            for item in items
        ]
    timings["load_targets"] = round(time.monotonic() - started, 3)
    segment_ids = sorted({segment_id for item in items for segment_id in item.segment_ids})

    started = time.monotonic()
    conditioning = _condition_registry(registry_rows)
    timings["condition_registry"] = round(time.monotonic() - started, 3)
    index_by_id = {concept_id: index for index, concept_id in enumerate(conditioning.concept_ids)}
    source_vocabulary_by_id = {
        concept_id: conditioning.source_vocabularies[index] for index, concept_id in enumerate(conditioning.concept_ids)
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
    dense_windows_by_segment: dict[str, tuple[DenseEvidenceWindow, ...]] = {}
    packed_dense_windows_by_segment: dict[str, tuple[PackedDenseEvidenceWindow, ...]] = {}
    if {CHANNEL_C, CHANNEL_CW, CHANNEL_CP, CHANNEL_D} & set(wanted):
        started = time.monotonic()
        mapper, mapper_facts = _build_mapper(
            registry_rows, index_dir=index_dir, fallback_mapper=fallback_mapper, wanted=wanted
        )
        timings["concept_mapper"] = round(time.monotonic() - started, 3)
        query_representations: dict[str, Any] = {}
        if CHANNEL_C in wanted:
            whole_segment_facts = _query_token_facts(
                mapper,
                [_segment_text(units_by_id[segment_id]) for segment_id in segment_ids],
            )
            mapper_facts.update(whole_segment_facts)
            query_representations[CHANNEL_C] = {
                "version": DENSE_CHANNEL_VERSION,
                "input": "wholeSegmentEvidence",
                "source_order": "canonicalJsonMappingOrder",
                "model_truncation": "implicitWhenOverLimit",
                **whole_segment_facts,
            }
        if CHANNEL_CW in wanted:
            dense_windows_by_segment, window_facts = _dense_evidence_window_set(
                units_by_id=units_by_id,
                segment_ids=segment_ids,
                mapper=mapper,
            )
            query_representations[CHANNEL_CW] = window_facts
        if CHANNEL_CP in wanted:
            packed_dense_windows_by_segment, packed_window_facts = _packed_dense_evidence_window_set(
                units_by_id=units_by_id,
                segment_ids=segment_ids,
                mapper=mapper,
            )
            query_representations[CHANNEL_CP] = packed_window_facts
        if query_representations:
            mapper_facts["query_representations"] = query_representations

    bm25_mapper: ConceptMapper | None = None
    bm25_facts: dict[str, Any] = {}
    if CHANNEL_E in wanted:
        started = time.monotonic()
        built_bm25 = BM25ConceptMapper.build(registry_rows)
        bm25_mapper = built_bm25
        bm25_facts = built_bm25.facts()
        bm25_facts["seconds"] = round(time.monotonic() - started, 3)
        timings["bm25_index"] = bm25_facts["seconds"]

    candidate_source_facts = dict(candidate_registry.source_facts)
    candidate_lineage_by_id: Mapping[str, Mapping[str, Any]] = {}
    if candidate_registry.managed_source is not None:
        lookup_index_identity, lookup_index_manifest = derive_lookup_index_identity(
            candidate_registry=candidate_registry,
            configurations=configurations,
            mapper_facts=mapper_facts,
            bm25_facts=bm25_facts,
        )
        candidate_source_facts, candidate_lineage_by_id = finalize_candidate_lineage(
            candidate_registry=candidate_registry,
            lookup_index_identity=lookup_index_identity,
            lookup_index_manifest=lookup_index_manifest,
            mapper_facts=mapper_facts,
            channels=sorted(
                {
                    channel
                    for configuration in configurations
                    for channel in (configuration.channels if configuration.name != "v1" else ("v1",))
                }
            ),
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
            dense_windows=dense_windows_by_segment.get(segment_id),
            packed_dense_windows=packed_dense_windows_by_segment.get(segment_id),
            bm25_mapper=bm25_mapper,
            keywords=keywords_by_segment.get(segment_id, ()),
            limit=limit,
            include_v1=any(configuration.name == "v1" for configuration in configurations),
        )
        for segment_id in segment_ids
    }
    timings["channel_rankings"] = round(time.monotonic() - started, 3)
    concepts_by_id = {
        str(concept.get("concept_id") or ""): concept for concept in registry_rows if concept.get("concept_id")
    }
    prompt_preflight: dict[str, dict[str, Any]] = {}
    fitted_by_configuration: dict[str, dict[str, list[str]]] = {}
    for configuration in configurations:
        facts, fitted = fit_configuration_prompts(
            configuration,
            channels_by_segment=channels_by_segment,
            conditioning=conditioning,
            concepts_by_id=concepts_by_id,
            limit=limit,
        )
        prompt_preflight[configuration.name] = facts
        fitted_by_configuration[configuration.name] = fitted

    started = time.monotonic()
    results = []
    for configuration in configurations:
        measured = measure_configuration(
            configuration,
            items=items,
            channels_by_segment=channels_by_segment,
            conditioning=conditioning,
            source_vocabulary_by_id=source_vocabulary_by_id,
            candidate_lineage_by_id=candidate_lineage_by_id,
            limit=limit,
            fitted_by_segment=fitted_by_configuration[configuration.name],
        )
        measured["prompt_preflight"] = prompt_preflight[configuration.name]
        measured["evaluation_scope"] = "development_only"
        measured["accuracy_verdict_eligible"] = False
        measured["foreign_reviewed_target_count"] = len(foreign_reviewed_targets)
        results.append(measured)
    timings["measure"] = round(time.monotonic() - started, 3)

    input_facts = {
        "dataset_dir": str(dataset_dir),
        "selection_file": str(selection_file),
        "candidate_source": candidate_source_facts,
        "selector_registry_file": str(candidate_registry.selector_file),
        "selector_registry_sha256": sha256_file(candidate_registry.selector_file),
        "registry_row_count": len(registry_rows),
        "eligible_concept_count": len(conditioning.concept_ids),
        "gold_file": str(Path(dataset_dir) / GOLD_FILE),
        "gold_sha256": sha256_file(Path(dataset_dir) / GOLD_FILE),
        "prompt_input_token_budget": PROMPT_INPUT_TOKEN_BUDGET,
    }
    if managed_target_set is not None and targets_file is not None:
        input_facts.update(
            {
                "targets_file": str(targets_file),
                "targets_sha256": sha256_file(Path(targets_file)),
                "target_dataset_id": managed_target_set.dataset_id,
            }
        )
    elif resolved_file is not None:
        input_facts.update(
            {
                "resolved_file": str(resolved_file),
                "resolved_sha256": sha256_file(Path(resolved_file)),
            }
        )

    adequate_target_rows = [
        {
            "item_id": item.item_id,
            "label": item.label,
            "concept_ids": list(item.adequate_target_ids),
        }
        for item in items
        if item.adequate_target_ids
    ]
    managed_target_rows = [
        {
            "item_id": item.item_id,
            "label": item.label,
            "not_represented": item.not_represented,
            "targets": [
                {
                    "concept_id": target.concept_id,
                    "pref_label": target.pref_label,
                    "grade": target.grade,
                    "adequate_for_development": (target.adequate_for_development),
                }
                for target in item.registered_targets
            ],
        }
        for item in items
    ]
    exactly_bound_count = (
        sum(len(item.registered_targets) for item in items) if managed_target_set is not None else len(bound_adequate)
    )
    return {
        "schema_version": "candidate-selector-ablation-v4",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "code_identity": experiment_code_identity(),
        "inputs": input_facts,
        "settings": {
            "limit": limit,
            "channel_depth": CHANNEL_DEPTH,
            "rrf_k": ANCHOR_RRF_K,
            "dense_channel_version": DENSE_CHANNEL_VERSION,
            "dense_evidence_window_version": DENSE_EVIDENCE_WINDOW_VERSION,
            "dense_packed_evidence_version": DENSE_PACKED_EVIDENCE_VERSION,
            "keyword_channel_version": KEYWORD_CHANNEL_VERSION,
            "bm25_channel_version": BM25_CHANNEL_VERSION,
        },
        "item_count": len(items),
        "evaluation_boundary": inputs.evaluation_facts,
        "segment_count": len(segment_ids),
        "exact_alias_targets": [
            {"item_id": item.item_id, "label": item.label, "concept_ids": list(item.exact_alias_ids)}
            for item in items
            if item.exact_alias_ids
        ],
        "managed_targets": (managed_target_rows if managed_target_set is not None else []),
        "adequate_targets": adequate_target_rows,
        "target_dataset": (
            {
                "id": managed_target_set.dataset_id,
                "sourceEvidence": dict(managed_target_set.source_facts),
                "vocabularyUniverse": dict(managed_target_set.vocabulary_facts),
                "review": dict(managed_target_set.review_facts),
            }
            if managed_target_set is not None
            else None
        ),
        "reviewed_target_binding": {
            "exactly_bound_count": exactly_bound_count,
            "represented_item_count": sum(bool(item.registered_targets) for item in items),
            "not_represented_count": sum(item.not_represented for item in items),
            "foreign_or_unbound_count": len(foreign_reviewed_targets),
            "foreign_or_unbound": foreign_reviewed_targets,
            "label_rebinding_performed": False,
        },
        "concept_mapper": mapper_facts,
        "bm25": bm25_facts,
        "keywords": {**keyword_facts, "calls": keyword_calls},
        "timings_seconds": timings,
        "results": results,
        "candidate_rows": flat_candidate_lineage_rows(results),
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


def _dense_evidence_window_set(
    *,
    units_by_id: Mapping[str, ExtractionUnit],
    segment_ids: Sequence[str],
    mapper: Any,
) -> tuple[dict[str, tuple[DenseEvidenceWindow, ...]], dict[str, Any]]:
    """Build and identify the exact no-truncation queries used by channel Cw."""

    embedder = getattr(mapper, "embedder", None)
    counter = getattr(embedder, "model_token_count", None)
    ceiling = getattr(embedder, "max_input_tokens", None)
    if counter is None or type(ceiling) is not int or ceiling <= 0:
        raise AblationError("channel Cw requires the dense model's exact token counter and input ceiling")

    by_segment: dict[str, tuple[DenseEvidenceWindow, ...]] = {}
    records: list[dict[str, Any]] = []
    total_fields = 0
    split_fields = 0
    source_characters = 0
    covered_characters = 0
    for segment_id in segment_ids:
        unit = units_by_id[segment_id]
        windows = dense_evidence_windows(
            unit,
            token_counter=counter,
            max_input_tokens=ceiling,
        )
        by_segment[segment_id] = windows
        raw_fields = unit.input["untrusted_evidence_fields"]["fields"]
        total_fields += len(raw_fields)
        source_characters += sum(len(str(value)) for value in raw_fields.values())
        field_window_counts = Counter(window.field_key for window in windows)
        split_fields += sum(1 for count in field_window_counts.values() if count > 1)
        covered_characters += sum(len(window.text) for window in windows)
        for window in windows:
            records.append(
                {
                    "windowId": window.window_id,
                    "segmentId": window.segment_id,
                    "fieldKey": window.field_key,
                    "fieldOrdinal": window.field_ordinal,
                    "windowOrdinal": window.window_ordinal,
                    "fieldStartChar": window.field_start_char,
                    "fieldEndChar": window.field_end_char,
                    "sourceField": window.source_field,
                    "sourceStartChar": window.source_start_char,
                    "sourceEndChar": window.source_end_char,
                    "modelTokenCount": window.model_token_count,
                    "queryTextSha256": "sha256:" + hashlib.sha256(window.text.encode("utf-8")).hexdigest(),
                }
            )
    if covered_characters != source_characters:
        raise AblationError("dense evidence windows did not cover every source character exactly once")
    digest = "sha256:" + hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()
    token_counts = [window.model_token_count for windows in by_segment.values() for window in windows]
    return by_segment, {
        "version": DENSE_EVIDENCE_WINDOW_VERSION,
        "input": "untrustedEvidenceFieldsOnly",
        "source_order": "numericEvidenceSuffix",
        "boundary_policy": DENSE_EVIDENCE_WINDOW_BOUNDARY_POLICY,
        "overlap_characters": 0,
        "max_input_tokens": ceiling,
        "segment_count": len(segment_ids),
        "source_field_count": total_fields,
        "split_source_field_count": split_fields,
        "query_count": len(records),
        "query_token_max": max(token_counts, default=0),
        "queries_truncated": sum(count > ceiling for count in token_counts),
        "source_character_count": source_characters,
        "covered_character_count": covered_characters,
        "query_set_digest": digest,
    }


def _packed_dense_evidence_window_set(
    *,
    units_by_id: Mapping[str, ExtractionUnit],
    segment_ids: Sequence[str],
    mapper: Any,
) -> tuple[dict[str, tuple[PackedDenseEvidenceWindow, ...]], dict[str, Any]]:
    """Build and identify complete packed queries independently of fusion."""

    embedder = getattr(mapper, "embedder", None)
    counter = getattr(embedder, "model_token_count", None)
    ceiling = getattr(embedder, "max_input_tokens", None)
    if counter is None or type(ceiling) is not int or ceiling <= 0:
        raise AblationError("channel Cp requires the dense model's exact token counter and input ceiling")

    by_segment: dict[str, tuple[PackedDenseEvidenceWindow, ...]] = {}
    records: list[dict[str, Any]] = []
    source_field_count = 0
    source_character_count = 0
    separator_character_count = 0
    covered_source_characters = 0
    empty_source_field_count = 0
    for segment_id in segment_ids:
        unit = units_by_id[segment_id]
        windows = packed_dense_evidence_windows(
            unit,
            token_counter=counter,
            max_input_tokens=ceiling,
        )
        by_segment[segment_id] = windows
        raw_fields = unit.input["untrusted_evidence_fields"]["fields"]
        source_field_count += len(raw_fields)
        source_character_count += sum(len(str(value)) for value in raw_fields.values())
        non_empty_field_count = sum(bool(value) for value in raw_fields.values())
        empty_source_field_count += len(raw_fields) - non_empty_field_count
        separator_character_count += max(0, non_empty_field_count - 1) * len(DENSE_PACKED_EVIDENCE_SEPARATOR)
        for window in windows:
            fragments = [fragment.facts() for fragment in window.fragments]
            covered_source_characters += sum(
                fragment.source_end_char - fragment.source_start_char for fragment in window.fragments
            )
            records.append(
                {
                    "windowId": window.window_id,
                    "segmentId": window.segment_id,
                    "windowOrdinal": window.window_ordinal,
                    "modelTokenCount": window.model_token_count,
                    "queryTextSha256": "sha256:" + hashlib.sha256(window.text.encode("utf-8")).hexdigest(),
                    "fragments": fragments,
                }
            )
    if covered_source_characters != source_character_count:
        raise AblationError("packed dense evidence did not cover every source character exactly once")
    query_character_count = sum(len(window.text) for windows in by_segment.values() for window in windows)
    if query_character_count != source_character_count + separator_character_count:
        raise AblationError("packed dense evidence lost or duplicated query separator characters")
    digest = "sha256:" + hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()
    token_counts = [window.model_token_count for windows in by_segment.values() for window in windows]
    return by_segment, {
        "version": DENSE_PACKED_EVIDENCE_VERSION,
        "input": "untrustedEvidenceFieldsOnly",
        "source_order": "numericEvidenceSuffix",
        "packing": "crossFieldContiguousStream",
        "separator": "LF",
        "boundary_policy": DENSE_EVIDENCE_WINDOW_BOUNDARY_POLICY,
        "overlap_characters": 0,
        "metadata_model_token_budget": 0,
        "evidence_model_token_ceiling": ceiling,
        "evidence_model_token_ceiling_includes_special_tokens": True,
        "overlap_model_token_budget": 0,
        "max_input_tokens": ceiling,
        "segment_count": len(segment_ids),
        "source_field_count": source_field_count,
        "empty_source_field_count": empty_source_field_count,
        "empty_source_field_policy": "validatedZeroWidthFieldsOmittedFromQueries",
        "query_count": len(records),
        "query_token_max": max(token_counts, default=0),
        "queries_truncated": sum(count > ceiling for count in token_counts),
        "source_character_count": source_character_count,
        "covered_source_character_count": covered_source_characters,
        "separator_character_count": separator_character_count,
        "query_character_count": query_character_count,
        "query_set_digest": digest,
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
                "keywords_file_sha256": sha256_file(keywords_path),
                **_keyword_content_facts(keywords_by_segment),
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
        {
            "generated": False,
            "call_count": 0,
            "keywords_file": str(keywords_file),
            "keywords_file_sha256": sha256_file(Path(keywords_file)),
            **_keyword_content_facts(keywords_by_segment),
        },
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help=("Managed runs write experiment.json, candidates.parquet, metrics.json, and decision.md here."),
    )
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--selection-file", type=Path, default=None)
    parser.add_argument(
        "--managed-release",
        type=Path,
        default=None,
        help="Verified RefSpec managed-release bundle manifest.",
    )
    parser.add_argument(
        "--managed-release-digest",
        default=None,
        help="Expected sha256 digest of --managed-release.",
    )
    parser.add_argument(
        "--permission-facet",
        default=DEFAULT_PERMISSION_FACET_IRI,
        help="Exact RefSpec facet requested from the selected OutputProfile.",
    )
    parser.add_argument(
        "--permission-assignment-role",
        default=DEFAULT_PERMISSION_ASSIGNMENT_ROLE_IRI,
        help=("Exact Rulespec assignment-role IRI requested from the selected OutputProfile."),
    )
    parser.add_argument(
        "--permission-resource-route",
        default=DEFAULT_PERMISSION_RESOURCE_ROUTE,
        help="Exact RefSpec resource route requested for candidate use.",
    )
    parser.add_argument(
        "--candidate-language",
        default="en",
        help="Default display language for managed-release candidates.",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Migration-only fused registry; requires --allow-legacy-registry.",
    )
    parser.add_argument(
        "--allow-legacy-registry",
        action="store_true",
        help="Explicitly opt into the migration-only fused registry.",
    )
    parser.add_argument(
        "--targets",
        type=Path,
        default=DEFAULT_TARGETS,
        help=(
            "Managed-release-native development targets. The default file "
            "uses exact RefSpec member IRIs and explicit notRepresented outcomes."
        ),
    )
    parser.add_argument(
        "--resolved",
        type=Path,
        default=None,
        help="Migration-only fused target file; requires --allow-legacy-targets.",
    )
    parser.add_argument(
        "--allow-legacy-targets",
        action="store_true",
        help="Explicitly opt into the historical fused-registry target format.",
    )
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=None,
        help=("Physical dense-index directory. Managed runs default to an isolated directory under --output-dir."),
    )
    parser.add_argument("--limit", type=int, default=PROMPT_CONCEPT_LIMIT)
    parser.add_argument(
        "--configurations",
        nargs="+",
        default=list(DEFAULT_CONFIGURATION_NAMES),
        help=(
            "Subset of configurations to measure. Defaults exclude channel D, "
            "which requires --generate-keywords or --keywords."
        ),
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
    parser.add_argument(
        "--require-adoption-ready",
        action="store_true",
        help="Refuse this run as an adoption verdict unless the untouched-holdout gate passes.",
    )
    parser.add_argument(
        "--decision",
        choices=("continue", "investigate", "stop"),
        default="investigate",
        help="Development-only disposition recorded in decision.md.",
    )
    parser.add_argument(
        "--hypothesis",
        default=(
            "At least one requested selector improves represented-item "
            "retrieval or rank over the first requested configuration."
        ),
        help="Question this development run tests.",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Configuration used as the comparison baseline; defaults to the first requested configuration.",
    )
    parser.add_argument(
        "--decision-rule",
        default=(
            "Investigate any represented target missed by every requested "
            "configuration; continue only on a measurable retrieval or rank "
            "improvement over the baseline."
        ),
        help="Rule for interpreting the development metrics.",
    )
    parser.add_argument(
        "--stop-rule",
        default=(
            "Stop selector tuning after two consecutive development runs show "
            "no represented-item retrieval improvement."
        ),
        help="Condition that ends this experiment sequence.",
    )
    parser.add_argument(
        "--rationale",
        default=None,
        help="Plain-language reason for the development decision.",
    )
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir)
    baseline = args.baseline or args.configurations[0]
    if baseline not in args.configurations:
        parser.error("--baseline must name one of --configurations")
    managed_requested = args.managed_release is not None
    if managed_requested:
        if not args.managed_release_digest:
            parser.error("--managed-release requires --managed-release-digest")
        if args.registry is not None or args.allow_legacy_registry:
            parser.error("--managed-release cannot be combined with the legacy registry")
        if args.resolved is not None or args.allow_legacy_targets:
            parser.error("--managed-release cannot be combined with fused-registry targets")
        if output_dir.exists() and any(output_dir.iterdir()):
            parser.error("--output-dir must be absent or empty for a managed experiment")
        work_dir = output_dir.parent / f".{output_dir.name}.work"
        registry_file = None
        index_dir = args.index_dir or (work_dir / "lookup-index")
        run_output_dir = work_dir
    else:
        if args.managed_release_digest:
            parser.error("--managed-release-digest requires --managed-release")
        if not args.allow_legacy_registry:
            parser.error(
                "pass --managed-release with its exact manifest digest, or explicitly opt into --allow-legacy-registry"
            )
        if not args.allow_legacy_targets:
            parser.error("legacy registry runs also require --allow-legacy-targets")
        if args.targets != DEFAULT_TARGETS:
            parser.error("--targets requires --managed-release")
        registry_file = args.registry or DEFAULT_REGISTRY
        resolved_file = args.resolved or DEFAULT_RESOLVED
        index_dir = args.index_dir or DEFAULT_INDEX_DIR
        run_output_dir = output_dir
    if managed_requested:
        resolved_file = None

    document = run_ablation(
        dataset_dir=args.dataset_dir,
        selection_file=args.selection_file or (args.run_dir / SELECTION_FILE_NAME),
        targets_file=(args.targets if managed_requested else None),
        resolved_file=resolved_file,
        index_dir=index_dir,
        output_dir=run_output_dir,
        configuration_names=args.configurations,
        managed_release_manifest=args.managed_release,
        managed_release_manifest_digest=args.managed_release_digest,
        permission_facet_iri=args.permission_facet,
        permission_assignment_role_iri=(args.permission_assignment_role),
        permission_resource_route=args.permission_resource_route,
        candidate_default_language=args.candidate_language,
        registry_file=registry_file,
        allow_legacy_registry=args.allow_legacy_registry,
        allow_legacy_targets=args.allow_legacy_targets,
        limit=args.limit,
        generate=args.generate_keywords,
        keywords_file=args.keywords,
        fallback_mapper=args.fallback_mapper,
        require_adoption_verdict=args.require_adoption_ready,
    )
    document["experiment_protocol"] = {
        "hypothesis": args.hypothesis,
        "baseline": baseline,
        "decisionRule": args.decision_rule,
        "stopRule": args.stop_rule,
        "evaluationBoundary": "permanentDevelopmentOnly",
        "laterEndToEndCheck": (
            "Re-run the chosen selector through assignment, grounding, and "
            "the declared product query after exact managed-release targets "
            "and an independent holdout exist."
        ),
    }
    table = markdown_table(document["results"])
    if managed_requested:
        rationale = args.rationale or (
            f"Measured {len(document['results'])} selector configuration(s) "
            "against the permanent development set. Review stage-specific "
            "metrics and unbound target identities before the next iteration."
        )
        artifacts = write_experiment_artifacts(
            output_dir,
            document,
            document["candidate_rows"],
            decision=args.decision,
            rationale=rationale,
        )
        print(
            json.dumps(
                {
                    "experiment_directory": str(artifacts.directory),
                    "evaluation_scope": "developmentOnly",
                    "decision": args.decision,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "ablation.json").write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        (output_dir / "ablation.md").write_text(table + "\n")
        write_parquet_rows(
            output_dir / "candidates.parquet",
            columns=CANDIDATE_LINEAGE_COLUMNS,
            rows=document["candidate_rows"],
        )
    print(table)
    print()
    print(json.dumps({"timings_seconds": document["timings_seconds"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Focused regressions for the RefSpec vocabulary-enrichment runtime."""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from spicy_regs.enrichment.reference_runtime import (
    MAPPING_IMPORT_REQUIRED_FEATURES,
    REQUIRED_IMPORT_FEATURES,
    ConceptEventParticipant,
    ConceptLabel,
    ConceptRelation,
    EnrichmentConfiguration,
    EnrichmentDeploymentDecision,
    EnrichmentEvaluationResult,
    EnrichmentProfile,
    ImportFeatureCoverage,
    IndexedVocabularyExpression,
    OutputProfile,
    ReferenceRuntimeError,
    ReferenceRuntimeStore,
    RegistryDeploymentDecision,
    RegistryImportCoverageReport,
    RegistryReconciliationReport,
    VocabularyUniverseFreeze,
    adapt_source_terms_for_migration,
    assert_conforming_vocabulary_rows,
    bind_ranked_candidates,
    canonical_payload_digest,
    canonical_text_digest,
    indexed_expression_id,
    materialize_open_label_value_assertion,
    migrate_legacy_concepts,
    normalize_unicode_text,
    reject_legacy_conforming_payload,
    require_payload_digest,
    require_vocabulary_universe_freeze,
    seal_payload,
    validate_lifecycle_participants,
)
from spicy_regs.evaluation_boundary import (
    EvaluationBoundaryError,
    partition_leakage_facts,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
REFSPEC_SCHEMA_ROOT = REPO_ROOT / "RefSpec" / "bindings" / "json" / "1.0" / "schemas"
FUSION_TOOL = REPO_ROOT / "tools" / "fuse_concept_registries.py"

NOW = "2026-07-29T12:00:00Z"
ACTOR = "urn:test:actor:runtime"
ACTIVITY = "urn:test:activity:runtime"
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64

FACET = "urn:ref:facet:general-subject"
OTHER_FACET = "urn:ref:facet:specialist-subject"
ROLE = "urn:rkaf:assignmentPrimary"
OTHER_ROLE = "urn:rkaf:assignmentMention"
SCHEME = "urn:test:scheme:subjects"
OTHER_SCHEME = "urn:test:scheme:specialists"
RELEASE = "urn:test:release:subjects"
IMPORT = "urn:test:import:subjects"
DISTRIBUTION = "urn:test:artifact:subjects"


def digest_ref(name: str, digest: str = DIGEST_A) -> dict[str, str]:
    return {"id": f"urn:test:{name}", "digest": digest}


def versioned_ref(
    name: str,
    *,
    version: str = "1",
    digest: str = DIGEST_A,
) -> dict[str, str]:
    return {
        "id": f"urn:test:{name}",
        "version": version,
        "digest": digest,
    }


def component_pin(
    name: str,
    *,
    revision: str = "1",
    digest: str = DIGEST_A,
) -> dict[str, str]:
    return {
        "id": f"urn:test:component:{name}",
        "revision": revision,
        "digest": digest,
    }


def concept_label(
    label_id: str,
    *,
    concept: str = "urn:test:concept:water",
    scheme: str = SCHEME,
    role: str = "preferred",
    literal: str = "Water policy",
    language: str = "en",
    migration_only: bool = False,
) -> ConceptLabel:
    return ConceptLabel(
        label_id=label_id,
        concept_iri=concept,
        scheme_iri=scheme,
        release_iri=RELEASE,
        import_snapshot_id=IMPORT,
        distribution_artifact_id=DISTRIBUTION,
        source_property_iri={
            "preferred": "http://www.w3.org/2004/02/skos/core#prefLabel",
            "alternate": "http://www.w3.org/2004/02/skos/core#altLabel",
            "hidden": "http://www.w3.org/2004/02/skos/core#hiddenLabel",
        }[role],
        label_role=role,
        original_literal=literal,
        language_tag=language,
        migration_only=migration_only,
    )


def concept_relation(
    relation_id: str,
    *,
    parent: str,
    predicate: str = "http://www.w3.org/2004/02/skos/core#broader",
    object_scheme: str = SCHEME,
    migration_only: bool = False,
) -> ConceptRelation:
    return ConceptRelation(
        relation_id=relation_id,
        release_iri=RELEASE,
        import_snapshot_id=IMPORT,
        distribution_artifact_id=DISTRIBUTION,
        subject_concept_iri="urn:test:concept:water",
        subject_scheme_iri=SCHEME,
        predicate_iri=predicate,
        object_concept_iri=parent,
        object_scheme_iri=object_scheme,
        source_property_or_path="skos:broader",
        migration_only=migration_only,
    )


def lifecycle_rows(operation: str) -> tuple[ConceptEventParticipant, ...]:
    counts = {
        "deprecation": (1, 0),
        "withdrawal": (1, 0),
        "replacement": (1, 1),
        "promotion": (1, 1),
        "demotion": (1, 1),
        "split": (1, 2),
        "merge": (2, 1),
    }[operation]
    predecessor_kind = "local" if operation == "promotion" else "registered"
    successor_kind = "local" if operation == "demotion" else "registered"
    event = f"event-{operation}"
    predecessors = tuple(
        ConceptEventParticipant(
            event_id=event,
            operation=operation,
            participant_role="predecessor",
            concept_iri=f"urn:test:concept:{operation}:before:{index}",
            concept_kind=predecessor_kind,
            release_iri=f"urn:test:release:{operation}:before",
            complete_membership=True,
            ordinal=index,
        )
        for index in range(counts[0])
    )
    successors = tuple(
        ConceptEventParticipant(
            event_id=event,
            operation=operation,
            participant_role="successor",
            concept_iri=f"urn:test:concept:{operation}:after:{index}",
            concept_kind=successor_kind,
            release_iri=f"urn:test:release:{operation}:after",
            complete_membership=True,
            ordinal=index,
        )
        for index in range(counts[1])
    )
    return (*predecessors, *successors)


def lifecycle_release_membership(
    rows: tuple[ConceptEventParticipant, ...] | list[ConceptEventParticipant],
) -> dict[str, dict[str, Any]]:
    memberships: dict[str, dict[str, Any]] = {}
    for row in rows:
        membership = memberships.setdefault(
            row.release_iri,
            {
                "completeMembership": True,
                "members": [],
            },
        )
        membership["members"].append(row.concept_iri)
    return memberships


@pytest.mark.parametrize(
    "operation",
    [
        "deprecation",
        "withdrawal",
        "replacement",
        "split",
        "merge",
        "promotion",
        "demotion",
    ],
)
def test_every_lifecycle_operation_accepts_its_exact_cardinality(
    operation: str,
) -> None:
    rows = lifecycle_rows(operation)
    validate_lifecycle_participants(
        rows,
        release_membership=lifecycle_release_membership(rows),
    )


def test_lifecycle_rejects_duplicate_members_release_drift_and_wrong_promotion_type() -> None:
    replacement = list(lifecycle_rows("replacement"))
    replacement[1] = replace(
        replacement[1],
        concept_iri=replacement[0].concept_iri,
    )
    with pytest.raises(ReferenceRuntimeError, match="duplicate participant"):
        validate_lifecycle_participants(
            replacement,
            release_membership=lifecycle_release_membership(replacement),
        )

    split = list(lifecycle_rows("split"))
    split[2] = replace(split[2], release_iri="urn:test:release:other")
    with pytest.raises(ReferenceRuntimeError, match="successor release"):
        validate_lifecycle_participants(
            split,
            release_membership=lifecycle_release_membership(split),
        )

    promotion = list(lifecycle_rows("promotion"))
    promotion[0] = replace(promotion[0], concept_kind="registered")
    with pytest.raises(ReferenceRuntimeError, match="local to registered"):
        validate_lifecycle_participants(
            promotion,
            release_membership=lifecycle_release_membership(promotion),
        )


def test_lifecycle_rejects_a_participant_missing_from_its_pinned_release() -> None:
    rows = lifecycle_rows("replacement")
    membership = lifecycle_release_membership(rows)
    membership[rows[1].release_iri]["members"] = []
    with pytest.raises(ReferenceRuntimeError, match="is not a member"):
        validate_lifecycle_participants(
            rows,
            release_membership=membership,
        )


def test_lifecycle_rejects_the_same_release_on_both_sides() -> None:
    rows = list(lifecycle_rows("replacement"))
    rows[1] = replace(rows[1], release_iri=rows[0].release_iri)
    with pytest.raises(
        ReferenceRuntimeError,
        match="predecessor and successor releases must differ",
    ):
        validate_lifecycle_participants(
            rows,
            release_membership=lifecycle_release_membership(rows),
        )


def test_multilingual_labels_and_multiple_parents_round_trip(
    tmp_path: Path,
) -> None:
    labels = (
        concept_label("label-en"),
        concept_label(
            "label-es",
            literal="Política del agua",
            language="es",
        ),
        concept_label(
            "label-zh",
            literal="水政策",
            language="zh-Hant",
        ),
        concept_label(
            "label-environment",
            concept="urn:test:concept:environment",
            literal="Environment",
        ),
        concept_label(
            "label-infrastructure",
            concept="urn:test:concept:infrastructure",
            literal="Infrastructure",
        ),
    )
    relations = (
        concept_relation("relation-a", parent="urn:test:concept:environment"),
        concept_relation("relation-b", parent="urn:test:concept:infrastructure"),
    )
    store = ReferenceRuntimeStore(tmp_path)
    participants = lifecycle_rows("split")
    membership = lifecycle_release_membership(participants)
    membership[RELEASE] = {
        "completeMembership": True,
        "members": [
            "urn:test:concept:water",
            "urn:test:concept:environment",
            "urn:test:concept:infrastructure",
        ],
    }
    store.write_vocabulary_rows(
        labels=labels,
        relations=relations,
        participants=participants,
        release_membership=membership,
    )
    rows = store.read_vocabulary_rows()
    assert {row["language_tag"] for row in rows["concept_labels"]} == {"en", "es", "zh-Hant"}
    assert {row["object_concept_iri"] for row in rows["concept_relations"]} == {
        "urn:test:concept:environment",
        "urn:test:concept:infrastructure",
    }
    assert len(rows["concept_event_participants"]) == 3
    assert all(row["migration_only"] is False for table_rows in rows.values() for row in table_rows)
    assert all(
        row["complete_membership"] is True and isinstance(row["ordinal"], int)
        for row in rows["concept_event_participants"]
    )


def test_label_collision_uses_rdf_lexical_equality_not_search_normalization() -> None:
    assert_conforming_vocabulary_rows(
        (
            concept_label(
                "preferred",
                literal="Café",
            ),
            concept_label(
                "alternate",
                role="alternate",
                literal="Cafe\u0301",
            ),
        ),
        (),
        (),
    )
    with pytest.raises(ReferenceRuntimeError, match="disjoint"):
        assert_conforming_vocabulary_rows(
            (
                concept_label("preferred", literal="Café"),
                concept_label(
                    "alternate",
                    role="alternate",
                    literal="Café",
                ),
            ),
            (),
            (),
        )


def test_hierarchy_requires_absolute_scheme_internal_skos_predicates() -> None:
    with pytest.raises(ReferenceRuntimeError, match="absolute IRI"):
        concept_relation(
            "compact",
            parent="urn:test:concept:parent",
            predicate="skos:broader",
        )
    with pytest.raises(ReferenceRuntimeError, match="scheme-internal"):
        concept_relation(
            "cross-scheme",
            parent="urn:test:concept:parent",
            object_scheme=OTHER_SCHEME,
        )
    concept_relation(
        "related",
        parent="urn:test:concept:neighbor",
        predicate="http://www.w3.org/2004/02/skos/core#related",
    )


def test_hierarchy_targets_resolve_to_exact_catalog_and_release_membership() -> None:
    labels = (
        concept_label("child"),
        concept_label(
            "parent",
            concept="urn:test:concept:parent",
            literal="Parent",
        ),
    )
    relation = concept_relation(
        "resolved",
        parent="urn:test:concept:parent",
    )
    membership = {
        RELEASE: {
            "completeMembership": True,
            "members": [
                "urn:test:concept:water",
                "urn:test:concept:parent",
            ],
        }
    }
    assert_conforming_vocabulary_rows(
        labels,
        (relation,),
        (),
        release_membership=membership,
    )

    with pytest.raises(ReferenceRuntimeError, match="target does not resolve"):
        assert_conforming_vocabulary_rows(
            labels,
            (
                concept_relation(
                    "arbitrary-target",
                    parent="urn:test:concept:not-in-catalog",
                ),
            ),
            (),
            release_membership=membership,
        )

    incomplete = {
        RELEASE: {
            "completeMembership": True,
            "members": ["urn:test:concept:water"],
        }
    }
    with pytest.raises(ReferenceRuntimeError, match="target is not a member"):
        assert_conforming_vocabulary_rows(
            labels,
            (relation,),
            (),
            release_membership=incomplete,
        )


def feature_row(
    feature: str,
    *,
    source: int = 0,
    parsed: int = 0,
    indexed: int = 0,
    source_digest: str = DIGEST_A,
    parsed_digest: str = DIGEST_A,
    indexed_digest: str = DIGEST_A,
    parse_explanation: str | None = None,
    index_explanation: str | None = None,
) -> ImportFeatureCoverage:
    return ImportFeatureCoverage(
        feature=feature,
        source_observed_count=source,
        parsed_count=parsed,
        indexed_count=indexed,
        explicitly_excluded_count=0,
        failed_count=0,
        source_observed_digest=source_digest,
        parsed_digest=parsed_digest,
        indexed_digest=indexed_digest,
        parse_difference_explanation=parse_explanation,
        index_difference_explanation=index_explanation,
    )


def coverage_report() -> RegistryImportCoverageReport:
    profile = output_profile()
    return RegistryImportCoverageReport(
        report_id="urn:test:coverage:subjects",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="immutable",
        output_profile=dict(profile.reference),
        import_snapshot=digest_ref("import:subjects"),
        reference_resource_release=versioned_ref("release:subjects"),
        distribution_artifacts=(digest_ref("artifact:subjects"),),
        import_profile=versioned_ref("import-profile:skos"),
        parser_version="source-parser-v1",
        index_snapshot=digest_ref("index:subjects"),
        activity=ACTIVITY,
        receipt="urn:test:receipt:coverage",
        feature_rows=tuple(feature_row(feature) for feature in sorted(REQUIRED_IMPORT_FEATURES)),
    )


def mapping_coverage_report() -> RegistryImportCoverageReport:
    feature_rows = tuple(
        replace(
            row,
            required_for_candidate_or_output=(
                row.feature in MAPPING_IMPORT_REQUIRED_FEATURES
            ),
        )
        for row in coverage_report().feature_rows
    )
    return replace(
        coverage_report(),
        report_id="urn:test:coverage:mappings",
        import_snapshot=digest_ref("import:mappings"),
        reference_resource_release=versioned_ref("release:mappings"),
        distribution_artifacts=(digest_ref("artifact:mappings"),),
        index_snapshot=digest_ref("index:mappings"),
        receipt="urn:test:receipt:coverage:mappings",
        feature_rows=feature_rows,
    )


def test_coverage_report_reconciles_every_required_feature() -> None:
    payload = coverage_report().sealed_payload()
    assert len(payload["features"]) == 10
    require_payload_digest(payload)


def test_coverage_fails_silent_hierarchy_loss() -> None:
    rows = [feature_row(feature) for feature in sorted(REQUIRED_IMPORT_FEATURES)]
    hierarchy = rows.index(next(row for row in rows if row.feature == "hierarchy"))
    rows[hierarchy] = feature_row(
        "hierarchy",
        source=1,
        parsed=0,
        indexed=0,
    )
    report = replace(coverage_report(), feature_rows=tuple(rows))
    with pytest.raises(ReferenceRuntimeError, match="source-to-parsed"):
        report.payload()


def test_coverage_allows_explained_equal_count_digest_transforms() -> None:
    row = feature_row(
        "labels",
        source=2,
        parsed=2,
        indexed=2,
        source_digest=DIGEST_A,
        parsed_digest=DIGEST_B,
        indexed_digest=DIGEST_C,
        parse_explanation="JSON-LD language maps were expanded.",
        index_explanation="Unicode text was normalized for search.",
    )
    assert row.payload()["parseDifferenceExplanation"]
    assert row.payload()["indexDifferenceExplanation"]


def registry_deployment_decision(
    *,
    coverage: Mapping[str, Any] | None = None,
    profile: Mapping[str, Any] | None = None,
) -> RegistryDeploymentDecision:
    coverage_record = coverage or coverage_report().sealed_payload()
    profile_record = profile or output_profile().sealed_payload()
    return RegistryDeploymentDecision(
        decision_id="urn:test:registry-deployment:subjects",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="effective",
        environment={
            "id": "urn:test:environment:production",
            "classification": "production",
        },
        registry_import_snapshot=dict(coverage_record["registryImportSnapshot"]),
        reference_resource_release=dict(coverage_record["referenceResourceRelease"]),
        coverage_report={
            "id": coverage_record["id"],
            "digest": coverage_record["canonicalPayloadDigest"],
        },
        output_profile={
            "id": profile_record["id"],
            "version": profile_record["version"],
            "digest": profile_record["contentDigest"],
        },
        selection_state="selected",
        effective_at=NOW,
        reason="Coverage passed and governance authorization is effective.",
        activity=ACTIVITY,
        rulespec_attestation_refs=("urn:test:attestation:registry-subjects",),
        local_adoption_refs=("urn:test:adoption:registry-subjects",),
        authorization_validations=(
            {
                "authorizationRef": ("urn:test:attestation:registry-subjects"),
                "kind": "rulespecAttestation",
                "validationReceipt": digest_ref("validation:registry-attestation"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
            {
                "authorizationRef": "urn:test:adoption:registry-subjects",
                "kind": "localAdoption",
                "validationReceipt": digest_ref("validation:registry-adoption"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
        ),
    )


def test_registry_deployment_selects_only_exact_passing_coverage() -> None:
    coverage = coverage_report().sealed_payload()
    profile = output_profile().sealed_payload()
    decision = registry_deployment_decision(
        coverage=coverage,
        profile=profile,
    )
    payload = decision.sealed_payload(
        coverage_report_record=coverage,
        output_profile_record=profile,
    )
    assert payload["selectionState"] == "selected"
    require_payload_digest(payload)

    with pytest.raises(ReferenceRuntimeError, match="supplied exact"):
        decision.payload()

    failing_coverage = replace(
        coverage_report(),
        report_status="fail",
    ).sealed_payload()
    failing_decision = registry_deployment_decision(
        coverage=failing_coverage,
        profile=profile,
    )
    with pytest.raises(ReferenceRuntimeError, match="passing import coverage"):
        failing_decision.payload(
            coverage_report_record=failing_coverage,
            output_profile_record=profile,
        )


def test_registry_deployment_requires_exact_authorization_validations() -> None:
    coverage = coverage_report().sealed_payload()
    profile = output_profile().sealed_payload()
    decision = registry_deployment_decision(
        coverage=coverage,
        profile=profile,
    )
    incomplete = replace(
        decision,
        authorization_validations=decision.authorization_validations[:1],
    )
    with pytest.raises(
        ReferenceRuntimeError,
        match="at least two authorization",
    ):
        incomplete.payload(
            coverage_report_record=coverage,
            output_profile_record=profile,
        )


def expression(
    *,
    scheme: str = SCHEME,
    member: str = "urn:test:concept:water",
    source: str = "http://www.w3.org/2004/02/skos/core#prefLabel",
    literal: str = "水政策",
    language: str | None = "zh-Hant",
    datatype: str | None = None,
    release: dict[str, str] | None = None,
) -> IndexedVocabularyExpression:
    release_pin = release or versioned_ref("release:subjects")
    import_pin = digest_ref("import:subjects")
    distribution_pin = digest_ref("artifact:subjects")
    expression_id = indexed_expression_id(
        reference_resource_release=release_pin,
        registry_import_snapshot=import_pin,
        distribution_artifact=distribution_pin,
        scheme_iri=scheme,
        member_iri=member,
        source_property_or_path=source,
        original_literal=literal,
        language_tag=language,
        datatype_iri=datatype,
    )
    indexed = normalize_unicode_text(literal)
    return IndexedVocabularyExpression(
        expression_id=expression_id,
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="active",
        reference_resource_release=release_pin,
        registry_import_snapshot=import_pin,
        distribution_artifact=distribution_pin,
        scheme_iri=scheme,
        member_iri=member,
        source_property_or_path=source,
        original_literal=literal,
        language_tag=language,
        datatype_iri=datatype,
        normalization_policy=versioned_ref("normalization:unicode-nfkc"),
        indexed_text=indexed,
        indexed_text_digest=canonical_text_digest(indexed),
        indexed_representation_version="labels-v1",
        index_snapshot=digest_ref("index:subjects"),
        activity=ACTIVITY,
        receipt="urn:test:receipt:index",
    )


def test_indexed_expression_preserves_unicode_and_exact_identity() -> None:
    payload = expression().sealed_payload()
    assert payload["originalLiteral"] == "水政策"
    assert payload["language"] == "zh-Hant"
    assert payload["indexedText"] == "水政策"
    require_payload_digest(payload)
    assert expression(scheme=SCHEME).expression_id != expression(scheme=OTHER_SCHEME).expression_id
    assert expression(member="urn:test:concept:other").expression_id != expression().expression_id
    assert expression(source="/native/label").expression_id != expression().expression_id


def test_indexed_expression_requires_exactly_one_language_or_datatype() -> None:
    typed = expression(
        language=None,
        datatype="http://www.w3.org/2001/XMLSchema#string",
    )
    payload = typed.payload()
    assert "language" not in payload
    assert payload["datatype"].endswith("#string")
    with pytest.raises(ReferenceRuntimeError, match="exactly one"):
        expression(language=None, datatype=None).payload()
    with pytest.raises(ReferenceRuntimeError, match="exactly one"):
        expression(
            language="en",
            datatype="http://www.w3.org/2001/XMLSchema#string",
        ).payload()


def test_indexed_expression_rejects_arbitrary_identity_and_ascii_only_policy() -> None:
    with pytest.raises(ReferenceRuntimeError, match="exact source identity"):
        replace(expression(), expression_id="urn:test:expression:made-up").payload()
    with pytest.raises(ReferenceRuntimeError, match="ASCII-only"):
        replace(
            expression(),
            normalization_policy={
                "id": "urn:test:legacy-ascii-v1",
                "version": "1",
                "digest": DIGEST_A,
            },
        ).payload()


def enrichment_profile() -> EnrichmentProfile:
    return EnrichmentProfile(
        profile_id="urn:test:enrichment-profile:core",
        version="1",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="immutable",
        facets=(
            {
                "iri": FACET,
                "label": "General subject",
                "definition": "A reusable subject classification.",
                "inclusionCues": ["Substantive policy topic"],
                "exclusionCues": ["Named entity"],
                "compatibleResourceRoutes": [
                    "document",
                    "externalReference",
                ],
                "compatibleAssignmentPredicates": [ROLE],
            },
            {
                "iri": OTHER_FACET,
                "label": "Specialist subject",
                "definition": "A specialist subject classification.",
                "inclusionCues": ["Domain-specific topic"],
                "exclusionCues": ["General topic"],
                "compatibleResourceRoutes": ["document", "entity"],
                "compatibleAssignmentPredicates": [OTHER_ROLE],
            },
        ),
    )


def output_profile() -> OutputProfile:
    enrichment = enrichment_profile()
    release_pin = versioned_ref("release:subjects")
    import_pin = digest_ref("import:subjects")
    mapping_pin = digest_ref("import:mappings")
    source_release = versioned_ref("release:mapping-source")
    target_release = versioned_ref("release:mapping-target")
    return OutputProfile(
        profile_id="urn:test:output-profile:core",
        version="1",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="immutable",
        enrichment_profile=dict(enrichment.reference),
        acceptance_policies=(versioned_ref("acceptance-policy:core"),),
        publication_views=(versioned_ref("view:accepted"),),
        release_permissions=(
            {
                "facet": FACET,
                "assignmentRole": ROLE,
                "referenceResourceRelease": release_pin,
                "registryImportSnapshot": import_pin,
                "requiredImportFeatures": sorted(REQUIRED_IMPORT_FEATURES),
                "candidateUse": True,
                "acceptedOutputUse": True,
            },
            {
                "facet": OTHER_FACET,
                "assignmentRole": OTHER_ROLE,
                "referenceResourceRelease": versioned_ref("release:specialists"),
                "registryImportSnapshot": digest_ref("import:specialists"),
                "requiredImportFeatures": sorted(REQUIRED_IMPORT_FEATURES),
                "candidateUse": True,
                "acceptedOutputUse": True,
            },
        ),
        mapping_permissions=(
            {
                "facet": FACET,
                "assignmentRole": ROLE,
                "mappingSnapshot": mapping_pin,
                "sourceRelease": source_release,
                "targetRelease": target_release,
                "relation": "http://www.w3.org/2004/02/skos/core#exactMatch",
                "direction": "sourceToTarget",
                "candidateUse": True,
                "acceptedOutputUse": False,
            },
        ),
        open_label_permissions=(
            {
                "facet": FACET,
                "assignmentRole": ROLE,
                "mode": "explicitLanguage",
                "candidateUse": True,
                "acceptedOutputUse": True,
            },
            {
                "facet": OTHER_FACET,
                "assignmentRole": OTHER_ROLE,
                "mode": "declaredDefaultLanguage",
                "defaultLanguage": "en",
                "candidateUse": True,
                "acceptedOutputUse": True,
            },
        ),
        enrichment_profile_record=enrichment,
    )


def test_output_profile_requires_one_exact_permission_row() -> None:
    profile = output_profile()
    profile.authorize_release(
        facet=FACET,
        assignment_role=ROLE,
        resource_route="document",
        reference_resource_release=versioned_ref("release:subjects"),
        registry_import_snapshot=digest_ref("import:subjects"),
        coverage_report=coverage_report(),
        accepted_output=True,
    )
    with pytest.raises(ReferenceRuntimeError, match="exactly one complete"):
        profile.authorize_release(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            reference_resource_release=versioned_ref("release:specialists"),
            registry_import_snapshot=digest_ref("import:specialists"),
            coverage_report=coverage_report(),
            accepted_output=True,
        )
    with pytest.raises(ReferenceRuntimeError, match="exactly one complete"):
        profile.authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=mapping_coverage_report(),
            accepted_output=True,
        )


def test_output_profile_authorizes_mapping_with_exact_passing_coverage() -> None:
    row = output_profile().authorize_mapping(
        facet=FACET,
        assignment_role=ROLE,
        resource_route="document",
        mapping_snapshot=digest_ref("import:mappings"),
        source_release=versioned_ref("release:mapping-source"),
        target_release=versioned_ref("release:mapping-target"),
        relation="http://www.w3.org/2004/02/skos/core#exactMatch",
        direction="sourceToTarget",
        coverage_report=mapping_coverage_report(),
        accepted_output=False,
    )
    assert row["mappingSnapshot"] == digest_ref("import:mappings")


@pytest.mark.parametrize(
    ("coverage", "message"),
    [
        (None, "exact coverage"),
        (
            replace(mapping_coverage_report(), report_status="fail"),
            "passing coverage",
        ),
        (
            replace(
                mapping_coverage_report(),
                output_profile=versioned_ref("output-profile:other"),
            ),
            "exact OutputProfile",
        ),
        (
            replace(
                mapping_coverage_report(),
                import_snapshot=digest_ref("import:mappings:other"),
            ),
            "import snapshot",
        ),
    ],
)
def test_output_profile_rejects_missing_failing_or_mismatched_mapping_coverage(
    coverage: RegistryImportCoverageReport | None,
    message: str,
) -> None:
    with pytest.raises(ReferenceRuntimeError, match=message):
        output_profile().authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=coverage,
            accepted_output=False,
        )


def test_output_profile_requires_exact_mapping_release_and_endpoint_pins() -> None:
    malformed_mapping_release = replace(
        mapping_coverage_report(),
        reference_resource_release=digest_ref("release:mappings"),
    )
    with pytest.raises(ReferenceRuntimeError, match="version"):
        output_profile().authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=malformed_mapping_release,
            accepted_output=False,
        )
    with pytest.raises(ReferenceRuntimeError, match="exactly one complete"):
        output_profile().authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source:other"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=mapping_coverage_report(),
            accepted_output=False,
        )


@pytest.mark.parametrize("feature", ["mappings", "identifiers", "membership"])
def test_output_profile_requires_mapping_import_feature_flags(feature: str) -> None:
    feature_rows = tuple(
        replace(row, required_for_candidate_or_output=False) if row.feature == feature else row
        for row in mapping_coverage_report().feature_rows
    )
    with pytest.raises(ReferenceRuntimeError, match="exactly match"):
        output_profile().authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=replace(
                mapping_coverage_report(),
                feature_rows=feature_rows,
            ),
            accepted_output=False,
        )


def test_output_profile_rejects_an_extra_mapping_import_feature_flag() -> None:
    feature_rows = tuple(
        replace(row, required_for_candidate_or_output=True)
        if row.feature == "labels"
        else row
        for row in mapping_coverage_report().feature_rows
    )
    with pytest.raises(ReferenceRuntimeError, match="exactly match"):
        output_profile().authorize_mapping(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mapping_snapshot=digest_ref("import:mappings"),
            source_release=versioned_ref("release:mapping-source"),
            target_release=versioned_ref("release:mapping-target"),
            relation="http://www.w3.org/2004/02/skos/core#exactMatch",
            direction="sourceToTarget",
            coverage_report=replace(
                mapping_coverage_report(),
                feature_rows=feature_rows,
            ),
            accepted_output=False,
        )


def test_output_profile_rejects_duplicate_selector_with_different_booleans() -> None:
    profile = output_profile()
    duplicate = {
        **profile.release_permissions[0],
        "acceptedOutputUse": False,
    }
    with pytest.raises(ReferenceRuntimeError, match="selector tuple"):
        replace(
            profile,
            release_permissions=(*profile.release_permissions, duplicate),
        ).payload()


def test_output_profile_materializes_default_language_only_from_exact_row() -> None:
    profile = output_profile()
    row = profile.authorize_open_label(
        facet=OTHER_FACET,
        assignment_role=OTHER_ROLE,
        resource_route="document",
        mode="declaredDefaultLanguage",
        default_language="en",
        accepted_output=True,
    )
    assert row["defaultLanguage"] == "en"
    with pytest.raises(ReferenceRuntimeError, match="exactly one complete"):
        profile.authorize_open_label(
            facet=OTHER_FACET,
            assignment_role=OTHER_ROLE,
            resource_route="document",
            mode="declaredDefaultLanguage",
            default_language="es",
            accepted_output=True,
        )


def test_output_profile_resolves_facet_role_route_and_coverage() -> None:
    profile = output_profile()
    with pytest.raises(ReferenceRuntimeError, match="not defined"):
        replace(
            profile,
            release_permissions=(
                {
                    **profile.release_permissions[0],
                    "facet": "urn:ref:facet:unknown",
                },
            ),
        ).payload()
    with pytest.raises(ReferenceRuntimeError, match="assignment role"):
        replace(
            profile,
            release_permissions=(
                {
                    **profile.release_permissions[0],
                    "assignmentRole": OTHER_ROLE,
                },
            ),
        ).payload()
    with pytest.raises(ReferenceRuntimeError, match="resource route"):
        profile.authorize_release(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="entity",
            reference_resource_release=versioned_ref("release:subjects"),
            registry_import_snapshot=digest_ref("import:subjects"),
            coverage_report=coverage_report(),
            accepted_output=True,
        )
    wrong_profile_coverage = replace(
        coverage_report(),
        output_profile=versioned_ref("output-profile:other"),
    )
    with pytest.raises(ReferenceRuntimeError, match="exact OutputProfile"):
        profile.authorize_release(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            reference_resource_release=versioned_ref("release:subjects"),
            registry_import_snapshot=digest_ref("import:subjects"),
            coverage_report=wrong_profile_coverage,
            accepted_output=True,
        )
    with pytest.raises(ReferenceRuntimeError, match="passing coverage"):
        profile.authorize_release(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            reference_resource_release=versioned_ref("release:subjects"),
            registry_import_snapshot=digest_ref("import:subjects"),
            coverage_report=replace(coverage_report(), report_status="fail"),
            accepted_output=True,
        )
    mismatched_features = tuple(
        replace(
            row,
            required_for_candidate_or_output=False,
        )
        if row.feature == "notes"
        else row
        for row in coverage_report().feature_rows
    )
    with pytest.raises(ReferenceRuntimeError, match="requirement flags"):
        profile.authorize_release(
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            reference_resource_release=versioned_ref("release:subjects"),
            registry_import_snapshot=digest_ref("import:subjects"),
            coverage_report=replace(
                coverage_report(),
                feature_rows=mismatched_features,
            ),
            accepted_output=True,
        )


def test_open_label_builder_materializes_default_and_evidence() -> None:
    graph = materialize_open_label_value_assertion(
        output_profile=output_profile(),
        facet=OTHER_FACET,
        assignment_role=OTHER_ROLE,
        resource_route="document",
        mode="declaredDefaultLanguage",
        declared_default_language="en",
        literal="Air quality",
        language_tag=None,
        assertion_id="urn:test:assertion:open-label",
        subject_iri="urn:test:artifact:one",
        extraction_activity_iri="urn:test:activity:extract",
        asserted_at=NOW,
        evidence_binding_id="urn:test:evidence:open-label",
        source_fragment_iris=("urn:test:fragment:one",),
    )
    assertion = graph["assertion"]
    evidence = graph["evidenceBinding"]
    assert assertion["rkaf:assertsValue"] == {
        "@value": "Air quality",
        "@language": "en",
    }
    assert assertion["rkaf:openLabelFacet"] == OTHER_FACET
    assert assertion["rkaf:openLabelRole"] == OTHER_ROLE
    assert evidence["rkaf:bindsAssertion"] == assertion["@id"]
    assert evidence["rkaf:evidentiaryFunction"] == "rkaf:supports"


def test_open_label_builder_rejects_untagged_or_ungrounded_output() -> None:
    with pytest.raises(ReferenceRuntimeError, match="language"):
        materialize_open_label_value_assertion(
            output_profile=output_profile(),
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mode="explicitLanguage",
            declared_default_language=None,
            literal="Air quality",
            language_tag=None,
            assertion_id="urn:test:assertion:open-label",
            subject_iri="urn:test:artifact:one",
            extraction_activity_iri="urn:test:activity:extract",
            asserted_at=NOW,
            evidence_binding_id="urn:test:evidence:open-label",
            source_fragment_iris=("urn:test:fragment:one",),
        )
    with pytest.raises(ReferenceRuntimeError, match="must not be empty"):
        materialize_open_label_value_assertion(
            output_profile=output_profile(),
            facet=FACET,
            assignment_role=ROLE,
            resource_route="document",
            mode="explicitLanguage",
            declared_default_language=None,
            literal="Air quality",
            language_tag="en",
            assertion_id="urn:test:assertion:open-label",
            subject_iri="urn:test:artifact:one",
            extraction_activity_iri="urn:test:activity:extract",
            asserted_at=NOW,
            evidence_binding_id="urn:test:evidence:open-label",
            source_fragment_iris=(),
        )


def configuration() -> EnrichmentConfiguration:
    output = output_profile()
    behavior = component_pin("behavior")
    return EnrichmentConfiguration(
        configuration_id="urn:test:configuration:core",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="immutable",
        implementation={
            "id": "urn:test:implementation:spicy-regs",
            "revision": "abcdef1",
            "build": "local-test",
            "runtime": component_pin("python-runtime"),
            "dependencyLockDigest": DIGEST_A,
        },
        enrichment_profile=dict(output.enrichment_profile),
        output_profile=dict(output.reference),
        acceptance_policy=versioned_ref("acceptance-policy:core"),
        schemas=(versioned_ref("schema:ref-json-binding"),),
        input_corpora=(versioned_ref("corpus:gold-universe"),),
        vocabulary={
            "referenceResourceReleases": [
                versioned_ref("release:subjects"),
                versioned_ref("release:specialists"),
                versioned_ref("release:mapping-source"),
                versioned_ref("release:mapping-target"),
            ],
            "registryImportSnapshots": [
                digest_ref("import:subjects"),
                digest_ref("import:specialists"),
            ],
            "mappingReleases": [versioned_ref("release:mappings")],
            "mappingSnapshots": [digest_ref("import:mappings")],
            "candidateTargetUniverseDigest": DIGEST_C,
            "registryDeploymentDecisions": [
                digest_ref("deployment:registry-subjects"),
                digest_ref("deployment:registry-specialists"),
                digest_ref("deployment:registry-mappings"),
            ],
        },
        indexes=(
            {
                "indexSnapshot": digest_ref("index:subjects"),
                "indexedExpressionCorpusDigest": DIGEST_A,
                "indexedRepresentationVersion": "labels-v1",
                "normalizationPolicy": versioned_ref("normalization:unicode-nfkc"),
            },
        ),
        candidate_channels=(
            {
                "id": "urn:test:channel:bm25",
                "retriever": component_pin("retriever"),
                "queryConstruction": component_pin("query"),
                "ordering": component_pin("ordering"),
                "fusion": component_pin("fusion"),
                "deduplication": component_pin("deduplication"),
                "quota": {
                    "maximumCandidates": 40,
                    "policy": component_pin("quota"),
                },
                "truncation": component_pin("truncation"),
                "fallbackPolicy": component_pin("fallback"),
            },
        ),
        models=(),
        prompts=(component_pin("prompt"),),
        tool_policies=(component_pin("tools"),),
        budgets=(
            {
                "stage": "urn:test:stage:candidate-generation",
                "inputBytes": 100000,
                "outputBytes": 100000,
                "tokens": 10000,
                "milliseconds": 30000,
                "candidates": 40,
                "costMicrounits": 0,
            },
        ),
        determinism=(
            {
                "stage": "urn:test:stage:candidate-generation",
                "status": "deterministic",
                "seed": "fixed",
                "replayControls": component_pin("replay"),
            },
        ),
        other_behavior_pins=(behavior,),
        secret_version_refs=(),
        output_profile_record=output,
    )


def sealed_gold_manifest(
    config: EnrichmentConfiguration,
) -> dict[str, Any]:
    development_item = {
        "id": "urn:test:gold-item:development",
        "split": "development",
        "sourceResource": "urn:test:source:development",
        "renditionArtifact": digest_ref(
            "artifact:development",
            DIGEST_B,
        ),
        "partitionKeys": {
            "conceptIdentity": ["urn:test:concept:water"],
            "exactMatchCluster": ["urn:test:cluster:water"],
            "alias": ["Water governance"],
            "sourceIdentity": ["urn:test:source:development"],
            "artifactDigest": [DIGEST_B],
            "textDigest": [DIGEST_B],
            "nearDuplicateCluster": ["urn:test:near-duplicate:development"],
        },
        "partitionEvidence": {
            "sourceTextDigest": DIGEST_B,
            "vocabularyExpressionCorpusDigest": DIGEST_A,
            "exactMatchGraphDigest": DIGEST_B,
            "nearDuplicateAnalysisDigest": DIGEST_C,
            "receipt": "urn:test:receipt:partition:development",
        },
    }
    holdout_item = {
        "id": "urn:test:gold-item:holdout",
        "split": "holdout",
        "sourceResource": "urn:test:source:holdout",
        "renditionArtifact": digest_ref("artifact:holdout", DIGEST_C),
        "partitionKeys": {
            "conceptIdentity": [],
            "exactMatchCluster": [],
            "alias": ["Emerging subject"],
            "sourceIdentity": ["urn:test:source:holdout"],
            "artifactDigest": [DIGEST_C],
            "textDigest": [DIGEST_C],
            "nearDuplicateCluster": ["urn:test:near-duplicate:holdout"],
        },
        "partitionEvidence": {
            "sourceTextDigest": DIGEST_C,
            "vocabularyExpressionCorpusDigest": DIGEST_A,
            "exactMatchGraphDigest": DIGEST_B,
            "nearDuplicateAnalysisDigest": DIGEST_C,
            "receipt": "urn:test:receipt:partition:holdout",
        },
    }
    items = (development_item, holdout_item)
    partition_dimensions = tuple(
        {
            "dimension": dimension,
            "itemKeys": [
                {
                    "item": item["id"],
                    "values": list(item["partitionKeys"][dimension]),
                }
                for item in items
            ],
        }
        for dimension in sorted(
            {
                "conceptIdentity",
                "exactMatchCluster",
                "alias",
                "sourceIdentity",
                "artifactDigest",
                "textDigest",
                "nearDuplicateCluster",
            }
        )
    )
    return seal_payload(
        {
            "id": "urn:test:gold:sealed",
            "type": "urn:ref:type:SealedGoldManifest",
            "corpusDigest": DIGEST_A,
            "items": list(items),
            "vocabularyUniverse": {
                "referenceResourceReleases": list(config.vocabulary["referenceResourceReleases"]),
                "registryImportSnapshots": list(config.vocabulary["registryImportSnapshots"]),
                "mappingReleases": list(config.vocabulary["mappingReleases"]),
                "mappingSnapshots": list(config.vocabulary["mappingSnapshots"]),
                "indexedExpressionCorpusDigests": [index["indexedExpressionCorpusDigest"] for index in config.indexes],
                "enrichmentProfile": dict(config.enrichment_profile),
                "outputProfile": dict(config.output_profile),
                "normalizationPolicy": dict(config.indexes[0]["normalizationPolicy"]),
                "candidateTargetUniverseDigest": config.vocabulary["candidateTargetUniverseDigest"],
            },
            "expectations": [
                {
                    "id": "urn:test:expectation:represented",
                    "item": development_item["id"],
                    "registeredTargets": [
                        {
                            "target": "urn:test:concept:water",
                            "grade": "exact",
                        }
                    ],
                },
                {
                    "id": "urn:test:expectation:not-represented",
                    "item": holdout_item["id"],
                    "registeredTargets": [
                        {
                            "grade": "notRepresented",
                        }
                    ],
                },
            ],
            "partitionReport": {
                "inputDigests": [DIGEST_A, DIGEST_B, DIGEST_C],
                "dimensions": list(partition_dimensions),
            },
        }
    )


def evaluation_result(
    config: EnrichmentConfiguration,
    *,
    verdict: str = "pass",
    gate_passed: bool = True,
) -> EnrichmentEvaluationResult:
    gold = sealed_gold_manifest(config)
    return EnrichmentEvaluationResult(
        result_id="urn:test:evaluation:core",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="immutable",
        configuration={
            "id": config.configuration_id,
            "digest": config.digest,
        },
        sealed_gold_manifest={
            "id": gold["id"],
            "digest": gold["canonicalPayloadDigest"],
        },
        evaluation_protocol=versioned_ref("evaluation-protocol:core"),
        predeclared_measures=("urn:test:measure:recall-at-40",),
        thresholds=(
            {
                "measure": "urn:test:measure:recall-at-40",
                "operator": "atLeast",
                "value": "0.90",
            },
        ),
        configured_strata=(
            {
                "stratum": "urn:test:stratum:all",
                "minimumSampleSize": 1,
                "observedSampleSize": 2,
                "passed": gate_passed,
            },
        ),
        exclusions=(),
        uncertainty_method=versioned_ref("uncertainty:bootstrap"),
        observed_measures=(
            {
                "measure": "urn:test:measure:recall-at-40",
                "value": "0.95",
                "uncertaintyLower": "0.90",
                "uncertaintyUpper": "1.00",
            },
        ),
        measure_populations=(
            {
                "measure": "urn:test:measure:reachable-candidate-recall",
                "populationKind": "reachableRegisteredCandidateRecall",
                "includedExpectations": ("urn:test:expectation:represented",),
                "excludedExpectations": ("urn:test:expectation:not-represented",),
            },
            {
                "measure": "urn:test:measure:target-availability",
                "populationKind": "targetAvailability",
                "includedExpectations": (
                    "urn:test:expectation:represented",
                    "urn:test:expectation:not-represented",
                ),
                "excludedExpectations": (),
            },
            {
                "measure": "urn:test:measure:open-set",
                "populationKind": "openSet",
                "includedExpectations": (
                    "urn:test:expectation:represented",
                    "urn:test:expectation:not-represented",
                ),
                "excludedExpectations": (),
            },
        ),
        gates=tuple(
            {
                "id": f"urn:test:gate:{dimension}",
                "dimension": dimension,
                "subject": f"urn:test:gate-subject:{dimension}",
                "passed": gate_passed,
                "reason": "The preregistered threshold was evaluated.",
            }
            for dimension in sorted(
                {
                    "stage",
                    "source",
                    "subtype",
                    "facet",
                    "role",
                    "predicate",
                    "privacy",
                    "risk",
                    "latency",
                    "cost",
                    "product",
                }
            )
        ),
        evaluator="urn:test:evaluator:independent",
        activity=ACTIVITY,
        evaluated_at=NOW,
        output_artifact_digests=(DIGEST_A,),
        verdict=verdict,
        configuration_record=config,
        gold_manifest_record=gold,
    )


def deployment_decision(
    config: EnrichmentConfiguration,
    evaluation: EnrichmentEvaluationResult,
) -> EnrichmentDeploymentDecision:
    return EnrichmentDeploymentDecision(
        decision_id="urn:test:deployment:production",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="effective",
        environment={
            "id": "urn:test:environment:production",
            "classification": "production",
        },
        configuration={
            "id": config.configuration_id,
            "digest": config.digest,
        },
        evaluation_result={
            "id": evaluation.result_id,
            "digest": evaluation.digest,
        },
        output_profile=dict(config.output_profile),
        selection_state="selected",
        effective_at=NOW,
        reason="All preregistered gates passed.",
        activity=ACTIVITY,
        predecessor_decision=None,
        superseding_decision=None,
        rulespec_attestation_refs=("urn:test:attestation:deployment",),
        local_adoption_refs=("urn:test:adoption:deployment",),
        authorization_validations=(
            {
                "authorizationRef": "urn:test:attestation:deployment",
                "kind": "rulespecAttestation",
                "validationReceipt": digest_ref("validation:attestation:deployment"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
            {
                "authorizationRef": "urn:test:adoption:deployment",
                "kind": "localAdoption",
                "validationReceipt": digest_ref("validation:adoption:deployment"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
        ),
    )


def test_configuration_digest_changes_with_behavior_pins() -> None:
    config = configuration()
    changed_prompt = replace(
        config,
        prompts=(component_pin("prompt", revision="2", digest=DIGEST_B),),
    )
    changed_budget = replace(
        config,
        budgets=(
            {
                **config.budgets[0],
                "candidates": 41,
            },
        ),
    )
    changed_index = replace(
        config,
        indexes=(
            {
                **config.indexes[0],
                "indexedExpressionCorpusDigest": DIGEST_B,
            },
        ),
    )
    assert (
        len(
            {
                config.digest,
                changed_prompt.digest,
                changed_budget.digest,
                changed_index.digest,
            }
        )
        == 4
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("indexes", ({},), "indexes\\[0\\]"),
        ("candidate_channels", ({"id": "urn:test:channel:empty"},), "candidateChannels"),
        ("models", ({},), "models\\[0\\]"),
        ("budgets", ({},), "budgets\\[0\\]"),
        ("determinism", ({},), "determinism\\[0\\]"),
        ("other_behavior_pins", ({},), "otherBehaviorPins\\[0\\]"),
    ],
)
def test_configuration_rejects_empty_or_partial_nested_records(
    field: str,
    value: tuple[Mapping[str, Any], ...],
    message: str,
) -> None:
    with pytest.raises(ReferenceRuntimeError, match=message):
        replace(configuration(), **{field: value}).payload()


def test_configuration_requires_candidate_universe_and_permission_pins() -> None:
    config = configuration()
    missing_universe = {
        key: value for key, value in config.vocabulary.items() if key != "candidateTargetUniverseDigest"
    }
    with pytest.raises(ReferenceRuntimeError, match="candidateTargetUniverseDigest"):
        replace(config, vocabulary=missing_universe).payload()

    missing_endpoint = {
        **config.vocabulary,
        "referenceResourceReleases": [
            value
            for value in config.vocabulary["referenceResourceReleases"]
            if value["id"] != "urn:test:release:mapping-target"
        ],
    }
    with pytest.raises(ReferenceRuntimeError, match="mapping endpoint"):
        replace(config, vocabulary=missing_endpoint).payload()

    missing_import = {
        **config.vocabulary,
        "registryImportSnapshots": [digest_ref("import:subjects")],
    }
    with pytest.raises(ReferenceRuntimeError, match="registry-import"):
        replace(config, vocabulary=missing_import).payload()


def test_configuration_validates_model_budget_and_determinism_types() -> None:
    config = configuration()
    model = {
        "id": "urn:test:model:one",
        "revision": "1",
        "providerConfiguration": component_pin("provider"),
        "endpointConfiguration": component_pin("endpoint"),
        "inferenceParameters": {"temperature": "0.0"},
        "structuredOutputSchema": versioned_ref("schema:model"),
    }
    replace(config, models=(model,)).payload()
    with pytest.raises(ReferenceRuntimeError, match="inferenceParameters"):
        replace(
            config,
            models=({**model, "inferenceParameters": {}},),
        ).payload()
    with pytest.raises(ReferenceRuntimeError, match="costMicrounits"):
        replace(
            config,
            budgets=({**config.budgets[0], "costMicrounits": False},),
        ).payload()
    with pytest.raises(ReferenceRuntimeError, match="status"):
        replace(
            config,
            determinism=({**config.determinism[0], "status": "bestEffort"},),
        ).payload()


def test_evaluation_and_deployment_require_exact_passing_configuration() -> None:
    config = configuration()
    evaluation = evaluation_result(config)
    evaluation.payload(configuration=config)
    deployment = deployment_decision(config, evaluation)
    deployment.payload(configuration=config, evaluation=evaluation)

    with pytest.raises(ReferenceRuntimeError, match="supplied exact"):
        deployment.payload()
    with pytest.raises(ReferenceRuntimeError, match="configuration digest"):
        replace(
            evaluation,
            configuration={
                "id": config.configuration_id,
                "digest": DIGEST_B,
            },
        ).payload(configuration=config)
    failed = evaluation_result(config, verdict="fail", gate_passed=False)
    with pytest.raises(ReferenceRuntimeError, match="passing evaluation"):
        deployment_decision(config, failed).payload(
            configuration=config,
            evaluation=failed,
        )


def test_passing_evaluation_enforces_thresholds_uncertainty_and_gate_dimensions() -> None:
    config = configuration()
    evaluation = evaluation_result(config)

    below_threshold = replace(
        evaluation,
        observed_measures=(
            {
                "measure": "urn:test:measure:recall-at-40",
                "value": "0.89",
                "uncertaintyLower": "0.80",
                "uncertaintyUpper": "0.95",
            },
        ),
    )
    with pytest.raises(ReferenceRuntimeError, match="misses thresholds"):
        below_threshold.payload()

    above_ceiling = replace(
        evaluation,
        thresholds=(
            {
                "measure": "urn:test:measure:recall-at-40",
                "operator": "atMost",
                "value": "0.90",
            },
        ),
    )
    with pytest.raises(ReferenceRuntimeError, match="misses thresholds"):
        above_ceiling.payload()

    invalid_uncertainty = replace(
        evaluation,
        observed_measures=(
            {
                "measure": "urn:test:measure:recall-at-40",
                "value": "0.95",
                "uncertaintyLower": "0.96",
                "uncertaintyUpper": "1.00",
            },
        ),
    )
    with pytest.raises(ReferenceRuntimeError, match="must contain"):
        invalid_uncertainty.payload()

    missing_gate_dimension = replace(
        evaluation,
        gates=tuple(gate for gate in evaluation.gates if gate["dimension"] != "product"),
    )
    with pytest.raises(ReferenceRuntimeError, match="every core gate dimension"):
        missing_gate_dimension.payload()


def test_evaluation_measure_declarations_thresholds_and_observations_are_one_to_one() -> None:
    evaluation = evaluation_result(configuration())
    second_observation = {
        "measure": "urn:test:measure:precision",
        "value": "0.98",
        "uncertaintyLower": "0.95",
        "uncertaintyUpper": "1.00",
    }

    with pytest.raises(ReferenceRuntimeError, match="exact one-to-one"):
        replace(
            evaluation,
            observed_measures=(
                *evaluation.observed_measures,
                second_observation,
            ),
        ).payload()

    with pytest.raises(ReferenceRuntimeError, match="exact one-to-one"):
        replace(
            evaluation,
            predeclared_measures=(
                *evaluation.predeclared_measures,
                second_observation["measure"],
            ),
            observed_measures=(
                *evaluation.observed_measures,
                second_observation,
            ),
        ).payload()

    duplicate_threshold = {
        "measure": "urn:test:measure:recall-at-40",
        "operator": "atMost",
        "value": "1.00",
    }
    with pytest.raises(ReferenceRuntimeError, match="more than one threshold"):
        replace(
            evaluation,
            thresholds=(
                *evaluation.thresholds,
                duplicate_threshold,
            ),
        ).payload()


def test_not_represented_expectations_stay_out_of_reachable_recall() -> None:
    evaluation = evaluation_result(configuration())
    invalid_population = {
        **evaluation.measure_populations[0],
        "includedExpectations": (
            "urn:test:expectation:represented",
            "urn:test:expectation:not-represented",
        ),
        "excludedExpectations": (),
    }
    with pytest.raises(
        ReferenceRuntimeError,
        match="excluded from reachable",
    ):
        replace(
            evaluation,
            measure_populations=(
                invalid_population,
                *evaluation.measure_populations[1:],
            ),
        ).payload()


def test_evaluation_rejects_gold_from_a_different_output_profile() -> None:
    config = configuration()
    evaluation = evaluation_result(config)
    assert evaluation.gold_manifest_record is not None
    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    changed_gold["vocabularyUniverse"]["outputProfile"]["digest"] = DIGEST_B
    changed_gold = seal_payload(changed_gold)
    changed_evaluation = replace(
        evaluation,
        sealed_gold_manifest={
            "id": changed_gold["id"],
            "digest": changed_gold["canonicalPayloadDigest"],
        },
        gold_manifest_record=changed_gold,
    )
    with pytest.raises(ReferenceRuntimeError, match="output-profile pins"):
        changed_evaluation.payload()


def test_evaluation_reconciles_every_exact_gold_vocabulary_pin() -> None:
    config = configuration()
    evaluation = evaluation_result(config)
    assert evaluation.gold_manifest_record is not None

    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    changed_gold["vocabularyUniverse"]["referenceResourceReleases"][0]["digest"] = DIGEST_B
    changed_gold = seal_payload(changed_gold)
    with pytest.raises(
        ReferenceRuntimeError,
        match="different referenceResourceReleases",
    ):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": changed_gold["id"],
                "digest": changed_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=changed_gold,
        ).payload()

    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    changed_gold["vocabularyUniverse"]["candidateTargetUniverseDigest"] = DIGEST_B
    changed_gold = seal_payload(changed_gold)
    with pytest.raises(
        ReferenceRuntimeError,
        match="different candidate target universes",
    ):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": changed_gold["id"],
                "digest": changed_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=changed_gold,
        ).payload()

    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    changed_gold["vocabularyUniverse"]["indexedExpressionCorpusDigests"] = [DIGEST_C]
    for item in changed_gold["items"]:
        item["partitionEvidence"]["vocabularyExpressionCorpusDigest"] = DIGEST_C
    changed_gold = seal_payload(changed_gold)
    with pytest.raises(
        ReferenceRuntimeError,
        match="different indexed-expression corpora",
    ):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": changed_gold["id"],
                "digest": changed_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=changed_gold,
        ).payload()


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("enrichment-profile", "enrichment-profile pins"),
        ("registry-imports", "registryImportSnapshots"),
        ("mapping-releases", "mappingReleases"),
        ("mapping-snapshots", "mappingSnapshots"),
        ("normalization-policy", "normalization policies"),
        ("gold-corpus", "omit the sealed gold corpus"),
    ],
)
def test_evaluation_rejects_each_remaining_gold_configuration_drift(
    case: str,
    message: str,
) -> None:
    evaluation = evaluation_result(configuration())
    assert evaluation.gold_manifest_record is not None
    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    universe = changed_gold["vocabularyUniverse"]
    if case == "enrichment-profile":
        universe["enrichmentProfile"]["digest"] = DIGEST_B
    elif case == "registry-imports":
        universe["registryImportSnapshots"][0]["digest"] = DIGEST_B
    elif case == "mapping-releases":
        universe["mappingReleases"][0]["digest"] = DIGEST_B
    elif case == "mapping-snapshots":
        universe["mappingSnapshots"][0]["digest"] = DIGEST_B
    elif case == "normalization-policy":
        universe["normalizationPolicy"]["digest"] = DIGEST_B
    elif case == "gold-corpus":
        changed_gold["corpusDigest"] = DIGEST_C
    else:  # pragma: no cover - parametrization is closed above
        raise AssertionError(case)
    changed_gold = seal_payload(changed_gold)
    with pytest.raises(ReferenceRuntimeError, match=message):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": changed_gold["id"],
                "digest": changed_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=changed_gold,
        ).payload()


def test_evaluation_rejects_unsealed_partition_keys_and_evidence() -> None:
    evaluation = evaluation_result(configuration())
    assert evaluation.gold_manifest_record is not None

    leaky_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    leaky_gold["items"][1]["partitionKeys"]["alias"] = list(leaky_gold["items"][0]["partitionKeys"]["alias"])
    leaky_gold = seal_payload(leaky_gold)
    with pytest.raises(ReferenceRuntimeError, match="leaks alias"):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": leaky_gold["id"],
                "digest": leaky_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=leaky_gold,
        ).payload()

    unpinned_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    unpinned_gold["items"][0]["partitionEvidence"]["exactMatchGraphDigest"] = "sha256:" + "d" * 64
    unpinned_gold = seal_payload(unpinned_gold)
    with pytest.raises(
        ReferenceRuntimeError,
        match="evidence digests are not pinned",
    ):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": unpinned_gold["id"],
                "digest": unpinned_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=unpinned_gold,
        ).payload()


def test_evaluation_rejects_partition_report_drift() -> None:
    evaluation = evaluation_result(configuration())
    assert evaluation.gold_manifest_record is not None
    changed_gold = json.loads(json.dumps(evaluation.gold_manifest_record))
    alias_dimension = next(row for row in changed_gold["partitionReport"]["dimensions"] if row["dimension"] == "alias")
    alias_dimension["itemKeys"][0]["values"] = ["Unsealed alias"]
    changed_gold = seal_payload(changed_gold)
    with pytest.raises(
        ReferenceRuntimeError,
        match="partitionReport alias keys differ",
    ):
        replace(
            evaluation,
            sealed_gold_manifest={
                "id": changed_gold["id"],
                "digest": changed_gold["canonicalPayloadDigest"],
            },
            gold_manifest_record=changed_gold,
        ).payload()


def reconciliation_report() -> RegistryReconciliationReport:
    first_release = versioned_ref("release:official-a")
    second_release = versioned_ref("release:official-b", digest=DIGEST_B)
    return RegistryReconciliationReport(
        report_id="urn:test:reconciliation:official",
        recorded_at=NOW,
        recorded_by=ACTOR,
        operational_state="resolved",
        inputs=(
            {
                "id": "urn:test:reconciliation-input:official-a",
                "referenceResourceRelease": first_release,
                "distributionArtifacts": (digest_ref("artifact:official-a"),),
                "registryImportSnapshot": digest_ref("import:official-a"),
                "stageDigests": (digest_ref("stage:official-a"),),
            },
            {
                "id": "urn:test:reconciliation-input:official-b",
                "referenceResourceRelease": second_release,
                "distributionArtifacts": (digest_ref("artifact:official-b"),),
                "registryImportSnapshot": digest_ref("import:official-b"),
                "stageDigests": (digest_ref("stage:official-b"),),
            },
        ),
        compared_items=(
            {
                "kind": "member",
                "left": "member-a",
                "right": "member-b",
            },
        ),
        differences=(
            {
                "id": "urn:test:difference:member",
                "kind": "member",
                "inputRefs": (
                    "urn:test:reconciliation-input:official-a",
                    "urn:test:reconciliation-input:official-b",
                ),
                "description": "The official publications differ.",
                "resolution": "selectedInput",
            },
        ),
        concept_mappings=(),
        precedence_policy=versioned_ref("precedence:official"),
        rulespec_authority_refs=("urn:test:authority:official",),
        attestation_refs=("urn:test:attestation:official",),
        local_adoption_refs=("urn:test:adoption:official",),
        authorization_validations=(
            {
                "authorizationRef": "urn:test:authority:official",
                "kind": "rulespecAuthority",
                "validationReceipt": digest_ref("validation:authority:official"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
            {
                "authorizationRef": "urn:test:attestation:official",
                "kind": "rulespecAttestation",
                "validationReceipt": digest_ref("validation:attestation:official"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
            {
                "authorizationRef": "urn:test:adoption:official",
                "kind": "localAdoption",
                "validationReceipt": digest_ref("validation:adoption:official"),
                "validator": "urn:test:validator:rulespec",
                "validatedAt": NOW,
                "effective": True,
            },
        ),
        unresolved_items=(),
        activity=ACTIVITY,
        outcome="selectedInput",
        selected_input_release=first_release,
    )


def test_unresolved_reconciliation_cannot_authorize_a_union() -> None:
    valid = reconciliation_report()
    valid.sealed_payload()
    unresolved_difference = {
        **valid.differences[0],
        "resolution": "unresolved",
    }
    unresolved = replace(
        valid,
        differences=(unresolved_difference,),
        unresolved_items=("urn:test:difference:member",),
        outcome="unresolved",
        selected_input_release=None,
    )
    payload = unresolved.sealed_payload()
    assert payload["synthesizedUnionAuthorized"] is False
    assert "selectedInputRelease" not in payload
    with pytest.raises(ReferenceRuntimeError, match="cannot authorize"):
        replace(
            unresolved,
            reconciled_release=versioned_ref("release:synthesized"),
        ).payload()


def test_reconciliation_differences_name_exact_input_identifiers() -> None:
    report = reconciliation_report()
    unrelated_refs = {
        **report.differences[0],
        "inputRefs": (
            "urn:test:unrelated:one",
            "urn:test:unrelated:two",
        ),
    }
    with pytest.raises(
        ReferenceRuntimeError,
        match="exact reconciliation input identifiers",
    ):
        replace(
            report,
            differences=(unrelated_refs,),
        ).payload()


def test_synthesized_reconciliation_requires_external_governance_validation() -> None:
    selected = reconciliation_report()
    reconciled = replace(
        selected,
        differences=(
            {
                **selected.differences[0],
                "resolution": "reconciled",
            },
        ),
        outcome="reconciledReleaseAuthorized",
        selected_input_release=None,
        reconciled_release=versioned_ref("release:reconciled"),
    )
    with pytest.raises(
        ReferenceRuntimeError,
        match="externally validated Rulespec governance",
    ):
        reconciled.payload()
    with pytest.raises(ReferenceRuntimeError, match="did not authorize"):
        reconciled.payload(governance_validator=lambda _request: False)

    requests: list[Mapping[str, Any]] = []

    def validate_governance(request: Mapping[str, Any]) -> bool:
        requests.append(request)
        return (
            request["attestationRefs"] == ["urn:test:attestation:official"]
            and request["localAdoptionRefs"] == ["urn:test:adoption:official"]
            and request["reconciledRelease"]["id"] == "urn:test:release:reconciled"
        )

    payload = reconciled.sealed_payload(governance_validator=validate_governance)
    assert payload["synthesizedUnionAuthorized"] is True
    assert requests


def test_reconciliation_requires_receipts_for_exact_governance_refs() -> None:
    report = reconciliation_report()
    with pytest.raises(
        ReferenceRuntimeError,
        match="exactly validate every",
    ):
        replace(
            report,
            rulespec_authority_refs=("urn:test:authority:other",),
        ).payload()


def test_ranked_candidates_require_exact_expression_lineage() -> None:
    catalog = {
        "concept-water": {
            "conceptIri": "urn:test:concept:water",
            "schemeIri": SCHEME,
            "facet": FACET,
            "referenceResourceRelease": RELEASE,
            "registryImportSnapshot": IMPORT,
            "indexSnapshot": "urn:test:index:subjects",
        }
    }
    bound = bind_ranked_candidates(
        ["concept-water"],
        channel="bm25s-lucene-e1",
        concept_catalog=catalog,
        expression_ids_by_concept={"concept-water": ("urn:ref:indexed-expression:abc",)},
    )
    assert bound[0]["rank"] == 1
    assert bound[0]["indexedExpressionIds"]
    with pytest.raises(ReferenceRuntimeError, match="no indexed expression"):
        bind_ranked_candidates(
            ["concept-water"],
            channel="bm25s-lucene-e1",
            concept_catalog=catalog,
            expression_ids_by_concept={},
        )


def _load_fusion_tool() -> Any:
    spec = importlib.util.spec_from_file_location(
        "fuse_concept_registries_runtime_test",
        FUSION_TOOL,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_existing_source_parser_output_is_preserved_only_in_quarantine() -> None:
    module = _load_fusion_tool()
    term = module.SourceTerm(
        pref_label="Water policy",
        alt_labels=["Water governance"],
        dropped_alt_labels=["水政策"],
        broader_labels=["Environment", "Infrastructure"],
    )
    batch = adapt_source_terms_for_migration(
        [term],
        term_identities=(
            {
                "conceptIri": "urn:authority:concept:water",
                "schemeIri": SCHEME,
                "prefLabelLanguage": "en",
                "altLabelLanguages": {
                    "Water governance": "en",
                    "水政策": "zh-Hant",
                },
                "broaderConcepts": (
                    {
                        "sourceLabel": "Environment",
                        "conceptIri": "urn:authority:concept:environment",
                        "schemeIri": SCHEME,
                    },
                    {
                        "sourceLabel": "Infrastructure",
                        "conceptIri": "urn:authority:concept:infrastructure",
                        "schemeIri": SCHEME,
                    },
                ),
            },
        ),
        release_iri=RELEASE,
        import_snapshot_id=IMPORT,
        distribution_artifact_id=DISTRIBUTION,
    )
    assert [row.original_literal for row in batch.labels] == [
        "Water policy",
        "Water governance",
        "水政策",
    ]
    assert len(batch.relations) == 2
    assert {row.concept_iri for row in batch.labels} == {"urn:authority:concept:water"}
    with pytest.raises(ReferenceRuntimeError, match="migration rows"):
        assert_conforming_vocabulary_rows(
            batch.labels,
            batch.relations,
            batch.participants,
        )


def test_legacy_flat_registry_is_readable_but_never_conforming() -> None:
    batch = migrate_legacy_concepts(
        [
            {
                "concept_id": "child",
                "pref_label": "Water",
                "alt_labels_json": '["Aguas"]',
                "broader_id": "parent",
                "replaced_by": None,
                "status": "active",
            },
            {
                "concept_id": "parent",
                "pref_label": "Environment",
                "alt_labels_json": "[]",
                "broader_id": None,
                "replaced_by": None,
                "status": "active",
            },
        ],
        release_iri=RELEASE,
        scheme_iri=SCHEME,
        import_snapshot_id=IMPORT,
        distribution_artifact_id=DISTRIBUTION,
        default_language="und",
    )
    assert batch.production_eligible is False
    assert len(batch.relations) == 1
    with pytest.raises(ReferenceRuntimeError, match="migration rows"):
        assert_conforming_vocabulary_rows(
            batch.labels,
            batch.relations,
            (),
        )
    with pytest.raises(ReferenceRuntimeError, match="legacy read-only"):
        reject_legacy_conforming_payload({"broader_id": "parent"})
    with pytest.raises(ReferenceRuntimeError, match="not production authority"):
        reject_legacy_conforming_payload({"registryAuthority": "fused-registry-v1"})


def partitioned_answers() -> dict[str, Any]:
    def artifact(split: str) -> dict[str, Any]:
        suffix = "train" if split == "train" else "holdout"
        digest = f"{suffix}-artifact"
        return {
            "split": split,
            "artifact_digest": digest,
            "expected_tags": [],
            "partitionKeys": {
                "conceptIdentity": [f"urn:test:concept:{suffix}"],
                "exactMatchCluster": [f"urn:test:exact-cluster:{suffix}"],
                "alias": [f"{suffix} label"],
                "sourceIdentity": [f"urn:test:source:{suffix}"],
                "artifactDigest": [digest],
                "textDigest": [f"{suffix}-text"],
                "nearDuplicateCluster": [f"urn:test:near-duplicate:{suffix}"],
            },
        }

    return {"artifacts": [artifact("train"), artifact("holdout")]}


@pytest.mark.parametrize(
    "dimension",
    [
        "conceptIdentity",
        "exactMatchCluster",
        "alias",
        "sourceIdentity",
        "artifactDigest",
        "textDigest",
        "nearDuplicateCluster",
    ],
)
def test_complete_partition_rejects_every_leakage_dimension(
    dimension: str,
) -> None:
    answers = partitioned_answers()
    train = answers["artifacts"][0]["partitionKeys"][dimension]
    answers["artifacts"][1]["partitionKeys"][dimension] = list(train)
    if dimension == "artifactDigest":
        answers["artifacts"][1]["artifact_digest"] = train[0]
    with pytest.raises(EvaluationBoundaryError, match="shared_"):
        partition_leakage_facts(
            answers,
            [],
            require_complete=True,
        )


def test_adoption_partition_rejects_a_missing_dimension() -> None:
    answers = partitioned_answers()
    answers["artifacts"][1]["partitionKeys"].pop("nearDuplicateCluster")
    with pytest.raises(EvaluationBoundaryError, match="missing dimensions"):
        partition_leakage_facts(
            answers,
            [],
            require_complete=True,
        )


def vocabulary_freeze() -> dict[str, Any]:
    return VocabularyUniverseFreeze(
        freeze_id="urn:test:vocabulary-freeze:one",
        registry_releases=(
            {
                "release": RELEASE,
                "releaseDigest": DIGEST_A,
                "importSnapshot": IMPORT,
                "coverageReport": "urn:test:coverage:subjects",
                "coverageReportDigest": DIGEST_A,
            },
        ),
        mapping_releases=(
            {
                "release": "urn:test:release:mappings",
                "releaseDigest": DIGEST_B,
                "mappingSnapshot": "urn:test:import:mappings",
                "coverageReport": "urn:test:coverage:mappings",
                "coverageReportDigest": DIGEST_B,
            },
        ),
        output_profile={
            "id": output_profile().profile_id,
            "version": output_profile().version,
            "digest": output_profile().content_digest,
        },
        frozen_at=NOW,
        frozen_by=ACTIVITY,
    ).sealed_payload()


def test_vocabulary_freeze_pins_registry_mapping_and_output_profile() -> None:
    frozen = vocabulary_freeze()
    require_vocabulary_universe_freeze(frozen)
    changed = json.loads(json.dumps(frozen))
    changed["registryReleases"][0]["releaseDigest"] = DIGEST_C
    with pytest.raises(ReferenceRuntimeError, match="mismatch"):
        require_vocabulary_universe_freeze(changed)


def runtime_records() -> dict[str, dict[str, Any]]:
    profile = output_profile()
    profile_payload = profile.sealed_payload()
    coverage_payload = coverage_report().sealed_payload()
    registry_deployment_payload = registry_deployment_decision(
        coverage=coverage_payload,
        profile=profile_payload,
    ).sealed_payload(
        coverage_report_record=coverage_payload,
        output_profile_record=profile_payload,
    )
    config = configuration()
    evaluation = evaluation_result(config)
    deployment = deployment_decision(config, evaluation)
    return {
        "enrichment-profile.schema.json": enrichment_profile().sealed_payload(),
        "output-profile.schema.json": profile_payload,
        "registry-import-coverage-report.schema.json": coverage_payload,
        "indexed-vocabulary-expression.schema.json": expression().sealed_payload(),
        "registry-reconciliation-report.schema.json": reconciliation_report().sealed_payload(),
        "registry-deployment-decision.schema.json": registry_deployment_payload,
        "enrichment-configuration.schema.json": config.sealed_payload(),
        "enrichment-evaluation-result.schema.json": evaluation.sealed_payload(configuration=config),
        "enrichment-deployment-decision.schema.json": deployment.sealed_payload(
            configuration=config,
            evaluation=evaluation,
        ),
    }


def schema_registry() -> tuple[dict[str, dict[str, Any]], Registry]:
    assert REFSPEC_SCHEMA_ROOT.is_dir(), "RefSpec JSON Binding schemas are required for runtime conformance tests"
    schemas: dict[str, dict[str, Any]] = {}
    registry = Registry()
    for path in sorted(REFSPEC_SCHEMA_ROOT.glob("*.schema.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        schemas[path.name] = schema
        registry = registry.with_resource(
            schema["$id"],
            Resource.from_contents(schema),
        )
    return schemas, registry


def validate_runtime_record_bundle(
    records: Sequence[Mapping[str, Any]],
) -> None:
    schemas, registry = schema_registry()
    schema_by_type = {
        schema.get("properties", {}).get("type", {}).get("const"): schema
        for schema in schemas.values()
        if schema.get("properties", {}).get("type", {}).get("const")
    }
    errors: list[str] = []
    for record in records:
        schema = schema_by_type.get(record.get("type"))
        if schema is None:
            errors.append(f"unsupported type {record.get('type')!r}")
            continue
        validator = Draft202012Validator(
            schema,
            registry=registry,
            format_checker=FormatChecker(),
        )
        errors.extend(error.message for error in validator.iter_errors(record))
    if errors:
        raise ReferenceRuntimeError("binding schema invalid: " + "; ".join(errors))


def test_record_store_requires_matching_type_and_binding_validation(
    tmp_path: Path,
) -> None:
    store = ReferenceRuntimeStore(tmp_path)
    payload = configuration().sealed_payload()

    with pytest.raises(ReferenceRuntimeError, match="explicit REF JSON"):
        store.put_record("enrichment-configuration", payload)
    with pytest.raises(ReferenceRuntimeError, match="requires payload type"):
        store.put_record(
            "output-profile",
            payload,
            binding_validator=validate_runtime_record_bundle,
        )

    invalid = dict(payload)
    invalid.pop("schemas")
    invalid = seal_payload(invalid)
    with pytest.raises(ReferenceRuntimeError, match="binding schema invalid"):
        store.put_record(
            "enrichment-configuration",
            invalid,
            binding_validator=validate_runtime_record_bundle,
        )

    path = store.put_record(
        "enrichment-configuration",
        payload,
        binding_validator=validate_runtime_record_bundle,
    )
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == payload


def test_record_store_accepts_registry_deployment_decisions(
    tmp_path: Path,
) -> None:
    records = runtime_records()
    payload = records["registry-deployment-decision.schema.json"]
    linked = tuple(
        records[name]
        for name in (
            "output-profile.schema.json",
            "registry-import-coverage-report.schema.json",
        )
    )
    path = ReferenceRuntimeStore(tmp_path).put_record(
        "registry-deployment-decision",
        payload,
        binding_validator=validate_runtime_record_bundle,
        linked_records=linked,
    )
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == payload
    with pytest.raises(ReferenceRuntimeError, match="requires payload type"):
        ReferenceRuntimeStore(tmp_path).put_record(
            "registry-deployment-decision",
            configuration().sealed_payload(),
            binding_validator=validate_runtime_record_bundle,
        )


def test_every_runtime_record_validates_against_ref_json_binding() -> None:
    schemas, registry = schema_registry()
    errors: list[str] = []
    for schema_name, record in runtime_records().items():
        validator = Draft202012Validator(
            schemas[schema_name],
            registry=registry,
            format_checker=FormatChecker(),
        )
        for error in validator.iter_errors(record):
            location = ".".join(str(item) for item in error.absolute_path)
            errors.append(f"{schema_name}:{location or '$'}: {error.message}")
        require_payload_digest(record)
    assert not errors, "\n".join(errors)


def test_canonical_payload_rules_match_binding_boundaries() -> None:
    record = seal_payload(
        {
            "id": "urn:test:record:canonical",
            "type": "urn:test:type:Canonical",
            "label": "水政策",
            "count": 1,
        }
    )
    assert canonical_payload_digest(record) == record["canonicalPayloadDigest"]
    with pytest.raises(ReferenceRuntimeError, match="null"):
        canonical_payload_digest({"value": None})
    with pytest.raises(ReferenceRuntimeError, match="JSON float"):
        canonical_payload_digest({"value": 0.5})

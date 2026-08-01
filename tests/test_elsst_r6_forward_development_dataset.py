"""Checks for the source-derived, development-only ELSST R6 dataset."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS, OWL, RDF, SKOS

from refspec import EnrichmentProfile, OutputProfile, ReferenceRuntimeError
from refspec.release_graph import load_pinned_rulespec_validator, validate_rulespec_graph
from spicy_regs.enrichment.experiment_artifacts import write_experiment_artifacts
from spicy_regs.enrichment.open_set import (
    DevelopmentOpenLabelError,
    materialize_development_open_label,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RULESPEC_ROOT = REPO_ROOT.parent / "rulespec"
EVIDENCE_ROOT = REPO_ROOT / "docs" / "evidence" / "elsst-r6-forward-development-dataset-2026-07-29"
MANIFEST = json.loads((EVIDENCE_ROOT / "dataset-manifest.json").read_text(encoding="utf-8"))
SOURCE_ARTIFACTS = json.loads((EVIDENCE_ROOT / "source-artifacts.json").read_text(encoding="utf-8"))["sourceArtifacts"]
ROWS = json.loads((EVIDENCE_ROOT / "rows.json").read_text(encoding="utf-8"))["rows"]

SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
OPEN_LABEL_FACET = "urn:ref:facet:general-subject"
OPEN_LABEL_ROLE = "rkaf:assignmentPrimary"
OPEN_LABEL_ACTIVITY = "urn:spicy-regs:activity:elsst-r6-forward-development:open-set"
OPEN_LABEL_RUN = "urn:spicy-regs:run:elsst-r6-forward-development:open-set"
OPEN_LABEL_EXTRACTOR = "urn:spicy-regs:extractor:development-open-set"
OPEN_LABEL_EXTRACTOR_VERSION = "1"
OPEN_LABEL_ASSERTED_AT = "2026-07-29T00:00:00Z"
OPEN_SET_ROW_IDS = frozenset(
    {
        "missing-clean-fuel-credit",
        "missing-critical-habitat",
        "missing-hazard-communication",
        "missing-qualified-cash-arrangement",
    }
)
TEST_DIGEST = "sha256:" + "a" * 64


def _sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _english_values(graph: Graph, subject: URIRef, predicate: URIRef) -> set[str]:
    return {
        str(value)
        for value in graph.objects(subject, predicate)
        if isinstance(value, Literal) and value.language == "en"
    }


def _asset_paths() -> dict[str, Path]:
    source_root = REPO_ROOT / MANIFEST["inputs"]["sourceCorpus"]["root"]
    segmentation = MANIFEST["inputs"]["segmentation"]
    return {
        "source_root": source_root,
        "membership": source_root / "evaluation_membership.parquet",
        "provenance": source_root / "source_provenance.parquet",
        "segments": (REPO_ROOT / segmentation["root"] / segmentation["segmentsFile"]),
        "vocabulary": (REPO_ROOT / MANIFEST["inputs"]["vocabulary"]["localContentAddressedPath"]),
    }


def _versioned_ref(identifier: str) -> dict[str, str]:
    return {
        "id": identifier,
        "version": "1",
        "digest": TEST_DIGEST,
    }


def _candidate_only_open_label_profile() -> OutputProfile:
    enrichment = EnrichmentProfile(
        profile_id="urn:spicy-regs:enrichment-profile:elsst-r6-forward-development",
        version="1",
        recorded_at=OPEN_LABEL_ASSERTED_AT,
        recorded_by="urn:spicy-regs:actor:development-test",
        operational_state="immutable",
        facets=(
            {
                "iri": OPEN_LABEL_FACET,
                "label": "General subject",
                "definition": "A source-grounded subject phrase used in development lookup.",
                "inclusionCues": ["Substantive subject wording in the source"],
                "exclusionCues": ["A nearby controlled concept accepted only by lexical similarity"],
                "compatibleResourceRoutes": ["document"],
                "compatibleAssignmentPredicates": [OPEN_LABEL_ROLE],
            },
        ),
    )
    return OutputProfile(
        profile_id="urn:spicy-regs:output-profile:elsst-r6-forward-development",
        version="1",
        recorded_at=OPEN_LABEL_ASSERTED_AT,
        recorded_by="urn:spicy-regs:actor:development-test",
        operational_state="immutable",
        enrichment_profile=dict(enrichment.reference),
        acceptance_policies=(_versioned_ref("urn:spicy-regs:policy:development-open-set"),),
        publication_views=(_versioned_ref("urn:spicy-regs:view:development-only"),),
        open_label_permissions=(
            {
                "facet": OPEN_LABEL_FACET,
                "assignmentRole": OPEN_LABEL_ROLE,
                "mode": "explicitLanguage",
                "candidateUse": True,
                "acceptedOutputUse": False,
            },
        ),
        enrichment_profile_record=enrichment,
    )


def _source_text(
    pinned_assets: dict[str, Path],
    artifact: dict,
) -> str:
    source_field_name = artifact["sourceField"].split(".", 1)[1]
    source_rows = pq.read_table(
        pinned_assets["source_root"] / f"{artifact['sourceTable']}.parquet",
        columns=[artifact["keyField"], source_field_name],
    ).to_pylist()
    matches = [row[source_field_name] for row in source_rows if row[artifact["keyField"]] == artifact["subjectId"]]
    assert len(matches) == 1, artifact["id"]
    assert isinstance(matches[0], str)
    return matches[0]


@pytest.fixture(scope="module")
def pinned_assets() -> dict[str, Path]:
    paths = _asset_paths()
    source_root = paths["source_root"]
    required = [
        paths["membership"],
        paths["provenance"],
        paths["segments"],
        paths["vocabulary"],
        *{source_root / f"{artifact['sourceTable']}.parquet" for artifact in SOURCE_ARTIFACTS},
    ]
    missing = sorted(str(path.relative_to(REPO_ROOT)) for path in required if not path.is_file())
    if missing:
        pytest.skip(
            f"The forward-development verification needs the pinned local source snapshots: {', '.join(missing)}"
        )
    return paths


@pytest.fixture(scope="module")
def elsst_graph(pinned_assets: dict[str, Path]) -> Graph:
    vocabulary_path = pinned_assets["vocabulary"]
    vocabulary = MANIFEST["inputs"]["vocabulary"]
    assert vocabulary_path.stat().st_size == vocabulary["byteLength"]
    assert _sha256_file(vocabulary_path) == vocabulary["distributionDigest"]
    return Graph().parse(vocabulary_path, format="turtle")


def test_manifest_keeps_all_rows_development_only_and_unsealed() -> None:
    assert MANIFEST["datasetId"] == "elsst-r6-forward-development-v1-proposal"
    assert MANIFEST["partition"] == "developmentOnly"
    assert MANIFEST["reviewStatus"] == "proposedUnsealed"
    assert MANIFEST["rowCount"] == len(ROWS) == 19
    assert MANIFEST["derivation"]["method"] == "directSourceAndPinnedVocabulary"
    assert MANIFEST["derivation"]["usesFusedRegistry"] is False
    assert MANIFEST["derivation"]["usesPriorRegistryJudgments"] is False
    assert MANIFEST["rightsHandling"]["rightsNotesRestrictDatasetUse"] is False

    row_ids = [row["rowId"] for row in ROWS]
    source_ids = [artifact["id"] for artifact in SOURCE_ARTIFACTS]
    assert len(row_ids) == len(set(row_ids))
    assert len(source_ids) == len(set(source_ids))
    assert len(SOURCE_ARTIFACTS) == 13
    assert sum(row["expectedOutcome"] == "target" for row in ROWS) == 15
    assert sum(row["expectedOutcome"] == "notRepresented" for row in ROWS) == 4

    source_id_set = set(source_ids)
    required_routes = set(MANIFEST["requiredRoutesForNotRepresented"])
    for artifact in SOURCE_ARTIFACTS:
        assert artifact["id"] in source_id_set
        assert SHA256_RE.fullmatch(artifact["artifactDigest"])
        assert SHA256_RE.fullmatch(artifact["sourceFieldDigest"])
        assert SHA256_RE.fullmatch(artifact["nativeDistributionDigest"])
        assert artifact["rightsNote"]
        assert artifact["rightsNoteRestrictsUse"] is False

    for row in ROWS:
        assert row["partition"] == "developmentOnly"
        assert row["reviewStatus"] == "proposedUnsealed"
        assert row["sourceArtifactId"] in source_id_set

        mention = row["mention"]
        evidence = row["evidence"]
        segment = row["segment"]
        assert mention["language"] == "en"
        assert mention["startChar"] < mention["endChar"]
        assert evidence["startChar"] <= mention["startChar"]
        assert mention["endChar"] <= evidence["endChar"]
        assert mention["text"] in evidence["text"]
        assert _sha256_text(mention["text"]) == mention["digest"]
        assert _sha256_text(evidence["text"]) == evidence["digest"]
        assert segment["configId"] == MANIFEST["inputs"]["segmentation"]["configId"]
        assert segment["sourceSliceStartChar"] <= mention["startChar"]
        assert mention["endChar"] <= segment["sourceSliceEndChar"]

        if row["expectedOutcome"] == "target":
            target = row["target"]
            assert target["active"] is True
            assert target["matchedExpression"]["language"] == "en"
            assert target["matchedExpression"]["value"] == mention["text"].upper()
            assert target["releaseConceptIri"] != target["stableConceptIri"]
        else:
            missing = row["notRepresented"]
            assert set(missing["requiredRoutes"]) == required_routes
            assert missing["includedInReachableCandidateRecallDenominator"] is False
            assert missing["nearConceptsNotAccepted"]


def test_real_source_fields_and_segments_match_every_row(
    pinned_assets: dict[str, Path],
) -> None:
    source_root = pinned_assets["source_root"]
    artifact_by_id = {artifact["id"]: artifact for artifact in SOURCE_ARTIFACTS}

    memberships = pq.read_table(pinned_assets["membership"]).to_pylist()
    membership_by_subject = {(row["source_table"], row["subject_id"]): row for row in memberships}
    provenance = pq.read_table(pinned_assets["provenance"]).to_pylist()
    provenance_by_source = {(row["source_table"], row["native_id"], row["target_field"]): row for row in provenance}
    segment_rows = pq.read_table(pinned_assets["segments"]).to_pylist()
    segments_by_id = {
        row["segment_id"]: row
        for row in segment_rows
        if row["config_id"] == MANIFEST["inputs"]["segmentation"]["configId"]
    }

    source_text_by_artifact: dict[str, str] = {}
    for artifact in SOURCE_ARTIFACTS:
        source_field_name = artifact["sourceField"].split(".", 1)[1]
        source_rows = pq.read_table(
            source_root / f"{artifact['sourceTable']}.parquet",
            columns=[artifact["keyField"], source_field_name],
        ).to_pylist()
        matches = [row for row in source_rows if row[artifact["keyField"]] == artifact["subjectId"]]
        assert len(matches) == 1, artifact["id"]
        source_text = matches[0][source_field_name]
        assert isinstance(source_text, str)
        assert _sha256_text(source_text) == artifact["sourceFieldDigest"]
        source_text_by_artifact[artifact["id"]] = source_text

        membership = membership_by_subject[(artifact["sourceTable"], artifact["subjectId"])]
        assert membership["subject_type"] == artifact["subjectType"]
        assert "sha256:" + membership["artifact_digest"] == artifact["artifactDigest"]

        source_provenance = provenance_by_source[
            (
                artifact["sourceTable"],
                artifact["subjectId"],
                source_field_name,
            )
        ]
        assert source_provenance["case_id"] == artifact["caseId"]
        assert source_provenance["source_url"] == artifact["sourceUrl"]
        assert source_provenance["retrieved_on"] == artifact["retrievedOn"]
        assert source_provenance["media_type"] == artifact["mediaType"]
        assert source_provenance["representation"] == artifact["representation"]
        assert "sha256:" + source_provenance["extracted_sha256"] == artifact["sourceFieldDigest"]
        assert "sha256:" + source_provenance["source_sha256"] == artifact["nativeDistributionDigest"]
        assert source_provenance["extraction_method"] == artifact["extractionMethod"]
        assert source_provenance["extraction_version"] == artifact["extractionVersion"]
        assert source_provenance["rights_note"] == artifact["rightsNote"]

    for row in ROWS:
        artifact = artifact_by_id[row["sourceArtifactId"]]
        source_text = source_text_by_artifact[artifact["id"]]
        mention = row["mention"]
        evidence = row["evidence"]
        segment = row["segment"]

        assert source_text[mention["startChar"] : mention["endChar"]] == mention["text"], row["rowId"]
        assert source_text[evidence["startChar"] : evidence["endChar"]] == evidence["text"], row["rowId"]

        source_segment = segments_by_id[segment["segmentId"]]
        assert source_segment["source_table"] == artifact["sourceTable"]
        assert source_segment["subject_type"] == artifact["subjectType"]
        assert source_segment["subject_id"] == artifact["subjectId"]
        assert source_segment["artifact_digest"] == artifact["artifactDigest"].removeprefix("sha256:")
        assert int(source_segment["ordinal"]) == segment["ordinal"]

        containing_slices = [
            source_slice
            for source_slice in json.loads(source_segment["slices_json"])
            if (
                source_slice["source_field"] == artifact["sourceField"]
                and int(source_slice["start_char"]) <= mention["startChar"]
                and mention["endChar"] <= int(source_slice["end_char"])
            )
        ]
        assert len(containing_slices) == 1, row["rowId"]
        source_slice = containing_slices[0]
        assert int(source_slice["start_char"]) == segment["sourceSliceStartChar"]
        assert int(source_slice["end_char"]) == segment["sourceSliceEndChar"]
        assert "sha256:" + source_slice["source_sha256"] == artifact["sourceFieldDigest"]
        assert source_slice["text"] == source_text[segment["sourceSliceStartChar"] : segment["sourceSliceEndChar"]]


def test_elsst_r6_targets_aliases_identities_and_hierarchy(
    elsst_graph: Graph,
) -> None:
    for row in ROWS:
        if row["expectedOutcome"] != "target":
            continue

        target = row["target"]
        target_iri = URIRef(target["releaseConceptIri"])
        stable_iri = URIRef(target["stableConceptIri"])
        expression = target["matchedExpression"]
        expression_predicate = URIRef(expression["predicate"])

        assert (target_iri, RDF.type, SKOS.Concept) in elsst_graph
        assert target["prefLabel"]["en"] in _english_values(
            elsst_graph,
            target_iri,
            SKOS.prefLabel,
        )
        assert expression["value"] in _english_values(
            elsst_graph,
            target_iri,
            expression_predicate,
        )
        assert (target_iri, DCTERMS.isVersionOf, stable_iri) in elsst_graph
        assert not any(
            value.toPython() is True
            for value in elsst_graph.objects(target_iri, OWL.deprecated)
            if isinstance(value, Literal)
        )

        expected_parents = {parent["releaseConceptIri"] for parent in target["directBroader"]}
        actual_parents = {str(parent) for parent in elsst_graph.objects(target_iri, SKOS.broader)}
        assert actual_parents == expected_parents, row["rowId"]
        for parent in target["directBroader"]:
            parent_iri = URIRef(parent["releaseConceptIri"])
            assert parent["prefLabel"]["en"] in _english_values(
                elsst_graph,
                parent_iri,
                SKOS.prefLabel,
            )
            assert (
                parent_iri,
                DCTERMS.isVersionOf,
                URIRef(parent["stableConceptIri"]),
            ) in elsst_graph


def test_lifecycle_rows_forbid_deprecated_predecessors(
    elsst_graph: Graph,
) -> None:
    lifecycle_rows = [row for row in ROWS if "lifecycle" in row]
    accepted_targets = {row["target"]["releaseConceptIri"] for row in ROWS if row["expectedOutcome"] == "target"}
    assert len(lifecycle_rows) == 2

    for row in lifecycle_rows:
        lifecycle = row["lifecycle"]
        predecessor = lifecycle["forbiddenPredecessor"]
        predecessor_iri = URIRef(predecessor["releaseConceptIri"])
        target_iri = URIRef(row["target"]["releaseConceptIri"])

        assert lifecycle["operation"] == "replacement"
        assert lifecycle["requiredSuccessor"] == str(target_iri)
        assert predecessor_iri != target_iri
        assert str(predecessor_iri) not in accepted_targets
        assert predecessor["deprecated"] is True
        assert predecessor["prefLabel"]["en"] in _english_values(
            elsst_graph,
            predecessor_iri,
            SKOS.prefLabel,
        )
        assert (
            predecessor_iri,
            DCTERMS.isVersionOf,
            URIRef(predecessor["stableConceptIri"]),
        ) in elsst_graph
        assert any(
            value.toPython() is True
            for value in elsst_graph.objects(predecessor_iri, OWL.deprecated)
            if isinstance(value, Literal)
        )
        assert (
            predecessor_iri,
            URIRef(lifecycle["replacementPredicate"]),
            target_iri,
        ) in elsst_graph


def test_not_represented_rows_are_absent_and_keep_safe_routes(
    elsst_graph: Graph,
) -> None:
    missing_rows = [row for row in ROWS if row["expectedOutcome"] == "notRepresented"]
    assert len(missing_rows) == 4
    required_routes = set(MANIFEST["requiredRoutesForNotRepresented"])

    for row in missing_rows:
        missing = row["notRepresented"]
        phrase = row["mention"]["text"].casefold()
        predicates = [URIRef(value) for value in missing["verifiedAbsentPredicates"]]
        assert set(missing["requiredRoutes"]) == required_routes
        assert missing["includedInReachableCandidateRecallDenominator"] is False

        for predicate in predicates:
            english_values = (
                str(value).casefold()
                for _, value in elsst_graph.subject_objects(predicate)
                if isinstance(value, Literal) and value.language == "en"
            )
            assert phrase not in english_values, (row["rowId"], str(predicate))

        for near_concept in missing["nearConceptsNotAccepted"]:
            near_iri = URIRef(near_concept["releaseConceptIri"])
            assert near_concept["prefLabel"]["en"] in _english_values(
                elsst_graph,
                near_iri,
                SKOS.prefLabel,
            )
            assert (
                near_iri,
                DCTERMS.isVersionOf,
                URIRef(near_concept["stableConceptIri"]),
            ) in elsst_graph


@pytest.mark.legacy_rulespec_combined
def test_real_not_represented_rows_materialize_grounded_candidate_open_labels(
    pinned_assets: dict[str, Path],
    tmp_path: Path,
) -> None:
    missing_rows = [row for row in ROWS if row["expectedOutcome"] == "notRepresented"]
    represented_rows = [row for row in ROWS if row["expectedOutcome"] == "target"]
    assert {row["rowId"] for row in missing_rows} == OPEN_SET_ROW_IDS
    artifact_by_id = {artifact["id"]: artifact for artifact in SOURCE_ARTIFACTS}
    profile = _candidate_only_open_label_profile()

    materialized: list[dict] = []
    for row in missing_rows:
        artifact = artifact_by_id[row["sourceArtifactId"]]
        graph = materialize_development_open_label(
            dataset_id=MANIFEST["datasetId"],
            row=row,
            source_artifact=artifact,
            source_text=_source_text(pinned_assets, artifact),
            output_profile=profile,
            facet=OPEN_LABEL_FACET,
            assignment_role=OPEN_LABEL_ROLE,
            resource_route="document",
            extraction_activity_iri=OPEN_LABEL_ACTIVITY,
            extraction_run_iri=OPEN_LABEL_RUN,
            extractor_iri=OPEN_LABEL_EXTRACTOR,
            extractor_version=OPEN_LABEL_EXTRACTOR_VERSION,
            asserted_at=OPEN_LABEL_ASSERTED_AT,
        )
        materialized.append(dict(graph))

        assertion = graph["assertion"]
        evidence = graph["evidenceBinding"]
        activity = graph["extractionActivity"]
        source = graph["sourceArtifact"]
        fragment = graph["sourceFragment"]
        position = graph["positionSelector"]
        quote = graph["quoteSelector"]
        grounding = graph["sourceGrounding"]
        assert assertion["rkaf:assertsPredicate"] == "rkaf:openLabel"
        assert assertion["rkaf:assertsValue"] == {
            "@value": row["mention"]["text"],
            "@language": row["mention"]["language"],
        }
        assert assertion["rkaf:openLabelFacet"] == OPEN_LABEL_FACET
        assert assertion["rkaf:openLabelRole"] == OPEN_LABEL_ROLE
        assert assertion["rkaf:hasExtractionProvenance"] == (OPEN_LABEL_ACTIVITY)
        assert assertion["rkaf:usageEligibility"] == "rkaf:searchOnly"
        assert evidence["rkaf:bindsAssertion"] == assertion["@id"]
        assert evidence["rkaf:bindsSourceFragment"] == [fragment["@id"]]
        assert activity == {
            "@id": OPEN_LABEL_ACTIVITY,
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:deterministicParse",
            "rkaf:extractionRun": OPEN_LABEL_RUN,
            "rkaf:extractedBy": OPEN_LABEL_EXTRACTOR,
            "rkaf:extractorVersion": OPEN_LABEL_EXTRACTOR_VERSION,
        }
        assert source == {
            "@id": assertion["rkaf:assertsSubject"],
            "@type": "rkaf:Artifact",
            "rkaf:hasArtifactIdentifier": [artifact["sourceUrl"]],
            "rkaf:artifactIdentifierScheme": ["rkaf:partner-defined"],
            "rkaf:hasContentDigest": artifact["artifactDigest"],
        }
        assert fragment["@type"] == "rkaf:SourceFragment"
        assert fragment["oa:hasSource"] == source["@id"]
        assert fragment["oa:hasSelector"] == [position["@id"], quote["@id"]]
        assert fragment["rkaf:selectorKind"] == [
            "oa:TextPositionSelector",
            "oa:TextQuoteSelector",
        ]
        assert fragment["rkaf:fragmentIdentityScheme"] == "rkaf:published-fragment"
        assert fragment["rkaf:sourceArtifactDigest"] == artifact["artifactDigest"]
        assert fragment["rkaf:fragmentContentDigest"] == row["evidence"]["digest"]
        assert position == {
            "@id": position["@id"],
            "@type": "oa:TextPositionSelector",
            "oa:start": row["evidence"]["startChar"],
            "oa:end": row["evidence"]["endChar"],
            "rkaf:coordinateSystem": "rkaf:unicode-codepoint",
        }
        assert quote == {
            "@id": quote["@id"],
            "@type": "oa:TextQuoteSelector",
            "oa:exact": row["evidence"]["text"],
        }
        assert grounding["artifactDigest"] == artifact["artifactDigest"]
        assert grounding["nativeDistributionDigest"] == artifact["nativeDistributionDigest"]
        assert grounding["sourceField"] == artifact["sourceField"]
        assert grounding["sourceFieldDigest"] == artifact["sourceFieldDigest"]
        assert grounding["mentionDigest"] == row["mention"]["digest"]
        assert grounding["evidenceDigest"] == row["evidence"]["digest"]
        assert grounding["partition"] == "developmentOnly"
        assert grounding["reviewStatus"] == "proposedUnsealed"

    validator = load_pinned_rulespec_validator(RULESPEC_ROOT)
    context_document = json.loads(
        (validator.working_directory / "context" / "rkaf-context.jsonld").read_text(
            encoding="utf-8"
        )
    )
    portable_node_keys = (
        "assertion",
        "evidenceBinding",
        "extractionActivity",
        "sourceArtifact",
        "sourceFragment",
        "positionSelector",
        "quoteSelector",
    )
    nodes_by_id: dict[str, dict] = {}
    for materialized_graph in materialized:
        assert "sourceGrounding" not in portable_node_keys
        for key in portable_node_keys:
            node = materialized_graph[key]
            existing = nodes_by_id.setdefault(node["@id"], node)
            assert existing == node
    rulespec_graph = {
        "@context": context_document["@context"],
        "@graph": list(nodes_by_id.values()),
    }
    assert validate_rulespec_graph(rulespec_graph, validator=validator) == ()

    assertion_ids = {graph["assertion"]["@id"] for graph in materialized}
    assert len(materialized) == len(assertion_ids) == len(OPEN_SET_ROW_IDS)
    represented_ids = {f"urn:spicy-regs:expectation:{row['rowId']}" for row in represented_rows}
    missing_ids = {f"urn:spicy-regs:expectation:{row['rowId']}" for row in missing_rows}
    result = {
        "schema_version": "elsst-r6-real-open-set-v1",
        "generated_at": OPEN_LABEL_ASSERTED_AT,
        "inputs": {
            "dataset_dir": str(EVIDENCE_ROOT),
            "targets_file": str(EVIDENCE_ROOT / "rows.json"),
            "targets_sha256": _sha256_file(EVIDENCE_ROOT / "rows.json").removeprefix("sha256:"),
            "target_dataset_id": MANIFEST["datasetId"],
        },
        "settings": {
            "open_label_materializer_version": "refspec-candidate-v1",
        },
        "evaluation_boundary": {
            "dataset_id": MANIFEST["datasetId"],
            "eligible": False,
        },
        "timings_seconds": {},
        "reviewed_target_binding": {
            "reviewStatus": MANIFEST["reviewStatus"],
        },
        "results": [
            {
                "configuration": "source-grounded-open-label",
                "channels": ["openLabel"],
                "quotas": False,
                "note": "Development-only open-set route; no registered candidate is invented.",
                "item_count": len(ROWS),
                "represented_item_count": len(represented_rows),
                "represented_item_surfaced": 0,
                "adequate_target_count": 0,
                "adequate_kept": 0,
                "evaluation_scope": "developmentOnly",
                "accuracy_verdict_eligible": False,
                "reachable_candidate_expectation_ids": sorted(represented_ids),
                "excluded_not_represented_expectation_ids": sorted(missing_ids),
                "open_label_assertion_ids": sorted(assertion_ids),
                "items": [],
            },
        ],
    }
    artifacts = write_experiment_artifacts(
        tmp_path / "real-open-set",
        result,
        [],
        decision="continue",
        rationale=(
            "Keep the four source-grounded open labels in development while "
            "independent application review remains outstanding."
        ),
    )
    experiment = json.loads(artifacts.experiment.read_text())
    metrics = json.loads(artifacts.metrics.read_text())
    measured = metrics["results"][0]

    assert experiment["eligibility"] == {
        "acceptedOutputEligible": False,
        "adoptionEligible": False,
        "candidateUseOnly": True,
        "promotionAuthorized": False,
        "scope": "developmentOnly",
    }
    assert measured["stages"]["available"] == {
        "count": len(represented_rows),
        "population": len(ROWS),
        "rate": round(len(represented_rows) / len(ROWS), 6),
    }
    assert measured["stages"]["retrieved"] == {
        "count": 0,
        "available": len(represented_rows),
        "rate": 0.0,
    }
    assert set(measured["reachable_candidate_expectation_ids"]) == represented_ids
    assert set(measured["excluded_not_represented_expectation_ids"]) == missing_ids
    assert set(measured["open_label_assertion_ids"]) == assertion_ids
    assert represented_ids.isdisjoint(missing_ids)


def test_real_open_label_route_rejects_source_drift_and_promotion(
    pinned_assets: dict[str, Path],
) -> None:
    row = next(row for row in ROWS if row["expectedOutcome"] == "notRepresented")
    artifact = next(artifact for artifact in SOURCE_ARTIFACTS if artifact["id"] == row["sourceArtifactId"])
    source_text = _source_text(pinned_assets, artifact)
    profile = _candidate_only_open_label_profile()
    common = {
        "dataset_id": MANIFEST["datasetId"],
        "row": row,
        "source_artifact": artifact,
        "source_text": source_text,
        "output_profile": profile,
        "facet": OPEN_LABEL_FACET,
        "assignment_role": OPEN_LABEL_ROLE,
        "resource_route": "document",
        "extraction_activity_iri": OPEN_LABEL_ACTIVITY,
        "extraction_run_iri": OPEN_LABEL_RUN,
        "extractor_iri": OPEN_LABEL_EXTRACTOR,
        "extractor_version": OPEN_LABEL_EXTRACTOR_VERSION,
        "asserted_at": OPEN_LABEL_ASSERTED_AT,
    }

    with pytest.raises(
        DevelopmentOpenLabelError,
        match="source field digest",
    ):
        materialize_development_open_label(
            **{
                **common,
                "source_text": source_text + "drift",
            }
        )

    drifted_row = json.loads(json.dumps(row))
    drifted_row["evidence"]["text"] += "drift"
    with pytest.raises(
        DevelopmentOpenLabelError,
        match="evidence text or digest drifted",
    ):
        materialize_development_open_label(
            **{
                **common,
                "row": drifted_row,
            }
        )

    reachable_row = json.loads(json.dumps(row))
    reachable_row["notRepresented"]["includedInReachableCandidateRecallDenominator"] = True
    with pytest.raises(
        DevelopmentOpenLabelError,
        match="outside reachable-candidate recall",
    ):
        materialize_development_open_label(
            **{
                **common,
                "row": reachable_row,
            }
        )

    with pytest.raises(
        ReferenceRuntimeError,
        match="acceptedOutputUse=true",
    ):
        profile.authorize_open_label(
            facet=OPEN_LABEL_FACET,
            assignment_role=OPEN_LABEL_ROLE,
            resource_route="document",
            mode="explicitLanguage",
            default_language=None,
            accepted_output=True,
        )

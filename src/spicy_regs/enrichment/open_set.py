"""Ground source-explicit open-label candidates without promoting them.

This is a deliberately narrow Spicy Regs adapter. RefSpec owns the output
profile and portable Rulespec records; Spicy verifies its exact source slice
and asks the RefSpec builder for candidate-use materialization.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

from refspec import OutputProfile, ReferenceRuntimeError, materialize_open_label_value_assertion

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANDIDATE_USAGE = "rkaf:searchOnly"


class DevelopmentOpenLabelError(ValueError):
    """A development row is not exactly grounded in its pinned source."""


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DevelopmentOpenLabelError(f"{label} must be an object")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DevelopmentOpenLabelError(f"{label} must be a non-empty string")
    return value


def _digest(value: object, label: str) -> str:
    digest = _text(value, label)
    if not _SHA256.fullmatch(digest):
        raise DevelopmentOpenLabelError(f"{label} must be sha256:<64 lowercase hex>")
    return digest


def _offset(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise DevelopmentOpenLabelError(f"{label} must be a non-negative integer")
    return value


def _text_digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def materialize_development_open_label(
    *,
    dataset_id: str,
    row: Mapping[str, Any],
    source_artifact: Mapping[str, Any],
    source_text: str,
    output_profile: OutputProfile,
    facet: str,
    assignment_role: str,
    resource_route: str,
    extraction_activity_iri: str,
    extraction_run_iri: str,
    extractor_iri: str,
    extractor_version: str,
    asserted_at: str,
) -> Mapping[str, Mapping[str, Any]]:
    """Build one exact-source, candidate-use-only ``rkaf:openLabel`` graph."""

    dataset = _text(dataset_id, "dataset_id")
    row_id = _text(row.get("rowId"), "row.rowId")
    if row.get("expectedOutcome") != "notRepresented":
        raise DevelopmentOpenLabelError("open-label routing requires expectedOutcome=notRepresented")
    if row.get("partition") != "developmentOnly" or row.get("reviewStatus") != "proposedUnsealed":
        raise DevelopmentOpenLabelError("open-label routing requires a proposed, development-only row")

    missing = _mapping(row.get("notRepresented"), "row.notRepresented")
    routes = missing.get("requiredRoutes")
    if not isinstance(routes, list) or "openLabel" not in routes:
        raise DevelopmentOpenLabelError("notRepresented row does not permit the openLabel route")
    if missing.get("includedInReachableCandidateRecallDenominator") is not False:
        raise DevelopmentOpenLabelError("notRepresented row must stay outside reachable-candidate recall")

    artifact_id = _text(source_artifact.get("id"), "source_artifact.id")
    if row.get("sourceArtifactId") != artifact_id:
        raise DevelopmentOpenLabelError("row sourceArtifactId does not match the pinned source artifact")
    artifact_digest = _digest(source_artifact.get("artifactDigest"), "source_artifact.artifactDigest")
    field_digest = _digest(source_artifact.get("sourceFieldDigest"), "source_artifact.sourceFieldDigest")
    native_digest = _digest(
        source_artifact.get("nativeDistributionDigest"),
        "source_artifact.nativeDistributionDigest",
    )
    source_field = _text(source_artifact.get("sourceField"), "source_artifact.sourceField")
    if _text_digest(source_text) != field_digest:
        raise DevelopmentOpenLabelError("pinned source field digest does not match the supplied source text")

    mention = _mapping(row.get("mention"), "row.mention")
    evidence = _mapping(row.get("evidence"), "row.evidence")
    mention_start = _offset(mention.get("startChar"), "row.mention.startChar")
    mention_end = _offset(mention.get("endChar"), "row.mention.endChar")
    evidence_start = _offset(evidence.get("startChar"), "row.evidence.startChar")
    evidence_end = _offset(evidence.get("endChar"), "row.evidence.endChar")
    if not (evidence_start <= mention_start < mention_end <= evidence_end <= len(source_text)):
        raise DevelopmentOpenLabelError("mention and evidence offsets do not form one grounded source span")

    mention_text = _text(mention.get("text"), "row.mention.text")
    evidence_text = _text(evidence.get("text"), "row.evidence.text")
    mention_digest = _digest(mention.get("digest"), "row.mention.digest")
    evidence_digest = _digest(evidence.get("digest"), "row.evidence.digest")
    if source_text[mention_start:mention_end] != mention_text or _text_digest(mention_text) != mention_digest:
        raise DevelopmentOpenLabelError("mention text or digest drifted from the pinned source")
    if source_text[evidence_start:evidence_end] != evidence_text or _text_digest(evidence_text) != evidence_digest:
        raise DevelopmentOpenLabelError("evidence text or digest drifted from the pinned source")
    language = _text(mention.get("language"), "row.mention.language")

    fragment_identity = {
        "artifactDigest": artifact_digest,
        "evidenceDigest": evidence_digest,
        "evidenceEndChar": evidence_end,
        "evidenceStartChar": evidence_start,
        "nativeDistributionDigest": native_digest,
        "sourceField": source_field,
        "sourceFieldDigest": field_digest,
    }
    fragment_hash = _identity_digest(fragment_identity)
    artifact_iri = f"urn:spicy-regs:source-artifact:sha256:{artifact_digest.removeprefix('sha256:')}"
    fragment_iri = f"urn:spicy-regs:source-fragment:sha256:{fragment_hash}"
    position_selector_iri = f"{fragment_iri}:selector:position"
    quote_selector_iri = f"{fragment_iri}:selector:quote"
    assertion_hash = _identity_digest(
        {
            "assignmentRole": assignment_role,
            "datasetId": dataset,
            "facet": facet,
            "fragment": fragment_iri,
            "language": language,
            "mentionDigest": mention_digest,
            "resourceRoute": resource_route,
        }
    )

    try:
        graph = materialize_open_label_value_assertion(
            output_profile=output_profile,
            facet=facet,
            assignment_role=assignment_role,
            resource_route=resource_route,
            mode="explicitLanguage",
            declared_default_language=None,
            literal=mention_text,
            language_tag=language,
            assertion_id=f"urn:spicy-regs:open-label-assertion:sha256:{assertion_hash}",
            subject_iri=artifact_iri,
            extraction_activity_iri=extraction_activity_iri,
            asserted_at=asserted_at,
            evidence_binding_id=f"urn:spicy-regs:open-label-evidence:sha256:{assertion_hash}",
            source_fragment_iris=(fragment_iri,),
            usage_eligibility=_CANDIDATE_USAGE,
            accepted_output=False,
        )
    except ReferenceRuntimeError as error:
        raise DevelopmentOpenLabelError(str(error)) from error

    return {
        **graph,
        "extractionActivity": {
            "@id": _text(extraction_activity_iri, "extraction_activity_iri"),
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:deterministicParse",
            "rkaf:extractionRun": _text(extraction_run_iri, "extraction_run_iri"),
            "rkaf:extractedBy": _text(extractor_iri, "extractor_iri"),
            "rkaf:extractorVersion": _text(extractor_version, "extractor_version"),
        },
        "sourceArtifact": {
            "@id": artifact_iri,
            "@type": "rkaf:Artifact",
            "rkaf:hasArtifactIdentifier": [
                _text(source_artifact.get("sourceUrl"), "source_artifact.sourceUrl"),
            ],
            "rkaf:artifactIdentifierScheme": ["rkaf:partner-defined"],
            "rkaf:hasContentDigest": artifact_digest,
        },
        "sourceFragment": {
            "@id": fragment_iri,
            "@type": "rkaf:SourceFragment",
            "oa:hasSource": artifact_iri,
            "oa:hasSelector": [
                position_selector_iri,
                quote_selector_iri,
            ],
            "rkaf:selectorKind": [
                "oa:TextPositionSelector",
                "oa:TextQuoteSelector",
            ],
            "rkaf:fragmentIdentityScheme": "rkaf:published-fragment",
            "rkaf:sourceArtifactDigest": artifact_digest,
            "rkaf:fragmentContentDigest": evidence_digest,
        },
        "positionSelector": {
            "@id": position_selector_iri,
            "@type": "oa:TextPositionSelector",
            "oa:start": evidence_start,
            "oa:end": evidence_end,
            "rkaf:coordinateSystem": "rkaf:unicode-codepoint",
        },
        "quoteSelector": {
            "@id": quote_selector_iri,
            "@type": "oa:TextQuoteSelector",
            "oa:exact": evidence_text,
        },
        "sourceGrounding": {
            "datasetId": dataset,
            "rowId": row_id,
            "sourceArtifactId": artifact_id,
            **fragment_identity,
            "mentionDigest": mention_digest,
            "mentionStartChar": mention_start,
            "mentionEndChar": mention_end,
            "partition": "developmentOnly",
            "reviewStatus": "proposedUnsealed",
        },
    }


__all__ = [
    "DevelopmentOpenLabelError",
    "materialize_development_open_label",
]

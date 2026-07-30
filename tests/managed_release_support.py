"""Shared selected managed-release fixture for Spicy integration tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from refspec.registry.federal_register_thesaurus import (
    parse_federal_register_thesaurus,
)
from refspec.registry.federal_register_vertical_slice import (
    LocalCandidateGovernance,
    build_federal_register_vertical_slice,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RULESPEC_ROOT = REPO_ROOT.parent / "rulespec"
RECORDED_AT = "2026-07-29T17:00:00Z"
RECORDED_BY = "urn:test:agent:spicy-managed-release"

SOURCE = """FEDERAL REGISTER THESAURUS OF INDEXING TERMS
November 16, 1995

Alphabetic list of indexing terms, with references to preferred or
related terms:

Eligibility policy (01)
Poultry slaughter inspection (13)
"""


def build_selected_managed_bundle(
    root: Path,
) -> tuple[dict[str, Any], Path]:
    """Build one gate-authorized bundle containing the projection test term."""

    parsed = parse_federal_register_thesaurus(SOURCE)
    bundle = build_federal_register_vertical_slice(
        parsed,
        rulespec_root=RULESPEC_ROOT,
        recorded_at=RECORDED_AT,
        recorded_by=RECORDED_BY,
        governance=LocalCandidateGovernance(
            actor_iri="urn:test:actor:spicy-local-reviewer",
            organization_iri="urn:test:organization:spicy-regs",
            effective_at=RECORDED_AT,
        ),
    )
    bundle.write_to(root)
    publication = bundle.publication_release_manifest
    deployment = next(
        record
        for record in bundle.operational_records
        if record.get("type") == "urn:ref:type:RegistryDeploymentDecision"
        and record.get("selectionState") == "selected"
    )
    import_snapshot = deployment["registryImportSnapshot"]
    release = deployment["referenceResourceRelease"]
    corpus = publication["expressionCorpusSnapshot"]
    concept = next(
        node
        for node in bundle.rulespec_graph["@graph"]
        if node.get("skos:prefLabel", {}).get("en") == "Poultry slaughter inspection"
    )
    expression = next(
        record
        for record in bundle.indexed_expressions
        if record.get("member") == concept["@id"]
        and record.get("semanticProperty") == "http://www.w3.org/2004/02/skos/core#prefLabel"
    )
    expression_ids = [record["id"] for record in bundle.indexed_expressions if record.get("member") == concept["@id"]]
    support = {
        "PUBLICATION_ID": publication["id"],
        "RELEASE_ID": release["id"],
        "RELEASE_VERSION": release["version"],
        "RELEASE_DIGEST": release["digest"],
        "IMPORT_ID": import_snapshot["id"],
        "IMPORT_DIGEST": import_snapshot["digest"],
        "CORPUS_ID": corpus["id"],
        "CORPUS_DIGEST": corpus["digest"],
        "MEMBER_ID": concept["@id"],
        "SCHEME_ID": concept["skos:inScheme"],
        "EXPRESSION_ID": expression["id"],
        "EXPRESSION_IDS": expression_ids,
    }
    return support, root / "managed-release-bundle.json"

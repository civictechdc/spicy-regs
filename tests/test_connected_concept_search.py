"""Cross-domain anchors improve lookup without merging concept identities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from refspec import ManagedReleaseConceptMapping
from refspec.registry import (
    ConceptDomainBridge,
    ConceptDomainSourceConcept,
    ConceptDomainSourceSnapshot,
)

from spicy_regs.docpipeline.rkaf_projection import (
    managed_release_candidate_vocabulary,
)
from spicy_regs.enrichment.connected_concepts import (
    CONNECTED_SELECTOR_VERSION,
    ConnectedConceptSearchError,
    select_connected_candidate_concepts,
)
from spicy_regs.enrichment.managed_release import (
    ManagedReleaseCandidateSource,
)
from spicy_regs.ontology.concepts import (
    select_candidate_concepts_anchored_v2,
)
from tests.managed_release_support import build_selected_managed_bundle


def _concept(
    concept_id: str,
    label: str,
    *,
    vocabulary: str,
    alternate: tuple[str, ...] = (),
) -> dict:
    return {
        "concept_id": concept_id,
        "facet": "subject",
        "source_vocabulary": vocabulary,
        "scheme": "subject",
        "pref_label": label,
        "alt_labels_json": json.dumps(list(alternate)),
        "definition": "",
        "status": "active",
        "external_ids_json": "[]",
    }


def _mapping(
    *,
    mapping_id: str = "urn:ref:mapping:icpsr-fr:refugees",
    source: str = "https://example.test/icpsr/refugees",
    target: str = "urn:ref:fr:refugees",
    relation: str = "skos:closeMatch",
) -> ManagedReleaseConceptMapping:
    return ManagedReleaseConceptMapping(
        mapping_iri=mapping_id,
        source_member_iri=source,
        relation_iri=relation,
        target_member_iri=target,
        source_release_iri="urn:ref:icpsr:release:test",
        target_release_iri="urn:ref:fr:release:test",
        record={},
    )


def test_icpsr_alias_reaches_regulatory_concept_without_relabeling_it():
    target = _concept(
        "urn:ref:fr:refugees",
        "Refugees",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "https://example.test/icpsr/refugees",
        "Refugees",
        vocabulary="https://example.test/icpsr",
        alternate=("asylum seekers",),
    )

    assert (
        select_candidate_concepts_anchored_v2(
            "Protections for asylum seekers arriving at the border.",
            [target],
        )
        == []
    )
    results = select_connected_candidate_concepts(
        "Protections for asylum seekers arriving at the border.",
        lookup_concepts=[target, anchor],
        output_concepts=[target],
        mappings=[_mapping()],
    )

    assert [row["concept_id"] for row in results] == [
        "urn:ref:fr:refugees"
    ]
    assert results[0]["pref_label"] == "Refugees"
    assert results[0]["candidate_channels"] == ["mappedNeighbor"]
    assert results[0]["selector_version"] == CONNECTED_SELECTOR_VERSION
    assert results[0]["candidate_rank"] == 1
    assert results[0]["candidate_score"] > 0
    assert results[0]["candidate_score_state"] == "produced"
    assert results[0]["indexed_representation_version"]
    assert results[0]["selected_channel"] == "mappedNeighbor"
    assert (
        results[0]["selected_mapping_path"]
        == results[0]["mapping_paths"][0]
    )
    assert results[0]["mapping_paths"] == [
        {
            "mapping_iri": "urn:ref:mapping:icpsr-fr:refugees",
            "relation_iri": "skos:closeMatch",
            "source_member_iri": (
                "https://example.test/icpsr/refugees"
            ),
            "target_member_iri": "urn:ref:fr:refugees",
            "source_release_iri": "urn:ref:icpsr:release:test",
            "target_release_iri": "urn:ref:fr:release:test",
            "direction": "sourceToTarget",
        }
    ]


def test_expansion_is_one_hop_and_does_not_return_anchor_concepts():
    target = _concept(
        "urn:ref:fr:refugees",
        "Refugees",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "https://example.test/icpsr/refugees",
        "Refugees",
        vocabulary="https://example.test/icpsr",
        alternate=("asylum seekers",),
    )
    results = select_connected_candidate_concepts(
        "Rules for asylum seekers.",
        lookup_concepts=[target, anchor],
        output_concepts=[target],
        mappings=[_mapping()],
    )

    assert [row["concept_id"] for row in results] == [
        "urn:ref:fr:refugees"
    ]
    assert all(
        row["concept_id"] != anchor["concept_id"]
        for row in results
    )


def test_same_label_does_not_create_an_implicit_mapping():
    target = _concept(
        "urn:ref:fr:civil-rights",
        "Civil rights",
        vocabulary="urn:ref:fr:scheme",
    )
    other = _concept(
        "https://example.test/icpsr/civil-rights",
        "Civil rights",
        vocabulary="https://example.test/icpsr",
        alternate=("constitutional rights",),
    )

    results = select_connected_candidate_concepts(
        "Constitutional rights protections.",
        lookup_concepts=[target, other],
        output_concepts=[target],
        mappings=[],
    )

    assert [row["concept_id"] for row in results] == [
        "urn:ref:fr:civil-rights"
    ]
    assert results[0]["candidate_channels"] == ["lexical"]
    assert results[0]["mapping_paths"] == []
    assert all(
        row["concept_id"] != other["concept_id"]
        for row in results
    )


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        (
            _mapping(source="urn:missing:source"),
            "is not indexed",
        ),
        (
            _mapping(target="urn:missing:target"),
            "is not authorized for output",
        ),
        (
            _mapping(relation="skos:broader"),
            "unsupported concept mapping relation",
        ),
    ],
)
def test_invalid_mapping_inputs_fail_closed(
    mapping: ManagedReleaseConceptMapping,
    message: str,
):
    target = _concept(
        "urn:ref:fr:refugees",
        "Refugees",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "https://example.test/icpsr/refugees",
        "Refugees",
        vocabulary="https://example.test/icpsr",
        alternate=("asylum seekers",),
    )

    with pytest.raises(ConnectedConceptSearchError, match=message):
        select_connected_candidate_concepts(
            "Asylum seekers.",
            lookup_concepts=[target, anchor],
            output_concepts=[target],
            mappings=[mapping],
        )


def test_result_order_is_deterministic():
    target_a = _concept(
        "urn:ref:fr:a",
        "Alpha",
        vocabulary="urn:ref:fr:scheme",
    )
    target_b = _concept(
        "urn:ref:fr:b",
        "Beta",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "urn:ref:icpsr:anchor",
        "Asylum seekers",
        vocabulary="urn:ref:icpsr:scheme",
    )
    mappings = [
        ManagedReleaseConceptMapping(
            mapping_iri="urn:ref:mapping:b",
            source_member_iri=anchor["concept_id"],
            relation_iri="skos:closeMatch",
            target_member_iri=target_b["concept_id"],
            source_release_iri="urn:ref:icpsr:release:test",
            target_release_iri="urn:ref:fr:release:test",
            record={},
        ),
        ManagedReleaseConceptMapping(
            mapping_iri="urn:ref:mapping:a",
            source_member_iri=anchor["concept_id"],
            relation_iri="skos:closeMatch",
            target_member_iri=target_a["concept_id"],
            source_release_iri="urn:ref:icpsr:release:test",
            target_release_iri="urn:ref:fr:release:test",
            record={},
        ),
    ]

    first = select_connected_candidate_concepts(
        "Asylum seekers.",
        lookup_concepts=[target_a, target_b, anchor],
        output_concepts=[target_a, target_b],
        mappings=mappings,
    )
    second = select_connected_candidate_concepts(
        "Asylum seekers.",
        lookup_concepts=[target_a, target_b, anchor],
        output_concepts=[target_a, target_b],
        mappings=list(reversed(mappings)),
    )

    assert first == second
    assert [row["concept_id"] for row in first] == [
        "urn:ref:fr:a",
        "urn:ref:fr:b",
    ]


def test_managed_release_uses_bridge_concepts_only_as_search_anchors(
    tmp_path: Path,
):
    support, manifest_path = build_selected_managed_bundle(tmp_path)
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=(
            "sha256:" + hashlib.sha256(
                manifest_path.read_bytes()
            ).hexdigest()
        ),
        lookup_index_manifest={
            "id": "urn:test:lookup-index:connected-concepts:v1",
            "digest": "sha256:" + "d" * 64,
        },
        permission_facet_iri="urn:ref:facet:general-subject",
        permission_assignment_role_iri=(
            "https://rulespec.org/ns/v1#assignmentPrimary"
        ),
        permission_resource_route="document",
    )
    anchor_id = "https://example.test/thesaurus/poultry-inspection"
    source_release = "urn:test:source-release:poultry"
    mapping = ManagedReleaseConceptMapping(
        mapping_iri="urn:test:mapping:poultry",
        source_member_iri=anchor_id,
        relation_iri="skos:closeMatch",
        target_member_iri=str(support["MEMBER_ID"]),
        source_release_iri=source_release,
        target_release_iri=str(support["RELEASE_ID"]),
        record={},
    )
    bridge = ConceptDomainBridge(
        development_only=True,
        source_snapshot=ConceptDomainSourceSnapshot(
            url="https://example.test/source.json",
            revision="test",
            sha256="sha256:" + "a" * 64,
        ),
        source_scheme_iri="https://example.test/thesaurus",
        source_release_iri=source_release,
        target_release_iri=str(support["RELEASE_ID"]),
        source_concepts=(
            ConceptDomainSourceConcept(
                concept_iri=anchor_id,
                preferred_labels={
                    "en": "Poultry slaughter inspection"
                },
                alternate_labels={
                    "en": ("broiler dressing",)
                },
                definitions={},
                evidence_url=anchor_id,
                record={},
            ),
        ),
        mappings=(mapping,),
        artifact_sha256="sha256:" + "b" * 64,
        record={},
    )

    vocabulary = managed_release_candidate_vocabulary(
        source,
        default_language="en",
        concept_domain_bridges=(bridge,),
    )
    results = select_connected_candidate_concepts(
        "Standards for broiler dressing plants.",
        lookup_concepts=vocabulary.lookup_rows,
        output_concepts=vocabulary.selector_rows,
        mappings=vocabulary.candidate_mappings,
    )

    assert anchor_id in {
        row["concept_id"] for row in vocabulary.lookup_rows
    }
    assert anchor_id not in {
        row["concept_id"] for row in vocabulary.selector_rows
    }
    assert anchor_id not in vocabulary.concepts
    assert [row["concept_id"] for row in results] == [
        support["MEMBER_ID"]
    ]
    assert "mappedNeighbor" in results[0]["candidate_channels"]
    assert results[0]["mapping_paths"][0]["mapping_iri"] == (
        mapping.mapping_iri
    )
    assert vocabulary.mapping_sha256 is not None


def test_preferred_label_match_outranks_the_same_wording_as_an_alias():
    preferred_target = _concept(
        "urn:ref:fr:preferred",
        "Worker safeguards",
        vocabulary="urn:ref:fr:scheme",
    )
    alias_target = _concept(
        "urn:ref:fr:alias",
        "Occupational safety",
        vocabulary="urn:ref:fr:scheme",
    )
    preferred_anchor = _concept(
        "urn:ref:source:preferred",
        "Worker protection",
        vocabulary="urn:ref:source:a",
    )
    alias_anchor = _concept(
        "urn:ref:source:alias",
        "Employment standards",
        vocabulary="urn:ref:source:b",
        alternate=("worker protection",),
    )

    results = select_connected_candidate_concepts(
        "The rule establishes worker protection requirements.",
        lookup_concepts=[
            preferred_target,
            alias_target,
            preferred_anchor,
            alias_anchor,
        ],
        output_concepts=[preferred_target, alias_target],
        mappings=[
            _mapping(
                mapping_id="urn:ref:mapping:preferred",
                source=preferred_anchor["concept_id"],
                target=preferred_target["concept_id"],
            ),
            _mapping(
                mapping_id="urn:ref:mapping:alias",
                source=alias_anchor["concept_id"],
                target=alias_target["concept_id"],
            ),
        ],
    )

    assert [row["concept_id"] for row in results] == [
        preferred_target["concept_id"],
        alias_target["concept_id"],
    ]


def test_label_reused_across_many_vocabularies_is_discounted():
    shared_target = _concept(
        "urn:ref:fr:shared",
        "Administrative policy",
        vocabulary="urn:ref:fr:scheme",
    )
    specific_target = _concept(
        "urn:ref:fr:specific",
        "Refugees",
        vocabulary="urn:ref:fr:scheme",
    )
    shared_anchors = [
        _concept(
            f"urn:ref:source:shared:{index}",
            "General policy",
            vocabulary=f"urn:ref:source:vocabulary:{index}",
        )
        for index in range(4)
    ]
    specific_anchor = _concept(
        "urn:ref:source:specific",
        "Displaced persons",
        vocabulary="urn:ref:source:specific",
        alternate=("asylum seekers",),
    )

    results = select_connected_candidate_concepts(
        "General policy protections for asylum seekers.",
        lookup_concepts=[
            shared_target,
            specific_target,
            *shared_anchors,
            specific_anchor,
        ],
        output_concepts=[shared_target, specific_target],
        mappings=[
            _mapping(
                mapping_id="urn:ref:mapping:shared",
                source=shared_anchors[0]["concept_id"],
                target=shared_target["concept_id"],
            ),
            _mapping(
                mapping_id="urn:ref:mapping:specific",
                source=specific_anchor["concept_id"],
                target=specific_target["concept_id"],
            ),
        ],
    )

    assert [row["concept_id"] for row in results] == [
        specific_target["concept_id"],
        shared_target["concept_id"],
    ]


@pytest.mark.parametrize(
    "relation",
    [
        "skos:closeMatch",
        "skos:broadMatch",
        "skos:relatedMatch",
    ],
)
def test_exact_output_term_stays_above_a_broader_mapped_neighbor(
    relation: str,
):
    exact = _concept(
        "urn:ref:fr:hazardous-materials-transportation",
        "Hazardous materials transportation",
        vocabulary="urn:ref:fr:scheme",
    )
    broad = _concept(
        "urn:ref:fr:hazardous-substances",
        "Hazardous substances",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "urn:ref:source:hazardous-substances",
        "Hazardous substances",
        vocabulary="urn:ref:source:scheme",
        alternate=("hazardous materials",),
    )

    results = select_connected_candidate_concepts(
        "Requirements for hazardous materials transportation.",
        lookup_concepts=[exact, broad, anchor],
        output_concepts=[exact, broad],
        mappings=[
            _mapping(
                mapping_id="urn:ref:mapping:hazardous-substances",
                source=anchor["concept_id"],
                target=broad["concept_id"],
                relation=relation,
            )
        ],
    )

    assert [row["concept_id"] for row in results[:2]] == [
        exact["concept_id"],
        broad["concept_id"],
    ]


def test_specific_alias_can_outrank_a_one_word_preferred_label():
    generic = _concept(
        "urn:ref:fr:housing",
        "Housing",
        vocabulary="urn:ref:fr:scheme",
    )
    specific = _concept(
        "urn:ref:fr:public-housing",
        "Public housing",
        vocabulary="urn:ref:fr:scheme",
    )
    anchor = _concept(
        "urn:ref:source:public-housing",
        "Public housing",
        vocabulary="urn:ref:source:scheme",
        alternate=("housing projects",),
    )

    results = select_connected_candidate_concepts(
        "Housing projects",
        lookup_concepts=[generic, specific, anchor],
        output_concepts=[generic, specific],
        mappings=[
            _mapping(
                mapping_id="urn:ref:mapping:public-housing",
                source=anchor["concept_id"],
                target=specific["concept_id"],
            )
        ],
    )

    assert [row["concept_id"] for row in results[:2]] == [
        specific["concept_id"],
        generic["concept_id"],
    ]

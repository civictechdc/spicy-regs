"""The model-facing vocabulary path reads a local interface, not RefSpec code."""

from __future__ import annotations

import builtins
import hashlib
import json
import subprocess
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.candidate_release import (
    CandidateReleaseError,
    CandidateSelectionReceipt,
    VocabularyAtlasCandidateSource,
)
from spicy_regs.docpipeline.rkaf_projection import (
    candidate_release_vocabulary,
)

ASSET_DIGEST = "a" * 64
RELEASE_DIGEST = "sha256:" + "b" * 64
FACET = "urn:ref:facet:general-subject"
ROLE = "https://rulespec.org/ns/v1#assignmentPrimary"
ROUTE = "document"
CHECKED_ATLAS_ROOT = (
    Path(__file__).resolve().parents[1]
    / "RefSpec"
    / "bindings"
    / "atlas"
    / "1.0"
    / "examples"
    / "federal-register-thesaurus-2025"
)


@dataclass(frozen=True)
class _Member:
    member_iri: str
    release_iri: str
    scheme_iri: str
    record: Mapping[str, Any]


@dataclass(frozen=True)
class _Expression:
    member_iri: str
    original_literal: str
    language_tag: str | None
    semantic_property_iri: str


class _PublishedFileReader:
    """Small stand-in for a product-local reader of published release files."""

    usage_ceiling = "candidateUseOnly"
    lookup_index_manifest = {
        "id": "urn:test:lookup-index:subjects:v1",
        "digest": "sha256:" + "4" * 64,
    }

    def __init__(self) -> None:
        self._member = _Member(
            member_iri="urn:test:concept:poultry-inspection",
            release_iri="urn:test:release:subjects:v1",
            scheme_iri="urn:test:scheme:subjects",
            record={
                "@id": "urn:test:concept:poultry-inspection",
                "@type": "skos:Concept",
            },
        )
        self._expressions = (
            _Expression(
                member_iri=self._member.member_iri,
                original_literal="Poultry inspection",
                language_tag="en",
                semantic_property_iri=("http://www.w3.org/2004/02/skos/core#prefLabel"),
            ),
            _Expression(
                member_iri=self._member.member_iri,
                original_literal="Slaughter inspection",
                language_tag="en",
                semantic_property_iri=("http://www.w3.org/2004/02/skos/core#altLabel"),
            ),
        )
        self.candidate_selection = CandidateSelectionReceipt(
            source_asset={
                "type": "VocabularyAtlasAsset",
                "assetId": "urn:ref:vocabulary-atlas:" + ASSET_DIGEST,
                "manifestDigest": "sha256:" + "5" * 64,
                "outputDigest": "sha256:" + "6" * 64,
            },
            resource_route="document",
            reference_resource_release={
                "id": self._member.release_iri,
                "digest": "sha256:" + "1" * 64,
            },
            facet_iri=FACET,
            assignment_role_iri=ROLE,
        )

    def lookup_member(self, member_iri: str) -> _Member | None:
        return self._member if member_iri == self._member.member_iri else None

    def iter_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[_Expression]:
        for expression in self._expressions:
            if member_iri is None or expression.member_iri == member_iri:
                yield expression


def test_model_path_import_does_not_require_refspec() -> None:
    script = """
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'model path imported {name}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
from spicy_regs.docpipeline.rkaf_projection import candidate_release_vocabulary
assert callable(candidate_release_vocabulary)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_ordinary_enrichment_imports_do_not_require_refspec() -> None:
    script = """
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'ordinary enrichment imported {name}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
import spicy_regs.enrichment as enrichment
from spicy_regs.enrichment.experiment_artifacts import DEVELOPMENT_DECISIONS
assert callable(enrichment.select_connected_candidate_concepts)
assert DEVELOPMENT_DECISIONS == {'continue', 'investigate', 'stop'}
assert not hasattr(enrichment, 'ManagedReleaseCandidateSource')
assert not hasattr(enrichment, 'authorize_managed_accepted_assignment')
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_candidate_vocabulary_accepts_file_reader_without_importing_refspec(
    monkeypatch,
) -> None:
    reader = _PublishedFileReader()
    original_import = builtins.__import__

    def reject_refspec(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "refspec" or name.startswith("refspec."):
            raise AssertionError(f"candidate lookup imported {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_refspec)

    vocabulary = candidate_release_vocabulary(
        reader,
        default_language="en",
    )

    assert tuple(vocabulary.concepts) == ("urn:test:concept:poultry-inspection",)
    assert vocabulary.selector_rows == (
        {
            "concept_id": "urn:test:concept:poultry-inspection",
            "facet": "subject",
            "source_vocabulary": "urn:test:scheme:subjects",
            "scheme": "subject",
            "pref_label": "Poultry inspection",
            "alt_labels_json": '["Slaughter inspection"]',
            "definition": "",
            "status": "active",
            "external_ids_json": "[]",
        },
    )


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _write_atlas(root: Path) -> dict[str, str]:
    implementation = {
        "id": "urn:test:implementation:atlas-reader-fixture",
        "version": "1.0",
        "sourceModules": [
            {
                "path": "fixture/generator",
                "digest": "sha256:" + "1" * 64,
            }
        ],
        "runtime": {"fixture": "1"},
    }
    inputs = [
        {
            "role": "ManagedReleaseView",
            "manifestDigest": "sha256:" + "2" * 64,
            "publicationReleaseId": "urn:test:publication:subjects:v1",
            "rulespecGraph": {
                "id": "urn:test:rulespec-graph:subjects:v1",
                "digest": "sha256:" + "3" * 64,
            },
        },
        {
            "role": "RulespecCoreRelease",
            "fileDigest": "sha256:" + "4" * 64,
            "releaseId": "urn:rulespec:core:" + "5" * 64,
            "releaseDigest": "sha256:" + "5" * 64,
        },
    ]
    policies = {
        "releaseFacts": "copiedManagedReleaseFactsOnly",
        "analysis": "replaceableMachineAnalysis",
        "labelEquality": "clusterOnly",
        "mappingEligibility": "twoIndependentMachinesSearchOnly",
        "humanFeedback": "appendOnlyNonAuthorizing",
    }
    generation = {
        "format": "refspec-vocabulary-atlas-nquads-1.0",
        "inputs": inputs,
        "implementation": implementation,
        "policies": policies,
    }
    generation_digest = _digest(_canonical(generation))
    asset_id = "urn:ref:vocabulary-atlas:" + generation_digest.removeprefix("sha256:")
    release = "urn:test:release:subjects:v1"
    member = "urn:test:concept:poultry-inspection"
    scheme = "urn:test:scheme:subjects"
    release_graph = asset_id + ":release-facts"
    analysis_graph = asset_id + ":analysis"
    lines = sorted(
        [
            f"<{member}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://rulespec.org/ns/v1#RegisteredConcept> <{release_graph}> .",
            f"<{member}> <http://www.w3.org/2004/02/skos/core#inScheme> <{scheme}> <{release_graph}> .",
            f'<{member}> <http://www.w3.org/2004/02/skos/core#prefLabel> "Poultry inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#altLabel> "Slaughter inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#hiddenLabel> "Bird inspection"@en <{release_graph}> .',
            f'<{member}> <http://www.w3.org/2004/02/skos/core#definition> "Inspection of poultry processing."@en <{release_graph}> .',
            f"<{release}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://rulespec.org/ns/v1#ReferenceResourceRelease> <{release_graph}> .",
            f'<{release}> <https://rulespec.org/ns/v1#referenceReleaseDigest> "{RELEASE_DIGEST}" <{release_graph}> .',
            f"<{release}> <http://www.w3.org/ns/prov#hadMember> <{member}> <{release_graph}> .",
            f"<{member}> <https://refspec.org/ns/vocabulary-atlas/v1#memberOfRelease> <{release}> <{analysis_graph}> .",
            f"<{analysis_graph}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://refspec.org/ns/vocabulary-atlas/v1#ReplaceableAnalysis> <{analysis_graph}> .",
            f"<{analysis_graph}> <http://www.w3.org/ns/prov#wasDerivedFrom> <urn:test:publication:subjects:v1> <{analysis_graph}> .",
        ]
    )
    nquads = ("\n".join(lines) + "\n").encode()
    output_digest = _digest(nquads)
    manifest: dict[str, Any] = {
        "id": asset_id,
        "type": "urn:ref:type:VocabularyAtlasManifest",
        "schemaVersion": "1.0",
        "format": "refspec-vocabulary-atlas-nquads-1.0",
        "generationDigest": generation_digest,
        "inputs": inputs,
        "implementation": implementation,
        "policies": policies,
        "graphs": [
            {
                "role": "releaseFacts",
                "id": release_graph,
                "quadCount": 9,
            },
            {
                "role": "analysis",
                "id": analysis_graph,
                "quadCount": 3,
            },
        ],
        "output": {
            "path": "atlas.nq",
            "mediaType": "application/n-quads",
            "digest": output_digest,
            "byteLength": len(nquads),
            "quadCount": 12,
        },
        "counts": {
            "managedReleases": 1,
            "releaseFacts": 9,
            "analysisFacts": 3,
            "labelClusters": 0,
            "mappingCandidates": 0,
            "searchOnlyMappings": 0,
            "machineValidations": 0,
            "feedback": 0,
        },
    }
    manifest["canonicalPayloadDigest"] = _digest(_canonical(manifest))
    manifest_raw = _canonical(manifest) + b"\n"
    root.mkdir()
    (root / "atlas.nq").write_bytes(nquads)
    (root / "atlas-manifest.json").write_bytes(manifest_raw)
    return {
        "asset_id": asset_id,
        "manifest_digest": _digest(manifest_raw),
        "output_digest": output_digest,
        "release_id": release,
        "release_digest": RELEASE_DIGEST,
        "member_id": member,
    }


def _open_atlas(root: Path, pins: Mapping[str, str]) -> VocabularyAtlasCandidateSource:
    return VocabularyAtlasCandidateSource.open(
        root / "atlas-manifest.json",
        expected_asset_id=pins["asset_id"],
        expected_manifest_digest=pins["manifest_digest"],
        expected_output_digest=pins["output_digest"],
        reference_release_id=pins["release_id"],
        reference_release_digest=pins["release_digest"],
        facet_iri=FACET,
        assignment_role_iri=ROLE,
        resource_route=ROUTE,
        lookup_index_manifest={
            "id": "urn:test:lookup-index:atlas:v1",
            "digest": "sha256:" + "6" * 64,
        },
    )


def test_atlas_reader_supplies_only_selected_release_candidates(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)

    source = _open_atlas(root, pins)
    vocabulary = candidate_release_vocabulary(
        source,
        default_language="en",
    )

    assert source.usage_ceiling == "diagnosticCandidateOnly"
    assert source.candidate_selection.reference_resource_release == {
        "id": pins["release_id"],
        "digest": pins["release_digest"],
    }
    assert tuple(vocabulary.concepts) == (pins["member_id"],)
    concept = vocabulary.concepts[pins["member_id"]]
    assert concept.preferred_labels == {"en": "Poultry inspection"}
    assert concept.alternate_labels == {"en": "Slaughter inspection"}
    assert concept.hidden_labels == {"en": "Bird inspection"}
    assert concept.definitions == {"en": "Inspection of poultry processing."}
    assert not vocabulary.candidate_mappings


def test_atlas_reader_rejects_an_external_output_pin_mismatch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)

    with pytest.raises(CandidateReleaseError, match="selected pin"):
        VocabularyAtlasCandidateSource.open(
            root / "atlas-manifest.json",
            expected_asset_id=pins["asset_id"],
            expected_manifest_digest=pins["manifest_digest"],
            expected_output_digest="sha256:" + "f" * 64,
            reference_release_id=pins["release_id"],
            reference_release_digest=pins["release_digest"],
            facet_iri=FACET,
            assignment_role_iri=ROLE,
            resource_route=ROUTE,
            lookup_index_manifest={
                "id": "urn:test:lookup-index:atlas:v1",
                "digest": "sha256:" + "6" * 64,
            },
        )


def test_atlas_reader_rejects_boolean_semantic_counts(tmp_path: Path) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    manifest_path = root / "atlas-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["counts"]["feedback"] = False
    manifest.pop("canonicalPayloadDigest")
    manifest["canonicalPayloadDigest"] = _digest(_canonical(manifest))
    manifest_raw = _canonical(manifest) + b"\n"
    manifest_path.write_bytes(manifest_raw)
    pins["manifest_digest"] = _digest(manifest_raw)

    with pytest.raises(CandidateReleaseError, match="nonnegative integers"):
        _open_atlas(root, pins)


def test_atlas_reader_rejects_noncanonical_nquads_spelling(tmp_path: Path) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    nquads_path = root / "atlas.nq"
    nquads = nquads_path.read_bytes().replace(
        b'"Poultry inspection"@en',
        b'"\\u0050oultry inspection"@en',
    )
    assert nquads != nquads_path.read_bytes()
    nquads_path.write_bytes(nquads)

    manifest_path = root / "atlas-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["output"].update(
        {
            "digest": _digest(nquads),
            "byteLength": len(nquads),
        }
    )
    manifest.pop("canonicalPayloadDigest")
    manifest["canonicalPayloadDigest"] = _digest(_canonical(manifest))
    manifest_raw = _canonical(manifest) + b"\n"
    manifest_path.write_bytes(manifest_raw)
    pins["manifest_digest"] = _digest(manifest_raw)
    pins["output_digest"] = _digest(nquads)

    with pytest.raises(CandidateReleaseError, match="bytes are not canonical"):
        _open_atlas(root, pins)


def test_atlas_reader_cannot_be_hand_constructed() -> None:
    with pytest.raises(
        CandidateReleaseError,
        match="must be opened from pinned files",
    ):
        VocabularyAtlasCandidateSource(
            candidate_selection=CandidateSelectionReceipt(
                source_asset={"type": "VocabularyAtlasAsset"},
                reference_resource_release={
                    "id": "urn:test:release",
                    "digest": RELEASE_DIGEST,
                },
                facet_iri=FACET,
                assignment_role_iri=ROLE,
                resource_route=ROUTE,
            ),
            lookup_index_manifest={
                "id": "urn:test:index",
                "digest": "sha256:" + "6" * 64,
            },
            members={},
            expressions=(),
            _verification_token=object(),
        )


def test_atlas_candidate_execution_blocks_all_refspec_imports(
    tmp_path: Path,
) -> None:
    root = tmp_path / "atlas"
    pins = _write_atlas(root)
    script = f"""
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'candidate execution imported {{name}}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
from spicy_regs.candidate_release import VocabularyAtlasCandidateSource
from spicy_regs.docpipeline.rkaf_projection import candidate_release_vocabulary
source = VocabularyAtlasCandidateSource.open(
    {str(root / "atlas-manifest.json")!r},
    expected_asset_id={pins["asset_id"]!r},
    expected_manifest_digest={pins["manifest_digest"]!r},
    expected_output_digest={pins["output_digest"]!r},
    reference_release_id={pins["release_id"]!r},
    reference_release_digest={pins["release_digest"]!r},
    facet_iri={FACET!r},
    assignment_role_iri={ROLE!r},
    resource_route={ROUTE!r},
    lookup_index_manifest={{'id': 'urn:test:index', 'digest': 'sha256:' + '6' * 64}},
)
vocabulary = candidate_release_vocabulary(source, default_language='en')
assert tuple(vocabulary.concepts) == ({pins["member_id"]!r},)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_checked_complete_atlas_is_a_file_only_candidate_source() -> None:
    """One complete RefSpec build feeds SpicyRegs without RefSpec code."""

    script = f"""
import builtins
original_import = builtins.__import__
def reject_refspec(name, *args, **kwargs):
    if name == 'refspec' or name.startswith('refspec.'):
        raise AssertionError(f'candidate execution imported {{name}}')
    return original_import(name, *args, **kwargs)
builtins.__import__ = reject_refspec
from spicy_regs.candidate_release import VocabularyAtlasCandidateSource
from spicy_regs.docpipeline.rkaf_projection import candidate_release_vocabulary
source = VocabularyAtlasCandidateSource.open(
    {str(CHECKED_ATLAS_ROOT / "atlas-manifest.json")!r},
    expected_asset_id='urn:ref:vocabulary-atlas:9069a26d36c2695a02edb501dc51011f48aee382d96a0e200cd2c1d3574d7dec',
    expected_manifest_digest='sha256:956cab4f20477933ef015c2c87647ebb9cc40c4c68247a93b10dab8b113f60f1',
    expected_output_digest='sha256:8e1eaf2265874863981fe9322e0a0e286c01c43e598b091736b556ea424e830a',
    reference_release_id='urn:ref:federal-register-thesaurus:2025-04-01:reference-resource-release:v1',
    reference_release_digest='sha256:30742a82b3e268942aec713a02c5ae4264eadea36aa61b564ffc93eeecfd5fe6',
    facet_iri={FACET!r},
    assignment_role_iri={ROLE!r},
    resource_route={ROUTE!r},
    lookup_index_manifest={{'id': 'urn:test:checked-atlas-index', 'digest': 'sha256:' + '7' * 64}},
)
vocabulary = candidate_release_vocabulary(source, default_language='en')
print(len(vocabulary.concepts), len(vocabulary.selector_rows), len(vocabulary.candidate_mappings))
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "705 705 0"

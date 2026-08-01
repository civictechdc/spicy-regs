"""File-only vocabulary candidates for the SpicyRegs diagnostic model path.

RefSpec publishes the vocabulary atlas.  SpicyRegs verifies the two published
files and reads only exact release membership and authored SKOS text from the
``releaseFacts`` graph.  Facet, assignment-role, and route choices are local
SpicyRegs configuration; opening an atlas does not grant deployment or
accepted-output authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, Self, runtime_checkable

from rdflib import BNode, Dataset, Literal, Namespace, URIRef
from rdflib.namespace import PROV, RDF, SKOS

ATLAS_FORMAT = "refspec-vocabulary-atlas-nquads-1.0"
ATLAS_MANIFEST_TYPE = "urn:ref:type:VocabularyAtlasManifest"

_ATLAS = Namespace("https://refspec.org/ns/vocabulary-atlas/v1#")
_RKAF = Namespace("https://rulespec.org/ns/v1#")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_ASSET_ID = re.compile(r"^urn:ref:vocabulary-atlas:[0-9a-f]{64}$")
_ABSOLUTE_IRI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:[^\s<>]+$")
_SAFE_INTEGER = 9_007_199_254_740_991

_MANIFEST_FIELDS = {
    "id",
    "type",
    "schemaVersion",
    "format",
    "generationDigest",
    "inputs",
    "implementation",
    "policies",
    "graphs",
    "output",
    "counts",
    "canonicalPayloadDigest",
}
_COUNT_FIELDS = {
    "managedReleases",
    "releaseFacts",
    "analysisFacts",
    "labelClusters",
    "mappingCandidates",
    "searchOnlyMappings",
    "machineValidations",
    "feedback",
}
_POLICIES = {
    "releaseFacts": "copiedManagedReleaseFactsOnly",
    "analysis": "replaceableMachineAnalysis",
    "labelEquality": "clusterOnly",
    "mappingEligibility": "twoIndependentMachinesSearchOnly",
    "humanFeedback": "appendOnlyNonAuthorizing",
}
_VERIFIED_ATLAS_READER = object()


class CandidateReleaseError(ValueError):
    """A candidate source is not the exact, internally consistent input named."""


def _digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise CandidateReleaseError(f"{label} must be sha256:<64 lowercase hex>")
    return value


def _require_iri(value: object, label: str) -> str:
    if not isinstance(value, str) or _ABSOLUTE_IRI.fullmatch(value) is None:
        raise CandidateReleaseError(f"{label} must be an absolute IRI")
    return value


def _is_json_integer(value: object) -> bool:
    """Accept JSON integers without treating booleans as counts."""

    return isinstance(value, int) and not isinstance(value, bool)


def _canonical_value(value: object, path: str = "$") -> None:
    if value is None:
        raise CandidateReleaseError(f"{path}: null is forbidden")
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > _SAFE_INTEGER:
            raise CandidateReleaseError(f"{path}: integer exceeds the interoperable JSON range")
        return
    if isinstance(value, float):
        raise CandidateReleaseError(f"{path}: floating-point numbers are forbidden")
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _canonical_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CandidateReleaseError(f"{path}: object keys must be strings")
            _canonical_value(item, f"{path}.{key}")
        return
    raise CandidateReleaseError(f"{path}: unsupported JSON value")


def _canonical_json(value: object) -> bytes:
    _canonical_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CandidateReleaseError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _reject_number(value: str) -> None:
    raise CandidateReleaseError(f"JSON number {value!r} is not a canonical integer")


def _read_regular_file(path: Path | str, label: str) -> tuple[Path, bytes]:
    selected = Path(path)
    if selected.is_symlink():
        raise CandidateReleaseError(f"{label} must not be a symlink")
    try:
        resolved = selected.resolve(strict=True)
    except FileNotFoundError as error:
        raise CandidateReleaseError(f"{label} does not exist") from error
    if not resolved.is_file():
        raise CandidateReleaseError(f"{label} must be a regular file")
    return resolved, resolved.read_bytes()


def _load_manifest(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CandidateReleaseError("atlas manifest is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise CandidateReleaseError("atlas manifest root must be an object")
    if raw != _canonical_json(value) + b"\n":
        raise CandidateReleaseError("atlas manifest is not canonical REF JSON")
    return value


def _frozen_reference(value: Mapping[str, str], label: str) -> Mapping[str, str]:
    if set(value) != {"id", "digest"}:
        raise CandidateReleaseError(f"{label} must contain exactly id and digest")
    return MappingProxyType(
        {
            "id": _require_iri(value.get("id"), f"{label} id"),
            "digest": _require_digest(value.get("digest"), f"{label} digest"),
        }
    )


@dataclass(frozen=True, slots=True)
class CandidateSelectionReceipt:
    """SpicyRegs-local selection inputs recorded with diagnostic candidates."""

    source_asset: Mapping[str, str]
    reference_resource_release: Mapping[str, str]
    facet_iri: str
    assignment_role_iri: str
    resource_route: str

    def __post_init__(self) -> None:
        source_asset = dict(self.source_asset)
        if not source_asset or any(
            not isinstance(key, str) or not isinstance(value, str) or not value for key, value in source_asset.items()
        ):
            raise CandidateReleaseError("candidate source asset must be a non-empty string map")
        object.__setattr__(self, "source_asset", MappingProxyType(source_asset))
        object.__setattr__(
            self,
            "reference_resource_release",
            _frozen_reference(
                self.reference_resource_release,
                "candidate reference resource release",
            ),
        )
        _require_iri(self.facet_iri, "candidate facet")
        _require_iri(self.assignment_role_iri, "candidate assignment role")
        if not isinstance(self.resource_route, str) or not self.resource_route.strip():
            raise CandidateReleaseError("candidate resource route is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "sourceAsset": dict(self.source_asset),
            "referenceResourceRelease": dict(self.reference_resource_release),
            "facet": self.facet_iri,
            "assignmentRole": self.assignment_role_iri,
            "resourceRoute": self.resource_route,
        }


class CandidateReleaseMember(Protocol):
    """One exact member exposed by a published vocabulary release."""

    member_iri: str
    release_iri: str
    scheme_iri: str
    record: Mapping[str, Any]


class CandidateReleaseExpression(Protocol):
    """One authored SKOS string exposed for candidate lookup."""

    member_iri: str
    original_literal: str
    language_tag: str | None
    semantic_property_iri: str


@runtime_checkable
class CandidateReleaseSource(Protocol):
    """Small read-only seam used by the SpicyRegs diagnostic model path."""

    usage_ceiling: str
    candidate_selection: CandidateSelectionReceipt
    lookup_index_manifest: Mapping[str, str]

    def lookup_member(
        self,
        member_iri: str,
    ) -> CandidateReleaseMember | None:
        """Resolve one exact member identifier."""

    def iter_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[CandidateReleaseExpression]:
        """Iterate authored SKOS strings from the selected exact release."""


@dataclass(frozen=True, slots=True)
class _AtlasMember:
    member_iri: str
    release_iri: str
    scheme_iri: str
    record: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _AtlasExpression:
    member_iri: str
    original_literal: str
    language_tag: str | None
    semantic_property_iri: str


def _one_uri(values: Sequence[Any], label: str) -> URIRef:
    if len(values) != 1 or not isinstance(values[0], URIRef):
        raise CandidateReleaseError(f"{label} must be exactly one IRI")
    return values[0]


def _one_literal(values: Sequence[Any], label: str) -> Literal:
    if len(values) != 1 or not isinstance(values[0], Literal):
        raise CandidateReleaseError(f"{label} must be exactly one literal")
    return values[0]


def _validate_manifest(
    manifest: Mapping[str, Any],
    *,
    expected_asset_id: str,
) -> tuple[dict[str, Mapping[str, Any]], Mapping[str, Any]]:
    if set(manifest) != _MANIFEST_FIELDS:
        raise CandidateReleaseError("atlas manifest fields differ from format 1.0")
    if manifest.get("type") != ATLAS_MANIFEST_TYPE:
        raise CandidateReleaseError("atlas manifest type differs")
    if manifest.get("schemaVersion") != "1.0" or manifest.get("format") != ATLAS_FORMAT:
        raise CandidateReleaseError("atlas manifest format differs from 1.0")
    if manifest.get("id") != expected_asset_id:
        raise CandidateReleaseError("atlas asset id differs from the selected id")

    payload_digest = _require_digest(
        manifest.get("canonicalPayloadDigest"),
        "atlas canonical payload digest",
    )
    payload = {key: value for key, value in manifest.items() if key != "canonicalPayloadDigest"}
    if payload_digest != _digest(_canonical_json(payload)):
        raise CandidateReleaseError("atlas canonical payload digest differs")

    generation = {
        "format": manifest["format"],
        "inputs": manifest["inputs"],
        "implementation": manifest["implementation"],
        "policies": manifest["policies"],
    }
    generation_digest = _digest(_canonical_json(generation))
    if manifest.get("generationDigest") != generation_digest:
        raise CandidateReleaseError("atlas generation digest differs")
    if expected_asset_id != "urn:ref:vocabulary-atlas:" + generation_digest.removeprefix("sha256:"):
        raise CandidateReleaseError("atlas asset id differs from its generation digest")

    if manifest.get("policies") != _POLICIES:
        raise CandidateReleaseError("atlas policies differ from format 1.0")

    inputs = manifest.get("inputs")
    if not isinstance(inputs, list):
        raise CandidateReleaseError("atlas inputs must be an array")
    roles = [item.get("role") if isinstance(item, Mapping) else None for item in inputs]
    if (
        roles.count("ManagedReleaseView") < 1
        or roles.count("RulespecCoreRelease") != 1
        or roles.count("CrosswalkBundle") > 1
        or any(role not in {"ManagedReleaseView", "RulespecCoreRelease", "CrosswalkBundle"} for role in roles)
    ):
        raise CandidateReleaseError("atlas input roles differ from format 1.0")

    graphs = manifest.get("graphs")
    if not isinstance(graphs, list) or len(graphs) != 2:
        raise CandidateReleaseError("atlas must declare exactly two graphs")
    graph_by_role: dict[str, Mapping[str, Any]] = {}
    for row in graphs:
        if not isinstance(row, Mapping) or set(row) != {"role", "id", "quadCount"}:
            raise CandidateReleaseError("atlas graph declaration differs from format 1.0")
        role = row.get("role")
        if role not in {"releaseFacts", "analysis"} or role in graph_by_role:
            raise CandidateReleaseError("atlas graph roles differ")
        expected_graph_id = expected_asset_id + (":release-facts" if role == "releaseFacts" else ":analysis")
        if row.get("id") != expected_graph_id:
            raise CandidateReleaseError(f"atlas {role} graph id differs")
        if not _is_json_integer(row.get("quadCount")) or row["quadCount"] < 1:
            raise CandidateReleaseError(f"atlas {role} graph count must be positive")
        graph_by_role[str(role)] = row

    output = manifest.get("output")
    if not isinstance(output, Mapping) or set(output) != {
        "path",
        "mediaType",
        "digest",
        "byteLength",
        "quadCount",
    }:
        raise CandidateReleaseError("atlas output declaration differs from format 1.0")
    if output.get("path") != "atlas.nq" or output.get("mediaType") != "application/n-quads":
        raise CandidateReleaseError("atlas output path or media type differs")
    _require_digest(output.get("digest"), "atlas output digest")
    if not _is_json_integer(output.get("byteLength")) or output["byteLength"] < 1:
        raise CandidateReleaseError("atlas output byte length must be positive")
    if not _is_json_integer(output.get("quadCount")) or output["quadCount"] < 1:
        raise CandidateReleaseError("atlas output quad count must be positive")

    counts = manifest.get("counts")
    if not isinstance(counts, Mapping) or set(counts) != _COUNT_FIELDS:
        raise CandidateReleaseError("atlas semantic counts differ from format 1.0")
    if any(not _is_json_integer(value) or value < 0 for value in counts.values()):
        raise CandidateReleaseError("atlas semantic counts must be nonnegative integers")
    if counts["managedReleases"] < 1 or counts["releaseFacts"] < 1:
        raise CandidateReleaseError("atlas release counts must be positive")

    return graph_by_role, output


def _parse_dataset(
    raw: bytes,
    *,
    graph_by_role: Mapping[str, Mapping[str, Any]],
    output: Mapping[str, Any],
    counts: Mapping[str, int],
) -> Dataset:
    if not raw.endswith(b"\n") or b"\r" in raw:
        raise CandidateReleaseError("atlas N-Quads must use LF and one terminal LF")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CandidateReleaseError("atlas N-Quads is not UTF-8") from error
    lines = text.splitlines()
    if not lines or any(not line or line != line.strip() for line in lines):
        raise CandidateReleaseError("atlas N-Quads has blank or padded lines")
    if lines != sorted(lines):
        raise CandidateReleaseError("atlas N-Quads statements are not sorted")

    dataset = Dataset(default_union=False)
    try:
        dataset.parse(data=text, format="nquads")
    except Exception as error:  # rdflib exposes parser-specific subclasses.
        raise CandidateReleaseError("atlas N-Quads is invalid") from error

    serialized = dataset.serialize(format="nquads")
    serialized_text = serialized.decode("utf-8") if isinstance(serialized, bytes) else serialized
    canonical = ("\n".join(sorted(line for line in serialized_text.splitlines() if line.strip())) + "\n").encode(
        "utf-8"
    )
    if canonical != raw:
        raise CandidateReleaseError("atlas N-Quads bytes are not canonical")

    declared = {str(row["id"]) for row in graph_by_role.values()}
    graph_counts = {graph_id: 0 for graph_id in declared}
    quad_count = 0
    for subject, predicate, value, context in dataset.quads((None, None, None, None)):
        graph_id = getattr(context, "identifier", context)
        if any(isinstance(term, BNode) for term in (subject, predicate, value, graph_id)):
            raise CandidateReleaseError("atlas N-Quads must not contain blank nodes")
        if not isinstance(graph_id, URIRef) or str(graph_id) not in declared:
            raise CandidateReleaseError("atlas N-Quads contains an undeclared or default graph")
        graph_counts[str(graph_id)] += 1
        quad_count += 1

    if quad_count != len(lines):
        raise CandidateReleaseError("atlas N-Quads repeats a statement")
    if quad_count != output["quadCount"]:
        raise CandidateReleaseError("atlas output quad count differs")
    for role, row in graph_by_role.items():
        if graph_counts[str(row["id"])] != row["quadCount"]:
            raise CandidateReleaseError(f"atlas {role} graph count differs")
    if graph_counts[str(graph_by_role["releaseFacts"]["id"])] != counts["releaseFacts"]:
        raise CandidateReleaseError("atlas releaseFacts semantic count differs")
    if graph_counts[str(graph_by_role["analysis"]["id"])] != counts["analysisFacts"]:
        raise CandidateReleaseError("atlas analysis semantic count differs")
    return dataset


@dataclass(frozen=True, slots=True, init=False)
class VocabularyAtlasCandidateSource:
    """Verified, release-scoped reader for a published atlas distribution."""

    candidate_selection: CandidateSelectionReceipt
    lookup_index_manifest: Mapping[str, str]
    _members: Mapping[str, _AtlasMember]
    _expressions: tuple[_AtlasExpression, ...]
    usage_ceiling: str = "diagnosticCandidateOnly"

    def __init__(
        self,
        *,
        candidate_selection: CandidateSelectionReceipt,
        lookup_index_manifest: Mapping[str, str],
        members: Mapping[str, _AtlasMember],
        expressions: tuple[_AtlasExpression, ...],
        _verification_token: object,
    ) -> None:
        if _verification_token is not _VERIFIED_ATLAS_READER:
            raise CandidateReleaseError("VocabularyAtlasCandidateSource must be opened from pinned files")
        object.__setattr__(self, "candidate_selection", candidate_selection)
        object.__setattr__(self, "lookup_index_manifest", lookup_index_manifest)
        object.__setattr__(self, "_members", members)
        object.__setattr__(self, "_expressions", expressions)
        object.__setattr__(self, "usage_ceiling", "diagnosticCandidateOnly")

    @classmethod
    def open(
        cls,
        manifest_path: Path | str,
        *,
        expected_asset_id: str,
        expected_manifest_digest: str,
        expected_output_digest: str,
        reference_release_id: str,
        reference_release_digest: str,
        facet_iri: str,
        assignment_role_iri: str,
        resource_route: str,
        lookup_index_manifest: Mapping[str, str],
        nquads_path: Path | str | None = None,
    ) -> Self:
        """Open exact files without importing or reproducing RefSpec."""

        if _ASSET_ID.fullmatch(expected_asset_id) is None:
            raise CandidateReleaseError("expected atlas asset id must be urn:ref:vocabulary-atlas:<64 lowercase hex>")
        expected_manifest_digest = _require_digest(
            expected_manifest_digest,
            "expected atlas manifest digest",
        )
        expected_output_digest = _require_digest(
            expected_output_digest,
            "expected atlas output digest",
        )
        reference_release_id = _require_iri(
            reference_release_id,
            "selected reference release id",
        )
        reference_release_digest = _require_digest(
            reference_release_digest,
            "selected reference release digest",
        )

        resolved_manifest, manifest_raw = _read_regular_file(
            manifest_path,
            "atlas manifest",
        )
        if _digest(manifest_raw) != expected_manifest_digest:
            raise CandidateReleaseError("atlas manifest digest differs from the selected pin")
        manifest = _load_manifest(manifest_raw)
        graph_by_role, output = _validate_manifest(
            manifest,
            expected_asset_id=expected_asset_id,
        )

        selected_nquads = resolved_manifest.parent / "atlas.nq" if nquads_path is None else Path(nquads_path)
        _, nquads_raw = _read_regular_file(selected_nquads, "atlas N-Quads")
        if _digest(nquads_raw) != expected_output_digest:
            raise CandidateReleaseError("atlas N-Quads digest differs from the selected pin")
        if output["digest"] != expected_output_digest:
            raise CandidateReleaseError("atlas manifest output digest differs from the selected pin")
        if output["byteLength"] != len(nquads_raw):
            raise CandidateReleaseError("atlas output byte length differs")
        counts = manifest["counts"]
        dataset = _parse_dataset(
            nquads_raw,
            graph_by_role=graph_by_role,
            output=output,
            counts=counts,
        )

        release_graph = dataset.graph(URIRef(str(graph_by_role["releaseFacts"]["id"])))
        analysis_graph = dataset.graph(URIRef(str(graph_by_role["analysis"]["id"])))
        managed_release_count = sum(1 for item in manifest["inputs"] if item.get("role") == "ManagedReleaseView")
        if counts["managedReleases"] != managed_release_count:
            raise CandidateReleaseError("atlas managed release count differs")

        for member, release in analysis_graph.subject_objects(_ATLAS.memberOfRelease):
            if not isinstance(member, URIRef) or not isinstance(release, URIRef):
                raise CandidateReleaseError("atlas membership must connect two IRIs")
            if (release, RDF.type, _RKAF.ReferenceResourceRelease) not in release_graph:
                raise CandidateReleaseError("atlas membership release is not a ReferenceResourceRelease")
            release_digests = tuple(release_graph.objects(release, _RKAF.referenceReleaseDigest))
            release_digest = _one_literal(
                release_digests,
                f"atlas release {release} digest",
            )
            _require_digest(str(release_digest), f"atlas release {release} digest")
            if (release, PROV.hadMember, member) not in release_graph:
                raise CandidateReleaseError("atlas analysis membership is absent from release facts")

        release = URIRef(reference_release_id)
        if (release, RDF.type, _RKAF.ReferenceResourceRelease) not in release_graph:
            raise CandidateReleaseError("selected release is not an atlas ReferenceResourceRelease")
        digest_literal = _one_literal(
            tuple(release_graph.objects(release, _RKAF.referenceReleaseDigest)),
            "selected reference release digest",
        )
        if str(digest_literal) != reference_release_digest:
            raise CandidateReleaseError("selected reference release digest differs")

        selected_members = tuple(sorted(release_graph.objects(release, PROV.hadMember), key=str))
        if not selected_members or any(not isinstance(member, URIRef) for member in selected_members):
            raise CandidateReleaseError("selected release must contain IRI members")

        text_predicates = (
            SKOS.prefLabel,
            SKOS.altLabel,
            SKOS.hiddenLabel,
            SKOS.definition,
        )
        members: dict[str, _AtlasMember] = {}
        expressions: list[_AtlasExpression] = []
        for member in selected_members:
            assert isinstance(member, URIRef)
            if (member, _ATLAS.memberOfRelease, release) not in analysis_graph:
                raise CandidateReleaseError(f"atlas analysis omits selected member {member}")
            if (member, RDF.type, _RKAF.RegisteredConcept) not in release_graph:
                raise CandidateReleaseError(f"atlas selected member {member} is not a RegisteredConcept")
            scheme = _one_uri(
                tuple(release_graph.objects(member, SKOS.inScheme)),
                f"atlas member {member} scheme",
            )
            types = sorted(str(value) for value in release_graph.objects(member, RDF.type))
            record: dict[str, Any] = {
                "@id": str(member),
                "@type": types[0] if len(types) == 1 else types,
                str(SKOS.inScheme): str(scheme),
            }
            for predicate in text_predicates:
                language_values: dict[str, list[str]] = {}
                for value in release_graph.objects(member, predicate):
                    if not isinstance(value, Literal):
                        raise CandidateReleaseError(f"atlas member {member} {predicate} must be a literal")
                    language = value.language
                    expressions.append(
                        _AtlasExpression(
                            member_iri=str(member),
                            original_literal=str(value),
                            language_tag=language,
                            semantic_property_iri=str(predicate),
                        )
                    )
                    language_values.setdefault(language or "@none", []).append(str(value))
                if language_values:
                    record[str(predicate)] = {
                        language: values[0] if len(values) == 1 else sorted(values)
                        for language, values in sorted(language_values.items())
                    }
            members[str(member)] = _AtlasMember(
                member_iri=str(member),
                release_iri=reference_release_id,
                scheme_iri=str(scheme),
                record=MappingProxyType(record),
            )

        expressions.sort(
            key=lambda item: (
                item.member_iri,
                item.semantic_property_iri,
                item.language_tag or "",
                item.original_literal,
            )
        )
        source_asset = {
            "type": "VocabularyAtlasAsset",
            "assetId": expected_asset_id,
            "manifestDigest": expected_manifest_digest,
            "outputDigest": expected_output_digest,
        }
        return cls(
            candidate_selection=CandidateSelectionReceipt(
                source_asset=source_asset,
                reference_resource_release={
                    "id": reference_release_id,
                    "digest": reference_release_digest,
                },
                facet_iri=facet_iri,
                assignment_role_iri=assignment_role_iri,
                resource_route=resource_route,
            ),
            lookup_index_manifest=_frozen_reference(
                lookup_index_manifest,
                "lookup index manifest",
            ),
            members=MappingProxyType(members),
            expressions=tuple(expressions),
            _verification_token=_VERIFIED_ATLAS_READER,
        )

    def lookup_member(self, member_iri: str) -> _AtlasMember | None:
        """Resolve only an exact member of the selected release."""

        return self._members.get(member_iri)

    def iter_expressions(
        self,
        *,
        member_iri: str | None = None,
    ) -> Iterator[_AtlasExpression]:
        """Yield only SKOS strings attached to selected exact members."""

        for expression in self._expressions:
            if member_iri is None or expression.member_iri == member_iri:
                yield expression


class CandidateMapping(Protocol):
    """One explicit mapping carried by the legacy lookup bridge."""

    mapping_iri: str
    source_member_iri: str
    relation_iri: str
    target_member_iri: str
    source_release_iri: str
    target_release_iri: str


class CandidateBridgeConcept(Protocol):
    """One source-domain concept retained by the legacy lookup bridge."""

    concept_iri: str
    preferred_labels: Mapping[str, str]
    alternate_labels: Mapping[str, Sequence[str]]
    definitions: Mapping[str, Sequence[str]]
    evidence_url: str


@runtime_checkable
class CandidateConceptBridge(Protocol):
    """Compatibility view of an optional development-only concept bridge."""

    development_only: bool
    source_scheme_iri: str
    source_release_iri: str
    target_release_iri: str
    source_concepts: Sequence[CandidateBridgeConcept]
    mappings: Sequence[CandidateMapping]
    artifact_sha256: str

"""Publish source-profile facts without importing another product's code."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from spicy_regs.source_profiles import SOURCE_PROFILES, STEP4_ACTIVE_SOURCE_TABLES

PROFILE_CATALOG_FORMAT = "spicyregs-source-profile-catalog/experimental-v0"
APPLICABILITY_INPUT_FORMAT = "spicyregs-profile-resource-applicability-input/experimental-v0"
APPLICABILITY_FORMAT = "spicyregs-profile-resource-applicability/experimental-v0"
REFSPEC_CATALOG_FORMAT = "refspec-resource-catalog/experimental-v0"

RESOURCE_RELATIONSHIPS = {
    "nativeCodeOrClassification",
    "nativeIdentifier",
    "nativeStructure",
    "sourceAssignedVocabulary",
}

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class SourceProfileArtifactError(ValueError):
    """Raised when a source-profile artifact is incomplete or inconsistent."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise SourceProfileArtifactError(f"duplicate JSON object key {key!r}")
        value[key] = item
    return value


def load_json(path: Path) -> dict[str, Any]:
    """Read one strict JSON object."""

    def reject_constant(value: str) -> None:
        raise SourceProfileArtifactError(f"non-finite JSON number {value!r}")

    value = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    if not isinstance(value, dict):
        raise SourceProfileArtifactError(f"{path} must contain one JSON object")
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def render_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2) + "\n"


def _require_keys(value: Mapping[str, Any], expected: set[str], location: str) -> None:
    actual = set(value)
    if actual != expected:
        raise SourceProfileArtifactError(
            f"{location} keys differ; missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def _string(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceProfileArtifactError(f"{location} must be a non-empty string")
    return value


def _strings(value: Any, location: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise SourceProfileArtifactError(
            f"{location} must be a{' possibly empty' if allow_empty else ' non-empty'} list"
        )
    result = [_string(item, f"{location}[]") for item in value]
    if len(set(result)) != len(result):
        raise SourceProfileArtifactError(f"{location} contains duplicate values")
    return result


def _digest(value: Any, location: str) -> str:
    digest = _string(value, location)
    if not _SHA256.fullmatch(digest):
        raise SourceProfileArtifactError(f"{location} must be a lowercase SHA-256 digest")
    return digest


def _seal(payload: Mapping[str, Any], *, kind: str) -> dict[str, Any]:
    digest = canonical_sha256(payload)
    return {
        **payload,
        f"{kind}Digest": digest,
        f"{kind}Id": f"urn:spicy-regs:{kind.replace('Catalog', '-catalog').replace('applicability', 'applicability')}:"
        + digest.removeprefix("sha256:"),
    }


def _verify_seal(value: Mapping[str, Any], *, kind: str) -> None:
    digest_field = f"{kind}Digest"
    id_field = f"{kind}Id"
    digest = _digest(value.get(digest_field), digest_field)
    payload = {key: item for key, item in value.items() if key not in {digest_field, id_field}}
    if digest != canonical_sha256(payload):
        raise SourceProfileArtifactError(f"{kind} digest does not match its canonical payload")
    expected_id = (
        f"urn:spicy-regs:{kind.replace('Catalog', '-catalog').replace('applicability', 'applicability')}:"
        + digest.removeprefix("sha256:")
    )
    if value.get(id_field) != expected_id:
        raise SourceProfileArtifactError(f"{kind} identifier does not match its digest")


def _verify_refspec_catalog(catalog: Mapping[str, Any]) -> set[str]:
    if catalog.get("format") != REFSPEC_CATALOG_FORMAT:
        raise SourceProfileArtifactError(f"unsupported RefSpec catalog format {catalog.get('format')!r}")
    digest = _digest(catalog.get("catalogDigest"), "RefSpec catalogDigest")
    payload = {key: value for key, value in catalog.items() if key not in {"catalogDigest", "catalogId"}}
    if digest != canonical_sha256(payload):
        raise SourceProfileArtifactError("RefSpec catalog digest does not match its canonical payload")
    expected_id = f"urn:ref:resource-catalog:{digest.removeprefix('sha256:')}"
    if catalog.get("catalogId") != expected_id:
        raise SourceProfileArtifactError("RefSpec catalog identifier does not match its digest")
    resources = catalog.get("resources")
    if not isinstance(resources, list):
        raise SourceProfileArtifactError("RefSpec catalog resources must be a list")
    resource_ids = {
        _string(row.get("resourceId"), "RefSpec catalog resourceId") for row in resources if isinstance(row, Mapping)
    }
    if len(resource_ids) != len(resources):
        raise SourceProfileArtifactError("RefSpec catalog resource identifiers must be unique objects")
    return resource_ids


def build_source_profile_catalog(*, recorded_at: str) -> dict[str, Any]:
    """Build the immutable description of SpicyRegs source profiles."""

    _string(recorded_at, "recordedAt")
    profiles = [
        {
            "access": profile.access.as_dict(),
            "active": profile.source_table in STEP4_ACTIVE_SOURCE_TABLES,
            "allowedSchemes": list(profile.allowed_schemes),
            "idColumns": list(profile.id_columns),
            "mode": profile.mode,
            "profileId": profile.profile_id,
            "regionAdapterId": profile.region_adapter_id,
            "sourceTable": profile.source_table,
            "subjectType": profile.subject_type,
            "textColumns": list(profile.text_columns),
        }
        for profile in sorted(SOURCE_PROFILES, key=lambda item: item.profile_id)
    ]
    payload = {
        "experimental": True,
        "format": PROFILE_CATALOG_FORMAT,
        "profiles": profiles,
        "recordedAt": recorded_at,
        "summary": {
            "activeProfileCount": sum(row["active"] for row in profiles),
            "deferredProfileCount": sum(not row["active"] for row in profiles),
            "profileCount": len(profiles),
        },
    }
    return _seal(payload, kind="profileCatalog")


def _validate_applicability_input(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    _require_keys(value, {"format", "profiles", "recordedAt"}, "applicability input")
    if value["format"] != APPLICABILITY_INPUT_FORMAT:
        raise SourceProfileArtifactError(f"unsupported applicability input format {value['format']!r}")
    _string(value["recordedAt"], "applicability input recordedAt")
    profiles = value["profiles"]
    if not isinstance(profiles, list):
        raise SourceProfileArtifactError("applicability input profiles must be a list")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(profiles):
        if not isinstance(raw, Mapping):
            raise SourceProfileArtifactError(f"applicability profiles[{index}] must be an object")
        _require_keys(raw, {"evidenceFields", "profileId", "resourceRelationships"}, f"profiles[{index}]")
        profile_id = _string(raw["profileId"], f"profiles[{index}].profileId")
        if profile_id in seen:
            raise SourceProfileArtifactError(f"applicability input repeats profile {profile_id!r}")
        seen.add(profile_id)
        evidence_fields = _strings(raw["evidenceFields"], f"profiles[{index}].evidenceFields")
        relationships = raw["resourceRelationships"]
        if not isinstance(relationships, list):
            raise SourceProfileArtifactError(f"profiles[{index}].resourceRelationships must be a list")
        checked: list[dict[str, Any]] = []
        seen_resources: set[str] = set()
        for relationship_index, relationship in enumerate(relationships):
            if not isinstance(relationship, Mapping):
                raise SourceProfileArtifactError(
                    f"profiles[{index}].resourceRelationships[{relationship_index}] must be an object"
                )
            _require_keys(
                relationship,
                {"relationships", "resourceId"},
                f"profiles[{index}].resourceRelationships[{relationship_index}]",
            )
            resource_id = _string(relationship["resourceId"], "resource relationship resourceId")
            if resource_id in seen_resources:
                raise SourceProfileArtifactError(f"profile {profile_id} repeats resource {resource_id!r}")
            seen_resources.add(resource_id)
            uses = _strings(relationship["relationships"], f"profile {profile_id} relationships")
            if unknown := set(uses) - RESOURCE_RELATIONSHIPS:
                raise SourceProfileArtifactError(
                    f"profile {profile_id} uses unsupported resource relationships: {sorted(unknown)}"
                )
            checked.append({"relationships": sorted(uses), "resourceId": resource_id})
        result.append(
            {
                "evidenceFields": evidence_fields,
                "profileId": profile_id,
                "resourceRelationships": sorted(checked, key=lambda row: row["resourceId"]),
            }
        )
    return result


def build_profile_resource_applicability(
    applicability_input: Mapping[str, Any],
    profile_catalog: Mapping[str, Any],
    refspec_catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Join source facts to exact profile and RefSpec catalog identities."""

    _verify_seal(profile_catalog, kind="profileCatalog")
    if profile_catalog.get("format") != PROFILE_CATALOG_FORMAT:
        raise SourceProfileArtifactError(f"unsupported profile catalog format {profile_catalog.get('format')!r}")
    resource_ids = _verify_refspec_catalog(refspec_catalog)
    profiles = _validate_applicability_input(applicability_input)
    declared_profile_ids = {profile.profile_id for profile in SOURCE_PROFILES}
    input_profile_ids = {row["profileId"] for row in profiles}
    if input_profile_ids != declared_profile_ids:
        raise SourceProfileArtifactError(
            "applicability profile coverage differs; "
            f"missing={sorted(declared_profile_ids - input_profile_ids)}, "
            f"extra={sorted(input_profile_ids - declared_profile_ids)}"
        )
    catalog_profiles = profile_catalog.get("profiles")
    if not isinstance(catalog_profiles, Sequence) or isinstance(catalog_profiles, (str, bytes)):
        raise SourceProfileArtifactError("profile catalog profiles must be a list")
    if {row.get("profileId") for row in catalog_profiles if isinstance(row, Mapping)} != declared_profile_ids:
        raise SourceProfileArtifactError("profile catalog does not describe the declared profiles")
    referenced_resources = {
        relationship["resourceId"] for profile in profiles for relationship in profile["resourceRelationships"]
    }
    if unknown := referenced_resources - resource_ids:
        raise SourceProfileArtifactError(f"applicability references unknown RefSpec resources: {sorted(unknown)}")

    output_profiles = []
    for row in sorted(profiles, key=lambda item: item["profileId"]):
        relationships = row["resourceRelationships"]
        output_profiles.append(
            {
                **row,
                "knownGap": (
                    "No source-native controlled-resource relationship is declared for this profile."
                    if not relationships
                    else "Evidence is limited to the declared source-native fields; distribution availability is stated by the pinned RefSpec catalog."
                ),
            }
        )

    payload = {
        "experimental": True,
        "format": APPLICABILITY_FORMAT,
        "profileCatalog": {
            "digest": profile_catalog["profileCatalogDigest"],
            "id": profile_catalog["profileCatalogId"],
        },
        "profiles": output_profiles,
        "recordedAt": applicability_input["recordedAt"],
        "refspecResourceCatalog": {
            "digest": refspec_catalog["catalogDigest"],
            "id": refspec_catalog["catalogId"],
        },
        "summary": {
            "profileCount": len(output_profiles),
            "resourceRelationshipCount": sum(len(row["resourceRelationships"]) for row in output_profiles),
            "referencedResourceCount": len(referenced_resources),
        },
    }
    return _seal(payload, kind="applicability")


def validate_source_profile_artifacts(
    profile_catalog: Mapping[str, Any],
    applicability: Mapping[str, Any],
    applicability_input: Mapping[str, Any],
    refspec_catalog: Mapping[str, Any],
) -> None:
    """Require both checked artifacts to equal deterministic generation."""

    expected_profile_catalog = build_source_profile_catalog(
        recorded_at=_string(applicability_input.get("recordedAt"), "recordedAt")
    )
    if profile_catalog != expected_profile_catalog:
        raise SourceProfileArtifactError("checked source-profile catalog differs from deterministic generation")
    expected_applicability = build_profile_resource_applicability(
        applicability_input,
        profile_catalog,
        refspec_catalog,
    )
    if applicability != expected_applicability:
        raise SourceProfileArtifactError("checked profile-resource applicability differs from deterministic generation")

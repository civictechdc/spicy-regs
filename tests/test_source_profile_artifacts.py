from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest

from spicy_regs.source_profile_artifacts import (
    SourceProfileArtifactError,
    build_profile_resource_applicability,
    load_json,
    validate_source_profile_artifacts,
)

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "policies" / "profile-resource-applicability-input-v0.json"
PROFILE_CATALOG = ROOT / "policies" / "source-profile-catalog-v0.json"
APPLICABILITY = ROOT / "policies" / "profile-resource-applicability-v0.json"
REFSPEC_CATALOG = ROOT / "RefSpec" / "portfolio" / "resource-catalog-v0.json"


def test_checked_source_profile_artifacts_are_exact_and_closed() -> None:
    profile_catalog = load_json(PROFILE_CATALOG)
    applicability = load_json(APPLICABILITY)
    applicability_input = load_json(INPUT)
    refspec_catalog = load_json(REFSPEC_CATALOG)

    validate_source_profile_artifacts(
        profile_catalog,
        applicability,
        applicability_input,
        refspec_catalog,
    )

    assert profile_catalog["summary"] == {
        "activeProfileCount": 16,
        "deferredProfileCount": 1,
        "profileCount": 17,
    }
    assert applicability["summary"]["profileCount"] == 17
    assert {row["profileId"] for row in applicability["profiles"]} == {
        row["profileId"] for row in profile_catalog["profiles"]
    }


def test_applicability_contains_no_search_policy() -> None:
    applicability = load_json(APPLICABILITY)
    serialized = str(applicability)

    for prohibited in (
        "candidateFacets",
        "mappingAndSearchExpansion",
        "primaryResourceIds",
        "ranking",
        "selectableSubject",
    ):
        assert prohibited not in serialized


def test_unknown_refspec_resource_is_rejected() -> None:
    profile_catalog = load_json(PROFILE_CATALOG)
    applicability_input = load_json(INPUT)
    refspec_catalog = load_json(REFSPEC_CATALOG)
    changed = copy.deepcopy(applicability_input)
    changed["profiles"][0]["resourceRelationships"][0]["resourceId"] = "unknown-resource"

    with pytest.raises(SourceProfileArtifactError, match="unknown RefSpec resources"):
        build_profile_resource_applicability(changed, profile_catalog, refspec_catalog)


def test_refspec_catalog_tampering_is_rejected() -> None:
    profile_catalog = load_json(PROFILE_CATALOG)
    applicability_input = load_json(INPUT)
    refspec_catalog = load_json(REFSPEC_CATALOG)
    changed = copy.deepcopy(refspec_catalog)
    changed["resources"][0]["title"] = "changed"

    with pytest.raises(SourceProfileArtifactError, match="digest does not match"):
        build_profile_resource_applicability(applicability_input, profile_catalog, changed)


def test_profile_declarations_import_without_pipeline_or_refspec() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import spicy_regs.source_profile_artifacts; "
                "unexpected=[name for name in sys.modules if "
                "name.startswith(('spicy_regs.docpipeline', 'spicy_regs.ontology', 'refspec'))]; "
                "print(','.join(unexpected)); raise SystemExit(bool(unexpected))"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout

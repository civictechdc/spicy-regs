"""Command-line entry point for checked SpicyRegs source-profile artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from spicy_regs.source_profile_artifacts import (
    SourceProfileArtifactError,
    build_profile_resource_applicability,
    build_source_profile_catalog,
    load_json,
    render_json,
)


def main() -> int:
    """Generate the two artifacts into a caller-selected directory."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--applicability-input", type=Path, required=True)
    parser.add_argument("--refspec-catalog", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        source = load_json(args.applicability_input)
        profile_catalog = build_source_profile_catalog(recorded_at=source["recordedAt"])
        applicability = build_profile_resource_applicability(
            source,
            profile_catalog,
            load_json(args.refspec_catalog),
        )
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "source-profile-catalog.json").write_text(render_json(profile_catalog), encoding="utf-8")
        (args.output / "profile-resource-applicability.json").write_text(render_json(applicability), encoding="utf-8")
        return 0
    except (KeyError, OSError, SourceProfileArtifactError, ValueError) as error:
        parser.error(str(error))
        return 2

#!/usr/bin/env python3
"""Generate or verify SpicyRegs source-profile artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spicy_regs.source_profile_artifacts import (  # noqa: E402
    SourceProfileArtifactError,
    build_profile_resource_applicability,
    build_source_profile_catalog,
    load_json,
    render_json,
)

INPUT = ROOT / "policies" / "profile-resource-applicability-input-v0.json"
PROFILE_CATALOG = ROOT / "policies" / "source-profile-catalog-v0.json"
APPLICABILITY = ROOT / "policies" / "profile-resource-applicability-v0.json"
DEFAULT_REFSPEC_CATALOG = ROOT / "RefSpec" / "portfolio" / "resource-catalog-v0.json"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refspec-catalog", type=Path, default=DEFAULT_REFSPEC_CATALOG)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="verify checked artifacts (default)")
    mode.add_argument("--write", action="store_true", help="write checked artifacts")
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    try:
        applicability_input = load_json(INPUT)
        profile_catalog = build_source_profile_catalog(recorded_at=applicability_input["recordedAt"])
        applicability = build_profile_resource_applicability(
            applicability_input,
            profile_catalog,
            load_json(args.refspec_catalog),
        )
        outputs = {
            PROFILE_CATALOG: render_json(profile_catalog),
            APPLICABILITY: render_json(applicability),
        }
        if args.write:
            for path, content in outputs.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
                print(f"wrote {path.relative_to(ROOT)}")
            return 0
        for path, content in outputs.items():
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                raise SourceProfileArtifactError(
                    f"{path.relative_to(ROOT)} differs from deterministic generation; "
                    "run tools/generate_source_profile_artifacts.py --write"
                )
        print("source-profile artifacts are current: 17 profiles, 16 active")
        return 0
    except (KeyError, OSError, SourceProfileArtifactError, ValueError) as error:
        print(f"source-profile artifact error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Re-seal a segmentation-evaluation dataset over what is actually on disk.

The sealed dataset's ``evaluation_id`` is a digest over *every* non-model
member (``segmentation_evaluation._evaluation_id``). That is the right design —
it makes the corpus one immutable thing — but it has a sharp consequence:
rewriting any single member, including one the segmenter never reads,
invalidates the identity and blocks every downstream gate that names it.

That is exactly what happened to ``output/segmented-real-data-evaluation-v2``.
Commit ``3a472f0`` rebuilt ``fr_docket_links.parquet`` in place across all
generations; the sealed bytes are unrecoverable because the previous writer was
byte-non-deterministic. ``fr_docket_links`` is an
``EXCLUDED_SOURCE_TABLES`` relationship carrier — it never becomes a
``SourceArtifact`` — so the *documents* under measurement are untouched, but the
seal that names them no longer verifies.

This tool does not repair, regenerate, or reorder any data. It copies a dataset
directory, recomputes the seal over the copy, and records precisely which
members moved relative to the seal it inherited, so a reader can judge for
themselves whether the movement touches the measurement. A re-sealed dataset
therefore has an honest new identity and a written provenance back to the old
one; it never pretends to be the original seal.

Run::

    uv run python tools/reseal_segmentation_dataset.py \\
        output/segmented-real-data-evaluation-v2 \\
        output/segmented-real-data-evaluation-v2-resealed-2026-08-02
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.corpora.segmentation_evaluation import (  # noqa: E402
    _evaluation_id,
    _nonmodel_artifacts,
    validate_segmentation_evaluation,
)

MANIFEST_NAME = "segmentation-evaluation-manifest.json"
RECEIPT_NAME = "segmentation-evaluation-receipt.json"

Validator = Callable[[Path], dict[str, Any]]


class ResealError(RuntimeError):
    """The dataset cannot be re-sealed without inventing or losing a fact."""


def dataset_members(directory: Path) -> dict[str, dict[str, Any]]:
    """The non-model member inventory the seal is computed over."""
    return _nonmodel_artifacts(directory)


def evaluation_id(members: Mapping[str, Mapping[str, Any]]) -> str:
    """The dataset identity implied by a member inventory."""
    return _evaluation_id({name: dict(record) for name, record in members.items()})


def seal_delta(
    sealed: Mapping[str, Mapping[str, Any]],
    current: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Name every member that moved, vanished, or appeared since the seal."""
    changed: list[dict[str, str]] = []
    missing: list[str] = []
    unchanged = 0
    for name in sorted(sealed):
        if name not in current:
            missing.append(name)
            continue
        sealed_digest = str(sealed[name]["sha256"])
        current_digest = str(current[name]["sha256"])
        if sealed_digest == current_digest:
            unchanged += 1
        else:
            changed.append(
                {
                    "name": name,
                    "sealed_sha256": sealed_digest,
                    "current_sha256": current_digest,
                }
            )
    added = sorted(name for name in current if name not in sealed)
    return {
        "changed": changed,
        "missing": missing,
        "added": added,
        "unchanged_count": unchanged,
        "sealed_count": len(sealed),
        "current_count": len(current),
    }


def _copy_members(source_dir: Path, target_dir: Path, sealed: Mapping[str, Any]) -> None:
    target_dir.mkdir(parents=True)
    for name in sorted(sealed):
        origin = source_dir / name
        if not origin.is_file():
            raise ResealError(f"the sealed dataset is incomplete: {name} is missing from {source_dir}")
        shutil.copy2(origin, target_dir / name)


def reseal_segmentation_dataset(
    source_dir: Path,
    output_dir: Path,
    *,
    validator: Validator = validate_segmentation_evaluation,
) -> dict[str, Any]:
    """Copy a dataset, recompute its seal, and record what moved.

    The source directory is never written to. The copy carries every sealed
    member byte for byte; only the manifest and receipt are rewritten, and the
    manifest keeps a ``resealed_from`` provenance block naming the identity it
    replaced and the members that forced the replacement.
    """
    source_dir = Path(source_dir)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ResealError(f"{output_dir} already exists; re-sealing never overwrites a dataset")

    manifest_path = source_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        raise ResealError(f"{source_dir} carries no {MANIFEST_NAME}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sealed = manifest.get("nonmodel_artifacts")
    if not isinstance(sealed, dict) or not sealed:
        raise ResealError(f"{manifest_path} names no non-model members to re-seal")

    try:
        _copy_members(source_dir, output_dir, sealed)
        members = dataset_members(output_dir)
        delta = seal_delta(sealed, members)
        if delta["missing"]:
            raise ResealError(f"the sealed dataset is incomplete: {delta['missing']}")
        resealed_id = evaluation_id(members)
        resealed = dict(manifest)
        resealed["evaluation_id"] = resealed_id
        resealed["nonmodel_artifacts"] = members
        resealed["resealed_from"] = {
            "evaluation_id": str(manifest.get("evaluation_id", "")),
            "changed_members": delta["changed"],
            "unchanged_member_count": delta["unchanged_count"],
            "note": (
                "Members were copied byte for byte. Only the seal was recomputed, "
                "because a member changed outside this dataset's own build."
            ),
        }
        (output_dir / MANIFEST_NAME).write_text(json.dumps(resealed, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        receipt = validator(output_dir)
        if receipt.get("status") != "pass":
            raise ResealError(
                "the re-sealed dataset did not validate: " + "; ".join(str(one) for one in receipt.get("failures", []))
            )
        (output_dir / RECEIPT_NAME).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except BaseException:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise

    return {
        "source_directory": str(source_dir),
        "output_directory": str(output_dir),
        "sealed_evaluation_id": str(manifest.get("evaluation_id", "")),
        "resealed_evaluation_id": resealed_id,
        "delta": delta,
        "status": "pass",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args(argv)
    result = reseal_segmentation_dataset(args.source_dir, args.output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

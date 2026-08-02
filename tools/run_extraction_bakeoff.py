"""Drive the extraction bakeoff: one candidate per process, two runs, one summary.

Each candidate runs out-of-process via ``tools/extraction_bakeoff_worker.py`` in
whichever interpreter has it installed, so no candidate's imports enter the
project environment and so wall time and peak RSS belong to that candidate alone.

Determinism is measured, not assumed: every candidate runs **twice, in two
separate processes**, and the two per-file digest lists are compared. Separate
processes is the point — a same-process rerun cannot catch output that depends on
``PYTHONHASHSEED``, and this project has already lost a day to a drift of exactly
that shape.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_PYTHON = ".venv/bin/python"
SCRATCH_PYTHON = "/tmp/xbake/venv/bin/python"

#: Which interpreter each candidate needs. The project environment holds the
#: incumbent and Docling; everything else lives in the throwaway venv so an
#: unadopted candidate never becomes a project dependency by accident.
PROJECT_CANDIDATES = frozenset({"incumbent", "incumbent_visible", "docling", "pypdf", "lxml", "lxml_structural", "bs4"})

MEAN_KEYS = ("anchor_tok", "anchor_tri", "unit_exact", "deletion", "insertion", "elapsed_s")


def _run(candidate: str, media: str, listing: Path, *, full: bool, python: str) -> dict[str, Any]:
    command = [
        python,
        "tools/extraction_bakeoff_worker.py",
        "--candidate",
        candidate,
        "--media",
        media,
        "--files",
        str(listing),
    ]
    if full:
        command.append("--full-metrics")
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0 or not completed.stdout.strip():
        return {"candidate": candidate, "media": media, "fatal": (completed.stderr or "")[-400:], "rows": []}
    return json.loads(completed.stdout)


def _summarize(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("rows", [])
    ok = [row for row in rows if "error" not in row]
    failed = [row for row in rows if "error" in row]
    summary: dict[str, Any] = {
        "candidate": payload.get("candidate"),
        "media": payload.get("media"),
        "files": len(rows),
        "ok": len(ok),
        "failed": len(failed),
        "errors": sorted({row["error"].split(":")[0] for row in failed})[:5],
        "total_s": payload.get("total_s"),
        "peak_rss_mb": payload.get("peak_rss_mb"),
        "versions": payload.get("versions"),
    }
    for key in MEAN_KEYS:
        values = [row[key] for row in ok if isinstance(row.get(key), (int, float))]
        summary[key] = round(statistics.fmean(values), 6) if values else None
        if key in {"anchor_tri", "unit_exact"} and values:
            summary[f"{key}_min"] = round(min(values), 6)
    structures = [row["structure"] for row in ok if isinstance(row.get(key := "structure"), int)]
    summary["structure_mean"] = round(statistics.fmean(structures), 1) if structures else None
    markers = [row["fr_markers"] for row in ok if isinstance(row.get("fr_markers"), int)]
    reference_markers = [row["fr_markers_ref"] for row in ok if isinstance(row.get("fr_markers_ref"), int)]
    if markers and reference_markers:
        summary["fr_markers_kept"] = sum(markers)
        summary["fr_markers_available"] = sum(reference_markers)
        summary["fr_marker_loss_docs"] = sum(1 for row in ok if row.get("fr_markers", 0) < row.get("fr_markers_ref", 0))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True, choices=["text/html", "application/xml", "application/pdf"])
    parser.add_argument("--files", required=True, help="newline-delimited file of input paths")
    parser.add_argument("--candidates", required=True, help="comma-separated candidate names")
    parser.add_argument("--output", required=True, help="directory for the sealed result")
    parser.add_argument("--label", required=True, help="arm name, e.g. fr-html")
    parser.add_argument("--no-full-metrics", action="store_true", help="skip anchoring and loss (PDF arms)")
    arguments = parser.parse_args()

    listing = Path(arguments.files)
    destination = Path(arguments.output)
    destination.mkdir(parents=True, exist_ok=True)
    inputs = [Path(line) for line in listing.read_text().splitlines() if line.strip()]
    corpus_digest = hashlib.sha256(
        b"".join(hashlib.sha256(path.read_bytes()).digest() for path in sorted(inputs))
    ).hexdigest()

    summaries: list[dict[str, Any]] = []
    for candidate in arguments.candidates.split(","):
        candidate = candidate.strip()
        if not candidate:
            continue
        python = REPO_PYTHON if candidate in PROJECT_CANDIDATES else SCRATCH_PYTHON
        first = _run(candidate, arguments.media, listing, full=not arguments.no_full_metrics, python=python)
        summary = _summarize(first)
        second = _run(candidate, arguments.media, listing, full=False, python=python)
        digests_a = [row.get("sha256") for row in first.get("rows", [])]
        digests_b = [row.get("sha256") for row in second.get("rows", [])]
        summary["deterministic"] = bool(digests_a) and digests_a == digests_b
        summary["run_digest"] = hashlib.sha256(json.dumps(digests_a).encode()).hexdigest()[:16]
        summaries.append(summary)
        print(json.dumps(summary), file=sys.stderr, flush=True)
        (destination / f"{arguments.label}-{candidate}.json").write_text(json.dumps(first, indent=1, sort_keys=True))

    report = {
        "schema_version": "extraction-bakeoff-v1",
        "label": arguments.label,
        "media": arguments.media,
        "corpus_files": len(inputs),
        "corpus_digest": corpus_digest,
        "repo_python": sys.version.split()[0],
        "summaries": summaries,
    }
    path = destination / f"{arguments.label}-summary.json"
    path.write_text(json.dumps(report, indent=1, sort_keys=True))
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

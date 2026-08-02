#!/usr/bin/env python
"""Re-derive what the two sealed OLRC sources resolve, through the loaders.

Every number this prints is produced by :mod:`spicy_regs.ontology.act_index`
reading pinned parquet, not by the script that discovered it. That distinction
already mattered once: before ``ActIndex.from_artifact`` existed, the published
corpus counts depended on a collapse policy that lived in no committed code, and
first-wins and last-wins gave different answers from identical bytes.

Three measurements, all re-derivable:

* **corpus** -- the act-relative citations in a sealed detection artifact, how
  many resolve, by which source, and why the rest do not.
* **ambiguous combinations** -- every (act, act section) whose Table III key
  carries more than one classification row for that section. These are the
  combinations the page-range rule narrows but deliberately does not decide;
  this counts how many the second source decides and how many still refuse.
* **source comparison** -- over every key both sources cover, how often they
  agree, how often they disagree, and how often only one of them has anything.
  Table III's coverage bounds this: only the acts its artifact fetched are
  comparable at all, and that denominator is reported next to the counts.

Usage::

    uv run python tools/measure_act_relative_resolution.py \\
        --act-index output/usc-act-index-2026-08-02 \\
        --source-credits output/usc-source-credit-index-2026-08-02 \\
        --detection output/citation-bakeoff-2026-08-02/detection.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.ontology.act_index import (  # noqa: E402
    ActIndex,
    SourceCreditIndex,
    resolve_act_relative_citation,
)
from spicy_regs.ontology.citations import (  # noqa: E402
    ActRelativeCitation,
    canonical_usc_iri,
    find_act_relative_citations,
)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _measure_corpus(detection: Path, index: ActIndex, credits: SourceCreditIndex | None) -> dict:
    payload = json.loads(detection.read_text(encoding="utf-8"))
    names = frozenset(index.table3_key_by_name) | frozenset(index.alias_by_name)
    citations = [
        citation
        for record in payload["records"]
        for citation in find_act_relative_citations(record["text"], act_names=names)
    ]
    resolutions = [resolve_act_relative_citation(c, index=index, source_credits=credits) for c in citations]
    resolved = [r for r in resolutions if r.iri]
    return {
        "citations_found": len(citations),
        "resolved": len(resolved),
        "unresolved": len(resolutions) - len(resolved),
        "resolved_by": dict(sorted(Counter(r.answered_by for r in resolved).items())),
        "unresolved_reasons": dict(sorted(Counter(r.unresolved_reason for r in resolutions if not r.iri).items())),
    }


def _ambiguous_combinations(index: ActIndex) -> list[tuple[str, str]]:
    """Every (act name, act section) whose Table III rows do not decide alone."""
    names_by_key: dict[str, list[str]] = {}
    for name, key in index.table3_key_by_name.items():
        names_by_key.setdefault(key, []).append(name)
    return [
        (name, section)
        for key, sections in index.classifications.items()
        for section, rows in sections.items()
        if len(rows) > 1
        for name in names_by_key.get(key, ())
    ]


def _measure_ambiguous(index: ActIndex, credits: SourceCreditIndex | None) -> dict:
    combinations = _ambiguous_combinations(index)
    resolved_by: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    for name, section in combinations:
        resolution = resolve_act_relative_citation(
            ActRelativeCitation(name, name, section), index=index, source_credits=credits
        )
        # The invariant is that exactly one of these is set; naming both
        # conditions keeps that readable rather than assumed.
        if resolution.answered_by is not None:
            resolved_by[resolution.answered_by] += 1
        elif resolution.unresolved_reason is not None:
            reasons[resolution.unresolved_reason] += 1
    pairs = sum(1 for sections in index.classifications.values() for rows in sections.values() if len(rows) > 1)
    return {
        "ambiguous_pairs": pairs,
        "combinations": len(combinations),
        "resolved": sum(resolved_by.values()),
        "resolved_by": dict(sorted(resolved_by.items())),
        "still_refusing": dict(sorted(reasons.items())),
    }


def _measure_sources(index: ActIndex, credits: SourceCreditIndex) -> dict:
    """Compare the two sources wherever both could have something to say.

    Restricted to public laws Table III was actually fetched for. Comparing
    against acts whose Table III page was never requested would report this
    build's coverage bound as a property of the sources.
    """
    covered = set(index.classifications)
    starts = {key: dict(pairs) for key, pairs in index.division_starts.items()}
    agree = disagree = table3_silent = credit_inexpressible = 0
    disagreements: list[dict[str, str]] = []
    considered = 0
    for (public_law, division, act_section), targets in credits.targets.items():
        if public_law not in covered or len({(t.usc_title, t.usc_section) for t in targets}) > 1:
            continue
        considered += 1
        target = targets[0]
        try:
            credit_iri = canonical_usc_iri(target.usc_title, target.usc_section)
        except ValueError:
            credit_inexpressible += 1
            continue
        rows = index.classifications.get(public_law, {}).get(act_section, ())
        by_division = starts.get(public_law) or {}
        if division in by_division:
            low = by_division[division]
            later = sorted(page for page in by_division.values() if page > low)
            high = later[0] if later else 1 << 30
            rows = tuple(row for row in rows if row[3] is not None and low <= row[3] <= high)
        table3_iris = set()
        for usc_title, usc_section, status, _page in rows:
            if status or not (usc_title and usc_section):
                continue
            try:
                table3_iris.add(canonical_usc_iri(usc_title, usc_section))
            except ValueError:
                continue
        if not table3_iris:
            table3_silent += 1
        elif credit_iri in table3_iris:
            agree += 1
        else:
            disagree += 1
            disagreements.append(
                {
                    "key": f"{public_law}|{division}|{act_section}",
                    "table3": ",".join(sorted(table3_iris)),
                    "source_credits": credit_iri,
                }
            )
    return {
        "table3_public_laws_fetched": len(covered),
        "comparable_unambiguous_triples": considered,
        "table3_has_no_in_division_row": table3_silent,
        "agree": agree,
        "disagree": disagree,
        "credit_target_not_expressible": credit_inexpressible,
        "disagreements": sorted(disagreements, key=lambda row: row["key"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--act-index", type=Path, required=True)
    parser.add_argument("--source-credits", type=Path, default=None)
    parser.add_argument("--detection", type=Path, default=None)
    args = parser.parse_args(argv)

    index = ActIndex.from_artifact(args.act_index)
    credits = SourceCreditIndex.from_artifact(args.source_credits) if args.source_credits else None

    report: dict[str, object] = {"sources": {"act_index": str(args.act_index)}}
    if args.source_credits:
        report["sources"]["source_credits"] = str(args.source_credits)
    if args.detection:
        report["corpus"] = _measure_corpus(args.detection, index, credits)
    report["ambiguous"] = _measure_ambiguous(index, credits)
    if credits is not None:
        report["source_comparison"] = _measure_sources(index, credits)
    print(canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

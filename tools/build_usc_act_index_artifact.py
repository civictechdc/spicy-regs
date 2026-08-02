#!/usr/bin/env python
"""Build the pinned U.S. Code act index: popular names + Table III.

Two tables and a receipt, sealed the way ``build_agency_crosswalk_artifact.py``
and ``build_date_event_artifact.py`` seal theirs. They are the durable inputs
:mod:`spicy_regs.ontology.act_index` resolves against — as scripts they are
unreproducible, as digest-pinned parquet they are consumable.

Outputs, all deterministic and byte-identical across rebuilds from one cache:

* ``usc-popular-names.parquet`` — one row per stated fact in the Popular Name
  Tool: the name, what the tool says about it, its Table III key, its U.S. Code
  anchor, and its alias target.
* ``usc-act-sections.parquet`` — one row per act section Table III classifies,
  for every act reached.
* ``quarantine.parquet`` — every record the build declined, with a reason.
* ``receipt.json`` — input digests, row counts, parser version, the pinned
  rules, and every coverage gap by name.

**Coverage is bounded on purpose.** Table III is one request per act and the
tool lists 12,963 of them, so ``--acts-for`` selects the acts a corpus actually
cites and the receipt records which acts were reached. An act whose page cannot
be read is recorded as ``source_incomplete`` rather than silently missing: a
consumer must see a hole, never a wrong answer.

Usage::

    uv run python tools/build_usc_act_index_artifact.py \\
        --output output/usc-act-index-2026-08-02 \\
        --cache /tmp/olrc-cache \\
        --acts-for output/citation-bakeoff-2026-08-02/detection.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.ontology.act_index import (  # noqa: E402
    ALIAS_MAX_DEPTH,
    ALIAS_YEAR_RULE,
    UNRESOLVED_REASONS,
)
from spicy_regs.ontology.citations import (  # noqa: E402
    find_act_relative_citations,
    normalize_popular_name,
)
from spicy_regs.sources.uscode_olrc import (  # noqa: E402
    POPULAR_NAMES_URL,
    PopularNameIndex,
    fetch,
    parse_popular_names,
    parse_table3,
    table3_url,
)

ARTIFACT_SCHEMA_VERSION = "usc-act-index-artifact-v1"

#: The parser this artifact was produced by. Bump when a parse changes shape,
#: so a receipt cannot silently describe bytes a different parser would read.
PARSER_VERSION = "uscode-olrc-parser-v1"

POPULAR_NAME_COLUMNS = (
    "name",
    "name_key",
    "content_type",
    "table3_key",
    "usc_title",
    "usc_section",
    "see_also",
    "see_also_key",
    "release_point",
)
ACT_SECTION_COLUMNS = ("table3_key", "act_section", "usc_title", "usc_section", "status")
QUARANTINE_COLUMNS = ("source", "reason", "table3_key", "raw_value")

#: A build must not seal a secret. The scan is over what is written, not what
#: was read, so a credential in an environment variable cannot reach the file.
_SECRET_LIKE = re.compile(r"\b(?:sk-(?:proj-)?[A-Za-z0-9_-]{20,}|api[_-]?key=[^\s&]{8,})\b", re.IGNORECASE)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def text_sha256(text: str) -> str:
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _pin_path(path: Path) -> str:
    """Record a repo-relative path when possible, else the basename.

    Keeping absolute scratch paths out of the receipt keeps rebuilds from
    different working directories byte-identical.
    """
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return resolved.name


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    """Write VARCHAR columns in a fixed order, sorted, so bytes are stable."""
    ordered = sorted(rows, key=lambda row: tuple("" if row.get(c) is None else str(row[c]) for c in columns))
    table = pa.table(
        {c: pa.array([None if r.get(c) is None else str(r[c]) for r in ordered], pa.string()) for c in columns}
    )
    pq.write_table(table, path, compression="zstd", sorting_columns=None)


def _scan_for_secrets(rows: list[dict[str, Any]], where: str) -> None:
    for row in rows:
        for key, value in row.items():
            if value is not None and _SECRET_LIKE.search(str(value)):
                raise SystemExit(f"refusing to seal a secret-like value in {where}.{key}")


def acts_cited_by(detection_path: Path, names: frozenset[str]) -> list[str]:
    """The normalized act names a detection artifact's strings actually cite."""
    payload = json.loads(detection_path.read_text(encoding="utf-8"))
    texts = [record["text"] for record in payload["records"]]
    return sorted({c.act_key for text in texts for c in find_act_relative_citations(text, act_names=names)})


def build(output_dir: Path, *, cache_dir: Path | None, detection_path: Path | None, act_keys: list[str]) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    quarantine: list[dict[str, Any]] = []

    popular_names_html = fetch(POPULAR_NAMES_URL, cache_dir=cache_dir)
    records = parse_popular_names(popular_names_html)
    index = PopularNameIndex.from_records(records, normalize=normalize_popular_name)

    name_rows: list[dict[str, Any]] = []
    for record in records:
        if not record.name:
            quarantine.append(
                {"source": "popular_names", "reason": "entry_without_name", "table3_key": None, "raw_value": None}
            )
            continue
        name_rows.append(
            {
                "name": record.name,
                "name_key": normalize_popular_name(record.name),
                "content_type": record.content_type,
                "table3_key": record.table3_key,
                "usc_title": record.usc_title,
                "usc_section": record.usc_section,
                "see_also": record.see_also,
                "see_also_key": normalize_popular_name(record.see_also) if record.see_also else None,
                "release_point": record.release_point,
            }
        )

    wanted = list(act_keys)
    if detection_path is not None:
        wanted = sorted(set(wanted) | set(acts_cited_by(detection_path, index.names)))

    resolved_keys: dict[str, str] = {}
    for name_key in wanted:
        table3_key = index.table3_key(name_key)
        if table3_key is None:
            quarantine.append(
                {
                    "source": "requested_acts",
                    "reason": "act_not_in_index",
                    "table3_key": None,
                    "raw_value": name_key,
                }
            )
            continue
        resolved_keys[name_key] = table3_key

    section_rows: list[dict[str, Any]] = []
    incomplete: list[dict[str, str]] = []
    for table3_key in sorted(set(resolved_keys.values())):
        try:
            document = fetch(table3_url(table3_key), cache_dir=cache_dir)
            parsed = parse_table3(document)
        except Exception as error:  # noqa: BLE001 - any failure is a coverage hole
            parsed, detail = [], f"{type(error).__name__}: {str(error)[:160]}"
        else:
            detail = None if parsed else "page rendered no classification rows"
        if not parsed:
            incomplete.append(
                {
                    "table3_key": table3_key,
                    "url": table3_url(table3_key),
                    "detail": detail or "unknown",
                    "missing": "all classifications for this act",
                }
            )
            quarantine.append(
                {
                    "source": "table3",
                    "reason": "source_incomplete",
                    "table3_key": table3_key,
                    "raw_value": detail,
                }
            )
            continue
        for row in parsed:
            section_rows.append(
                {
                    "table3_key": table3_key,
                    "act_section": row.act_section,
                    "usc_title": row.usc_title,
                    "usc_section": row.usc_section,
                    "status": row.status,
                }
            )

    _scan_for_secrets(name_rows, "usc-popular-names")
    _scan_for_secrets(section_rows, "usc-act-sections")

    names_path = output_dir / "usc-popular-names.parquet"
    sections_path = output_dir / "usc-act-sections.parquet"
    quarantine_path = output_dir / "quarantine.parquet"
    _write_parquet(names_path, POPULAR_NAME_COLUMNS, name_rows)
    _write_parquet(sections_path, ACT_SECTION_COLUMNS, section_rows)
    _write_parquet(quarantine_path, QUARANTINE_COLUMNS, quarantine)

    receipt = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "coverage": {
            "acts_requested": len(wanted),
            "acts_reached": len(set(resolved_keys.values())) - len(incomplete),
            "acts_incomplete": len(incomplete),
            "popular_name_rows": len(name_rows),
            "distinct_names": len({r["name_key"] for r in name_rows}),
            "act_section_rows": len(section_rows),
            "quarantine_rows": len(quarantine),
            "quarantine_reasons": dict(sorted(Counter(r["reason"] for r in quarantine).items())),
        },
        # A named hole, not a silent one. Downstream sees `source_incomplete`
        # for every citation into these acts rather than a wrong answer.
        "source_incomplete": sorted(incomplete, key=lambda row: row["table3_key"]),
        "inputs": {
            "popular_names_url": POPULAR_NAMES_URL,
            "popular_names_digest": text_sha256(popular_names_html),
            "popular_names_bytes": len(popular_names_html.encode("utf-8")),
            "detection_artifact": _pin_path(detection_path) if detection_path else None,
            "detection_digest": file_sha256(detection_path) if detection_path else None,
        },
        "rules": {
            "alias_year_rule": ALIAS_YEAR_RULE,
            "alias_year_rule_derivation": (
                "the trailing year is how the Popular Name Tool distinguishes acts, so an alias "
                "target missing one may have it SUPPLIED but never DROPPED, and only when exactly "
                "one act supplies it; 'Clean Air Act Amendments' would be 1966, 1970 and 1977, and "
                "choosing among them would invent a citation the source never made"
            ),
            "alias_max_depth": ALIAS_MAX_DEPTH,
            "unresolved_reasons": list(UNRESOLVED_REASONS),
            "name_normalization": "casefold, collapse whitespace, strip edge punctuation, straighten apostrophes and dashes",
        },
        "outputs": {
            _pin_path(path): {"digest": file_sha256(path), "rows": pq.ParquetFile(path).metadata.num_rows}
            for path in (names_path, sections_path, quarantine_path)
        },
    }
    receipt_text = canonical_json(receipt)
    if _SECRET_LIKE.search(receipt_text):
        raise SystemExit("refusing to seal a secret-like value in receipt.json")
    (output_dir / "receipt.json").write_text(receipt_text, encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, default=None, help="on-disk fetch cache")
    parser.add_argument("--acts-for", type=Path, default=None, help="detection.json whose strings select the acts")
    parser.add_argument("--act", action="append", default=[], help="an extra popular name to include")
    args = parser.parse_args(argv)
    receipt = build(
        args.output,
        cache_dir=args.cache,
        detection_path=args.acts_for,
        act_keys=[normalize_popular_name(a) for a in args.act],
    )
    print(canonical_json(receipt["coverage"]))
    for hole in receipt["source_incomplete"]:
        print(f"source_incomplete: {hole['table3_key']} -> {hole['detail']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

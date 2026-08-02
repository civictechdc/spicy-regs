#!/usr/bin/env python
"""Build the pinned U.S. Code source-credit index from the OLRC's USLM XML.

The **second** of the two independent sources
:mod:`spicy_regs.ontology.act_index` resolves an act-relative citation against.
The first, Table III, is keyed by the enacting public law alone and cannot tell
apart the dozens of acts one public law may enact. Every section of the Code
carries a source credit that states the division per section, which is exactly
what Table III lacks:

    26 U.S.C. 6038E  <-  (Added Pub. L. 116-260, div. EE, title I, § 107(d)(1),
                          Dec. 27, 2020, 134 Stat. 3048.)

Sealed the way ``build_usc_act_index_artifact.py`` and
``build_agency_crosswalk_artifact.py`` seal theirs: deterministic rows, a
digest-pinned receipt, repo-relative paths, a secret scan over what is written,
and byte-identical rebuilds.

**It is not a tiebreaker over Table III, and it is not built as one.** The two
sources have different coverage: measured on release point 119-102, of the 222
unambiguous triples here whose public law Table III was also fetched for, 176
have no in-division Table III row at all. 26 U.S.C. 6038E is one of them. A
disagreement between the sources is a coverage fact about one of them, and the
resolver refuses rather than picking a winner.

Outputs, all deterministic and byte-identical across rebuilds from one archive:

* ``usc-source-credits.parquet`` -- one row per retained credit:
  ``(public law, division, act section) -> U.S. Code section``, carrying the
  Statutes at Large volume and page and the verbatim USLM identifier. A triple
  naming several sections is **kept and marked** ``refusal='multi_target'``,
  because "the source said two things" is a different fact from "the source said
  nothing".
* ``quarantine.parquet`` -- every credit the strict rule matched and the build
  could not attribute, with a reason.
* ``receipt.json`` -- the archive and per-title input digests, every count, and
  the pinned rules with their derivations.

Usage::

    uv run python tools/build_usc_source_credit_artifact.py \\
        --output output/usc-source-credit-index-2026-08-02 \\
        --archive /tmp/uscall.zip \\
        --release-point 119-102
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.sources.uscode_uslm import (  # noqa: E402
    QUARANTINE_REASONS,
    STRICT_ENACTMENT_RULE,
    USLM_SECTION_DASH_RULE,
    scan_release_zip,
    uslm_release_url,
)

ARTIFACT_SCHEMA_VERSION = "usc-source-credit-artifact-v1"

#: The parser this artifact was produced by. Bump when a parse changes shape, so
#: a receipt cannot silently describe bytes a different parser would read.
PARSER_VERSION = "uscode-uslm-parser-v1"

#: How a triple naming more than one U.S. Code section is carried.
MULTI_TARGET_POLICY = "retain-and-mark-refusal-multi_target-v1"

CREDIT_COLUMNS = (
    "public_law",
    "division",
    "act_section",
    "usc_title",
    "usc_section",
    "usc_identifier",
    "statutes_at_large_volume",
    "statutes_at_large_page",
    "target_count",
    "refusal",
)
QUARANTINE_COLUMNS = ("source", "reason", "public_law", "division", "act_section", "raw_value")

#: A build must not seal a secret. The scan is over what is written, not what was
#: read, so a credential in an environment variable cannot reach the file.
_SECRET_LIKE = re.compile(r"\b(?:sk-(?:proj-)?[A-Za-z0-9_-]{20,}|api[_-]?key=[^\s&]{8,})\b", re.IGNORECASE)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


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


def build(output_dir: Path, *, archive: Path, release_point: str) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    scan, members = scan_release_zip(archive)

    targets: dict[tuple[str, str, str], set[tuple[str, str]]] = defaultdict(set)
    for credit in scan.credits:
        targets[(credit.public_law, credit.division, credit.act_section)].add((credit.usc_title, credit.usc_section))

    credit_rows: list[dict[str, Any]] = []
    for credit in scan.credits:
        key = (credit.public_law, credit.division, credit.act_section)
        count = len(targets[key])
        credit_rows.append(
            {
                "public_law": credit.public_law,
                "division": credit.division,
                "act_section": credit.act_section,
                "usc_title": credit.usc_title,
                "usc_section": credit.usc_section,
                "usc_identifier": credit.usc_identifier,
                "statutes_at_large_volume": credit.statutes_at_large_volume,
                "statutes_at_large_page": credit.statutes_at_large_page,
                "target_count": str(count),
                # Kept and marked, never dropped: "the source said two things"
                # is a different fact from "the source said nothing".
                "refusal": "multi_target" if count > 1 else None,
            }
        )

    quarantine_rows = [
        {
            "source": "uslm_source_credit",
            "reason": entry.reason,
            "public_law": entry.public_law,
            "division": entry.division,
            "act_section": entry.act_section,
            "raw_value": entry.raw_value or None,
        }
        for entry in scan.quarantine
    ]

    _scan_for_secrets(credit_rows, "usc-source-credits")
    _scan_for_secrets(quarantine_rows, "quarantine")

    credits_path = output_dir / "usc-source-credits.parquet"
    quarantine_path = output_dir / "quarantine.parquet"
    _write_parquet(credits_path, CREDIT_COLUMNS, credit_rows)
    _write_parquet(quarantine_path, QUARANTINE_COLUMNS, quarantine_rows)

    receipt = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "coverage": {
            "titles": len(members),
            "source_credits_scanned": scan.credits_scanned,
            "credits_naming_a_division": scan.credits_naming_a_division,
            "credits_outside_a_section": scan.credits_outside_a_section,
            "strict_matches": scan.strict_matches,
            "triples": len(targets),
            "unambiguous_triples": sum(1 for v in targets.values() if len(v) == 1),
            "multi_target_triples": sum(1 for v in targets.values() if len(v) > 1),
            "distinct_public_law_division_pairs": len({(k[0], k[1]) for k in targets}),
            "rows": len(credit_rows),
            "rows_refusing": sum(1 for r in credit_rows if r["refusal"]),
            "rows_without_a_statutes_at_large_page": sum(1 for r in credit_rows if not r["statutes_at_large_page"]),
            "quarantine_rows": len(quarantine_rows),
            "quarantine_reasons": dict(sorted(Counter(r["reason"] for r in quarantine_rows).items())),
        },
        "inputs": {
            "release_point": release_point,
            "release_url": uslm_release_url(release_point),
            "archive": _pin_path(archive),
            "archive_digest": file_sha256(archive),
            "archive_bytes": archive.stat().st_size,
            # The archive digest says which bundle; the member digests say which
            # bytes each count was read from.
            "titles": [{"member": member, "digest": digest} for member, digest in members],
        },
        "rules": {
            "strict_enactment_rule": STRICT_ENACTMENT_RULE,
            "strict_enactment_rule_derivation": (
                "A source credit lists the original enactment AND every later amendment, so an "
                "expression accepting any 'Pub. L. N-M, div. X ... section S' anywhere in a credit "
                "pairs a division with a section number belonging to a different citation in the "
                "same credit -- measured, 13,122 triples of which 2,916 name more than one U.S. "
                "Code section. Reading the role by proximity to the word 'amended' does not fix "
                "it: it credits 26 U.S.C. 7652 to (116-260, div. EE, sec. 107), which enacted "
                "26 U.S.C. 6038E, and 7652's credit never uses the word 'amended' at all -- it "
                "names the same act section at 134 Stat. 3046 where the enactment sits at 3048. "
                "Requiring an explicit enactment construction ('Added Pub. L. ...' or 'as added "
                "Pub. L. ...') removes that false positive. What it does not retain it does not "
                "guess at: 22 U.S.C. 2714a reads '(Pub. L. 114-94, div. C, title XXXII, sec. "
                "32101, ...)' with no such construction and this index carries no row for it."
            ),
            "section_dash_rule": USLM_SECTION_DASH_RULE,
            "section_dash_rule_derivation": (
                "USLM spells a section suffix with an EN DASH ('/us/usc/t16/s824s-1' with U+2013) "
                "while Table III, the citation grammar and rkaf:us-usc all spell it with a "
                "hyphen. Verified on this release point: title 16 alone carries 1,487 en-dash "
                "section identifiers and zero hyphen ones, so it is a spelling convention of the "
                "source rather than a distinction it draws. The verbatim identifier is carried in "
                "usc_identifier, so straightening loses nothing."
            ),
            "multi_target_policy": MULTI_TARGET_POLICY,
            "multi_target_policy_derivation": (
                "A triple naming several U.S. Code sections is kept, marked refusal='multi_target' "
                "on every one of its rows, and refused at resolution time. Dropping it would tell "
                "a consumer the source was silent when it was plural, and those are different "
                "facts that call for different fixes."
            ),
            "quarantine_reasons": list(QUARANTINE_REASONS),
        },
        "outputs": {
            _pin_path(path): {"digest": file_sha256(path), "rows": pq.ParquetFile(path).metadata.num_rows}
            for path in (credits_path, quarantine_path)
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
    parser.add_argument("--archive", type=Path, required=True, help="the whole-Code USLM zip")
    parser.add_argument("--release-point", required=True, help='the release point the archive is, e.g. "119-102"')
    args = parser.parse_args(argv)
    if not args.archive.exists():
        print(
            f"missing archive {args.archive}; fetch {uslm_release_url(args.release_point)} (~109 MB)",
            file=sys.stderr,
        )
        return 2
    receipt = build(args.output, archive=args.archive, release_point=args.release_point)
    print(canonical_json(receipt["coverage"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

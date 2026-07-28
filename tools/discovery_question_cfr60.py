"""Discovery question 1: every docket touching `40 CFR 60`.

A product-level experiment, not a component benchmark. It derives the expected
docket set by scanning the **raw source snapshot** — `federal_register`,
`fr_docket_links`, `documents`, `dockets` — with matching logic written here
and imported from nowhere, then runs the **system** query over the published
`rule_targets` table and scores the two against each other on identity, link,
filter, and aggregate measures.

Independence is the whole point, so this file deliberately re-implements two
things the repository already owns:

* Regulations.gov identifier normalization (upper-case, syntax-gated), stated
  from `docs/tables/` semantics rather than imported from
  `spicy_regs.ontology.citations`; and
* CFR reference matching over the Federal Register's structured
  `cfr_references_json` entries, which are `{title, part, chapter,
  citation_url}` dictionaries — `title` arrives as an int, `part` as an int or
  a string, and either may be null.

If those re-implementations disagree with the transform, the disagreement is
the finding.

Declared recall boundary
------------------------

Unified Agenda CFR values live on editioned agenda observations and are never
projected through RIN equality into `rule_targets`
(`docs/tables/rule_targets.md`). So the answer is "every docket with
**action evidence** touching 40 CFR 60", not "every docket any source
associates with 40 CFR 60". The script measures the size of that boundary on
this snapshot rather than asserting it is small: it scans the agenda's own
`cfr_references_json` for 40 CFR 60 and reports how many additional dockets a
RIN projection would have reached.

Usage::

    python tools/discovery_question_cfr60.py --snapshot output/<dir> \\
        --out docs/evidence/discovery-slice-2026-07-28/question-1-cfr-40-60.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

from discovery_scoring import (  # noqa: E402  (path insert must precede the import)
    compare_counts,
    predicate_exactness,
    score_sets,
    snapshot_identity,
)

QUESTION = "Every docket touching 40 CFR 60"
TARGET_TITLE = "40"
TARGET_PART = "60"

# Near-misses that must never appear. `40-600` and `40-601` are real parts that
# a prefix filter (`cfr_ref LIKE '40-60%'`) swallows; `10 CFR 60` is the same
# part number under a different title.
NEAR_MISS_TARGETS = (("40", "600"), ("40", "601"), ("10", "60"))

SOURCE_FILES = (
    "dockets.parquet",
    "documents.parquet",
    "federal_register.parquet",
    "fr_docket_links.parquet",
    "unified_agenda.parquet",
)
SYSTEM_FILES = ("rule_targets.parquet",)

# Independently restated from `docs/tables/` identifier semantics.
_REGSGOV_ID = re.compile(r"[A-Z0-9]+(?:[-_][A-Z0-9]+)*")


def normalize_docket(value: object) -> str | None:
    """Canonical Regulations.gov docket id, or None when the syntax fails."""
    if value is None:
        return None
    text = str(value).strip().upper()
    return text if _REGSGOV_ID.fullmatch(text) else None


def cfr_entry_matches(entry: object, title: str, part: str) -> bool:
    """True when a Federal Register CFR reference names exactly this title/part.

    Comparison is on the decimal string form of each component, so the int
    `40` and the string `"40"` agree while `"600"` never matches `"60"`.
    """
    if not isinstance(entry, dict):
        return False
    raw_title = entry.get("title")
    raw_part = entry.get("part")
    if raw_title is None or raw_part is None:
        return False
    return str(raw_title).strip() == title and str(raw_part).strip() == part


def load_json_array(value: object) -> list[Any]:
    """Parse a JSON-serialized array column, returning [] for anything else."""
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> Iterator[tuple]:
    yield from con.execute(sql).fetchall()


def _trusted_dockets(con: duckdb.DuckDBPyConnection, snapshot: Path) -> set[str]:
    """Dockets Regulations.gov evidences directly, from either raw table."""
    trusted: set[str] = set()
    for table in ("dockets", "documents"):
        for (value,) in _rows(con, f"select distinct docket_id from '{snapshot / table}.parquet'"):
            docket = normalize_docket(value)
            if docket is not None:
                trusted.add(docket)
    return trusted


def _fr_documents_citing(con: duckdb.DuckDBPyConnection, snapshot: Path, title: str, part: str) -> set[str]:
    """Federal Register document numbers whose CFR references name title/part."""
    matched: set[str] = set()
    sql = (
        f"select document_number, cfr_references_json from '{snapshot / 'federal_register'}.parquet' "
        "where cfr_references_json is not null and cfr_references_json <> '[]'"
    )
    for document_number, references in _rows(con, sql):
        if not document_number:
            continue
        if any(cfr_entry_matches(entry, title, part) for entry in load_json_array(references)):
            matched.add(str(document_number))
    return matched


def _docket_index(con: duckdb.DuckDBPyConnection, snapshot: Path, trusted: set[str]) -> tuple[dict, dict]:
    """FR document number to dockets, by each of the two evidence paths."""
    by_link: dict[str, set[str]] = defaultdict(set)
    sql = f"select document_number, docket_id from '{snapshot / 'fr_docket_links'}.parquet'"
    for document_number, docket_id in _rows(con, sql):
        docket = normalize_docket(docket_id)
        if document_number and docket is not None and docket in trusted:
            by_link[str(document_number)].add(docket)

    by_document: dict[str, set[str]] = defaultdict(set)
    sql = f"select fr_doc_num, docket_id from '{snapshot / 'documents'}.parquet' where fr_doc_num is not null"
    for fr_doc_num, docket_id in _rows(con, sql):
        docket = normalize_docket(docket_id)
        if fr_doc_num and docket is not None and docket in trusted:
            by_document[str(fr_doc_num)].add(docket)
    return by_link, by_document


def derive_expectation(
    con: duckdb.DuckDBPyConnection,
    snapshot: Path,
    *,
    trusted: set[str],
    by_link: dict[str, set[str]],
    by_document: dict[str, set[str]],
) -> dict[str, Any]:
    """Build the expected, forbidden, and ambiguous sets from raw sources only."""

    def dockets_for(title: str, part: str) -> tuple[set[str], set[str], set[str]]:
        fr_documents = _fr_documents_citing(con, snapshot, title, part)
        linked = set().union(*(by_link.get(number, set()) for number in fr_documents)) if fr_documents else set()
        corroborated = (
            set().union(*(by_document.get(number, set()) for number in fr_documents)) if fr_documents else set()
        )
        return fr_documents, linked, corroborated

    fr_documents, via_link, via_document = dockets_for(TARGET_TITLE, TARGET_PART)
    expected = via_link | via_document

    forbidden: set[str] = set()
    near_miss_detail: dict[str, int] = {}
    for title, part in NEAR_MISS_TARGETS:
        _, near_link, near_document = dockets_for(title, part)
        only_near = (near_link | near_document) - expected
        near_miss_detail[f"{title} CFR {part}"] = len(only_near)
        forbidden |= only_near

    # Ambiguous: a title-40 reference whose part component is absent cannot be
    # resolved to part 60 either way. Any docket reachable *only* through such
    # a reference is neither expected nor forbidden.
    partless_documents: set[str] = set()
    sql = (
        f"select document_number, cfr_references_json from '{snapshot / 'federal_register'}.parquet' "
        "where cfr_references_json is not null and cfr_references_json <> '[]'"
    )
    for document_number, references in _rows(con, sql):
        if not document_number:
            continue
        for entry in load_json_array(references):
            if (
                isinstance(entry, dict)
                and str(entry.get("title")).strip() == TARGET_TITLE
                and entry.get("part") is None
            ):
                partless_documents.add(str(document_number))
                break
    ambiguous = set()
    for number in partless_documents:
        ambiguous |= by_link.get(number, set()) | by_document.get(number, set())
    ambiguous -= expected | forbidden

    return {
        "trusted_dockets": len(trusted),
        "fr_documents_citing_target": sorted(fr_documents),
        "expected": sorted(expected),
        "expected_via_fr_cfr_ref": sorted(via_link),
        "expected_via_document_fr_doc": sorted(via_document),
        "forbidden": sorted(forbidden),
        "forbidden_by_near_miss": near_miss_detail,
        "ambiguous": sorted(ambiguous),
    }


def measure_agenda_boundary(con: duckdb.DuckDBPyConnection, snapshot: Path, expected: set[str]) -> dict[str, Any]:
    """Size the declared agenda-only recall boundary on this snapshot.

    The agenda states CFR references as prose (`"40 CFR 60"`, `"40 CFR part
    60, subpart A"`), so the match is a regular expression over the part token
    rather than a structured comparison.
    """
    pattern = re.compile(r"\b40\s+CFR\b[^;|]{0,40}?(?<![\d.])60(?![\d.])", re.IGNORECASE)
    agenda_rins: set[str] = set()
    sql = (
        f"select rin, cfr_references_json from '{snapshot / 'unified_agenda'}.parquet' "
        "where cfr_references_json is not null and cfr_references_json <> '[]'"
    )
    for rin, references in _rows(con, sql):
        if not rin:
            continue
        if any(pattern.search(str(entry or "")) for entry in load_json_array(references)):
            agenda_rins.add(str(rin).strip().upper())

    reachable: set[str] = set()
    if agenda_rins:
        rin_list = ", ".join(f"'{rin}'" for rin in sorted(agenda_rins))
        for (docket_id,) in _rows(
            con,
            f"select distinct docket_id from '{snapshot / 'rule_targets'}.parquet' where rin in ({rin_list})",
        ):
            docket = normalize_docket(docket_id)
            if docket is not None:
                reachable.add(docket)
    return {
        "agenda_rins_naming_target": sorted(agenda_rins),
        "dockets_reachable_by_rin_projection": len(reachable),
        "additional_dockets_beyond_expected": sorted(reachable - expected),
    }


def run_system_query(con: duckdb.DuckDBPyConnection, snapshot: Path) -> list[dict[str, Any]]:
    """The system query: decomposed CFR components over `rule_targets`.

    Filtering on `cfr_title`/`cfr_part` rather than on the compact `cfr_ref`
    key is deliberate — `cfr_ref` carries an optional `.section` suffix, so an
    equality test on it silently drops section-level rows and a prefix test on
    it swallows part 600.
    """
    sql = f"""
        select docket_id, cfr_ref, cfr_title, cfr_part, cfr_section, rin, source, evidence_id
        from '{snapshot / "rule_targets"}.parquet'
        where cfr_title = '{TARGET_TITLE}' and cfr_part = '{TARGET_PART}'
    """
    columns = [
        "docket_id",
        "cfr_ref",
        "cfr_title",
        "cfr_part",
        "cfr_section",
        "rin",
        "source",
        "evidence_id",
    ]
    return [dict(zip(columns, row)) for row in con.execute(sql).fetchall()]


def compare_filter_formulations(con: duckdb.DuckDBPyConnection, snapshot: Path) -> dict[str, int]:
    """Three readings of "40 CFR 60" a user might plausibly write."""
    table = f"'{snapshot / 'rule_targets'}.parquet'"
    formulations = {
        "components": "cfr_title = '40' and cfr_part = '60'",
        "compact_equality": "cfr_ref = '40-60'",
        "compact_prefix": "cfr_ref like '40-60%'",
    }
    return {
        name: con.execute(f"select count(distinct docket_id) from {table} where {clause}").fetchone()[0]
        for name, clause in formulations.items()
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=QUESTION)
    parser.add_argument("--snapshot", required=True, type=Path, help="directory holding the frozen parquet snapshot")
    parser.add_argument("--out", type=Path, help="write the experiment record here as JSON")
    args = parser.parse_args(argv)

    snapshot = args.snapshot
    identity = snapshot_identity(snapshot, SOURCE_FILES + SYSTEM_FILES)

    con = duckdb.connect()
    trusted = _trusted_dockets(con, snapshot)
    by_link, by_document = _docket_index(con, snapshot, trusted)
    expectation = derive_expectation(con, snapshot, trusted=trusted, by_link=by_link, by_document=by_document)
    expected = set(expectation["expected"])
    returned_rows = run_system_query(con, snapshot)
    returned = {row["docket_id"] for row in returned_rows}

    link = score_sets(
        expected=expected,
        returned=returned,
        forbidden=expectation["forbidden"],
        ambiguous=expectation["ambiguous"],
    )

    # Filter exactness is a *row* property, distinct from the set score: every
    # returned row must carry the selected components, and the corroborating
    # `document_fr_doc` path must not have admitted a row whose CFR components
    # are null (that path emits RIN-only edges when a document has no citation).
    filter_score = predicate_exactness(
        returned_rows,
        predicate=lambda row: row["cfr_title"] == TARGET_TITLE and row["cfr_part"] == TARGET_PART,
        describe=lambda row: f"{row['docket_id']}/{row['cfr_ref']}/{row['source']}",
        unknown=lambda row: row["cfr_ref"] is None,
        unknown_universe=con.execute(
            f"select count(*) from '{snapshot / 'rule_targets'}.parquet' where cfr_ref is null"
        ).fetchone()[0],
    )

    # Aggregates are compared only at the level of detail the table declares —
    # one row per docket, normalized CFR target, optional RIN, and source. The
    # count of *evidencing Federal Register documents* is deliberately not
    # compared here: `rule_targets` folds repeated evidence for one logical
    # edge into a date span and retains a single `evidence_id`, so that count
    # is unrecoverable by construction. It is measured under `diagnostics`
    # instead, where it reads as a declared limit rather than a failed test.
    aggregate = compare_counts(
        {
            "distinct_dockets": len(expected),
            "dockets_via_fr_cfr_ref": len(expectation["expected_via_fr_cfr_ref"]),
            "dockets_via_document_fr_doc": len(expectation["expected_via_document_fr_doc"]),
        },
        {
            "distinct_dockets": len(returned),
            "dockets_via_fr_cfr_ref": len({row["docket_id"] for row in returned_rows if row["source"] == "fr_cfr_ref"}),
            "dockets_via_document_fr_doc": len(
                {row["docket_id"] for row in returned_rows if row["source"] == "document_fr_doc"}
            ),
        },
    )

    linked_fr_documents = {
        number for number in expectation["fr_documents_citing_target"] if by_link.get(number) or by_document.get(number)
    }
    diagnostics = {
        "fr_documents_citing_target": len(expectation["fr_documents_citing_target"]),
        "fr_documents_citing_target_linked_to_trusted_docket": len(linked_fr_documents),
        "distinct_evidence_ids_retained": len({row["evidence_id"] for row in returned_rows}),
        "note": (
            "Evidence folding keeps one evidence_id per logical edge, so the "
            "number of Federal Register documents that evidence a docket's 40 "
            "CFR 60 target cannot be recovered from rule_targets."
        ),
    }

    record = {
        "question": QUESTION,
        "snapshot": {"directory": str(snapshot), "sha256": identity},
        "expectation": expectation,
        "recall_boundary": measure_agenda_boundary(con, snapshot, expected),
        "system": {
            "filter": "rule_targets.cfr_title = '40' and rule_targets.cfr_part = '60'",
            "rows": len(returned_rows),
            "distinct_dockets": len(returned),
            "rows_by_source": {
                source: sum(1 for row in returned_rows if row["source"] == source)
                for source in sorted({row["source"] for row in returned_rows})
            },
            "returned": sorted(returned),
            "filter_formulations_distinct_dockets": compare_filter_formulations(con, snapshot),
        },
        "scores": {
            "link": link.as_dict(),
            "filter": filter_score.as_dict(),
            "aggregate": aggregate.as_dict(),
        },
        "diagnostics": diagnostics,
    }

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"question": QUESTION, "scores": record["scores"]}, indent=2, sort_keys=True))
    return 0 if link.exact and filter_score.exactness == 1.0 and aggregate.matches else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

"""Discovery question 2: every active rulemaking depending on `42 U.S.C. 7401`.

The system path is the corrected join —
`authority_edges` (RIN) → `agenda_item_proceedings` → `proceedings` — with the
active-state filter recorded in `docs/decisions.md` (2026-07-28, "Active
rulemaking"): a proceeding is active when its `current_stage` is an evidenced
non-terminal stage. Stage-unknown proceedings are reported, never counted.

The expectation is derived independently for the leg this question actually
tests: the **authority leg**. Raw `unified_agenda.legal_authority_json` strings
are scanned with a matcher written here, not imported from
`spicy_regs.ontology.citations`, so a parser defect shows up as a score rather
than as agreement with itself. The proceeding-identity leg is held constant —
re-deriving 511,643 proceedings from raw evidence is a different experiment —
but it is not taken on faith either: the script independently scans raw
`dockets.rin`, `documents.additional_rins`, and
`federal_register.regulation_id_numbers_json` and checks that every expected
RIN carrying direct action evidence has a row in `agenda_item_proceedings`.

Fan-out is reported explicitly. One RIN can track many proceedings
(`1625-AA00` tracks thousands), and a question phrased "every active
rulemaking" is answered in proceedings, so a silent multiplication would be
invisible in the identifier count alone.

Usage::

    python tools/discovery_question_usc7401.py --snapshot output/<dir> \\
        [--authority-edges path/to/authority_edges.parquet] \\
        --out docs/evidence/discovery-slice-2026-07-28/question-2-usc-42-7401.json
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

from spicy_regs.ontology.citations import normalize_rin

sys.path.insert(0, str(Path(__file__).resolve().parent))

from discovery_scoring import (  # noqa: E402  (path insert must precede the import)
    compare_counts,
    predicate_exactness,
    score_sets,
    snapshot_identity,
)

QUESTION = "Every active rulemaking depending on 42 U.S.C. 7401"
TARGET_TITLE = "42"
TARGET_SECTION = "7401"

# The recorded active-state definition. An allowlist of evidenced non-terminal
# stages, not `NOT IN ('final','withdrawn')`: the two select the same rows only
# because SQL drops NULLs from a NOT IN, and 42% of `proceedings` has a NULL
# stage. See docs/decisions.md, 2026-07-28.
ACTIVE_STAGES = ("prerule", "proposed", "supplemental", "longterm")
TERMINAL_STAGES = ("final", "withdrawn")

# Near-miss authorities: other Clean Air Act sections in the same U.S.C. title.
# A rulemaking citing only 42 U.S.C. 7411 does not depend on 7401.
NEAR_MISS_SECTIONS = ("7410", "7411", "7412", "7413", "7414", "7420", "7425", "7429", "7430", "7436")

SOURCE_FILES = (
    "unified_agenda.parquet",
    "dockets.parquet",
    "documents.parquet",
    "federal_register.parquet",
)
SYSTEM_FILES = ("authority_edges.parquet", "agenda_item_proceedings.parquet", "proceedings.parquet")

# A U.S.C. anchor: a title number immediately followed by the code's name in
# any of the punctuation forms the agenda uses.
_USC_ANCHOR = re.compile(r"\b(?P<title>\d{1,2})\s*U\.?\s*S\.?\s*C\.?", re.IGNORECASE)
# Where an anchor's section window must stop: another citation grammar begins.
_COMPETING_CITATION = re.compile(
    r"(?:Pub\.?\s*L\.?|Public\s+Law|\bStat\.|\bCFR\b|\bE\.?\s*O\.?\s*\d|Executive\s+Order)",
    re.IGNORECASE,
)
# A section token, optionally a range: `7401`, `1831p-1`, `7401-7671q`.
_SECTION_TOKEN = re.compile(r"\b(?P<start>\d+[A-Za-z]*)(?:\s*(?:-|–|—|to)\s*(?P<end>\d+[A-Za-z]*))?\b")


def load_json_array(value: object) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _numeric_prefix(token: str) -> int | None:
    match = re.match(r"\d+", token)
    return int(match.group()) if match else None


def classify_usc_reference(text: str, title: str, section: str) -> str:
    """Classify one authority string against a target U.S.C. title and section.

    Returns one of:

    ``"names"``
        The string names the section, either on its own (`42 U.S.C. 7401`,
        `42 U.S.C. 7401 et seq.`) or as the first endpoint of a range
        (`42 U.S.C. 7401-7671q`, `42 U.S.C. 7401 to 7671q`). Both forms put the
        section's own digits in the source text, so both are expected results.

    ``"spans"``
        A range under the right title covers the section numerically without
        naming it (`42 U.S.C. 7300-7500`). Defensible either way, so it is
        recorded as ambiguous rather than decided here.

    ``"absent"``
        Neither.
    """
    verdict = "absent"
    anchors = list(_USC_ANCHOR.finditer(text))
    for index, anchor in enumerate(anchors):
        if str(int(anchor.group("title"))) != title:
            continue
        stop = anchors[index + 1].start() if index + 1 < len(anchors) else len(text)
        window = text[anchor.end() : stop]
        competing = _COMPETING_CITATION.search(window)
        if competing:
            window = window[: competing.start()]
        for token in _SECTION_TOKEN.finditer(window):
            start, end = token.group("start"), token.group("end")
            if start == section:
                return "names"
            if end is None:
                continue
            low, high, target = _numeric_prefix(start), _numeric_prefix(end), _numeric_prefix(section)
            if low is not None and high is not None and target is not None and low < target <= high:
                verdict = "spans"
    return verdict


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> Iterator[tuple]:
    yield from con.execute(sql).fetchall()


def _in_list(values: set[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in sorted(values))


def derive_authority_expectation(con: duckdb.DuckDBPyConnection, snapshot: Path) -> dict[str, Any]:
    """Scan raw agenda authority strings for the target citation."""
    naming: set[str] = set()
    spanning: set[str] = set()
    near_miss: set[str] = set()
    forms: dict[str, int] = defaultdict(int)

    sql = f"select rin, legal_authority_json from '{snapshot / 'unified_agenda'}.parquet'"
    for raw_rin, authorities in _rows(con, sql):
        rin = normalize_rin(raw_rin)
        if rin is None:
            continue
        verdicts = set()
        for authority in load_json_array(authorities):
            text = str(authority or "").strip()
            if not text:
                continue
            verdict = classify_usc_reference(text, TARGET_TITLE, TARGET_SECTION)
            verdicts.add(verdict)
            if verdict == "names":
                forms[text] += 1
            if verdict == "absent" and any(
                classify_usc_reference(text, TARGET_TITLE, other) == "names" for other in NEAR_MISS_SECTIONS
            ):
                verdicts.add("near_miss")
        if "names" in verdicts:
            naming.add(rin)
        elif "spans" in verdicts:
            spanning.add(rin)
        elif "near_miss" in verdicts:
            near_miss.add(rin)

    return {
        "rins_naming_target": sorted(naming),
        "rins_spanning_target": sorted(spanning),
        "rins_citing_near_miss_only": sorted(near_miss),
        "authority_forms": dict(sorted(forms.items(), key=lambda item: (-item[1], item[0]))),
    }


def raw_action_evidence(con: duckdb.DuckDBPyConnection, snapshot: Path, rins: set[str]) -> dict[str, list[str]]:
    """RINs a raw action artifact reports directly, with one evidence id each."""
    evidence: dict[str, set[str]] = defaultdict(set)

    for docket_id, raw_rin in _rows(
        con, f"select docket_id, rin from '{snapshot / 'dockets'}.parquet' where rin is not null"
    ):
        rin = normalize_rin(raw_rin)
        if rin in rins:
            evidence[rin].add(f"docket:{docket_id}")

    sql = (
        f"select document_id, additional_rins from '{snapshot / 'documents'}.parquet' "
        "where additional_rins is not null and additional_rins <> '[]'"
    )
    for document_id, additional in _rows(con, sql):
        for value in load_json_array(additional):
            rin = normalize_rin(value)
            if rin in rins:
                evidence[rin].add(f"document:{document_id}")

    sql = (
        f"select document_number, regulation_id_numbers_json from '{snapshot / 'federal_register'}.parquet' "
        "where regulation_id_numbers_json is not null and regulation_id_numbers_json <> '[]'"
    )
    for document_number, numbers in _rows(con, sql):
        for value in load_json_array(numbers):
            rin = normalize_rin(value)
            if rin in rins:
                evidence[rin].add(f"federal_register:{document_number}")

    return {rin: sorted(ids) for rin, ids in sorted(evidence.items())}


def proceedings_for_rins(con: duckdb.DuckDBPyConnection, snapshot: Path, rins: set[str]) -> dict[str, set[str]]:
    """Proceeding ids each RIN tracks, via `agenda_item_proceedings`."""
    if not rins:
        return {}
    sql = (
        f"select rin, proceeding_id from '{snapshot / 'agenda_item_proceedings'}.parquet' "
        f"where rin in ({_in_list(rins)})"
    )
    tracked: dict[str, set[str]] = defaultdict(set)
    for rin, proceeding_id in _rows(con, sql):
        if rin and proceeding_id:
            tracked[str(rin)].add(str(proceeding_id))
    return dict(tracked)


def stage_of(con: duckdb.DuckDBPyConnection, snapshot: Path, proceeding_ids: set[str]) -> dict[str, str | None]:
    if not proceeding_ids:
        return {}
    sql = (
        f"select proceeding_id, current_stage from '{snapshot / 'proceedings'}.parquet' "
        f"where proceeding_id in ({_in_list(proceeding_ids)})"
    )
    return {str(pid): stage for pid, stage in _rows(con, sql)}


def run_system_query(con: duckdb.DuckDBPyConnection, snapshot: Path, authority_edges: Path) -> dict[str, Any]:
    """The system query: authority edge → agenda item link → active proceeding."""
    stages = ", ".join(f"'{stage}'" for stage in ACTIVE_STAGES)
    sql = f"""
        select distinct p.proceeding_id, p.current_stage, a.rin, a.agenda_item_id, a.source, a.evidence_id
        from '{authority_edges}' e
        join '{snapshot / "agenda_item_proceedings"}.parquet' a on a.rin = e.rin
        join '{snapshot / "proceedings"}.parquet' p on p.proceeding_id = a.proceeding_id
        where e.usc_title = '{TARGET_TITLE}' and e.usc_section = '{TARGET_SECTION}'
          and p.current_stage in ({stages})
    """
    columns = ["proceeding_id", "current_stage", "rin", "agenda_item_id", "source", "evidence_id"]
    rows = [dict(zip(columns, row)) for row in con.execute(sql).fetchall()]
    authority_rins = {
        str(rin)
        for (rin,) in _rows(
            con,
            f"select distinct rin from '{authority_edges}' "
            f"where usc_title = '{TARGET_TITLE}' and usc_section = '{TARGET_SECTION}'",
        )
        if rin
    }
    return {"rows": rows, "authority_rins": authority_rins}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=QUESTION)
    parser.add_argument("--snapshot", required=True, type=Path, help="directory holding the frozen parquet snapshot")
    parser.add_argument(
        "--authority-edges",
        type=Path,
        help="override the authority_edges parquet (to score a rebuild against the same snapshot)",
    )
    parser.add_argument("--out", type=Path, help="write the experiment record here as JSON")
    args = parser.parse_args(argv)

    snapshot = args.snapshot
    identity = snapshot_identity(snapshot, SOURCE_FILES + SYSTEM_FILES)
    authority_edges = args.authority_edges or (snapshot / "authority_edges.parquet")
    if not authority_edges.exists():
        raise FileNotFoundError(f"authority_edges parquet not found: {authority_edges}")
    identity["authority_edges.parquet(scored)"] = snapshot_identity(authority_edges.parent, (authority_edges.name,))[
        authority_edges.name
    ]

    con = duckdb.connect()
    authority = derive_authority_expectation(con, snapshot)
    expected_rins = set(authority["rins_naming_target"])
    ambiguous_rins = set(authority["rins_spanning_target"])
    near_miss_rins = set(authority["rins_citing_near_miss_only"])

    tracked = proceedings_for_rins(con, snapshot, expected_rins)
    expected_proceedings = set().union(*tracked.values()) if tracked else set()
    expected_stages = stage_of(con, snapshot, expected_proceedings)
    expected_active = {pid for pid, stage in expected_stages.items() if stage in ACTIVE_STAGES}

    near_tracked = proceedings_for_rins(con, snapshot, near_miss_rins)
    near_proceedings = set().union(*near_tracked.values()) if near_tracked else set()
    near_stages = stage_of(con, snapshot, near_proceedings)
    forbidden_active = {pid for pid, stage in near_stages.items() if stage in ACTIVE_STAGES} - expected_active

    ambiguous_tracked = proceedings_for_rins(con, snapshot, ambiguous_rins)
    ambiguous_proceedings = set().union(*ambiguous_tracked.values()) if ambiguous_tracked else set()
    ambiguous_active = {
        pid for pid, stage in stage_of(con, snapshot, ambiguous_proceedings).items() if stage in ACTIVE_STAGES
    } - expected_active

    system = run_system_query(con, snapshot, authority_edges)
    returned = {row["proceeding_id"] for row in system["rows"]}

    link = score_sets(
        expected=expected_active,
        returned=returned,
        forbidden=forbidden_active,
        ambiguous=ambiguous_active,
    )
    authority_link = score_sets(expected=expected_rins, returned=system["authority_rins"])

    # The proceeding-identity leg, checked at RIN level against raw evidence.
    evidence = raw_action_evidence(con, snapshot, expected_rins)
    rins_with_raw_evidence = set(evidence)
    rins_linked = set(tracked)
    proceeding_link = score_sets(expected=rins_with_raw_evidence, returned=rins_linked)

    filter_score = predicate_exactness(
        system["rows"],
        predicate=lambda row: row["current_stage"] in ACTIVE_STAGES,
        describe=lambda row: f"{row['proceeding_id']}/{row['current_stage']}",
        unknown=lambda row: row["current_stage"] is None,
        unknown_universe=sum(1 for stage in expected_stages.values() if stage is None),
    )

    aggregate = compare_counts(
        {
            "authority_rins": len(expected_rins),
            "tracked_proceedings": len(expected_proceedings),
            "active_proceedings": len(expected_active),
        },
        {
            "authority_rins": len(system["authority_rins"]),
            "tracked_proceedings": len(
                set().union(*proceedings_for_rins(con, snapshot, system["authority_rins"]).values())
                if system["authority_rins"]
                else set()
            ),
            "active_proceedings": len(returned),
        },
    )

    fan_out = {rin: len(pids) for rin, pids in sorted(tracked.items(), key=lambda kv: (-len(kv[1]), kv[0]))}
    stage_mix: dict[str, int] = defaultdict(int)
    for stage in expected_stages.values():
        stage_mix["stage_unknown" if stage is None else stage] += 1

    record = {
        "question": QUESTION,
        "active_definition": {
            "rule": f"proceedings.current_stage in {list(ACTIVE_STAGES)}",
            "terminal_stages": list(TERMINAL_STAGES),
            "unknown_stage_treatment": "reported, never counted as active",
            "ledger_entry": "docs/decisions.md, 2026-07-28 — Active rulemaking",
        },
        "snapshot": {
            "directory": str(snapshot),
            "authority_edges_scored": str(authority_edges),
            "sha256": identity,
        },
        "expectation": {
            **authority,
            "expected_proceedings": sorted(expected_proceedings),
            "expected_active_proceedings": sorted(expected_active),
            "forbidden_active_proceedings": sorted(forbidden_active),
            "ambiguous_active_proceedings": sorted(ambiguous_active),
            "stage_mix_of_tracked_proceedings": dict(sorted(stage_mix.items())),
            "rins_with_raw_action_evidence": sorted(rins_with_raw_evidence),
        },
        "recall_boundary": {
            "rins_naming_target_without_action_evidence": sorted(expected_rins - rins_with_raw_evidence),
            "note": (
                "agenda_item_proceedings exists only where a docket, "
                "regulations.gov document, or Federal Register artifact directly "
                "reports the RIN; agenda equality cannot manufacture a link."
            ),
        },
        "system": {
            "join": "authority_edges -> agenda_item_proceedings -> proceedings",
            "authority_rins": sorted(system["authority_rins"]),
            "rows": len(system["rows"]),
            "distinct_proceedings": len(returned),
            "returned": sorted(returned),
        },
        "fan_out": {
            "proceedings_per_expected_rin": fan_out,
            "max": max(fan_out.values(), default=0),
            "rins_tracking_multiple_proceedings": sorted(rin for rin, n in fan_out.items() if n > 1),
            "multiplication_ratio": (len(expected_proceedings) / len(rins_linked) if rins_linked else 0.0),
        },
        "scores": {
            "authority_link": authority_link.as_dict(),
            "proceeding_link": proceeding_link.as_dict(),
            "link": link.as_dict(),
            "filter": filter_score.as_dict(),
            "aggregate": aggregate.as_dict(),
        },
    }

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {"question": QUESTION, "scores": record["scores"], "fan_out": record["fan_out"]},
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if link.exact and filter_score.exactness == 1.0 and aggregate.matches else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

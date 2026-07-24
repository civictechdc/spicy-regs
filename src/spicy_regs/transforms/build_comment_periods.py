"""Transform: materialize continuous and reopened public-comment intervals."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.common import (
    ATTESTATION_COLUMNS,
    JsonReadStats,
    RunContext,
    canonical_json,
    iter_parquet_rows,
    parse_json_list,
    stable_id,
    write_parquet_rows,
)

OUTPUT = "comment_periods.parquet"
ACTOR_ID = "spicy-regs:comment-periods:v1"
_RIN = re.compile(r"^\d{4}-[A-Z]{2}\d{2}$")

COLUMNS = (
    "comment_period_id",
    "proceeding_id",
    "rin",
    "docket_id",
    "open_date",
    "close_date",
    "source",
    "evidence_ids_json",
    *ATTESTATION_COLUMNS,
)


@dataclass(frozen=True)
class _Interval:
    proceeding_id: str
    rin: str | None
    docket_id: str | None
    start: date
    end: date
    source: str
    evidence_id: str


def _day(value: object) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _rin(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized if _RIN.fullmatch(normalized) else None


def _merge_intervals(intervals: list[_Interval]) -> list[dict]:
    grouped: dict[tuple[str, str | None, str | None], list[_Interval]] = defaultdict(list)
    for interval in intervals:
        grouped[(interval.proceeding_id, interval.rin, interval.docket_id)].append(interval)
    merged: list[dict] = []
    for (proceeding_id, rin, docket_id), values in grouped.items():
        values.sort(key=lambda interval: (interval.start, interval.end, interval.source, interval.evidence_id))
        current_start: date | None = None
        current_end: date | None = None
        sources: set[str] = set()
        evidence: set[str] = set()

        def flush() -> None:
            if current_start is None or current_end is None:
                return
            merged.append(
                {
                    "comment_period_id": stable_id(
                        "comment_period",
                        proceeding_id,
                        docket_id,
                        current_start.isoformat(),
                    ),
                    "proceeding_id": proceeding_id,
                    "rin": rin,
                    "docket_id": docket_id,
                    "open_date": current_start.isoformat(),
                    "close_date": current_end.isoformat(),
                    "source": "+".join(sorted(sources)),
                    "evidence_ids_json": canonical_json(sorted(evidence)),
                }
            )

        for interval in values:
            if current_start is None:
                current_start, current_end = interval.start, interval.end
                sources, evidence = {interval.source}, {interval.evidence_id}
                continue
            assert current_end is not None
            if interval.start <= current_end + timedelta(days=1):
                current_end = max(current_end, interval.end)
                sources.add(interval.source)
                evidence.add(interval.evidence_id)
                continue
            flush()
            current_start, current_end = interval.start, interval.end
            sources, evidence = {interval.source}, {interval.evidence_id}
        flush()
    return merged


def build_comment_periods(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build comment periods, coalescing extensions but preserving reopenings."""
    required = {
        name: output_dir / f"{name}.parquet"
        for name in ("proceedings", "documents", "federal_register", "fr_docket_links")
    }
    missing = [path.name for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"comment_periods inputs missing from {output_dir}: {', '.join(missing)}")
    context = RunContext.resolve(run_id=run_id, asserted_at=asserted_at, prefix="comment-periods")
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()

    proceeding_by_id: dict[str, dict] = {}
    dockets_by_proceeding: dict[str, set[str]] = {}
    proceeding_ids_by_docket: dict[str, set[str]] = defaultdict(set)
    proceeding_ids_by_rin: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(required["proceedings"]):
        proceeding_id = str(row["proceeding_id"])
        proceeding_by_id[proceeding_id] = row
        if row.get("rin"):
            proceeding_ids_by_rin[str(row["rin"]).upper()].add(proceeding_id)
        dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        docket_set = set() if dockets is None else {str(docket) for docket in dockets}
        dockets_by_proceeding[proceeding_id] = docket_set
        for docket in docket_set:
            proceeding_ids_by_docket[docket].add(proceeding_id)

    intervals: list[_Interval] = []
    inverted_by_source: Counter[str] = Counter()
    inverted_examples: list[str] = []
    ambiguous_document_intervals = 0
    ambiguous_fr_intervals = 0

    def add_interval(
        *,
        proceeding_id: str,
        docket_id: object,
        start: object,
        end: object,
        source: str,
        evidence_id: object,
    ) -> None:
        open_date, close_date = _day(start), _day(end)
        if open_date is None or close_date is None:
            return
        if close_date < open_date:
            inverted_by_source[source] += 1
            if len(inverted_examples) < 5:
                inverted_examples.append(f"{source} {evidence_id}: {open_date.isoformat()}..{close_date.isoformat()}")
            return
        proceeding = proceeding_by_id[proceeding_id]
        intervals.append(
            _Interval(
                proceeding_id=proceeding_id,
                rin=str(proceeding["rin"]) if proceeding.get("rin") else None,
                docket_id=str(docket_id) if docket_id else None,
                start=open_date,
                end=close_date,
                source=source,
                evidence_id=str(evidence_id),
            )
        )

    for row in iter_parquet_rows(required["documents"]):
        docket = row.get("docket_id")
        if not docket or not row.get("comment_end_date"):
            continue
        target_ids = set(proceeding_ids_by_docket.get(str(docket), ()))
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        rins = [] if raw_rins is None else [rin for value in raw_rins if (rin := _rin(value))]
        if rins:
            rin_targets = set().union(*(proceeding_ids_by_rin.get(rin, set()) for rin in rins))
            target_ids &= rin_targets
        elif len(target_ids) > 1:
            ambiguous_document_intervals += 1
            target_ids = set()
        for proceeding_id in target_ids:
            add_interval(
                proceeding_id=proceeding_id,
                docket_id=docket,
                start=row.get("comment_start_date") or row.get("posted_date"),
                end=row.get("comment_end_date"),
                source="documents.comment_end_date",
                evidence_id=row.get("document_id"),
            )

    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(required["fr_docket_links"], columns=("document_number", "docket_id")):
        if row.get("document_number") and row.get("docket_id"):
            linked_dockets_by_fr[str(row["document_number"])].add(str(row["docket_id"]))

    for row in iter_parquet_rows(required["federal_register"]):
        if not row.get("comments_close_on") or not row.get("publication_date"):
            continue
        document_number = str(row.get("document_number") or "")
        raw_rins = parse_json_list(
            row.get("regulation_id_numbers_json"),
            stats=json_stats,
            table="federal_register",
            row_id=document_number,
            column="regulation_id_numbers_json",
        )
        target_ids: set[str] = set()
        rins = [] if raw_rins is None else [rin for value in raw_rins if (rin := _rin(value))]
        if rins:
            target_ids.update(*(proceeding_ids_by_rin.get(rin, set()) for rin in rins))
        dockets = linked_dockets_by_fr.get(document_number, set())
        docket_target_ids: set[str] = set()
        for docket in dockets:
            docket_target_ids.update(proceeding_ids_by_docket.get(docket, ()))
        if target_ids and docket_target_ids:
            target_ids &= docket_target_ids
        elif docket_target_ids:
            target_ids = docket_target_ids
        if len(target_ids) > 1 and not rins:
            ambiguous_fr_intervals += 1
            target_ids = set()
        elif len(target_ids) > 1 and not docket_target_ids:
            unscoped = {proceeding_id for proceeding_id in target_ids if not dockets_by_proceeding.get(proceeding_id)}
            if len(unscoped) == 1:
                target_ids = unscoped
            else:
                ambiguous_fr_intervals += 1
                target_ids = set()
        for proceeding_id in target_ids:
            matching_dockets = dockets & dockets_by_proceeding[proceeding_id]
            for docket in matching_dockets or {None}:
                add_interval(
                    proceeding_id=proceeding_id,
                    docket_id=docket,
                    start=row.get("publication_date"),
                    end=row.get("comments_close_on"),
                    source="federal_register.comments_close_on",
                    evidence_id=document_number,
                )

    rows = _merge_intervals(intervals)
    for row in rows:
        row.update(provenance)
    rows.sort(key=lambda row: (row["proceeding_id"], row["open_date"], row.get("docket_id") or ""))
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("comment_periods")
    if inverted_by_source:
        logger.warning(
            "comment_periods: skipped {:,} inverted source intervals ({}); examples: {}",
            sum(inverted_by_source.values()),
            ", ".join(f"{source}={count:,}" for source, count in sorted(inverted_by_source.items())),
            "; ".join(inverted_examples),
        )
    if ambiguous_fr_intervals:
        logger.warning(
            "comment_periods: skipped {:,} FR intervals whose reused RIN had no unique docket component",
            ambiguous_fr_intervals,
        )
    if ambiguous_document_intervals:
        logger.warning(
            "comment_periods: skipped {:,} document intervals whose docket had no unique proceeding component",
            ambiguous_document_intervals,
        )
    reopenings = sum(count - 1 for count in Counter(row["proceeding_id"] for row in rows).values() if count > 1)
    logger.info("Comment periods: {:,} rows ({:,} reopened intervals beyond the first)", len(rows), reopenings)
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file

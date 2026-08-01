"""Transform: materialize continuous and reopened public-comment intervals."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.ontology.citations import normalize_regsgov_identifier, normalize_rin
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
ACTOR_ID = "spicy-regs:comment-periods:v2"

COLUMNS = (
    "comment_period_id",
    "proceeding_ids_json",
    "rins_json",
    "docket_ids_json",
    "open_date",
    "close_date",
    "source",
    "opened_by_artifact_ids_json",
    "evidence_ids_json",
    *ATTESTATION_COLUMNS,
)


@dataclass(frozen=True)
class _Interval:
    proceeding_ids: tuple[str, ...]
    rins: tuple[str, ...]
    docket_ids: tuple[str, ...]
    start: date
    end: date
    source: str
    evidence_id: str
    opened_by_artifact_id: str


def _day(value: object) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _artifact_url(source: str, identifier: object) -> str | None:
    value = str(identifier or "").strip()
    if not value:
        return None
    escaped = quote(value, safe="-._~")
    if source == "documents.comment_end_date":
        return f"https://www.regulations.gov/document/{escaped}"
    if source == "federal_register.comments_close_on":
        return f"https://www.federalregister.gov/d/{escaped}"
    return None


def _merge_intervals(intervals: list[_Interval]) -> list[dict]:
    """Coalesce extensions without losing joint or unresolved anchors.

    Docket anchors are the grouping key when available because they remain
    usable while Proceeding identity is unresolved. Proceeding anchors are
    unioned only from uniquely resolved source assertions. A docket-less
    interval falls back to its resolved Proceeding anchors.
    """
    grouped: dict[tuple[str, tuple[str, ...]], list[_Interval]] = defaultdict(list)
    for interval in intervals:
        anchor = (
            ("dockets", interval.docket_ids)
            if interval.docket_ids
            else ("proceedings", interval.proceeding_ids)
        )
        grouped[anchor].append(interval)

    merged: list[dict] = []
    for _, values in grouped.items():
        values.sort(
            key=lambda interval: (
                interval.start,
                interval.end,
                interval.source,
                interval.evidence_id,
            )
        )
        current_start: date | None = None
        current_end: date | None = None
        proceeding_ids: set[str] = set()
        rins: set[str] = set()
        docket_ids: set[str] = set()
        sources: set[str] = set()
        evidence: set[str] = set()
        opened_by: set[str] = set()

        def flush() -> None:
            if current_start is None or current_end is None:
                return
            sorted_proceedings = sorted(proceeding_ids)
            sorted_dockets = sorted(docket_ids)
            merged.append(
                {
                    "comment_period_id": stable_id(
                        "comment_period",
                        canonical_json(sorted_proceedings),
                        canonical_json(sorted_dockets),
                        current_start.isoformat(),
                    ),
                    "proceeding_ids_json": canonical_json(sorted_proceedings),
                    "rins_json": canonical_json(sorted(rins)),
                    "docket_ids_json": canonical_json(sorted_dockets),
                    "open_date": current_start.isoformat(),
                    "close_date": current_end.isoformat(),
                    "source": "+".join(sorted(sources)),
                    "opened_by_artifact_ids_json": canonical_json(sorted(opened_by)),
                    "evidence_ids_json": canonical_json(sorted(evidence)),
                }
            )

        def begin(interval: _Interval) -> None:
            nonlocal current_start, current_end
            nonlocal proceeding_ids, rins, docket_ids, sources, evidence, opened_by
            current_start, current_end = interval.start, interval.end
            proceeding_ids = set(interval.proceeding_ids)
            rins = set(interval.rins)
            docket_ids = set(interval.docket_ids)
            sources = {interval.source}
            evidence = {interval.evidence_id}
            opened_by = {interval.opened_by_artifact_id}

        for interval in values:
            if current_start is None:
                begin(interval)
                continue
            assert current_end is not None
            if interval.start <= current_end + timedelta(days=1):
                current_end = max(current_end, interval.end)
                proceeding_ids.update(interval.proceeding_ids)
                rins.update(interval.rins)
                docket_ids.update(interval.docket_ids)
                sources.add(interval.source)
                evidence.add(interval.evidence_id)
                if interval.start == current_start:
                    opened_by.add(interval.opened_by_artifact_id)
                continue
            flush()
            begin(interval)
        flush()
    return merged


def build_comment_periods(
    output_dir: Path,
    *,
    run_id: str | None = None,
    asserted_at: str | None = None,
) -> Path:
    """Build comment periods, retaining docket-only and joint intervals."""
    required = {
        name: output_dir / f"{name}.parquet"
        for name in (
            "proceedings",
            "dockets",
            "documents",
            "federal_register",
            "fr_docket_links",
        )
    }
    missing = [path.name for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"comment_periods inputs missing from {output_dir}: {', '.join(missing)}"
        )
    context = RunContext.resolve(
        run_id=run_id,
        asserted_at=asserted_at,
        prefix="comment-periods",
    )
    provenance = context.provenance(method="deterministic", actor_id=ACTOR_ID)
    json_stats = JsonReadStats()

    proceeding_by_id: dict[str, dict] = {}
    dockets_by_proceeding: dict[str, set[str]] = {}
    proceeding_ids_by_docket: dict[str, set[str]] = defaultdict(set)
    proceeding_ids_by_fr_document: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(required["proceedings"]):
        proceeding_id = str(row["proceeding_id"])
        proceeding_by_id[proceeding_id] = row
        dockets = parse_json_list(
            row.get("docket_ids_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="docket_ids_json",
        )
        docket_set = (
            set()
            if dockets is None
            else {
                normalized
                for docket in dockets
                if (normalized := normalize_regsgov_identifier(docket)) is not None
            }
        )
        dockets_by_proceeding[proceeding_id] = docket_set
        for docket in docket_set:
            proceeding_ids_by_docket[docket].add(proceeding_id)
        fr_documents = parse_json_list(
            row.get("fr_document_numbers_json"),
            stats=json_stats,
            table="proceedings",
            row_id=proceeding_id,
            column="fr_document_numbers_json",
        )
        if fr_documents is not None:
            for document_number in fr_documents:
                proceeding_ids_by_fr_document[str(document_number)].add(
                    proceeding_id
                )

    trusted_dockets = {
        normalized
        for row in iter_parquet_rows(required["dockets"], columns=("docket_id",))
        if (normalized := normalize_regsgov_identifier(row.get("docket_id"))) is not None
    }

    intervals: list[_Interval] = []
    inverted_by_source: Counter[str] = Counter()
    inverted_examples: list[str] = []
    ambiguous_document_intervals = 0
    ambiguous_fr_intervals = 0
    unanchored_intervals = 0

    def add_interval(
        *,
        proceeding_ids: set[str],
        docket_ids: set[str],
        rins: set[str],
        start: object,
        end: object,
        source: str,
        evidence_id: object,
    ) -> None:
        nonlocal unanchored_intervals
        open_date, close_date = _day(start), _day(end)
        evidence = str(evidence_id or "").strip()
        opened_by = _artifact_url(source, evidence)
        if open_date is None or close_date is None or not evidence or opened_by is None:
            return
        if close_date < open_date:
            inverted_by_source[source] += 1
            if len(inverted_examples) < 5:
                inverted_examples.append(
                    f"{source} {evidence}: {open_date.isoformat()}..{close_date.isoformat()}"
                )
            return
        if not proceeding_ids and not docket_ids:
            unanchored_intervals += 1
            return
        resolved_rins = set(rins)
        resolved_rins.update(
            rin
            for proceeding_id in proceeding_ids
            if (rin := normalize_rin(proceeding_by_id[proceeding_id].get("rin"))) is not None
        )
        intervals.append(
            _Interval(
                proceeding_ids=tuple(sorted(proceeding_ids)),
                rins=tuple(sorted(resolved_rins)),
                docket_ids=tuple(sorted(docket_ids)),
                start=open_date,
                end=close_date,
                source=source,
                evidence_id=evidence,
                opened_by_artifact_id=opened_by,
            )
        )

    for row in iter_parquet_rows(required["documents"]):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if docket is None or not row.get("comment_end_date"):
            continue
        # The document endpoint is itself a source-of-record membership signal.
        trusted_dockets.add(docket)
        docket_targets = set(proceeding_ids_by_docket.get(docket, ()))
        raw_rins = parse_json_list(
            row.get("additional_rins"),
            stats=json_stats,
            table="documents",
            row_id=row.get("document_id"),
            column="additional_rins",
        )
        rins = (
            set()
            if raw_rins is None
            else {rin for value in raw_rins if (rin := normalize_rin(value)) is not None}
        )
        # The source-backed docket is action identity. A RIN is retained as
        # interval metadata but never filters or selects a Proceeding.
        candidates = docket_targets
        proceeding_ids = candidates if len(candidates) == 1 else set()
        if len(candidates) > 1:
            ambiguous_document_intervals += 1
        add_interval(
            proceeding_ids=proceeding_ids,
            docket_ids={docket},
            rins=rins,
            start=row.get("comment_start_date") or row.get("posted_date"),
            end=row.get("comment_end_date"),
            source="documents.comment_end_date",
            evidence_id=row.get("document_id"),
        )

    linked_dockets_by_fr: dict[str, set[str]] = defaultdict(set)
    for row in iter_parquet_rows(
        required["fr_docket_links"],
        columns=("document_number", "docket_id"),
    ):
        docket = normalize_regsgov_identifier(row.get("docket_id"))
        if row.get("document_number") and docket in trusted_dockets:
            linked_dockets_by_fr[str(row["document_number"])].add(docket)

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
        rins = (
            set()
            if raw_rins is None
            else {rin for value in raw_rins if (rin := normalize_rin(value)) is not None}
        )
        dockets = set(linked_dockets_by_fr.get(document_number, ()))
        docket_targets: set[str] = set()
        for docket in dockets:
            docket_targets.update(proceeding_ids_by_docket.get(docket, ()))
        artifact_targets = set(
            proceeding_ids_by_fr_document.get(document_number, ())
        )
        # Direct artifact membership is strongest. Docket membership is the
        # fallback for older rows that predate the artifact projection.
        candidates = artifact_targets or docket_targets
        proceeding_ids = candidates if len(candidates) == 1 else set()
        if len(candidates) > 1:
            ambiguous_fr_intervals += 1
        add_interval(
            proceeding_ids=proceeding_ids,
            docket_ids=dockets,
            rins=rins,
            start=row.get("publication_date"),
            end=row.get("comments_close_on"),
            source="federal_register.comments_close_on",
            evidence_id=document_number,
        )

    rows = _merge_intervals(intervals)
    for row in rows:
        row.update(provenance)
    rows.sort(
        key=lambda row: (
            row["docket_ids_json"],
            row["proceeding_ids_json"],
            row["open_date"],
        )
    )
    out_file = write_parquet_rows(output_dir / OUTPUT, columns=COLUMNS, rows=rows)
    json_stats.log("comment_periods")
    if inverted_by_source:
        logger.warning(
            "comment_periods: skipped {:,} inverted source intervals ({}); examples: {}",
            sum(inverted_by_source.values()),
            ", ".join(
                f"{source}={count:,}"
                for source, count in sorted(inverted_by_source.items())
            ),
            "; ".join(inverted_examples),
        )
    if ambiguous_fr_intervals:
        logger.warning(
            "comment_periods: retained {:,} ambiguous FR intervals with docket-only anchors",
            ambiguous_fr_intervals,
        )
    if ambiguous_document_intervals:
        logger.warning(
            "comment_periods: retained {:,} ambiguous document intervals with docket-only anchors",
            ambiguous_document_intervals,
        )
    if unanchored_intervals:
        logger.warning(
            "comment_periods: skipped {:,} intervals with neither a resolved Proceeding nor a source-backed Docket",
            unanchored_intervals,
        )
    anchor_counts = Counter(
        (row["proceeding_ids_json"], row["docket_ids_json"]) for row in rows
    )
    reopenings = sum(count - 1 for count in anchor_counts.values() if count > 1)
    docket_only = sum(
        row["proceeding_ids_json"] == "[]" and row["docket_ids_json"] != "[]"
        for row in rows
    )
    logger.info(
        "Comment periods: {:,} rows ({:,} reopened; {:,} docket-only)",
        len(rows),
        reopenings,
        docket_only,
    )
    assert pq.ParquetFile(out_file).schema_arrow.names == list(COLUMNS)
    return out_file

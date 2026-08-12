"""Build the unified typed date-event artifact from published parquet tables.

The mission surface is "find out in time to participate": deadline data is
ingested but unpublished, so this tool materializes one typed, digest-pinned
event stream from three published tables:

* ``comment_periods`` — coalesced public-comment intervals (extension
  semantics live upstream in ``build_comment_periods.py``); each in-bounds
  interval emits a ``comment_open`` and a ``comment_close`` event.
* ``fr_docket_links`` — Federal Register document dates; ``effective_on``
  emits ``effective`` and ``comments_close_on`` emits ``comment_close``
  events, deduplicated per (document, date) with docket anchors unioned.
* ``fcc_proceedings`` — FCC ECFS comment and reply-comment windows;
  ``reply_comment_close`` is the FCC-only deadline type with no analogue.
  ECFS records these windows only sometimes ("often null" per the data
  dictionary) — the receipt measures exactly how often in the pinned input.

Sanity bounds are the validated comment-period bounds — close year < 1994,
close year > 2028, duration > 5 x 365 days — applied uniformly to every
source's dates and receipted per source and reason. Out-of-bounds, inverted,
and unparseable rows land in a typed quarantine partition
(``quarantine.parquet``) with machine-readable reasons; nothing is silently
dropped. The bounds were externally validated on ``comment_periods`` only;
their uniform application to the other sources is this artifact's pinned
policy, not an upstream claim.

Outputs (under ``--output``): ``date-events.parquet``, ``quarantine.parquet``
and a deterministic canonical-JSON ``receipt.json`` pinning input digests,
artifact digests, and counts. Rebuilding from byte-identical inputs with the
same library versions reproduces the receipt byte-for-byte (no timestamps
inside sealed surfaces — the pattern from ``draw_search_holdout.py``).

``--slice-output`` additionally emits a small fixture slice (the first N
events per type and first M quarantine rows per source, ordered by
identifier) with its own receipt pinning the parent digests — the vendoring
surface for downstream hermetic tests, following the spicysearch
vocabulary-atlas vendoring pattern.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]

ARTIFACT_SCHEMA_VERSION = "date-event-artifact-v1"
SANITY_BOUNDS_POLICY = "comment-period-sanity-bounds-v1"
SLICE_RULE = "first-n-per-type-by-event-id-v1"

#: Validated bounds (measured on the 302,300-row comment_periods pin):
#: a close year before this is out of bounds.
MIN_YEAR = 1994
#: A close year after this is out of bounds.
MAX_YEAR = 2028
#: An interval longer than this many days is out of bounds.
MAX_DURATION_DAYS = 5 * 365

EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "event_date",
    "document_ref",
    "docket_refs_json",
    "proceeding_refs_json",
    "rin_refs_json",
    "source",
    "evidence_field",
    "evidence_refs_json",
)

QUARANTINE_COLUMNS = (
    "quarantine_id",
    "source",
    "evidence_field",
    "document_ref",
    "docket_refs_json",
    "proceeding_refs_json",
    "rin_refs_json",
    "open_date",
    "close_date",
    "event_date",
    "reasons_json",
)

_COMMENT_PERIOD_COLUMNS = (
    "comment_period_id",
    "proceeding_ids_json",
    "rins_json",
    "docket_ids_json",
    "open_date",
    "close_date",
    "source",
    "evidence_ids_json",
)

_FR_LINK_COLUMNS = ("docket_id", "document_number", "effective_on", "comments_close_on")

_FCC_COLUMNS = (
    "name",
    "comment_start_date",
    "comment_end_date",
    "reply_comment_start_date",
    "reply_comment_end_date",
)

#: The FCC coverage label is a pinned statement about the ingestion design:
#: ``build_fcc_ecfs.py`` bounds a first filings run to the trailing 30 days
#: (FILINGS_FIRST_RUN_DAYS), so FCC filing coverage begins 2026-06-30 in the
#: current pin and nothing earlier is represented.
FCC_COVERAGE_FLOOR_LABEL = (
    "fcc coverage floor: fcc_filings ingestion begins 2026-06-30 "
    "(FILINGS_FIRST_RUN_DAYS=30 first run); nothing before it is represented. "
    "fcc_proceedings window fields are recorded by ECFS only sometimes; see "
    "fcc_proceedings_rows_with_any_window for this pin's measurement."
)

COMMENT_PERIODS_LABEL = (
    "comment_periods intervals arrive extension-coalesced from "
    "build_comment_periods.py; one interval emits one comment_open and one "
    "comment_close event."
)

SANITY_BOUNDS_LABEL = (
    "bounds (close year < 1994, close year > 2028, duration > 1825 days) were "
    "externally validated on comment_periods only; this artifact applies them "
    "uniformly to every source's dates as pinned policy, with per-source "
    "quarantine counts in the receipt."
)


def canonical_json(value: object) -> str:
    """Serialize deterministically (origin: spicy_regs/ontology/common.py:84)."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_id(prefix: str, payload: object, *, length: int = 24) -> str:
    """Content-derived identifier (origin: spicy_regs/ontology/common.py:71)."""

    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest[:length]}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _parse_day(value: object) -> date | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _json_list(value: object) -> list[str]:
    if value is None or str(value).strip() == "":
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return sorted({str(item) for item in parsed if str(item).strip()})


def _bound_reasons(value: date) -> list[str]:
    reasons = []
    if value.year < MIN_YEAR:
        reasons.append("date_before_1994")
    if value.year > MAX_YEAR:
        reasons.append("date_after_2028")
    return reasons


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


class _Collector:
    """Accumulate typed events and typed quarantine rows with counters."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.quarantine: list[dict[str, Any]] = []
        self.inverted: Counter[str] = Counter()
        self.quarantine_reasons: dict[str, Counter[str]] = defaultdict(Counter)

    def add_event(
        self,
        *,
        event_type: str,
        event_date: date,
        source: str,
        evidence_field: str,
        document_ref: str | None = None,
        docket_refs: list[str] | None = None,
        proceeding_refs: list[str] | None = None,
        rin_refs: list[str] | None = None,
        evidence_refs: list[str] | None = None,
    ) -> None:
        row = {
            "event_type": event_type,
            "event_date": event_date.isoformat(),
            "document_ref": document_ref,
            "docket_refs_json": canonical_json(docket_refs or []),
            "proceeding_refs_json": canonical_json(proceeding_refs or []),
            "rin_refs_json": canonical_json(rin_refs or []),
            "source": source,
            "evidence_field": evidence_field,
            "evidence_refs_json": canonical_json(evidence_refs or []),
        }
        row["event_id"] = stable_id("urn:spicy-regs:date-event", row)
        self.events.append(row)

    def add_quarantine(
        self,
        *,
        source: str,
        evidence_field: str,
        reasons: list[str],
        document_ref: str | None = None,
        docket_refs: list[str] | None = None,
        proceeding_refs: list[str] | None = None,
        rin_refs: list[str] | None = None,
        open_date: str | None = None,
        close_date: str | None = None,
        event_date: str | None = None,
    ) -> None:
        ordered_reasons = sorted(set(reasons))
        row = {
            "source": source,
            "evidence_field": evidence_field,
            "document_ref": document_ref,
            "docket_refs_json": canonical_json(docket_refs or []),
            "proceeding_refs_json": canonical_json(proceeding_refs or []),
            "rin_refs_json": canonical_json(rin_refs or []),
            "open_date": open_date,
            "close_date": close_date,
            "event_date": event_date,
            "reasons_json": canonical_json(ordered_reasons),
        }
        row["quarantine_id"] = stable_id("urn:spicy-regs:date-event-quarantine", row)
        self.quarantine.append(row)
        for reason in ordered_reasons:
            self.quarantine_reasons[source][reason] += 1


def _read_rows(path: Path, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return pq.read_table(path, columns=list(columns)).to_pylist()


def _collect_comment_periods(collector: _Collector, path: Path) -> int:
    rows = _read_rows(path, _COMMENT_PERIOD_COLUMNS)
    for row in rows:
        open_day = _parse_day(row.get("open_date"))
        close_day = _parse_day(row.get("close_date"))
        anchors = {
            "docket_refs": _json_list(row.get("docket_ids_json")),
            "proceeding_refs": _json_list(row.get("proceeding_ids_json")),
            "rin_refs": _json_list(row.get("rins_json")),
        }
        evidence_refs = _json_list(row.get("evidence_ids_json"))
        if open_day is None or close_day is None:
            collector.add_quarantine(
                source="comment_periods",
                evidence_field="open_date+close_date",
                reasons=["unparseable_date"],
                open_date=str(row.get("open_date") or "") or None,
                close_date=str(row.get("close_date") or "") or None,
                **anchors,
            )
            continue
        if close_day < open_day:
            collector.inverted["comment_periods"] += 1
            collector.add_quarantine(
                source="comment_periods",
                evidence_field="open_date+close_date",
                reasons=["inverted_interval"],
                open_date=open_day.isoformat(),
                close_date=close_day.isoformat(),
                **anchors,
            )
            continue
        reasons = _bound_reasons(close_day)
        if (close_day - open_day).days > MAX_DURATION_DAYS:
            reasons.append("duration_over_5y")
        if reasons:
            collector.add_quarantine(
                source="comment_periods",
                evidence_field="open_date+close_date",
                reasons=reasons,
                open_date=open_day.isoformat(),
                close_date=close_day.isoformat(),
                **anchors,
            )
            continue
        for event_type, event_day, evidence_field in (
            ("comment_open", open_day, "open_date"),
            ("comment_close", close_day, "close_date"),
        ):
            collector.add_event(
                event_type=event_type,
                event_date=event_day,
                source="comment_periods",
                evidence_field=evidence_field,
                evidence_refs=evidence_refs,
                **anchors,
            )
    return len(rows)


def _collect_fr_docket_links(collector: _Collector, path: Path) -> int:
    rows = _read_rows(path, _FR_LINK_COLUMNS)
    # One published row per (document, docket): deduplicate to one event per
    # (document, field, date) with the docket anchors unioned.
    grouped: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in rows:
        document_number = str(row.get("document_number") or "").strip()
        if not document_number:
            continue
        docket = str(row.get("docket_id") or "").strip()
        for field in ("effective_on", "comments_close_on"):
            value = row.get(field)
            if value is None or str(value).strip() == "":
                continue
            key = (document_number, field, str(value))
            if docket:
                grouped[key].add(docket)
            else:
                grouped.setdefault(key, set())
    event_types = {"effective_on": "effective", "comments_close_on": "comment_close"}
    for (document_number, field, raw_value), dockets in sorted(
        grouped.items(), key=lambda item: item[0]
    ):
        day = _parse_day(raw_value)
        docket_refs = sorted(dockets)
        if day is None:
            collector.add_quarantine(
                source="fr_docket_links",
                evidence_field=field,
                reasons=["unparseable_date"],
                document_ref=document_number,
                docket_refs=docket_refs,
                event_date=raw_value,
            )
            continue
        reasons = _bound_reasons(day)
        if reasons:
            collector.add_quarantine(
                source="fr_docket_links",
                evidence_field=field,
                reasons=reasons,
                document_ref=document_number,
                docket_refs=docket_refs,
                event_date=day.isoformat(),
            )
            continue
        collector.add_event(
            event_type=event_types[field],
            event_date=day,
            source="fr_docket_links",
            evidence_field=field,
            document_ref=document_number,
            docket_refs=docket_refs,
            evidence_refs=[document_number],
        )
    return len(rows)


def _collect_fcc_proceedings(collector: _Collector, path: Path) -> tuple[int, int]:
    rows = _read_rows(path, _FCC_COLUMNS)
    collector.inverted.setdefault("fcc_proceedings", 0)
    rows_with_any_window = 0
    windows = (
        ("comment_start_date", "comment_open"),
        ("comment_end_date", "comment_close"),
        ("reply_comment_end_date", "reply_comment_close"),
    )
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        if any(
            row.get(field) is not None and str(row.get(field)).strip()
            for field in (
                "comment_start_date",
                "comment_end_date",
                "reply_comment_start_date",
                "reply_comment_end_date",
            )
        ):
            rows_with_any_window += 1
        parsed = {field: _parse_day(row.get(field)) for field, _ in windows}
        parsed["reply_comment_start_date"] = _parse_day(row.get("reply_comment_start_date"))
        for start_field, end_field in (
            ("comment_start_date", "comment_end_date"),
            ("reply_comment_start_date", "reply_comment_end_date"),
        ):
            start_day, end_day = parsed.get(start_field), parsed.get(end_field)
            if start_day is not None and end_day is not None and end_day < start_day:
                collector.inverted["fcc_proceedings"] += 1
                collector.add_quarantine(
                    source="fcc_proceedings",
                    evidence_field=f"{start_field}+{end_field}",
                    reasons=["inverted_interval"],
                    proceeding_refs=[name],
                    open_date=start_day.isoformat(),
                    close_date=end_day.isoformat(),
                )
                parsed[start_field] = None
                parsed[end_field] = None
        for field, event_type in windows:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                continue
            day = parsed.get(field)
            if day is None:
                collector.add_quarantine(
                    source="fcc_proceedings",
                    evidence_field=field,
                    reasons=["unparseable_date"],
                    proceeding_refs=[name],
                    event_date=str(value),
                )
                continue
            reasons = _bound_reasons(day)
            if reasons:
                collector.add_quarantine(
                    source="fcc_proceedings",
                    evidence_field=field,
                    reasons=reasons,
                    proceeding_refs=[name],
                    event_date=day.isoformat(),
                )
                continue
            collector.add_event(
                event_type=event_type,
                event_date=day,
                source="fcc_proceedings",
                evidence_field=field,
                proceeding_refs=[name],
                evidence_refs=[name],
            )
    return len(rows), rows_with_any_window


def _write_string_table(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    schema = pa.schema([(column, pa.string()) for column in columns])
    arrays = [pa.array([row.get(column) for row in rows], type=pa.string()) for column in columns]
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path, compression="zstd")


def build_artifact(
    *,
    comment_periods: Path,
    fr_docket_links: Path,
    fcc_proceedings: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Build the artifact and return its (already written) receipt."""

    collector = _Collector()
    collector.inverted.setdefault("comment_periods", 0)
    comment_period_rows = _collect_comment_periods(collector, comment_periods)
    fr_rows = _collect_fr_docket_links(collector, fr_docket_links)
    fcc_rows, fcc_rows_with_any_window = _collect_fcc_proceedings(collector, fcc_proceedings)

    events = sorted(
        collector.events,
        key=lambda row: (row["event_type"], row["event_date"], row["event_id"]),
    )
    quarantine = sorted(collector.quarantine, key=lambda row: (row["source"], row["quarantine_id"]))

    output_dir.mkdir(parents=True, exist_ok=True)
    events_path = output_dir / "date-events.parquet"
    quarantine_path = output_dir / "quarantine.parquet"
    _write_string_table(events_path, EVENT_COLUMNS, events)
    _write_string_table(quarantine_path, QUARANTINE_COLUMNS, quarantine)

    events_by_type: Counter[str] = Counter(row["event_type"] for row in events)
    events_by_source: Counter[str] = Counter(row["source"] for row in events)
    events_by_type_and_source: dict[str, Counter[str]] = defaultdict(Counter)
    for row in events:
        events_by_type_and_source[row["event_type"]][row["source"]] += 1
    quarantined_by_source: Counter[str] = Counter(row["source"] for row in quarantine)

    counts = {
        "events_total": len(events),
        "events_by_type": dict(sorted(events_by_type.items())),
        "events_by_source": dict(sorted(events_by_source.items())),
        "events_by_type_and_source": {
            event_type: dict(sorted(sources.items()))
            for event_type, sources in sorted(events_by_type_and_source.items())
        },
        "quarantined_rows_total": len(quarantine),
        "quarantined_rows_by_source": dict(sorted(quarantined_by_source.items())),
        "quarantine_by_source_and_reason": {
            source: dict(sorted(reasons.items()))
            for source, reasons in sorted(collector.quarantine_reasons.items())
        },
        "inverted_intervals_by_source": dict(sorted(collector.inverted.items())),
    }
    receipt = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "sanity_bounds_policy": SANITY_BOUNDS_POLICY,
        "inputs": {
            "comment_periods": {
                "path": _pin_path(comment_periods),
                "sha256": file_sha256(comment_periods),
                "rows": comment_period_rows,
            },
            "fr_docket_links": {
                "path": _pin_path(fr_docket_links),
                "sha256": file_sha256(fr_docket_links),
                "rows": fr_rows,
            },
            "fcc_proceedings": {
                "path": _pin_path(fcc_proceedings),
                "sha256": file_sha256(fcc_proceedings),
                "rows": fcc_rows,
            },
        },
        "artifacts": {
            "date-events.parquet": {"sha256": file_sha256(events_path), "rows": len(events)},
            "quarantine.parquet": {"sha256": file_sha256(quarantine_path), "rows": len(quarantine)},
        },
        "counts": counts,
        "coverage_labels": {
            "fcc_coverage_floor": FCC_COVERAGE_FLOOR_LABEL,
            "fcc_proceedings_rows_with_any_window": fcc_rows_with_any_window,
            "comment_periods_note": COMMENT_PERIODS_LABEL,
            "sanity_bounds_note": SANITY_BOUNDS_LABEL,
        },
    }
    receipt["artifact_id"] = stable_id("urn:spicy-regs:date-event-artifact", receipt)
    (output_dir / "receipt.json").write_text(canonical_json(receipt) + "\n", encoding="utf-8")
    return receipt


def build_fixture_slice(
    *,
    artifact_dir: Path,
    output_dir: Path,
    events_per_type: int = 25,
    quarantine_rows_per_source: int = 5,
) -> dict[str, Any]:
    """Slice the built artifact for downstream vendored hermetic tests."""

    parent_receipt = json.loads((artifact_dir / "receipt.json").read_text(encoding="utf-8"))
    events = pq.read_table(artifact_dir / "date-events.parquet").to_pylist()
    quarantine = pq.read_table(artifact_dir / "quarantine.parquet").to_pylist()

    sliced_events: list[dict[str, Any]] = []
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        by_type[row["event_type"]].append(row)
    for event_type in sorted(by_type):
        chosen = sorted(by_type[event_type], key=lambda row: row["event_id"])[:events_per_type]
        sliced_events.extend(chosen)
    sliced_events.sort(key=lambda row: (row["event_type"], row["event_date"], row["event_id"]))

    sliced_quarantine: list[dict[str, Any]] = []
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in quarantine:
        by_source[row["source"]].append(row)
    for source in sorted(by_source):
        chosen = sorted(by_source[source], key=lambda row: row["quarantine_id"])
        sliced_quarantine.extend(chosen[:quarantine_rows_per_source])
    sliced_quarantine.sort(key=lambda row: (row["source"], row["quarantine_id"]))

    output_dir.mkdir(parents=True, exist_ok=True)
    events_path = output_dir / "date-events.parquet"
    quarantine_path = output_dir / "quarantine.parquet"
    _write_string_table(events_path, EVENT_COLUMNS, sliced_events)
    _write_string_table(quarantine_path, QUARANTINE_COLUMNS, sliced_quarantine)

    events_by_type: Counter[str] = Counter(row["event_type"] for row in sliced_events)
    receipt = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "slice_rule": SLICE_RULE,
        "parameters": {
            "events_per_type": events_per_type,
            "quarantine_rows_per_source": quarantine_rows_per_source,
        },
        "parent_artifact_id": parent_receipt["artifact_id"],
        "parent_events_sha256": parent_receipt["artifacts"]["date-events.parquet"]["sha256"],
        "parent_quarantine_sha256": parent_receipt["artifacts"]["quarantine.parquet"]["sha256"],
        "sanity_bounds_policy": parent_receipt["sanity_bounds_policy"],
        "coverage_labels": parent_receipt["coverage_labels"],
        "artifacts": {
            "date-events.parquet": {"sha256": file_sha256(events_path), "rows": len(sliced_events)},
            "quarantine.parquet": {
                "sha256": file_sha256(quarantine_path),
                "rows": len(sliced_quarantine),
            },
        },
        "counts": {
            "events_total": len(sliced_events),
            "events_by_type": dict(sorted(events_by_type.items())),
            "quarantined_rows_total": len(sliced_quarantine),
        },
    }
    receipt["artifact_id"] = stable_id("urn:spicy-regs:date-event-artifact-slice", receipt)
    (output_dir / "receipt.json").write_text(canonical_json(receipt) + "\n", encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--comment-periods",
        type=Path,
        default=REPO_ROOT / "output/rin-ontology-revision-candidate/comment_periods.parquet",
    )
    parser.add_argument(
        "--fr-docket-links",
        type=Path,
        default=REPO_ROOT / "output/rin-ontology-revision-candidate/fr_docket_links.parquet",
    )
    parser.add_argument(
        "--fcc-proceedings",
        type=Path,
        default=REPO_ROOT / "output/mixed-real-data-corpus-v2/fcc_proceedings.parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "output/date-event-artifact-2026-08-01",
    )
    parser.add_argument("--slice-output", type=Path, default=None)
    parser.add_argument("--slice-events-per-type", type=int, default=25)
    parser.add_argument("--slice-quarantine-per-source", type=int, default=5)
    args = parser.parse_args(argv)

    receipt = build_artifact(
        comment_periods=args.comment_periods,
        fr_docket_links=args.fr_docket_links,
        fcc_proceedings=args.fcc_proceedings,
        output_dir=args.output,
    )
    print(f"artifact_id: {receipt['artifact_id']}", file=sys.stderr)
    print(f"receipt: {args.output / 'receipt.json'}", file=sys.stderr)
    print(canonical_json(receipt["counts"]), file=sys.stderr)
    if args.slice_output is not None:
        slice_receipt = build_fixture_slice(
            artifact_dir=args.output,
            output_dir=args.slice_output,
            events_per_type=args.slice_events_per_type,
            quarantine_rows_per_source=args.slice_quarantine_per_source,
        )
        print(f"slice artifact_id: {slice_receipt['artifact_id']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

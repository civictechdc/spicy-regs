#!/usr/bin/env python3
"""Gate: published column values against the value lists their publishers document.

``--check`` (the default) diffs the documented domains parsed from the pinned
publisher captures against the checked-in observed snapshot, and fails on any
finding the ledger in :mod:`spicy_regs.sources.source_domains` does not record —
in either direction. It reads no network and no parquet, so it runs anywhere.

``--observe --data-dir DIR`` re-observes the published tables from parquet and
prints the same report against live data; add ``--write-snapshot`` to re-pin the
checked-in snapshot from that observation. ``DIR`` holds the published tables
under their own names (``documents.parquet`` and so on) — a fresh download of
the R2 tables, or any directory built from them.

Exit status: 0 when every finding is recorded, 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spicy_regs.sources.source_domains import (  # noqa: E402
    DEFAULT_SOURCE_DOMAIN_DIR,
    OBSERVED_SNAPSHOT_FILENAME,
    OBSERVED_SNAPSHOT_FORMAT_VERSION,
    DomainFinding,
    ObservedDomain,
    ObservedSnapshot,
    SourceDomainError,
    documented_domains,
    domain_findings,
    load_observed_snapshot,
    stale_accepted_findings,
    unrecorded_findings,
)

SOURCE_DOMAIN_DIR = ROOT / DEFAULT_SOURCE_DOMAIN_DIR

# The published tables the observed half is drawn from, and where each one is
# published. A snapshot records the digest it actually read, so a re-observation
# against a different build is visible rather than silent.
PUBLISHED_TABLE_URLS = {
    "dockets": "https://r2.spicy-regs.dev/dockets.parquet",
    "documents": "https://r2.spicy-regs.dev/documents.parquet",
    "federal_register": "https://r2.spicy-regs.dev/federal_register.parquet",
    "unified_agenda": "https://r2.spicy-regs.dev/unified_agenda.parquet",
}


def _connect():
    import duckdb

    connection = duckdb.connect()
    connection.execute(f"SET home_directory='{tempfile.gettempdir()}'")
    return connection


def _file_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
            length += len(chunk)
    return "sha256:" + digest.hexdigest(), length


def observe(data_dir: Path, *, observed_at: str, producer_revision: str) -> ObservedSnapshot:
    """Scan the published tables in ``data_dir`` for each declared domain's distinct values."""

    documented = documented_domains(SOURCE_DOMAIN_DIR)
    tables = sorted({domain.table for domain in documented.values()})
    connection = _connect()
    sources = []
    row_counts: dict[str, int] = {}
    for table in tables:
        if table not in PUBLISHED_TABLE_URLS:
            raise SourceDomainError(f"no publisher URL is recorded for the published table {table!r}")
        path = data_dir / f"{table}.parquet"
        if not path.is_file():
            raise SourceDomainError(f"{data_dir} lacks the published table {table}.parquet")
        digest, byte_length = _file_identity(path)
        (row_count,) = connection.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()
        row_counts[table] = int(row_count)
        sources.append(
            {
                "byte_length": byte_length,
                "bytes_digest": digest,
                "publisher_url": PUBLISHED_TABLE_URLS[table],
                "row_count": int(row_count),
                "table": table,
            }
        )

    observed: dict[str, ObservedDomain] = {}
    for key, domain in documented.items():
        path = data_dir / f"{domain.table}.parquet"
        rows = connection.execute(
            f"SELECT \"{domain.column}\" AS value, count(*) AS n FROM read_parquet('{path}') "
            f"GROUP BY 1 ORDER BY n DESC, 1"
        ).fetchall()
        null_count = sum(int(count) for value, count in rows if value is None)
        observed[key] = ObservedDomain(
            key=key,
            table=domain.table,
            column=domain.column,
            value_counts=tuple((str(value), int(count)) for value, count in rows if value is not None),
            null_count=null_count,
            row_count=row_counts[domain.table],
        )
    return ObservedSnapshot(
        observed_at=observed_at,
        producer_revision=producer_revision,
        sources=tuple(sources),
        domains=observed,
    )


def render_snapshot(snapshot: ObservedSnapshot) -> str:
    payload = {
        "domains": [
            {
                "column": domain.column,
                "key": domain.key,
                "null_count": domain.null_count,
                "row_count": domain.row_count,
                "table": domain.table,
                "value_counts": [[value, count] for value, count in domain.value_counts],
            }
            for domain in sorted(snapshot.domains.values(), key=lambda one: one.key)
        ],
        "format_version": OBSERVED_SNAPSHOT_FORMAT_VERSION,
        "observed_at": snapshot.observed_at,
        "producer_revision": snapshot.producer_revision,
        "sources": list(snapshot.sources),
    }
    return json.dumps(payload, sort_keys=True, indent=2) + "\n"


def _report(findings: tuple[DomainFinding, ...], unrecorded: tuple[DomainFinding, ...]) -> None:
    for finding in findings:
        support = "" if finding.row_count is None else f" on {finding.row_count:,} rows"
        flag = "UNRECORDED" if finding in unrecorded else "recorded"
        print(f"  [{flag}] {finding.domain_key}: {finding.kind} {finding.value!r}{support}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.observe and not args.data_dir:
        parser.error("--observe needs --data-dir")
    if args.write_snapshot and not args.observe:
        parser.error("--write-snapshot re-pins an observation; it needs --observe")
    if args.write_snapshot and not (args.observed_at and args.producer_revision):
        parser.error("--write-snapshot needs --observed-at and --producer-revision; a snapshot states its provenance")

    documented = documented_domains(SOURCE_DOMAIN_DIR)
    if args.observe:
        snapshot = observe(
            Path(args.data_dir),
            observed_at=args.observed_at,
            producer_revision=args.producer_revision,
        )
    else:
        snapshot = load_observed_snapshot(SOURCE_DOMAIN_DIR)

    findings = domain_findings(documented, snapshot.domains)
    unrecorded = unrecorded_findings(findings)
    stale = stale_accepted_findings(findings)

    print(
        f"{len(documented)} documented domains against {snapshot.observed_at} "
        f"({snapshot.producer_revision[:12]}): {len(findings)} findings"
    )
    _report(findings, unrecorded)

    if args.write_snapshot:
        target = SOURCE_DOMAIN_DIR / OBSERVED_SNAPSHOT_FILENAME
        target.write_text(render_snapshot(snapshot), encoding="utf-8")
        print(f"wrote {target.relative_to(ROOT)}")

    for finding in unrecorded:
        print(
            f"error: {finding.domain_key} {finding.kind} {finding.value!r} is not in "
            "ACCEPTED_DOMAIN_FINDINGS; record it with a reason or fix the drift",
            file=sys.stderr,
        )
    for entry in stale:
        print(
            f"error: ACCEPTED_DOMAIN_FINDINGS records {entry.domain_key} {entry.kind} {entry.value!r}, "
            "which the data no longer produces; delete the entry",
            file=sys.stderr,
        )
    return 1 if unrecorded or stale else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--observe",
        action="store_true",
        help="re-observe from parquet in --data-dir instead of reading the checked-in snapshot",
    )
    parser.add_argument("--data-dir", help="directory holding the published tables as <table>.parquet")
    parser.add_argument("--observed-at", default="", help="observation timestamp to record in a written snapshot")
    parser.add_argument(
        "--producer-revision",
        default="",
        help="SpicyRegs revision that produced the observed tables",
    )
    parser.add_argument("--write-snapshot", action="store_true", help="re-pin the checked-in observed snapshot")
    return parser


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SourceDomainError as error:  # pragma: no cover - CLI surface
        print(f"source-domain error: {error}", file=sys.stderr)
        raise SystemExit(1) from error

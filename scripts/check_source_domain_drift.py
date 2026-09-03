#!/usr/bin/env python3
"""Compare published column values against the value lists their publishers document.

Run with no arguments to diff the documented domains — parsed from the pinned
publisher captures — against the checked-in observed snapshot, failing on any
finding the ledger in :mod:`spicy_regs.sources.source_domains` does not record,
in either direction. It reads no network and no parquet, so it runs anywhere, in
well under a second.

Be precise about what that default run is. Both of its inputs are files in this
repository: a publisher capture pinned by digest, and one dated observation of
the published tables. Neither is refetched here, so the result is a function of
the tree and changes only when someone changes one of those files. That makes
this a lock on a dated finding, not a live drift detector — it fails the moment
a re-pin moves either half without the ledger moving with it, which is exactly
when a human is looking. The offline half runs in CI through the test suite;
refreshing the observed half is the manual step below.

``--observe --data-dir DIR`` re-observes the published tables from parquet and
prints the same report against that data; add ``--write-snapshot`` to re-pin the
checked-in snapshot from that observation. ``DIR`` holds the published tables
under their own names (``documents.parquet`` and so on) — a fresh download of
the published corpus, or any directory built from it. Re-pinning a capture is
the other manual step: refetch it from the ``source_url`` its manifest entry
names, and record the digest and length of what came back.

Exit status: 0 when every finding is recorded, 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path

from spicy_regs.data_dictionary import DEFAULT_R2_BASE_URL, TABLES
from spicy_regs.sources.source_domains import (
    DEFAULT_SOURCE_DOMAIN_DIR,
    OBSERVED_SNAPSHOT_FILENAME,
    OBSERVED_SNAPSHOT_FORMAT_VERSION,
    DocumentedDomain,
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

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DOMAIN_DIR = ROOT / DEFAULT_SOURCE_DOMAIN_DIR


def published_table_url(table: str) -> str:
    """Where a published table is served, derived from the registry that publishes it."""

    return f"{DEFAULT_R2_BASE_URL}/{table}.parquet"


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


def observe(
    data_dir: Path,
    documented: Mapping[str, DocumentedDomain],
    *,
    observed_at: str,
    producer_revision: str,
) -> ObservedSnapshot:
    """Scan the published tables in ``data_dir`` for each declared domain's distinct values.

    Takes the register the caller already parsed, so the observation and the diff
    it feeds are the same set of domains read from the same captures once.
    """

    tables = sorted({domain.table for domain in documented.values()})
    connection = _connect()
    sources = []
    row_counts: dict[str, int] = {}
    for table in tables:
        # Where the table is published comes from the registry that publishes it,
        # never from a second list kept here. A hand-kept copy is how the first
        # version of this snapshot recorded a hostname the project does not serve.
        if table not in TABLES:
            raise SourceDomainError(f"{table!r} is not one of the published tables in spicy_regs.data_dictionary")
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
                "publisher_url": published_table_url(table),
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
            documented,
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

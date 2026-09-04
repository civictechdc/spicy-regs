"""Transform: build ``bill_subjects.parquet`` — one bill's subject assignment.

The seam this closes is stated in :mod:`~spicy_regs.transforms.build_congress_bills`:
``congress_bills`` is list-level only, and the Library of Congress subject
assignment (one ``policy_area`` from a ~33-term controlled list, plus any number
of ``legislative subjects``) is detail-endpoint-only. This is the per-bill
enrichment pass that fetches it, via
:class:`~spicy_regs.sources.bill_subjects.BillSubjectsFetcher`.

**Why a sibling table and not extra columns on ``congress_bills``.** The two
artifacts have utterly different fetch economics: the list ingest walks a handful
of pages a day, while enrichment is one request per bill across a 192k-row
archive. Folding them together would put a multi-week backfill inside a daily
30-minute job, and would give one R2 key two writers. So this publishes its own
artifact on its own cron, keyed by the same ``bill_id`` — the ``fr_docket_links``
shape, joined with a one-line ``LEFT JOIN``::

    SELECT b.*, s.policy_area, s.subjects_json
    FROM congress_bills b LEFT JOIN bill_subjects s USING (bill_id)

**Incremental, resumable, bounded.** Each run:

1. Best-effort downloads the prior ``bill_subjects.parquet`` from R2.
2. Selects bills in ``congress_bills.parquet`` that the current carrier has not
   answered for yet, newest Congress first, capped at :data:`MAX_BILLS_PER_RUN`
   so the run fits inside the CI timeout (the ``build_lobbying_filings``
   ``MAX_WINDOW_DAYS`` idea, counted in bills rather than days).
3. Fetches each, writing a row only when the carrier *answered*. A timeout or a
   5xx writes nothing, so the bill is simply picked up next run — no half-written
   state, and a re-run is never wasted work.
4. Merges prior + new on ``bill_id``, preferring the fresh row.

A bill the carrier definitively 404s gets a row with a null ``policy_area`` and
its carrier recorded, so the next run does not ask again. Should a key later
appear and switch the run to the deeper Congress.gov carrier, those rows *are*
re-asked — the carrier that had nothing is not the carrier that might.
"""

from __future__ import annotations

from typing import Protocol

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.bill_subjects import (
    CARRIER_API,
    CARRIER_BULKDATA,
    FIRST_CONGRESS,
    BillSubjects,
    BillSubjectsFetcher,
    FetchCounts,
    FetchCounts,
)

OUTPUT = "bill_subjects.parquet"
BILLS_INPUT = "congress_bills.parquet"

#: Bills to enrich per run, per carrier. Each is what that carrier's crawl rate
#: fits inside the reusable rollup workflow's 30-minute timeout: GPO's bulk data
#: answers at ~4.7 bills a second (5,000 in ~18 minutes), while Congress.gov's
#: documented 5,000-requests-an-hour budget caps the API carrier at 1.33 a
#: second (2,000 in ~25 minutes). A cap of 5,000 on the API carrier would spend
#: the whole hourly budget in one run and leave nothing for a retry.
MAX_BILLS_PER_RUN = {CARRIER_API: 2_000, CARRIER_BULKDATA: 5_000}

#: The published schema: all VARCHAR, keyed by ``bill_id``, joinable straight to
#: ``congress_bills``. The subject list is a JSON string so the table stays flat
#: and portable, matching ``lobbying_filings``' JSON-valued columns.
COLUMNS = (
    "bill_id",
    "policy_area",
    "subjects_json",
    "subject_count",
    "carrier",
    "enriched_at",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _shape(bill_id: str, policy_area: str | None, subjects: tuple[str, ...], carrier: str, now: str) -> dict:
    """Map one fetched assignment onto the published column shape."""
    return {
        "bill_id": bill_id,
        "policy_area": policy_area,
        "subjects_json": json.dumps(list(subjects)),
        "subject_count": str(len(subjects)),
        "carrier": carrier,
        "enriched_at": now,
    }


def _pending_bills(
    bills_file: Path,
    prior_file: Path,
    *,
    carrier: str,
    have_prior: bool,
    limit: int,
) -> list[tuple[str, str, str, str]]:
    """Bills this carrier still owes an answer for, newest Congress first.

    A bill is pending when the prior table has no row for it, or has a row that
    another carrier left empty — the current carrier may well hold what that one
    did not. Bills below the carrier's coverage floor are never asked for; the
    request could only 404, and burning the run's budget on a guaranteed miss is
    how a backfill stops making progress.
    """
    import duckdb

    floor = FIRST_CONGRESS[carrier]
    if have_prior:
        # The OR group is parenthesized on purpose: without it the coverage-floor
        # and not-null filters below would bind only to the second branch, and a
        # first-time bill would be asked for regardless of the floor.
        pending = f"""
            LEFT JOIN read_parquet('{prior_file}') AS prior ON prior.bill_id = bill.bill_id
            WHERE (
                prior.bill_id IS NULL
                OR (prior.policy_area IS NULL AND prior.carrier IS DISTINCT FROM '{carrier}')
            )
        """
    else:
        pending = "WHERE TRUE"

    rows = duckdb.sql(
        f"""
        SELECT bill.bill_id, bill.congress, bill.bill_type, bill.bill_number
        FROM read_parquet('{bills_file}') AS bill
        {pending}
          AND bill.bill_id IS NOT NULL
          AND bill.bill_type IS NOT NULL
          AND bill.bill_number IS NOT NULL
          AND TRY_CAST(bill.congress AS INTEGER) >= {floor}
        ORDER BY TRY_CAST(bill.congress AS INTEGER) DESC, bill.bill_id
        LIMIT {int(limit)}
        """
    ).fetchall()
    return [(str(a), str(b), str(c), str(d)) for a, b, c, d in rows]


def _adopt_local_output(out_file: Path, prior_file: Path) -> bool:
    """Treat an artifact left by a previous local run as this run's prior."""
    if not out_file.exists():
        return False
    out_file.replace(prior_file)
    logger.info("Bill subjects: adopting the local {} as this run's prior", out_file.name)
    return True


def _log_counts(counts: FetchCounts, *, carrier: str, attempted: int, elapsed: float) -> None:
    """Report the run the way the repo's other incremental transforms do."""
    rate = attempted / elapsed if elapsed > 0 else 0.0
    logger.info(
        "Bill subjects: carrier={} attempted={:,} answered={:,} with_policy_area={:,} "
        "subjects_only={:,} unassigned={:,} not_held={:,} failed={:,} in {:.0f}s ({:.1f} bills/s)",
        carrier,
        attempted,
        counts.answered,
        counts.with_policy_area,
        counts.subjects_only,
        counts.unassigned,
        counts.not_held,
        counts.failed,
        elapsed,
        rate,
    )
    for name, n in sorted(counts.policy_areas.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        logger.info("Bill subjects:   {:>6,}  {}", n, name)


def _log_coverage(out_file: Path, bills_file: Path) -> None:
    """State enriched/total against the bill table, in named numbers."""
    import duckdb

    row = duckdb.sql(
        f"""
        SELECT
            (SELECT count(*) FROM read_parquet('{bills_file}')) AS bills,
            (SELECT count(*) FROM read_parquet('{out_file}')) AS rows,
            (SELECT count(*) FROM read_parquet('{out_file}') WHERE policy_area IS NOT NULL) AS with_area
        """
    ).fetchone()
    if not row:
        return
    bills, rows, with_area = row
    share = (100.0 * with_area / bills) if bills else 0.0
    logger.info(
        "Bill subjects: {:,} rows, {:,} with a policy area, against {:,} bills ({:.1f}% of the archive)",
        rows,
        with_area,
        bills,
        share,
    )


class SubjectsFetcher(Protocol):
    """What the enrichment pass needs from a fetcher: the real one, or a test's."""

    carrier: str
    counts: FetchCounts

    @property
    def first_congress(self) -> int: ...
    def subjects_for(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None: ...
    def close(self) -> None: ...


def enrich_bill_subjects(
    output_dir: Path,
    *,
    max_bills: int | None = None,
    fetcher: SubjectsFetcher | None = None,
) -> Path:
    """Build ``bill_subjects.parquet`` (bounded, resumable enrichment pass).

    ``max_bills`` defaults to the chosen carrier's per-run cap, which is what
    that carrier's crawl rate fits inside the CI timeout.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    bills_file = output_dir / BILLS_INPUT
    prior_file = output_dir / "_bill_subjects_prior.parquet"

    if not bills_file.exists():
        raise RuntimeError(f"Bill subjects: {BILLS_INPUT} not found in {output_dir} — prime it from R2 first")

    # 1. Pull the prior table (best effort — absence just means a fresh backfill).
    #    A backfill spread over many runs must never lose ground, so a local
    #    artifact from a previous run counts as a prior in its own right: without
    #    this, an R2-less re-run would silently overwrite everything enriched so
    #    far and start again from the top of the archive.
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file) or _adopt_local_output(out_file, prior_file)
    if have_prior:
        logger.info("Bill subjects: merging against prior table {}", prior_file)
    else:
        logger.info("Bill subjects: no prior table found — starting the backfill")

    owns_fetcher = fetcher is None
    fetcher = fetcher or BillSubjectsFetcher()
    if max_bills is None:
        max_bills = MAX_BILLS_PER_RUN[fetcher.carrier]
    logger.info(
        "Bill subjects: carrier {} (bills from the {}th Congress on), up to {:,} bills this run",
        fetcher.carrier,
        fetcher.first_congress,
        max_bills,
    )

    # 2. Pick this run's slice, newest Congress first.
    pending = _pending_bills(
        bills_file,
        prior_file,
        carrier=fetcher.carrier,
        have_prior=have_prior,
        limit=max_bills,
    )
    logger.info("Bill subjects: {:,} bills pending this run", len(pending))

    # 3. Fetch. Only an answer becomes a row; a failure leaves the bill for next run.
    now = datetime.now(UTC).isoformat(timespec="seconds")
    started = datetime.now(UTC)
    rows: list[dict] = []
    try:
        for seen, (bill_id, congress, bill_type, bill_number) in enumerate(pending, start=1):
            result = fetcher.subjects_for(congress, bill_type, bill_number)
            if result is not None:
                rows.append(_shape(bill_id, result.policy_area, result.subjects, result.carrier, now))
            if seen % 1_000 == 0:
                logger.info("Bill subjects: {:,}/{:,} bills fetched...", seen, len(pending))
    finally:
        if owns_fetcher:
            fetcher.close()
    elapsed = (datetime.now(UTC) - started).total_seconds()
    _log_counts(fetcher.counts, carrier=fetcher.carrier, attempted=len(pending), elapsed=elapsed)

    # 4. Merge prior + new, dedup on bill_id preferring the fresh row.
    new_file = output_dir / "_bill_subjects_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY bill_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE bill_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY enriched_at DESC, bill_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    _log_coverage(out_file, bills_file)
    return out_file

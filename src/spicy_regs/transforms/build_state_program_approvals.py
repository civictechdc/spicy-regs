"""Transform: build ``state_program_approvals.parquet`` from ``federal_register``.

One row per (Federal Register document, state) for rules and proposed rules in
which a federal agency approves, authorizes or incorporates a state's own rules
or program: EPA state implementation plans, RCRA and UST authorizations, OSM's
SMCRA state programs, OSHA state plans, and so on.

**Phase 1 (metadata).** A document qualifies when one of its CFR references is
on the allow-list in :mod:`~spicy_regs.transforms.state_programs`; its state(s)
come from the title. No downloads.

**Phase 2 (state citations).** For states with a citation parser
(:data:`~spicy_regs.transforms.state_programs.CITATION_STATES`, Virginia and
Maryland for now) the document body is fetched from ``body_html_url`` and
parsed for state code citations (``9VAC5-10-20``, ``COMAR 26.11.08.10``) and
the state effective date. Results carry over from the previously published
table, so a daily run only fetches documents it hasn't parsed yet, at most
:data:`MAX_FETCHES_PER_RUN` of them (newest first); the first build fills in
over several nightly runs. A parser
change bumps ``PARSER_VERSION``, which is stamped in the Parquet file metadata;
a version mismatch discards the carried-over results and re-parses everything.

``state_citations_json`` is NULL when the body was not parsed (state has no
parser yet, or the fetch failed and will be retried next run) and ``[]`` when it
was parsed and nothing was found. A run aborts before writing when more than
:data:`MAX_FETCH_FAILURE_RATE` of its fetches fail, so an outage can't publish
a table whose citations quietly went missing.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.fr_bodies import FrBodyUnavailable, fetch_body_html, make_client
from spicy_regs.transforms.state_programs import (
    CITATION_STATES,
    PROSE_DATE_PROGRAMS,
    PARSER_VERSION,
    extract_citations,
    program_for,
    states_in_title,
)

OUTPUT = "state_program_approvals.parquet"

SCHEMA = pa.schema(
    [
        ("document_number", pa.string()),
        ("publication_date", pa.string()),
        ("document_type", pa.string()),
        ("title", pa.string()),
        ("jurisdiction", pa.string()),
        ("program", pa.string()),
        ("cfr_title", pa.int64()),
        ("cfr_part", pa.int64()),
        ("docket_ids_json", pa.string()),
        ("state_citations_json", pa.string()),
        ("state_effective_date", pa.string()),
        ("html_url", pa.string()),
    ]
)
COLUMNS = tuple(SCHEMA.names)

_PARSER_VERSION_KEY = b"spicy_regs.citation_parser_version"

#: Abort when more than this share of body fetches fail (with a small absolute
#: allowance so one dead link in a small incremental run doesn't page).
MAX_FETCH_FAILURE_RATE = 0.05
_MIN_FAILURES_TO_ABORT = 5

#: Pause between body fetches, to stay polite to federalregister.gov.
FETCH_PAUSE_S = 0.2

#: Bodies fetched per run, newest documents first. federalregister.gov takes
#: ~5 s to render a body it hasn't cached, so the first build (~930 VA/MD
#: bodies) or a PARSER_VERSION bump would outrun the 30-minute job timeout.
#: The rest stay NULL and are picked up by later runs, since results carry over.
MAX_FETCHES_PER_RUN = 200

Fetch = Callable[[str], str]


def select_rows(fr_file: Path) -> list[dict]:
    """Phase 1: allow-listed rules/proposed rules exploded to one row per state."""
    import duckdb

    con = duckdb.connect()
    cursor = con.execute(
        f"""
        SELECT document_number, publication_date, document_type, title,
               cfr_references_json, docket_ids_json, html_url, body_html_url
        FROM read_parquet('{fr_file}')
        WHERE document_type IN ('Rule', 'Proposed Rule')
          AND cfr_references_json IS NOT NULL
        """
    )
    names = [d[0] for d in cursor.description]
    rows: list[dict] = []
    for record in cursor.fetchall():
        doc = dict(zip(names, record, strict=True))
        try:
            refs = json.loads(doc["cfr_references_json"])
        except (TypeError, ValueError):
            continue
        match = program_for(refs if isinstance(refs, list) else None)
        if match is None:
            continue
        program, cfr_title, cfr_part = match
        for state in states_in_title(doc["title"]):
            rows.append(
                {
                    "document_number": doc["document_number"],
                    "publication_date": doc["publication_date"],
                    "document_type": doc["document_type"],
                    "title": doc["title"],
                    "jurisdiction": state,
                    "program": program,
                    "cfr_title": cfr_title,
                    "cfr_part": cfr_part,
                    "docket_ids_json": doc["docket_ids_json"],
                    "state_citations_json": None,
                    "state_effective_date": None,
                    "html_url": doc["html_url"],
                    "_body_html_url": doc["body_html_url"],
                }
            )
    con.close()
    return rows


def load_prior_citations(prior_file: Path) -> dict[tuple[str, str], tuple[str, str | None]]:
    """Parsed citations from the prior table, keyed by (document_number, jurisdiction).

    Returns ``{}`` when the prior table was written by a different parser version.
    """
    meta = pq.read_metadata(prior_file).metadata or {}
    prior_version = meta.get(_PARSER_VERSION_KEY, b"").decode() or None
    if prior_version != str(PARSER_VERSION):
        logger.info(
            "State programs: prior table parser version {} != {} — re-parsing all bodies",
            prior_version,
            PARSER_VERSION,
        )
        return {}
    table = pq.read_table(
        prior_file, columns=["document_number", "jurisdiction", "state_citations_json", "state_effective_date"]
    )
    return {
        (r["document_number"], r["jurisdiction"]): (r["state_citations_json"], r["state_effective_date"])
        for r in table.to_pylist()
        if r["state_citations_json"] is not None
    }


def attach_citations(
    rows: list[dict],
    prior: dict[tuple[str, str], tuple[str, str | None]],
    fetch: Fetch,
    *,
    pause_s: float = FETCH_PAUSE_S,
    max_fetches: int | None = None,
) -> tuple[int, int]:
    """Phase 2: fill citations for supported states in place. Returns (fetched, failed).

    Fetches at most ``max_fetches`` bodies (default :data:`MAX_FETCHES_PER_RUN`),
    newest publication date first.
    """
    if max_fetches is None:
        max_fetches = MAX_FETCHES_PER_RUN
    pending: dict[str, list[dict]] = {}
    reused = 0
    for row in rows:
        if row["jurisdiction"] not in CITATION_STATES:
            continue
        cached = prior.get((row["document_number"], row["jurisdiction"]))
        if cached is not None:
            row["state_citations_json"], row["state_effective_date"] = cached
            reused += 1
        elif row["_body_html_url"]:
            pending.setdefault(row["_body_html_url"], []).append(row)

    queue = sorted(pending.items(), key=lambda item: item[1][0]["publication_date"] or "", reverse=True)
    deferred = max(0, len(queue) - max_fetches)
    queue = queue[:max_fetches]
    logger.info(
        "State programs: {:,} bodies to fetch, {:,} deferred to later runs ({:,} rows reused)",
        len(queue),
        deferred,
        reused,
    )
    fetched = failed = 0
    for i, (url, doc_rows) in enumerate(queue, 1):
        if i > 1 and pause_s:
            time.sleep(pause_s)
        try:
            body = fetch(url)
        except FrBodyUnavailable as exc:
            failed += 1
            logger.warning("State programs: {}", exc)
            continue
        fetched += 1
        for row in doc_rows:
            citations, effective = extract_citations(
                row["jurisdiction"], body, prose_dates=row.get("program") in PROSE_DATE_PROGRAMS
            )
            row["state_citations_json"] = json.dumps(citations)
            # A state rule is in effect before the federal agency acts on it; a
            # later date was read from text about the federal action itself.
            pub = row.get("publication_date")
            row["state_effective_date"] = None if effective and pub and effective > pub else effective
        if i % 100 == 0:
            logger.info("State programs: {:,}/{:,} bodies fetched", i, len(queue))

    attempts = fetched + failed
    if failed >= _MIN_FAILURES_TO_ABORT and failed > attempts * MAX_FETCH_FAILURE_RATE:
        raise RuntimeError(
            f"State programs: {failed:,} of {attempts:,} body fetches failed "
            f"(> {MAX_FETCH_FAILURE_RATE:.0%}) — refusing to write the table"
        )
    return fetched, failed


def write_table(rows: list[dict], out_file: Path) -> None:
    rows = sorted(rows, key=lambda r: r["publication_date"] or "", reverse=True)
    rows.sort(key=lambda r: r["jurisdiction"])  # stable: by state, newest first
    table = pa.Table.from_pylist([{c: r[c] for c in COLUMNS} for r in rows], schema=SCHEMA)
    table = table.replace_schema_metadata({_PARSER_VERSION_KEY: str(PARSER_VERSION).encode()})
    # Small row groups so `WHERE jurisdiction = ?` prunes.
    pq.write_table(table, out_file, compression="zstd", row_group_size=10_000)


def build_state_program_approvals(output_dir: Path, *, fetch: Fetch | None = None) -> Path:
    """Build ``state_program_approvals.parquet`` from ``federal_register.parquet``."""
    fr_file = output_dir / "federal_register.parquet"
    if not fr_file.exists():
        raise FileNotFoundError(f"federal_register.parquet not found in {output_dir}")

    rows = select_rows(fr_file)
    logger.info(
        "State programs: {:,} rows across {:,} documents",
        len(rows),
        len({r["document_number"] for r in rows}),
    )

    prior_file = output_dir / "_state_program_approvals_prior.parquet"
    prior: dict[tuple[str, str], tuple[str, str | None]] = {}
    if prior_file.exists() or r2.download(OUTPUT, prior_file):
        prior = load_prior_citations(prior_file)
        prior_file.unlink(missing_ok=True)
    else:
        logger.info("State programs: no prior table — parsing every supported body")

    if fetch is None:
        with make_client() as client:
            fetched, failed = attach_citations(rows, prior, lambda url: fetch_body_html(client, url))
    else:
        fetched, failed = attach_citations(rows, prior, fetch)
    logger.info("State programs: fetched {:,} bodies, {:,} failed (retried next run)", fetched, failed)

    out_file = output_dir / OUTPUT
    write_table(rows, out_file)
    logger.info("State program approvals: {:,} rows", len(rows))
    return out_file

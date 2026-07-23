"""Transforms: build ``fcc_proceedings.parquet`` and ``fcc_filings.parquet``.

The FCC layer of the dataset: proceedings are the FCC's docket equivalent and
filings are its comment equivalent, both ingested from the ECFS public API
(see :mod:`spicy_regs.sources.fcc_ecfs`). Column conventions match the other
external-source tables — all VARCHAR, array fields serialized as JSON strings.

Both tables are incremental, following the federal_register pattern:

1. Best-effort download the prior parquet from R2.
2. Fetch only records dated since the prior table's max date (minus a short
   overlap to catch late-disseminated / corrected records).
3. Dedup the union on the table key, preferring the freshly fetched row.

With no prior table, proceedings backfill from the ECFS epoch (~20K records —
cheap). Filings do NOT: ECFS holds tens of millions of filings, so a filings
first run is bounded to the last :data:`FILINGS_FIRST_RUN_DAYS` days unless an
explicit ``since`` (``FCC_SINCE``) is passed. Deeper backfills are expected to
run scoped to specific proceedings (``FCC_PROCEEDINGS``) and/or in date slices.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.fcc_ecfs import ECFS_EPOCH, FccEcfsFilingsReader, FccEcfsProceedingsReader

PROCEEDINGS_OUTPUT = "fcc_proceedings.parquet"
FILINGS_OUTPUT = "fcc_filings.parquet"

# Re-scan this many days before the last stored date on each run, so records
# disseminated or corrected after their nominal date are picked up.
OVERLAP_DAYS = 7

# A filings run with no prior table and no explicit since covers this many
# trailing days instead of walking the (enormous) full archive.
FILINGS_FIRST_RUN_DAYS = 30

PROCEEDING_COLUMNS = (
    "name",
    "id_proceeding",
    "description",
    "bureau_code",
    "bureau_name",
    "rulemaking_or_docket",
    "filing_status",
    "date_created",
    "date_closed",
    "comment_start_date",
    "comment_end_date",
    "reply_comment_start_date",
    "reply_comment_end_date",
    "filed_by",
)
_PROCEEDING_SCHEMA = pa.schema([(c, pa.string()) for c in PROCEEDING_COLUMNS])

FILING_COLUMNS = (
    "id_submission",
    "proceeding_names_json",
    "submission_type",
    "express_comment",
    "date_received",
    "date_submission",
    "date_disseminated",
    "filing_status",
    "viewing_status",
    "exparte_or_late_filed",
    "filers_json",
    "authors_json",
    "lawfirms_json",
    "bureaus_json",
    "text_data",
    "total_page_count",
    "documents_json",
    "filing_url",
)
_FILING_SCHEMA = pa.schema([(c, pa.string()) for c in FILING_COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL (ids/counts/flags come as ints)."""
    if value is None:
        return None
    return str(value)


def _names(entries: list | None) -> list[str]:
    """Extract the ``name`` of each dict in an API list field, dropping empties."""
    out: list[str] = []
    for entry in entries or []:
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str) and name:
                out.append(name)
    return out


def _dict_field(raw: dict, key: str) -> dict:
    """A dict-valued API field, or {} when absent/mistyped."""
    value = raw.get(key)
    return value if isinstance(value, dict) else {}


def _shape_proceeding(raw: dict) -> dict:
    """Map one raw ECFS proceeding onto the published column shape."""
    # The proceedings endpoint nests the bureau as {code, name}; filings embed
    # proceedings with flat bureau_code / bureau_name. Accept both.
    bureau = _dict_field(raw, "bureau")
    return {
        "name": raw.get("name"),
        "id_proceeding": _s(raw.get("id_proceeding")),
        "description": raw.get("description_display") or raw.get("description"),
        "bureau_code": bureau.get("code") or raw.get("bureau_code"),
        "bureau_name": bureau.get("name") or raw.get("bureau_name"),
        "rulemaking_or_docket": raw.get("flag_rulemaking_or_docket"),
        "filing_status": raw.get("filingStatus"),
        "date_created": raw.get("date_proceeding_created"),
        "date_closed": raw.get("date_closed"),
        "comment_start_date": raw.get("comment_start_date"),
        "comment_end_date": raw.get("comment_end_date"),
        "reply_comment_start_date": raw.get("comment_reply_start_date"),
        "reply_comment_end_date": raw.get("comment_reply_end_date"),
        "filed_by": raw.get("filed_by"),
    }


def _shape_filing(raw: dict) -> dict:
    """Map one raw ECFS filing onto the published column shape."""
    id_submission = _s(raw.get("id_submission"))
    submissiontype = _dict_field(raw, "submissiontype")
    filingstatus = _dict_field(raw, "filingstatus")
    viewingstatus = _dict_field(raw, "viewingstatus")
    raw_documents = raw.get("documents")
    documents = raw_documents if isinstance(raw_documents, list) else []
    return {
        "id_submission": id_submission,
        "proceeding_names_json": json.dumps(_names(raw.get("proceedings"))),
        "submission_type": submissiontype.get("description"),
        "express_comment": _s(raw.get("express_comment")),
        "date_received": raw.get("date_received"),
        "date_submission": raw.get("date_submission"),
        "date_disseminated": raw.get("date_disseminated"),
        "filing_status": filingstatus.get("description"),
        "viewing_status": viewingstatus.get("description"),
        "exparte_or_late_filed": raw.get("exparte_or_late_filed"),
        "filers_json": json.dumps(_names(raw.get("filers"))),
        "authors_json": json.dumps(_names(raw.get("authors"))),
        "lawfirms_json": json.dumps(_names(raw.get("lawfirms"))),
        "bureaus_json": json.dumps(_names(raw.get("bureaus"))),
        "text_data": raw.get("text_data"),
        "total_page_count": _s(raw.get("total_page_count")),
        "documents_json": json.dumps(
            [
                {"filename": d.get("filename"), "src": d.get("src")}
                for d in documents
                if isinstance(d, dict) and (d.get("filename") or d.get("src"))
            ]
        ),
        "filing_url": f"https://www.fcc.gov/ecfs/filing/{id_submission}" if id_submission else None,
    }


def _prior_max_date(prior_file: Path, column: str) -> date | None:
    """Largest ``column`` value in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max({column}) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _merge_incremental(
    output_dir: Path,
    *,
    output: str,
    scratch_prefix: str,
    columns: tuple[str, ...],
    schema: pa.Schema,
    key: str,
    order_by: str,
    rows: list[dict],
    prior_file: Path,
    have_prior: bool,
) -> Path:
    """Union prior + freshly fetched rows, dedup on ``key`` preferring fresh."""
    import duckdb

    out_file = output_dir / output
    new_file = output_dir / f"{scratch_prefix}_new.parquet"
    table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(columns)
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
                    PARTITION BY {key} ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE {key} IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY {order_by} DESC, {key}
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)
    return out_file


def build_fcc_proceedings(output_dir: Path, *, since: date | None = None) -> Path:
    """Build ``fcc_proceedings.parquet`` (incremental merge with the prior table)."""
    prior_file = output_dir / "_fcc_proceedings_prior.parquet"

    have_prior = prior_file.exists() or r2.download(PROCEEDINGS_OUTPUT, prior_file)
    if since is None:
        prior_max = _prior_max_date(prior_file, "date_created") if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else ECFS_EPOCH
    logger.info("FCC proceedings: fetching proceedings created since {}", since)

    reader = FccEcfsProceedingsReader(since=since)
    rows = [_shape_proceeding(p) for p in reader.iter_records()]
    logger.info("FCC proceedings: fetched {:,} proceedings this run", len(rows))

    out = _merge_incremental(
        output_dir,
        output=PROCEEDINGS_OUTPUT,
        scratch_prefix="_fcc_proceedings",
        columns=PROCEEDING_COLUMNS,
        schema=_PROCEEDING_SCHEMA,
        key="name",
        order_by="date_created",
        rows=rows,
        prior_file=prior_file,
        have_prior=have_prior,
    )
    total = pq.ParquetFile(out).metadata.num_rows
    logger.info("FCC proceedings: {:,} rows", total)
    return out


def build_fcc_filings(
    output_dir: Path,
    *,
    since: date | None = None,
    proceedings: tuple[str, ...] = (),
) -> Path:
    """Build ``fcc_filings.parquet`` (incremental merge with the prior table).

    ``proceedings`` scopes the fetch to specific proceeding names — used for
    targeted backfills of big dockets without walking all of ECFS.
    """
    prior_file = output_dir / "_fcc_filings_prior.parquet"

    have_prior = prior_file.exists() or r2.download(FILINGS_OUTPUT, prior_file)
    if since is None:
        prior_max = _prior_max_date(prior_file, "date_received") if have_prior else None
        if prior_max:
            since = prior_max - timedelta(days=OVERLAP_DAYS)
        else:
            since = date.today() - timedelta(days=FILINGS_FIRST_RUN_DAYS)
            logger.warning(
                "FCC filings: no prior table — bounding the first run to the last {} days "
                "(since {}). Pass FCC_SINCE / FCC_PROCEEDINGS for a deeper, scoped backfill.",
                FILINGS_FIRST_RUN_DAYS,
                since,
            )
    logger.info("FCC filings: fetching filings received since {} (proceedings={})", since, proceedings or "all")

    reader = FccEcfsFilingsReader(since=since, proceedings=proceedings)
    rows = [_shape_filing(f) for f in reader.iter_records()]
    logger.info("FCC filings: fetched {:,} filings this run", len(rows))

    out = _merge_incremental(
        output_dir,
        output=FILINGS_OUTPUT,
        scratch_prefix="_fcc_filings",
        columns=FILING_COLUMNS,
        schema=_FILING_SCHEMA,
        key="id_submission",
        order_by="date_received",
        rows=rows,
        prior_file=prior_file,
        have_prior=have_prior,
    )
    total = pq.ParquetFile(out).metadata.num_rows
    logger.info("FCC filings: {:,} rows", total)
    return out

"""Transform: build ``court_opinion_bodies.parquet`` — actual opinion text.

This is the table the ``court-opinion-v1`` profile has been forward-declaring.
That profile lists ``html_with_citations`` and ``plain_text`` among its text
columns, and until now no published table carried either: they are CourtListener
opinion fields, and nothing in the corpus read CourtListener opinions. The bulk
``opinions`` dump carries both verbatim, so this builder is where the declared
seam finally resolves against real bytes.

**One opinion per row, not one decision per row.** A cluster (see
``build_court_opinion_clusters``) is the decision; an opinion is one voice within
it — majority, concurrence, dissent, each with its own author and its own text.
``cluster_id`` joins the two, and through the cluster's ``cl_docket_id`` to
``court_dockets``.

**Why this ingest is bounded, and by what.** The 2026-06-30 ``opinions`` dump is
50.8 GiB compressed and about 422 GiB decompressed at its observed 8.3x ratio.
Two independent limits bite:

* *Disk.* Landing the compressed dump alone would take this workstation below
  the 100 GiB free-space floor the project holds itself to. It is therefore
  streamed and never written to disk, and the full backfill is refused outright
  when headroom is short — see ``check_headroom``.
* *Time.* The bucket serves one connection at roughly 1.7-2.0 MiB/s, so a single
  full pass is about 8.6 hours. That is a scheduled-job cost, not a workstation
  one.

So a local run takes a *recorded slice*: ``max_records`` and/or
``max_compressed_bytes`` bound it, and the builder logs exactly what the bound
produced — rows, id range, date range, bytes read — so the resulting table's
coverage is a stated number rather than an impression. ``cluster_ids`` narrows a
pass to specific decisions, which is how the documented full backfill targets the
APA docket set without keeping the other ~10 million opinions.
"""

from __future__ import annotations

import shutil
from collections.abc import Container, Sized
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.courtlistener_bulk import (
    CourtListenerBulkReader,
    find_dump,
    latest_dump_date,
    list_bulk_dumps,
)

OUTPUT = "court_opinion_bodies.parquet"
DATASET = "opinions"

#: Free space this project refuses to eat into, in bytes. A bulk ingest that
#: would cross it is stopped and recorded rather than run.
DISK_HEADROOM_FLOOR = 100 * 2**30

#: Compressed parquet bytes one opinion row costs, measured on the 250,000-row
#: 2026-08-22 build: 1,735,931,994 bytes / 250,000 = 6,944 B/row, rounded up.
BYTES_PER_OPINION_ROW = 8 * 2**10

#: Opinions per cluster, used as a ceiling when sizing a ``cluster_ids`` pass.
#: The most any single cluster carried in that same 250,000-row build was 8
#: (majority, concurrences, dissents, and the odd combined rendering); doubling
#: it keeps the estimate an over-estimate, which is the only direction a disk
#: guard may err in.
OPINIONS_PER_CLUSTER_CEILING = 16

#: Rows buffered before each parquet batch write. Opinion bodies are large, so
#: this batch is much smaller than the cluster builder's.
BATCH_ROWS = 2_000

#: The raw dump columns that can hold a body. CourtListener populates whichever
#: one the upstream source provided, so "has text" is a question about the set,
#: not about ``plain_text`` alone.
_TEXT_FIELDS = (
    "plain_text",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "html_with_citations",
    "xml_harvard",
    "xml_scan",
)

COLUMNS = (
    "opinion_id",
    "cluster_id",
    "opinion_type",
    "author_str",
    "author_id",
    "joined_by_str",
    "per_curiam",
    "sha1",
    "page_count",
    "download_url",
    "local_path",
    "extracted_by_ocr",
    "plain_text",
    "html_with_citations",
    "available_text_fields",
    "text_char_count",
    "date_created",
    "date_modified",
    "dump_date",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def check_headroom(needed_bytes: int, *, path: Path | None = None) -> None:
    """Refuse an ingest that would take free space below the project floor.

    Raises rather than warning: a run that silently fills the disk is worse than
    a run that did not happen, and the whole point of recording sizes first is to
    be able to make this call before the bytes arrive.
    """
    usage = shutil.disk_usage(path or Path.home())
    remaining = usage.free - needed_bytes
    if remaining < DISK_HEADROOM_FLOOR:
        raise RuntimeError(
            f"CourtListener bulk: refusing to ingest {needed_bytes / 2**30:.1f} GiB — "
            f"would leave {remaining / 2**30:.1f} GiB free, below the "
            f"{DISK_HEADROOM_FLOOR / 2**30:.0f} GiB floor "
            f"(currently {usage.free / 2**30:.1f} GiB free)"
        )


def estimate_output_bytes(dump_size: int, cluster_ids: Container[str] | None) -> int:
    """Bytes on disk an unbounded pass will actually cost.

    The dump is *streamed* — decompressed inline, never landed — so what the
    volume pays for is the parquet this run writes, not the 50.8 GiB it reads.
    For an unfiltered pass those are close enough that the dump's compressed size
    is the right stand-in (the measured output is 45-55 GiB). For a
    ``cluster_ids`` pass they are nothing alike: the same 8.6 hours of reading
    produces a table sized by the *targets*, and charging it 50.8 GiB refuses a
    run that costs megabytes.

    Falling back to the dump size for a filter whose length is unknowable keeps
    the guard conservative when it cannot do arithmetic.
    """
    if cluster_ids is None or not isinstance(cluster_ids, Sized):
        return dump_size
    return len(cluster_ids) * OPINIONS_PER_CLUSTER_CEILING * BYTES_PER_OPINION_ROW


def _shape(row: dict, *, dump_date: date | None) -> dict:
    """Map one bulk ``opinions`` CSV row onto the published columns."""
    present = [name for name in _TEXT_FIELDS if row.get(name)]
    plain = _s(row.get("plain_text"))
    with_citations = _s(row.get("html_with_citations"))
    longest = max((len(str(row[name])) for name in present), default=0)
    return {
        "opinion_id": _s(row.get("id")),
        "cluster_id": _s(row.get("cluster_id")),
        "opinion_type": _s(row.get("type")),
        "author_str": _s(row.get("author_str")),
        "author_id": _s(row.get("author_id")),
        "joined_by_str": _s(row.get("joined_by_str")),
        "per_curiam": _s(row.get("per_curiam")),
        "sha1": _s(row.get("sha1")),
        "page_count": _s(row.get("page_count")),
        "download_url": _s(row.get("download_url")),
        "local_path": _s(row.get("local_path")),
        "extracted_by_ocr": _s(row.get("extracted_by_ocr")),
        "plain_text": plain,
        "html_with_citations": with_citations,
        "available_text_fields": ",".join(present) if present else None,
        "text_char_count": str(longest),
        "date_created": _s(row.get("date_created")),
        "date_modified": _s(row.get("date_modified")),
        "dump_date": dump_date.isoformat() if dump_date else None,
    }


class _BatchWriter:
    """Append shaped rows to a parquet file in bounded batches."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows: list[dict] = []
        self.written = 0
        self._writer: pq.ParquetWriter | None = None

    def add(self, row: dict) -> None:
        self.rows.append(row)
        if len(self.rows) >= BATCH_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=_SCHEMA)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, _SCHEMA, compression="zstd")
        self._writer.write_table(table)
        self.written += len(self.rows)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is None:
            pq.write_table(_SCHEMA.empty_table(), self.path, compression="zstd")
        else:
            self._writer.close()


def build_court_opinion_bodies(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_records: int | None = None,
    max_compressed_bytes: int | None = None,
    cluster_ids: Container[str] | None = None,
) -> Path:
    """Build ``court_opinion_bodies.parquet`` from a bounded slice of the dump.

    Returns the written path. The exact bound reached is logged and is the number
    that belongs in the coverage record — this builder never claims completeness
    it did not achieve.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_bodies_prior.parquet"
    new_file = output_dir / "_bodies_new.parquet"

    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    logger.info(
        "Opinion bodies: {}",
        f"merging against prior table {prior_file}" if have_prior else "no prior table — first build",
    )

    if local_file is None:
        objects = list_bulk_dumps()
        resolved = dump_date or latest_dump_date(objects, DATASET)
        if resolved is None:
            raise RuntimeError(f"CourtListener bulk: no published {DATASET} dump found")
        published = find_dump(objects, DATASET, resolved)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {DATASET} dump for {resolved}")
        logger.info(
            "Opinion bodies: dump {} is {:.3f} GiB compressed; bound = {} rows / {} bytes / {} cluster ids",
            published.filename,
            published.size / 2**30,
            max_records or "unbounded",
            max_compressed_bytes or "unbounded",
            len(cluster_ids) if isinstance(cluster_ids, Sized) else "unbounded",
        )
        # An unbounded pass would stream the whole dump. Check that the *output*
        # it implies still leaves headroom before a single byte moves.
        if max_records is None and max_compressed_bytes is None:
            needed = estimate_output_bytes(published.size, cluster_ids)
            logger.info(
                "Opinion bodies: unbounded pass, estimated output {:.3f} GiB",
                needed / 2**30,
            )
            check_headroom(needed, path=output_dir)
    else:
        resolved = dump_date
        logger.info("Opinion bodies: reading local dump {}", local_file)

    row_filter = None
    if cluster_ids is not None:

        def row_filter(row: dict) -> bool:  # noqa: F811
            return (row.get("cluster_id") or "") in cluster_ids

    writer = _BatchWriter(new_file)
    reader = CourtListenerBulkReader(
        DATASET,
        dump_date=resolved,
        local_file=local_file,
        max_records=max_records,
        max_compressed_bytes=max_compressed_bytes,
        row_filter=row_filter,
    )
    for row in reader.iter_records():
        writer.add(_shape(row, dump_date=resolved))
    writer.close()

    logger.info(
        "Opinion bodies: bound reached — {:,} rows scanned, {:,} kept, {:.3f} GiB compressed read",
        reader.rows_scanned,
        reader.rows_yielded,
        reader.compressed_bytes / 2**30,
    )

    # A first build has no prior table to merge against and one input, whose
    # opinion_id is the publisher's primary key — so the dedup cannot remove a
    # row and the whole COPY is a sort. It is an expensive sort: ranking 250,000
    # rows that each carry kilobytes of opinion text ran the 4 GiB duckdb budget
    # out of memory, and holding the staged and merged copies at once is 3.5 GiB
    # of disk against a 100 GiB floor. So promote the staged file instead, and
    # pay for it in row order.
    if not have_prior:
        new_file.replace(out_file)
        prior_file.unlink(missing_ok=True)
        total = pq.ParquetFile(out_file).metadata.num_rows
        logger.info("Court opinion bodies: {:,} rows (first build, dump order)", total)
        return out_file

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
                    PARTITION BY opinion_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE opinion_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY CAST(opinion_id AS BIGINT) DESC
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2000);
        """
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court opinion bodies: {:,} rows", total)
    return out_file

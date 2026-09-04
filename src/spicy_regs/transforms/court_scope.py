"""Where a decision was decided — the court scope the cluster dump omits.

``court_opinion_clusters`` is the whole CourtListener corpus: ten million
decisions from 3,361 courts, of which 397 are federal and 2,618 are state. A
consumer asking "what have the federal courts said" has no way to ask it,
because the ``opinion-clusters`` dump carries no ``court_id`` at all. That
column lives on the *docket*, one join away and in a different 4.67 GiB file.

This module is that join, in two pieces:

* :func:`build_docket_court_map` streams the ``dockets`` dump for two columns
  and throws the other fifty away. It is 46 minutes of reading to produce a few
  hundred megabytes, which is the cheapest form the answer comes in — there is
  no smaller published dataset that maps a docket to its court (checked against
  the publisher's own listing of 46 datasets, 2026-08-22).
* :func:`court_jurisdictions` reads the 81 KB ``courts`` dump, which is where
  ``F``/``FD``/``FB``/``FS``/``FBP`` (federal) part company with ``ST``/``S``
  (state) and the tribal, territorial and military codes.

**"Federal" here means the publisher's jurisdiction code begins with F.** That
is a decision about someone else's taxonomy, so it is one function
(:func:`is_federal`) rather than a condition spelled out at each call site, and
the raw code travels alongside the boolean so a consumer who disagrees can
reclassify without re-reading 4.67 GiB.
"""

from __future__ import annotations

import json
from array import array
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources.courtlistener_bulk import (
    CourtListenerBulkReader,
    published_object_pin,
)

DOCKETS_DATASET = "dockets"
COURTS_DATASET = "courts"

#: Rows buffered per parquet batch. Two narrow columns, so this can be large.
BATCH_ROWS = 500_000

#: ``docket_number`` is not needed to answer "which court", and it is captured
#: anyway because the pass that gets ``court_id`` has the row in hand and a
#: second pass costs 111 minutes of the publisher's bandwidth.
#:
#: It is here for a measured reason. CourtListener carries **two docket records
#: for one case** — a RECAP/PACER one and a scraper one — and the opinion cluster
#: hangs off the scraper one while a nature-of-suit search returns the RECAP one.
#: Measured 2026-08-22, that hides at least 249 APA decisions, 184 of them in
#: D.D.C., which matched 0.0% of its 1,571 APA dockets against peers at 15-17%.
#: Two records for one case share a docket number within a court, so
#: ``(court_id, docket_number)`` is the exact join where matching case-name
#: prose is a guess.
MAP_COLUMNS = ("cl_docket_id", "court_id", "docket_number")

#: The columns :class:`CourtScope` actually reads. Named separately so a map
#: captured before ``docket_number`` existed still loads.
SCOPE_COLUMNS = ("cl_docket_id", "court_id")

_MAP_SCHEMA = pa.schema([(c, pa.string()) for c in MAP_COLUMNS])

#: CourtListener jurisdiction codes that denote a federal court, as published in
#: the ``courts`` dump: F (appellate), FD (district), FB (bankruptcy), FBP
#: (bankruptcy appellate panel), FS (special). Every one starts with F, and no
#: non-federal code does — state is S/ST/SA/SS/SAG, tribal T*, territorial TT/TS,
#: military M*, international I, committee C.
FEDERAL_PREFIX = "F"


def is_federal(jurisdiction: str | None) -> bool:
    """Whether a CourtListener jurisdiction code denotes a federal court."""
    return bool(jurisdiction) and str(jurisdiction).startswith(FEDERAL_PREFIX)


def court_jurisdictions(*, dump_date: date | None = None, local_file: Path | None = None) -> dict[str, str]:
    """Map ``court_id`` to its published jurisdiction code.

    The ``courts`` dump is 81 KB, so this is read whole and held in memory —
    3,361 rows is a dict, not a table.
    """
    reader = CourtListenerBulkReader(COURTS_DATASET, dump_date=dump_date, local_file=local_file)
    codes = {row["id"]: (row.get("jurisdiction") or "") for row in reader.iter_records() if row.get("id")}
    federal = sum(1 for code in codes.values() if is_federal(code))
    logger.info(
        "Court scope: {:,} courts, {:,} federal / {:,} not",
        len(codes),
        federal,
        len(codes) - federal,
    )
    return codes


class CourtScope:
    """Answer "which court, and is it federal" for a docket id, in constant time.

    The obvious shape is a dict, and the obvious shape does not fit: the
    ``dockets`` dump holds roughly 72 million rows, and 72 million Python string
    keys is gigabytes before a single value. But docket ids are dense integers
    and there are only 3,361 courts, so the map is a *dense array of small
    integers* — one ``unsigned short`` per docket id, indexing a court
    vocabulary — which is about 150 MB for the whole corpus and is built once
    per run.

    Index 0 is reserved for "no court recorded", so a docket the dump does not
    place is distinguishable from one placed in the first court seen.
    """

    __slots__ = ("_courts", "_index", "_jurisdictions", "size")

    def __init__(self, courts: list[str], index: array, jurisdictions: dict[str, str]):
        self._courts = courts
        self._index = index
        self._jurisdictions = jurisdictions
        self.size = len(index)

    @classmethod
    def from_map(cls, map_file: Path, jurisdictions: dict[str, str]) -> CourtScope:
        """Load a ``(cl_docket_id, court_id)`` parquet into the dense index."""
        parquet = pq.ParquetFile(map_file)
        courts: list[str] = [""]
        court_slots: dict[str, int] = {}
        index = array("H", [0])
        rows = 0

        def grow_to(position: int) -> None:
            """Extend the index so ``position`` is addressable, doubling as it goes."""
            target = max(position + 1, len(index) * 2)
            index.frombytes(bytes(index.itemsize * (target - len(index))))

        for batch in parquet.iter_batches(batch_size=1_000_000, columns=list(SCOPE_COLUMNS)):
            docket_ids = batch.column("cl_docket_id").to_pylist()
            court_ids = batch.column("court_id").to_pylist()
            for docket_id, court_id in zip(docket_ids, court_ids, strict=True):
                rows += 1
                if not docket_id or not court_id:
                    continue
                try:
                    position = int(docket_id)
                except ValueError:
                    continue
                if position < 0:
                    continue
                slot = court_slots.get(court_id)
                if slot is None:
                    slot = len(courts)
                    court_slots[court_id] = slot
                    courts.append(court_id)
                if position >= len(index):
                    grow_to(position)
                index[position] = slot
        logger.info(
            "Court scope: indexed {:,} dockets across {:,} courts ({:.0f} MiB)",
            rows,
            len(courts) - 1,
            index.buffer_info()[1] * index.itemsize / 2**20,
        )
        return cls(courts, index, jurisdictions)

    def for_docket(self, docket_id: str | None) -> tuple[str | None, str | None, str | None]:
        """``(court_id, jurisdiction, is_federal)`` for one docket, all NULL if unknown."""
        if not docket_id:
            return None, None, None
        try:
            position = int(docket_id)
        except ValueError:
            return None, None, None
        if position < 0 or position >= self.size:
            return None, None, None
        slot = self._index[position]
        if slot == 0:
            return None, None, None
        court_id = self._courts[slot]
        return court_id, *self.for_court(court_id)[1:]

    def for_court(self, court_id: str | None) -> tuple[str | None, str | None, str | None]:
        """``(court_id, jurisdiction, is_federal)`` for a court named directly.

        The ``/search/?type=o`` catch-up already knows its court, so it does not
        need the docket map — but it does need the same classification, spelled
        the same way.
        """
        if not court_id:
            return None, None, None
        jurisdiction = self._jurisdictions.get(court_id)
        if jurisdiction is None:
            # A court the dump does not describe: say so rather than call it
            # non-federal, which is a claim this data cannot support.
            return court_id, None, None
        return court_id, jurisdiction, "t" if is_federal(jurisdiction) else "f"


def build_docket_court_map(
    output_dir: Path,
    *,
    dump_date: date | None = None,
    local_file: Path | None = None,
    max_compressed_bytes: int | None = None,
) -> Path:
    """Stream the ``dockets`` dump into a ``(cl_docket_id, court_id)`` table.

    Cached by dump date: the map only changes when the publisher cuts a new
    dump, so a second local build in the same quarter costs nothing rather than
    46 minutes. Delete the file to force a re-read.
    """
    stamp = dump_date.isoformat() if dump_date else "local"
    out_file = output_dir / f"docket_courts-{stamp}.parquet"
    if out_file.exists():
        cached = pq.ParquetFile(out_file)
        missing = [c for c in MAP_COLUMNS if c not in cached.schema_arrow.names]
        logger.info(
            "Court scope: reusing cached docket->court map {} ({:,} rows)",
            out_file,
            cached.metadata.num_rows,
        )
        if missing:
            # Not rebuilt automatically: that is 111 minutes of someone else's
            # bandwidth, and it is not this function's call to spend it. Said
            # loudly instead, because the alternative is a reconciliation that
            # silently cannot run.
            logger.warning(
                "Court scope: cached map predates {} — delete it to recapture. "
                "Duplicate-docket reconciliation is unavailable without it.",
                ", ".join(missing),
            )
        return out_file

    output_dir.mkdir(parents=True, exist_ok=True)
    reader = CourtListenerBulkReader(
        DOCKETS_DATASET,
        dump_date=dump_date,
        local_file=local_file,
        max_compressed_bytes=max_compressed_bytes,
    )
    staging = out_file.with_suffix(".partial.parquet")
    writer: pq.ParquetWriter | None = None
    batch: list[dict] = []
    written = 0

    def flush() -> None:
        nonlocal writer, written
        if not batch:
            return
        table = pa.Table.from_pylist(batch, schema=_MAP_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(staging, _MAP_SCHEMA, compression="zstd")
        writer.write_table(table)
        written += len(batch)
        batch.clear()

    try:
        for row in reader.iter_records():
            docket_id = row.get("id")
            if not docket_id:
                continue
            batch.append(
                {
                    "cl_docket_id": docket_id,
                    "court_id": row.get("court_id"),
                    "docket_number": row.get("docket_number"),
                }
            )
            if len(batch) >= BATCH_ROWS:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()
        elif staging.exists():
            staging.unlink()

    if writer is None:
        pq.write_table(_MAP_SCHEMA.empty_table(), staging, compression="zstd")
    # Only name the file after the pass that filled it finished, so an
    # interrupted 46-minute stream cannot be mistaken for a cached complete map.
    staging.replace(out_file)
    logger.info(
        "Court scope: docket->court map has {:,} dockets ({:.2f} GiB compressed read, {} resume(s)) -> {:.1f} MiB",
        written,
        reader.compressed_bytes / 2**30,
        reader.resumes,
        out_file.stat().st_size / 2**20,
    )
    write_map_receipt(out_file, reader=reader, dump_date=dump_date, rows=written)
    return out_file


def write_map_receipt(
    map_file: Path,
    *,
    reader: CourtListenerBulkReader,
    dump_date: date | None,
    rows: int,
) -> Path:
    """Record what the map cost and which published object it came from.

    46 minutes of someone else's bandwidth is a capture, and a capture that
    cannot say what it read is a file. The object is pinned by the publisher's
    own byte size and last-modified stamp, so a map built against a re-cut dump
    is a detectable difference rather than an unexplained one.
    """
    receipt: dict[str, object] = {
        "artifact": map_file.name,
        "written_at": datetime.now(UTC).isoformat(),
        "bounds": {
            "columns": list(MAP_COLUMNS),
            "max_compressed_bytes": reader.max_compressed_bytes,
            "rows_scanned": reader.rows_scanned,
            "rows_written": rows,
            "compressed_bytes_read": reader.compressed_bytes,
            "resumes": reader.resumes,
            "stopped_early": reader.stopped_early,
        },
        "result": {"bytes": map_file.stat().st_size, "dockets": rows},
    }
    if dump_date is not None and reader.local_file is None:
        try:
            receipt["source"] = {
                "publisher": "CourtListener bulk data",
                **published_object_pin(DOCKETS_DATASET, dump_date),
            }
        except Exception as exc:  # noqa: BLE001 - a receipt must not fail a capture
            receipt["source"] = {"publisher": "CourtListener bulk data", "error": str(exc)}
    else:
        receipt["source"] = {
            "publisher": "CourtListener bulk data",
            "local_file": str(reader.local_file) if reader.local_file else None,
            "dump_date": dump_date.isoformat() if dump_date else None,
        }
    path = map_file.with_suffix(".receipt.json")
    path.write_text(json.dumps(receipt, indent=2) + "\n")
    logger.info("Court scope: receipt written to {}", path)
    return path

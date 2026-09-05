"""Hermetic tests for the CourtListener bulk-dump ingest (no network).

Covers the pieces with real logic: parsing the publisher's S3 listing into
dataset/date pairs (which is how coverage gets checked at all), the streaming
bzip2 CSV reader and its two bounds, the raw-row → published-schema mappings for
both new tables, and the disk-headroom guard that is supposed to refuse a
backfill rather than fill the volume.
"""

from __future__ import annotations

import bz2
import io
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.courtlistener_bulk import (
    BulkObject,
    CourtListenerBulkReader,
    find_dump,
    latest_dump_date,
)
from spicy_regs.transforms.build_court_opinion_bodies import (
    COLUMNS as BODY_COLUMNS,
)
from spicy_regs.transforms.build_court_opinion_bodies import (
    BYTES_PER_OPINION_ROW,
    DISK_HEADROOM_FLOOR,
    OPINIONS_PER_CLUSTER_CEILING,
    check_headroom,
    estimate_output_bytes,
)
from spicy_regs.transforms.build_court_opinion_bodies import _shape as shape_body
from spicy_regs.transforms.build_court_opinion_clusters import (
    COLUMNS as CLUSTER_COLUMNS,
)
from spicy_regs.transforms.build_court_opinion_clusters import (
    _shape_bulk,
    _shape_search,
)

# One real row from the 2026-06-30 opinions dump's column set, trimmed to the
# fields the transform reads. `html_lawbox` is populated while `plain_text` is
# empty — the exact case that makes "no text" and "no plain_text" different
# questions.
_RAW_OPINION = {
    "id": "11422346",
    "cluster_id": "10954746",
    "type": "010combined",
    "author_str": "Amit P. Mehta",
    "author_id": "3227",
    "joined_by_str": "",
    "per_curiam": "f",
    "sha1": "f1b0a6170c709af85ed4f18f1ff2d7cbebcc697d",
    "page_count": "17",
    "download_url": "https://ecf.dcd.uscourts.gov/cgi-bin/show_public_doc?2025cv3854-20",
    "local_path": "pdf/2026/08/21/zeevi_v._united_states_department_of_state.pdf",
    "extracted_by_ocr": "f",
    "plain_text": "",
    "html": "",
    "html_lawbox": "<p>MEMORANDUM OPINION</p>",
    "html_with_citations": "",
    "date_created": "2026-08-21T14:02:11.000Z",
    "date_modified": "2026-08-21T14:02:11.000Z",
}

_RAW_CLUSTER = {
    "id": "10954746",
    "docket_id": "73299709",
    "case_name": "Zeevi v. United States Department of State",
    "case_name_short": "Zeevi",
    "case_name_full": "Yoav ZEEVI v. UNITED STATES DEPARTMENT OF STATE",
    "date_filed": "2026-08-21",
    "date_filed_is_approximate": "f",
    "judges": "Amit P. Mehta",
    "precedential_status": "Published",
    "citation_count": "0",
    "slug": "zeevi-v-united-states-department-of-state",
    "date_created": "2026-08-21T14:02:11.000Z",
}


def _csv_bz2(tmp_path: Path, name: str, header: str, rows: list[str]) -> Path:
    """Write a bzip2 CSV the reader can stream, exactly as the dumps are shaped."""
    path = tmp_path / name
    body = "\n".join([header, *rows]) + "\n"
    path.write_bytes(bz2.compress(body.encode("utf-8")))
    return path


# -- listing / enumeration ---------------------------------------------------


def test_bulk_object_splits_dataset_from_dump_date():
    """Coverage is checked per dataset per dump, so both must parse out of the key."""
    obj = BulkObject("bulk-data/opinion-clusters-2026-06-30.csv.bz2", 2_457_231_057, "2026-06-30T04:11:47.000Z")
    assert obj.dataset == "opinion-clusters"
    assert obj.dump_date == date(2026, 6, 30)
    assert obj.filename == "opinion-clusters-2026-06-30.csv.bz2"
    assert obj.url.endswith("/bulk-data/opinion-clusters-2026-06-30.csv.bz2")

    # The bucket also holds undated one-off exports; those must not masquerade
    # as a dated dump of some dataset.
    undated = BulkObject("bulk-data/scotus_network.csv", 7_000, "2024-04-04T00:00:00.000Z")
    assert undated.dump_date is None
    assert undated.dataset == "scotus_network"


def test_published_object_pin_identifies_what_a_capture_read():
    """A receipt naming a filename has named a filename, not a thing.

    The publisher's listing carries the object's exact byte size and
    last-modified stamp, which is what makes two runs comparable and makes "the
    dump was re-cut under us" a detectable event rather than an unexplained
    difference in row counts. DocSpec pins the *population* at
    ``fixtures/courtlistener-bulk-v1/``; this pins the one object a run read,
    and lets that be checked against DocSpec's before the reading starts.
    """
    from spicy_regs.sources.courtlistener_bulk import published_object_pin

    listing = [
        BulkObject(
            "bulk-data/opinions-2026-06-30.csv.bz2",
            54_561_543_156,
            "2026-06-30T09:56:48.000Z",
        ),
        BulkObject("bulk-data/courts-2026-06-30.csv.bz2", 81_180, "2026-06-30T09:00:26.000Z"),
    ]

    pin = published_object_pin("opinions", date(2026, 6, 30), objects=listing)
    assert pin["bytes"] == 54_561_543_156
    assert pin["last_modified"] == "2026-06-30T09:56:48.000Z"
    assert pin["filename"] == "opinions-2026-06-30.csv.bz2"
    assert pin["listing_object_count"] == 2

    # Held against an expectation, it is a precondition rather than a note —
    # which is the only useful place to discover a changed object when reading
    # it costs 8.6 hours.
    published_object_pin(
        "opinions",
        date(2026, 6, 30),
        objects=listing,
        expect_bytes=54_561_543_156,
        expect_last_modified="2026-06-30T09:56:48.000Z",
    )
    with pytest.raises(RuntimeError, match="the publisher's object changed"):
        published_object_pin("opinions", date(2026, 6, 30), objects=listing, expect_bytes=1)
    with pytest.raises(RuntimeError, match="the publisher's object changed"):
        published_object_pin(
            "opinions",
            date(2026, 6, 30),
            objects=listing,
            expect_last_modified="2026-07-01T00:00:00.000Z",
        )
    with pytest.raises(RuntimeError, match="no opinions dump published"):
        published_object_pin("opinions", date(2026, 3, 31), objects=listing)


def test_latest_dump_date_and_find_dump_pick_one_published_object():
    objects = [
        BulkObject("bulk-data/opinions-2026-03-31.csv.bz2", 54_190_000_000, ""),
        BulkObject("bulk-data/opinions-2026-06-30.csv.bz2", 54_561_543_156, ""),
        BulkObject("bulk-data/courts-2026-06-30.csv.bz2", 81_180, ""),
    ]
    assert latest_dump_date(objects, "opinions") == date(2026, 6, 30)
    assert latest_dump_date(objects, "nonexistent") is None

    found = find_dump(objects, "opinions", date(2026, 6, 30))
    assert found is not None and found.size == 54_561_543_156
    assert find_dump(objects, "opinions", date(2020, 1, 1)) is None


# -- streaming reader --------------------------------------------------------


def test_reader_streams_rows_and_normalizes_blanks(tmp_path: Path):
    path = _csv_bz2(
        tmp_path,
        "courts-2026-06-30.csv.bz2",
        "id,short_name,jurisdiction,notes",
        ["ca9,Ninth Circuit,F,", "scotus,Supreme Court,F,seat of last resort"],
    )
    rows = list(CourtListenerBulkReader("courts", local_file=path).iter_records())
    assert [r["id"] for r in rows] == ["ca9", "scotus"]
    # An empty CSV field is absence, not the empty string — downstream NULL
    # handling depends on that being decided here rather than per-transform.
    assert rows[0]["notes"] is None
    assert rows[1]["notes"] == "seat of last resort"


def test_reader_honors_the_record_bound_and_reports_it(tmp_path: Path):
    """A bounded run must be *recorded* as bounded, so coverage is not overclaimed."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id",
        [f"{i},{i * 10}" for i in range(1, 51)],
    )
    reader = CourtListenerBulkReader("opinions", local_file=path, max_records=7)
    rows = list(reader.iter_records())
    assert len(rows) == 7
    assert reader.rows_yielded == 7
    assert reader.stopped_early is True

    unbounded = CourtListenerBulkReader("opinions", local_file=path)
    assert len(list(unbounded.iter_records())) == 50
    assert unbounded.stopped_early is False


def test_reader_applies_the_row_filter_before_materializing(tmp_path: Path):
    """Filtering is what makes a targeted pass over a 50 GiB dump affordable."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id",
        ["1,100", "2,200", "3,100"],
    )
    wanted = {"100"}
    reader = CourtListenerBulkReader(
        "opinions", local_file=path, row_filter=lambda row: (row.get("cluster_id") or "") in wanted
    )
    rows = list(reader.iter_records())
    assert [r["id"] for r in rows] == ["1", "3"]
    assert reader.rows_scanned == 3
    assert reader.rows_yielded == 2


def test_reader_reads_the_dumps_backslash_escaped_quotes(tmp_path: Path):
    """The dumps escape an embedded quote as ``\\"``, not as the doubled ``""``.

    Read with the stdlib default dialect this does not raise — it *desyncs*, and
    the prose after the escaped quote becomes the next record's first column.
    Measured on the real 2026-06-30 opinion-clusters dump that corrupted 1,987 of
    the first 3,000 rows and dropped ``docket_id`` on two thirds of them, which
    would have silently destroyed the docket join. A regression here is a data
    corruption, not a parse error, so it is pinned.
    """
    path = _csv_bz2(
        tmp_path,
        "opinion-clusters-2026-06-30.csv.bz2",
        "id,case_name,docket_id",
        [r'"7290305","Ex parte \"Doe\", Inc.","64278691"', '"7290306","Plain v. Simple","64278692"'],
    )
    rows = list(CourtListenerBulkReader("opinion-clusters", local_file=path).iter_records())
    assert [r["id"] for r in rows] == ["7290305", "7290306"]
    assert rows[0]["case_name"] == 'Ex parte "Doe", Inc.'
    # The row after the escaped quote must still be a row, with its join key intact.
    assert rows[0]["docket_id"] == "64278691"
    assert rows[1]["docket_id"] == "64278692"


def test_reader_handles_embedded_newlines_in_opinion_text(tmp_path: Path):
    """Opinion bodies contain newlines inside quoted fields; a naive line split loses rows."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,plain_text",
        ['1,"line one\nline two"', "2,short"],
    )
    rows = list(CourtListenerBulkReader("opinions", local_file=path).iter_records())
    assert len(rows) == 2
    assert rows[0]["plain_text"] == "line one\nline two"


# -- resuming a long transfer ------------------------------------------------


class _FlakyResponse:
    """Serve bytes from an offset and die once, the way a long socket does."""

    def __init__(
        self,
        payload: bytes,
        *,
        offset: int,
        fail_after: int | None,
        status: int | None = None,
    ) -> None:
        self._payload = payload
        self._pos = offset
        self._served = 0
        self._fail_after = fail_after
        self.status = status if status is not None else (206 if offset else 200)

    def read(self, size: int) -> bytes:
        if self._fail_after is not None and self._served >= self._fail_after:
            raise OSError("connection reset by peer")
        chunk = self._payload[self._pos : self._pos + size]
        self._pos += len(chunk)
        self._served += len(chunk)
        return chunk

    def close(self) -> None:
        return None


def test_counting_stream_resumes_a_dropped_transfer_at_the_exact_offset(monkeypatch):
    """8.6 hours on one socket will be interrupted; the pass must survive it.

    The resumed request must start at the compressed byte already consumed and
    feed the *same* decompressor — bzip2 wants its bytes in order, not in one
    connection. If the offset were wrong the failure would not be an error, it
    would be corrupt text, so this pins the recovered bytes against the original.
    """
    from spicy_regs.sources import courtlistener_bulk
    from spicy_regs.sources.courtlistener_bulk import _CountingStream

    original = ("id,body\n" + "".join(f"{i},row {i}\n" for i in range(4000))).encode()
    payload = bz2.compress(original)
    # Read in small bites so the drop lands mid-dump, as a real one would.
    monkeypatch.setattr(courtlistener_bulk, "_CHUNK", 1024)
    assert len(payload) > 4096, "test payload must be big enough to interrupt mid-stream"

    ranges: list[int] = []

    def reopen(offset: int):
        ranges.append(offset)
        return _FlakyResponse(payload, offset=offset, fail_after=None)

    stream = _CountingStream(_FlakyResponse(payload, offset=0, fail_after=2048), reopen=reopen)
    recovered = io.BufferedReader(stream).read()

    assert recovered == original
    assert stream.resumes == 1
    # Resumed once, from what was actually consumed — not from zero, not a guess.
    assert ranges == [2048]
    assert stream.compressed_bytes == len(payload)


def test_a_resume_that_restarts_the_stream_is_refused_not_spliced(monkeypatch):
    """A server that ignores Range answers 200 and starts over from byte zero.

    Splicing that onto a transfer already gigabytes in does not raise — the
    decompressor happily produces garbage that looks like rows. Refusing is the
    only safe answer, and it has to be checked rather than assumed, because the
    whole point of the resume is that nobody is watching when it happens.
    """
    from spicy_regs.sources import courtlistener_bulk
    from spicy_regs.sources.courtlistener_bulk import _CountingStream

    payload = bz2.compress(b"id,body\n" + b"".join(b"%d,row\n" % i for i in range(4000)))
    monkeypatch.setattr(courtlistener_bulk, "_CHUNK", 1024)

    def restart_from_zero(offset: int):  # noqa: ARG001 - the bug being simulated
        return _FlakyResponse(payload, offset=0, fail_after=None, status=200)

    stream = _CountingStream(_FlakyResponse(payload, offset=0, fail_after=2048), reopen=restart_from_zero)
    with pytest.raises(RuntimeError, match="not 206"):
        io.BufferedReader(stream).read()


def test_counting_stream_reads_a_concatenated_bzip2_dump():
    """``pbzip2`` writes many streams; a plain decompressor stops at the first.

    The publisher's dumps are single-stream today. If that ever changes, a
    decompressor that raises ``EOFError`` past the first boundary would end a
    pass early — which is a coverage number that is quietly wrong, the failure
    mode this ingest most has to avoid.
    """
    from spicy_regs.sources.courtlistener_bulk import _CountingStream

    payload = bz2.compress(b"id,body\n1,first\n") + bz2.compress(b"2,second\n")
    stream = _CountingStream(_FlakyResponse(payload, offset=0, fail_after=None))
    assert io.BufferedReader(stream).read() == b"id,body\n1,first\n2,second\n"


def test_reader_raises_on_a_broken_local_read_rather_than_resuming(tmp_path: Path):
    """Resume is a network affordance; a local file has no ``Range`` to ask for."""
    from spicy_regs.sources.courtlistener_bulk import _CountingStream

    stream = _CountingStream(_FlakyResponse(b"anything", offset=0, fail_after=0))
    with pytest.raises(OSError, match="connection reset"):
        io.BufferedReader(stream).read()


# -- shaping -----------------------------------------------------------------


def test_body_shape_matches_the_published_schema_and_records_text_provenance():
    row = shape_body(_RAW_OPINION, dump_date=date(2026, 6, 30))
    assert set(row) == set(BODY_COLUMNS)
    assert row["opinion_id"] == "11422346"
    assert row["cluster_id"] == "10954746"
    assert row["dump_date"] == "2026-06-30"

    # plain_text is empty upstream, so it stays NULL rather than becoming "".
    assert row["plain_text"] is None
    assert row["html_with_citations"] is None
    # ...but the row is not textless, and the table must say which rendering exists.
    assert row["available_text_fields"] == "html_lawbox"
    assert row["text_char_count"] == str(len("<p>MEMORANDUM OPINION</p>"))


def test_body_shape_reports_a_genuinely_textless_opinion_as_zero():
    bare = {**_RAW_OPINION, "html_lawbox": ""}
    row = shape_body(bare, dump_date=None)
    assert row["available_text_fields"] is None
    assert row["text_char_count"] == "0"
    assert row["dump_date"] is None


def test_cluster_shape_matches_schema_and_renames_the_docket_join_key():
    row = _shape_bulk(_RAW_CLUSTER)
    assert set(row) == set(CLUSTER_COLUMNS)
    assert row["cluster_id"] == "10954746"
    # The dump calls it docket_id; court_dockets calls it cl_docket_id. The join
    # only reads the same on both sides if the rename happens here.
    assert row["cl_docket_id"] == "73299709"
    assert row["absolute_url"] == (
        "https://www.courtlistener.com/opinion/10954746/zeevi-v-united-states-department-of-state/"
    )
    assert row["ingest_source"] == "bulk"


def test_search_shape_leaves_dump_only_columns_null():
    """The catch-up API is narrower than the dump; absent prose must stay absent."""
    result = {
        "cluster_id": "10954746",
        "docket_id": "73299709",
        "caseName": "Zeevi v. United States Department of State",
        "dateFiled": "2026-08-21",
        "status": "Published",
        "absolute_url": "/opinion/10954746/zeevi/",
    }
    row = _shape_search(result)
    assert set(row) == set(CLUSTER_COLUMNS)
    assert row["cl_docket_id"] == "73299709"
    assert row["ingest_source"] == "search"
    assert row["absolute_url"] == "https://www.courtlistener.com/opinion/10954746/zeevi/"
    # Not supplied by /search/ — and not fabricated.
    assert row["syllabus"] is None
    assert row["headnotes"] is None
    assert row["headmatter"] is None


# -- the disk guard ----------------------------------------------------------


def test_check_headroom_refuses_an_ingest_that_would_cross_the_floor(tmp_path: Path, monkeypatch):
    """The floor exists so a backfill fails loudly instead of filling the volume."""
    import shutil as shutil_module

    free = DISK_HEADROOM_FLOOR + 10 * 2**30

    def fake_usage(_path):
        return shutil_module._ntuple_diskusage(total=free * 4, used=free * 3, free=free)

    # The transform calls ``shutil.disk_usage`` through the stdlib module, so
    # patching it there covers the real call site.
    monkeypatch.setattr(shutil_module, "disk_usage", fake_usage)

    # 5 GiB still leaves 5 GiB above the floor.
    check_headroom(5 * 2**30, path=tmp_path)

    # The real 2026-06-30 opinions dump does not fit, and must say so.
    with pytest.raises(RuntimeError, match="below the 100 GiB floor"):
        check_headroom(54_561_543_156, path=tmp_path)


def test_estimate_output_bytes_charges_a_targeted_pass_for_its_output():
    """A filtered pass reads 50.8 GiB and writes almost nothing; the guard must know.

    The dump is streamed, never landed, so the disk cost of any pass is the
    parquet it writes. Charging a 1,155-cluster APA pass the whole dump's size
    refuses a run that costs tens of megabytes — the guard would be stopping the
    single highest-value follow-up for a cost it does not incur.
    """
    dump = 54_561_543_156

    # No filter: the output is the whole corpus, and the dump size stands in.
    assert estimate_output_bytes(dump, None) == dump

    # The real APA target set. Well under a gibibyte, so it clears a 7 GiB margin.
    apa = estimate_output_bytes(dump, {str(i) for i in range(1155)})
    assert apa < 2**30
    assert apa == 1155 * OPINIONS_PER_CLUSTER_CEILING * BYTES_PER_OPINION_ROW

    # Still refuses the thing it exists to refuse: filtering on every cluster in
    # the corpus is a backfill wearing a filter, and must be sized as one.
    assert estimate_output_bytes(dump, {str(i) for i in range(200_000)}) > 20 * 2**30

    # A filter whose size cannot be known falls back to the conservative number
    # rather than guessing small.
    class _Unsized:
        def __contains__(self, _item: object) -> bool:
            return True

    assert estimate_output_bytes(dump, _Unsized()) == dump


# -- first build promotes rather than merges ---------------------------------


def test_first_build_promotes_the_staged_table_without_merging(tmp_path: Path, monkeypatch):
    """With no prior table the merge is a sort the machine cannot always afford.

    One dump, whose id column is the publisher's primary key, so the dedup can
    remove nothing. Running the COPY anyway held the staged and merged copies on
    disk at once and — on 250,000 rows carrying kilobytes of opinion text each —
    ran duckdb's memory budget out entirely. The first build promotes instead.
    """
    from spicy_regs.sources import r2
    from spicy_regs.transforms.build_court_opinion_bodies import build_court_opinion_bodies

    monkeypatch.setattr(r2, "download", lambda *_: False)
    dump = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id,type,plain_text,html_with_citations",
        ['"11","101","010combined","body one",""', '"12","102","040dissent","","<p>body two</p>"'],
    )
    out = build_court_opinion_bodies(tmp_path, dump_date=date(2026, 6, 30), local_file=dump)

    stored = pq.read_table(out).to_pylist()
    assert [r["opinion_id"] for r in stored] == ["11", "12"]
    assert set(stored[0]) == set(BODY_COLUMNS)
    assert stored[0]["plain_text"] == "body one"
    assert stored[1]["html_with_citations"] == "<p>body two</p>"
    assert stored[1]["available_text_fields"] == "html_with_citations"
    # The staged file must not survive as a second copy of the same rows.
    assert not (tmp_path / "_bodies_new.parquet").exists()

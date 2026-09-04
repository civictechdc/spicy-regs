"""Reader connector for the CourtListener bulk-data dumps (quarterly CSV exports).

Complements :mod:`spicy_regs.sources.courtlistener`, which reads the v4
``/search/`` endpoint for *docket* metadata. This module reads the other half of
the publisher's surface: the periodic full-table CSV dumps at
``storage.courtlistener.com/bulk-data/``, which are the **only keyless source of
opinion text**. The REST ``/opinions/`` and ``/clusters/`` endpoints both answer
``401`` without a token (verified 2026-08-22); ``/search/`` and ``/courts/`` do
not. So a bulk read is not an optimization here, it is the only road.

**The listing is the publisher's enumeration.** The bucket answers the S3 v2
list API, so ``list_bulk_dumps`` recovers every published object with its exact
byte size and last-modified stamp. That listing is what makes coverage
*checkable*: a claim about how much of a dump we ingested is measured against
the publisher's own row counts, not against our hopes.

**Streaming, not landing.** The dumps are bzip2 and some are enormous — the
2026-06-30 ``opinions`` dump is 50.8 GiB compressed and roughly 422 GiB
decompressed at its observed 8.3x ratio. Landing that is not an option on a
workstation, so the reader decompresses *as it downloads* and never holds more
than a buffer in memory. ``max_records`` / ``max_compressed_bytes`` bound a run
so a partial ingest is a deliberate, recorded slice rather than a timeout.

**Politeness.** Every request carries an honest identifying User-Agent. The
bucket serves at roughly 1.7-2.0 MiB/s per connection; the reader takes that as
given rather than opening parallel connections to work around it.
"""

from __future__ import annotations

from typing import Any, Protocol

import bz2
import csv
import io
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from loguru import logger

from spicy_regs.sources.base import Reader

#: Public read endpoint for the dumps themselves.
BULK_BASE_URL = "https://storage.courtlistener.com/bulk-data"

#: S3 REST endpoint for the same bucket. ``storage.courtlistener.com`` is a
#: CloudFront-style alias that does not answer the list API; the bucket host
#: does, so enumeration goes here and byte reads go to ``BULK_BASE_URL``.
BULK_LIST_URL = "https://com-courtlistener-storage.s3.amazonaws.com/"
BULK_PREFIX = "bulk-data/"

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

#: Identify honestly. CourtListener publishes this data for public reuse; the
#: least we owe them is a contactable agent string.
USER_AGENT = "spicy-regs/0.1 (+https://spicy-regs.dev) courtlistener-bulk-ingest"

_TIMEOUT = 180.0
_MAX_RETRIES = 5
_CHUNK = 4 << 20
_PROGRESS_EVERY = 50_000

#: Opinion text rows carry entire judicial opinions in one CSV field, which is
#: far past the stdlib default. Raise once, at import, to the platform maximum.
csv.field_size_limit(sys.maxsize)

#: The dumps escape an embedded quote as ``\"``, not as the doubled ``""`` the
#: stdlib assumes. Reading them with the default dialect does not fail — it
#: *desyncs*: the reader treats the escaped quote as the end of the field, and
#: the prose after it becomes the next record's first column. Measured on the
#: 2026-06-30 ``opinion-clusters`` dump, the default dialect corrupts 1,987 of
#: the first 3,000 rows and drops ``docket_id`` on two thirds of them, which
#: would have quietly destroyed the join this whole ingest exists to make. With
#: ``escapechar`` set, the same 3,000 rows parse clean and every one keeps its
#: docket. ``doublequote`` stays True so a literal ``""`` empty field still reads.
CSV_DIALECT: dict[str, Any] = {"escapechar": "\\", "doublequote": True}


@dataclass(frozen=True)
class BulkObject:
    """One published object in the bulk-data prefix."""

    key: str
    size: int
    last_modified: str

    @property
    def filename(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def dataset(self) -> str | None:
        """Dataset name with the dump date stripped (``opinions``, ``courts``...)."""
        name = self.filename
        for suffix in (".csv.bz2", ".sql", ".sh", ".csv", ".zip"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        if len(name) > 11 and name[-11] == "-":
            stem, tail = name[:-11], name[-10:]
            if _is_iso_date(tail):
                return stem
        return name or None

    @property
    def dump_date(self) -> date | None:
        """The dump's date stamp, or None for the undated one-off exports."""
        name = self.filename
        for suffix in (".csv.bz2", ".sql", ".sh", ".csv", ".zip"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        tail = name[-10:]
        return date.fromisoformat(tail) if _is_iso_date(tail) else None

    @property
    def url(self) -> str:
        return f"{BULK_BASE_URL}/{self.filename}"


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _request(url: str, *, extra_headers: dict[str, str] | None = None):
    headers = {"User-Agent": USER_AGENT}
    if extra_headers:
        headers.update(extra_headers)
    return urllib.request.Request(url, headers=headers)


def _open(url: str, *, extra_headers: dict[str, str] | None = None):
    """Open a URL with bounded retries and exponential backoff."""
    last: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return urllib.request.urlopen(_request(url, extra_headers=extra_headers), timeout=_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 - urllib raises a wide family
            last = exc
            if attempt == _MAX_RETRIES:
                break
            backoff = min(2**attempt, 60)
            logger.warning(
                "CourtListener bulk: {} (attempt {}/{}), retrying in {}s",
                exc,
                attempt,
                _MAX_RETRIES,
                backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"CourtListener bulk: giving up on {url}") from last


def list_bulk_dumps(prefix: str = BULK_PREFIX) -> list[BulkObject]:
    """Enumerate every object under the bulk-data prefix, with exact sizes.

    This is the publisher's own enumeration of what exists. Coverage claims are
    checked against it, so it is fetched rather than assumed.
    """
    found: list[BulkObject] = []
    token: str | None = None
    while True:
        query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            query["continuation-token"] = token
        with _open(BULK_LIST_URL + "?" + urllib.parse.urlencode(query)) as response:
            root = ET.fromstring(response.read())
        for node in root.findall("s3:Contents", _S3_NS):
            found.append(
                BulkObject(
                    key=node.findtext("s3:Key", "", _S3_NS),
                    size=int(node.findtext("s3:Size", "0", _S3_NS)),
                    last_modified=node.findtext("s3:LastModified", "", _S3_NS),
                )
            )
        if root.findtext("s3:IsTruncated", "false", _S3_NS) != "true":
            break
        token = root.findtext("s3:NextContinuationToken", None, _S3_NS)
        if not token:
            break
    logger.info("CourtListener bulk: listing has {:,} objects", len(found))
    return found


def published_object_pin(
    dataset: str,
    dump_date: date,
    *,
    objects: list[BulkObject] | None = None,
    expect_bytes: int | None = None,
    expect_last_modified: str | None = None,
) -> dict[str, object]:
    """Identify the published object a capture read, in receipt form.

    A capture that says "streamed the 2026-06-30 opinions dump" has named a
    filename, not a thing. The publisher's listing carries the object's exact
    byte size and last-modified stamp, which is what makes two runs comparable
    and what makes "the dump changed under us" a detectable event rather than an
    unexplained difference in row counts.

    **The population itself is DocSpec's to pin**, at
    ``fixtures/courtlistener-bulk-v1/`` — it captures this listing verbatim,
    digests it, and distinguishes an object the publisher withdrew from one we
    declined. This function does not re-derive any of that. It records the one
    object a run actually read, and ``expect_bytes`` / ``expect_last_modified``
    let a caller hold that record against DocSpec's pin *before* spending 8.6
    hours reading it.
    """
    listing = objects if objects is not None else list_bulk_dumps()
    published = find_dump(listing, dataset, dump_date)
    if published is None:
        raise RuntimeError(f"CourtListener bulk: no {dataset} dump published for {dump_date}")
    if expect_bytes is not None and published.size != expect_bytes:
        raise RuntimeError(
            f"CourtListener bulk: {published.filename} is {published.size} bytes, "
            f"not the pinned {expect_bytes} — the publisher's object changed"
        )
    if expect_last_modified is not None and published.last_modified != expect_last_modified:
        raise RuntimeError(
            f"CourtListener bulk: {published.filename} was last modified "
            f"{published.last_modified}, not the pinned {expect_last_modified} — "
            f"the publisher's object changed"
        )
    return {
        "dataset": dataset,
        "dump_date": dump_date.isoformat(),
        "filename": published.filename,
        "url": published.url,
        "bytes": published.size,
        "last_modified": published.last_modified,
        "listing_object_count": len(listing),
        "listing_host": BULK_LIST_URL,
    }


def latest_dump_date(objects: list[BulkObject], dataset: str) -> date | None:
    """Newest dump date published for ``dataset``, or None if it has none."""
    dates = [o.dump_date for o in objects if o.dataset == dataset and o.dump_date]
    return max(dates) if dates else None


def find_dump(objects: list[BulkObject], dataset: str, dump_date: date) -> BulkObject | None:
    """The published object for one dataset at one dump date."""
    for obj in objects:
        if obj.dataset == dataset and obj.dump_date == dump_date:
            return obj
    return None


class _UnrangeableResume(RuntimeError):
    """A resume the server answered with a whole new stream instead of a range."""


class _StreamedResponse(Protocol):
    """The slice of an HTTP response body this reader touches."""

    def read(self, amt: int, /) -> bytes: ...
    def close(self) -> None: ...


class _CountingStream(io.RawIOBase):
    """Adapt an HTTP response to a readable stream, decompressing bzip2 inline.

    Tracks compressed bytes pulled so a caller can stop on a byte budget without
    waiting for a row count.

    **Resumable.** A full pass over the ``opinions`` dump is 8.6 hours on one
    socket, and a socket held open that long will occasionally be dropped by
    something between here and the bucket. Failing at hour seven with nothing to
    show for it is the difference between this ingest being feasible and not, so
    a read error reopens the transfer with an HTTP ``Range`` starting at the
    exact compressed offset already consumed and keeps feeding the *same*
    decompressor — bzip2 needs its compressed bytes in order, not in one socket.
    ``reopen`` is the callable that performs that ranged re-request; without one
    (a local file) a read error still propagates.

    **Multi-stream aware.** ``bzip2`` writes one stream; ``pbzip2`` writes a
    concatenation of them, and a plain ``BZ2Decompressor`` raises ``EOFError``
    the moment it is fed a byte past the first stream's end. The publisher's
    dumps read as single-stream today, but a compressor change upstream would
    otherwise truncate a pass silently-ish at a stream boundary, so a finished
    decompressor is replaced and its ``unused_data`` carried over.
    """

    def __init__(
        self,
        response: _StreamedResponse,
        *,
        max_compressed_bytes: int | None = None,
        reopen: Callable[[int], _StreamedResponse] | None = None,
    ) -> None:
        self._response: _StreamedResponse = response
        self._reopen = reopen
        self._decompressor = bz2.BZ2Decompressor()
        self._buffer = b""
        self._max_compressed_bytes = max_compressed_bytes
        self.compressed_bytes = 0
        self.decompressed_bytes = 0
        self.resumes = 0
        self._exhausted = False

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        """Close whichever response is current — after a resume it is not the first."""
        try:
            self._response.close()
        except Exception:  # noqa: BLE001, S110 - closing a broken socket
            pass
        super().close()

    def _read_chunk(self) -> bytes:
        """Pull the next compressed chunk, reconnecting mid-stream if need be."""
        try:
            return self._response.read(_CHUNK)
        except Exception as exc:  # noqa: BLE001 - urllib/ssl raise a wide family
            if self._reopen is None:
                raise
            logger.warning(
                "CourtListener bulk: transfer failed after {:.3f} GiB ({}); resuming",
                self.compressed_bytes / 2**30,
                exc,
            )
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                self._response.close()
            except Exception:  # noqa: BLE001, S110 - closing a broken socket
                pass
            try:
                resumed = self._reopen(self.compressed_bytes)
                # A server that ignores Range answers 200 and starts over from
                # byte zero. Feeding that to a decompressor already 30 GiB into
                # the stream does not fail — it produces garbage rows that look
                # like data, which is the one outcome worse than the dropped
                # connection this is recovering from.
                status = getattr(resumed, "status", None)
                if self.compressed_bytes and status != 206:
                    # Not retryable: a server that ignores Range will ignore it
                    # again, and every retry is another chance to splice.
                    raise _UnrangeableResume(
                        f"CourtListener bulk: resume at byte {self.compressed_bytes} "
                        f"answered {status}, not 206 — refusing to splice a restarted "
                        f"stream onto a partial one"
                    )
                self._response = resumed
                self.resumes += 1
                return self._response.read(_CHUNK)
            except _UnrangeableResume:
                raise
            except Exception as exc:  # noqa: BLE001
                if attempt == _MAX_RETRIES:
                    raise RuntimeError(
                        f"CourtListener bulk: could not resume at byte {self.compressed_bytes} after {attempt} attempts"
                    ) from exc
                backoff = min(2**attempt, 60)
                logger.warning(
                    "CourtListener bulk: resume attempt {}/{} failed ({}), retrying in {}s",
                    attempt,
                    _MAX_RETRIES,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
        return b""  # pragma: no cover - the loop either returns or raises

    def _decompress(self, chunk: bytes) -> bytes:
        """Decompress one chunk, rolling over a concatenated bzip2 stream."""
        out = self._decompressor.decompress(chunk)
        while self._decompressor.eof and self._decompressor.unused_data:
            leftover = self._decompressor.unused_data
            self._decompressor = bz2.BZ2Decompressor()
            out += self._decompressor.decompress(leftover)
        return out

    def readinto(self, target) -> int:  # type: ignore[override]
        while not self._buffer and not self._exhausted:
            if self._max_compressed_bytes is not None and self.compressed_bytes >= self._max_compressed_bytes:
                self._exhausted = True
                break
            chunk = self._read_chunk()
            if not chunk:
                self._exhausted = True
                break
            self.compressed_bytes += len(chunk)
            self._buffer = self._decompress(chunk)
            self.decompressed_bytes += len(self._buffer)
        if not self._buffer:
            return 0
        size = min(len(target), len(self._buffer))
        target[:size] = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return size


class CourtListenerBulkReader(Reader):
    """Yield raw CSV rows from one CourtListener bulk dump, decompressed inline.

    Pure source: rows are yielded as the publisher wrote them (all values are
    strings; the empty string is normalized to ``None``). Shaping belongs to the
    ``transforms/build_court_*`` builders.

    ``local_file`` reads an already-downloaded ``.bz2`` instead of the network,
    which is how the small dumps are handled once cached. ``max_records`` and
    ``max_compressed_bytes`` bound a run; ``row_filter`` drops rows before they
    are materialized, which is what keeps a filtered pass over a huge dump cheap.
    """

    def __init__(
        self,
        dataset: str,
        *,
        dump_date: date | None = None,
        local_file: Path | None = None,
        max_records: int | None = None,
        max_compressed_bytes: int | None = None,
        row_filter: Callable[[dict], bool] | None = None,
    ) -> None:
        self.dataset = dataset
        self.dump_date = dump_date
        self.local_file = local_file
        self.max_records = max_records
        self.max_compressed_bytes = max_compressed_bytes
        self.row_filter = row_filter
        #: Populated during iteration so a caller can record the exact bound hit.
        self.rows_scanned = 0
        self.rows_yielded = 0
        self.compressed_bytes = 0
        self.decompressed_bytes = 0
        self.stopped_early = False
        self.source_url: str | None = None
        #: How many times the transfer had to be reconnected mid-dump. Belongs in
        #: a receipt: a pass that resumed twice read the same bytes as one that
        #: did not, but it is not the same run and should not be recorded as one.
        self.resumes = 0

    def _stream(self):
        if self.local_file is not None:
            self.source_url = str(self.local_file)
            return open(self.local_file, "rb"), None  # noqa: SIM115
        if self.dump_date is None:
            raise ValueError("dump_date is required when reading over the network")
        filename = f"{self.dataset}-{self.dump_date.isoformat()}.csv.bz2"
        url = f"{BULK_BASE_URL}/{filename}"
        self.source_url = url
        logger.info("CourtListener bulk: streaming {}", url)
        response = _open(url)
        return response, response

    def iter_records(self) -> Iterator[dict]:
        handle, response = self._stream()
        try:
            if response is None:
                raw: io.BufferedIOBase = io.BufferedReader(
                    _LocalBz2Stream(handle, max_compressed_bytes=self.max_compressed_bytes)
                )
                counter = raw.raw  # type: ignore[assignment]
            else:
                url = self.source_url
                counter = _CountingStream(
                    handle,
                    max_compressed_bytes=self.max_compressed_bytes,
                    reopen=lambda offset: _open(str(url), extra_headers={"Range": f"bytes={offset}-"}),
                )
                raw = io.BufferedReader(counter)  # type: ignore[arg-type]
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
            for row in csv.DictReader(text, **CSV_DIALECT):
                self.rows_scanned += 1
                if self.rows_scanned % _PROGRESS_EVERY == 0:
                    logger.info(
                        "CourtListener bulk {}: {:,} rows scanned, {:,} kept, {:.2f} GiB compressed",
                        self.dataset,
                        self.rows_scanned,
                        self.rows_yielded,
                        counter.compressed_bytes / 2**30,
                    )
                record = {k: (v if v != "" else None) for k, v in row.items() if k is not None}
                if self.row_filter is not None and not self.row_filter(record):
                    continue
                self.rows_yielded += 1
                yield record
                if self.max_records is not None and self.rows_yielded >= self.max_records:
                    self.stopped_early = True
                    break
            else:
                self.stopped_early = bool(
                    self.max_compressed_bytes is not None and counter._exhausted  # noqa: SLF001
                )
            self.compressed_bytes = counter.compressed_bytes
            self.decompressed_bytes = counter.decompressed_bytes
            self.resumes = counter.resumes
        finally:
            handle.close()
        logger.info(
            "CourtListener bulk {}: {:,} scanned / {:,} yielded ({:.2f} GiB compressed read, {} resume(s))",
            self.dataset,
            self.rows_scanned,
            self.rows_yielded,
            self.compressed_bytes / 2**30,
            self.resumes,
        )


class _LocalBz2Stream(_CountingStream):
    """``_CountingStream`` over an open local file rather than an HTTP response."""

    def __init__(self, handle, *, max_compressed_bytes: int | None = None) -> None:
        super().__init__(handle, max_compressed_bytes=max_compressed_bytes)

"""Reader connector for the OIRA/OMB Unified Agenda published at reginfo.gov.

The Unified Agenda of Regulatory and Deregulatory Actions is the semiannual
catalog, edited by the Office of Information and Regulatory Affairs (OIRA), of
the rulemakings each federal agency has under active development. Every entry is
keyed by a **Regulation Identifier Number (RIN)** — the same RIN that appears in
the Federal Register (``regulation_id_numbers``) — which makes the Unified Agenda
the upstream, forward-looking view of the rulemaking lifecycle: it lists actions
that are *planned* long before any proposed or final rule reaches the Federal
Register or opens a comment period on regulations.gov.

The reader is a *pure source*: it yields one dict per RIN record, with keys
normalized to what :func:`~spicy_regs.transforms.build_unified_agenda._shape`
reads. Shaping those dicts into the published 17-column schema is the job of
that transform.

Source of truth — the per-edition XML export
--------------------------------------------
reginfo.gov publishes each agenda edition as a single machine-readable XML file
downloaded through ``XMLViewFileAction``::

    https://www.reginfo.gov/public/do/XMLViewFileAction?f=REGINFO_RIN_DATA_{EDITION}.xml

where ``EDITION`` is ``YYYYMM`` with MM in {04 (Spring), 10 (Fall)} — e.g.
``REGINFO_RIN_DATA_202510.xml`` (Fall 2025), ``REGINFO_RIN_DATA_202410.xml``
(Fall 2024). (One legacy file is ``REGINFO_RIN_DATA_2012.xml``.) No API key is
required; reginfo.gov is fully open.

Edition discovery
-----------------
There is no index of published editions, and a request for an edition that has
not been published still answers ``200``, with an HTML error page instead of
XML. So the latest edition is found by probing: :func:`candidate_editions`
lists plausible editions newest-first from today's date (bounded by
:data:`MAX_LOOKBACK_EDITIONS`), and :func:`discover_editions` streams just the
first bytes of each candidate, treating anything but an XML body as "not
published". Incremental callers pass the newest edition they already store as
``after`` and get back only the published editions newer than it.

.. note::
   The older ``eAgendaXmlReport`` endpoint returns the **HTML listing page**, not
   data — it is not a machine-readable export. ``XMLViewFileAction`` is the real
   export and is what this reader fetches.

Each file is a single ``<REGINFO_RIN_DATA>`` root containing many ``<RIN_INFO>``
records. Files run to tens of MB, so the reader stream-parses with
:func:`xml.etree.ElementTree.iterparse` and clears each record (and the root's
accumulated child shells) as it goes, keeping memory bounded regardless of file
size. Observed per-record structure (see ``_normalize``)::

    <RIN_INFO>
      <RIN>0503-AA80</RIN>
      <PUBLICATION><PUBLICATION_ID>202410</PUBLICATION_ID>...</PUBLICATION>
      <AGENCY><CODE>0503</CODE><NAME>...</NAME><ACRONYM>AgSEC</ACRONYM></AGENCY>
      <PARENT_AGENCY>...</PARENT_AGENCY>
      <RULE_TITLE>...</RULE_TITLE>
      <ABSTRACT>...</ABSTRACT>
      <PRIORITY_CATEGORY>...</PRIORITY_CATEGORY>
      <RIN_STATUS>...</RIN_STATUS>
      <RULE_STAGE>...</RULE_STAGE>
      <MAJOR>No</MAJOR>
      <CFR_LIST><CFR>7 CFR 1.900-1.903</CFR>...</CFR_LIST>
      <LEGAL_AUTHORITY_LIST><LEGAL_AUTHORITY>5 U.S.C. 301</LEGAL_AUTHORITY>...</LEGAL_AUTHORITY_LIST>
      <TIMETABLE_LIST>
        <TIMETABLE><TTBL_ACTION>...</TTBL_ACTION><TTBL_DATE>11/20/2024</TTBL_DATE>
                   <FR_CITATION>89 FR 91529</FR_CITATION></TIMETABLE>
        ...
      </TIMETABLE_LIST>
    </RIN_INFO>
"""

from __future__ import annotations

import io
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from datetime import date

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# --------------------------------------------------------------------------- #
# Endpoint constants — validated live against reginfo.gov (public, no API key).
# --------------------------------------------------------------------------- #
API_BASE = "https://www.reginfo.gov/public/do"

# The per-edition Unified Agenda XML export. ``{edition}`` is YYYYMM; the file
# name pattern is REGINFO_RIN_DATA_{edition}.xml served through XMLViewFileAction.
XML_EXPORT_ENDPOINT = f"{API_BASE}/XMLViewFileAction"
XML_FILE_TEMPLATE = "REGINFO_RIN_DATA_{edition}.xml"

# Root element and the repeated per-RIN record element in the export.
_ROOT_TAG = "REGINFO_RIN_DATA"
_RECORD_TAG = "RIN_INFO"

# Editions are ``YYYYMM`` with MM in {04 (Spring), 10 (Fall)}.
EDITION_MONTHS = ("10", "04")  # newest-first within a year

# How many candidate editions (counting back from this year's Fall edition)
# discovery probes before giving up. Eight is four years: agendas are sometimes
# published months late, but never years, and anything older than this is a
# historical backfill (#172), not "the latest edition".
MAX_LOOKBACK_EDITIONS = 8

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky fetch fails slow-then-recovers rather than dropping records.
_TIMEOUT = httpx.Timeout(300.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Probing reads only the start of the body: enough to see ``<?xml`` past any
# leading whitespace (the HTML error page starts with ~20 blank lines).
_PROBE_MAX_BYTES = 64 * 1024
_PROBE_RETRIES = 3


class UnifiedAgendaUnavailable(RuntimeError):
    """reginfo.gov could not be reached, or answered with no recognizable export."""


def _is_xml(data: bytes) -> bool:
    """True if ``data`` starts (after whitespace) like an XML document.

    A missing edition comes back as ``200`` with an HTML error page, so the
    status code alone cannot tell published from unpublished.
    """
    return data.lstrip().startswith(b"<?xml")


def candidate_editions(today: date | None = None, *, count: int = MAX_LOOKBACK_EDITIONS) -> list[str]:
    """Return ``count`` plausible editions newest-first, starting at this year's Fall.

    E.g. on 2026-11-12: ``["202610", "202604", "202510", "202504", ...]``. This
    year's editions are always included even if their month hasn't arrived yet:
    probing a not-yet-published edition costs one small request.
    """
    year = (today or date.today()).year
    out: list[str] = []
    while len(out) < count:
        for month in EDITION_MONTHS:
            if len(out) == count:
                break
            out.append(f"{year}{month}")
        year -= 1
    return out


def probe_edition(client: httpx.Client, edition: str) -> bool:
    """Return whether ``edition``'s XML export is published, reading only its first bytes.

    ``False`` means reginfo.gov answered but not with XML (the HTML error page
    for an unpublished edition, or a non-retryable 4xx). Transport errors and
    429/5xx are retried; if they persist, raises :class:`UnifiedAgendaUnavailable`
    so callers never mistake "reginfo.gov is down" for "not published".
    """
    params = {"f": XML_FILE_TEMPLATE.format(edition=edition)}
    for attempt in range(1, _PROBE_RETRIES + 1):
        try:
            with client.stream("GET", XML_EXPORT_ENDPOINT, params=params) as resp:
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                if resp.status_code != 200:
                    return False
                head = b""
                for chunk in resp.iter_bytes():
                    head += chunk
                    if len(head.lstrip()) >= len(b"<?xml") or len(head) >= _PROBE_MAX_BYTES:
                        break
                return _is_xml(head)
        except httpx.HTTPError as exc:
            if attempt == _PROBE_RETRIES:
                raise UnifiedAgendaUnavailable(
                    f"could not probe edition {edition} after {attempt} attempts: {exc}"
                ) from exc
            backoff = min(2**attempt, 30)
            logger.warning(
                "Unified Agenda: probe of {} failed ({}), attempt {}/{}, retrying in {}s",
                edition,
                exc,
                attempt,
                _PROBE_RETRIES,
                backoff,
            )
            time.sleep(backoff)
    raise AssertionError("unreachable")  # pragma: no cover


def discover_editions(
    after: str | None = None,
    *,
    today: date | None = None,
    client: httpx.Client | None = None,
) -> tuple[str, ...]:
    """Return the published editions to fetch, oldest-first.

    - ``after=None`` (nothing stored yet): probe candidates newest-first and
      return just the first published one, i.e. the latest edition.
    - ``after="YYYYMM"`` (the newest stored edition): probe every candidate
      newer than it and return those that are published, oldest-first. ``()``
      means the stored table is already current.

    Raises :class:`UnifiedAgendaUnavailable` if reginfo.gov cannot be reached,
    or if no candidate *and* not even ``after`` itself probes as published:
    that means the export endpoint changed, not that nothing is new, and
    reporting "up to date" would silently freeze the table again.
    """
    if client is None:
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as owned:
            return discover_editions(after, today=today, client=owned)

    candidates = candidate_editions(today)
    if after is not None:
        candidates = [c for c in candidates if c > after]

    published: list[str] = []
    for edition in candidates:  # newest-first
        if probe_edition(client, edition):
            logger.info("Unified Agenda: edition {} is published", edition)
            published.append(edition)
            if after is None:
                break  # nothing stored: the latest edition is enough
        else:
            logger.info("Unified Agenda: edition {} is not published", edition)

    if published:
        return tuple(sorted(published))
    if after is not None and probe_edition(client, after):
        logger.info("Unified Agenda: no edition newer than stored {}; up to date", after)
        return ()
    stored = f" or the stored edition {after}" if after else ""
    raise UnifiedAgendaUnavailable(
        f"no published Unified Agenda edition among {candidate_editions(today)}{stored}; "
        f"has {XML_EXPORT_ENDPOINT} changed?"
    )


class UnifiedAgendaReader(Reader):
    """Yields normalized Unified Agenda RIN dicts for one or more agenda editions.

    ``editions`` defaults to the latest published edition, found at run time
    via :func:`discover_editions`. Callers doing incremental runs pass only the
    edition(s) newer than what is already stored; the transform handles merging
    with the prior table and dedup on (``rin``, ``agenda_edition``).

    After iteration, :attr:`edition_counts` maps each fetched edition to the
    number of records it yielded (``0`` if its download failed).
    """

    def __init__(
        self,
        *,
        editions: tuple[str, ...] | None = None,
        verbose: bool = False,
    ) -> None:
        self.editions = editions
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0
        self.edition_counts: dict[str, int] = {}

    def iter_records(self) -> Iterator[dict]:
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
            self._client = client
            editions = self.editions or discover_editions(client=client)
            logger.info("Unified Agenda: fetching editions {}", ", ".join(editions))
            for edition in editions:
                yield from self._fetch_edition(edition)
        logger.info("Unified Agenda: yielded {:,} records", self._seen)

    # -- edition fetching ----------------------------------------------------

    def _fetch_edition(self, edition: str) -> Iterator[dict]:
        """Download one edition's XML export and stream-parse its RIN records.

        Each yielded dict is annotated with the ``agenda_edition`` it came from,
        since it is half of the (``rin``, ``agenda_edition``) dedup key.
        """
        self.edition_counts[edition] = 0
        data = self._download(edition)
        if data is None:
            return
        collected = 0

        # iterparse over the in-memory bytes; grab the root on the first event so
        # we can drop processed record shells and keep the tree from growing.
        context = ET.iterparse(io.BytesIO(data), events=("start", "end"))
        _, root = next(context)  # first ("start", <REGINFO_RIN_DATA>)
        for event, elem in context:
            if event != "end" or elem.tag != _RECORD_TAG:
                continue
            record = _normalize(elem, edition)
            collected += 1
            self.edition_counts[edition] = collected
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("Unified Agenda: {:,} records so far...", self._seen)
            yield record
            # Free this record's subtree and the root's accumulated child shells.
            elem.clear()
            root.clear()
        logger.info("Unified Agenda: edition {} yielded {:,} records", edition, collected)

    def _download(self, edition: str) -> bytes | None:
        """GET one edition's XML file with bounded retries + exponential backoff.

        Returns the raw XML bytes, or None if every attempt failed.
        """
        assert self._client is not None
        params = {"f": XML_FILE_TEMPLATE.format(edition=edition)}
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(XML_EXPORT_ENDPOINT, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                data = resp.content
                if not _is_xml(data):
                    # A non-XML body (e.g. the HTML listing page) means the edition
                    # is missing or the endpoint changed — surface it, don't parse.
                    logger.error(
                        "Unified Agenda: edition {} did not return XML (got {} bytes starting {!r})",
                        edition,
                        len(data),
                        data[:60],
                    )
                    return None
                return data
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("Unified Agenda: giving up on edition {} after {} attempts: {}", edition, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Unified Agenda: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        return None


# --------------------------------------------------------------------------- #
# XML → normalized dict. Keys here are exactly what ``_shape`` reads.
# --------------------------------------------------------------------------- #


def _text(elem: ET.Element | None, path: str) -> str | None:
    """Return the stripped text at ``path`` under ``elem``, or None if empty."""
    if elem is None:
        return None
    value = elem.findtext(path)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _text_list(elem: ET.Element, path: str) -> list[str]:
    """Return the stripped, non-empty text of every element matching ``path``."""
    out: list[str] = []
    for child in elem.findall(path):
        if child.text and child.text.strip():
            out.append(child.text.strip())
    return out


def _normalize(elem: ET.Element, edition: str) -> dict:
    """Map one ``<RIN_INFO>`` element onto the keys ``_shape`` consumes.

    ``agency_code``/``agency_name`` come from the RIN-owning ``<AGENCY>`` (its
    ``<ACRONYM>``, falling back to the numeric ``<CODE>``, and ``<NAME>``).
    """
    agency = elem.find("AGENCY")
    timetable = [
        {
            "action": _text(tt, "TTBL_ACTION"),
            "date": _text(tt, "TTBL_DATE"),
            "fr_citation": _text(tt, "FR_CITATION"),
        }
        for tt in elem.findall("TIMETABLE_LIST/TIMETABLE")
    ]
    return {
        "rin": _text(elem, "RIN"),
        "agency_code": _text(agency, "ACRONYM") or _text(agency, "CODE"),
        "agency_name": _text(agency, "NAME"),
        "title": _text(elem, "RULE_TITLE"),
        "abstract": _text(elem, "ABSTRACT"),
        "priority_category": _text(elem, "PRIORITY_CATEGORY"),
        "rin_status": _text(elem, "RIN_STATUS"),
        "rule_stage": _text(elem, "RULE_STAGE"),
        "major": _text(elem, "MAJOR"),
        "publication_id": _text(elem, "PUBLICATION/PUBLICATION_ID"),
        "agenda_edition": edition,
        "cfr_references": _text_list(elem, "CFR_LIST/CFR"),
        "legal_authority": _text_list(elem, "LEGAL_AUTHORITY_LIST/LEGAL_AUTHORITY"),
        "timetable": timetable,
    }

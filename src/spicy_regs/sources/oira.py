"""Reader connectors for OIRA's EO 12866 regulatory review data at reginfo.gov.

Under Executive Order 12866, every significant rule goes to OMB's Office of
Information and Regulatory Affairs (OIRA) for review before it is published,
typically once before the proposed rule and again before the final rule. This
is the one stage of the rulemaking lifecycle between "the agency drafted it"
and "it appeared in the Federal Register". reginfo.gov publishes two datasets
for it, separate from the Unified Agenda (:mod:`spicy_regs.sources.unified_agenda`):

- **Reviews**: one record per rule submitted to OIRA, with the dates OIRA
  received it and concluded review, and the outcome.
- **EO 12866 meetings**: OIRA's log of meetings with outside parties about a
  rule under review. This is the only systematic record of outside contact on
  a rule before its text is public, for every agency except the FCC.

Both are keyed by **RIN**, so they join to ``unified_agenda.rin`` and to
``federal_register.regulation_id_numbers_json``.

The readers are *pure sources*: they yield one normalized dict per record.
Shaping into the published columns is the job of
:mod:`spicy_regs.transforms.build_oira`.

Source of truth: bulk XML exports
---------------------------------
reginfo.gov's "XML Reports" page (:data:`REPORT_LIST_URL`) links every export
it serves, each downloaded through the same ``XMLViewFileAction`` endpoint the
Unified Agenda uses. No API key is required. The files used here:

- ``EO_RULE_COMPLETED_{YYYY}.xml``: reviews concluded in calendar year
  ``YYYY``, one file per year from 1981 through last year (~0.3-0.8 MB each).
- ``EO_RULE_COMPLETED_YTD.xml``: reviews concluded so far this year.
- ``EO_RULES_UNDER_REVIEW.xml``: reviews still pending at OIRA. No
  ``DATE_COMPLETED`` / ``DECISION``.
- ``EO_12866_Meeting_Data_{First,Second}_Half_{YYYY}.xml``: the meeting log.
  As of 2026-11 reginfo.gov publishes only the two 2024 halves. Earlier and
  later meetings are only in the HTML search at ``eom12866Search``.
- ``AGY_AGENCY_LIST.xml``: maps the numeric agency code (the RIN's first four
  digits) to an acronym and name.

The file names are not fully predictable (meeting files have appeared for only
some periods), so :func:`list_report_files` reads them off the report list page
rather than guessing. That page is the index reginfo.gov maintains for exactly
this purpose. Because the review files total only ~20 MB, the build is a full
refresh: every run re-reads everything, so corrections reginfo.gov makes to
past records (``DATE_PUBLISHED`` is often filled in months later) are picked up.

Review record (``<OIRA_DATA>`` root, repeated ``<REGACT>``)::

    <REGACT>
      <AGENCY_CODE>0331</AGENCY_CODE>
      <RIN>0331-AA10</RIN>
      <TITLE>Removal of National Environmental Policy Act Implementing Regulations</TITLE>
      <STAGE>Final Rule</STAGE>
      <ECONOMICALLY_SIGNIFICANT>No</ECONOMICALLY_SIGNIFICANT>
      <DATE_RECEIVED>2025-08-11</DATE_RECEIVED>
      <LEGAL_DEADLINE>None</LEGAL_DEADLINE>
      <DATE_COMPLETED>2025-12-02</DATE_COMPLETED>
      <DECISION>Consistent with Change</DECISION>
      <DATE_PUBLISHED></DATE_PUBLISHED>
      <MAJOR>Yes</MAJOR>
      ... plus Yes/No flags: PANDEMIC_RESPONSE, HEALTH_CARE_ACT, DODD_FRANK_ACT,
      INTERNATIONAL_IMPACTS, UNFUNDED_MANDATES, HOMELAND_SECURITY,
      REGULATORY_FLEXIBILITY_ANALYSIS, SMALL_ENTITIES_AFFECTED,
      FEDERALISM_IMPLICATIONS, TCJA (newer files only), REVIEW_EXTENDED (rare)
    </REGACT>

Reviews have **no unique identifier** in the export. A RIN can be reviewed
several times (once per stage, and resubmissions), and pre-2000 files even hold
distinct reviews that share RIN, stage, and received date. Older files also
use older wording for ``DECISION`` (``Consistent w/no change`` rather than
``Consistent without Change``). The reader passes values through unchanged.

Meeting record (``<RESULTS>`` root, repeated ``<ROW>``; encoding ``Cp1252``)::

    <ROW>
      <RIN>0581-AE30</RIN>
      <MEETING_ID>628973</MEETING_ID>
      <TITLE>Price Discovery and Transparency in Markets for Fed Cattle</TITLE>
      <AGENCYSUBAGENCYACRONYM>0581-USDA/AMS</AGENCYSUBAGENCYACRONYM>
      <RULE_STAGE_DESC>Prerule Stage</RULE_STAGE_DESC>
      <MEETINGDATETIME>02-OCT-24</MEETINGDATETIME>
      <REQUESTOR>Campaign for Family Farms and the Environment</REQUESTOR>
      <REQUESTOR_NAME>Patty  Lovera</REQUESTOR_NAME>
      <TYPE_CD>C</TYPE_CD>
      <MEETING_TYPE>Completed</MEETING_TYPE>
      <SUMMARY></SUMMARY>
    </ROW>

Each row is one meeting-log entry: a requesting organization and its contact
for a meeting on a rule. A meeting with several requesting parties spans
several rows. ``MEETING_ID`` is present in the second-half 2024 file but absent
from the first-half file, so it cannot serve as a key. Text fields arrive
double-escaped (``&#040;`` for ``(``), which :func:`_text` undoes. The full
attendee list, including the OIRA and agency staff present, is only on each
meeting's HTML detail page, which this reader does not scrape.
"""

from __future__ import annotations

import html
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from datetime import datetime

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://www.reginfo.gov/public/do"
REPORT_LIST_URL = f"{API_BASE}/XMLReportList"
XML_EXPORT_ENDPOINT = f"{API_BASE}/XMLViewFileAction"

AGENCY_LIST_FILE = "AGY_AGENCY_LIST.xml"
REVIEWS_YTD_FILE = "EO_RULE_COMPLETED_YTD.xml"
REVIEWS_PENDING_FILE = "EO_RULES_UNDER_REVIEW.xml"
_REVIEWS_YEAR_RE = re.compile(r"^EO_RULE_COMPLETED_(\d{4})\.xml$")
_MEETINGS_RE = re.compile(r"^EO_12866_Meeting_Data_[A-Za-z_]+_\d{4}\.xml$")
# Every export link on the report list page has this shape.
_FILE_LINK_RE = re.compile(r"XMLViewFileAction\?f=([A-Za-z0-9_.-]+\.xml)")

_REVIEW_TAG = "REGACT"
_MEETING_TAG = "ROW"

# Yes/No flags on a review that are kept together in one JSON column.
REVIEW_FLAG_TAGS = (
    "PANDEMIC_RESPONSE",
    "HEALTH_CARE_ACT",
    "DODD_FRANK_ACT",
    "INTERNATIONAL_IMPACTS",
    "UNFUNDED_MANDATES",
    "HOMELAND_SECURITY",
    "REGULATORY_FLEXIBILITY_ANALYSIS",
    "SMALL_ENTITIES_AFFECTED",
    "FEDERALISM_IMPLICATIONS",
    "TCJA",
)

_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5
# A full refresh is ~50 requests; space them out so the run is gentle on reginfo.gov.
_REQUEST_DELAY_S = 0.5


class OiraUnavailable(RuntimeError):
    """reginfo.gov could not be reached, or did not serve an expected export."""


def _is_xml(data: bytes) -> bool:
    """True if ``data`` starts (after whitespace) like an XML document rather than HTML.

    A missing file comes back as ``200`` with an HTML error page, so the status
    code alone does not show whether the export exists. Older review files
    (e.g. 1981) have no ``<?xml`` declaration and open directly on ``<OIRA_DATA``.
    """
    head = data.lstrip()[:20].lower()
    return head.startswith(b"<") and not head.startswith((b"<html", b"<!doctype"))


def parse_report_list(html: str) -> list[str]:
    """Return every ``XMLViewFileAction`` file name linked from the report list page."""
    return sorted(set(_FILE_LINK_RE.findall(html)))


def review_files(files: list[str]) -> list[str]:
    """Pick the review exports out of ``files``: per-year files oldest-first, then YTD, then pending.

    Raises :class:`OiraUnavailable` if the YTD or under-review export is not
    listed, since that means the report list page changed shape, not that
    there is nothing to fetch.
    """
    years = sorted(f for f in files if _REVIEWS_YEAR_RE.match(f))
    missing = [f for f in (REVIEWS_YTD_FILE, REVIEWS_PENDING_FILE) if f not in files]
    if missing or not years:
        raise OiraUnavailable(
            f"report list at {REPORT_LIST_URL} is missing {missing or 'per-year review files'}; has the page changed?"
        )
    return [*years, REVIEWS_YTD_FILE, REVIEWS_PENDING_FILE]


def meeting_files(files: list[str]) -> list[str]:
    """Pick the EO 12866 meeting-log exports out of ``files``."""
    found = sorted(f for f in files if _MEETINGS_RE.match(f))
    if not found:
        raise OiraUnavailable(
            f"report list at {REPORT_LIST_URL} lists no EO 12866 meeting files; has the page changed?"
        )
    return found


# --------------------------------------------------------------------------- #
# XML helpers
# --------------------------------------------------------------------------- #

_WS = re.compile(r"\s+")


def _text(elem: ET.Element, tag: str) -> str | None:
    """Return the unescaped, whitespace-collapsed text of ``tag`` under ``elem``, or None if empty.

    The meeting exports escape entities twice (``&amp;#040;`` in the raw XML), so
    the parsed text still holds ``&#040;``; :func:`html.unescape` restores ``(``.
    """
    value = elem.findtext(tag)
    if value is None:
        return None
    value = _WS.sub(" ", html.unescape(value)).strip()
    return value or None


def _iso_date(value: str | None) -> str | None:
    """Normalize a reginfo date to ISO ``YYYY-MM-DD``.

    Review files use ISO already; meeting files use Oracle's ``DD-MON-YY``.
    Anything unparseable becomes None rather than a malformed date.
    """
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(value[:11].strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_agency_list(data: bytes) -> dict[str, tuple[str | None, str | None]]:
    """Map numeric agency code (``"2060"``) to ``(acronym, name)`` from ``AGY_AGENCY_LIST.xml``."""
    out: dict[str, tuple[str | None, str | None]] = {}
    for agency in ET.fromstring(data).iter("AGENCY"):
        code = _text(agency, "AGENCY_CODE")
        if code:
            out[code] = (_text(agency, "ACRONYM"), _text(agency, "NAME"))
    return out


def normalize_review(elem: ET.Element, source_file: str) -> dict:
    """Map one ``<REGACT>`` element onto the keys ``build_oira._shape_review`` reads."""
    return {
        "rin": _text(elem, "RIN"),
        "agency_number": _text(elem, "AGENCY_CODE"),
        "title": _text(elem, "TITLE"),
        "stage": _text(elem, "STAGE"),
        "economically_significant": _text(elem, "ECONOMICALLY_SIGNIFICANT"),
        "major": _text(elem, "MAJOR"),
        "legal_deadline": _text(elem, "LEGAL_DEADLINE"),
        "received_date": _iso_date(_text(elem, "DATE_RECEIVED")),
        "concluded_date": _iso_date(_text(elem, "DATE_COMPLETED")),
        "decision": _text(elem, "DECISION"),
        "published_date": _iso_date(_text(elem, "DATE_PUBLISHED")),
        "review_extended": _text(elem, "REVIEW_EXTENDED"),
        "flags": {tag.lower(): _text(elem, tag) for tag in REVIEW_FLAG_TAGS if _text(elem, tag) is not None},
        "under_review": source_file == REVIEWS_PENDING_FILE,
        "source_file": source_file,
    }


def normalize_meeting(elem: ET.Element, source_file: str) -> dict:
    """Map one meeting ``<ROW>`` onto the keys ``build_oira._shape_meeting`` reads.

    ``AGENCYSUBAGENCYACRONYM`` is ``"<code>-<acronym path>"``, e.g. ``0581-USDA/AMS``.
    """
    agency = _text(elem, "AGENCYSUBAGENCYACRONYM") or ""
    number, _, acronym = agency.partition("-")
    return {
        "meeting_id": _text(elem, "MEETING_ID"),
        "rin": _text(elem, "RIN"),
        "agency_number": number or None,
        "agency_acronym": acronym or None,
        "title": _text(elem, "TITLE"),
        "rule_stage": _text(elem, "RULE_STAGE_DESC"),
        "meeting_date": _iso_date(_text(elem, "MEETINGDATETIME")),
        "meeting_status": _text(elem, "MEETING_TYPE"),
        "requestor_organization": _text(elem, "REQUESTOR"),
        "requestor_name": _text(elem, "REQUESTOR_NAME"),
        "summary": _text(elem, "SUMMARY"),
        "source_file": source_file,
    }


# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #


class _OiraXmlReader(Reader):
    """Shared download logic: list the exports, fetch each, parse its records.

    ``files`` overrides discovery. After iteration :attr:`file_counts` maps each
    fetched file to the number of records it yielded, and :attr:`agencies` holds
    the agency-code map (empty if ``AGY_AGENCY_LIST.xml`` could not be fetched).
    Any listed file that cannot be downloaded raises :class:`OiraUnavailable`:
    the build is a full refresh, so a missing file would silently drop rows.
    """

    record_tag: str

    def __init__(self, *, files: list[str] | None = None, client: httpx.Client | None = None) -> None:
        self.files = files
        self._client = client
        self.file_counts: dict[str, int] = {}
        self.agencies: dict[str, tuple[str | None, str | None]] = {}

    def select_files(self, listed: list[str]) -> list[str]:
        raise NotImplementedError

    def normalize(self, elem: ET.Element, source_file: str) -> dict | None:
        raise NotImplementedError

    def iter_records(self) -> Iterator[dict]:
        if self._client is not None:
            yield from self._iter(self._client)
            return
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
            yield from self._iter(client)

    def _iter(self, client: httpx.Client) -> Iterator[dict]:
        files = self.files
        if files is None:
            files = self.select_files(list_report_files(client))
        self.agencies = self._agency_map(client)
        logger.info("OIRA: fetching {} file(s)", len(files))
        for name in files:
            data = download_file(client, name)
            count = 0
            for elem in ET.fromstring(data).iter(self.record_tag):
                record = self.normalize(elem, name)
                if record is None:
                    continue
                count += 1
                yield record
            self.file_counts[name] = count
            logger.info("OIRA: {} yielded {:,} records", name, count)

    @staticmethod
    def _agency_map(client: httpx.Client) -> dict[str, tuple[str | None, str | None]]:
        """Fetch the agency list, best effort: without it rows keep the numeric code."""
        try:
            return parse_agency_list(download_file(client, AGENCY_LIST_FILE))
        except (OiraUnavailable, ET.ParseError) as exc:
            logger.warning("OIRA: agency list unavailable ({}); agency_code falls back to the numeric code", exc)
            return {}


class OiraReviewsReader(_OiraXmlReader):
    """Yields normalized EO 12866 review dicts: every concluded year, this year so far, and pending.

    The YTD file can overlap a per-year file around New Year (the old year's
    file appears while YTD still covers it). YTD rows concluded in a year that
    has its own file are dropped, so each review is read from exactly one file.
    """

    record_tag = _REVIEW_TAG

    def select_files(self, listed: list[str]) -> list[str]:
        return review_files(listed)

    def _iter(self, client: httpx.Client) -> Iterator[dict]:
        if self.files is None:
            self.files = self.select_files(list_report_files(client))
        self._years_with_files = {m.group(1) for f in self.files if (m := _REVIEWS_YEAR_RE.match(f))}
        yield from super()._iter(client)

    def normalize(self, elem: ET.Element, source_file: str) -> dict | None:
        record = normalize_review(elem, source_file)
        concluded = record["concluded_date"]
        if source_file == REVIEWS_YTD_FILE and concluded and concluded[:4] in self._years_with_files:
            return None
        return record


class OiraMeetingsReader(_OiraXmlReader):
    """Yields normalized EO 12866 meeting-log dicts from every meeting export listed."""

    record_tag = _MEETING_TAG

    def select_files(self, listed: list[str]) -> list[str]:
        return meeting_files(listed)

    def normalize(self, elem: ET.Element, source_file: str) -> dict | None:
        return normalize_meeting(elem, source_file)


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #


def _get(client: httpx.Client, url: str, *, params: dict | None = None, what: str) -> bytes:
    """GET with bounded retries + exponential backoff on transport errors and 429/5xx."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500 and exc.response.status_code != 429:
                raise OiraUnavailable(f"{what}: HTTP {exc.response.status_code}") from exc
            last: Exception = exc
        except httpx.HTTPError as exc:
            last = exc
        if attempt == _MAX_RETRIES:
            raise OiraUnavailable(f"{what}: giving up after {attempt} attempts: {last}") from last
        backoff = min(2**attempt, 30)
        logger.warning(
            "OIRA: {} failed ({}), attempt {}/{}, retrying in {}s", what, last, attempt, _MAX_RETRIES, backoff
        )
        time.sleep(backoff)
    raise AssertionError("unreachable")  # pragma: no cover


def list_report_files(client: httpx.Client) -> list[str]:
    """Return every export file name linked from reginfo.gov's XML report list page."""
    html = _get(client, REPORT_LIST_URL, what="report list").decode("utf-8", errors="replace")
    files = parse_report_list(html)
    if not files:
        raise OiraUnavailable(f"no export links found on {REPORT_LIST_URL}; has the page changed?")
    return files


def download_file(client: httpx.Client, name: str) -> bytes:
    """Download one ``XMLViewFileAction`` export, raising unless the body is XML."""
    time.sleep(_REQUEST_DELAY_S)
    data = _get(client, XML_EXPORT_ENDPOINT, params={"f": name}, what=name)
    if not _is_xml(data):
        raise OiraUnavailable(f"{name} did not return XML (got {len(data)} bytes starting {data[:60]!r})")
    return data

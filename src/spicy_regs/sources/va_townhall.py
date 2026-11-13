"""Reader connector for the Virginia Regulatory Town Hall (townhall.virginia.gov).

There is no state equivalent of regulations.gov, so state rulemaking has to be
read one state at a time. Virginia's Town Hall is the first: it publishes
regulatory actions, their stages and the full text of public comments in one
place, and its model maps closely onto the federal tables:

=====================  ==========================  ==============================
Town Hall              Federal equivalent          Page
=====================  ==========================  ==============================
Action                 docket                      ``L/ViewAction.cfm?actionid=N``
Stage (NOIRA, ...)     document                    ``L/ViewStage.cfm?stageid=N``
Comment forum          comments on a document      ``L/ViewComments.cfm?stageid=N``
=====================  ==========================  ==============================

Comment forums also exist for general notices (``generalnoticeid=N``) and
petitions for rulemaking (``petitionid=N``).

Crawl rules
-----------
There is no API or bulk export, so this is the repo's first HTML-scraped
source. ``robots.txt`` allows ``/L/`` with ``Crawl-delay: 5`` and disallows
``/L./`` and ``/l./`` (with the dot), so every request goes to an ``/L/`` path
and :class:`TownHallClient` spaces requests at least :data:`CRAWL_DELAY_S`
apart. The site's Public Comment Policy states that comments are public records.

What the site does, as observed in 2026-11:

- A missing ID (stage, action or forum) returns ``200`` with an **empty body**.
- A stage that is not publicly viewable yet (a draft) redirects to
  ``statelogin.cfm?error=This stage is not yet publicly viewable``. The client
  does not follow that redirect and treats the stage as missing too.
- IDs are served all the way back to 1999 (stage 100, action 10).
- ``ViewComments.cfm`` pages forums at 50 comments. The page form is a POST with
  ``vPage`` / ``vPerPage``, and larger pages are honored up to a cap: in
  2026-11, 2,500 worked, while 5,000 and 10,000 silently fell back to 50. The
  cap has moved before, so :meth:`VaTownHallReader._forum` asks for
  :data:`FORUM_PAGE_SIZE` per page and keeps paging until it has the count the
  forum header prints. That stays correct even if the site falls back to 50.
- Pages are UTF-8 (the HTTP header is right; the ``<meta>`` tag claims
  windows-1251 and is wrong).
- Comment bodies are user HTML pasted into the page verbatim, often a whole
  ``<!DOCTYPE html><html><body>...`` document per comment. A DOM parser would
  nest the rest of the page inside the first comment, so forums are split on
  the ``<div id="cboxN" class="Cbox">`` markers that open each comment, and only
  the body is run through :class:`html.parser.HTMLParser` for its text.

Parsing uses the standard library only (regex over the page plus
``html.parser``), so the source adds no dependency. Every parser is tested
against trimmed copies of real pages in ``tests/fixtures/va_townhall/``.

Crawl plan
----------
A full backfill is ~11,400 stages, ~7,100 actions, ~3,500 general-notice and
~460 petition forums, and a few thousand stage forums with comments: about two
days of wall time at the required delay. :class:`VaTownHallReader` therefore
runs under a time budget and is resumable:

1. **Discover** (5 requests): the open and recently closed forum lists and the
   "actions underway" lists (in progress, completed, became effective).
2. **Refresh**: every forum on those lists, plus every listed stage that is new
   or was last fetched more than :data:`REFRESH_AFTER_DAYS` ago. A stage on the
   completed or became-effective list is also refetched when the stored copy
   does not yet show it complete. (Those two lists take a ``DayCount`` but, as
   of 2026-11, return the same ~130 and ~70 stages whatever it is set to, so
   membership alone does not mean the stage just changed.)
   Finally, any stored stage whose forum holds fewer comments than the stage's
   ``comment_count`` has its forum refetched (see ``KnownState.forums``).
3. **Backfill** with the time left: walk stage, then general-notice, then
   petition IDs downward from a cursor, skipping IDs already stored. The cursor
   is returned in :attr:`VaTownHallReader.cursors` and persisted by the caller,
   so the next run resumes where this one stopped.

The reader is a *pure source*: it yields one dict per page record, tagged with
``kind`` (``"stage"``, ``"action"`` or ``"comment"``). Shaping into the
published tables is the job of :mod:`spicy_regs.transforms.build_va_townhall`.
"""

from __future__ import annotations

import html
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import partial
from html.parser import HTMLParser
from typing import NamedTuple, TypeVar
from urllib.parse import quote

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

JURISDICTION = "VA"

BASE_URL = "https://townhall.virginia.gov/L/"

#: ``Crawl-delay`` from townhall.virginia.gov/robots.txt.
CRAWL_DELAY_S = 5.0

#: A listed stage is refetched once its stored copy is older than this.
REFRESH_AFTER_DAYS = 7

#: Window for the "recently closed forums" list (and the ``DayCount`` sent to the
#: completed / became-effective lists, which ignore it). Daily runs only need a
#: few days; a week covers missed runs.
RECENT_DAYS = 7

#: ``vPerPage`` posted to ``ViewComments.cfm``. This is under the site's cap
#: (between 2,500 and 5,000 in 2026-11), so nearly every forum is one request.
FORUM_PAGE_SIZE = 1_000

USER_AGENT = "spicy-regs/1.0 (+https://github.com/civictechdc/spicy-regs)"

#: Where the site redirects a stage that is not publicly viewable yet (a draft).
_LOGIN_REDIRECT = "statelogin.cfm"

_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5

FORUM_STAGE = "stage"
FORUM_GENERAL_NOTICE = "general_notice"
FORUM_PETITION = "petition"

#: Query parameter that names each forum type on ``ViewComments.cfm``.
FORUM_PARAMS = {
    FORUM_STAGE: "stageid",
    FORUM_GENERAL_NOTICE: "generalnoticeid",
    FORUM_PETITION: "petitionid",
}
_PARAM_FORUMS = {v: k for k, v in FORUM_PARAMS.items()}

#: Backfill order: stages (and their actions and forums) first, since they carry
#: the rulemaking record; then the general-notice and petition forums.
BACKFILL_KINDS = (FORUM_STAGE, FORUM_GENERAL_NOTICE, FORUM_PETITION)


_T = TypeVar("_T")


class TownHallUnavailable(RuntimeError):
    """townhall.virginia.gov could not be reached after retries."""


class TownHallParseError(ValueError):
    """A non-empty page did not have the shape the parser expects."""


# --------------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------------- #

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_SCRIPT_STYLE = re.compile(r"(?is)<(script|style)\b.*?</\1>")
_FLAGS = re.IGNORECASE | re.DOTALL


def _strip(fragment: str | None) -> str:
    """Tags removed, entities decoded, whitespace (including ``&nbsp;``) collapsed."""
    if not fragment:
        return ""
    text = html.unescape(_TAG.sub(" ", _SCRIPT_STYLE.sub(" ", fragment)))
    return _WS.sub(" ", text).strip()


def _or_none(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


class _TextExtractor(HTMLParser):
    """Plain text from an HTML fragment, keeping paragraph and line breaks."""

    _BLOCK = frozenset("p div br li tr h1 h2 h3 h4 h5 h6 blockquote pre hr table ul ol".split())
    _SKIP = frozenset({"script", "style", "head", "title"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in self._SKIP:
            self._skip += 1
        elif tag == "li":
            self.parts.append("\n- ")
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


_BLANK_LINES = re.compile(r"\n\s*\n+")


def html_to_text(fragment: str) -> str:
    """Readable plain text for a comment body: one blank line between paragraphs."""
    parser = _TextExtractor()
    parser.feed(fragment)
    parser.close()
    lines = [_WS.sub(" ", line.replace("\xa0", " ")).strip() for line in "".join(parser.parts).split("\n")]
    return _BLANK_LINES.sub("\n\n", "\n".join(lines)).strip()


def _iso_date(value: str | None) -> str | None:
    """``M/D/YYYY`` or ``M/D/YY`` to ISO ``YYYY-MM-DD``; None if absent or unparseable."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


_DATE = r"(\d{1,2}/\d{1,2}/\d{2,4})"
_VAC = re.compile(r"(\d+)\s*VAC\s*(\d+)\s*[-‐‑‒–]\s*(\d+)")


def vac_chapter(text: str | None) -> str | None:
    """Normalize a chapter citation as Town Hall prints it to ``9VAC25-194``.

    Town Hall shows ``[9 VAC 25 ‑ 194]`` (non-breaking spaces and a
    non-breaking hyphen). The compact form is the chapter prefix of the section
    citations in ``state_program_approvals.state_citations_json``
    (``9VAC25-194-10``), so the two join on a prefix match.
    """
    m = _VAC.search(text or "")
    return f"{m.group(1)}VAC{m.group(2)}-{m.group(3)}" if m else None


def _abs_url(href: str) -> str:
    """Absolute URL for a link on an ``/L/`` page. ``GetFile`` paths use backslashes."""
    href = html.unescape(href.strip())
    if href.startswith(("http://", "https://")):
        return href
    return BASE_URL + quote(href.lstrip("/"), safe="/?=&.:-_%")


# --------------------------------------------------------------------------- #
# Page parsers (pure functions over page HTML)
# --------------------------------------------------------------------------- #


def _content(page: str) -> str:
    """The page body below the site header and navigation."""
    i = page.find('id="contentwide"')
    return page[i:] if i >= 0 else page


def _header_entity(page: str, link_re: str) -> tuple[int | None, str | None]:
    """``(id, name)`` from the agency / board header row whose icon links ``link_re``."""
    m = re.search(link_re + r"(\d+).*?</a>\s*</div>\s*<div[^>]*>(.*?)</div>", page, _FLAGS)
    return (int(m.group(1)), _or_none(_strip(m.group(2)))) if m else (None, None)


def parse_header(page: str) -> dict:
    """Agency, board and chapter from the header every action/stage/forum page carries."""
    content = _content(page)
    agency_id, agency = _header_entity(content, r"ViewAgency\.cfm\?AgencyNumber=")
    board_id, board = _header_entity(content, r"ViewBoard\.cfm\?BoardID=")
    chapter_id = chapter_title = chapter_citation = None
    # On stage pages the header block closes between the icon and the title,
    # so allow more than one closing </div>.
    m = re.search(r"ViewChapter\.cfm\?ChapterID=(\d+).*?</a>(?:\s*</div>)+\s*<div[^>]*>(.*?)</div>", content, _FLAGS)
    if m:
        chapter_id = int(m.group(1))
        block = m.group(2)
        span = re.search(r"<span", block, re.IGNORECASE)
        chapter_title = _or_none(_strip(block[: span.start()] if span else block))
        chapter_citation = vac_chapter(_strip(block))
    return {
        "agency_id": agency_id,
        "agency": agency,
        "board_id": board_id,
        "board": board,
        "chapter_id": chapter_id,
        "chapter_title": chapter_title,
        "chapter_citation": chapter_citation,
    }


def _stage_rows(page: str) -> list[tuple[int, str | None, str | None]]:
    """``(stage_id, stage_type, status)`` for each row of a "Stages associated" table.

    The action page's table has a Stage ID column first; the stage page's
    pop-up table puts the type in the link text. Both are handled.
    """
    i = page.find("Stages associated with this regulatory action")
    if i < 0:
        return []
    j = page.find("</table>", i)
    table = page[i : j if j > 0 else len(page)]
    rows = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", table, _FLAGS):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, _FLAGS)
        link = re.search(r"ViewStage\.cfm\?stageid=(\d+)[^>]*>(.*?)</a>", row, _FLAGS)
        if not link or len(cells) < 2:
            continue
        stage_id = int(link.group(1))
        if len(cells) >= 3:  # action page: id | type | status
            stage_type, status = _strip(cells[1]), _strip(cells[2])
        else:  # stage page pop-up: "type (id)" | status
            stage_type, status = _strip(link.group(2)), _strip(cells[1])
        rows.append((stage_id, _or_none(stage_type), _or_none(status)))
    return rows


def _documents(content: str) -> list[dict]:
    """The stage's Documents table: title, date and URL of each agency document."""
    m = re.search(r">\s*Documents\s*</td>(.*?)</table>", content, _FLAGS)
    if not m:
        return []
    docs = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(1), _FLAGS):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, _FLAGS)
        if not cells:
            continue
        title = _strip(cells[0])
        if not title:
            continue
        href = re.search(r'href="([^"]+)"', cells[0], re.IGNORECASE)
        when = re.search(_DATE, _strip(cells[1])) if len(cells) > 1 else None
        docs.append(
            {
                "title": title,
                "date": _iso_date(when.group(1)) if when else None,
                "url": _abs_url(href.group(1)) if href else None,
            }
        )
    return docs


def parse_stage(page: str, stage_id: int) -> dict | None:
    """Parse ``ViewStage.cfm``. None for an empty page (no such stage)."""
    if not page.strip():
        return None
    content = _content(page)
    text = _strip(content)
    ids = re.search(r"Action\s+(\d+)\s*/\s*Stage\s+(\d+)", text)
    if not ids or int(ids.group(2)) != stage_id:
        raise TownHallParseError(f"stage {stage_id}: no 'Action N / Stage {stage_id}' marker")

    stage_type = status = None
    for sid, s_type, s_status in _stage_rows(content):
        if sid == stage_id:
            stage_type, status = s_type, s_status
    if stage_type is None:
        head = re.search(r"Stage\s+\d+\s+(.*?)\s+Documents\b", text)
        stage_type = _or_none(re.sub(r"\s+Stage$", "", head.group(1))) if head else None

    # The status table: everything from the main block to the contact details.
    i = content.find('id="maindiv"')
    j = content.find("Contact Information", i if i >= 0 else 0)
    status_text = _strip(content[i if i >= 0 else 0 : j if j > 0 else len(content)])

    reg_date = re.search(r"Publication Date:\s*" + _DATE, status_text)
    vol = re.search(r"Volume:\s*(\d+)\s*Issue:\s*(\d+)", status_text)
    effective = re.search(r"Effective Date\s*" + _DATE, status_text)
    period = re.search(r"Comment Period\s+(.*?)(?:\d[\d,]*\s+comments|$)", status_text)
    period_text = period.group(1) if period else ""
    end = re.search(r"(?:Ends|Ended)(?:\s+on)?\s+" + _DATE, period_text)
    count = re.search(r"Comment Period\b.*?(\d[\d,]*)\s+comments", status_text)

    register_date = _iso_date(reg_date.group(1)) if reg_date else None
    comment_end = _iso_date(end.group(1)) if end else None
    return {
        "kind": "stage",
        "stage_id": stage_id,
        "action_id": int(ids.group(1)),
        "stage_type": stage_type,
        "status": status,
        # Town Hall prints only the end of the comment period. Under the
        # Virginia APA the period opens when the stage is published in the
        # Virginia Register, so the publication date is the start.
        "comment_start_date": register_date if comment_end else None,
        "comment_end_date": comment_end,
        "comment_count": int(count.group(1).replace(",", "")) if count else None,
        "register_volume": int(vol.group(1)) if vol else None,
        "register_issue": int(vol.group(2)) if vol else None,
        "register_date": register_date,
        "effective_date": _iso_date(effective.group(1)) if effective else None,
        "documents": _documents(content),
        **parse_header(page),
    }


def parse_action(page: str, action_id: int) -> dict | None:
    """Parse ``ViewAction.cfm``. None for an empty page (no such action)."""
    if not page.strip():
        return None
    content = _content(page)
    text = _strip(content)
    title = re.search(r"Action:\s*(.*?)\s+Action\s+(\d+)\b", text)
    if not title or int(title.group(2)) != action_id:
        raise TownHallParseError(f"action {action_id}: no 'Action: <title> Action {action_id}' marker")
    summary = re.search(r"Action Summary\s+(.*?)\s+Chapters Affected\b", text)
    chapters = re.search(r"Chapters Affected\s+(.*?)\s+Executive Branch Review\b", text)
    withdrawn = re.search(r"\[Withdrawn\s+" + _DATE + r"\]", text)
    stages = _stage_rows(content)
    return {
        "kind": "action",
        "action_id": action_id,
        "title": _or_none(title.group(1)),
        "summary": _or_none(summary.group(1)) if summary else None,
        "chapters_affected": _or_none(chapters.group(1)) if chapters else None,
        "withdrawn_date": _iso_date(withdrawn.group(1)) if withdrawn else None,
        "stage_ids": [sid for sid, _, _ in stages],
        **parse_header(page),
    }


_CBOX = re.compile(r'<div\s+id="cbox(\d+)"\s+class="Cbox"\s*>', re.IGNORECASE)
_POSTED = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2}\s*[ap]m)", re.IGNORECASE)

#: Tokens after a comma that belong to the person's name, not an organization.
_NAME_SUFFIXES = frozenset(
    "jr jr. sr sr. ii iii iv v md m.d. phd ph.d. rn bsn msn np dnp do d.o. esq esq. cpa pe p.e. lcsw lpc dds dvm "
    "pharmd mph mba ma ms msw jd j.d. aprn crna lmhp fnp cna pa pa-c".split()
)


def split_commenter(raw: str | None) -> tuple[str | None, str | None]:
    """Split Town Hall's ``"Name, Organization"`` commenter field.

    The form has separate name and organization boxes and the page joins them
    with a comma, so the first comma usually separates them. Commas that only
    introduce a name suffix or credential (``Jane Doe, PhD, Acme``) stay with
    the name. A name the commenter typed with commas of its own can still be
    split wrongly; ``commenter_raw`` keeps the field as printed.
    """
    if not raw:
        return None, None
    parts = [p.strip() for p in raw.split(",")]
    n = 1
    while n < len(parts) and parts[n].lower() in _NAME_SUFFIXES:
        n += 1
    name = ", ".join(p for p in parts[:n] if p)
    org = ", ".join(p for p in parts[n:] if p)
    return name or None, org or None


def parse_forum(page: str, forum_type: str, forum_id: int) -> tuple[int | None, list[dict]] | None:
    """Parse a ``ViewComments.cfm`` forum page.

    Returns ``(header_count, comments)``, or None for an empty page (no such
    forum). ``header_count`` is the "N comments" the page prints (None when it
    prints none, as on a forum with no comments).
    """
    if not page.strip():
        return None
    content = _content(page)
    header = re.search(r"<p>\s*([\d,]+)\s+comments?\s*</p>", content, re.IGNORECASE)
    header_count = int(header.group(1).replace(",", "")) if header else None
    stage_id = forum_id if forum_type == FORUM_STAGE else None

    starts = list(_CBOX.finditer(content))
    if not starts and header_count is None and "No Comments Found" not in content:
        raise TownHallParseError(f"{forum_type} forum {forum_id}: no comments and no 'No Comments Found' marker")

    comments = []
    for n, m in enumerate(starts):
        end = starts[n + 1].start() if n + 1 < len(starts) else len(content)
        comments.append(_parse_comment(m.group(1), content[m.end() : end], forum_type, forum_id, stage_id))
    return header_count, comments


def _parse_comment(comment_id: str, chunk: str, forum_type: str, forum_id: int, stage_id: int | None) -> dict:
    """One comment box: posted time, commenter, title and body."""
    head_end = chunk.find('class="divComment"')
    head = chunk[: head_end if head_end >= 0 else len(chunk)]
    posted = _POSTED.search(_strip(head))
    commenter = re.search(r"<strong>\s*Commenter:\s*</strong>(.*?)<br\s*/?>", head, _FLAGS)
    title = re.search(r"<br\s*/?>\s*<br\s*/?>\s*<strong>(.*?)</strong>", head, _FLAGS)

    body = ""
    if head_end >= 0:
        start = chunk.find(">", head_end) + 1
        foot = chunk.rfind("CommentID:")
        stop = chunk.rfind("<div", start, foot) if foot > 0 else -1
        body = chunk[start : stop if stop > start else len(chunk)]
        body = re.sub(r"(?is)\s*</div>\s*$", "", body)

    posted_date = posted_at = None
    if posted:
        posted_date = _iso_date(posted.group(1))
        try:
            clock = datetime.strptime(posted.group(2).replace(" ", "").lower(), "%I:%M%p").strftime("%H:%M")
            posted_at = f"{posted_date}T{clock}" if posted_date else None
        except ValueError:
            posted_at = None

    raw = _or_none(_strip(commenter.group(1))) if commenter else None
    name, org = split_commenter(raw)
    return {
        "kind": "comment",
        "comment_id": int(comment_id),
        "forum_type": forum_type,
        "forum_id": forum_id,
        "stage_id": stage_id,
        "commenter": name,
        "organization": org,
        "commenter_raw": raw,
        "title": _or_none(_strip(title.group(1))) if title else None,
        "comment": html_to_text(body) or None,
        "posted_date": posted_date,
        "posted_at": posted_at,
    }


def parse_forum_list(page: str) -> list[tuple[str, int]]:
    """``(forum_type, forum_id)`` for every forum linked from a ``Forums.cfm`` list."""
    found = re.findall(r"comments\.cfm\?(stageid|generalnoticeid|petitionid)=(\d+)", page, re.IGNORECASE)
    return sorted({(_PARAM_FORUMS[p.lower()], int(i)) for p, i in found})


def parse_stage_list(page: str) -> list[int]:
    """Every stage ID linked from a ``NowInProgress.cfm`` list."""
    return sorted({int(i) for i in re.findall(r"viewstage\.cfm\?stageid=(\d+)", page, re.IGNORECASE)})


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #


class TownHallClient:
    """HTTP client for ``/L/`` pages that honours ``Crawl-delay``.

    Every request (including retries) waits until at least ``delay`` seconds
    after the previous one. ``sleep`` and ``clock`` are injectable for tests.
    """

    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        delay: float = CRAWL_DELAY_S,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client or httpx.Client(
            timeout=_TIMEOUT, follow_redirects=True, headers={"User-Agent": USER_AGENT}
        )
        self._delay = delay
        self._sleep = sleep
        self._clock = clock
        self._last: float | None = None
        self.requests = 0

    def close(self) -> None:
        self._client.close()

    def _wait(self) -> None:
        if self._last is not None:
            remaining = self._delay - (self._clock() - self._last)
            if remaining > 0:
                self._sleep(remaining)
        self._last = self._clock()

    def get(self, page: str, *, params: dict | None = None, data: dict | None = None) -> str:
        """Fetch ``BASE_URL + page`` (POST when ``data`` is given) and return its text."""
        url = BASE_URL + page
        what = f"{page}?{'&'.join(f'{k}={v}' for k, v in (params or {}).items())}"
        last: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            self._wait()
            self.requests += 1
            try:
                if data is None:
                    resp = self._client.get(url, params=params, follow_redirects=False)
                else:
                    resp = self._client.post(url, params=params, data=data, follow_redirects=False)
                if resp.is_redirect:
                    if _LOGIN_REDIRECT in resp.headers.get("location", "").lower():
                        return ""  # not publicly viewable (a draft): same as a missing ID
                    self._wait()
                    self.requests += 1
                    resp = self._client.get(resp.url.join(resp.headers["location"]), follow_redirects=True)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp.content.decode("utf-8", errors="replace")
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise TownHallUnavailable(f"{what}: HTTP {exc.response.status_code}") from exc
                last = exc
            except httpx.HTTPError as exc:
                last = exc
            if attempt < _MAX_RETRIES:
                backoff = min(2**attempt * 5, 120)
                logger.warning(
                    "Town Hall: {} failed ({}), attempt {}/{}, retrying in {}s",
                    what,
                    last,
                    attempt,
                    _MAX_RETRIES,
                    backoff,
                )
                self._sleep(backoff)
        raise TownHallUnavailable(f"{what}: giving up after {_MAX_RETRIES} attempts: {last}") from last

    def stage(self, stage_id: int) -> str:
        return self.get("ViewStage.cfm", params={"stageid": stage_id})

    def action(self, action_id: int) -> str:
        return self.get("ViewAction.cfm", params={"actionid": action_id})

    def forum(self, forum_type: str, forum_id: int, page: int = 1) -> str:
        return self.get(
            "ViewComments.cfm",
            params={FORUM_PARAMS[forum_type]: forum_id},
            data={"vPage": page, "vPerPage": FORUM_PAGE_SIZE},
        )


# --------------------------------------------------------------------------- #
# Reader
# --------------------------------------------------------------------------- #


class StoredStage(NamedTuple):
    """The parts of a stored ``state_stages`` row the crawl decides on."""

    fetched_at: str | None
    comment_count: int | None
    status: str | None = None

    @property
    def complete(self) -> bool:
        """The stored status already shows the stage finished (complete or withdrawn)."""
        status = (self.status or "").lower()
        return "complete" in status or "withdrawn" in status


@dataclass
class KnownState:
    """What the published tables already hold, so the crawl can skip it."""

    stages: dict[int, StoredStage] = field(default_factory=dict)
    actions: set[int] = field(default_factory=set)
    #: Forums stored complete. A stage forum holding fewer comments than the
    #: stage's ``comment_count`` should be left out, so the crawl refetches it.
    forums: set[tuple[str, int]] = field(default_factory=set)


class VaTownHallReader(Reader):
    """Crawls Town Hall within a time budget, yielding stage, action and comment dicts.

    ``cursors`` maps each of :data:`BACKFILL_KINDS` to the next ID the backfill
    will fetch (walking down; 0 means done). Missing kinds are initialized from
    the highest ID the discovery lists show. After iteration, ``cursors`` holds
    the resume point, ``error`` holds the :class:`TownHallUnavailable` that
    stopped the crawl (if any), and ``parse_failures`` lists pages that did not
    parse. A site outage stops the crawl but does not raise, so the caller can
    publish what was fetched before it; the caller decides whether to fail.
    """

    def __init__(
        self,
        *,
        client: TownHallClient | None = None,
        known: KnownState | None = None,
        cursors: dict[str, int] | None = None,
        deadline: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        today: date | None = None,
    ) -> None:
        # The reader closes a client it created itself, not one it was given.
        self._owns_client = client is None
        self.client = client or TownHallClient()
        self.known = known or KnownState()
        self.cursors: dict[str, int] = dict(cursors or {})
        self.deadline = deadline
        self._clock = clock
        self.today = today or date.today()
        self.error: TownHallUnavailable | None = None
        self.parse_failures: list[str] = []
        self.forum_count_mismatches: list[str] = []
        self.pages = 0
        self._done_stages: set[int] = set()
        self._done_actions: set[int] = set()
        self._done_forums: set[tuple[str, int]] = set()

    # -- budget ------------------------------------------------------------ #

    def _time_left(self) -> bool:
        return self.deadline is None or self._clock() < self.deadline

    # -- unit fetches -------------------------------------------------------- #

    def _fetch(self, label: str, fetch: Callable[[], str], parse: Callable[[str], _T | None]) -> _T | None:
        """Fetch and parse one page; a parse error is recorded and returns None."""
        page = fetch()
        self.pages += 1
        try:
            return parse(page)
        except TownHallParseError as exc:
            logger.warning("Town Hall: could not parse {}: {}", label, exc)
            self.parse_failures.append(label)
            return None

    def _forum(self, forum_type: str, forum_id: int) -> Iterator[dict]:
        if (forum_type, forum_id) in self._done_forums:
            return
        self._done_forums.add((forum_type, forum_id))
        comments: dict[int, dict] = {}
        header_count: int | None = None
        page = 1
        while True:
            parsed = self._fetch(
                f"{forum_type} forum {forum_id}" + (f" page {page}" if page > 1 else ""),
                partial(self.client.forum, forum_type, forum_id, page),
                lambda p: parse_forum(p, forum_type, forum_id),
            )
            if not parsed:
                break
            count, batch = parsed
            if page == 1:
                header_count = count
            new = [c for c in batch if c["comment_id"] not in comments]
            comments.update((c["comment_id"], c) for c in new)
            # Keep paging while the header promises more and pages still add
            # comments. If the site ignores the page size and serves 50 at a
            # time, this still walks every page.
            if not new or header_count is None or len(comments) >= header_count:
                break
            page += 1
        if header_count is not None and header_count != len(comments):
            # Hidden or removed comments would show up here.
            self.forum_count_mismatches.append(
                f"{forum_type} {forum_id}: page says {header_count}, parsed {len(comments)}"
            )
        yield from comments.values()

    def _action(self, action_id: int) -> Iterator[dict]:
        if action_id in self._done_actions:
            return
        self._done_actions.add(action_id)
        record = self._fetch(
            f"action {action_id}", lambda: self.client.action(action_id), lambda p: parse_action(p, action_id)
        )
        if record:
            yield record

    def _stage(self, stage_id: int, *, refetch_action: bool, forum_if_changed: bool = True) -> Iterator[dict]:
        """A stage page, then its action page and comment forum when they need fetching."""
        if stage_id in self._done_stages:
            return
        self._done_stages.add(stage_id)
        record = self._fetch(
            f"stage {stage_id}", lambda: self.client.stage(stage_id), lambda p: parse_stage(p, stage_id)
        )
        if not record:
            return
        yield record
        action_id = record["action_id"]
        if refetch_action or action_id not in self.known.actions:
            yield from self._action(action_id)
        count = record["comment_count"] or 0
        prior = self.known.stages.get(stage_id)
        if (
            forum_if_changed
            and count > 0
            and (prior is None or prior.comment_count != count or (FORUM_STAGE, stage_id) not in self.known.forums)
        ):
            yield from self._forum(FORUM_STAGE, stage_id)

    # -- phases ------------------------------------------------------------ #

    def _discover(self) -> tuple[list[tuple[str, int]], list[int], set[int]]:
        """Fetch the forum and stage lists. Returns (forums, listed stages, changed stages)."""
        recent = RECENT_DAYS
        forums: set[tuple[str, int]] = set()
        for params in ({}, {"time": "past", "daysclosed": recent}):
            forums.update(parse_forum_list(self.client.get("Forums.cfm", params=params or None)))
        listed: set[int] = set(parse_stage_list(self.client.get("NowInProgress.cfm")))
        changed: set[int] = set()
        for qtype in ("Completed", "BecomeEffective"):
            changed.update(
                parse_stage_list(self.client.get("NowInProgress.cfm", params={"Qtype": qtype, "DayCount": recent}))
            )
        self.pages += 5
        logger.info(
            "Town Hall: {} forums listed, {} stages in progress, {} recently completed/effective",
            len(forums),
            len(listed),
            len(changed),
        )
        return sorted(forums, key=lambda f: (-f[1], f[0])), sorted(listed | changed, reverse=True), changed

    def _stale(self, stage_id: int) -> bool:
        prior = self.known.stages.get(stage_id)
        if prior is None or not prior.fetched_at:
            return True
        return date.fromisoformat(prior.fetched_at[:10]) <= self.today - timedelta(days=REFRESH_AFTER_DAYS)

    def _newly_complete(self, stage_id: int) -> bool:
        prior = self.known.stages.get(stage_id)
        return prior is None or not prior.complete

    def _refresh(self, forums: list[tuple[str, int]], stages: list[int], changed: set[int]) -> Iterator[dict]:
        for forum_type, forum_id in forums:
            if not self._time_left():
                return
            if forum_type == FORUM_STAGE:
                # The forum is fetched here unconditionally (its count may have
                # moved since the stage page was cached), so skip it in _stage.
                yield from self._stage(forum_id, refetch_action=True, forum_if_changed=False)
            yield from self._forum(forum_type, forum_id)
        for stage_id in stages:
            if not self._time_left():
                return
            if self._stale(stage_id) or (stage_id in changed and self._newly_complete(stage_id)):
                yield from self._stage(stage_id, refetch_action=True)
        # Stored stages whose forum is missing or incomplete, for example forums
        # cut short by an earlier fetch. The stage page itself is still current.
        for stage_id, prior in sorted(self.known.stages.items(), reverse=True):
            if (prior.comment_count or 0) > 0 and (FORUM_STAGE, stage_id) not in self.known.forums:
                if not self._time_left():
                    return
                yield from self._forum(FORUM_STAGE, stage_id)

    def _backfill(self) -> Iterator[dict]:
        for kind in BACKFILL_KINDS:
            while self.cursors.get(kind, 0) > 0:
                if not self._time_left():
                    return
                next_id = self.cursors[kind]
                if kind == FORUM_STAGE:
                    if next_id not in self.known.stages:
                        yield from self._stage(next_id, refetch_action=False)
                elif (kind, next_id) not in self.known.forums:
                    yield from self._forum(kind, next_id)
                # Advance only once the ID's records have all been yielded.
                self.cursors[kind] = next_id - 1

    def iter_records(self) -> Iterator[dict]:
        try:
            forums, stages, changed = self._discover()
            highest = {
                FORUM_STAGE: max([*stages, *(i for t, i in forums if t == FORUM_STAGE)], default=0),
                FORUM_GENERAL_NOTICE: max((i for t, i in forums if t == FORUM_GENERAL_NOTICE), default=0),
                FORUM_PETITION: max((i for t, i in forums if t == FORUM_PETITION), default=0),
            }
            for kind in BACKFILL_KINDS:
                self.cursors.setdefault(kind, highest[kind])
            yield from self._refresh(forums, stages, changed)
            yield from self._backfill()
        except TownHallUnavailable as exc:
            logger.error("Town Hall: crawl stopped early: {}", exc)
            self.error = exc
        finally:
            if self._owns_client:
                self.client.close()
        logger.info(
            "Town Hall: {} pages ({} requests), cursors {}, {} parse failures",
            self.pages,
            self.client.requests,
            self.cursors,
            len(self.parse_failures),
        )

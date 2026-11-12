"""Pure helpers for the ``state_program_approvals`` rollup (no I/O).

Three jobs, each kept separate so it can be tested on fixed strings:

* :data:`PROGRAM_PARTS` / :func:`program_for` — the CFR-part allow-list that
  decides whether a Federal Register document is a state-program action at all.
* :func:`states_in_title` — which state(s) a document concerns, from its title.
* :func:`extract_citations` — phase 2: the state code sections and the state
  effective date, parsed from the document's full-text HTML. Per-state
  patterns; only the states in :data:`CITATION_STATES` are supported.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from html import unescape
from html.parser import HTMLParser

# -- CFR allow-list -----------------------------------------------------------

# (cfr_title, cfr_part) -> program. Only parts where a federal agency approves,
# authorizes or incorporates a *state's own* rules or program. Measured against
# federal_register (Rules + Proposed Rules whose titles name a state):
#
# Deliberately left out:
# * 40 CFR 60 / 61 / 63 — mostly delegations of federal standards to states,
#   not approval of state rules. (Some 112(l) approvals hide in 61/63; they
#   need a title-pattern filter before they can be added.)
# * 40 CFR 81 — EPA's own area designations. Redesignations that approve a
#   state's maintenance plan also cite part 52, so they are still caught.
# * 40 CFR 131 — mostly federally promulgated water quality standards *for* a
#   state, i.e. EPA overriding the state, the opposite of an approval.
# * 40 CFR 123 / 142 / 145 / 233 — the rows in federal_register are national
#   rules; state NPDES / drinking-water primacy approvals are FR notices
#   without CFR references.
# * 40 CFR 49 — tribal implementation plans (tribes, not states).
# * Location-only parts that name states in titles: 33 CFR 100/110/117/165
#   (Coast Guard), 14 CFR 71 (airspace), 47 CFR 73 (FM allotments), 7 CFR 9xx
#   (marketing orders), 50 CFR (fisheries, subsistence).
_EPA_PARTS: dict[int, str] = {
    52: "sip",  # Clean Air Act state implementation plans
    55: "ocs_air",  # OCS air rules: EPA incorporates the adjacent state's rules
    62: "state_plan_111d",  # CAA 111(d)/129 state plans for existing sources
    70: "title_v",  # state operating permit programs
    147: "uic",  # state underground injection control programs
    239: "mswlf",  # state municipal solid waste landfill permit programs
    271: "rcra",  # RCRA hazardous waste program authorization
    272: "rcra",  # incorporation by reference of authorized RCRA programs
    281: "ust",  # underground storage tank program approval
    282: "ust",  # incorporation by reference of approved UST programs
}

PROGRAM_PARTS: dict[tuple[int, int], str] = {(40, part): program for part, program in _EPA_PARTS.items()}
# OSHA state plans (1956 = public-employee-only plans). 29 CFR 1953 is left out:
# its rows are all national procedure rules.
PROGRAM_PARTS[(29, 1952)] = "osha_state_plan"
PROGRAM_PARTS[(29, 1956)] = "osha_state_plan"
# SMCRA: 30 CFR 901-950 hold one part per state (946 = Virginia). Parts for
# states without primacy hold OSM's federal program for that state instead.
for _part in range(901, 951):
    PROGRAM_PARTS[(30, _part)] = "smcra"


def _as_int(value: object) -> int | None:
    """``cfr_references_json`` types title/part inconsistently (52 vs "52")."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def program_for(cfr_references: list[dict] | None) -> tuple[str, int, int] | None:
    """Return ``(program, cfr_title, cfr_part)`` for the first allow-listed reference.

    FR lists a document's primary CFR part first, so the first match wins.
    """
    for ref in cfr_references or []:
        if not isinstance(ref, dict):
            continue
        title, part = _as_int(ref.get("title")), _as_int(ref.get("part"))
        if title is None or part is None:
            continue
        program = PROGRAM_PARTS.get((title, part))
        if program:
            return program, title, part
    return None


# -- State matching -----------------------------------------------------------

STATE_CODES: dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "Puerto Rico": "PR",
    "Guam": "GU",
    "Virgin Islands": "VI",
    "American Samoa": "AS",
    "Northern Mariana Islands": "MP",
}
_CODES = frozenset(STATE_CODES.values())
_BY_LOWER = {name.lower(): code for name, code in STATE_CODES.items()}

# Longest names first, so "West Virginia" is consumed before "Virginia" can
# match inside it (and likewise "Arkansas" / "Kansas" via \b).
_NAME_ALT = "|".join(re.escape(n) for n in sorted(STATE_CODES, key=len, reverse=True))

# A title segment piece that is *only* a state: "Virginia", "State of Utah",
# "the Commonwealth of Virginia", "States of Michigan", or a USPS code ("NC").
_PIECE = re.compile(
    rf"^(?:the\s+)?(?:(?:States?|Commonwealth|Territory)\s+of\s+(?:the\s+)?)?({_NAME_ALT})$",
    re.IGNORECASE,
)

# Fallback: the first state name anywhere in the title. Excludes mentions that
# name a place, not the state: "Kansas City", "Lake Michigan", "Georgia-Pacific"
# (a company), and "Washington" in "Washington, DC" / "Metropolitan Washington"
# / "Washington County".
_MENTION = re.compile(
    rf"(?<!Lake )(?<!Metropolitan )\b({_NAME_ALT})\b"
    r"(?!\s+City\b)(?!-Pacific)(?!,?\s*D\.?\s?C\b)(?!\s+County\b)"
)


_WASHINGTON_DC = re.compile(r"\bWashington,?\s*D\.\s?C\.|\bWashington,?\s*DC\b")


def _leads_with_state(segment: str) -> bool:
    """Whether a segment *starts* with a state, honoring the mention exclusions."""
    segment = segment.strip()
    code = re.match(r"([A-Z]{2})\b", segment)
    return (code is not None and code.group(1) in _CODES) or _MENTION.match(segment) is not None


def _piece_code(piece: str) -> str | None:
    piece = piece.strip().rstrip(".")
    if piece in _CODES:  # USPS codes are matched case-sensitively ("OR" not "or")
        return piece
    m = _PIECE.match(piece)
    return _BY_LOWER[m.group(1).lower()] if m else None


def states_in_title(title: str | None) -> list[str]:
    """USPS codes of the state(s) a state-program document's title concerns.

    1. **Segments.** FR titles separate the state from the subject with
       semicolons or colons: ``Air Plan Approval; Virginia; ...``,
       ``...; District of Columbia, Maryland, and Virginia; ...``,
       ``Air Plan Approval; FL; GA; KY: ...``. A segment counts only when every
       comma/``and``-separated piece of it is a state, so area names such as
       ``Cincinnati, Ohio-Kentucky-Indiana Area`` don't add states. A leading
       state piece also counts (``Air Plan Approval; ID, Updates to ...``), as
       does a trailing USPS code (``City of Memphis, TN``).
    2. **First mention.** Otherwise, the first state name in the title
       (``Revisions to the California State Implementation Plan, ...``,
       ``Kentucky Regulatory Program``). Only the first: later mentions are
       usually area or company names.

    Returns ``[]`` for national rules that name no state.
    """
    if not title:
        return []
    title = _WASHINGTON_DC.sub("District of Columbia", title)
    found: list[str] = []
    for i, segment in enumerate(re.split(r"[;:]", title)):
        pieces = [p for p in re.split(r",|\band\b|&", segment) if p.strip()]
        codes = [_piece_code(p) for p in pieces]
        if pieces and all(codes):
            found.extend(c for c in codes if c)
        elif i > 0 and codes and codes[0] and _leads_with_state(segment):
            found.append(codes[0])
        elif len(pieces) > 1 and pieces[-1].strip() in _CODES:
            # A sub-state jurisdiction tagged with its state: "City of Memphis, TN".
            found.append(pieces[-1].strip())
    if not found:
        m = _MENTION.search(title)
        if m:
            found.append(_BY_LOWER[m.group(1).lower()])
    return list(dict.fromkeys(found))


# -- Phase 2: state citations from the document body -------------------------


_BLOCK_TAGS = frozenset(
    {"p", "br", "div", "li", "table", "tr", "td", "th", "caption", "h1", "h2", "h3", "h4", "h5", "h6"}
)


class _Body(HTMLParser):
    """Collects the document's visible text and the cells of every table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.tables: list[list[list[str]]] = []
        self._stack: list[list[list[str]]] = []
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in _BLOCK_TAGS:
            # A hard break, so prose patterns can't join text across blocks or
            # table cells ("...(COMAR)" | "26.11.08 Control of ...").
            self.text.append(" | ")
        if tag == "table":
            self._stack.append([])
        elif tag == "tr" and self._stack:
            self._stack[-1].append([])
        elif tag in ("td", "th") and self._stack:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._cell is not None and self._stack and self._stack[-1]:
            self._stack[-1][-1].append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop())

    def handle_data(self, data: str) -> None:
        self.text.append(data)
        if self._cell is not None:
            self._cell.append(data)


# State code citation patterns. Each returns a normalized citation string.
#
# Virginia Administrative Code: official form "9VAC5-10-20" (title 9, agency 5,
# chapter 10, section 20). FR prose writes "9 VAC 5-10-20" or "9VAC5-10-20";
# chapter-level references ("9VAC5-140") are kept too.
_VA_PROSE = re.compile(r"\b(\d{1,2})\s?VAC\s?(\d{1,3})-(\d{1,4})(?:-(\d{1,5}))?\b")
# A VA amendatory-table group header: "9 VAC 5, Chapter 10 General Definitions".
_VA_GROUP = re.compile(r"^(\d{1,2})\s?VAC\s?(\d{1,3})\b")
# A VA table cell, relative to its group header: "5-10-20" or "5-10-20 to 5-10-30".
_VA_CELL = re.compile(r"^(\d{1,3})-(\d{1,4})(?:-(\d{1,5}))?\b")

# Code of Maryland Regulations: "COMAR 26.11.08.10" (title.subtitle.chapter.reg).
# Prose prefixes it with "COMAR" (sometimes "(COMAR)"); amendatory-table cells
# hold the bare number. Regulation numbers can carry a letter suffix (".10A").
_MD_NUM = r"(\d{2}\.\d{2}\.\d{2}(?:\.\d{2}[A-Z]?)?)"
_MD_PROSE = re.compile(rf"\bCOMAR\)?\s*{_MD_NUM}(?![\d.])")
_MD_CELL = re.compile(rf"^{_MD_NUM}(?![\d.])")


def _va(text: str, tables: list[list[list[str]]]) -> list[str]:
    out = []
    for m in _VA_PROSE.finditer(text):
        out.append(f"{m.group(1)}VAC{m.group(2)}-{m.group(3)}" + (f"-{m.group(4)}" if m.group(4) else ""))
    for rows in _state_tables(tables):
        prefix = None
        for row in rows:
            first = row[0] if row else ""
            if (g := _VA_GROUP.match(first)) is not None:
                prefix = f"{g.group(1)}VAC"
                continue
            if prefix and len(row) > 1 and (c := _VA_CELL.match(first)) is not None:
                out.append(f"{prefix}{c.group(1)}-{c.group(2)}" + (f"-{c.group(3)}" if c.group(3) else ""))
    return out


def _md(text: str, tables: list[list[list[str]]]) -> list[str]:
    out = [m.group(1) for m in _MD_PROSE.finditer(text)]
    for rows in _state_tables(tables):
        for row in rows:
            # Renumbered entries read "10.18.08/26.11.08.01": keep both numbers.
            if len(row) > 1:
                for part in row[0].split("/"):
                    if (c := _MD_CELL.match(part.strip())) is not None:
                        out.append(c.group(1))
    return [f"COMAR {c}" for c in out]


_CITATION_PARSERS = {"VA": _va, "MD": _md}

#: States whose body citations are parsed. Rows for other states keep
#: ``state_citations_json`` NULL.
CITATION_STATES: frozenset[str] = frozenset(_CITATION_PARSERS)

#: Programs whose prose "became effective on <date>" refers to the state rule.
#: Elsewhere (RCRA, UST, SMCRA, ...) it usually dates the federal approval.
PROSE_DATE_PROGRAMS: frozenset[str] = frozenset({"sip", "state_plan_111d", "title_v", "ocs_air"})

#: Bump when a parser changes so the next build re-fetches and re-parses every
#: document instead of reusing the prior table's results.
PARSER_VERSION = 1

_EFFECTIVE_HEADER = re.compile(r"state\s+effective\s+date", re.IGNORECASE)


def _state_tables(tables: list[list[list[str]]]) -> list[list[list[str]]]:
    """Amendatory tables (EPA-approved state regulations) — they have a state-effective-date column."""
    return [rows for rows in tables if any(_EFFECTIVE_HEADER.search(cell) for row in rows[:3] for cell in row)]


_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y")


def _parse_date(value: str) -> date | None:
    value = " ".join(value.replace(".", "").split())
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


_PROSE_EFFECTIVE = re.compile(
    # Past tense only: "becomes effective on" is the federal action's own date
    # (e.g. RCRA/UST authorizations), which falls after publication.
    r"became\s+(?:state-)?effective\s+(?:on\s+)?"
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4})"
)


def _effective_date(text: str, tables: list[list[list[str]]], *, prose: bool) -> str | None:
    """Latest date in the amendatory table's state-effective-date column.

    Proposed rules have no amendatory table; for those, fall back to prose
    ("...and the revision became effective on April 11, 2024"). The document's
    own effective date is phrased "This final rule is effective on ...", which
    the prose pattern doesn't match. In RCRA/UST documents "became effective"
    describes EPA's own approval, so callers turn the fallback off there.
    """
    dates: list[date] = []
    for rows in _state_tables(tables):
        col = None
        for row in rows:
            if col is None:
                col = next((i for i, cell in enumerate(row) if _EFFECTIVE_HEADER.search(cell)), None)
                continue
            if col < len(row) and (d := _parse_date(row[col])) is not None:
                dates.append(d)
    if not dates and prose:
        dates = [d for m in _PROSE_EFFECTIVE.finditer(text) if (d := _parse_date(m.group(1))) is not None]
    return max(dates).isoformat() if dates else None


def extract_citations(state: str, body_html: str, *, prose_dates: bool = True) -> tuple[list[str], str | None]:
    """Parse a document body for ``state``'s code citations and state effective date.

    Returns ``(sorted unique citations, ISO state effective date or None)``.
    ``prose_dates=False`` reads the date from amendatory tables only.
    Raises ``KeyError`` for a state without a parser (see :data:`CITATION_STATES`).
    """
    parser = _CITATION_PARSERS[state]
    body = _Body()
    body.feed(body_html)
    body.close()
    text = " ".join(unescape("".join(body.text)).split())
    citations = sorted(set(parser(text, body.tables)))
    return citations, _effective_date(text, body.tables, prose=prose_dates)

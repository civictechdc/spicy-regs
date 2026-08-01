"""Citation grammars and Rulespec canonical identifier expansion."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import cast
from urllib.parse import quote

_CFR_COMPACT = re.compile(
    r"^\s*(?P<title>[1-9]\d*)-(?P<part>\d+)(?:\.(?P<section>[A-Za-z0-9][A-Za-z0-9.-]*))?\s*$",
    re.IGNORECASE,
)
_CFR_STANDARD = re.compile(
    r"(?P<title>[1-9]\d*)\s*C\.?\s*F\.?\s*R\.?"
    r"\s*(?:parts?|pt\.?|§{1,2}|sections?|secs?\.?)?\s*"
    r"(?P<part>\d+)(?:\.(?P<section>[A-Za-z0-9][A-Za-z0-9.-]*))?",
    re.IGNORECASE,
)
_CFR_TITLE_PART = re.compile(
    r"(?:title\s+)?(?P<title>[1-9]\d*)\s*[,;:-]?\s*"
    r"(?:C\.?\s*F\.?\s*R\.?\s*)?(?:parts?|pt\.?)\s+(?P<part>\d+)"
    r"(?:\.(?P<section>[A-Za-z0-9][A-Za-z0-9.-]*))?",
    re.IGNORECASE,
)

_USC_STANDARD = re.compile(
    r"(?P<title>[1-9]\d*)\s*U\.?\s*S\.?\s*C\.?"
    r"(?:\s*(?:§{1,2}|sections?|secs?\.?))?\s*"
    r"(?P<section>\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)"
    # A spelled range tail. The hyphenated spelling is already inside
    # ``section`` (the hyphen is also part of the section grammar, so it has to
    # be); this covers ``7401 to 7671q`` and the dash characters the section
    # token cannot hold. Whether either spelling is really a range is decided
    # by :func:`_usc_section_range`, never by the shape of the separator.
    r"(?:(?:\s+(?:to|through)\s+|\s*[–—]\s*)(?P<range_end>\d+[A-Za-z]?))?",
    re.IGNORECASE,
)
_USC_LIST_TAIL = re.compile(
    r"(?:,|\band\b|\bor\b)\s*(?P<section>\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)\b"
    r"(?!\s*(?:U\.?\s*S\.?\s*C|C\.?\s*F\.?\s*R|stat\b))",
    re.IGNORECASE,
)
_USC_TITLE_FORM = re.compile(
    r"(?:sec(?:tion)?\.?\s+)(?P<section>\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)"
    r"\s+of\s+title\s+(?P<title>[1-9]\d*)",
    re.IGNORECASE,
)
_PUBLIC_LAW = re.compile(
    r"(?:public\s+law|pub\.?\s*l\.?|p\.?\s*l\.?)\s*(?:no\.?\s*)?"
    r"(?P<congress>[1-9]\d*)\s*[-–—]\s*(?P<number>[1-9]\d*)",
    re.IGNORECASE,
)
_STATUTES_AT_LARGE = re.compile(
    r"(?P<volume>[1-9]\d*)\s+stat\.?\s+(?P<page>[1-9]\d*)",
    re.IGNORECASE,
)
_EXECUTIVE_ORDER = re.compile(
    r"(?:executive\s+order|exec\.?\s*order|e\.?\s*o\.?)\s*(?:no\.?\s*)?(?P<number>[1-9]\d*)",
    re.IGNORECASE,
)

_IGNORABLE_TAIL = re.compile(
    r"^[\s,;:.]*(?:et\s+seq\.?|as\s+amended|and\s+following|ff\.?)?[\s,;:.]*$",
    re.IGNORECASE,
)


def _digits(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return str(int(text)) if text.isdigit() else None


def _section(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    text = re.sub(r"\([^)]*\)", "", text)
    return text or None


def _cfr_section(value: object) -> str | None:
    """Normalize a Rulespec CFR section suffix, excluding subsection detail."""
    text = _section(value)
    if text is None:
        return None
    return text if re.fullmatch(r"\d+[a-z]{0,3}(?:-[0-9a-z]+)*", text) else None


_USC_SECTION_ATOM = re.compile(r"(?P<number>\d+)(?P<suffix>[a-z]*)")


def _usc_section_key(section: str | None) -> tuple[int, str] | None:
    """Order a U.S.C. section by numeric stem, then letter suffix.

    ``7671`` < ``7671a`` < ``7671q`` < ``7672``. Returns ``None`` for anything
    that is not a single well-formed section token, so a caller can never
    compare two values it does not understand.
    """
    if not section:
        return None
    match = _USC_SECTION_ATOM.fullmatch(section)
    return (int(match["number"]), match["suffix"]) if match else None


def _usc_section_range(section: str | None, range_end: str | None = None) -> tuple[str | None, str | None]:
    """Split a U.S.C. section token into ``(section, range_end)``.

    A hyphen means two different things in the U.S. Code. In ``1395w-4``,
    ``1831p-1`` and ``300j-9`` it is part of one section's name; in
    ``7401-7671q`` and ``1702-1715z`` it separates the endpoints of a range.
    Nothing in the character sequence distinguishes them, so the ordering does:
    a range is a pair whose second endpoint sorts strictly after its first.
    Compound section names never satisfy that, because their suffix is a small
    ordinal (``1831p-1``: 1831 > 1) rather than a later section.

    Fail-closed by construction. An unordered or unparsable pair keeps the
    original token whole rather than guessing, which is why ``42 U.S.C. 1484-86``
    (an abbreviated range) stays a single opaque section: reading it as
    1484-to-86 would be wrong, and reading it as 1484-to-1486 would be an
    invention.
    """
    if not section:
        return (section, None)
    if range_end is not None:
        start, end = section, range_end
    elif section.count("-") == 1:
        start, end = section.split("-")
    else:
        return (section, None)
    low, high = _usc_section_key(start), _usc_section_key(end)
    if low is None or high is None or low >= high:
        return (section, None)
    return (start, end)


def usc_section_covers(section: object, *, start: object, end: object = None) -> bool:
    """Whether an ``authority_edges`` row's section covers ``section``.

    A row whose ``usc_section_end`` is null denotes the single section in
    ``usc_section``. A row that carries one denotes the **closed interval**
    ``[usc_section, usc_section_end]`` — the endpoints are the two sections the
    source text actually names, and the members between them are deliberately
    never materialized, because U.S. Code ranges are sparse and lettered
    (``42 U.S.C. 7401-7671q`` spans the Clean Air Act, whose sections do not
    enumerate to a dense integer sequence).

    So ``usc_section = '7401'`` finds a range's first section and nothing
    inside it; this predicate is the documented way to ask the containment
    question without inventing rows.
    """
    target = _usc_section_key(_section(section))
    low = _usc_section_key(_section(start))
    if target is None or low is None:
        return False
    high = _usc_section_key(_section(end))
    return target == low if high is None else low <= target <= high


@dataclass(frozen=True)
class CfrCitation:
    title: str
    part: str
    section: str | None = None

    @property
    def cfr_ref(self) -> str:
        suffix = f".{self.section}" if self.section else ""
        return f"{self.title}-{self.part}{suffix}"

    @property
    def iri(self) -> str:
        return canonical_cfr_iri(self.title, self.part, self.section)


@dataclass(frozen=True)
class AuthorityCitation:
    authority_type: str
    parse_status: str
    usc_title: str | None = None
    usc_section: str | None = None
    usc_section_end: str | None = None
    pl_number: str | None = None
    statute_at_large: str | None = None
    executive_order: str | None = None

    @property
    def canonical_iri(self) -> str | None:
        if self.authority_type == "usc" and self.usc_title and self.usc_section:
            return canonical_usc_iri(self.usc_title, self.usc_section)
        if self.authority_type == "public_law" and self.pl_number:
            return canonical_pl_iri(self.pl_number)
        if self.authority_type == "eo" and self.executive_order:
            return f"urn:rkaf:us:eo:{int(self.executive_order)}"
        return None


def canonical_cfr_iri(title: object, part: object, section: object = None) -> str:
    title_number = _digits(title)
    part_number = _digits(part)
    section_number = _cfr_section(section)
    if not title_number or not part_number:
        raise ValueError(f"invalid CFR identifier components: title={title!r}, part={part!r}")
    if section is not None and section_number is None:
        raise ValueError(f"invalid CFR section: {section!r}")
    suffix = f".{section_number}" if section_number else ""
    return f"urn:rkaf:us:cfr:{title_number}:{part_number}{suffix}"


def canonical_usc_iri(title: object, section: object) -> str:
    title_number = _digits(title)
    section_number = _section(section)
    if not title_number or not section_number or not re.fullmatch(r"\d+[a-z]?(?:-\d+[a-z]?)?", section_number):
        raise ValueError(f"invalid U.S.C. identifier components: title={title!r}, section={section!r}")
    return f"urn:rkaf:us:usc:{title_number}:{section_number}"


#: A Regulation Identifier Number: a four-digit agency code, then a two-letter
#: sub-agency code and a two-digit sequence. The one definition of the shape —
#: every reader of a RIN column goes through :func:`normalize_rin`.
_RIN = re.compile(r"^\d{4}-[A-Z]{2}\d{2}$")


def normalize_rin(value: object) -> str | None:
    """Return the canonical RIN a value states, or ``None`` when it states none."""
    text = str(value or "").strip().upper()
    return text if _RIN.fullmatch(text) else None


def canonical_rin_iri(rin: object) -> str:
    value = normalize_rin(rin)
    if value is None:
        raise ValueError(f"invalid RIN: {rin!r}")
    return f"urn:rkaf:us:rin:{value}"


def canonical_frdoc_iri(document_number: object) -> str:
    value = str(document_number).strip()
    if not re.fullmatch(r"\d{4}-\d{5}", value):
        raise ValueError(f"invalid Federal Register document number: {document_number!r}")
    return f"urn:rkaf:us:frdoc:{value}"


def federal_register_identifier(document_number: object) -> tuple[str, str]:
    """Expand an FR document number without misclassifying legacy identifiers.

    Rulespec's ``us-frdoc`` lexical space currently covers only
    ``YYYY-NNNNN``. The Federal Register corpus also contains official legacy
    numbers (for example ``E7-21559``) and correction identifiers (for example
    ``C1-2026-13078``). Those values remain losslessly identifiable through
    Rulespec's ``partner-defined`` escape hatch until its scheme is broadened.
    """
    value = str(document_number).strip()
    if not value or any(ord(character) < 32 for character in value):
        raise ValueError(f"invalid Federal Register document number: {document_number!r}")
    try:
        return ("rkaf:us-frdoc", canonical_frdoc_iri(value))
    except ValueError:
        return (
            "rkaf:partner-defined",
            f"urn:spicy-regs:frdoc:{quote(value, safe='')}",
        )


def normalize_regsgov_identifier(identifier: object) -> str | None:
    """Return a canonical Regulations.gov identifier when syntax permits it."""
    value = str(identifier or "").strip().upper()
    return value if re.fullmatch(r"[A-Z0-9]+(?:[-_][A-Z0-9]+)*", value) else None


#: Federal Register metadata writes a docket id behind a human label — "Docket
#: No. FSIS-2025-0012", "Doc. No. AMS-SC-24-0046", "Docket Number X", and
#: sometimes the department names itself first ("DHS Docket No. USCIS-2025-0004").
#: The label is presentation, not identity.
_DOCKET_LABEL_PREFIX = re.compile(
    r"^\s*(?:[A-Za-z]{2,6}\s+)?(?:docket|doc\.?)\s*(?:no\.?|nos\.?|number|id)?\s*",
    re.IGNORECASE,
)

#: What a stripped label must have uncovered for the remainder to be identity:
#: a Regulations.gov docket names its organization, then the year, then the
#: sequence — "FAA-2026-3485", "AMS-SC-24-0046", "FDA-2011-N-0002",
#: "OSHA-V05-2-2006-0785". The organization token opens on a letter, and that is
#: the whole difference between reading "DHS Docket No. USCIS-2025-0004" and
#: turning "MM Docket No. 98-213" into "98-213": a remainder that opens on a
#: number is what the label was numbering, not an identifier hiding behind it.
#: Measured on output/rin-ontology-revision-candidate, requiring the shape
#: refuses 5,214 of the 5,506 mutilated references and costs no real docket.
_REGSGOV_DOCKET_SHAPE = re.compile(r"[A-Z][A-Z0-9]*(?:[-_][A-Z0-9]+)*[-_]\d{2}(?:\d{2})?(?:[-_][A-Z0-9]+)*[-_]\d+")


def normalize_docket_reference(reference: object) -> str | None:
    """Return the Regulations.gov docket identifier a reference states, if any.

    Strip-then-validate, in that order and only in that order. A value that is
    already a well-formed identifier is returned untouched, because the label
    grammar overlaps real agency codes: Commerce dockets are ``DOC-2010-0001``,
    and stripping the ``DOC`` a label rule sees there would destroy the very
    identifier being read. Only a value the scheme cannot express is offered to
    the label rule, and only then is the remainder validated.

    Validating the remainder is stricter than validating the stated value: a
    label may only uncover a docket, never manufacture one, so the remainder has
    to look like a docket id (:data:`_REGSGOV_DOCKET_SHAPE`) and not merely like
    something the scheme could spell. Otherwise "MM Docket No. 98-213" publishes
    the FCC proceeding number "98-213" as a Regulations.gov docket.

    ``None`` means the reference names no Regulations.gov docket — a refusal,
    not a repair. Callers quarantine it; nothing here invents a match.
    """
    stated = str(reference or "").strip()
    if not stated:
        return None
    direct = normalize_regsgov_identifier(stated)
    if direct is not None:
        return direct
    uncovered = normalize_regsgov_identifier(_DOCKET_LABEL_PREFIX.sub("", stated, count=1))
    if uncovered is None or not _REGSGOV_DOCKET_SHAPE.fullmatch(uncovered):
        return None
    return uncovered


def canonical_regsgov_iri(identifier: object) -> str:
    value = normalize_regsgov_identifier(identifier)
    if value is None:
        raise ValueError(f"invalid regulations.gov identifier: {identifier!r}")
    return f"urn:rkaf:us:regsgov:{value}"


def canonical_pl_iri(pl_number: object) -> str:
    value = str(pl_number).strip().replace("–", "-").replace("—", "-")
    match = re.fullmatch(r"([1-9]\d*)-([1-9]\d*)", value)
    if not match:
        raise ValueError(f"invalid public-law number: {pl_number!r}")
    return f"urn:rkaf:us:pl:{int(match.group(1))}-{int(match.group(2))}"


def _cfr_from_match(match: re.Match[str]) -> CfrCitation | None:
    title = _digits(match.group("title"))
    part = _digits(match.group("part"))
    raw_section = match.groupdict().get("section")
    section = _cfr_section(raw_section)
    if not title or not part:
        return None
    if raw_section is not None and section is None:
        return None
    return CfrCitation(title=title, part=part, section=section)


def parse_cfr_citation(value: object) -> list[CfrCitation]:
    """Parse a CFR reference from API dictionaries, compact keys, or prose.

    A source string may contain more than one citation. Duplicate normalized
    citations are collapsed while preserving their first occurrence.
    """
    if isinstance(value, dict):
        components = cast(dict[str, object], value)
        title = _digits(components.get("title"))
        part = _digits(components.get("part"))
        section = _section(components.get("section"))
        return [CfrCitation(title, part, section)] if title and part else []
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    compact = _CFR_COMPACT.fullmatch(text)
    if compact:
        citation = _cfr_from_match(compact)
        return [citation] if citation else []

    found: list[CfrCitation] = []
    for pattern in (_CFR_STANDARD, _CFR_TITLE_PART):
        for match in pattern.finditer(text):
            citation = _cfr_from_match(match)
            if citation is not None and citation not in found:
                found.append(citation)

    # Common UA form: "40 CFR Parts 60 and 63". The primary expression finds
    # the first part; expand simple trailing part numbers under the same title.
    if found:
        first = found[0]
        tail_start = next(iter(_CFR_STANDARD.finditer(text)), None)
        if tail_start:
            tail = text[tail_start.end() :]
            for match in re.finditer(r"(?:,|\band\b|\bor\b)\s*(\d+)(?!\s*C\.?F\.?R\.?)", tail, re.IGNORECASE):
                candidate = CfrCitation(first.title, str(int(match.group(1))), None)
                if candidate not in found:
                    found.append(candidate)
    return found


def _usc_list_expansion(text: str) -> list[tuple[str, str, str | None]]:
    """Expand a U.S.C. section list under the title that introduces it.

    Common UA form: "42 U.S.C. 1395, 1396, 1397". The primary expression
    requires a title before every section, so it finds only the first; expand
    simple trailing section numbers under the same title, mirroring the CFR
    tail expansion in :func:`parse_cfr_citation`. Each expansion stops at the
    next explicit U.S.C. citation so a later title is never mis-attributed, and
    a number that leads another citation form ("117 Stat. 429") is not taken.

    A listed member may itself be a range ("42 U.S.C. 7401, 7671a-7671q"), so
    each one is split by the same rule the leading citation uses.
    """
    matches = list(_USC_STANDARD.finditer(text))
    expanded: list[tuple[str, str, str | None]] = []
    for index, match in enumerate(matches):
        title = _digits(match.group("title"))
        if not title:
            continue
        stop = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        for tail in _USC_LIST_TAIL.finditer(text[match.end() : stop]):
            section, section_end = _usc_section_range(_section(tail.group("section")))
            if section is not None:
                expanded.append((title, section, section_end))
    return expanded


def _status_for_span(text: str, start: int, end: int) -> str:
    """Status for a citation that covers ``text[start:end]`` and nothing else.

    The span is passed rather than taken from the match because a U.S.C. match
    may consume a range tail it then declines to read as a range; the declined
    characters are not covered, so they must still count against ``ok``.
    """
    remainder = f"{text[:start]} {text[end:]}"
    return "ok" if _IGNORABLE_TAIL.fullmatch(remainder) else "partial"


def parse_authority_citation(value: object) -> list[AuthorityCitation]:
    """Parse the common legal-authority forms used by the Unified Agenda.

    Every input produces at least one result. Unknown text is retained as an
    ``other``/``failed`` result; recognized citations embedded in extra prose
    are marked ``partial`` rather than discarded.
    """
    text = "" if value is None else str(value).strip()
    if not text:
        return [AuthorityCitation(authority_type="other", parse_status="failed")]

    citations: list[AuthorityCitation] = []
    patterns: tuple[tuple[re.Pattern[str], str], ...] = (
        (_USC_STANDARD, "usc"),
        (_USC_TITLE_FORM, "usc"),
        (_PUBLIC_LAW, "public_law"),
        (_STATUTES_AT_LARGE, "statute_at_large"),
        (_EXECUTIVE_ORDER, "eo"),
    )
    for pattern, authority_type in patterns:
        for match in pattern.finditer(text):
            if authority_type == "usc":
                usc_section, usc_section_end = _usc_section_range(
                    _section(match.group("section")),
                    _section(match.groupdict().get("range_end")),
                )
                # A range tail the ordering rule declined stays uncovered text,
                # so ``12 U.S.C. 1831p–1`` is still a partial parse of 1831p
                # rather than an ``ok`` one that quietly dropped the suffix.
                covered_end = (
                    match.end("section")
                    if match.groupdict().get("range_end") is not None and usc_section_end is None
                    else match.end()
                )
                citation = AuthorityCitation(
                    authority_type="usc",
                    parse_status=_status_for_span(text, match.start(), covered_end),
                    usc_title=_digits(match.group("title")),
                    usc_section=usc_section,
                    usc_section_end=usc_section_end,
                )
                if citation not in citations:
                    citations.append(citation)
                continue
            status = _status_for_span(text, match.start(), match.end())
            if authority_type == "public_law":
                citation = AuthorityCitation(
                    authority_type="public_law",
                    parse_status=status,
                    pl_number=f"{int(match.group('congress'))}-{int(match.group('number'))}",
                )
            elif authority_type == "statute_at_large":
                citation = AuthorityCitation(
                    authority_type="statute_at_large",
                    parse_status=status,
                    statute_at_large=f"{int(match.group('volume'))}-{int(match.group('page'))}",
                )
            else:
                citation = AuthorityCitation(
                    authority_type="eo",
                    parse_status=status,
                    executive_order=str(int(match.group("number"))),
                )
            if citation not in citations:
                citations.append(citation)

    # A section list is never covered by a single citation, so every member is
    # ``partial`` — the same status the multi-citation normalization below
    # would assign anyway.
    for usc_title, usc_section, usc_section_end in _usc_list_expansion(text):
        listed = AuthorityCitation(
            authority_type="usc",
            parse_status="partial",
            usc_title=usc_title,
            usc_section=usc_section,
            usc_section_end=usc_section_end,
        )
        if listed not in citations:
            citations.append(listed)

    if not citations:
        return [AuthorityCitation(authority_type="other", parse_status="failed")]
    if len(citations) > 1:
        # ``replace`` rather than a restated field list: a hand-copied one is
        # how the citation columns fell out of the dedup key in 91db195.
        citations = [replace(item, parse_status="partial") for item in citations]
    return citations

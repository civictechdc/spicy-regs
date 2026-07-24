"""Citation grammars and Rulespec canonical identifier expansion."""

from __future__ import annotations

import re
from dataclasses import dataclass
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
    r"(?P<section>\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)",
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
    section_number = _section(section)
    if not title_number or not part_number:
        raise ValueError(f"invalid CFR identifier components: title={title!r}, part={part!r}")
    suffix = f".{section_number}" if section_number else ""
    return f"urn:rkaf:us:cfr:{title_number}:{part_number}{suffix}"


def canonical_usc_iri(title: object, section: object) -> str:
    title_number = _digits(title)
    section_number = _section(section)
    if not title_number or not section_number or not re.fullmatch(r"\d+[a-z]?(?:-\d+[a-z]?)?", section_number):
        raise ValueError(f"invalid U.S.C. identifier components: title={title!r}, section={section!r}")
    return f"urn:rkaf:us:usc:{title_number}:{section_number}"


def canonical_rin_iri(rin: object) -> str:
    value = str(rin).strip().upper()
    if not re.fullmatch(r"\d{4}-[A-Z]{2}\d{2}", value):
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


def canonical_regsgov_iri(identifier: object) -> str:
    value = str(identifier).strip().upper()
    if not value or not re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+)+", value):
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
    section = _section(match.groupdict().get("section"))
    if not title or not part:
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


def _status_for_match(text: str, match: re.Match[str]) -> str:
    remainder = f"{text[: match.start()]} {text[match.end() :]}"
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
            status = _status_for_match(text, match)
            if authority_type == "usc":
                citation = AuthorityCitation(
                    authority_type="usc",
                    parse_status=status,
                    usc_title=_digits(match.group("title")),
                    usc_section=_section(match.group("section")),
                )
            elif authority_type == "public_law":
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

    if not citations:
        return [AuthorityCitation(authority_type="other", parse_status="failed")]
    if len(citations) > 1:
        citations = [
            AuthorityCitation(
                authority_type=item.authority_type,
                parse_status="partial",
                usc_title=item.usc_title,
                usc_section=item.usc_section,
                pl_number=item.pl_number,
                statute_at_large=item.statute_at_large,
                executive_order=item.executive_order,
            )
            for item in citations
        ]
    return citations

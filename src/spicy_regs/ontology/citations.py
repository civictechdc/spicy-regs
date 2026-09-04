"""Citation grammars and Rulespec canonical identifier expansion."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from collections.abc import Container
from typing import cast
from urllib.parse import quote

_CFR_COMPACT = re.compile(
    r"^\s*(?P<title>[1-9]\d*)-(?P<part>\d+)(?:\.(?P<section>[A-Za-z0-9][A-Za-z0-9.-]*))?\s*$",
    re.IGNORECASE,
)

#: The Code of Federal Regulations has 50 titles.
#:
#: Every other CFR branch is held in place by something the source text says:
#: :data:`_CFR_STANDARD` requires the literal "CFR", :data:`_CFR_TITLE_PART`
#: requires the word "part". The compact key is the internal ``title-part``
#: spelling of :attr:`CfrCitation.cfr_ref` and has no anchor at all, so pointed
#: at free text it reads any bare ``N-M`` as a citation — greedily, which is how
#: ``'5401-5405'`` published ``urn:rkaf:us:cfr:5401:5405``
#: (docs/evidence/citation-bakeoff-2026-08-02.md, "False positives"). With no
#: anchor to require, the shape has to carry the refusal, and the only fact
#: available in the shape is whether the title is one the CFR has.
_CFR_TITLE_COUNT = 50

#: A section's inner dots and hyphens are part of its name ("60.5-1"); a
#: trailing one is the sentence's punctuation, not the section's.
#:
#: The greedy ``[A-Za-z0-9][A-Za-z0-9.-]*`` this replaces read "49 CFR 900.42."
#: as section "900.42.", which :func:`_cfr_section` then refused — and
#: :func:`_cfr_from_match` drops the *whole* citation when a section was
#: written and could not be read, so a question typed as a sentence lost its
#: citation entirely rather than losing the period. Requiring the last
#: character to be alphanumeric hands the trailing punctuation back to the
#: sentence.
#:
#: Ported from SpicySearch ``identifiers.py:149``. That module's ``_LEFT`` and
#: ``_RIGHT`` boundary guards are deliberately not part of this port: they fix
#: a different defect, and this file's 25 compiled patterns carry no boundary
#: lookbehind at all, so importing the guards for one of them would state a
#: discipline the other 24 do not keep.
_CFR_SECTION_CAPTURE = r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?"

_CFR_STANDARD = re.compile(
    r"(?P<title>[1-9]\d*)\s*C\.?\s*F\.?\s*R\.?"
    r"\s*(?:parts?|pt\.?|§{1,2}|sections?|secs?\.?)?\s*"
    rf"(?P<part>\d+)(?:\.(?P<section>{_CFR_SECTION_CAPTURE}))?",
    re.IGNORECASE,
)
#: The other 24 compiled patterns in this module, including this one and
#: :data:`_CFR_COMPACT`, keep their own section spelling. The port is scoped to
#: the one expression the two regression cases reach; widening it is a separate
#: decision about a grammar nobody has measured.
_CFR_TITLE_PART = re.compile(
    r"(?:title\s+)?(?P<title>[1-9]\d*)\s*[,;:-]?\s*"
    r"(?:C\.?\s*F\.?\s*R\.?\s*)?(?:parts?|pt\.?)\s+(?P<part>\d+)"
    r"(?:\.(?P<section>[A-Za-z0-9][A-Za-z0-9.-]*))?",
    re.IGNORECASE,
)

#: "3 CFR, 1977 Comp., p. 123" — a Title 3 *compilation* locator.
#:
#: Title 3 of the CFR is the annual compilation of presidential documents, so
#: this form points at the page an Executive Order was printed on. It is not a
#: CFR section reference and there is no 3 CFR § 1977. Only title 3 compiles
#: presidential documents, so only title 3 is recognized: another title's
#: "Comp." is a form this parser has never seen and has no meaning to give.
#:
#: Both the single-year volume ("1977 Comp.") and the multi-year one
#: ("1949-1953 Comp") occur; the page is optional, because a volume alone
#: locates no single order.
#: A comma may fall on either side of the volume — "3 CFR, 1977 Comp." and
#: "3 CFR 1979, Comp." both occur — and the range may be spelled rather than
#: dashed ("1949 to 1953"). Every separator here is punctuation around the two
#: facts that matter, the volume and the page, so each is optional and none of
#: them decides anything.
_EO_COMPILATION = re.compile(
    r"\b3\s*C\.?\s*F\.?\s*R\.?\s*,?\s*"
    r"(?P<start>(?:1[789]|20)\d{2})"
    # The separator set is closed, not enumerated. Whatever joins two years in
    # front of "Comp." is punctuation between a volume's endpoints -- a dash, a
    # slash, "to", "through", "thru", "and" -- and none of it is ever a part
    # number. Enumerating instead left "through", the ordinary legal spelling,
    # still minting urn:rkaf:us:cfr:3:1949.
    r"(?:\s*(?:[-–—/]|to|thru|through|and)\s*(?P<end>(?:1[789]|20)\d{2}))?"
    r"\s*,?\s*Comp\.?"
    r"(?:\s*,?\s*(?:pp?\.?|pages?)\s*(?P<page>\d+))?",
    re.IGNORECASE,
)

_USC_STANDARD = re.compile(
    r"(?P<title>[1-9]\d*)\s*U\.?\s*S\.?\s*"
    # The code names itself three ways on this corpus: abbreviated ("U.S.C."),
    # written out ("49 U.S. Code 106"), and as the annotated edition
    # ("50 U.S.C.A. 4701(a)"). All three are the same code, so all three read to
    # the same title and section.
    r"(?:Code\b|C\.?(?:\s*A\.?)?)"
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
#: "49 U.S.C. ch. 311" — a U.S. Code *chapter*, the unit above a section.
#:
#: Measured as the largest well-defined slice of the bakeoff's shared-miss cell:
#: 31 strings neither the project nor CiteURL detected
#: (docs/evidence/citation-bakeoff-2026-08-02.md, "The measured gain"). The
#: corpus spells the abbreviation every way it can — "ch.", "Ch", "ch.13" with
#: no space, "chapter", "Chapter" — and the chapter number may carry a letter
#: ("chapter 13A").
#:
#: Unlike a section, a chapter number never contains a hyphen — there is no
#: chapter "1395w-4" — so a hyphen after one is always a separator and the range
#: tail can accept it directly. It still has to be followed by a number:
#: "22 USC Ch. 34- The Peace Corps Act" cites chapter 34, and the dash there is
#: punctuation before a title. Whether an accepted pair is really a range is
#: decided by the ordering rule, never by the separator.
_USC_CHAPTER = re.compile(
    r"(?P<title>[1-9]\d*)\s*U\.?\s*S\.?\s*(?:Code\b|C\.?(?:\s*A\.?)?)"
    r"\s*(?:chapters?|chaps?\.?|chs?\.?)\s*"
    r"(?P<chapter>\d+[A-Za-z]?)\b"
    r"(?:(?:\s+(?:to|through)\s+|\s*[-–—]\s*)(?P<chapter_end>\d+[A-Za-z]?)\b)?",
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
#: A code that names itself instead of its title number. The Internal Revenue
#: Code *is* title 26 of the U.S. Code, so "I.R.C. 337(d)" and "26 U.S.C.
#: 337(d)" are two spellings of one section and must reach one identifier. The
#: title is read off the code the text names, never inferred from anything else,
#: which is why each such code gets its own expression and its own pinned title
#: in :data:`_NAMED_CODE_USC_TITLE` rather than a shared "guess the code" rule.
#:
#: The abbreviation is three letters and appears inside ordinary words, so the
#: expression is anchored on a word boundary and publishes nothing without a
#: section behind it — naming a code is not citing one.
_INTERNAL_REVENUE_CODE = re.compile(
    r"\bI\.?\s*R\.?\s*C\.?(?:\s*(?:§{1,2}|sections?|secs?\.?))?\s*"
    r"(?P<section>\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)",
    re.IGNORECASE,
)
_NAMED_CODE_USC_TITLE: dict[re.Pattern[str], str] = {_INTERNAL_REVENUE_CODE: "26"}

_PUBLIC_LAW = re.compile(
    # "Public Law", "Pub. L.", "Pub. Law", "P.L." — one law, four spellings.
    r"(?:pub(?:lic)?\.?\s*l(?:aw)?\.?|p\.?\s*l\.?)\s*(?:no\.?\s*)?"
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


def _cfr_title_exists(title: str | None) -> bool:
    """Whether a normalized title number names a title of the CFR."""
    return title is not None and 1 <= int(title) <= _CFR_TITLE_COUNT


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


#: A U.S. Code chapter is not expressible in Rulespec's ``rkaf:us-usc`` lexical
#: space, which the compiled profile constrains to
#: ``^urn:rkaf:us:usc:[1-9][0-9]*:[1-9][0-9]*[a-z]*(-[0-9a-z]+)*$`` — a title
#: and a **section**. Reusing it for a chapter would be worse than merely
#: irregular: title 5 has both a chapter 131 and a section 131, and they are
#: different provisions, so ``urn:rkaf:us:usc:5:131`` would name two objects and
#: be indistinguishable downstream from a correct citation.
#:
#: So a chapter takes the route :func:`federal_register_identifier` already
#: takes for legacy FR document numbers: Rulespec's ``partner-defined`` escape
#: hatch, under this repo's own URN prefix, until the scheme is broadened.
USC_CHAPTER_IDENTIFIER_SCHEME = "rkaf:partner-defined"


@dataclass(frozen=True)
class UscChapterCitation:
    """A U.S. Code chapter: the unit a section belongs to, not a section.

    A range carries its two endpoints and never its members, the same rule
    :class:`AuthorityCitation` follows for sections and for the same reason —
    the members between them are not enumerable without inventing chapters.
    :attr:`iri` names the first chapter, which is a chapter the text states.
    """

    title: str
    chapter: str
    chapter_end: str | None = None

    @property
    def iri(self) -> str:
        return canonical_usc_chapter_iri(self.title, self.chapter)


@dataclass(frozen=True)
class ExecutiveOrderCompilation:
    """Where an Executive Order was printed, not which order it is.

    "3 CFR, 1977 Comp., p. 123" locates a presidential document by the page of
    the Title 3 annual compilation it appears on. The order is what the string
    identifies; the page is only how it points there.

    **This type carries no identifier, and that is the whole point.** Two wrong
    answers are available and both are refused. Reading it as a CFR section
    mints ``urn:rkaf:us:cfr:3:1977`` for a section that does not exist — the
    reading the bakeoff found in CiteURL (``title=3, section=1977``), which
    additionally discards the page, the one component that identifies the order.
    Reading it as an Executive Order requires an order number that is not in the
    string, so publishing one would be an invention.

    Resolving it honestly needs an index this repo does not have: a mapping from
    (Title 3 compilation volume, page) to Executive Order number. The Federal
    Register tables carry ``executive_order_number`` beside an *FR* volume and
    page, which is a different citation system; ``cfr_sections`` carries
    current-edition section metadata from GovInfo, not historical compilations.
    A resolver would need the compilation's own front matter or GovInfo's
    Title 3 compilation packages, neither of which is ingested.

    Until then the honest output is the locator itself, typed so a consumer
    cannot mistake it for an identifier.
    """

    compilation_start: str
    compilation_end: str | None = None
    page: str | None = None


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


def canonical_usc_chapter_iri(title: object, chapter: object) -> str:
    """Expand a U.S. Code chapter under :data:`USC_CHAPTER_IDENTIFIER_SCHEME`.

    Alphabetic suffixes normalize to lowercase, the rule ``rkaf:us-usc`` states
    for sections, so "chapter 13A" and "chapter 13a" are one chapter.
    """
    title_number = _digits(title)
    chapter_number = _section(chapter)
    if not title_number or not chapter_number or not re.fullmatch(r"[1-9]\d*[a-z]?", chapter_number):
        raise ValueError(f"invalid U.S.C. chapter components: title={title!r}, chapter={chapter!r}")
    return f"urn:spicy-regs:usc-chapter:{title_number}:{chapter_number}"


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

#: The spellings a stringified null leaves behind. Matches the sentinel set
#: ``spicy_regs.docpipeline.rkaf_projection._clean`` removes, so both readers of
#: a docket column agree on what "no reference" looks like.
_UNSTATED_SENTINELS = frozenset({"", "None", "nan", "null"})


def docket_reference_as_stated(reference: object) -> str:
    """Return the reference text a source states, or ``""`` when it states none.

    One cleaning for every reader of a docket column, so the link table and the
    RKAF projection quarantine and drop exactly the same strings. Three kinds of
    value state nothing: an empty one, the sentinel a stringified null leaves
    behind, and a bare label — "Docket No." with nothing behind it is
    presentation with nothing to present, and publishing it would key a docket
    on decoration. A value the scheme can already express is never read as a
    label, for the same reason :func:`normalize_docket_reference` validates
    before it strips.
    """
    text = "" if reference is None else str(reference).strip()
    if text in _UNSTATED_SENTINELS:
        return ""
    if normalize_regsgov_identifier(text) is not None:
        return text
    return "" if not _DOCKET_LABEL_PREFIX.sub("", text, count=1).strip() else text


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
    stated = docket_reference_as_stated(reference)
    if not stated:
        return None
    direct = normalize_regsgov_identifier(stated)
    if direct is not None:
        return direct
    uncovered = normalize_regsgov_identifier(_DOCKET_LABEL_PREFIX.sub("", stated, count=1))
    if uncovered is None or not _REGSGOV_DOCKET_SHAPE.fullmatch(uncovered):
        return None
    return uncovered


#: Decoration must be followed by whitespace or a colon. A bare hyphen does
#: not qualify, so a real identifier like ``DOC-2005-0010`` (Commerce) or a
#: hypothetical ``DOCKET-2020-0001`` cannot be truncated into a false match.
DOCKET_DECORATION_PATTERN = r"^\s*(?:docket\s*(?:no|number)?|doc\.?\s*no)\.?[\s:]+"

DOCKET_NORMALIZATION_RULES = (
    "strip_leading_docket_decorations",
    "remove_internal_whitespace",
    "uppercase",
)

_DOCKET_DECORATION = re.compile(DOCKET_DECORATION_PATTERN, re.IGNORECASE)
_INTERNAL_WHITESPACE = re.compile(r"\s+")


def normalize_docket_id(value: object) -> str:
    """Reduce a docket reference to its comparison key.

    A key, not an identifier, and the distinction is what makes it safe.
    :func:`normalize_docket_reference` answers "what docket does this reference
    state?" and refuses anything that is not a Regulations.gov docket; this
    answers "what do I compare it against?" and refuses nothing, because a key
    that matches no docket is simply a key that matches no docket. Comparing is
    not identifying, and a key licenses no join on its own — a key that names
    more than one docket is a collision the joiner quarantines, never resolves.

    Applies :data:`DOCKET_NORMALIZATION_RULES` in order: strip leading Federal
    Register docket decorations (repeatedly, since some references carry two),
    remove internal whitespace that split a real identifier, and upper-case.
    Returns ``""`` when nothing survives.

    Proven in 54f07a6 across 276,326 dockets: 88,073 link rows recovered, zero
    normalized keys covering two dockets
    (docs/corpus-edge-coverage-findings-2026-07-24.md finding #1, RULE-010).
    """
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    previous = None
    while previous != text:
        previous = text
        text = _DOCKET_DECORATION.sub("", text).strip()
    return _INTERNAL_WHITESPACE.sub("", text).upper()


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
    # Every branch carries the title bound, not only the unanchored one. The
    # anchor was never the guarantee: `_CFR_TITLE_PART` reads "<N>, Part <M>",
    # so a *part* number can be read as a title, and a real Unified Agenda value
    # -- "Part 2300, Part 2336, and Part 2339 of 2 CFR" -- published
    # urn:rkaf:us:cfr:2300:2336 in 14 generations from a publishing path.
    if not _cfr_title_exists(title):
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
        if citation is None or not _cfr_title_exists(citation.title):
            return []
        return [citation]

    # A Title 3 compilation locator ("3 CFR, 1977 Comp., p. 123") opens on
    # something a CFR expression can read as title-and-part, and the reading is
    # wrong: it names a page in the presidential compilation, not a section.
    # Refusing its span — rather than the whole string — leaves a real citation
    # standing beside it. See :class:`ExecutiveOrderCompilation`.
    compilations = [match.span() for match in _EO_COMPILATION.finditer(text)]

    def locates_a_compilation(position: int) -> bool:
        return any(start <= position < end for start, end in compilations)

    found: list[CfrCitation] = []
    for pattern in (_CFR_STANDARD, _CFR_TITLE_PART):
        for match in pattern.finditer(text):
            if locates_a_compilation(match.start()):
                continue
            citation = _cfr_from_match(match)
            if citation is not None and citation not in found:
                found.append(citation)

    # Common UA form: "40 CFR Parts 60 and 63". The primary expression finds
    # the first part; expand simple trailing part numbers under the same title.
    #
    # ``(?!\d)`` is the boundary this expansion was missing. The negative
    # lookahead for "CFR" was meant to stop the expansion from swallowing the
    # opening of the *next* citation, but a bare ``\d+`` backtracks out from
    # under it: in "1CFR9,10CFR1" the engine tried "10", failed the CFR
    # lookahead, gave back the "0", and matched "1" — publishing a phantom
    # 1 CFR 1 beside the two real citations the primary expression had already
    # found. Refusing to stop in the middle of a number leaves the lookahead
    # nothing to backtrack into, and a part number never abuts a digit it does
    # not own.
    if found:
        first = found[0]
        tail_start = next(
            (match for match in _CFR_STANDARD.finditer(text) if not locates_a_compilation(match.start())),
            None,
        )
        if tail_start:
            tail = text[tail_start.end() :]
            for match in re.finditer(r"(?:,|\band\b|\bor\b)\s*(\d+)(?!\d)(?!\s*C\.?F\.?R\.?)", tail, re.IGNORECASE):
                if locates_a_compilation(tail_start.end() + match.start()):
                    continue
                candidate = CfrCitation(first.title, str(int(match.group(1))), None)
                if candidate not in found:
                    found.append(candidate)
    return found


#: A section reference, in the spellings the corpus uses after an act name:
#: "sec. 112", "section 111", "secs. 2791", "§112".
_ACT_SECTION = re.compile(
    r"(?:sec(?:tion)?s?\.?|§{1,2})\s*(?P<section>\d+[A-Za-z]?)",
    re.IGNORECASE,
)

#: "div. L", "division J", "Division EE" — the division of the enacting public
#: law a citation names, when it names one. A public law may enact dozens of
#: acts, one per division, so this is the discriminator the source text carries.
_CITED_DIVISION = re.compile(r"\bdiv(?:ision)?\.?\s+(?P<division>[A-Z]{1,3})\b")

#: The inverted spelling: "sec. 3505 of the Modernization of Cosmetics ... Act".
_ACT_SECTION_OF_THE = re.compile(r"\A\s*of\s+(?:the\s+)?", re.IGNORECASE)

#: No popular name in the Popular Name Tool is longer than this many words, and
#: bounding the backward scan keeps recognition linear in the length of the text
#: rather than quadratic.
_MAX_ACT_NAME_WORDS = 24

#: Punctuation a name may pick up from the sentence around it.
_NAME_EDGE = re.compile(r"^[\s(\"'“”]+|[\s,;:.)\"'“”]+$")
_CURLY_APOSTROPHE = re.compile(r"[’‘`]")
_LONG_DASH = re.compile(r"[–—]")


def normalize_popular_name(name: object) -> str:
    """The key a popular name joins on.

    Case, whitespace, sentence punctuation and the difference between a curly
    and a straight apostrophe are all spelling, not identity: the Popular Name
    Tool writes "Workers’ Compensation Act" and prose writes "Workers'
    Compensation Act", and they are one act. Internal commas are kept, because
    "Federal Food, Drug, and Cosmetic Act" is how that act is named.
    """
    text = _NAME_EDGE.sub("", str(name or ""))
    text = _CURLY_APOSTROPHE.sub("'", _LONG_DASH.sub("-", text))
    return re.sub(r"\s+", " ", text).strip().lower()


@dataclass(frozen=True)
class ActRelativeCitation:
    """A provision cited through the act that created it.

    "Clean Air Act section 111" identifies a real provision, but names no code,
    title or section number — it resolves only through the Office of the Law
    Revision Counsel's tables (``spicy_regs.sources.uscode_olrc``), so this type
    carries what the text said and nothing it did not.
    """

    act_name: str
    act_key: str
    section: str
    #: The division the citation itself names, when it names one. ``None`` means
    #: the text stated none — never that it stated the whole law.
    division: str | None = None


def find_act_relative_citations(text: object, *, act_names: Container[str]) -> list[ActRelativeCitation]:
    """Find act-relative citations whose act ``act_names`` knows.

    **The index is the grammar.** ``act_names`` holds normalized popular names —
    in production, the 13,627 the OLRC publishes — and a span is an act name
    only if the index says so. The alternative, recognizing a shape
    (capitalized words ending in "Act"), was measured against the 4,777 sealed
    authority strings and matched "U.S.C." 108 times.

    Longest match wins, because one popular name may end with another: the Clean
    Air Act Amendments of 1977 are not the Clean Air Act, and a shortest-match
    rule would silently cite the wrong statute.

    An act this index does not name is not read. The corpus writes "INA sec.
    103(a)(1)" and "PHS Act secs. 2791(b)(5)", and inferring which acts those
    abbreviate is precisely the guess the identity fence exists to stop.
    """
    document = "" if text is None else str(text)
    found: list[ActRelativeCitation] = []
    for marker in _ACT_SECTION.finditer(document):
        section = _section(marker.group("section"))
        named = _longest_name_before(document[: marker.start()], act_names) or _longest_name_after(
            document[marker.end() :], act_names
        )
        if section is None or named is None:
            continue
        # A division stated anywhere across the citation's own span belongs to
        # it: "Consolidated Appropriations Act of 2018, div. L, title IV, sec.
        # 410" puts it between the name and the section, and the inverted
        # spelling puts it after. The window is the span, not the string, so a
        # second citation's division is never borrowed.
        window = document[max(0, marker.start() - len(named) - 40) : marker.end() + 40]
        stated_division = _CITED_DIVISION.search(window)
        citation = ActRelativeCitation(
            act_name=named,
            act_key=normalize_popular_name(named),
            section=section,
            division=stated_division.group("division") if stated_division else None,
        )
        if citation not in found:
            found.append(citation)
    return found


def _longest_name_before(before: str, act_names: Container[str]) -> str | None:
    """The longest known act name ending where a section reference begins."""
    words = before.split()
    for length in range(min(_MAX_ACT_NAME_WORDS, len(words)), 0, -1):
        candidate = " ".join(words[-length:])
        if normalize_popular_name(candidate) in act_names:
            return _NAME_EDGE.sub("", candidate)
    return None


def _longest_name_after(after: str, act_names: Container[str]) -> str | None:
    """The longest known act name in "… of the <Act>" following a section."""
    opening = _ACT_SECTION_OF_THE.match(after)
    if opening is None:
        return None
    words = after[opening.end() :].split()
    for length in range(min(_MAX_ACT_NAME_WORDS, len(words)), 0, -1):
        candidate = " ".join(words[:length])
        if normalize_popular_name(candidate) in act_names:
            return _NAME_EDGE.sub("", candidate)
    return None


def parse_usc_chapter_citation(value: object) -> list[UscChapterCitation]:
    """Parse U.S. Code chapter references, in order of appearance.

    A chapter reference needs the code that numbers it. A bare "Chapter 33"
    names no title, so it identifies nothing and is not read — attaching it to
    whatever title appeared nearby would be a guess, and the corpus contains
    exactly that string.

    A listed chapter ("47 U.S.C. chs. 2, 5, 9, 13") is expanded under the title
    that introduced it, by the same expression and the same stop rule
    :func:`_usc_list_expansion` uses for sections — so a number that leads a
    different citation ("46 U.S.C. ch. 553, 49 CFR 1.93(a)") is not taken as a
    chapter.
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    found: list[UscChapterCitation] = []
    matches = list(_USC_CHAPTER.finditer(text))
    for index, match in enumerate(matches):
        title = _digits(match.group("title"))
        # A chapter number is ordered exactly like a section: numeric stem, then
        # letter suffix. The pair is a range only when the second endpoint sorts
        # strictly after the first; otherwise the first chapter stands alone.
        chapter, chapter_end = _usc_section_range(
            _section(match.group("chapter")),
            _section(match.group("chapter_end")),
        )
        if not title or not chapter:
            continue
        for citation in (
            UscChapterCitation(title=title, chapter=chapter, chapter_end=chapter_end),
            *(
                UscChapterCitation(title=title, chapter=listed)
                for listed in _listed_chapters(text, match, matches[index + 1 :])
            ),
        ):
            if citation not in found:
                found.append(citation)
    return found


def _listed_chapters(text: str, match: re.Match[str], later: list[re.Match[str]]) -> list[str]:
    """Chapter numbers listed after ``match`` and before the next chapter citation."""
    stop = later[0].start() if later else len(text)
    listed = (_section(tail.group("section")) for tail in _USC_LIST_TAIL.finditer(text[match.end() : stop]))
    return [chapter for chapter in listed if chapter]


def parse_eo_compilation_citation(value: object) -> list[ExecutiveOrderCompilation]:
    """Recognize Title 3 compilation locators without identifying anything.

    Returns one :class:`ExecutiveOrderCompilation` per locator in the text, in
    order of appearance. The type carries no identifier on purpose — see its
    docstring for the two wrong answers this refuses and the index that would
    make a right one possible.
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    return [
        ExecutiveOrderCompilation(
            compilation_start=match.group("start"),
            compilation_end=match.group("end"),
            page=match.group("page"),
        )
        for match in _EO_COMPILATION.finditer(text)
    ]


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
        (_INTERNAL_REVENUE_CODE, "usc"),
        (_PUBLIC_LAW, "public_law"),
        (_STATUTES_AT_LARGE, "statute_at_large"),
        (_EXECUTIVE_ORDER, "eo"),
    )
    for pattern, authority_type in patterns:
        # A named code states its title without spelling a number, so the title
        # comes from the expression that recognized the code.
        named_code_title = _NAMED_CODE_USC_TITLE.get(pattern)
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
                    usc_title=_digits(match.groupdict().get("title")) or named_code_title,
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

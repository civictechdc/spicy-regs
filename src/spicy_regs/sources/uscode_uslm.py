"""Read the U.S. Code's own source credits from the OLRC's USLM XML.

Every section of the Code carries a ``<sourceCredit>`` naming the law that
enacted it and every law that has amended it since:

    (Added Pub. L. 116-260, div. EE, title I, § 107(d)(1), Dec. 27, 2020,
     134 Stat. 3048.)

That is a **second independent statement** of the join
:mod:`spicy_regs.ontology.act_index` gets from OLRC's Table III, and it states
something Table III does not: the *division* of the enacting public law, per
section. Table III is keyed by the enacting public law alone, and one public law
may enact dozens of acts -- 116-260 enacts 94 -- so a division stated per section
is exactly the discriminator Table III lacks.

**The two sources are complementary, not redundant.** Measured on release point
119-102 against the sealed act index: of the 222 unambiguous triples this module
retains for public laws Table III was fetched for, **176 have no in-division
Table III row at all**. 26 U.S.C. 6038E is one of them. A disagreement between
the sources is a coverage fact, not noise, and neither is a tiebreaker over the
other.

**The rule is a refusal rule, and the mess is why.** A credit lists the
enactment and every amendment, so an expression that accepts any ``Pub. L. N-M,
div. X ... § S`` anywhere in a credit pairs a division with a section number
belonging to a different citation in the same credit. Measured: 13,122 triples,
of which 2,916 map to more than one U.S. Code section. Reading role by proximity
to the word "amended" does not fix it -- it credits 26 U.S.C. 7652 to
(116-260, div. EE, § 107), and that credit never says "amended".

:data:`STRICT_ENACTMENT_RULE` requires the citation to sit in an explicit
enactment construction -- ``Added Pub. L. ...`` or ``as added Pub. L. ...``.
That collapses the population to 2,202 triples, 1,877 of them naming exactly one
U.S. Code section, and it removes the 7652 false positive. What it does not
retain, it does not guess at: 22 U.S.C. 2714a reads ``(Pub. L. 114-94, div. C,
title XXXII, § 32101, ...)`` with no enactment construction, and this module
carries no row for it.

**Why an XML parse rather than a regular expression over the file.** Unlike the
Popular Name Tool's flat generated HTML, USLM is well-formed XML whose
``<section>`` elements nest, and a credit must be attributed to the section that
*contains* it. A nearest-preceding-tag scan attributes a credit sitting after a
nested close to the wrong section. The ancestry is the fact being read, so it is
read structurally. The strict rule itself is an expression over the credit's own
flattened text, because the construction it looks for is prose.
"""

from __future__ import annotations

import io
import re
import xml.etree.ElementTree as ElementTree
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

#: The whole Code for one release point, one zip. Measured: 109 MB, ~49 s, 58
#: titles. There is no per-section endpoint worth 51,548 requests.
USLM_RELEASE_URL_TEMPLATE = (
    "https://uscode.house.gov/download/releasepoints/us/pl/{congress}/{law}/xml_uscAll@{congress}-{law}.zip"
)

#: Require an explicit enactment construction. See the module docstring for the
#: measured alternative and why it is unsafe.
STRICT_ENACTMENT_RULE = "added-or-as-added-pub-law-division-act-section-v1"

#: USLM spells a section suffix with an EN DASH (``/us/usc/t16/s824s–1``);
#: Table III, the citation grammar and ``rkaf:us-usc`` all spell it with a
#: hyphen. Verified on release point 119-102: title 16 alone carries 1,487
#: en-dash section identifiers and **zero** hyphen ones, so this is a spelling
#: convention of the source rather than a distinction it draws. Straightening it
#: is what lets the two OLRC surfaces join at all; the verbatim identifier is
#: carried alongside so nothing is lost.
USLM_SECTION_DASH_RULE = "straighten-uslm-en-dash-to-hyphen-v1"

#: Every way this module declines to read a credit. Codes are data: the artifact
#: records them per row and the receipt counts them.
QUARANTINE_REASONS = (
    #: The credit sits under no ``<section>`` at all -- a chapter-level or
    #: appendix credit. 549 of the release point's 51,548 credits are like this,
    #: and none of them carries an enactment construction with a division.
    "credit_outside_usc_section",
    #: The enclosing section's identifier is not ``/us/usc/tN/sX``. The appendix
    #: titles carry ``/us/usc/t18a/pl/91/538/s1`` and its kin.
    "section_identifier_unparsable",
)

_RELEASE_POINT = re.compile(r"^(?P<congress>[1-9]\d{0,2})-(?P<law>[1-9]\d*)$")

#: A U.S. Code section identifier, and only that shape. A subsection
#: (``/us/usc/t26/s6038E/a``) or an appendix path is not a section identifier
#: and must not be read as one.
_USC_SECTION_IDENTIFIER = re.compile(r"^/us/usc/t(?P<title>[1-9]\d*)/s(?P<section>[^/]+)$")

#: Every dash the Statutes at Large, USLM and prose spell a range with.
_DASH = re.compile(r"[‐‑‒–—―]")

#: The enactment construction, the public law, its division, and the act section.
#:
#: ``lead`` is the whole rule: an empty lead is an amendment (or a re-statement
#: of the amended act's own citation) and is discarded. Intervening structural
#: units -- ``title I``, ``subtitle B``, ``ch. 2`` -- are crossed but never
#: captured, because the act section is the last one and the earlier ones are
#: not it.
_ENACTMENT = re.compile(
    r"(?P<lead>as\s+added\s+|added\s+|)"
    r"Pub\.\s*L\.\s*(?P<congress>[1-9]\d{0,2})[-‐-―](?P<law>[1-9]\d*)"
    r"\s*,\s*div\.\s*(?P<division>[A-Z]{1,3})\b"
    r"(?:\s*,\s*(?:title|subtitle|part|ch\.|chapter|subch\.|subchapter)[^,§]{0,40})*"
    r"\s*,?\s*§+\s*(?P<section>[1-9]\d*[A-Za-z]?)",
    re.IGNORECASE,
)

_STATUTES_AT_LARGE = re.compile(r"(?P<volume>[1-9]\d{0,2})\s+Stat\.\s+(?P<page>[1-9]\d{0,4})")

_LEADS = frozenset({"added", "as added"})


def uslm_release_url(release_point: str) -> str:
    """The whole-Code zip for a release point such as ``"119-102"``.

    A release point of any other shape is refused rather than turned into a URL
    that would 404, so a build fails on the key rather than on the response.
    """
    match = _RELEASE_POINT.fullmatch(release_point or "")
    if match is None:
        raise ValueError(f"invalid U.S. Code release point: {release_point!r}")
    return USLM_RELEASE_URL_TEMPLATE.format(congress=match.group("congress"), law=match.group("law"))


def normalize_usc_section(value: object) -> str:
    """Straighten a USLM section suffix to the spelling everything else uses.

    Implements :data:`USLM_SECTION_DASH_RULE`. Nothing else about the section is
    touched: this is a dash, not a normalization policy.
    """
    return _DASH.sub("-", str(value or ""))


@dataclass(frozen=True)
class SourceCredit:
    """One U.S. Code section, and the act section that added it."""

    public_law: str
    division: str
    act_section: str
    usc_title: str
    usc_section: str
    #: The USLM identifier verbatim, en dash and all, so the row stays auditable
    #: against the bytes it was read from.
    usc_identifier: str
    statutes_at_large_volume: str | None = None
    statutes_at_large_page: str | None = None


@dataclass(frozen=True)
class QuarantinedCredit:
    """A credit that matched the strict rule and could not be attributed."""

    reason: str
    public_law: str
    division: str
    act_section: str
    raw_value: str

    def __post_init__(self) -> None:
        if self.reason not in QUARANTINE_REASONS:
            raise ValueError(f"undeclared quarantine reason: {self.reason!r}")


@dataclass(frozen=True)
class CreditScan:
    """What one USLM title said, including what it said nothing usable about."""

    credits: list[SourceCredit] = field(default_factory=list)
    quarantine: list[QuarantinedCredit] = field(default_factory=list)
    credits_scanned: int = 0
    credits_naming_a_division: int = 0
    credits_outside_a_section: int = 0
    strict_matches: int = 0

    def merge(self, other: CreditScan) -> CreditScan:
        return CreditScan(
            credits=self.credits + other.credits,
            quarantine=self.quarantine + other.quarantine,
            credits_scanned=self.credits_scanned + other.credits_scanned,
            credits_naming_a_division=self.credits_naming_a_division + other.credits_naming_a_division,
            credits_outside_a_section=self.credits_outside_a_section + other.credits_outside_a_section,
            strict_matches=self.strict_matches + other.strict_matches,
        )


def iter_source_credits(document: bytes | str) -> Iterator[tuple[str | None, str]]:
    """Yield ``(enclosing section identifier, credit text)`` for one USLM title.

    The identifier is the nearest **ancestor** ``<section>``'s, which is why this
    walks the tree: a credit that follows a nested section's close tag has a
    different nearest-preceding tag than it has ancestor.
    """
    payload = document.encode("utf-8") if isinstance(document, str) else document
    stack: list[str | None] = []
    for event, element in ElementTree.iterparse(io.BytesIO(payload), events=("start", "end")):
        tag = element.tag.rsplit("}", 1)[-1]
        if event == "start":
            stack.append(element.get("identifier") if tag == "section" else None)
            continue
        if tag == "sourceCredit":
            yield next((s for s in reversed(stack) if s), None), _flatten(element)
        stack.pop()


def _flatten(element: ElementTree.Element) -> str:
    """The credit's visible text.

    Only ASCII whitespace is collapsed. USLM writes ``§ 107`` with a narrow
    no-break space, and rewriting it would be editing the source to suit the
    expression rather than the other way round.
    """
    return re.sub(r"[ \t\r\n]+", " ", "".join(element.itertext())).strip()


def scan_source_credits(document: bytes | str) -> CreditScan:
    """Apply :data:`STRICT_ENACTMENT_RULE` to one USLM title's source credits."""
    credits: list[SourceCredit] = []
    quarantine: list[QuarantinedCredit] = []
    scanned = naming_a_division = outside = strict_matches = 0

    for identifier, text in iter_source_credits(document):
        scanned += 1
        if "div." in text:
            naming_a_division += 1
        matches = list(_ENACTMENT.finditer(text))
        for position, match in enumerate(matches):
            if (match.group("lead") or "").strip().lower() not in _LEADS:
                continue
            strict_matches += 1
            public_law = f"{match.group('congress')}-{match.group('law')}"
            division, act_section = match.group("division"), match.group("section")
            section = _USC_SECTION_IDENTIFIER.fullmatch(identifier or "")
            if section is None:
                if identifier is None:
                    outside += 1
                quarantine.append(
                    QuarantinedCredit(
                        reason="credit_outside_usc_section" if identifier is None else "section_identifier_unparsable",
                        public_law=public_law,
                        division=division,
                        act_section=act_section,
                        raw_value=identifier or "",
                    )
                )
                continue
            # The page belongs to the citation it follows, so the search window
            # ends where the next citation begins. A credit listing several
            # enactments states several pages and they are not interchangeable.
            end = matches[position + 1].start() if position + 1 < len(matches) else len(text)
            page = _STATUTES_AT_LARGE.search(text, match.end(), end)
            credits.append(
                SourceCredit(
                    public_law=public_law,
                    division=division,
                    act_section=act_section,
                    usc_title=section.group("title"),
                    usc_section=normalize_usc_section(section.group("section")),
                    usc_identifier=identifier or "",
                    statutes_at_large_volume=page.group("volume") if page else None,
                    statutes_at_large_page=page.group("page") if page else None,
                )
            )

    return CreditScan(
        credits=credits,
        quarantine=quarantine,
        credits_scanned=scanned,
        credits_naming_a_division=naming_a_division,
        credits_outside_a_section=outside,
        strict_matches=strict_matches,
    )


def scan_release_zip(archive: Path) -> tuple[CreditScan, list[tuple[str, str]]]:
    """Scan every title in a whole-Code release zip.

    Returns the merged scan and one ``(member, sha256)`` pair per title, sorted,
    so a receipt pins the bytes each count was read from rather than only the
    zip they arrived in.
    """
    import hashlib
    import zipfile

    scan = CreditScan()
    members: list[tuple[str, str]] = []
    with zipfile.ZipFile(archive) as bundle:
        for name in sorted(n for n in bundle.namelist() if n.endswith(".xml")):
            payload = bundle.read(name)
            members.append((name, f"sha256:{hashlib.sha256(payload).hexdigest()}"))
            scan = scan.merge(scan_source_credits(payload))
    return scan, members

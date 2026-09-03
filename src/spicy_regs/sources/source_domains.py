"""Documented column domains, and the drift between them and what we observe.

A *source domain* is a controlled value list that a publisher documents for one
column of one published table: regulations.gov's ``documentType`` enum, the
Unified Agenda's ``RULE_STAGE`` list, and so on. This module holds the
publisher's half of that claim and the machinery to hold our own data to it.

Why it exists. The published tables are pass-through — ``build_federal_register``
and ``build_unified_agenda`` copy the publisher's strings verbatim — so the values
in a column *are* whatever the publisher emitted, and nothing until now compared
that against what the publisher said it would emit. The two directions differ in
what they mean:

* an **undocumented** value (observed, not documented) says the documented
  enumeration is incomplete. A consumer that switches on the documented list
  silently mishandles those rows.
* an **unobserved** value (documented, not observed) says either the snapshot is
  bounded — the Unified Agenda snapshot holds one semiannual edition, and no
  edition need exercise every documented value — or the publisher retired a
  value without saying so.

Neither is automatically an error, and neither is automatically fine. Both are
therefore carried in :data:`ACCEPTED_DOMAIN_FINDINGS`, a closed ledger: every
finding needs a recorded reason, and a ledger entry that stops being observed
fails just as loudly as an unrecorded finding does. That is what keeps the ledger
from becoming a place where drift goes to be forgotten.

The documented half is not transcribed. Both publisher documents are checked in
under ``sample-data/source-domains/`` as exact bytes, bound to their SHA-256
digest, byte length, publisher URL and observation time by
``documented-enumeration-capture-manifest-v1.json``, and every documented value
is *parsed out of those bytes* on each run. A hand-typed list would rot silently;
a parse against pinned bytes cannot.

What the digest pin does and does not prove. It proves the capture has not
changed since it was pinned, so no value can be edited into or out of the
publisher's document without the pin failing. It does not prove the bytes are
what the publisher serves today, because nothing here refetches ``source_url``
— re-pinning a capture is a deliberate manual act, and it is that act, not this
module, that surfaces a change in the publisher's own enumeration. Both halves
of the comparison are therefore checked-in files, and the result is a function
of the tree: a lock on a dated finding rather than a live drift detector. The
lock is the useful part, because it fails when someone moves one half without
moving the ledger with it.

Note what the reginfo.gov XSD does *not* do: it declares every one of these
elements as an unrestricted ``xs:string`` and states the controlled list only in
an ``xs:documentation`` sentence. There is no ``xs:enumeration`` anywhere in the
file. So the "documented" domain for the Unified Agenda is publisher prose read
by a parser that refuses any sentence it does not recognise.

Parsers here are pure — bytes in, values out, no network and no I/O beyond the
manifest and snapshot reads — and the two document parses are memoized on those
bytes, because one capture states the domain of several columns.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from functools import lru_cache
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import yaml

DEFAULT_SOURCE_DOMAIN_DIR = Path("sample-data/source-domains")
CAPTURE_MANIFEST_FILENAME = "documented-enumeration-capture-manifest-v1.json"
OBSERVED_SNAPSHOT_FILENAME = "observed-domain-snapshot-2026-08-03.json"

CAPTURE_MANIFEST_FORMAT_VERSION = "spicyregs-source-domains/capture-v1"
OBSERVED_SNAPSHOT_FORMAT_VERSION = "spicyregs-source-domains/observed-v1"

_XSD_NS = "{http://www.w3.org/2001/XMLSchema}"


class SourceDomainError(Exception):
    """A capture failed verification, or a publisher document drifted from its captured shape."""


# --- pinned publisher captures ----------------------------------------------


@dataclass(frozen=True)
class DocumentedEnumerationCapture:
    """One exact publisher document that states a controlled value list."""

    key: str
    publisher: str
    states: str
    path: str
    media_type: str
    bytes_digest: str
    byte_length: int
    source_url: str
    observed_at: str
    publisher_revision: str


# A manifest entry states exactly the capture's fields, so the set is derived from
# the dataclass rather than kept beside it: a field added to one cannot go missing
# from the other.
_CAPTURE_FIELDS = frozenset(field.name for field in fields(DocumentedEnumerationCapture))


def load_capture_manifest(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, DocumentedEnumerationCapture]:
    """Read the capture manifest, keyed by capture key. Does not read the captures themselves."""

    path = Path(root) / CAPTURE_MANIFEST_FILENAME
    try:
        manifest = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise SourceDomainError(f"capture manifest {path} is unreadable: {error}") from error
    if not isinstance(manifest, Mapping) or manifest.get("format_version") != CAPTURE_MANIFEST_FORMAT_VERSION:
        raise SourceDomainError(f"capture manifest {path} is not {CAPTURE_MANIFEST_FORMAT_VERSION}")
    captures: dict[str, DocumentedEnumerationCapture] = {}
    for entry in manifest.get("captures") or ():
        if not isinstance(entry, Mapping) or set(entry) != _CAPTURE_FIELDS:
            raise SourceDomainError(f"capture manifest {path} carries a drifted capture entry")
        key = str(entry["key"])
        if key in captures:
            raise SourceDomainError(f"capture manifest {path} repeats capture key {key!r}")
        captures[key] = DocumentedEnumerationCapture(
            key=key,
            publisher=str(entry["publisher"]),
            states=str(entry["states"]),
            path=str(entry["path"]),
            media_type=str(entry["media_type"]),
            bytes_digest=str(entry["bytes_digest"]),
            byte_length=int(entry["byte_length"]),
            source_url=str(entry["source_url"]),
            observed_at=str(entry["observed_at"]),
            publisher_revision=str(entry["publisher_revision"]),
        )
    if not captures:
        raise SourceDomainError(f"capture manifest {path} names no captures")
    return captures


def read_capture(capture: DocumentedEnumerationCapture, *, root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> bytes:
    """Return the capture's bytes, refusing any file whose digest or length differs."""

    path = Path(root) / capture.path
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise SourceDomainError(f"capture {capture.key} is missing at {path}: {error}") from error
    if len(payload) != capture.byte_length:
        raise SourceDomainError(f"capture {capture.key} is {len(payload)} bytes, pinned at {capture.byte_length}")
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    if digest != capture.bytes_digest:
        raise SourceDomainError(f"capture {capture.key} hashes to {digest}, pinned at {capture.bytes_digest}")
    return payload


# --- parsing the regulations.gov OpenAPI document ---------------------------


# Memoized on the exact bytes, so a changed capture is a different entry and never
# a stale hit. One capture states the domain of several columns, and parsing this
# 60 KB document is 35 ms — 97% of the module's runtime — so parsing it once per
# capture rather than once per column is the whole cost of the register.
@lru_cache(maxsize=4)
def _parse_yaml(payload: bytes) -> Mapping[str, Any]:
    try:
        document = yaml.safe_load(payload)
    except yaml.YAMLError as error:
        raise SourceDomainError(f"openapi capture is not well-formed YAML: {error}") from error
    if not isinstance(document, Mapping):
        raise SourceDomainError("openapi capture is not a YAML mapping")
    return document


def openapi_schema_enum(payload: bytes, schema_name: str) -> tuple[str, ...]:
    """Return the ``enum`` members of one ``components.schemas`` entry, in source order.

    ``yaml.safe_load`` constructs no arbitrary Python objects, which is the only
    loader worth pointing at a fetched document. It also settles the capture's
    one whitespace quirk without help: ``- Nonrulemaking `` is a plain scalar,
    and a plain scalar's trailing space is not part of its value. Nothing else
    is normalized — ``Supporting & Related Material`` keeps its literal
    ampersand because that is what the API returns.
    """

    document = _parse_yaml(payload)
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, Mapping) else None
    if not isinstance(schemas, Mapping) or schema_name not in schemas:
        raise SourceDomainError(f"openapi capture declares no components.schemas.{schema_name}")
    schema = schemas[schema_name]
    members = schema.get("enum") if isinstance(schema, Mapping) else None
    if not isinstance(members, list) or not members:
        raise SourceDomainError(f"openapi schema {schema_name!r} states no enum member list")
    for member in members:
        if not isinstance(member, str):
            raise SourceDomainError(f"openapi schema {schema_name!r} enum carries a non-string member {member!r}")
    if len(set(members)) != len(members):
        raise SourceDomainError(f"openapi schema {schema_name!r} repeats an enum member")
    return tuple(members)


# --- parsing the reginfo.gov Unified Agenda XSD -----------------------------

# Every controlled Unified Agenda field is declared ``<xs:restriction
# base="xs:string"/>`` with no facets; the value list exists only as this
# sentence in the element's xs:documentation. Values are double-quoted, which is
# what lets "Substantive, Nonsignificant" survive as one value despite its comma.
_XSD_OPTIONS_PREFIX = re.compile(r"^One of the following options:\s*")
_XSD_QUOTED_OPTION = re.compile(r'"([^"]+)"')

# A DOCTYPE is where entity-expansion and XXE payloads live; this publisher emits
# none. Same guard the document-population readers apply.
_ROOT_ELEMENT_START = re.compile(rb"<[A-Za-z_]")


# Memoized like the YAML parse above, and for the same reason: four documented
# domains are read out of this one document. Callers only read the tree.
@lru_cache(maxsize=4)
def _parse_xsd(payload: bytes) -> ET.Element:
    match = _ROOT_ELEMENT_START.search(payload)
    prolog = payload if match is None else payload[: match.start()]
    if b"<!doctype" in prolog.lower():
        raise SourceDomainError("reginfo XSD contains a DOCTYPE declaration — refusing to parse")
    try:
        return ET.fromstring(payload)
    except ET.ParseError as error:
        raise SourceDomainError(f"reginfo XSD is not well-formed XML: {error}") from error


def xsd_documented_options(payload: bytes, element_name: str) -> tuple[tuple[str, ...], int]:
    """Return one element's documented options and the raw count the sentence lists.

    The sentence is publisher prose, and this publisher's prose repeats itself:
    ``PRIORITY_CATEGORY`` lists ``Not Major`` twice. A literal duplicate is folded
    into one value — inventing a second meaning for it would be worse — and the
    raw count is returned alongside so a caller can pin both numbers.
    """

    root = _parse_xsd(payload)
    matches = [element for element in root.iter(f"{_XSD_NS}element") if element.get("name") == element_name]
    if len(matches) != 1:
        raise SourceDomainError(f"reginfo XSD declares element {element_name!r} {len(matches)} times, expected once")
    documentation = matches[0].find(f"{_XSD_NS}annotation/{_XSD_NS}documentation")
    if documentation is None or not (documentation.text or "").strip():
        raise SourceDomainError(f"reginfo XSD element {element_name!r} carries no documentation string")
    sentence = (documentation.text or "").strip()
    if _XSD_OPTIONS_PREFIX.match(sentence) is None:
        raise SourceDomainError(f"reginfo XSD element {element_name!r} does not state an option list: {sentence!r}")
    raw = [value.strip() for value in _XSD_QUOTED_OPTION.findall(sentence)]
    if not raw:
        raise SourceDomainError(f"reginfo XSD element {element_name!r} quotes no options")
    folded: list[str] = []
    for value in raw:
        if value not in folded:
            folded.append(value)
    return tuple(folded), len(raw)


# --- the register -----------------------------------------------------------


@dataclass(frozen=True)
class DocumentedDomain:
    """One published column, the values its publisher documents, and where it says so."""

    key: str
    table: str
    column: str
    capture_key: str
    locator: str
    values: tuple[str, ...]
    raw_option_count: int


@dataclass(frozen=True)
class _DomainDeclaration:
    key: str
    table: str
    column: str
    capture_key: str
    locator: str
    element: str
    expected_values: int
    expected_raw_options: int


# Six columns, across three of the published tables. The absences, so the set is
# read as a decision and its known hole is not mistaken for one:
#
# * ``TTBL_ACTION`` (XSD L441-448) is a decided exclusion. The publisher's own
#   data treats the field as free text — the 2026-08-03 snapshot's
#   ``timetable_json`` holds 1,139 distinct actions over 10,533 entries against
#   34 documented ones — so a check against its list would report a thousand
#   findings and gate nothing. Its sentence also opens "One of the following:"
#   with unquoted values, which :func:`xsd_documented_options` could not read
#   even if we wanted it: the exclusion and the parser's reach coincide here,
#   and only the first of those is a reason.
# * ``federal_register.document_type`` is absent because no pinned publisher
#   document states its list. The FR API documentation page is not captured
#   here, and a domain nobody published is not a documented domain.
# * ``comments.category`` (documented by OpenAPI ``SubmitterType``, L908-911)
#   and ``comments.document_type`` (by ``DocumentType``, the same list already
#   checked against ``documents``) are an open gap, not a decision. ``comments``
#   is published as a flat ``comments.parquet`` monolith like the rest, so both
#   are observable; they are uncovered only because the 2026-08-03 observation
#   did not scan that table. Extending them needs a re-observation, not a code
#   change here.
_DECLARATIONS: tuple[_DomainDeclaration, ...] = (
    _DomainDeclaration(
        key="regulations-gov-document-type",
        table="documents",
        column="document_type",
        capture_key="regulations-gov-openapi-v4",
        locator="components.schemas.DocumentType.enum (lines 893-898)",
        element="DocumentType",
        expected_values=5,
        expected_raw_options=5,
    ),
    _DomainDeclaration(
        key="regulations-gov-docket-type",
        table="dockets",
        column="docket_type",
        capture_key="regulations-gov-openapi-v4",
        locator="components.schemas.DocketType.enum (lines 902-904)",
        element="DocketType",
        expected_values=2,
        expected_raw_options=2,
    ),
    _DomainDeclaration(
        key="unified-agenda-rule-stage",
        table="unified_agenda",
        column="rule_stage",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/RULE_STAGE xs:documentation (line 66)",
        element="RULE_STAGE",
        expected_values=6,
        expected_raw_options=6,
    ),
    _DomainDeclaration(
        key="unified-agenda-priority-category",
        table="unified_agenda",
        column="priority_category",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/PRIORITY_CATEGORY xs:documentation (line 50)",
        element="PRIORITY_CATEGORY",
        expected_values=6,
        expected_raw_options=7,
    ),
    _DomainDeclaration(
        key="unified-agenda-rin-status",
        table="unified_agenda",
        column="rin_status",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/RIN_STATUS xs:documentation (line 58)",
        element="RIN_STATUS",
        expected_values=2,
        expected_raw_options=2,
    ),
    _DomainDeclaration(
        key="unified-agenda-major",
        table="unified_agenda",
        column="major",
        capture_key="reginfo-rin-data-xsd",
        locator="RIN_INFOType/MAJOR xs:documentation (line 74)",
        element="MAJOR",
        expected_values=3,
        expected_raw_options=3,
    ),
)

DOMAIN_KEYS: tuple[str, ...] = tuple(declaration.key for declaration in _DECLARATIONS)


def documented_domains(root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR) -> dict[str, DocumentedDomain]:
    """Parse every documented domain out of the pinned publisher captures.

    Counts are pinned per domain: a publisher that adds or drops a documented
    value changes the capture, which changes its digest, which this refuses to
    read until the new capture is pinned deliberately.
    """

    captures = load_capture_manifest(root)
    payloads: dict[str, bytes] = {}
    domains: dict[str, DocumentedDomain] = {}
    for declaration in _DECLARATIONS:
        capture = captures.get(declaration.capture_key)
        if capture is None:
            raise SourceDomainError(f"domain {declaration.key} names unpinned capture {declaration.capture_key!r}")
        if capture.key not in payloads:
            payloads[capture.key] = read_capture(capture, root=root)
        payload = payloads[capture.key]
        if capture.media_type == "application/yaml":
            values = openapi_schema_enum(payload, declaration.element)
            raw_options = len(values)
        else:
            values, raw_options = xsd_documented_options(payload, declaration.element)
        if len(values) != declaration.expected_values or raw_options != declaration.expected_raw_options:
            raise SourceDomainError(
                f"domain {declaration.key} parses {len(values)} values from {raw_options} options, "
                f"pinned at {declaration.expected_values} from {declaration.expected_raw_options}"
            )
        domains[declaration.key] = DocumentedDomain(
            key=declaration.key,
            table=declaration.table,
            column=declaration.column,
            capture_key=capture.key,
            locator=f"{capture.path} {declaration.locator}",
            values=values,
            raw_option_count=raw_options,
        )
    return domains


# --- the observed half ------------------------------------------------------


@dataclass(frozen=True)
class ObservedDomain:
    """The distinct values one published column actually carries, with row support."""

    key: str
    table: str
    column: str
    value_counts: tuple[tuple[str, int], ...]
    null_count: int
    row_count: int


@dataclass(frozen=True)
class ObservedSnapshot:
    """One dated observation of every domain column, and the tables it was drawn from."""

    observed_at: str
    producer_revision: str
    sources: tuple[Mapping[str, object], ...]
    domains: dict[str, ObservedDomain]


def load_observed_snapshot(
    root: Path | str = DEFAULT_SOURCE_DOMAIN_DIR,
    *,
    filename: str = OBSERVED_SNAPSHOT_FILENAME,
) -> ObservedSnapshot:
    """Read the checked-in observed snapshot."""

    path = Path(root) / filename
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise SourceDomainError(f"observed snapshot {path} is unreadable: {error}") from error
    if not isinstance(payload, Mapping) or payload.get("format_version") != OBSERVED_SNAPSHOT_FORMAT_VERSION:
        raise SourceDomainError(f"observed snapshot {path} is not {OBSERVED_SNAPSHOT_FORMAT_VERSION}")
    domains: dict[str, ObservedDomain] = {}
    for entry in payload.get("domains") or ():
        key = str(entry["key"])
        if key in domains:
            raise SourceDomainError(f"observed snapshot {path} repeats domain key {key!r}")
        domains[key] = ObservedDomain(
            key=key,
            table=str(entry["table"]),
            column=str(entry["column"]),
            value_counts=tuple((str(value), int(count)) for value, count in entry["value_counts"]),
            null_count=int(entry["null_count"]),
            row_count=int(entry["row_count"]),
        )
    if not domains:
        raise SourceDomainError(f"observed snapshot {path} observes no domains")
    return ObservedSnapshot(
        observed_at=str(payload["observed_at"]),
        producer_revision=str(payload["producer_revision"]),
        sources=tuple(payload.get("sources") or ()),
        domains=domains,
    )


# --- the diff ---------------------------------------------------------------

UNDOCUMENTED = "undocumented-value"
UNOBSERVED = "unobserved-value"
FINDING_KINDS = (UNDOCUMENTED, UNOBSERVED)


@dataclass(frozen=True)
class DomainFinding:
    """One value that the publisher's list and our data disagree about."""

    domain_key: str
    kind: str
    value: str
    row_count: int | None

    @property
    def identifier(self) -> str:
        return f"{self.domain_key}/{self.kind}/{self.value}"


def domain_findings(
    documented: Mapping[str, DocumentedDomain],
    observed: Mapping[str, ObservedDomain],
) -> tuple[DomainFinding, ...]:
    """Diff each documented domain against its observation, in both directions."""

    if set(documented) != set(observed):
        missing = sorted(set(documented) - set(observed))
        extra = sorted(set(observed) - set(documented))
        raise SourceDomainError(f"domain coverage differs; unobserved={missing} undeclared={extra}")
    findings: list[DomainFinding] = []
    for key in sorted(documented):
        declared = documented[key]
        seen = observed[key]
        if (seen.table, seen.column) != (declared.table, declared.column):
            raise SourceDomainError(
                f"domain {key} is declared on {declared.table}.{declared.column} "
                f"but observed on {seen.table}.{seen.column}"
            )
        counts = dict(seen.value_counts)
        for value, count in seen.value_counts:
            if value not in declared.values:
                findings.append(DomainFinding(key, UNDOCUMENTED, value, count))
        for value in declared.values:
            if value not in counts:
                findings.append(DomainFinding(key, UNOBSERVED, value, None))
    return tuple(findings)


# --- the closed ledger of accepted findings ---------------------------------


@dataclass(frozen=True)
class AcceptedFinding:
    """One recorded finding: what it is, and why it is a finding rather than an error."""

    domain_key: str
    kind: str
    value: str
    reason: str


# Every finding the 2026-08-03 snapshot produces, each with the reason it is
# recorded rather than fixed. The ledger is closed in both directions: an
# unrecorded finding fails the gate, and so does a recorded finding the data no
# longer produces, because an exception nothing exercises is a claim nobody
# checked. The captures and their pins live in sample-data/source-domains/.
ACCEPTED_DOMAIN_FINDINGS: tuple[AcceptedFinding, ...] = (
    AcceptedFinding(
        domain_key="regulations-gov-document-type",
        kind=UNDOCUMENTED,
        value="Public Submission",
        reason=(
            "regulations.gov returns this documentType on 373 document rows and labels it in its own web UI, "
            "but the pinned v4 OpenAPI DocumentType enum lists only five values and does not include it. "
            "The publisher's documentation is incomplete; the data is not wrong."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNDOCUMENTED,
        value="First Time Published in The Unified Agenda",
        reason=(
            "Title-cased in the data, sentence-cased in the schema documentation "
            '("First time published in the Unified Agenda"). Same value, different bytes: an exact-match '
            "consumer built from the documentation matches no row at all."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNDOCUMENTED,
        value="Previously Published in The Unified Agenda",
        reason=(
            "The other half of the same casing drift; the schema documents "
            '"Previously published in the Unified Agenda".'
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNOBSERVED,
        value="First time published in the Unified Agenda",
        reason="The sentence-cased documented spelling occurs on no row; the data carries the title-cased form.",
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rin-status",
        kind=UNOBSERVED,
        value="Previously published in the Unified Agenda",
        reason="The sentence-cased documented spelling occurs on no row; the data carries the title-cased form.",
    ),
    AcceptedFinding(
        domain_key="unified-agenda-rule-stage",
        kind=UNOBSERVED,
        value="No Stage",
        reason=(
            "The snapshot holds one semiannual edition (202510), and an edition states only the stages its "
            "RINs are in. Absence here is edition-boundedness, not a retired value."
        ),
    ),
    AcceptedFinding(
        domain_key="unified-agenda-priority-category",
        kind=UNOBSERVED,
        value="Not Major",
        reason=(
            "Absent from edition 202510 for the same reason as No Stage. The XSD documentation also lists "
            "this value twice, which is the publisher's own typo rather than two categories."
        ),
    ),
)


def unrecorded_findings(findings: Sequence[DomainFinding]) -> tuple[DomainFinding, ...]:
    """Findings the ledger does not account for — the gate's failure condition."""

    recorded = {(entry.domain_key, entry.kind, entry.value) for entry in ACCEPTED_DOMAIN_FINDINGS}
    return tuple(finding for finding in findings if (finding.domain_key, finding.kind, finding.value) not in recorded)


def stale_accepted_findings(findings: Sequence[DomainFinding]) -> tuple[AcceptedFinding, ...]:
    """Ledger entries the data no longer produces — the other half of the gate."""

    observed = {(finding.domain_key, finding.kind, finding.value) for finding in findings}
    return tuple(
        entry for entry in ACCEPTED_DOMAIN_FINDINGS if (entry.domain_key, entry.kind, entry.value) not in observed
    )

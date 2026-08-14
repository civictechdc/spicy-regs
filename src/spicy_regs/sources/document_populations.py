"""Readers for exact publisher captures that enumerate a document population.

A *document population capture* is one publisher response that lists which
documents exist rather than carrying any document's body: CBO's per-Congress
publication feed, an FCC ECFS filing-search page and the proceedings named on
it, a GovInfo package summary and the PREMIS record that publishes that
package's official file digests. Acquisition coverage starts here — you cannot
state what a run missed without a publisher-issued enumeration to miss it
against — so these captures and their parsers sit next to the other source
connectors rather than in a corpus or an evaluation tree.

The captures are pinned bytes, not live fetches. Each one is bound to its
SHA-256 digest, byte length, publisher URL, and observation timestamp in
``sample-data/document-populations/document-population-capture-manifest-v1.json``;
:func:`read_capture` re-verifies both digest and length on every read, so a
parser can never run against bytes other than the ones the manifest names.

Every parser here is strict on shape, in the same spirit as the readers that
fetch: a publisher that changes its field set raises
:class:`DocumentPopulationError` rather than silently yielding fewer records.
An empty population and a refused request look identical to a lenient parser,
which is exactly the failure that loses coverage, so the CBO parser also
refuses a bot-challenge body outright (:data:`CHALLENGE_MARKERS`).

Parsers are *pure*: bytes in, plain dicts out, no network and no I/O beyond the
manifest read. Nothing here publishes a table; see the module-level notes on
each publisher for what a full-population acquisition would still need.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

DEFAULT_CAPTURE_MANIFEST_PATH = Path("sample-data/document-populations/document-population-capture-manifest-v1.json")

CAPTURE_MANIFEST_FORMAT_VERSION = "spicyregs-document-populations/v1"


class DocumentPopulationError(Exception):
    """A capture failed verification, or a publisher payload drifted from its captured shape."""


# --- pinned captures --------------------------------------------------------


@dataclass(frozen=True)
class PopulationCapture:
    """One exact publisher response, bound to the bytes the manifest names."""

    key: str
    publisher: str
    population: str
    role: str
    path: str
    media_type: str
    bytes_digest: str
    byte_length: int
    source_url: str
    observation_basis: str
    observed_at: str
    record_count: int | None


_CAPTURE_REQUIRED_FIELDS = frozenset(
    {
        "byte_length",
        "bytes_digest",
        "key",
        "media_type",
        "observation_basis",
        "observed_at",
        "path",
        "population",
        "publisher",
        "role",
        "source_url",
    }
)
# A refusal capture enumerates nothing, so it states no record count.
_CAPTURE_FIELDS = _CAPTURE_REQUIRED_FIELDS | {"record_count"}


def load_capture_manifest(
    manifest_path: Path | str = DEFAULT_CAPTURE_MANIFEST_PATH,
) -> dict[str, PopulationCapture]:
    """Read the capture manifest, keyed by capture key. Does not read the captures themselves."""

    path = Path(manifest_path)
    try:
        manifest = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise DocumentPopulationError(f"capture manifest {path} is unreadable: {error}") from error
    if not isinstance(manifest, Mapping) or manifest.get("format_version") != CAPTURE_MANIFEST_FORMAT_VERSION:
        raise DocumentPopulationError(f"capture manifest {path} is not {CAPTURE_MANIFEST_FORMAT_VERSION}")
    captures: dict[str, PopulationCapture] = {}
    for entry in manifest.get("captures") or ():
        if (
            not isinstance(entry, Mapping)
            or not _CAPTURE_FIELDS.issuperset(entry)
            or not _CAPTURE_REQUIRED_FIELDS.issubset(entry)
        ):
            raise DocumentPopulationError(f"capture manifest {path} carries a drifted capture entry")
        key = str(entry["key"])
        if key in captures:
            raise DocumentPopulationError(f"capture manifest {path} repeats capture key {key!r}")
        captures[key] = PopulationCapture(
            key=key,
            publisher=str(entry["publisher"]),
            population=str(entry["population"]),
            role=str(entry["role"]),
            path=str(entry["path"]),
            media_type=str(entry["media_type"]),
            bytes_digest=str(entry["bytes_digest"]),
            byte_length=int(entry["byte_length"]),
            source_url=str(entry["source_url"]),
            observation_basis=str(entry["observation_basis"]),
            observed_at=str(entry["observed_at"]),
            record_count=None if entry.get("record_count") is None else int(entry["record_count"]),
        )
    if not captures:
        raise DocumentPopulationError(f"capture manifest {path} names no captures")
    return captures


def read_capture(capture: PopulationCapture, *, root: Path | str | None = None) -> bytes:
    """Return the capture's bytes, refusing any file whose digest or length differs."""

    base = Path(root) if root is not None else DEFAULT_CAPTURE_MANIFEST_PATH.parent
    path = base / capture.path
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise DocumentPopulationError(f"capture {capture.key} is missing at {path}: {error}") from error
    if len(payload) != capture.byte_length:
        raise DocumentPopulationError(f"capture {capture.key} is {len(payload)} bytes, pinned at {capture.byte_length}")
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    if digest != capture.bytes_digest:
        raise DocumentPopulationError(f"capture {capture.key} hashes to {digest}, pinned at {capture.bytes_digest}")
    return payload


# --- XML hygiene ------------------------------------------------------------

# A DOCTYPE is where entity-expansion ("billion laughs") and XXE payloads live;
# none of these publishers emits one. Same guard the GAO RSS reader applies.
# In XML a DOCTYPE must precede the root element, and only an element start tag
# opens with a name character — `<?` is a processing instruction, `<!` a comment
# or a declaration — so everything before the first `<name` is the prolog.
_ROOT_ELEMENT_START = re.compile(rb"<[A-Za-z_]")


def _parse_xml(payload: bytes, *, what: str) -> ET.Element:
    match = _ROOT_ELEMENT_START.search(payload)
    prolog = payload if match is None else payload[: match.start()]
    if b"<!doctype" in prolog.lower():
        raise DocumentPopulationError(f"{what} contains a DOCTYPE declaration — refusing to parse")
    try:
        return ET.fromstring(payload)
    except ET.ParseError as error:
        raise DocumentPopulationError(f"{what} is not well-formed XML: {error}") from error


# --- CBO: per-Congress publication feed -------------------------------------

# CBO serves one publication feed per Congress from this path. The
# cost-estimates/xml feed that carries CBO's own topic labels and fiscal facets
# sits behind a DataDome bot wall (see CHALLENGE_MARKERS); these per-Congress
# files sit on a different CDN tier and serve plain HTTP 200. They are a
# discovery channel — title, date, publication URL, bill number — and carry no
# topic labels, budget-function codes, mandate flags, or PAYGO flag.
CBO_PER_CONGRESS_FEED_URL_TEMPLATE = "https://www.cbo.gov/rss/{congress}congress-cost-estimates.xml"
CBO_COST_ESTIMATES_FEED_URL = "https://www.cbo.gov/cost-estimates/xml"

# Publication URLs are the population's identity: one document, one URL.
CBO_PUBLICATION_URL = re.compile(r"^https://www\.cbo\.gov/publication/(?P<publication_id>[1-9][0-9]*)$")

# The feed is CBO's own <response>/<item> shape, not RSS 2.0. Every item carries
# exactly these five children, in this order.
CBO_ITEM_TAGS = ("Title", "Date", "Link", "Description", "Bill_Number")

CBO_PUBLICATION_COLUMNS = (
    "publication_id",
    "publication_url",
    "title",
    "published",
    "description",
    "bill_number",
    "feed_item_key",
)

# Bodies a bot wall returns in place of the feed. A lenient parser reads one of
# these as "the population is empty", which is the coverage failure worth
# failing closed on.
CHALLENGE_MARKERS = (
    b"captcha-delivery.com",
    b"please enable js and disable any ad blocker",
    b"cf-chl-",
    b"challenge-platform",
    b"cf-mitigated",
    b"attention required! | cloudflare",
    b"just a moment...</title>",
)


def cbo_per_congress_feed_url(congress: int) -> str:
    """Return the publication-feed URL for one Congress, e.g. 119."""

    if congress <= 0:
        raise DocumentPopulationError(f"congress must be a positive number, got {congress}")
    return CBO_PER_CONGRESS_FEED_URL_TEMPLATE.format(congress=congress)


def is_bot_challenge(payload: bytes) -> bool:
    """True if the payload is an edge bot-challenge body rather than publisher content."""

    lowered = payload.lower()
    return any(marker in lowered for marker in CHALLENGE_MARKERS)


def parse_cbo_publication_feed(payload: bytes) -> list[dict]:
    """Parse one CBO per-Congress publication feed into one record per publication.

    ``bill_number`` is the one optional field: CBO leaves it empty on procedural
    items such as weekly House suspension-calendar notices, so an empty element
    becomes ``None`` rather than drift. Everything else is required, and a
    repeated or non-conforming publication URL is refused — the URL is the
    population's identity, so a duplicate would silently collapse two documents
    into one.
    """

    if is_bot_challenge(payload):
        raise DocumentPopulationError("CBO returned a bot-challenge body, not the publication feed")
    root = _parse_xml(payload, what="CBO publication feed")
    if root.tag != "response":
        raise DocumentPopulationError(f"CBO publication feed root is <{root.tag}>, expected <response>")
    items = list(root)
    if not items:
        raise DocumentPopulationError("CBO publication feed carries no items")

    rows: list[dict] = []
    seen: set[str] = set()
    for ordinal, item in enumerate(items):
        if item.tag != "item":
            raise DocumentPopulationError(f"CBO feed item {ordinal} is <{item.tag}>, expected <item>")
        tags = tuple(child.tag for child in item)
        if tags != CBO_ITEM_TAGS:
            raise DocumentPopulationError(
                f"CBO feed item {ordinal} carries {list(tags)}, expected {list(CBO_ITEM_TAGS)}"
            )
        key = (item.attrib.get("key") or "").strip()
        if not key:
            raise DocumentPopulationError(f"CBO feed item {ordinal} is missing its key attribute")
        link = _required_text(item.findtext("Link"), f"CBO feed item {ordinal} Link")
        match = CBO_PUBLICATION_URL.fullmatch(link)
        if match is None:
            raise DocumentPopulationError(f"CBO feed item {ordinal} Link is not a CBO publication URL: {link!r}")
        if link in seen:
            raise DocumentPopulationError(f"CBO publication feed repeats publication {link}")
        seen.add(link)
        bill_number = (item.findtext("Bill_Number") or "").strip()
        rows.append(
            {
                "publication_id": match.group("publication_id"),
                "publication_url": link,
                "title": _required_text(item.findtext("Title"), f"CBO feed item {ordinal} Title"),
                "published": _required_text(item.findtext("Date"), f"CBO feed item {ordinal} Date"),
                "description": _required_text(item.findtext("Description"), f"CBO feed item {ordinal} Description"),
                "bill_number": bill_number or None,
                "feed_item_key": key,
            }
        )
    return rows


# --- GovInfo: package summary and PREMIS fixity -----------------------------

# GovInfo publishes a package summary and, separately, a PREMIS preservation
# record carrying the official SHA-256 of each file it distributes. The CFR
# granule reader in spicy_regs.sources.cfr_sections walks *inside* a package;
# these two read the package itself, which is where the publisher states the
# digest a downloaded volume must hash to.
GOVINFO_PACKAGE_SUMMARY_URL_TEMPLATE = "https://api.govinfo.gov/packages/{package_id}/summary"
GOVINFO_PACKAGE_PREMIS_URL_TEMPLATE = "https://api.govinfo.gov/packages/{package_id}/premis"

GOVINFO_CFR_PACKAGE_ID = re.compile(r"^CFR-\d{4}-title\d{1,3}-vol\d{1,3}$")

# The six rendition roles GovInfo publishes for a CFR annual-edition package.
GOVINFO_DOWNLOAD_ROLES = frozenset({"premisLink", "xmlLink", "txtLink", "zipLink", "modsLink", "pdfLink"})

_PREMIS_NS = "info:lc/xmlns/premis-v2"
_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def _premis(tag: str) -> str:
    return f"{{{_PREMIS_NS}}}{tag}"


def govinfo_package_summary_url(package_id: str) -> str:
    """Return the summary URL for one GovInfo package."""

    return GOVINFO_PACKAGE_SUMMARY_URL_TEMPLATE.format(package_id=package_id)


def govinfo_package_premis_url(package_id: str) -> str:
    """Return the PREMIS fixity URL for one GovInfo package."""

    return GOVINFO_PACKAGE_PREMIS_URL_TEMPLATE.format(package_id=package_id)


def parse_govinfo_cfr_package_summary(payload: bytes) -> dict:
    """Parse one GovInfo CFR package summary into its identity and version fields.

    Only CFR annual-edition packages are certified: other GovInfo collections
    publish a different summary shape, so a non-CFR ``collectionCode`` is
    refused rather than half-parsed.
    """

    try:
        root = json.loads(payload)
    except ValueError as error:
        raise DocumentPopulationError(f"GovInfo package summary is not valid JSON: {error}") from error
    if not isinstance(root, Mapping):
        raise DocumentPopulationError("GovInfo package summary is not a JSON object")

    package_id = _required_text(root.get("packageId"), "GovInfo packageId")
    if GOVINFO_CFR_PACKAGE_ID.fullmatch(package_id) is None:
        raise DocumentPopulationError(f"GovInfo packageId is not a CFR package identifier: {package_id!r}")
    collection_code = _required_text(root.get("collectionCode"), "GovInfo collectionCode")
    if collection_code != "CFR":
        raise DocumentPopulationError(f"this parser certifies collectionCode CFR only, got {collection_code!r}")

    part_range = root.get("partRange")
    if not isinstance(part_range, Mapping) or set(part_range) != {"from", "to"}:
        raise DocumentPopulationError("GovInfo partRange drifted from {'from', 'to'}")

    download = root.get("download")
    if not isinstance(download, Mapping) or set(download) != GOVINFO_DOWNLOAD_ROLES:
        raise DocumentPopulationError("GovInfo download drifted from the six known rendition roles")
    links: dict[str, str] = {}
    for role, value in download.items():
        link = _required_text(value, f"GovInfo download.{role}")
        if urlsplit(link).hostname != "api.govinfo.gov" or package_id not in link:
            raise DocumentPopulationError(f"GovInfo download.{role} does not name {package_id} on api.govinfo.gov")
        links[str(role)] = link

    counts: dict[str, str] = {}
    for field in ("pages", "titleNumber", "volumeCount"):
        text = _required_text(root.get(field), f"GovInfo {field}")
        if not text.isdigit():
            raise DocumentPopulationError(f"GovInfo {field} must be a decimal digit string, got {text!r}")
        counts[field] = text

    return {
        "package_id": package_id,
        "collection_code": collection_code,
        "collection_name": _required_text(root.get("collectionName"), "GovInfo collectionName"),
        "title": _required_text(root.get("title"), "GovInfo title"),
        "title_number": counts["titleNumber"],
        "part_from": _required_text(part_range["from"], "GovInfo partRange.from"),
        "part_to": _required_text(part_range["to"], "GovInfo partRange.to"),
        "date_issued": _required_text(root.get("dateIssued"), "GovInfo dateIssued"),
        "last_modified": _required_text(root.get("lastModified"), "GovInfo lastModified"),
        "doc_class": _required_text(root.get("docClass"), "GovInfo docClass"),
        "document_type": _required_text(root.get("documentType"), "GovInfo documentType"),
        "category": _required_text(root.get("category"), "GovInfo category"),
        "publisher": _required_text(root.get("publisher"), "GovInfo publisher"),
        "sudoc_class_number": _required_text(root.get("suDocClassNumber"), "GovInfo suDocClassNumber"),
        "pages": counts["pages"],
        "volume_count": counts["volumeCount"],
        "details_url": _required_text(root.get("detailsLink"), "GovInfo detailsLink"),
        "granules_url": _required_text(root.get("granulesLink"), "GovInfo granulesLink"),
        "download_urls_json": json.dumps(links, sort_keys=True),
    }


def parse_govinfo_package_fixity(payload: bytes, *, package_id: str) -> list[dict]:
    """Parse a package's PREMIS record into the publisher's own SHA-256 file digests.

    A ``file`` object without a ``fixity`` element is skipped: GovInfo computes
    fixity for only a subset of a package's objects. A fixity element with any
    other algorithm is drift — nothing but SHA-256 has been observed here — and
    so is a digest that is not 64 lowercase hex characters, an ``originalName``
    that does not belong to ``package_id``, or a content location outside
    ``www.govinfo.gov``.
    """

    root = _parse_xml(payload, what="GovInfo PREMIS record")
    if root.tag != _premis("premis"):
        raise DocumentPopulationError("GovInfo PREMIS root element is not a premis-v2 <premis> document")

    rows: list[dict] = []
    seen: set[str] = set()
    for obj in root.findall(_premis("object")):
        if obj.get(f"{{{_XSI_NS}}}type") != "file":
            continue
        fixity = obj.find(f"{_premis('objectCharacteristics')}/{_premis('fixity')}")
        if fixity is None:
            continue

        identifier_type = obj.findtext(f"{_premis('objectIdentifier')}/{_premis('objectIdentifierType')}")
        if identifier_type != "FDsys ACP":
            raise DocumentPopulationError(f"PREMIS objectIdentifierType is {identifier_type!r}, expected 'FDsys ACP'")
        object_id = _required_text(
            obj.findtext(f"{_premis('objectIdentifier')}/{_premis('objectIdentifierValue')}"),
            "PREMIS objectIdentifierValue",
        )
        if object_id in seen:
            raise DocumentPopulationError(f"PREMIS record repeats objectIdentifierValue {object_id!r}")
        seen.add(object_id)

        algorithm = fixity.findtext(_premis("messageDigestAlgorithm"))
        if algorithm != "SHA-256":
            raise DocumentPopulationError(f"PREMIS object {object_id} uses fixity algorithm {algorithm!r}")
        digest = (fixity.findtext(_premis("messageDigest")) or "").strip().lower()
        if _HEX64.fullmatch(digest) is None:
            raise DocumentPopulationError(f"PREMIS object {object_id} has a malformed SHA-256 digest")

        original_name = _required_text(obj.findtext(_premis("originalName")), f"PREMIS object {object_id} originalName")
        if not original_name.startswith(package_id):
            raise DocumentPopulationError(f"PREMIS originalName {original_name!r} does not belong to {package_id!r}")

        storage = f"{_premis('storage')}/{_premis('contentLocation')}"
        location_type = obj.findtext(f"{storage}/{_premis('contentLocationType')}")
        if location_type != "URI":
            raise DocumentPopulationError(f"PREMIS object {object_id} contentLocationType is {location_type!r}")
        # GovInfo prefixes the location with a human label ("Public Access
        # Rendition https://..."); the URI is the final whitespace-separated token.
        location = _required_text(
            obj.findtext(f"{storage}/{_premis('contentLocationValue')}"),
            f"PREMIS object {object_id} contentLocationValue",
        ).rsplit(" ", 1)[-1]
        parsed = urlsplit(location)
        if parsed.scheme != "https" or parsed.hostname != "www.govinfo.gov":
            raise DocumentPopulationError(f"PREMIS object {object_id} content location is not a govinfo.gov URL")

        size = obj.findtext(f"{_premis('objectCharacteristics')}/{_premis('size')}")
        media_type = obj.findtext(
            f"{_premis('objectCharacteristics')}/{_premis('format')}"
            f"/{_premis('formatDesignation')}/{_premis('formatName')}"
        )
        rows.append(
            {
                "package_id": package_id,
                "object_identifier": object_id,
                "original_name": original_name,
                "media_type": media_type,
                "byte_length": int(size) if size is not None and size.strip().isdigit() else None,
                "bytes_digest": f"sha256:{digest}",
                "content_url": location,
            }
        )
    if not rows:
        raise DocumentPopulationError(f"PREMIS record for {package_id} publishes no SHA-256 fixity")
    return rows


def _required_text(value: object, what: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DocumentPopulationError(f"{what} must be non-empty text")
    return value.strip()

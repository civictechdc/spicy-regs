"""Contract tests for the Docling Office parser adapter.

Docling is the v3 parser of last resort: it runs only where a source publishes no
better native XML, HTML, or JSON structure. These tests hold the adapter to the
boundary the design draws around it — provider objects enter, project-owned
records come out, and nothing else in the tree imports the package.

Every test here runs with Docling *uninstalled*, exactly as a plain ``uv sync``
environment has it, and drives the adapter with injected stand-ins. The loaded
converter, the real content-layer enum, and real table, formula, and speaker-note
behavior are covered against the pinned releases in
``tests/test_docpipeline_adapter_docling_real.py``::

    uv run --frozen --extra docling pytest tests/test_docpipeline_adapter_docling_real.py
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import math
import sys
import time
from collections.abc import Callable, Iterator
from importlib import import_module
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from spicy_regs.docpipeline.adapters import docling as adapter_module
from spicy_regs.docpipeline.adapters.docling import (
    ACCEPTED_CONVERSION_STATUSES,
    ADAPTER_MAPPING_REVISION,
    CONTENT_FROM_ORIG,
    CONTENT_FROM_TABLE_CELLS,
    CONTENT_FROM_TEXT,
    CONTENT_LAYERS,
    CONVERSION_STATUSES,
    COORDINATE_ORIGINS,
    DEFERRED_FORMATS,
    DOC_ITEM_LABELS,
    DOCLING_CORE_PACKAGE,
    DOCLING_CORE_VERSION,
    DOCLING_PACKAGE,
    DOCLING_VERSION,
    ELEMENT_SEPARATOR,
    FALLBACK_ERROR_TYPE,
    FORMAT_DOCX,
    FORMAT_IMAGE,
    FORMAT_PDF,
    FORMAT_PPTX,
    FORMAT_UNKNOWN,
    FORMAT_XLSX,
    HEADING_KINDS,
    MAX_CAPTION_REFS_PER_ITEM,
    MAX_CELLS_PER_TABLE,
    MAX_ERROR_TYPE_CHARS,
    MAX_HEADING_LEVEL,
    MAX_MAPPED_CHARACTERS,
    MAX_PAGE_NUMBER,
    MAX_PROVENANCE_CHARACTER_INDEX,
    MAX_PROVIDER_ERRORS,
    MAX_REFERENCE_CHARS,
    MAX_REGIONS_PER_ITEM,
    MAX_TABLE_CELL_CHARACTERS,
    MAX_TABLE_DIMENSION,
    MAX_TOTAL_CAPTION_REFS,
    MAX_TOTAL_REGIONS,
    MAX_TOTAL_TABLE_CELLS,
    MAX_TREE_DEPTH,
    NO_CONTENT,
    NO_COORDINATES,
    PARSED_TEXT_OFFSETS,
    PARSER_EVIDENCE_GRADE,
    PARSER_PAGE_COORDINATES,
    PIPELINE_SIMPLE,
    PROVIDER_INPUT_FORMATS,
    SOURCE_EXACT_EVIDENCE_GRADE,
    SOURCE_NAME_ENCODING,
    SUPPORTED_FORMATS,
    TABLE_KINDS,
    TABLE_SERIALIZATION,
    DoclingConfigurationError,
    DoclingDocumentParser,
    DoclingParseError,
    DoclingUnavailableError,
    DocumentParser,
    OffsetSemantics,
    PageRegion,
    ParsedDocument,
    ParsedDocumentResult,
    ParsedElement,
    ParsedTable,
    ParsedTableCell,
    ParserCall,
    bounded_error_type,
    encoded_source_name,
    installed_package_version,
    sanitized_source_name,
)

OMITTED = object()
"""Marks a provider attribute a real Docling item would simply not have."""


# --- provider stand-ins ----------------------------------------------------


class FakeEnum:
    """Provider enum stand-in: the adapter must read ``.value``, never ``str()``.

    ``docling`` labels, statuses, categories, content layers, and coordinate
    origins are all ``str`` enums whose ``str()`` is ``ClassName.MEMBER``. A
    record that carried that string would leak provider type names into project
    data.
    """

    def __init__(self, value: str) -> None:
        self.value = value

    def __str__(self) -> str:
        return f"DocItemLabel.{self.value.upper()}"


class FakeBoundingBox:
    """Docling ``BoundingBox`` stand-in with its four page-space edges.

    ``origin=OMITTED`` models a rectangle carrying no ``coord_origin`` at all,
    which says nothing about which corner its numbers count from.
    """

    def __init__(
        self,
        left: Any = 72.0,
        top: Any = 700.5,
        right: Any = 540.25,
        bottom: Any = 688.0,
        origin: Any = "BOTTOMLEFT",
    ) -> None:
        setattr(self, "l", left)
        self.t = top
        self.r = right
        self.b = bottom
        if origin is not OMITTED:
            self.coord_origin: Any = FakeEnum(origin) if isinstance(origin, str) else origin


class FakeProvenance:
    """Docling ``ProvenanceItem`` stand-in: page, rectangle, and char span."""

    def __init__(self, page_no: Any = 1, bbox: Any = OMITTED, charspan: Any = (0, 5)) -> None:
        self.page_no = page_no
        self.bbox = FakeBoundingBox() if bbox is OMITTED else bbox
        self.charspan = charspan


class FakeRef:
    """Docling ``RefItem`` stand-in: an opaque in-document reference."""

    def __init__(self, cref: str) -> None:
        self.cref = cref


class FakeTableCell:
    """docling-core ``TableCell`` stand-in: the only place a table's values live.

    Both descriptions of one cell's shape are present, exactly as the pinned
    release has them: half-open grid offsets and a row/column span. Either span
    may be ``OMITTED``, modelling a cell that declares no span attribute at all —
    which the pinned ``TableCell`` never does, since both fields carry a default.
    """

    def __init__(
        self,
        row: Any = 0,
        column: Any = 0,
        text: Any = "",
        row_span: Any = 1,
        col_span: Any = 1,
        column_header: Any = False,
        row_header: Any = False,
        row_end: Any = OMITTED,
        col_end: Any = OMITTED,
    ) -> None:
        self.start_row_offset_idx = row
        self.start_col_offset_idx = column
        self.end_row_offset_idx = (row + (1 if row_span is OMITTED else row_span)) if row_end is OMITTED else row_end
        self.end_col_offset_idx = (column + (1 if col_span is OMITTED else col_span)) if col_end is OMITTED else col_end
        if row_span is not OMITTED:
            self.row_span = row_span
        if col_span is not OMITTED:
            self.col_span = col_span
        self.text = text
        # ``Any`` on purpose: the pinned release declares these ``bool``, and the
        # adapter must refuse a value that merely looks true rather than coerce it.
        self.column_header: Any = column_header
        self.row_header: Any = row_header


class FakeTableData:
    """docling-core ``TableData`` stand-in: counts plus cells, and no ``text``."""

    def __init__(self, cells: Any, rows: Any = 2, columns: Any = 2) -> None:
        self.table_cells = cells
        self.num_rows = rows
        self.num_cols = columns


class FakeItem:
    """Docling document-item stand-in exposing only the surface the adapter reads.

    ``text=OMITTED`` models a ``PictureItem`` or a ``TableItem``, neither of which
    carries a ``text`` attribute at all; ``data`` models a table's cells and
    ``orig`` the pre-normalization source docling-core keeps beside ``text``.
    """

    def __init__(
        self,
        *,
        label: Any = "text",
        text: Any = "",
        orig: Any = OMITTED,
        data: Any = OMITTED,
        captions: Any = OMITTED,
        self_ref: Any = "#/texts/0",
        parent: Any = "#/body",
        prov: Any = None,
        level: int | None = None,
        content_layer: Any = "body",
    ) -> None:
        self.label = FakeEnum(label) if isinstance(label, str) else label
        self.content_layer = FakeEnum(content_layer) if isinstance(content_layer, str) else content_layer
        if text is not OMITTED:
            self.text = text
        if orig is not OMITTED:
            self.orig = orig
        if data is not OMITTED:
            self.data = data
        if captions is not OMITTED:
            # ``Any`` on purpose: a malformed provider may hand back anything here.
            self.captions: Any = [FakeRef(one) for one in captions]
        self.self_ref = self_ref
        if parent is not OMITTED:
            self.parent = FakeRef(parent) if isinstance(parent, str) else parent
        self.prov = [FakeProvenance()] if prov is None else prov
        if level is not None:
            self.level = level


class FakeDoclingDocument:
    """``DoclingDocument`` stand-in yielding (item, tree level) in reading order."""

    def __init__(self, items: list[Any], pages: Any = None) -> None:
        self.items = items
        self.pages = {1: object(), 2: object()} if pages is None else pages
        self.iterate_calls: list[dict[str, Any]] = []

    def iterate_items(self, **options: Any) -> Iterator[tuple[Any, int]]:
        self.iterate_calls.append(dict(options))
        return iter([(item, 1) for item in self.items])


class FakeErrorItem:
    """``ErrorItem`` stand-in: a closed category beside a message we never copy."""

    def __init__(self, category: str = "policy", message: str = "boom /secret/scan.docx") -> None:
        self.category = FakeEnum(category)
        self.error_message = message


class FakeConversion:
    """``ConversionResult`` stand-in: status, errors, format, and the document.

    ``provider_format`` defaults to the format the fixtures really declare, the
    way a real success always reports the backend that ran. A test that wants the
    missing-format condition asks for it explicitly.
    """

    def __init__(
        self,
        document: Any,
        *,
        status: str = "success",
        errors: tuple[Any, ...] = (),
        provider_format: str | None = FORMAT_DOCX,
    ) -> None:
        self.status = FakeEnum(status)
        self.document = document
        self.errors = list(errors)
        self.input = SimpleNamespace(format=FakeEnum(provider_format) if provider_format is not None else None)


class FakeConverter:
    """``DocumentConverter`` stand-in that records exactly what it was handed."""

    def __init__(self, conversion: Any = None, *, error: BaseException | None = None) -> None:
        self.conversion = conversion
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def convert(self, source: Any, **options: Any) -> Any:
        # Read the handed file while it still exists: the adapter deletes its
        # temporary directory as soon as the call returns.
        path = Path(str(source))
        self.calls.append({"name": path.name, "content": path.read_bytes(), "options": dict(options)})
        if self.error is not None:
            raise self.error
        return self.conversion


# --- hermetic environment --------------------------------------------------

BLOCKED_PACKAGES = ("docling", "docling_core")


def blocked_package(name: str) -> bool:
    return any(name == blocked or name.startswith(f"{blocked}.") for blocked in BLOCKED_PACKAGES)


class BlockedImportFinder:
    """Meta-path finder that fails any import of the blocked packages."""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if blocked_package(fullname):
            raise ImportError(f"{fullname} is deliberately unavailable in these tests")
        return None


def uninstalled_distribution(package: str) -> str:
    raise PackageNotFoundError(package)


@pytest.fixture(autouse=True)
def docling_uninstalled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run every test in this file without Docling installed.

    The adapter promises that an injected converter needs neither the Docling
    packages nor their metadata. That is a property of the whole file, not of one
    test that remembers to check it: the packages are made unimportable and every
    distribution reports itself absent, so tests inject a version reader wherever
    the pin check is not itself the subject.
    """
    for name in [name for name in sys.modules if blocked_package(name)]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "meta_path", [BlockedImportFinder(), *sys.meta_path])
    monkeypatch.setattr(adapter_module, "version", uninstalled_distribution)


def fake_version_reader(
    docling: str | None = DOCLING_VERSION,
    core: str | None = DOCLING_CORE_VERSION,
) -> Callable[[str], str | None]:
    """Resolve versions from a fake installation instead of the real environment."""

    def read(package: str) -> str | None:
        return {DOCLING_PACKAGE: docling, DOCLING_CORE_PACKAGE: core}.get(package)

    return read


# --- fixtures --------------------------------------------------------------

TITLE_TEXT = "Water Quality Rule"
PAGE_HEADER_TEXT = "EPA-HQ-OW-2026-0001"
SCOPE_HEADING = "§ 1.1 Scope"
SCOPE_BODY = "This part applies to  discharges—including stormwater.\nSee § 1.2."
TABLE_CAPTION = "Table 1. Effluent limits"
TABLE_TEXT = "Pollutant\tLimit\nBOD5\t30 mg/L"
FORMULA_SOURCE = "C = m / V"
LIMITS_HEADING = "§ 1.2 Limits"
LIMITS_BODY = "Effluent limits are 30 mg/L."
SPEAKER_NOTE = "Speaker note: confirm the 30 mg/L figure with OW."

SOURCE_BYTES = b"PK\x03\x04fixture office bytes\n"
SOURCE_NAME = "epa-2026-0001.docx"
SOURCE_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
SOURCE_SHA256 = hashlib.sha256(SOURCE_BYTES).hexdigest()

TABLE_CELLS = (
    FakeTableCell(row=0, column=0, text="Pollutant", column_header=True),
    FakeTableCell(row=0, column=1, text="Limit", column_header=True),
    FakeTableCell(row=1, column=0, text="BOD5"),
    FakeTableCell(row=1, column=1, text="30 mg/L"),
)


def sample_items() -> list[FakeItem]:
    """Eleven items in reading order, spanning three content layers."""
    return [
        FakeItem(
            label="title",
            text=TITLE_TEXT,
            self_ref="#/texts/0",
            prov=[FakeProvenance(page_no=1, charspan=(0, len(TITLE_TEXT)))],
        ),
        # Furniture: a page header Docling's own default would never return.
        FakeItem(label="page_header", text=PAGE_HEADER_TEXT, self_ref="#/texts/1", content_layer="furniture", prov=[]),
        FakeItem(label="section_header", text=SCOPE_HEADING, self_ref="#/texts/2", level=1, prov=[]),
        FakeItem(
            label="text",
            text=SCOPE_BODY,
            self_ref="#/texts/3",
            prov=[FakeProvenance(page_no=1), FakeProvenance(page_no=2, bbox=FakeBoundingBox(top=120.0))],
        ),
        FakeItem(label="caption", text=TABLE_CAPTION, self_ref="#/texts/4", prov=[FakeProvenance(page_no=2)]),
        FakeItem(
            label="table",
            text=OMITTED,
            data=FakeTableData(TABLE_CELLS),
            captions=["#/texts/4"],
            self_ref="#/tables/0",
            prov=[FakeProvenance(page_no=2)],
        ),
        # A formula whose text was emptied still carries its source in ``orig``.
        FakeItem(label="formula", text="", orig=FORMULA_SOURCE, self_ref="#/texts/5", prov=[]),
        FakeItem(label="picture", text=OMITTED, captions=[], self_ref="#/pictures/0", prov=[FakeProvenance(page_no=2)]),
        FakeItem(label="section_header", text=LIMITS_HEADING, self_ref="#/texts/6", level=1, prov=[]),
        FakeItem(label="text", text=LIMITS_BODY, self_ref="#/texts/7", prov=[FakeProvenance(page_no=2)]),
        # Notes: PowerPoint speaker notes and Word comments live here.
        FakeItem(label="text", text=SPEAKER_NOTE, self_ref="#/texts/8", content_layer="notes", prov=[]),
    ]


EXPECTED_TEXT = ELEMENT_SEPARATOR.join(
    [
        TITLE_TEXT,
        PAGE_HEADER_TEXT,
        SCOPE_HEADING,
        SCOPE_BODY,
        TABLE_CAPTION,
        TABLE_TEXT,
        FORMULA_SOURCE,
        LIMITS_HEADING,
        LIMITS_BODY,
        SPEAKER_NOTE,
    ]
)


def parser(items: list[Any] | None = None, **overrides: Any) -> DoclingDocumentParser:
    settings: dict[str, Any] = {
        "version_reader": fake_version_reader(),
        "converter": FakeConverter(FakeConversion(FakeDoclingDocument(sample_items() if items is None else items))),
    }
    settings.update(overrides)
    return DoclingDocumentParser(**settings)


def parse(items: list[Any] | None = None, **overrides: Any) -> ParsedDocumentResult:
    return parser(items, **overrides).parse(SOURCE_BYTES, source_name=SOURCE_NAME, media_type=SOURCE_MEDIA_TYPE)


def element(document: ParsedDocument, parser_ref: str) -> ParsedElement:
    return next(one for one in document.elements if one.parser_ref == parser_ref)


# --- pinned contract -------------------------------------------------------


def test_module_pins_both_selected_releases_and_never_claims_source_evidence() -> None:
    assert (DOCLING_PACKAGE, DOCLING_VERSION) == ("docling", "2.115.0")
    assert (DOCLING_CORE_PACKAGE, DOCLING_CORE_VERSION) == ("docling-core", "2.87.1")
    assert PARSER_EVIDENCE_GRADE != SOURCE_EXACT_EVIDENCE_GRADE
    assert dataclasses.astuple(PARSED_TEXT_OFFSETS) == ("adapter-parsed-text", "unicode-codepoints", "half-open")
    # The mapping revision is named in every ``parser_id``, so what a record claims
    # about the mapping it was produced under is only true while this is bumped
    # whenever the accepted scalars or the failure records change. Asserted
    # exactly, so a change to either is a deliberate edit here rather than a
    # silent one there.
    assert ADAPTER_MAPPING_REVISION == "office-mapping-6"


@pytest.mark.parametrize(
    ("docling_version", "core_version"),
    [(DOCLING_VERSION, "2.86.0"), ("2.114.0", DOCLING_CORE_VERSION), (None, None)],
)
def test_parser_verifies_both_installed_versions(docling_version: str | None, core_version: str | None) -> None:
    assert parser(version_reader=fake_version_reader()).package_version == DOCLING_VERSION

    with pytest.raises(DoclingUnavailableError, match="differs from the pinned contract"):
        parser(version_reader=fake_version_reader(docling_version, core_version))

    # The pin is read from an installation, never declared by a caller, and
    # nothing is installed here — so the default reader refuses. An injected
    # converter records the version of an installation; it never stands in for one.
    signature = inspect.signature(DoclingDocumentParser)
    assert signature.parameters["version_reader"].default is installed_package_version
    assert "package_version" not in signature.parameters
    assert installed_package_version(DOCLING_PACKAGE) is None
    with pytest.raises(DoclingUnavailableError, match="differs from the pinned contract"):
        parser(version_reader=installed_package_version)


class ForgedVersion(str):
    """A version that answers comparison, formatting, conversion, and length itself.

    ``version_reader`` is an injected callable, so what it hands back is untrusted
    like any other boundary value. A ``str`` subclass defines ``__eq__`` and
    ``__ne__``, so it can declare itself equal to both pins; it defines
    ``__format__``, and the parser identity is built by interpolation.
    """

    touched: list[str] = []

    def __eq__(self, other: Any) -> bool:
        type(self).touched.append("eq")
        return True

    def __ne__(self, other: Any) -> bool:
        type(self).touched.append("ne")
        return False

    def __hash__(self) -> int:
        type(self).touched.append("hash")
        return 0

    def __format__(self, specification: str) -> str:
        del specification
        type(self).touched.append("format")
        return "leaked AKIAIOSFODNN7EXAMPLE /var/secrets/scan.docx " + "X" * 10_000

    def __str__(self) -> str:
        type(self).touched.append("str")
        return "leaked AKIAIOSFODNN7EXAMPLE " + "X" * 10_000

    def __len__(self) -> int:
        type(self).touched.append("len")
        return 1


@pytest.mark.parametrize("position", ["docling", "core", "both"])
def test_a_version_reader_returning_a_str_subtype_cannot_bypass_either_pin(position: str) -> None:
    """The pin is only a pin if the value it compares is really a built-in string.

    A subtype declaring itself equal walked through both comparisons, stayed in
    both recorded version fields, and then wrote whatever its ``__format__``
    chose into ``parser_id``. Because the pin is refused, no parser exists to
    carry it: there is no identity to taint and no record to enter.
    """
    ForgedVersion.touched = []
    forged = ForgedVersion("9.9.9")
    versions = {
        "docling": (forged, DOCLING_CORE_VERSION),
        "core": (DOCLING_VERSION, forged),
        "both": (forged, forged),
    }[position]
    reader = fake_version_reader(*versions)

    with pytest.raises(DoclingUnavailableError, match="differs from the pinned contract") as failure:
        parser(version_reader=reader)
    # The helper itself, not only the parser that calls it.
    with pytest.raises(DoclingUnavailableError, match="differs from the pinned contract"):
        adapter_module.require_pinned_docling_versions(reader)
    # And the pin is refused *before* anything would reach the provider: with no
    # converter injected, a bypass would go on to import Docling, which is
    # unavailable here and would fail differently.
    with pytest.raises(DoclingUnavailableError, match="differs from the pinned contract"):
        DoclingDocumentParser(version_reader=reader)

    message = str(failure.value)
    assert len(message) < 200
    for fragment in ("leaked", "AKIA", "secrets", "scan.docx", "XXXXXXXXXX", "9.9.9"):
        assert fragment not in message
    # And nothing the subtype defines ever ran: the type is settled before any
    # comparison, formatting, conversion, hash, or length touches the value.
    assert ForgedVersion.touched == []


def test_the_recorded_versions_are_this_adapters_own_pinned_constants() -> None:
    """What is persisted is the contract, not whatever the reader handed back.

    Once both pins verify, the two values are known exactly — so the record
    carries this module's own constants and never keeps a caller's object alive
    inside a published receipt.
    """
    equal = {DOCLING_PACKAGE: "".join(DOCLING_VERSION), DOCLING_CORE_PACKAGE: "".join(DOCLING_CORE_VERSION)}
    # The probe is only a probe if these really are distinct objects.
    assert equal[DOCLING_PACKAGE] is not DOCLING_VERSION
    assert equal[DOCLING_CORE_PACKAGE] is not DOCLING_CORE_VERSION

    built = parser(version_reader=equal.get)

    assert built.package_version is DOCLING_VERSION
    assert built.core_package_version is DOCLING_CORE_VERSION
    call = built.parse(SOURCE_BYTES, source_name=SOURCE_NAME).call
    assert call.package_version is DOCLING_VERSION
    assert call.core_package_version is DOCLING_CORE_VERSION


# --- a version reader that raises -------------------------------------------
#
# ``version_reader`` is an injected callable, so *raising* is a shape it can take
# like any value it returns. It runs before a parser exists — before an identity,
# a policy, or a record — so whatever comes out of it escapes as itself unless the
# lookup is normalized where it happens. ``forged_refusal``, ``hostile_exception``
# and ``SECRET_FRAGMENTS`` are defined further down, beside the other tests about
# exceptions this module must never read anything off.


def exploding_version_reader(error: BaseException, *, at: str) -> Callable[[str], str | None]:
    """A reader that raises for one package and answers the pin for the other.

    Answering the pin for the other package is what makes the position meaningful:
    one catch wrapped around the whole loop would pass for the first package and
    fail for the second, so both are driven.
    """
    installed = {DOCLING_PACKAGE: DOCLING_VERSION, DOCLING_CORE_PACKAGE: DOCLING_CORE_VERSION}

    def read(package: str) -> str | None:
        if package == at:
            raise error
        return installed[package]

    return read


VERSION_READER_FAILURES = [
    # Ordinary callback failures, each carrying text no adapter message may repeat.
    ("runtime", lambda: RuntimeError(SECRET_REFUSAL_MESSAGE)),
    ("lookup", lambda: KeyError(SECRET_REFUSAL_MESSAGE)),
    ("value", lambda: ValueError(SECRET_REFUSAL_MESSAGE)),
    # The metadata error the default reader handles inside itself, raised by an
    # injected one instead — where nothing was catching it.
    ("package-not-found", lambda: PackageNotFoundError(SECRET_REFUSAL_MESSAGE)),
    # This module's own private refusals, exactly, built the way provider code
    # can: a code this adapter really declares, and open-ended attributes beside it.
    ("forged-refusal", lambda: forged_refusal_with_code("refusal", "no_iterate_items")),
    ("forged-limit", lambda: forged_refusal_with_code("limit", "item_bound")),
    # And a dynamically named class, where the hostile part is the type itself.
    ("hostile-type", lambda: hostile_exception()),
]


@pytest.mark.parametrize("package", [DOCLING_PACKAGE, DOCLING_CORE_PACKAGE])
@pytest.mark.parametrize(
    ("name", "build"),
    VERSION_READER_FAILURES,
    ids=[one[0] for one in VERSION_READER_FAILURES],
)
def test_a_version_reader_that_raises_is_refused_in_this_adapters_own_words(
    package: str, name: str, build: Callable[[], BaseException]
) -> None:
    """A raised lookup is this adapter's finding, stated in this adapter's words.

    The callback's own exception used to escape with its own class and its own
    message — before a parser, an identity, or a record existed to hold anything
    to a contract. A forged private refusal escaping here is worse still: it names
    a code this module declares, so it would go on to select fixed limit text for
    a bound nothing reached.
    """
    del name
    reader = exploding_version_reader(build(), at=package)
    expected = f"{package} version could not be verified"

    for raise_it in (
        lambda: adapter_module.require_pinned_docling_versions(reader),
        lambda: parser(version_reader=reader),
        # With no converter injected, a bypass would go on to import Docling,
        # which is unavailable here and would fail differently.
        lambda: DoclingDocumentParser(version_reader=reader),
    ):
        with pytest.raises(DoclingUnavailableError) as failure:
            raise_it()

        # The exact public type and the exact fixed sentence, naming only this
        # module's own package constant.
        assert type(failure.value) is DoclingUnavailableError
        assert str(failure.value) == expected
        # And no receipt: the pin is checked before an identity can be built from
        # a caller's arguments, so there is no honest ``ParserCall`` to carry.
        assert not hasattr(failure.value, "call")
        # Nothing was read off the callback's exception — not its message, not its
        # type name, not an attribute it chose to carry.
        message = str(failure.value)
        assert len(message) < 200
        for fragment in (*SECRET_FRAGMENTS, "Secret" + "X" * 10, "PackageNotFound", "KeyError", "RuntimeError"):
            assert fragment not in message


def test_a_raised_version_lookup_is_a_different_finding_from_a_missing_or_wrong_one() -> None:
    """Three conditions, and the two already closed keep saying what they said."""

    def message_for(reader: Callable[[str], str | None]) -> str:
        with pytest.raises(DoclingUnavailableError) as failure:
            adapter_module.require_pinned_docling_versions(reader)
        return str(failure.value)

    raised = message_for(exploding_version_reader(RuntimeError("boom"), at=DOCLING_PACKAGE))
    missing = message_for(fake_version_reader(None, DOCLING_CORE_VERSION))
    wrong = message_for(fake_version_reader("2.114.0", DOCLING_CORE_VERSION))
    pinned = f"{DOCLING_PACKAGE} version differs from the pinned contract: {DOCLING_VERSION} is required"

    assert raised == f"{DOCLING_PACKAGE} version could not be verified"
    assert missing == pinned
    assert wrong == pinned
    assert raised != pinned
    # A reader that answers both pins is still the only way through.
    assert adapter_module.require_pinned_docling_versions(fake_version_reader()) == (
        DOCLING_VERSION,
        DOCLING_CORE_VERSION,
    )


# --- content preservation --------------------------------------------------


def test_parser_returns_exact_text_elements_and_call_record_together() -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    result = parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME, media_type=SOURCE_MEDIA_TYPE)

    assert isinstance(result, ParsedDocumentResult)
    assert isinstance(result.document, ParsedDocument)
    assert result.document.text == EXPECTED_TEXT
    assert result.document.source_sha256 == SOURCE_SHA256
    assert result.document.source_bytes == len(SOURCE_BYTES)
    assert result.document.input_format == FORMAT_DOCX
    assert result.document.evidence_grade == PARSER_EVIDENCE_GRADE
    assert result.document.offsets == PARSED_TEXT_OFFSETS
    assert [one.kind for one in result.document.elements] == [
        "title",
        "page_header",
        "section_header",
        "text",
        "caption",
        "table",
        "formula",
        "picture",
        "section_header",
        "text",
        "text",
    ]
    assert {one.parent_ref for one in result.document.elements} == {"#/body"}

    # SimplePipeline needs no page or file bound from the caller: the byte limit
    # is this adapter's own, enforced before anything is written.
    assert converter.calls == [{"name": "source.docx", "content": SOURCE_BYTES, "options": {"raises_on_error": False}}]

    call = result.call
    assert isinstance(call, ParserCall)
    assert (call.provider, call.operation) == ("docling", "document-parse")
    assert (call.package_name, call.package_version) == ("docling", DOCLING_VERSION)
    assert (call.core_package_name, call.core_package_version) == ("docling-core", DOCLING_CORE_VERSION)
    assert call.source_name == SOURCE_NAME
    assert call.source_name_sha256 == hashlib.sha256(SOURCE_NAME.encode()).hexdigest()
    assert call.source_name_sanitized is False
    assert (call.media_type, call.input_format) == (SOURCE_MEDIA_TYPE, FORMAT_DOCX)
    assert call.source_sha256 == SOURCE_SHA256
    assert call.conversion_status == "success"
    assert (call.provider_error_count, call.provider_error_categories) == (0, ())
    assert call.provider_input_format == FORMAT_DOCX
    assert call.page_count == 2
    assert (call.element_count, call.usable_element_count) == (11, 10)
    assert call.usable_character_count == sum(len(one.text) for one in result.document.elements)
    assert call.character_count == len(EXPECTED_TEXT)
    assert call.content_layers_present == ("body", "furniture", "notes")
    assert (call.table_count, call.table_cell_count) == (1, 4)
    assert (call.omission_count, call.omitted_kinds) == (1, ("picture",))
    assert call.elements_without_coordinates == 5
    assert call.coordinate_grade == PARSER_PAGE_COORDINATES
    assert call.evidence_grade == PARSER_EVIDENCE_GRADE
    assert (call.status, call.provider_invoked, call.attempt_count) == ("completed", True, 1)
    assert (call.failure_reason, call.error_type) == (None, None)
    assert isinstance(call.duration_ms, float)


def test_every_content_layer_is_requested_and_recorded_on_what_it_produced() -> None:
    document = FakeDoclingDocument(sample_items())

    result = parser(converter=FakeConverter(FakeConversion(document))).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    # Docling's default is ``body`` alone, which drops page headers and footers,
    # PowerPoint speaker notes, and Word comments before anything can record them.
    assert document.iterate_calls == [{"included_content_layers": set(CONTENT_LAYERS)}]
    assert set(CONTENT_LAYERS) == {"body", "furniture", "background", "invisible", "notes"}

    layers = {one.parser_ref: one.content_layer for one in result.document.elements}
    assert (layers["#/texts/1"], layers["#/texts/8"]) == ("furniture", "notes")
    assert element(result.document, "#/texts/1").text == PAGE_HEADER_TEXT
    assert element(result.document, "#/texts/8").text == SPEAKER_NOTE
    assert result.document.tables[0].content_layer == "body"


def test_an_element_from_a_layer_the_parser_did_not_request_fails_closed() -> None:
    with pytest.raises(DoclingParseError, match="content layer is not one this parser requested"):
        parse(items=[FakeItem(text="alpha", content_layer="speculative")])


def test_table_cell_values_survive_as_records_and_a_labeled_serialization() -> None:
    document = parse().document

    assert document.tables == (
        ParsedTable(
            parser_ref="#/tables/0",
            parent_ref="#/body",
            element_ordinal=5,
            content_layer="body",
            row_count=2,
            column_count=2,
            cells=(
                ParsedTableCell(0, 1, 0, 1, "Pollutant", True, False),
                ParsedTableCell(0, 1, 1, 2, "Limit", True, False),
                ParsedTableCell(1, 2, 0, 1, "BOD5", False, False),
                ParsedTableCell(1, 2, 1, 2, "30 mg/L", False, False),
            ),
            caption_refs=("#/texts/4",),
            serialization=TABLE_SERIALIZATION,
            serialization_ambiguous=False,
            regions=(document.elements[5].regions[0],),
            coordinate_grade=PARSER_PAGE_COORDINATES,
        ),
    )

    table = element(document, "#/tables/0")
    assert table.text == TABLE_TEXT
    assert "30 mg/L" in document.text
    # The text in the document is derived; the cells above are the provider's.
    assert table.content_source == CONTENT_FROM_TABLE_CELLS
    assert table.text_usable is True
    assert element(document, "#/texts/4").text == TABLE_CAPTION


def test_merged_cells_and_separator_bearing_text_keep_every_exact_value() -> None:
    merged = FakeItem(
        label="table",
        text=OMITTED,
        self_ref="#/tables/1",
        prov=[],
        data=FakeTableData(
            (
                FakeTableCell(row=0, column=0, text="Spanning header", col_span=2, column_header=True),
                FakeTableCell(row=1, column=0, text="Line\twith\ttabs"),
                FakeTableCell(row=1, column=1, text="two\nlines"),
            )
        ),
    )

    document = parse(items=[merged]).document

    table = document.tables[0]
    assert [(cell.row_start, cell.row_end, cell.column_start, cell.column_end, cell.text) for cell in table.cells] == [
        (0, 1, 0, 2, "Spanning header"),
        (1, 2, 0, 1, "Line\twith\ttabs"),
        (1, 2, 1, 2, "two\nlines"),
    ]
    # The merged cell writes once, at its top-left, and the rest of its span is
    # empty; no value is lost, and every cell survives verbatim in ``cells``.
    assert document.text == "Spanning header\t\nLine\twith\ttabs\ttwo\nlines"
    # The flat rendering cannot be split back into these cells; the cells can.
    assert table.serialization_ambiguous is True


@pytest.mark.parametrize(
    ("data", "message", "reason"),
    [
        # Two descriptions of one cell that do not agree.
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a", row_end=3),)),
            "spans disagree with its row and column offsets",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a", col_end=9),)),
            "spans disagree with its row and column offsets",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a", row_end=0),)),
            "table cell geometry is missing",
            "malformed_element",
        ),
        # A span the provider never declared. The pinned ``TableCell`` always
        # carries both, so defaulting an absent one to 1 invented the agreement
        # the check above exists to verify — and made a merged cell readable as
        # unmerged.
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a", row_span=OMITTED),)),
            "table cell geometry is missing",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a", col_span=OMITTED),)),
            "table cell geometry is missing",
            "malformed_element",
        ),
        # Two descriptions of one *table* that do not agree: a cell reaching past
        # the declared grid used to widen it silently, so the recorded row and
        # column counts described a shape the cells contradicted.
        (
            FakeTableData((FakeTableCell(row=1, column=0, text="a"),), rows=1, columns=1),
            "lies outside the declared row and column counts",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=2, text="a"),), rows=1, columns=2),
            "lies outside the declared row and column counts",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a"),), rows=0, columns=0),
            "lies outside the declared row and column counts",
            "malformed_element",
        ),
        # A grid that is empty in one dimension only. ``0xN`` and ``Nx0`` are not
        # grids any cell can sit in and not shapes this serialization can lay out,
        # and the area check alone waves both through: their product is zero.
        (
            FakeTableData((), rows=0, columns=5),
            "empty in one dimension only",
            "malformed_element",
        ),
        (
            FakeTableData((), rows=5, columns=0),
            "empty in one dimension only",
            "malformed_element",
        ),
        # The same declaration with a dimension large enough that the zero-product
        # would otherwise have hidden it entirely.
        (
            FakeTableData((), rows=0, columns=10**12),
            "empty in one dimension only",
            "malformed_element",
        ),
        # One declared dimension past its own bound, beside a small one. Bounding
        # only the product lets this through whenever the other dimension is 0 or
        # 1, and the declared counts are what size the serialization grid.
        (
            FakeTableData((), rows=MAX_TABLE_DIMENSION + 1, columns=1),
            "row or column count past the recorded dimension bound",
            "table_dimension_limit",
        ),
        (
            FakeTableData((), rows=1, columns=MAX_TABLE_DIMENSION + 1),
            "row or column count past the recorded dimension bound",
            "table_dimension_limit",
        ),
        # One position, two values. A shared top-left was already refused; these
        # are merged rectangles that overlap without sharing an anchor, which used
        # to pass and leave the grid claiming one position for two cells.
        (
            FakeTableData(
                (
                    FakeTableCell(row=0, column=0, text="wide", col_span=2),
                    FakeTableCell(row=0, column=1, text="collides"),
                ),
                rows=1,
                columns=2,
            ),
            "share one grid position",
            "malformed_element",
        ),
        (
            FakeTableData(
                (
                    FakeTableCell(row=0, column=0, text="tall", row_span=2),
                    FakeTableCell(row=1, column=0, text="collides"),
                ),
                rows=2,
                columns=1,
            ),
            "share one grid position",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a"), FakeTableCell(row=0, column=0, text="b"))),
            "share one grid position",
            "malformed_element",
        ),
        # A grid declared enormous and delivered empty: the area bound is applied
        # before the no-cells shortcut, so it cannot pass as a checked table. It is
        # this adapter's own bound, so the receipt names the bound rather than
        # calling the provider's output malformed.
        (
            FakeTableData((), rows=MAX_CELLS_PER_TABLE, columns=2),
            "exceeds the recorded per-table cell bound",
            "table_cell_limit",
        ),
        (
            FakeTableData((FakeTableCell(row=0, column=0, text="a"),), rows=MAX_CELLS_PER_TABLE, columns=2),
            "exceeds the recorded per-table cell bound",
            "table_cell_limit",
        ),
        # A table that never says what shape it is.
        (
            SimpleNamespace(table_cells=(FakeTableCell(row=0, column=0, text="a"),)),
            "table shape is missing or invalid",
            "malformed_element",
        ),
        (
            FakeTableData((FakeTableCell(text="a"),), rows=-1, columns=1),
            "table shape is missing or invalid",
            "malformed_element",
        ),
    ],
)
def test_a_table_shape_this_serialization_cannot_represent_fails_closed(data: Any, message: str, reason: str) -> None:
    with pytest.raises(DoclingParseError, match=message) as failure:
        parse(items=[FakeItem(label="table", text=OMITTED, data=data, self_ref="#/tables/3", prov=[])])

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", reason)


def test_a_declared_grid_is_either_wholly_empty_or_positive_in_both_dimensions() -> None:
    """``0x0`` is a real empty table; ``0xN`` and ``Nx0`` are not tables at all.

    Their product is zero, so an area bound alone accepts every one of them —
    including ``0 x 10**12``, which would then be recorded as a table this adapter
    had checked.
    """
    empty = parse(
        items=[
            caption_item(),
            FakeItem(
                label="table",
                text=OMITTED,
                data=FakeTableData((), rows=0, columns=0),
                self_ref="#/tables/0",
                prov=[],
            ),
        ]
    ).document

    assert (empty.tables[0].row_count, empty.tables[0].column_count) == (0, 0)
    assert empty.omissions[-1].reason == "empty-table"


# --- an item's label and its shape have to agree ----------------------------


def test_the_table_bearing_kinds_are_the_labels_the_pinned_table_item_may_carry() -> None:
    # ``TableItem.label`` is ``Literal[DOCUMENT_INDEX, TABLE]`` and ``TableItem``
    # is the only pinned item class declaring ``data: TableData``. A real-provider
    # test holds this set to that ``Literal``'s own members.
    assert TABLE_KINDS == {"table", "document_index"}
    assert TABLE_KINDS <= DOC_ITEM_LABELS


@pytest.mark.parametrize("kind", sorted(TABLE_KINDS))
def test_every_table_bearing_kind_is_mapped_as_a_table(kind: str) -> None:
    document = parse(
        items=[FakeItem(label=kind, text=OMITTED, data=FakeTableData(TABLE_CELLS), self_ref="#/tables/0", prov=[])]
    ).document

    assert (document.elements[0].kind, document.elements[0].content_source) == (kind, CONTENT_FROM_TABLE_CELLS)
    assert document.tables[0].parser_ref == document.elements[0].parser_ref
    assert [cell.text for cell in document.tables[0].cells] == ["Pollutant", "Limit", "BOD5", "30 mg/L"]
    assert document.text == TABLE_TEXT


@pytest.mark.parametrize("kind", sorted(TABLE_KINDS))
@pytest.mark.parametrize("data", [OMITTED, None, SimpleNamespace()])
def test_a_table_labeled_item_with_no_table_data_fails_closed(kind: str, data: Any) -> None:
    # The release blocker in its other direction: a ``TableItem`` has no ``.text``
    # at all, so a table-labelled item whose ``TableData`` cannot be read is a
    # table with every cell value missing. Mapping it as an ordinary element
    # records it as an omission and drops the grid without saying so.
    with pytest.raises(DoclingParseError, match="table-labeled item carries no table data") as failure:
        parse(items=[FakeItem(label=kind, text=OMITTED, data=data, self_ref="#/tables/0", prov=[])])

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


@pytest.mark.parametrize("kind", ["text", "picture", "caption", "form", "key_value_region", "chart"])
def test_a_non_table_kind_carrying_table_cells_fails_closed(kind: str) -> None:
    # No pinned item class other than ``TableItem`` declares ``data: TableData``,
    # so this is provider output the mapping was never written against. Reading it
    # as a table would publish a ``ParsedTable`` — with a serialization, a grid,
    # and cell records — for an item the provider never called a table.
    with pytest.raises(DoclingParseError, match="table data under a label that is not a table") as failure:
        parse(items=[FakeItem(label=kind, text=OMITTED, data=FakeTableData(TABLE_CELLS), self_ref="#/x/0", prov=[])])

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


def test_a_non_table_kind_without_table_cells_is_unaffected_by_the_agreement_check() -> None:
    # ``.data`` alone is not table data: the attribute the check turns on is
    # ``data.table_cells``, so an unrelated payload does not make an item a table
    # and does not fail an otherwise sound parse either.
    document = parse(
        items=[FakeItem(label="text", text="alpha", data=SimpleNamespace(), self_ref="#/texts/0")]
    ).document

    assert (document.tables, document.text) == ((), "alpha")


@pytest.mark.parametrize("field", ["column_header", "row_header"])
@pytest.mark.parametrize("value", [1, 0, "true", "", None, object()])
def test_a_header_flag_that_is_not_a_boolean_fails_closed(field: str, value: Any) -> None:
    # ``bool()`` on whatever arrives would read a missing field as "not a header"
    # and any non-empty provider object as "header", so a header row could be
    # recorded either way with nothing noticing.
    cells = (FakeTableCell(row=0, column=0, text="Pollutant", **{field: value}),)

    with pytest.raises(DoclingParseError, match=f"table cell {field} is not a boolean") as failure:
        parse(items=[FakeItem(label="table", text=OMITTED, data=FakeTableData(cells), self_ref="#/t/9", prov=[])])

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


def test_a_header_flag_the_provider_does_not_declare_at_all_fails_closed() -> None:
    naked = SimpleNamespace(
        start_row_offset_idx=0,
        end_row_offset_idx=1,
        start_col_offset_idx=0,
        end_col_offset_idx=1,
        row_span=1,
        col_span=1,
        text="Pollutant",
    )

    with pytest.raises(DoclingParseError, match="table cell column_header is not a boolean"):
        parse(items=[FakeItem(label="table", text=OMITTED, data=FakeTableData((naked,)), self_ref="#/t/8", prov=[])])


def test_header_flags_are_recorded_exactly_as_the_provider_declared_them() -> None:
    cells = (
        FakeTableCell(row=0, column=0, text="Pollutant", column_header=True, row_header=False),
        FakeTableCell(row=1, column=0, text="BOD5", column_header=False, row_header=True),
    )

    document = parse(
        items=[FakeItem(label="table", text=OMITTED, data=FakeTableData(cells, rows=2, columns=1), self_ref="#/t/7")]
    ).document

    flags = [(cell.column_header, cell.row_header) for cell in document.tables[0].cells]
    assert flags == [(True, False), (False, True)]
    # Exactly ``True`` and ``False``, not something that answers like them.
    assert all(type(one) is bool for pair in flags for one in pair)


class SpoofedBoolean:
    """An object that reports ``bool`` as its class without being one.

    ``isinstance(value, bool)`` takes the exact-type fast path first and then falls
    back to ``value.__class__`` — an ordinary attribute lookup provider code
    defines. One of these passed the header-flag check, was returned unchanged,
    and landed on a :class:`ParsedTableCell`, where a plain record is supposed to
    hold exact built-ins and strict JSON has no spelling for it at all.
    """

    @property
    def __class__(self) -> Any:
        return bool


class UnreadableBoolean:
    """A header flag whose ``__class__`` raises a refusal stating a declared code.

    Asking a provider object what class it is runs provider code, and a check that
    asks selects whatever that code raises. ``item_bound`` is a code this adapter
    declares, so what escaped chose fixed limit text and an ``item_limit`` receipt
    for a document of four cells. See the forged-refusal section below.
    """

    @property
    def __class__(self) -> Any:
        raise forged_refusal_with_code("limit", "item_bound")


@pytest.mark.parametrize("field", ["column_header", "row_header"])
@pytest.mark.parametrize("flag", [SpoofedBoolean, UnreadableBoolean], ids=["reports-bool", "raises-from-class"])
def test_a_header_flag_that_only_claims_to_be_a_boolean_never_reaches_a_record(field: str, flag: type) -> None:
    """The flag has to *be* a boolean, so nothing is asked and nothing can answer.

    ``type(value) is bool`` consults no attribute the provider defines, which
    settles both objects at once: the one that would have entered a record and
    strict JSON, and the one whose answer was a forged limit.
    """
    cells = (FakeTableCell(row=0, column=0, text="Pollutant", **{field: flag()}),)
    item = FakeItem(label="table", text=OMITTED, data=FakeTableData(cells), self_ref="#/tables/0", prov=[])

    with pytest.raises(DoclingParseError) as failure:
        parse(items=[item])

    call = cast(ParserCall, failure.value.call)
    message = str(failure.value)
    payload = json.dumps(call.as_json_dict(), allow_nan=False)

    # This adapter's own fixed text for the condition it really found, and the
    # stage that found it — never a bound nothing reached.
    assert message == f"docling table cell {field} is not a boolean"
    assert call.failure_reason == "malformed_element"
    assert call.failure_reason in RECORDED_FAILURE_REASONS
    for fragment in SECRET_FRAGMENTS:
        assert fragment not in message
        assert fragment not in payload
    # And the helper alone states the same code, without consulting the object.
    with pytest.raises(DoclingParseError) as direct:
        adapter_module._cell_flag(flag(), code=f"table_cell_{field}")
    assert adapter_module._refusal_code(direct.value) == f"table_cell_{field}"


def test_formula_source_is_read_from_orig_when_the_provider_empties_text() -> None:
    document = parse().document

    formula = element(document, "#/texts/5")
    assert formula.text == FORMULA_SOURCE
    assert formula.content_source == CONTENT_FROM_ORIG
    assert element(document, "#/texts/3").content_source == CONTENT_FROM_TEXT


def test_items_without_semantic_content_become_omissions_that_keep_their_location() -> None:
    document = parse().document

    assert [(one.parser_ref, one.kind, one.reason, one.content_layer) for one in document.omissions] == [
        ("#/pictures/0", "picture", "no-text-content", "body")
    ]
    picture = element(document, "#/pictures/0")
    assert (picture.text, picture.text_usable) == ("", False)
    assert picture.start_char == picture.end_char
    assert picture.coordinate_grade == PARSER_PAGE_COORDINATES
    # Quarantined content keeps whatever the parser knew about where it sat.
    assert document.omissions[0].regions == picture.regions
    assert document.omissions[0].coordinate_grade == PARSER_PAGE_COORDINATES


def blank_item(reason: str, *, captions: Any = OMITTED) -> FakeItem:
    """One item that yields no usable character, in each shape that can happen."""
    if reason == "no-text-content":
        return FakeItem(label="text", text="   \n\t ", self_ref="#/blank/0", prov=[FakeProvenance(page_no=2)])
    cells = (
        ()
        if reason == "empty-table"
        else (FakeTableCell(row=0, column=0, text="  "), FakeTableCell(row=0, column=1, text="\t"))
    )
    return FakeItem(
        label="table",
        text=OMITTED,
        data=FakeTableData(cells, rows=len(cells) and 1, columns=len(cells)),
        captions=captions,
        self_ref="#/blank/0",
        prov=[FakeProvenance(page_no=2)],
    )


@pytest.mark.parametrize("reason", ["empty-table", "blank-table-cells", "no-text-content"])
def test_content_with_nothing_usable_in_it_is_recorded_as_lost_not_as_text(reason: str) -> None:
    document = parse(items=[*sample_items(), blank_item(reason)]).document

    omission = document.omissions[-1]
    assert (omission.parser_ref, omission.reason) == ("#/blank/0", reason)
    blank = element(document, "#/blank/0")
    assert (blank.text, blank.text_usable) == ("", False)
    assert blank.start_char == blank.end_char
    # Whitespace never enters the parsed text, so the usable text is untouched.
    assert document.text == EXPECTED_TEXT
    # Quarantined content keeps the parser location it had.
    assert omission.regions == blank.regions != ()
    if reason == "blank-table-cells":
        # The raw cells survive on the table exactly as the provider gave them,
        # even though nothing in them could become usable text.
        assert [cell.text for cell in document.tables[-1].cells] == ["  ", "\t"]
    # Nothing was mapped from any of these shapes, so none of them names a source
    # the mapped text came from — a blank table's serialization is never built,
    # let alone recorded as this element's content.
    assert blank.content_source == NO_CONTENT


def test_elements_with_no_usable_character_between_them_fail_closed() -> None:
    # The release blocker this file was rewritten for: a table-only document whose
    # cells were dropped used to complete with zero characters.
    table_only = [FakeItem(label="table", text=OMITTED, data=FakeTableData((), rows=0, columns=0), self_ref="#/t/0")]

    with pytest.raises(DoclingParseError, match="produced elements but no usable text") as failure:
        parse(items=table_only)

    call = cast(ParserCall, failure.value.call)
    assert call.status == "failed"
    assert call.failure_reason == "no_usable_text"
    assert (call.element_count, call.usable_character_count) == (1, 0)
    assert call.omission_count == 1


def test_a_malformed_caption_reference_fails_closed_rather_than_unlinking_a_caption() -> None:
    broken = FakeItem(label="table", text=OMITTED, data=FakeTableData(TABLE_CELLS), self_ref="#/tables/5", prov=[])
    broken.captions = [FakeRef("")]

    with pytest.raises(DoclingParseError, match="in-document reference is malformed"):
        parse(items=[broken])

    shapeless = FakeItem(label="picture", text=OMITTED, self_ref="#/pictures/1", prov=[])
    shapeless.captions = object()
    with pytest.raises(DoclingParseError, match="in-document reference list is malformed"):
        parse(items=[shapeless])


def caption_item(self_ref: str = "#/texts/0", text: str = TABLE_CAPTION) -> FakeItem:
    return FakeItem(label="caption", text=text, self_ref=self_ref, prov=[])


def captioned_table(captions: Any, *, self_ref: str = "#/tables/0", cells: Any = TABLE_CELLS) -> FakeItem:
    return FakeItem(
        label="table",
        text=OMITTED,
        data=FakeTableData(cells),
        captions=captions,
        self_ref=self_ref,
        prov=[],
    )


def test_a_caption_reference_is_resolved_against_the_elements_really_emitted() -> None:
    # A caption may be emitted after the item that names it, so resolution waits
    # until every item has been read.
    document = parse(items=[captioned_table(["#/texts/0"]), caption_item()]).document

    assert document.tables[0].caption_refs == ("#/texts/0",)
    assert element(document, "#/texts/0").kind == "caption"
    assert document.text == f"{TABLE_TEXT}{ELEMENT_SEPARATOR}{TABLE_CAPTION}"


def test_two_items_may_share_one_caption_target_because_the_provider_allows_it() -> None:
    """The provider's caption edges are many-to-many, so this adapter keeps them.

    ``FloatingItem.captions`` is a plain ``list[RefItem]``; nothing in the pinned
    release stops two tables from pointing at one caption, and refusing it would
    fail a document the provider itself can produce.
    """
    document = parse(
        items=[
            captioned_table(["#/texts/0"], self_ref="#/tables/0"),
            captioned_table(["#/texts/0"], self_ref="#/tables/1"),
            caption_item(),
        ]
    ).document

    assert [table.caption_refs for table in document.tables] == [("#/texts/0",), ("#/texts/0",)]


def test_a_caption_reference_may_name_any_element_the_provider_emitted() -> None:
    """``add_table(caption=...)`` takes any ``TextItem`` or ``RefItem``, whatever its label.

    Restricting the target to a ``caption``-labelled element refused links the
    pinned release makes routinely, and the label restriction was never the
    adapter's to impose: which text may be quoted as a caption is a ``source.py``
    decision, made against locked bytes.
    """
    ordinary = FakeItem(label="text", text="Ordinary body text.", self_ref="#/texts/0", prov=[])
    footnote = FakeItem(label="footnote", text="1. See § 1.2.", self_ref="#/texts/1", prov=[])

    document = parse(items=[captioned_table(["#/texts/0", "#/texts/1"]), ordinary, footnote]).document

    assert document.tables[0].caption_refs == ("#/texts/0", "#/texts/1")
    assert [element(document, ref).kind for ref in document.tables[0].caption_refs] == ["text", "footnote"]
    # Nothing here graded the target as evidence, or as anything else.
    assert all(one.text_usable for one in document.elements)


def test_a_quarantined_table_keeps_the_caption_link_it_had() -> None:
    items = [caption_item(), blank_item("blank-table-cells", captions=["#/texts/0"])]

    document = parse(items=items).document

    # Nothing usable in the table, and the only text that says what it held is
    # still linked to it rather than quietly detached.
    assert document.omissions[-1].caption_refs == ("#/texts/0",)
    assert document.text == TABLE_CAPTION


@pytest.mark.parametrize(
    ("items", "message"),
    [
        # A reference to an element the provider never emitted: a link no consumer
        # can follow, and the loss this adapter exists to prevent if dropped.
        ([captioned_table(["#/texts/9"]), caption_item()], "resolves to no emitted element"),
        ([captioned_table(["#/tables/0"]), caption_item()], "itself as its own caption"),
        # One edge recorded twice is not two edges.
        ([captioned_table(["#/texts/0", "#/texts/0"]), caption_item()], "names one caption more than once"),
    ],
)
def test_an_unresolvable_or_invalid_caption_relationship_fails_closed(items: list[Any], message: str) -> None:
    with pytest.raises(DoclingParseError, match=message) as failure:
        parse(items=items)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "malformed_element")


def test_offsets_round_trip_and_say_what_they_address() -> None:
    document = parse().document

    for one in document.elements:
        assert document.text[one.start_char : one.end_char] == one.text
    assert document.text.count(SCOPE_BODY) == 1
    assert "—" in document.text and "  discharges" in document.text

    body = element(document, "#/texts/3")
    assert body.start_char == EXPECTED_TEXT.index(SCOPE_BODY)
    assert body.end_char == body.start_char + len(SCOPE_BODY)
    assert document.offsets.target == "adapter-parsed-text"
    assert document.element_separator == ELEMENT_SEPARATOR


def test_usable_text_and_coordinate_grade_are_separate_and_never_source_exact() -> None:
    document = parse().document

    graded = {one.parser_ref: (one.text_usable, one.coordinate_grade) for one in document.elements}
    # Ordinary parsed text with page geometry: usable, still only parser evidence.
    assert graded["#/texts/3"] == (True, PARSER_PAGE_COORDINATES)
    # A heading Docling located nowhere, as every DOCX element is.
    assert graded["#/texts/2"] == (True, NO_COORDINATES)
    # A picture: located, but nothing to read.
    assert graded["#/pictures/0"] == (False, PARSER_PAGE_COORDINATES)

    assert SOURCE_EXACT_EVIDENCE_GRADE not in {one.coordinate_grade for one in document.elements}
    assert SOURCE_EXACT_EVIDENCE_GRADE not in json.dumps(parse().call.as_json_dict())

    # Whatever geometry the provider did give is kept verbatim, rectangle and
    # provider character span alike.
    assert element(document, "#/texts/3").regions == (
        PageRegion(1, 72.0, 700.5, 540.25, 688.0, "BOTTOMLEFT", 0, 5),
        PageRegion(2, 72.0, 120.0, 540.25, 688.0, "BOTTOMLEFT", 0, 5),
    )
    assert (document.elements[0].regions[0].char_start, document.elements[0].regions[0].char_end) == (
        0,
        len(TITLE_TEXT),
    )


def test_parser_records_the_provider_heading_path_without_inventing_one() -> None:
    document = parse().document

    assert [one.heading_path for one in document.elements] == [
        (),
        # The furniture page header sits between the body title and the body
        # heading, and belongs to neither: its own layer has no heading.
        (),
        (TITLE_TEXT,),
        *[(TITLE_TEXT, SCOPE_HEADING)] * 5,
        (TITLE_TEXT,),
        (TITLE_TEXT, LIMITS_HEADING),
        # A speaker note is not under the body's last heading either.
        (),
    ]


def test_heading_context_never_crosses_a_content_layer() -> None:
    # Interleaved on purpose: under one shared heading stack, each of these
    # non-body headings became the recorded context of the body text that followed
    # it, and the body title annotated all four other layers.
    interleaved = [
        FakeItem(label="title", text="Body Title", self_ref="#/texts/0", prov=[]),
        FakeItem(
            label="section_header",
            text="Furniture Heading",
            self_ref="#/texts/1",
            level=1,
            content_layer="furniture",
            prov=[],
        ),
        FakeItem(label="text", text="Body under the body title.", self_ref="#/texts/2", prov=[]),
        FakeItem(
            label="section_header",
            text="Background Heading",
            self_ref="#/texts/3",
            level=1,
            content_layer="background",
            prov=[],
        ),
        FakeItem(label="text", text="Footer text.", self_ref="#/texts/4", content_layer="furniture", prov=[]),
        FakeItem(label="title", text="Invisible Title", self_ref="#/texts/5", content_layer="invisible", prov=[]),
        FakeItem(label="section_header", text="Body Heading", self_ref="#/texts/6", level=1, prov=[]),
        FakeItem(label="text", text="Background text.", self_ref="#/texts/7", content_layer="background", prov=[]),
        FakeItem(label="text", text="Speaker note.", self_ref="#/texts/8", content_layer="notes", prov=[]),
        FakeItem(label="text", text="Body under the body heading.", self_ref="#/texts/9", prov=[]),
        FakeItem(label="text", text="Invisible text.", self_ref="#/texts/10", content_layer="invisible", prov=[]),
    ]
    document = parse(items=interleaved).document

    paths = {one.parser_ref: (one.content_layer, one.heading_path) for one in document.elements}
    assert paths["#/texts/2"] == ("body", ("Body Title",))
    assert paths["#/texts/9"] == ("body", ("Body Title", "Body Heading"))
    # Each other layer carries its own headings and only its own.
    assert paths["#/texts/4"] == ("furniture", ("Furniture Heading",))
    assert paths["#/texts/7"] == ("background", ("Background Heading",))
    assert paths["#/texts/10"] == ("invisible", ("Invisible Title",))
    # Notes had no heading of their own, so a note carries none — not the body's.
    assert paths["#/texts/8"] == ("notes", ())
    # And a heading's own path stays inside its layer too.
    assert paths["#/texts/6"] == ("body", ("Body Title",))
    assert paths["#/texts/3"] == ("background", ())


def test_preserved_text_is_recorded_per_layer_and_claims_nothing_beyond_preservation() -> None:
    """``text_usable`` says the provider's nonblank text was kept. Nothing more.

    It is not an evidence grade, an embedding or segmentation decision, or a
    quotation licence: a page footer, a speaker note, and an invisible-layer string
    are all preserved text, and none of them is body content. The record hands a
    consumer the layer, the content source, and the coordinate grade so
    ``source.py`` can decide; the adapter never decides for it.
    """
    layered = [
        FakeItem(label="text", text=f"{layer} text", self_ref=f"#/texts/{index}", content_layer=layer, prov=[])
        for index, layer in enumerate(CONTENT_LAYERS)
    ]
    blank = [
        FakeItem(label="text", text="  \t ", self_ref=f"#/blank/{index}", content_layer=layer, prov=[])
        for index, layer in enumerate(CONTENT_LAYERS)
    ]

    document = parse(items=[*layered, *blank]).document

    # Preserved is exactly "nonblank parser text was kept", on every layer alike.
    assert {one.parser_ref: one.text_usable for one in document.elements} == {
        **{f"#/texts/{index}": True for index in range(len(CONTENT_LAYERS))},
        **{f"#/blank/{index}": False for index in range(len(CONTENT_LAYERS))},
    }
    assert all(one.text_usable is bool(one.text.strip()) for one in document.elements)
    # Every element still says which layer it came from and how well it was
    # located, which is what a consumer excludes or promotes on.
    assert {one.content_layer for one in document.elements} == set(CONTENT_LAYERS)
    assert {one.coordinate_grade for one in document.elements} == {NO_COORDINATES}
    assert SOURCE_EXACT_EVIDENCE_GRADE not in json.dumps(parse(items=layered).call.as_json_dict())

    call = parse(items=[*layered, *blank]).call
    # The counts follow the same narrow meaning, across every layer.
    assert call.usable_element_count == len(CONTENT_LAYERS)
    assert call.usable_character_count == sum(len(one.text) for one in document.elements if one.text_usable)
    assert call.content_layers_present == tuple(sorted(CONTENT_LAYERS))


def test_call_records_are_stable_for_the_same_source() -> None:
    first, second = parse().call, parse().call

    assert dataclasses.replace(first, duration_ms=0.0) == dataclasses.replace(second, duration_ms=0.0)

    other = parser().parse(b"PK\x03\x04other bytes\n", source_name=SOURCE_NAME)
    assert other.call.source_sha256 != first.source_sha256
    assert other.call.media_type is None


# --- effective configuration ------------------------------------------------


def test_the_policy_states_only_what_simple_pipeline_really_applies() -> None:
    policy = parser().policy

    assert policy.pipeline == PIPELINE_SIMPLE
    assert policy.supported_formats == (FORMAT_DOCX, FORMAT_PPTX, FORMAT_XLSX)
    assert policy.content_layers == CONTENT_LAYERS
    assert [
        policy.remote_services_enabled,
        policy.external_plugins_allowed,
        policy.picture_classification_enabled,
        policy.picture_description_enabled,
        policy.chart_extraction_enabled,
    ] == [False] * 5
    # SimplePipeline never reads ``document_timeout`` and exposes no
    # pre-conversion page gate a limit could be applied at. No in-process adapter
    # can hold a wall-clock, CPU, memory, or archive-expansion bound on the library
    # call it is inside of either — that is ``source.py``'s process gate. The
    # record says all three instead of naming bounds that are not held.
    assert [
        policy.document_timeout_enforced,
        policy.page_limit_enforced,
        policy.process_containment_enforced,
    ] == [False] * 3
    assert policy.converter_source == "injected"

    # Every mapping bound the adapter really does enforce, stated in the record it
    # publishes, so a receipt names the limits its output was produced under —
    # each at the scope its name declares.
    assert (policy.max_items, policy.max_tables) == (adapter_module.MAX_ITEMS, adapter_module.MAX_TABLES)
    assert (policy.max_cells_per_table, policy.max_mapped_characters) == (MAX_CELLS_PER_TABLE, MAX_MAPPED_CHARACTERS)
    assert (policy.max_total_table_cells, policy.max_table_dimension) == (MAX_TOTAL_TABLE_CELLS, MAX_TABLE_DIMENSION)
    # Retained cell characters are their own bound, at their own scope: mapped
    # text is what a consumer receives, and a whitespace-only table contributes
    # none of it while still keeping every cell value.
    assert policy.max_table_cell_characters == MAX_TABLE_CELL_CHARACTERS
    assert (policy.max_heading_level, policy.max_reference_chars) == (MAX_HEADING_LEVEL, MAX_REFERENCE_CHARS)
    assert (policy.max_caption_refs_per_item, policy.max_total_caption_refs) == (
        MAX_CAPTION_REFS_PER_ITEM,
        MAX_TOTAL_CAPTION_REFS,
    )
    assert (policy.max_regions_per_item, policy.max_total_regions) == (MAX_REGIONS_PER_ITEM, MAX_TOTAL_REGIONS)
    assert policy.max_provider_errors == MAX_PROVIDER_ERRORS
    assert policy.max_source_bytes == adapter_module.DEFAULT_MAX_SOURCE_BYTES
    # The provider-controlled scalars a completed record carries have stated
    # comparators too, not just its collections.
    assert (policy.max_page_number, policy.max_provenance_char_index) == (
        MAX_PAGE_NUMBER,
        MAX_PROVENANCE_CHARACTER_INDEX,
    )
    assert (policy.max_tree_depth, policy.max_error_type_chars) == (MAX_TREE_DEPTH, MAX_ERROR_TYPE_CHARS)
    # And ``max_page_number`` is a bound on what may be persisted, not a page
    # limit applied to a conversion: the flag beside it stays false.
    assert policy.page_limit_enforced is False
    # The mapping itself is identified, not just the provider releases.
    assert (policy.mapping_revision, policy.table_serialization) == (ADAPTER_MAPPING_REVISION, TABLE_SERIALIZATION)
    assert policy.text_offsets == PARSED_TEXT_OFFSETS
    assert policy.heading_kinds == tuple(sorted(HEADING_KINDS))

    # The whole record and the whole constructor, so a model, OCR, device,
    # thread, or timeout setting cannot reappear without this test naming it.
    assert {field.name for field in dataclasses.fields(policy)} == {
        "pipeline",
        "mapping_revision",
        "content_layers",
        "element_separator",
        "heading_kinds",
        "table_serialization",
        "text_offsets",
        "supported_formats",
        "max_source_bytes",
        "max_items",
        "max_tables",
        "max_cells_per_table",
        "max_total_table_cells",
        "max_table_dimension",
        "max_mapped_characters",
        "max_table_cell_characters",
        "max_heading_level",
        "max_tree_depth",
        "max_reference_chars",
        "max_caption_refs_per_item",
        "max_total_caption_refs",
        "max_regions_per_item",
        "max_total_regions",
        "max_page_number",
        "max_provenance_char_index",
        "max_provider_errors",
        "max_error_type_chars",
        "remote_services_enabled",
        "external_plugins_allowed",
        "picture_classification_enabled",
        "picture_description_enabled",
        "chart_extraction_enabled",
        "document_timeout_enforced",
        "page_limit_enforced",
        "process_containment_enforced",
        "converter_source",
    }
    # ``max_source_bytes`` stays the only numeric setting a caller may choose; the
    # mapping bounds are policy constants, not per-call configuration.
    assert set(inspect.signature(DoclingDocumentParser).parameters) == {
        "max_source_bytes",
        "converter",
        "version_reader",
    }


def test_parser_id_and_policy_digest_bind_every_output_affecting_setting() -> None:
    baseline = parser()

    assert baseline.parser_id == (
        f"docling:{DOCLING_VERSION}:docling-core:{DOCLING_CORE_VERSION}"
        f":{ADAPTER_MAPPING_REVISION}:{baseline.policy_digest[:16]}"
    )
    assert baseline.parser_id == parser().parser_id
    assert parser(max_source_bytes=1_000).parser_id != baseline.parser_id
    assert parse().call.policy_digest == baseline.policy_digest
    assert parse().call.policy == baseline.policy


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        ("ADAPTER_MAPPING_REVISION", "office-mapping-next"),
        ("TABLE_SERIALIZATION", "comma-separated-rows"),
        ("ELEMENT_SEPARATOR", "\n"),
        ("CONTENT_LAYERS", ("body",)),
        ("HEADING_KINDS", frozenset({"title"})),
        ("SUPPORTED_FORMATS", frozenset({FORMAT_DOCX})),
        ("PARSED_TEXT_OFFSETS", OffsetSemantics(target="something-else", unit="bytes", interval="closed")),
        ("MAX_ITEMS", 7),
        ("MAX_TABLES", 7),
        ("MAX_CELLS_PER_TABLE", 7),
        ("MAX_TOTAL_TABLE_CELLS", 7),
        ("MAX_TABLE_DIMENSION", 7),
        ("MAX_MAPPED_CHARACTERS", 7),
        ("MAX_TABLE_CELL_CHARACTERS", 7),
        ("MAX_HEADING_LEVEL", 7),
        # Large enough that the fixture's own references still fit: the assertion
        # is that changing a bound changes the identity, not that it refuses.
        ("MAX_REFERENCE_CHARS", 64),
        ("MAX_CAPTION_REFS_PER_ITEM", 7),
        ("MAX_TOTAL_CAPTION_REFS", 7),
        ("MAX_REGIONS_PER_ITEM", 7),
        ("MAX_TOTAL_REGIONS", 7),
        ("MAX_PROVIDER_ERRORS", 7),
        # The scalar bounds decide what an accepted record may hold, so they are
        # part of the mapping's identity like every other bound.
        ("MAX_PAGE_NUMBER", 7),
        ("MAX_PROVENANCE_CHARACTER_INDEX", 7),
        ("MAX_TREE_DEPTH", 7),
        ("MAX_ERROR_TYPE_CHARS", 7),
    ],
)
# This test asserts *identity*, and only identity: rebinding a module constant
# proves the policy digest covers it, never that the adapter reads that constant
# at the moment it enforces something. Where a bound is also compiled into a
# pattern at import — ``MAX_ERROR_TYPE_CHARS`` into ``_EXCEPTION_TYPE_NAME`` — the
# enforcement is asserted separately, by
# ``test_the_recorded_error_type_bound_is_enforced_at_runtime_not_only_in_a_pattern``.
def test_every_output_affecting_semantic_changes_the_parser_identity(
    monkeypatch: pytest.MonkeyPatch, setting: str, value: Any
) -> None:
    # Two records carrying one ``parser_id`` must describe one mapping. The pinned
    # provider versions alone cannot promise that: the separator, the layers
    # requested, the heading rule, the table serialization, the offset semantics,
    # and every recorded bound all decide what the mapped output is.
    baseline = parser()
    monkeypatch.setattr(adapter_module, setting, value)

    changed = parser()

    assert changed.policy != baseline.policy
    assert changed.policy_digest != baseline.policy_digest
    assert changed.parser_id != baseline.parser_id
    # And the record a caller receives carries the identity it was parsed under.
    assert parse(items=[FakeItem(text="alpha", self_ref="#/texts/0", prov=[])]).call.parser_id == changed.parser_id


@pytest.mark.parametrize("value", [0, -1, True, 1.5, float("nan"), float("inf"), "64"])
def test_parser_refuses_a_byte_bound_that_is_not_a_positive_integer(value: Any) -> None:
    # Every numeric setting is an integer bound the adapter itself enforces, so a
    # bool, a float, or a non-finite value is refused rather than recorded.
    with pytest.raises(DoclingConfigurationError, match="max_source_bytes must be a positive integer"):
        parser(max_source_bytes=value)


def test_a_record_holding_a_non_finite_number_refuses_to_serialize() -> None:
    call = parse().call

    assert math.isfinite(call.duration_ms)
    assert json.loads(json.dumps(call.as_json_dict(), allow_nan=False))["status"] == "completed"

    for broken in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(DoclingParseError, match="not finite"):
            dataclasses.replace(call, duration_ms=broken).as_json_dict()


# --- format detection -------------------------------------------------------


@pytest.mark.parametrize(
    ("source_name", "media_type", "expected_format", "expected_suffix"),
    [
        ("letter.docx", None, FORMAT_DOCX, ".docx"),
        ("letter.DOCX", None, FORMAT_DOCX, ".docx"),
        ("letter", SOURCE_MEDIA_TYPE, FORMAT_DOCX, ".docx"),
        ("letter.docx", "application/octet-stream", FORMAT_DOCX, ".docx"),
        ("table.xlsx", None, FORMAT_XLSX, ".xlsx"),
        ("deck.pptx", None, FORMAT_PPTX, ".pptx"),
    ],
)
def test_format_detection_uses_the_logical_name_or_the_media_type(
    source_name: str, media_type: str | None, expected_format: str, expected_suffix: str
) -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), provider_format=expected_format))

    result = parser(converter=converter).parse(SOURCE_BYTES, source_name=source_name, media_type=media_type)

    assert result.document.input_format == expected_format
    assert result.call.input_format == expected_format
    assert converter.calls[0]["name"] == f"source{expected_suffix}"


@pytest.mark.parametrize(
    ("source_name", "media_type", "detected"),
    [
        ("rule.pdf", None, FORMAT_PDF),
        ("rule", "application/pdf", FORMAT_PDF),
        ("scan.tiff", None, FORMAT_IMAGE),
        ("scan", "image/png", FORMAT_IMAGE),
    ],
)
def test_a_paginated_format_is_refused_by_name_before_the_provider_runs(
    source_name: str, media_type: str | None, detected: str
) -> None:
    assert SUPPORTED_FORMATS == {FORMAT_DOCX, FORMAT_PPTX, FORMAT_XLSX}
    assert DEFERRED_FORMATS == {FORMAT_PDF, FORMAT_IMAGE}
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))
    assert parser(converter=converter).supported_formats == SUPPORTED_FORMATS

    with pytest.raises(DoclingParseError, match="recognized but not implemented") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=source_name, media_type=media_type)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.provider_invoked, call.attempt_count) == ("format_not_implemented", False, 0)
    # The record names the real format, so a later paginated adapter can find it.
    assert call.input_format == detected
    assert call.source_sha256 == SOURCE_SHA256
    assert converter.calls == []
    # The refusal states what is actually missing, not a workaround that is gone.
    message = str(failure.value)
    assert "model manifest" in message and "OCR" in message
    assert "model store" not in message


@pytest.mark.parametrize(
    ("source_name", "media_type", "message"),
    [
        ("rule.xlsx", SOURCE_MEDIA_TYPE, "different input formats"),
        ("rule.txt", None, "not one this adapter recognizes"),
        ("rule.doc", None, "not one this adapter recognizes"),
        ("rule", None, "not one this adapter recognizes"),
        ("rule.docx", "not a media type", "not a valid type/subtype"),
        ("rule.docx", "x" * 200, "exceeds the recorded length bound"),
    ],
)
def test_an_undecidable_format_fails_closed_without_persisting_the_bad_value(
    source_name: str, media_type: str | None, message: str
) -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    with pytest.raises(DoclingParseError, match=message) as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=source_name, media_type=media_type)

    call = cast(ParserCall, failure.value.call)
    assert call.failure_reason == "unsupported_input"
    assert (call.provider_invoked, call.attempt_count) == (False, 0)
    assert call.input_format == FORMAT_UNKNOWN
    assert call.media_type is None
    assert call.source_sha256 == SOURCE_SHA256
    assert converter.calls == []


def test_a_successful_conversion_must_prove_the_format_it_parsed() -> None:
    # Docling sniffs content and picks the backend itself, so a successful
    # conversion can still be the wrong pipeline for the recorded format — and a
    # success that names no format proves nothing at all.
    mismatch = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), provider_format=FORMAT_XLSX))
    with pytest.raises(DoclingParseError, match="a different input format than the one detected") as failure:
        parser(converter=mismatch).parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    call = cast(ParserCall, failure.value.call)
    assert (call.conversion_status, call.failure_reason) == ("success", "format_mismatch")
    assert (call.input_format, call.provider_input_format) == (FORMAT_DOCX, FORMAT_XLSX)

    missing = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), provider_format=None))
    with pytest.raises(DoclingParseError, match="no input format for a successful conversion") as absent:
        parser(converter=missing).parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    assert cast(ParserCall, absent.value.call).failure_reason == "provider_format_missing"

    assert parse().call.provider_input_format == FORMAT_DOCX


# --- resource and identifier safety -----------------------------------------


def test_over_limit_source_bytes_fail_closed_before_anything_is_written() -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    with pytest.raises(DoclingParseError, match="exceed the recorded input limit") as failure:
        parser(converter=converter, max_source_bytes=len(SOURCE_BYTES) - 1).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.provider_invoked) == ("source_bytes_over_limit", False)
    assert call.source_bytes == len(SOURCE_BYTES)
    assert call.policy.max_source_bytes == len(SOURCE_BYTES) - 1
    assert converter.calls == []

    # The bound is exact: the adapter enforces it itself, before it writes a byte.
    at_limit = parser(max_source_bytes=len(SOURCE_BYTES))
    assert at_limit.parse(SOURCE_BYTES, source_name=SOURCE_NAME).call.source_bytes == len(SOURCE_BYTES)


def texts(count: int, text: str = "alpha") -> list[FakeItem]:
    return [FakeItem(text=text, self_ref=f"#/texts/{index}", prov=[]) for index in range(count)]


def tables(count: int) -> list[FakeItem]:
    return [
        FakeItem(label="table", text=OMITTED, data=FakeTableData(TABLE_CELLS), self_ref=f"#/tables/{index}", prov=[])
        for index in range(count)
    ]


def captions_item(count: int) -> list[FakeItem]:
    """One item naming ``count`` distinct captions, plus the captions themselves."""
    refs = [f"#/texts/{index}" for index in range(count)]
    return [
        captioned_table(refs, self_ref="#/tables/0"),
        *[caption_item(self_ref=ref, text=f"Caption {ref}") for ref in refs],
    ]


def regions_item(count: int) -> list[FakeItem]:
    return [FakeItem(text="alpha", self_ref="#/texts/0", prov=[FakeProvenance(page_no=1) for _ in range(count)])]


def wide_table(cells_per_table: int, count: int) -> list[FakeItem]:
    """``count`` tables of ``cells_per_table`` single-row cells each."""
    return [
        FakeItem(
            label="table",
            text=OMITTED,
            data=FakeTableData(
                tuple(FakeTableCell(row=0, column=column, text="x") for column in range(cells_per_table)),
                rows=1,
                columns=cells_per_table,
            ),
            self_ref=f"#/tables/{index}",
            prov=[],
        )
        for index in range(count)
    ]


@pytest.mark.parametrize(
    ("setting", "limit", "items", "reason"),
    [
        ("MAX_ITEMS", 2, texts(3), "item_limit"),
        ("MAX_TABLES", 1, tables(2), "table_limit"),
        # Refused while the items are still arriving, so nothing unbounded is held.
        ("MAX_MAPPED_CHARACTERS", 5, texts(1, "alphabet"), "character_limit"),
        # And refused exactly, over the joined text a consumer receives: two
        # three-character elements are six characters of content and eight of text.
        ("MAX_MAPPED_CHARACTERS", 7, texts(2, "abc"), "character_limit"),
        # One table's cells, and its declared area.
        ("MAX_CELLS_PER_TABLE", 2, wide_table(3, 1), "table_cell_limit"),
        ("MAX_TABLE_DIMENSION", 2, wide_table(3, 1), "table_dimension_limit"),
        # And the document-wide total, which is what ``table_cell_count`` is read
        # against: without it, two tables of two cells each sit under a per-table
        # bound of two and the receipt's count has no ceiling in the record.
        ("MAX_TOTAL_TABLE_CELLS", 3, wide_table(2, 2), "total_table_cell_limit"),
        # The characters a table's cells hold, which the parse retains on a
        # ``ParsedTable`` whether or not they ever become mapped text.
        ("MAX_TABLE_CELL_CHARACTERS", 2, wide_table(3, 1), "table_cell_character_limit"),
        # Caption references, per item and across the document.
        ("MAX_CAPTION_REFS_PER_ITEM", 2, captions_item(3), "caption_reference_limit"),
        ("MAX_TOTAL_CAPTION_REFS", 2, captions_item(3), "caption_reference_limit"),
        # Page regions, per item and across the document.
        ("MAX_REGIONS_PER_ITEM", 2, regions_item(3), "page_region_limit"),
        ("MAX_TOTAL_REGIONS", 2, regions_item(3), "page_region_limit"),
    ],
)
def test_converted_output_cannot_grow_past_a_recorded_mapping_bound(
    monkeypatch: pytest.MonkeyPatch, setting: str, limit: int, items: list[FakeItem], reason: str
) -> None:
    monkeypatch.setattr(adapter_module, setting, limit)

    with pytest.raises(DoclingParseError, match="bound") as failure:
        parse(items=items)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", reason)
    # The receipt states the limit the refusal was made under, not just that one hit.
    assert getattr(call.policy, setting.lower()) == limit
    assert json.loads(json.dumps(call.as_json_dict(), allow_nan=False))["failure_reason"] == reason


@pytest.mark.parametrize(
    ("setting", "limit", "items"),
    [
        ("MAX_ITEMS", 3, texts(3)),
        ("MAX_TABLES", 2, tables(2)),
        ("MAX_MAPPED_CHARACTERS", 8, texts(2, "abc")),
        ("MAX_CELLS_PER_TABLE", 3, wide_table(3, 1)),
        ("MAX_TABLE_DIMENSION", 3, wide_table(3, 1)),
        ("MAX_TOTAL_TABLE_CELLS", 4, wide_table(2, 2)),
        ("MAX_TABLE_CELL_CHARACTERS", 3, wide_table(3, 1)),
        ("MAX_CAPTION_REFS_PER_ITEM", 3, captions_item(3)),
        ("MAX_TOTAL_CAPTION_REFS", 3, captions_item(3)),
        ("MAX_REGIONS_PER_ITEM", 3, regions_item(3)),
        ("MAX_TOTAL_REGIONS", 3, regions_item(3)),
    ],
)
def test_a_document_exactly_at_a_mapping_bound_is_mapped_normally(
    monkeypatch: pytest.MonkeyPatch, setting: str, limit: int, items: list[FakeItem]
) -> None:
    # Every bound is a ceiling, not a threshold: the same document that fails one
    # over completes exactly at it. The pair is what makes the recorded limit
    # readable as the number it actually is.
    monkeypatch.setattr(adapter_module, setting, limit)

    call = parse(items=items).call

    assert (call.status, call.failure_reason) == ("completed", None)
    assert getattr(call.policy, setting.lower()) == limit


def test_a_document_inside_every_mapping_bound_is_mapped_normally() -> None:
    # The bounds are ceilings, not thresholds: the sample document sits under all
    # of them and is unaffected.
    call = parse().call

    assert call.status == "completed"
    assert call.element_count < call.policy.max_items
    assert call.table_count < call.policy.max_tables
    assert call.character_count < call.policy.max_mapped_characters
    # The per-table bound and the document-wide comparator for the same count.
    assert call.table_cell_count < call.policy.max_cells_per_table
    assert call.table_cell_count < call.policy.max_total_table_cells


@pytest.mark.parametrize(
    ("raw", "persisted"),
    [
        ("epa-2026-0001.docx", "epa-2026-0001.docx"),
        ("https://files.example.gov/a/b/epa rule.docx?X-Amz-Signature=deadbeef", "epa_rule.docx"),
        ("https://files.example.gov/deck.pptx#slide=3", "deck.pptx"),
        ("/var/folders/T/tmp0000/scan.xlsx", "scan.xlsx"),
        ("..\\..\\Windows\\System32\\config.docx", "config.docx"),
        ("../../etc/passwd", "source"),
        ("AKIAIOSFODNN7EXAMPLE.docx", "source.docx"),
        (f"{'a1' * 24}.docx", "source.docx"),
        (f"{'section-' * 40}.docx", f"{('section-' * 40)[:123]}.docx"),
        # A hostile "extension" this module does not recognize is not carried.
        (f"{'a1' * 24}.docx{'x' * 300}", "source"),
        (f"{'section-' * 40}.exe", ("section-" * 40)[:128]),
        ("?", "source"),
    ],
)
def test_an_untrusted_source_name_is_sanitized_and_bounded_before_it_is_persisted(raw: str, persisted: str) -> None:
    name, sanitized = sanitized_source_name(raw)

    assert name == persisted
    assert len(name) <= adapter_module.MAX_SOURCE_NAME_CHARS
    # The flag says exactly one thing: the persisted name is not the caller's own.
    assert sanitized is (name != raw)


@pytest.mark.parametrize("attribute", ["self_ref", "parent", "captions"])
def test_a_provider_reference_longer_than_the_recorded_bound_fails_closed(attribute: str) -> None:
    """Reference strings land verbatim on every element, table, and omission.

    The pinned ``RefItem`` pattern bounds a reference's shape but not its length,
    so without a bound of this adapter's own one provider value could grow every
    project-owned record that carries it.
    """
    long_reference = "#/texts/" + "0" * MAX_REFERENCE_CHARS
    items: list[Any] = {
        "self_ref": lambda: [FakeItem(text="alpha", prov=[], self_ref=long_reference)],
        "parent": lambda: [FakeItem(text="alpha", prov=[], parent=long_reference)],
        "captions": lambda: [captioned_table([long_reference]), caption_item(self_ref=long_reference)],
    }[attribute]()

    with pytest.raises(DoclingParseError, match="exceeds the recorded reference length bound") as failure:
        parse(items=items)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "reference_limit")
    assert call.policy.max_reference_chars == MAX_REFERENCE_CHARS
    assert long_reference not in json.dumps(call.as_json_dict())


def test_a_provider_reference_exactly_at_the_recorded_bound_is_kept() -> None:
    # The bound is a ceiling, not a threshold: a reference of exactly this length
    # is carried through, in both places one is recorded.
    exact = "#/texts/" + "0" * (MAX_REFERENCE_CHARS - len("#/texts/"))
    assert len(exact) == MAX_REFERENCE_CHARS

    document = parse(items=[FakeItem(text="alpha", prov=[], self_ref=exact, parent=exact)]).document

    assert (document.elements[0].parser_ref, document.elements[0].parent_ref) == (exact, exact)


def test_the_provider_error_list_is_bounded_while_it_is_read(monkeypatch: pytest.MonkeyPatch) -> None:
    # ``provider_error_count`` and ``provider_error_categories`` are provider
    # metadata that reaches an accepted record, so the list they come from is
    # bounded as it arrives rather than held whole and counted afterwards.
    monkeypatch.setattr(adapter_module, "MAX_PROVIDER_ERRORS", 2)
    errors = tuple(FakeErrorItem("policy") for _ in range(3))
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), errors=errors))

    with pytest.raises(DoclingParseError, match="more conversion errors than the recorded error bound") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    # A recorded bound names itself; it is not the provider's metadata that is
    # malformed, it is this adapter that refused.
    assert (call.status, call.failure_reason) == ("failed", "provider_error_limit")
    assert call.error_type == "_MappingLimitExceeded"
    assert call.policy.max_provider_errors == 2

    # And exactly at the bound the same conversion is read and reported normally.
    at_bound = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), errors=errors[:2]))
    with pytest.raises(DoclingParseError, match="reported conversion errors") as accepted:
        parser(converter=at_bound).parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    assert cast(ParserCall, accepted.value.call).provider_error_count == 2


@pytest.mark.parametrize("value", [17, b"rule.docx", None, object(), ["rule.docx"]])
def test_a_source_name_that_is_not_a_string_is_refused_deliberately(value: Any) -> None:
    """Caller misuse becomes a stated refusal, not an ``AttributeError`` from a helper.

    No call record accompanies this one, and that is the honest outcome: every
    field of a record derives from the caller's exact identifier — above all
    ``source_name_sha256``, which must cover that exact value — and there is no
    safe encoding of a non-string to hash. Fabricating one would put an invented
    identity in a receipt a reader would join on.
    """
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    with pytest.raises(DoclingParseError, match="source_name must be a string") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=cast(str, value))

    assert failure.value.call is None
    assert converter.calls == []


@pytest.mark.parametrize("value", [17, b"application/pdf", object(), ["text/plain"]])
def test_a_media_type_that_is_not_a_string_is_refused_with_a_complete_receipt(value: Any) -> None:
    # Unlike a bad ``source_name``, an identity *can* be constructed here — the
    # name and the bytes are both sound — so the refusal carries the same complete,
    # secret-free record any other unusable media type gets, and the caller's
    # value is not persisted.
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    with pytest.raises(DoclingParseError, match="media_type is not a string") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME, media_type=cast(str, value))

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.provider_invoked, call.attempt_count) == ("unsupported_input", False, 0)
    assert (call.input_format, call.media_type) == (FORMAT_UNKNOWN, None)
    assert (call.source_sha256, call.source_name) == (SOURCE_SHA256, SOURCE_NAME)
    payload = call.as_json_dict()
    assert set(payload) == {field.name for field in dataclasses.fields(ParserCall)}
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload
    assert converter.calls == []


def test_a_signed_url_is_neither_persisted_nor_written_to_disk_but_stays_identifiable() -> None:
    signed = "https://files.example.gov/private/rule.docx?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=" + "f" * 64
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    call = parser(converter=converter).parse(SOURCE_BYTES, source_name=signed).call

    assert (call.source_name, call.source_name_sanitized) == ("rule.docx", True)
    assert call.source_name_sha256 == hashlib.sha256(signed.encode()).hexdigest()
    assert converter.calls[0]["name"] == "source.docx"
    for secret in ("X-Amz", "AKIA", "f" * 64, "https", "private"):
        assert secret not in json.dumps(call.as_json_dict())


# --- provider-controlled scalars a record persists ---------------------------


class LevelledDocument(FakeDoclingDocument):
    """A document whose ``iterate_items`` yields the tree level it was handed.

    ``iterate_items`` is where a provider-controlled *depth* enters the mapping,
    and the default stand-in always yields ``1`` — so the level has to be
    steerable for the bound on it to be exercised at all.
    """

    def __init__(self, items: list[Any], level: Any) -> None:
        super().__init__(items)
        self.level = level

    def iterate_items(self, **options: Any) -> Iterator[tuple[Any, Any]]:
        self.iterate_calls.append(dict(options))
        return iter([(item, self.level) for item in self.items])


def parse_at_tree_level(level: Any) -> ParsedDocumentResult:
    document = LevelledDocument([FakeItem(text="alpha", self_ref="#/texts/0", prov=[])], level)
    return parser(converter=FakeConverter(FakeConversion(document))).parse(SOURCE_BYTES, source_name=SOURCE_NAME)


def parse_at_page_number(value: Any) -> ParsedDocumentResult:
    return parse(items=[FakeItem(text="alpha", self_ref="#/texts/0", prov=[FakeProvenance(page_no=value)])])


def parse_at_character_index(value: Any) -> ParsedDocumentResult:
    located = FakeItem(text="alpha", self_ref="#/texts/0", prov=[FakeProvenance(charspan=(0, value))])
    return parse(items=[located])


def page_number_of(result: ParsedDocumentResult) -> int | None:
    return result.document.elements[0].regions[0].page_number


def character_index_of(result: ParsedDocumentResult) -> int | None:
    return result.document.elements[0].regions[0].char_end


def tree_level_of(result: ParsedDocumentResult) -> int | None:
    return result.document.elements[0].tree_level


PERSISTED_PROVIDER_SCALARS = [
    ("page number", MAX_PAGE_NUMBER, parse_at_page_number, page_number_of, "page_number_limit", "page-number bound"),
    (
        "provenance character index",
        MAX_PROVENANCE_CHARACTER_INDEX,
        parse_at_character_index,
        character_index_of,
        "character_span_limit",
        "character-index bound",
    ),
    ("tree depth", MAX_TREE_DEPTH, parse_at_tree_level, tree_level_of, "tree_depth_limit", "tree-depth bound"),
]
"""Every provider-controlled integer that lands *verbatim* on a project record.

A page ordinal, a provenance character index, and an element's tree depth are
each read straight off a provider object and persisted unchanged, so each needs
its own stated ceiling. The tuple carries what a test needs to drive one: the
bound, how to reach it, how to read the persisted value back, the reason the
receipt names, and the phrase the refusal uses.
"""

SCALAR_IDS = [name for name, *_ in PERSISTED_PROVIDER_SCALARS]

ARBITRARY_PRECISION = 10**100
"""A provider integer no fixed-width record could hold and no bound would admit."""


@pytest.mark.parametrize(
    ("name", "bound", "build", "read", "reason", "phrase"), PERSISTED_PROVIDER_SCALARS, ids=SCALAR_IDS
)
def test_a_provider_scalar_exactly_at_its_recorded_bound_is_mapped_and_persisted(
    name: str, bound: int, build: Callable[[Any], ParsedDocumentResult], read: Any, reason: str, phrase: str
) -> None:
    # Every scalar bound is a ceiling, not a threshold. The pair with the test
    # below is what makes the number recorded in the policy readable as the limit
    # it actually is, rather than as one the adapter is off by one about.
    del name, reason, phrase

    result = build(bound)

    assert result.call.status == "completed"
    assert read(result) == bound


@pytest.mark.parametrize(
    ("name", "bound", "build", "read", "reason", "phrase"), PERSISTED_PROVIDER_SCALARS, ids=SCALAR_IDS
)
def test_a_provider_scalar_one_past_its_recorded_bound_fails_closed(
    name: str, bound: int, build: Callable[[Any], ParsedDocumentResult], read: Any, reason: str, phrase: str
) -> None:
    del name, read

    with pytest.raises(DoclingParseError, match=phrase) as failure:
        build(bound + 1)

    call = cast(ParserCall, failure.value.call)
    # A recorded bound names itself: the provider did nothing malformed, this
    # adapter refused to persist a scalar past the ceiling its receipt states.
    assert (call.status, call.failure_reason) == ("failed", reason)
    assert call.error_type == "_MappingLimitExceeded"
    # And the refused number is nowhere in the record it produced.
    assert str(bound + 1) not in json.dumps(call.as_json_dict())


@pytest.mark.parametrize(
    ("name", "bound", "build", "read", "reason", "phrase"), PERSISTED_PROVIDER_SCALARS, ids=SCALAR_IDS
)
def test_an_arbitrary_precision_provider_scalar_is_refused_rather_than_persisted(
    name: str, bound: int, build: Callable[[Any], ParsedDocumentResult], read: Any, reason: str, phrase: str
) -> None:
    """The condition the bounds exist for: a Python integer has no width at all.

    Without a ceiling each of these landed verbatim on a frozen record and went
    on to a receipt and a Parquet row — a value no fixed-width column can hold and
    no reader has a comparator for.
    """
    del name, bound, read, phrase

    with pytest.raises(DoclingParseError, match="bound") as failure:
        build(ARBITRARY_PRECISION)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", reason)
    assert str(ARBITRARY_PRECISION) not in json.dumps(call.as_json_dict())


@pytest.mark.parametrize("value", [True, False, -1, "1", 1.0, None])
@pytest.mark.parametrize(
    ("name", "bound", "build", "read", "reason", "phrase"), PERSISTED_PROVIDER_SCALARS, ids=SCALAR_IDS
)
def test_a_provider_scalar_that_is_not_a_plain_nonnegative_integer_fails_closed(
    name: str,
    bound: int,
    build: Callable[[Any], ParsedDocumentResult],
    read: Any,
    reason: str,
    phrase: str,
    value: Any,
) -> None:
    # ``bool`` is the one that matters most: it *is* an ``int`` in Python, so a
    # lenient reader records ``True`` as page 1 and ``False`` as depth 0. None of
    # these is a scalar the provider's model declares, so none is recorded as one.
    del name, bound, read, reason, phrase

    with pytest.raises(DoclingParseError) as failure:
        build(value)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "malformed_element")


def test_a_page_count_past_the_recorded_bound_fails_closed_without_claiming_a_page_limit() -> None:
    """``page_count`` is a persisted provider scalar like any other.

    And bounding it is *not* a page limit: by the time a page collection is
    counted the conversion is long over, which is why ``page_limit_enforced``
    stays false beside a stated ``max_page_number``.
    """

    class ManyPages:
        def __len__(self) -> int:
            return MAX_PAGE_NUMBER + 1

    document = FakeDoclingDocument(sample_items(), pages=ManyPages())

    with pytest.raises(DoclingParseError, match="more pages than the recorded page-number bound") as failure:
        parser(converter=FakeConverter(FakeConversion(document))).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "page_count_limit")
    assert (call.policy.max_page_number, call.policy.page_limit_enforced) == (MAX_PAGE_NUMBER, False)


PERSISTED_INTEGER_BOUNDS: dict[str, str | int] = {
    # Provider scalars, read verbatim off a provider object.
    "page_number": "max_page_number",
    "char_start": "max_provenance_char_index",
    "char_end": "max_provenance_char_index",
    "tree_level": "max_tree_depth",
    "page_count": "max_page_number",
    "provider_error_count": "max_provider_errors",
    # Table geometry, provider-declared and provider-placed.
    "row_start": "max_table_dimension",
    "row_end": "max_table_dimension",
    "column_start": "max_table_dimension",
    "column_end": "max_table_dimension",
    "row_count": "max_table_dimension",
    "column_count": "max_table_dimension",
    # Counts and offsets this adapter derives, each under the bound that made it.
    "ordinal": "max_items",
    "element_ordinal": "max_items",
    "element_count": "max_items",
    "usable_element_count": "max_items",
    "omission_count": "max_items",
    "elements_without_coordinates": "max_items",
    "start_char": "max_mapped_characters",
    "end_char": "max_mapped_characters",
    "character_count": "max_mapped_characters",
    "usable_character_count": "max_mapped_characters",
    "table_count": "max_tables",
    "table_cell_count": "max_total_table_cells",
    "source_bytes": "max_source_bytes",
    # One parse is one call; this adapter never retries.
    "attempt_count": 1,
}
"""Every integer field an accepted record carries, and the ceiling it sits under.

A ``str`` names a :class:`ParserPolicy` field, so the comparator travels in the
receipt beside the count; an ``int`` is a bound the adapter's own structure fixes.
"""


def test_no_integer_in_an_accepted_record_is_an_unbounded_provider_scalar() -> None:
    """The sweep behind the individual bounds: nothing numeric escapes one.

    Checking the three scalars that were unbounded says nothing about the fourth
    nobody thought of. This walks every field of a completed
    :class:`ParsedDocumentResult` instead, and fails on any integer whose name is
    not in the table above — so a new provider-controlled integer cannot be added
    to a record without a bound being chosen for it here.
    """
    result = parse()
    policy = result.call.policy
    seen: set[str] = set()

    def bounded(value: Any, path: str) -> None:
        # The policy holds the bounds themselves, and a bound is not bounded.
        if not isinstance(value, int) or isinstance(value, bool) or path.startswith("result.call.policy"):
            return
        field = path.rsplit(".", 1)[-1].split("[", 1)[0]
        assert field in PERSISTED_INTEGER_BOUNDS, f"{path} is an integer with no stated bound"
        limit = PERSISTED_INTEGER_BOUNDS[field]
        ceiling = limit if isinstance(limit, int) else getattr(policy, limit)
        assert value <= ceiling, f"{path} is {value}, past the {limit} of {ceiling}"
        seen.add(field)

    walk_records(result, "result", bounded)

    # And the table describes this record rather than a remembered one: every
    # entry in it is an integer the sample parse really produces.
    assert seen == set(PERSISTED_INTEGER_BOUNDS)


# --- exact built-in provider scalars ----------------------------------------
#
# Every bound above is a comparison, a length, or a membership test. A provider
# subtype of ``int`` or ``str`` decides how each of those is answered, so the
# bounds only mean what they say if the value being bounded is really the built-in
# type. These tests drive each reader family with a subtype built to answer
# whatever it likes, at the reader and through a whole parse.


class ComparingInt(int):
    """A provider integer that answers every comparison the way it likes.

    An ``int`` subclass *is* an ``int``, so ``isinstance`` admits it and every
    bound the adapter applies is one of these methods. One of these carrying
    ``10**100`` — or ``-1`` — used to walk straight through the check and onto a
    frozen record.
    """

    def __lt__(self, other: Any) -> bool:
        return False

    def __le__(self, other: Any) -> bool:
        return True

    def __gt__(self, other: Any) -> bool:
        return False

    def __ge__(self, other: Any) -> bool:
        return True


class ConvertingInt(int):
    """A provider integer whose ``__int__`` reports something other than its value.

    ``int(value)`` was the normalization the readers used, and it runs *this*: the
    number a record carried was whichever one the provider's own conversion named,
    not the one the bound had been checked against.
    """

    def __int__(self) -> int:
        return 1

    def __index__(self) -> int:
        return 1


BUILTIN_SCALARS = (bool, int, float, str, type(None))
"""The only types a project-owned record may hold a scalar as — exactly, not by kind."""


def assert_only_builtin_scalars(record: Any, path: str) -> set[str]:
    """Fail on any scalar in a record tree whose type is not exactly a built-in.

    Returns every path the sweep really visited, so a caller can assert that a
    field it cares about was covered rather than trusting that the walk reached
    it.
    """
    visited: set[str] = set()

    def exact_builtin(value: Any, at: str) -> None:
        if isinstance(value, tuple) or (dataclasses.is_dataclass(value) and not isinstance(value, type)):
            return
        visited.add(at)
        assert type(value) in BUILTIN_SCALARS, f"{at} is a {type(value)!r}, not an exact built-in"

    walk_records(record, path, exact_builtin)
    return visited


def test_the_probes_really_are_the_bypass_they_model() -> None:
    """The subtypes above have to defeat the checks they stand in for, or prove nothing."""
    huge = ComparingInt(ARBITRARY_PRECISION)
    assert isinstance(huge, int) and type(huge) is not int
    # Every ``isinstance``-and-compare guard this adapter used to run, answered.
    assert not huge > MAX_PAGE_NUMBER
    assert not ComparingInt(-1) < 1
    # And the conversion that was supposed to normalize one.
    assert int(ConvertingInt(ARBITRARY_PRECISION)) == 1


def table_item(cells: tuple[Any, ...], rows: Any = 1, columns: Any = 1) -> FakeItem:
    """One table whose declared shape and cells are exactly what a test hands over."""
    return FakeItem(
        label="table",
        text=OMITTED,
        data=FakeTableData(cells, rows=rows, columns=columns),
        self_ref="#/tables/0",
        prov=[],
    )


def parse_at_character_span_start(value: Any) -> ParsedDocumentResult:
    located = FakeItem(text="alpha", self_ref="#/texts/0", prov=[FakeProvenance(charspan=(value, 5))])
    return parse(items=[located])


def parse_at_table_row_count(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=0, column=0, text="x"),), rows=value, columns=1)])


def parse_at_table_column_count(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=0, column=0, text="x"),), rows=1, columns=value)])


def parse_at_cell_row_offset(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=value, row_end=1, column=0, text="x"),))])


def parse_at_cell_column_offset(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=0, column=value, col_end=1, text="x"),))])


def parse_at_cell_row_span(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=0, row_end=1, column=0, text="x", row_span=value),))])


def parse_at_cell_column_span(value: Any) -> ParsedDocumentResult:
    return parse(items=[table_item((FakeTableCell(row=0, column=0, col_end=1, text="x", col_span=value),))])


def parse_at_heading_level(value: Any) -> ParsedDocumentResult:
    return parse(items=[FakeItem(label="section_header", text="Scope", self_ref="#/texts/0", prov=[], level=value)])


PROVIDER_INTEGER_POSITIONS = [
    ("provenance page number", parse_at_page_number),
    ("provenance charspan start", parse_at_character_span_start),
    ("provenance charspan end", parse_at_character_index),
    ("iterate_items tree level", parse_at_tree_level),
    ("declared table row count", parse_at_table_row_count),
    ("declared table column count", parse_at_table_column_count),
    ("table cell row offset", parse_at_cell_row_offset),
    ("table cell column offset", parse_at_cell_column_offset),
    ("table cell row span", parse_at_cell_row_span),
    ("table cell column span", parse_at_cell_column_span),
    ("heading level", parse_at_heading_level),
]
"""Every position a provider integer enters this adapter through.

The three in :data:`PERSISTED_PROVIDER_SCALARS` land verbatim on a record; the
table geometry sizes an allocation and bounds a grid; the heading level bounds
every element's recorded heading path. All of them are read through one reader,
and each is driven here rather than only the family that was fixed last.
"""

INTEGER_POSITION_IDS = [name for name, _ in PROVIDER_INTEGER_POSITIONS]


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(ComparingInt(ARBITRARY_PRECISION), id="comparison-overriding-huge"),
        pytest.param(ComparingInt(-1), id="comparison-overriding-negative"),
        pytest.param(ConvertingInt(ARBITRARY_PRECISION), id="conversion-overriding-huge"),
    ],
)
@pytest.mark.parametrize(("name", "build"), PROVIDER_INTEGER_POSITIONS, ids=INTEGER_POSITION_IDS)
def test_no_provider_integer_position_accepts_an_int_subtype(
    name: str, build: Callable[[Any], ParsedDocumentResult], value: Any
) -> None:
    """A subtype is refused on its type, before any bound is asked of it.

    Not one of these is a number the pinned releases can emit — every field is a
    plain ``int`` there — and each of them defeats a check this adapter makes, so
    the type is settled first and the comparison that follows is ``int``'s own.
    """
    del name

    with pytest.raises(DoclingParseError) as failure:
        build(value)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "malformed_element")
    assert str(ARBITRARY_PRECISION) not in json.dumps(call.as_json_dict(), allow_nan=False)
    # And nothing of the subtype survives into the record the refusal carries.
    assert_only_builtin_scalars(call, "call")


@pytest.mark.parametrize("value", [True, False, "1", 1.0, 1 + 0j])
@pytest.mark.parametrize(("name", "build"), PROVIDER_INTEGER_POSITIONS, ids=INTEGER_POSITION_IDS)
def test_no_provider_integer_position_accepts_a_value_that_is_not_an_integer(
    name: str, build: Callable[[Any], ParsedDocumentResult], value: Any
) -> None:
    # ``bool`` matters most: it *is* an ``int`` in Python, so a lenient reader
    # records ``True`` as page one, depth one, a one-row grid, or heading level
    # one. A heading level used to fall back silently for every value here rather
    # than refusing the one the provider really declared.
    del name

    with pytest.raises(DoclingParseError) as failure:
        build(value)

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


@pytest.mark.parametrize(
    ("level", "accepted"),
    [(1, True), (MAX_HEADING_LEVEL, True), (0, False), (-1, False), (MAX_HEADING_LEVEL + 1, False)],
)
def test_a_heading_level_is_held_to_the_range_the_pinned_release_declares(level: int, accepted: bool) -> None:
    """docling-core declares ``LevelNumber`` as ``Annotated[int, Ge(1), Le(100)]``.

    So one and one hundred are levels, and nothing outside is. A level below the
    range used to fall back to the default rather than being refused, which
    recorded a heading context the provider never described.
    """
    if accepted:
        assert parse_at_heading_level(level).call.status == "completed"
        return

    with pytest.raises(DoclingParseError) as failure:
        parse_at_heading_level(level)

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


class ShiftingFloat(float):
    """A coordinate that answers finite the first time it is converted and then infinite.

    The bounding-box reader called ``float(value)`` once to test finiteness and
    again to store, so a subtype answering differently each time put a number with
    no JSON spelling on a frozen record — one that then refused to serialize at the
    persistence boundary, long after the parse had reported success.
    """

    def __init__(self, value: float) -> None:
        del value
        self.converted = 0

    def __float__(self) -> float:
        self.converted += 1
        return 1.0 if self.converted == 1 else math.inf


def test_a_bounding_box_coordinate_must_be_an_exact_builtin_number() -> None:
    shifting = ShiftingFloat(1.0)
    # The probe really is the bypass: the old reader tested one value and stored
    # another, and neither call is a comparison this adapter could have caught.
    assert (float(shifting), math.isinf(float(shifting))) == (1.0, True)

    with pytest.raises(DoclingParseError, match="bounding box is missing or non-finite") as failure:
        parse(items=[FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(left=ShiftingFloat(1.0)))])])

    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"
    # An exact ``int`` still converts, because ``int.__float__`` is the built-in's.
    document = parse(items=[FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(left=72))])]).document
    assert type(document.elements[0].regions[0].left) is float


def test_a_caller_byte_bound_that_is_an_int_subtype_is_refused_before_it_is_recorded() -> None:
    # ``max_source_bytes`` is compared against the real byte count and then hashed
    # into ``parser_id`` and published in the policy, so a subtype would both
    # weaken the bound and put a caller's own type in a record.
    with pytest.raises(DoclingConfigurationError, match="max_source_bytes must be a positive integer"):
        parser(max_source_bytes=ComparingInt(-1))
    with pytest.raises(DoclingConfigurationError, match="max_source_bytes must be a positive integer"):
        parser(max_source_bytes=ConvertingInt(ARBITRARY_PRECISION))


def test_every_scalar_in_a_returned_record_is_an_exact_builtin_type() -> None:
    """The structural half of the tests above: what a record is allowed to hold.

    A provider subtype passing a bound is only harmful because it is then
    *persisted*. This walks a completed result and fails on any scalar whose type
    is not exactly a built-in — a subclass of ``str`` or ``int`` included, which
    ``isinstance`` would have called a match.
    """
    visited = assert_only_builtin_scalars(parse(), "result")

    # And the sweep really reached the fields an injected boundary decides, rather
    # than passing because the walk stopped short of them. Both version fields
    # come from ``version_reader``, which is a caller-supplied callable.
    assert {"result.call.package_version", "result.call.core_package_version"} <= visited
    assert "result.call.parser_id" in visited
    # A record built the long way round covers them too: a reader handing back
    # equal-but-distinct strings still persists this module's own constants.
    equal = {DOCLING_PACKAGE: "".join(DOCLING_VERSION), DOCLING_CORE_PACKAGE: "".join(DOCLING_CORE_VERSION)}
    injected = parser(version_reader=equal.get).parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    assert {"result.call.package_version", "result.call.core_package_version"} <= assert_only_builtin_scalars(
        injected, "result"
    )
    assert injected.call.package_version is DOCLING_VERSION
    assert injected.call.core_package_version is DOCLING_CORE_VERSION

    # And the table header flags, which are the record's only booleans a provider
    # supplies. The fixture populates them with both values, so the sweep visits a
    # true flag and a false one rather than passing on a table that has neither.
    document = parse().document
    flags = {
        f"result.document.tables[{table_index}].cells[{cell_index}].{name}"
        for table_index, table in enumerate(document.tables)
        for cell_index in range(len(table.cells))
        for name in ("column_header", "row_header")
    }
    assert flags and flags <= visited
    recorded = {
        one for table in document.tables for cell in table.cells for one in (cell.column_header, cell.row_header)
    }
    assert recorded == {True, False}
    assert all(type(one) is bool for one in recorded)


# --- exact built-in provider strings ----------------------------------------


class ShortLyingText(str):
    """Text that understates its own length, so every length bound passes on a lie.

    ``len()`` is what bounds an item's text, a formula's ``orig``, a cell value,
    and a reference, and a ``str`` subclass answers it. One of these reached the
    joins, the serialization, and the persisted records holding ten thousand
    characters while claiming one.
    """

    def __len__(self) -> int:
        return 1


def impostor_token(claimed: str, held: str) -> str:
    """A ``str`` subtype that answers any closed-set test as ``claimed``.

    Membership in a ``frozenset`` is a hash and an equality, and a subclass owns
    both — and its comparison is tried *first*, because it is the subtype. So one
    of these passed every closed provider-token set this adapter validates against
    and was then recorded as the label, layer, status, or format it claimed.
    """

    class Impostor(str):
        def __eq__(self, other: Any) -> bool:
            return other == claimed

        def __ne__(self, other: Any) -> bool:
            return other != claimed

        def __hash__(self) -> int:
            return hash(claimed)

    return Impostor(held)


OVERSIZED_TEXT = "X" * 10_000


def test_the_string_probes_really_are_the_bypass_they_model() -> None:
    lying = ShortLyingText(OVERSIZED_TEXT)
    assert isinstance(lying, str) and len(lying) == 1 and str.__len__(lying) == len(OVERSIZED_TEXT)
    impostor = impostor_token("body", OVERSIZED_TEXT)
    assert isinstance(impostor, str) and impostor in CONTENT_LAYERS and str.__len__(impostor) == len(OVERSIZED_TEXT)


PROVIDER_TEXT_POSITIONS: list[tuple[str, Callable[[Any], list[Any]], str]] = [
    (
        "element text",
        lambda text: [FakeItem(text=text, self_ref="#/texts/0", prov=[])],
        "element text is not a string",
    ),
    (
        "formula orig",
        lambda text: [FakeItem(label="formula", text="", orig=text, self_ref="#/texts/0", prov=[])],
        "element orig is not a string",
    ),
    (
        "table cell text",
        lambda text: [table_item((FakeTableCell(row=0, column=0, text=text),))],
        "table cell text is not a string",
    ),
    (
        "self reference",
        lambda text: [FakeItem(text="alpha", self_ref=text, prov=[])],
        "element reference is missing",
    ),
    (
        "parent reference",
        lambda text: [FakeItem(text="alpha", parent=text, prov=[])],
        "in-document reference is malformed",
    ),
    (
        "caption reference",
        lambda text: [captioned_table([text])],
        "in-document reference is malformed",
    ),
]

TEXT_POSITION_IDS = [name for name, *_ in PROVIDER_TEXT_POSITIONS]


@pytest.mark.parametrize(("name", "build", "message"), PROVIDER_TEXT_POSITIONS, ids=TEXT_POSITION_IDS)
def test_no_provider_text_position_accepts_a_str_subtype(
    name: str, build: Callable[[Any], list[Any]], message: str
) -> None:
    """Refused on its type, before any length bound is asked of it.

    Refusing late is not refusing: an oversized value that understates itself
    reaches the joins, the table serialization, and the record before a length
    bound could notice, and the string that lands there is a provider type either
    way.
    """
    del name

    with pytest.raises(DoclingParseError, match=message) as failure:
        parse(items=build(ShortLyingText(OVERSIZED_TEXT)))

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "malformed_element")
    assert OVERSIZED_TEXT not in json.dumps(call.as_json_dict(), allow_nan=False)
    assert_only_builtin_scalars(call, "call")


PROVIDER_TOKEN_POSITIONS: list[tuple[str, str, Callable[[Any], DoclingDocumentParser], str, str]] = [
    (
        "element label",
        "text",
        lambda token: parser(items=[FakeItem(label=token, text="alpha", self_ref="#/texts/0", prov=[])]),
        "element label is missing or malformed",
        "malformed_element",
    ),
    (
        "content layer",
        "body",
        lambda token: parser(items=[FakeItem(text="alpha", self_ref="#/texts/0", prov=[], content_layer=token)]),
        "content layer is not one this parser requested",
        "malformed_element",
    ),
    (
        "conversion status",
        "success",
        lambda token: parser(converter=FakeConverter(broken_conversion(status=FakeEnum(token)))),
        "conversion status is missing or malformed",
        "malformed_conversion",
    ),
    (
        "provider input format",
        FORMAT_DOCX,
        lambda token: parser(converter=FakeConverter(broken_conversion(input=SimpleNamespace(format=FakeEnum(token))))),
        "input format is malformed",
        "malformed_conversion",
    ),
]

TOKEN_POSITION_IDS = [name for name, *_ in PROVIDER_TOKEN_POSITIONS]


@pytest.mark.parametrize(
    ("name", "claimed", "build", "message", "reason"), PROVIDER_TOKEN_POSITIONS, ids=TOKEN_POSITION_IDS
)
def test_no_closed_token_set_accepts_a_str_subtype_that_claims_membership(
    name: str, claimed: str, build: Callable[[Any], DoclingDocumentParser], message: str, reason: str
) -> None:
    del name

    with pytest.raises(DoclingParseError, match=message) as failure:
        build(impostor_token(claimed, OVERSIZED_TEXT)).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", reason)
    assert OVERSIZED_TEXT not in json.dumps(call.as_json_dict(), allow_nan=False)


def test_a_pinned_enum_value_is_an_ordinary_builtin_string_and_still_passes() -> None:
    """The exactness rule costs the releases nothing, which is why it is affordable.

    A pinned ``str`` enum's ``.value`` is a plain built-in string, so every token
    the adapter validates against a closed set still reads normally. A
    real-provider test holds this against the enums themselves.
    """
    result = parse()

    assert result.call.status == "completed"
    assert {type(one.kind) for one in result.document.elements} == {str}
    assert {type(one.content_layer) for one in result.document.elements} == {str}
    assert (type(result.call.conversion_status), type(result.call.provider_input_format)) == (str, str)


class ForgedCategory(str):
    """A failure category that keeps its own type through the one lenient reader."""

    def lower(self) -> str:
        return self


def test_a_failure_category_is_copied_into_a_builtin_string_even_though_it_is_read_leniently() -> None:
    """The one deliberately lenient boundary still refuses to persist a provider type.

    Error categories are read on a parse that has already failed, where demanding
    exact membership would replace a real finding with a metadata complaint. So
    the shape stays lenient and the *type* does not: the value is copied into a
    built-in string before anything measures it or records it.
    """
    forged = SimpleNamespace(category=SimpleNamespace(value=ForgedCategory("policy")), error_message="boom")
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), errors=(forged,)))

    with pytest.raises(DoclingParseError, match="reported conversion errors") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert call.provider_error_categories == ("policy",)
    assert {type(category) for category in call.provider_error_categories} == {str}


class ForgedSourceName(str):
    """A logical identifier whose ``encode`` returns bytes it did not hold.

    ``source_name_sha256`` is the join key a reader trusts to cover the caller's
    exact identifier, and it is built from ``encode`` — which a subtype defines.
    """

    def encode(self, *arguments: Any, **options: Any) -> bytes:
        return b"not the caller's identifier"


def test_a_source_name_that_is_a_str_subtype_is_refused_before_an_identity_is_minted() -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    with pytest.raises(DoclingParseError, match="source_name must be a string") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=ForgedSourceName("rule.docx"))

    # No record, for the same reason a non-string gets none: there is no honest
    # identity to build one from, and a fabricated digest is worse than none.
    assert failure.value.call is None
    assert converter.calls == []
    # And the two helpers that mint the identity refuse it standing on their own.
    with pytest.raises(DoclingParseError, match="source_name must be a string"):
        encoded_source_name(ForgedSourceName("rule.docx"))
    with pytest.raises(DoclingParseError, match="source_name must be a string"):
        sanitized_source_name(ForgedSourceName("rule.docx"))


class ForgedMediaType(str):
    """A media type that understates its length and splits into something else."""

    def __len__(self) -> int:
        return 10

    def split(self, *arguments: Any, **options: Any) -> list[str]:
        return ["application/pdf"]


def test_a_media_type_that_is_a_str_subtype_cannot_choose_the_format_that_is_recorded() -> None:
    """A subtype used to decide the parse: it passed the length bound on a lie and
    then handed back whatever ``split`` chose, so a caller naming nothing in
    particular got a receipt saying ``pdf``.
    """
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))
    forged = ForgedMediaType("application/pdf; " + "f" * 5_000)

    with pytest.raises(DoclingParseError, match="media_type is not a string") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name="rule", media_type=forged)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.input_format, call.media_type) == ("unsupported_input", FORMAT_UNKNOWN, None)
    assert converter.calls == []
    assert "f" * 100 not in json.dumps(call.as_json_dict(), allow_nan=False)


class ForgedErrorName(str):
    """A class name that passes the pattern and then formats itself as something else."""

    def __format__(self, specification: str) -> str:
        del specification
        return "leaked /secret/scan.docx " + "X" * 10_000


class ForgedNameMetaclass(type):
    """A metaclass whose ``__name__`` hands back that subtype, the way a provider can."""

    @property
    def __name__(cls) -> str:  # noqa: N805  # a metaclass property receives the class
        return ForgedErrorName("RuntimeError")


def test_an_exception_type_name_that_is_a_str_subtype_never_reaches_a_message_or_a_record() -> None:
    """A name is only safe if it is a built-in string when it is interpolated.

    ``bounded_error_type`` used to hand back whatever ``__name__`` returned once
    the pattern matched, and the failure message interpolates that value — so a
    subtype defining ``__format__`` wrote its own payload into the public message.
    """
    assert bounded_error_type(ForgedErrorName("RuntimeError")) == FALLBACK_ERROR_TYPE
    forged = ForgedNameMetaclass("Boom", (RuntimeError,), {})("boom")
    assert bounded_error_type(forged) == FALLBACK_ERROR_TYPE

    with pytest.raises(DoclingParseError) as failure:
        parser(converter=FakeConverter(error=forged)).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.error_type) == ("provider_error", FALLBACK_ERROR_TYPE)
    payload = json.dumps(call.as_json_dict(), allow_nan=False)
    for fragment in ("leaked", "secret", "scan.docx", "XXXXXXXXXX"):
        assert fragment not in str(failure.value)
        assert fragment not in payload


def test_the_recorded_error_type_bound_is_the_only_length_comparator() -> None:
    """One number decides what a recorded ``error_type`` may be, at both ends.

    ``_EXCEPTION_TYPE_NAME`` used to encode the same bound at import, so the
    compiled pattern and the recorded ``max_error_type_chars`` were two
    enforcement points that could disagree: raising the constant left the pattern
    capped, and lowering it below the fallback's own length made
    :func:`bounded_error_type` return a token longer than the comparator beside
    it. The pattern now says shape and nothing else.
    """
    # Shape only: the pattern accepts a name of any length, and the explicit
    # length check is what refuses one.
    assert adapter_module._EXCEPTION_TYPE_NAME.fullmatch("Z" * 100_000)
    assert bounded_error_type("Z" * MAX_ERROR_TYPE_CHARS) == "Z" * MAX_ERROR_TYPE_CHARS
    assert bounded_error_type("Z" * (MAX_ERROR_TYPE_CHARS + 1)) == FALLBACK_ERROR_TYPE
    # The fallback is a token that fits every bound this adapter supports, and the
    # floor says so rather than leaving it to arithmetic nobody stated.
    assert adapter_module.MIN_ERROR_TYPE_CHARS == len(FALLBACK_ERROR_TYPE)
    assert MAX_ERROR_TYPE_CHARS >= adapter_module.MIN_ERROR_TYPE_CHARS


@pytest.mark.parametrize("bound", ["recorded", "lowered", "raised"])
def test_a_supported_error_type_bound_is_enforced_directly_and_through_a_record(
    monkeypatch: pytest.MonkeyPatch, bound: str
) -> None:
    """Every supported bound, at the helper and through a complete failure record.

    A bound is only real if the token that reaches ``ParserCall.error_type`` obeys
    it — including the fallback, which is why the floor exists — and if raising it
    really admits the longer names it now allows.
    """
    limit = {
        "recorded": MAX_ERROR_TYPE_CHARS,
        "lowered": adapter_module.MIN_ERROR_TYPE_CHARS,
        "raised": 128,
    }[bound]
    if bound != "recorded":
        monkeypatch.setattr(adapter_module, "MAX_ERROR_TYPE_CHARS", limit)

    kept = "Z" * limit
    refused = "Z" * (limit + 1)

    assert bounded_error_type(kept) == kept
    assert bounded_error_type(refused) == FALLBACK_ERROR_TYPE
    assert len(bounded_error_type(refused)) <= limit
    assert bounded_error_type(FALLBACK_ERROR_TYPE) == FALLBACK_ERROR_TYPE

    def recorded(name: str) -> str | None:
        built = parser(converter=FakeConverter(error=type(name, (RuntimeError,), {})("boom /secret/scan.docx")))
        with pytest.raises(DoclingParseError) as failure:
            built.parse(SOURCE_BYTES, source_name=SOURCE_NAME)
        call = cast(ParserCall, failure.value.call)
        assert call.failure_reason == "provider_error"
        return call.error_type

    assert recorded(kept) == kept
    assert recorded(refused) == FALLBACK_ERROR_TYPE
    assert len(cast(str, recorded(refused))) <= limit


# --- provider exception type identity ---------------------------------------


HOSTILE_EXCEPTION_NAME = "Secret" + "X" * 100_000
"""A dynamically built class name: oversized, and named after what it might hold."""


def hostile_exception(name: str = HOSTILE_EXCEPTION_NAME) -> BaseException:
    """A provider exception whose *type* is the hostile part, not its message."""
    return type(name, (RuntimeError,), {})("boom /secret/scan.docx")


class UnreadableName(type):
    """A metaclass that raises from ``__name__``, the way a hostile provider can."""

    @property
    def __name__(cls) -> str:  # noqa: N805  # a metaclass property receives the class
        raise RuntimeError("boom /secret/scan.docx")


@pytest.mark.parametrize(
    ("name", "kept"),
    [
        # Ordinary class names, kept whole — including this adapter's own.
        ("ValueError", "ValueError"),
        ("RuntimeError", "RuntimeError"),
        ("UnicodeEncodeError", "UnicodeEncodeError"),
        ("_MappingLimitExceeded", "_MappingLimitExceeded"),
        # Exactly at the bound, and one past it. A name is replaced whole rather
        # than truncated, so no prefix of a hostile one is ever carried.
        ("Z" * MAX_ERROR_TYPE_CHARS, "Z" * MAX_ERROR_TYPE_CHARS),
        ("Z" * (MAX_ERROR_TYPE_CHARS + 1), FALLBACK_ERROR_TYPE),
        ("E" * 100_000, FALLBACK_ERROR_TYPE),
        # Not a class-name token at all: whitespace, a path, a newline, a
        # separator, a leading digit, non-ASCII, and nothing.
        ("Bad Name", FALLBACK_ERROR_TYPE),
        ("/etc/passwd", FALLBACK_ERROR_TYPE),
        ("Bad\nName", FALLBACK_ERROR_TYPE),
        ("Error;DROP TABLE receipts", FALLBACK_ERROR_TYPE),
        ("9Error", FALLBACK_ERROR_TYPE),
        ("Erreü", FALLBACK_ERROR_TYPE),
        ("", FALLBACK_ERROR_TYPE),
        # Credential-shaped: a class name is provider-controlled text, and a
        # dynamically built one can carry the thing it was built around.
        ("SecretKeyError", FALLBACK_ERROR_TYPE),
        ("BearerTokenAuthError", FALLBACK_ERROR_TYPE),
        ("AKIAIOSFODNN7EXAMPLEError", FALLBACK_ERROR_TYPE),
        ("PasswordExpired", FALLBACK_ERROR_TYPE),
        # An opaque token, which is what a signature looks like beside a name.
        ("A" + "0123456789abcdef" * 2, FALLBACK_ERROR_TYPE),
        ("Er" + "9f3a" * 8, FALLBACK_ERROR_TYPE),
    ],
)
def test_an_exception_type_name_is_kept_whole_or_replaced_whole(name: str, kept: str) -> None:
    assert bounded_error_type(name) == kept
    # Whatever survives is short, ASCII, and shaped like an identifier.
    result = bounded_error_type(name)
    assert len(result) <= MAX_ERROR_TYPE_CHARS
    assert result.isascii() and result.isprintable()


def test_anything_that_names_an_exception_type_is_reduced_to_one_safe_token() -> None:
    """The helper takes the three things a caller actually has, and never raises.

    An exception, a name, or ``None`` — because reading ``__name__`` can itself
    raise from a hostile metaclass, and no failure path may fail again while
    recording why it failed.
    """
    assert bounded_error_type(RuntimeError("boom /secret/scan.docx")) == "RuntimeError"
    assert bounded_error_type(hostile_exception()) == FALLBACK_ERROR_TYPE
    assert bounded_error_type(UnreadableName("Unreadable", (Exception,), {})()) == FALLBACK_ERROR_TYPE
    for value in (None, 17, object(), b"RuntimeError", ["RuntimeError"]):
        assert bounded_error_type(value) == FALLBACK_ERROR_TYPE
    # The fallback is fixed and non-secret by construction, not a truncation.
    assert FALLBACK_ERROR_TYPE == "provider_exception"
    assert bounded_error_type(FALLBACK_ERROR_TYPE) == FALLBACK_ERROR_TYPE


def exploding_metadata(error: BaseException) -> FakeConversion:
    """A conversion whose error category raises ``error`` while it is read."""

    class Category:
        def __str__(self) -> str:
            raise error

    return broken_conversion(errors=[SimpleNamespace(category=Category())])


def unreadable_document(error: BaseException) -> FakeConversion:
    """A conversion whose document raises ``error`` from ``iterate_items``."""

    class Document:
        def iterate_items(self, **options: Any) -> Any:
            del options
            raise error

        pages = None

    return broken_conversion(document=Document())


def failing_mapping(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Make the text-mapping stage raise a parse error whose *class* is hostile.

    A subclass of the public ``DoclingParseError``, not one of the adapter's own
    private refusals — which is exactly what a hostile provider can raise, and why
    the recorded reason below is ``provider_error`` rather than ``text_mapping``.
    """
    hostile = type(name, (DoclingParseError,), {})

    def refuse(readings: Any) -> Any:
        del readings
        raise hostile("mapped text could not be assembled")

    monkeypatch.setattr(adapter_module, "_assemble", refuse)


@pytest.mark.parametrize("name", [HOSTILE_EXCEPTION_NAME, "AwsSecretAccessKeyError", "Bad Name /secret/scan.docx"])
@pytest.mark.parametrize("path", ["converter_call", "conversion_metadata", "document_iteration", "mapping_failure"])
def test_a_provider_exception_name_reaches_neither_the_message_nor_the_record(
    monkeypatch: pytest.MonkeyPatch, path: str, name: str
) -> None:
    """A class name is provider-controlled text on every path that records one.

    A dynamically built exception can carry a hundred thousand characters or a
    credential in its ``__name__``, and that name reached both the public failure
    message and a persisted ``ParserCall.error_type``. All four paths reduce it to
    one bounded, sanitized token first.
    """
    expected = {
        "converter_call": "provider_error",
        "conversion_metadata": "malformed_conversion",
        "document_iteration": "provider_error",
        # A ``DoclingParseError`` subclass nothing in this module built is provider
        # output like any other exception, however it is spelled.
        "mapping_failure": "provider_error",
    }[path]
    if path == "mapping_failure":
        failing_mapping(monkeypatch, name if name.isidentifier() else HOSTILE_EXCEPTION_NAME)
        built = parser()
    elif path == "converter_call":
        built = parser(converter=FakeConverter(error=hostile_exception(name)))
    elif path == "conversion_metadata":
        built = parser(converter=FakeConverter(exploding_metadata(hostile_exception(name))))
    else:
        built = parser(converter=FakeConverter(unreadable_document(hostile_exception(name))))

    with pytest.raises(DoclingParseError) as failure:
        built.parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", expected)
    assert call.error_type == FALLBACK_ERROR_TYPE
    # The public message stays a sentence, not a provider payload.
    message = str(failure.value)
    assert len(message) < 200
    # And the record is complete, strict JSON, and free of the name and its parts.
    payload = json.dumps(call.as_json_dict(), allow_nan=False)
    assert json.loads(payload)["error_type"] == FALLBACK_ERROR_TYPE
    assert len(payload) < 5_000
    for fragment in ("XXXXXXXXXX", "Secret", "secret", "AKIA", "AwsSecret", "scan.docx", "boom"):
        assert fragment not in message
        assert fragment not in payload


# --- whose message may be repeated ------------------------------------------


SECRET_PROVIDER_MESSAGE = (
    "AKIAIOSFODNN7EXAMPLE bearer sk-live-9f3a /var/secrets/scan.docx quoting the source: " + "s" * 100_000
)
"""What a provider exception's message may really hold: a credential, a path, a document."""


class RaisingStatus:
    """A conversion whose ``status`` raises while the adapter reads its metadata.

    Reading provider metadata runs provider code — a pydantic property, a lazily
    built model — and that code chooses which exception it raises, including this
    module's own public one.
    """

    def __init__(self, error: BaseException) -> None:
        self._error = error
        self.document = FakeDoclingDocument(sample_items())
        self.errors: list[Any] = []
        self.input = SimpleNamespace(format=FakeEnum(FORMAT_DOCX))

    @property
    def status(self) -> Any:
        raise self._error


def provider_raised_parse_error(kind: str) -> BaseException:
    """The public parse error a provider may raise, and a subclass of it."""
    if kind == "public":
        return DoclingParseError(SECRET_PROVIDER_MESSAGE)
    return type("ProviderParseError", (DoclingParseError,), {})(SECRET_PROVIDER_MESSAGE)


@pytest.mark.parametrize("kind", ["public", "subclass"])
@pytest.mark.parametrize(
    ("stage", "reason", "stage_text"),
    [
        ("conversion metadata", "malformed_conversion", "docling conversion metadata could not be read"),
        ("document iteration", "provider_error", "docling document could not be read"),
    ],
)
def test_a_provider_raised_parse_error_never_lends_its_message_to_a_public_failure(
    stage: str, reason: str, stage_text: str, kind: str
) -> None:
    """``DoclingParseError`` is public, documented, and importable — so a provider
    may raise it, or subclass it, with a hundred-thousand-character message that
    quotes the source document and a credential. That message used to be copied
    straight into the failure a caller sees, because the trust test was
    ``isinstance``. Only a refusal this module itself built is repeated now.
    """
    error = provider_raised_parse_error(kind)
    conversion = RaisingStatus(error) if stage == "conversion metadata" else unreadable_document(error)

    with pytest.raises(DoclingParseError) as failure:
        parser(converter=FakeConverter(conversion)).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", reason)
    # Fixed stage text plus the bounded, sanitized type name — and nothing else.
    message = str(failure.value)
    assert message == f"{stage_text}: {bounded_error_type(error)}"
    assert len(message) < 200
    payload = json.dumps(call.as_json_dict(), allow_nan=False)
    assert json.loads(payload)["failure_reason"] == reason
    assert len(payload) < 5_000
    for fragment in ("AKIA", "bearer", "sk-live", "secrets", "scan.docx", "ssssssssss"):
        assert fragment not in message
        assert fragment not in payload


@pytest.mark.parametrize("kind", ["public", "subclass"])
def test_a_provider_raised_parse_error_inside_the_mapping_is_provider_output_too(kind: str) -> None:
    """The same rule for the reader that walks provider items.

    ``_read_items`` touches a provider object per field, so any of them can raise
    the public class. A recorded reason of ``malformed_element`` would also be a
    lie: nothing this adapter validated refused anything.
    """

    class RaisingLabel:
        self_ref = "#/texts/0"

        @property
        def label(self) -> Any:
            raise provider_raised_parse_error(kind)

    conversion = FakeConversion(FakeDoclingDocument([RaisingLabel()]))

    with pytest.raises(DoclingParseError) as failure:
        parser(converter=FakeConverter(conversion)).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "provider_error")
    assert str(failure.value).startswith("docling document could not be read: ")
    payload = json.dumps(call.as_json_dict(), allow_nan=False)
    for fragment in ("AKIA", "bearer", "sk-live", "secrets", "scan.docx", "ssssssssss"):
        assert fragment not in str(failure.value)
        assert fragment not in payload


def test_this_modules_own_refusal_still_says_exactly_what_it_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The other half of the rule, so the fix does not blunt real findings.

    A refusal this module states keeps its precise, stable message and its stage
    reason, at every catch boundary — that is what a receipt is read for.
    """
    metadata = FakeConverter(broken_conversion(status=object()))
    with pytest.raises(DoclingParseError, match="docling conversion status is missing or malformed") as refused:
        parser(converter=metadata).parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    assert cast(ParserCall, refused.value.call).failure_reason == "malformed_conversion"

    with pytest.raises(DoclingParseError, match="docling element text is not a string") as element:
        parse(items=[FakeItem(text=17, self_ref="#/texts/0", prov=[])])
    assert cast(ParserCall, element.value.call).failure_reason == "malformed_element"

    # The text-mapping stage is driven directly: no parse reaches its own refusals
    # any more, because every item is measured before it is read.
    def refuse(readings: Any) -> Any:
        del readings
        raise adapter_module._AdapterRefusal("text_round_trip")

    monkeypatch.setattr(adapter_module, "_assemble", refuse)
    with pytest.raises(DoclingParseError, match="does not round-trip through the parsed text") as mapping:
        parse()
    assert cast(ParserCall, mapping.value.call).failure_reason == "text_mapping"


def test_a_recorded_bound_names_itself_however_it_is_reached() -> None:
    # ``_MappingLimitExceeded`` is this module's own refusal too, and the reason it
    # carries survives both trust checks rather than being flattened to a stage.
    with pytest.raises(DoclingParseError, match="page number is past the recorded page-number bound") as failure:
        parse_at_page_number(MAX_PAGE_NUMBER + 1)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.error_type) == ("page_number_limit", "_MappingLimitExceeded")


def test_origin_is_settled_where_the_provider_ran_not_by_an_exceptions_class() -> None:
    """Class identity proves nothing about authorship, so it decides nothing.

    Provider code shares this interpreter: it can import either private class,
    subclass it, and raise it, and it can set any attribute on what it raises. So
    origin is recorded at the point the provider actually ran — every attribute
    read, iterator step, entry shape, length, index, and conversion goes through a
    wrapper that classifies whatever comes out of it as provider output — and what
    survives to a catch boundary is asked only whether it states a code this module
    declares. The completeness of that claim is checked from the source itself, by
    ``test_no_provider_iteration_entry_is_taken_apart_outside_the_provider_boundary``.
    """

    class ExplodingField:
        @property
        def boom(self) -> Any:
            raise forged_refusal("limit")

    # A forged refusal raised out of provider code is provider output, whatever
    # class it claims to be, and it carries no text of its own.
    with pytest.raises(DoclingParseError) as wrapped:
        adapter_module._field(ExplodingField(), "boom")
    assert isinstance(wrapped.value, adapter_module._ProviderFailure)
    assert adapter_module._refusal_code(wrapped.value) is None
    for fragment in SECRET_FRAGMENTS:
        assert fragment not in str(wrapped.value)

    # Iterator advancement and provider ``__str__`` are provider execution too.
    class Exploding:
        def __iter__(self) -> Any:
            return self

        def __next__(self) -> Any:
            raise forged_refusal("refusal")

        def __str__(self) -> str:
            raise forged_refusal("refusal")

    with pytest.raises(adapter_module._ProviderFailure):
        list(adapter_module._provider_entries(Exploding(), "reference_list_malformed"))
    with pytest.raises(adapter_module._ProviderFailure):
        adapter_module._provider_string(Exploding())

    # And a code is only rendered when this module declares it.
    assert adapter_module._refusal_code(adapter_module._AdapterRefusal("item_bound")) == "item_bound"
    assert adapter_module._refusal_code(forged_refusal("refusal")) is None
    assert adapter_module._refusal_code(DoclingParseError("provider text")) is None
    assert adapter_module._refusal_code(RuntimeError("provider text")) is None
    # A subclass a provider defines states a declared code at worst, and then can
    # say only what this module already says for it.
    forged_subclass = type("Forged", (adapter_module._AdapterRefusal,), {})("item_bound")
    assert adapter_module._refusal_code(forged_subclass) == "item_bound"
    assert str(forged_subclass) == adapter_module._REFUSALS["item_bound"][0]


def test_the_receipt_boundary_re_checks_an_error_type_it_was_handed() -> None:
    """Defense in depth at the one place an ``error_type`` becomes published data.

    Every path above already sanitizes; this is what keeps that true of a path
    added later, since a record is the only way an ``error_type`` leaves here.
    """
    built = parser()
    identity = built._identity(SOURCE_BYTES, SOURCE_NAME, None, started=time.monotonic())

    def record(error_type: str | None) -> ParserCall:
        reading = adapter_module._ProviderReading(failure_reason="provider_error", error_type=error_type)
        return built._call_record(identity=identity, started=time.monotonic(), status="failed", reading=reading)

    assert record(HOSTILE_EXCEPTION_NAME).error_type == FALLBACK_ERROR_TYPE
    assert record("Bad Name").error_type == FALLBACK_ERROR_TYPE
    assert record("AwsSecretAccessKeyError").error_type == FALLBACK_ERROR_TYPE
    # A sound name still survives it, and no name at all stays no name at all.
    assert record("RuntimeError").error_type == "RuntimeError"
    assert record(None).error_type is None


# --- a refusal provider code forged -----------------------------------------
#
# An underscore is a convention, not a capability. Provider code runs *in this
# process*: it can import ``_AdapterRefusal`` and ``_MappingLimitExceeded``, build
# one with whatever ``args``, ``code``, and ``reason`` it likes, and raise it from
# a property, an iterator, or a model validator the adapter is reading. So exact
# class identity answers nothing about authorship, and neither an exception's
# message nor an open-ended field on it may reach public or persisted output.

SECRET_REFUSAL_MESSAGE = (
    "AKIAIOSFODNN7EXAMPLE bearer sk-live-9f3a /var/secrets/scan.docx quoting the source: " + "s" * 100_000
)
"""What provider code puts in a forged refusal: a credential, a path, a document."""

SECRET_REFUSAL_REASON = "leaked_reason_AKIAIOSFODNN7EXAMPLE_/var/secrets/scan.docx"
"""And in the open-ended receipt field a limit refusal used to be trusted for."""

SECRET_FRAGMENTS = ("AKIA", "bearer", "sk-live", "secrets", "scan.docx", "ssssssssss", "leaked_reason")

RECORDED_FAILURE_REASONS = frozenset(
    {
        # Refused before the provider ran.
        "unsupported_input", "format_not_implemented", "source_bytes_over_limit",
        # Origin and stage.
        "provider_error", "malformed_conversion", "malformed_element", "text_mapping",
        # What the provider's own account of the parse said.
        "conversion_status", "provider_errors", "provider_format_missing", "format_mismatch",
        "no_elements", "no_usable_text",
        # A recorded mapping bound, each naming itself.
        "item_limit", "table_limit", "table_cell_limit", "total_table_cell_limit",
        "table_cell_character_limit", "table_dimension_limit", "character_limit",
        "character_span_limit", "reference_limit", "caption_reference_limit",
        "page_region_limit", "page_number_limit", "page_count_limit", "tree_depth_limit",
        "provider_error_limit",
    }
)  # fmt: skip
"""Every ``failure_reason`` a record may carry — a closed, adapter-owned set.

Written out here rather than read from the adapter, so a reason invented at a
catch boundary, or one a provider chose, fails this file rather than passing it.
"""


def forged_refusal(kind: str) -> BaseException:
    """One of this module's own private refusals, built the way provider code can.

    ``__new__`` plus ``BaseException.__init__`` rather than the constructor: the
    type is exactly the private class either way, ``args`` — and with it
    ``str()`` — is whatever the forger chose, and the receipt fields the adapter
    reads are ordinary attributes any object may carry.
    """
    forged = {"refusal": adapter_module._AdapterRefusal, "limit": adapter_module._MappingLimitExceeded}[kind]
    error = forged.__new__(forged)
    BaseException.__init__(error, SECRET_REFUSAL_MESSAGE)
    error.code = SECRET_REFUSAL_MESSAGE
    error.reason = SECRET_REFUSAL_REASON
    return error


def forged_refusal_with_code(kind: str, code: str) -> BaseException:
    """A forged private refusal stating a code this adapter really declares.

    Strictly worse than an undeclared one. A declared code *renders*: a forged
    ``item_bound`` selects this adapter's own fixed limit text and puts
    ``item_limit`` in a published receipt for a bound nothing came near. The
    open-ended fields stay hostile beside it, so anything read off the exception
    still shows up.
    """
    error = forged_refusal(kind)
    error.code = code  # ty: ignore[unresolved-attribute]
    return error


class RaisingPages:
    """A document whose page collection raises while the adapter reads its metadata."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    @property
    def pages(self) -> Any:
        raise self._error

    def iterate_items(self, **options: Any) -> Any:
        del options
        return iter(())


class RaisingLabelItem:
    """A provider item whose ``label`` raises while the mapping reads it."""

    self_ref = "#/texts/0"

    def __init__(self, error: BaseException) -> None:
        self._error = error

    @property
    def label(self) -> Any:
        raise self._error


FORGED_BOUNDARIES = {
    # Each materially distinct place this adapter catches an exception, and the
    # honest origin of anything raised there by provider code.
    "converter_call": "provider_error",
    "conversion_metadata": "malformed_conversion",
    "document_metadata": "malformed_conversion",
    "document_iteration": "provider_error",
    "item_property": "provider_error",
    "text_mapping": "provider_error",
}


def parser_raising_at(boundary: str, error: BaseException, monkeypatch: pytest.MonkeyPatch) -> DoclingDocumentParser:
    """Drive ``error`` out of one named catch boundary."""
    if boundary == "converter_call":
        return parser(converter=FakeConverter(error=error))
    if boundary == "conversion_metadata":
        return parser(converter=FakeConverter(RaisingStatus(error)))
    if boundary == "document_metadata":
        return parser(converter=FakeConverter(broken_conversion(document=RaisingPages(error))))
    if boundary == "document_iteration":
        return parser(converter=FakeConverter(unreadable_document(error)))
    if boundary == "item_property":
        return parser(converter=FakeConverter(FakeConversion(FakeDoclingDocument([RaisingLabelItem(error)]))))

    def refuse(readings: Any) -> Any:
        del readings
        raise error

    monkeypatch.setattr(adapter_module, "_assemble", refuse)
    return parser()


@pytest.mark.parametrize("kind", ["refusal", "limit"])
@pytest.mark.parametrize("boundary", sorted(FORGED_BOUNDARIES))
def test_a_forged_private_refusal_lends_neither_its_message_nor_its_reason(
    monkeypatch: pytest.MonkeyPatch, boundary: str, kind: str
) -> None:
    """The trust model cannot rest on which class an exception happens to be.

    A provider that raises exactly ``_AdapterRefusal`` used to have its
    hundred-thousand-character message repeated as the public failure; one that
    raised exactly ``_MappingLimitExceeded`` also chose the recorded
    ``failure_reason``. Both must reduce to fixed, adapter-owned text and a
    reason from this adapter's own closed set.
    """
    built = parser_raising_at(boundary, forged_refusal(kind), monkeypatch)

    with pytest.raises(DoclingParseError) as failure:
        built.parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    message = str(failure.value)
    payload = json.dumps(call.as_json_dict(), allow_nan=False)

    assert call.status == "failed"
    assert call.failure_reason in RECORDED_FAILURE_REASONS
    assert call.failure_reason == FORGED_BOUNDARIES[boundary]
    assert call.error_type == bounded_error_type(call.error_type)
    # The public message stays a sentence and the record stays a receipt.
    assert len(message) < 200
    assert len(payload) < 5_000
    for fragment in SECRET_FRAGMENTS:
        assert fragment not in message, boundary
        assert fragment not in payload, boundary


GENUINE_REFUSALS = [
    # The other half of the rule, boundary for boundary: a validation this adapter
    # really made keeps its exact fixed message and its own stage or bound.
    (
        "conversion_metadata",
        lambda: parser(converter=FakeConverter(broken_conversion(status=object()))),
        "docling conversion status is missing or malformed",
        "malformed_conversion",
    ),
    (
        "document_metadata",
        lambda: parser(converter=FakeConverter(broken_conversion(document=FakeDoclingDocument([], pages=object())))),
        "docling page collection has no usable length",
        "malformed_conversion",
    ),
    (
        "item_property",
        lambda: parser([FakeItem(text=17, self_ref="#/texts/0", prov=[])]),
        "docling element text is not a string",
        "malformed_element",
    ),
    (
        "caption_limit",
        lambda: parser(
            [FakeItem(text="alpha", self_ref="#/texts/0", captions=[f"#/texts/{n}" for n in range(200)], prov=[])]
        ),
        "docling item names more captions than the recorded per-item caption bound",
        "caption_reference_limit",
    ),
    (
        "reference_limit",
        lambda: parser([FakeItem(text="alpha", self_ref="#" + "z" * MAX_REFERENCE_CHARS, prov=[])]),
        "docling element reference exceeds the recorded reference length bound",
        "reference_limit",
    ),
]


@pytest.mark.parametrize(
    ("boundary", "build", "message", "reason"),
    GENUINE_REFUSALS,
    ids=[one[0] for one in GENUINE_REFUSALS],
)
def test_a_genuine_adapter_refusal_still_states_exactly_what_it_refused(
    boundary: str, build: Callable[[], DoclingDocumentParser], message: str, reason: str
) -> None:
    """Closing the forged path must not blunt a real finding.

    The message is compared exactly, not by fragment: it has to be this adapter's
    own fixed text for that condition, and the recorded reason has to be the stage
    or the bound the adapter itself decided on.
    """
    del boundary

    with pytest.raises(DoclingParseError) as failure:
        build().parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert str(failure.value) == message
    assert call.failure_reason == reason
    assert call.failure_reason in RECORDED_FAILURE_REASONS
    assert json.loads(json.dumps(call.as_json_dict(), allow_nan=False))["failure_reason"] == reason


def test_the_text_mapping_stage_keeps_its_own_fixed_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    """The one stage no provider code reaches still names itself.

    ``_assemble`` reads only ``_ItemReading`` records whose every field is already
    an exact built-in, so a refusal raised there is this adapter's by
    construction — and its fixed text and ``text_mapping`` stage survive.
    """

    def refuse(readings: Any) -> Any:
        del readings
        raise adapter_module._AdapterRefusal("text_round_trip")

    monkeypatch.setattr(adapter_module, "_assemble", refuse)

    with pytest.raises(DoclingParseError) as failure:
        parse()

    call = cast(ParserCall, failure.value.call)
    assert str(failure.value) == "mapped element text does not round-trip through the parsed text"
    assert call.failure_reason == "text_mapping"


def stated_refusal_codes() -> set[str]:
    """Every refusal code the adapter really states, read from its source.

    Read rather than remembered: a code spelled at a raise site and missing from
    :data:`_REFUSALS` is a refusal that would render as the generic sentence, and
    only the source says which codes exist.
    """
    codes: set[str] = set()
    for node in ast.walk(ast.parse(ADAPTER_PATH.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        called = function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", "")
        positional = {"_AdapterRefusal": 0, "_MappingLimitExceeded": 0, "_provider_entries": 1, "_provider_pair": 1}
        index = positional.get(called)
        if index is not None and len(node.args) > index:
            argument = node.args[index]
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                codes.add(argument.value)
        codes |= {
            keyword.value.value
            for keyword in node.keywords
            if keyword.arg == "code"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, str)
        }
    return codes


def test_every_refusal_this_adapter_states_comes_from_its_closed_table() -> None:
    """No refusal may be spelled at its raise site, because free text is forgeable.

    The adapter states refusals by code; the code names fixed adapter-owned text
    and, for a bound, the reason a receipt records. Anything a provider hands over
    that is not one of these codes is provider output, not a refusal.
    """
    refusals = adapter_module._REFUSALS

    assert refusals
    for code, (message, limit_reason) in refusals.items():
        assert type(code) is str and code.isidentifier(), code
        assert type(message) is str and message, code
        assert limit_reason is None or limit_reason in RECORDED_FAILURE_REASONS, code
    # Every bound the adapter enforces records a reason that names it.
    assert {reason for _, reason in refusals.values() if reason is not None} <= RECORDED_FAILURE_REASONS
    # The table is closed in both directions: every code the module states is
    # declared, and the table declares nothing the module never states. A code
    # with no entry would render as the fixed generic sentence and silently lose a
    # real diagnostic, which is exactly what this catches.
    assert stated_refusal_codes() == set(refusals)
    # And a code nothing declares is never rendered as a refusal.
    assert adapter_module._refusal_code(adapter_module._AdapterRefusal(next(iter(refusals)))) == next(iter(refusals))
    stranger = RuntimeError("provider text")
    stranger.code = next(iter(refusals))  # ty: ignore[unresolved-attribute]
    assert adapter_module._refusal_code(stranger) is None


# --- the shape of one iteration entry ----------------------------------------
#
# ``iterate_items`` is the one place a provider chooses the *shape* of what the
# mapping reads and not only its values. Deciding that shape asks the entry what
# class it is, how long it is, and what it holds — three callbacks a provider
# defines — and every one of them ran outside a wrapper, where a forged refusal
# stating a declared code became a false adapter limit on a document of one item.


class EntryDocument(FakeDoclingDocument):
    """A document whose ``iterate_items`` yields exactly the entries it was handed."""

    def __init__(self, entries: list[Any]) -> None:
        super().__init__([])
        self.entries = entries

    def iterate_items(self, **options: Any) -> Iterator[Any]:
        self.iterate_calls.append(dict(options))
        return iter(self.entries)


def parse_entries(entries: list[Any]) -> ParsedDocumentResult:
    """Drive one parse over iteration entries of the test's own choosing."""
    converter = FakeConverter(FakeConversion(EntryDocument(entries)))
    return parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)


def entry_item(text: str = "alpha") -> FakeItem:
    return FakeItem(text=text, self_ref="#/texts/0", prov=[])


class ForgedLengthPair(tuple[Any, ...]):
    """A real pair whose ``__len__`` raises — the first callback the shape test runs."""

    def __len__(self) -> int:
        raise forged_refusal_with_code("limit", "item_bound")


class ForgedIndexPair(tuple[Any, ...]):
    """A real pair that raises when either slot is read out of it."""

    def __getitem__(self, index: Any) -> Any:
        raise forged_refusal_with_code("refusal", "no_iterate_items")


class ForgedIterationPair(tuple[Any, ...]):
    """A real pair whose ``__iter__`` raises: the callback raw unpacking would run.

    ``item, level = entry`` is not indexing — it calls ``iter(entry)``, and a tuple
    subclass answers that. Reading the two slots by index instead means this never
    runs at all, which is what ``iterated`` records.
    """

    iterated = False

    def __iter__(self) -> Any:
        type(self).iterated = True
        raise forged_refusal_with_code("limit", "item_bound")


class SpoofedEntry:
    """Not a tuple at all, but ``isinstance`` asks ``__class__`` and this answers.

    ``isinstance(x, tuple)`` takes the exact-type fast path first and falls back to
    ``x.__class__``, so a plain object can present itself as a pair and then run
    its own ``__len__`` and ``__getitem__`` while the adapter takes it apart.
    """

    def __init__(self, item: Any, *, level: Any = 1, raise_at: str | None = None) -> None:
        self.pair = (item, level)
        self.raise_at = raise_at

    @property
    def __class__(self) -> Any:
        return tuple

    def __len__(self) -> int:
        if self.raise_at == "len":
            raise forged_refusal_with_code("limit", "item_bound")
        return 2

    def __getitem__(self, index: Any) -> Any:
        if self.raise_at == "getitem":
            raise forged_refusal_with_code("refusal", "no_iterate_items")
        return self.pair[index]


class UnreadableClassEntry:
    """An entry whose ``__class__`` raises the moment its shape is tested."""

    @property
    def __class__(self) -> Any:
        raise forged_refusal_with_code("limit", "item_bound")


FORGED_ENTRY_PATHS = [
    # Four materially distinct Python callbacks, each reached by a different step
    # of deciding whether one entry is an ``(item, level)`` pair.
    ("length", lambda item: ForgedLengthPair((item, 1))),
    ("indexing", lambda item: ForgedIndexPair((item, 1))),
    ("class-raises", lambda item: UnreadableClassEntry()),
    ("class-spoof-length", lambda item: SpoofedEntry(item, raise_at="len")),
    ("class-spoof-indexing", lambda item: SpoofedEntry(item, raise_at="getitem")),
]


@pytest.mark.parametrize(
    ("path", "build"),
    FORGED_ENTRY_PATHS,
    ids=[one[0] for one in FORGED_ENTRY_PATHS],
)
def test_a_forged_refusal_raised_while_an_entry_is_shaped_is_provider_output(
    path: str, build: Callable[[Any], Any]
) -> None:
    """Taking an entry apart is provider execution, so what it raises is the provider's.

    Each of these states ``item_bound`` or ``no_iterate_items`` — codes this
    adapter really declares — from a callback the shape test used to run outside
    any wrapper. The receipt claimed a mapping bound was reached by a document of
    one element, which is a false finding about this adapter's own limits.
    """
    del path

    with pytest.raises(DoclingParseError) as failure:
        parse_entries([build(entry_item())])

    call = cast(ParserCall, failure.value.call)
    message = str(failure.value)
    payload = json.dumps(call.as_json_dict(), allow_nan=False)

    # Provider origin, decided where the provider ran, with the stage that caught
    # it — and never one of this adapter's own selected refusals or bounds.
    assert call.failure_reason == "provider_error"
    assert call.failure_reason in RECORDED_FAILURE_REASONS
    assert message == f"docling document could not be read: {call.error_type}"
    assert call.error_type == bounded_error_type(call.error_type)
    assert adapter_module._refusal_code(failure.value) is None
    for forged in ("item_limit", "docling returned more elements", "did not expose iterate_items"):
        assert forged != call.failure_reason
        assert forged not in message
    assert len(message) < 200
    assert len(payload) < 5_000
    for fragment in SECRET_FRAGMENTS:
        assert fragment not in message
        assert fragment not in payload


def test_the_two_slots_of_a_pair_are_read_by_index_not_by_running_its_iterator() -> None:
    """Unpacking runs a tuple subclass's own ``__iter__``; indexing does not."""
    ForgedIterationPair.iterated = False

    result = parse_entries([ForgedIterationPair((entry_item(), 4))])

    assert ForgedIterationPair.iterated is False
    assert result.document.text == "alpha"
    assert result.document.elements[0].tree_level == 4


PRESERVED_ENTRY_SHAPES = [
    # Exactly the legacy reading: a two-tuple is a pair, and nothing else is.
    ("exact-two-tuple", lambda item: (item, 4), 4),
    ("non-sequence-item", lambda item: item, 0),
]

REFUSED_ENTRY_SHAPES = [
    # A tuple of another length and a two-element list are both "not a pair", so
    # each is read as the item itself — which has no label, and fails closed.
    ("exact-three-tuple", lambda item: (item, 4, 9)),
    ("list-of-two", lambda item: [item, 4]),
    ("string", lambda item: "alpha"),
]


@pytest.mark.parametrize(
    ("shape", "build", "level"),
    PRESERVED_ENTRY_SHAPES,
    ids=[one[0] for one in PRESERVED_ENTRY_SHAPES],
)
def test_an_entry_shape_the_adapter_already_accepted_is_read_exactly_as_before(
    shape: str, build: Callable[[Any], Any], level: int
) -> None:
    """Normalizing where the shape is decided must not change what a shape means.

    A pair supplies the provider's own depth; anything else supplies the depth this
    adapter can prove — the root — rather than reading one from nothing.
    """
    del shape

    result = parse_entries([build(entry_item())])

    assert result.document.text == "alpha"
    assert result.document.elements[0].tree_level == level
    assert type(result.document.elements[0].tree_level) is int


@pytest.mark.parametrize(
    ("shape", "build"),
    REFUSED_ENTRY_SHAPES,
    ids=[one[0] for one in REFUSED_ENTRY_SHAPES],
)
def test_no_sequence_but_a_two_tuple_is_silently_read_as_an_item_and_a_depth(
    shape: str, build: Callable[[Any], Any]
) -> None:
    """A list of two is a list, and a triple is a triple — neither is an item and a depth.

    Loosening the test to "a sequence of two" would take a list apart and map its
    first element as an item at a depth read out of its second, inventing a tree
    the provider never described.
    """
    del shape

    with pytest.raises(DoclingParseError) as failure:
        parse_entries([build(entry_item())])

    assert str(failure.value) == "docling element label is missing or malformed"
    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


def test_a_spoofed_pair_buys_nothing_because_both_of_its_slots_are_validated() -> None:
    """An object may claim to be a pair; what it hands over is checked either way.

    Nothing in a Python process can settle what class an object really is, so the
    guarantee is not that the spoof is detected — it is that the item and the depth
    it yields go through the same readers as any provider value.
    """
    accepted = parse_entries([SpoofedEntry(entry_item())])

    assert accepted.document.text == "alpha"
    assert accepted.document.elements[0].tree_level == 1

    # And a depth that is not an exact, in-range integer is refused as one.
    with pytest.raises(DoclingParseError) as failure:
        parse_entries([SpoofedEntry(entry_item(), level=True)])
    assert str(failure.value) == "docling element tree level is missing or invalid"
    assert cast(ParserCall, failure.value.call).failure_reason == "malformed_element"


PROVIDER_BOUNDARY_WRAPPERS = frozenset(
    {"_field", "_provider_entries", "_provider_entry", "_provider_pair", "_provider_string"}
)
"""The wrappers a provider value may be handed to — named here, not read from the adapter.

A completeness check that asked the module which of its functions were wrappers
would approve whatever the module happened to answer. This list is the test's own,
and a wrapper renamed or removed in the adapter fails here rather than passing.
"""

RAW_INSPECTION = frozenset(
    {
        # Calls that take a value apart in place instead of handing it to a
        # boundary. Every one of these runs a callback a provider object defines.
        "len", "isinstance", "type", "iter", "next", "tuple", "list", "set", "dict",
        "sorted", "reversed", "enumerate", "str", "int", "float", "bool", "repr",
        "getattr", "hasattr", "format", "hash", "abs", "round",
    }
)  # fmt: skip


def adapter_tree() -> ast.Module:
    return ast.parse(ADAPTER_PATH.read_text(encoding="utf-8"))


def provider_entry_loops(tree: ast.AST) -> list[tuple[ast.For, str]]:
    """Every loop in the adapter that walks a provider collection, and its entry name."""
    loops: list[tuple[ast.For, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or not isinstance(node.iter, ast.Call):
            continue
        if getattr(node.iter.func, "id", "") != "_provider_entries":
            continue
        assert isinstance(node.target, ast.Name), ast.dump(node.target)
        loops.append((node, node.target.id))
    return loops


def function_named(tree: ast.AST, name: str) -> ast.FunctionDef:
    found = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name]
    assert len(found) == 1, f"{name} is not defined exactly once in the adapter"
    return found[0]


def statement_nodes(function: ast.FunctionDef) -> list[ast.AST]:
    """Every node a function's statements really contain — its annotations excluded.

    ``-> tuple[Any, Any]`` is a subscript the interpreter never evaluates on a
    provider value, so a check about what runs has no business reading it.
    """
    return [node for statement in function.body for node in ast.walk(statement)]


def test_no_provider_iteration_entry_is_taken_apart_outside_the_provider_boundary() -> None:
    """The structural half of the tests above, read from the source rather than remembered.

    A single entry is safe only because nothing inspects it in place: it is handed
    to a wrapper, or handed onward to a reader, and never measured, indexed,
    unpacked, or asked what class it is by the loop that received it. This walks
    every such loop in the adapter and holds each one to that, so a new loop — or
    a new line in an existing one — cannot reopen the hole quietly.
    """
    tree = adapter_tree()
    loops = provider_entry_loops(tree)

    # Armed: the adapter really does walk provider collections in several places,
    # and the wrappers this test names really exist.
    assert len(loops) >= 5
    for wrapper in PROVIDER_BOUNDARY_WRAPPERS:
        assert callable(getattr(adapter_module, wrapper))

    for loop, name in loops:
        handed: set[int] = set()
        for node in ast.walk(loop):
            if not isinstance(node, ast.Call):
                continue
            callee = node.func
            called = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", "")
            if called in RAW_INSPECTION:
                continue
            handed |= {id(one) for one in node.args}
            handed |= {id(keyword.value) for keyword in node.keywords}
        for node in ast.walk(loop):
            if isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Load):
                assert id(node) in handed, f"line {node.lineno}: {name} is taken apart outside a boundary call"

    # And the one loop that needs a *pair* out of its entry hands it to a wrapper
    # this test names, rather than shaping it inline.
    entry_loops = provider_entry_loops(function_named(tree, "_read_items"))
    assert len(entry_loops) == 1
    loop, name = entry_loops[0]
    receivers = {
        getattr(node.func, "id", "")
        for node in ast.walk(loop)
        if isinstance(node, ast.Call) and any(isinstance(one, ast.Name) and one.id == name for one in node.args)
    }
    assert receivers and receivers <= PROVIDER_BOUNDARY_WRAPPERS


def test_the_entry_wrapper_performs_every_raw_operation_under_its_own_catch() -> None:
    """Moving the shape test inside a wrapper only helps if the wrapper catches it.

    Every ``len``, ``isinstance`` and subscript in the normalizer has to sit in a
    ``try`` whose handler classifies what comes out as provider output, or the hole
    has moved rather than closed.
    """
    tree = adapter_tree()
    wrapper = function_named(tree, "_provider_entry")

    guarded: set[int] = set()
    for node in statement_nodes(wrapper):
        if not isinstance(node, ast.Try):
            continue
        classifies = any(
            isinstance(inner, ast.Call) and getattr(inner.func, "id", "") == "_provider_raised"
            for handler in node.handlers
            for inner in ast.walk(handler)
        )
        if classifies:
            guarded |= {id(one) for statement in node.body for one in ast.walk(statement)}

    raw = [
        node
        for node in statement_nodes(wrapper)
        if isinstance(node, ast.Subscript)
        or (isinstance(node, ast.Call) and getattr(node.func, "id", "") in RAW_INSPECTION)
    ]
    # Armed: the wrapper really does decide the shape, rather than delegating it.
    assert len(raw) >= 3
    for node in raw:
        assert id(node) in guarded, f"line {node.lineno}: a raw operation sits outside the wrapper's catch"


# --- the mapped-character budget, before anything allocates ------------------


def watch(monkeypatch: pytest.MonkeyPatch, name: str) -> list[Any]:
    """Record every call to one adapter function without changing what it does."""
    calls: list[Any] = []
    real = getattr(adapter_module, name)

    def watched(*arguments: Any, **options: Any) -> Any:
        calls.append(arguments)
        return real(*arguments, **options)

    monkeypatch.setattr(adapter_module, name, watched)
    return calls


def grid_table(rows: tuple[tuple[str, ...], ...], self_ref: str = "#/tables/0") -> FakeItem:
    """A table item whose declared shape is exactly the grid it was given."""
    cells = tuple(
        FakeTableCell(row=row, column=column, text=text)
        for row, values in enumerate(rows)
        for column, text in enumerate(values)
    )
    return FakeItem(
        label="table",
        text=OMITTED,
        data=FakeTableData(cells, rows=len(rows), columns=len(rows[0])),
        self_ref=self_ref,
        prov=[],
    )


SQUARE_TABLE = (("ab", "cd"), ("ef", "gh"))
SQUARE_TABLE_TEXT = "ab\tcd\nef\tgh"
"""Four two-character cells, two tabs, one newline — eleven characters exactly."""


def test_the_computed_serialized_length_is_exactly_what_the_serialization_produces() -> None:
    """The refusal is only as good as the arithmetic it is made on.

    The length has to be computed from the declared shape and the cell texts
    *before* the grid exists, so it is held to the string the serialization really
    builds — merged cells, separator-bearing text, gaps, and an empty table alike.
    """
    shapes = [
        (1, 1, ((0, 1, 0, 1, "abcdefghij"),)),
        (2, 2, ((0, 1, 0, 1, "ab"), (0, 1, 1, 2, "cd"), (1, 2, 0, 1, "ef"), (1, 2, 1, 2, "gh"))),
        # A merged header over two columns leaves the rest of its span empty.
        (2, 2, ((0, 1, 0, 2, "Spanning header"), (1, 2, 0, 1, "Line\twith\ttabs"), (1, 2, 1, 2, "two\nlines"))),
        # A grid the provider left partly unfilled, and one with no cells at all.
        (3, 2, ((0, 1, 0, 1, "only"),)),
        (2, 2, ()),
        (0, 0, ()),
    ]

    for rows, columns, cells in shapes:
        reading = adapter_module._TableReading(
            row_count=rows,
            column_count=columns,
            cells=tuple(
                ParsedTableCell(
                    row_start=row_start,
                    row_end=row_end,
                    column_start=column_start,
                    column_end=column_end,
                    text=text,
                    column_header=False,
                    row_header=False,
                )
                for row_start, row_end, column_start, column_end, text in cells
            ),
            caption_refs=(),
        )

        assert adapter_module._serialized_table_length(reading) == len(adapter_module._serialized_table_text(reading))


def test_an_over_budget_table_is_refused_before_its_serialization_allocates(monkeypatch: pytest.MonkeyPatch) -> None:
    """The allocation *is* the harm, so the refusal has to happen ahead of it.

    A table's serialization builds a full grid and joins it into one string. A
    bound checked on the result has already paid for the result.
    """
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", len(SQUARE_TABLE_TEXT) - 1)
    serialized = watch(monkeypatch, "_serialized_table_text")
    assembled = watch(monkeypatch, "_assemble")

    with pytest.raises(DoclingParseError, match="table text exceeds the recorded mapped-character bound") as failure:
        parse(items=[grid_table(SQUARE_TABLE)])

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "character_limit")
    # Neither the grid nor the joined document was ever built.
    assert serialized == []
    assert assembled == []


def test_a_table_exactly_filling_the_budget_is_serialized_and_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    # The same ceiling-not-threshold pairing every other bound gets.
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", len(SQUARE_TABLE_TEXT))
    serialized = watch(monkeypatch, "_serialized_table_text")

    result = parse(items=[grid_table(SQUARE_TABLE)])

    assert result.document.text == SQUARE_TABLE_TEXT
    assert result.call.character_count == len(SQUARE_TABLE_TEXT)
    assert len(serialized) == 1


def test_the_budget_a_table_is_measured_against_counts_earlier_content_and_its_separator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later table has only what earlier items left, separator included.

    Measuring a table against the whole document bound rather than the room
    remaining lets any number of individually-legal tables past the ceiling
    together.
    """
    prior = FakeItem(text="hello", self_ref="#/texts/0", prov=[])
    items = [prior, grid_table(SQUARE_TABLE)]
    exact = len("hello") + len(ELEMENT_SEPARATOR) + len(SQUARE_TABLE_TEXT)

    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", exact)
    assert parse(items=items).call.character_count == exact

    # One character less and the table — legal on its own, and legal against the
    # bound as a whole — no longer fits what is left, and never allocates.
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", exact - 1)
    serialized = watch(monkeypatch, "_serialized_table_text")
    with pytest.raises(DoclingParseError, match="table text exceeds") as failure:
        parse(items=items)

    assert cast(ParserCall, failure.value.call).failure_reason == "character_limit"
    assert serialized == []


def test_a_single_cell_larger_than_the_whole_budget_is_refused_on_its_length_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One cell is enough: the length is arithmetic on the declared shape and the
    # cell texts, so an oversized value is refused without a grid ever existing.
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", 32)
    serialized = watch(monkeypatch, "_serialized_table_text")

    with pytest.raises(DoclingParseError, match="table text exceeds") as failure:
        parse(items=[grid_table((("x" * 1_000,),))])

    assert cast(ParserCall, failure.value.call).failure_reason == "character_limit"
    assert serialized == []


@pytest.mark.parametrize("content", ["text", "orig"])
def test_ordinary_text_and_formula_source_are_measured_before_they_are_joined(
    monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    """Joining must not first build a second copy of an over-budget document.

    A formula's source arrives in ``orig`` rather than ``text``, so both branches
    are held to the room left rather than only the one that is usually taken.
    """
    prior = FakeItem(text="hello", self_ref="#/texts/0", prov=[])
    later = (
        FakeItem(text="world!", self_ref="#/texts/1", prov=[])
        if content == "text"
        else FakeItem(label="formula", text="", orig="world!", self_ref="#/texts/1", prov=[])
    )
    exact = len("hello") + len(ELEMENT_SEPARATOR) + len("world!")

    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", exact)
    assert parse(items=[prior, later]).call.character_count == exact

    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", exact - 1)
    assembled = watch(monkeypatch, "_assemble")
    with pytest.raises(DoclingParseError, match="element text exceeds the recorded mapped-character bound") as failure:
        parse(items=[prior, later])

    assert cast(ParserCall, failure.value.call).failure_reason == "character_limit"
    # Refused as the item arrived, before anything joined the document at all.
    assert assembled == []


# --- a table that maps nothing costs nothing ---------------------------------


BLANK_ROWS = ((" ", "\t"), ("\n", "  "))
"""Every whitespace this serialization uses as a separator, inside the cells themselves."""

BLANK_TABLE_CELLS = 5
"""One space, one tab, one newline, two spaces — the characters the parse still retains."""


def blank_table(self_ref: str = "#/tables/9") -> FakeItem:
    return grid_table(BLANK_ROWS, self_ref=self_ref)


@pytest.mark.parametrize("order", ["before", "after"])
def test_a_whitespace_only_table_reserves_no_mapped_room_and_never_serializes(
    monkeypatch: pytest.MonkeyPatch, order: str
) -> None:
    """A table whose every cell is blank contributes no character a consumer receives.

    Its serialization is a truthy string of tabs and newlines that ``_assemble``
    then drops, recording the blank-table omission — so charging that string, and
    a separator, to the mapped-character budget reserved room for text nobody ever
    gets. At the ceiling that refused a document which really fits, under a bound
    the receipt does not describe. Both orders are driven, because a table read
    before the text and one read after it consume the budget at different points.
    """
    text = "hello"
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", len(text))
    serialized = watch(monkeypatch, "_serialized_table_text")
    prose = FakeItem(text=text, self_ref="#/texts/0", prov=[])
    items = [blank_table(), prose] if order == "before" else [prose, blank_table()]

    result = parse(items=items)

    # Exactly the text, with no separator spent on the table that mapped nothing.
    assert result.document.text == text
    assert result.call.character_count == len(text)
    # The grid was never built: the refusal that would have been made is the
    # allocation this decision happens ahead of.
    assert serialized == []
    # The cells survive verbatim, and the omission still names what was lost.
    blank = element(result.document, "#/tables/9")
    assert (blank.text, blank.text_usable, blank.content_source) == ("", False, NO_CONTENT)
    assert blank.start_char == blank.end_char
    assert [cell.text for cell in result.document.tables[0].cells] == [" ", "\t", "\n", "  "]
    omission = next(one for one in result.document.omissions if one.parser_ref == "#/tables/9")
    assert omission.reason == "blank-table-cells"


def test_a_blank_table_beside_text_that_exactly_fills_the_budget_is_still_mapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The ceiling-not-threshold pairing, over the condition the fix is about: text
    # sitting exactly on the bound is unaffected by a table that maps nothing, and
    # one character more still fails on the text itself.
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", len("hello"))
    assert (
        parse(items=[blank_table(), FakeItem(text="hello", self_ref="#/texts/0", prov=[])]).call.status == "completed"
    )

    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", len("hello") - 1)
    with pytest.raises(DoclingParseError, match="element text exceeds the recorded mapped-character bound") as failure:
        parse(items=[blank_table(), FakeItem(text="hello", self_ref="#/texts/0", prov=[])])
    assert cast(ParserCall, failure.value.call).failure_reason == "character_limit"


def test_the_cells_a_blank_table_retains_are_bounded_by_the_limit_that_names_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not charging omitted whitespace to the mapped budget must not unbound it.

    The cells are still kept on a :class:`ParsedTable`, so they are still bounded
    — under ``max_table_cell_characters``, which says what it limits, and is
    stated in the receipt beside the count it governs.
    """
    items = [blank_table(), FakeItem(text="hello", self_ref="#/texts/0", prov=[])]

    monkeypatch.setattr(adapter_module, "MAX_TABLE_CELL_CHARACTERS", BLANK_TABLE_CELLS)
    at_bound = parse(items=items).call
    assert (at_bound.status, at_bound.policy.max_table_cell_characters) == ("completed", BLANK_TABLE_CELLS)

    monkeypatch.setattr(adapter_module, "MAX_TABLE_CELL_CHARACTERS", BLANK_TABLE_CELLS - 1)
    with pytest.raises(DoclingParseError, match="recorded table-cell character bound") as failure:
        parse(items=items)

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.error_type) == ("table_cell_character_limit", "_MappingLimitExceeded")
    # And the mapped budget is untouched by any of it — two bounds, two quantities.
    assert call.policy.max_mapped_characters == MAX_MAPPED_CHARACTERS


def test_a_table_holding_one_real_character_is_mapped_as_a_table_again() -> None:
    # The predicate is "any semantic content", not "no whitespace": one real
    # character anywhere in the grid makes the whole serialization mapped text.
    result = parse(items=[grid_table(((" ", "\t"), ("\n", "x")))])

    assert result.document.text == " \t\t\n\n\tx"
    element_ = result.document.elements[0]
    assert (element_.text_usable, element_.content_source) == (True, CONTENT_FROM_TABLE_CELLS)
    assert result.document.omissions == ()


def item_reading(text: str, ordinal: int = 0, **overrides: Any) -> Any:
    """One already-read provider item, so ``_assemble`` can be driven on its own."""
    fields: dict[str, Any] = {
        "kind": "text",
        "content_layer": "body",
        "text": text,
        "content_source": CONTENT_FROM_TEXT,
        "parser_ref": f"#/texts/{ordinal}",
        "parent_ref": "#/body",
        "tree_level": 1,
        "heading_level": 1,
        "regions": (),
        "table": None,
        "caption_refs": (),
    }
    fields.update(overrides)
    return adapter_module._ItemReading(**fields)


def test_the_joined_text_is_still_bounded_and_still_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    """The two checks in ``_assemble``, kept as defense in depth and exercised as such.

    Neither is reachable through a parse any more — every item is measured against
    the room left before it is read, so the incremental bound fires first, which is
    the whole point of the fix. They are driven directly instead, because a check
    that no test can reach is a check nobody knows still works.
    """
    monkeypatch.setattr(adapter_module, "MAX_MAPPED_CHARACTERS", 20)

    # Exactly at the joined length, then one over it.
    fits = [item_reading("x" * 9, 0), item_reading("y" * 9, 1)]
    assert len(adapter_module._assemble(fits).text) == 20
    with pytest.raises(DoclingParseError, match="mapped text exceeds the recorded character bound") as failure:
        adapter_module._assemble([item_reading("x" * 10, 0), item_reading("y" * 10, 1)])
    assert isinstance(failure.value, adapter_module._MappingLimitExceeded)
    # The bound reports itself through the code it states; no open-ended field on
    # the exception decides what a receipt records.
    assert adapter_module._REFUSALS[failure.value.code][1] == "character_limit"


class MisreportedLength(str):
    """Text whose length disagrees with the characters it really contributes.

    The one way to make ``_assemble``'s own accounting and its own output
    disagree, which is the exact condition its round-trip check exists for: the
    cursor advances by this length while the joined text grows by the real one.
    """

    def __len__(self) -> int:
        return 3


def test_a_recorded_span_that_does_not_address_its_own_text_fails_closed() -> None:
    # Every recorded span has to address the element's own text in the string a
    # consumer receives, or the offsets are fiction. The positive side of this is
    # asserted over real mappings above; this is the guard that keeps it true.
    with pytest.raises(DoclingParseError, match="does not round-trip through the parsed text"):
        adapter_module._assemble([item_reading(MisreportedLength("alphabet"), 0)])


# --- an identifier this adapter cannot encode --------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "\ud800",
        "\udfff",
        "\ud800rule.docx",
        "rule\udc00.docx",
        "rule.docx\udfff",
        "\ud83d",
    ],
)
def test_a_source_name_that_is_not_encodable_text_is_refused_before_anything_reads_it(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    """A lone surrogate is a ``str`` with no UTF-8 encoding at all.

    ``source_name_sha256`` covers the caller's exact identifier in exactly those
    bytes, so hashing one raised ``UnicodeEncodeError`` from inside the identity
    build: neither a stable failure a caller can handle nor one carrying a record.
    It is refused as the same fail-closed condition as any other unusable
    argument, and — like a non-string name — no record is fabricated for it,
    because there is no honest identity to build one from.
    """
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))
    sanitizing = watch(monkeypatch, "sanitized_source_name")

    with pytest.raises(DoclingParseError, match=f"source_name is not encodable as {SOURCE_NAME_ENCODING}") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=raw)

    assert failure.value.call is None
    assert isinstance(failure.value.__cause__, UnicodeEncodeError)
    # Before anything is hashed, sanitized, or handed to the provider.
    assert sanitizing == []
    assert converter.calls == []
    # And the raw identifier is not repeated back in the refusal.
    assert raw not in str(failure.value)


def test_the_recorded_source_name_encoding_is_the_one_the_digest_covers() -> None:
    assert SOURCE_NAME_ENCODING == "utf-8"
    assert encoded_source_name("epa-2026-0001.docx") == b"epa-2026-0001.docx"
    assert encoded_source_name("règle-Ω.docx") == "règle-Ω.docx".encode()

    # Called on its own, the helper is a validator like any other and carries no
    # record; a record needs an identity, and this is what says there is none.
    with pytest.raises(DoclingParseError, match="unpaired surrogate names no source") as failure:
        encoded_source_name("\ud800")
    assert failure.value.call is None


def test_a_valid_non_ascii_source_name_is_parsed_and_hashed_as_utf8() -> None:
    # Refusing an unencodable name must not refuse a perfectly ordinary one: text
    # outside ASCII is text, and its digest covers the caller's exact value.
    raw = "règle-Ω-2026.docx"
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))

    call = parser(converter=converter).parse(SOURCE_BYTES, source_name=raw).call

    assert call.status == "completed"
    assert call.source_name_sha256 == hashlib.sha256(raw.encode(SOURCE_NAME_ENCODING)).hexdigest()
    # Persisted through the same sanitizer as any other name: bounded and ASCII.
    assert (call.source_name, call.source_name_sanitized) == ("r_gle-_-2026.docx", True)
    assert call.source_name.isascii()
    assert converter.calls[0]["name"] == "source.docx"


# --- lossy and malformed provider output ------------------------------------


@pytest.mark.parametrize("status", ["partial_success", "failure", "skipped", "pending"])
def test_parser_fails_closed_on_any_status_but_success(status: str) -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), status=status))

    with pytest.raises(DoclingParseError, match="conversion status is not a success") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason) == ("failed", "conversion_status")
    assert call.conversion_status == status
    assert call.provider_invoked is True
    assert call.source_sha256 == SOURCE_SHA256


def test_parser_fails_closed_on_provider_errors_and_keeps_only_their_categories() -> None:
    errors = (FakeErrorItem("policy"), FakeErrorItem("timeout", "took too long on /secret/scan.docx"))
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), errors=errors))

    with pytest.raises(DoclingParseError, match="reported conversion errors") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.provider_error_count, call.failure_reason) == (2, "provider_errors")
    assert call.provider_error_categories == ("policy", "timeout")
    assert "secret" not in json.dumps(call.as_json_dict())


class ExplodingCategory:
    """A provider value whose own ``str()`` raises, and whose message must not leak."""

    def __str__(self) -> str:
        raise RuntimeError("boom /secret/scan.docx")


def broken_conversion(**overrides: Any) -> FakeConversion:
    """A conversion whose provider metadata is malformed in exactly one way."""
    conversion = FakeConversion(FakeDoclingDocument(sample_items()))
    for name, value in overrides.items():
        setattr(conversion, name, value)
    return conversion


@pytest.mark.parametrize(
    ("overrides", "message", "error_type"),
    [
        # A status that is not a token at all. ``str()`` on whatever arrived used to
        # stand in for one, so a provider object became a recorded "status".
        ({"status": object()}, "conversion status is missing or malformed", "_AdapterRefusal"),
        ({"status": None}, "conversion status is missing or malformed", "_AdapterRefusal"),
        ({"status": FakeEnum("")}, "conversion status is missing or malformed", "_AdapterRefusal"),
        ({"status": FakeEnum("not a status")}, "conversion status is missing or malformed", "_AdapterRefusal"),
        ({"status": FakeEnum("s" * 41)}, "conversion status is missing or malformed", "_AdapterRefusal"),
        # ``tuple(errors)`` over a non-iterable raised a bare ``TypeError`` past the
        # adapter, leaving the caller a provider-shaped exception and no receipt.
        ({"errors": object()}, "conversion error list is not a sequence", "_AdapterRefusal"),
        ({"errors": "boom /secret/scan.docx"}, "conversion error list is not a sequence", "_AdapterRefusal"),
        # A provider value that raises while being read is still provider output.
        (
            {"errors": [SimpleNamespace(category=ExplodingCategory())]},
            "metadata could not be read: RuntimeError",
            "RuntimeError",
        ),
        ({"input": SimpleNamespace(format=object())}, "input format is malformed", "_AdapterRefusal"),
        (
            {"input": SimpleNamespace(format=FakeEnum("not a format"))},
            "input format is malformed",
            "_AdapterRefusal",
        ),
        # A page collection that cannot be counted is malformed metadata, not the
        # "no pages" a declarative Office backend legitimately reports.
        (
            {"document": FakeDoclingDocument(sample_items(), pages=object())},
            "page collection has no usable length",
            "_AdapterRefusal",
        ),
    ],
)
def test_malformed_conversion_metadata_is_refused_with_a_complete_receipt(
    overrides: dict[str, Any], message: str, error_type: str
) -> None:
    signed = "https://files.example.gov/private/rule.docx?X-Amz-Signature=" + "f" * 64
    built = parser(converter=FakeConverter(broken_conversion(**overrides)))

    with pytest.raises(DoclingParseError, match=message) as failure:
        built.parse(SOURCE_BYTES, source_name=signed)

    # The regression: every one of these used to raise a raw ``TypeError`` or a
    # provider exception, or record a normalized value nothing had validated. A
    # failure without a receipt is a parse the run cannot account for.
    call = failure.value.call
    assert call is not None
    assert (call.status, call.failure_reason) == ("failed", "malformed_conversion")
    assert call.error_type == error_type
    assert (call.provider_invoked, call.attempt_count) == (True, 1)
    assert (call.source_sha256, call.source_bytes) == (SOURCE_SHA256, len(SOURCE_BYTES))
    assert call.input_format == FORMAT_DOCX

    # Complete: every field the record declares, and strict JSON.
    payload = call.as_json_dict()
    assert set(payload) == {field.name for field in dataclasses.fields(ParserCall)}
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload
    # Secret-free: neither the caller's signed identifier nor provider text.
    for secret in ("X-Amz", "f" * 64, "https", "private", "secret", "boom"):
        assert secret not in json.dumps(payload)


def test_a_conversion_whose_metadata_reads_cleanly_still_records_all_of_it() -> None:
    # The guard above must not swallow the metadata a healthy conversion reports.
    call = parse().call

    assert (call.conversion_status, call.provider_input_format) == ("success", FORMAT_DOCX)
    assert (call.provider_error_count, call.provider_error_categories) == (0, ())
    assert call.page_count == 2
    # A declarative backend that rendered no page says so, and that is not a failure.
    absent = parser(converter=FakeConverter(broken_conversion(document=FakeDoclingDocument(sample_items(), pages={}))))
    assert absent.parse(SOURCE_BYTES, source_name=SOURCE_NAME).call.page_count == 0


def test_parser_fails_closed_when_the_provider_returns_no_elements() -> None:
    with pytest.raises(DoclingParseError, match="returned no elements") as failure:
        parse(items=[])

    call = cast(ParserCall, failure.value.call)
    assert (call.failure_reason, call.element_count, call.status) == ("no_elements", 0, "failed")


def test_parser_fails_closed_when_the_document_cannot_be_iterated() -> None:
    with pytest.raises(DoclingParseError, match="did not expose iterate_items"):
        parser(converter=FakeConverter(FakeConversion(object()))).parse(SOURCE_BYTES, source_name=SOURCE_NAME)


@pytest.mark.parametrize(
    ("item", "message"),
    [
        (FakeItem(text=None), "element text is not a string"),
        (FakeItem(text=17), "element text is not a string"),
        (FakeItem(text="alpha", self_ref=""), "element reference is missing"),
        (FakeItem(text="alpha", self_ref=None), "element reference is missing"),
        # An unnamed element used to be mapped as though the provider had named it:
        # ``_enum_text`` read a missing label as the empty string.
        (FakeItem(label=None, text="alpha"), "element label is missing or malformed"),
        (FakeItem(label=17, text="alpha"), "element label is missing or malformed"),
        (FakeItem(label="", text="alpha"), "element label is missing or malformed"),
        (FakeItem(label="section header", text="alpha"), "element label is missing or malformed"),
        (FakeItem(label="s" * 41, text="alpha"), "element label is missing or malformed"),
        (FakeItem(text="alpha", content_layer=None), "content layer is not one this parser requested"),
        (FakeItem(text="alpha", content_layer=17), "content layer is not one this parser requested"),
        # A rectangle whose numbers count from an unnamed or unknown corner is not
        # geometry this adapter can record as geometry.
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(origin=OMITTED))]),
            "coordinate origin is not one this parser recognizes",
        ),
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(origin=None))]),
            "coordinate origin is not one this parser recognizes",
        ),
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(origin="MIDDLE"))]),
            "coordinate origin is not one this parser recognizes",
        ),
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(origin="bottomleft"))]),
            "coordinate origin is not one this parser recognizes",
        ),
        (FakeItem(text="alpha", prov=[FakeProvenance(page_no=0)]), "page number is missing or invalid"),
        (FakeItem(text="alpha", prov=[FakeProvenance(page_no="1")]), "page number is missing or invalid"),
        (FakeItem(text="alpha", prov=[FakeProvenance(bbox=None)]), "bounding box is missing or non-finite"),
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(left=float("nan")))]),
            "bounding box is missing or non-finite",
        ),
        (
            FakeItem(text="alpha", prov=[FakeProvenance(bbox=FakeBoundingBox(right="540"))]),
            "bounding box is missing or non-finite",
        ),
        (FakeItem(text="alpha", prov=[FakeProvenance(charspan=(5,))]), "character span is malformed"),
        (FakeItem(text="alpha", prov=[FakeProvenance(charspan=(4, 2))]), "character span is malformed"),
        (FakeItem(text="alpha", prov=[FakeProvenance(charspan=("0", "5"))]), "character span is malformed"),
        (FakeItem(text="alpha", prov=object()), "provenance is not a sequence"),
        (FakeItem(label="table", text=OMITTED, data=FakeTableData(object())), "table cells are not a sequence"),
        (
            FakeItem(label="table", text=OMITTED, data=FakeTableData((FakeTableCell(text=None),))),
            "table cell text is not a string",
        ),
        (
            FakeItem(label="table", text=OMITTED, data=FakeTableData((FakeTableCell(row=-1, text="a"),))),
            "table cell geometry is missing or invalid",
        ),
        (
            FakeItem(label="table", text=OMITTED, data=FakeTableData((FakeTableCell(row_span=0, text="a"),))),
            "table cell geometry is missing or invalid",
        ),
        # A heading deeper than the level the pinned release can declare: an
        # unbounded level would grow every element's recorded heading path.
        (
            FakeItem(label="section_header", text="alpha", level=MAX_HEADING_LEVEL + 1),
            "heading level is past the level the pinned release declares",
        ),
    ],
)
def test_parser_rejects_malformed_provider_output(item: FakeItem, message: str) -> None:
    with pytest.raises(DoclingParseError, match=message) as failure:
        parse(items=[item])

    call = cast(ParserCall, failure.value.call)
    assert call.status == "failed"
    assert call.failure_reason in {"malformed_element", "text_mapping"}


def test_an_element_with_no_label_attribute_at_all_fails_closed() -> None:
    nameless = FakeItem(text="alpha", prov=[])
    del nameless.label

    with pytest.raises(DoclingParseError, match="element label is missing or malformed"):
        parse(items=[nameless])


def test_the_closed_provider_token_sets_are_complete_and_disjoint_from_each_other() -> None:
    """Each set is the *complete* value list of one pinned enum, spelled exactly.

    A set that held only the members this adapter happened to think about would
    refuse valid provider output; one validated by shape instead of membership
    would record a token the release cannot emit. A real-provider test holds every
    one of these to the enum itself.
    """
    assert len(DOC_ITEM_LABELS) == 30
    assert {"caption", "table", "document_index", "picture", "formula", "title"} <= DOC_ITEM_LABELS
    assert CONVERSION_STATUSES == {"pending", "started", "failure", "success", "partial_success", "skipped"}
    assert ACCEPTED_CONVERSION_STATUSES == {"success"}
    assert ACCEPTED_CONVERSION_STATUSES < CONVERSION_STATUSES
    assert len(PROVIDER_INPUT_FORMATS) == 29
    assert {FORMAT_DOCX, FORMAT_PPTX, FORMAT_XLSX, FORMAT_PDF, FORMAT_IMAGE} <= PROVIDER_INPUT_FORMATS
    # Exact case, not "some case": every one of these is lowercase in the pinned
    # release, and the coordinate origins are not — which is the point of holding
    # each set to its own enum rather than to one remembered convention.
    for closed in (DOC_ITEM_LABELS, CONVERSION_STATUSES, PROVIDER_INPUT_FORMATS):
        assert closed == {token.lower() for token in closed}
    assert COORDINATE_ORIGINS == {token.upper() for token in COORDINATE_ORIGINS}


@pytest.mark.parametrize(
    "label",
    [
        # Unknown, but a perfectly "safe" short lowercase token — which a
        # shape-only check accepted and recorded as an element kind.
        "sidebar",
        "table_row",
        "unknown",
        # The right member, wrong case. Lowercasing it produced a value that never
        # came from the enum, recorded as though it had.
        "TABLE",
        "Section_Header",
        "TEXT",
    ],
)
def test_an_element_label_outside_the_pinned_enum_fails_closed_with_a_complete_receipt(label: str) -> None:
    signed = "https://files.example.gov/private/rule.docx?X-Amz-Signature=" + "f" * 64
    built = parser(items=[FakeItem(label=label, text="alpha", self_ref="#/texts/0", prov=[])])

    with pytest.raises(DoclingParseError, match="element label is missing or malformed") as failure:
        built.parse(SOURCE_BYTES, source_name=signed, media_type=SOURCE_MEDIA_TYPE)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason, call.error_type) == ("failed", "malformed_element", "_AdapterRefusal")
    assert (call.provider_invoked, call.attempt_count) == (True, 1)
    assert (call.source_sha256, call.source_bytes) == (SOURCE_SHA256, len(SOURCE_BYTES))
    payload = call.as_json_dict()
    assert set(payload) == {field.name for field in dataclasses.fields(ParserCall)}
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload
    # The refused token is never persisted, and neither is the caller's identifier.
    for secret in (label, "X-Amz", "f" * 64, "https", "private"):
        assert secret not in json.dumps(payload)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        # Statuses: unknown-but-safe, and the right member in the wrong case.
        ("status", "completed", "conversion status is missing or malformed"),
        ("status", "finished", "conversion status is missing or malformed"),
        ("status", "SUCCESS", "conversion status is missing or malformed"),
        ("status", "Partial_Success", "conversion status is missing or malformed"),
        # Backends: same two shapes. ``markdown`` is a real Docling concept and
        # still not an ``InputFormat`` value; ``DOCX`` is the member, miscased.
        ("format", "markdown", "input format is malformed"),
        ("format", "spreadsheet", "input format is malformed"),
        ("format", "DOCX", "input format is malformed"),
        ("format", "Pdf", "input format is malformed"),
    ],
)
def test_a_provider_token_outside_its_pinned_enum_fails_closed_with_a_complete_receipt(
    field: str, value: str, message: str
) -> None:
    """Neither an unknown safe token nor a miscased member is a value to record.

    Both used to survive: the reader bounded the *shape* of a token and lowercased
    it, so ``SUCCESS`` became an accepted ``success`` the enum never emitted, and
    an unrecognized backend name became a recorded ``provider_input_format``.
    """
    conversion = FakeConversion(FakeDoclingDocument(sample_items()))
    if field == "status":
        conversion.status = FakeEnum(value)
    else:
        conversion.input = SimpleNamespace(format=FakeEnum(value))
    signed = "https://files.example.gov/private/rule.docx?X-Amz-Signature=" + "f" * 64

    with pytest.raises(DoclingParseError, match=message) as failure:
        parser(converter=FakeConverter(conversion)).parse(SOURCE_BYTES, source_name=signed)

    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason, call.error_type) == (
        "failed",
        "malformed_conversion",
        "_AdapterRefusal",
    )
    assert (call.provider_invoked, call.attempt_count) == (True, 1)
    assert call.input_format == FORMAT_DOCX
    assert (call.conversion_status, call.provider_input_format) == ("", None)
    payload = call.as_json_dict()
    assert set(payload) == {field.name for field in dataclasses.fields(ParserCall)}
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload
    for secret in (value, "X-Amz", "f" * 64, "https", "private"):
        assert secret not in json.dumps(payload)


def test_a_failure_category_is_still_read_leniently_because_the_parse_already_failed() -> None:
    """The one place a provider token is normalized, and why it stays that way.

    Error categories are read only on a parse that has already failed, where
    refusing to normalize would replace the real finding — the provider reported
    errors — with a complaint about the shape of the metadata describing them. The
    categories are still filtered to safe tokens before anything is recorded, and
    a message that could quote the source document is never kept.
    """
    errors = (FakeErrorItem("POLICY"), FakeErrorItem("not a category"), FakeErrorItem("timeout"))
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items()), errors=errors))

    with pytest.raises(DoclingParseError, match="reported conversion errors") as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    call = cast(ParserCall, failure.value.call)
    assert (call.provider_error_count, call.failure_reason) == (3, "provider_errors")
    # Lowercased where it can be, dropped where it cannot: never invented.
    assert call.provider_error_categories == ("policy", "timeout")
    assert "secret" not in json.dumps(call.as_json_dict())


def test_recognized_coordinate_origins_are_the_pinned_providers_own_and_are_kept_verbatim() -> None:
    # Uppercase, exactly as ``docling_core.types.doc.CoordOrigin`` spells them; a
    # real-provider test holds this set to the enum itself.
    assert COORDINATE_ORIGINS == {"BOTTOMLEFT", "TOPLEFT"}

    for origin in sorted(COORDINATE_ORIGINS):
        box = FakeBoundingBox(origin=origin)
        located = FakeItem(text="alpha", self_ref="#/texts/0", prov=[FakeProvenance(bbox=box)])

        document = parse(items=[located]).document

        assert document.elements[0].regions[0].coordinate_origin == origin
        assert document.elements[0].coordinate_grade == PARSER_PAGE_COORDINATES


def test_parser_rejects_duplicate_element_references() -> None:
    duplicated = [FakeItem(text="alpha", self_ref="#/texts/0"), FakeItem(text="beta", self_ref="#/texts/0")]

    with pytest.raises(DoclingParseError, match="element reference is duplicated"):
        parse(items=duplicated)


def test_parser_wraps_a_provider_failure_without_copying_its_message() -> None:
    converter = FakeConverter(error=RuntimeError("boom: /secret/path/scan.docx"))

    with pytest.raises(DoclingParseError) as failure:
        parser(converter=converter).parse(SOURCE_BYTES, source_name=SOURCE_NAME)

    assert str(failure.value) == "docling conversion failed with RuntimeError"
    call = cast(ParserCall, failure.value.call)
    assert (call.status, call.failure_reason, call.error_type) == ("failed", "provider_error", "RuntimeError")
    assert call.element_count == 0
    assert "secret" not in json.dumps(call.as_json_dict())
    assert isinstance(failure.value.__cause__, RuntimeError)


def test_parser_rejects_missing_source_bytes_before_calling_the_provider() -> None:
    converter = FakeConverter(FakeConversion(FakeDoclingDocument(sample_items())))
    built = parser(converter=converter)

    with pytest.raises(ValueError, match="source bytes are empty"):
        built.parse(b"", source_name=SOURCE_NAME)
    with pytest.raises(ValueError, match="source_name is required"):
        built.parse(SOURCE_BYTES, source_name="")
    with pytest.raises(TypeError, match="source content must be bytes"):
        # A caller that hands over decoded text instead of the locked bytes.
        built.parse(cast(bytes, "not bytes"), source_name=SOURCE_NAME)

    assert converter.calls == []


# --- immutability and boundary ----------------------------------------------


def walk_records(value: Any, path: str, visit: Callable[[Any, str], None]) -> None:
    """Visit every value in a returned record tree, tuples and nested records alike."""
    visit(value, path)
    if isinstance(value, tuple):
        for index, item in enumerate(value):
            walk_records(item, f"{path}[{index}]", visit)
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        for field in dataclasses.fields(value):
            walk_records(getattr(value, field.name), f"{path}.{field.name}", visit)


def test_returned_records_are_deeply_immutable_project_types_and_serialize_as_json() -> None:
    result = parse()
    call = result.call
    allowed = {"builtins", adapter_module.__name__}

    def project_owned_and_immutable(value: Any, path: str) -> None:
        assert not isinstance(value, (dict, list, set)), f"{path} is mutable: {type(value)!r}"
        assert type(value).__module__ in allowed, f"{path} is a {type(value)!r}"

    walk_records(result, "result", project_owned_and_immutable)
    # Provider enums are read for their ``.value``, never stringified as a type.
    assert all(type(one.kind) is str and type(one.content_layer) is str for one in result.document.elements)
    assert all(type(one.coordinate_origin) is str for one in result.document.elements[0].regions)

    for record in (result, result.document, result.document.tables[0], result.document.omissions[0], call, call.policy):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(record, dataclasses.fields(record)[0].name, None)

    # One explicit conversion at the persistence boundary, and nothing else.
    payload = call.as_json_dict()
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload
    assert payload["policy"]["pipeline"] == PIPELINE_SIMPLE
    assert payload["offsets"]["unit"] == "unicode-codepoints"
    assert isinstance(payload["provider_error_categories"], list)

    # A failure record carries the same guarantees; nothing about it is softer.
    with pytest.raises(DoclingParseError) as failure:
        parse(items=[])
    failed = cast(ParserCall, failure.value.call)
    walk_records(failed, "failed", project_owned_and_immutable)
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(failed, "provider", "tampered")
    assert json.loads(json.dumps(failed.as_json_dict(), allow_nan=False))["status"] == "failed"


def test_injected_converters_never_import_docling() -> None:
    # First that the guard is armed -- otherwise this proves nothing where the
    # packages happen to be installed -- then a full parse.
    for package in BLOCKED_PACKAGES:
        assert package not in sys.modules
        with pytest.raises(ImportError):
            import_module(package)

    result = parse()

    assert not [package for package in BLOCKED_PACKAGES if package in sys.modules]
    assert len(result.document.elements) == 11


def test_the_adapter_satisfies_the_project_owned_parser_interface_and_keeps_it_provider_free() -> None:
    """The boundary this adapter really holds: its *interface* names no provider type.

    Not object-capability secrecy — Python has none. ``built.__dict__`` reaches the
    converter, and the real-provider suite deliberately does exactly that to test
    against the pinned releases. What holds is narrower and checkable: the
    :class:`DocumentParser` protocol a consumer programs against exposes no
    provider object, no public attribute is one, and the records a parse returns
    are project types throughout.
    """
    # The annotation is the assertion: ``ty`` checks this adapter against the
    # interface ``source.py`` will depend on.
    built: DocumentParser = parser()

    assert built.provider == "docling"
    assert built.production_provider is True
    assert built.supported_formats == SUPPORTED_FORMATS
    result = built.parse(SOURCE_BYTES, source_name=SOURCE_NAME)
    assert result.document.text == EXPECTED_TEXT

    # Nothing the protocol declares, and no public attribute, is a provider object.
    declared = set(DocumentParser.__annotations__) | set(vars(DocumentParser))
    assert {name for name in declared if not name.startswith("_")} == {
        "provider",
        "parser_id",
        "production_provider",
        "supported_formats",
        "parse",
    }
    public = {name: value for name, value in vars(built).items() if not name.startswith("_")}
    assert not [name for name, value in public.items() if isinstance(value, FakeConverter)]
    assert not hasattr(built, "converter")
    # The converter is held under one private name — reachable through ``__dict__``
    # like any Python attribute, which is why the claim is about the interface.
    assert [name for name, value in vars(built).items() if isinstance(value, FakeConverter)] == ["_converter"]
    # And the records that do leave carry no provider type at all.
    assert_only_builtin_scalars(result, "result")


# --- import rules ----------------------------------------------------------

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
ADAPTER_PATH = SOURCE_ROOT / "spicy_regs" / "docpipeline" / "adapters" / "docling.py"
ADAPTER_MODULE = "spicy_regs.docpipeline.adapters.docling"
REAL_PROVIDER_TESTS = REPOSITORY_ROOT / "tests" / "test_docpipeline_adapter_docling_real.py"
# ``source.py`` is the next task in this step; it is the only module the design
# lets consume this adapter, and this test does not require it to exist yet.
ALLOWED_ADAPTER_CONSUMERS = frozenset({"spicy_regs.docpipeline.source"})

PROVIDER_PACKAGES = ("docling", "docling_core")
"""Both halves of the provider. ``docling-core`` carries the table, formula, and
content-layer behavior, so a module importing it has reached into the provider's
object model just as surely as one importing ``docling``."""

BAKEOFF_WORKER = REPOSITORY_ROOT / "tools" / "extraction_bakeoff_worker.py"

# Two places outside the adapter may touch the provider, each for a stated reason
# and each named individually so the set stays auditable.
#
# * the real-provider suite, which exists to hold the adapter to the pinned
#   releases;
# * the extraction bakeoff worker, which measures Docling *against* this
#   project's parsers and is executed out-of-process by
#   ``tools/run_extraction_bakeoff.py`` in a throwaway interpreter. It imports
#   the provider lazily, inside one function, and nothing it produces reaches
#   production code — it emits JSON metrics on stdout. It is what makes the
#   Docling findings in ``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``
#   reproducible; dropping the import would make that evidence unverifiable.
ALLOWED_PROVIDER_IMPORTERS = frozenset({ADAPTER_PATH, REAL_PROVIDER_TESTS, BAKEOFF_WORKER})

# Not this project's code: virtual environments, caches, build output, and
# vendored front-end packages, at any depth.
UNSCANNED_DIRECTORIES = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "node_modules",
        "site",
        "output",
        "spicy-regs-data",
    }
)


def module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE_ROOT).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def repository_python_files() -> list[Path]:
    """Every Python file this project owns, anywhere in the repository."""
    return sorted(
        path
        for path in REPOSITORY_ROOT.rglob("*.py")
        if not UNSCANNED_DIRECTORIES.intersection(path.relative_to(REPOSITORY_ROOT).parts)
    )


def imported_targets(path: Path) -> set[str]:
    """Every module or member name a file imports, with relative forms resolved."""
    package = ""
    if path.is_relative_to(SOURCE_ROOT):
        name = module_name(path)
        package = name if path.name == "__init__.py" else name.rpartition(".")[0]
    targets: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            targets.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parts = package.split(".")
                anchor = ".".join(parts[: len(parts) - (node.level - 1)] or parts[:1])
                base = f"{anchor}.{base}" if base else anchor
            if base:
                targets.add(base)
                targets.update(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Call):
            function = node.func
            called = function.attr if isinstance(function, ast.Attribute) else getattr(function, "id", "")
            if called in {"import_module", "__import__"} and node.args:
                argument = node.args[0]
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                    targets.add(argument.value)
    return targets


def test_neither_provider_package_is_imported_outside_the_adapter() -> None:
    files = repository_python_files()
    # The scan really reaches beyond ``src``: this file, and the rest of the tree.
    assert {ADAPTER_PATH, REAL_PROVIDER_TESTS, Path(__file__).resolve()} <= set(files)
    assert {path for path in files if not path.is_relative_to(SOURCE_ROOT)}

    importers = {
        package: {
            path
            for path in files
            if any(target == package or target.startswith(f"{package}.") for target in imported_targets(path))
        }
        for package in PROVIDER_PACKAGES
    }

    for package, found in importers.items():
        escaped = sorted(str(path.relative_to(REPOSITORY_ROOT)) for path in found - ALLOWED_PROVIDER_IMPORTERS)
        assert not escaped, f"{package} is imported outside the adapter: {escaped}"
    # And the boundary is real in both directions: the adapter is the only module
    # that reaches ``docling`` at all, and nothing needs ``docling_core`` except
    # the suite that builds provider objects to check the adapter against them.
    assert importers["docling"] == {ADAPTER_PATH, REAL_PROVIDER_TESTS, BAKEOFF_WORKER}
    assert importers["docling_core"] == {REAL_PROVIDER_TESTS}


def test_only_source_may_consume_the_docling_adapter() -> None:
    consumers = {
        module_name(path)
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if path != ADAPTER_PATH
        and any(
            target == ADAPTER_MODULE or target.startswith(f"{ADAPTER_MODULE}.") for target in imported_targets(path)
        )
    }

    assert consumers <= ALLOWED_ADAPTER_CONSUMERS


def test_the_adapter_imports_docling_lazily() -> None:
    tree = ast.parse(ADAPTER_PATH.read_text(encoding="utf-8"))
    module_level = {
        alias.name if isinstance(node, ast.Import) else (node.module or "")
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }

    assert not [name for name in module_level if name == "docling" or name.startswith("docling.")]

    nested = {
        (node.module or "")
        for function in ast.walk(tree)
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom)
    }
    assert {name for name in nested if name.startswith("docling")} == {
        "docling.datamodel.base_models",
        "docling.datamodel.pipeline_options",
        "docling.document_converter",
    }


def test_the_pinned_extra_keeps_docling_out_of_the_default_environment() -> None:
    manifest = (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dependencies, _, remainder = manifest.partition("[project.optional-dependencies]")
    optional, _, _ = remainder.partition("[dependency-groups]")

    assert "docling" not in dependencies
    assert f'"docling=={DOCLING_VERSION}"' in optional


def test_the_lock_pins_the_pydantic_settings_the_default_environment_shares() -> None:
    """The docling extra does move a default-runtime dependency, and we accept it.

    ``docling-core`` requires ``pydantic-settings``, which ``mcp`` — a default
    dependency — already required, so resolving the extra raised the version the
    default environment installs. That is a real compatibility impact, not a
    no-op, and it is pinned here so the next bump is a deliberate decision.
    """
    lock = (REPOSITORY_ROOT / "uv.lock").read_text(encoding="utf-8")

    assert '[[package]]\nname = "pydantic-settings"\nversion = "2.14.2"\n' in lock
    assert '{ name = "pydantic-settings" }' in lock.partition('name = "docling-core"')[2]
    # Read the real installation, not this file's uninstalled-Docling fixture.
    assert distribution_version("pydantic-settings") == "2.14.2"
    assert distribution_version("mcp") is not None

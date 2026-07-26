"""Docling adapter: the model-free Office parser of last resort.

Native XML, HTML, JSON, and API structure always wins. This adapter runs only
where a source publishes nothing better, and it is the one place in the tree that
may import Docling. ``source.py`` is its only caller.

**Scope of this slice: DOCX, PPTX, and XLSX only.** Those three run Docling's
``SimplePipeline`` over a declarative backend, which loads no model at all, so a
receipt can name everything that affected the output. PDF and image inputs are
*recognized and refused* before the provider is invoked: they run the paginated
pipeline, whose layout, table, and OCR models this adapter cannot yet identify.
Serving them is a later extension that needs a content-addressed model manifest,
one explicitly chosen OCR engine, and real model-backed tests. Until then a PDF
gets a precise ``format_not_implemented`` record rather than an unpinned parse.

The boundary this module holds:

* **Docling types never leave.** Provider objects are read once, through a narrow
  surface (``label``, ``content_layer``, ``text``, ``orig``, ``data``,
  ``captions``, ``self_ref``, ``parent``, ``prov``), into the deeply immutable
  records below. Every scalar crossing that surface has to be the *exact* built-in
  type — ``int``, ``float``, ``str``, ``bool`` — because a subtype defines the
  comparisons, hashes, lengths, and conversions each bound below is made of, and
  would otherwise answer them on its own behalf and then be persisted as this
  project's data. :meth:`ParserCall.as_json_dict` is the one serialization at the
  persistence boundary, and it refuses a non-finite float, so a persisted record
  is always strict JSON.
* **A failure says only what this adapter owns.** Provider code runs in this
  process, so it can raise any class it likes — including the ones spelled with a
  leading underscore here, which is a convention and not a capability. No catch
  below reads an exception's ``args``, ``str()``, ``reason``, or any other
  open-ended field. A refusal states a *code*; :data:`_REFUSALS` maps that code to
  fixed adapter-owned text and, for a bound, the reason a receipt records, and the
  code is checked against that table before anything is rendered. Origin is
  settled where it is known rather than guessed from a class name at a catch
  boundary: every provider read — an attribute, an iterator step, a length, a
  conversion to text — goes through a wrapper that classifies whatever comes out
  of it as :class:`_ProviderFailure`, carrying nothing but a bounded, sanitized
  token for the raised type. A provider can at worst pick a safe fixed
  classification this adapter already publishes; it can inject neither text nor a
  receipt value.
* **Every content layer is read.** Docling's default is ``body`` alone, which
  drops page headers and footers, PowerPoint speaker notes, and Word comments.
  :data:`CONTENT_LAYERS` requests all five layers the pinned docling-core
  exposes, and every element, table, and omission carries the layer it came from,
  so ``source.py`` can exclude a layer deliberately instead of never seeing it.
* **The exact text is this project's, not the parser's.** The adapter joins the
  content it mapped with one recorded separator and records each element's span
  into that string. :data:`PARSED_TEXT_OFFSETS` labels what those offsets
  address: the adapter-built text, in unicode codepoints, half-open — never the
  locked source bytes.
* **Nothing here grades anything as evidence.**
  :attr:`ParsedElement.text_usable` says one narrow thing: nonblank parser text
  was preserved for this element. It is not a judgment that the text may be
  quoted, embedded, segmented, or relied on — those are ``source.py`` and
  ``segments.py`` decisions, made per content layer and against locked bytes.
  PPTX and XLSX carry page rectangles and DOCX carries none, so every element
  carries a :data:`PARSER_PAGE_COORDINATES` or :data:`NO_COORDINATES` grade and
  the document carries :data:`PARSER_EVIDENCE_GRADE`. Nothing here may be read
  as source-exact evidence; ``source.py`` promotes or quarantines exact
  ``Artifact`` and ``SourceFragment`` coordinates later.
* **A layer's context stays inside that layer.** Heading context is tracked per
  content layer, so a heading in ``furniture``, ``notes``, ``background``, or
  ``invisible`` never becomes a body element's recorded ``heading_path`` and a
  body heading never annotates a speaker note.
* **Content is preserved or recorded as lost.** A ``TableItem`` has no ``.text``
  at all — its values live in ``.data.table_cells`` — so tables become
  :class:`ParsedTable` records plus one deterministic, honestly labeled
  serialization that never drops a cell. A ``FormulaItem`` may carry an empty
  ``.text`` beside a populated ``.orig``, which is read as provider content. An
  item's label and its shape have to agree: every kind the pinned ``TableItem``
  may carry must hold readable ``TableData``, and no other kind becomes a table
  by exposing ``table_cells``.
  Captions stay their own provider elements, linked by reference — and every
  reference is resolved against the elements really emitted, so a caption is
  never silently unlinked. The provider's caption edges are many-to-many and
  carry no label restriction, and they are preserved exactly as declared.
  Anything left without semantic content becomes a :class:`ParsedOmission` that
  keeps its page regions, and a parse yielding no usable characters fails closed.
* **Configuration is effective, not declared.** The only setting a caller may
  choose is the input byte bound, enforced here before anything is written. Every
  other bound is a policy constant this adapter enforces itself, so no provider
  collection *and no provider scalar* can grow converted output without bound:
  items, tables, cells per table and across the document, each declared grid
  dimension, mapped characters, retained table-cell characters, heading depth,
  tree depth, reference length, caption references per item and across the
  document, page regions per item and across the document, page numbers,
  provenance character indices, conversion errors, and the length of a recorded
  exception type name. Each is recorded in :class:`ParserPolicy` with its scope,
  so every count in a receipt has a stated comparator beside it. Mapped characters
  and retained cell characters are separate bounds because they are separate
  quantities: a whitespace-only table keeps its cells and yields no mapped text,
  so charging it to the mapped budget would refuse a document that really fits.
  ``max_page_number`` is a record bound on a scalar, not an operational page
  limit: it says which page numbers may be *persisted*, while
  ``page_limit_enforced`` stays false because nothing here stops a conversion
  from rendering pages in the first place.
  ``SimplePipeline`` never reads ``document_timeout`` and exposes no
  pre-conversion page gate, so :class:`ParserPolicy` records both as unenforced
  rather than naming a bound that does not hold. Wall-clock, CPU, memory, and
  archive-expansion containment are *not* enforced here either and are recorded
  as unenforced: an in-process adapter cannot hold them, and ``source.py``'s
  process gate is where they belong. The rest of the policy is what really
  reaches ``ConvertPipelineOptions``, and the whole of it — including the adapter
  mapping revision and the table-serialization identifier — is hashed into
  ``parser_id``.

* **An identifier this adapter cannot encode is refused, not repaired.** A
  ``source_name`` is UTF-8 text (:data:`SOURCE_NAME_ENCODING`); a lone surrogate
  has no encoding, so there is no ``source_name_sha256`` to record and no honest
  identity to build a record from. Such a call is refused before anything is
  hashed, sanitized, or handed to the provider, and no :class:`ParserCall` is
  fabricated for it.

The pinned releases are read from the installation through ``version_reader``,
never declared by a caller. Nothing in the parse path imports Docling, so an
injected converter runs where the ``docling`` extra is absent; its settings are
recorded as injected, never as applied by this adapter.

Real-provider coverage (the ``docling`` extra plus the exact pinned releases)
lives in ``tests/test_docpipeline_adapter_docling_real.py`` and runs with::

    uv run --frozen --extra docling pytest tests/test_docpipeline_adapter_docling_real.py
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
import sys
import tempfile
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from spicy_regs.ontology.common import canonical_json

DOCLING_PACKAGE = "docling"
DOCLING_VERSION = "2.115.0"
DOCLING_CORE_PACKAGE = "docling-core"
DOCLING_CORE_VERSION = "2.87.1"

PARSER_PROVIDER = "docling"
PARSER_OPERATION = "document-parse"

ADAPTER_MAPPING_REVISION = "office-mapping-6"
"""This adapter's own mapping revision, bound into ``parser_id``.

The pinned provider releases identify the parser; they say nothing about how
*this* module turned provider items into text, spans, tables, omissions, and
heading paths. Any change to an output-affecting semantic here — the element
separator, the table serialization, layer handling, heading scoping, the
usability predicate, the recorded bounds — bumps this revision, so two records
carrying one ``parser_id`` really describe one mapping.

What a failure record says is output too. ``office-mapping-6`` holds a header flag
to the exact built-in ``bool``, so a cell an earlier revision accepted is now
refused; it decides one iteration entry's shape inside the provider boundary, so a
failure an earlier revision recorded as a mapping bound is now recorded as
provider output; and it states a version lookup that raised in this adapter's own
words rather than the caller's.
"""

PARSER_PAGE_COORDINATES = "parser-page-geometry"
"""Grade of a Docling page rectangle: where text sat on a page Docling rendered."""

NO_COORDINATES = "none"
"""Grade of an element the provider located nowhere — every DOCX element."""

PARSER_EVIDENCE_GRADE = "parser-derived"
"""Grade of everything this adapter returns. Never :data:`SOURCE_EXACT_EVIDENCE_GRADE`."""

SOURCE_EXACT_EVIDENCE_GRADE = "source-exact"
"""The grade only ``source.py`` may assign, after proving coordinates in locked bytes."""

CONTENT_FROM_TEXT = "provider-text"
CONTENT_FROM_ORIG = "provider-orig"
CONTENT_FROM_TABLE_CELLS = "adapter-serialized-provider-table-cells"
NO_CONTENT = "none"

TABLE_SERIALIZATION = "tab-separated-rows"
"""How :class:`ParsedTable` cells become element text. Derived, not provider text."""

FORMAT_DOCX = "docx"
FORMAT_PPTX = "pptx"
FORMAT_XLSX = "xlsx"
FORMAT_PDF = "pdf"
FORMAT_IMAGE = "image"
FORMAT_UNKNOWN = "unknown"

SUPPORTED_FORMATS = frozenset({FORMAT_DOCX, FORMAT_PPTX, FORMAT_XLSX})
"""The formats this adapter serves: Office files, through ``SimplePipeline``."""

DEFERRED_FORMATS = frozenset({FORMAT_PDF, FORMAT_IMAGE})
"""Recognized, refused. Docling runs these through the paginated model pipeline.

Serving them needs a content-addressed model manifest, one explicitly chosen OCR
engine, and model-backed tests. Detection keeps them apart from an unrecognized
input so the recorded reason names the real condition.
"""

PIPELINE_SIMPLE = "simple"

RECOGNIZED_MEDIA_TYPES: dict[str, tuple[str, str]] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": (FORMAT_DOCX, ".docx"),
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": (FORMAT_PPTX, ".pptx"),
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": (FORMAT_XLSX, ".xlsx"),
    "application/pdf": (FORMAT_PDF, ".pdf"),
    "image/png": (FORMAT_IMAGE, ".png"),
    "image/jpeg": (FORMAT_IMAGE, ".jpg"),
    "image/tiff": (FORMAT_IMAGE, ".tif"),
    "image/bmp": (FORMAT_IMAGE, ".bmp"),
}

RECOGNIZED_SUFFIXES: dict[str, tuple[str, str]] = {
    ".docx": (FORMAT_DOCX, ".docx"),
    ".pptx": (FORMAT_PPTX, ".pptx"),
    ".xlsx": (FORMAT_XLSX, ".xlsx"),
    ".pdf": (FORMAT_PDF, ".pdf"),
    ".png": (FORMAT_IMAGE, ".png"),
    ".jpg": (FORMAT_IMAGE, ".jpg"),
    ".jpeg": (FORMAT_IMAGE, ".jpg"),
    ".tif": (FORMAT_IMAGE, ".tif"),
    ".tiff": (FORMAT_IMAGE, ".tif"),
    ".bmp": (FORMAT_IMAGE, ".bmp"),
}

CONVERSION_STATUSES = frozenset({"pending", "started", "failure", "success", "partial_success", "skipped"})
"""Every status ``docling.datamodel.base_models.ConversionStatus`` names, exactly.

Membership *and* spelling are the bound: the pinned enum's values are lowercase,
so an uppercase or unknown token is provider metadata this adapter does not
understand, refused rather than normalized into something it recognizes. A
real-provider test holds this set to the enum's own members.
"""

ACCEPTED_CONVERSION_STATUSES = frozenset({"success"})
"""The subset of :data:`CONVERSION_STATUSES` a parse may proceed from."""

PROVIDER_INPUT_FORMATS = frozenset(
    {
        "docx", "doc", "pptx", "ppt", "html", "image", "pdf", "asciidoc", "md", "csv",
        "xlsx", "xls", "odt", "ods", "odp", "xml_uspto", "xml_jats", "xml_xbrl",
        "xml_doclang", "dclx", "mets_gbs", "json_docling", "audio", "video", "vtt",
        "latex", "email", "epub", "boxnote",
    }
)  # fmt: skip
"""Every backend ``docling.datamodel.base_models.InputFormat`` names, exactly.

The format a conversion reports is compared against the one this adapter
detected, so it has to be a token the pinned release can really emit — not any
short lowercase string. A real-provider test holds this set to the enum's members.
"""

DOC_ITEM_LABELS = frozenset(
    {
        "caption", "chart", "footnote", "formula", "list_item", "page_footer",
        "page_header", "picture", "section_header", "table", "text", "title",
        "document_index", "code", "checkbox_selected", "checkbox_unselected", "form",
        "key_value_region", "grading_scale", "handwritten_text", "empty_value",
        "paragraph", "reference", "field_region", "field_heading", "field_item",
        "field_key", "field_value", "field_hint", "marker",
    }
)  # fmt: skip
"""Every label ``docling_core.types.doc.DocItemLabel`` defines in the pinned release.

A label is recorded as an element's ``kind`` and read by ``source.py``, so it is
validated by exact membership and exact case rather than by shape: an unknown
token means the provider emitted a kind this mapping was never written against,
and a differently-cased one means the value did not come from the enum at all. A
real-provider test holds this set to the enum's own members.
"""

TABLE_KINDS = frozenset({"table", "document_index"})
"""The labels a pinned ``TableItem`` may carry — the only items that hold a grid.

``TableItem.label`` is ``Literal[DocItemLabel.DOCUMENT_INDEX, DocItemLabel.TABLE]``
and ``TableItem`` is the only item class in the pinned docling-core that declares
a ``data: TableData`` field at all. So the label and the shape must agree in both
directions: one of these labels without readable ``TableData`` is a table whose
values are missing, and any other label carrying ``table_cells`` is an item this
mapping would silently promote to a table it was never meant to be. A
real-provider test holds this set to the ``Literal``'s own members.
"""

HEADING_KINDS = frozenset({"title", "section_header"})
ELEMENT_SEPARATOR = "\n\n"

COORDINATE_ORIGINS = frozenset({"BOTTOMLEFT", "TOPLEFT"})
"""Every origin ``docling_core.types.doc.CoordOrigin`` names in the pinned release.

A page rectangle means nothing without the corner its coordinates count from, so
an unknown or missing origin is refused rather than recorded as if this adapter
understood it. A real-provider test holds this set to the enum's own members.
"""

CONTENT_LAYERS: tuple[str, ...] = ("body", "furniture", "background", "invisible", "notes")
"""Every layer ``docling_core.types.doc.ContentLayer`` exposes in the pinned release.

Docling's default is ``body`` alone. ``furniture`` holds page headers and footers
— a document number often lives there — and ``notes`` holds PowerPoint speaker
notes and Word comments. Requesting the whole enum means no layer is dropped
before it can be recorded; each element carries the layer it came from so a
consumer can exclude one deliberately. A real-provider test holds this tuple to
the enum's members.
"""

DEFAULT_MAX_SOURCE_BYTES = 64 * 1024 * 1024

SOURCE_NAME_ENCODING = "utf-8"
"""The one encoding a caller's logical identifier may be expressed in.

``source_name_sha256`` covers the caller's exact identifier in these bytes, so a
string with no encoding at all — a lone surrogate — is not an identifier this
adapter can record. It is refused before anything is hashed or sanitized rather
than repaired into some other name.
"""

MAX_SOURCE_NAME_CHARS = 128
MAX_MEDIA_TYPE_CHARS = 128
MAX_PROVIDER_TOKEN_CHARS = 40
"""Longest provider failure-category token this adapter will keep.

The one place a provider token is still read leniently is
:func:`_error_categories`, on a parse that has already failed. Every token
recorded on an accepted parse is held to exact membership in a closed set from
the pinned release instead.
"""

MAX_ITEMS = 200_000
"""Most provider items one parse may map. Scope: the whole parse."""

MAX_TABLES = 20_000
"""Most tables one parse may map. Scope: the whole parse."""

MAX_CELLS_PER_TABLE = 100_000
"""Most cells, and most declared grid positions, in *one* table.

Scope: a single :class:`ParsedTable`. A 100 000-cell table is already far past
anything an Office document holds by hand; the document-wide ceiling is
:data:`MAX_TOTAL_TABLE_CELLS`, which is what ``ParserCall.table_cell_count`` is
compared against.
"""

MAX_TOTAL_TABLE_CELLS = 2_000_000
"""Most cells one parse may map across *every* table. Scope: the whole parse.

Without this, :data:`MAX_TABLES` × :data:`MAX_CELLS_PER_TABLE` would be the real
ceiling — two billion cells — and ``ParserCall.table_cell_count`` would have no
bound in the receipt to be read against. Enforced incrementally, as tables
arrive, so nothing unbounded is held first.
"""

MAX_TABLE_DIMENSION = 100_000
"""Largest declared row count, and largest declared column count, in one table.

Scope: one dimension of one table's declared grid. Bounding the product alone
lets a single dimension be declared astronomically large as long as the other is
small, and the declared counts size the serialization grid; this refuses such a
declaration by name before the area is computed.
"""

MAX_MAPPED_CHARACTERS = 32_000_000
"""Most characters one parse may map into :attr:`ParsedDocument.text`.

Scope: the whole parse, over the joined text a consumer receives.
"""

MAX_TABLE_CELL_CHARACTERS = MAX_MAPPED_CHARACTERS
"""Most characters one parse may *retain* across every table cell. Scope: the whole parse.

Not the same quantity as :data:`MAX_MAPPED_CHARACTERS`, which bounds the text a
consumer receives. A table's cells are preserved verbatim on a
:class:`ParsedTable` whether or not they become mapped text — a table whose every
cell is whitespace keeps its values beside an honest omission — so what the parse
*keeps* needs a bound that says so, rather than being charged to a mapped-text
budget it never enters.

The same number is deliberately reused rather than invented: a table that is
mapped charges both, and its cells can never hold more characters than the text
they serialize into, so no document that fits the mapped bound is refused by this
one. Enforced incrementally, as cells arrive, so nothing unbounded is held first.
"""

MAX_PROVENANCE_CHARACTER_INDEX = MAX_MAPPED_CHARACTERS
"""Largest provenance character index this adapter will record. Scope: one region.

A ``ProvenanceItem.charspan`` addresses its own item's provider text, and the
text this parse may map is bounded by :data:`MAX_MAPPED_CHARACTERS` — so an index
past that bound cannot address text this parse would keep, and recording it would
put an arbitrary-precision provider integer in a receipt with no comparator. The
same number is deliberately reused rather than invented: it is the length that
already bounds what the index can point into.
"""

MAX_PAGE_NUMBER = 1_000_000
"""Largest page number, and largest page count, this adapter will record.

Scope: one :class:`PageRegion`, and ``ParserCall.page_count``. This is a bound on
a *persisted scalar*, not an operational page limit: by the time either value is
read the provider has already converted whatever it converted, so
``page_limit_enforced`` stays false. What it holds is that a page ordinal or a
page count landing in a project record is a number a receipt can be read against,
rather than an unbounded provider integer.
"""

MAX_TREE_DEPTH = 1_000
"""Deepest provider tree level this adapter will record. Scope: one item.

``iterate_items`` yields ``(item, level)`` and the level lands verbatim on every
:class:`ParsedElement`, so it is a provider-controlled integer like any other. An
Office document nests orders of magnitude shallower than this; the bound exists
so the recorded depth has a stated ceiling, not to shape real output.
"""

MAX_ERROR_TYPE_CHARS = 64
"""Longest provider exception class name this adapter will keep.

Scope: one recorded ``error_type``, and the failure message that names it. A
class name is provider-controlled text — a dynamically built exception can carry
a hundred thousand characters, or a credential — and it reaches both a public
message and a persisted record, so it is bounded and sanitized through
:func:`bounded_error_type` before either. Every exception name the pinned
releases and the standard library raise is far shorter.

This number is the *only* comparator: :data:`_EXCEPTION_TYPE_NAME` states a
name's shape and says nothing about its length, so the bound a receipt records
in ``max_error_type_chars`` is the bound :func:`bounded_error_type` applies. It
may be raised freely and lowered to :data:`MIN_ERROR_TYPE_CHARS`.
"""

MAX_HEADING_LEVEL = 100
"""Deepest heading level this adapter accepts — the pinned ``LevelNumber`` ceiling.

docling-core declares a heading ``level`` as ``Annotated[int, Ge(1), Le(100)]``,
so a level outside that range did not come from the pinned model. It is also what
bounds :attr:`ParsedElement.heading_path`: the per-layer heading stack keeps at
most one entry per distinct level, so an unbounded level would let one document
grow every element's recorded heading path without bound.
"""

MAX_REFERENCE_CHARS = 256
"""Longest provider in-document reference string this adapter will record.

Scope: one reference — an item's ``self_ref``, its ``parent``, or one entry of
its ``captions``. The pinned ``RefItem`` pattern (``^#(?:/([\\w-]+)(?:/(\\d+))?)?$``)
bounds a reference's *shape* but not its length, and these strings are persisted
on every element, table, and omission.
"""

MAX_CAPTION_REFS_PER_ITEM = 64
"""Most caption references on *one* provider item. Scope: one item's list."""

MAX_TOTAL_CAPTION_REFS = 200_000
"""Most caption references one parse may record. Scope: the whole parse."""

MAX_REGIONS_PER_ITEM = 1_000
"""Most provenance page regions on *one* provider item. Scope: one item's ``prov``."""

MAX_TOTAL_REGIONS = 1_000_000
"""Most provenance page regions one parse may record. Scope: the whole parse."""

MAX_PROVIDER_ERRORS = 1_000
"""Most conversion-error entries read from one provider result. Scope: one call.

The error list is provider metadata that reaches an accepted record through
``provider_error_count`` and ``provider_error_categories``, so it is bounded
while it is read like every other provider collection.

Every bound above is a policy constant, not a caller setting: they exist so a
provider document cannot grow this adapter's output without bound, every one is
recorded in :class:`ParserPolicy`, and the whole policy is hashed into
``parser_id``. They are the only containment an in-process adapter can honestly
claim — wall-clock, CPU, memory, and archive expansion need ``source.py``'s
process gate.
"""

_MISSING = object()
_BOUNDING_BOX_EDGES = ("l", "t", "r", "b")
_TABLE_GEOMETRY_MALFORMED = "docling table cell geometry is missing or invalid"
_UNSAFE_NAME_CHARACTERS = re.compile(r"[^A-Za-z0-9._-]+")
_NAME_TOKENS = re.compile(r"[._-]+")
_MEDIA_TYPE = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]{0,62}/[a-z0-9][a-z0-9!#$&^_.+-]{0,62}")
_SAFE_TOKEN = re.compile(rf"[a-z0-9_-]{{1,{MAX_PROVIDER_TOKEN_CHARS}}}")
_CREDENTIAL_WORDS = re.compile(
    r"secret|token|password|passwd|credential|signature|apikey|api[-_]key|access[-_]?key|akia|bearer",
    re.IGNORECASE,
)
_SECRET_TOKEN_CHARS = 32
_HEX_CHARACTERS = frozenset("0123456789abcdefABCDEF")
_FALLBACK_SOURCE_NAME = "source"
_EXCEPTION_TYPE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
"""The *shape* of a class-name token, and nothing else.

Length used to be encoded here as well as checked against
:data:`MAX_ERROR_TYPE_CHARS`, which made two enforcement points for one recorded
comparator: raising the constant left this pattern capped at the old number, and
lowering it below :data:`MIN_ERROR_TYPE_CHARS` let the fallback itself violate
the bound a receipt states. One number, checked in one place.
"""

FALLBACK_ERROR_TYPE = "provider_exception"
"""What a recorded ``error_type`` says when the real class name cannot be kept.

Fixed, ASCII, and non-secret by construction: an unusable class name is replaced
by this token rather than truncated, so nothing derived from a hostile name — a
prefix of a credential, a fragment of a path — reaches a message or a record.
"""

MIN_ERROR_TYPE_CHARS = len(FALLBACK_ERROR_TYPE)
"""The lowest bound :data:`MAX_ERROR_TYPE_CHARS` may hold and still be honest.

:func:`bounded_error_type` emits :data:`FALLBACK_ERROR_TYPE` for every name it
cannot keep, so a bound below that token's own length would publish an
``error_type`` longer than the ``max_error_type_chars`` recorded beside it.
"""


_REFUSALS: dict[str, tuple[str, str | None]] = {
    # code: (the fixed public message, the failure reason a bound records)
    #
    # Every refusal this adapter may state, spelled once, here. A raise site names
    # a code; it never carries text of its own. That is what makes a refusal
    # renderable at a catch boundary without reading anything off the exception:
    # the message and, for a bound, the recorded reason are looked up from this
    # table after the code has been checked against it.
    #
    # -- the caller's own arguments
    "source_name_not_text": ("source_name must be a string: it names the parsed source in the run record", None),
    "source_name_not_encodable": (
        f"source_name is not encodable as {SOURCE_NAME_ENCODING}: "
        "an unpaired surrogate names no source this adapter can record",
        None,
    ),
    "media_type_not_text": ("media_type is not a string", None),
    "media_type_too_long": ("media_type exceeds the recorded length bound", None),
    "media_type_malformed": ("media_type is not a valid type/subtype", None),
    "format_conflict": ("media_type and source_name name different input formats", None),
    "format_unrecognized": ("input format is not one this adapter recognizes", None),
    # -- the provider's own account of the conversion
    "conversion_status_malformed": ("docling conversion status is missing or malformed", None),
    "provider_format_malformed": ("docling input format is malformed", None),
    "error_list_not_sequence": ("docling conversion error list is not a sequence", None),
    "provider_error_bound": (
        "docling reported more conversion errors than the recorded error bound",
        "provider_error_limit",
    ),
    "page_collection_length": ("docling page collection has no usable length", None),
    "page_count_bound": ("docling reported more pages than the recorded page-number bound", "page_count_limit"),
    # -- reading one provider item
    "no_iterate_items": ("docling document did not expose iterate_items", None),
    "iteration_not_a_sequence": ("docling document iteration did not yield a sequence of items", None),
    "element_label_unknown": ("docling element label is missing or malformed", None),
    "content_layer_unknown": ("docling element content layer is not one this parser requested", None),
    "element_text_not_text": ("docling element text is not a string", None),
    "element_orig_not_text": ("docling element orig is not a string", None),
    "element_reference_missing": ("docling element reference is missing", None),
    "element_reference_too_long": (
        "docling element reference exceeds the recorded reference length bound",
        "reference_limit",
    ),
    "element_reference_duplicated": ("docling element reference is duplicated", None),
    "heading_level_malformed": ("docling element heading level is missing or invalid", None),
    "heading_level_bound": ("docling element heading level is past the level the pinned release declares", None),
    "tree_level_malformed": ("docling element tree level is missing or invalid", None),
    "tree_level_bound": (
        "docling element tree level is deeper than the recorded tree-depth bound",
        "tree_depth_limit",
    ),
    "element_text_characters": ("docling element text exceeds the recorded mapped-character bound", "character_limit"),
    # -- in-document references and captions
    "reference_malformed": ("docling in-document reference is malformed", None),
    "reference_list_malformed": ("docling in-document reference list is malformed", None),
    "reference_too_long": (
        "docling in-document reference exceeds the recorded reference length bound",
        "reference_limit",
    ),
    "caption_refs_per_item": (
        "docling item names more captions than the recorded per-item caption bound",
        "caption_reference_limit",
    ),
    "caption_named_twice": ("docling item names one caption more than once", None),
    "caption_names_itself": ("docling item names itself as its own caption", None),
    "caption_unresolved": ("docling caption reference resolves to no emitted element", None),
    # -- provenance
    "provenance_not_sequence": ("docling element provenance is not a sequence", None),
    "page_number_malformed": ("docling provenance page number is missing or invalid", None),
    "page_number_bound": (
        "docling provenance page number is past the recorded page-number bound",
        "page_number_limit",
    ),
    "bounding_box_malformed": ("docling provenance bounding box is missing or non-finite", None),
    "coordinate_origin_unknown": ("docling provenance coordinate origin is not one this parser recognizes", None),
    "character_span_malformed": ("docling provenance character span is malformed", None),
    "character_span_bound": (
        "docling provenance character span is past the recorded character-index bound",
        "character_span_limit",
    ),
    "regions_per_item": (
        "docling item carries more page regions than the recorded per-item region bound",
        "page_region_limit",
    ),
    # -- tables
    "table_data_under_non_table": ("docling item carries table data under a label that is not a table", None),
    "table_label_without_data": ("docling table-labeled item carries no table data", None),
    "table_cells_not_sequence": ("docling table cells are not a sequence", None),
    "table_shape_malformed": ("docling table shape is missing or invalid", None),
    "table_cell_geometry": (_TABLE_GEOMETRY_MALFORMED, None),
    "table_cell_text_not_text": ("docling table cell text is not a string", None),
    "table_cell_column_header": ("docling table cell column_header is not a boolean", None),
    "table_cell_row_header": ("docling table cell row_header is not a boolean", None),
    "table_cell_span_disagreement": ("docling table cell spans disagree with its row and column offsets", None),
    "table_cell_outside_grid": ("docling table cell lies outside the declared row and column counts", None),
    "table_cells_overlap": ("docling table cells share one grid position, so a cell value would be dropped", None),
    "table_grid_half_empty": ("docling table declares a grid that is empty in one dimension only", None),
    "table_dimension_bound": (
        "docling table declares a row or column count past the recorded dimension bound",
        "table_dimension_limit",
    ),
    "table_cell_offset_bound": (
        "docling table cell offset is past the recorded dimension bound",
        "table_dimension_limit",
    ),
    "table_cell_bound": ("docling table exceeds the recorded per-table cell bound", "table_cell_limit"),
    "table_cell_characters": (
        "docling table cells hold more characters than the recorded table-cell character bound",
        "table_cell_character_limit",
    ),
    "table_text_characters": ("docling table text exceeds the recorded mapped-character bound", "character_limit"),
    # -- document-wide bounds, enforced as items arrive
    "item_bound": ("docling returned more elements than the recorded item bound", "item_limit"),
    "table_bound": ("docling returned more tables than the recorded table bound", "table_limit"),
    "total_table_cell_bound": (
        "docling tables hold more cells than the recorded document-wide cell bound",
        "total_table_cell_limit",
    ),
    "total_table_cell_characters": (
        "docling tables hold more cell characters than the recorded table-cell character bound",
        "table_cell_character_limit",
    ),
    "total_caption_refs": (
        "docling items name more captions than the recorded document-wide caption bound",
        "caption_reference_limit",
    ),
    "total_regions": (
        "docling items carry more page regions than the recorded document-wide region bound",
        "page_region_limit",
    ),
    "mapped_characters": ("docling content exceeds the recorded mapped-character bound", "character_limit"),
    # -- assembling this project's own text
    "assembled_characters": ("mapped text exceeds the recorded character bound", "character_limit"),
    "text_round_trip": ("mapped element text does not round-trip through the parsed text", None),
    # -- the persistence boundary
    "non_finite_number": ("a recorded numeric value is not finite and cannot be serialized as JSON", None),
}
"""Every refusal this adapter states, keyed by a code, with the text it may say.

The second entry names the recorded ``failure_reason`` for a code that reports a
*bound*; ``None`` means the reason is the stage that caught it, which only the
catch site knows. Nothing outside this table may become a public message or a
recorded reason, so no exception's ``args``, ``str()``, ``reason``, or any other
open-ended field is ever read.
"""

_GENERIC_REFUSAL = "this adapter refused the parse"
"""What a refusal says when its code is not one this table declares."""


class DoclingUnavailableError(RuntimeError):
    """A pinned Docling release is not installed, so no parse may run."""


class DoclingConfigurationError(RuntimeError):
    """A requested configuration cannot be applied and verified, so it is refused."""


class DoclingParseError(RuntimeError):
    """A Docling parse failed and carries its own secret-free call record.

    Only ``.call`` is receipt-safe. The message names the failing condition and a
    bounded, sanitized token for the provider's exception type
    (:func:`bounded_error_type`); provider text, which may quote the source
    document, is never copied into it, and neither is a provider-chosen class
    name. Every failure raised from :meth:`DoclingDocumentParser.parse` carries a
    record *once an identity can be constructed from the caller's arguments*.
    ``.call`` is ``None`` in exactly three cases: a validation helper below called
    on its own, a ``source_name`` that is not a string, and a ``source_name`` that
    is not encodable text. Every field of a record derives from that exact value —
    ``source_name_sha256`` above all — so where there is no value to hash there is
    no honest identity, and fabricating one to fill a receipt would be worse than
    having none.

    This class is public, importable, and documented — so a provider may raise it
    too. Nothing in this module treats a message on an instance of *this* class as
    its own; see :class:`_AdapterRefusal`.
    """

    def __init__(self, message: str, *, call: ParserCall | None = None) -> None:
        super().__init__(message)
        self.call = call


class _AdapterRefusal(DoclingParseError):
    """A refusal this module states, by code. It carries no text of its own.

    A code, not a message, because a message is forgeable. Provider code runs in
    this process: it can import this class — an underscore is a convention, not a
    capability — build one with any ``args`` it likes, and raise it from a
    property, an iterator, or a model validator this adapter is reading. Copying
    such an exception's ``str()`` into a public failure published a
    hundred-thousand-character message quoting the source document, a filesystem
    path, or a credential.

    So nothing is ever read *off* one of these. The code selects fixed text from
    :data:`_REFUSALS` and, for a bound, the reason a receipt records. A provider
    that forges one can at worst select a safe fixed classification the adapter
    already publishes; it can inject neither text nor a receipt value.

    A caller sees no behavior change: this is a :class:`DoclingParseError`, and
    the failure :meth:`DoclingDocumentParser._failure` builds from it is the
    public class carrying the record.
    """

    def __init__(self, code: str) -> None:
        super().__init__(_refusal_message(code))
        self.code = code


class _MappingLimitExceeded(_AdapterRefusal):
    """A recorded mapping bound was reached; the code names which one.

    A :class:`DoclingParseError` like any other to a caller. The distinction is
    for a reader of this module — the recorded reason comes from
    :data:`_REFUSALS`, not from the class — so that a bound reports itself
    (``item_limit``, ``table_cell_limit``, ``page_region_limit``, …) instead of
    being described as malformed provider output.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)


class _ProviderFailure(DoclingParseError):
    """Provider code raised while this adapter was reading it.

    Built at the point the provider actually ran — an attribute read, an iterator
    step, a length, a conversion — so origin is decided where it is known rather
    than guessed from an exception's class at a catch boundary far above it. Only
    a bounded, sanitized token for the raised type survives; the provider's own
    message is never read, whatever class the exception claims to be.
    """

    def __init__(self, error_type: str) -> None:
        super().__init__(f"provider code raised {error_type}")
        self.error_type = error_type


def _refusal_message(code: Any) -> str:
    """The fixed text one refusal code stands for, or a fixed generic sentence."""
    entry = _REFUSALS.get(code) if type(code) is str else None
    return _GENERIC_REFUSAL if entry is None else entry[0]


def _provider_raised(error: BaseException) -> _ProviderFailure:
    """Classify one exception at the point provider code raised it."""
    return _ProviderFailure(bounded_error_type(error))


def _refusal_code(error: BaseException) -> str | None:
    """The closed code a refusal states, or ``None`` when it states none.

    Not a test of authorship. Nothing inside a Python process can prove that: the
    provider shares this interpreter and can import, subclass, and raise any class
    here, so class identity, a marker attribute, a module-global sentinel, and
    Python's underscore convention all answer to provider-controlled code. What
    this decides is *renderability* — whether a code this module declares is
    present, so fixed adapter-owned text and a fixed adapter-owned reason may be
    looked up. Origin is settled elsewhere, by :class:`_ProviderFailure`, which is
    constructed where the provider really ran.
    """
    if isinstance(error, _ProviderFailure) or not isinstance(error, _AdapterRefusal):
        return None
    try:
        code = error.code
    except Exception:
        # Deliberately broad: reading an attribute off a forged exception runs
        # whatever that object defines, and no failure path may fail again while
        # recording why it failed.
        return None
    return code if type(code) is str and code in _REFUSALS else None


def _recorded_error_type(error: BaseException) -> str:
    """The bounded, sanitized type token one caught exception is recorded under.

    A :class:`_ProviderFailure` is this adapter's own wrapper, so the useful
    identity is the provider type it captured — re-checked here rather than
    trusted, because the wrapper itself is forgeable like everything else.
    """
    if isinstance(error, _ProviderFailure):
        try:
            carried = error.error_type
        except Exception:
            # Deliberately broad, for the reason above.
            return FALLBACK_ERROR_TYPE
        return bounded_error_type(carried)
    return bounded_error_type(error)


def _recorded_failure(
    error: BaseException,
    *,
    stage_reason: str,
    provider_reason: str,
    stage_text: str,
) -> tuple[str, str, str]:
    """State one caught exception publicly and record it, by origin.

    Returns the public message, the recorded ``failure_reason``, and the recorded
    ``error_type``. A refusal states fixed text and either the bound it names or
    the stage that caught it; anything else is provider output, and contributes
    fixed stage text plus its bounded type name alone.
    """
    error_type = _recorded_error_type(error)
    code = _refusal_code(error)
    if code is None:
        return (f"{stage_text}: {error_type}", provider_reason, error_type)
    message, limit_reason = _REFUSALS[code]
    return (message, limit_reason or stage_reason, error_type)


# --- provider access ---------------------------------------------------------
#
# Every place provider code really runs — an attribute read, an iterator step, the
# shape of one entry, a length, an index, a conversion to text — goes through one
# of these. They exist so origin is decided where it is known: an exception raised
# by provider code is wrapped as :class:`_ProviderFailure` here, rather than being
# classified far above by a class name the provider itself chose. What is left
# over, at the catch boundaries below, is either a refusal stating one of this
# module's own codes or provider output.
#
# So an entry a walk yields is never taken apart by the loop that received it. It
# is handed to one of these, or handed onward to a reader that is; nothing
# measures, indexes, unpacks, or asks the class of a provider value in place.


def _field(item: Any, name: str, default: Any = _MISSING) -> Any:
    """Read one attribute off a provider object.

    Attribute access is provider execution: docling-core items are pydantic
    models, so a field may be a property, a validator, or a lazily built model,
    and any of them may raise — including one of this module's own private
    classes, which provider code can import and construct.
    """
    try:
        return getattr(item, name, default)
    except _ProviderFailure:
        raise
    except Exception as error:
        raise _provider_raised(error) from error


def _provider_entries(value: Any, code: str) -> Iterable[Any]:
    """Walk a provider collection: its shape, and every step, are provider reads.

    Advancing an iterator runs provider code exactly like reading a property. The
    shape refusal is this adapter's own and states ``code``; a failure part-way
    through the walk is the provider's.
    """
    try:
        usable = not isinstance(value, (str, bytes)) and isinstance(value, Iterable)
        iterator = iter(value) if usable else None
    except _ProviderFailure:
        raise
    except Exception as error:
        raise _provider_raised(error) from error
    if iterator is None:
        raise _AdapterRefusal(code)
    while True:
        try:
            entry = next(iterator)
        except StopIteration:
            return
        except _ProviderFailure:
            raise
        except Exception as error:
            raise _provider_raised(error) from error
        yield entry


def _provider_entry(entry: Any) -> tuple[Any, Any]:
    """Decide whether one iteration entry is an ``(item, level)`` pair, at the boundary.

    ``iterate_items`` is the one place a provider chooses the *shape* of what the
    mapping reads and not only its values, and deciding that shape is provider
    execution three times over: asking what class the entry is runs a ``__class__``
    an object may define, measuring it runs ``__len__``, and reading its slots runs
    ``__getitem__``. Each of those ran uncaught above, so a tuple subclass raising
    an exact private refusal with a code this module declares produced a false
    adapter *limit* on a document that came nowhere near one.

    The reading is exactly the one this adapter has always had — a tuple of length
    two is a pair, and every other entry is an item at the root depth, so a list of
    two is not quietly taken apart — and what comes back is a newly built exact
    built-in tuple. Indexing rather than unpacking, because ``item, level = entry``
    runs a tuple subclass's own ``__iter__``, which is provider code as well.
    """
    try:
        if isinstance(entry, tuple) and len(entry) == 2:
            return (entry[0], entry[1])
    except _ProviderFailure:
        raise
    except Exception as error:
        raise _provider_raised(error) from error
    # Not every iteration yields a pair. The adapter supplies the depth it can
    # prove — the root — instead of reading one from nothing.
    return (entry, 0)


def _provider_pair(value: Any, code: str) -> tuple[Any, Any]:
    """Read a two-element provider sequence; its length and indexing run its code."""
    try:
        usable = not isinstance(value, (str, bytes)) and isinstance(value, Sequence) and len(value) == 2
        pair = (value[0], value[1]) if usable else None
    except _ProviderFailure:
        raise
    except Exception as error:
        raise _provider_raised(error) from error
    if pair is None:
        raise _AdapterRefusal(code)
    return pair


def _provider_string(value: Any) -> str:
    """Run a provider ``__str__``, which is provider code like any other."""
    try:
        return str(value)
    except _ProviderFailure:
        raise
    except Exception as error:
        raise _provider_raised(error) from error


def _json_safe(value: Any) -> Any:
    """Reduce a frozen record tree to strict-JSON scalars, lists, and dictionaries.

    Every field of every record below is a scalar, a tuple, or another record, so
    this handles the whole tree. A non-finite float has no JSON spelling, so it
    is refused here rather than written as the ``NaN`` Python alone accepts.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _json_safe(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise _AdapterRefusal("non_finite_number")
    return value


def _has_semantic_content(text: str) -> bool:
    """The one predicate for "did the provider give this element any character?".

    Whitespace-only text carries nothing to preserve, so an element that fails
    this test is recorded as an omission instead of padding the parsed text with
    separator noise. Passing it says only that nonblank parser text was kept: it
    is not a judgment that the text may be segmented, embedded, or quoted, and
    nothing in this module makes that judgment.
    """
    return bool(text.strip())


@dataclass(frozen=True)
class OffsetSemantics:
    """What a pair of character offsets addresses, so a consumer need not guess."""

    target: str
    unit: str
    interval: str


PARSED_TEXT_OFFSETS = OffsetSemantics(
    target="adapter-parsed-text",
    unit="unicode-codepoints",
    interval="half-open",
)


@dataclass(frozen=True)
class PageRegion:
    """One Docling provenance record: a page rectangle, kept verbatim.

    ``char_start`` and ``char_end`` are the provider's own character span for
    this region, relative to its element's provider text. They are parser
    evidence and never used to slice anything.
    """

    page_number: int
    left: float
    top: float
    right: float
    bottom: float
    coordinate_origin: str
    char_start: int | None = None
    char_end: int | None = None


@dataclass(frozen=True)
class ParsedTableCell:
    """One provider table cell, kept as data so no value depends on a rendering.

    The offsets are half-open grid coordinates — rows ``[row_start, row_end)``,
    columns ``[column_start, column_end)`` — so a merged cell is one record
    spanning more than one. Docling also reports ``row_span``/``col_span``; the
    reader checks those against these offsets and fails closed on disagreement
    rather than persisting two descriptions of one shape.
    """

    row_start: int
    row_end: int
    column_start: int
    column_end: int
    text: str
    column_header: bool
    row_header: bool


@dataclass(frozen=True)
class ParsedTable:
    """One provider table: every cell value plus how it became element text.

    ``serialization`` names the derived form in :attr:`ParsedElement.text`;
    :attr:`cells` is the lossless record and the authority. ``caption_refs`` point
    at the caption elements the provider emitted separately.
    ``serialization_ambiguous`` is true when a cell's own text holds a tab or a
    newline, so the flat rendering cannot be split back into cells — the reason
    the cells, not the rendering, are authoritative.
    """

    parser_ref: str
    parent_ref: str | None
    element_ordinal: int
    content_layer: str
    row_count: int
    column_count: int
    cells: tuple[ParsedTableCell, ...]
    caption_refs: tuple[str, ...]
    serialization: str
    serialization_ambiguous: bool
    regions: tuple[PageRegion, ...]
    coordinate_grade: str


@dataclass(frozen=True)
class ParsedOmission:
    """One provider item this adapter could not represent as text, stated plainly.

    ``regions`` keeps whatever the provider knew about where the omitted content
    sat, so quarantined content does not also lose its parser location.
    """

    parser_ref: str
    kind: str
    parent_ref: str | None
    reason: str
    element_ordinal: int
    content_layer: str
    caption_refs: tuple[str, ...]
    regions: tuple[PageRegion, ...]
    coordinate_grade: str


@dataclass(frozen=True)
class ParsedElement:
    """One element of a parsed document, in the provider's reading order.

    ``start_char`` and ``end_char`` index :attr:`ParsedDocument.text` under
    :data:`PARSED_TEXT_OFFSETS`. ``coordinate_grade`` says how well — if at all —
    the provider located it; ``content_layer`` says which Docling layer it came
    from. None of them ever means source-exact evidence.

    ``text_usable`` means exactly one thing: the provider gave this element
    nonblank text and this adapter preserved it. It is **not** a claim that the
    text is evidence, that it may be embedded or segmented, or that it may be
    quoted — a page footer, a speaker note, and an invisible-layer string are all
    ``text_usable`` and none of them is body content. Those decisions belong to
    ``source.py`` and ``segments.py``, which read ``content_layer``,
    ``content_source``, and ``coordinate_grade`` to make them.

    ``heading_path`` is the heading context *of this element's own content layer*.
    A body heading never annotates a note, and a heading in furniture,
    background, invisible, or notes never annotates body text.
    """

    ordinal: int
    kind: str
    text: str
    start_char: int
    end_char: int
    tree_level: int
    content_source: str
    content_layer: str
    text_usable: bool
    coordinate_grade: str
    heading_path: tuple[str, ...] = ()
    parser_ref: str = ""
    parent_ref: str | None = None
    regions: tuple[PageRegion, ...] = ()


@dataclass(frozen=True)
class ParsedDocument:
    """The project-owned result of one parse: exact text, elements, and what was lost."""

    text: str
    elements: tuple[ParsedElement, ...]
    tables: tuple[ParsedTable, ...]
    omissions: tuple[ParsedOmission, ...]
    source_sha256: str
    source_bytes: int
    input_format: str
    element_separator: str = ELEMENT_SEPARATOR
    offsets: OffsetSemantics = PARSED_TEXT_OFFSETS
    evidence_grade: str = PARSER_EVIDENCE_GRADE


@dataclass(frozen=True)
class ParserPolicy:
    """Every output-affecting setting this parser holds, hashed into ``parser_id``.

    The five ``ConvertPipelineOptions`` flags are stated rather than left to a
    Docling default, so an upgrade cannot quietly enable a model or a plugin.
    ``mapping_revision``, ``table_serialization``, ``element_separator``,
    ``heading_kinds``, ``text_offsets``, and ``content_layers`` are the semantics
    that decide what the mapped output *is*, so a change to any of them changes
    ``parser_id`` rather than silently producing different records under the old
    identity.

    The ``max_*`` bounds beyond ``max_source_bytes`` are the mapping bounds this
    adapter enforces itself; recording them means a receipt states the limits the
    output was produced under, and each names its own scope. ``max_cells_per_table``
    bounds one table; ``max_total_table_cells`` is the document-scope comparator
    for ``ParserCall.table_cell_count``. ``max_mapped_characters`` and
    ``max_table_cell_characters`` are two different quantities and say so: the
    first bounds the text a consumer receives, the second the cell values the
    parse retains whether or not they are mapped, which is what a whitespace-only
    table costs. The paired ``*_per_item`` and
    ``*_total_*`` bounds do the same for caption references and page regions: one
    provider item cannot grow one record without bound, and one document cannot
    grow the parse without bound. ``max_page_number``,
    ``max_provenance_char_index``, ``max_tree_depth``, and ``max_error_type_chars``
    bound the provider-controlled *scalars* a completed record carries — a page
    ordinal, a provenance character index, an element's tree depth, and a recorded
    exception type name — so no field of an accepted record is an unbounded
    provider integer or an unbounded provider string.

    The three ``*_enforced`` flags are false on purpose, and ``max_page_number``
    does not soften ``page_limit_enforced``: bounding a page ordinal a record may
    carry is not the same as stopping a conversion at a page. ``SimplePipeline``
    hands the whole file to a declarative backend in one call and never reads
    ``document_timeout``; it exposes no pre-conversion page gate either, so there
    is no point at which a page limit could be applied; and no
    in-process adapter can enforce a wall-clock, CPU, memory, or
    archive-expansion bound on a library call it is inside of — that containment
    is ``source.py``'s process gate, and claiming it here would be a false
    receipt. ``max_source_bytes`` is enforced here before any byte is written.
    """

    pipeline: str
    mapping_revision: str
    content_layers: tuple[str, ...]
    element_separator: str
    heading_kinds: tuple[str, ...]
    table_serialization: str
    text_offsets: OffsetSemantics
    supported_formats: tuple[str, ...]
    max_source_bytes: int
    max_items: int
    max_tables: int
    max_cells_per_table: int
    max_total_table_cells: int
    max_table_dimension: int
    max_mapped_characters: int
    max_table_cell_characters: int
    max_heading_level: int
    max_tree_depth: int
    max_reference_chars: int
    max_caption_refs_per_item: int
    max_total_caption_refs: int
    max_regions_per_item: int
    max_total_regions: int
    max_page_number: int
    max_provenance_char_index: int
    max_provider_errors: int
    max_error_type_chars: int
    remote_services_enabled: bool
    external_plugins_allowed: bool
    picture_classification_enabled: bool
    picture_description_enabled: bool
    chart_extraction_enabled: bool
    document_timeout_enforced: bool
    page_limit_enforced: bool
    process_containment_enforced: bool
    converter_source: str

    def as_json_dict(self) -> dict[str, Any]:
        return _json_safe(self)

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json(self.as_json_dict()).encode()).hexdigest()


@dataclass(frozen=True)
class ParserCall:
    """The secret-free record of one parse, success or failure.

    Deeply immutable by construction, and :meth:`as_json_dict` is the only
    conversion a receipt writer needs. One parse is one call: this adapter never
    retries, because a local parse of the same bytes with the same policy
    produces the same result or the same failure.

    ``usable_element_count`` and ``usable_character_count`` count elements and
    characters whose nonblank parser text was preserved, across every content
    layer. Like :attr:`ParsedElement.text_usable`, they claim nothing about
    evidence, embedding, segmentation, or quotation eligibility.
    """

    provider: str
    operation: str
    package_name: str
    package_version: str
    core_package_name: str
    core_package_version: str
    parser_id: str
    policy_digest: str
    policy: ParserPolicy
    source_name: str
    source_name_sha256: str
    source_name_sanitized: bool
    media_type: str | None
    input_format: str
    source_sha256: str
    source_bytes: int
    conversion_status: str
    provider_error_count: int
    provider_error_categories: tuple[str, ...]
    provider_input_format: str | None
    page_count: int | None
    element_count: int
    usable_element_count: int
    usable_character_count: int
    character_count: int
    content_layers_present: tuple[str, ...]
    table_count: int
    table_cell_count: int
    omission_count: int
    omitted_kinds: tuple[str, ...]
    elements_without_coordinates: int
    coordinate_grade: str
    evidence_grade: str
    offsets: OffsetSemantics
    status: str
    provider_invoked: bool
    attempt_count: int
    duration_ms: float
    failure_reason: str | None
    error_type: str | None

    def as_json_dict(self) -> dict[str, Any]:
        """Serialize for a receipt or Parquet row; no provider or mutable value inside."""
        return _json_safe(self)


@dataclass(frozen=True)
class ParsedDocumentResult:
    """One parser call: the parsed document and its immutable call record."""

    document: ParsedDocument
    call: ParserCall


class DocumentParser(Protocol):
    """Exact source bytes in; project-owned text, elements, and a call record out.

    This is the fallback-parser interface, kept deliberately narrow and separate
    from the four provider interfaces the v3 design names. It is not a generic
    provider abstraction: ``source.py`` is its only consumer.
    """

    provider: str
    parser_id: str
    production_provider: bool
    supported_formats: frozenset[str]

    def parse(
        self,
        content: bytes,
        *,
        source_name: str,
        media_type: str | None = None,
    ) -> ParsedDocumentResult: ...


def installed_package_version(package: str) -> str | None:
    """Report an installed distribution version without importing the package."""
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def require_pinned_docling_versions(
    version_reader: Callable[[str], str | None] = installed_package_version,
) -> tuple[str, str]:
    """Hold every parse to the pinned Docling and docling-core builds.

    ``version_reader`` resolves installed distribution versions, so the pin is
    verified against an installation rather than declared by a caller. A missing
    package and a wrong version are the same failure: the parser this run would
    use is not the one the decision record selected. Both packages matter — table,
    formula, and content-layer behavior is docling-core's.

    What the reader hands back is an untrusted string like every other boundary
    value here, and it reaches further than most: both comparisons, both recorded
    version fields, and ``parser_id``, which is built by interpolation. So the
    exact built-in type is settled before anything compares, formats, converts,
    hashes, or measures it, and what is persisted afterwards is this module's own
    pinned constants.

    Raising is a shape the reader can take like any value it returns, and it does
    so before a parser exists — before an identity, a policy, or a record. So each
    lookup is normalized where it happens, into this adapter's own fixed sentence
    naming this module's own package constant. Nothing is read off what the
    callback raised: not its message, not its type, not an attribute it carries.
    A hostile one raising an exact private refusal would otherwise state a code
    this module declares and go on to select fixed text for a bound nothing
    reached.
    """
    for package, pinned in ((DOCLING_PACKAGE, DOCLING_VERSION), (DOCLING_CORE_PACKAGE, DOCLING_CORE_VERSION)):
        try:
            found = version_reader(package)
        except Exception as error:
            # Deliberately broad, and deliberately mute: this is a distinct finding
            # from a missing version and from a wrong one, and it says only that
            # the pin for this package could not be checked.
            raise DoclingUnavailableError(f"{package} version could not be verified") from error
        # Exact built-in first, and nothing else touches the value until it holds.
        # ``version_reader`` is injected, so what it returns is an untrusted string
        # boundary: a ``str`` subclass defines ``__eq__`` and ``__ne__``, so it can
        # declare itself equal to both pins, and ``__format__``, so it can write
        # whatever it likes into the identity built by interpolation below. The
        # value is never interpolated into this message either — naming what was
        # found would run exactly the method being refused.
        if type(found) is not str or found != pinned:
            raise DoclingUnavailableError(f"{package} version differs from the pinned contract: {pinned} is required")
    # The adapter's own constants, not the caller's objects: once both pins verify,
    # both values are known exactly, and a published record should carry this
    # module's strings rather than keep a caller's alive inside a receipt.
    return (DOCLING_VERSION, DOCLING_CORE_VERSION)


# --- exact built-in scalars --------------------------------------------------
#
# The type primitives both sections below are built on. Every untrusted scalar —
# a caller's identifier and a provider's every field alike — is taken as the
# *exact* built-in type or refused. ``isinstance`` is not that test and neither is
# a conversion: an ``int`` or ``str`` subclass defines its own ``__lt__``,
# ``__gt__``, ``__eq__``, ``__hash__``, ``__len__``, ``__int__``, ``__str__``, and
# ``encode``. Every bound in this module is one of those, so a subclass answering
# each of them the way it likes slips ``10**100``, a negative, or a
# hundred-thousand-character string past the check and into a frozen project
# record — and ``int(value)`` is no repair, because it runs the subclass's own
# ``__int__``. ``type(value) is int`` and ``type(value) is str`` are the whole
# test: they reject ``bool`` (an ``int`` in Python) and every subclass, and the
# comparison, hash, or length that follows is the built-in's own and cannot be
# overridden. The pinned releases declare each of these fields as a plain ``int``,
# ``float``, ``str``, or ``bool``, and a pinned ``str`` enum's ``.value`` is an
# ordinary built-in string, so nothing the releases really emit is refused.


def _exact_int(value: Any, *, minimum: int, code: str) -> int:
    """Read one provider integer that must be an exact built-in ``int``.

    The type is settled before any comparison runs, so ``value < minimum`` is
    ``int``'s own and cannot answer for a subclass. The value is returned as it
    arrived because it is already the built-in type a record may carry.
    """
    if type(value) is not int or value < minimum:
        raise _AdapterRefusal(code)
    return value


def _exact_float(value: Any, *, code: str) -> float:
    """Read one provider coordinate that must be an exact built-in ``float``.

    An exact ``int`` converts, because ``int.__float__`` is the built-in's own. A
    ``float`` subclass does not: it decides what its own ``__float__`` returns,
    and the previous reader called ``float(value)`` once to test finiteness and
    again to store, so a subclass answering finite and then infinite put a value
    with no JSON spelling on a frozen record.
    """
    if type(value) is int:
        value = float(value)
    if type(value) is not float or not math.isfinite(value):
        raise _AdapterRefusal(code)
    return value


def _exact_text(value: Any, *, code: str) -> str:
    """Read one provider string that must be an exact built-in ``str``.

    Every length bound, join, hash, and set membership below runs on the value
    this returns, and a ``str`` subclass answers ``__len__``, ``__eq__``,
    ``__hash__``, ``encode``, and every method it likes. Refusing the subtype is
    what makes the checks that follow mean what they say.
    """
    if type(value) is not str:
        raise _AdapterRefusal(code)
    return value


def _canonical_text(value: str) -> str:
    """Copy any ``str`` into an exact built-in one, running none of its own methods.

    For the one boundary that stays deliberately lenient — a failure category on
    a parse that has already failed — where refusing a subtype would replace a
    real finding with a metadata complaint. ``str.__getitem__`` is looked up on
    ``str`` itself rather than on the value, so a subclass cannot substitute its
    own slicing, and a full slice of a subclass always builds a new exact ``str``
    over the characters really stored. Revalidation still happens afterwards,
    against the copy.
    """
    if type(value) is str:
        return value
    return str.__getitem__(value, slice(None))


# --- untrusted identifiers ---------------------------------------------------


def _secret_like(name: str) -> bool:
    """Judge a cleaned name conservatively: a credential word or an opaque token.

    The token rule is deliberately blunt — a long unseparated run of letters and
    digits is far more likely a signature than a document name — because a false
    positive costs a replaced label beside a preserved digest, and a false
    negative puts a secret in a published table.
    """
    if _CREDENTIAL_WORDS.search(name):
        return True
    for token in _NAME_TOKENS.split(name):
        if len(token) < _SECRET_TOKEN_CHARS:
            continue
        if all(character in _HEX_CHARACTERS for character in token):
            return True
        if any(character.isdigit() for character in token) and any(character.isalpha() for character in token):
            return True
    return False


def bounded_error_type(value: Any) -> str:
    """Reduce anything that names an exception type to one short, safe ASCII token.

    The single place an exception's *identity* becomes text. A provider exception
    is an ordinary Python object: its class name is provider-controlled, may be
    built at runtime, and may be a hundred thousand characters long, hold a path,
    a newline, or a credential. That name reaches a public failure message and a
    persisted ``ParserCall.error_type``, so every path that records one comes
    through here — including the receipt boundary itself, which re-checks a token
    it was handed rather than trusting it.

    Only a plain ASCII class-name token survives: an identifier-shaped name inside
    :data:`MAX_ERROR_TYPE_CHARS` that :func:`_secret_like` does not judge opaque or
    credential-bearing. Anything else becomes :data:`FALLBACK_ERROR_TYPE`, which
    is a fixed token rather than a truncation, so no fragment of a hostile name is
    carried. A name is replaced whole or kept whole.

    The length bound is applied here and only here. ``_EXCEPTION_TYPE_NAME`` states
    shape alone, so the number this compares against is the one
    ``ParserPolicy.max_error_type_chars`` records — raising it really admits longer
    names, and lowering it to :data:`MIN_ERROR_TYPE_CHARS` really refuses shorter
    ones while the fallback still fits.

    Accepts an exception, a name, or ``None``, because those are the three things
    a caller has: reading ``__name__`` can itself raise from a hostile metaclass,
    and that must not escape the one helper written to contain it. A metaclass may
    also *return* a ``str`` subclass from ``__name__`` — one whose ``__len__``
    understates the characters it really holds — so only an exact built-in string
    is measured at all, and the recorded bound is applied to it directly rather
    than left to the pattern that happens to encode the same number.
    """
    name = value
    if isinstance(value, BaseException):
        try:
            name = type(value).__name__
        except Exception:
            # Deliberately broad: a metaclass may raise from ``__name__``, and no
            # failure path may fail again while recording why it failed.
            return FALLBACK_ERROR_TYPE
    if type(name) is not str or len(name) > MAX_ERROR_TYPE_CHARS:
        return FALLBACK_ERROR_TYPE
    if not _EXCEPTION_TYPE_NAME.fullmatch(name) or _secret_like(name):
        return FALLBACK_ERROR_TYPE
    return name


def encoded_source_name(source_name: Any) -> bytes:
    """Encode a caller's identifier in the one encoding a record can carry.

    These are the exact bytes ``source_name_sha256`` covers, so this is also where
    a string that is not really text is refused. A Python ``str`` may hold an
    unpaired surrogate, which has no UTF-8 encoding at all: hashing it raises
    ``UnicodeEncodeError`` from inside the identity build, which is neither a
    stable failure a caller can handle nor one carrying a record. Refused here,
    before anything is hashed, sanitized, or handed to the provider, it is the
    same fail-closed refusal as any other unusable argument.

    A ``str`` subtype is refused for the same reason and one more: it defines its
    own ``encode``, so the digest a record joins on would cover bytes the subtype
    chose rather than the identifier the caller named.
    """
    _exact_text(source_name, code="source_name_not_text")
    try:
        return source_name.encode(SOURCE_NAME_ENCODING)
    except UnicodeEncodeError as error:
        raise _AdapterRefusal("source_name_not_encodable") from error


def sanitized_source_name(source_name: Any) -> tuple[str, bool]:
    """Reduce an untrusted logical identifier to a bounded, secret-free name.

    A caller's ``source_name`` may be a signed URL, an absolute path, or a token.
    Only the final path component survives, without query or fragment, restricted
    to a safe character set; only a suffix this module recognizes as a format is
    carried onto a replaced or truncated name, so a hostile 300-character
    "extension" cannot ride along. Every branch is bounded by
    :data:`MAX_SOURCE_NAME_CHARS`.

    The second value is ``source_name_sanitized``: true whenever the persisted
    name differs at all from the caller's exact input, so a reader joins on
    ``source_name_sha256``, which always covers that exact value. Both promises
    need an exact built-in string: a subtype defines ``split``, ``strip``, and
    ``__eq__``, so it could steer what is persisted and then declare the result
    unchanged.
    """
    _exact_text(source_name, code="source_name_not_text")
    without_fragment = source_name.split("#", 1)[0]
    without_query = without_fragment.split("?", 1)[0]
    component = PurePosixPath(without_query.replace("\\", "/")).name
    cleaned = _UNSAFE_NAME_CHARACTERS.sub("_", component).strip("._-")
    suffix = Path(cleaned.lower()).suffix
    suffix = suffix if suffix in RECOGNIZED_SUFFIXES else ""
    if not cleaned:
        persisted = _FALLBACK_SOURCE_NAME
    elif _secret_like(cleaned):
        persisted = f"{_FALLBACK_SOURCE_NAME}{suffix}"
    else:
        persisted = cleaned
    if len(persisted) > MAX_SOURCE_NAME_CHARS:
        stem = persisted[: max(1, MAX_SOURCE_NAME_CHARS - len(suffix))]
        persisted = f"{stem}{suffix}"[:MAX_SOURCE_NAME_CHARS]
    return (persisted, persisted != source_name)


def validated_media_type(media_type: Any) -> str:
    """Bound and validate a caller-supplied media type before it is persisted.

    The runtime type is checked first, and exactly. A caller may hand over
    anything; without this, a non-string reached ``len()`` and left the caller a
    bare ``TypeError`` and no call record at all. Refusing it here makes it the
    same recorded fail-closed refusal as an unparseable media type, and the
    caller's value is never persisted either way. A ``str`` subtype is refused
    with it: it answers ``__len__`` and ``split`` itself, so it could pass the
    length bound while carrying far more, and land in a record as a provider type.
    """
    if type(media_type) is not str:
        raise _AdapterRefusal("media_type_not_text")
    if len(media_type) > MAX_MEDIA_TYPE_CHARS:
        raise _AdapterRefusal("media_type_too_long")
    bare = media_type.split(";", 1)[0].strip().lower()
    if not _MEDIA_TYPE.fullmatch(bare):
        raise _AdapterRefusal("media_type_malformed")
    return bare


def detect_input_format(source_name: str, media_type: str | None) -> tuple[str, str]:
    """Resolve one recognized input format and the suffix the adapter will write.

    The logical name and the media type must agree when both resolve; a conflict
    is a fail-closed condition, because the wrong backend is the wrong parse. A
    media type outside the recognized set does not veto a recognized extension —
    Docling's content sniffing plus the provider-format check still fail a real
    mismatch closed. The returned suffix is the adapter's own, so an untrusted
    name never reaches the filesystem. A recognized-but-deferred format (PDF,
    image) resolves here so the caller can refuse it by name; only an
    unrecognized input raises from this function.
    """
    by_media = RECOGNIZED_MEDIA_TYPES.get(media_type) if media_type else None
    by_suffix = RECOGNIZED_SUFFIXES.get(Path(source_name.lower()).suffix)
    if by_media and by_suffix and by_media[0] != by_suffix[0]:
        raise _AdapterRefusal("format_conflict")
    resolved = by_media or by_suffix
    if resolved is None:
        raise _AdapterRefusal("format_unrecognized")
    return resolved


# --- provider reading --------------------------------------------------------


def _enum_text(value: Any) -> str:
    """Read a provider enum or string as plain text; no provider type escapes.

    Lenient on purpose, and used for one thing only: provider error categories,
    which are filtered against :data:`_SAFE_TOKEN` afterwards and only ever read
    on a parse that is *already failing*, where refusing to normalize would
    replace a real finding with a metadata complaint. Every provider token this
    adapter records on an accepted parse goes through :func:`_known_token`, which
    demands exact membership in a closed set from the pinned release.

    Lenient about the *shape*, never about the type that leaves: a subtype is
    copied into a built-in string through :func:`_canonical_text` before anything
    measures it, so the token that reaches a record is this project's own.
    """
    if value is None:
        return ""
    inner = _field(value, "value", value)
    if isinstance(inner, str):
        return _canonical_text(inner)
    # ``str()`` runs a provider ``__str__``, which may itself return a subtype.
    return _canonical_text(_provider_string(inner))


def _known_token(value: Any, allowed: frozenset[str] | tuple[str, ...], *, code: str) -> str:
    """Read one provider enum or string that must be a member of a closed set.

    Where the pinned release defines every value a field can hold, exact
    membership is the bound — same spelling, same case. An unrecognized member
    fails the parse instead of being recorded as though this adapter knew what it
    meant, and a differently-cased one fails instead of being lowercased into a
    value that did not come from the enum.

    Membership is only a bound if the value is a built-in string: a ``str``
    subclass carries its own ``__eq__`` and ``__hash__``, so it can answer any
    membership test it likes and then be recorded as the label, layer, status,
    format, or origin it claimed to be. A pinned enum's ``.value`` is an ordinary
    built-in string, so the exact check costs the release nothing.
    """
    inner = _field(value, "value", value)
    if type(inner) is not str or inner not in allowed:
        raise _AdapterRefusal(code)
    return inner


def _reference_text(value: Any) -> str | None:
    """Read a Docling in-document reference as an opaque, bounded string.

    ``None`` in is ``None`` out: a root item legitimately has no parent. Anything
    else that is not a non-empty reference string is malformed. The length bound
    is this adapter's own — the pinned ``RefItem`` pattern constrains a
    reference's shape but not its size — and these strings are persisted on every
    element, table, and omission.
    """
    if value is None:
        return None
    reference = _exact_text(_field(value, "cref", value), code="reference_malformed")
    if not reference:
        raise _AdapterRefusal("reference_malformed")
    if len(reference) > MAX_REFERENCE_CHARS:
        raise _MappingLimitExceeded("reference_too_long")
    return reference


def _reference_texts(value: Any) -> tuple[str, ...]:
    """Read a bounded list of in-document references; a malformed entry fails closed.

    Skipping one would silently unlink a caption from its table, exactly the quiet
    loss this adapter exists to prevent. An item with no ``captions`` attribute is
    not malformed — that is ``None``, and yields no references. The cardinality is
    bounded *while the list is read*, so an unbounded provider list cannot be held
    before anything checks it.
    """
    if value is None:
        return ()
    references: list[str] = []
    for entry in _provider_entries(value, "reference_list_malformed"):
        reference = _reference_text(entry)
        if reference is None:
            raise _AdapterRefusal("reference_malformed")
        references.append(reference)
        if len(references) > MAX_CAPTION_REFS_PER_ITEM:
            raise _MappingLimitExceeded("caption_refs_per_item")
    return tuple(references)


def _grid_index(value: Any, *, minimum: int) -> int:
    """Read one table-cell grid coordinate, inside the largest grid this adapter maps.

    :func:`_validate_table_grid` already refuses any cell outside the declared
    grid, and the declared grid is held to :data:`MAX_TABLE_DIMENSION` — so no
    coordinate past that bound could ever reach an accepted record. Naming the
    bound here refuses one as the cell is read, before span arithmetic runs on an
    arbitrary-precision integer, and names the same limit the declared shape
    would have been refused under.
    """
    index = _exact_int(value, minimum=minimum, code="table_cell_geometry")
    if index > MAX_TABLE_DIMENSION:
        raise _MappingLimitExceeded("table_cell_offset_bound")
    return index


def _character_span(value: Any) -> tuple[int | None, int | None]:
    """Read the provider's character span for one region, or reject its shape.

    Both indices land verbatim on a :class:`PageRegion`, so their magnitude is
    bounded as well as their shape: :data:`MAX_PROVENANCE_CHARACTER_INDEX` is the
    length of text this parse could map at all, and an index past it addresses
    nothing this adapter would keep.
    """
    if value is None:
        return (None, None)
    first, second = _provider_pair(value, "character_span_malformed")
    # Exact built-in integers first: the ordering and the magnitude bound below are
    # both comparisons, and a subclass answers those itself.
    start = _exact_int(first, minimum=0, code="character_span_malformed")
    end = _exact_int(second, minimum=0, code="character_span_malformed")
    if start > end:
        raise _AdapterRefusal("character_span_malformed")
    if end > MAX_PROVENANCE_CHARACTER_INDEX:
        # ``start <= end``, so bounding the end bounds both.
        raise _MappingLimitExceeded("character_span_bound")
    return (start, end)


def _page_regions(item: Any) -> tuple[PageRegion, ...]:
    """Map one item's provenance into verbatim, validated, bounded page rectangles.

    The cardinality is bounded *while the list is read*: provenance is a provider
    collection that lands verbatim on an accepted :class:`ParsedElement`,
    :class:`ParsedTable`, and :class:`ParsedOmission`, so an unbounded one would
    grow project-owned records without anything checking it.
    """
    provenance = _field(item, "prov", ())
    if provenance is None:
        return ()
    regions: list[PageRegion] = []
    for entry in _provider_entries(provenance, "provenance_not_sequence"):
        if len(regions) >= MAX_REGIONS_PER_ITEM:
            raise _MappingLimitExceeded("regions_per_item")
        page = _exact_int(_field(entry, "page_no", None), minimum=1, code="page_number_malformed")
        if page > MAX_PAGE_NUMBER:
            # A record bound on a persisted ordinal, not a limit on how many pages
            # a conversion may have: the conversion is already over by now.
            raise _MappingLimitExceeded("page_number_bound")
        box = _field(entry, "bbox", None)
        edges = [_exact_float(_field(box, edge, None), code="bounding_box_malformed") for edge in _BOUNDING_BOX_EDGES]
        char_start, char_end = _character_span(_field(entry, "charspan", None))
        regions.append(
            PageRegion(
                page_number=page,
                left=edges[0],
                top=edges[1],
                right=edges[2],
                bottom=edges[3],
                coordinate_origin=_known_token(
                    _field(box, "coord_origin", None),
                    COORDINATE_ORIGINS,
                    code="coordinate_origin_unknown",
                ),
                char_start=char_start,
                char_end=char_end,
            )
        )
    return tuple(regions)


@dataclass(frozen=True)
class _TableReading:
    """One provider table, already reduced to plain cell data."""

    row_count: int
    column_count: int
    cells: tuple[ParsedTableCell, ...]
    caption_refs: tuple[str, ...]


@dataclass(frozen=True)
class _ItemReading:
    """One provider item, already reduced to plain data."""

    kind: str
    content_layer: str
    text: str
    content_source: str
    parser_ref: str
    parent_ref: str | None
    tree_level: int
    heading_level: int
    regions: tuple[PageRegion, ...]
    table: _TableReading | None
    caption_refs: tuple[str, ...]


def _cell_flag(value: Any, *, code: str) -> bool:
    """Read a provider header flag as the boolean it declares, not as truthiness.

    ``column_header`` and ``row_header`` are ``bool`` fields on the pinned
    ``TableCell``. Coercing whatever arrives with ``bool()`` would turn a missing
    field into ``False`` and any non-empty provider object into ``True``, so a
    header row could be recorded either way without anything noticing.

    The exact built-in type, like every other scalar a record may hold, and for
    the same two reasons. ``isinstance`` asks the object what class it is, so an
    object answering ``bool`` passed the check and was returned unchanged onto a
    :class:`ParsedTableCell` — breaking the plain-record invariant and leaving
    strict JSON with a value it has no spelling for. And asking at all runs
    provider code, so an object raising from ``__class__`` chose what this
    adapter's own type check appeared to find. ``type(value) is bool`` consults
    nothing: it rejects every subclass and every impostor, and the pinned
    ``TableCell`` declares both fields as plain ``bool``.
    """
    if type(value) is not bool:
        raise _AdapterRefusal(code)
    return value


def _read_cell(cell: Any) -> ParsedTableCell:
    """Read one ``TableCell``, holding its two descriptions of one shape together.

    docling-core reports both half-open grid offsets and a row/column span, and
    they must agree: a cell that cannot be placed unambiguously is a shape this
    serialization cannot represent, so the parse fails closed rather than guessing
    which description to believe.
    """
    # Exact text, before anything measures it: a cell's characters are summed into
    # the serialized length that decides whether the grid may be built at all, and
    # a subclass that understates its own length would be laid out anyway.
    text = _exact_text(_field(cell, "text"), code="table_cell_text_not_text")
    row_start = _grid_index(_field(cell, "start_row_offset_idx"), minimum=0)
    row_end = _grid_index(_field(cell, "end_row_offset_idx"), minimum=1)
    column_start = _grid_index(_field(cell, "start_col_offset_idx"), minimum=0)
    column_end = _grid_index(_field(cell, "end_col_offset_idx"), minimum=1)
    # Required, not defaulted to 1: the pinned ``TableCell`` declares both spans
    # with a default, so a real cell always carries them. Supplying 1 for an
    # absent attribute would invent the agreement the check below is here to
    # verify, and a cell with no span at all would silently pass as unmerged.
    row_span = _grid_index(_field(cell, "row_span"), minimum=1)
    column_span = _grid_index(_field(cell, "col_span"), minimum=1)
    if (row_end - row_start, column_end - column_start) != (row_span, column_span):
        raise _AdapterRefusal("table_cell_span_disagreement")
    return ParsedTableCell(
        row_start=row_start,
        row_end=row_end,
        column_start=column_start,
        column_end=column_end,
        text=text,
        column_header=_cell_flag(_field(cell, "column_header"), code="table_cell_column_header"),
        row_header=_cell_flag(_field(cell, "row_header"), code="table_cell_row_header"),
    )


def _validate_table_grid(reading: _TableReading) -> None:
    """Hold the declared grid to a real shape, and every cell inside it and alone.

    ``num_rows``/``num_cols`` and the cell rectangles are two descriptions of one
    shape, so a cell reaching past the declared grid fails the parse instead of
    silently widening it — the same reason a cell whose span contradicts its
    offsets fails. Overlap between *complete* rectangles is refused too, not just a
    shared top-left: two merged cells covering one position mean the grid cannot
    hold both values, and a serialization that wrote one of them would drop the
    other.

    The declared shape is checked before the cells are examined at all, in three
    steps. A grid is either wholly empty (``0x0``) or positive in both dimensions:
    ``0xN`` and ``Nx0`` describe a table with rows but no columns, or columns but
    no rows, which is not a grid any cell can sit in and not a shape this
    serialization can lay out. Each declared dimension is then bounded on its own,
    because bounding only the product lets one dimension be declared
    astronomically large beside a small one. Only then is the area bounded — so a
    grid declared enormous and delivered empty cannot pass as a table this adapter
    checked.
    """
    rows, columns = reading.row_count, reading.column_count
    if (rows == 0) != (columns == 0):
        raise _AdapterRefusal("table_grid_half_empty")
    if rows > MAX_TABLE_DIMENSION or columns > MAX_TABLE_DIMENSION:
        raise _MappingLimitExceeded("table_dimension_bound")
    if rows * columns > MAX_CELLS_PER_TABLE:
        raise _MappingLimitExceeded("table_cell_bound")
    # Bounded by the area check above: every rectangle is inside the declared grid
    # and no position is visited twice, so this marks at most MAX_CELLS_PER_TABLE.
    occupied: set[tuple[int, int]] = set()
    for cell in reading.cells:
        if cell.row_end > reading.row_count or cell.column_end > reading.column_count:
            raise _AdapterRefusal("table_cell_outside_grid")
        for row in range(cell.row_start, cell.row_end):
            for column in range(cell.column_start, cell.column_end):
                if (row, column) in occupied:
                    raise _AdapterRefusal("table_cells_overlap")
                occupied.add((row, column))


def _read_table(item: Any, kind: str, cell_room: int) -> _TableReading | None:
    """Read a ``TableItem``'s cells, which are the only place its values exist.

    The label and the shape have to agree, in both directions. In the pinned
    docling-core, ``TableItem`` is the only item class declaring ``data:
    TableData``, and its label is one of :data:`TABLE_KINDS`. So a table-labeled
    item with no readable ``TableData`` is a table whose values are missing —
    recording it as an ordinary text element would drop every cell, the exact
    loss this adapter exists to prevent — and any other label carrying
    ``table_cells`` is an item this mapping would silently promote to a table,
    inventing a :class:`ParsedTable` the provider never described.

    ``cell_room`` is what is left of :data:`MAX_TABLE_CELL_CHARACTERS`. Cells are
    kept verbatim on a :class:`ParsedTable` whether or not they become mapped
    text, so their characters are counted *as they arrive* under the bound that
    names what it limits — retained cell data — rather than under the
    mapped-character bound, which describes text a consumer receives.
    """
    data = _field(item, "data", None)
    cells = _field(data, "table_cells")
    declares_table = data is not None and cells is not _MISSING
    if kind not in TABLE_KINDS:
        if declares_table:
            raise _AdapterRefusal("table_data_under_non_table")
        return None
    if not declares_table:
        raise _AdapterRefusal("table_label_without_data")
    read: list[ParsedTableCell] = []
    characters = 0
    for cell in _provider_entries(cells, "table_cells_not_sequence"):
        parsed = _read_cell(cell)
        read.append(parsed)
        characters += len(parsed.text)
        if len(read) > MAX_CELLS_PER_TABLE:
            raise _MappingLimitExceeded("table_cell_bound")
        if characters > cell_room:
            raise _MappingLimitExceeded("table_cell_characters")
    reading = _TableReading(
        # Required, not defaulted: a table whose declared shape is missing is
        # malformed, and defaulting to zero would call it an empty grid.
        row_count=_exact_int(_field(data, "num_rows"), minimum=0, code="table_shape_malformed"),
        column_count=_exact_int(_field(data, "num_cols"), minimum=0, code="table_shape_malformed"),
        cells=tuple(read),
        caption_refs=_reference_texts(_field(item, "captions", None)),
    )
    _validate_table_grid(reading)
    return reading


def _table_cell_characters(reading: _TableReading) -> int:
    """The characters a table's cells hold, mapped or not — what the parse retains."""
    return sum(len(cell.text) for cell in reading.cells)


def _table_has_semantic_content(reading: _TableReading) -> bool:
    """Whether a table holds any character worth mapping, decided before any layout.

    :func:`_has_semantic_content` is the same predicate :func:`_assemble` applies
    to the finished string, asked earlier and per cell, so the two cannot
    disagree. A table whose every cell and separator is whitespace serializes to a
    truthy string that ``_assemble`` then drops as an omission: charging that
    string to the mapped-character budget reserved room for text no consumer ever
    receives, and near the ceiling refused a document that really fits.
    """
    return any(_has_semantic_content(cell.text) for cell in reading.cells)


def _serialized_table_length(reading: _TableReading) -> int:
    """The exact length :func:`_serialized_table_text` will produce, computed first.

    The grid is laid out from the *declared* shape, which
    :func:`_validate_table_grid` has already held to a real, bounded rectangle
    containing every cell exactly once — so the serialized length is arithmetic on
    numbers already in hand: every cell's own text, one tab between the columns of
    each row, and one newline between rows. Knowing it before the grid is
    allocated is what lets an over-budget table be refused without materializing
    or copying it; a single cell holding more characters than the whole document
    may map is refused on the strength of its length alone.
    """
    if not reading.cells:
        return 0
    tabs = reading.row_count * (reading.column_count - 1)
    newlines = reading.row_count - 1
    return sum(len(cell.text) for cell in reading.cells) + tabs + newlines


def _serialized_table_text(reading: _TableReading) -> str:
    """Lay the provider's cells out deterministically as tab-separated rows.

    A serialization of data the provider already produced, not a second table
    parser: every value also survives verbatim in :class:`ParsedTable`. The grid
    is exactly the shape the provider declared — :func:`_validate_table_grid` has
    already refused any cell outside it and any overlap — so a merged cell writes
    once, at its top-left position, leaving the rest of its span empty, and no
    cell can silently displace another.

    Nothing calls this before :func:`_serialized_table_length` has been checked
    against the document's remaining character budget: this function is the
    allocation, so the refusal has to happen ahead of it.
    """
    if not reading.cells:
        return ""
    grid = [["" for _ in range(reading.column_count)] for _ in range(reading.row_count)]
    for cell in reading.cells:
        grid[cell.row_start][cell.column_start] = cell.text
    return "\n".join("\t".join(row) for row in grid)


def _within_budget(length: int, remaining: int, *, code: str) -> None:
    """Hold one item's mapped text to what is left of the document's budget."""
    if length > remaining:
        raise _MappingLimitExceeded(code)


def _read_item_text(item: Any, kind: str, *, mapped_room: int, cell_room: int) -> tuple[str, str, _TableReading | None]:
    """Read one item's content from wherever the provider actually keeps it.

    ``mapped_room`` is what is left of :data:`MAX_MAPPED_CHARACTERS` once
    everything mapped before this item, and the separator that would precede it,
    is counted; ``cell_room`` is what is left of
    :data:`MAX_TABLE_CELL_CHARACTERS`. Every branch is measured against its own
    bound *before* the text becomes a string this adapter holds, so an
    over-budget document is refused rather than built and then rejected.
    """
    table = _read_table(item, kind, cell_room)
    if table is not None:
        if not _table_has_semantic_content(table):
            # Every cell blank, or no cells at all. The cells are still preserved
            # and ``_assemble`` still records the blank-table omission — but this
            # item contributes no mapped text, so it reserves no mapped-character
            # room, consumes no separator, and never builds a grid.
            return ("", NO_CONTENT, table)
        _within_budget(_serialized_table_length(table), mapped_room, code="table_text_characters")
        return (_serialized_table_text(table), CONTENT_FROM_TABLE_CELLS, table)
    raw = _field(item, "text")
    if raw is _MISSING:
        # A picture carries no ``text`` attribute at all. That is normal Docling
        # output, not malformed output; it becomes a recorded omission below.
        return ("", NO_CONTENT, None)
    text = _exact_text(raw, code="element_text_not_text")
    if _has_semantic_content(text):
        # The provider already holds this string; the bound stops it from being
        # kept and then joined into a second copy of an over-budget document.
        _within_budget(len(text), mapped_room, code="element_text_characters")
        return (text, CONTENT_FROM_TEXT, None)
    # docling-core keeps a pre-normalization ``orig`` beside ``text``; a formula
    # whose ``text`` was emptied still carries its source there.
    raw_original = _field(item, "orig", None)
    if raw_original is None:
        return ("", NO_CONTENT, None)
    original = _exact_text(raw_original, code="element_orig_not_text")
    if _has_semantic_content(original):
        _within_budget(len(original), mapped_room, code="element_text_characters")
        return (original, CONTENT_FROM_ORIG, None)
    return ("", NO_CONTENT, None)


def _tree_level(value: Any) -> int:
    """Read the provider's own tree depth for one item, or refuse it.

    ``iterate_items`` yields ``(item, level)`` and that level lands verbatim on
    every :class:`ParsedElement`, so it is a provider-controlled integer like any
    other. A bool is not a depth, a subtype of ``int`` is not a depth this adapter
    can compare, a negative number is not a depth, and a value of any magnitude at
    all would be persisted with nothing in the receipt to read it against — so
    each is refused rather than quietly read as zero, which is what a lenient
    reader recorded before.
    """
    level = _exact_int(value, minimum=0, code="tree_level_malformed")
    if level > MAX_TREE_DEPTH:
        raise _MappingLimitExceeded("tree_level_bound")
    return level


def _read_item(item: Any, tree_level: int, *, mapped_room: int, cell_room: int) -> _ItemReading:
    """Read one provider item through the narrow surface this adapter depends on."""
    kind = _known_token(_field(item, "label", None), DOC_ITEM_LABELS, code="element_label_unknown")
    layer = _known_token(_field(item, "content_layer", None), CONTENT_LAYERS, code="content_layer_unknown")
    text, content_source, table = _read_item_text(item, kind, mapped_room=mapped_room, cell_room=cell_room)
    parser_ref = _exact_text(_field(item, "self_ref", None), code="element_reference_missing")
    if not parser_ref:
        raise _AdapterRefusal("element_reference_missing")
    if len(parser_ref) > MAX_REFERENCE_CHARS:
        raise _MappingLimitExceeded("element_reference_too_long")
    level = _field(item, "level", None)
    if level is None:
        # Docling gives numbered headings their own level and leaves a document
        # title without one; a title therefore sits above every numbered heading.
        heading_level = 0 if kind == "title" else 1
    else:
        # The pinned docling-core declares a heading level as
        # ``Annotated[int, Ge(1), Le(100)]``, so anything outside that — a bool, a
        # subtype answering its own comparisons, a level below one, or a deeper
        # one — did not come from the pinned model. An unbounded level would also
        # let one document grow every element's recorded heading path without
        # bound, because the per-layer stack keeps one entry per distinct level.
        heading_level = _exact_int(level, minimum=1, code="heading_level_malformed")
        if heading_level > MAX_HEADING_LEVEL:
            raise _AdapterRefusal("heading_level_bound")
    return _ItemReading(
        kind=kind,
        content_layer=layer,
        text=text,
        content_source=content_source,
        parser_ref=parser_ref,
        parent_ref=_reference_text(_field(item, "parent", None)),
        tree_level=tree_level,
        heading_level=heading_level,
        regions=_page_regions(item),
        table=table,
        caption_refs=table.caption_refs if table else _reference_texts(_field(item, "captions", None)),
    )


def _resolve_caption_refs(readings: Sequence[_ItemReading]) -> None:
    """Hold every caption reference to an element this parse really emitted.

    A caption is its own provider element, linked to the table or picture it
    describes only by reference, so the reference is the whole relationship. It has
    to be resolved once every item has been read, because a caption may be emitted
    after the item that names it.

    What is checked is *resolvability*, and nothing beyond it. The pinned
    ``DoclingDocument.add_table`` accepts ``caption: Optional[Union[TextItem,
    RefItem]]`` — any ``TextItem``, whatever its label, or a bare reference — and
    ``FloatingItem.captions`` is a plain ``list[RefItem]`` that nothing stops two
    items from pointing at the same entry of. So the provider's caption edges are
    many-to-many and unrestricted by label, and this adapter preserves them as
    declared rather than refusing shapes the release itself produces.

    What is still refused is a reference that cannot describe a relationship at
    all: one that lands on nothing (a link no consumer can follow), one that names
    its own item, and one that appears twice in a single item's list (which would
    record one edge as two). A malformed or empty reference has already failed in
    :func:`_reference_text`. Dropping any of these quietly would unlink a table
    from the only text that says what it holds — the loss this adapter exists to
    prevent — so the parse fails closed and names the condition.

    Nothing here grades a caption as evidence or decides what may be quoted:
    ``source.py`` reads the recorded edge and makes that call.
    """
    emitted = {reading.parser_ref for reading in readings}
    for reading in readings:
        if len(set(reading.caption_refs)) != len(reading.caption_refs):
            raise _AdapterRefusal("caption_named_twice")
        for reference in reading.caption_refs:
            if reference == reading.parser_ref:
                raise _AdapterRefusal("caption_names_itself")
            if reference not in emitted:
                raise _AdapterRefusal("caption_unresolved")


def _read_items(document: Any) -> tuple[_ItemReading, ...]:
    """Read every provider item in reading order, from every content layer.

    The recorded mapping bounds are enforced as items arrive, not after the whole
    document is held in memory, so provider output cannot grow this adapter's
    mapping without bound before anything checks it.

    Both character budgets are carried *into* each item rather than checked after
    it, because the mapping's largest allocations — a table's serialization, and
    the cell values a table retains whether or not they are mapped — happen inside
    the read. ``characters`` counts what :func:`_assemble` will join, separators
    included, so what an item is measured against is the document's real remaining
    room and not just its own size; ``cell_characters`` counts what the parse
    keeps on :class:`ParsedTable` records, which is a different quantity as soon
    as a table holds nothing but whitespace.
    """
    iterate = _field(document, "iterate_items", None)
    if not callable(iterate):
        raise _AdapterRefusal("no_iterate_items")
    try:
        entries = iterate(included_content_layers=set(CONTENT_LAYERS))
    except _ProviderFailure:
        raise
    except Exception as error:
        # Calling into the provider is provider execution like any other read.
        raise _provider_raised(error) from error
    readings: list[_ItemReading] = []
    seen: set[str] = set()
    tables = 0
    table_cells = 0
    cell_characters = 0
    caption_refs = 0
    regions = 0
    characters = 0
    for entry in _provider_entries(entries, "iteration_not_a_sequence"):
        # An exact built-in tuple, built at the boundary: what is unpacked here runs
        # no provider code, because deciding the entry's shape already happened
        # where a failure is honestly the provider's.
        item, level = _provider_entry(entry)
        # Text already mapped, plus the separator this item's own text would be
        # joined with, is room this item cannot have.
        separator = len(ELEMENT_SEPARATOR) if characters else 0
        reading = _read_item(
            item,
            _tree_level(level),
            mapped_room=max(MAX_MAPPED_CHARACTERS - characters - separator, 0),
            cell_room=max(MAX_TABLE_CELL_CHARACTERS - cell_characters, 0),
        )
        if reading.parser_ref in seen:
            raise _AdapterRefusal("element_reference_duplicated")
        seen.add(reading.parser_ref)
        readings.append(reading)
        if reading.table is not None:
            tables += 1
            table_cells += len(reading.table.cells)
            cell_characters += _table_cell_characters(reading.table)
        caption_refs += len(reading.caption_refs)
        regions += len(reading.regions)
        characters += len(reading.text) + (separator if reading.text else 0)
        if len(readings) > MAX_ITEMS:
            raise _MappingLimitExceeded("item_bound")
        if tables > MAX_TABLES:
            raise _MappingLimitExceeded("table_bound")
        if table_cells > MAX_TOTAL_TABLE_CELLS:
            # The document-scope comparator for ``ParserCall.table_cell_count``:
            # the per-table bound alone would leave that count ceilinged only by
            # MAX_TABLES x MAX_CELLS_PER_TABLE.
            raise _MappingLimitExceeded("total_table_cell_bound")
        if cell_characters > MAX_TABLE_CELL_CHARACTERS:
            # Defense in depth, like the mapped-character check below: every table
            # was already read against the room left before it arrived.
            raise _MappingLimitExceeded("total_table_cell_characters")
        if caption_refs > MAX_TOTAL_CAPTION_REFS:
            raise _MappingLimitExceeded("total_caption_refs")
        if regions > MAX_TOTAL_REGIONS:
            raise _MappingLimitExceeded("total_regions")
        if characters > MAX_MAPPED_CHARACTERS:
            # Defense in depth. Each item was already measured against the room
            # left before it was read, so reaching this means the accounting and
            # the mapping disagree — which is a refusal, not a rounding.
            raise _MappingLimitExceeded("mapped_characters")
    _resolve_caption_refs(readings)
    return tuple(readings)


@dataclass(frozen=True)
class _Mapping:
    """The whole project-owned mapping of one provider document."""

    text: str
    elements: tuple[ParsedElement, ...]
    tables: tuple[ParsedTable, ...]
    omissions: tuple[ParsedOmission, ...]


def _assemble(readings: Sequence[_ItemReading]) -> _Mapping:
    """Build this project's exact text, every element's span, and what was lost."""
    pieces: list[str] = []
    elements: list[ParsedElement] = []
    tables: list[ParsedTable] = []
    omissions: list[ParsedOmission] = []
    # One heading stack per content layer. A single shared stack let a heading in
    # furniture, notes, background, or invisible become the recorded context of the
    # body text that followed it — and let a body heading annotate a speaker note.
    # Each layer's headings describe that layer alone.
    headings: dict[str, list[tuple[int, str]]] = {layer: [] for layer in CONTENT_LAYERS}
    cursor = 0
    for ordinal, reading in enumerate(readings):
        # One predicate decides preservation everywhere: whitespace-only text
        # neither enters the parsed text nor counts as a preserved element.
        usable = _has_semantic_content(reading.text)
        text = reading.text if usable else ""
        # ``_read_item`` already held the layer to CONTENT_LAYERS, so this is a hit.
        layer_headings = headings[reading.content_layer]
        is_heading = reading.kind in HEADING_KINDS and usable
        if is_heading:
            # A heading's own path is its ancestors, so its peers and deeper
            # predecessors leave its layer's stack before the path is recorded.
            while layer_headings and layer_headings[-1][0] >= reading.heading_level:
                layer_headings.pop()
        start = cursor
        if text:
            if pieces:
                pieces.append(ELEMENT_SEPARATOR)
                cursor += len(ELEMENT_SEPARATOR)
                start = cursor
            pieces.append(text)
            cursor += len(text)
        coordinate_grade = PARSER_PAGE_COORDINATES if reading.regions else NO_COORDINATES
        elements.append(
            ParsedElement(
                ordinal=ordinal,
                kind=reading.kind,
                text=text,
                start_char=start,
                end_char=cursor,
                tree_level=reading.tree_level,
                content_source=reading.content_source,
                content_layer=reading.content_layer,
                text_usable=usable,
                coordinate_grade=coordinate_grade,
                heading_path=tuple(heading for _, heading in layer_headings),
                parser_ref=reading.parser_ref,
                parent_ref=reading.parent_ref,
                regions=reading.regions,
            )
        )
        if reading.table is not None:
            tables.append(
                ParsedTable(
                    parser_ref=reading.parser_ref,
                    parent_ref=reading.parent_ref,
                    element_ordinal=ordinal,
                    content_layer=reading.content_layer,
                    row_count=reading.table.row_count,
                    column_count=reading.table.column_count,
                    cells=reading.table.cells,
                    caption_refs=reading.table.caption_refs,
                    serialization=TABLE_SERIALIZATION,
                    serialization_ambiguous=any("\t" in cell.text or "\n" in cell.text for cell in reading.table.cells),
                    regions=reading.regions,
                    coordinate_grade=coordinate_grade,
                )
            )
        if not usable:
            omissions.append(
                ParsedOmission(
                    parser_ref=reading.parser_ref,
                    kind=reading.kind,
                    parent_ref=reading.parent_ref,
                    reason=(
                        "no-text-content"
                        if reading.table is None
                        else ("blank-table-cells" if reading.table.cells else "empty-table")
                    ),
                    element_ordinal=ordinal,
                    content_layer=reading.content_layer,
                    caption_refs=reading.caption_refs,
                    regions=reading.regions,
                    coordinate_grade=coordinate_grade,
                )
            )
        if is_heading:
            layer_headings.append((reading.heading_level, text))
    text = "".join(pieces)
    if len(text) > MAX_MAPPED_CHARACTERS:
        # The incremental bound in ``_read_items`` stops runaway growth; this is the
        # exact one, over the joined text a consumer actually receives.
        raise _MappingLimitExceeded("assembled_characters")
    for element in elements:
        if text[element.start_char : element.end_char] != element.text:
            raise _AdapterRefusal("text_round_trip")
    return _Mapping(text=text, elements=tuple(elements), tables=tuple(tables), omissions=tuple(omissions))


def _page_count(document: Any) -> int | None:
    """Count the pages the provider reported, or refuse a collection with no count.

    ``None`` is a real answer — a declarative Office backend renders no pages — but
    a page collection that cannot be counted is malformed metadata, not an absent
    count, and silently reading it as ``None`` would put "no pages" in a receipt
    for a document the provider described some other way.

    The count is bounded like every other persisted provider integer. That is a
    bound on the recorded scalar, not a page limit applied to the conversion:
    ``page_limit_enforced`` stays false because the conversion has already
    happened by the time this is read.
    """
    pages = _field(document, "pages", None)
    if pages is None:
        return None
    try:
        # ``len`` narrows through the C length slot, so this is a built-in ``int``
        # holding a real count no matter what ``__len__`` claims to return.
        count = len(pages)
    except TypeError as error:
        # No usable length *at all* — this adapter's own finding about the shape
        # of the metadata it was handed, not a provider failure.
        raise _AdapterRefusal("page_collection_length") from error
    except _ProviderFailure:
        raise
    except Exception as error:
        # A ``__len__`` that ran and then failed is provider code failing.
        raise _provider_raised(error) from error
    if count > MAX_PAGE_NUMBER:
        raise _MappingLimitExceeded("page_count_bound")
    return count


def _conversion_errors(conversion: Any) -> tuple[Any, ...]:
    """Read the provider's error list as a bounded sequence, or refuse its shape.

    ``tuple()`` over a value that is not iterable raises ``TypeError`` — which,
    uncaught, would leave the caller with a bare provider-shaped exception and no
    call record at all. The list is also provider metadata that reaches a record
    through ``provider_error_count`` and ``provider_error_categories``, so its
    cardinality is bounded while it is read like every other provider collection.
    """
    errors = _field(conversion, "errors", None)
    if errors is None:
        return ()
    read: list[Any] = []
    for error in _provider_entries(errors, "error_list_not_sequence"):
        read.append(error)
        if len(read) > MAX_PROVIDER_ERRORS:
            raise _MappingLimitExceeded("provider_error_bound")
    return tuple(read)


def _error_categories(errors: Iterable[Any]) -> tuple[str, ...]:
    """Keep the provider's closed failure categories; never its error messages.

    :func:`_enum_text` has already copied each value into a built-in string, so
    ``lower`` here is ``str``'s own and the token that survives
    :data:`_SAFE_TOKEN` is a project-owned scalar rather than a provider subtype
    that answered the pattern on its behalf.
    """
    categories = {_enum_text(_field(error, "category", None)).lower() for error in errors}
    return tuple(sorted(category for category in categories if _SAFE_TOKEN.fullmatch(category)))


def _provider_input_format(conversion: Any) -> str | None:
    """Read the backend the provider says it ran, as an exact ``InputFormat`` value.

    ``None`` means the provider named no format at all, which the caller refuses as
    an unproven pipeline. A present-but-malformed format is a different condition:
    malformed metadata, refused as such. Membership is exact — this token is
    compared against the format this adapter detected, so anything the pinned
    ``InputFormat`` cannot emit describes a pipeline nothing confirmed.
    """
    declared = _field(_field(conversion, "input", None), "format", None)
    if declared is None:
        return None
    return _known_token(declared, PROVIDER_INPUT_FORMATS, code="provider_format_malformed")


# --- effective configuration -------------------------------------------------


def _load_converter(policy: ParserPolicy) -> Any:
    """Build a Docling converter that applies this policy to the Office formats.

    The only function in the project that imports Docling, and it runs only when
    no converter was injected. All three formats run ``SimplePipeline`` over a
    declarative backend, so one options shape covers them; each gets its own
    instance so no format is reconfigurable through another's object.
    """
    # Resolvable only with the optional ``docling`` extra installed, which is
    # the point: the default environment parses native structure instead.
    from docling.datamodel.base_models import InputFormat  # ty: ignore[unresolved-import]
    from docling.datamodel.pipeline_options import ConvertPipelineOptions  # ty: ignore[unresolved-import]
    from docling.document_converter import (  # ty: ignore[unresolved-import]
        DocumentConverter,
        ExcelFormatOption,
        PowerpointFormatOption,
        WordFormatOption,
    )

    def simple_options() -> Any:
        return ConvertPipelineOptions(
            # No remote service and no plugin may substitute a model or backend,
            # and no enrichment stage may load one behind this parser's back.
            enable_remote_services=policy.remote_services_enabled,
            allow_external_plugins=policy.external_plugins_allowed,
            do_picture_classification=policy.picture_classification_enabled,
            do_picture_description=policy.picture_description_enabled,
            do_chart_extraction=policy.chart_extraction_enabled,
        )

    # ``dict[Any, Any]``: the three concrete option classes share the ``FormatOption``
    # base the converter declares, and a dict of the union is not that dict.
    options: dict[Any, Any] = {
        InputFormat.DOCX: WordFormatOption(pipeline_options=simple_options()),
        InputFormat.PPTX: PowerpointFormatOption(pipeline_options=simple_options()),
        InputFormat.XLSX: ExcelFormatOption(pipeline_options=simple_options()),
    }
    return DocumentConverter(allowed_formats=list(options), format_options=options)


@dataclass(frozen=True)
class _SourceIdentity:
    """What one parse call was asked to read, after validation."""

    source_name: str
    source_name_sha256: str
    source_name_sanitized: bool
    media_type: str | None
    input_format: str
    file_suffix: str
    sha256: str
    byte_count: int


@dataclass(frozen=True)
class _ProviderReading:
    """What the provider reported about one conversion, as plain data."""

    conversion_status: str = ""
    provider_error_count: int = 0
    provider_error_categories: tuple[str, ...] = ()
    provider_input_format: str | None = None
    page_count: int | None = None
    mapping: _Mapping = _Mapping(text="", elements=(), tables=(), omissions=())
    failure_reason: str | None = None
    error_type: str | None = None

    def replace(self, **changes: Any) -> _ProviderReading:
        return dataclasses.replace(self, **changes)


class DoclingDocumentParser:
    """Pinned Docling Office parser over ``docling.document_converter``.

    One parse is one call: it never retries, because a local parse of the same
    bytes with the same policy produces the same result or the same failure.
    There is no successful-empty outcome either — a parser that produces no
    usable character from real source bytes is a lossy parse, and the empty-input
    case is rejected before the provider is called.
    """

    provider = PARSER_PROVIDER
    package_name = DOCLING_PACKAGE
    core_package_name = DOCLING_CORE_PACKAGE
    production_provider = True

    def __init__(
        self,
        *,
        max_source_bytes: int = DEFAULT_MAX_SOURCE_BYTES,
        converter: Any | None = None,
        version_reader: Callable[[str], str | None] = installed_package_version,
    ) -> None:
        self.package_version, self.core_package_version = require_pinned_docling_versions(version_reader)
        # Exact, like every provider integer: this one is recorded in the policy,
        # hashed into ``parser_id``, and compared against the real byte count, so a
        # subtype answering its own comparisons would both weaken the bound and
        # put a caller's type in a published record.
        if type(max_source_bytes) is not int or max_source_bytes < 1:
            raise DoclingConfigurationError("max_source_bytes must be a positive integer")

        self.converter_source = "injected" if converter is not None else "loaded"
        self.policy = ParserPolicy(
            pipeline=PIPELINE_SIMPLE,
            mapping_revision=ADAPTER_MAPPING_REVISION,
            content_layers=CONTENT_LAYERS,
            element_separator=ELEMENT_SEPARATOR,
            heading_kinds=tuple(sorted(HEADING_KINDS)),
            table_serialization=TABLE_SERIALIZATION,
            text_offsets=PARSED_TEXT_OFFSETS,
            supported_formats=tuple(sorted(SUPPORTED_FORMATS)),
            max_source_bytes=max_source_bytes,
            # Policy constants, not caller settings: the bounds that keep converted
            # output from growing without bound, recorded so a receipt states them.
            max_items=MAX_ITEMS,
            max_tables=MAX_TABLES,
            max_cells_per_table=MAX_CELLS_PER_TABLE,
            max_total_table_cells=MAX_TOTAL_TABLE_CELLS,
            max_table_dimension=MAX_TABLE_DIMENSION,
            max_mapped_characters=MAX_MAPPED_CHARACTERS,
            max_table_cell_characters=MAX_TABLE_CELL_CHARACTERS,
            max_heading_level=MAX_HEADING_LEVEL,
            max_tree_depth=MAX_TREE_DEPTH,
            max_reference_chars=MAX_REFERENCE_CHARS,
            max_caption_refs_per_item=MAX_CAPTION_REFS_PER_ITEM,
            max_total_caption_refs=MAX_TOTAL_CAPTION_REFS,
            max_regions_per_item=MAX_REGIONS_PER_ITEM,
            max_total_regions=MAX_TOTAL_REGIONS,
            # Bounds on the provider-controlled scalars a record carries. A page
            # ordinal and a page count are recorded within ``max_page_number``;
            # that is a bound on what may be persisted, not a page limit applied
            # to a conversion, which is why ``page_limit_enforced`` stays false.
            max_page_number=MAX_PAGE_NUMBER,
            max_provenance_char_index=MAX_PROVENANCE_CHARACTER_INDEX,
            max_provider_errors=MAX_PROVIDER_ERRORS,
            max_error_type_chars=MAX_ERROR_TYPE_CHARS,
            remote_services_enabled=False,
            external_plugins_allowed=False,
            picture_classification_enabled=False,
            picture_description_enabled=False,
            chart_extraction_enabled=False,
            # SimplePipeline hands the whole file to a declarative backend in one
            # call, so it never reads ``document_timeout`` and exposes no
            # pre-conversion page gate a limit could be applied at. Nor can this
            # adapter stop the library call it is inside of on a clock, a memory
            # ceiling, or an archive that expands: that is ``source.py``'s process
            # gate. Saying so beats naming a false limit.
            document_timeout_enforced=False,
            page_limit_enforced=False,
            process_containment_enforced=False,
            converter_source=self.converter_source,
        )
        self.policy_digest = self.policy.digest
        self.parser_id = (
            f"{DOCLING_PACKAGE}:{self.package_version}"
            f":{DOCLING_CORE_PACKAGE}:{self.core_package_version}"
            # Named in the identity, not only hashed into it: the mapping revision
            # is the one part of a record's identity a reader may need to compare
            # by eye against this module.
            f":{ADAPTER_MAPPING_REVISION}"
            f":{self.policy_digest[:16]}"
        )
        self.supported_formats = frozenset(SUPPORTED_FORMATS)
        if converter is None:
            converter = _load_converter(self.policy)
        # Private on purpose: this is the one Docling object in the module, and
        # exposing it would hand a caller a provider type.
        self._converter = converter

    def parse(
        self,
        content: bytes,
        *,
        source_name: str,
        media_type: str | None = None,
    ) -> ParsedDocumentResult:
        """Parse exact source bytes into project records plus an immutable call record."""
        if not isinstance(content, (bytes, bytearray)):
            raise TypeError("source content must be bytes")
        if not content:
            raise ValueError("source bytes are empty; a parse needs exact source content")
        if type(source_name) is not str:
            # Exact, not ``isinstance``: a ``str`` subtype defines its own
            # ``encode``, ``split``, ``__len__``, and ``__eq__``, so it could steer
            # the persisted name, the digest that names it, or both.
            # No record accompanies this one, and deliberately so: every field of
            # a call record is derived from the caller's exact identifier — the
            # sanitized name and, above all, ``source_name_sha256``, which has to
            # cover that exact value. There is no safe encoding of a non-string to
            # hash, and inventing one would put a fabricated identity in a receipt
            # that a reader would join on. A recorded refusal needs an identity
            # this adapter can honestly construct; this one cannot.
            raise _AdapterRefusal("source_name_not_text")
        if not source_name:
            raise ValueError("source_name is required: it names the parsed source in the run record")
        # Before the name is hashed, sanitized, or a byte is written: an identifier
        # with no encoding is one no record can carry, and the same reasoning as
        # above applies — there is nothing honest to build a receipt from.
        encoded_source_name(source_name)
        payload = bytes(content)
        started = time.monotonic()
        identity = self._identity(payload, source_name, media_type, started=started)
        with tempfile.TemporaryDirectory(prefix="spicy-regs-docling-") as directory:
            # The adapter names the file, never the caller: an untrusted logical
            # identifier must not reach the filesystem or the format choice.
            path = Path(directory) / f"{_FALLBACK_SOURCE_NAME}{identity.file_suffix}"
            path.write_bytes(payload)
            try:
                conversion = self._converter.convert(path, raises_on_error=False)
            except Exception as error:
                # The exception's *type name* is provider-controlled text like any
                # other, so it is bounded and sanitized before it reaches either
                # the public message or the record. Nothing else about it is read:
                # a converter that raises one of this module's own private classes
                # is still the provider raising, and this stage text is fixed.
                error_type = _recorded_error_type(error)
                raise self._failure(
                    f"docling conversion failed with {error_type}",
                    identity=identity,
                    started=started,
                    reading=_ProviderReading(failure_reason="provider_error", error_type=error_type),
                ) from error
            reading = self._read_conversion(conversion, identity=identity, started=started)
        return ParsedDocumentResult(
            document=ParsedDocument(
                text=reading.mapping.text,
                elements=reading.mapping.elements,
                tables=reading.mapping.tables,
                omissions=reading.mapping.omissions,
                source_sha256=identity.sha256,
                source_bytes=identity.byte_count,
                input_format=identity.input_format,
            ),
            call=self._call_record(identity=identity, started=started, status="completed", reading=reading),
        )

    def _identity(
        self,
        payload: bytes,
        source_name: str,
        media_type: str | None,
        *,
        started: float,
    ) -> _SourceIdentity:
        """Validate and bound everything about the request before anything is written."""
        safe_name, sanitized = sanitized_source_name(source_name)

        def identify(input_format: str, suffix: str, checked_type: str | None) -> _SourceIdentity:
            return _SourceIdentity(
                source_name=safe_name,
                source_name_sha256=hashlib.sha256(encoded_source_name(source_name)).hexdigest(),
                source_name_sanitized=sanitized,
                media_type=checked_type,
                input_format=input_format,
                file_suffix=suffix,
                sha256=hashlib.sha256(payload).hexdigest(),
                byte_count=len(payload),
            )

        def refuse(message: str, identity: _SourceIdentity, reason: str) -> DoclingParseError:
            reading = _ProviderReading(failure_reason=reason)
            return self._failure(message, identity=identity, started=started, reading=reading, provider_invoked=False)

        try:
            checked_type = validated_media_type(media_type) if media_type is not None else None
            input_format, suffix = detect_input_format(safe_name, checked_type)
        except DoclingParseError as error:
            # An unusable media type is never persisted, and an unrecognized
            # format still gets a recorded fail-closed parse of real bytes. The
            # message comes from the refusal's code, never from the exception.
            raise refuse(
                _refusal_message(_refusal_code(error)),
                identify(FORMAT_UNKNOWN, "", None),
                "unsupported_input",
            ) from error
        identity = identify(input_format, suffix, checked_type)
        if input_format not in SUPPORTED_FORMATS:
            # Recognized, and deliberately not served in this slice. The record
            # names the real format so a later paginated adapter can find it.
            raise refuse(
                f"input format {input_format} is recognized but not implemented in this adapter: "
                "a paginated parse needs a content-addressed model manifest, one explicit OCR "
                "engine, and model-backed tests before it may run",
                identity,
                "format_not_implemented",
            )
        if identity.byte_count > self.policy.max_source_bytes:
            raise refuse("source bytes exceed the recorded input limit", identity, "source_bytes_over_limit")
        return identity

    def _read_conversion(
        self,
        conversion: Any,
        *,
        identity: _SourceIdentity,
        started: float,
    ) -> _ProviderReading:
        """Check the provider's own account of the parse, then map it."""

        def malformed(error: Exception, carried: _ProviderReading) -> DoclingParseError:
            # One rule, applied by origin rather than by class name: a refusal
            # states fixed text for its code and, for a bound, the reason that
            # names the bound; anything the provider raised contributes fixed
            # stage text plus its bounded type name alone. Either way the record
            # is complete and names the same stage.
            message, reason, error_type = _recorded_failure(
                error,
                stage_reason="malformed_conversion",
                provider_reason="malformed_conversion",
                stage_text="docling conversion metadata could not be read",
            )
            return self._failure(
                message,
                identity=identity,
                started=started,
                reading=carried.replace(failure_reason=reason, error_type=error_type),
            )

        try:
            errors = _conversion_errors(conversion)
            reading = _ProviderReading(
                conversion_status=_known_token(
                    _field(conversion, "status", None),
                    CONVERSION_STATUSES,
                    code="conversion_status_malformed",
                ),
                provider_error_count=len(errors),
                provider_error_categories=_error_categories(errors),
                provider_input_format=_provider_input_format(conversion),
            )
        except Exception as error:
            # Deliberately broad: reading provider metadata must not be able to
            # raise past this adapter. Nothing about the conversion could be
            # normalized, so the record carries none of it — that is the finding.
            raise malformed(error, _ProviderReading()) from error

        def fail(message: str, **changes: Any) -> DoclingParseError:
            # ``reading`` is read at call time, so every failure carries whatever
            # the provider had told us by then.
            return self._failure(message, identity=identity, started=started, reading=reading.replace(**changes))

        def mapping_failure(error: Exception, stage_reason: str, stage_text: str) -> DoclingParseError:
            # One place decides how a mapping failure is recorded, for both mapping
            # stages: a recorded bound names itself, a refusal states the stage
            # that made it, and anything the provider raised — a pydantic
            # property, a lazily built model, an iterator, whatever class it chose
            # — is provider output with fixed stage text and a bounded type name.
            message, reason, error_type = _recorded_failure(
                error,
                stage_reason=stage_reason,
                provider_reason="provider_error",
                stage_text=stage_text,
            )
            return fail(message, failure_reason=reason, error_type=error_type)

        if reading.conversion_status not in ACCEPTED_CONVERSION_STATUSES:
            raise fail("docling conversion status is not a success", failure_reason="conversion_status")
        if reading.provider_error_count:
            raise fail("docling reported conversion errors", failure_reason="provider_errors")
        if reading.provider_input_format is None:
            # A success has to say which backend ran. Without it the receipt
            # would describe a pipeline nothing confirmed.
            raise fail(
                "docling reported no input format for a successful conversion",
                failure_reason="provider_format_missing",
            )
        if reading.provider_input_format != identity.input_format:
            # Docling sniffs content and picks the backend itself. When that
            # disagrees with the format this adapter detected and recorded, the
            # parse ran through a pipeline the receipt does not describe.
            raise fail(
                "docling parsed a different input format than the one detected",
                failure_reason="format_mismatch",
            )
        try:
            # Reaching the document is metadata access too: a provider object can
            # raise from attribute lookup, and that must not escape as itself.
            document = _field(conversion, "document", None)
            reading = reading.replace(page_count=_page_count(document))
        except Exception as error:
            # Deliberately broad, for the same reason as the metadata read above.
            raise malformed(error, reading) from error
        try:
            items = _read_items(document)
        except Exception as error:
            # Deliberately broad, and routed through the one place that decides
            # whose message may be repeated.
            raise mapping_failure(error, "malformed_element", "docling document could not be read") from error
        if not items:
            raise fail("docling returned no elements for non-empty source bytes", failure_reason="no_elements")
        try:
            mapping = _assemble(items)
        except DoclingParseError as error:
            raise mapping_failure(error, "text_mapping", "mapped text could not be assembled") from error
        reading = reading.replace(mapping=mapping)
        if not _has_semantic_content(mapping.text):
            # Elements with no usable character between them is a lossy parse —
            # a table whose cells were blank, or a picture-only slide — not a
            # completed one. Every omission is already recorded above.
            raise fail("docling produced elements but no usable text", failure_reason="no_usable_text")
        return reading

    def _failure(
        self,
        message: str,
        *,
        identity: _SourceIdentity,
        started: float,
        reading: _ProviderReading,
        provider_invoked: bool = True,
    ) -> DoclingParseError:
        return DoclingParseError(
            message,
            call=self._call_record(
                identity=identity,
                started=started,
                status="failed",
                reading=reading,
                provider_invoked=provider_invoked,
            ),
        )

    def _call_record(
        self,
        *,
        identity: _SourceIdentity,
        started: float,
        status: str,
        reading: _ProviderReading,
        provider_invoked: bool = True,
    ) -> ParserCall:
        """Build the immutable, secret-free record of one parse, success or failure."""
        mapping = reading.mapping
        usable = tuple(element for element in mapping.elements if element.text_usable)
        return ParserCall(
            provider=self.provider,
            operation=PARSER_OPERATION,
            package_name=self.package_name,
            package_version=self.package_version,
            core_package_name=self.core_package_name,
            core_package_version=self.core_package_version,
            parser_id=self.parser_id,
            policy_digest=self.policy_digest,
            policy=self.policy,
            source_name=identity.source_name,
            source_name_sha256=identity.source_name_sha256,
            source_name_sanitized=identity.source_name_sanitized,
            media_type=identity.media_type,
            input_format=identity.input_format,
            source_sha256=identity.sha256,
            source_bytes=identity.byte_count,
            conversion_status=reading.conversion_status,
            provider_error_count=reading.provider_error_count,
            provider_error_categories=reading.provider_error_categories,
            provider_input_format=reading.provider_input_format,
            page_count=reading.page_count,
            element_count=len(mapping.elements),
            usable_element_count=len(usable),
            usable_character_count=sum(len(element.text) for element in usable),
            character_count=len(mapping.text),
            content_layers_present=tuple(sorted({element.content_layer for element in mapping.elements})),
            table_count=len(mapping.tables),
            table_cell_count=sum(len(table.cells) for table in mapping.tables),
            omission_count=len(mapping.omissions),
            omitted_kinds=tuple(sorted({omission.kind for omission in mapping.omissions})),
            elements_without_coordinates=len([one for one in mapping.elements if not one.regions]),
            coordinate_grade=(
                PARSER_PAGE_COORDINATES if any(one.regions for one in mapping.elements) else NO_COORDINATES
            ),
            evidence_grade=PARSER_EVIDENCE_GRADE,
            offsets=PARSED_TEXT_OFFSETS,
            status=status,
            provider_invoked=provider_invoked,
            attempt_count=1 if provider_invoked else 0,
            duration_ms=round((time.monotonic() - started) * 1_000, 3),
            failure_reason=reading.failure_reason,
            # Sanitized again at the boundary that persists it. Every path above
            # already goes through ``bounded_error_type``; this is what makes that
            # true of any path added later, since this is the one place an
            # ``error_type`` becomes part of a published record.
            error_type=None if reading.error_type is None else bounded_error_type(reading.error_type),
        )


# --- worker entry ------------------------------------------------------------
#
# ``python -m spicy_regs.docpipeline.adapters.docling JOB`` is the whole worker.
# ``source.py`` owns the containment around it — the temporary directory, the
# stripped environment, the wall clock, the process-group termination, and the
# byte caps — and this end owns exactly one thing: read the caller's job, parse
# the exact bytes it names, and write the adapter's own strict JSON record to
# the path the caller chose. No pickle, no provider type, no stdout protocol.

WORKER_STATUS_COMPLETED = "completed"
WORKER_STATUS_FAILED = "failed"
WORKER_STATUS_UNAVAILABLE = "unavailable"
"""The three answers a worker may write. The parent classifies; this end reports."""

WORKER_INVALID_REQUEST = "invalid_request"
"""Recorded when the caller's own job named bytes or a name no parse can accept."""


def _write_worker_record(path: Path, record: dict[str, Any]) -> None:
    """Write one strict-JSON record. Everything inside is a built-in scalar."""
    path.write_text(json.dumps(record, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def run_worker(
    job_path: str | Path,
    *,
    parser_factory: Callable[..., DocumentParser] | None = None,
) -> int:
    """Serve one job file and return this process's exit status.

    Zero means "a well-formed record was written", including for a parse that
    failed or an extra that is not installed — those are outcomes a receipt
    records, not crashes. A non-zero status means no record could be written at
    all, which the parent classifies on its own terms.

    ``parser_factory`` exists so this entry point can be exercised without the
    optional extra; production leaves it alone and gets the pinned parser.
    """
    try:
        job = json.loads(Path(job_path).read_text(encoding="utf-8"))
        input_path = Path(str(job["input_path"]))
        result_path = Path(str(job["result_path"]))
        source_name = job["source_name"]
        media_type = job.get("media_type")
        max_source_bytes = int(job.get("max_source_bytes") or DEFAULT_MAX_SOURCE_BYTES)
        payload = input_path.read_bytes()
    except Exception:
        # The caller's own job could not be read, so there is no result path to
        # write to and nothing honest to say. The parent has the classification.
        return 1
    try:
        build = DoclingDocumentParser if parser_factory is None else parser_factory
        parser = build(max_source_bytes=max_source_bytes)
    except (DoclingUnavailableError, DoclingConfigurationError):
        # The pinned release is absent or a setting cannot be applied: one word,
        # no message, because neither is a statement about the source bytes.
        _write_worker_record(result_path, {"status": WORKER_STATUS_UNAVAILABLE})
        return 0
    try:
        result = parser.parse(payload, source_name=source_name, media_type=media_type)
        record = {
            "status": WORKER_STATUS_COMPLETED,
            "call": result.call.as_json_dict(),
            "document": _json_safe(result.document),
        }
    except DoclingParseError as error:
        call = getattr(error, "call", None)
        record = {
            "status": WORKER_STATUS_FAILED,
            "failure_reason": None if call is None else call.failure_reason,
            "call": None if call is None else call.as_json_dict(),
        }
    except (TypeError, ValueError):
        # Empty bytes, an empty name, or a name that is not text: the request
        # itself, not the parse. Named as such, and never echoed back.
        record = {"status": WORKER_STATUS_FAILED, "failure_reason": WORKER_INVALID_REQUEST, "call": None}
    _write_worker_record(result_path, record)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run one job named on the command line. One argument, always."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1:
        return 2
    return run_worker(arguments[0])


if __name__ == "__main__":  # pragma: no cover - exercised through the process gate
    raise SystemExit(main())

"""Source adapters that map native fields into one general element contract."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Literal, Protocol

SegmentationMode = Literal[
    "atomic-record",
    "structured-children",
    "hierarchical-document",
]
ADAPTER_VERSION = "source-elements-v2"

_HEADING = re.compile(
    r"^(?:"
    r"section\b|§|title\b|part\b|subpart\b|chapter\b|"
    r"[IVXLC]+\.\s|"
    r"\d+(?:\.\d+)*(?:[.)]|\s+-)"
    r")",
    re.IGNORECASE,
)
_BODY_COLUMNS = frozenset(
    {
        "abstract",
        "comment",
        "text_content",
        "text_data",
        "description",
        "body",
        "body_html",
        "body_text",
        "body_xml",
        "content_text",
        "html_text",
        "opinion_text",
        "pdf_text",
        "summary",
        "text",
        "full_text",
        "xml_text",
    }
)
_MARKUP_BLOCKS = frozenset(
    {
        "article",
        "chapter",
        "div",
        "enum",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "legis-body",
        "li",
        "p",
        "paragraph",
        "part",
        "pre",
        "section",
        "subsection",
        "table",
        "tbody",
        "td",
        "text",
        "th",
        "title",
        "tr",
    }
)
_MARKUP_HEADINGS = frozenset(
    {
        "chapter",
        "enum",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "part",
        "title",
    }
)


@dataclass(frozen=True)
class ElementDraft:
    """Parser-neutral exact coordinates for one source element."""

    kind: str
    source_field: str
    start_char: int
    end_char: int
    parent_ordinal: int | None = None
    ancestor_path: tuple[str, ...] = ()
    evidence_eligible: bool = True


class SourceElementAdapter(Protocol):
    """Map source fields into exact, gap-free element coordinates."""

    adapter_id: str

    def elements(
        self,
        fields: dict[str, str],
    ) -> list[ElementDraft]: ...


def _paragraph_ranges(text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    start = 0
    for match in re.finditer(r"\n[ \t]*\n", text):
        end = match.end()
        if end > start:
            ranges.append((start, end))
        start = end
    if start < len(text):
        ranges.append((start, len(text)))
    return ranges or [(0, len(text))]


def _looks_like_heading(text: str) -> bool:
    candidate = text.strip()
    if not candidate or len(candidate) > 180 or "\n" in candidate:
        return False
    return bool(_HEADING.match(candidate)) or (
        len(candidate.split()) <= 12
        and candidate[-1:] not in {".", "!", "?", ";", ","}
    )


@dataclass(frozen=True)
class _MarkupEvent:
    start_char: int
    tag: str
    parent_event: int | None


class _MarkupBoundaryParser(HTMLParser):
    """Collect source-positioned native block starts without rewriting text."""

    def __init__(self, text: str) -> None:
        super().__init__(convert_charrefs=False)
        self.events: list[_MarkupEvent] = []
        self._structural_stack: list[tuple[str, int]] = []
        self._line_starts = [0]
        self._line_starts.extend(
            match.end() for match in re.finditer("\n", text)
        )

    def _index(self) -> int:
        line, offset = self.getpos()
        return self._line_starts[line - 1] + offset

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        normalized = tag.casefold()
        if normalized not in _MARKUP_BLOCKS:
            return
        event_index = len(self.events)
        self.events.append(
            _MarkupEvent(
                start_char=self._index(),
                tag=normalized,
                parent_event=(
                    self._structural_stack[-1][1]
                    if self._structural_stack
                    else None
                ),
            )
        )
        self._structural_stack.append(
            (normalized, event_index)
        )

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        normalized = tag.casefold()
        if normalized in _MARKUP_BLOCKS:
            self.events.append(
                _MarkupEvent(
                    start_char=self._index(),
                    tag=normalized,
                    parent_event=(
                        self._structural_stack[-1][1]
                        if self._structural_stack
                        else None
                    ),
                )
            )

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.casefold()
        for index in range(
            len(self._structural_stack) - 1,
            -1,
            -1,
        ):
            if self._structural_stack[index][0] == normalized:
                del self._structural_stack[index:]
                break


class _TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _visible_markup_text(value: str) -> str:
    parser = _TextCollector()
    parser.feed(value)
    parser.close()
    return " ".join("".join(parser.parts).split())


def _markup_kind(tag: str) -> str:
    if tag in _MARKUP_HEADINGS:
        return "heading"
    if tag in {"li"}:
        return "list-item"
    if tag in {"table", "tbody"}:
        return "table"
    if tag in {"tr", "td", "th"}:
        return "table-row"
    if tag in {"section", "subsection", "article", "div"}:
        return "section"
    return "paragraph"


def _markup_ranges(
    source_field: str,
    text: str,
) -> list[ElementDraft] | None:
    if not text.lstrip().startswith("<") or ">" not in text:
        return None
    parser = _MarkupBoundaryParser(text)
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return None
    events = [
        event
        for index, event in enumerate(parser.events)
        if index == 0
        or event.start_char != parser.events[index - 1].start_char
    ]
    if not events:
        return None
    prefix_count = int(events[0].start_char > 0)
    result: list[ElementDraft] = []
    if prefix_count:
        result.append(
            ElementDraft(
                kind="markup-prolog",
                source_field=source_field,
                start_char=0,
                end_char=events[0].start_char,
            )
        )
    heading_path: tuple[str, ...] = ()
    for index, event in enumerate(events):
        end = (
            events[index + 1].start_char
            if index + 1 < len(events)
            else len(text)
        )
        if end <= event.start_char:
            continue
        kind = _markup_kind(event.tag)
        value = text[event.start_char:end]
        if kind == "heading":
            heading = _visible_markup_text(value)
            if heading:
                heading_path = (heading,)
        result.append(
            ElementDraft(
                kind=kind,
                source_field=source_field,
                start_char=event.start_char,
                end_char=end,
                parent_ordinal=(
                    prefix_count + event.parent_event
                    if event.parent_event is not None
                    else None
                ),
                ancestor_path=(
                    () if kind == "heading" else heading_path
                ),
            )
        )
    return result


def _json_array_ranges(text: str) -> list[tuple[int, int]] | None:
    """Return gap-free ranges, one per top-level JSON array item."""
    decoder = json.JSONDecoder()
    position = 0
    while position < len(text) and text[position].isspace():
        position += 1
    if position >= len(text) or text[position] != "[":
        return None
    array_start = position
    position += 1
    value_ends: list[int] = []
    while True:
        while position < len(text) and (
            text[position].isspace() or text[position] == ","
        ):
            position += 1
        if position >= len(text):
            return None
        if text[position] == "]":
            break
        try:
            _, end = decoder.raw_decode(text, position)
        except json.JSONDecodeError:
            return None
        value_ends.append(end)
        position = end
    tail = position + 1
    while tail < len(text) and text[tail].isspace():
        tail += 1
    if tail != len(text):
        return None
    if not value_ends:
        return [(0, len(text))]
    ranges: list[tuple[int, int]] = []
    start = 0 if array_start == 0 else array_start
    for end in value_ends:
        ranges.append((start, end))
        start = end
    ranges[-1] = (ranges[-1][0], len(text))
    return ranges


class AtomicFieldAdapter:
    adapter_id = f"atomic-fields:{ADAPTER_VERSION}"

    def elements(self, fields: dict[str, str]) -> list[ElementDraft]:
        return [
            ElementDraft(
                kind=(
                    "heading"
                    if source_field.rsplit(".", 1)[-1]
                    in {"title", "name", "heading", "legal_business_name"}
                    else (
                        "structured-field"
                        if source_field.endswith("_json")
                        else "field"
                    )
                ),
                source_field=source_field,
                start_char=0,
                end_char=len(text),
            )
            for source_field, text in fields.items()
        ]


class StructuredChildAdapter:
    adapter_id = f"structured-children:{ADAPTER_VERSION}"

    def elements(self, fields: dict[str, str]) -> list[ElementDraft]:
        result: list[ElementDraft] = []
        for source_field, text in fields.items():
            ranges = (
                _json_array_ranges(text)
                if source_field.endswith("_json")
                else None
            )
            if ranges is None or len(ranges) == 1:
                result.append(
                    ElementDraft(
                        kind=(
                            "structured-field"
                            if source_field.endswith("_json")
                            else "field"
                        ),
                        source_field=source_field,
                        start_char=0,
                        end_char=len(text),
                    )
                )
                continue
            parent_ordinal = len(result)
            result.append(
                ElementDraft(
                    kind="structured-array",
                    source_field=source_field,
                    start_char=0,
                    end_char=len(text),
                    evidence_eligible=False,
                )
            )
            for start, end in ranges:
                result.append(
                    ElementDraft(
                        kind="structured-child",
                        source_field=source_field,
                        start_char=start,
                        end_char=end,
                        parent_ordinal=parent_ordinal,
                    )
                )
        return result


class HierarchicalTextAdapter:
    adapter_id = f"hierarchical-text:{ADAPTER_VERSION}"

    def elements(self, fields: dict[str, str]) -> list[ElementDraft]:
        result: list[ElementDraft] = []
        for source_field, text in fields.items():
            column = source_field.rsplit(".", 1)[-1]
            if column not in _BODY_COLUMNS:
                result.extend(AtomicFieldAdapter().elements({source_field: text}))
                continue
            markup = _markup_ranges(source_field, text)
            if markup is not None:
                base = len(result)
                result.extend(
                    ElementDraft(
                        kind=draft.kind,
                        source_field=draft.source_field,
                        start_char=draft.start_char,
                        end_char=draft.end_char,
                        parent_ordinal=(
                            base + draft.parent_ordinal
                            if draft.parent_ordinal is not None
                            else None
                        ),
                        ancestor_path=draft.ancestor_path,
                        evidence_eligible=draft.evidence_eligible,
                    )
                    for draft in markup
                )
                continue
            heading_ordinal: int | None = None
            heading_path: tuple[str, ...] = ()
            for start, end in _paragraph_ranges(text):
                value = text[start:end]
                kind = "heading" if _looks_like_heading(value) else "paragraph"
                ordinal = len(result)
                if kind == "heading":
                    heading_ordinal = ordinal
                    heading_path = (value.strip(),)
                result.append(
                    ElementDraft(
                        kind=kind,
                        source_field=source_field,
                        start_char=start,
                        end_char=end,
                        parent_ordinal=(
                            None if kind == "heading" else heading_ordinal
                        ),
                        ancestor_path=(
                            () if kind == "heading" else heading_path
                        ),
                    )
                )
        return result


def adapter_for(mode: SegmentationMode) -> SourceElementAdapter:
    """Resolve the declared profile mode to one parser-neutral adapter."""
    if mode == "atomic-record":
        return AtomicFieldAdapter()
    if mode == "structured-children":
        return StructuredChildAdapter()
    if mode == "hierarchical-document":
        return HierarchicalTextAdapter()
    raise ValueError(f"Unknown segmentation mode: {mode}")

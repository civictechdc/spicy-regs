"""One candidate extractor, one format, many files — measured in one process.

Runs in whichever interpreter has the candidate installed. The driver
(``tools/run_extraction_bakeoff.py``) invokes it out-of-process so a candidate's
imports never enter the project environment and so peak RSS and wall time belong
to that candidate alone.

What it measures, and why each one is the question that matters here:

* **anchor_tok / anchor_tri** — the reversibility test. A span is reversible only
  if the exact characters the extractor emitted occur verbatim in the source. A
  single token is a floor; a three-token window carries the whitespace *between*
  tokens, so it fails the moment an extractor normalizes, reflows, or decodes an
  entity. Sampling is by fixed stride, never random, so a rerun samples the same
  positions.
* **deletion** — the trafilatura lesson. Measured against a neutral reference:
  every text node outside the non-content elements, entity-decoded. Extraction
  quality is not what a tool keeps, it is what it silently drops.
* **fr_markers** — the same lesson at document scale. The Federal Register's
  ``SUMMARY:``/``DATES:``/``ADDRESSES:`` headers are what regulatory lexical
  retrieval ranks on. Losing one is not a rounding error.
* **structure** — how many structural units the candidate *itself* emits. A
  candidate that returns a flat string scores ``null``, not zero: the distinction
  between "found none" and "does not model structure" is the whole buy-vs-build
  question for the formats whose markup already states their structure.

Most candidate imports below cannot resolve in the project environment, and that
is the design rather than an oversight: an unadopted comparator must never become
a project dependency. ``tools/citation_bakeoff_citeurl_worker.py`` carries the
same unresolved-import diagnostics for the same reason. A candidate that is not
installed in the interpreter running this file reports an ``error`` row, which is
itself a measurement — that is how ``unstructured``'s broken PDF path was found.

Nothing here writes into the repository. Output is one JSON object on stdout.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import resource
import sys
import time
from collections.abc import Sequence
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

NON_CONTENT = frozenset({"iframe", "noscript", "script", "style", "svg", "template"})
VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)

#: The Federal Register preamble labels regulatory lexical retrieval ranks on.
FR_MARKERS = ("SUMMARY:", "DATES:", "ADDRESSES:", "FOR FURTHER INFORMATION CONTACT:", "SUPPLEMENTARY INFORMATION:")

#: How many probe positions per document. Fixed stride, never sampled randomly.
PROBES = 300


class _Reference(HTMLParser):
    """Every text node outside the non-content elements, entities decoded.

    Deliberately not the incumbent's extractor: the reference has to be neutral
    or the deletion numbers only say "differs from what we already do".
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._stack: list[tuple[str, bool]] = []
        self._depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        name = tag.casefold()
        if name in VOID:
            return
        suppress = name in NON_CONTENT
        self._stack.append((name, suppress))
        self._depth += int(suppress)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index][0] == name:
                self._depth -= sum(int(s) for _, s in self._stack[index:])
                del self._stack[index:]
                return

    def handle_data(self, data: str) -> None:
        if self._depth == 0:
            self.parts.append(data)


def reference_text(markup: str) -> str:
    parser = _Reference()
    try:
        parser.feed(markup)
        parser.close()
    except (AssertionError, ValueError):
        return ""
    return "".join(parser.parts)


def _probe_positions(count: int, wanted: int) -> range:
    if count <= 0:
        return range(0)
    return range(0, count, max(1, count // wanted))


def _windows(unit: str) -> tuple[list[str], list[str]]:
    """Single tokens, and three-token windows carved with their own whitespace."""
    tokens = unit.split()
    if not tokens:
        return [], []
    starts: list[int] = []
    cursor = 0
    for token in tokens:
        found = unit.find(token, cursor)
        if found < 0:
            break
        starts.append(found)
        cursor = found + len(token)
    triples = [
        unit[starts[index] : starts[index + 2] + len(tokens[index + 2])] for index in range(max(0, len(starts) - 2))
    ]
    return tokens, triples


def anchoring(units: Sequence[str], source: str) -> dict[str, Any]:
    """Fraction of emitted text that occurs verbatim in the raw source.

    Probes are taken *within* a unit and never across a unit join, so the number
    measures the candidate, not this harness's choice of separator.
    ``anchor_tok`` probes single whitespace tokens — a floor, since a short common
    word occurs somewhere in almost any document. ``anchor_tri`` probes three-token
    windows carrying the whitespace *between* the tokens, which is the number that
    decides whether a quoted passage can be resolved back to source bytes.
    """
    tokens: list[str] = []
    triples: list[str] = []
    for unit in units:
        unit_tokens, unit_triples = _windows(unit)
        tokens.extend(unit_tokens)
        triples.extend(unit_triples)
    if not tokens:
        return {"anchor_tok": None, "anchor_tri": None, "unit_exact": None, "probes_tok": 0, "probes_tri": 0}
    token_positions = list(_probe_positions(len(tokens), PROBES))
    triple_positions = list(_probe_positions(len(triples), PROBES))
    unit_positions = list(_probe_positions(len(units), PROBES))
    return {
        "anchor_tok": round(sum(tokens[i] in source for i in token_positions) / len(token_positions), 6),
        "anchor_tri": (
            round(sum(triples[i] in source for i in triple_positions) / len(triple_positions), 6)
            if triple_positions
            else None
        ),
        # The strongest form of the same question: is the whole emitted unit a
        # verbatim slice of the source? Only a candidate that returns source
        # slices can score 1.0 here.
        "unit_exact": (
            round(sum(bool(units[i].strip()) and units[i] in source for i in unit_positions) / len(unit_positions), 6)
            if unit_positions
            else None
        ),
        "probes_tok": len(token_positions),
        "probes_tri": len(triple_positions),
    }


def loss(extracted: str, reference: str) -> dict[str, Any]:
    """Reference tokens the candidate did not emit, and tokens it invented."""
    want = collections.Counter(reference.split())
    got = collections.Counter(extracted.split())
    total = sum(want.values())
    if total == 0:
        return {"deletion": None, "insertion": None, "ref_tokens": 0, "out_tokens": sum(got.values())}
    missing = sum((want - got).values())
    extra = sum((got - want).values())
    out_total = sum(got.values())
    return {
        "deletion": round(missing / total, 6),
        "insertion": round(extra / out_total, 6) if out_total else None,
        "ref_tokens": total,
        "out_tokens": out_total,
    }


#: What a candidate returns: the separate strings it emits as objects, or one
#: flat blob for the candidates that model no objects at all.
Units = str | list[str]


# --- candidates -------------------------------------------------------------
#
# Each returns (units, structure_count_or_None), where units is a list of the
# strings the candidate emits as separate objects — or a single string for the
# candidates that emit one flat blob. ``None`` structure means the candidate
# models no structure at all, which is a different answer from "found zero".


def _c_incumbent(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from spicy_regs.docpipeline.source import native_structural_passage_spans

    text = data.decode("utf-8", errors="replace")
    spans = native_structural_passage_spans("body", text, media_type=media)
    return [text[start:end] for start, end in spans], len(spans)


def _c_incumbent_visible(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    """The incumbent's spans reduced to visible text — what a reader would see."""
    from spicy_regs.docpipeline.source import _indexable_markup_text, native_structural_passage_spans

    text = data.decode("utf-8", errors="replace")
    spans = native_structural_passage_spans("body", text, media_type=media)
    return [_indexable_markup_text(text[start:end]) for start, end in spans], len(spans)


def _c_lxml(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import lxml.etree as etree
    import lxml.html

    if media == "text/html":
        tree = lxml.html.fromstring(data)
        return tree.text_content(), None
    parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=True)
    root = etree.fromstring(data, parser=parser)
    return "".join(root.itertext()), None


def _c_lxml_structural(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    """lxml plus a hand-written structural walk — the 'build it' arm."""
    import lxml.etree as etree
    import lxml.html

    if media == "text/html":
        tree = lxml.html.fromstring(data)
        nodes = tree.xpath("//h1|//h2|//h3|//h4|//h5|//h6|//p|//li|//td|//th")
    else:
        parser = etree.XMLParser(resolve_entities=False, no_network=True, recover=True)
        root = etree.fromstring(data, parser=parser)
        nodes = [element for element in root.iter() if isinstance(element.tag, str)]
    return [node.text_content() if hasattr(node, "text_content") else "".join(node.itertext()) for node in nodes], len(
        nodes
    )


def _c_bs4(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(data, "html.parser" if media == "text/html" else "xml")
    return soup.get_text(), None


def _c_selectolax(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from selectolax.lexbor import LexborHTMLParser

    return LexborHTMLParser(data.decode("utf-8", errors="replace")).text(), None


def _c_html_text(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import html_text

    return html_text.extract_text(data.decode("utf-8", errors="replace")), None


def _c_html_text_raw(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import html_text

    return html_text.extract_text(data.decode("utf-8", errors="replace"), guess_layout=False), None


def _c_inscriptis(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from inscriptis import get_text

    return get_text(data.decode("utf-8", errors="replace")), None


def _c_resiliparse(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from resiliparse.extract.html2text import extract_plain_text

    return extract_plain_text(data.decode("utf-8", errors="replace")), None


def _c_resiliparse_main(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from resiliparse.extract.html2text import extract_plain_text

    return extract_plain_text(data.decode("utf-8", errors="replace"), main_content=True), None


def _c_unstructured(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from unstructured.partition.html import partition_html

    elements = partition_html(text=data.decode("utf-8", errors="replace"))
    titles = sum(1 for element in elements if element.category in {"Title", "Header"})
    return [str(element) for element in elements], titles


def _c_unstructured_xml(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from unstructured.partition.xml import partition_xml

    elements = partition_xml(text=data.decode("utf-8", errors="replace"))
    titles = sum(1 for element in elements if element.category in {"Title", "Header"})
    return [str(element) for element in elements], titles


def _c_docling(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    from docling.datamodel.base_models import DocumentStream
    from docling.document_converter import DocumentConverter

    stream = DocumentStream(name=path.name, stream=__import__("io").BytesIO(data))
    result = DocumentConverter().convert(stream)
    document = result.document
    headings = sum(
        1 for item, _ in document.iterate_items() if getattr(item, "label", "") in {"section_header", "title"}
    )
    return document.export_to_markdown(), headings


def _c_pypdf(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import io

    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    return [page.extract_text() or "" for page in reader.pages], len(reader.pages)


def _c_pdfplumber(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(data)) as pdf:
        return [page.extract_text() or "" for page in pdf.pages], len(pdf.pages)


def _c_pymupdf(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import fitz

    document = fitz.open(stream=data, filetype="pdf")
    units = [page.get_text() for page in document]
    pages = document.page_count
    document.close()
    return units, pages


def _c_pymupdf4llm(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import fitz
    import pymupdf4llm

    document = fitz.open(stream=data, filetype="pdf")
    text = pymupdf4llm.to_markdown(document, show_progress=False)
    pages = document.page_count
    document.close()
    return [text], pages


def _c_unstructured_pdf(data: bytes, path: Path, media: str) -> tuple[Units, int | None]:
    import io

    from unstructured.partition.pdf import partition_pdf

    elements = partition_pdf(file=io.BytesIO(data), strategy="fast")
    titles = sum(1 for element in elements if element.category in {"Title", "Header"})
    return [str(element) for element in elements], titles


CANDIDATES = {
    "incumbent": _c_incumbent,
    "incumbent_visible": _c_incumbent_visible,
    "lxml": _c_lxml,
    "lxml_structural": _c_lxml_structural,
    "bs4": _c_bs4,
    "selectolax": _c_selectolax,
    "html_text": _c_html_text,
    "html_text_raw": _c_html_text_raw,
    "inscriptis": _c_inscriptis,
    "resiliparse": _c_resiliparse,
    "resiliparse_main": _c_resiliparse_main,
    "unstructured": _c_unstructured,
    "unstructured_xml": _c_unstructured_xml,
    "docling": _c_docling,
    "pypdf": _c_pypdf,
    "pdfplumber": _c_pdfplumber,
    "pymupdf": _c_pymupdf,
    "pymupdf4llm": _c_pymupdf4llm,
    "unstructured_pdf": _c_unstructured_pdf,
}


def _versions(names: list[str]) -> dict[str, str]:
    import importlib.metadata as metadata

    wanted = {
        "lxml": "lxml",
        "bs4": "beautifulsoup4",
        "selectolax": "selectolax",
        "html_text": "html-text",
        "html_text_raw": "html-text",
        "inscriptis": "inscriptis",
        "resiliparse": "resiliparse",
        "resiliparse_main": "resiliparse",
        "unstructured": "unstructured",
        "unstructured_xml": "unstructured",
        "unstructured_pdf": "unstructured",
        "docling": "docling",
        "pypdf": "pypdf",
        "pdfplumber": "pdfplumber",
        "pymupdf": "pymupdf",
        "pymupdf4llm": "pymupdf4llm",
    }
    found: dict[str, str] = {}
    for name in names:
        dist = wanted.get(name)
        if not dist:
            continue
        try:
            found[dist] = metadata.version(dist)
        except Exception:  # noqa: BLE001 - a missing dist is a reportable fact
            found[dist] = "not-installed"
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True, choices=sorted(CANDIDATES))
    parser.add_argument("--media", required=True, choices=["text/html", "application/xml", "application/pdf"])
    parser.add_argument("--files", required=True, help="newline-delimited file of input paths")
    parser.add_argument("--full-metrics", action="store_true", help="compute anchoring and loss (text formats)")
    arguments = parser.parse_args()

    run = CANDIDATES[arguments.candidate]
    paths = [Path(line) for line in Path(arguments.files).read_text().splitlines() if line.strip()]
    rows: list[dict[str, Any]] = []
    started = time.perf_counter()
    for path in paths:
        data = path.read_bytes()
        row: dict[str, Any] = {"file": path.name, "source_bytes": len(data)}
        began = time.perf_counter()
        try:
            produced, structure = run(data, path, arguments.media)
        except Exception as error:  # noqa: BLE001 - a candidate's failure is the measurement
            row["error"] = f"{type(error).__name__}: {str(error)[:160]}"
            rows.append(row)
            continue
        row["elapsed_s"] = round(time.perf_counter() - began, 4)
        units = [produced] if isinstance(produced, str) else list(produced)
        text = "\n".join(units)
        row["chars"] = len(text)
        row["units"] = len(units)
        row["sha256"] = hashlib.sha256(text.encode()).hexdigest()
        row["structure"] = structure
        if arguments.full_metrics:
            source = data.decode("utf-8", errors="replace")
            reference = reference_text(source) if arguments.media == "text/html" else _xml_reference(source)
            row.update(anchoring(units, source))
            row.update(loss(text, reference))
            row["fr_markers"] = sum(1 for marker in FR_MARKERS if marker in re.sub(r"\s+", " ", text))
            row["fr_markers_ref"] = sum(1 for marker in FR_MARKERS if marker in re.sub(r"\s+", " ", reference))
        rows.append(row)
    payload = {
        "candidate": arguments.candidate,
        "media": arguments.media,
        "files": len(paths),
        "total_s": round(time.perf_counter() - started, 3),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024), 1),
        "python": sys.version.split()[0],
        "versions": _versions([arguments.candidate]),
        "rows": rows,
    }
    json.dump(payload, sys.stdout)
    return 0


def _xml_reference(markup: str) -> str:
    """Every character node in an XML document, entity references resolved.

    Uses the same decoding stdlib path as the HTML reference so the two
    references answer the same question about the same kind of loss.
    """
    return reference_text(markup)


if __name__ == "__main__":
    raise SystemExit(main())

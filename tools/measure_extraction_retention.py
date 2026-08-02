"""Measure what fraction of a source's visible text survives extraction.

This exists to derive the coverage floor in
``spicy_regs.docpipeline.source`` from data rather than from taste. It reports
the retention distribution over every real corpus this project can reach, so the
floor can be placed where legitimate documents are clearly above it and the
measured failures are clearly below it, with the margin stated.

Retention is defined per *coordinate system*, because the formats do not share
one:

* ``markup-visible`` — HTML and XML. The denominator is the visible text of the
  whole source field, computed independently of the extractor by walking text
  nodes outside the non-content elements and decoding entity references. The
  numerator is the visible text of the passages the extractor returned. Both
  sides are characters of text a reader would see, so the ratio means "how much
  of the document survived".
* ``parsed-per-source-byte`` — PDF and Office. There is no source text to
  compare against; the parser *is* the only reader. The denominator is source
  bytes and the numerator is extracted characters, so the ratio is a text
  density, not a fraction, and it cannot be compared against the markup numbers.

Reported per corpus: count, min, the low percentiles that set the margin, median,
and mean. The floor goes below the observed minimum of legitimate documents by a
stated margin; a floor with no margin is a future false refusal.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from spicy_regs.docpipeline.source import (
    reference_visible_text,
    visible_retention,
)

MARKUP_UNIT = "markup-visible"
DENSITY_UNIT = "parsed-per-source-byte"


@dataclass(frozen=True)
class Corpus:
    """One named population of real files, measured under one unit."""

    label: str
    media_type: str
    unit: str
    paths: tuple[Path, ...]


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return ordered[index]


def _markup_retention(path: Path, media_type: str) -> float | None:
    text = path.read_bytes().decode("utf-8", errors="replace")
    reference = reference_visible_text(text)
    if not reference:
        return None
    return visible_retention("body", text, media_type=media_type)


def _pdf_density(path: Path) -> float | None:
    from pypdf import PdfReader

    data = path.read_bytes()
    reader = PdfReader(path)
    extracted = "\n".join(page.extract_text() or "" for page in reader.pages)
    if not data:
        return None
    return len(extracted) / len(data)


def measure(corpus: Corpus) -> dict[str, object]:
    values: list[float] = []
    skipped = 0
    for path in corpus.paths:
        try:
            value = _markup_retention(path, corpus.media_type) if corpus.unit == MARKUP_UNIT else _pdf_density(path)
        except Exception as error:  # noqa: BLE001 - a file this cannot read is a reportable fact
            print(f"  skip {path.name}: {type(error).__name__}: {error}", file=sys.stderr)
            skipped += 1
            continue
        if value is None:
            skipped += 1
            continue
        values.append(value)
    if not values:
        return {"label": corpus.label, "unit": corpus.unit, "files": 0, "skipped": skipped}
    return {
        "label": corpus.label,
        "unit": corpus.unit,
        "files": len(values),
        "skipped": skipped,
        "min": round(min(values), 6),
        "p01": round(_percentile(values, 0.01), 6),
        "p05": round(_percentile(values, 0.05), 6),
        "p50": round(_percentile(values, 0.50), 6),
        "mean": round(statistics.fmean(values), 6),
        "max": round(max(values), 6),
    }


def _corpora(root: Path) -> Iterator[Corpus]:
    body = root / "output/body-retrieval-corpus-2026-08-02"
    cache = root / "output/segmentation-source-cache-v2"
    uslm = Path("/tmp/xbake/uslm")

    def files(directory: Path, pattern: str) -> tuple[Path, ...]:
        return tuple(sorted(directory.glob(pattern))) if directory.is_dir() else ()

    yield Corpus("federal-register-html", "text/html", MARKUP_UNIT, files(body / "cache/documents", "*.html"))
    yield Corpus("federal-register-xml", "application/xml", MARKUP_UNIT, files(body / "cache-xml/documents", "*.xml"))
    yield Corpus("uslm-xml", "application/xml", MARKUP_UNIT, files(uslm, "*.xml"))
    yield Corpus("cfr-xml", "application/xml", MARKUP_UNIT, files(cache, "cfr-*.xml"))
    yield Corpus("bill-xml", "application/xml", MARKUP_UNIT, files(cache, "bill-*.xml"))
    yield Corpus("gao-html", "text/html", MARKUP_UNIT, files(cache, "gao-*.html"))
    yield Corpus("segmentation-fr-html", "text/html", MARKUP_UNIT, files(cache, "federal-register-*.html"))
    yield Corpus("pdf-pypdf", "application/pdf", DENSITY_UNIT, files(cache, "*.pdf"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument("--output", help="write the distribution here as JSON")
    arguments = parser.parse_args()

    rows = []
    for corpus in _corpora(Path(arguments.root)):
        if not corpus.paths:
            print(f"  (no files) {corpus.label}", file=sys.stderr)
            continue
        row = measure(corpus)
        rows.append(row)
        print(json.dumps(row), file=sys.stderr, flush=True)

    report = {"schema_version": "extraction-retention-v1", "corpora": rows}
    rendered = json.dumps(report, indent=1, sort_keys=True)
    if arguments.output:
        Path(arguments.output).write_text(rendered)
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

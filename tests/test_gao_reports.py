"""Hermetic tests for the GAO reports RSS ingest (no network).

Covers the pieces with real logic: the raw-item → published-schema mapping
(``_shape``), the report-id extraction from a product URL, the RFC-822 date
parse, the DOCTYPE / entity-expansion guard, and the feed parsing itself over a
fixed RSS fixture.
"""

from __future__ import annotations

from spicy_regs.sources.gao_reports import GaoReportsReader, _has_doctype, _item_to_dict
from spicy_regs.transforms.build_gao_reports import (
    COLUMNS,
    _published_date,
    _report_id,
    _shape,
)

_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Reports News from the GAO</title>
    <item>
      <title>Navy Ship Modernization</title>
      <link>https://www.gao.gov/products/gao-26-107974</link>
      <description>What GAO Found. The Navy is behind schedule.</description>
      <pubDate>Fri, 17 Jul 2026 07:10:42 -0400</pubDate>
    </item>
    <item>
      <title>Second Product</title>
      <link>https://www.gao.gov/products/gao-26-108520/</link>
      <description>Body two.</description>
      <pubDate>Thu, 16 Jul 2026 09:00:00 -0400</pubDate>
    </item>
  </channel>
</rss>
"""

_RAW_ITEM = {
    "title": "Navy Ship Modernization",
    "link": "https://www.gao.gov/products/gao-26-107974",
    "description": "What GAO Found. The Navy is behind schedule.",
    "pub_date": "Fri, 17 Jul 2026 07:10:42 -0400",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ITEM)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_fields():
    row = _shape(_RAW_ITEM)
    assert row["report_id"] == "gao-26-107974"
    assert row["title"] == "Navy Ship Modernization"
    assert row["report_type"] == "Report"
    assert row["published_date"] == "2026-07-17"
    assert row["abstract"].startswith("What GAO Found")
    # Reserved columns default to empty JSON arrays.
    assert row["agencies_json"] == "[]"
    assert row["topics_json"] == "[]"
    assert row["url"] == "https://www.gao.gov/products/gao-26-107974"


def test_report_id_extraction():
    assert _report_id("https://www.gao.gov/products/gao-26-107974") == "gao-26-107974"
    # Trailing slash and mixed case are normalized.
    assert _report_id("https://www.gao.gov/products/GAO-26-108520/") == "gao-26-108520"
    assert _report_id(None) is None
    assert _report_id("") is None


def test_published_date_parses_rfc822():
    assert _published_date("Fri, 17 Jul 2026 07:10:42 -0400") == "2026-07-17"
    # Unparseable / missing values degrade to None, not an exception.
    assert _published_date("not a date") is None
    assert _published_date(None) is None


# -- feed parsing ------------------------------------------------------------


def test_reader_parses_items(monkeypatch):
    reader = GaoReportsReader()
    monkeypatch.setattr(reader, "_fetch", lambda: _FEED)
    rows = list(reader.iter_records())
    assert [r["title"] for r in rows] == ["Navy Ship Modernization", "Second Product"]
    assert rows[0]["link"] == "https://www.gao.gov/products/gao-26-107974"


def test_reader_respects_max_records(monkeypatch):
    reader = GaoReportsReader(max_records=1)
    monkeypatch.setattr(reader, "_fetch", lambda: _FEED)
    rows = list(reader.iter_records())
    assert len(rows) == 1


def test_reader_yields_nothing_on_fetch_failure(monkeypatch):
    reader = GaoReportsReader()
    monkeypatch.setattr(reader, "_fetch", lambda: None)
    assert list(reader.iter_records()) == []


def test_reader_refuses_doctype(monkeypatch):
    """A feed with a DOCTYPE (entity-expansion vector) is refused, not parsed."""
    malicious = b'<?xml version="1.0"?>\n<!DOCTYPE rss [<!ENTITY x "boom">]>\n<rss><channel></channel></rss>'
    reader = GaoReportsReader()
    monkeypatch.setattr(reader, "_fetch", lambda: malicious)
    assert list(reader.iter_records()) == []


def test_has_doctype_guard():
    assert _has_doctype(b'<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY x "y">]><rss></rss>')
    assert not _has_doctype(_FEED)
    # A DOCTYPE only counts in the prolog, before the root element.
    assert not _has_doctype(b"<rss><item><description>mentions doctype</description></item></rss>")


def test_item_to_dict_missing_children():
    import xml.etree.ElementTree as ET

    item = ET.fromstring("<item><title>Only a title</title></item>")
    row = _item_to_dict(item)
    assert row["title"] == "Only a title"
    assert row["link"] is None
    assert row["description"] is None
    assert row["pub_date"] is None

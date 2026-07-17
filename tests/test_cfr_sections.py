"""Hermetic tests for the GovInfo CFR section ingest (no network).

Covers the pieces with real logic: the api.data.gov key resolution fallback
chain, the reader's keyless no-op behavior + default edition window, and the
raw-granule → published-schema mapping (``_shape``), which derives CFR
title/part/section purely from the granule ID grammar + list-level fields (no
per-granule ``/summary`` call). No live network calls are made.
"""

from __future__ import annotations

import datetime as dt

from spicy_regs.sources.cfr_sections import API_KEY_ENV_VARS, CfrSectionsReader, _resolve_api_key
from spicy_regs.transforms.build_cfr_sections import COLUMNS, _cfr_ref, _shape

# A CONTENT/section granule with real list-level fields + package stamps, using
# the real GovInfo ID grammar. Note: no cfrTitle/cfrPart/cfrSection fields exist
# on list-level granules — everything is parsed from the IDs.
_SECTION_GRANULE = {
    "granuleId": "CFR-2024-title40-vol1-sec1-1",
    "granuleClass": "CONTENT",
    "title": "Definitions.",
    "dateIssued": "2024-07-01",
    "granuleLink": "https://api.govinfo.gov/packages/CFR-2024-title40-vol1/granules/CFR-2024-title40-vol1-sec1-1/summary",
    "_package_id": "CFR-2024-title40-vol1",
    "_package_last_modified": "2026-07-16T20:57:39Z",
    "_package_title": "Protection of Environment",
}

# A part-level CONTENT granule (part token present, no section).
_PART_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-part700",
    "granuleClass": "CONTENT",
    "title": "Reserved",
    "_package_id": "CFR-2024-title48-vol5",
    "_package_last_modified": "2026-01-02T00:00:00Z",
}

# A chapter appendix granule (part token present via app parent, section absent).
_APPENDIX_GRANULE = {
    "granuleId": "CFR-2024-title48-vol1-part3-app1",
    "granuleClass": "APPENDIX",
    "title": "Cost Accounting Standards.",
    "_package_id": "CFR-2024-title48-vol1",
}

# A NODE granule (chapter) — no part, no section.
_NODE_GRANULE = {
    "granuleId": "CFR-2024-title48-vol5-chap7",
    "granuleClass": "NODE",
    "title": "AGENCY FOR INTERNATIONAL DEVELOPMENT",
    "_package_id": "CFR-2024-title48-vol5",
}


# -- API key resolution -------------------------------------------------------


def _clear_key_env(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_resolve_api_key_none_when_unset(monkeypatch):
    _clear_key_env(monkeypatch)
    assert _resolve_api_key() is None


def test_resolve_api_key_prefers_data_gov(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "primary")
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "primary"


def test_resolve_api_key_fallback_chain(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "tertiary")
    assert _resolve_api_key() == "tertiary"
    monkeypatch.setenv("GOVINFO_API_KEY", "secondary")
    assert _resolve_api_key() == "secondary"


def test_resolve_api_key_ignores_blank(monkeypatch):
    _clear_key_env(monkeypatch)
    monkeypatch.setenv("DATA_GOV_API_KEY", "   ")
    monkeypatch.setenv("GOVINFO_API_KEY", "real")
    assert _resolve_api_key() == "real"


def test_reader_keyless_is_noop(monkeypatch):
    """With no key resolvable, the reader yields nothing and never hits the net."""
    _clear_key_env(monkeypatch)
    reader = CfrSectionsReader()
    assert list(reader.iter_records()) == []


def test_reader_default_edition_window():
    """Default window is last-year..this-year (CFR editions are annual)."""
    reader = CfrSectionsReader(api_key="x")
    this_year = dt.date.today().year
    assert reader.until_year == this_year
    assert reader.since_year == this_year - 1


# -- raw granule -> published schema mapping ----------------------------------


def test_shape_produces_exact_schema():
    row = _shape(_SECTION_GRANULE)
    assert set(row) == set(COLUMNS)


def test_shape_parses_section_granule():
    row = _shape(_SECTION_GRANULE)
    assert row["granule_id"] == "CFR-2024-title40-vol1-sec1-1"
    assert row["package_id"] == "CFR-2024-title40-vol1"
    # CFR title number parsed from the id, not the (heading) ``title`` field.
    assert row["title"] == "40"
    assert row["edition_year"] == "2024"
    # This title expresses sections as CONTENT granules with no ``part`` token.
    assert row["part"] is None
    assert row["section"] == "1-1"
    assert row["heading"] == "Definitions."
    assert row["structure_level"] == "CONTENT"
    # last_modified comes from the enclosing package stamp.
    assert row["last_modified"] == "2026-07-16T20:57:39Z"
    # Canonical govinfo details URL built from package + granule ids.
    assert row["url"] == "https://www.govinfo.gov/app/details/CFR-2024-title40-vol1/CFR-2024-title40-vol1-sec1-1"


def test_shape_parses_part_granule():
    row = _shape(_PART_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "700"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-700"
    assert row["structure_level"] == "CONTENT"
    assert row["edition_year"] == "2024"


def test_shape_parses_appendix_granule():
    row = _shape(_APPENDIX_GRANULE)
    assert row["title"] == "48"
    assert row["part"] == "3"
    assert row["section"] is None
    assert row["cfr_ref"] == "48-3"
    assert row["structure_level"] == "APPENDIX"


def test_shape_node_granule_has_no_part_or_section():
    row = _shape(_NODE_GRANULE)
    assert row["title"] == "48"
    assert row["part"] is None
    assert row["section"] is None
    # No part -> no cfr_ref.
    assert row["cfr_ref"] is None
    assert row["structure_level"] == "NODE"


def test_shape_edition_year_falls_back_to_date_issued():
    row = _shape({"granuleId": "weird-id", "dateIssued": "2019-03-04"})
    assert row["edition_year"] == "2019"


def test_shape_cfr_ref_full_citation():
    # A section granule that also carries a part token yields a full title-part.section ref.
    row = _shape(
        {
            "granuleId": "CFR-2024-title21-vol1-part1-sec5",
            "granuleClass": "SECTION",
            "_package_id": "CFR-2024-title21-vol1",
        }
    )
    assert row["title"] == "21"
    assert row["part"] == "1"
    assert row["section"] == "5"
    assert row["cfr_ref"] == "21-1.5"


def test_cfr_ref_degrades_gracefully():
    # Full citation, part-only, then null when title or part is missing.
    assert _cfr_ref("40", "60", "1") == "40-60.1"
    assert _cfr_ref("40", "60", None) == "40-60"
    assert _cfr_ref("40", None, None) is None
    assert _cfr_ref(None, None, None) is None


def test_shape_handles_missing_fields():
    row = _shape({"granuleId": "x"})
    assert row["granule_id"] == "x"
    assert row["package_id"] is None
    assert row["cfr_ref"] is None
    assert row["title"] is None
    assert row["part"] is None
    assert row["section"] is None
    assert row["heading"] is None
    # No package id -> no canonical url.
    assert row["url"] is None

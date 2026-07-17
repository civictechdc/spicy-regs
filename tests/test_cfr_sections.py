"""Hermetic tests for the GovInfo CFR section ingest (no network).

Covers the two pieces with real logic: the api.data.gov key resolution fallback
chain, the reader's keyless no-op behavior, and the raw-granule → published-schema
mapping (``_shape``). No live network calls are made.
"""

from __future__ import annotations

from spicy_regs.sources.cfr_sections import API_KEY_ENV_VARS, CfrSectionsReader, _resolve_api_key
from spicy_regs.transforms.build_cfr_sections import COLUMNS, _cfr_ref, _shape

_RAW_GRANULE = {
    "granuleId": "CFR-2024-title40-vol9-sec60-1",
    "_package_id": "CFR-2024-title40-vol9",
    "cfrTitle": 40,
    "cfrPart": 60,
    "cfrSection": "1",
    "heading": "Applicability and designation of affected facility.",
    "granuleClass": "SECTION",
    "editionYear": 2024,
    "lastModified": "2024-01-05T12:00:00Z",
    "detailsLink": "https://api.govinfo.gov/packages/CFR-2024-title40-vol9/granules/CFR-2024-title40-vol9-sec60-1/summary",
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


# -- raw granule -> published schema mapping ----------------------------------


def test_shape_produces_exact_schema():
    row = _shape(_RAW_GRANULE)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_stringifies_fields():
    row = _shape(_RAW_GRANULE)
    assert row["granule_id"] == "CFR-2024-title40-vol9-sec60-1"
    assert row["package_id"] == "CFR-2024-title40-vol9"
    # Numeric title/part stringify (schema is all-VARCHAR).
    assert row["title"] == "40"
    assert row["part"] == "60"
    assert row["section"] == "1"
    assert row["heading"] == "Applicability and designation of affected facility."
    assert row["structure_level"] == "SECTION"
    assert row["edition_year"] == "2024"
    assert row["last_modified"] == "2024-01-05T12:00:00Z"
    assert row["url"].endswith("/summary")


def test_shape_composes_cfr_ref():
    row = _shape(_RAW_GRANULE)
    assert row["cfr_ref"] == "40-60.1"


def test_cfr_ref_degrades_gracefully():
    # Full citation, part-only, title-only, and empty.
    assert _cfr_ref(40, 60, "1") == "40-60.1"
    assert _cfr_ref(40, 60, None) == "40-60"
    assert _cfr_ref(40, None, None) == "40"
    assert _cfr_ref(None, None, None) is None


def test_shape_handles_missing_fields():
    row = _shape({"granuleId": "x"})
    assert row["granule_id"] == "x"
    assert row["cfr_ref"] is None
    assert row["title"] is None
    assert row["heading"] is None
    assert row["url"] is None

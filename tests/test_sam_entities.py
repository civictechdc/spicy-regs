"""Hermetic tests for the SAM.gov entity ingest (no network).

Covers the pieces with real logic: the raw-entity -> published-schema mapping
(``_shape``), the API-key resolution fallback chain, the keyless no-op, and the
page walk with early stop at ``max_records``. The fixture below is a trimmed but
faithful copy of a real ``entityData[]`` record from the v4 ``/entities`` list
response.
"""

from __future__ import annotations

from spicy_regs.sources.sam_entities import (
    API_KEY_ENV_VARS,
    PER_PAGE,
    SamEntitiesReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_sam_entities import COLUMNS, _shape

_RAW_ENTITY = {
    "entityRegistration": {
        "samRegistered": "Yes",
        "ueiSAM": "TXBDHEGXWKD6",
        "cageCode": "9ABC1",
        "legalBusinessName": "Imperial Law Associates Pvt. Ltd.",
        "dbaName": "Imperial Law",
        "purposeOfRegistrationDesc": "Federal Assistance Awards",
        "registrationStatus": "Active",
        "registrationDate": "2026-07-17",
        "registrationExpirationDate": "2027-07-17",
        "exclusionStatusFlag": "N",
    },
    "coreData": {
        "entityInformation": {"entityURL": "www.lawimperial.com"},
        "physicalAddress": {
            "city": "Kathmandu",
            "stateOrProvinceCode": "NY",
            "zipCode": "44600",
            "countryCode": "NPL",
        },
        "congressionalDistrict": "12",
        "generalInformation": {
            "entityStructureDesc": "Corporate Entity (Not Tax Exempt)",
            "entityTypeDesc": "Business or Organization",
            "profitStructureDesc": "For Profit Organization",
        },
    },
    "assertions": {"goodsAndServices": {"primaryNaics": "541110"}},
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ENTITY)
    # Every published column present, and nothing extra (18-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 18


def test_shape_maps_nested_fields():
    row = _shape(_RAW_ENTITY)
    assert row["uei"] == "TXBDHEGXWKD6"
    assert row["cage_code"] == "9ABC1"
    assert row["legal_business_name"] == "Imperial Law Associates Pvt. Ltd."
    assert row["dba_name"] == "Imperial Law"
    # coreData.generalInformation.*
    assert row["entity_structure_desc"] == "Corporate Entity (Not Tax Exempt)"
    assert row["entity_type_desc"] == "Business or Organization"
    assert row["profit_structure_desc"] == "For Profit Organization"
    # coreData.physicalAddress.*
    assert row["state"] == "NY"
    assert row["city"] == "Kathmandu"
    assert row["zip_code"] == "44600"
    # scalars coerced to str
    assert row["congressional_district"] == "12"
    assert row["primary_naics"] == "541110"
    # entityRegistration.*
    assert row["registration_status"] == "Active"
    assert row["registration_date"] == "2026-07-17"
    assert row["registration_expiration_date"] == "2027-07-17"
    assert row["exclusion_status_flag"] == "N"
    assert row["purpose_of_registration_desc"] == "Federal Assistance Awards"
    # coreData.entityInformation.entityURL
    assert row["entity_url"] == "www.lawimperial.com"


def test_shape_handles_missing_nested_objects():
    # A registration with no coreData / assertions still yields a UEI-keyed row.
    row = _shape({"entityRegistration": {"ueiSAM": "ABC123456789"}})
    assert row["uei"] == "ABC123456789"
    assert row["state"] is None
    assert row["primary_naics"] is None
    assert row["entity_url"] is None
    assert row["entity_structure_desc"] is None
    # Fully empty payload degrades to an all-null row, not a KeyError.
    empty = _shape({})
    assert empty["uei"] is None
    assert set(empty) == set(COLUMNS)


def test_shape_coerces_int_scalars_to_str():
    row = _shape(
        {"assertions": {"goodsAndServices": {"primaryNaics": 541110}}, "coreData": {"congressionalDistrict": 3}}
    )
    assert row["primary_naics"] == "541110"
    assert row["congressional_district"] == "3"


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("SAM_API_KEY", "sam-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_reader_yields_nothing_without_key(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = SamEntitiesReader()
    # No key configured: a keyless run is a no-op, not a crash.
    assert list(reader.iter_records()) == []


def test_reader_caps_page_size():
    reader = SamEntitiesReader(per_page=500)
    assert reader.per_page == PER_PAGE


# -- pagination --------------------------------------------------------------


def _entity(uei: str) -> dict:
    return {"entityRegistration": {"ueiSAM": uei}}


def test_paginate_walks_pages_until_short_page(monkeypatch):
    """Full pages advance the page counter; a short page ends pagination."""
    reader = SamEntitiesReader(api_key="test", per_page=2)
    pages = {
        0: {"entityData": [_entity("A"), _entity("B")]},
        1: {"entityData": [_entity("C")]},  # short page -> stop
    }
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page, {"entityData": []}))
    got = [r["entityRegistration"]["ueiSAM"] for r in reader._paginate()]
    assert got == ["A", "B", "C"]


def test_paginate_stops_at_max_records(monkeypatch):
    """``max_records`` bounds the walk mid-page (no full backfill)."""
    reader = SamEntitiesReader(api_key="test", per_page=3, max_records=2)
    pages = {0: {"entityData": [_entity("A"), _entity("B"), _entity("C")]}}
    monkeypatch.setattr(reader, "_get_page", lambda page: pages.get(page, {"entityData": []}))
    got = [r["entityRegistration"]["ueiSAM"] for r in reader._paginate()]
    assert got == ["A", "B"]

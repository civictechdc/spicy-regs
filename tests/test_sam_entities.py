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


def test_resolve_api_key_prefers_sam_specific_key(monkeypatch):
    # SAM.gov needs a SAM-authorized key, so SAM_API_KEY must win over the
    # generic api.data.gov key (which 404s on SAM) when both are set.
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("SAM_API_KEY", "sam-key")
    assert _resolve_api_key() == "sam-key"


def test_api_key_env_var_precedence_order():
    # Explicit contract: SAM-specific first, generic data.gov next, regs last.
    assert API_KEY_ENV_VARS == ("SAM_API_KEY", "DATA_GOV_API_KEY", "REGULATIONS_GOV_API_KEY")


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


def _page(records: list[dict], *, next_link: str | None) -> dict:
    links: dict[str, str] = {}
    if next_link is not None:
        links["nextLink"] = next_link
    return {"entityData": records, "links": links}


def _by_page_get(pages: dict[int, dict]):
    """Fake ``_get`` that dispatches on the ``page`` value (from params or URL query)."""

    def _get(url, params):
        if params is not None:
            page = int(params["page"])
        else:
            from urllib.parse import parse_qs, urlparse

            page = int(parse_qs(urlparse(url).query)["page"][0])
        return pages.get(page, _page([], next_link=None))

    return _get


def test_paginate_follows_nextlink_until_absent(monkeypatch):
    """The walk follows ``links.nextLink`` across pages and stops when it is gone."""
    reader = SamEntitiesReader(api_key="test", per_page=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {
        0: _page([_entity("A"), _entity("B")], next_link=f"{base}?api_key=MASKED&page=1&size=2"),
        1: _page([_entity("C"), _entity("D")], next_link=f"{base}?api_key=MASKED&page=2&size=2"),
        2: _page([_entity("E")], next_link=f"{base}?api_key=MASKED&page=3&size=2"),  # short -> stop
    }
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = [r["entityRegistration"]["ueiSAM"] for r in reader._paginate()]
    assert got == ["A", "B", "C", "D", "E"]


def test_paginate_stops_when_nextlink_missing(monkeypatch):
    """A full final page with no ``nextLink`` ends the walk (no extra fetch)."""
    reader = SamEntitiesReader(api_key="test", per_page=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {
        0: _page([_entity("A"), _entity("B")], next_link=f"{base}?api_key=MASKED&page=1&size=2"),
        1: _page([_entity("C"), _entity("D")], next_link=None),  # full page, no nextLink -> stop
    }
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = [r["entityRegistration"]["ueiSAM"] for r in reader._paginate()]
    assert got == ["A", "B", "C", "D"]


def test_paginate_stops_at_max_records(monkeypatch):
    """``max_records`` bounds the walk mid-page (no full backfill)."""
    reader = SamEntitiesReader(api_key="test", per_page=3, max_records=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {0: _page([_entity("A"), _entity("B"), _entity("C")], next_link=f"{base}?api_key=MASKED&page=1&size=3")}
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = [r["entityRegistration"]["ueiSAM"] for r in reader._paginate()]
    assert got == ["A", "B"]


def test_authorize_next_link_reinjects_real_key():
    """SAM masks the api_key in nextLink; the reader swaps in the configured key."""
    reader = SamEntitiesReader(api_key="real-secret")
    masked = "https://api.sam.gov/entity-information/v4/entities?api_key=REPLACE_WITH_API_KEY&page=5&size=10&registrationStatus=A"
    out = reader._authorize_next_link(masked)
    from urllib.parse import parse_qs, urlparse

    q = parse_qs(urlparse(out).query)
    assert q["api_key"] == ["real-secret"]
    assert q["page"] == ["5"]
    assert q["size"] == ["10"]
    assert q["registrationStatus"] == ["A"]

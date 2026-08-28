"""Hermetic tests for the SAM.gov entity ingest (no network).

Covers the pieces with real logic: the raw-entity -> published-schema mapping
(``_shape``), the API-key resolution fallback chain, the keyless no-op, the bulk
*extract* path (trigger -> download-URL discovery -> defensive file parse, with
``registrationDate`` year-windowing and ``max_records`` bounding), and the
*partition* path's adaptive date-window subdivision + paginated walk. The fixture
below is a trimmed but faithful copy of a real ``entityData[]`` record from the v4
``/entities`` response.
"""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from datetime import date
from pathlib import Path

import pytest

from spicy_regs.sources import sam_entities as sam
from spicy_regs.sources.sam_entities import (
    API_KEY_ENV_VARS,
    PER_PAGE,
    SamEntitiesReader,
    _find_download_url,
    _parse_extract_bytes,
    _range_literal,
    _resolve_api_key,
    _us_date,
    _year_range_literal,
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


def test_reader_rejects_unknown_mode():
    with pytest.raises(ValueError, match="mode must be"):
        SamEntitiesReader(mode="bogus")


# -- date literals -----------------------------------------------------------


def test_us_date_and_range_literals():
    assert _us_date(date(2020, 1, 5)) == "01/05/2020"
    assert _range_literal(date(2020, 1, 1), date(2020, 12, 31)) == "[01/01/2020,12/31/2020]"
    assert _year_range_literal(2021) == "[01/01/2021,12/31/2021]"


# -- extract: download-URL discovery -----------------------------------------


def test_find_download_url_prefers_placeholder_link():
    payload = {
        "totalRecords": 764850,
        "links": {"selfLink": "https://api.sam.gov/entity-information/v4/entities?api_key=X"},
        "download": "https://api.sam.gov/comp/extracts/ENTITY_123.json?api_key=REPLACE_WITH_API_KEY",
    }
    assert _find_download_url(payload) == payload["download"]


def test_find_download_url_falls_back_to_download_like_url():
    payload = {"result": {"fileUrl": "https://api.sam.gov/comp/extractfile/download/abc.zip"}}
    assert _find_download_url(payload) == "https://api.sam.gov/comp/extractfile/download/abc.zip"


def test_find_download_url_none_when_absent():
    assert _find_download_url({"totalRecords": 0, "entityData": []}) is None


# -- extract: defensive file parsing -----------------------------------------


def _entity(uei: str) -> dict:
    return {"entityRegistration": {"ueiSAM": uei}}


def _ueis(records) -> list[str]:
    return [r["entityRegistration"]["ueiSAM"] for r in records]


def test_parse_extract_envelope_json():
    raw = json.dumps({"entityData": [_entity("A"), _entity("B")]}).encode()
    assert _ueis(_parse_extract_bytes(raw)) == ["A", "B"]


def test_parse_extract_bare_array():
    raw = json.dumps([_entity("A"), _entity("C")]).encode()
    assert _ueis(_parse_extract_bytes(raw)) == ["A", "C"]


def test_parse_extract_ndjson():
    raw = ("\n".join(json.dumps(_entity(u)) for u in ("A", "B", "C")) + "\n").encode()
    assert _ueis(_parse_extract_bytes(raw)) == ["A", "B", "C"]


def test_parse_extract_gzip_envelope():
    raw = gzip.compress(json.dumps({"entityData": [_entity("Z")]}).encode())
    assert _ueis(_parse_extract_bytes(raw)) == ["Z"]


def test_parse_extract_zip_member():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ENTITY.json", json.dumps([_entity("Q"), _entity("R")]))
    assert _ueis(_parse_extract_bytes(buf.getvalue())) == ["Q", "R"]


def test_parse_extract_empty_or_garbage():
    assert list(_parse_extract_bytes(b"")) == []
    assert list(_parse_extract_bytes(b"not json at all")) == []


# -- extract: orchestration (year windows + max_records) ---------------------


def test_extract_windows_over_years_and_dedups_disjoint(monkeypatch):
    """Each registrationDate year triggers its own extract; results union across years."""
    reader = SamEntitiesReader(api_key="k", since_year=2019, until_year=2021)
    calls: list[str] = []

    def fake_extract_window(year):
        calls.append(str(year))
        # one record per year, disjoint UEIs
        yield from reader._emit(_entity(f"Y{year}"))

    monkeypatch.setattr(reader, "_extract_window", fake_extract_window)
    got = _ueis(reader._iter_extract())
    assert got == ["Y2019", "Y2020", "Y2021"]
    assert calls == ["2019", "2020", "2021"]


def test_extract_single_window_when_year_windows_disabled(monkeypatch):
    reader = SamEntitiesReader(api_key="k", year_windows=False)
    seen_years: list[int | None] = []

    def fake_extract_window(year):
        seen_years.append(year)
        yield from reader._emit(_entity("A"))

    monkeypatch.setattr(reader, "_extract_window", fake_extract_window)
    got = _ueis(reader._iter_extract())
    assert got == ["A"]
    assert seen_years == [None]  # a single unscoped extract, no year windowing


def test_extract_stops_at_max_records_across_windows(monkeypatch):
    """max_records bounds the run mid-window and prevents further extract requests."""
    reader = SamEntitiesReader(api_key="k", since_year=2019, until_year=2021, max_records=2)
    calls: list[int] = []

    def fake_extract_window(year):
        calls.append(year)
        for i in range(5):
            yield from reader._emit(_entity(f"{year}-{i}"))

    monkeypatch.setattr(reader, "_extract_window", fake_extract_window)
    got = _ueis(reader._iter_extract())
    assert got == ["2019-0", "2019-1"]
    # The 2020/2021 windows are never requested — the budget was already spent.
    assert calls == [2019]


def test_extract_window_triggers_then_downloads(monkeypatch):
    """_extract_window: trigger call -> find URL -> download -> yield records."""
    reader = SamEntitiesReader(api_key="k")
    triggered: list[dict] = []

    def fake_get(url, params):
        triggered.append(params)
        return {"download": "https://api.sam.gov/x/f.json?api_key=REPLACE_WITH_API_KEY"}

    monkeypatch.setattr(reader, "_get", fake_get)
    monkeypatch.setattr(reader, "_download_extract", lambda url: iter([_entity("A"), _entity("B")]))
    got = _ueis(reader._extract_window(2022))
    assert got == ["A", "B"]
    # The trigger request asked for a JSON extract scoped to the year.
    assert triggered[0]["format"] == "json"
    assert triggered[0]["registrationDate"] == "[01/01/2022,12/31/2022]"


def test_extract_window_skips_when_no_download_url(monkeypatch):
    reader = SamEntitiesReader(api_key="k")
    monkeypatch.setattr(reader, "_get", lambda url, params: {"totalRecords": 0})
    called = {"downloaded": False}

    def fake_download(url):
        called["downloaded"] = True
        yield from ()

    monkeypatch.setattr(reader, "_download_extract", fake_download)
    assert list(reader._extract_window(2022)) == []
    assert called["downloaded"] is False


def test_extract_window_falls_back_to_inline_data(monkeypatch):
    """A small window returned inline (no extract URL) is still ingested."""
    reader = SamEntitiesReader(api_key="k")
    monkeypatch.setattr(
        reader, "_get", lambda url, params: {"totalRecords": 2, "entityData": [_entity("A"), _entity("B")]}
    )
    # _download_extract must not be reached when there is no URL but inline data exists.
    monkeypatch.setattr(reader, "_download_extract", lambda url: (_ for _ in ()).throw(AssertionError("unexpected")))
    assert _ueis(reader._extract_window(2022)) == ["A", "B"]


# -- extract: download polling ----------------------------------------------


class _FakeResp:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected raise_for_status at {self.status_code}")


class _FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[str] = []

    def get(self, url):
        self.calls.append(url)
        return self._responses.pop(0)


def test_download_extract_polls_until_ready(monkeypatch):
    monkeypatch.setattr(sam.time, "sleep", lambda *_: None)
    reader = SamEntitiesReader(api_key="secret")
    body = json.dumps({"entityData": [_entity("A"), _entity("B")]}).encode()
    fake = _FakeClient([_FakeResp(202), _FakeResp(202), _FakeResp(200, body)])
    monkeypatch.setattr(reader, "_client", fake)
    got = _ueis(reader._download_extract("https://api.sam.gov/x/f.json?api_key=REPLACE_WITH_API_KEY"))
    assert got == ["A", "B"]
    # The masked placeholder was swapped for the real key on the download request.
    assert "REPLACE_WITH_API_KEY" not in fake.calls[0]
    assert "api_key=secret" in fake.calls[0]


def test_download_extract_gives_up_after_poll_max(monkeypatch):
    monkeypatch.setattr(sam.time, "sleep", lambda *_: None)
    monkeypatch.setattr(sam, "_EXTRACT_POLL_MAX", 3)
    reader = SamEntitiesReader(api_key="secret")
    fake = _FakeClient([_FakeResp(202), _FakeResp(202), _FakeResp(202)])
    monkeypatch.setattr(reader, "_client", fake)
    assert list(reader._download_extract("https://api.sam.gov/x/f.json")) == []


# -- reinject key ------------------------------------------------------------


def test_reinject_key_reinjects_query_param():
    """SAM masks the api_key in nextLink; the reader swaps in the configured key."""
    reader = SamEntitiesReader(api_key="real-secret")
    masked = (
        "https://api.sam.gov/entity-information/v4/entities"
        "?api_key=REPLACE_WITH_API_KEY&page=5&size=10&registrationStatus=A"
    )
    out = reader._reinject_key(masked)
    from urllib.parse import parse_qs, urlparse

    q = parse_qs(urlparse(out).query)
    assert q["api_key"] == ["real-secret"]
    assert q["page"] == ["5"]
    assert q["size"] == ["10"]
    assert q["registrationStatus"] == ["A"]


def test_reinject_key_swaps_placeholder_token_in_path():
    reader = SamEntitiesReader(api_key="real-secret")
    # Extract URLs can carry the placeholder anywhere, including outside the query.
    masked = "https://api.sam.gov/comp/extractfile/REPLACE_WITH_API_KEY/ENTITY.json"
    out = reader._reinject_key(masked)
    assert "REPLACE_WITH_API_KEY" not in out
    assert "api_key=real-secret" in out


# -- partition walk: adaptive subdivision ------------------------------------


def test_partition_subdivides_window_over_ceiling(monkeypatch):
    """A window whose totalRecords exceeds the ceiling is halved until pageable."""
    reader = SamEntitiesReader(mode="partition", api_key="k", since_year=2020, until_year=2020)

    # Full year is over the ceiling; each half is under it.
    def fake_total(gte, lte):
        span_days = (lte - gte).days
        return 8000 if span_days > 200 else 4000

    paged: list[tuple[date, date]] = []

    def fake_page(gte, lte):
        paged.append((gte, lte))
        yield from reader._emit(_entity(f"{gte.isoformat()}..{lte.isoformat()}"))

    monkeypatch.setattr(reader, "_window_total", fake_total)
    monkeypatch.setattr(reader, "_page_window", fake_page)
    got = _ueis(reader._iter_partitioned())

    # The year split into two disjoint, contiguous halves; each leaf was paged once.
    assert len(paged) == 2
    (g1, l1), (g2, l2) = paged
    assert g1 == date(2020, 1, 1)
    assert l2 == date(2020, 12, 31)
    assert g2 == date.fromordinal(l1.toordinal() + 1)  # no gap, no overlap
    assert got == [f"{g1}..{l1}", f"{g2}..{l2}"]


def test_partition_pages_window_under_ceiling_without_split(monkeypatch):
    reader = SamEntitiesReader(mode="partition", api_key="k", since_year=2020, until_year=2020)
    monkeypatch.setattr(reader, "_window_total", lambda gte, lte: 12)
    paged: list[tuple[date, date]] = []

    def fake_page(gte, lte):
        paged.append((gte, lte))
        yield from reader._emit(_entity("solo"))

    monkeypatch.setattr(reader, "_page_window", fake_page)
    got = _ueis(reader._iter_partitioned())
    assert paged == [(date(2020, 1, 1), date(2020, 12, 31))]  # single window, no split
    assert got == ["solo"]


def test_partition_single_day_over_ceiling_warns_but_pages(monkeypatch):
    """A single day still over the ceiling is paged (best-effort), not infinitely split."""
    reader = SamEntitiesReader(mode="partition", api_key="k", since_year=2020, until_year=2020)
    monkeypatch.setattr(reader, "_window_total", lambda gte, lte: 9000)  # always over ceiling
    paged: list[tuple[date, date]] = []

    def fake_page(gte, lte):
        paged.append((gte, lte))
        yield from ()

    monkeypatch.setattr(reader, "_page_window", fake_page)
    list(reader._iter_partitioned())
    # Recursion bottomed out at single days (365/366 of them), each paged once.
    assert len(paged) >= 365
    assert all(gte == lte for gte, lte in paged)


# -- partition walk: paginated nextLink follow -------------------------------


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


def test_page_window_follows_nextlink_until_absent(monkeypatch):
    reader = SamEntitiesReader(mode="partition", api_key="test", per_page=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {
        0: _page([_entity("A"), _entity("B")], next_link=f"{base}?api_key=MASKED&page=1&size=2"),
        1: _page([_entity("C"), _entity("D")], next_link=f"{base}?api_key=MASKED&page=2&size=2"),
        2: _page([_entity("E")], next_link=f"{base}?api_key=MASKED&page=3&size=2"),  # short -> stop
    }
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = _ueis(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))
    assert got == ["A", "B", "C", "D", "E"]


def test_page_window_stops_when_nextlink_missing(monkeypatch):
    reader = SamEntitiesReader(mode="partition", api_key="test", per_page=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {
        0: _page([_entity("A"), _entity("B")], next_link=f"{base}?api_key=MASKED&page=1&size=2"),
        1: _page([_entity("C"), _entity("D")], next_link=None),  # full page, no nextLink -> stop
    }
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = _ueis(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))
    assert got == ["A", "B", "C", "D"]


def test_page_window_stops_at_max_records(monkeypatch):
    reader = SamEntitiesReader(mode="partition", api_key="test", per_page=3, max_records=2)
    base = "https://api.sam.gov/entity-information/v4/entities"
    pages = {0: _page([_entity("A"), _entity("B"), _entity("C")], next_link=f"{base}?api_key=MASKED&page=1&size=3")}
    monkeypatch.setattr(reader, "_get", _by_page_get(pages))
    got = _ueis(reader._page_window(date(2020, 1, 1), date(2020, 12, 31)))
    assert got == ["A", "B"]


# -- rollup: bounded rotation + env overrides --------------------------------


def test_rotating_year_covers_full_range_over_a_cycle():
    from spicy_regs.pipelines.rollups.sam_entities import _MIN_REGISTRATION_YEAR, _rotating_year

    today = date(2026, 7, 18)
    span = today.year - _MIN_REGISTRATION_YEAR + 1
    seen = {_rotating_year(date.fromordinal(today.toordinal() + d)) for d in range(span)}
    # A full rotation touches every year in [min, current] exactly once.
    assert seen == set(range(_MIN_REGISTRATION_YEAR, today.year + 1))


def test_int_env_parses_blank_and_bad_values(monkeypatch):
    from spicy_regs.pipelines.rollups.sam_entities import _int_env

    monkeypatch.delenv("SAM_MAX_RECORDS", raising=False)
    assert _int_env("SAM_MAX_RECORDS") is None
    monkeypatch.setenv("SAM_MAX_RECORDS", "  ")
    assert _int_env("SAM_MAX_RECORDS") is None
    monkeypatch.setenv("SAM_MAX_RECORDS", "not-a-number")
    assert _int_env("SAM_MAX_RECORDS") is None
    monkeypatch.setenv("SAM_MAX_RECORDS", "25000")
    assert _int_env("SAM_MAX_RECORDS") == 25000


def test_rollup_build_defaults_to_rotating_single_year(monkeypatch):
    """With no env overrides the scheduled build fetches one rotating year window."""
    from spicy_regs.pipelines.rollups import sam_entities as rollup

    for var in ("SAM_INGEST_MODE", "SAM_SINCE_YEAR", "SAM_UNTIL_YEAR", "SAM_MAX_RECORDS"):
        monkeypatch.delenv(var, raising=False)
    captured: dict = {}

    def fake_build(output_dir, **kwargs):
        captured.update(kwargs)
        return output_dir / "sam_entities.parquet"

    monkeypatch.setattr(rollup, "build_sam_entities", fake_build)
    rollup.SamEntitiesRollup().build(Path("/tmp/out"))
    assert captured["mode"] == "extract"
    assert captured["since_year"] == captured["until_year"]  # a single year window
    assert captured["max_records"] is None


def test_rollup_build_honours_explicit_range(monkeypatch, tmp_path):
    from spicy_regs.pipelines.rollups import sam_entities as rollup

    monkeypatch.setenv("SAM_SINCE_YEAR", "2000")
    monkeypatch.setenv("SAM_UNTIL_YEAR", "2026")
    monkeypatch.setenv("SAM_MAX_RECORDS", "0")  # blank/0 -> unbounded
    monkeypatch.setenv("SAM_INGEST_MODE", "partition")
    captured: dict = {}

    def fake_build(output_dir, **kwargs):
        captured.update(kwargs)
        return output_dir / "sam_entities.parquet"

    monkeypatch.setattr(rollup, "build_sam_entities", fake_build)
    rollup.SamEntitiesRollup().build(tmp_path)
    assert captured == {"mode": "partition", "since_year": 2000, "until_year": 2026, "max_records": None}

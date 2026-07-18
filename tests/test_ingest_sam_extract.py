"""Hermetic test for the SAM public-extract parser (no network)."""

from __future__ import annotations

import duckdb

from spicy_regs.ingest_sam_extract import build_sam_entities_from_extract


def _record(**pos: str) -> str:
    """Build one 142-field pipe-delimited record with given 0-indexed positions set."""
    fields = [""] * 142
    for k, v in pos.items():
        fields[int(k[1:])] = v  # keys like "c0", "c18"
    return "|".join(fields)


_FIXTURE = "\n".join(
    [
        "BOF PUBLIC V2 00000000 20260705 0000002 0000000",
        _record(
            c0="ABCDEFGHIJ12",
            c3="1A2B3",
            c5="A",
            c6="Z2",
            c7="20200115",
            c8="20270115",
            c11="ACME WIDGETS INC",
            c12="ACME",
            c17="RANCHO CORDOVA",
            c18="CA",
            c19="95742",
            c22="06",
            c26="www.acme.example",
            c27="2L",
            c32="444110",
        ),
        _record(
            c0="ZYXWVUTSRQ98",
            c3="",
            c5="E",
            c6="Z1",
            c7="20150301",
            c8="20240301",
            c11="OLD CO LLC",
            c18="TX",
            c19="75001",
            c32="",
        ),
        "EOF PUBLIC V2 00000000 20260705 0000002 0000000",
    ]
)


def test_extract_parses_and_maps(tmp_path):
    src = tmp_path / "SAM_PUBLIC_MONTHLY_V2_20260705.dat"
    src.write_text(_FIXTURE + "\n")
    out = tmp_path / "sam_entities.parquet"
    build_sam_entities_from_extract(src, out)

    rows = duckdb.sql(f"SELECT * FROM read_parquet('{out}') ORDER BY uei").fetchall()
    cols = [d[0] for d in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()]
    assert cols == [
        "uei",
        "cage_code",
        "legal_business_name",
        "dba_name",
        "entity_structure_desc",
        "entity_type_desc",
        "profit_structure_desc",
        "state",
        "city",
        "zip_code",
        "congressional_district",
        "primary_naics",
        "registration_status",
        "registration_date",
        "registration_expiration_date",
        "exclusion_status_flag",
        "purpose_of_registration_desc",
        "entity_url",
    ]
    assert len(rows) == 2  # BOF/EOF sentinel lines dropped

    r = dict(zip(cols, rows[0]))  # ABCDEFGHIJ12
    assert r["uei"] == "ABCDEFGHIJ12"
    assert r["cage_code"] == "1A2B3"
    assert r["legal_business_name"] == "ACME WIDGETS INC"
    assert r["dba_name"] == "ACME"
    assert r["state"] == "CA" and r["city"] == "RANCHO CORDOVA" and r["zip_code"] == "95742"
    assert r["congressional_district"] == "06"
    assert r["primary_naics"] == "444110"
    assert r["entity_structure_desc"] == "2L"
    assert r["entity_url"] == "www.acme.example"
    assert r["registration_status"] == "Active"  # A -> Active
    assert r["purpose_of_registration_desc"] == "All Awards"  # Z2 -> mapped
    assert r["registration_date"] == "2020-01-15"  # YYYYMMDD -> YYYY-MM-DD
    assert r["registration_expiration_date"] == "2027-01-15"
    # Fields the extract doesn't cleanly provide are NULL.
    assert r["entity_type_desc"] is None
    assert r["profit_structure_desc"] is None
    assert r["exclusion_status_flag"] is None

    r2 = dict(zip(cols, rows[1]))  # ZYXWVUTSRQ98
    assert r2["registration_status"] == "Expired"  # E -> Expired
    assert r2["purpose_of_registration_desc"] == "Federal Assistance Awards"  # Z1
    assert r2["cage_code"] is None and r2["primary_naics"] is None  # empty -> NULL

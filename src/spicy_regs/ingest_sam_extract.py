"""Ingest the SAM.gov public monthly *entity extract* into ``sam_entities``.

The SAM Entity Management API is daily-quota-gated (≈1,000 req/day for a
non-federal keyed account) and caps a single query's pagination at ~5,000
records, so the live-API reader (``sources/sam_entities.py``) can only ever
sample the ~885K registered entities. The **public monthly extract** is the
full set in one flat file — this module parses it and publishes the complete
``sam_entities`` table.

Get the file (no API quota involved): SAM.gov → **Data Services** → *Entity
Registration* → *Public* → download ``SAM_PUBLIC_MONTHLY_V2_YYYYMMDD.ZIP`` and
unzip the ``.dat``. (Or the extract API:
``https://api.sam.gov/data-services/v1/extracts?api_key=...&fileType=ENTITY&sensitivity=PUBLIC``.)

Usage::

    uv run python -m spicy_regs.ingest_sam_extract SAM_PUBLIC_MONTHLY_V2_20260705.dat
    uv run python -m spicy_regs.ingest_sam_extract <file>.dat --output out.parquet --upload

File format: pipe-delimited, **142 fields**, no quoting; a ``BOF PUBLIC V2 ...``
header line and an ``EOF PUBLIC V2 ...`` trailer line wrap the data records.

Field map (1-indexed extract position → published column), confirmed against the
2026-07 extract:

===================================  ========  ================================
column                               position  notes
===================================  ========  ================================
uei                                  1         12-char Unique Entity ID (PK)
cage_code                            4
legal_business_name                  12
dba_name                             13
entity_structure_desc                28        raw structure CODE (see note)
state                                19        physical-address state
city                                 18
zip_code                             20
congressional_district               23
primary_naics                        33        primary NAICS (list starts here)
registration_status                  6         A→Active, E→Expired
registration_date                    8         YYYYMMDD → YYYY-MM-DD
registration_expiration_date         9         YYYYMMDD → YYYY-MM-DD
purpose_of_registration_desc         7         Z1/Z2/Z5 mapped, else raw code
entity_url                           27
===================================  ========  ================================

Extract limitations vs the API: the extract does not cleanly expose
``entity_type_desc``, ``profit_structure_desc``, or ``exclusion_status_flag``
(exclusions are a separate SAM extract), so those are published NULL; and
``entity_structure_desc`` carries SAM's raw structure code rather than the API's
prose description.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

OUTPUT = "sam_entities.parquet"

# 1-indexed extract position -> 0-indexed DuckDB column name (read as c0..c141).
POS = {
    "uei": "c0",
    "cage_code": "c3",
    "legal_business_name": "c11",
    "dba_name": "c12",
    "entity_structure_desc": "c27",
    "state": "c18",
    "city": "c17",
    "zip_code": "c19",
    "congressional_district": "c22",
    "primary_naics": "c32",
    "registration_status_code": "c5",
    "purpose_code": "c6",
    "registration_date_raw": "c7",
    "registration_expiration_date_raw": "c8",
    "entity_url": "c26",
}

# SAM code -> description for the small, stable code sets we surface.
STATUS_MAP = {"A": "Active", "E": "Expired"}
PURPOSE_MAP = {"Z1": "Federal Assistance Awards", "Z2": "All Awards", "Z5": "IGT Only"}

_NUM_FIELDS = 142


def _read_csv_expr(src: Path) -> str:
    cols = "{" + ",".join(f"'c{i}':'VARCHAR'" for i in range(_NUM_FIELDS)) + "}"
    # No quoting (flat file with literal quotes in data); skip the BOF header;
    # the EOF trailer is dropped by the UEI-length filter below.
    return (
        f"read_csv('{src}', delim='|', header=false, auto_detect=false, quote='', "
        f"escape='', null_padding=true, ignore_errors=true, parallel=false, skip=1, "
        f"max_line_size=20000000, columns={cols})"
    )


def _case_map(col: str, mapping: dict[str, str]) -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE trim({col}) {whens} ELSE NULLIF(trim({col}),'') END"


def build_sam_entities_from_extract(src: Path, out: Path) -> Path:
    """Parse a SAM public entity extract ``.dat`` into ``sam_entities`` parquet."""
    import duckdb

    def nz(c: str) -> str:
        return f"NULLIF(trim({c}),'')"

    def dt(c: str) -> str:
        return f"CASE WHEN length(trim({c}))=8 THEN substr({c},1,4)||'-'||substr({c},5,2)||'-'||substr({c},7,2) END"

    logger.info("SAM extract: parsing {}", src)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
          SELECT uei, cage_code, legal_business_name, dba_name, entity_structure_desc,
                 entity_type_desc, profit_structure_desc, state, city, zip_code,
                 congressional_district, primary_naics, registration_status,
                 registration_date, registration_expiration_date, exclusion_status_flag,
                 purpose_of_registration_desc, entity_url
          FROM (
            SELECT {nz(POS["uei"])} uei, {nz(POS["cage_code"])} cage_code,
              {nz(POS["legal_business_name"])} legal_business_name, {nz(POS["dba_name"])} dba_name,
              {nz(POS["entity_structure_desc"])} entity_structure_desc,
              CAST(NULL AS VARCHAR) entity_type_desc, CAST(NULL AS VARCHAR) profit_structure_desc,
              {nz(POS["state"])} state, {nz(POS["city"])} city, {nz(POS["zip_code"])} zip_code,
              {nz(POS["congressional_district"])} congressional_district, {nz(POS["primary_naics"])} primary_naics,
              {_case_map(POS["registration_status_code"], STATUS_MAP)} registration_status,
              {dt(POS["registration_date_raw"])} registration_date,
              {dt(POS["registration_expiration_date_raw"])} registration_expiration_date,
              CAST(NULL AS VARCHAR) exclusion_status_flag,
              {_case_map(POS["purpose_code"], PURPOSE_MAP)} purpose_of_registration_desc,
              {nz(POS["entity_url"])} entity_url,
              ROW_NUMBER() OVER (
                PARTITION BY {POS["uei"]}
                ORDER BY ({POS["registration_status_code"]}='A') DESC, {POS["registration_expiration_date_raw"]} DESC
              ) rn
            FROM {_read_csv_expr(src)}
            WHERE length(trim({POS["uei"]}))=12
          ) WHERE rn=1
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        """
    )
    con.close()
    rows = pq.ParquetFile(out).metadata.num_rows
    logger.info("SAM extract: {:,} entities -> {}", rows, out)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest a SAM public entity extract into sam_entities.")
    ap.add_argument("dat", type=Path, help="Path to SAM_PUBLIC_MONTHLY_V2_YYYYMMDD.dat")
    ap.add_argument("--output", type=Path, default=Path("output") / OUTPUT, help="Output parquet path")
    ap.add_argument("--upload", action="store_true", help="Publish the result to R2 (needs R2 creds)")
    args = ap.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out = build_sam_entities_from_extract(args.dat, args.output)
    if args.upload:
        from spicy_regs.sources import r2

        r2.upload_file(out, remote_key=OUTPUT)
        logger.info("Uploaded {} to R2", OUTPUT)


if __name__ == "__main__":
    main()

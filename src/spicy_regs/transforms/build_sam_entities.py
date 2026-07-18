"""Transform: build ``sam_entities.parquet`` from the SAM.gov Entity API (v4).

Produces an 18-column all-VARCHAR schema keyed on ``uei`` (the Unique Entity ID),
the federal entity registry that anchors organization/entity resolution across
the corpus — the same UEI the dashboard uses to tie a commenting organization to
its registered identity.

Incremental by design. A full re-fetch of the entire registry every run would be
wasteful, rate-limited, *and* would trip the R2 catastrophic-shrink guard on any
short run. Instead we:

1. Best-effort download the prior ``sam_entities.parquet`` from R2.
2. Fetch a *bounded* window of active registrations (``max_records``) this run.
3. Dedup the union on ``uei``, preferring the freshly fetched row.

With no prior table (first run) step 2 seeds the table; subsequent runs refresh
and extend coverage. A full backfill is never triggered implicitly — raise
``max_records`` deliberately for a wider pull.

Scope is deliberately **list-level only**: every column comes from the
``/entities`` list payload, so there are no per-entity detail fetches.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.sam_entities import SamEntitiesReader

OUTPUT = "sam_entities.parquet"

# Bounded default window pulled per run (a scheduled ingest must never attempt a
# full ~hundreds-of-thousands-row backfill). Raise deliberately for a wider pull.
DEFAULT_MAX_RECORDS = 5_000

# The published schema: 18 columns, all VARCHAR, in a fixed order. ``uei`` is the
# primary / dedup key. Nested paths (source field) are noted where non-obvious.
COLUMNS = (
    "uei",  # entityRegistration.ueiSAM
    "cage_code",  # entityRegistration.cageCode
    "legal_business_name",  # entityRegistration.legalBusinessName
    "dba_name",  # entityRegistration.dbaName
    "entity_structure_desc",  # coreData.generalInformation.entityStructureDesc
    "entity_type_desc",  # coreData.generalInformation.entityTypeDesc
    "profit_structure_desc",  # coreData.generalInformation.profitStructureDesc
    "state",  # coreData.physicalAddress.stateOrProvinceCode
    "city",  # coreData.physicalAddress.city
    "zip_code",  # coreData.physicalAddress.zipCode
    "congressional_district",  # coreData.congressionalDistrict
    "primary_naics",  # assertions.goodsAndServices.primaryNaics
    "registration_status",  # entityRegistration.registrationStatus
    "registration_date",  # entityRegistration.registrationDate
    "registration_expiration_date",  # entityRegistration.registrationExpirationDate
    "exclusion_status_flag",  # entityRegistration.exclusionStatusFlag
    "purpose_of_registration_desc",  # entityRegistration.purposeOfRegistrationDesc
    "entity_url",  # coreData.entityInformation.entityURL
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (some fields come as ints.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw SAM.gov entity onto the published column shape.

    Missing nested objects degrade to null rather than raising, so a sparse
    registration (e.g. one with no ``coreData``) still produces a row keyed by
    its UEI.
    """
    reg = doc.get("entityRegistration") or {}
    core = doc.get("coreData") or {}
    entity_info = core.get("entityInformation") or {}
    address = core.get("physicalAddress") or {}
    general = core.get("generalInformation") or {}
    goods = (doc.get("assertions") or {}).get("goodsAndServices") or {}
    return {
        "uei": reg.get("ueiSAM"),
        "cage_code": reg.get("cageCode"),
        "legal_business_name": reg.get("legalBusinessName"),
        "dba_name": reg.get("dbaName"),
        "entity_structure_desc": general.get("entityStructureDesc"),
        "entity_type_desc": general.get("entityTypeDesc"),
        "profit_structure_desc": general.get("profitStructureDesc"),
        "state": address.get("stateOrProvinceCode"),
        "city": address.get("city"),
        "zip_code": address.get("zipCode"),
        "congressional_district": _s(core.get("congressionalDistrict")),
        "primary_naics": _s(goods.get("primaryNaics")),
        "registration_status": reg.get("registrationStatus"),
        "registration_date": reg.get("registrationDate"),
        "registration_expiration_date": reg.get("registrationExpirationDate"),
        "exclusion_status_flag": reg.get("exclusionStatusFlag"),
        "purpose_of_registration_desc": reg.get("purposeOfRegistrationDesc"),
        "entity_url": entity_info.get("entityURL"),
    }


def build_sam_entities(
    output_dir: Path,
    *,
    max_records: int | None = DEFAULT_MAX_RECORDS,
    registration_status: str = "A",
) -> Path:
    """Build ``sam_entities.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_sam_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a fresh seed).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("SAM entities: merging against prior table {}", prior_file)
    else:
        logger.info("SAM entities: no prior table found — seeding a new table")

    # 2. Fetch + shape into a "new rows" parquet.
    reader = SamEntitiesReader(registration_status=registration_status, max_records=max_records)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_sam_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("SAM entities: fetched {:,} entities this run", len(rows))

    # 3. Merge prior + new, dedup on uei preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY uei ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE uei IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY legal_business_name, uei
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("SAM entities: {:,} rows", total)
    return out_file

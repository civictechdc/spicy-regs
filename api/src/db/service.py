from math import inf
from duckdb import DataError
import polars as pl
from .models import RegulationsDataTypes
from .config import refresh_cache
from .utils import _build_base_query, _build_where_clause
from . import get_connection
from ..logger import logger


def get_data_df(
    data_type_enum: RegulationsDataTypes,
    agency_code,
    docket_id: str = None,
    max_cache_age_hours: int = inf,
) -> pl.DataFrame:
    """Get data with smart caching based on age"""
    conn = get_connection()
    data_type = data_type_enum.value
    logger.info(
        f"Getting data for {data_type} with agency code {agency_code} and docket id {docket_id}"
    )
    table_name = f"{data_type}_cache"
    where_clause = _build_where_clause(agency_code, docket_id)

    # Check cache age
    cache_age = conn.sql(f"""
    SELECT 
        COUNT(*) as count,
        MAX(cached_at) as last_updated,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(cached_at)))/3600 as age_hours
    FROM {table_name} 
    WHERE {where_clause}
    """).fetchone()
    logger.info(f"Cache age: {cache_age}")

    try:
        if cache_age[0] > 0 and cache_age[2] < max_cache_age_hours:
            # Use cached data
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
            return conn.sql(query).pl()
        else:
            # Cache is stale or doesn't exist, refresh
            refresh_cache(agency_code, docket_id, data_type_enum)
            return get_data_df(
                data_type_enum, agency_code, docket_id, max_cache_age_hours
            )
    except DataError:
        logger.exception("Error getting data fallback to live query from S3")
        # Fallback to live query
        query = _build_base_query(data_type, agency_code, docket_id)
        if agency_code or docket_id:
            query += f" WHERE {where_clause.replace('agency_code', 'f.agency_code').replace('docket_id', 'f.docket_id')}"
        return conn.sql(query).pl()

from . import get_connection
from .constants import FIELD_MAPPINGS

def _build_base_query(data_type: str, agency_code: str = None, docket_id: str = None) -> str:
    """Build base query for different data types"""
    mapping = FIELD_MAPPINGS[data_type]
    fields = ",\n        ".join(mapping['fields'])
    
    # Build path based on parameters
    if agency_code and docket_id:
        path = f"s3://mirrulations/raw-data/{agency_code}/{docket_id}/text-{docket_id}/{mapping['path_pattern']}"
    elif agency_code:
        path = f"s3://mirrulations/raw-data/{agency_code}/*/*/{mapping['path_pattern']}"
    else:
        path = f"s3://mirrulations/raw-data/*/*/{mapping['path_pattern']}"
    
    return f"""
    SELECT
        f.agency_code,
        f.docket_id,
        f.year,
        f.content AS raw_json,
        {fields}
    FROM (
        SELECT
            filename,
            content,
            split_part(filename, '/', 5) as agency_code,
            split_part(filename, '/', 6) as docket_id,
            split_part(split_part(filename, '/', 6), '-', 2) as year
        FROM read_text('{path}')
    ) f
    """

def _build_where_clause(agency_code: str = None, docket_id: str = None) -> str:
    """Build WHERE clause for queries"""
    where_clause = []
    if agency_code:
        where_clause.append(f"agency_code = '{agency_code.upper()}'")
    if docket_id:
        where_clause.append(f"docket_id = '{docket_id.upper()}'")
    return " AND ".join(where_clause) if where_clause else "1=1"

def get_cache_stats() -> dict:
    """Get cache statistics"""
    conn = get_connection()
    stats = {}
    
    for data_type in FIELD_MAPPINGS.keys():
        table_name = f"{data_type}_cache"
        cache_stats = conn.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT agency_code) as agencies,
            MIN(cached_at) as oldest_cache,
            MAX(cached_at) as newest_cache
        FROM {table_name}
        """).fetchone()
        
        stats[data_type] = {
            'total_records': cache_stats[0],
            'agencies': cache_stats[1],
            'oldest_cache': cache_stats[2],
            'newest_cache': cache_stats[3]
        }
    
    return stats
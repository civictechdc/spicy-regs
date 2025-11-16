from fastapi import HTTPException
from ..db.models import RegulationsDataTypes
from . import get_connection
from .constants import FIELD_MAPPINGS
from .utils import _build_base_query, _build_where_clause
from ..logger import logger

def create_empty_tables():
    """Create empty tables without loading any data"""
    conn = get_connection()
    for data_type in FIELD_MAPPINGS.keys():
        table_name = f"{data_type}_cache"
        
        # Build column definitions
        columns = [
            "agency_code VARCHAR",
            "docket_id VARCHAR", 
            "year VARCHAR",
            "raw_json TEXT"
        ]
        
        # Add specific fields for each data type
        if data_type == 'dockets':
            columns.extend([
                "docket_type VARCHAR",
                "modify_date TIMESTAMP",
                "title TEXT"
            ])
        elif data_type == 'comments':
            columns.extend([
                "comment_id VARCHAR",
                "category VARCHAR",
                "comment TEXT",
                "document_type VARCHAR",
                "modify_date TIMESTAMP",
                "posted_date TIMESTAMP",
                "receive_date TIMESTAMP",
                "subtype VARCHAR",
                "title TEXT",
                "withdrawn BOOLEAN"
            ])
        elif data_type == 'documents':
            columns.extend([
                "document_id VARCHAR",
                "category VARCHAR",
                "document_type VARCHAR",
                "comment_start_date TIMESTAMP",
                "comment_end_date TIMESTAMP",
                "modify_date TIMESTAMP",
                "posted_date TIMESTAMP",
                "receive_date TIMESTAMP",
                "page_count INTEGER",
                "withdrawn BOOLEAN"
            ])
        
        columns.append("cached_at TIMESTAMP")
        # Create table with proper schema
        column_defs = ",\n    ".join(columns)
        conn.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_defs}
        )
        """)
    logger.info("Tables created successfully!")

def initialize_tables():
    """Create persistent tables for caching data"""
    conn = get_connection()
    for data_type in FIELD_MAPPINGS.keys():
        table_name = f"{data_type}_cache"
        query = _build_base_query(data_type)
        
        conn.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT *, CURRENT_TIMESTAMP as cached_at
        FROM ({query})
        """)
    logger.info("Tables initialized successfully!")

def create_indexes():
    """Create indexes for faster queries"""
    conn = get_connection()
    for data_type in FIELD_MAPPINGS.keys():
        table_name = f"{data_type}_cache"
        conn.sql(f"CREATE INDEX IF NOT EXISTS idx_{data_type}_agency ON {table_name}(agency_code)")
        conn.sql(f"CREATE INDEX IF NOT EXISTS idx_{data_type}_docket_id ON {table_name}(docket_id)")
    logger.info("Indexes created successfully!")

def refresh_cache(agency_code: str, docket_id: str, data_type_enum: RegulationsDataTypes):
    conn = get_connection()
    """Refresh cache for specific agency and data type"""   
    if not agency_code or not data_type_enum:
        raise ValueError("agency_code and data_type_enum are required")
    
    data_type = data_type_enum.value
    logger.info(f"Refreshing {data_type} cache for agency code: {agency_code} and docket id: {docket_id}")
    table_name = f"{data_type}_cache"
    where_clause = _build_where_clause(agency_code, docket_id)
    conn.sql(f"DELETE FROM {table_name} WHERE {where_clause}")
    
    # Insert new data
    query = _build_base_query(data_type, agency_code=agency_code, docket_id=docket_id)
    result = conn.sql(f"SELECT COUNT(*) FROM ({query}) q").fetchone()
    if result[0] == 0:
        raise HTTPException(status_code=404, detail=f"No records found for agency code: {agency_code} and docket id: {docket_id}")
    
    logger.info(f"Inserting {result[0]} records into {table_name}")
    conn.sql(f"""
    INSERT INTO {table_name}
    SELECT *, CURRENT_TIMESTAMP as cached_at
    FROM ({query}) q
    """)
    # Check that rows were inserted
    new_count = conn.sql(f"SELECT COUNT(*) FROM {table_name} WHERE agency_code = '{agency_code}'").fetchone()[0]
    logger.info(f"Inserted {new_count} records into {table_name}")
    logger.info(f"Refreshed {data_type} cache for agency code: {agency_code} successfully!")

def initialize_database():
    """Initialize the database with tables and indexes"""
    create_empty_tables()
    create_indexes()
    logger.info("Database initialized successfully!")
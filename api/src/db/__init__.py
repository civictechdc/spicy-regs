import duckdb


def get_connection():
    """Get a connection to the database."""
    conn = duckdb.connect("main.db")
    conn.sql("INSTALL httpfs;")
    conn.sql("LOAD httpfs;")
    return conn

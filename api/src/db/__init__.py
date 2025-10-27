import duckdb


conn = duckdb.connect("main.db")
conn.sql("INSTALL httpfs;")
conn.sql("LOAD httpfs;")

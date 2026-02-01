import duckdb

DB_PATH = "market_data.duckdb"

def get_duckdb(read_only=True):
    return duckdb.connect(DB_PATH, read_only=read_only)

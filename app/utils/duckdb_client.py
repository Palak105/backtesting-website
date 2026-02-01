import duckdb
import os

def get_duckdb():
    con = duckdb.connect()

    # Enable HTTP / S3 support
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # DuckDB expects hostname ONLY (no https://)
    endpoint = os.environ["R2_ENDPOINT"] \
        .replace("https://", "") \
        .replace("http://", "")

    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{os.environ["R2_ACCESS_KEY"]}';
        SET s3_secret_access_key='{os.environ["R2_SECRET_KEY"]}';
        SET s3_region='auto';
        SET s3_use_ssl=true;
    """)

    return con

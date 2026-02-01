import duckdb
import os

def get_duckdb():
    con = duckdb.connect()

    # Enable HTTP / S3 support
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Cloudflare R2 = S3-compatible
    con.execute(f"""
        SET s3_endpoint='{os.environ["R2_ENDPOINT"]}';
        SET s3_access_key_id='{os.environ["R2_ACCESS_KEY"]}';
        SET s3_secret_access_key='{os.environ["R2_SECRET_KEY"]}';
        SET s3_region='auto';
        SET s3_use_ssl=true;
    """)

    return con

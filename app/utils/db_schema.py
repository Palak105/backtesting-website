from app.utils.duckdb_client import get_duckdb
import os

EXCLUDED_COLUMNS = {
    "Symbol",
    "Date",
    "datetime",
    "timeframe",
    "MarketCapCategory",
    "Industry"
}

def get_indicator_columns():
    con = get_duckdb()
    bucket = os.environ["R2_BUCKET"]

    cols = con.execute(
        f"""
        DESCRIBE
        SELECT *
        FROM 's3://{bucket}/market_data.parquet'
        """
    ).fetchall()

    indicators = []

    for col_name, col_type, *_ in cols:
        if col_name in EXCLUDED_COLUMNS:
            continue

        value_type = "boolean" if col_type.lower() == "boolean" else "number"

        indicators.append({
            "key": col_name,
            "label": col_name.replace("_", " "),
            "valueType": value_type,
            "operators": ["="] if value_type == "boolean"
                         else [">", ">=", "<", "<=", "="]
        })

    return indicators

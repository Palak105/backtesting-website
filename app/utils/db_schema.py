import duckdb
import logging

logger = logging.getLogger(__name__)

DUCKDB_PATH = "market_data.duckdb"

EXCLUDED_COLUMNS = {
    "Symbol",
    "Date",
    "datetime",
    "timeframe",
    "MarketCapCategory",
    "Industry"
}

def get_indicator_columns():
    try:
        con = duckdb.connect(DUCKDB_PATH, read_only=True)

        # ---- get table columns ----
        cols = con.execute(
            "DESCRIBE market_data"
        ).fetchall()
        # DESCRIBE -> (column_name, column_type, null, key, default, extra)

        # ---- indicator metadata (optional table) ----
        meta = {}
        try:
            rows = con.execute(
                "SELECT key, value_type FROM indicator_metadata"
            ).fetchall()
            meta = {k: v for k, v in rows}
        except Exception:
            # metadata table may not exist yet
            meta = {}

        con.close()

        indicators = []

        for col_name, col_type, *_ in cols:
            if col_name in EXCLUDED_COLUMNS:
                continue

            value_type = meta.get(col_name, "number")

            indicators.append({
                "key": col_name,
                "label": col_name.replace("_", " "),
                "valueType": value_type,
                "operators": (
                    ["="] if value_type == "boolean"
                    else [">", ">=", "<", "<=", "="]
                )
            })

        return indicators

    except Exception as e:
        logger.error(f"Failed to load indicator columns: {e}")
        return []

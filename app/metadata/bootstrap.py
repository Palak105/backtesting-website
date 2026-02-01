from sqlalchemy import text
from app.database import engine

EXCLUDED_COLUMNS = {
    "Symbol",
    "Date",
    "datetime",
    "timeframe",
    "MarketCapCategory",
    "Industry"
}

def detect_boolean_columns(conn):
    """
    Boolean = only contains 0/1/NULL
    """
    result = conn.execute(
        text("PRAGMA table_info(market_data)")
    ).fetchall()

    boolean_cols = set()

    for _, col, col_type, *_ in result:
        if col in EXCLUDED_COLUMNS:
            continue

        # only numeric columns are candidates
        if col_type.upper() not in ("INTEGER", "INT", "BOOLEAN"):
            continue

        count = conn.execute(
            text(f'''
                SELECT COUNT(*)
                FROM market_data
                WHERE "{col}" NOT IN (0,1)
                  AND "{col}" IS NOT NULL
                LIMIT 1
            ''')
        ).scalar()

        if count == 0:
            boolean_cols.add(col)

    return boolean_cols


def bootstrap_indicator_metadata():
    with engine.connect() as conn:
        # 1️⃣ Create table if not exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS indicator_metadata (
                key TEXT PRIMARY KEY,
                value_type TEXT NOT NULL
            )
        """))

        # 2️⃣ Fetch existing metadata keys
        existing = {
            r[0]
            for r in conn.execute(text("SELECT key FROM indicator_metadata"))
        }

        # 3️⃣ Get all columns
        columns = conn.execute(
            text("PRAGMA table_info(market_data)")
        ).fetchall()

        boolean_cols = detect_boolean_columns(conn)

        inserts = []

        for _, col, *_ in columns:
            if col in EXCLUDED_COLUMNS:
                continue
            if col in existing:
                continue

            value_type = "boolean" if col in boolean_cols else "number"
            inserts.append({"key": col, "value_type": value_type})

        # 4️⃣ Bulk insert
        if inserts:
            conn.execute(
                text("""
                    INSERT INTO indicator_metadata (key, value_type)
                    VALUES (:key, :value_type)
                """),
                inserts
            )

        conn.commit()

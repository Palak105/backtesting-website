import duckdb
import pandas as pd
import os
from datetime import timedelta

from app.utils.duckdb_client import get_duckdb


def run_backtest(
    entry_df: pd.DataFrame,
    timeframe: str,
    target_pct: float,
    sl_pct: float,
    max_holding_days: int = 30
):
    """
    Simple candle-based backtest:
    - Entry at close
    - Exit on Target / SL / Time exit
    """

    if entry_df.empty:
        return pd.DataFrame()

    # Normalize common column name variants coming from frontend payloads.
    # Accept: Symbol/symbol, Date/date, Close/close (optional).
    df = entry_df.copy()
    rename_map = {}
    if "symbol" in df.columns and "Symbol" not in df.columns:
        rename_map["symbol"] = "Symbol"
    if "date" in df.columns and "Date" not in df.columns:
        rename_map["date"] = "Date"
    if "close" in df.columns and "Close" not in df.columns:
        rename_map["close"] = "Close"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "Symbol" not in df.columns or "Date" not in df.columns:
        return pd.DataFrame()

    # Ensure Date is comparable/usable in DuckDB params.
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df = df[df["Date"].notna()]
    if df.empty:
        return pd.DataFrame()

    con = get_duckdb()
    bucket = os.environ["R2_BUCKET"]

    results = []

    for _, row in df.iterrows():
        symbol = row["Symbol"]
        entry_date = row["Date"]

        entry_price = row["Close"] if "Close" in df.columns else None
        if entry_price is None or (isinstance(entry_price, float) and pd.isna(entry_price)):
            # Look up the entry close for that symbol/date/timeframe.
            price_row = con.execute(
                """
                SELECT Close
                FROM 's3://{bucket}/market_data.parquet'
                WHERE Symbol = ?
                  AND timeframe = ?
                  AND Date = ?
                LIMIT 1
                """.format(bucket=bucket),
                [symbol, timeframe, entry_date],
            ).fetchone()
            entry_price = price_row[0] if price_row else None

        if entry_price is None:
            # Can't backtest without an entry price.
            continue

        target_price = entry_price * (1 + target_pct / 100)
        sl_price = entry_price * (1 - sl_pct / 100)
        max_exit_date = entry_date + timedelta(days=max_holding_days)

        candles = con.execute(
            """
            SELECT Date, High, Low, Close
            FROM 's3://{bucket}/market_data.parquet'
            WHERE Symbol = ?
              AND timeframe = ?
              AND Date > ?
              AND Date <= ?
            ORDER BY Date ASC
            """.format(bucket=bucket),
            [symbol, timeframe, entry_date, max_exit_date]
        ).df()

        exit_reason = "TIME_EXIT"
        exit_price = candles.iloc[-1]["Close"] if not candles.empty else entry_price
        exit_date = candles.iloc[-1]["Date"] if not candles.empty else entry_date

        for _, c in candles.iterrows():
            if c["High"] >= target_price:
                exit_reason = "TARGET"
                exit_price = target_price
                exit_date = c["Date"]
                break

            if c["Low"] <= sl_price:
                exit_reason = "STOPLOSS"
                exit_price = sl_price
                exit_date = c["Date"]
                break

        pnl_pct = ((exit_price - entry_price) / entry_price) * 100

        results.append({
            "symbol": symbol,
            "entry_date": entry_date,
            "entry_price": round(entry_price, 2),
            "exit_date": exit_date,
            "exit_price": round(exit_price, 2),
            "pnl_pct": round(pnl_pct, 2),
            "exit_reason": exit_reason
        })

    con.close()
    return pd.DataFrame(results)

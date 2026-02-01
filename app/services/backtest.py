import duckdb
import pandas as pd
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

    con = get_duckdb()
    bucket = os.environ["R2_BUCKET"]

    df = con.execute(
        f"""
        SELECT Date, High, Low, Close
        FROM 's3://{bucket}/market_data.parquet'
        WHERE Symbol = ?
        AND timeframe = ?
        AND Date > ?
        ORDER BY Date ASC
        """,
        [symbol, timeframe, entry_date]
    ).df()

        exit_reason = "TIME_EXIT"
        exit_price = entry_price
        exit_date = entry_date

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

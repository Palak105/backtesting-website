import duckdb
import pandas as pd
from datetime import timedelta

DB_PATH = "market_data.duckdb"


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

    con = duckdb.connect(DB_PATH, read_only=True)
    results = []

    for _, row in entry_df.iterrows():
        symbol = row["symbol"]
        entry_date = row["date"]
        entry_price = row["close"]

        target_price = entry_price * (1 + target_pct / 100)
        sl_price = entry_price * (1 - sl_pct / 100)

        max_exit_date = entry_date + timedelta(days=max_holding_days)

        candles = con.execute(
            """
            SELECT Date, High, Low, Close
            FROM market_data
            WHERE Symbol = ?
              AND timeframe = ?
              AND Date > ?
              AND Date <= ?
            ORDER BY Date ASC
            """,
            [symbol, timeframe, entry_date, max_exit_date]
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

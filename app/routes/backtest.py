from fastapi import APIRouter, HTTPException
from app.services.backtest import run_backtest

router = APIRouter()

@router.post("/run")
def backtest(payload: dict):
    """
    Expects:
    {
      "timeframe": "1D",
      "targetPct": 5,
      "slPct": 2,
      "entries": [
        { "symbol": "RELIANCE", "date": "2024-01-10", "close": 2450.5 }
      ]
    }
    """

    timeframe = payload.get("timeframe")
    target_pct = payload.get("targetPct")
    sl_pct = payload.get("slPct")
    entries = payload.get("entries")

    if not timeframe or target_pct is None or sl_pct is None or not entries:
        raise HTTPException(status_code=400, detail="Invalid backtest payload")

    try:
        import pandas as pd

        entry_df = pd.DataFrame(entries)

        results_df = run_backtest(
            entry_df=entry_df,
            timeframe=timeframe,
            target_pct=target_pct,
            sl_pct=sl_pct
        )

        return {
            "results": results_df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

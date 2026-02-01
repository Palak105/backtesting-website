from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.filters import router as filter_router
from app.routes import metadata
from app.routes.backtest import router as backtest_router

app = FastAPI(title="Backtesting Platform")

# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- ROUTERS ----------------
app.include_router(metadata.router)
app.include_router(filter_router, prefix="/filters")
app.include_router(backtest_router, prefix="/backtest")

# ---------------- HEALTH ----------------
@app.get("/health")
def health():
    return {"status": "ok"}

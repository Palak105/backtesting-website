from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.filters import router as filter_router
from app.routes import metadata
from app.routes.backtest import router as backtest_router
from app.metadata.bootstrap import bootstrap_indicator_metadata

app = FastAPI(title="Backtesting Platform")

# ---------------- STARTUP ----------------
@app.on_event("startup")
def startup():
    bootstrap_indicator_metadata()

# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # React app (lock later)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- ROUTERS ----------------
app.include_router(metadata.router)
app.include_router(filter_router, prefix="/filters")
app.include_router(backtest_router, prefix="/backtest")

# ---------------- HEALTH CHECK ----------------
@app.get("/health")
def health():
    return {"status": "ok"}

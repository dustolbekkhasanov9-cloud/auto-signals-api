from fastapi import FastAPI, Query
from signal_engine import analyze_symbol

app = FastAPI(title="AutoSignal API")

DEFAULT_SYMBOLS = [
    "EURUSD=X",
    "GBPUSD=X",
    "USDJPY=X",
    "AUDUSD=X",
    "USDCHF=X",
    "USDCAD=X",
    "NZDUSD=X",
    "EURJPY=X",
]


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/signal")
def get_signal(symbol: str = Query(default="EURUSD=X")):
    return analyze_symbol(symbol)


@app.get("/signals")
def get_signals():
    return {"items": [analyze_symbol(symbol) for symbol in DEFAULT_SYMBOLS]}
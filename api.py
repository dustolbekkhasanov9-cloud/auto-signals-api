from contextlib import asynccontextmanager
from datetime import datetime, timezone
import asyncio
import logging
from typing import Dict, Any

from fastapi import FastAPI, Query, HTTPException
from signal_engine import analyze_symbol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autosignal-api")

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

REFRESH_SECONDS = 30

signal_cache: Dict[str, Dict[str, Any]] = {}
last_updated_at: str | None = None
last_refresh_status: str = "starting"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_error_payload(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "price": None,
        "entry_price": None,
        "signal": "NONE",
        "confidence": 0.0,
        "rsi": None,
        "tp": None,
        "sl": None,
        "market_regime": "UNKNOWN",
        "chart_prices": [],
        "chart_labels": [],
        "entry_time": "Нет сигнала",
        "exit_time": "Нет сигнала",
        "reason": reason,
    }


async def analyze_symbol_safe(symbol: str) -> dict:
    try:
        # analyze_symbol синхронная функция, поэтому уводим в отдельный поток
        result = await asyncio.to_thread(analyze_symbol, symbol)
        if not isinstance(result, dict):
            return make_error_payload(symbol, "Некорректный ответ анализа")
        return result
    except Exception as e:
        logger.exception("Ошибка анализа символа %s: %s", symbol, e)
        return make_error_payload(symbol, f"Ошибка анализа: {str(e)}")


async def refresh_all_signals() -> None:
    global signal_cache, last_updated_at, last_refresh_status

    logger.info("Обновление сигналов началось")
    tasks = [analyze_symbol_safe(symbol) for symbol in DEFAULT_SYMBOLS]
    results = await asyncio.gather(*tasks)

    new_cache: Dict[str, Dict[str, Any]] = {}
    for item in results:
        symbol = item.get("symbol")
        if symbol:
            new_cache[symbol] = item

    if new_cache:
        signal_cache = new_cache
        last_updated_at = now_iso()
        last_refresh_status = "ok"
        logger.info("Обновление сигналов завершено: %s символов", len(signal_cache))
    else:
        last_refresh_status = "error"
        logger.warning("Обновление сигналов не дало результатов")


async def background_refresh_loop() -> None:
    global last_refresh_status

    while True:
        try:
            await refresh_all_signals()
        except Exception as e:
            last_refresh_status = "error"
            logger.exception("Ошибка фонового обновления: %s", e)

        await asyncio.sleep(REFRESH_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.refresh_task = asyncio.create_task(background_refresh_loop())

    # первый прогрев сразу при старте
    try:
        await refresh_all_signals()
    except Exception as e:
        logger.exception("Ошибка стартового прогрева: %s", e)

    yield

    refresh_task = getattr(app.state, "refresh_task", None)
    if refresh_task:
        refresh_task.cancel()
        try:
            await refresh_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="AutoSignal API", lifespan=lifespan)


@app.get("/")
def root():
    return {
        "status": "ok",
        "symbols_count": len(DEFAULT_SYMBOLS),
        "cache_size": len(signal_cache),
        "refresh_seconds": REFRESH_SECONDS,
        "last_updated_at": last_updated_at,
        "last_refresh_status": last_refresh_status,
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "cache_ready": len(signal_cache) > 0,
        "last_updated_at": last_updated_at,
        "last_refresh_status": last_refresh_status,
    }


@app.get("/signal")
def get_signal(symbol: str = Query(default="EURUSD=X")):
    if symbol in signal_cache:
        return signal_cache[symbol]

    # если символ не из стандартного списка — можно посчитать отдельно
    if symbol not in DEFAULT_SYMBOLS:
        result = analyze_symbol(symbol)
        return result

    raise HTTPException(status_code=503, detail="Сигнал еще не готов, попробуй через несколько секунд")


@app.get("/signals")
def get_signals():
    items = [signal_cache[symbol] for symbol in DEFAULT_SYMBOLS if symbol in signal_cache]

    return {
        "items": items,
        "meta": {
            "last_updated_at": last_updated_at,
            "last_refresh_status": last_refresh_status,
            "refresh_seconds": REFRESH_SECONDS,
            "count": len(items),
        },
    }


@app.post("/refresh")
async def manual_refresh():
    await refresh_all_signals()
    return {
        "status": "ok",
        "message": "Сигналы обновлены вручную",
        "last_updated_at": last_updated_at,
        "count": len(signal_cache),
    }
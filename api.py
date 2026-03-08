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

DEFAULT_TIMEFRAME = "1h"
DEFAULT_DURATION_TYPE = "short"

REFRESH_SECONDS = 30

signal_cache: Dict[str, Dict[str, Any]] = {}
last_updated_at: str | None = None
last_refresh_status: str = "starting"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_error_payload(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "signal": "NONE",
        "confidence": 0.0,
        "reason": reason,
    }


async def analyze_symbol_safe(symbol: str) -> dict:
    try:
        result = await asyncio.to_thread(
            analyze_symbol,
            symbol,
            DEFAULT_TIMEFRAME,
            DEFAULT_DURATION_TYPE,
        )

        if not isinstance(result, dict):
            return make_error_payload(symbol, "Некорректный ответ анализа")

        return result

    except Exception as e:
        logger.exception("Ошибка анализа %s: %s", symbol, e)
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
        logger.info("Сигналы обновлены: %s", len(signal_cache))
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
def get_signal(
    symbol: str = Query(default="EURUSD=X"),
    timeframe: str = Query(default=DEFAULT_TIMEFRAME),
    duration_type: str = Query(default=DEFAULT_DURATION_TYPE),
):

    # если пользователь запрашивает нестандартный символ
    if symbol not in DEFAULT_SYMBOLS:
        return analyze_symbol(symbol, timeframe, duration_type)

    # если стандартный — берем из кэша
    if symbol in signal_cache:
        cached = signal_cache[symbol].copy()

        # если пользователь указал другой таймфрейм — пересчитываем
        if timeframe != DEFAULT_TIMEFRAME or duration_type != DEFAULT_DURATION_TYPE:
            return analyze_symbol(symbol, timeframe, duration_type)

        return cached

    raise HTTPException(
        status_code=503,
        detail="Сигнал еще не готов, попробуй через несколько секунд",
    )


@app.get("/signals")
def get_signals(
    timeframe: str = Query(default=DEFAULT_TIMEFRAME),
    duration_type: str = Query(default=DEFAULT_DURATION_TYPE),
):

    if timeframe == DEFAULT_TIMEFRAME and duration_type == DEFAULT_DURATION_TYPE:
        items = [signal_cache[s] for s in DEFAULT_SYMBOLS if s in signal_cache]

        return {
            "items": items,
            "meta": {
                "last_updated_at": last_updated_at,
                "last_refresh_status": last_refresh_status,
                "refresh_seconds": REFRESH_SECONDS,
                "count": len(items),
            },
        }

    # если пользователь выбрал другой таймфрейм — считаем на лету
    items = [
        analyze_symbol(symbol, timeframe, duration_type)
        for symbol in DEFAULT_SYMBOLS
    ]

    return {
        "items": items,
        "meta": {
            "timeframe": timeframe,
            "duration_type": duration_type,
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
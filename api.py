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

active_signals: list[dict] = []
signal_history: list[dict] = []

MAX_HISTORY_ITEMS = 300


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_error_payload(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "signal": "NONE",
        "confidence": 0.0,
        "reason": reason,
    }
def add_signals_to_active(items: list[dict]) -> None:
    global active_signals

    for item in items:
        if not isinstance(item, dict):
            continue

        signal = item.get("signal", "NONE")
        symbol = item.get("symbol", "")
        timeframe = item.get("timeframe", DEFAULT_TIMEFRAME)
        duration_type = item.get("duration_type", DEFAULT_DURATION_TYPE)
        entry_time_iso = item.get("entry_time_iso", "")
        strategy = item.get("strategy", "")

        if signal not in ("BUY", "SELL"):
            continue

        signal_id = f"{symbol}_{signal}_{timeframe}_{entry_time_iso}"

        active_item = {
            "symbol": symbol,
            "signal": signal,
            "confidence": item.get("confidence", 0.0),
            "signal_quality": item.get("signal_quality", 0.0),
            "price": item.get("price"),
            "entry_price": item.get("entry_price"),
            "tp": item.get("tp"),
            "sl": item.get("sl"),
            "rsi": item.get("rsi", 0.0),
            "market_regime": item.get("market_regime", "UNKNOWN"),
            "higher_timeframe_bias": item.get("higher_timeframe_bias", "NONE"),
            "strategy": strategy,
            "timeframe": timeframe,
            "duration_type": duration_type,
            "recommended_expiry": item.get("recommended_expiry", ""),
            "entry_time": item.get("entry_time", ""),
            "exit_time": item.get("exit_time", ""),
            "entry_time_iso": entry_time_iso,
            "exit_time_iso": item.get("exit_time_iso", ""),
            "reason": item.get("reason", ""),
            "chart_prices": item.get("chart_prices", []),
            "chart_labels": item.get("chart_labels", []),
            "candle_buy_bonus": item.get("candle_buy_bonus", 0.0),
            "candle_sell_bonus": item.get("candle_sell_bonus", 0.0),
            "level_buy_bonus": item.get("level_buy_bonus", 0.0),
            "level_sell_bonus": item.get("level_sell_bonus", 0.0),
            "trend_strength": item.get("trend_strength", 0.0),
            "volatility_ratio": item.get("volatility_ratio", 0.0),
            "result": "OPEN",
            "saved_at": now_iso(),
        }

        duplicate_exists = any(
            a.get("id") == signal_id
            for a in active_signals
        )

        if not duplicate_exists:
            active_signals.insert(0, active_item)
def add_signals_to_history(items: list[dict]) -> None:
    global signal_history

    for item in items:
        if not isinstance(item, dict):
            continue

        signal = item.get("signal", "NONE")
        symbol = item.get("symbol", "")
        timeframe = item.get("timeframe", DEFAULT_TIMEFRAME)
        duration_type = item.get("duration_type", DEFAULT_DURATION_TYPE)
        entry_time_iso = item.get("entry_time_iso", "")
        strategy = item.get("strategy", "")

        if signal == "NONE":
            continue

        history_item = {
            "symbol": symbol,
            "signal": signal,
            "confidence": item.get("confidence", 0.0),
            "signal_quality": item.get("signal_quality", 0.0),
            "price": item.get("price"),
            "entry_price": item.get("entry_price"),
            "tp": item.get("tp"),
            "sl": item.get("sl"),
            "rsi": item.get("rsi", 0.0),
            "market_regime": item.get("market_regime", "UNKNOWN"),
            "higher_timeframe_bias": item.get("higher_timeframe_bias", "NONE"),
            "strategy": strategy,
            "timeframe": timeframe,
            "duration_type": duration_type,
            "recommended_expiry": item.get("recommended_expiry", ""),
            "entry_time": item.get("entry_time", ""),
            "exit_time": item.get("exit_time", ""),
            "entry_time_iso": entry_time_iso,
            "exit_time_iso": item.get("exit_time_iso", ""),
            "reason": item.get("reason", ""),
            "chart_prices": item.get("chart_prices", []),
            "chart_labels": item.get("chart_labels", []),
            "candle_buy_bonus": item.get("candle_buy_bonus", 0.0),
            "candle_sell_bonus": item.get("candle_sell_bonus", 0.0),
            "level_buy_bonus": item.get("level_buy_bonus", 0.0),
            "level_sell_bonus": item.get("level_sell_bonus", 0.0),
            "trend_strength": item.get("trend_strength", 0.0),
            "volatility_ratio": item.get("volatility_ratio", 0.0),
            "result": "OPEN",
            "saved_at": now_iso(),
        }

        duplicate_exists = any(
            h.get("symbol") == symbol
            and h.get("signal") == signal
            and h.get("timeframe") == timeframe
            and h.get("duration_type") == duration_type
            and h.get("entry_time_iso") == entry_time_iso
            and h.get("strategy") == strategy
            for h in signal_history
        )

        if not duplicate_exists:
            signal_history.insert(0, history_item)

    signal_history = signal_history[:MAX_HISTORY_ITEMS]
def update_closed_history_results() -> None:
    global active_signals, signal_history

    now_utc = datetime.now(timezone.utc)
    still_active: list[dict] = []

    for item in active_signals:
        if item.get("result") != "OPEN":
            still_active.append(item)
            continue

        exit_time_iso = item.get("exit_time_iso", "")
        if not exit_time_iso:
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        try:
            exit_dt = datetime.fromisoformat(exit_time_iso.replace("Z", "+00:00"))
        except Exception:
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        if exit_dt > now_utc:
            still_active.append(item)
            continue

        symbol = item.get("symbol")
        timeframe = item.get("timeframe", DEFAULT_TIMEFRAME)
        duration_type = item.get("duration_type", DEFAULT_DURATION_TYPE)

        if not symbol:
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        try:
            latest = analyze_symbol(symbol, timeframe, duration_type)
        except Exception:
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        current_price = latest.get("price")
        entry_price = item.get("entry_price")
        signal = item.get("signal")

        if current_price is None or entry_price is None or signal not in ("BUY", "SELL"):
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        try:
            current_price = float(current_price)
            entry_price = float(entry_price)
        except Exception:
            item["result"] = "CLOSED"
            signal_history.insert(0, item)
            continue

        if signal == "BUY":
            item["result"] = "TP" if current_price >= entry_price else "SL"
        elif signal == "SELL":
            item["result"] = "TP" if current_price <= entry_price else "SL"
        else:
            item["result"] = "CLOSED"

        signal_history.insert(0, item)

    active_signals = still_active
    signal_history = signal_history[:MAX_HISTORY_ITEMS]

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

            # сохраняем найденные сигналы
            add_signals_to_active(results)

            # сохраняем их снимок в историю

            # проверяем какие сигналы уже закрылись
            update_closed_history_results()

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

@app.get("/active_signals")
def get_active_signals(limit: int = 50):
    open_items = [
        item for item in active_signals
        if item.get("result") == "OPEN"
    ]

    return {
        "items": open_items[:limit],
        "count": len(open_items),
        "limit": limit,
        "last_updated_at": last_updated_at,
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

@app.get("/history")
def get_history(limit: int = 50):
    closed_items = [
        item for item in signal_history
        if item.get("result") in ("TP", "SL", "CLOSED")
    ]

    return {
        "items": closed_items[:limit],
        "count": len(closed_items),
        "limit": limit,
        "last_updated_at": last_updated_at,
    }
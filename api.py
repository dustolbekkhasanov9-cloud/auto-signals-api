from news_feed import build_feed
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
import asyncio
import json
import logging
import os
import sqlite3
import threading
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query

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

SCAN_TIMEFRAMES = ["5m", "10m", "30m", "1h"]
SCAN_DURATION_TYPES = ["short", "long"]

signal_cache: Dict[str, Dict[str, Any]] = {}
scan_cache: Dict[str, Dict[str, Any]] = {}
last_updated_at: str | None = None
last_refresh_status: str = "starting"

active_signals: list[dict] = []
signal_history: list[dict] = []

refresh_lock = asyncio.Lock()

MAX_HISTORY_ITEMS = 300

DB_PATH = os.environ.get("KIKI_DB_PATH", "kiki_state.db")
db_lock = threading.Lock()
DATABASE_URL = os.environ.get("DATABASE_URL")


def parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_cache_key(symbol: str, timeframe: str, duration_type: str) -> str:
    return f"{symbol}|{timeframe}|{duration_type}"


def make_error_payload(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "signal": "NONE",
        "confidence": 0.0,
        "reason": reason,
    }


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_pg_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )


def init_postgres() -> None:
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_signals_pg (
            id SERIAL PRIMARY KEY,
            signal_key TEXT UNIQUE NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_history_pg (
            id SERIAL PRIMARY KEY,
            signal_key TEXT UNIQUE NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

def init_db() -> None:
    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def save_state_to_db() -> None:
    global active_signals, signal_history

    with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO app_state (state_key, state_value)
                VALUES (?, ?)
                ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value
                """,
                ("active_signals", json.dumps(active_signals, ensure_ascii=False)),
            )

            conn.execute(
                """
                INSERT INTO app_state (state_key, state_value)
                VALUES (?, ?)
                ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value
                """,
                ("signal_history", json.dumps(signal_history, ensure_ascii=False)),
            )

            conn.commit()
        finally:
            conn.close()

    try:
        save_state_to_postgres()
    except Exception as e:
        logger.exception("Postgres save failed: %s", e)


def save_state_to_postgres() -> None:
    global active_signals, signal_history

    conn = get_pg_connection()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE active_signals_pg RESTART IDENTITY;")
        cur.execute("TRUNCATE TABLE signal_history_pg RESTART IDENTITY;")

        for item in active_signals:
            signal_key = make_signal_key_str(item)
            cur.execute(
                """
                INSERT INTO active_signals_pg (signal_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                """,
                (signal_key, json.dumps(item, ensure_ascii=False)),
            )

        for item in signal_history:
            signal_key = make_signal_key_str(item)
            cur.execute(
                """
                INSERT INTO signal_history_pg (signal_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                """,
                (signal_key, json.dumps(item, ensure_ascii=False)),
            )

        conn.commit()
    finally:
        cur.close()
        conn.close()

def load_state_from_db() -> None:
    global active_signals, signal_history

    with db_lock:
        conn = get_db_connection()
        try:
            rows = conn.execute(
                "SELECT state_key, state_value FROM app_state"
            ).fetchall()

            loaded: dict[str, str] = {
                row["state_key"]: row["state_value"] for row in rows
            }

            active_raw = loaded.get("active_signals")
            history_raw = loaded.get("signal_history")

            active_signals = json.loads(active_raw) if active_raw else []
            signal_history = json.loads(history_raw) if history_raw else []

            if not isinstance(active_signals, list):
                active_signals = []

            if not isinstance(signal_history, list):
                signal_history = []

            signal_history = signal_history[:MAX_HISTORY_ITEMS]
        finally:
            conn.close()

def load_state_from_postgres() -> None:
    global active_signals, signal_history

    conn = get_pg_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT payload FROM active_signals_pg ORDER BY id ASC;")
        active_rows = cur.fetchall()

        cur.execute("SELECT payload FROM signal_history_pg ORDER BY id DESC;")
        history_rows = cur.fetchall()

        active_signals = [row["payload"] for row in active_rows] if active_rows else []
        signal_history = [row["payload"] for row in history_rows] if history_rows else []

        if not isinstance(active_signals, list):
            active_signals = []

        if not isinstance(signal_history, list):
            signal_history = []

        signal_history = signal_history[:MAX_HISTORY_ITEMS]
    finally:
        cur.close()
        conn.close()

def make_signal_key(item: dict) -> tuple:
    entry_time_iso = item.get("entry_time_iso", "") or ""
    exit_time_iso = item.get("exit_time_iso", "") or ""

    if entry_time_iso:
        try:
            entry_time_iso = parse_iso_utc(entry_time_iso).strftime("%Y-%m-%dT%H:%M")
        except Exception:
            entry_time_iso = str(entry_time_iso)[:16]

    if exit_time_iso:
        try:
            exit_time_iso = parse_iso_utc(exit_time_iso).strftime("%Y-%m-%dT%H:%M")
        except Exception:
            exit_time_iso = str(exit_time_iso)[:16]

    entry_price = item.get("entry_price")
    if entry_price is not None:
        try:
            entry_price = round(float(entry_price), 5)
        except Exception:
            entry_price = None

    return (
        item.get("symbol"),
        item.get("signal"),
        item.get("timeframe"),
        item.get("duration_type"),
        entry_time_iso,
        exit_time_iso,
        entry_price,
    )


def make_signal_key_str(item: dict) -> str:
    key = make_signal_key(item)
    return "|".join("" if x is None else str(x) for x in key)


def deduplicate_active_signals() -> None:
    global active_signals

    seen = set()
    unique_items = []

    for item in active_signals:
        key = make_signal_key(item)
        if key in seen:
            continue
        seen.add(key)
        unique_items.append(item)

    active_signals = unique_items


def history_duplicate_exists(item: dict) -> bool:
    symbol = item.get("symbol")
    signal = item.get("signal")
    timeframe = item.get("timeframe")
    duration_type = item.get("duration_type")
    entry_time_iso = item.get("entry_time_iso")
    exit_time_iso = item.get("exit_time_iso")

    return any(
        h.get("symbol") == symbol
        and h.get("signal") == signal
        and h.get("timeframe") == timeframe
        and h.get("duration_type") == duration_type
        and h.get("entry_time_iso") == entry_time_iso
        and h.get("exit_time_iso") == exit_time_iso
        for h in signal_history
    )


def get_historical_exit_prices_bulk(
    symbol: str, exit_times_iso: list[str]
) -> dict[str, float | None]:
    result: dict[str, float | None] = {}

    if not exit_times_iso:
        return result

    try:
        exit_dts = [parse_iso_utc(x) for x in exit_times_iso]
        min_dt = min(exit_dts) - timedelta(minutes=10)
        max_dt = max(exit_dts) + timedelta(minutes=2)

    try:
        df = yf.download(
            symbol,
            start=min_dt,
            end=max_dt,
            interval="1m",
            progress=False,
        )
    except Exception:
        return {x: None for x in exit_times_iso}

if df is None or len(df) == 0:
    return {x: None for x in exit_times_iso}
    

        if df is None or df.empty:
            return {x: None for x in exit_times_iso}

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        for exit_time_iso in exit_times_iso:
            exit_dt = parse_iso_utc(exit_time_iso)
            eligible = df[df.index <= exit_dt]

            if eligible.empty:
                result[exit_time_iso] = None
                continue

            close_value = eligible.iloc[-1]["Close"]
            if hasattr(close_value, "item"):
                close_value = close_value.item()

            result[exit_time_iso] = float(close_value)

        return result

    except Exception as e:
        logger.exception("Bulk historical price error %s %s", symbol, e)
        return {x: None for x in exit_times_iso}


def finalize_closed_signal(
    item: dict,
    exit_price: float | None,
    close_reason: str = ""
) -> dict:
    item = dict(item)
    item["closed_at_iso"] = now_iso()
    item["close_reason"] = close_reason or ""
    item["status"] = "history"

    entry_price = item.get("entry_price")
    signal = item.get("signal")

    if entry_price is None or signal not in ("BUY", "SELL"):
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        return item

    try:
        entry_price = float(entry_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        return item

    if exit_price is None:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        return item

    try:
        exit_price = float(exit_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        return item

    item["exit_price"] = round(exit_price, 5)

    if signal == "BUY":
        profit_value = exit_price - entry_price
        item["result"] = "TP" if exit_price >= entry_price else "SL"
    else:
        profit_value = entry_price - exit_price
        item["result"] = "TP" if exit_price <= entry_price else "SL"

    profit_percent = (profit_value / entry_price * 100) if entry_price else 0.0

    item["profit_value"] = round(profit_value, 5)
    item["profit_percent"] = round(profit_percent, 3)

    return item

def add_signals_to_active(items: list[dict]) -> None:
    global active_signals

    existing_keys = {make_signal_key(s) for s in active_signals}
    state_changed = False

    for item in items:
        signal = item.get("signal")
        entry_time_iso = item.get("entry_time_iso")

        if signal not in ("BUY", "SELL"):
            continue

        if not entry_time_iso:
            continue

        key = make_signal_key(item)

        if key in existing_keys:
            logger.info("DUPLICATE SKIPPED: %s", key)
            continue

        logger.info("ACTIVE SIGNAL ADDED: %s", key)

        active_signals.append(
            {
                **item,
                "status": "active",
                "closed_at_iso": None,
                "result": "OPEN",
            }
        )
        existing_keys.add(key)
        state_changed = True

    if state_changed:
        save_state_to_db()


def update_closed_history_results() -> None:
    global active_signals, signal_history

    now_utc = datetime.now(timezone.utc)
    still_active: list[dict] = []
    expired_items_by_symbol: dict[str, list[dict]] = {}

    for item in active_signals:
        if item.get("result") != "OPEN":
            continue

        exit_time_iso = item.get("exit_time_iso", "")

        if not exit_time_iso:
            closed_item = finalize_closed_signal(
                item,
                exit_price=None,
                close_reason="missing_exit_time"
            )
            if not history_duplicate_exists(closed_item):
                signal_history.insert(0, closed_item)
            continue

        try:
            exit_dt = parse_iso_utc(exit_time_iso)
        except Exception:
            closed_item = finalize_closed_signal(
                item,
                exit_price=None,
                close_reason="bad_exit_time"
            )
            if not history_duplicate_exists(closed_item):
                signal_history.insert(0, closed_item)
            continue

        if exit_dt > now_utc:
            still_active.append(item)
            continue

        symbol = item.get("symbol")
        if not symbol:
            closed_item = finalize_closed_signal(
                item,
                exit_price=None,
                close_reason="missing_symbol"
            )
            if not history_duplicate_exists(closed_item):
                signal_history.insert(0, closed_item)
            continue

        expired_items_by_symbol.setdefault(symbol, []).append(item)

    for symbol, items in expired_items_by_symbol.items():
        exit_times_iso = [
            item.get("exit_time_iso")
            for item in items
            if item.get("exit_time_iso")
        ]

        price_map = get_historical_exit_prices_bulk(symbol, exit_times_iso)

        for item in items:
            exit_time_iso = item.get("exit_time_iso", "")
            exit_price = price_map.get(exit_time_iso)

            if exit_price is None:
                closed_item = finalize_closed_signal(
                    item,
                    exit_price=None,
                    close_reason="exit_price_not_found"
                )
                if not history_duplicate_exists(closed_item):
                    signal_history.insert(0, closed_item)
                continue

            closed_item = finalize_closed_signal(
                item,
                exit_price=exit_price,
                close_reason="market_price_found"
            )

            if not history_duplicate_exists(closed_item):
                signal_history.insert(0, closed_item)

    active_signals = still_active
    signal_history = signal_history[:MAX_HISTORY_ITEMS]

    logger.info(
        "HISTORY UPDATE: active=%s history=%s waiting=%s tp=%s sl=%s",
        len(active_signals),
        len(signal_history),
        len([x for x in signal_history if x.get("result") == "WAITING_RESULT"]),
        len([x for x in signal_history if x.get("result") == "TP"]),
        len([x for x in signal_history if x.get("result") == "SL"]),
    )

    save_state_to_db()


def update_waiting_history_results() -> None:
    global signal_history

    waiting_items_by_symbol: dict[str, list[dict]] = {}

    for item in signal_history:
        if item.get("result") != "WAITING_RESULT":
            continue

        symbol = item.get("symbol")
        exit_time_iso = item.get("exit_time_iso")

        if not symbol or not exit_time_iso:
            continue

        waiting_items_by_symbol.setdefault(symbol, []).append(item)

    updated = False

    for symbol, items in waiting_items_by_symbol.items():
        exit_times_iso = [
            item.get("exit_time_iso")
            for item in items
            if item.get("exit_time_iso")
        ]

        price_map = get_historical_exit_prices_bulk(symbol, exit_times_iso)

        for idx, hist_item in enumerate(signal_history):
            if hist_item.get("result") != "WAITING_RESULT":
                continue
            if hist_item.get("symbol") != symbol:
                continue

            exit_time_iso = hist_item.get("exit_time_iso", "")
            exit_price = price_map.get(exit_time_iso)

            if exit_price is None:
                continue

            signal_history[idx] = finalize_closed_signal(
                hist_item,
                exit_price=exit_price,
                close_reason="waiting_result_resolved"
            )
            updated = True

    if updated:
        signal_history = signal_history[:MAX_HISTORY_ITEMS]

        logger.info(
            "WAITING RESOLVE: history=%s waiting=%s tp=%s sl=%s",
            len(signal_history),
            len([x for x in signal_history if x.get("result") == "WAITING_RESULT"]),
            len([x for x in signal_history if x.get("result") == "TP"]),
            len([x for x in signal_history if x.get("result") == "SL"]),
        )

    save_state_to_db()
    
async def analyze_symbol_safe(symbol: str, timeframe: str, duration_type: str) -> dict:
    try:
        result = await asyncio.to_thread(
            analyze_symbol,
            symbol,
            timeframe,
            duration_type,
        )

        if not isinstance(result, dict):
            return make_error_payload(symbol, "Некорректный ответ анализа")

        return result

    except Exception as e:
        logger.exception(
            "Ошибка анализа %s %s %s: %s",
            symbol,
            timeframe,
            duration_type,
            e,
        )
        return make_error_payload(symbol, f"Ошибка анализа: {str(e)}")


async def refresh_all_signals() -> None:
    global signal_cache, scan_cache, last_updated_at, last_refresh_status

    async with refresh_lock:
        logger.info("Фоновое обновление сигналов началось")

        tasks = []
        task_keys = []

        for symbol in DEFAULT_SYMBOLS:
            for timeframe in SCAN_TIMEFRAMES:
                for duration_type in SCAN_DURATION_TYPES:
                    tasks.append(analyze_symbol_safe(symbol, timeframe, duration_type))
                    task_keys.append((symbol, timeframe, duration_type))

        results = await asyncio.gather(*tasks)

        new_cache: Dict[str, Dict[str, Any]] = {}
        new_scan_cache: Dict[str, Dict[str, Any]] = {}
        all_results: list[dict] = []

        for i, item in enumerate(results):
            symbol, timeframe, duration_type = task_keys[i]

            if not isinstance(item, dict):
                continue

            item["symbol"] = item.get("symbol", symbol)
            item["timeframe"] = item.get("timeframe", timeframe)
            item["duration_type"] = item.get("duration_type", duration_type)

            all_results.append(item)

            cache_key = make_cache_key(symbol, timeframe, duration_type)
            new_scan_cache[cache_key] = item

            if timeframe == DEFAULT_TIMEFRAME and duration_type == DEFAULT_DURATION_TYPE:
                item_symbol = item.get("symbol")
                if item_symbol:
                    new_cache[item_symbol] = item

        if new_cache:
            signal_cache = new_cache
            scan_cache = new_scan_cache
            last_updated_at = now_iso()
            last_refresh_status = "ok"

            add_signals_to_active(all_results)
            deduplicate_active_signals()
            update_closed_history_results()
            update_waiting_history_results()

            logger.info(
                "Сигналы обновлены: cache=%s, scan_cache=%s, scanned=%s, active=%s, history=%s",
                len(signal_cache),
                len(scan_cache),
                len(all_results),
                len(active_signals),
                len(signal_history),
            )
        else:
            last_refresh_status = "error"
            logger.warning("Фоновое обновление не дало результатов")


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
    init_postgres()

    init_db()

    try:
        load_state_from_postgres()
        logger.info("STATE LOADED FROM POSTGRES")
    except Exception as e:
        logger.exception("POSTGRES LOAD FAILED, FALLBACK SQLITE: %s", e)
        load_state_from_db()

    deduplicate_active_signals()
    update_closed_history_results()
    update_waiting_history_results()
    save_state_to_db()

    try:
        await refresh_all_signals()
    except Exception as e:
        logger.exception("Ошибка стартового прогрева: %s", e)

    app.state.refresh_task = asyncio.create_task(background_refresh_loop())

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
        "scan_cache_size": len(scan_cache),
        "active_signals_count": len(active_signals),
        "history_count": len(signal_history),
        "refresh_seconds": REFRESH_SECONDS,
        "last_updated_at": last_updated_at,
        "last_refresh_status": last_refresh_status,
    }


@app.get("/health")
def health():
    waiting_count = len([x for x in signal_history if x.get("result") == "WAITING_RESULT"])
    tp_count = len([x for x in signal_history if x.get("result") == "TP"])
    sl_count = len([x for x in signal_history if x.get("result") == "SL"])

    return {
        "status": "ok",
        "cache_ready": len(signal_cache) > 0,
        "scan_cache_ready": len(scan_cache) > 0,
        "active_signals_count": len(active_signals),
        "history_count": len(signal_history),
        "history_waiting_count": waiting_count,
        "history_tp_count": tp_count,
        "history_sl_count": sl_count,
        "last_updated_at": last_updated_at,
        "last_refresh_status": last_refresh_status,
    }


@app.get("/signal")
def get_signal(
    symbol: str = Query(default="EURUSD=X"),
    timeframe: str = Query(default=DEFAULT_TIMEFRAME),
    duration_type: str = Query(default=DEFAULT_DURATION_TYPE),
):
    if symbol not in DEFAULT_SYMBOLS:
        raise HTTPException(status_code=404, detail="Символ не поддерживается")

    cache_key = make_cache_key(symbol, timeframe, duration_type)
    cached = scan_cache.get(cache_key)

    if cached:
        return cached.copy()

    raise HTTPException(
        status_code=503,
        detail="Сигнал еще не готов, попробуй через несколько секунд",
    )


@app.get("/signals")
def get_signals(
    timeframe: str = Query(default=DEFAULT_TIMEFRAME),
    duration_type: str = Query(default=DEFAULT_DURATION_TYPE),
):
    items = []

    for symbol in DEFAULT_SYMBOLS:
        cache_key = make_cache_key(symbol, timeframe, duration_type)
        item = scan_cache.get(cache_key)

        if item:
            items.append(item)

    return {
        "items": items,
        "meta": {
            "timeframe": timeframe,
            "duration_type": duration_type,
            "last_updated_at": last_updated_at,
            "last_refresh_status": last_refresh_status,
            "refresh_seconds": REFRESH_SECONDS,
            "count": len(items),
        },
    }


@app.get("/active_signals")
def get_active_signals(limit: int = 150):
    update_closed_history_results()
    update_waiting_history_results()

    now_utc = datetime.now(timezone.utc)
    fresh_items: list[dict] = []
    seen = set()

    for item in active_signals:
        if item.get("result") != "OPEN":
            continue

        exit_time_iso = item.get("exit_time_iso")
        if not exit_time_iso:
            continue

        try:
            exit_dt = parse_iso_utc(exit_time_iso)
        except Exception:
            continue

        if exit_dt <= now_utc:
            continue

        dedupe_key = make_signal_key(item)

        if dedupe_key in seen:
            logger.info("ACTIVE API DEDUPE SKIPPED: %s", dedupe_key)
            continue

        seen.add(dedupe_key)
        fresh_items.append(item)

    return {
        "items": fresh_items[:limit],
        "count": len(fresh_items),
        "limit": limit,
        "last_updated_at": last_updated_at,
        "server_now_utc": now_utc.isoformat(),
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
def get_history(limit: int = 150):
    update_closed_history_results()
    update_waiting_history_results()

    history_items = [
        item for item in signal_history
        if item.get("result") in ("WAITING_RESULT", "TP", "SL")
    ]

    waiting_items = [
        item for item in history_items
        if item.get("result") == "WAITING_RESULT"
    ]

    final_items = [
        item for item in history_items
        if item.get("result") in ("TP", "SL")
    ]

    ordered_items = waiting_items + final_items

    return {
        "items": ordered_items[:limit],
        "count": len(ordered_items),
        "limit": limit,
        "last_updated_at": last_updated_at,
    }


@app.get("/debug/storage")
def debug_storage():
    return {
        "active_signals_count": len(active_signals),
        "history_count": len(signal_history),
        "history_waiting_count": len([x for x in signal_history if x.get("result") == "WAITING_RESULT"]),
        "history_tp_count": len([x for x in signal_history if x.get("result") == "TP"]),
        "history_sl_count": len([x for x in signal_history if x.get("result") == "SL"]),
        "database_url_set": bool(DATABASE_URL),
    }
    
@app.get("/feed")
def get_feed():
    return build_feed()

